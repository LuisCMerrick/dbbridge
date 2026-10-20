package config

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"dbbridge/internal/fileutil"
)

func pairOptions(t *testing.T) GeneratePairOptions {
	t.Helper()
	o := DefaultGeneratePairOptions()
	o.OutputDir = filepath.Join(t.TempDir(), "pair paths $percent% space")
	return o
}

func loadGeneratedPair(t *testing.T, files GeneratedPairFiles) (*Config, *Config) {
	t.Helper()
	a, err := LoadConfig(files.ConfigA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadConfig(files.ConfigB)
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

func TestGeneratePairModeCarrierRecoveryMatrix(t *testing.T) {
	for _, mode := range []string{"bidirectional", "a-to-b", "b-to-a"} {
		for _, protocol := range []string{"auto", "ws", "http-sse"} {
			for _, recovery := range []bool{false, true} {
				t.Run(mode+"/"+protocol+"/"+map[bool]string{false: "off", true: "on"}[recovery], func(t *testing.T) {
					o := pairOptions(t)
					o.Mode, o.Protocol, o.Recovery = mode, protocol, recovery
					o.A.TargetID, o.B.TargetID = "database-at-a", "database-at-b"
					o.A.TargetAddress, o.B.TargetAddress = "127.0.0.2:3306", "[::1]:1433"
					files, err := GeneratePair(o)
					if err != nil {
						t.Fatal(err)
					}
					a, b := loadGeneratedPair(t, files)
					if a.Node.ID != "A" || b.Node.ID != "B" || !files.TokenCreated {
						t.Fatal("pair IDs or newly generated token differ")
					}
					for i, cfg := range []*Config{a, b} {
						if cfg.Transport.Protocol != protocol || cfg.Transport.Recovery.Enabled != recovery {
							t.Fatal("pair carrier/recovery differs")
						}
						outbound := i == 0 && mode != "b-to-a" || i == 1 && mode != "a-to-b"
						inbound := i == 0 && mode != "a-to-b" || i == 1 && mode != "b-to-a"
						if cfg.HTTP.Enabled != inbound || (len(cfg.Listeners) == 1) != outbound || (len(cfg.Targets) == 1) != inbound {
							t.Fatal("pair direction does not match mode")
						}
					}
					checkDirection := func(local, remote *Config, expectedURL, target, cidr string) {
						t.Helper()
						peer := local.Peers[0]
						if peer.ID != remote.Node.ID || peer.ExpectedNodeID != remote.Node.ID || peer.Endpoints[0].URL != expectedURL || !peer.TLS.IsVerify() {
							t.Fatal("peer identity, endpoint, or TLS verification mismatch")
						}
						if local.Listeners[0].Target != target || remote.Auth.InboundPeers[0].ID != local.Node.ID || remote.Auth.InboundPeers[0].AllowedTargets[0] != target || remote.Targets[target].AllowedCIDRs[0] != cidr {
							t.Fatal("opposite target or inbound ACL mismatch")
						}
						if peer.Token != remote.Auth.InboundPeers[0].Tokens[0] {
							t.Fatal("outbound and inbound authentication tokens differ")
						}
					}
					if mode != "b-to-a" {
						checkDirection(a, b, "http://127.0.0.1:18081", "database-at-b", "::1/128")
					}
					if mode != "a-to-b" {
						checkDirection(b, a, "http://127.0.0.1:18080", "database-at-a", "127.0.0.2/32")
					}
					secret, err := os.ReadFile(files.TokenFile)
					if err != nil || len(bytes.TrimSpace(secret)) != 64 {
						t.Fatal("missing shared random token")
					}
					for _, path := range []string{files.ConfigA, files.ConfigB, files.TokenFile} {
						data, err := os.ReadFile(path)
						if err != nil || path != files.TokenFile && bytes.Contains(data, bytes.TrimSpace(secret)) {
							t.Fatal("config contains secret or generated file is unreadable")
						}
						if runtime.GOOS != "windows" {
							info, _ := os.Stat(path)
							if info.Mode().Perm() != 0600 {
								t.Fatal("generated file is not private")
							}
						}
					}
					assertNoPairTemporaryFiles(t, o.OutputDir)
				})
			}
		}
	}
}

func TestGeneratePairOverwriteAndValidationRollback(t *testing.T) {
	o := pairOptions(t)
	files, err := GeneratePair(o)
	if err != nil {
		t.Fatal(err)
	}
	before := readPairFiles(t, files)
	if _, err = GeneratePair(o); err == nil {
		t.Fatal("overwrote pair without force")
	}
	o.Force, o.A.Listen, o.B.Listen = true, "127.0.0.1:13316", "127.0.0.1:13317"
	files, err = GeneratePair(o)
	if err != nil {
		t.Fatal(err)
	}
	after := readPairFiles(t, files)
	if files.TokenCreated || !bytes.Equal(before[2], after[2]) || bytes.Equal(before[0], after[0]) || bytes.Equal(before[1], after[1]) {
		t.Fatal("force did not update both configs while reusing token")
	}
	o.B.TargetAddress = "database.internal:3306"
	if _, err = GeneratePair(o); err == nil {
		t.Fatal("accepted hostname backend without CIDR")
	}
	if current := readPairFiles(t, files); !bytes.Equal(current[0], after[0]) || !bytes.Equal(current[1], after[1]) || !bytes.Equal(current[2], after[2]) {
		t.Fatal("failed second config validation changed existing pair/token")
	}
	assertNoPairTemporaryFiles(t, o.OutputDir)
	bad := pairOptions(t)
	bad.B.TLSCert, bad.B.TLSKey = "missing.crt", "missing.key"
	if _, err = GeneratePair(bad); err == nil {
		t.Fatal("accepted missing second-node TLS material")
	}
	entries, err := os.ReadDir(bad.OutputDir)
	if err != nil || len(entries) != 0 {
		t.Fatal("failed generation left partial configs or a new token")
	}
}

func TestGeneratePairExistingSecondFileAndTokenPathProtection(t *testing.T) {
	for _, existing := range []string{"peer-b.yaml", "shared.token"} {
		t.Run(existing, func(t *testing.T) {
			o := pairOptions(t)
			if err := os.MkdirAll(o.OutputDir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(o.OutputDir, existing)
			if err := os.WriteFile(path, []byte("preserve-existing\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if existing == "shared.token" {
				o.B.TargetAddress = "invalid-address"
			}
			if _, err := GeneratePair(o); err == nil {
				t.Fatal("accepted existing config or invalid second backend")
			}
			entries, _ := os.ReadDir(o.OutputDir)
			data, _ := os.ReadFile(path)
			if len(entries) != 1 || string(data) != "preserve-existing\n" {
				t.Fatal("existing data changed or partial pair left behind")
			}
		})
	}
	for _, name := range []string{"peer-a.yaml", "peer-b.yaml"} {
		o := pairOptions(t)
		o.TokenFile = filepath.Join(o.OutputDir, name)
		if _, err := GeneratePair(o); err == nil {
			t.Fatal("accepted configuration as shared token path")
		}
	}
}

func TestPublishPairRollbackOnSecondWriteFailure(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "replace"}[force], func(t *testing.T) {
			dir := t.TempDir()
			outputs := []pairOutput{{path: filepath.Join(dir, "peer-a.yaml"), data: []byte("new-a")}, {path: filepath.Join(dir, "peer-b.yaml"), data: []byte("new-b")}}
			if force {
				for _, output := range outputs {
					if err := os.WriteFile(output.path, []byte("old-"+filepath.Base(output.path)), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			keep, err := publishPair(outputs, force, func(path string, data []byte, mode fs.FileMode) (bool, error) {
				if path == outputs[1].path {
					return false, errors.New("injected second publication failure")
				}
				return fileutil.WriteNew(path, data, mode)
			})
			if err == nil || keep {
				t.Fatal("second write failure did not report successful rollback")
			}
			for _, output := range outputs {
				data, readErr := os.ReadFile(output.path)
				if force && (readErr != nil || string(data) != "old-"+filepath.Base(output.path)) || !force && !os.IsNotExist(readErr) {
					t.Fatal("original pair was not restored")
				}
			}
			assertNoPairTemporaryFiles(t, dir)
		})
	}
}

func TestGeneratePairEndpointsAndExternalToken(t *testing.T) {
	o := pairOptions(t)
	o.A.NodeID, o.B.NodeID = "factory", "cloud"
	o.A.TargetAddress, o.A.AllowedCIDRs = "database.internal:1433", []string{"10.0.0.0/8"}
	o.A.PeerURLs = []string{"https://b-primary.example.com", "https://b-backup.example.com"}
	o.B.PeerURLs = []string{"https://a.example.com"}
	o.TokenFile = filepath.Join(t.TempDir(), "common.token")
	files, err := GeneratePair(o)
	if err != nil {
		t.Fatal(err)
	}
	a, b := loadGeneratedPair(t, files)
	if len(a.Peers[0].Endpoints) != 2 || a.Peers[0].Endpoints[1].Priority != 2 || a.Peers[0].Endpoints[1].URL != o.A.PeerURLs[1] || b.Peers[0].Endpoints[0].URL != o.B.PeerURLs[0] {
		t.Fatal("per-node failover endpoints differ")
	}
	if a.Peers[0].Token != b.Auth.InboundPeers[0].Tokens[0] || b.Peers[0].Token != a.Auth.InboundPeers[0].Tokens[0] {
		t.Fatal("external shared token references differ")
	}
	for _, tc := range []struct{ listen, cert, expected string }{
		{"0.0.0.0:8080", "", "http://127.0.0.1:8080"},
		{"[::]:8443", "cert", "https://[::1]:8443"},
		{"192.0.2.1:8080", "", "http://192.0.2.1:8080"},
	} {
		url, err := pairEndpoint(GeneratePairNodeOptions{HTTPListen: tc.listen, TLSCert: tc.cert})
		if err != nil || url != tc.expected {
			t.Fatalf("endpoint=%q error=%v", url, err)
		}
	}
}

func TestGeneratePairTLSAndRemotePlaintextPolicy(t *testing.T) {
	o := pairOptions(t)
	o.A.TLSCert, o.A.TLSKey = testCertificatePair(t)
	o.B.TLSCert, o.B.TLSKey = testCertificatePair(t)
	files, err := GeneratePair(o)
	if err != nil {
		t.Fatal(err)
	}
	a, b := loadGeneratedPair(t, files)
	if !a.HTTP.TLS.Enabled || !b.HTTP.TLS.Enabled || a.Peers[0].Endpoints[0].URL != "https://127.0.0.1:18081" || b.Peers[0].Endpoints[0].URL != "https://127.0.0.1:18080" {
		t.Fatal("TLS settings did not determine both remote endpoint schemes")
	}
	o = pairOptions(t)
	o.B.PeerURLs = []string{"http://192.0.2.1:18080"}
	if _, err = GeneratePair(o); err == nil {
		t.Fatal("remote plaintext was enabled implicitly")
	}
	entries, _ := os.ReadDir(o.OutputDir)
	if len(entries) != 0 {
		t.Fatal("rejected second endpoint left a partial pair or token")
	}
	o.AllowInsecureHTTP = true
	if _, err = GeneratePair(o); err != nil {
		t.Fatal(err)
	}
}

func TestGeneratePairRejectsNonRegularAndAliasedOutputs(t *testing.T) {
	o := pairOptions(t)
	o.Force = true
	if err := os.MkdirAll(filepath.Join(o.OutputDir, "peer-b.yaml"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := GeneratePair(o); err == nil {
		t.Fatal("force replaced a directory")
	}
	for _, link := range []struct {
		name string
		make func(string, string) error
	}{{"hardlink", os.Link}, {"symlink", os.Symlink}} {
		t.Run(link.name, func(t *testing.T) {
			o := pairOptions(t)
			o.Force = true
			if err := os.MkdirAll(o.OutputDir, 0700); err != nil {
				t.Fatal(err)
			}
			o.TokenFile = filepath.Join(o.OutputDir, "existing.token")
			const original = "keep-this-existing-token\n"
			if err := os.WriteFile(o.TokenFile, []byte(original), 0600); err != nil {
				t.Fatal(err)
			}
			if err := link.make(o.TokenFile, filepath.Join(o.OutputDir, "peer-b.yaml")); err != nil {
				t.Skipf("link unavailable: %v", err)
			}
			if _, err := GeneratePair(o); err == nil {
				t.Fatal("accepted aliased config/token output")
			}
			data, err := os.ReadFile(o.TokenFile)
			if err != nil || string(data) != original {
				t.Fatal("aliased output changed the existing token")
			}
		})
	}
}

func assertNoPairTemporaryFiles(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".dbbridge-pair-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary pair files remain: %v, %v", matches, err)
	}
}

func readPairFiles(t *testing.T, files GeneratedPairFiles) [3][]byte {
	t.Helper()
	var data [3][]byte
	for i, path := range []string{files.ConfigA, files.ConfigB, files.TokenFile} {
		var err error
		data[i], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	return data
}
