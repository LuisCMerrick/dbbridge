package config

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func generateOptions(t *testing.T, role string) GenerateOptions {
	t.Helper()
	return GenerateOptions{Output: filepath.Join(t.TempDir(), "peer.yaml"), Role: role, NodeID: "A", PeerID: "B", Listen: "127.0.0.1:13306", HTTPListen: "127.0.0.1:18080", TargetID: "mysql", TargetAddress: "127.0.0.1:3306", Protocol: "auto", Recovery: true, MaxSessions: 200}
}
func TestGenerateRoleCarrierRecoveryMatrix(t *testing.T) {
	for _, role := range []string{"ingress", "egress", "peer"} {
		for _, mode := range []string{"auto", "ws", "http-sse"} {
			for _, recovery := range []bool{false, true} {
				t.Run(role+"/"+mode+"/"+map[bool]string{false: "off", true: "on"}[recovery], func(t *testing.T) {
					o := generateOptions(t, role)
					o.Protocol = mode
					o.Recovery = recovery
					o.PeerURLs = []string{"https://primary.invalid", "https://backup.invalid"}
					files, err := Generate(o)
					if err != nil {
						t.Fatal(err)
					}
					cfg, err := LoadConfig(files.Config)
					if err != nil {
						t.Fatal(err)
					}
					if cfg.Transport.Protocol != mode || cfg.Transport.Recovery.Enabled != recovery || cfg.HTTP.Enabled != (role != "ingress") || (len(cfg.Listeners) > 0) != (role != "egress") {
						t.Fatal("generated role/carrier/recovery differs")
					}
					if role != "egress" && (len(cfg.Peers[0].Endpoints) != 2 || !cfg.Peers[0].TLS.IsVerify()) {
						t.Fatal("failover or TLS verification missing")
					}
					if role != "ingress" && cfg.Targets["mysql"].AllowedCIDRs[0] != "127.0.0.1/32" {
						t.Fatal("target allowlist not restricted")
					}
					secret, err := os.ReadFile(files.TokenFile)
					if err != nil || len(bytes.TrimSpace(secret)) != 64 {
						t.Fatal("missing random token")
					}
					if runtime.GOOS != "windows" {
						info, _ := os.Stat(files.TokenFile)
						if info.Mode().Perm() != 0600 {
							t.Fatal("token permissions")
						}
					}
				})
			}
		}
	}
}
func TestGenerateOverwriteAndRollback(t *testing.T) {
	o := generateOptions(t, "ingress")
	files, err := Generate(o)
	if err != nil {
		t.Fatal(err)
	}
	original, _ := os.ReadFile(files.Config)
	token, _ := os.ReadFile(files.TokenFile)
	if _, err = Generate(o); err == nil {
		t.Fatal("overwrote config without force")
	}
	o.Force = true
	o.Listen = "127.0.0.1:13307"
	if _, err = Generate(o); err != nil {
		t.Fatal(err)
	}
	updated, _ := os.ReadFile(files.Config)
	sameToken, _ := os.ReadFile(files.TokenFile)
	if bytes.Equal(original, updated) || !bytes.Equal(token, sameToken) {
		t.Fatal("force did not replace only config")
	}
	o.Protocol = "unsupported"
	if _, err = Generate(o); err == nil {
		t.Fatal("invalid config generated")
	}
	after, _ := os.ReadFile(files.Config)
	if !bytes.Equal(updated, after) {
		t.Fatal("failed generation changed existing config")
	}
	bad := generateOptions(t, "peer")
	bad.Protocol = "unsupported"
	if _, err = Generate(bad); err == nil {
		t.Fatal("invalid mode accepted")
	}
	if _, err = os.Stat(bad.Output); !os.IsNotExist(err) {
		t.Fatal("invalid config left behind")
	}
	if _, err = os.Stat(filepath.Join(filepath.Dir(bad.Output), "peer.token")); !os.IsNotExist(err) {
		t.Fatal("new secret left behind after failed validation")
	}
}
func TestGenerateSharedTokenAndHostnamePolicy(t *testing.T) {
	a := generateOptions(t, "egress")
	a.NodeID = "B"
	a.PeerID = "A"
	server, err := Generate(a)
	if err != nil {
		t.Fatal(err)
	}
	b := generateOptions(t, "ingress")
	b.TokenFile = server.TokenFile
	client, err := Generate(b)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := LoadConfig(server.Config)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := LoadConfig(client.Config)
	if err != nil {
		t.Fatal(err)
	}
	if ca.Peers[0].Token != cb.Auth.InboundPeers[0].Tokens[0] || client.TokenCreated {
		t.Fatal("matching peers do not share the existing token")
	}
	o := generateOptions(t, "egress")
	o.TargetAddress = "db.internal:3306"
	if _, err = Generate(o); err == nil {
		t.Fatal("hostname target silently got an unrestricted CIDR policy")
	}
	o.AllowedCIDRs = []string{"10.0.0.0/8"}
	if _, err = Generate(o); err != nil {
		t.Fatal(err)
	}
}
func TestGenerateTLSMaterialValidation(t *testing.T) {
	o := generateOptions(t, "egress")
	cert, key := testCertificatePair(t)
	o.TLSCert = cert
	o.TLSKey = key
	if _, err := Generate(o); err != nil {
		t.Fatal(err)
	}
	o = generateOptions(t, "egress")
	o.TLSCert = "missing.crt"
	o.TLSKey = "missing.key"
	if _, err := Generate(o); err == nil {
		t.Fatal("accepted missing TLS material")
	}
}
