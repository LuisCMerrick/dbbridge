package config

import (
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"testing"

	"dbbridge/internal/fileutil"
)

// A file that was created before its publication failed (for example a
// failed directory sync) is rolled back with the rest of the pair, so no
// configuration is left behind that references a token the caller removes.
func TestPublishPairRollsBackFileCreatedBeforeFailure(t *testing.T) {
	dir := t.TempDir()
	outputs := []pairOutput{{path: filepath.Join(dir, "peer-a.yaml"), data: []byte("a\n")}, {path: filepath.Join(dir, "peer-b.yaml"), data: []byte("b\n")}}
	n := 0
	keep, err := publishPair(outputs, false, func(path string, data []byte, mode fs.FileMode) (bool, error) {
		created, err := fileutil.WriteNew(path, data, mode)
		if err != nil {
			return created, err
		}
		n++
		if n == 2 {
			return true, errors.New("injected directory sync failure after creation")
		}
		return true, nil
	})
	if err == nil || keep {
		t.Fatalf("err=%v keepToken=%v", err, keep)
	}
	for _, output := range outputs {
		if _, statErr := os.Stat(output.path); !os.IsNotExist(statErr) {
			t.Errorf("rollback left %s", filepath.Base(output.path))
		}
	}
}

// The quota sum of several inbound identities saturates instead of wrapping,
// so huge quotas cannot pass the partition check.
func TestInboundQuotaSumDoesNotOverflow(t *testing.T) {
	f := &AuthConfigFile{Version: 1, InboundPeers: []InboundPeerConfig{
		{ID: "peer-a", MaxSessions: math.MaxInt},
		{ID: "peer-b", MaxSessions: math.MaxInt},
	}}
	if _, err := ValidateCredentialSnapshot(f, CredentialConstraints{MaxSessions: 200}); err == nil {
		t.Fatal("quota partition with overflowing sum accepted")
	}
	f.InboundPeers = []InboundPeerConfig{{ID: "peer-a", MaxSessions: 100}, {ID: "peer-b", MaxSessions: 100}}
	if _, err := ValidateCredentialSnapshot(f, CredentialConstraints{MaxSessions: 200}); err != nil {
		t.Fatalf("exact partition rejected: %v", err)
	}
}

// transport.h2_pool_size is bounded.
func TestH2PoolSizeBound(t *testing.T) {
	for _, tc := range []struct {
		size  int
		valid bool
	}{{1, true}, {MaxH2PoolSize, true}, {MaxH2PoolSize + 1, false}, {4096, false}} {
		c := &Config{Node: NodeConfig{ID: "A"}, Transport: TransportConfig{H2PoolSize: tc.size}}
		if err := c.ApplyDefaultsAndValidate(); (err == nil) != tc.valid {
			t.Errorf("h2_pool_size=%d: err=%v", tc.size, err)
		}
	}
}
