package auth

import (
	"dbbridge/internal/config"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInvalidAuthSnapshotRetainsOld(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "auth.yaml")
	t.Setenv("DBB_TEST_AUTH_TOKEN", "old-secret")
	valid := []byte("version: 1\ninbound_peers:\n- id: a\n  token_envs: [DBB_TEST_AUTH_TOKEN]\n  allowed_targets: [db]\n")
	os.WriteFile(p, valid, 0600)
	a := NewAuthenticator(&config.Config{ConfigFileDir: d, Auth: config.AuthConfig{CredentialsFile: p, ReloadInterval: time.Hour}})
	defer a.Close()
	if _, e := a.AuthenticateBearer("Bearer old-secret"); e != nil {
		t.Fatal(e)
	}
	os.WriteFile(p, []byte("version: 999\nunknown_security_field: true\ninbound_peers: []\n"), 0600)
	future := time.Now().Add(time.Second)
	os.Chtimes(p, future, future)
	a.loadCredentialsFile()
	if _, e := a.AuthenticateBearer("Bearer old-secret"); e != nil {
		t.Fatal("invalid version/unknown fields replaced last valid authentication snapshot")
	}
}
func TestTokenUniqueness(t *testing.T) {
	a := NewAuthenticator(&config.Config{Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "a", Tokens: []string{"same"}, AllowedTargets: []string{"db-a"}}, {ID: "b", Tokens: []string{"same"}, AllowedTargets: []string{"db-b"}}}}})
	defer a.Close()
	if _, err := a.AuthenticateBearer("Bearer same"); err == nil {
		t.Fatal("same token assigned to two peer identities, map iteration determines authenticated owner")
	}
}
