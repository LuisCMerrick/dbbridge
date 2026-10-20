package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testCertificatePair(t *testing.T) (string, string) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func testProductionExample(t *testing.T, node string) *Config {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", "configs", "production"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "peer-"+node+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	cert, key := testCertificatePair(t)
	s := strings.ReplaceAll(string(b), "/etc/dbbridge/tls/"+node+".crt", filepath.ToSlash(cert))
	s = strings.ReplaceAll(s, "/etc/dbbridge/tls/"+node+".key", filepath.ToSlash(key))
	// The credentials file references tokens/*.token next to it.
	work := t.TempDir()
	auth, err := os.ReadFile(filepath.Join(dir, "auth-"+node+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(work, "tokens"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a-to-b", "b-to-a"} {
		if err = os.WriteFile(filepath.Join(work, "tokens", name+".token"), []byte("example-"+name+"-0123456789abcdef\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(work, "auth-"+node+".yaml"), auth, 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(work, "config.yaml")
	if err = os.WriteFile(path, []byte(s), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestTLSCertificatePreflight(t *testing.T) {
	cert, key := testCertificatePair(t)
	_, wrongKey := testCertificatePair(t)
	for _, tc := range []struct {
		name, cert, key string
		valid           bool
	}{
		{"valid_relative_paths", filepath.Base(cert), filepath.Base(key), true},
		{"mismatched_key", cert, wrongKey, false},
		{"malformed_certificate", key, key, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{Node: NodeConfig{ID: "B"}, ConfigFileDir: filepath.Dir(cert), HTTP: HTTPConfig{Enabled: true, TLS: TLSConfig{Enabled: true, CertFile: tc.cert, KeyFile: tc.key}}}
			if err := c.ApplyDefaultsAndValidate(); (err == nil) != tc.valid {
				t.Fatalf("valid=%t error=%v", tc.valid, err)
			}
		})
	}
}
