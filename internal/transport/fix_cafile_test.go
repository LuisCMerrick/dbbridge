package transport

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"dbbridge/internal/config"
)

func TestFixCAFileTrustsPrivateCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(ca, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	verify := true
	p := &config.PeerConfig{ID: "p", Token: "tok", Endpoints: []config.EndpointConfig{{URL: srv.URL}}}
	p.TLS.Verify = &verify
	p.TLS.CAFile = ca
	c := NewTripleTransportClient(p, &config.TransportConfig{}, "pi_test")
	if c.tlsClientConfig.RootCAs == nil {
		t.Fatal("ca_file not loaded into RootCAs")
	}
	resp, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: c.tlsClientConfig.Clone()}}).Get(srv.URL)
	if err != nil {
		t.Fatalf("private CA not trusted: %v", err)
	}
	resp.Body.Close()

	p.TLS.CAFile = filepath.Join(t.TempDir(), "missing.pem")
	c2 := NewTripleTransportClient(p, &config.TransportConfig{}, "pi_test")
	if _, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: c2.tlsClientConfig.Clone()}}).Get(srv.URL); err == nil {
		t.Fatal("unreadable ca_file must fail closed")
	}
}
