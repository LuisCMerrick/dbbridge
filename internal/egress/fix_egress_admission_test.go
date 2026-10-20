package egress

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/session"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// egrStart starts a listening server with a short pre-auth timeout.
func egrStart(t *testing.T, cfg *config.Config, preAuth time.Duration) (*Server, string) {
	t.Helper()
	a := auth.NewAuthenticator(cfg)
	t.Cleanup(a.Close)
	s := NewServer(cfg, a, session.NewRegistry(1<<20, 1<<22))
	s.preAuthTimeout = preAuth
	if e := s.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, s.listener.Addr().String()
}

func dialFrom(t *testing.T, ip, addr string) net.Conn {
	t.Helper()
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(ip)}, Timeout: time.Second}
	c, e := d.Dial("tcp", addr)
	if e != nil {
		t.Skipf("cannot dial from %s: %v", ip, e)
	}
	return c
}

func probeStatus(t *testing.T, addr, token string) (int, error) {
	t.Helper()
	b, _ := json.Marshal(protocol.ProbeRequest{ProtocolVersion: "1.4", Nonce: "n"})
	r, _ := http.NewRequest("POST", "http://"+addr+"/v1/peer/probe", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	cl := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, e := cl.Do(r)
	if e != nil {
		return 0, e
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// X-SEC-1/EGR-3: silent unauthenticated sockets neither charge the
// data-plane metadata budget nor fill the connection limit: one IP holds only
// a bounded number of them, each for at most the pre-auth timeout, and an
// authenticated client still gets through.
func TestFixXSEC1UnauthenticatedSocketsBounded(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Resources = resource.NewManager(256<<20, 32<<20, 32<<20, 1) // 68 inbound connections
	_, addr := egrStart(t, cfg, 400*time.Millisecond)
	var hogs []net.Conn
	defer func() {
		for _, c := range hogs {
			c.Close()
		}
	}()
	for i := 0; i < 68; i++ {
		hogs = append(hogs, dialFrom(t, "127.0.0.2", addr))
	}
	time.Sleep(100 * time.Millisecond)
	if used := cfg.Resources.Metadata.Used(); used != 0 {
		t.Fatalf("sockets charged %d bytes of data-plane metadata", used)
	}
	if used := cfg.Resources.InboundConnections.Used(); used > 17 {
		t.Fatalf("one IP holds %d unauthenticated sockets", used)
	}
	if st, e := probeStatus(t, addr, "tokA"); e != nil || st != 200 {
		t.Fatalf("authenticated probe while hogged: %d %v", st, e)
	}
	// Silent sockets are closed after the pre-auth timeout.
	c := hogs[0]
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	start := time.Now()
	if _, e := c.Read(make([]byte, 1)); e == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("silent socket kept for %v (err %v)", time.Since(start), e)
	}
}

// EGR-3: a 401 closes the connection instead of keeping it alive.
func TestFixEGR3UnauthorizedClosesKeepAlive(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	_, addr := egrStart(t, cfg, 0)
	c, e := net.Dial("tcp", addr)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	fmt.Fprintf(c, "POST /v1/peer/probe HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer wrong\r\nContent-Length: 0\r\n\r\n")
	br := bufio.NewReader(c)
	resp, e := http.ReadResponse(br, nil)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 || !resp.Close {
		t.Fatalf("status %d close %v", resp.StatusCode, resp.Close)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, e := io.ReadAll(br); e != nil {
		t.Fatalf("connection still open after 401: %v", e)
	}
	for _, path := range []string{"/nope", "/v1/sessions"} {
		r, _ := http.Get("http://" + addr + path)
		r.Body.Close()
		if !r.Close || (r.StatusCode != 404 && r.StatusCode != 405) {
			t.Fatalf("%s: %d close=%v", path, r.StatusCode, r.Close)
		}
	}
}

// X-SEC-2: an unauthenticated request is refused with 401 before it takes a
// request slot or request metadata.
func TestFixXSEC2AuthenticateBeforeBudgets(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	_, ts := egrServer(t, cfg)
	limit := cfg.Resources.ControlRequests.Limit()
	if !cfg.Resources.ControlRequests.Try(limit) {
		t.Fatal("setup")
	}
	defer cfg.Resources.ControlRequests.Put(limit)
	r, _ := http.NewRequest("POST", ts.URL+"/v1/peer/probe", strings.NewReader("{}"))
	r.Header.Set("Authorization", "Bearer wrong")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("unauthenticated request with budgets exhausted: %d, want 401", resp.StatusCode)
	}
	if cfg.Resources.ControlMetadata.Used() != 0 || cfg.Resources.ProbeMetadata.Used() != 0 {
		t.Fatal("unauthenticated request charged metadata")
	}
}

// X-SEC-3: one peer trickling control bodies holds at most its share of the
// control request slots; another peer can still DELETE its session.
func TestFixXSEC3ControlBudgetPerPeer(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{
		{ID: "V", Tokens: []string{"tokV"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
		{ID: "G", Tokens: []string{"tokG"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
	})
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	_, addr := egrStart(t, cfg, 0)
	url := "http://" + addr
	_, _, victim := egrCreate(t, url, "tokV", "v1", "db")
	_, _, greedy := egrCreate(t, url, "tokG", "g1", "db")
	if victim.SessionID == "" || greedy.SessionID == "" {
		t.Fatal("setup creates failed")
	}
	slots := int(cfg.Resources.ControlRequests.Limit())
	var hogs []net.Conn
	defer func() {
		for _, c := range hogs {
			c.Close()
		}
	}()
	for i := 0; i < slots; i++ {
		c, e := net.Dial("tcp", addr)
		if e != nil {
			t.Fatal(e)
		}
		fmt.Fprintf(c, "POST /v1/sessions/%s/ack?epoch=1 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tokG\r\n%s: %s\r\n%s: bi\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{", greedy.SessionID, protocol.HeaderSessionToken, greedy.SessionToken, protocol.HeaderInstanceID)
		hogs = append(hogs, c)
	}
	time.Sleep(300 * time.Millisecond)
	if held := cfg.Resources.ControlRequests.Used(); held > auth.PeerShare(int64(slots), 2) {
		t.Fatalf("greedy peer holds %d of %d control slots", held, slots)
	}
	if st := egrDelete(t, url, "tokV", victim); st != 200 {
		t.Fatalf("victim DELETE while greedy peer trickles: %d", st)
	}
}

// EGR-10: a repeated Authorization header is refused.
func TestFixEGR10DuplicateAuthorization(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	_, ts := egrServer(t, cfg)
	r, _ := http.NewRequest("POST", ts.URL+"/v1/peer/probe", strings.NewReader("{}"))
	r.Header.Add("Authorization", "Bearer tokA")
	r.Header.Add("Authorization", "Bearer other")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("duplicate Authorization: %d", resp.StatusCode)
	}
}

// EGR-3 over TLS/HTTP2: the pre-auth clamp is lifted once a stream on the
// connection authenticates, so a pooled authenticated connection outlives
// the pre-auth timeout.
func TestFixEGR3AuthenticatedH2ConnOutlivesPreAuth(t *testing.T) {
	srv, cfg, _, _ := reviewServer(t)
	srv.preAuthTimeout = 300 * time.Millisecond
	dummy := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	cert := dummy.TLS.Certificates[0]
	dummy.Close()
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	_ = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600)
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600)
	cfg.HTTP.Listen = "127.0.0.1:0"
	cfg.HTTP.TLS = config.TLSConfig{Enabled: true, CertFile: certPath, KeyFile: keyPath}
	if err = srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	conns := 0
	tr := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DialContext: func(ctx context.Context, n, a string) (net.Conn, error) {
		conns++
		return (&net.Dialer{}).DialContext(ctx, n, a)
	}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 2 * time.Second}
	probe := func() {
		r, _ := http.NewRequest("POST", "https://"+srv.listener.Addr().String()+"/v1/peer/probe", strings.NewReader(`{"protocol_version":"1.4","expected_node_id":"B","nonce":"n"}`))
		r.Header.Set("Authorization", "Bearer token")
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.ProtoMajor != 2 || resp.StatusCode != 200 {
			t.Fatalf("%s %d", resp.Proto, resp.StatusCode)
		}
	}
	probe()
	time.Sleep(800 * time.Millisecond)
	probe()
	if conns != 1 {
		t.Fatalf("authenticated HTTP/2 connection was dropped (%d dials)", conns)
	}
}
