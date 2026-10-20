package config

import (
	"testing"
	"time"
)

const intBidi = "listeners: [{id: l, listen: '127.0.0.1:13306', peer: B, target: db}]\n" +
	"http: {enabled: true, listen: '127.0.0.1:18443', allow_insecure_listen: true}\n" +
	"targets: {db: {address: '127.0.0.1:3306', allowed_cidrs: ['127.0.0.1/32']}}\n" +
	"auth: {inbound_peers: [{id: B, token_envs: [DBB_FIX_TOKEN], allowed_targets: [db]}]}\n"

// X-SEC-5: a bidirectional node reserves a quarter of the sessions and an
// eighth of the metadata for its local listeners in the shared Resources; an
// ingress-only node reserves nothing (egress admission is not involved).
func TestIntegrationXSEC5BidirectionalReserve(t *testing.T) {
	cfg, err := fixLoad(t, fixPeerConfig("https://b.example.invalid", "", intBidi))
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.Resources
	s, m := r.IngressReserve()
	if s != max(1, r.Sessions.Limit()/4) || m != r.Metadata.Limit()/8 {
		t.Fatalf("reserve %d/%d (limits %d/%d)", s, m, r.Sessions.Limit(), r.Metadata.Limit())
	}
	// Egress admission stops at the reserve; local ingress can still take it.
	for r.TryEgressSession() {
	}
	if free := r.Sessions.Limit() - r.Sessions.Used(); free != s || !r.Sessions.Try(1) {
		t.Fatalf("egress left %d sessions, want %d", free, s)
	}
	cfg, err = fixLoad(t, fixPeerConfig("https://b.example.invalid", "", "listeners: [{id: l, listen: '127.0.0.1:13306', peer: B, target: db}]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s, m := cfg.Resources.IngressReserve(); s != 0 || m != 0 {
		t.Fatalf("ingress-only node reserved %d/%d", s, m)
	}
}

// TRN-8: transport.peer_dial_timeout bounds peer dials and falls back to
// transport.dial_timeout.
func TestIntegrationTRN8PeerDialTimeout(t *testing.T) {
	cfg, err := fixLoad(t, fixPeerConfig("https://b.example.invalid", "", "transport: {dial_timeout: 3s}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Transport.PeerDial() != 3*time.Second {
		t.Fatalf("fallback peer dial %v", cfg.Transport.PeerDial())
	}
	cfg, err = fixLoad(t, fixPeerConfig("https://b.example.invalid", "", "transport: {dial_timeout: 3s, peer_dial_timeout: 750ms}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Transport.PeerDial() != 750*time.Millisecond || cfg.Transport.DialTimeout != 3*time.Second {
		t.Fatalf("peer dial %v dial %v", cfg.Transport.PeerDial(), cfg.Transport.DialTimeout)
	}
	if _, err = fixLoad(t, fixPeerConfig("https://b.example.invalid", "", "transport: {peer_dial_timeout: -1s}\n")); err == nil {
		t.Fatal("negative peer_dial_timeout accepted")
	}
}
