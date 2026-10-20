package auth

import (
	"dbbridge/internal/config"
	"testing"
)

// Per-peer share of a global budget leaves room for other peers.
func TestReserveFairPartitionsGlobalBudget(t *testing.T) {
	for _, c := range []struct{ global, peers, want int64 }{{16, 1, 16}, {16, 2, 12}, {16, 3, 7}, {4, 3, 1}, {1, 5, 1}, {2, 2, 1}} {
		if got := PeerShare(c.global, int(c.peers)); got != c.want {
			t.Fatalf("PeerShare(%d,%d)=%d want %d", c.global, c.peers, got, c.want)
		}
	}
	a := NewAuthenticator(&config.Config{Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"token-a"}, AllowedTargets: []string{"db"}},
		{ID: "B", Tokens: []string{"token-b"}, AllowedTargets: []string{"db"}},
	}}})
	defer a.Close()
	var releases []func()
	for i := 0; i < 3; i++ {
		r, ok := a.ReserveFair("A", "create", 4)
		if !ok {
			t.Fatalf("reservation %d refused", i)
		}
		releases = append(releases, r)
	}
	if _, ok := a.ReserveFair("A", "create", 4); ok {
		t.Fatal("peer A exceeded its share")
	}
	if r, ok := a.ReserveFair("A", "probe", 4); !ok {
		t.Fatal("kinds must be independent")
	} else {
		r()
	}
	if r, ok := a.ReserveFair("B", "create", 4); !ok {
		t.Fatal("peer B starved")
	} else {
		r()
	}
	releases[0]()
	releases[0]() // idempotent
	if r, ok := a.ReserveFair("A", "create", 4); !ok {
		t.Fatal("release did not return the slot")
	} else {
		r()
	}
	if _, ok := a.ReserveFair("nobody", "create", 4); ok {
		t.Fatal("unknown peer admitted")
	}
}

// Digest-based bearer matching still distinguishes tokens exactly and
// still ignores disabled peers.
func TestBearerDigestCompare(t *testing.T) {
	off := false
	a := NewAuthenticator(&config.Config{Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"token-a", "token-a2"}, AllowedTargets: []string{"db"}},
		{ID: "D", Enabled: &off, Tokens: []string{"token-d"}, AllowedTargets: []string{"db"}},
	}}})
	defer a.Close()
	for tok, want := range map[string]string{"token-a": "A", "token-a2": "A", "token-a ": "", "token-": "", "token-d": "", "token-aa": ""} {
		got, _ := a.AuthenticateBearer("Bearer " + tok)
		if got != want {
			t.Fatalf("%q -> %q want %q", tok, got, want)
		}
	}
	if !a.PeerTokenValid("A", TokenDigest("Bearer token-a2")) || a.PeerTokenValid("A", TokenDigest("Bearer token-d")) || a.PeerTokenValid("D", TokenDigest("Bearer token-d")) {
		t.Fatal("PeerTokenValid")
	}
}
