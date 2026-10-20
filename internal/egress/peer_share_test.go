package egress

import (
	"bytes"
	"crypto/sha256"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Request-level metadata of remote peers (create, /up, stream attach) never
// takes the Metadata the local listeners of a bidirectional node keep.
func TestRequestMetadataLeavesIngressReserve(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Resources = resource.NewManager(256<<20, 32<<20, 2<<20, 8) // Metadata 1 MiB, reserve 128 KiB
	cfg.Listeners = []config.ListenerConfig{{ID: "l1", Listen: "127.0.0.1:0", Peer: "A"}}
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	_, addr := egrStart(t, cfg, 0)
	res := cfg.Resources
	_, reserve := res.IngressReserve()
	// Remote sessions hold all they may; a pending create request then
	// needs request metadata.
	if !res.TryEgressMetadata(res.EgressMetadataLimit() - 32<<10) {
		t.Fatal("setup")
	}
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	statuses := make(chan string, 4)
	for i := 0; i < 2; i++ {
		c, e := net.Dial("tcp", addr)
		if e != nil {
			t.Fatal(e)
		}
		conns = append(conns, c)
		fmt.Fprintf(c, "POST /v1/sessions HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tokA\r\n%s: bi\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{", protocol.HeaderInstanceID)
		go func() {
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			line, _ := io.ReadAll(io.LimitReader(c, 12))
			statuses <- string(line)
		}()
	}
	if got := <-statuses; !strings.Contains(got, "429") {
		t.Fatalf("request beyond the egress metadata was not refused: %q", got)
	}
	if !res.Metadata.Try(reserve) {
		t.Fatalf("local ingress reserve taken by remote requests: used %d of %d", res.Metadata.Used(), res.Metadata.Limit())
	}
	res.Metadata.Put(reserve)
}

func upFrame(t *testing.T, url, token, stream string, cr protocol.CreateSessionResponse, off int, body []byte, length int) (int, string) {
	t.Helper()
	sum := sha256.Sum256(body)
	r, _ := http.NewRequest("POST", fmt.Sprintf("%s/v1/sessions/%s/up?epoch=1", url, cr.SessionID), bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	r.Header.Set(protocol.HeaderSessionToken, cr.SessionToken)
	r.Header.Set(protocol.HeaderStreamID, stream)
	r.Header.Set(protocol.HeaderOffset, fmt.Sprint(off))
	r.Header.Set(protocol.HeaderLength, fmt.Sprint(length))
	r.Header.Set(protocol.HeaderSHA256, hex.EncodeToString(sum[:]))
	r.Header.Set("Content-Type", "application/octet-stream")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// A peer's WS readers waiting for frame bodies hold codec from that peer's
// share only: another peer's data plane keeps working.
func TestWSReadersStayWithinPeerCodecShare(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 40},
		{ID: "C", Tokens: []string{"tokC"}, AllowedTargets: []string{"db"}, MaxSessions: 4},
	})
	cfg.Resources = resource.NewManager(256<<20, 32<<10, 32<<20, 64)
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	s, ts := egrServer(t, cfg)
	share := auth.PeerShare(cfg.Resources.Codec.Limit(), 2)
	frame, _ := protocol.EncodeWSFrameChecked(1, 0, bytes.Repeat([]byte{'x'}, 1024))
	for i := 0; i < 32; i++ {
		cr := wsCreate(t, ts.URL, "tokA", fmt.Sprintf("a%d", i))
		c, _, e := (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}, WriteBufferSize: 128}).Dial(wsURLOf(ts.URL), wsHeader("tokA", cr))
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		w, _ := c.NextWriter(websocket.BinaryMessage)
		_, _ = w.Write(frame[:200]) // header and a few body bytes; the message never completes
	}
	time.Sleep(300 * time.Millisecond)
	if used := s.codecFor("A").Used(); used > share {
		t.Fatalf("peer A holds %d codec bytes, share %d", used, share)
	}
	st, _, cr := egrCreate(t, ts.URL, "tokC", "c1", "db")
	if st != 201 {
		t.Fatalf("create C %d", st)
	}
	stream, _ := egrOpenSSE(t, ts.URL, "tokC", cr)
	if e := s.registry.Get(cr.SessionID).ActivateDataPlane(stream); e != nil {
		t.Fatal(e)
	}
	body := bytes.Repeat([]byte{'y'}, 1000)
	if st, raw := upFrame(t, ts.URL, "tokC", stream, cr, 0, body, len(body)); st != 200 && st != 202 {
		t.Fatalf("peer C /up while A's WS readers wait: %d %s", st, raw)
	}
}

// On a bidirectional node the egress codec shares leave the local ingress
// its Codec reserve.
func TestEgressCodecLeavesIngressReserve(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Listeners = []config.ListenerConfig{{ID: "l1", Listen: "127.0.0.1:0", Peer: "A"}}
	s, _ := egrServer(t, cfg)
	res := cfg.Resources
	floor := res.IngressCodecReserve()
	if floor != res.Codec.Limit()/8 {
		t.Fatalf("codec reserve %d of %d", floor, res.Codec.Limit())
	}
	a := s.codecFor("A")
	if a.Try(res.Codec.Limit() - floor + 1) {
		t.Fatal("egress share took the local ingress reserve")
	}
	if !a.Try(res.Codec.Limit() - floor) {
		t.Fatal("egress share refused the egress part")
	}
	defer a.Put(res.Codec.Limit() - floor)
	if !res.Codec.Try(floor) {
		t.Fatal("local ingress cannot use its reserve")
	}
	res.Codec.Put(floor)
}

// The codec reserve never leaves the egress less than one maximal charge.
func TestIngressCodecReserveKeepsOneEgressFrame(t *testing.T) {
	cfg := testCfg(t, nil)
	cfg.Listeners = []config.ListenerConfig{{ID: "l1", Listen: "127.0.0.1:0", Peer: "A"}}
	cfg.Resources = resource.NewManager(256<<20, sseDownCost(1024)+2048, 32<<20, 8)
	applyIngressCodecReserve(cfg)
	if got := cfg.Resources.EgressCodecLimit(); got < maxCodecCharge(cfg) {
		t.Fatalf("egress codec %d below one charge %d", got, maxCodecCharge(cfg))
	}
	if cfg.Resources.IngressCodecReserve() != 2048 {
		t.Fatalf("reserve %d", cfg.Resources.IngressCodecReserve())
	}
}

// Downstream WS frames are charged their binary size, SSE frames their
// Base64 event text.
func TestDownstreamCodecChargePerTransport(t *testing.T) {
	if got := wsDownCost(256 << 10); got != 2*(256<<10)+protocol.WSBinaryHeaderLen {
		t.Fatalf("ws charge %d", got)
	}
	if got := sseDownCost(256 << 10); got != 6*(256<<10)+16384 {
		t.Fatalf("sse charge %d", got)
	}
	cfg := testCfg(t, nil)
	cfg.HTTP.AllowedTransports = []string{"ws"}
	if got := maxCodecCharge(cfg); got != int64(16<<10) {
		t.Fatalf("ws-only max charge for 1 KiB frames %d", got)
	}
	cfg.HTTP.AllowedTransports = []string{"ws", "http-sse"}
	if got := maxCodecCharge(cfg); got != sseDownCost(1024) {
		t.Fatalf("max charge %d", got)
	}
}

// An /up frame above the negotiated max_frame_size is refused 413 unread.
func TestOversizedUpFrameIsFrameTooLarge(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	s, ts := egrServer(t, cfg)
	st, _, cr := egrCreate(t, ts.URL, "tokA", "u1", "db")
	if st != 201 {
		t.Fatalf("create %d", st)
	}
	stream, _ := egrOpenSSE(t, ts.URL, "tokA", cr)
	if e := s.registry.Get(cr.SessionID).ActivateDataPlane(stream); e != nil {
		t.Fatal(e)
	}
	body := bytes.Repeat([]byte{'z'}, 2048)
	if st, raw := upFrame(t, ts.URL, "tokA", stream, cr, 0, body, len(body)); st != 413 || !strings.Contains(raw, protocol.ErrFrameTooLarge) {
		t.Fatalf("oversized /up: %d %s", st, raw)
	}
	if st, raw := upFrame(t, ts.URL, "tokA", stream, cr, 0, body[:100], 99); st != 400 || !strings.Contains(raw, protocol.ErrBadFrame) {
		t.Fatalf("length mismatch: %d %s", st, raw)
	}
}

// Each enabled peer's codec share is its fair part of the egress Codec.
func TestPeerCodecShareFollowsEnabledPeers(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
		{ID: "B", Tokens: []string{"tokB"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
		{ID: "C", Tokens: []string{"tokC"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
	})
	s, _ := egrServer(t, cfg)
	if got, want := s.codecFor("A").Limit(), auth.PeerShare(cfg.Resources.EgressCodecLimit(), 3); got != want {
		t.Fatalf("peer share %d, want %d", got, want)
	}
}
