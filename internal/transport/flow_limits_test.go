package transport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"dbbridge/internal/protocol"
)

// X-DP-3 protocol: CreateSession offers the flow-limits extension and checks
// the limits an egress advertises; a plain 1.4 answer stays valid.
func TestCreateSessionFlowLimits(t *testing.T) {
	req := protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}
	cases := []struct {
		name        string
		receipt     int
		frames      int
		wantErr     bool
		wantReceipt int
	}{
		{"plain", 0, 0, false, 0},
		{"advertised", 64, 512, false, 64},
		{"receipt not above inflight", 8, 512, true, 0},
		{"negative receipt", -1, 512, true, 0},
		{"negative frames", 64, -1, true, 0},
		{"huge receipt clamped", protocol.MaxAdvertisedFrames + 1, 512, false, protocol.MaxAdvertisedFrames},
		{"huge frames clamped", 64, protocol.MaxAdvertisedFrames * 4, false, 64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			offered := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				offered = protocol.HasFeature(r.Header, protocol.FeatureFlowLimits)
				lim := req
				lim.UpReceiptLimit, lim.DownMaxFrames = tc.receipt, tc.frames
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(201)
				_ = json.NewEncoder(w).Encode(protocol.CreateSessionResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", SessionID: "0123456789abcdef0123456789abcdef", SessionToken: "t", Epoch: "1", Transport: protocol.TransportMode{Mode: "ws"}, Limits: lim, UnattachedTimeoutMs: 1000})
			}))
			defer srv.Close()
			c := NewTripleTransportClientForURL(srv.URL, "tok", fixCfg(), "ing", true)
			r, err := c.CreateSession(context.Background(), "db", req, "cr", protocol.RecoveryNegotiation{}, "bi", "ws")
			if !offered {
				t.Fatal("create request did not offer flow-limits")
			}
			if tc.wantErr {
				if !errors.Is(err, ErrProtocol) {
					t.Fatalf("want protocol error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.Limits.UpReceiptLimit != tc.wantReceipt {
				t.Fatalf("receipt limit %d", r.Limits.UpReceiptLimit)
			}
			if r.Limits.DownMaxFrames > protocol.MaxAdvertisedFrames {
				t.Fatalf("down frames %d not clamped", r.Limits.DownMaxFrames)
			}
			if r.Limits.FlowLimitsNegotiated() != (tc.receipt != 0) {
				t.Fatalf("negotiated=%v", r.Limits.FlowLimitsNegotiated())
			}
		})
	}
}
