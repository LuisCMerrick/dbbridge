package config

import "testing"

func TestMinCodecBudgetFollowsTransports(t *testing.T) {
	const frame = 256 << 10
	sse := int64(6*frame + 16384)
	ws := int64(2*frame + 56)
	for name, tc := range map[string]struct {
		c    Config
		want int64
	}{
		"egress ws and sse": {Config{HTTP: HTTPConfig{Enabled: true, AllowedTransports: []string{"ws", "http-sse"}}}, sse},
		"egress ws only":    {Config{HTTP: HTTPConfig{Enabled: true, AllowedTransports: []string{"ws"}}}, ws},
		"ws with listeners": {Config{HTTP: HTTPConfig{Enabled: true, AllowedTransports: []string{"ws"}}, Listeners: []ListenerConfig{{ID: "l"}}}, sse},
		"sse bidirectional": {Config{HTTP: HTTPConfig{Enabled: true, AllowedTransports: []string{"ws", "http-sse"}}, Listeners: []ListenerConfig{{ID: "l"}}}, sse + frame + 56},
		"ws with peers":     {Config{HTTP: HTTPConfig{Enabled: true, AllowedTransports: []string{"ws"}}, Peers: []PeerConfig{{ID: "p"}}}, sse},
		"ingress only":      {Config{}, sse},
	} {
		tc.c.Transport.MaxFrameSize = frame
		if got := tc.c.minCodecBudget(); got != tc.want {
			t.Errorf("%s: %d, want %d", name, got, tc.want)
		}
	}
	small := Config{HTTP: HTTPConfig{Enabled: true, AllowedTransports: []string{"ws"}}}
	small.Transport.MaxFrameSize = 1024
	if got := small.minCodecBudget(); got != 16<<10 {
		t.Errorf("small ws-only frame: %d", got)
	}
}
