package config

import (
	"strings"
	"testing"
	"time"
)

// Changing one documented knob derives the unset neighbours it constrains.
func TestDependentDefaultsFollowTheirConfiguredNeighbour(t *testing.T) {
	url := "https://b.example.invalid"
	cfg, err := fixLoad(t, fixPeerConfig(url, "    failover: {healthcheck_interval: 2s}\n", ""))
	if err != nil || cfg.Peers[0].Failover.HealthcheckTimeout != time.Second {
		t.Fatalf("healthcheck_interval only: %v %+v", err, cfg)
	}
	cfg, err = fixLoad(t, fixPeerConfig(url, "", "transport: {recovery: {grace: 4s}}\n"))
	if err != nil || cfg.Transport.Recovery.BarrierTimeout != 2*time.Second {
		t.Fatalf("grace only: %v", err)
	}
	cfg, err = fixLoad(t, fixPeerConfig(url, "", "transport: {buffer_per_direction: 128KiB}\n"))
	if err != nil {
		t.Fatalf("buffer_per_direction 128KiB: %v", err)
	}
	if tr := cfg.Transport; tr.UpWindowBytes != 128<<10 || tr.MaxFrameSize != 128<<10 || tr.FrameSize != 32<<10 || tr.AckBatchBytes != 32<<10 {
		t.Fatalf("derived sizes: up=%d max_frame=%d frame=%d ack=%d", tr.UpWindowBytes, tr.MaxFrameSize, tr.FrameSize, tr.AckBatchBytes)
	}
	cfg, err = fixLoad(t, fixPeerConfig(url, "", "transport: {global_buffer_limit: 64MiB}\n"))
	if err != nil || cfg.Transport.SuspendedBufferLimit != 16<<20 {
		t.Fatalf("global_buffer_limit only: %v", err)
	}
}

// The default ack batch is at most a quarter of buffer_per_direction.
func TestDefaultAckBatchIsAtMostAQuarterOfTheBuffer(t *testing.T) {
	for _, bpd := range []string{"2MiB", "4MiB", "16MiB"} {
		cfg, err := fixLoad(t, fixPeerConfig("https://b.example.invalid", "", "transport: {buffer_per_direction: "+bpd+"}\n"))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Transport.AckBatchBytes*4 > cfg.Transport.BufferPerDirection {
			t.Fatalf("%s: ack_batch_bytes %d exceeds a quarter", bpd, cfg.Transport.AckBatchBytes)
		}
	}
}

// A constraint error says which side is a default.
func TestConstraintErrorsNameDefaultNeighbours(t *testing.T) {
	_, err := fixLoad(t, fixPeerConfig("https://b.example.invalid", "", "transport: {buffer_per_direction: 256KiB, max_frame_size: 512KiB}\n"))
	if err == nil || !strings.Contains(err.Error(), "max_frame_size (524288) must be <= up_window_bytes (262144, default)") {
		t.Fatalf("error does not mark the default: %v", err)
	}
	_, err = fixLoad(t, fixPeerConfig("https://b.example.invalid", "    failover: {healthcheck_interval: 2s, healthcheck_timeout: 3s}\n", ""))
	if err == nil || !strings.Contains(err.Error(), "healthcheck_timeout (3s) must be < healthcheck_interval (2s)") {
		t.Fatalf("explicit healthcheck values: %v", err)
	}
}
