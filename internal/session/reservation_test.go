package session

import (
	"crypto/sha256"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestReservedUpFrameOwnership(t *testing.T) {
	s, _ := testSession(t, false)
	raw := resource.New(8192)
	r := NewRegistry(8192, 8192)
	r.SetResources(raw, nil, time.Minute)
	r.Register(s)
	defer r.CloseAll()
	s.SetTransportMode("ws")
	stream, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateDataPlane(stream); err != nil {
		t.Fatal(err)
	}
	data := []byte("reserved")
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	// Out of order frame stays pending, so ownership is observable before writes.
	raw.Try(int64(len(data)))
	_, accepted, _, consumed, err := s.IngestUpFrameReserved(1, 8, len(data), sha, data)
	if err != nil || !accepted || !consumed {
		t.Fatalf("accepted=%t consumed=%t error=%v", accepted, consumed, err)
	}
	raw.Try(int64(len(data)))
	_, accepted, _, consumed, err = s.IngestUpFrameReserved(1, 8, len(data), sha, data)
	if err != nil || !accepted || consumed {
		t.Fatalf("duplicate accepted=%t consumed=%t error=%v", accepted, consumed, err)
	}
	raw.Put(int64(len(data)))
	raw.Try(int64(len(data)))
	_, _, _, consumed, err = s.IngestUpFrameReserved(1, 9, len(data), sha, data)
	if err == nil || consumed {
		t.Fatalf("overlap consumed=%t error=%v", consumed, err)
	}
	raw.Put(int64(len(data)))
	s.Close()
	if raw.Used() != 0 {
		t.Fatalf("payload not returned: %d", raw.Used())
	}
}

func TestVariableCreateMetadataReleased(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown", true: "expiry"}[expiry], func(t *testing.T) {
			metadata := resource.New(64 << 10)
			r := NewRegistry(8192, 8192)
			r.SetResources(nil, metadata, 100*time.Millisecond)
			defer r.CloseAll()
			key, target := "identity-key", strings.Repeat("x", 12<<10)
			status, _, _, _ := r.ReserveCreate(key, target, protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws")
			if status != CreateReserved {
				t.Fatal(status)
			}
			r.FailCreate(key, protocol.ErrTargetForbidden, "creation refused")
			if metadata.Used() < int64(len(key)+len(target)+createRecordCost) { // Compact tombstone reservation.
				t.Fatalf("undercharged: %d", metadata.Used())
			}
			// Repeating a failure keeps one record and one reservation.
			used := metadata.Used()
			r.ReserveCreate(key, target, protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws")
			if metadata.Used() != used {
				t.Fatal("retry charged twice")
			}
			if expiry {
				until := time.Now().Add(time.Second)
				for metadata.Used() != 0 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
			} else {
				r.CloseAll()
			}
			if metadata.Used() != 0 {
				t.Fatalf("metadata not released: %d", metadata.Used())
			}
		})
	}
}
