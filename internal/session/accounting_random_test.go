package session

import (
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

// accountingSeeds is a fixed seed set for the randomized accounting driver,
// so failures are reproducible and the test stays fast.
var accountingSeeds = []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946}

// TestRandomizedSessionAccountingBalances drives a session through random up
// frames, backend reads, emits with write errors, ACKs, transport drops,
// resumes and closes, and checks that global buffer accounting returns to
// zero and that no release is unbalanced.
func TestRandomizedSessionAccountingBalances(t *testing.T) {
	for _, seed := range accountingSeeds {
		rng := rand.New(rand.NewSource(seed))
		m := resource.NewManager(64<<20, 32<<20, 32<<20, 200)
		r := NewRegistry(64<<20, 64<<20)
		r.SetResources(m.Payload, m.Metadata, time.Minute)
		r.SetRetention(m.Suspended)
		before := resource.UnbalancedReleases()
		s := NewSession("p", "t", "i", afSink{}, 256<<10, 64<<10, 4, 8, true)
		r.Register(s)
		s.Configure(10*time.Second, time.Second, "", nil)
		ep := uint64(1)
		st, _ := s.ReserveStream(ep, "ws")
		s.Ack(ep, st, protocol.AckRequest{StreamID: st, ReadyReceived: true, DownDeliveredOffset: "0", UpConfirmedOffset: "0"})
		var upNext uint64
		emitted := uint64(0)
		rid := 0
		for i := 0; i < 200; i++ {
			switch rng.Intn(9) {
			case 0, 1: // upstream frame
				n := 1 + rng.Intn(2000)
				d := make([]byte, n)
				rng.Read(d)
				_, ok, _, e := s.IngestUpFrame(ep, upNext, n, afSHA(d), d)
				if ok && e == nil {
					upNext += uint64(n)
				}
			case 2: // backend read
				size := 1024 << rng.Intn(4)
				if s.ReserveDownRead(int64(size)) {
					buf := make([]byte, size)
					k := rng.Intn(size + 1)
					s.FinishDownRead(buf[:k])
				}
			case 3: // emit next frame
				n := s.NextDownFrameBytes(emitted)
				if n > 0 {
					end := emitted + uint64(n)
					if s.BeginEmit(ep, emitted, end) == nil {
						var we error
						if rng.Intn(5) == 0 {
							we = errors.New("x")
						}
						s.FinishEmit(ep, end, we)
						if we == nil {
							emitted = end
						}
					}
				}
			case 4: // ack
				s.Ack(ep, st, protocol.AckRequest{StreamID: st, DownDeliveredOffset: strconv.FormatUint(emitted, 10), UpConfirmedOffset: strconv.FormatUint(s.UpAckOffset(), 10), ProbeID: "x"})
			case 5: // drop transport
				s.DropTransport(ep)
			case 6: // resume
				rid++
				known := s.UpAckOffset()
				resp, e := s.Resume(&protocol.ResumeRequest{ResumeRequestID: "r" + strconv.Itoa(rid), ExpectedEpoch: strconv.FormatUint(ep, 10), Transport: protocol.TransportMode{Mode: "ws"}, KnownUpAckOffset: strconv.FormatUint(known, 10), UpNextOffset: strconv.FormatUint(max(upNext, known), 10), DownDeliveredOffset: strconv.FormatUint(s.DownAckOffset(), 10)}, time.Second)
				if e == nil {
					ep, _ = protocol.Offset(resp.Epoch)
					upNext, _ = protocol.Offset(resp.AckUpOffset)
					emitted, _ = protocol.Offset(resp.DownReplayFrom)
					st, _ = s.ReserveStream(ep, "ws")
					s.Ack(ep, st, protocol.AckRequest{StreamID: st, ReadyReceived: true, DownDeliveredOffset: resp.DownReplayFrom, UpConfirmedOffset: resp.AckUpOffset, ProbeID: "y"})
				}
			case 7:
				time.Sleep(time.Duration(rng.Intn(200)) * time.Microsecond)
			case 8:
				if rng.Intn(20) == 0 {
					s.Close()
				}
			}
			if s.GetState() == StateClosed {
				break
			}
		}
		s.Close()
		afWait(t, "writer exit", func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			return !s.inflightWriting
		})
		time.Sleep(2 * time.Millisecond)
		if g := r.CurrentGlobalBuffer(); g != 0 {
			t.Errorf("seed %d: global buffer leak %d (retained=%d)", seed, g, s.retainedBytes.Load())
		}
		if s.retainedBytes.Load() != 0 {
			t.Errorf("seed %d: retainedBytes=%d after close", seed, s.retainedBytes.Load())
		}
		if d := resource.UnbalancedReleases() - before; d != 0 {
			t.Errorf("seed %d: %d unbalanced releases", seed, d)
		}
		r.CloseAll()
	}
}
