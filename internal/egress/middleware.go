package egress

import (
	"context"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// closeIfUnread asks net/http to close the connection instead of draining a
// request body the handler is rejecting without reading.
func closeIfUnread(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 1 && (r.ContentLength != 0 || len(r.TransferEncoding) > 0) {
		w.Header().Set("Connection", "close")
	}
}

// reject writes an error for a request whose body has not been read.
func reject(w http.ResponseWriter, r *http.Request, status int, code, msg string, closed, retry bool) {
	closeIfUnread(w, r)
	writeJSONError(w, status, code, msg, closed, retry)
}

func methodNotAllowed(w http.ResponseWriter, r *http.Request, allow string) {
	w.Header().Set("Allow", allow)
	reject(w, r, 405, protocol.ErrBadFrame, "method", false, false)
}

func (s *Server) requestTimeout() time.Duration {
	if s.cfg.Transport.ControlRequestTimeout > 0 {
		return s.cfg.Transport.ControlRequestTimeout
	}
	return 10 * time.Second
}

// route is the admission class of a request, decided from its path before
// any budget is taken (no suffix guessing).
type route struct {
	method   string
	control  bool             // ControlRequests instead of DataRequests
	meta     *resource.Budget // request-level metadata budget
	metaKind string           // per-peer fairness key for meta
	metaCost int64
	stream   bool // SSE events / WS session: request budgets end when the stream starts
}

// controlCost is the metadata a control request holds: its (at most 16 KiB)
// body plus a fixed overhead, instead of a flat 64 KiB.
func controlCost(r *http.Request) int64 {
	n := r.ContentLength
	if n < 0 || n > 16<<10 {
		n = 16 << 10
	}
	return 4<<10 + n
}

func (s *Server) classify(r *http.Request) (route, bool) {
	res := s.cfg.Resources
	path := r.URL.Path
	switch path {
	case "/v1/peer/probe":
		return route{method: "POST", control: true, meta: res.ProbeMetadata, metaKind: "probe-meta", metaCost: controlCost(r)}, true
	case "/v1/sessions":
		return route{method: "POST", meta: res.Metadata, metaKind: "meta", metaCost: 64 << 10}, true
	case "/v1/transport/ws":
		return route{method: "GET", meta: res.Metadata, metaKind: "meta", metaCost: 16 << 10, stream: true}, true
	}
	rest, ok := strings.CutPrefix(path, "/v1/sessions/")
	if !ok {
		return route{}, false
	}
	_, action, _ := strings.Cut(rest, "/")
	switch action {
	case "":
		return route{method: "DELETE", control: true, meta: res.ControlMetadata, metaKind: "control-meta", metaCost: 4 << 10}, true
	case "events":
		return route{method: "GET", meta: res.Metadata, metaKind: "meta", metaCost: 16 << 10, stream: true}, true
	case "up":
		return route{method: "POST", meta: res.Metadata, metaKind: "meta", metaCost: 64 << 10}, true
	}
	// ack, fin, resume, and unknown actions (answered 404 by the handler).
	return route{method: "POST", control: true, meta: res.ControlMetadata, metaKind: "control-meta", metaCost: controlCost(r)}, true
}

// admission holds a request's budgets; release is idempotent so a stream
// handler can give them back once the long-lived stream has started.
type admission struct {
	once    sync.Once
	release func()
}

func (a *admission) done() { a.once.Do(a.release) }

type admissionKey struct{}

// releaseAdmission returns the request-level budgets of a request whose
// long-lived stream (SSE events, WS session) has started. The stream
// is bounded by the session itself, one per session.
func releaseAdmission(r *http.Request) {
	if a, ok := r.Context().Value(admissionKey{}).(*admission); ok {
		a.done()
	}
}

// preAuthReject answers a request refused before authentication and closes
// the connection, so the socket is not kept for another try.
func preAuthReject(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Connection", "close")
	writeJSONError(w, status, code, msg, false, false)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/peer/probe", s.handleProbe)
	mux.HandleFunc("/v1/sessions", s.handleSessions)
	mux.HandleFunc("/v1/sessions/", s.handleSessionRoutes)
	mux.HandleFunc("/v1/transport/ws", s.handleWS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.enter() {
			reject(w, r, 503, protocol.ErrDraining, "stopped", false, true)
			return
		}
		defer s.wg.Done()
		w.Header().Set("Cache-Control", "no-store")
		// The instance ID is echoed only to callers that already present it
		// (the ingress client does, and uses the echo to trust error bodies);
		// authenticated responses set it in peer(). Anonymous scanners get none.
		if r.Header.Get(protocol.HeaderInstanceID) == s.cfg.Node.InstanceID {
			w.Header().Set(protocol.HeaderInstanceID, s.cfg.Node.InstanceID)
		}
		// Bound header-to-handler and body reads for every request. net/http
		// clears its own deadline after the headers when ReadTimeout is 0, and
		// would otherwise drain an unread declared body with no deadline at all.
		// /up extends this once the frame is validated; SSE and WS clear it once
		// the stream starts.
		rc := http.NewResponseController(w)
		_ = rc.SetReadDeadline(time.Now().Add(s.requestTimeout()))
		if (r.Method == "GET" || r.Method == "HEAD") && (r.ContentLength != 0 || len(r.TransferEncoding) > 0) {
			preAuthReject(w, 400, protocol.ErrBadFrame, "request body not allowed")
			return
		}
		count, size := 0, 0
		for k, vs := range r.Header {
			for _, v := range vs {
				count++
				size += len(k) + len(v) + 4
			}
			if (strings.HasPrefix(http.CanonicalHeaderKey(k), "X-Dbb-") || http.CanonicalHeaderKey(k) == "Authorization") && len(vs) != 1 {
				preAuthReject(w, 400, protocol.ErrBadFrame, "duplicate binding header")
				return
			}
		}
		if count > 64 || size > 16<<10 || r.Header.Get("Origin") != "" {
			preAuthReject(w, 400, protocol.ErrBadFrame, "invalid headers")
			return
		}
		if s.hub != nil && (r.URL.Path == protocol.ReverseTunnelPath || strings.HasPrefix(r.URL.Path, protocol.ReverseTunnelPath+"/")) {
			// Reverse tunnels authenticate with the reverse peer's token,
			// not an inbound identity; the hub checks it.
			s.hub.Serve(w, r, func() { markConnAuthenticated(r.Context()) })
			return
		}
		rt, known := s.classify(r)
		if !known {
			preAuthReject(w, 404, protocol.ErrBadFrame, "not found")
			return
		}
		if r.Method != rt.method {
			w.Header().Set("Allow", rt.method)
			preAuthReject(w, 405, protocol.ErrBadFrame, "method")
			return
		}
		// Authenticate before any budget is taken: unauthenticated requests
		// hold no request slot or metadata and lose the socket.
		peer, digest, err := s.auth.AuthenticateBearerDigest(r.Header.Get(protocol.HeaderPeerAuth))
		if err != nil {
			unauthorized(w, r)
			return
		}
		markConnAuthenticated(r.Context())
		_ = rc.SetReadDeadline(time.Now().Add(s.requestTimeout()))
		adm := &admission{}
		releases := make([]func(), 0, 4)
		adm.release = func() {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		}
		defer adm.done()
		requests, kind := s.cfg.Resources.DataRequests, "data-requests"
		if rt.control {
			requests, kind = s.cfg.Resources.ControlRequests, "control-requests"
		}
		// Request slots and request metadata are partitioned between peers so
		// one authenticated peer cannot hold all of them.
		if !s.admit(peer, kind, requests, 1, &releases) {
			atomic.AddInt64(&observability.DefaultMetrics.AdmissionRejected, 1)
			reject(w, r, 429, protocol.ErrQuotaExceeded, "request concurrency", false, true)
			return
		}
		if !s.admit(peer, rt.metaKind, rt.meta, rt.metaCost, &releases) {
			msg := "request metadata"
			if r.Method != "GET" {
				msg = "control metadata budget"
			}
			reject(w, r, 429, protocol.ErrQuotaExceeded, msg, false, true)
			return
		}
		ctx := context.WithValue(r.Context(), authKey{}, authInfo{peer: peer, digest: digest})
		if rt.stream {
			ctx = context.WithValue(ctx, admissionKey{}, adm)
		}
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

// admit takes n units of global for peer, after the peer's fair share of it.
// Request metadata charged to the data-plane Metadata budget comes from its
// egress part only: the local ingress reserve of a bidirectional node is
// neither taken nor counted in the per-peer share.
func (s *Server) admit(peer, kind string, global *resource.Budget, n int64, releases *[]func()) bool {
	res := s.cfg.Resources
	limit, try := global.Limit(), global.Try
	if global == res.Metadata {
		limit, try = res.EgressMetadataLimit(), res.TryEgressMetadata
	}
	release, fair := s.auth.ReserveFairN(peer, kind, limit, n)
	if !fair {
		return false
	}
	if !try(n) {
		release()
		return false
	}
	*releases = append(*releases, func() { global.Put(n); release() })
	return true
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.Handler().ServeHTTP(w, r) }
