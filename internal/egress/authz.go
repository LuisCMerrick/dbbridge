package egress

import (
	"context"
	"dbbridge/internal/auth"
	"dbbridge/internal/protocol"
	"net/http"
)

// onAuthReload closes the sessions whose identity was disabled or removed, or
// whose Target ACL no longer allows them. A live session
// is bound to its stable peer identity, not to the bearer token text that
// created or last controlled it: removing an old Peer Token only rejects the
// later requests that still present it (every create, control, WS attach,
// POST and resume request is authenticated against the current snapshot), and
// never destroys an authorized database session by itself.
// Holding s.mu serializes the sweep with session creation, which checks
// authorization under the same lock, so a create either sees the new snapshot
// or is registered before the sweep runs.
func (s *Server) onAuthReload(a *auth.Authenticator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.resizePeerCodec()
	for _, sess := range s.registry.GetAllSessions() {
		// AuthorizeTarget fails for a disabled or deleted identity and for a
		// Target removed from the identity's ACL.
		if a.AuthorizeTarget(sess.OwnerPeer, sess.TargetID) != nil {
			sess.CloseWithReason(protocol.ErrTargetForbidden)
		}
	}
}

// bindCred checks that the credential that authenticated this request is
// still valid for peer in the current snapshot; a request presenting a
// removed token is refused. It never terminates a session: a live
// session is bound to its peer identity, not to the token text.
func (s *Server) bindCred(peer string, d [32]byte) bool {
	return s.auth.PeerTokenValid(peer, d)
}

// authInfo is the result of the admission-time peer authentication.
type authInfo struct {
	peer   string
	digest [32]byte
}

type authKey struct{}

func authFrom(ctx context.Context) (authInfo, bool) {
	v, ok := ctx.Value(authKey{}).(authInfo)
	return v, ok
}

// credential returns the digest of the bearer credential of this request.
func credential(r *http.Request) [32]byte {
	if a, ok := authFrom(r.Context()); ok {
		return a.digest
	}
	return auth.TokenDigest(r.Header.Get(protocol.HeaderPeerAuth))
}

// peer returns the peer the admission middleware authenticated, or
// authenticates the request itself when the handler is reached directly.
func (s *Server) peer(w http.ResponseWriter, r *http.Request) (string, bool) {
	if a, ok := authFrom(r.Context()); ok {
		w.Header().Set(protocol.HeaderInstanceID, s.cfg.Node.InstanceID)
		return a.peer, true
	}
	p, e := s.auth.AuthenticateBearer(r.Header.Get(protocol.HeaderPeerAuth))
	if e != nil {
		unauthorized(w, r)
		return "", false
	}
	w.Header().Set(protocol.HeaderInstanceID, s.cfg.Node.InstanceID)
	return p, true
}

func (s *Server) instance(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get(protocol.HeaderInstanceID) == "" || r.Header.Get(protocol.HeaderInstanceID) != s.cfg.Node.InstanceID {
		reject(w, r, 409, protocol.ErrInstanceMismatch, "instance mismatch", false, false)
		return false
	}
	return true
}

// unauthorized rejects a request whose peer credential failed and closes the
// connection, so an unauthenticated client cannot keep a socket.
func unauthorized(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="dbbridge"`)
	w.Header().Set("Connection", "close")
	writeJSONError(w, 401, protocol.ErrUnauthorized, "invalid credentials", false, false)
}
