package egress

import (
	"dbbridge/internal/protocol"
	"net/http"
)

func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		methodNotAllowed(w, r, "POST")
		return
	}
	peer, ok := s.peer(w, r)
	if !ok {
		return
	}
	release, fair := s.auth.ReserveFair(peer, "probe", s.cfg.Resources.Probes.Limit())
	if !fair {
		reject(w, r, 429, protocol.ErrQuotaExceeded, "peer probe budget", false, true)
		return
	}
	defer release()
	if !s.cfg.Resources.Probes.Try(1) {
		reject(w, r, 429, protocol.ErrQuotaExceeded, "probe budget", false, true)
		return
	}
	defer s.cfg.Resources.Probes.Put(1)
	var q protocol.ProbeRequest
	if !decode(w, r, &q) {
		return
	}
	if q.ProtocolVersion != protocol.ProtocolVersionString {
		writeJSONError(w, 409, protocol.ErrProtocolMismatch, "protocol", false, false)
		return
	}
	if q.ExpectedNodeID != "" && q.ExpectedNodeID != s.cfg.Node.ID {
		writeJSONError(w, 409, protocol.ErrNodeMismatch, "node", false, false)
		return
	}
	if q.Nonce == "" || len(q.Nonce) > 128 {
		writeJSONError(w, 400, protocol.ErrBadFrame, "nonce", false, false)
		return
	}
	writeJSON(w, 200, protocol.ProbeResponse{ProtocolVersion: protocol.ProtocolVersionString, NodeID: s.cfg.Node.ID, InstanceID: s.cfg.Node.InstanceID, Nonce: q.Nonce, RecoveryAvailable: s.cfg.Transport.Recovery.Enabled, AcceptingNewSessions: s.registry.Accepting(), Transports: s.transports()})
}

func (s *Server) transports() []string {
	if len(s.cfg.HTTP.AllowedTransports) == 0 {
		return []string{"ws", "http-sse"}
	}
	return s.cfg.HTTP.AllowedTransports
}

func (s *Server) allows(mode string) bool {
	for _, m := range s.transports() {
		if m == mode {
			return true
		}
	}
	return false
}
