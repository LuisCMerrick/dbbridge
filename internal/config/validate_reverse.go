package config

import (
	"fmt"
	"time"
)

// validateReverse checks the reverse section (the database side of reverse
// mode), applies its defaults and resolves its token. A disabled section
// that still carries settings only produces a warning.
func (c *Config) validateReverse() error {
	r := &c.Reverse
	if !r.Enabled {
		if r.PeerNodeID != "" || r.TokenFile != "" || r.TokenEnv != "" || r.Transport != "" || r.TunnelsPerEndpoint != 0 || len(r.Endpoints) != 0 || r.Reconnect.MinBackoffRaw != nil || r.Reconnect.MaxBackoffRaw != nil {
			c.Warnings = append(c.Warnings, "reverse is configured but reverse.enabled is false; the settings are ignored")
		}
		return nil
	}
	fail := func(format string, args ...any) error { return WithCode(CodeReverse, fmt.Errorf(format, args...)) }

	if r.PeerNodeID == "" {
		return fail("reverse.peer_node_id is required when reverse.enabled is true")
	}
	if r.PeerNodeID == c.Node.ID {
		return fail("reverse.peer_node_id must differ from this node's node.id")
	}
	c.warnID("reverse peer node", r.PeerNodeID)

	switch r.Transport {
	case "":
		r.Transport = TunnelTransportAuto
	case TunnelTransportAuto, TunnelTransportWS, TunnelTransportSSE, TunnelTransportLongPoll:
	default:
		return fail("invalid reverse.transport: %q (must be auto, ws, http-sse or long-poll)", r.Transport)
	}

	if r.TunnelsPerEndpoint == 0 {
		r.TunnelsPerEndpoint = DefaultTunnelsPerEndpoint
	}
	if r.TunnelsPerEndpoint < 1 || r.TunnelsPerEndpoint > MaxTunnelsPerEndpoint {
		return fail("reverse.tunnels_per_endpoint must be 1-%d, got %d", MaxTunnelsPerEndpoint, r.TunnelsPerEndpoint)
	}

	if len(r.Endpoints) == 0 {
		return fail("reverse.endpoints cannot be empty when reverse.enabled is true")
	}
	if len(r.Endpoints) > c.Limits.MaxEndpointsPerPeer {
		return fail("reverse.endpoints length (%d) exceeds max_endpoints_per_peer (%d)", len(r.Endpoints), c.Limits.MaxEndpointsPerPeer)
	}
	if err := normalizeEndpoints(r.Endpoints, endpointOwner{prefix: "reverse", name: "reverse", insecureHint: "set reverse.allow_insecure_http: true"}, r.AllowInsecureHTTP); err != nil {
		return WithCode(CodeReverse, err)
	}
	if err := validateClientTLS(&r.TLS, r.Endpoints, "reverse", "endpoint", c.ConfigFileDir, c.offline); err != nil {
		return WithCode(CodeReverse, err)
	}

	var err error
	if r.Reconnect.MinBackoff, err = parseDuration(r.Reconnect.MinBackoffRaw, DefaultReverseMinBackoff); err != nil {
		return fail("invalid reverse.reconnect.min_backoff: %v", err)
	}
	if r.Reconnect.MaxBackoff, err = parseDuration(r.Reconnect.MaxBackoffRaw, DefaultReverseMaxBackoff); err != nil {
		return fail("invalid reverse.reconnect.max_backoff: %v", err)
	}
	// An unset max_backoff never falls below an explicit min_backoff.
	if rawUnset(r.Reconnect.MaxBackoffRaw) && r.Reconnect.MaxBackoff < r.Reconnect.MinBackoff {
		r.Reconnect.MaxBackoff = r.Reconnect.MinBackoff
	}
	if r.Reconnect.MinBackoff <= 0 {
		return fail("reverse.reconnect.min_backoff must be > 0")
	}
	if r.Reconnect.MaxBackoff < r.Reconnect.MinBackoff {
		return fail("reverse.reconnect.max_backoff (%v) must be >= min_backoff (%v)", r.Reconnect.MaxBackoff, r.Reconnect.MinBackoff)
	}
	if r.Reconnect.MaxBackoff > MaxReverseBackoff {
		return fail("reverse.reconnect.max_backoff (%v) must be <= %v", r.Reconnect.MaxBackoff, MaxReverseBackoff)
	}

	if r.TokenEnv == "" && r.TokenFile == "" {
		return fail("reverse needs a token: set reverse.token_file or reverse.token_env")
	}
	if r.TokenEnv != "" && r.TokenFile != "" {
		return fail("reverse: ambiguous credential source: both token_file %s and token_env %s are set", r.TokenFile, r.TokenEnv)
	}
	if !c.offline {
		token, file, err := resolveToken(r.TokenFile, r.TokenEnv, c.ConfigFileDir)
		if err != nil {
			return WithCode(CodeCredential, fmt.Errorf("reverse credential: %w", err))
		}
		if err := validateToken(token); err != nil {
			return WithCode(CodeCredential, fmt.Errorf("reverse credential: %w", err))
		}
		r.Token = token
		if file != "" {
			c.warnInsecurePermissions(file)
		}
	}

	if len(c.Targets) == 0 {
		c.Warnings = append(c.Warnings, "reverse.enabled is true but no targets are configured; sessions arriving over the tunnel have nothing to connect to")
	}
	return nil
}

// warnReverseGrace reports a recovery grace too short for a tunnel to come
// back: a first reconnect attempt that runs into peer_dial_timeout, the wait
// of min_backoff before the next attempt and the write barrier must all fit
// in recovery.grace, or sessions over a lost tunnel cannot resume.
func (c *Config) warnReverseGrace() {
	if !c.Reverse.Enabled || !c.Transport.Recovery.Enabled {
		return
	}
	grace := c.Transport.Recovery.Grace
	need := c.Transport.PeerDial() + c.Reverse.Reconnect.MinBackoff + c.Transport.Recovery.BarrierTimeout
	if grace <= need {
		c.Warnings = append(c.Warnings, fmt.Sprintf("reverse: transport.recovery.grace (%v) does not exceed peer_dial_timeout (%v) plus reverse.reconnect.min_backoff (%v) plus recovery.barrier_timeout (%v); a tunnel that fails its first reconnect cannot come back before sessions expire; raise recovery.grace or lower min_backoff", grace, c.Transport.PeerDial().Round(time.Millisecond), c.Reverse.Reconnect.MinBackoff, c.Transport.Recovery.BarrierTimeout))
	}
}

// ReverseToken re-reads the reverse tunnel token from its source, so a
// rotated token file takes effect on the next tunnel dial. When the source
// cannot be read or holds an invalid token, the token resolved at startup is
// returned with the error.
func (c *Config) ReverseToken() (string, error) {
	r := &c.Reverse
	token, _, err := resolveToken(r.TokenFile, r.TokenEnv, c.ConfigFileDir)
	if err == nil {
		err = validateToken(token)
	}
	if err != nil {
		return r.Token, fmt.Errorf("reverse credential: %w", err)
	}
	return token, nil
}
