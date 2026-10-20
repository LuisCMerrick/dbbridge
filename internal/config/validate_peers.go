package config

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// validatePeers checks peers and endpoints and resolves direct outbound
// tokens. peerMap receives every peer ID.
func (c *Config) validatePeers(peerMap map[string]bool) error {
	// Validate peers and resolve token
	var err error
	for i := range c.Peers {
		p := &c.Peers[i]
		if p.ID == "" {
			return fmt.Errorf("peers[%d].id cannot be empty", i)
		}
		if peerMap[p.ID] {
			return fmt.Errorf("duplicate peer id: %s", p.ID)
		}
		peerMap[p.ID] = true
		c.warnID("peer", p.ID)

		if p.ExpectedNodeID == "" {
			return fmt.Errorf("peers[%s].expected_node_id is required", p.ID)
		}
		if p.ExpectedNodeID == c.Node.ID {
			return fmt.Errorf("peers[%s].expected_node_id must differ from this node's node.id", p.ID)
		}
		if p.CredentialRef == "" {
			return fmt.Errorf("peers[%s].credential_ref is required", p.ID)
		}
		if p.Transport != nil && p.Transport.Protocol != "" {
			pr := p.Transport.Protocol
			if pr != "auto" && pr != "ws" && pr != "http-sse" {
				return fmt.Errorf("invalid peers[%s].transport.protocol: %q (must be auto, ws, or http-sse)", p.ID, pr)
			}
		}

		// Check recovery conflict: if global recovery is false, peer cannot explicitly set true
		if !c.Transport.Recovery.Enabled && p.Recovery != nil && p.Recovery.Enabled != nil && *p.Recovery.Enabled {
			return fmt.Errorf("peer[%s].recovery.enabled cannot be true when transport.recovery.enabled is false", p.ID)
		}

		if len(p.Endpoints) == 0 {
			return fmt.Errorf("peers[%s].endpoints cannot be empty", p.ID)
		}
		if len(p.Endpoints) > c.Limits.MaxEndpointsPerPeer {
			return fmt.Errorf("peers[%s].endpoints length (%d) exceeds max_endpoints_per_peer (%d)", p.ID, len(p.Endpoints), c.Limits.MaxEndpointsPerPeer)
		}

		// Validate endpoints
		epIDs := make(map[string]bool)
		epURLs := make(map[string]bool)
		for epIdx, ep := range p.Endpoints {
			if ep.ID == "" {
				return fmt.Errorf("peers[%s].endpoints[%d].id cannot be empty", p.ID, epIdx)
			}
			if epIDs[ep.ID] {
				return fmt.Errorf("duplicate endpoint id %q in peer %s", ep.ID, p.ID)
			}
			epIDs[ep.ID] = true

			if ep.URL == "" {
				return fmt.Errorf("peers[%s].endpoints[%s].url cannot be empty", p.ID, ep.ID)
			}
			parsedURL, parseErr := url.Parse(ep.URL)
			if parseErr != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
				return fmt.Errorf("peers[%s].endpoints[%s].url %q is invalid (must be http/https origin)", p.ID, ep.ID, ep.URL)
			}
			if parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.ForceQuery || parsedURL.Fragment != "" || parsedURL.Path != "" || parsedURL.Opaque != "" {
				return fmt.Errorf("peers[%s].endpoints[%s].url must be an origin without credentials, path, trailing slash, query or fragment", p.ID, ep.ID)
			}
			if port := parsedURL.Port(); port != "" || strings.HasSuffix(parsedURL.Host, ":") {
				if n, e := strconv.ParseUint(port, 10, 16); e != nil || n == 0 {
					return fmt.Errorf("peers[%s].endpoints[%s].url port must be 1-65535", p.ID, ep.ID)
				}
			}
			normURL := parsedURL.Scheme + "://" + strings.ToLower(parsedURL.Host)
			if parsedURL.Port() == "80" && parsedURL.Scheme == "http" || parsedURL.Port() == "443" && parsedURL.Scheme == "https" {
				host := strings.ToLower(parsedURL.Hostname())
				if strings.Contains(host, ":") {
					host = "[" + host + "]"
				}
				normURL = parsedURL.Scheme + "://" + host
			}
			p.Endpoints[epIdx].URL = strings.TrimRight(normURL, "/")
			if epURLs[normURL] {
				return fmt.Errorf("duplicate normalized endpoint url %q in peer %s", normURL, p.ID)
			}
			epURLs[normURL] = true

			if !c.Transport.AllowInsecureRemoteHTTP && !p.AllowInsecureHTTP && parsedURL.Scheme == "http" {
				if !isLoopbackHost(parsedURL.Hostname()) {
					return fmt.Errorf("peers[%s].endpoints[%s].url %q uses plaintext http to a non-loopback address; use https or set peers[].allow_insecure_http: true for this peer", p.ID, ep.ID, ep.URL)
				}
			}

			if ep.Priority < 0 {
				return fmt.Errorf("peers[%s].endpoints[%s].priority must be >= 0", p.ID, ep.ID)
			}
		}

		if err = validatePeerTLS(p, c.ConfigFileDir, c.offline); err != nil {
			return err
		}

		// Defaults for failover
		if p.Failover.Strategy == "" {
			p.Failover.Strategy = "priority"
		}
		if p.Failover.Strategy != "priority" {
			return fmt.Errorf("peers[%s].failover.strategy must be \"priority\", got %q", p.ID, p.Failover.Strategy)
		}
		if p.Failover.Failback == "" {
			p.FailbackOrDefault()
		}
		if p.Failover.Failback != "new_sessions" {
			return fmt.Errorf("peers[%s].failover.failback must be \"new_sessions\", got %q", p.ID, p.Failover.Failback)
		}

		p.Failover.HealthcheckInterval, err = parseDuration(p.Failover.HealthcheckIntervalRaw, 10*time.Second)
		if err != nil {
			return fmt.Errorf("invalid peers[%s].failover.healthcheck_interval: %w", p.ID, err)
		}
		p.Failover.HealthcheckTimeout, err = parseDuration(p.Failover.HealthcheckTimeoutRaw, 3*time.Second)
		if err != nil {
			return fmt.Errorf("invalid peers[%s].failover.healthcheck_timeout: %w", p.ID, err)
		}
		// An unset timeout follows a shortened interval.
		if rawUnset(p.Failover.HealthcheckTimeoutRaw) && p.Failover.HealthcheckInterval > 0 {
			p.Failover.HealthcheckTimeout = max(time.Millisecond, min(p.Failover.HealthcheckTimeout, p.Failover.HealthcheckInterval/2))
		}
		if p.Failover.HealthcheckTimeout <= 0 || p.Failover.HealthcheckInterval <= 0 || p.Failover.HealthcheckTimeout >= p.Failover.HealthcheckInterval {
			return fmt.Errorf("peers[%s].failover.healthcheck_timeout (%s) must be < healthcheck_interval (%s)", p.ID, valueNote(p.Failover.HealthcheckTimeout, p.Failover.HealthcheckTimeoutRaw), valueNote(p.Failover.HealthcheckInterval, p.Failover.HealthcheckIntervalRaw))
		}
		if p.Failover.UnhealthyThreshold <= 0 {
			p.Failover.UnhealthyThreshold = 2
		}
		if p.Failover.HealthyThreshold <= 0 {
			p.Failover.HealthyThreshold = 2
		}
		p.Failover.Cooldown, err = parseDuration(p.Failover.CooldownRaw, 5*time.Second)
		if err != nil {
			return fmt.Errorf("invalid peers[%s].failover.cooldown: %w", p.ID, err)
		}
		if p.Failover.MaxConcurrentProbes <= 0 {
			p.Failover.MaxConcurrentProbes = 2
		}

		if c.offline {
			if p.TokenEnv != "" && p.TokenFile != "" {
				return fmt.Errorf("peer[%s] credential: ambiguous credential source", p.ID)
			}
			if p.TokenEnv == "" && p.TokenFile == "" && p.Token == "" && c.Auth.CredentialsFile == "" {
				return fmt.Errorf("peers[%s] has no outbound credential: set token_env or token_file, or use auth.credentials_file", p.ID)
			}
			continue
		}
		if p.TokenEnv != "" || p.TokenFile != "" {
			token, file, err := resolveToken(p.TokenFile, p.TokenEnv, c.ConfigFileDir)
			if err != nil {
				return WithCode(CodeCredential, fmt.Errorf("peer[%s] credential: %w", p.ID, err))
			}
			p.Token = token
			if file != "" {
				c.warnInsecurePermissions(file)
			}
		}
		if p.Token != "" {
			if err := validateToken(p.Token); err != nil {
				return WithCode(CodeCredential, fmt.Errorf("peer[%s] credential: %w", p.ID, err))
			}
		}
		if p.Token == "" && c.Auth.CredentialsFile == "" {
			return fmt.Errorf("peers[%s] has no outbound credential: set token_env or token_file, or use auth.credentials_file", p.ID)
		}

	}
	return nil
}
