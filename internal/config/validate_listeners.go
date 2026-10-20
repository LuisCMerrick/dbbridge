package config

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

func (c *Config) validateListenersAndTargets(peerMap map[string]bool) error {
	// Validate listeners
	listenerIDs := make(map[string]bool)
	for i, l := range c.Listeners {
		if l.ID == "" {
			return fmt.Errorf("listeners[%d].id cannot be empty", i)
		}
		if listenerIDs[l.ID] {
			return fmt.Errorf("duplicate listener id: %s", l.ID)
		}
		listenerIDs[l.ID] = true
		c.warnID("listener", l.ID)
		if l.Listen == "" {
			return fmt.Errorf("listeners[%s].listen cannot be empty", l.ID)
		}
		if l.Peer == "" {
			return fmt.Errorf("listeners[%s].peer cannot be empty", l.ID)
		}
		if !peerMap[l.Peer] {
			return fmt.Errorf("listeners[%s] references non-existent peer: %s", l.ID, l.Peer)
		}
		if l.Target == "" {
			return fmt.Errorf("listeners[%s].target cannot be empty", l.ID)
		}
	}

	// Validate targets
	for tID, t := range c.Targets {
		if tID == "" {
			return errors.New("targets: target name cannot be empty")
		}
		c.warnID("target", tID)
		if t.Address == "" {
			return fmt.Errorf("target[%s].address cannot be empty", tID)
		}
		if t.Network != "" && t.Network != "tcp" && t.Network != "tcp4" && t.Network != "tcp6" {
			return fmt.Errorf("target[%s].network must be tcp, tcp4 or tcp6", tID)
		}
		if err := validateSocketAddress(t.Address, false); err != nil {
			return fmt.Errorf("target[%s]: %w", tID, err)
		}
		for i, cidr := range t.AllowedCIDRs {
			_, network, err := net.ParseCIDR(strings.TrimSpace(cidr))
			if err != nil {
				return fmt.Errorf("invalid CIDR in targets[%s].allowed_cidrs %q: %w", tID, cidr, err)
			}
			t.AllowedCIDRs[i] = network.String()
		}
		// A hostname target is re-resolved on every dial; without an
		// allow-list a changed DNS answer can point authenticated peers at
		// any internal address. gen-config already requires one.
		if host, _, err := net.SplitHostPort(t.Address); err == nil && net.ParseIP(strings.Trim(host, "[]")) == nil && len(t.AllowedCIDRs) == 0 {
			c.Warnings = append(c.Warnings, fmt.Sprintf("target %s uses hostname %q without allowed_cidrs; any address it resolves to will be dialed (DNS rebinding/SSRF risk); list the expected networks in allowed_cidrs", tID, host))
		}
		c.Targets[tID] = t
	}
	return nil
}

func (c *Config) validateListenAddresses() error {
	addresses := map[string]bool{}
	checkListen := func(addr string) error {
		if err := validateSocketAddress(addr, true); err != nil {
			return err
		}
		host, port, e := net.SplitHostPort(addr)
		if e != nil {
			return e
		}
		if _, e = strconv.ParseUint(port, 10, 16); e != nil {
			return e
		}
		key := net.JoinHostPort(host, port)
		if port != "0" {
			for existing := range addresses {
				eh, ep, _ := net.SplitHostPort(existing)
				if port == ep && listenHostsOverlap(host, eh) {
					return fmt.Errorf("conflicting listen address %s overlaps %s", key, existing)
				}
			}
		}
		addresses[key] = true
		return nil
	}
	if c.HTTP.Enabled {
		if e := checkListen(c.HTTP.Listen); e != nil {
			return e
		}
		h, _, _ := net.SplitHostPort(c.HTTP.Listen)
		if !c.HTTP.TLS.Enabled && !isLoopbackHost(h) {
			if !c.HTTP.AllowInsecureListen && !c.Transport.AllowInsecureRemoteHTTP {
				return errors.New("non-loopback HTTP needs TLS or explicit insecure opt-in (http.allow_insecure_listen: true)")
			}
			c.Warnings = append(c.Warnings, fmt.Sprintf("http.listen %s accepts plaintext HTTP on a non-loopback address; bearer tokens and database traffic are not encrypted", c.HTTP.Listen))
		}
	}
	if c.Admin.Enabled {
		if e := checkListen(c.Admin.Listen); e != nil {
			return fmt.Errorf("admin.listen %q: %w", c.Admin.Listen, e)
		}
		h, _, _ := net.SplitHostPort(c.Admin.Listen)
		if !c.Admin.AllowRemote && !isLoopbackHost(h) {
			return errors.New("admin.listen must be a loopback address (the admin endpoint has no TLS or authentication); set admin.allow_remote: true to override")
		}
		if !isLoopbackHost(h) {
			c.Warnings = append(c.Warnings, fmt.Sprintf("admin.listen %s exposes /metrics and /readyz without TLS or authentication; restrict it with a firewall", c.Admin.Listen))
		}
	}
	for _, l := range c.Listeners {
		if e := checkListen(l.Listen); e != nil {
			return e
		}
		// A local listener is an unauthenticated entry to the remote
		// database: anyone who can reach it uses this node's peer
		// credential.
		if h, _, _ := net.SplitHostPort(l.Listen); !isLoopbackHost(h) {
			c.Warnings = append(c.Warnings, fmt.Sprintf("listeners[%s].listen %s is not a loopback address: every host that can reach it gets a connection to peer %s target %s without authentication; bind 127.0.0.1 or firewall the port", l.ID, l.Listen, l.Peer, l.Target))
		}
	}
	return nil
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

// warnID flags identifiers that will be awkward in logs, metric labels and the
// peer's configuration. They are accepted for compatibility.
func (c *Config) warnID(kind, id string) {
	if id != "" && !idPattern.MatchString(id) {
		c.Warnings = append(c.Warnings, fmt.Sprintf("%s id %q should match %s (letters, digits, '.', '_', '-', at most 64 characters)", kind, id, idPattern.String()))
	}
}
