package config

import (
	"errors"
	"fmt"
	"time"

	"dbbridge/internal/resource"
)

// ApplyDefaultsAndValidate fills defaults, parses the raw transport values,
// resolves credentials and validates the whole configuration. The steps run in
// a fixed order; the first error wins.
func (c *Config) ApplyDefaultsAndValidate() error {
	return WithCode(CodeConfigInvalid, c.applyDefaultsAndValidate())
}

func (c *Config) applyDefaultsAndValidate() error {
	if err := c.applyBaseDefaults(); err != nil {
		return err
	}
	if err := c.applyTransportDefaults(); err != nil {
		return err
	}
	peerMap := make(map[string]bool)
	if err := c.validatePeers(peerMap); err != nil {
		return err
	}
	if err := c.validateListenersAndTargets(peerMap); err != nil {
		return err
	}
	if err := c.validateCredentials(); err != nil {
		return WithCode(CodeCredential, err)
	}
	if err := c.validateMetadataBudget(); err != nil {
		return err
	}
	if err := c.validateListenAddresses(); err != nil {
		return WithCode(CodeListen, err)
	}
	c.warnUnusedSections()
	c.warnRecoveryCooldown()
	if c.Resources == nil {
		c.Resources = resource.NewManager(c.Transport.GlobalBufferLimit, c.Transport.CodecBufferLimit, c.Transport.MetadataLimit, c.Limits.MaxSessions, c.Transport.SuspendedBufferLimit)
	}
	if c.HTTP.Enabled {
		// Bidirectional nodes keep a share for their own listeners.
		c.ApplyIngressReserve()
	}

	return nil
}

func (c *Config) applyBaseDefaults() error {
	if c.Node.ID == "" {
		return errors.New("node.id is required")
	}
	c.warnID("node", c.Node.ID)
	if c.Auth.ReloadIntervalRaw != nil {
		d, err := parseDuration(c.Auth.ReloadIntervalRaw, 0)
		if err != nil {
			return fmt.Errorf("invalid auth.reload_interval: %w", err)
		}
		c.Auth.ReloadInterval = d
	}
	if c.Auth.ReloadInterval < 0 {
		return errors.New("auth.reload_interval cannot be negative")
	}
	if c.Auth.ReloadInterval == 0 {
		c.Auth.ReloadInterval = 5 * time.Second
	}
	if c.Auth.ReloadInterval < MinReloadInterval {
		return fmt.Errorf("auth.reload_interval (%v) must be >= %v", c.Auth.ReloadInterval, MinReloadInterval)
	}
	if c.Auth.CredentialsFile != "" {
		if len(c.Auth.InboundPeers) != 0 {
			return errors.New("credentials_file cannot be mixed with inline inbound credentials")
		}
		for _, p := range c.Peers {
			if p.TokenEnv != "" || p.TokenFile != "" || p.Token != "" {
				return errors.New("credentials_file cannot be mixed with direct outbound credentials")
			}
		}
	}
	c.Node.InstanceID = "pi_" + generateRandomHex(16)

	// Limits defaults
	if c.Limits.MaxSessions < 0 || c.Limits.MaxEndpointsPerPeer < 0 || c.Transport.UpMaxInflightFrames < 0 || c.Transport.H2PoolSize < 0 || c.Transport.ReceiptLimitPerSession < 0 {
		return errors.New("negative resource limit")
	}
	if c.Limits.MaxSessions <= 0 {
		c.Limits.MaxSessions = 200
	}
	if c.Limits.MaxEndpointsPerPeer <= 0 {
		c.Limits.MaxEndpointsPerPeer = 16
	}

	// HTTP defaults
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = "127.0.0.1:18080"
	}

	// Admin defaults
	if c.Admin.Listen == "" {
		c.Admin.Listen = DefaultAdminListen
	}

	// HTTP defaults
	if c.HTTP.AllowedTransports != nil && len(c.HTTP.AllowedTransports) == 0 {
		return errors.New("http.allowed_transports cannot be empty")
	}
	if len(c.HTTP.AllowedTransports) == 0 {
		c.HTTP.AllowedTransports = []string{"ws", "http-sse"}
	}
	for _, tr := range c.HTTP.AllowedTransports {
		if tr != "ws" && tr != "http-sse" {
			return fmt.Errorf("invalid http.allowed_transports value: %q (must be ws or http-sse)", tr)
		}
	}
	// Check duplicates in AllowedTransports
	seenTr := make(map[string]bool)
	for _, tr := range c.HTTP.AllowedTransports {
		if seenTr[tr] {
			return fmt.Errorf("duplicate transport in http.allowed_transports: %q", tr)
		}
		seenTr[tr] = true
	}

	// Transport defaults
	if c.Transport.Protocol == "" {
		c.Transport.Protocol = "auto"
	}
	if c.Transport.Protocol != "auto" && c.Transport.Protocol != "ws" && c.Transport.Protocol != "http-sse" {
		return fmt.Errorf("invalid transport.protocol: %q (must be auto, ws, or http-sse)", c.Transport.Protocol)
	}
	if c.Transport.ProtocolVersion == "" {
		c.Transport.ProtocolVersion = "1.4"
	}
	if c.Transport.ProtocolVersion != "1.4" {
		return fmt.Errorf("protocol_version must be \"1.4\", got %q", c.Transport.ProtocolVersion)
	}
	return nil
}

// warnUnusedSections reports configuration that validates but can never take
// effect.
func (c *Config) warnUnusedSections() {
	if !c.HTTP.Enabled {
		if len(c.Auth.InboundPeers) > 0 || c.Auth.CredentialsFile != "" && len(c.Peers) == 0 {
			c.Warnings = append(c.Warnings, "auth inbound peers are configured but http.enabled is false; this node never accepts inbound sessions")
		}
		if len(c.Targets) > 0 {
			c.Warnings = append(c.Warnings, "targets are configured but http.enabled is false; they are only used by the inbound HTTP server")
		}
		if c.HTTP.TLS.Enabled || c.HTTP.TLS.CertFile != "" || c.HTTP.TLS.KeyFile != "" {
			c.Warnings = append(c.Warnings, "http.tls is configured but http.enabled is false; the TLS settings are ignored")
		}
	}
	if !c.HTTP.Enabled && len(c.Listeners) == 0 && len(c.Peers) == 0 {
		c.Warnings = append(c.Warnings, "neither http.enabled nor any peers or listeners are configured; the node has nothing to do")
	}
}
