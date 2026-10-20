package config

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"sort"
)

// CredentialConstraints are the facts of the main configuration that every
// credential snapshot must agree with, both at startup and on hot reload.
type CredentialConstraints struct {
	// Refs maps a peer ID to the credential_ref it needs from the snapshot.
	// Peers with a direct token are not listed.
	Refs map[string]string
	// Targets is the egress target table; nil skips the ACL target check
	// (standalone authenticator users without an egress role).
	Targets map[string]TargetConfig
	// MaxSessions is limits.max_sessions; <= 0 skips the quota checks.
	MaxSessions int
	// DirectPeers are peers whose direct tokens are included in the weak
	// token warnings.
	DirectPeers []PeerConfig
	// LocalIngress is set when this node also has local TCP listeners that
	// share the session budget with inbound peers (bidirectional nodes).
	LocalIngress bool
}

// CredentialConstraints returns the constraints a credentials snapshot for
// this (validated) configuration must satisfy. The authenticator should keep
// the result and pass it to ValidateCredentialSnapshot for each reload.
func (c *Config) CredentialConstraints() CredentialConstraints {
	cc := CredentialConstraints{Refs: map[string]string{}, Targets: c.Targets, MaxSessions: c.Limits.MaxSessions, DirectPeers: c.Peers, LocalIngress: len(c.Listeners) > 0}
	for _, p := range c.Peers {
		if p.Token == "" && p.TokenEnv == "" && p.TokenFile == "" {
			cc.Refs[p.ID] = p.CredentialRef
		}
	}
	return cc
}

// ValidateCredentialSnapshot is the single validation shared by startup,
// check-config and credential hot reload. It checks ACL targets,
// that the quotas of several enabled inbound peers partition
// limits.max_sessions, and that every credential_ref resolves. It returns
// non-fatal warnings (weak tokens, quota notes). A reload must keep the
// previous snapshot when an error is returned.
func ValidateCredentialSnapshot(f *AuthConfigFile, cc CredentialConstraints) ([]string, error) {
	enabledInbound, inboundQuota := 0, 0
	var warnings []string
	for _, ip := range f.InboundPeers {
		if cc.Targets != nil {
			for _, target := range ip.AllowedTargets {
				if _, ok := cc.Targets[target]; !ok {
					return nil, fmt.Errorf("unknown ACL target %q", target)
				}
			}
		}
		if ip.IsEnabled() {
			enabledInbound++
			// Saturating sum: quotas are positive, and a wrapped sum
			// would pass the partition check below.
			if ip.MaxSessions > math.MaxInt-inboundQuota {
				inboundQuota = math.MaxInt
			} else {
				inboundQuota += max(ip.MaxSessions, 0)
			}
			if cc.MaxSessions > 0 && ip.MaxSessions > cc.MaxSessions {
				warnings = append(warnings, fmt.Sprintf("inbound peer %s max_sessions (%d) exceeds limits.max_sessions (%d); the global limit applies", ip.ID, ip.MaxSessions, cc.MaxSessions))
			}
		}
	}
	// With several inbound peers, their quotas must partition the global
	// session limit so that one peer cannot take every slot.
	if cc.MaxSessions > 0 && enabledInbound > 1 && inboundQuota > cc.MaxSessions {
		return nil, fmt.Errorf("sum of inbound peer max_sessions (%d, an omitted quota counts as %d) exceeds limits.max_sessions (%d); set explicit per-peer quotas or raise the global limit", inboundQuota, DefaultInboundMaxSessions, cc.MaxSessions)
	}
	if cc.MaxSessions > 0 && cc.LocalIngress && enabledInbound > 0 && inboundQuota >= cc.MaxSessions {
		warnings = append(warnings, fmt.Sprintf("inbound peers may hold all %d sessions (quota sum %d) that local listeners share on this bidirectional node; set inbound max_sessions below limits.max_sessions to keep room for local clients", cc.MaxSessions, inboundQuota))
	}
	warnings = append(warnings, weakTokenWarnings(cc.DirectPeers, f)...)
	out := make(map[string]bool, len(f.OutboundPeers))
	for _, p := range f.OutboundPeers {
		out[p.ID] = true
	}
	peers := make([]string, 0, len(cc.Refs))
	for id := range cc.Refs {
		peers = append(peers, id)
	}
	sort.Strings(peers)
	for _, id := range peers {
		if ref := cc.Refs[id]; !out[ref] {
			return nil, fmt.Errorf("missing credential_ref %s", ref)
		}
	}
	return warnings, nil
}

// CredentialDigest is an explicit digest of everything in a resolved snapshot
// that affects authentication, including every token (hashed). Unlike a JSON
// encoding it does not depend on struct tags, so marking token fields
// json:"-" can never make a token rotation invisible.
func CredentialDigest(f *AuthConfigFile) [32]byte {
	h := sha256.New()
	str := func(s string) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	num := func(v int) { str(fmt.Sprint(v)) }
	num(f.Version)
	num(len(f.InboundPeers))
	for _, p := range f.InboundPeers {
		str(p.ID)
		str(fmt.Sprint(p.IsEnabled()))
		num(p.MaxSessions)
		num(len(p.AllowedTargets))
		for _, t := range p.AllowedTargets {
			str(t)
		}
		num(len(p.Tokens))
		for _, t := range p.Tokens {
			d := sha256.Sum256([]byte(t))
			str(string(d[:]))
		}
	}
	num(len(f.OutboundPeers))
	for _, p := range f.OutboundPeers {
		str(p.ID)
		d := sha256.Sum256([]byte(p.Token))
		str(string(d[:]))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func (c *Config) validateCredentials() error {
	f := AuthConfigFile{Version: 1, InboundPeers: c.Auth.InboundPeers}
	var refs []string
	if c.Auth.CredentialsFile != "" {
		path := c.Auth.CredentialsFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(c.ConfigFileDir, path)
		}
		loaded, r, e := readCredentials(path, !c.offline)
		if e != nil {
			return fmt.Errorf("credentials: %w", e)
		}
		// The credentials file names token sources but holds no secret,
		// so only the token files get the permission check.
		f, refs = *loaded, r
		c.warnCredentialSources([]string{path})
	} else {
		r, e := resolveCredentials(&f, c.ConfigFileDir, !c.offline)
		if e != nil {
			return fmt.Errorf("auth: %w", e)
		}
		refs = r
		c.Auth.InboundPeers = f.InboundPeers
	}
	warnings, err := ValidateCredentialSnapshot(&f, c.CredentialConstraints())
	if err != nil {
		return err
	}
	c.Warnings = append(c.Warnings, warnings...)
	c.warnCredentialSources(refs)
	if !c.offline {
		for _, path := range refs {
			c.warnInsecurePermissions(path)
		}
	}
	return nil
}
