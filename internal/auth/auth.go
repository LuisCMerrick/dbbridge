package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"dbbridge/internal/config"
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	ErrMissingAuth     = errors.New("missing or malformed authorization header")
	ErrInvalidToken    = errors.New("invalid peer token")
	ErrTargetForbidden = errors.New("target not allowed for peer")
	ErrMaxSessions     = errors.New("max sessions reached for peer")
)

type PeerAuthState struct {
	ID                           string
	Enabled                      bool
	Tokens                       []string
	AllowedTargets               map[string]bool
	MaxSessions, CurrentSessions int
	prechecks                    *precheckQuota
	tokenDigests                 [][32]byte
	inflight                     *inflightQuota
}
type Authenticator struct {
	mu             sync.RWMutex
	inboundPeers   map[string]*PeerAuthState
	outboundTokens map[string]string
	counts         map[string]int
	targets        map[string]config.TargetConfig
	// source is where credentials are re-read from on every reload: the
	// credentials file, or the inline credentials and token files of the
	// main configuration.
	source          config.CredentialSource
	referencedFiles []string
	digest          [32]byte
	initialErr      error
	stopCh          chan struct{}
	closeOnce       sync.Once
	onReload        []reloadListener
	nextListener    uint64
	revision        uint64
	// constraints are the main-config facts every reloaded snapshot must
	// satisfy, checked by the same config.ValidateCredentialSnapshot as
	// startup and check-config.
	constraints config.CredentialConstraints
	reloads     reloadState
}

type reloadListener struct {
	id uint64
	cb func(*Authenticator)
}

// NewAuthenticator loads the credentials of cfg and, when they include a
// credentials file or token files, re-reads them every auth.reload_interval
// and on Reload.
func NewAuthenticator(cfg *config.Config) *Authenticator {
	a := &Authenticator{inboundPeers: map[string]*PeerAuthState{}, outboundTokens: map[string]string{}, counts: map[string]int{}, stopCh: make(chan struct{})}
	if cfg.Targets != nil {
		a.targets = make(map[string]config.TargetConfig, len(cfg.Targets))
		for id, target := range cfg.Targets {
			a.targets[id] = target
		}
	}
	a.source = cfg.CredentialSource()
	a.constraints = cfg.CredentialConstraints()
	// Every outbound peer needs its entry in a reloaded snapshot.
	a.constraints.Refs = make(map[string]string, len(a.source.Refs))
	for peer, ref := range a.source.Refs {
		a.constraints.Refs[peer] = ref
	}
	a.constraints.Targets = a.targets
	a.reload("startup")
	if a.source.Reloadable() {
		interval := cfg.Auth.ReloadInterval
		if interval <= 0 {
			interval = 5 * time.Second
		}
		go a.reloadLoop(interval)
	}
	return a
}
func (a *Authenticator) Err() error { a.mu.RLock(); defer a.mu.RUnlock(); return a.initialErr }
func (a *Authenticator) Close()     { a.closeOnce.Do(func() { close(a.stopCh) }) }

// install publishes a snapshot and reports whether it differed from the
// installed one. With replaceOutbound false the outbound tokens are kept.
func (a *Authenticator) install(f *config.AuthConfigFile, refs []string, replaceOutbound bool) bool {
	// An explicit digest of every authentication-relevant field: unlike a
	// JSON encoding it cannot miss a token rotation.
	digest := config.CredentialDigest(f)
	a.mu.Lock()
	a.initialErr = nil
	if a.digest == digest {
		a.mu.Unlock()
		return false
	}
	a.digest = digest
	a.revision++
	peers := map[string]*PeerAuthState{}
	for _, p := range f.InboundPeers {
		v := &PeerAuthState{ID: p.ID, Enabled: p.IsEnabled(), Tokens: append([]string(nil), p.Tokens...), AllowedTargets: map[string]bool{}, MaxSessions: p.MaxSessions, CurrentSessions: a.counts[p.ID]}
		v.prechecks = newPrecheckQuota()
		v.inflight = newInflightQuota()
		if previous := a.inboundPeers[p.ID]; previous != nil {
			if previous.prechecks != nil {
				v.prechecks = previous.prechecks
			}
			if previous.inflight != nil {
				v.inflight = previous.inflight
			}
		}
		for _, token := range v.Tokens {
			v.tokenDigests = append(v.tokenDigests, sha256.Sum256([]byte(token)))
		}
		for _, id := range p.AllowedTargets {
			v.AllowedTargets[id] = true
		}
		peers[p.ID] = v
	}
	a.inboundPeers = peers
	if replaceOutbound {
		a.outboundTokens = outboundTokens(f, a.source.Refs)
	}
	a.referencedFiles = append([]string(nil), refs...)
	callbacks := make([]func(*Authenticator), 0, len(a.onReload))
	for _, l := range a.onReload {
		callbacks = append(callbacks, l.cb)
	}
	a.mu.Unlock()
	for _, cb := range callbacks {
		cb(a)
	}
	return true
}

// outboundTokens maps every peer to the token of its snapshot entry. The
// result is built in a fresh map keyed by peer ID only, so a peer ID that is
// also another peer's credential_ref can never pick up a token that was
// already remapped.
func outboundTokens(f *config.AuthConfigFile, refs map[string]string) map[string]string {
	byRef := make(map[string]string, len(f.OutboundPeers))
	for _, p := range f.OutboundPeers {
		byRef[p.ID] = p.Token
	}
	out := make(map[string]string, len(refs))
	for peer, ref := range refs {
		if token, ok := byRef[ref]; ok {
			out[peer] = token
		}
	}
	return out
}

func (a *Authenticator) AddReloadListener(cb func(*Authenticator)) { a.OnReload(cb) }

// OnReload registers cb like AddReloadListener and returns a function that
// unregisters it, so an owner that is closed stops receiving reloads.
func (a *Authenticator) OnReload(cb func(*Authenticator)) (remove func()) {
	a.mu.Lock()
	a.nextListener++
	id := a.nextListener
	a.onReload = append(a.onReload, reloadListener{id: id, cb: cb})
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, l := range a.onReload {
			if l.id == id {
				a.onReload = append(a.onReload[:i:i], a.onReload[i+1:]...)
				return
			}
		}
	}
}
func (a *Authenticator) GetOutboundToken(id string) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.outboundTokens[id]
}
func (a *Authenticator) AuthenticateBearer(h string) (string, error) {
	peer, _, err := a.AuthenticateBearerDigest(h)
	return peer, err
}

// AuthenticateBearerDigest is AuthenticateBearer that also returns the digest
// of the credential it accepted, parsed exactly once, for binding
// live sessions to that credential.
func (a *Authenticator) AuthenticateBearerDigest(h string) (string, [32]byte, error) {
	p := strings.SplitN(h, " ", 2)
	if len(p) != 2 || !strings.EqualFold(p[0], "Bearer") || p[1] == "" || strings.TrimSpace(p[1]) != p[1] {
		return "", [32]byte{}, ErrMissingAuth
	}
	presented := sha256.Sum256([]byte(p[1]))
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.initialErr != nil {
		return "", [32]byte{}, ErrInvalidToken
	}
	// Fixed-length digests are compared in constant time and every configured
	// token is visited, so neither token length nor match position leaks.
	matched := ""
	for id, state := range a.inboundPeers {
		for _, d := range state.tokenDigests {
			if subtle.ConstantTimeCompare(d[:], presented[:]) == 1 && state.Enabled {
				matched = id
			}
		}
	}
	if matched == "" {
		return "", [32]byte{}, ErrInvalidToken
	}
	return matched, presented, nil
}

// TokenDigest returns the SHA-256 digest of the bearer credential in an
// Authorization header value, for binding live sessions to the credential
// that last authenticated them.
func TokenDigest(h string) [32]byte {
	p := strings.SplitN(h, " ", 2)
	if len(p) != 2 {
		return sha256.Sum256(nil)
	}
	return sha256.Sum256([]byte(p[1]))
}

// PeerTokenValid reports whether the credential with digest d is still a
// configured token of the enabled peer.
func (a *Authenticator) PeerTokenValid(peer string, d [32]byte) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.initialErr != nil {
		return false
	}
	p := a.inboundPeers[peer]
	if p == nil || !p.Enabled {
		return false
	}
	ok := false
	for _, t := range p.tokenDigests {
		if subtle.ConstantTimeCompare(t[:], d[:]) == 1 {
			ok = true
		}
	}
	return ok
}
func (a *Authenticator) AuthorizeTarget(peer, target string) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	p := a.inboundPeers[peer]
	if p == nil || !p.Enabled {
		return ErrInvalidToken
	}
	if !p.AllowedTargets[target] {
		return ErrTargetForbidden
	}
	return nil
}
func (a *Authenticator) AuthorizeAndReserveTarget(peer, target string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.inboundPeers[peer]
	if p == nil || !p.Enabled {
		return ErrInvalidToken
	}
	if !p.AllowedTargets[target] {
		return ErrTargetForbidden
	}
	if p.MaxSessions > 0 && a.counts[peer] >= p.MaxSessions {
		return ErrMaxSessions
	}
	a.counts[peer]++
	p.CurrentSessions = a.counts[peer]
	return nil
}

// PeerMaxSessions returns the configured session quota of an enabled peer,
// or 0 when the peer is unknown or disabled.
func (a *Authenticator) PeerMaxSessions(peer string) int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if p := a.inboundPeers[peer]; p != nil && p.Enabled {
		return p.MaxSessions
	}
	return 0
}
func (a *Authenticator) DecrSession(peer string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.counts[peer] > 0 {
		a.counts[peer]--
	}
	if p := a.inboundPeers[peer]; p != nil {
		p.CurrentSessions = a.counts[peer]
	}
}
func ValidateSessionToken(a, b string) bool {
	return a != "" && b != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
func ValidateResumeToken(a, b string) bool { return ValidateSessionToken(a, b) }

func (a *Authenticator) Revision() uint64 { a.mu.RLock(); defer a.mu.RUnlock(); return a.revision }
