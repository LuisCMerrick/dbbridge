package config

import (
	"bytes"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

// DefaultInboundMaxSessions is the quota of an inbound peer that omits
// max_sessions. It equals the default global limit, so configurations with
// more than one inbound peer must set explicit quotas (see
// Config.ApplyDefaultsAndValidate).
const DefaultInboundMaxSessions = 200

// Reserve space in the 16 KiB header limit for session/instance/stream bindings.
const maxTokenBytes = 8 << 10

// MinTokenBytes is the recommended minimum bearer token length; shorter tokens
// are reported in Config.Warnings. gen-config writes 64 hex characters
// (32 random bytes).
const MinTokenBytes = 16

// errMalformedToken never includes the token value.
var errMalformedToken = errors.New("missing, empty, oversized or malformed token")

func validateToken(token string) error {
	if token == "" || len(token) > maxTokenBytes || strings.IndexFunc(token, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0 {
		return errMalformedToken
	}
	return nil
}

// resolveToken is shared by inline credentials, credential snapshots and reloads.
// A token file may have one line ending; other whitespace is never normalized.
// Errors name the source (variable or file) but never the token.
func resolveToken(file, env, dir string) (token, resolvedFile string, err error) {
	if file != "" && env != "" {
		return "", "", fmt.Errorf("ambiguous credential source: both token_file %s and token_env %s are set", file, env)
	}
	if file != "" {
		if !filepath.IsAbs(file) {
			file = filepath.Join(dir, file)
		}
		f, e := os.Open(file)
		if e != nil {
			return "", "", fmt.Errorf("token_file: %w", e)
		}
		b, readErr := io.ReadAll(io.LimitReader(f, maxTokenBytes+3))
		closeErr := f.Close()
		if readErr != nil {
			return "", "", fmt.Errorf("token_file %s: %w", file, readErr)
		}
		if closeErr != nil {
			return "", "", fmt.Errorf("token_file %s: %w", file, closeErr)
		}
		token = strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
		resolvedFile = file
		if err = validateToken(token); err != nil {
			return "", "", fmt.Errorf("token_file %s: %w", file, err)
		}
		return token, resolvedFile, nil
	}
	if env == "" {
		return "", "", errors.New("no token_file or token_env configured")
	}
	token = os.Getenv(env)
	if token == "" {
		return "", "", fmt.Errorf("token_env %s is unset or empty", env)
	}
	if err = validateToken(token); err != nil {
		return "", "", fmt.Errorf("token_env %s: %w", env, err)
	}
	return token, "", nil
}

func ReadCredentials(path string) (*AuthConfigFile, []string, error) {
	return readCredentials(path, true)
}

// readCredentials parses a credentials file. With resolve false (offline
// check-config) token sources are checked for presence only and no token file
// or environment variable is read.
func readCredentials(path string, resolve bool) (*AuthConfigFile, []string, error) {
	b, e := readFileLimited(path, 1<<20, "credentials file too large")
	if e != nil {
		return nil, nil, e
	}
	if e = validateSchema(b, AuthConfigFile{}); e != nil {
		return nil, nil, e
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	var f AuthConfigFile
	if e = d.Decode(&f); e != nil {
		return nil, nil, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, nil, errors.New("multiple credentials documents")
	}
	if f.Version != 1 {
		return nil, nil, errors.New("credentials version must be 1")
	}
	refs, e := resolveCredentials(&f, filepath.Dir(path), resolve)
	return &f, refs, e
}

func ResolveCredentials(f *AuthConfigFile, dir string) ([]string, error) {
	return resolveCredentials(f, dir, true)
}

func resolveCredentials(f *AuthConfigFile, dir string, resolve bool) ([]string, error) {
	refs := []string{}
	ids := map[string]bool{}
	tokens := map[string]string{}
	read := func(file, env string) (string, error) {
		if !resolve {
			if file != "" && env != "" {
				return "", errors.New("ambiguous credential source")
			}
			if file != "" {
				if !filepath.IsAbs(file) {
					file = filepath.Join(dir, file)
				}
				refs = append(refs, file)
			}
			return "", nil
		}
		s, path, err := resolveToken(file, env, dir)
		if err != nil {
			return "", err
		}
		if path != "" {
			refs = append(refs, path)
		}
		return s, nil
	}
	for i := range f.InboundPeers {
		p := &f.InboundPeers[i]
		name := p.ID
		if name == "" {
			name = fmt.Sprint(i)
		}
		if p.ID == "" || ids[p.ID] || p.MaxSessions < 0 {
			return nil, fmt.Errorf("inbound_peers[%s]: invalid or duplicate inbound identity/quota", name)
		}
		ids[p.ID] = true
		// An omitted quota is bounded; explicit YAML zero is rejected by schema.
		if p.MaxSessions == 0 {
			p.MaxSessions = DefaultInboundMaxSessions
		}
		if len(p.TokenFiles) > 0 && len(p.TokenEnvs) > 0 {
			return nil, fmt.Errorf("inbound_peers[%s]: mixed inbound token sources", name)
		}
		if len(p.TokenFiles) > 0 || len(p.TokenEnvs) > 0 {
			p.Tokens = nil
		}
		if !p.IsEnabled() {
			// A disabled identity never authenticates, so its token sources
			// are not read: revoking an identity must work even when its
			// compromised token file or variable has already been removed.
			p.Tokens = nil
			if err := checkTargetACL(name, p.AllowedTargets); err != nil {
				return nil, err
			}
			continue
		}
		sources := len(p.TokenFiles) + len(p.TokenEnvs)
		for _, file := range p.TokenFiles {
			s, e := read(file, "")
			if e != nil {
				return nil, fmt.Errorf("inbound_peers[%s]: %w", name, e)
			}
			if resolve {
				p.Tokens = append(p.Tokens, s)
			}
		}
		for _, env := range p.TokenEnvs {
			s, e := read("", env)
			if e != nil {
				return nil, fmt.Errorf("inbound_peers[%s]: %w", name, e)
			}
			if resolve {
				p.Tokens = append(p.Tokens, s)
			}
		}
		if !resolve {
			sources += len(p.Tokens)
			if p.IsEnabled() && (sources == 0 || len(p.AllowedTargets) == 0) {
				return nil, fmt.Errorf("inbound_peers[%s]: enabled identity needs tokens and targets", name)
			}
		} else if p.IsEnabled() && (len(p.Tokens) == 0 || len(p.AllowedTargets) == 0) {
			return nil, fmt.Errorf("inbound_peers[%s]: enabled identity needs tokens and targets", name)
		}
		// The same token listed twice for one identity (for example the
		// current and the next token during a rotation that has not started
		// yet) is accepted once; a token shared by two identities is not.
		unique := p.Tokens[:0:0]
		for _, s := range p.Tokens {
			if err := validateToken(s); err != nil {
				return nil, fmt.Errorf("inbound_peers[%s]: %w", name, err)
			}
			if owner, ok := tokens[s]; ok {
				if owner == p.ID {
					continue
				}
				return nil, fmt.Errorf("inbound peers %s and %s use the same token; every inbound identity needs its own token", owner, p.ID)
			}
			tokens[s] = p.ID
			unique = append(unique, s)
		}
		if resolve {
			p.Tokens = unique
		}
		if err := checkTargetACL(name, p.AllowedTargets); err != nil {
			return nil, err
		}
	}
	ids = map[string]bool{}
	for i := range f.OutboundPeers {
		p := &f.OutboundPeers[i]
		name := p.ID
		if name == "" {
			name = fmt.Sprint(i)
		}
		if p.ID == "" || ids[p.ID] || p.TokenFile == "" && p.TokenEnv == "" {
			return nil, fmt.Errorf("outbound_peers[%s]: invalid outbound credential (needs a unique id and token_file or token_env)", name)
		}
		ids[p.ID] = true
		s, e := read(p.TokenFile, p.TokenEnv)
		if e != nil {
			return nil, fmt.Errorf("outbound_peers[%s]: %w", name, e)
		}
		p.Token = s
	}
	return refs, nil
}

func checkTargetACL(name string, targets []string) error {
	seen := map[string]bool{}
	for _, id := range targets {
		if id == "" || seen[id] {
			return fmt.Errorf("inbound_peers[%s]: invalid target ACL (empty or duplicate target %q)", name, id)
		}
		seen[id] = true
	}
	return nil
}

func weakTokenWarnings(peers []PeerConfig, f *AuthConfigFile) []string {
	var out []string
	warn := func(kind, id, token string) {
		if token != "" && len(token) < MinTokenBytes {
			out = append(out, fmt.Sprintf("%s %s uses a bearer token shorter than %d bytes; use gen-config or 32 random bytes", kind, id, MinTokenBytes))
		}
	}
	for _, p := range peers {
		warn("peer", p.ID, p.Token)
	}
	for _, p := range f.InboundPeers {
		for _, t := range p.Tokens {
			warn("inbound peer", p.ID, t)
		}
	}
	for _, p := range f.OutboundPeers {
		warn("outbound credential", p.ID, p.Token)
	}
	return out
}
