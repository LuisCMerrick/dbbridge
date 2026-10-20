package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// CredentialSource describes where a running node reads its credentials
// from, so that the authenticator can re-read them on reload.
type CredentialSource struct {
	// File is the absolute path of auth.credentials_file, or "" when the
	// credentials are part of the main configuration (auth.inbound_peers
	// and peers[].token_file / token_env).
	File string
	// Refs maps each peer ID to the snapshot entry that holds its outbound
	// token: its credential_ref for a credentials file, or the peer ID
	// itself for inline credentials.
	Refs map[string]string

	dir     string
	inbound []InboundPeerConfig
	peers   []PeerConfig
	// configPath is the main configuration, whose auth.inbound_peers and
	// peers[].token_file / token_env are re-read on reload; "" when the
	// configuration was not loaded from a file.
	configPath string
}

// CredentialSource returns the credential source of a loaded configuration.
func (c *Config) CredentialSource() CredentialSource {
	s := CredentialSource{Refs: map[string]string{}, dir: c.ConfigFileDir}
	if c.Auth.CredentialsFile != "" {
		s.File = c.resolvePath(c.Auth.CredentialsFile)
		for _, p := range c.Peers {
			ref := p.CredentialRef
			if ref == "" {
				ref = p.ID
			}
			s.Refs[p.ID] = ref
		}
		return s
	}
	s.configPath = c.path
	s.inbound = cloneInbound(c.Auth.InboundPeers)
	for _, p := range c.Peers {
		if p.Token != "" || p.TokenFile != "" || p.TokenEnv != "" {
			s.peers = append(s.peers, PeerConfig{ID: p.ID, Token: p.Token, TokenFile: p.TokenFile, TokenEnv: p.TokenEnv})
			s.Refs[p.ID] = p.ID
		}
	}
	return s
}

func cloneInbound(in []InboundPeerConfig) []InboundPeerConfig {
	out := make([]InboundPeerConfig, len(in))
	for i, p := range in {
		out[i] = p
		out[i].TokenFiles = append([]string(nil), p.TokenFiles...)
		out[i].TokenEnvs = append([]string(nil), p.TokenEnvs...)
		out[i].Tokens = append([]string(nil), p.Tokens...)
		out[i].AllowedTargets = append([]string(nil), p.AllowedTargets...)
		if p.Enabled != nil {
			out[i].Enabled = BoolPtr(*p.Enabled)
		}
	}
	return out
}

// Read resolves a fresh credential snapshot from the source: every token
// file is read again and every token variable is looked up again. Outbound
// entries of inline credentials are keyed by peer ID. The returned paths are
// the token files that were read.
func (s CredentialSource) Read() (*AuthConfigFile, []string, error) {
	if s.File != "" {
		f, refs, err := ReadCredentials(s.File)
		if err != nil {
			return nil, nil, WithCode(CodeCredential, err)
		}
		return f, refs, nil
	}
	inbound, peers := s.inbound, s.peers
	if s.configPath != "" {
		var err error
		if inbound, peers, err = s.readInline(); err != nil {
			return nil, nil, err
		}
	}
	f := &AuthConfigFile{Version: 1, InboundPeers: cloneInbound(inbound)}
	refs, err := resolveCredentials(f, s.dir, true)
	if err != nil {
		return nil, nil, WithCode(CodeCredential, fmt.Errorf("auth: %w", err))
	}
	for _, p := range peers {
		token := p.Token
		if p.TokenFile != "" || p.TokenEnv != "" {
			t, file, e := resolveToken(p.TokenFile, p.TokenEnv, s.dir)
			if e != nil {
				return nil, nil, WithCode(CodeCredential, fmt.Errorf("peer[%s] credential: %w", p.ID, e))
			}
			token = t
			if file != "" {
				refs = append(refs, file)
			}
		}
		if e := validateToken(token); e != nil {
			return nil, nil, WithCode(CodeCredential, fmt.Errorf("peer[%s] credential: %w", p.ID, e))
		}
		f.OutboundPeers = append(f.OutboundPeers, OutboundPeerCredential{ID: p.ID, TokenFile: p.TokenFile, TokenEnv: p.TokenEnv, Token: token})
	}
	return f, refs, nil
}

// readInline re-reads the credential settings of the main configuration:
// auth.inbound_peers and the token_file / token_env of every peer that had a
// direct credential at startup. Other settings take effect only on restart.
func (s CredentialSource) readInline() ([]InboundPeerConfig, []PeerConfig, error) {
	data, err := readFileLimited(s.configPath, 4<<20, "config too large")
	if err != nil {
		return nil, nil, WithCode(CodeConfigRead, fmt.Errorf("re-read %s: %w", s.configPath, err))
	}
	if err = validateSchema(data, Config{}); err != nil {
		return nil, nil, WithCode(CodeConfigRead, fmt.Errorf("re-read %s: %w", s.configPath, err))
	}
	var c Config
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err = d.Decode(&c); err != nil {
		return nil, nil, WithCode(CodeConfigRead, fmt.Errorf("re-read %s: %w", s.configPath, err))
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, nil, WithCode(CodeConfigRead, fmt.Errorf("re-read %s: configuration must contain exactly one YAML document", s.configPath))
	}
	if c.Auth.CredentialsFile != "" {
		return nil, nil, WithCode(CodeCredential, errors.New("auth.credentials_file was added to the configuration; switching to a credentials file requires a restart"))
	}
	byID := make(map[string]PeerConfig, len(c.Peers))
	for _, p := range c.Peers {
		byID[p.ID] = p
	}
	peers := make([]PeerConfig, 0, len(s.peers))
	for _, old := range s.peers {
		p, ok := byID[old.ID]
		if !ok {
			return nil, nil, WithCode(CodeCredential, fmt.Errorf("peers[%s] was removed from the configuration; adding or removing peers requires a restart", old.ID))
		}
		next := PeerConfig{ID: p.ID, TokenFile: p.TokenFile, TokenEnv: p.TokenEnv}
		if p.TokenFile == "" && p.TokenEnv == "" {
			if old.TokenFile != "" || old.TokenEnv != "" {
				return nil, nil, WithCode(CodeCredential, fmt.Errorf("peers[%s] has no outbound credential: set token_env or token_file", p.ID))
			}
			next.Token = old.Token
		}
		peers = append(peers, next)
	}
	return c.Auth.InboundPeers, peers, nil
}

// TokenSources lists the token variables and token files a snapshot was
// read from, sorted. Token files can be rotated while the node runs; token
// variables are fixed for the lifetime of the process.
func TokenSources(f *AuthConfigFile, dir string) (envs, files []string) {
	envSet, fileSet := map[string]bool{}, map[string]bool{}
	file := func(path string) {
		if path == "" {
			return
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(dir, path)
		}
		fileSet[path] = true
	}
	for _, p := range f.InboundPeers {
		if !p.IsEnabled() {
			continue
		}
		for _, e := range p.TokenEnvs {
			envSet[e] = true
		}
		for _, f := range p.TokenFiles {
			file(f)
		}
	}
	for _, p := range f.OutboundPeers {
		if p.TokenEnv != "" {
			envSet[p.TokenEnv] = true
		}
		file(p.TokenFile)
	}
	for e := range envSet {
		envs = append(envs, e)
	}
	for f := range fileSet {
		files = append(files, f)
	}
	sort.Strings(envs)
	sort.Strings(files)
	return envs, files
}

// configTokenSources lists the token variables and files of the main
// configuration (inline credentials only).
func (c *Config) configTokenSources() (envs, files []string) {
	f := &AuthConfigFile{InboundPeers: c.Auth.InboundPeers}
	for _, p := range c.Peers {
		f.OutboundPeers = append(f.OutboundPeers, OutboundPeerCredential{ID: p.ID, TokenEnv: p.TokenEnv, TokenFile: p.TokenFile})
	}
	return TokenSources(f, c.ConfigFileDir)
}

// sandboxedPrefixes are trees the generated systemd unit hides from the
// service (ProtectHome, PrivateTmp) unless a file in them was referenced
// when the service was installed.
var sandboxedPrefixes = []string{"/home/", "/root/", "/run/user/", "/tmp/", "/var/tmp/"}

// warnCredentialSources reports token files in trees that the generated
// service unit may hide from the service.
func (c *Config) warnCredentialSources(credentialFiles []string) {
	if c.Auth.CredentialsFile == "" {
		_, files := c.configTokenSources()
		credentialFiles = append(credentialFiles, files...)
	}
	configDir := c.ConfigFileDir + string(filepath.Separator)
	for _, path := range credentialFiles {
		for _, prefix := range sandboxedPrefixes {
			if strings.HasPrefix(path, prefix) && !strings.HasPrefix(configDir, prefix) {
				tree := strings.TrimSuffix(prefix, "/")
				c.Warnings = append(c.Warnings, fmt.Sprintf("credential file %s is under %s but the configuration is not; a service installed with 'dbbridge install' sees %s only when a file there is referenced at install time, so keep credential files next to the configuration", path, tree, tree))
				break
			}
		}
	}
}

// Reloadable reports whether re-reading the source can ever yield different
// credentials: a credentials file, the main configuration file and any token
// file can change while the node runs; the values of environment variables
// cannot.
func (s CredentialSource) Reloadable() bool {
	if s.File != "" || s.configPath != "" {
		return true
	}
	for _, p := range s.inbound {
		if p.IsEnabled() && len(p.TokenFiles) > 0 {
			return true
		}
	}
	for _, p := range s.peers {
		if p.TokenFile != "" {
			return true
		}
	}
	return false
}

// Dir is the directory relative token files of the source resolve against.
func (s CredentialSource) Dir() string {
	if s.File != "" {
		return filepath.Dir(s.File)
	}
	return s.dir
}
