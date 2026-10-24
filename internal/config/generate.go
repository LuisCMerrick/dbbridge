package config

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"dbbridge/internal/fileutil"
	"gopkg.in/yaml.v3"
)

type GenerateOptions struct {
	Output, Role, NodeID, PeerID, Listen, HTTPListen string
	TargetID, TargetAddress, TokenFile, Protocol     string
	PeerTargetID                                     string
	// AdminListen is the loopback address of /healthz, /readyz and
	// /metrics; "" uses DefaultAdminListen.
	AdminListen                        string
	TLSCert, TLSKey                    string
	PeerURLs, AllowedCIDRs             []string
	Recovery, AllowInsecureHTTP, Force bool
	MaxSessions                        int
	// InboundMaxSessions is the inbound peer quota; 0 means MaxSessions.
	// A "peer" role node shares limits.max_sessions between its local
	// listeners and the remote peer, so GeneratePair sets half.
	InboundMaxSessions int
	// Reverse generates reverse mode (role ingress or egress only): the
	// ingress node becomes the public side (a reverse peer and an HTTP
	// server the tunnels dial in to); the egress node opens no socket and
	// dials PeerURLs (the public side) with a reverse section.
	Reverse bool
}

type GeneratedFiles struct {
	Config, TokenFile string
	TokenCreated      bool
}

// Generate writes a validated config and a reusable random token file. Existing
// token files are reused; --force only replaces the configuration itself.
func Generate(o GenerateOptions) (result GeneratedFiles, err error) {
	if o.Role != "ingress" && o.Role != "egress" && o.Role != "peer" {
		return result, errors.New("role must be ingress, egress, or peer")
	}
	if o.NodeID == "" || o.PeerID == "" || o.NodeID == o.PeerID {
		return result, errors.New("node-id and peer-id must be nonempty and different")
	}
	if o.Output == "" {
		return result, errors.New("output path is required")
	}
	output, err := filepath.Abs(o.Output)
	if err != nil {
		return result, err
	}
	var previous os.FileInfo
	if info, e := os.Lstat(output); e == nil {
		previous = info
		if !o.Force {
			return result, fmt.Errorf("config already exists: %s (use --force to replace it)", output)
		}
		if !info.Mode().IsRegular() {
			return result, errors.New("refusing to replace a non-regular config file")
		}
	} else if !os.IsNotExist(e) {
		return result, e
	}
	tokenPath := o.TokenFile
	if tokenPath == "" {
		tokenPath = strings.TrimSuffix(output, filepath.Ext(output)) + ".token"
	}
	tokenPath, err = filepath.Abs(tokenPath)
	if err != nil {
		return result, err
	}
	if output == tokenPath || runtime.GOOS == "windows" && strings.EqualFold(output, tokenPath) {
		return result, errors.New("config and token paths must differ")
	}
	if o.MaxSessions <= 0 {
		return result, errors.New("max-sessions must be positive")
	}
	if (o.TLSCert == "") != (o.TLSKey == "") {
		return result, errors.New("tls-cert and tls-key must be supplied together")
	}
	if o.Reverse && o.Role == "peer" {
		return result, errors.New("reverse mode needs role ingress (public side) or egress (database side)")
	}
	if o.Role == "ingress" && o.TLSCert != "" && !o.Reverse {
		return result, errors.New("ingress has no HTTP server; TLS server options need egress or peer role")
	}
	if o.Reverse && o.Role == "egress" && o.TLSCert != "" {
		return result, errors.New("the database side of reverse mode has no HTTP server; TLS server options belong to the public (ingress) side")
	}
	if len(o.PeerURLs) == 0 {
		o.PeerURLs = []string{"http://127.0.0.1:18080"}
	}
	if o.Role != "ingress" && len(o.AllowedCIDRs) == 0 {
		host, _, e := net.SplitHostPort(o.TargetAddress)
		if e != nil {
			return result, fmt.Errorf("target-address: %w", e)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return result, errors.New("hostname targets require an explicit --allow-cidr")
		}
		bits := 128
		if ip.To4() != nil {
			bits = 32
		}
		o.AllowedCIDRs = []string{fmt.Sprintf("%s/%d", ip.String(), bits)}
	}
	if err = os.MkdirAll(filepath.Dir(output), 0750); err != nil {
		return result, err
	}
	if err = os.MkdirAll(filepath.Dir(tokenPath), 0750); err != nil {
		return result, err
	}
	created := false
	if info, e := os.Lstat(tokenPath); e == nil {
		if !info.Mode().IsRegular() {
			return result, errors.New("token must be a regular file")
		}
	} else if !os.IsNotExist(e) {
		return result, e
	} else {
		var secret [32]byte
		if _, err = rand.Read(secret[:]); err != nil {
			return result, err
		}
		if created, err = fileutil.WriteNew(tokenPath, []byte(hex.EncodeToString(secret[:])+"\n"), 0600); err != nil {
			if created {
				_ = os.Remove(tokenPath)
			}
			return result, err
		}
	}
	defer func() {
		if err != nil && created {
			_ = os.Remove(tokenPath)
		}
	}()
	tokenFile, err := os.Open(tokenPath)
	if err != nil {
		return result, err
	}
	secret, err := io.ReadAll(io.LimitReader(tokenFile, maxTokenBytes+3))
	closeErr := tokenFile.Close()
	if err != nil {
		return result, err
	}
	if closeErr != nil {
		return result, closeErr
	}
	// The loader's limit and syntax rules apply, so a token accepted here
	// can never fail later in LoadConfig.
	value := strings.TrimSuffix(strings.TrimSuffix(string(secret), "\n"), "\r")
	if validateToken(value) != nil {
		return result, fmt.Errorf("token file %s is empty, larger than %d bytes, or malformed", tokenPath, maxTokenBytes)
	}
	tokenRef, e := filepath.Rel(filepath.Dir(output), tokenPath)
	if e != nil {
		tokenRef = tokenPath
	}
	tokenRef = filepath.ToSlash(tokenRef)
	adminListen := o.AdminListen
	if adminListen == "" {
		adminListen = DefaultAdminListen
	}
	m := map[string]any{
		"node":   map[string]any{"id": o.NodeID},
		"admin":  map[string]any{"enabled": true, "listen": adminListen},
		"limits": map[string]any{"max_sessions": o.MaxSessions},
		"transport": map[string]any{"protocol": o.Protocol, "protocol_version": ProtocolVersion, "sql_replay": false,
			"recovery": map[string]any{"enabled": o.Recovery, "grace": "30s", "barrier_timeout": "5s"}},
	}
	if limit := generatedMetadataLimitMiB(o.MaxSessions); limit > 0 {
		m["transport"].(map[string]any)["metadata_limit"] = fmt.Sprintf("%dMiB", limit)
	}
	if o.Role != "egress" {
		endpoints := []any{}
		for i, url := range o.PeerURLs {
			endpoints = append(endpoints, map[string]any{"id": fmt.Sprintf("link-%d", i+1), "url": url, "priority": i + 1})
		}
		peer := map[string]any{"id": o.PeerID, "expected_node_id": o.PeerID, "credential_ref": o.PeerID, "token_file": tokenRef, "tls": map[string]any{"verify": true}, "endpoints": endpoints}
		if o.AllowInsecureHTTP {
			peer["allow_insecure_http"] = true
		}
		if o.Reverse {
			// The peer dials in; its token authenticates the tunnel.
			peer = map[string]any{"id": o.PeerID, "mode": "reverse", "expected_node_id": o.PeerID, "credential_ref": o.PeerID, "token_file": tokenRef}
			httpCfg, e := generatedHTTP(o)
			if e != nil {
				return result, e
			}
			m["http"] = httpCfg
		}
		m["peers"] = []any{peer}
		remoteTarget := o.PeerTargetID
		if remoteTarget == "" {
			remoteTarget = o.TargetID
		}
		m["listeners"] = []any{map[string]any{"id": "database-local", "listen": o.Listen, "peer": o.PeerID, "target": remoteTarget}}
	}
	if o.Role != "ingress" {
		inbound := o.MaxSessions
		if o.InboundMaxSessions > 0 && o.InboundMaxSessions < inbound {
			inbound = o.InboundMaxSessions
		}
		if o.Reverse {
			endpoints := []any{}
			for i, url := range o.PeerURLs {
				endpoints = append(endpoints, map[string]any{"id": fmt.Sprintf("link-%d", i+1), "url": url, "priority": i + 1})
			}
			transport := o.Protocol
			if transport == "" {
				transport = "auto"
			}
			rev := map[string]any{"enabled": true, "peer_node_id": o.PeerID, "token_file": tokenRef, "transport": transport, "tls": map[string]any{"verify": true}, "endpoints": endpoints}
			if o.AllowInsecureHTTP {
				rev["allow_insecure_http"] = true
			}
			m["reverse"] = rev
		} else {
			httpCfg, e := generatedHTTP(o)
			if e != nil {
				return result, e
			}
			m["http"] = httpCfg
		}
		m["targets"] = map[string]any{o.TargetID: map[string]any{"network": "tcp", "address": o.TargetAddress, "allowed_cidrs": o.AllowedCIDRs}}
		m["auth"] = map[string]any{"inbound_peers": []any{map[string]any{"id": o.PeerID, "token_files": []string{tokenRef}, "allowed_targets": []string{o.TargetID}, "max_sessions": inbound}}}
	}
	var data bytes.Buffer
	data.WriteString("# Generated by dbbridge gen-config. Keep the token file private and share it with the matching peer.\n")
	data.WriteString("# Token files and the auth/token settings below are re-read every auth.reload_interval (default 5s) and on SIGHUP;\n")
	data.WriteString("# other settings take effect after a restart.\n")
	encoder := yaml.NewEncoder(&data)
	encoder.SetIndent(2)
	if err = encoder.Encode(m); err != nil {
		return result, err
	}
	if err = encoder.Close(); err != nil {
		return result, err
	}
	// Validate using the real loader, including credential and TLS material checks.
	temp, err := os.CreateTemp(filepath.Dir(output), ".dbbridge-config-*.yaml")
	if err != nil {
		return result, err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if _, err = temp.Write(data.Bytes()); err != nil {
		temp.Close()
		return result, err
	}
	if err = temp.Close(); err != nil {
		return result, err
	}
	if _, err = LoadConfig(tempName); err != nil {
		return result, fmt.Errorf("generated config validation: %w", err)
	}
	if o.Force {
		// The validated temp file was closed without fsync; flush it before
		// it replaces the output, then make the rename durable.
		if err = fileutil.PreserveAttributes(previous, tempName); err != nil {
			return result, fmt.Errorf("keep permissions of %s: %w", output, err)
		}
		if err = syncFile(tempName); err == nil {
			if err = os.Rename(tempName, output); err == nil {
				err = fileutil.SyncDir(filepath.Dir(output))
			}
		}
	} else {
		var wrote bool
		if wrote, err = fileutil.WriteNew(output, data.Bytes(), 0600); err != nil && wrote {
			// The configuration was created by this call; remove it so that
			// no output references a token the deferred cleanup removes.
			_ = os.Remove(output)
		}
	}
	if err != nil {
		return result, err
	}
	return GeneratedFiles{Config: output, TokenFile: tokenPath, TokenCreated: created}, nil
}

// generatedHTTP is the http section of a node that serves peers.
func generatedHTTP(o GenerateOptions) (map[string]any, error) {
	httpCfg := map[string]any{"enabled": true, "listen": o.HTTPListen}
	if o.TLSCert != "" {
		cert, e := filepath.Abs(o.TLSCert)
		if e != nil {
			return nil, e
		}
		key, e := filepath.Abs(o.TLSKey)
		if e != nil {
			return nil, e
		}
		httpCfg["tls"] = map[string]any{"enabled": true, "cert_file": filepath.ToSlash(cert), "key_file": filepath.ToSlash(key)}
	}
	return httpCfg, nil
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// generatedMetadataLimitMiB sizes transport.metadata_limit for a large
// --max-sessions so the generated pair passes the per-session metadata check.
// It returns 0 when the 32 MiB default suffices.
func generatedMetadataLimitMiB(maxSessions int) int64 {
	const perSession, overhead = 112 << 10, 4 << 20
	need := int64(maxSessions)*perSession + overhead
	if need <= 32<<20 {
		return 0
	}
	return (need + (1 << 20) - 1) >> 20
}
