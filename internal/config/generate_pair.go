package config

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"dbbridge/internal/fileutil"
)

type GeneratePairNodeOptions struct {
	NodeID, Listen, HTTPListen, TargetID, TargetAddress string
	// AdminListen is the node's loopback admin address.
	AdminListen            string
	TLSCert, TLSKey        string
	PeerURLs, AllowedCIDRs []string
}

type GeneratePairOptions struct {
	OutputDir, Mode, TokenFile, Protocol string
	A, B                                 GeneratePairNodeOptions
	Recovery, AllowInsecureHTTP, Force   bool
	MaxSessions                          int
	// Reverse generates reverse mode for a one-way pair (a-to-b or
	// b-to-a): the ingress node is the public side, the egress node (the
	// database side) dials it.
	Reverse bool
}

type GeneratedPairFiles struct {
	ConfigA, ConfigB, TokenFile string
	TokenCreated                bool
}

func DefaultGeneratePairOptions() GeneratePairOptions {
	return GeneratePairOptions{
		OutputDir: ".", Mode: "bidirectional", Protocol: "auto", Recovery: true, MaxSessions: 200,
		A: GeneratePairNodeOptions{NodeID: "A", Listen: "127.0.0.1:13306", HTTPListen: "127.0.0.1:18080", AdminListen: "127.0.0.1:18090", TargetID: "mysql", TargetAddress: "127.0.0.1:3306"},
		B: GeneratePairNodeOptions{NodeID: "B", Listen: "127.0.0.1:13307", HTTPListen: "127.0.0.1:18081", AdminListen: "127.0.0.1:18091", TargetID: "mysql", TargetAddress: "127.0.0.1:3306"},
	}
}

// GeneratePair always produces both ends. Both configurations are staged and
// validated before publishing either; ordinary publication failures roll back
// both files. Existing tokens are reused even when replacing configurations.
func GeneratePair(o GeneratePairOptions) (result GeneratedPairFiles, err error) {
	roles := [2]string{"peer", "peer"}
	switch o.Mode {
	case "bidirectional":
	case "a-to-b":
		roles = [2]string{"ingress", "egress"}
	case "b-to-a":
		roles = [2]string{"egress", "ingress"}
	default:
		return result, errors.New("mode must be bidirectional, a-to-b, or b-to-a")
	}
	if o.Reverse && o.Mode == "bidirectional" {
		return result, errors.New("reverse mode needs --mode a-to-b or b-to-a (the egress node is the database side behind NAT)")
	}
	if o.A.NodeID == "" || o.B.NodeID == "" || o.A.NodeID == o.B.NodeID {
		return result, errors.New("node IDs for A and B must be nonempty and different")
	}
	if o.OutputDir == "" {
		return result, errors.New("output directory is required")
	}
	dir, err := filepath.Abs(o.OutputDir)
	if err != nil {
		return result, err
	}
	paths := [2]string{filepath.Join(dir, "peer-a.yaml"), filepath.Join(dir, "peer-b.yaml")}
	token := o.TokenFile
	if token == "" {
		token = filepath.Join(dir, "shared.token")
	}
	token, err = filepath.Abs(token)
	if err != nil {
		return result, err
	}
	for _, path := range paths {
		if path == token || runtime.GOOS == "windows" && strings.EqualFold(path, token) {
			return result, errors.New("configuration and token paths must differ")
		}
		if err = checkPairOutput(path, o.Force); err != nil {
			return result, err
		}
		if cfgInfo, e := os.Stat(path); e == nil {
			if tokenInfo, e := os.Stat(token); e == nil && os.SameFile(cfgInfo, tokenInfo) {
				return result, errors.New("configuration and token files must differ")
			}
		}
	}
	if err = os.MkdirAll(dir, 0750); err != nil {
		return result, err
	}
	createdToken, keepToken := false, false
	var staged []string
	defer func() {
		for _, path := range staged {
			_ = os.Remove(path)
		}
		if createdToken && !keepToken {
			if e := os.Remove(token); e != nil && !os.IsNotExist(e) {
				err = errors.Join(err, fmt.Errorf("remove newly created token: %w", e))
			}
		}
	}()
	nodes := [2]GeneratePairNodeOptions{o.A, o.B}
	// Distinct admin ports let both nodes run on one host.
	for i, admin := range [2]string{DefaultAdminListen, "127.0.0.1:18091"} {
		if nodes[i].AdminListen == "" {
			nodes[i].AdminListen = admin
		}
	}
	outputs := make([]pairOutput, 2)
	for i, node := range nodes {
		other := nodes[1-i]
		urls := node.PeerURLs
		// Forward: the ingress dials the egress. Reverse: the egress (the
		// database side) dials the ingress (the public side).
		dialer := roles[i] != "egress"
		if o.Reverse {
			dialer = roles[i] == "egress"
		}
		if dialer && len(urls) == 0 {
			var endpoint string
			endpoint, err = pairEndpoint(other)
			if err != nil {
				return result, err
			}
			urls = []string{endpoint}
		}
		var stage string
		stage, err = reservePairPath(dir, ".dbbridge-pair-*.yaml")
		if err != nil {
			return result, err
		}
		staged = append(staged, stage)
		var generated GeneratedFiles
		generated, err = Generate(GenerateOptions{
			Output: stage, Role: roles[i], NodeID: node.NodeID, PeerID: other.NodeID,
			Listen: node.Listen, HTTPListen: node.HTTPListen, AdminListen: node.AdminListen,
			TargetID: node.TargetID, PeerTargetID: other.TargetID, TargetAddress: node.TargetAddress,
			TokenFile: token, Protocol: o.Protocol, TLSCert: node.TLSCert, TLSKey: node.TLSKey,
			PeerURLs: urls, AllowedCIDRs: node.AllowedCIDRs,
			Recovery: o.Recovery, AllowInsecureHTTP: o.AllowInsecureHTTP, MaxSessions: o.MaxSessions,
			InboundMaxSessions: inboundQuota(roles[i], o.MaxSessions),
			Reverse:            o.Reverse,
		})
		if err != nil {
			return result, fmt.Errorf("generate node %s: %w", node.NodeID, err)
		}
		createdToken = createdToken || generated.TokenCreated
		var data []byte
		data, err = os.ReadFile(stage)
		if err != nil {
			return result, err
		}
		outputs[i] = pairOutput{path: paths[i], data: data}
	}
	keepToken, err = publishPair(outputs, o.Force, fileutil.WriteNew)
	if err != nil {
		return result, err
	}
	return GeneratedPairFiles{ConfigA: paths[0], ConfigB: paths[1], TokenFile: token, TokenCreated: createdToken}, nil
}

func pairEndpoint(node GeneratePairNodeOptions) (string, error) {
	host, port, err := net.SplitHostPort(node.HTTPListen)
	if err != nil {
		return "", fmt.Errorf("node %s http-listen: %w", node.NodeID, err)
	}
	if ip := net.ParseIP(host); host == "" || ip != nil && ip.IsUnspecified() {
		host = "127.0.0.1"
		if ip != nil && ip.To4() == nil {
			host = "::1"
		}
	}
	scheme := "http"
	if node.TLSCert != "" {
		scheme = "https"
	}
	return scheme + "://" + net.JoinHostPort(host, port), nil
}

func checkPairOutput(path string, force bool) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing non-regular config file: %s", path)
	}
	if !force {
		return fmt.Errorf("config already exists: %s (use --force to replace the pair)", path)
	}
	return nil
}

func reservePairPath(dir, pattern string) (string, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	name := f.Name()
	closeErr := f.Close()
	removeErr := os.Remove(name)
	return name, errors.Join(closeErr, removeErr)
}

type pairOutput struct {
	path, backup string
	data         []byte
	written      bool
}

// The bool prevents deleting a new token after successful publication, or when
// rollback itself fails and a configuration might still reference that token.
func publishPair(outputs []pairOutput, force bool, writeNew func(string, []byte, fs.FileMode) (bool, error)) (keepToken bool, err error) {
	defer func() {
		if err == nil || keepToken {
			return
		}
		var rollbackErr error
		for i := len(outputs) - 1; i >= 0; i-- {
			item := outputs[i]
			if item.written {
				rollbackErr = errors.Join(rollbackErr, os.Remove(item.path))
			}
			if item.backup != "" {
				rollbackErr = errors.Join(rollbackErr, os.Rename(item.backup, item.path))
			}
		}
		if rollbackErr != nil {
			keepToken = true
			err = errors.Join(err, fmt.Errorf("restore original pair (backups retained where restoration failed): %w", rollbackErr))
		}
	}()
	for i := range outputs {
		item := &outputs[i]
		if err = checkPairOutput(item.path, force); err != nil {
			return false, err
		}
		var previous os.FileInfo
		if info, e := os.Lstat(item.path); e == nil {
			previous = info
			var backup string
			backup, err = reservePairPath(filepath.Dir(item.path), ".dbbridge-pair-backup-*")
			if err != nil {
				return false, err
			}
			if err = os.Rename(item.path, backup); err != nil {
				return false, err
			}
			item.backup = backup
			if err = fileutil.SyncDir(filepath.Dir(item.path)); err != nil {
				return false, err
			}
		} else if !os.IsNotExist(e) {
			return false, e
		}
		// A file this call created is rolled back even when its
		// publication failed after creation (for example a failed
		// directory sync).
		item.written, err = writeNew(item.path, item.data, 0600)
		if err != nil {
			return false, err
		}
		if previous != nil {
			if err = fileutil.PreserveAttributes(previous, item.path); err != nil {
				return false, fmt.Errorf("keep permissions of %s: %w", item.path, err)
			}
		}
	}
	keepToken = true
	dirs := map[string]bool{}
	for _, item := range outputs {
		if item.backup != "" {
			if e := os.Remove(item.backup); e != nil {
				err = errors.Join(err, fmt.Errorf("pair published; remove old backup %s: %w", item.backup, e))
			}
		}
		dirs[filepath.Dir(item.path)] = true
	}
	// Make the published pair (and removed backups) durable.
	for dir := range dirs {
		if e := fileutil.SyncDir(dir); e != nil {
			err = errors.Join(err, fmt.Errorf("pair published; sync directory %s: %w", dir, e))
		}
	}
	return keepToken, err
}

// inboundQuota leaves half of a bidirectional node's sessions to its local
// listeners so the remote peer cannot starve them.
func inboundQuota(role string, maxSessions int) int {
	if role == "peer" && maxSessions > 1 {
		return maxSessions / 2
	}
	return 0
}
