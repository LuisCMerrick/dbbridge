package config

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

func validateSocketAddress(address string, listener bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || !listener && n == 0 {
		return errors.New("TCP port must be numeric and in range")
	}
	if !listener && host == "" {
		return errors.New("target host is required")
	}
	if strings.TrimSpace(host) != host || strings.ContainsAny(host, "/\\?#@ \t\r\n") {
		return errors.New("invalid TCP host")
	}
	return nil
}

// listenHost describes the addresses one listen host binds.
type listenHost struct {
	all   bool     // "" or "::": every IPv4 and IPv6 address (dual stack)
	v4any bool     // 0.0.0.0: every IPv4 address
	ips   []net.IP // concrete addresses; localhost is 127.0.0.1 and ::1
	name  string   // other hostname, resolved only at bind time
}

func parseListenHost(h string) listenHost {
	h = strings.ToLower(strings.TrimSuffix(strings.Trim(h, "[]"), "."))
	switch h {
	case "", "::":
		return listenHost{all: true}
	case "localhost":
		return listenHost{ips: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}}
	}
	ip := net.ParseIP(h)
	if ip == nil {
		return listenHost{name: h}
	}
	if ip.IsUnspecified() {
		if ip.To4() != nil {
			return listenHost{v4any: true}
		}
		return listenHost{all: true}
	}
	return listenHost{ips: []net.IP{ip}}
}

// listenHostsOverlap reports whether two listeners on the same port would
// collide. localhost is 127.0.0.1 plus ::1, 0.0.0.0 covers every IPv4
// address and "::" or an empty host covers everything. An unresolved
// hostname conflicts with wildcards and with the same name only.
func listenHostsOverlap(a, b string) bool {
	x, y := parseListenHost(a), parseListenHost(b)
	if x.all || y.all {
		return true
	}
	hasV4 := func(l listenHost) bool {
		if l.v4any || l.name != "" {
			return true
		}
		for _, ip := range l.ips {
			if ip.To4() != nil {
				return true
			}
		}
		return false
	}
	if x.v4any && hasV4(y) || y.v4any && hasV4(x) {
		return true
	}
	if x.name != "" || y.name != "" {
		return x.name == y.name
	}
	for _, i := range x.ips {
		for _, j := range y.ips {
			if i.Equal(j) {
				return true
			}
		}
	}
	return false
}

// ValidateCredentialReferences applies the same immutable main-config constraints
// to both startup and hot-reload candidates. nil targets is useful for standalone
// authenticator users that do not configure an Egress role.
func ValidateCredentialReferences(f *AuthConfigFile, refs map[string]string, targets map[string]TargetConfig) error {
	out := make(map[string]bool, len(f.OutboundPeers))
	for _, p := range f.OutboundPeers {
		out[p.ID] = true
	}
	for _, ref := range refs {
		if !out[ref] {
			return fmt.Errorf("missing credential_ref %s", ref)
		}
	}
	if targets != nil {
		for _, p := range f.InboundPeers {
			for _, id := range p.AllowedTargets {
				if _, ok := targets[id]; !ok {
					return fmt.Errorf("unknown ACL target %q", id)
				}
			}
		}
	}
	return nil
}

// isLoopbackHost is the single loopback rule used for plaintext peer URLs,
// peer TLS verification and listener addresses: "localhost" and every
// address in 127.0.0.0/8 or ::1.
func isLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// validatePeerTLS enforces the client TLS options of one peer. Endpoint URLs
// must already be validated.
func validatePeerTLS(p *PeerConfig, dir string, offline bool) error {
	return validateClientTLS(&p.TLS, p.Endpoints, "peers["+p.ID+"]", "endpoint", dir, offline)
}

// validateClientTLS enforces client TLS options (verify, ca_file) for the
// endpoints they apply to; prefix names the section in messages. It resolves
// ca_file to an absolute path.
func validateClientTLS(t *TLSClientConfig, eps []EndpointConfig, prefix, kind, dir string, offline bool) error {
	if !t.IsVerify() {
		if t.CAFile != "" {
			return fmt.Errorf("%s.tls.ca_file cannot be combined with tls.verify: false", prefix)
		}
		if !t.AllowInsecureSkipVerify {
			for _, ep := range eps {
				u, err := url.Parse(ep.URL)
				if err != nil {
					return err
				}
				if u.Scheme == "https" && !isLoopbackHost(u.Hostname()) {
					return fmt.Errorf("%s.tls.verify: false for non-loopback %s %s requires tls.allow_insecure_skip_verify: true", prefix, kind, ep.ID)
				}
			}
		}
	}
	if t.CAFile == "" || offline {
		return nil
	}
	path := t.CAFile
	if !filepath.IsAbs(path) {
		path = filepath.Join(dir, path)
	}
	data, err := readFileLimited(path, 1<<20, "tls.ca_file too large")
	if err != nil {
		return WithCode(CodeTLS, fmt.Errorf("%s.tls.ca_file: %w", prefix, err))
	}
	if !x509.NewCertPool().AppendCertsFromPEM(data) {
		return WithCode(CodeTLS, fmt.Errorf("%s.tls.ca_file contains no PEM certificates", prefix))
	}
	t.CAFile = path
	return nil
}
