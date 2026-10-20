package transport

import (
	"crypto/tls"
	"crypto/x509"
	"dbbridge/internal/config"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// warnInsecureTLS logs loudly when certificate verification is disabled for a
// TLS peer: the bearer token, session tokens and all DB traffic are then open
// to a MITM. Gating tls.verify=false for non-loopback peers belongs to config
// validation; transport can only make the exposure visible.
func warnInsecureTLS(url string, verify bool) {
	if verify || !strings.HasPrefix(strings.ToLower(url), "https://") {
		return
	}
	slog.Warn("TLS certificate verification is DISABLED for peer (tls.verify=false); peer credentials and all traffic are exposed to man-in-the-middle attacks. Do not use in production.", "peer_url", url)
}

// loadPeerRoots loads tls.ca_file. Config validation already checked that the
// file is readable PEM; if it became unreadable since, fail closed with an
// empty pool rather than silently falling back to the system roots.
func loadPeerRoots(p *config.PeerConfig) *x509.CertPool {
	if p.TLS.CAFile == "" {
		return nil
	}
	pool := x509.NewCertPool()
	pem, err := os.ReadFile(p.TLS.CAFile)
	if err != nil || !pool.AppendCertsFromPEM(pem) {
		slog.Error("cannot load tls.ca_file; TLS to this peer will fail", "peer", p.ID, "ca_file", p.TLS.CAFile, "error", err)
		return x509.NewCertPool()
	}
	return pool
}

// wsTLS never offers h2: RFC 6455 Upgrade uses HTTP/1.1. Each Transport and
// Dialer owns its TLS configuration, including net/http's ALPN mutations.
func (c *TripleTransportClient) wsTLS() *tls.Config {
	var cfg *tls.Config
	if c.tlsClientConfig != nil {
		cfg = c.tlsClientConfig.Clone()
	} else {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !c.tlsVerify}
	}
	cfg.NextProtos = []string{"http/1.1"}
	return cfg
}

// wsTLSFailure maps certificate verification failures (and a non-TLS reply on
// a TLS port) to a fatal ErrProtocol, or returns nil for any other error.
func wsTLSFailure(err error) error {
	var verify *tls.CertificateVerificationError
	var authority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var record tls.RecordHeaderError
	if errors.As(err, &verify) || errors.As(err, &authority) || errors.As(err, &hostname) || errors.As(err, &invalid) || errors.As(err, &record) {
		return fmt.Errorf("%w: WS TLS verification: %v", ErrProtocol, err)
	}
	return nil
}
