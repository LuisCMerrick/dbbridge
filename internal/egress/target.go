package egress

import (
	"context"
	"errors"
	"net"
	"time"
)

// errTargetNotAllowed reports a target that resolved, but to no address its
// allowed_cidrs permit: an authorization refusal, unlike a resolver failure.
var errTargetNotAllowed = errors.New("no allowed target address")

// resolveTarget resolves a configured target once and returns every
// resolved address that the target's CIDR allow-list permits, in resolver
// order. Dialing these exact IPs closes the DNS-rebinding window; trying each
// in turn keeps a single dead A/AAAA record from failing every create.
// ctx bounds resolution, so the caller covers resolution and dialing with
// one deadline that Close cancels.
func resolveTarget(ctx context.Context, network, address string, cidrs []string) ([]string, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, errors.New("only TCP targets supported")
	}
	host, port, e := net.SplitHostPort(address)
	if e != nil {
		return nil, e
	}
	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		ips, e = net.DefaultResolver.LookupIP(ctx, "ip", host)
		if e != nil {
			return nil, e
		}
	}
	nets := []*net.IPNet{}
	for _, v := range cidrs {
		_, n, e := net.ParseCIDR(v)
		if e != nil {
			return nil, e
		}
		nets = append(nets, n)
	}
	var out []string
	seen := map[string]bool{}
	for _, ip := range ips {
		if network == "tcp4" && ip.To4() == nil || network == "tcp6" && ip.To4() != nil {
			continue
		}
		allowed := len(nets) == 0
		for _, n := range nets {
			if n.Contains(ip) {
				allowed = true
				break
			}
		}
		if addr := net.JoinHostPort(ip.String(), port); allowed && !seen[addr] {
			seen[addr] = true
			out = append(out, addr)
		}
	}
	if len(out) == 0 {
		return nil, errTargetNotAllowed
	}
	return out, nil
}

// dialTarget tries each allowed address within one overall timeout, giving
// every remaining address a fair share of the time that is left.
func dialTarget(ctx context.Context, network string, addrs []string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last error
	for i, addr := range addrs {
		deadline, _ := ctx.Deadline()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		share := max(remaining/time.Duration(len(addrs)-i), min(remaining, 2*time.Second))
		attempt, done := context.WithTimeout(ctx, share)
		conn, e := (&net.Dialer{}).DialContext(attempt, network, addr)
		done()
		if e == nil {
			return conn, nil
		}
		last = e
		if ctx.Err() != nil {
			break
		}
	}
	if last == nil {
		last = context.DeadlineExceeded
	}
	return nil, last
}
