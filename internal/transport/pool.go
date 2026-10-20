package transport

import (
	"context"
	"dbbridge/internal/resource"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Per-peer connection budgets used by dial(). They are the only limit on how
// many connections a pool opens (MaxConnsPerHost is unset), so HTTP/1.1
// deployments (e.g. behind an HTTP/1.1 reverse proxy) are not capped at a
// fixed 32 concurrent up POSTs or control calls per endpoint.
// SetResources resizes them from the node's session and connection limits:
// over HTTP/1.1 every SSE stream pins a data connection for its lifetime and
// each in-flight up POST needs another, so the data budget matches the
// process-wide data request budget (2 per session plus headroom).
const (
	defaultPeerDataConns    = 256
	defaultPeerControlConns = 64
	peerConnHeadroom        = 32
	idleConnsPerPool        = 64
)

// clientIdleConnTimeout drops idle peer connections well before the egress
// server (90s) and common reverse proxies (nginx keepalive_timeout 75s, many
// load balancers 60s) close them, so a non-replayable POST is not written on
// a connection the far side is closing at that moment.
const clientIdleConnTimeout = 50 * time.Second

func (c *TripleTransportClient) makePools() {
	headerTimeout := func(d time.Duration) time.Duration {
		if d > 0 {
			return d
		}
		return c.tCfg.ControlRequestTimeout
	}
	pingEvery := c.tCfg.Heartbeat
	if pingEvery <= 0 {
		pingEvery = 15 * time.Second
	}
	pool := func(timeout, respHeader time.Duration, idle int, control bool) *http.Client {
		tr := &http.Transport{TLSClientConfig: c.tlsClientConfig.Clone(), ForceAttemptHTTP2: true, MaxIdleConns: idle, MaxIdleConnsPerHost: idle, IdleConnTimeout: clientIdleConnTimeout, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: respHeader, DisableCompression: true}
		c.bindDial(tr, control)
		// Health-check h2 connections so a blackholed one is dropped from the
		// pool within about one heartbeat instead of per-stream timeouts.
		tr.HTTP2 = &http.HTTP2Config{SendPingTimeout: pingEvery, PingTimeout: 5 * time.Second}
		if c.tCfg.UseEnvironmentProxy {
			tr.Proxy = http.ProxyFromEnvironment
		}
		return &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	c.sseClient = pool(0, c.tCfg.ControlRequestTimeout, defaultPeerDataConns, false)
	// up POSTs wait for their response headers as long as the request itself
	// may take, not control_request_timeout.
	c.dataClient = pool(c.tCfg.UpRequestTimeout, headerTimeout(c.tCfg.UpRequestTimeout), idleConnsPerPool, false)
	c.controlClient = pool(c.tCfg.ControlRequestTimeout, c.tCfg.ControlRequestTimeout, idleConnsPerPool, true)
}

func (c *TripleTransportClient) SetResources(r *resource.Manager) {
	c.resources = r
	c.shared.mu.Lock()
	register := !c.shared.resourcesSet
	if !c.shared.resourcesSet {
		c.shared.dataConnections = resource.New(min(r.DataConnections.Limit(), 2*r.Sessions.Limit()+peerConnHeadroom))
		c.shared.controlConnections = resource.New(min(r.ControlConnections.Limit(), r.Sessions.Limit()+peerConnHeadroom))
		c.shared.resourcesSet = true
	}
	c.shared.mu.Unlock()
	if register {
		r.RegisterIdleEvictor(c.closeIdle)
	}
}

func (c *TripleTransportClient) closeIdle(control bool) { c.closeIdleExcept(control, nil) }

// bindDial makes tr dial through the connection budgets as one of this
// endpoint's pools.
func (c *TripleTransportClient) bindDial(tr *http.Transport, control bool) {
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return c.dial(ctx, network, address, control, tr)
	}
}

// dial opens a connection for pool self (nil for WebSocket dials) once it
// holds a per-peer and a process-wide connection slot. Waiting for a slot is
// bounded by the requesting call's own deadline (see budgetWaitContext), not
// by peer_dial_timeout, which bounds the TCP connect only; a wait that runs
// out wraps ErrLocalConnBudget.
func (c *TripleTransportClient) dial(ctx context.Context, network, address string, control bool, self http.RoundTripper) (net.Conn, error) {
	d := &net.Dialer{Timeout: c.tCfg.PeerDial(), KeepAlive: 30 * time.Second}
	c.shared.mu.Lock()
	peer := c.shared.dataConnections
	if control {
		peer = c.shared.controlConnections
	}
	c.shared.mu.Unlock()
	kind := "data"
	if control {
		kind = "control"
	}
	waitCtx, cancel := c.budgetWaitContext(ctx)
	defer cancel()
	if err := takeSlot(waitCtx, peer, "per-peer "+kind+" connections", func() { c.closeIdleExcept(control, self) }); err != nil {
		return nil, err
	}
	release := func() { peer.Put(1) }
	if c.resources != nil {
		global, metadata := c.resources.DataConnections, c.resources.Metadata
		if control {
			global, metadata = c.resources.ControlConnections, c.resources.ControlMetadata
		}
		if err := takeSlot(waitCtx, global, kind+" connections", func() { c.resources.EvictIdle(control) }); err != nil {
			peer.Put(1)
			return nil, err
		}
		if err := metadata.Take(waitCtx, 8<<10); err != nil {
			global.Put(1)
			peer.Put(1)
			return nil, fmt.Errorf("%w: connection metadata: %w", ErrLocalConnBudget, err)
		}
		release = func() { metadata.Put(8 << 10); global.Put(1); peer.Put(1) }
	}
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		release()
		return nil, err
	}
	return resource.LeaseConn(conn, release), nil
}

// nextSSEShard picks the SSE pool shard for the next subscription, creating
// the h2_pool_size shards on first use.
func (c *TripleTransportClient) nextSSEShard() *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sseShards) == 0 {
		n := c.tCfg.H2PoolSize
		if n <= 0 {
			n = 4
		}
		c.sseShards = append(c.sseShards, c.sseClient)
		for i := 1; i < n; i++ {
			tr := c.sseClient.Transport.(*http.Transport).Clone()
			c.bindDial(tr, false)
			c.sseShards = append(c.sseShards, &http.Client{Transport: tr, CheckRedirect: c.sseClient.CheckRedirect})
		}
	}
	selected := c.sseShards[c.sseNext%uint64(len(c.sseShards))]
	c.sseNext++
	return selected
}
