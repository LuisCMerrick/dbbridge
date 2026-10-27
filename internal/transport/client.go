package transport

import (
	"crypto/tls"
	"crypto/x509"
	"dbbridge/internal/config"
	"dbbridge/internal/resource"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

type binding struct{ id, mode string }
type clientShared struct {
	mu                                  sync.Mutex
	endpoints                           map[string]*TripleTransportClient
	token                               string
	provider                            func() string
	dataConnections, controlConnections *resource.Budget
	resourcesSet                        bool
	// dialer opens peer connections; nil means TCP (see peerDialer).
	dialer PeerDialer
}
type TripleTransportClient struct {
	resources                            *resource.Manager
	sseClient, dataClient, controlClient *http.Client
	sseShards                            []*http.Client
	sseNext                              uint64
	mu                                   sync.Mutex
	peerURL, instanceID, expectedNodeID  string
	tCfg                                 *config.TransportConfig
	tlsVerify                            bool
	tlsClientConfig                      *tls.Config
	shared                               *clientShared
	bindings                             sync.Map
	wsSpare                              wsSpareState // guarded by mu
	wsSpareClosed                        bool         // guarded by mu
}

func NewTripleTransportClientForURL(url, token string, cfg *config.TransportConfig, inst string, verify bool) *TripleTransportClient {
	return newTripleTransportClient(url, token, cfg, inst, verify, nil)
}

// newTripleTransportClient builds the client; roots, when non-nil, replaces the
// system trust store for this peer (tls.ca_file).
func newTripleTransportClient(url, token string, cfg *config.TransportConfig, inst string, verify bool, roots *x509.CertPool) *TripleTransportClient {
	c := &TripleTransportClient{peerURL: strings.TrimRight(url, "/"), instanceID: inst, tCfg: cfg, tlsVerify: verify, tlsClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !verify, RootCAs: roots}, shared: &clientShared{endpoints: map[string]*TripleTransportClient{}, token: token, dataConnections: resource.New(defaultPeerDataConns), controlConnections: resource.New(defaultPeerControlConns)}}
	c.makePools()
	c.shared.endpoints[c.peerURL] = c
	warnInsecureTLS(c.peerURL, verify)
	return c
}

func NewTripleTransportClient(p *config.PeerConfig, t *config.TransportConfig, inst string) *TripleTransportClient {
	if p.IsReverse() {
		// A reverse peer has no endpoints: its requests go to the virtual
		// endpoint over streams of the tunnel it dials in. Until a tunnel
		// is attached (SetPeerDialer) every dial fails fast.
		c := newTripleTransportClient(ReverseEndpointURL, p.Token, t, inst, true, nil)
		c.expectedNodeID = p.ExpectedNodeID
		c.shared.dialer = TunnelDialer{Timeout: t.PeerDial()}
		return c
	}
	url := p.Endpoints[0].URL
	c := newTripleTransportClient(url, p.Token, t, inst, p.TLS.IsVerify(), loadPeerRoots(p))
	c.expectedNodeID = p.ExpectedNodeID
	return c
}

func (c *TripleTransportClient) URL() string { return c.peerURL }
func (c *TripleTransportClient) WithURL(url string) *TripleTransportClient {
	url = strings.TrimRight(url, "/")
	c.shared.mu.Lock()
	defer c.shared.mu.Unlock()
	if v := c.shared.endpoints[url]; v != nil {
		return v
	}
	n := &TripleTransportClient{peerURL: url, instanceID: c.instanceID, expectedNodeID: c.expectedNodeID, tCfg: c.tCfg, tlsVerify: c.tlsVerify, tlsClientConfig: c.tlsClientConfig.Clone(), shared: c.shared, resources: c.resources}
	n.makePools()
	c.shared.endpoints[url] = n
	warnInsecureTLS(url, n.tlsVerify)
	return n
}
func (c *TripleTransportClient) SetTokenProvider(p func() string) {
	c.shared.mu.Lock()
	c.shared.provider = p
	c.shared.mu.Unlock()
}
func (c *TripleTransportClient) GetToken() string {
	c.shared.mu.Lock()
	p, t := c.shared.provider, c.shared.token
	c.shared.mu.Unlock()
	if p != nil {
		return p()
	}
	return t
}
func (c *TripleTransportClient) UpdateToken(t string) {
	c.shared.mu.Lock()
	c.shared.token = t
	c.shared.mu.Unlock()
}
func (c *TripleTransportClient) BindStream(sid string, ep uint64, id, mode string) {
	c.bindings.Store(fmt.Sprintf("%s:%d", sid, ep), binding{id, mode})
}
func (c *TripleTransportClient) UnbindStream(sid string, ep uint64) {
	c.bindings.Delete(fmt.Sprintf("%s:%d", sid, ep))
}
func (c *TripleTransportClient) getBinding(sid string, ep uint64) (binding, error) {
	v, ok := c.bindings.Load(fmt.Sprintf("%s:%d", sid, ep))
	if !ok {
		return binding{}, fmt.Errorf("%w: missing stream binding", ErrProtocol)
	}
	return v.(binding), nil
}

func (c *TripleTransportClient) Close() {
	c.dropWSSpare(true)
	c.closeIdle(false)
	c.closeIdle(true)
}
