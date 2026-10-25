package config

import (
	"time"

	"dbbridge/internal/resource"
)

type Config struct {
	Node      NodeConfig              `yaml:"node"`
	HTTP      HTTPConfig              `yaml:"http"`
	Admin     AdminConfig             `yaml:"admin"`
	Auth      AuthConfig              `yaml:"auth"`
	Peers     []PeerConfig            `yaml:"peers"`
	Listeners []ListenerConfig        `yaml:"listeners"`
	Targets   map[string]TargetConfig `yaml:"targets"`
	Limits    LimitsConfig            `yaml:"limits"`
	Transport TransportConfig         `yaml:"transport"`
	// Reverse makes this node the database side of reverse-mode peers: it
	// dials out to the public-side node and serves its egress role over the
	// tunnel instead of (or besides) http.listen.
	Reverse ReverseConfig `yaml:"reverse"`

	// Runtime generated
	ConfigFileDir string            `yaml:"-"`
	Resources     *resource.Manager `yaml:"-"`
	// Warnings lists non-fatal findings from validation (for example short
	// bearer tokens). Callers should log them at startup and in check-config.
	Warnings []string `yaml:"-"`
	// offline skips reading secrets and TLS material (check-config --offline).
	offline bool
	// path is the absolute path the configuration was loaded from; inline
	// credentials are re-read from it on reload.
	path string
}

type NodeConfig struct {
	ID         string `yaml:"id"`
	InstanceID string `yaml:"-"` // Generated 128-bit random instance identifier on startup
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type HTTPConfig struct {
	Enabled           bool      `yaml:"enabled"`
	Listen            string    `yaml:"listen"`
	TLS               TLSConfig `yaml:"tls"`
	AllowedTransports []string  `yaml:"allowed_transports"`
	// AllowInsecureListen permits plaintext HTTP on a non-loopback
	// http.listen.
	AllowInsecureListen bool `yaml:"allow_insecure_listen"`
}

type AdminConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
	// AllowRemote must be set to bind the unauthenticated, plaintext admin
	// endpoint to a non-loopback address.
	AllowRemote bool `yaml:"allow_remote"`
}

// DefaultAdminListen does not collide with the HTTP listeners gen-config
// assigns by default (127.0.0.1:18080 and 127.0.0.1:18081).
const DefaultAdminListen = "127.0.0.1:18090"

// MaxH2PoolSize bounds transport.h2_pool_size, the number of HTTP client
// shards per endpoint for SSE subscriptions.
const MaxH2PoolSize = 64

// ShutdownTeardownBudget bounds the work after the drain phase
// (transport.shutdown_timeout): closing ingress sessions with remote cleanup,
// the egress server and the admin endpoint.
const ShutdownTeardownBudget = 15 * time.Second

// MaxShutdownDuration is the longest a graceful Node.Stop may take. Service
// managers must allow at least this long before killing the process.
func (c *Config) MaxShutdownDuration() time.Duration {
	return c.Transport.ShutdownTimeout + ShutdownTeardownBudget
}

// MinReloadInterval bounds how often a credentials file is re-read.
const MinReloadInterval = time.Second

type TLSClientConfig struct {
	Verify *bool `yaml:"verify"`
	// AllowInsecureSkipVerify is the explicit opt-in required for
	// verify: false when any endpoint of the peer is not a loopback address.
	AllowInsecureSkipVerify bool `yaml:"allow_insecure_skip_verify"`
	// CAFile is an optional PEM bundle of trusted roots for this peer. After
	// validation it holds an absolute path to a readable file containing at
	// least one certificate. Empty means the system trust store.
	CAFile string `yaml:"ca_file"`
}

func (t *TLSClientConfig) IsVerify() bool {
	if t.Verify == nil {
		return true // verify by default
	}
	return *t.Verify
}

// EndpointConfig specifies a candidate endpoint for a logical Peer
type EndpointConfig struct {
	ID       string `yaml:"id"`
	URL      string `yaml:"url"`
	Priority int    `yaml:"priority"`
}

// FailoverConfig specifies the priority failover policy for a Peer
type FailoverConfig struct {
	Enabled             *bool         `yaml:"enabled"`
	Strategy            string        `yaml:"strategy"`
	Failback            string        `yaml:"failback"`
	HealthcheckInterval time.Duration `yaml:"-"`
	HealthcheckTimeout  time.Duration `yaml:"-"`
	UnhealthyThreshold  int           `yaml:"unhealthy_threshold"`
	HealthyThreshold    int           `yaml:"healthy_threshold"`
	Cooldown            time.Duration `yaml:"-"`
	MaxConcurrentProbes int           `yaml:"max_concurrent_probes"`

	HealthcheckIntervalRaw interface{} `yaml:"healthcheck_interval"`
	HealthcheckTimeoutRaw  interface{} `yaml:"healthcheck_timeout"`
	CooldownRaw            interface{} `yaml:"cooldown"`
}

func (f *FailoverConfig) IsEnabled(numEndpoints int) bool {
	if f.Enabled == nil {
		return numEndpoints > 1
	}
	return *f.Enabled
}

type PeerRecoveryConfig struct {
	Enabled *bool `yaml:"enabled,omitempty"`
}

type PeerTransportConfig struct {
	Protocol string `yaml:"protocol"`
}

// Peer modes (peers[].mode).
const (
	// PeerModeForward: this node connects to the peer's endpoints (default).
	PeerModeForward = "forward"
	// PeerModeReverse: the peer connects to this node and carries every
	// session over that tunnel; the peer has no endpoints.
	PeerModeReverse = "reverse"
)

type PeerConfig struct {
	ID string `yaml:"id"`
	// Mode is forward (default) or reverse; validation sets the default.
	Mode           string               `yaml:"mode"`
	ExpectedNodeID string               `yaml:"expected_node_id"`
	CredentialRef  string               `yaml:"credential_ref"`
	Endpoints      []EndpointConfig     `yaml:"endpoints"`
	Failover       FailoverConfig       `yaml:"failover"`
	Recovery       *PeerRecoveryConfig  `yaml:"recovery,omitempty"`
	Transport      *PeerTransportConfig `yaml:"transport,omitempty"`
	TokenEnv       string               `yaml:"token_env"`
	TokenFile      string               `yaml:"token_file"`
	Token          string               `yaml:"-"` // Resolved token
	TLS            TLSClientConfig      `yaml:"tls"`
	// AllowInsecureHTTP permits plaintext http:// endpoints of this peer
	// that are not loopback addresses.
	AllowInsecureHTTP bool `yaml:"allow_insecure_http"`
}

// IsReverse reports whether sessions to this peer run over a tunnel the peer
// dials in, rather than over connections to its endpoints.
func (p *PeerConfig) IsReverse() bool { return p.Mode == PeerModeReverse }

func (p *PeerConfig) EffectiveRecoveryEnabled(globalRecovery bool) bool {
	if !globalRecovery {
		return false
	}
	if p.Recovery != nil && p.Recovery.Enabled != nil {
		return *p.Recovery.Enabled
	}
	return globalRecovery
}

// Reverse tunnel transports (reverse.transport).
const (
	TunnelTransportAuto     = "auto"
	TunnelTransportWS       = "ws"
	TunnelTransportSSE      = "http-sse"
	TunnelTransportLongPoll = "long-poll"
)

// Reverse tunnel defaults and bounds.
const (
	DefaultTunnelsPerEndpoint = 2
	MaxTunnelsPerEndpoint     = 16
	DefaultReverseMinBackoff  = 500 * time.Millisecond
	DefaultReverseMaxBackoff  = 30 * time.Second
	// MaxReverseBackoff bounds reverse.reconnect.max_backoff.
	MaxReverseBackoff = 10 * time.Minute
)

// ReverseReconnectConfig is the tunnel reconnect backoff (exponential with
// jitter between min_backoff and max_backoff; the first reconnect after a
// tunnel loss does not wait).
type ReverseReconnectConfig struct {
	MinBackoffRaw interface{}   `yaml:"min_backoff"`
	MaxBackoffRaw interface{}   `yaml:"max_backoff"`
	MinBackoff    time.Duration `yaml:"-"`
	MaxBackoff    time.Duration `yaml:"-"`
}

// ReverseConfig is the database-side half of reverse mode. When enabled the
// node dials the public-side node at Endpoints, authenticates with the
// token, checks that the far side is PeerNodeID and serves its egress role
// (targets, inbound identities) over the tunnel.
type ReverseConfig struct {
	Enabled    bool   `yaml:"enabled"`
	PeerNodeID string `yaml:"peer_node_id"`
	TokenFile  string `yaml:"token_file"`
	TokenEnv   string `yaml:"token_env"`
	Token      string `yaml:"-"` // Resolved token
	// Transport is the tunnel carrier: auto (WebSocket, then SSE, then
	// long polling), ws, http-sse or long-poll.
	Transport          string                 `yaml:"transport"`
	TunnelsPerEndpoint int                    `yaml:"tunnels_per_endpoint"`
	Endpoints          []EndpointConfig       `yaml:"endpoints"`
	Reconnect          ReverseReconnectConfig `yaml:"reconnect"`
	TLS                TLSClientConfig        `yaml:"tls"`
	// AllowInsecureHTTP permits plaintext http:// tunnel endpoints that are
	// not loopback addresses.
	AllowInsecureHTTP bool `yaml:"allow_insecure_http"`
}

// ReverseEndpointURL is the origin of the one virtual endpoint a reverse
// peer gets after validation. Its requests run as plaintext HTTP inside the
// tunnel (the tunnel carries TLS) and the host is never resolved.
const ReverseEndpointURL = "http://reverse.tunnel.invalid"

// ReverseEndpointID is the ID of that virtual endpoint.
const ReverseEndpointID = "tunnel"

// MaxTunnelsPerPeer bounds the live tunnels the public side registers for
// one reverse peer.
const MaxTunnelsPerPeer = 64

// MaxStreamsPerTunnel bounds the streams open at once on one reverse tunnel.
const MaxStreamsPerTunnel = 1024

// TunnelStreamWindow is the receive window of one stream of a reverse tunnel:
// the bytes a peer may send on the stream before the reader consumes them.
// Each open stream charges it to the codec budget of the side that reads it,
// so the memory a tunnel can hold follows the streams in use rather than
// being reserved for an idle tunnel. It is codec_buffer_limit/128, at least
// 256 KiB (the multiplexer's initial window) and at most
// min(buffer_per_direction, 1 MiB): 256 KiB with the defaults.
func (t *TransportConfig) TunnelStreamWindow() int64 {
	hi := max(min(t.BufferPerDirection, 1<<20), 256<<10)
	return min(max(t.CodecBufferLimit/128, 256<<10), hi)
}

// HasReversePeers reports whether any peer uses reverse mode.
func (c *Config) HasReversePeers() bool {
	for i := range c.Peers {
		if c.Peers[i].IsReverse() {
			return true
		}
	}
	return false
}

// EgressRole reports whether this node serves inbound sessions: on
// http.listen, over reverse tunnels it dials out (reverse.enabled), or both.
func (c *Config) EgressRole() bool { return c.HTTP.Enabled || c.Reverse.Enabled }

type ListenerConfig struct {
	ID     string `yaml:"id"`
	Listen string `yaml:"listen"`
	Peer   string `yaml:"peer"`
	Target string `yaml:"target"`
}

type TargetConfig struct {
	Network      string   `yaml:"network"`
	Address      string   `yaml:"address"`
	AllowedCIDRs []string `yaml:"allowed_cidrs"`
}

type LimitsConfig struct {
	MaxSessions         int `yaml:"max_sessions"`
	MaxEndpointsPerPeer int `yaml:"max_endpoints_per_peer"`
}

type InboundPeerConfig struct {
	ID             string   `yaml:"id"`
	Enabled        *bool    `yaml:"enabled"`
	TokenFiles     []string `yaml:"token_files"`
	TokenEnvs      []string `yaml:"token_envs"`
	Tokens         []string `yaml:"-"` // Resolved credentials, never parsed from YAML
	AllowedTargets []string `yaml:"allowed_targets"`
	MaxSessions    int      `yaml:"max_sessions"`
}

func BoolPtr(b bool) *bool {
	return &b
}

func (ip *InboundPeerConfig) IsEnabled() bool {
	if ip.Enabled == nil {
		return true
	}
	return *ip.Enabled
}

type OutboundPeerCredential struct {
	ID        string `yaml:"id"`
	TokenFile string `yaml:"token_file"`
	TokenEnv  string `yaml:"token_env"`
	Token     string `yaml:"-"`
}

type AuthConfigFile struct {
	Version       int                      `yaml:"version"`
	InboundPeers  []InboundPeerConfig      `yaml:"inbound_peers"`
	OutboundPeers []OutboundPeerCredential `yaml:"outbound_peers"`
}

type AuthConfig struct {
	CredentialsFile string `yaml:"credentials_file"`
	// ReloadInterval is the parsed reload_interval. Like the transport
	// durations it accepts a duration string or numeric seconds.
	ReloadInterval    time.Duration       `yaml:"-"`
	ReloadIntervalRaw interface{}         `yaml:"reload_interval"`
	InboundPeers      []InboundPeerConfig `yaml:"inbound_peers"` // Direct config fallback
}

type RecoveryConfig struct {
	EnabledRaw        interface{} `yaml:"enabled"`
	GraceRaw          interface{} `yaml:"grace"`
	BarrierTimeoutRaw interface{} `yaml:"barrier_timeout"`

	Enabled        bool          `yaml:"-"`
	Grace          time.Duration `yaml:"-"`
	BarrierTimeout time.Duration `yaml:"-"`
}

type WSConfig struct {
	HandshakeTimeoutRaw interface{}   `yaml:"handshake_timeout"`
	WriteIdleTimeoutRaw interface{}   `yaml:"write_idle_timeout"`
	HandshakeTimeout    time.Duration `yaml:"-"`
	WriteIdleTimeout    time.Duration `yaml:"-"`
}

type TransportConfig struct {
	Protocol                   string         `yaml:"protocol"`
	ProtocolVersion            string         `yaml:"protocol_version"`
	WS                         WSConfig       `yaml:"ws"`
	FrameSizeRaw               interface{}    `yaml:"frame_size"`
	MaxFrameSizeRaw            interface{}    `yaml:"max_frame_size"`
	UpMaxInflightFrames        int            `yaml:"up_max_inflight_frames"`
	UpWindowBytesRaw           interface{}    `yaml:"up_window_bytes"`
	ReceiptLimitPerSession     int            `yaml:"receipt_limit_per_session"`
	H2PoolSize                 int            `yaml:"h2_pool_size"`
	HeartbeatRaw               interface{}    `yaml:"heartbeat"`
	FailureDetectTimeoutRaw    interface{}    `yaml:"failure_detect_timeout"`
	Recovery                   RecoveryConfig `yaml:"recovery"`
	UnattachedTimeoutRaw       interface{}    `yaml:"unattached_timeout"`
	CreateRetryTimeoutRaw      interface{}    `yaml:"create_retry_timeout"`
	AckBatchBytesRaw           interface{}    `yaml:"ack_batch_bytes"`
	AckMaxDelayRaw             interface{}    `yaml:"ack_max_delay"`
	UpAckMaxDelayRaw           interface{}    `yaml:"up_ack_max_delay"`
	ChunkSizeRaw               interface{}    `yaml:"chunk_size"`
	BufferPerDirectionRaw      interface{}    `yaml:"buffer_per_direction"`
	GlobalBufferLimitRaw       interface{}    `yaml:"global_buffer_limit"`
	SuspendedBufferLimitRaw    interface{}    `yaml:"suspended_buffer_limit"`
	CodecBufferLimitRaw        interface{}    `yaml:"codec_buffer_limit"`
	MetadataLimitRaw           interface{}    `yaml:"metadata_limit"`
	IdempotencyTombstoneTTLRaw interface{}    `yaml:"idempotency_tombstone_ttl"`
	DialTimeoutRaw             interface{}    `yaml:"dial_timeout"`
	PeerDialTimeoutRaw         interface{}    `yaml:"peer_dial_timeout"`
	ControlRequestTimeoutRaw   interface{}    `yaml:"control_request_timeout"`
	UpRequestTimeoutRaw        interface{}    `yaml:"up_request_timeout"`
	SocketWriteTimeoutRaw      interface{}    `yaml:"socket_write_timeout"`
	SSEWriteIdleTimeoutRaw     interface{}    `yaml:"sse_write_idle_timeout"`
	BackpressureTimeoutRaw     interface{}    `yaml:"backpressure_timeout"`
	ShutdownTimeoutRaw         interface{}    `yaml:"shutdown_timeout"`
	UseEnvironmentProxy        bool           `yaml:"use_environment_proxy"`
	// UserAgent is the User-Agent header of every request this node sends
	// to a peer (HTTP requests and WebSocket handshakes). Empty means
	// DefaultUserAgent ("DBBridge/<protocol version>").
	UserAgent string `yaml:"user_agent"`
	SQLReplay bool   `yaml:"sql_replay"`

	// Parsed typed values
	FrameSize               int           `yaml:"-"`
	MaxFrameSize            int           `yaml:"-"`
	UpWindowBytes           int64         `yaml:"-"`
	Heartbeat               time.Duration `yaml:"-"`
	FailureDetectTimeout    time.Duration `yaml:"-"`
	UnattachedTimeout       time.Duration `yaml:"-"`
	CreateRetryTimeout      time.Duration `yaml:"-"`
	AckBatchBytes           int64         `yaml:"-"`
	AckMaxDelay             time.Duration `yaml:"-"`
	UpAckMaxDelay           time.Duration `yaml:"-"`
	ChunkSize               int           `yaml:"-"`
	BufferPerDirection      int64         `yaml:"-"`
	GlobalBufferLimit       int64         `yaml:"-"`
	SuspendedBufferLimit    int64         `yaml:"-"`
	CodecBufferLimit        int64         `yaml:"-"`
	MetadataLimit           int64         `yaml:"-"`
	IdempotencyTombstoneTTL time.Duration `yaml:"-"`
	DialTimeout             time.Duration `yaml:"-"`
	// PeerDialTimeout bounds the TCP connect to a peer endpoint; 0 falls back
	// to DialTimeout. Waiting for a connection slot is bounded by the request's
	// own deadline instead.
	PeerDialTimeout       time.Duration `yaml:"-"`
	ControlRequestTimeout time.Duration `yaml:"-"`
	UpRequestTimeout      time.Duration `yaml:"-"`
	SocketWriteTimeout    time.Duration `yaml:"-"`
	SSEWriteIdleTimeout   time.Duration `yaml:"-"`
	BackpressureTimeout   time.Duration `yaml:"-"`
	ShutdownTimeout       time.Duration `yaml:"-"`
}

func (p *PeerConfig) FailbackOrDefault() {
	if p.Failover.Failback == "" {
		p.Failover.Failback = "new_sessions"
	}
}
