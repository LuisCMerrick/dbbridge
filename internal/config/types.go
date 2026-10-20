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

type PeerConfig struct {
	ID             string               `yaml:"id"`
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

func (p *PeerConfig) EffectiveRecoveryEnabled(globalRecovery bool) bool {
	if !globalRecovery {
		return false
	}
	if p.Recovery != nil && p.Recovery.Enabled != nil {
		return *p.Recovery.Enabled
	}
	return globalRecovery
}

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
	SQLReplay                  bool           `yaml:"sql_replay"`

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
