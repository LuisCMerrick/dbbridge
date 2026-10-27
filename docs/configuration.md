**English** | [简体中文](configuration.zh-CN.md)

# Configuration reference

[README](../README.md) · [Installation](installation.md) · **Configuration** · [Transport](transport.md) · [Security](security.md) · [Operations](operations.md) · [Examples](examples.md)

This page lists every configuration key with its type, default and constraints. Tuning advice for the `transport` section, the reverse-proxy requirements and TLS are in [Transport](transport.md); token rotation and revocation are in [Security](security.md).

- [1. File format and value syntax](#1-file-format-and-value-syntax)
- [2. Top-level layout](#2-top-level-layout)
- [3. `node`](#3-node)
- [4. `http`](#4-http)
- [5. `admin`](#5-admin)
- [6. `auth` and credentials](#6-auth-and-credentials)
- [7. `peers`](#7-peers)
- [8. `listeners`](#8-listeners)
- [9. `targets`](#9-targets)
- [10. `limits`](#10-limits)
- [11. `transport`](#11-transport)
- [12. Derived defaults](#12-derived-defaults)
- [13. Cross-field constraints](#13-cross-field-constraints)
- [14. Session capacity and the metadata budget](#14-session-capacity-and-the-metadata-budget)
- [15. Warnings](#15-warnings)
- [16. What is re-read while running](#16-what-is-re-read-while-running)

## 1. File format and value syntax

- The configuration is one YAML document of at most 4 MiB; a credentials file is at most 1 MiB, a CA bundle at most 1 MiB. YAML nesting is limited to 32 levels.
- Unknown keys are rejected (`field "x" not found in config.http`).
- Types are strict: strings must be YAML strings (`protocol_version: 1.4` without quotes is rejected; write `"1.4"`), booleans must be `true`/`false`, integers must be integers.
- An explicit `null` (`key:` with no value, or `~`) is rejected. Omit an optional key to get its default.
- **Sizes** are an integer number of bytes or a string `<digits>[ ]<unit>` with the unit `B`, `K`/`KB`/`KiB`, `M`/`MB`/`MiB` or `G`/`GB`/`GiB` (case-insensitive). All units are powers of 1024 (`1MB` = `1MiB` = 1,048,576 bytes). Decimals (`1.5MiB`) and `T` are not accepted.
- **Durations** are a Go duration string (`200ms`, `1m30s`; `h`, `m`, `s`, `ms`, `us`, `ns`; no `d`) or a YAML number meaning seconds (`5`, `0.5` = 500 ms). Negative values, NaN, infinity and overflowing values are rejected.
- An empty string for a size or duration means "use the default".
- These integer keys must be positive when present (0 is rejected, not treated as "default"): `limits.max_sessions`, `limits.max_endpoints_per_peer`, `transport.up_max_inflight_frames`, `transport.h2_pool_size`, `transport.receipt_limit_per_session`, `peers[].failover.healthy_threshold`, `peers[].failover.unhealthy_threshold`, `peers[].failover.max_concurrent_probes`, `inbound_peers[].max_sessions`.
- **Relative paths** in the main configuration (`auth.credentials_file`, `peers[].token_file`, `auth.inbound_peers[].token_files`, `http.tls.cert_file`, `http.tls.key_file`, `peers[].tls.ca_file`) are resolved against the directory of the main configuration file. Relative token paths inside a credentials file are resolved against the directory of the credentials file.
- IDs (`node.id`, peer, endpoint, listener, target and inbound identity IDs) should match `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`. Other non-empty IDs load but produce a warning, because IDs appear in logs, metric labels and the other node's configuration.
- Validation stops at the first error. Every error carries a stable code (`Error [DBB-CFG-002]: ...`); see [Operations → Configuration error codes](operations.md#9-configuration-error-codes).

## 2. Top-level layout

```yaml
node:        # identity of this node
http:        # inbound peer server (egress role): HTTP, WebSocket, SSE
admin:       # loopback /healthz, /readyz, /metrics
auth:        # inbound identities, target ACLs, credentials file
peers:       # outbound peers (ingress role): endpoints, failover, TLS, token
listeners:   # local TCP entry points for database clients
targets:     # databases this node may dial (egress role)
limits:      # session and endpoint limits
transport:   # carriers, flow control, recovery, budgets, timeouts
```

A node is an **ingress** when it has `listeners` and `peers`, an **egress** when `http.enabled: true` and it has `targets`, and **bidirectional** when it is both. The same binary serves every role.

## 3. `node`

| Key | Type | Default | Constraints |
|---|---|---|---|
| `node.id` | string | — (required) | Non-empty. The other node checks it against `peers[].expected_node_id`. |

Every start also generates a random instance ID (`pi_<32 hex digits>`). It is not configurable. Sessions are bound to the instance, so they do not survive a restart.

## 4. `http`

The inbound server that other nodes connect to (egress role).

| Key | Type | Default | Constraints / behaviour |
|---|---|---|---|
| `http.enabled` | bool | `false` | When `false`, `targets`, inbound credentials and `http.tls` have no effect (a warning says so). |
| `http.listen` | `host:port` | `127.0.0.1:18080` | Validated only when enabled. Empty host, `0.0.0.0` and `::` are allowed. A non-loopback address (loopback = `localhost`, `127.0.0.0/8`, `::1`) requires `http.tls.enabled: true` or `http.allow_insecure_listen: true` (the opt-in produces a warning). |
| `http.allow_insecure_listen` | bool | `false` | Allows plaintext HTTP on a non-loopback `http.listen`. Affects only this server. |
| `http.allowed_transports` | list of strings | `["ws", "http-sse"]` | Each `ws` or `http-sse`, no duplicates; an explicit `[]` is rejected. Advertised to probing peers. |
| `http.tls.enabled` | bool | `false` | Honoured only when `http.enabled`. TLS 1.2 or later. |
| `http.tls.cert_file` | path | — | Required with TLS. Loaded together with the key by `check-config` and at start. The certificate is read once at start; a renewed certificate takes effect after a restart. |
| `http.tls.key_file` | path | — | Required with TLS. Warning (Unix) if the file is group-writable or accessible by others. |

Fixed limits of this server: request headers ≤ 16 KiB in total, header read timeout 10 s, idle keep-alive 90 s. A connection that has not completed an authenticated request is closed after 5 s (TLS handshake, headers and idle time included); at most half of the inbound connection limit may be unauthenticated at a time, and one source IP may hold at most `max(8, that half / 4)` of them. Requests rejected before authentication (400, 401, 404, 405) close the connection.

## 5. `admin`

| Key | Type | Default | Constraints |
|---|---|---|---|
| `admin.enabled` | bool | `false` | `gen-config` and all shipped samples set `true` on a loopback address. |
| `admin.listen` | `host:port` | `127.0.0.1:18090` | Validated only when enabled. Must be loopback unless `admin.allow_remote: true`. Takes part in listen-conflict detection. |
| `admin.allow_remote` | bool | `false` | Allows a non-loopback `admin.listen`; produces a warning. The endpoint has no TLS and no authentication. |

Endpoints and limits are described in [Operations → Admin endpoints](operations.md#4-admin-endpoints).

## 6. `auth` and credentials

### 6.1 `auth`

| Key | Type | Default | Constraints |
|---|---|---|---|
| `auth.credentials_file` | path | — | Separate credentials file (§6.3). Cannot be combined with `auth.inbound_peers` or with `peers[].token_env`/`token_file`. |
| `auth.reload_interval` | duration | `5s` | How often credentials are re-read (§16). Omitted or `0` means 5 s; negative values and values between 0 and 1 s are rejected. |
| `auth.inbound_peers` | list | `[]` | Inline inbound identities (§6.2). |

Credentials come either from the main configuration (inline `auth.inbound_peers` plus `peers[].token_file`/`token_env`) or from one credentials file (inbound and outbound). Both forms are re-read every `reload_interval`, on `SIGHUP` (Linux) and on `sc.exe control <name> paramchange` (Windows service).

### 6.2 Inbound identity (`auth.inbound_peers[]` or `inbound_peers[]` in a credentials file)

| Key | Type | Default | Constraints |
|---|---|---|---|
| `id` | string | — | Required, unique. The name of a connecting node on this node. A request is attributed to an identity by its bearer token alone; by convention the ID is the connecting node's `node.id`. It appears in logs and metrics. |
| `enabled` | bool | `true` | A disabled identity needs no tokens; its `token_files`/`token_envs` are not read, so they may point to files that have already been deleted. Its `allowed_targets` are still syntax-checked. Disabling an identity by reload closes its live sessions. |
| `token_files` | list of paths | — | Token files. Cannot be combined with `token_envs` in the same identity. |
| `token_envs` | list of names | — | Environment variables holding tokens. |
| `allowed_targets` | list of target IDs | — | Required for an enabled identity. No empty or duplicate entries; each must exist in `targets` of the main configuration. A create for any other target is refused with `403 DBB_TARGET_FORBIDDEN`. |
| `max_sessions` | int | `200` | Positive. Concurrent sessions of this identity; above it `429 DBB_QUOTA_EXCEEDED`. |

An enabled identity needs at least one token and at least one allowed target. Every listed source of an enabled identity must resolve: a listed file must exist and a listed variable must be set and non-empty; there is no optional slot.

Quota rules:

- With more than one enabled identity, the sum of their `max_sessions` (an omitted quota counts as 200) must not exceed `limits.max_sessions`. This is an error.
- A single identity whose quota exceeds `limits.max_sessions` produces a warning; the global limit applies.
- On a node that also has `listeners`, an inbound quota sum ≥ `limits.max_sessions` produces a warning, because local clients share the same session budget. `gen-config` therefore gives the inbound identity half of `--max-sessions` in bidirectional mode.

### 6.3 Credentials file

```yaml
version: 1                       # required, must be 1
inbound_peers:                   # same item schema as §6.2
  - id: B
    token_files: [tokens/b-to-a.token]
    allowed_targets: [mssql]
    max_sessions: 100
outbound_peers:
  - id: B                        # matched by peers[].credential_ref
    token_file: tokens/a-to-b.token   # exactly one of token_file / token_env
```

| Key | Type | Constraints |
|---|---|---|
| `version` | int | Required, `1`. |
| `inbound_peers` | list | §6.2. |
| `outbound_peers[].id` | string | Required, unique. Every `peers[].credential_ref` must have an entry, at start and on every reload. |
| `outbound_peers[].token_file` | path | Relative to the credentials file. |
| `outbound_peers[].token_env` | name | Environment variable. |

Every `outbound_peers` entry is resolved, including entries no peer references yet.

### 6.4 Tokens

- A token is 1–8192 bytes without whitespace or control characters. A token file may end with a single `\n` or `\r\n`; any other content (an extra line) is rejected.
- A token shorter than 16 bytes loads but produces a warning. `gen-config` writes 32 random bytes, hex-encoded (`openssl rand -hex 32` gives the same format).
- The same token listed twice for one identity (for example while a rotation is prepared) is accepted once. A token used by two different identities is rejected: `inbound peers A and C use the same token; every inbound identity needs its own token`.
- Error messages name the source (`inbound_peers[plant-a]: token_env DBB_TOKEN_X is unset or empty`, `token_file /path: ...`) but never print a token.
- On Unix, a token file or TLS key that is group-writable or accessible by others produces a warning (the credentials file itself holds no secret and is not checked). Group read for a service group (0640 `root:dbbridge`) is the recommended way to share a file with the service account.
- **Token files are re-read on every reload; environment variables are read from the process environment, which does not change while the process runs.** A token taken from an environment variable therefore changes only with a restart; at the first load the node logs which variables it took tokens from.

## 7. `peers`

Outbound peers (ingress role). Each peer is the other node, reached through one or more endpoints.

| Key | Type | Default | Constraints |
|---|---|---|---|
| `id` | string | — | Required, unique. Local name of the other node, used by `listeners[].peer`, logs and metric labels. By convention it is the other node's `node.id`. |
| `expected_node_id` | string | — | Required; must differ from this node's `node.id`. The other node's `node.id` is verified on every probe. |
| `credential_ref` | string | — | Required, also with a direct token. With `auth.credentials_file` it selects the `outbound_peers[]` entry. |
| `token_file` | path | — | Bearer token sent to this peer. Exactly one of `token_file`/`token_env` unless `auth.credentials_file` is used (then neither is allowed). |
| `token_env` | name | — | As above, from an environment variable. |
| `endpoints` | list | — | 1 to `limits.max_endpoints_per_peer` entries. |
| `endpoints[].id` | string | — | Required, unique within the peer. |
| `endpoints[].url` | string | — | An `http://` or `https://` origin only: no user info, path (not even `/`), query or fragment; port 1–65535 when given. Host names are lower-cased and default ports dropped; duplicates after this normalisation are rejected. Plain `http://` to a non-loopback host requires `allow_insecure_http: true`. |
| `endpoints[].priority` | int | `0` | ≥ 0; lower is preferred. Set it explicitly on every endpoint: an omitted priority (0) beats an explicit 1. |
| `failover.enabled` | bool | `true` with more than one endpoint, else `false` | `false` uses only the best-priority endpoint. |
| `failover.strategy` | string | `priority` | Only `priority`. |
| `failover.failback` | string | `new_sessions` | Only `new_sessions`: when a preferred endpoint recovers, new sessions use it; established sessions stay where they are. |
| `failover.healthcheck_interval` | duration | `10s` | > 0. Interval of background probes. |
| `failover.healthcheck_timeout` | duration | `min(3s, healthcheck_interval/2)` | > 0 and < `healthcheck_interval`. Also the timeout of on-demand probes. |
| `failover.unhealthy_threshold` | int | `2` | Positive. Consecutive failed probes before an endpoint is `UNHEALTHY`. |
| `failover.healthy_threshold` | int | `2` | Positive. Consecutive successful probes that end `UNHEALTHY`. |
| `failover.cooldown` | duration | `5s` | ≥ 0. How long a failed endpoint (or a failed endpoint/carrier pair) is avoided; `0` disables the cooldown. A successful probe does not end a running cooldown. |
| `failover.max_concurrent_probes` | int | `2` | Positive. Concurrent probes for this peer; the process-wide probe budget is 16. |
| `recovery.enabled` | bool | inherits `transport.recovery.enabled` | `false` disables recovery towards this peer only. `true` while `transport.recovery.enabled: false` is an error. |
| `transport.protocol` | string | inherits `transport.protocol` | `auto`, `ws` or `http-sse` for this peer. |
| `tls.verify` | bool | `true` | Verify the peer certificate and host name. `false` towards a non-loopback `https` endpoint also requires `tls.allow_insecure_skip_verify: true`. Cannot be combined with `tls.ca_file`. |
| `tls.allow_insecure_skip_verify` | bool | `false` | Explicit opt-in for `verify: false` towards non-loopback hosts. |
| `tls.ca_file` | path | system trust store | PEM bundle with at least one certificate (≤ 1 MiB). Not read by `check-config --offline`. |
| `allow_insecure_http` | bool | `false` | Allows plaintext `http://` endpoints of this peer on non-loopback hosts. Affects only outbound connections. |

How the two nodes' settings meet is described in [Transport → Negotiated values](transport.md#2-negotiated-values).

## 8. `listeners`

Local TCP entry points. A database client connects here; the node forwards the connection to `target` on `peer`.

| Key | Type | Default | Constraints |
|---|---|---|---|
| `id` | string | — | Required, unique. |
| `listen` | `host:port` | — | Required. Port 0–65535 (0 = any free port). **A listener has no authentication**: every host that can reach it uses this node's credentials. A non-loopback address produces a warning. |
| `peer` | string | — | Required; a `peers[].id`. |
| `target` | string | — | Required. Not checked locally: it must exist in the other node's `targets` and be in this node's `allowed_targets` there. |

**Listen conflicts** (between `http.listen`, `admin.listen` and every listener; port 0 is exempt): two addresses conflict when the port is equal and the hosts overlap. `localhost` means `127.0.0.1` and `::1`; `0.0.0.0` overlaps every IPv4 address and every host name; `::` or an empty host overlaps everything; other host names overlap only the same name and wildcards.

## 9. `targets`

A map from target ID to the database this node may dial (egress role). Callers can only name a target ID; they can never supply an address.

| Key | Type | Default | Constraints |
|---|---|---|---|
| `targets.<id>` | object | — | ID non-empty. |
| `.network` | string | `tcp` | `tcp`, `tcp4` or `tcp6`. |
| `.address` | `host:port` | — | Required; host required; port 1–65535. |
| `.allowed_cidrs` | list of CIDRs | none (no restriction) | Addresses the target may resolve to. A host name that resolves only to addresses outside the list is refused with `403 DBB_TARGET_FORBIDDEN`. A host-name target without `allowed_cidrs` produces a warning (DNS rebinding risk). |

A resolver error or timeout is reported to the peer as `502 DBB_DIAL_FAILED`, like a refused TCP connection.

## 10. `limits`

| Key | Type | Default | Notes |
|---|---|---|---|
| `limits.max_sessions` | int | `200` | Positive. Concurrent sessions of the node (egress sessions plus local listener sessions). Must fit the metadata budget (§14). On a bidirectional node `max(1, max_sessions/4)` sessions (none when the limit is 1) and 1/8 of the metadata budget are reserved for local listeners. `dbbridge install` sizes `LimitNOFILE` from it. |
| `limits.max_endpoints_per_peer` | int | `16` | Positive. |

## 11. `transport`

How each value is used and when to change it is in [Transport](transport.md).

| Key | Type | Default | Constraints |
|---|---|---|---|
| `protocol` | string | `auto` | `auto`, `ws`, `http-sse`. |
| `protocol_version` | string | `"1.4"` | Must be the string `"1.4"`. |
| `ws.handshake_timeout` | duration | `5s` | > 0. Bounds the WebSocket upgrade (TCP, TLS, `101`). |
| `ws.write_idle_timeout` | duration | `60s` | > 0. |
| `frame_size` | size | `min(32KiB, max_frame_size)` | > 0, ≤ `max_frame_size`. |
| `max_frame_size` | size | `min(256KiB, up_window_bytes)` | > 0, ≤ 1 MiB, ≤ `up_window_bytes`. |
| `up_max_inflight_frames` | int | `8` | Positive; ≤ `metadata_limit / 192`. |
| `up_window_bytes` | size | `min(2MiB, buffer_per_direction)` | ≥ `max_frame_size`, ≤ `buffer_per_direction`. |
| `receipt_limit_per_session` | int | `max(64, 2 × up_max_inflight_frames + 8)` | ≥ `2 × up_max_inflight_frames + 8`; ≤ `metadata_limit / 160`. |
| `h2_pool_size` | int | `4` | 1–64. Maximum number of HTTP client shards (connections) per endpoint for SSE downstream subscriptions. Subscriptions share one shard; another is opened only when every shard already carries 8 concurrent subscriptions. |
| `heartbeat` | duration | `15s` | > 0. Carrier keep-alive; also the HTTP/2 PING interval (PING timeout 5 s). |
| `failure_detect_timeout` | duration | `max(45s, 3 × heartbeat)` | ≥ `3 × heartbeat`. |
| `recovery.enabled` | bool | `true` | |
| `recovery.grace` | duration | `30s` | > 0 when recovery is enabled. |
| `recovery.barrier_timeout` | duration | `min(5s, grace/2)` | > 0 and < `grace` when recovery is enabled. |
| `unattached_timeout` | duration | `30s` | > 0. |
| `create_retry_timeout` | duration | `30s` | > 0. |
| `ack_batch_bytes` | size | `max(1, min(1MiB, buffer_per_direction/4))` | > 0, < `buffer_per_direction`. |
| `ack_max_delay` | duration | `200ms` | > 0. |
| `up_ack_max_delay` | duration | `10ms` | > 0. |
| `chunk_size` | size | `32KiB` | > 0. |
| `buffer_per_direction` | size | `4MiB` | > 0. |
| `global_buffer_limit` | size | `256MiB` | > 0, > `suspended_buffer_limit`. |
| `suspended_buffer_limit` | size | `max(1, min(64MiB, global_buffer_limit/4))` | > 0, < `global_buffer_limit`. |
| `codec_buffer_limit` | size | `32MiB` | ≥ `6 × max_frame_size + 16KiB` when SSE can be used (see §13); ≥ `max(2 × max_frame_size + 56, 16KiB)` on an egress-only node that allows only `ws`. |
| `metadata_limit` | size | `32MiB` | ≥ 512 KiB; must hold `limits.max_sessions` sessions (§14). |
| `idempotency_tombstone_ttl` | duration | `10m` | ≥ `10m`. |
| `dial_timeout` | duration | `5s` | > 0. Egress → database (DNS and TCP). |
| `peer_dial_timeout` | duration | `dial_timeout` | > 0. Ingress → peer endpoint, TCP connect only. |
| `control_request_timeout` | duration | `10s` | > 0. |
| `up_request_timeout` | duration | `30s` | > 0. |
| `socket_write_timeout` | duration | `60s` | > 0. |
| `sse_write_idle_timeout` | duration | `60s` | > 0. |
| `backpressure_timeout` | duration | `120s` | > 0. |
| `shutdown_timeout` | duration | `30s` | > 0. How long a stop (first Ctrl-C, SIGTERM, service stop) waits for open sessions to finish before closing them; database clients usually keep pooled connections open, so a stop often waits the full time. Progress is logged every 5 s; a second Ctrl-C (or SIGTERM outside Windows) forces an immediate exit with status 130. |
| `use_environment_proxy` | bool | `false` | Use `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` (and lower-case forms) for peer connections. Read once per process. |
| `user_agent` | string | `DBBridge/<protocol version>` | `User-Agent` of every request and WebSocket handshake this node sends to a peer, instead of Go's `Go-http-client/1.1`/`2.0`. Printable ASCII, at most 256 bytes, no leading or trailing space. |
| `sql_replay` | bool | `false` | Must be `false`. |

## 12. Derived defaults

Some defaults follow a neighbouring value so that changing one key does not violate a constraint against a key you did not set. A value you set explicitly is always kept.

| Key | Default when unset | With all other defaults |
|---|---|---|
| `transport.failure_detect_timeout` | `max(45s, 3 × heartbeat)` | 45 s |
| `transport.up_window_bytes` | `min(2MiB, buffer_per_direction)` | 2 MiB |
| `transport.ack_batch_bytes` | `max(1, min(1MiB, buffer_per_direction/4))` | 1 MiB |
| `transport.max_frame_size` | `min(256KiB, up_window_bytes)` | 256 KiB |
| `transport.frame_size` | `min(32KiB, max_frame_size)` | 32 KiB |
| `transport.suspended_buffer_limit` | `max(1, min(64MiB, global_buffer_limit/4))` | 64 MiB |
| `transport.recovery.barrier_timeout` | `min(5s, grace/2)` (at least 1 ms) | 5 s |
| `transport.receipt_limit_per_session` | `max(64, 2 × up_max_inflight_frames + 8)` | 64 |
| `transport.peer_dial_timeout` | `dial_timeout` | 5 s |
| `peers[].failover.healthcheck_timeout` | `min(3s, healthcheck_interval/2)` (at least 1 ms) | 3 s |
| `peers[].failover.enabled` | `true` with more than one endpoint | — |

When a constraint fails against a default, the message says so, for example `max_frame_size (524288) must be <= up_window_bytes (262144, default)`.

## 13. Cross-field constraints

| Constraint | Applies |
|---|---|
| `frame_size ≤ max_frame_size ≤ 1 MiB` | always |
| `max_frame_size ≤ up_window_bytes ≤ buffer_per_direction` | always |
| `ack_batch_bytes < buffer_per_direction` | always |
| `suspended_buffer_limit < global_buffer_limit` | always |
| `failure_detect_timeout ≥ 3 × heartbeat` | always |
| `receipt_limit_per_session ≥ 2 × up_max_inflight_frames + 8` | always |
| `receipt_limit_per_session ≤ metadata_limit / 160`, `up_max_inflight_frames ≤ metadata_limit / 192` | always |
| `metadata_limit ≥ 512 KiB` and room for `limits.max_sessions` sessions | always (§14) |
| `codec_buffer_limit ≥ 6 × max_frame_size + 16 KiB` | when SSE can be used: `http-sse` in `http.allowed_transports`, any listener or peer, or `http.enabled: false` |
| `codec_buffer_limit ≥ max(2 × max_frame_size + 56, 16 KiB)` | egress-only node with `http.allowed_transports: [ws]` |
| `0 < barrier_timeout < grace` | when recovery is enabled |
| `peers[].recovery.enabled: true` only with `transport.recovery.enabled: true` | always |
| `0 < healthcheck_timeout < healthcheck_interval` | per peer |
| `idempotency_tombstone_ttl ≥ 10m` | always |
| `credentials_file` excludes inline `inbound_peers` and direct peer tokens | always |
| sum of enabled inbound `max_sessions` ≤ `limits.max_sessions` | more than one enabled identity |
| every `allowed_targets` entry exists in `targets` | always |
| every `peers[].credential_ref` exists in `outbound_peers` | with `credentials_file` |

## 14. Session capacity and the metadata budget

Each session reserves session metadata up front. The check is:

```
usable      = metadata_limit − 2 × min(metadata_limit / 4, 1 MiB)
per session ≈ 64 KiB + 160 × receipt_limit_per_session + 192 × up_max_inflight_frames
              + 32 × (buffer_per_direction / chunk_size + 513) + 8 KiB
max_sessions × per session ≤ usable
```

With all defaults a session needs about 103 KiB and the usable budget is 30 MiB, so `metadata_limit: 32MiB` holds at most **296** sessions. A larger `limits.max_sessions` is rejected with suggested values, for example `raise transport.metadata_limit to at least 33 MiB or lower limits.max_sessions to 296`. `gen-config --max-sessions N` writes a sufficient `metadata_limit` (`ceil((N × 112 KiB + 4 MiB) / 1 MiB)` MiB when that exceeds 32 MiB).

The same budget also holds the create records and tombstones that make session creation idempotent; it therefore also bounds the sustained rate of new sessions. See [Operations → Capacity planning](operations.md#10-capacity-planning).

## 15. Warnings

`check-config` prints non-fatal findings as `Warning: ...` on stderr; `run` logs them at start (`configuration warning`), and `install` prints them with the plan.

- token shorter than 16 bytes;
- token file or TLS key group-writable or accessible by others (Unix);
- credential file under `/home`, `/root`, `/run/user`, `/tmp` or `/var/tmp` while the configuration is not (such files are hidden from an installed service; keep credential files next to the configuration);
- host-name target without `allowed_cidrs`;
- non-loopback listener; non-loopback admin with `allow_remote`; plaintext non-loopback `http.listen` with the opt-in;
- non-canonical IDs;
- inbound quota warnings (§6.2);
- `http.enabled: false` with targets, inbound credentials or `http.tls`; a node with nothing to do;
- recovery that cannot succeed: `transport.recovery.grace` does not exceed `failover.cooldown + recovery.barrier_timeout` for a peer with a forced protocol (`ws` or `http-sse`) and a single usable endpoint. Raise `grace`, lower `cooldown`, use `auto` or add an endpoint.

## 16. What is re-read while running

| Change | Takes effect |
|---|---|
| Content of any token file in use (inline or in a credentials file) | Next reload |
| `auth.inbound_peers` in the main configuration: identities added, removed, enabled/disabled, `token_files`, `allowed_targets`, `max_sessions` | Next reload |
| `peers[].token_file` / `token_env` of an existing peer | Next reload |
| Whole credentials file (inbound and outbound) | Next reload |
| Value of an environment variable holding a token | Restart only |
| A new environment variable name for a token, under an installed service | Reinstall (the service environment snapshot is fixed at install time); prefer a token file |
| Adding or removing a peer, adding `auth.credentials_file` | Restart (a reload attempt is rejected with "requires a restart") |
| Any other key (peers, endpoints, listeners, targets, http, admin, limits, transport, TLS certificate and CA files, `reload_interval`) | Restart |
| `transport.shutdown_timeout`, `limits.max_sessions`, a port below 1024, file locations — for an installed service | Reinstall (`uninstall` + `install`), because the service definition is computed from them |

A reload happens every `auth.reload_interval`, on `SIGHUP` / `systemctl reload <name>` (Linux) and on `sc.exe control <name> paramchange` (Windows service). A rejected candidate keeps the previous credentials in effect; see [Operations → Reload semantics](operations.md#3-reload-semantics).
