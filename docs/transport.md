**English** | [简体中文](transport.zh-CN.md)

# Transport, tuning, reverse proxies and TLS

[README](../README.md) · [Installation](installation.md) · [Configuration](configuration.md) · **Transport** · [Security](security.md) · [Operations](operations.md) · [Examples](examples.md)

Every key mentioned here is listed with its type, default and constraints in the [Configuration reference](configuration.md#11-transport). This page explains what the values do, when to change them, and what a network path between two nodes must provide.

- [1. Carriers: WebSocket and HTTP/SSE](#1-carriers-websocket-and-httpsse)
- [2. Negotiated values](#2-negotiated-values)
- [3. Frames, windows and acknowledgements](#3-frames-windows-and-acknowledgements)
- [4. Failure detection, failover and recovery](#4-failure-detection-failover-and-recovery)
- [5. Timeouts](#5-timeouts)
- [6. Memory and connection budgets](#6-memory-and-connection-budgets)
- [7. Reverse proxies and gateways](#7-reverse-proxies-and-gateways)
- [8. TLS](#8-tls)

## 1. Carriers: WebSocket and HTTP/SSE

A session (one database client connection) travels over a **carrier**:

- **`ws`** — one WebSocket per session, subprotocol `dbbridge.v1.4`, binary data frames in both directions plus text control messages.
- **`http-sse`** — upstream data as HTTP `POST /v1/sessions/{id}/up` requests (one frame each, with a SHA-256 digest header); downstream data and control events as a Server-Sent Events stream (`GET /v1/sessions/{id}/events`). Acknowledgements and FIN use separate control requests.

Control operations (probe, create, ack, fin, resume, delete) always use their own HTTP connection pool, so they keep working while the data path is saturated.

`transport.protocol` (or `peers[].transport.protocol` for one peer) selects the carrier:

| Value | Behaviour |
|---|---|
| `auto` (default) | Prefer WebSocket. Before the first WebSocket session on an endpoint, the ingress sends a WebSocket precheck. When the path cannot carry WebSocket, the session uses SSE on the same endpoint. |
| `ws` | WebSocket only. |
| `http-sse` | SSE only. |

The egress advertises the carriers it accepts (`http.allowed_transports`) in its probe response; the ingress never offers a carrier the egress does not accept.

**When `auto` falls back to SSE.** Any plain HTTP answer to the precheck that is not a verified DBBridge answer and not a `401` means "WebSocket is not available on this path": for example a redirect, `400`, `403` without a DBBridge body, `404`, `405`, `407`, `410`, `413`, `414`, `426`, `429`, `431`, `501`, a `200` page, a gateway `5xx`, or a reset or timeout of the upgrade. Only the WebSocket carrier of that endpoint is cooled down (`failover.cooldown`); the ingress probes the endpoint again and uses SSE there. A verified `DBB_TRANSPORT_UNSUPPORTED` answer has the same effect.

**When it does not.** A `401` (verified or not) or a verified `403` is an authentication or authorization verdict: the session fails, nothing falls back. A protocol violation after the upgrade (missing or wrong subprotocol, wrong protocol version in the hello, malformed control JSON, invalid WebSocket frames), a TLS failure, or a node/instance/protocol mismatch marks the endpoint `MISMATCHED` and the ingress does not use SSE on that endpoint; it moves to another endpoint if failover allows.

Precheck results are cached for 30 s per endpoint; temporary answers (`408`, `425`, `429`, `5xx`) are not cached. While WebSocket is in use on an endpoint (a session used it within the last 2 minutes), the health check renews a successful result before it expires, and a successful result up to 5 minutes past its 30 s is still used while a background check renews it, so a new session does not wait for a precheck. If the WebSocket path broke meanwhile, that session's upgrade fails and the carrier cools down and falls back as described above. A fallback is counted once per created session in `dbb_session_events_total{event="fallback",from="ws",to="http-sse"}`.

The egress limits WebSocket prechecks per authenticated identity: 2 concurrent, 4 per second, burst 4. Above that it answers `429 DBB_QUOTA_EXCEEDED` with `Retry-After`; no database session is used.

**Connection setup cost.** On a warm peer a new local connection costs three round trips to the egress before its first byte reaches the database: create, the carrier (WebSocket upgrade, or the SSE event stream) and the data itself.

- New sessions reuse the latest successful health-check probe (at most 1.5 × `healthcheck_interval`, at most 15 s old) instead of probing first. If the egress restarted since, the create is refused with `DBB_INSTANCE_MISMATCH` and the session probes again; any other create failure also drops the cached answer, and a create that does not answer in time is guarded by a probe (see Mute paths). The health check runs once at start.
- After each WebSocket session dial the ingress opens one spare connection (TCP and TLS) to the endpoint; the next session sends its upgrade on it within 4 s. An unused spare is closed after 4 s and not renewed, so an idle peer carries no extra connections. No spare is kept when a proxy from the environment is in use. A spare the server closed is detected before use, and a spare that fails before any answer is replaced by one fresh dial.
- The egress answers the WebSocket upgrade and the SSE event stream with `X-DBB-Implicit-Ready: 1`: it then activates a new session's data plane on the first upstream frame or FIN naming the stream (only the `ready` message carries the stream ID), so the ingress sends data without waiting for its ready ACK to return; the ACK still follows at once. Without the header (an older egress, or a proxy that drops it) the ingress waits for the ready ACK as before. A resumed session always waits for its ready ACK.
- A new session no longer waits for the first heartbeat ping before sending data; the control channel echoes its nonce afterwards. A resume still confirms the nonce first.

Over HTTP/1.1 every SSE event stream needs a connection of its own, so `http-sse` without HTTP/2 to the egress costs one more round trip (two with TLS) when no idle connection is pooled.

## 2. Negotiated values

The two nodes do not need identical `transport` sections. At session creation the ingress sends its limits and the egress answers with the values that apply to the session:

| Value | Rule |
|---|---|
| `max_frame_size`, `up_max_inflight_frames`, `up_window_bytes`, `buffer_per_direction` (as the downstream buffer) | The smaller of the two nodes' values. |
| Recovery | Requested by the ingress (`transport.recovery.enabled` / `peers[].recovery.enabled`). If the egress has recovery disabled, the create fails with `409 DBB_RECOVERY_UNSUPPORTED`: disable recovery for that peer on the ingress, or enable it on the egress. |
| Recovery grace | The smaller of the two `recovery.grace` values. |
| First attach | The ingress must attach a carrier within the egress's `unattached_timeout`; after that the egress discards the session and the ingress ends it with reason `timeout`. |
| Carrier | Must be in the egress's `http.allowed_transports`, else `409 DBB_TRANSPORT_UNSUPPORTED`. |
| Protocol version | Both must be `1.4`, else `409 DBB_PROTOCOL_MISMATCH`. |

Keeping both nodes' `transport` sections the same makes behaviour easier to predict, but it is not required.

### Flow limits

Every create response carries the egress's flow limits: `limits.up_receipt_limit` (its `receipt_limit_per_session`) and `limits.down_max_frames` (512). Advertised values above 1,048,576 are read as 1,048,576; a response without them is a protocol error. The egress sets `ack_requested: true` in its `ack_up` control message when half of either limit is in use, and the ingress acknowledges at once. The ingress also acknowledges when unacknowledged upstream frames reach `min(up_max_inflight_frames + 4, up_receipt_limit / 2)` or downstream frames reach `min(32, down_max_frames / 4)`.

## 3. Frames, windows and acknowledgements

| Key | What it controls | When to change it |
|---|---|---|
| `frame_size` | Target size of upstream frames read from the client. | Rarely. |
| `max_frame_size` | Largest frame either side accepts (≤ 1 MiB). A larger SSE upstream frame is refused with `413 DBB_FRAME_TOO_LARGE`. | Lower it if a proxy limits request bodies below 256 KiB. |
| `chunk_size` | Read size from sockets and granularity of buffer accounting. | Rarely. |
| `buffer_per_direction` | Per-session, per-direction buffer. When full, the node stops reading from the source socket (backpressure). | Raise for high-bandwidth, high-latency links; it scales memory per session. |
| `up_window_bytes`, `up_max_inflight_frames` | Upstream bytes and frames in flight before an acknowledgement. | Raise `up_window_bytes` together with `buffer_per_direction` on long fat links. |
| `receipt_limit_per_session` | Upstream receipts the egress keeps for duplicate detection. | Raise only together with `up_max_inflight_frames`. |
| `ack_batch_bytes`, `ack_max_delay` | Downstream acknowledgement after this many bytes or this much time, whichever comes first. The ingress uses `min(ack_batch_bytes, negotiated downstream buffer / 2)`. | Rarely. |
| `up_ack_max_delay` | Upper bound for delaying an upstream acknowledgement. | Rarely. |

When a buffer is full the session waits up to `backpressure_timeout` for space; then it ends with reason `timeout`. While a session is suspended for recovery, the database side is not closed for backpressure; `recovery.grace` and `suspended_buffer_limit` bound it instead.

## 4. Failure detection, failover and recovery

**Failure detection.** Both sides send heartbeats every `heartbeat`. A carrier with no progress for `failure_detect_timeout` is considered lost. Progress is session traffic (an event or WebSocket message bound to the session, or a data frame still arriving); SSE comments and fields the parser ignores, such as keep-alive comments inserted by a proxy, do not count. Over HTTP/2 the node also sends a PING every `heartbeat` and closes the connection after 5 s without an answer.

**Failover.** Each peer has endpoints ordered by `priority`. The ingress probes them every `failover.healthcheck_interval`; `unhealthy_threshold` failures make an endpoint `UNHEALTHY`, `healthy_threshold` successes make it usable again. New sessions use the best usable endpoint; when a preferred endpoint recovers, only new sessions move to it (`failback: new_sessions`).

**Mute paths.** A path that starts dropping packets silently (a firewall rule, a dead link) is noticed by the ingress within about two probe times rather than after `control_request_timeout`. The guard timeout is 4 × the endpoint's last probe time, at least 1 s and at most `healthcheck_timeout`. A new session built on a cached probe answer whose create has not answered after 2 × the last probe time (at least 0.5 s) probes the endpoint alongside the create; any request to the endpoint that waits longer than the guard timeout for its response headers does the same. When that probe gets no answer within the guard timeout (an HTTP error answer does not count: the path works), the ingress closes the endpoint's connections that received nothing during the check, so every request still waiting on them fails at once and recovery moves established sessions; when another endpoint is usable the address also enters `failover.cooldown`, and the new session continues on the next endpoint. The log line is `peer endpoint not answering; closed its stalled connections`. Measured with two endpoints and a firewall DROP on the primary (RTT 84 ms): first new session 10.8 s → 1.6–1.8 s, established session 13.7 s → 2.9 s.

**Cooldown.** The first failure of a carrier on an endpoint cools that endpoint/carrier pair for `failover.cooldown`; a failure of the whole path (for example a refused TCP connection) cools the address. A cooldown is never shortened by a successful probe. When every candidate is cooling down, the ingress waits for the earliest one to end, within `create_retry_timeout` (create) or the recovery deadline (recovery). Purely local causes never cool anything: the local create deadline, the recovery deadline, a client that closed or reset its socket, node shutdown, and exhausted local budgets.

**Recovery.** With recovery enabled, a lost carrier does not end the session. The egress suspends the session and keeps unacknowledged data; the ingress re-attaches through any usable endpoint and carrier and resumes from the last acknowledged offsets in both directions. Before the resume completes, writes already in progress on the old carrier are allowed to finish for at most `recovery.barrier_timeout`; if that write barrier cannot be reached, the session ends with reason `barrier`. The whole recovery must finish within `recovery.grace`, counted from the fault and never extended. Suspended sessions together may hold at most `suspended_buffer_limit` bytes; above it the oldest suspended sessions holding data are ended first.

**Without recovery**, or when recovery fails, the session is terminated and the ingress closes the local client connection immediately, so the database client sees a closed connection and can reconnect. Recovery never crosses a process restart: a new instance answers `409 DBB_INSTANCE_MISMATCH` and the client reconnects.

**Terminal notice.** When the egress ends a session on its side (database connection lost, authorization revoked, buffer exhausted, frame conflict), it sends one final `error` control message on the current carrier (on WebSocket followed by a Close frame, code 1000, with the error code as the reason). The ingress then closes the local client without trying to resume.

**Recovery that cannot succeed.** With a forced carrier (`ws` or `http-sse`) and a single usable endpoint, the failed carrier stays cooled down for `failover.cooldown`; if `recovery.grace` does not exceed `cooldown + barrier_timeout`, recovery cannot succeed. `check-config` warns about this, and the ingress logs a warning when the grace negotiated with the egress falls to that level.

## 5. Timeouts

| Key | Side | Bounds |
|---|---|---|
| `dial_timeout` | egress | DNS resolution and TCP connect to the database. |
| `peer_dial_timeout` | ingress | TCP connect to a peer endpoint. |
| `ws.handshake_timeout` | ingress | WebSocket upgrade (TCP, TLS, `101`) and the wait for a connection slot for it. The first `ready` message is then awaited within `control_request_timeout`. |
| `control_request_timeout` | ingress | ack, fin, delete, resume, SSE subscription response headers, and the wait for a connection slot for them. |
| `up_request_timeout` | ingress, egress | One SSE upstream POST, including the wait for a connection slot. On the egress it caps the time a WebSocket frame body may take to arrive (5 s + 16 KiB/s of frame size); a slower frame drops the carrier. |
| `create_retry_timeout` | ingress | The whole create of a session, across retries and endpoints. |
| `unattached_timeout` | egress | Time from create until the first carrier attaches. |
| `socket_write_timeout` | both | A write to the local client or the database without progress. |
| `ws.write_idle_timeout`, `sse_write_idle_timeout` | both | A carrier write without progress. |
| `backpressure_timeout` | both | Waiting for buffer space. |
| `failure_detect_timeout` | both | A carrier without progress. |
| `shutdown_timeout` | both | The drain phase at stop; teardown afterwards takes at most 15 s more. |
| `failover.healthcheck_timeout` | ingress | One probe (background and on demand). |

Fixed values: TLS handshake 5 s; idle keep-alive connections of the ingress HTTP clients are dropped after 50 s; the egress server keeps idle connections 90 s; a `Retry-After` above 2 s is treated as 2 s. Redirects are never followed.

## 6. Memory and connection budgets

| Budget | Key | Notes |
|---|---|---|
| Payload | `global_buffer_limit` | All buffered session data of the process (`dbb_resource_in_use{resource="payload"}`). |
| Recovery retention | `suspended_buffer_limit` | Data kept for suspended sessions; part of the payload budget. |
| Codec | `codec_buffer_limit` | Encoding and decoding buffers. A WebSocket downstream frame costs about `2 × size + 56` bytes, an SSE frame about `6 × size + 16 KiB`; an SSE upstream line reserves according to its declared length. |
| Metadata | `metadata_limit` | Session state, receipts, create records and tombstones, request metadata. Of the limit, `2 × min(limit/4, 1 MiB)` is kept for control and probe requests. |
| Sessions | `limits.max_sessions` | |

**Per-identity shares on the egress.** When more than one inbound identity is enabled, each process-wide egress budget is partitioned so that one identity cannot starve the others: with *n* enabled identities, an identity may hold at most `(B − B/(2n)) / (n − 1)` of a budget *B*, so every identity always finds at least `B/(2n)` free. This applies to concurrent creates (16), probes (16) and resumes (16), request metadata, the codec budget and create records. A request over its share is answered `429 DBB_QUOTA_EXCEEDED` (message names the budget, for example `peer idempotency quota`) or, for the codec, `503 DBB_BUFFER_EXHAUSTED "codec budget"`; these answers are retryable. Shares follow the set of enabled identities on every credential reload.

**Bidirectional nodes** (with `http.enabled` and `listeners`) keep a reserve for their own local clients that remote peers cannot take: `max(1, max_sessions/4)` sessions, 1/8 of the metadata budget, and `max(codec_buffer_limit/8, max_frame_size + 56)` of the codec budget.

**Connections.**

| Budget | Size |
|---|---|
| Inbound connections (egress) | `4 × min(max_sessions, 8192) + 64`, 16 KiB of connection metadata each |
| Outbound data connections / requests (ingress) | `2 × min(max_sessions, 8192) + 32`; per peer also at most `2 × max_sessions + 32` |
| Outbound control connections / requests (ingress) | `min(min(max_sessions, 8192) + 32, 256)` |

A request waiting for a connection slot waits as long as its own deadline (see §5). When the wait runs out it fails with `local connection budget exhausted`; this is local backpressure, so the carrier is not faulted or cooled and up/FIN requests are retried on the same carrier.

The current use and limit of every budget is exported as `dbb_resource_in_use{resource}` / `dbb_resource_limit{resource}` ([Operations → Metrics](operations.md#5-metrics)). The sustained new-session rate is bounded by the create-record budget; see [Operations → Capacity planning](operations.md#10-capacity-planning).

## 7. Reverse proxies and gateways

A reverse proxy, load balancer or API gateway may sit in front of the egress `http.listen`. It must meet these requirements:

**Paths** — forward everything under `/v1/` unchanged:

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/peer/probe` | Probe: node, instance and protocol check |
| POST | `/v1/sessions` | Create a session |
| GET + Upgrade | `/v1/transport/ws` | WebSocket carrier and precheck |
| GET | `/v1/sessions/{id}/events` | SSE downstream stream |
| POST | `/v1/sessions/{id}/up` | SSE upstream frame |
| POST | `/v1/sessions/{id}/ack` | Acknowledgement |
| POST | `/v1/sessions/{id}/fin` | Half-close |
| POST | `/v1/sessions/{id}/resume` | Recovery |
| DELETE | `/v1/sessions/{id}?epoch=N` | Close |

**Request headers** — pass through unchanged: `Authorization` (bearer token), `X-DBB-Session-ID`, `X-DBB-Session-Token`, `X-DBB-Instance-ID`, `X-DBB-Epoch`, `X-DBB-Stream-ID`, `X-DBB-Offset`, `X-DBB-Length`, `X-DBB-SHA256`, `X-DBB-Nonce`, `X-DBB-Confirm-Up-Offset`, `X-DBB-Ack-Down-Offset`, `X-DBB-WS-Purpose`, `X-DBB-Features`, `Content-Length`. The total request header size must stay within 16 KiB.

**Response headers** — pass through `X-DBB-Instance-ID` (the ingress uses it to verify that an error answer really came from the peer), `X-DBB-Implicit-Ready` (saves one round trip per new session; see §1) and `Retry-After`. Do not replace DBBridge error bodies with proxy error pages.

**WebSocket** — forward the upgrade: `Upgrade`, `Connection`, `Sec-WebSocket-Key`, `Sec-WebSocket-Version`, `Sec-WebSocket-Protocol: dbbridge.v1.4` (and the response's `Sec-WebSocket-Protocol`). If the proxy cannot do this, use `auto` (sessions fall back to SSE) or `http-sse`.

**SSE** — do not buffer `GET /v1/sessions/{id}/events` responses (for nginx: `proxy_buffering off`), and do not buffer upstream POST bodies. Request bodies of `/up` are at most `max_frame_size` (256 KiB by default).

**Timeouts and keep-alive** — idle/read timeouts on the proxy must exceed `heartbeat` (15 s); 60 s or more is recommended. The ingress drops idle client connections after 50 s, so keep idle client connections open on the proxy for **at least 60 s** (the nginx default `keepalive_timeout 75s` is fine).

**No redirects, no retries to another backend** — the ingress does not follow redirects; every request of a session must reach the same egress process.

Illustrative nginx configuration (adapt to your environment):

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}
server {
    listen 443 ssl;
    server_name hq.example.com;
    ssl_certificate     /etc/nginx/tls/hq.crt;
    ssl_certificate_key /etc/nginx/tls/hq.key;
    keepalive_timeout 75s;

    location /v1/ {
        proxy_pass http://127.0.0.1:18080;     # http.listen of the egress
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_set_header Host $host;
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_read_timeout 120s;
        proxy_send_timeout 120s;
        client_max_body_size 2m;
    }
}
```

With the proxy terminating TLS, keep the egress `http.listen` on loopback (no `http.tls` needed) and point the ingress endpoint at `https://hq.example.com` (port 443, so no port in the URL).

## 8. TLS

**Server side (egress).** `http.tls.enabled: true` with `cert_file` and `key_file` serves HTTPS/WSS directly with TLS 1.2 or later. The pair is loaded by `check-config` and at start; a renewed certificate takes effect after a restart. Keep the key readable only by the service account (0600, or 0640 with a service group).

**Client side (ingress).** `https://` endpoints are verified against the system trust store or `peers[].tls.ca_file`, including the host name in the endpoint URL. Use ASCII host names (convert internationalised names to their `xn--` form) and certificates issued for exactly those names. `tls.verify: false` is accepted for loopback endpoints; for other hosts it also needs `tls.allow_insecure_skip_verify: true` and should only be used in a test lab.

**Plaintext.** A non-loopback `http.listen` without TLS needs `http.allow_insecure_listen: true`; a non-loopback `http://` endpoint needs `peers[].allow_insecure_http: true`. Both expose bearer tokens and database traffic on the network. Prefer TLS on the node or a local TLS-terminating proxy.

**Outbound HTTP proxies.** Peer connections ignore `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` unless `transport.use_environment_proxy: true`. The variables are read once per process.
