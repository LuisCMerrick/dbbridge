**English** | [简体中文](CHANGELOG.zh-CN.md)

# Changelog

The version of a build is set when it is built (`dbbridge version`).

## [1.4.26.10.27] - 2026-10-27

Protocol 1.4, wire-compatible with 1.4.26.10.25 nodes. Data-plane performance work from the optimization review, and a stall fix.

- **Fix: 5 s stall after a server-side close.** When the backend closed a connection first (HTTP `Connection: close`, short-lived connections), the egress could drop the carrier before the ingress saw the acknowledgement of its upstream FIN; the ingress counted that normal close as a carrier failure and cooled the endpoint transport for `failover.cooldown` (5 s), so the next new session waited about 5 s. In a loopback test about one in three short connections was affected. The ingress now confirms the close over the control channel whenever either FIN acknowledgement is still missing, as it already did for the downstream one.
- **Upstream digest verified once.** An upstream frame was hashed (SHA-256) up to three times on the egress (WebSocket decode, the transport handler, and again inside the session under its lock). It is now verified once per transport, outside the session lock, and the session adopts the transport's buffer instead of copying it.
- **Downstream frames encoded with one copy.** The egress sender reads frames as views of the session window and encodes them straight into the WebSocket frame or the SSE event (SHA-256 and Base64 streamed over the segments), instead of first copying each frame out of the window. SSE events are built as one byte buffer without `fmt`/string concatenation.
- **Ingress buffers.** Local-socket read buffers are recycled by power-of-two size class (a buffer whose HTTP send failed is never recycled); WebSocket frames are written as header plus payload through one message writer instead of being joined first; SSE frame lines are buffered at their declared size instead of growing by appends.
- **Egress backend reader** blocked on the global payload budget now waits on the budget's release notification (with a 250 ms safety poll) instead of polling every 10 ms.
- **Benchmarks.** `BenchmarkByteWindowDownstream`, `BenchmarkIngestUpFrame` and the loopback end-to-end `BenchmarkForward` (ws, http-sse). Loopback, 2 vCPU arm64, before → after: http-sse 140 → 250 MB/s with 54% fewer bytes allocated; ws throughput unchanged (≈280 MB/s, bound by SHA-256 and syscalls) with 45% fewer bytes allocated; upstream ingest of a 64 KiB frame 140 → 65 µs.
- **Docs.** Capacity planning describes when and how to set `GOMEMLIMIT` (not set by default).

Codec budget charges are unchanged (conservative): the new paths use less memory than they reserve.

## [1.4.26.10.25] - 2026-10-25

Protocol 1.4, compatible with 1.4.26.10.23 nodes. Fixes from the shutdown review.

- **User-Agent is configurable.** New `transport.user_agent`; every request and WebSocket handshake sent to a peer now carries it instead of Go's `Go-http-client/1.1`/`2.0`. Default `DBBridge/1.4`. Printable ASCII, at most 256 bytes.
- **Shutdown progress.** A stop that waits for open sessions (`transport.shutdown_timeout`, 30 s by default; pooled database connections usually keep it waiting the full time) now logs what it waits for at once and every 5 s, with the time left and a reminder that a second Ctrl-C forces the exit, and logs how many sessions it closes when the wait times out. Before, a first Ctrl-C looked like a hang.
- **Fix.** A health-check probe that never left this node (it timed out waiting for a local connection or request budget) was recorded as an endpoint failure, so local contention could mark a healthy endpoint UNHEALTHY and put it in cooldown. Local resource errors no longer change endpoint health.
- **Tests.** `TestLargeBidirectionalByteStream` bounds progress per block instead of the whole 50 MiB transfer at 15 s, so race builds on slower machines no longer fail a correctness test on throughput.

## [1.4.26.10.23] - 2026-10-23

Protocol 1.4. Maintenance and protocol hardening release.

- **SSE and carrier liveness.** SSE comments (such as proxy keep-alives) no longer count as session progress or consume event line limits. `http-sse` upstream frames now enforce `failure_detect_timeout` and remote backpressure timeouts while waiting for acknowledgement, matching WebSocket behaviour.
- **Connection pooling and flow control.** Outbound SSE subscriptions shard across up to `transport.h2_pool_size` (1–64) HTTP clients per endpoint. Ingress short reads compact their buffer allocation so small frames do not hold full-frame payload and recovery-retention budgets, and dynamic frame sizing clamps to the remaining send window.
- **Budgets and admission.** Bidirectional nodes size `codec_buffer_limit` and the local ingress codec reserve to cover SSE frame decoding alongside egress traffic. Egress session routes close HTTP/1.1 connections when rejecting requests before reading their body.
- **Configuration and credential reload.** Inline credential reloads reject added or duplicate `peers` entries (which require a restart). Inbound quota validation uses saturating addition. Disabled inline identities' token files are not required at service installation. `gen-config` rolls back newly created files if directory sync or pair publication fails.
- **Observability and service management.** Added `dbb_ingress_faults_total{source,reason}` and expanded terminal/fault reason classification for deterministic protocol refusals. WebSocket sessions closed during the HTTP 101 upgrade handoff still deliver their terminal `error` notice before closing the carrier. Windows service installation marks and verifies ownership of the Application Event Log source.
- **Release packaging and toolchain.** `scripts/build-release.sh` bundles `third_party/MODULES.txt` and dependency license files into release archives. Raised minimum Go toolchain to 1.27.3 (`GO-2026-6809`).

## [1.4.26.10.20] - 2026-10-20

First release. Protocol 1.4.

- **Roles.** One binary acts as ingress (local TCP listeners), egress (HTTP server in front of the databases) or both, as configured.
- **Carriers.** WebSocket, or HTTP POST + Server-Sent Events; `auto` falls back to SSE on the same endpoint when a proxy cannot carry WebSocket.
- **Failover and recovery.** Several endpoints per peer with priorities, health probes and cooldown of failed paths. Sessions with recovery enabled survive a carrier failure within `recovery.grace` without losing or duplicating a byte; otherwise the client connection is closed at once.
- **Security.** TLS, bearer-token identities, peer node identity check, per-identity target ACLs and quotas, CIDR allow-lists for targets, per-identity budget shares.
- **Credential reload.** Token files and credential settings are re-read every `auth.reload_interval`, on `SIGHUP` / `systemctl reload` and on `sc.exe control <name> paramchange`; disabling or removing an identity closes its sessions. Rejected reloads keep the previous credentials and are logged and exposed as metrics.
- **Bounded resources.** Memory, session metadata, connections and concurrency are budgeted; overload is answered with explicit, retryable errors.
- **Operations.** `gen-config` for matched pairs, `check-config` with stable error codes (`DBB-CFG-001` … `DBB-CFG-030`) and exit status 78 for configuration errors, service installation for systemd and Windows, loopback admin endpoint with `/healthz`, `/readyz` and Prometheus `/metrics`.
- **Release.** Packages for Linux amd64/arm64 and Windows amd64 with the license files and module list of linked dependencies, and `SHA256SUMS`; reproducible builds from `SOURCE_DATE_EPOCH`.
