**English** | [简体中文](CHANGELOG.zh-CN.md)

# Changelog

The version of a build is set when it is built (`dbbridge version`).

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
