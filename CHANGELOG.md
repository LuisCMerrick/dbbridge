**English** | [简体中文](CHANGELOG.zh-CN.md)

# Changelog

The version of a build is set when it is built (`dbbridge version`).

## [1.4.26.10.20] - 2026-10-20

First release. Protocol 1.4.

- **Roles.** One binary acts as ingress (local TCP listeners), egress (HTTP server in front of the databases) or both, as configured.
- **Carriers.** WebSocket, or HTTP POST + Server-Sent Events; `auto` falls back to SSE on the same endpoint when a proxy cannot carry WebSocket.
- **Failover and recovery.** Several endpoints per peer with priorities, health probes and cooldown of failed paths. Sessions with recovery enabled survive a carrier failure within `recovery.grace` without losing or duplicating a byte; otherwise the client connection is closed at once.
- **Security.** TLS, bearer-token identities, peer node identity check, per-identity target ACLs and quotas, CIDR allow-lists for targets, per-identity budget shares.
- **Credential reload.** Token files and credential settings are re-read every `auth.reload_interval`, on `SIGHUP` / `systemctl reload` and on `sc.exe control <name> paramchange`; disabling or removing an identity closes its sessions. Rejected reloads keep the previous credentials and are logged and exposed as metrics.
- **Bounded resources.** Memory, session metadata, connections and concurrency are budgeted; overload is answered with explicit, retryable errors.
- **Operations.** `gen-config` for matched pairs, `check-config` with stable error codes (`DBB-CFG-001` … `DBB-CFG-030`) and exit status 78 for configuration errors, service installation for systemd and Windows, loopback admin endpoint with `/healthz`, `/readyz` and Prometheus `/metrics`.
- **Release.** Packages for Linux amd64/arm64 and Windows amd64 with `SHA256SUMS`; reproducible builds from `SOURCE_DATE_EPOCH`.
