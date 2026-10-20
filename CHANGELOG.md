**English** | [简体中文](CHANGELOG.zh-CN.md)

# Changelog

The version of a build is set when it is built (`dbbridge version`); release notes are collected under `[Unreleased]` until a release is tagged.

## [Unreleased]

### Security

- **Revocation works in one step.** Setting an inbound identity to `enabled: false` (or removing it) and reloading closes all of its sessions, also when its token file or variable is deleted in the same change: a disabled identity's token sources are not read.
- **Rejected credential reloads are visible.** They are logged at ERROR (`credential reload rejected ...`, `code=DBB-CFG-020`, with `cause_code`), repeated every 5 minutes while the cause persists, and exposed as `dbb_auth_credentials_stale` and `dbb_auth_reload_last_success_timestamp_seconds`.
- **Inline credentials reload without restart.** For configurations without a credentials file (including all `gen-config` output), `auth.inbound_peers`, `peers[].token_file`/`token_env` and every token file are re-read every `auth.reload_interval`. `SIGHUP`, `systemctl reload <name>` and `sc.exe control <name> paramchange` trigger an immediate reload. Tokens from environment variables still need a restart; the node logs which variables are affected.
- **Per-identity budget shares** on the egress for request metadata, codec memory and create records: one peer cannot exhaust them for all others. On bidirectional nodes, remote peers cannot use the share of metadata and codec memory reserved for local listeners.
- `gen-config` writes `peers[].allow_insecure_http` for `--allow-insecure-http` instead of the broader deprecated `transport.allow_insecure_remote_http`.
- The install-time environment snapshot includes every token variable the configuration can resolve.
- `check-config`, `run` and `install` warn about credential files under `/home`, `/root`, `/run/user`, `/tmp` or `/var/tmp` that an installed service cannot see.

### Fixed

- **WebSocket behind proxies.** Any plain proxy or gateway answer to the WebSocket precheck (redirect, 407, 413, 429, gateway 5xx, ...) makes `auto` use SSE on the same endpoint; only the WebSocket carrier of that endpoint is cooled down, and the endpoint is not marked `MISMATCHED`.
- A slow first WebSocket `ready` message is awaited within `control_request_timeout`; `ws.handshake_timeout` bounds only the upgrade.
- Outbound connection budgets: the per-peer data connection cap is `2 × max_sessions + 32`; waiting for a connection slot is bounded by the request's own timeout; an exhausted local budget is treated as local backpressure (retried, never faulting or cooling a carrier).
- Small SSE frames stay readable under codec memory pressure.
- Flow-limits values above 1,048,576 are clamped instead of failing session creation; `ack_requested` is honoured only on sessions that negotiated the extension.
- Failed carriers are cooled down also with recovery disabled; local causes (client reset, shutdown, local deadlines and budgets) never cool a carrier or address.
- Shutdown: sessions are stopped concurrently within the shutdown deadline, and remote cleanup reaches backup endpoints when the primary is down.
- The recovery deadline is counted from the fault; the first attach is bounded by the egress's `unattached_timeout`.
- Codec memory used by early data is released when a carrier fails.
- The egress sends a final `error` message on the carrier when it ends a session (database lost, ACL revoked, buffer exhausted, frame conflict), so the ingress closes the client at once without a futile resume.
- DNS failures of a target are reported as `502 DBB_DIAL_FAILED` (counted in `dbb_dial_failed_total`); a create interrupted by shutdown gets `503 DBB_DRAINING`.
- A malformed WebSocket upgrade is answered `400 DBB_BAD_FRAME` and leaves the session attachable.
- An oversized SSE upstream frame is answered `413 DBB_FRAME_TOO_LARGE`.
- FIN and data conflicts close the session with `DBB_FRAME_CONFLICT`; an authorization race closes it with `DBB_TARGET_FORBIDDEN`.
- Retries of a pending or failed create are answered from the idempotency table (`202 DBB_CREATE_PENDING` or the cached answer) instead of a transient quota error.
- A session whose final FIN write fails after the peer acknowledged the FIN ends as a normal close; a resume claiming a FIN that was never sent is rejected.
- `/ack` reports `backpressure_reason: local_socket_write` whenever upstream data is blocked behind a database write.
- Windows: a configuration error stops the built-in service with service-specific exit code 78 and it is not restarted; logging off does not stop a node running in a console.
- `install` exits 78 for configuration and credential errors, like `check-config`.
- `install` writes the executable path you invoked (symbolic links kept) or `--exec-path`; `uninstall` waits for the full drain.
- All metric families have `# HELP` lines and a stable order. Fallbacks are counted once per created session; recovery durations are recorded per resume attempt and endpoint; `dbb_ingress_bound_sessions` counts only sessions with an active carrier.

### Changed

- **Derived defaults:** `ack_batch_bytes = min(1MiB, buffer_per_direction/4)`, `max_frame_size = min(256KiB, up_window_bytes)`, `frame_size = min(32KiB, max_frame_size)`, `suspended_buffer_limit = min(64MiB, global_buffer_limit/4)`, `recovery.barrier_timeout = min(5s, grace/2)`, `failover.healthcheck_timeout = min(3s, healthcheck_interval/2)`. With no other key changed, every default is the same as before. Constraint errors mark default values (`..., default`).
- **Stable error codes** `DBB-CFG-001` … `DBB-CFG-030` in CLI errors (`Error [DBB-CFG-002]: ...`) and in log records (`code=`). Exit status: 0, 78 (configuration), 130 (forced), 1 (other).
- `gen-config` enables the admin endpoint on loopback (`127.0.0.1:18090` for A, `127.0.0.1:18091` for B; options `--a-admin-listen`, `--b-admin-listen`).
- New `install --exec-path`. Per-command help (`run --help`, `check-config --help`, `install --help`).
- The ingress drops idle peer connections after 50 s; reverse proxies should keep idle client connections for at least 60 s.
- An egress-only node that allows only WebSocket needs a smaller minimum `codec_buffer_limit` (`max(2 × max_frame_size + 56, 16 KiB)`).
- Termination reasons `shutdown`, `local_socket`, `dial_failed` (egress could not reach the database) and `draining` (egress shutting down); cleanup events report `not_needed` and `skipped`. The `session terminated` log leaves `deadline` empty when no recovery was started. Normal closes and closes at shutdown are not counted as failures; `dbb_terminated_direct_total` counts failed sessions with recovery disabled, `dbb_terminated_after_resume_fail_total` failed sessions with recovery enabled.
- New metrics `dbb_auth_credentials_stale`, `dbb_auth_reload_last_success_timestamp_seconds`, `dbb_ingress_recovering_sessions`. `dbb_auth_reload_success_total` counts changed snapshots after start (not the initial load).
- Codec refusals of SSE upstream frames use the message `codec budget`.
- `check-config` warns when recovery cannot succeed because `recovery.grace` does not exceed `failover.cooldown + barrier_timeout` for a peer with a forced carrier and a single endpoint.
- Samples: admin enabled on loopback everywhere (`configs/peer-b*.yaml` on 18090); production samples use token files under `tokens/`; `configs/peer-b.yaml` lists a single token variable.
- Generated and sample systemd units have `ExecReload`. The WinSW sample restarts twice, then stays stopped.
- Release: per-platform packages (`.tar.gz`, `.zip` for Windows) with binary, documentation, samples and service files; `SHA256SUMS` covers every artifact including `BUILD-INFO.txt`; reproducible builds from `SOURCE_DATE_EPOCH`; tags with a suffix are published as pre-releases. CI runs on every push and pull request, including a Windows job.
- Documentation in English and Simplified Chinese; `docs/configuration-transport.md` is replaced by `docs/transport.md`, with new `docs/installation.md` and `docs/security.md`.

### Upgrade notes

- Nodes of this release interoperate with nodes of earlier protocol-1.4 releases; upgrade one node at a time ([Installation §8](docs/installation.md#8-upgrade)).
- **Reinstall installed services** (`dbbridge uninstall`, then `dbbridge install` with the same options) to get `ExecReload` in the systemd unit and the Windows behaviour of not restarting on configuration errors. Until then, services keep the definition they were installed with.
- Automation that expects exit status 1 from `install` for configuration errors must accept 78.
- Edits to `auth.inbound_peers` in a main configuration file take effect on the next reload (within `auth.reload_interval`), without a restart. Validate edits with `check-config` before saving them.
- Raise reverse-proxy keep-alive for idle client connections to at least 60 s if it is lower.
- Dashboards and alerts: add the `shutdown`, `local_socket`, `dial_failed`, `draining`, `not_needed` and `skipped` reasons; review alerts on `dbb_terminated_direct_total` / `dbb_terminated_after_resume_fail_total`, `dbb_auth_reload_success_total` and the `peer codec budget` message. The deprecated names `dbb_resume_success`, `dbb_resume_failed` and `dbb_client_close_count` are still emitted; move to the `_total` names.
- If you set `buffer_per_direction` without `ack_batch_bytes`, the derived `ack_batch_bytes` is a quarter of it; set `ack_batch_bytes` explicitly to keep a different value.
- The sustained new-session rate is bounded by the create-record budget, about 10 per second with defaults; workloads with many short connections should raise `transport.metadata_limit` ([Operations §10](docs/operations.md#10-capacity-planning)).
