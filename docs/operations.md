**English** | [简体中文](operations.zh-CN.md)

# Operations

[README](../README.md) · [Installation](installation.md) · [Configuration](configuration.md) · [Transport](transport.md) · [Security](security.md) · **Operations** · [Examples](examples.md)

- [1. CLI reference](#1-cli-reference)
- [2. Exit status, signals and lifecycle](#2-exit-status-signals-and-lifecycle)
- [3. Reload semantics](#3-reload-semantics)
- [4. Admin endpoints](#4-admin-endpoints)
- [5. Metrics](#5-metrics)
- [6. Logs and events](#6-logs-and-events)
- [7. Peer protocol error codes](#7-peer-protocol-error-codes)
- [8. Environment variables](#8-environment-variables)
- [9. Configuration error codes](#9-configuration-error-codes)
- [10. Capacity planning](#10-capacity-planning)
- [11. Troubleshooting](#11-troubleshooting)

## 1. CLI reference

```
dbbridge <command> [options]
```

| Command | Purpose |
|---|---|
| `run` | Run the node in the foreground or under a service manager. |
| `check-config` | Validate a configuration and exit. |
| `gen-config` | Generate a matched configuration pair and a shared token. |
| `install` | Install as a systemd service (Linux) or Windows service. |
| `uninstall` | Stop and remove a service installed by `dbbridge install`. |
| `version` (`-v`, `--version`) | Print version information. |
| `help` (`-h`, `--help`) | Print the help text. |

Options use Go flag syntax: one or two dashes, `--flag=value` or `--flag value`. Positional arguments are rejected. A first argument that is an option runs `run` with all arguments (`dbbridge -c peer.yaml`), and `dbbridge --install -c peer.yaml` is the same as `dbbridge install -c peer.yaml`. Without arguments the help text is printed and the exit status is 1. `<command> --help` prints that command's options to stderr and exits 0.

### `run`, `check-config`, `install`

| Option | Commands | Default | Notes |
|---|---|---|---|
| `--config`, `-c` | all three | — | Required. Its directory is the base for relative paths and the working directory of an installed service. |
| `--log-level` | all three | `info` | `debug`, `info`, `warn`, `error`. `install` writes it into the service definition. |
| `--log-format` | all three | `text` | `text` or `json` (recommended for production). |
| `--env-file` | all three | — | Service credential snapshot: a JSON object `{"NAME": "value"}` of at most 1 MiB, written by `install` and passed to the service automatically. It must be a regular file without group/other permissions, owned by the process user or root. Reserved names (for example `PATH`, `HOME`, proxy variables, `NOTIFY_SOCKET`, `LISTEN_*`, Go runtime variables, `SSL_CERT_*`, `TZ`, `LD_*`) are refused. A variable already set and non-empty in the environment wins over the snapshot (a warning is logged when they differ). |
| `--offline` | `check-config` | off | Validate schema, defaults, references, quotas and listen addresses without reading token files, environment variables, TLS certificates, keys or CA files. The credentials file itself is still read. |
| `--service-name` | `run`, `install` | `dbbridge` | `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`, without a `.service` suffix. Allows several instances on one host. `run` uses it for the Windows Event Log source. |
| `--service-user` | `install` | Linux `root`, Windows `LocalSystem` | Linux account name or uid; on Windows `LocalSystem`, `LocalService` or `NetworkService`. |
| `--exec-path` | `install` | the path dbbridge was invoked with | Executable written into the service definition, verbatim (symbolic links are kept). It must be, or link to, a regular file. |
| `--start` | `install` | off | Start the service after installing. |
| `--dry-run` | `install` | off | Print the installation plan as JSON (including the full systemd unit) and change nothing. |

Output: `check-config` prints `Config <path> is valid. Node ID: <id>` (with `--offline`: `... is structurally valid (offline: ...)`) and every warning as `Warning: ...` on stderr. Errors are printed as `Error [DBB-CFG-0xx]: <message>`, or `Error: <message>` for command-line errors.

### `gen-config`

Writes `peer-a.yaml`, `peer-b.yaml` and `shared.token` into the output directory. Both configurations are validated before they are written.

| Option | Default | Notes |
|---|---|---|
| `-o`, `--output` | `.` | Output directory. |
| `--mode` | `bidirectional` | `bidirectional`, `a-to-b` (A has the listener, B the database) or `b-to-a`. Both nodes are always generated. |
| `--token-file` | `<output>/shared.token` | Shared token path, relative to the current directory; the configurations reference it relative to their own directory. An existing token file is reused. |
| `--protocol` | `auto` | `auto`, `ws`, `http-sse`. |
| `--recovery` | `true` | `--recovery=false` disables session recovery on both nodes. |
| `--allow-insecure-http` | off | Allow plaintext `http://` peer URLs to non-loopback hosts (writes `peers[].allow_insecure_http: true`). |
| `--force` | off | Replace existing configurations (the token is never replaced). On Linux the replaced files keep their mode and owner; on Windows new files get a protected DACL and must be granted again. |
| `--max-sessions` | `200` | `limits.max_sessions` of both nodes. In bidirectional mode the inbound identity gets half. A sufficient `transport.metadata_limit` is written when needed. |
| `--a-node-id`, `--b-node-id` | `A`, `B` | |
| `--a-listen`, `--b-listen` | `127.0.0.1:13306`, `127.0.0.1:13307` | Local listener of each node. |
| `--a-http-listen`, `--b-http-listen` | `127.0.0.1:18080`, `127.0.0.1:18081` | Inbound HTTP listener. |
| `--a-admin-listen`, `--b-admin-listen` | `127.0.0.1:18090`, `127.0.0.1:18091` | Loopback admin endpoint (enabled). |
| `--a-target-id`, `--b-target-id` | `mysql`, `mysql` | Target ID; the other node's listener references it. |
| `--a-target-address`, `--b-target-address` | `127.0.0.1:3306`, `127.0.0.1:3306` | Database address. |
| `--a-allow-cidr`, `--b-allow-cidr` | the exact target IP (`/32` or `/128`) | Repeatable. Required for host-name targets. |
| `--a-peer-url`, `--b-peer-url` | the other node's HTTP listener | Repeatable (failover endpoints `link-1`, `link-2`, ... with priority 1, 2, ...). `https` when the other node has a certificate; a wildcard listener becomes the loopback address. Set it explicitly for remote deployments. |
| `--a-tls-cert`/`--a-tls-key`, `--b-tls-cert`/`--b-tls-key` | — | Existing certificate and key for the node's inbound HTTPS listener; both or neither. |

Generated configurations use inline credentials with token files (re-read on reload), the same token in both directions, `tls.verify: true`, admin on loopback, and listener ID `database-local`. The token is 32 random bytes, hex-encoded, mode 0600 (protected DACL on Windows); its value is never printed.

### `uninstall`

| Option | Default | Notes |
|---|---|---|
| `--service-name` | `dbbridge` | |
| `--dry-run` | off | Check that the service exists and was installed by DBBridge; change nothing. |

Only services carrying the DBBridge marker are removed. The configuration, credentials and the service environment snapshot are kept. On Linux, `uninstall` waits for the unit's stop timeout plus 30 s (at least 2 minutes) for the drain.

### `version`

```
dbbridge version <version> (commit: <commit>, built: <date>, runtime: <go version>, protocol: 1.4)
```

Release builds carry the release version, source identity and build date. A plain `go build` shows `1.4-dev`, and commit and date from the Go build information (or `unknown`).

## 2. Exit status, signals and lifecycle

| Exit status | Meaning |
|---|---|
| `0` | Success, including a clean stop after a termination request. |
| `78` | Configuration or credential error (codes `DBB-CFG-001` to `DBB-CFG-006`) from `run`, `check-config` or `install`, including an unusable `--env-file`. |
| `130` | A second termination request during the drain forced the exit. |
| `1` | Everything else: command-line errors (unknown command, invalid option value), start failure such as an address already in use (`DBB-CFG-010`), a fatal failure while running (`DBB-CFG-011`), service install/uninstall failures that are not configuration errors (`DBB-CFG-030`). |

**Service managers.** The generated systemd unit sets `Restart=on-failure`, `RestartSec=5s`, `RestartPreventExitStatus=78` and a start limit of 10 starts per 300 s: a configuration error is not retried, other failures are. The built-in Windows service reports a configuration error as service-specific exit code 78 (visible in `sc.exe query <name>` as `SERVICE_EXIT_CODE 78`), writes an Event Log error and is not restarted; other failures are restarted after 5 s (failure count reset after one day).

**Start.** The node starts the admin server, the inbound HTTP server and the listeners, then reports readiness (`READY=1` to systemd, *Running* to the Windows SCM) and `/readyz` returns 200.

**Stop.** `SIGINT`/`SIGTERM` (Linux), Ctrl-C/Ctrl-Break in a Windows console, or Stop/Shutdown/PreShutdown from the Windows SCM:

1. `/readyz` returns 503, systemd receives `STOPPING=1`, no new client connections or sessions are accepted (new creates get `503 DBB_DRAINING`).
2. Established sessions may finish during `transport.shutdown_timeout` (default 30 s).
3. Teardown closes the remaining sessions concurrently, including remote cleanup through backup endpoints when the primary is unreachable, then the HTTP and admin servers. Teardown takes at most 15 s, so a stop takes at most `shutdown_timeout + 15 s`.
4. A second termination request during the drain exits immediately with status 130.

**Reload.** `SIGHUP` (Linux; `systemctl reload <name>` sends it) and `sc.exe control <name> paramchange` (Windows service) re-read credentials immediately; they never stop the node. See §3.

**Fatal failure.** If a component fails while running (for example the inbound server stops serving), `/readyz` returns 503, the node drains and exits with status 1 (`DBB-CFG-011`), and the service manager restarts it.

## 3. Reload semantics

| | |
|---|---|
| Triggers | Every `auth.reload_interval` (default 5 s); `SIGHUP` / `systemctl reload <name>`; Windows `sc.exe control <name> paramchange`. |
| Read | Inline: `auth.inbound_peers` and the `token_file`/`token_env` of existing peers from the main configuration, plus every token file they name. Credentials file: the whole file plus every token file it names. Environment variables are read from the unchanged process environment. |
| Applied | The new snapshot is validated with the same rules as at start (ACL targets, quotas, `credential_ref`, duplicate tokens) and swapped atomically. Sessions of identities that were disabled or removed, and sessions to targets removed from an identity's `allowed_targets`, are closed with `DBB_TARGET_FORBIDDEN`. Per-identity budget shares follow the new set of enabled identities. |
| Not applied | Every other setting; see [Configuration → What is re-read while running](configuration.md#16-what-is-re-read-while-running). Adding or removing a peer, or adding `auth.credentials_file`, makes the reload fail with "... requires a restart". |
| Rejected candidate | The previous credentials stay in effect, including identities the candidate disables or removes. Log `ERROR credential reload rejected; ...` with `code=DBB-CFG-020`, `cause_code=<DBB-CFG-0xx>`, `trigger=interval` or `request`, and `err`. The error is logged once per distinct cause, repeated every 5 minutes while it persists and on every explicit request. `dbb_auth_credentials_stale` is 1 and `dbb_auth_reload_failed_total` increases. When a later reload succeeds: `INFO credential reload succeeded; the earlier reload error is resolved`. |
| Logs | `credentials reloaded` (changed), `credentials re-read; no change` (explicit request without change), `SIGHUP received; re-reading credentials`. |

## 4. Admin endpoints

Served on `admin.listen` when `admin.enabled: true`; never on the peer port. No TLS, no authentication.

| Path | Answer |
|---|---|
| `/healthz` | `200 ok` while the process runs. |
| `/readyz` | `200 ready` after a successful start; `503 not ready` before start, during the drain and after a fatal failure. |
| `/metrics` | Prometheus text format 0.0.4. The rendering is cached for 1 s. |

Limits: at most 16 concurrent connections (more are closed), header read timeout 5 s, read/write timeout 10 s, idle timeout 30 s, headers ≤ 8 KiB.

## 5. Metrics

Every family has `# HELP` and `# TYPE` lines and a stable order. Side: **E** = counted where the node acts as egress (inbound HTTP, database side), **I** = where it acts as ingress (listeners, client side), **both** = either. A family counted on the other side stays 0 on a one-way node; for example `dbb_up_bytes_total` is always 0 on a pure ingress node.

### Sessions and traffic

| Metric | Type | Side | Meaning |
|---|---|---|---|
| `dbb_active_sessions` | gauge | E | Egress sessions that are not suspended. |
| `dbb_suspended_sessions` | gauge | E | Egress sessions suspended for recovery. |
| `dbb_up_bytes_total` | counter | E | Bytes written to databases. |
| `dbb_down_bytes_total` | counter | I | Bytes written to local clients. |
| `dbb_post_count_total` | counter | E | SSE upstream POSTs received. |
| `dbb_piggyback_ack_total` | counter | E | Acknowledgements carried on data messages. |
| `dbb_explicit_ack_total` | counter | E | `/ack` requests received. |
| `dbb_up_fin_applied_total` | counter | E | Upstream FINs applied to the database connection. |
| `dbb_down_fin_acked_total` | counter | E | Downstream FINs acknowledged by the peer. |
| `dbb_dial_total` | counter | E | Database dial attempts. |
| `dbb_dial_failed_total` | counter | E | Failed database dials, including DNS failures. |
| `dbb_egress_carrier_sessions{transport}` | gauge | E | Egress sessions by carrier (`ws`, `http-sse`). |
| `dbb_egress_state_<STATE>` | gauge | E | Egress sessions per state (`CREATING`, `ATTACHING`, `ACTIVE`, `DRAINING`, `SUSPENDED`, `RESUMING`, `CLOSED`). Present only while at least one session in that state exists. |
| `dbb_egress_up_pending_frames`, `dbb_egress_down_retained_frames`, `dbb_egress_digest_receipts`, `dbb_egress_down_unconfirmed_bytes` | gauge | E | Sums over egress sessions. Present only while at least one egress session exists. |
| `dbb_ingress_bound_sessions{peer,endpoint,transport,recovery}` | gauge | I | Ingress sessions with an active carrier. |
| `dbb_ingress_recovering_sessions` | gauge | I | Ingress sessions inside a recovery window without an active carrier. |
| `dbb_ingress_up_pending_bytes` | gauge | I | Upstream bytes sent but not acknowledged. |
| `dbb_ingress_retained_frames` | gauge | I | Upstream frames kept for replay. |
| `dbb_ingress_connections` | gauge | I | Local client connections; present while at least one exists. |
| `dbb_ingress_payload_bytes`, `dbb_ingress_codec_bytes` | gauge | I | Payload and codec budget in use (process-wide). |
| `dbb_ingress_accept_rejected_total` | counter | I | Local client connections refused because a local budget was exhausted. |

### Session outcomes and recovery

| Metric | Type | Side | Meaning |
|---|---|---|---|
| `dbb_resume_success_total` | counter | E | Completed recoveries. |
| `dbb_resume_failed_total` | counter | I | Sessions that ended inside a recovery window. |
| `dbb_terminated_direct_total` | counter | I | Failed sessions with recovery disabled. |
| `dbb_terminated_after_resume_fail_total` | counter | I | Failed sessions with recovery enabled. |
| `dbb_client_close_count_total` | counter | I | Local client connections closed by DBBridge. |
| `dbb_client_close_duration_sum_ms` | counter | I | Sum of the time from termination decision to local close, in ms. |
| `dbb_remote_cleanup_failed_total` | counter | I | Remote DELETEs that failed. |
| `dbb_remote_cleanup_unknown_total` | counter | I | Remote DELETEs with an unknown outcome. |
| `dbb_session_events_total{role,event,reason,from,to}` | counter | both | Lifecycle events; label values in §6. |
| `dbb_operation_duration_seconds{role,phase}` | histogram | both | Duration of `barrier`, `local_close`, `recovery`, `cleanup`, `probe`, `precheck`, `attach`, `control`; buckets 0.001 … 60 s. |
| `dbb_endpoint_state{peer,endpoint,state}` | gauge | I | 1 for the endpoint's current state: `UNKNOWN`, `HEALTHY`, `UNHEALTHY`, `MISMATCHED`. |
| `dbb_phase_duration_seconds{peer,endpoint,phase,success}` | summary | I | Duration per endpoint of `probe`, `precheck`, `attach`, `control` and each resume attempt (`recovery`). |

Normal closes and closes during node shutdown are not counted as failures.

### Budgets, credentials and process

| Metric | Type | Side | Meaning |
|---|---|---|---|
| `dbb_buffer_memory_bytes` | gauge | both | Payload budget in use. |
| `dbb_suspended_mem_bytes` | gauge | E | Memory held by suspended sessions. |
| `dbb_recovery_retained_bytes`, `dbb_recovery_retained_limit` | gauge | both | Data retained for recovery and its limit (`suspended_buffer_limit`). |
| `dbb_resource_in_use{resource}`, `dbb_resource_limit{resource}` | gauge | both | Budget use and limit; `resource` ∈ `payload`, `codec`, `metadata`, `control_metadata`, `probe_metadata`, `sessions`, `probes`, `recovery`, `cleanup`, `creates`, `inbound_connections`, `conn_metadata`, `data_connections`, `control_connections`, `data_requests`, `control_requests`. |
| `dbb_resource_wait_seconds{resource}` | summary | both | Time spent waiting for a budget. |
| `dbb_backpressure_total` | counter | both | Times a session waited for buffer space. |
| `dbb_admission_rejected_total` | counter | E | Inbound connections and requests refused by admission control. |
| `dbb_auth_reload_success_total` | counter | both | Reloads that installed a changed credential snapshot (the initial load is not counted). |
| `dbb_auth_reload_failed_total` | counter | both | Rejected credential loads and reloads. |
| `dbb_auth_credentials_stale` | gauge | both | 1 while the last reload was rejected and the previous credentials are in effect. |
| `dbb_auth_reload_last_success_timestamp_seconds` | gauge | both | Unix time of the last accepted credential load. |
| `dbb_unbalanced_release_total` | counter | both | Accounting self-check; stays 0. A non-zero value indicates a defect worth reporting. |

**Deprecated names.** `dbb_resume_success`, `dbb_resume_failed` and `dbb_client_close_count` are emitted alongside their `_total` replacements for existing dashboards. Use the `_total` names; the old names will be removed in a future release.

**Suggested alerts.** `dbb_auth_credentials_stale == 1`; `/readyz` not 200; `rate(dbb_dial_failed_total[5m]) > 0`; `dbb_endpoint_state{state="MISMATCHED"} == 1`; `dbb_resource_in_use / dbb_resource_limit > 0.9` for `sessions`, `metadata`, `payload`; rising `dbb_terminated_*` or `dbb_resume_failed_total`.

## 6. Logs and events

Logs go to stderr (`--log-format text` or `json`): under systemd to the journal (`journalctl -u <name>`), under the built-in Windows service to the Application Event Log with the service name as source, under WinSW to the wrapper's log directory. Records related to configuration and startup carry `code=DBB-CFG-0xx`.

| Message | Level | Meaning |
|---|---|---|
| `starting dbbridge peer node` / `admin server started` | INFO | Start. |
| `configuration warning` | WARN | A `check-config` warning, at start. |
| `failed to load configuration` | ERROR | Exit 78. |
| `failed to start node` | ERROR | `DBB-CFG-010`, exit 1. |
| `received termination request, initiating graceful shutdown ...` / `node exited cleanly` | INFO | Stop. |
| `fatal background failure; stopping the node` | ERROR | `DBB-CFG-011`, exit 1. |
| `shutdown teardown exceeded its budget; abandoning remaining cleanup` | ERROR | Teardown did not finish in 15 s. |
| `credentials reloaded` / `credential reload rejected ...` / `credential reload succeeded; the earlier reload error is resolved` | INFO / ERROR / INFO | §3. |
| `tokens from environment variables are read once per process; ...` | INFO | Lists the variables that need a restart to change. |
| `session terminated` | INFO | An ingress session ended (fields below). |
| `session recovered` | INFO | A recovery completed. |
| `session cleanup` / `session cleanup skipped` | INFO / WARN | Remote DELETE after a termination (`via_backup=true` when sent through a backup endpoint). |
| `recovery grace does not exceed the failover cooldown of the only usable carrier; ...` | WARN | See [Transport §4](transport.md#4-failure-detection-failover-and-recovery). |
| `ingress connection rejected: local budget exhausted` | WARN | A local client was refused. |
| `ingress listener is bound to a non-loopback address; ...` | WARN | |

**`session terminated` fields:** `session`, `role=ingress`, `peer`, `endpoint`, `owner` (egress instance), `epoch`, `transport`, `recovery_enabled`, `attempts`, `deadline` (recovery deadline; empty when no recovery was started), `outcome_unknown`, `reason`, `local_close_ms`, `fault_time`, `terminal_time`, `local_closed_time`. Before a session was created, `session`, `endpoint` and `transport` are empty.

**`reason` values** (log field and `dbb_session_events_total{reason}`):

| Reason | Meaning |
|---|---|
| `normal` | Both directions finished (FINs applied and acknowledged). |
| `shutdown` | This node is stopping. |
| `local_socket` | The local client socket failed (read, write or half-close error). |
| `timeout` | A deadline expired: create, first attach, recovery grace, backpressure. |
| `network` | Peer unreachable or carrier lost without a more specific cause. |
| `protocol` | Protocol violation or frame conflict. |
| `unauthorized` / `forbidden` | `401` / `403` (token not accepted; target not allowed or authorization revoked). |
| `dial_failed` | The egress could not reach the database (`502 DBB_DIAL_FAILED`); check `dbb_dial_failed_total` on the egress. |
| `draining` | The egress is shutting down (`503 DBB_DRAINING`). |
| `backend_lost` | The egress lost the database connection abnormally. |
| `failed` | The egress reported the session closed abnormally. |
| `offset`, `quota`, `barrier`, `buffer` | Acknowledgement offset rejected; quota exceeded; recovery write barrier not reached; buffer budget exhausted. |
| `success`, `failed`, `unknown`, `transport` | Outcomes of `resume`, `cleanup`, `fallback` and precheck cache events. |
| `instance`, `authentication` | Part of the label set; not emitted by this release. |
| `not_needed`, `skipped` | Cleanup events: no remote DELETE was required; cleanup could not run (for example shutdown deadline). |
| `other` | Any value outside the set. |

**`event` values:** `fault`, `terminate`, `resume`, `fallback`, `cleanup`, `cache_hit`, `cache_miss`, `cache_invalidate`, `precheck`, `attach`. `role` is `ingress` or `egress`; `from`/`to` are `ws`, `http-sse`, `none` or `other`. On the egress, `terminate` uses the session's terminal category: `normal`, `backend_lost`, `timeout`, `forbidden`, `buffer`, `protocol`, `failed`.

## 7. Peer protocol error codes

These codes appear in HTTP answers between nodes and in logs; a **database client never sees them** — it sees its TCP connection closed. Body: `{"code": "...", "message": "...", "retryable": bool, "session_closed": bool}`.

| Code | Status | Retryable | Meaning / action |
|---|---|---|---|
| `DBB_UNAUTHORIZED` | 401 | no | Token missing or not accepted, or session token invalid. Check that the ingress token matches an enabled identity on the egress. |
| `DBB_TARGET_FORBIDDEN` | 403 (410 as a session's terminal code) | no | Target not in the identity's `allowed_targets`, address outside `allowed_cidrs`, or authorization revoked by a reload. |
| `DBB_SESSION_NOT_FOUND` | 404 | no | Unknown or already discarded session; the client reconnects. |
| `DBB_BAD_FRAME` | 400 (404 for an unknown action, 405 for a wrong method) | no | Malformed request, headers or body; also a malformed WebSocket upgrade. Usually a proxy altering requests. |
| `DBB_BAD_DIGEST` | 400 | no | SSE frame does not match `X-DBB-SHA256`: data altered in transit. |
| `DBB_BAD_ACK` | 400 | no | Acknowledgement offset out of range. |
| `DBB_FRAME_TOO_LARGE` | 413 | no | SSE upstream frame larger than the negotiated `max_frame_size`. |
| `DBB_STALE_EPOCH` | 409 | no | Request for an older epoch of a recovered session. |
| `DBB_STREAM_EXISTS` | 409 | no | A carrier is already attached for this epoch. |
| `DBB_IDEMPOTENCY_CONFLICT` | 409 | no | Same request ID with different parameters. |
| `DBB_RESUME_IN_PROGRESS` | 409 | yes | Another resume of the session is running. |
| `DBB_INSTANCE_MISMATCH` | 409 | no | The egress process restarted; sessions cannot continue. Clients reconnect. |
| `DBB_NODE_MISMATCH` | 409 | no | The egress `node.id` differs from `peers[].expected_node_id`. Fix the configuration or the endpoint URL. |
| `DBB_PROTOCOL_MISMATCH` | 409 | no | Protocol version other than 1.4. |
| `DBB_RECOVERY_DISABLED` | 409 | no | Resume requested for a session created without recovery. |
| `DBB_RECOVERY_UNSUPPORTED` | 409 | no | Recovery requested but disabled on the egress. Align `recovery.enabled`. |
| `DBB_FRAME_CONFLICT` | 409 / 410 | no | Data or FIN contradicts what was already received; the session is closed. |
| `DBB_REPLAY_TOO_OLD` | 409 | no | Replayed frame is older than the retained receipts. |
| `DBB_WINDOW_EXCEEDED` | 409 | yes | Too many frames in flight. |
| `DBB_TRANSPORT_UNSUPPORTED` | 409 | no | Carrier not in `http.allowed_transports` (or WebSocket subprotocol missing). |
| `DBB_TRANSPORT_MISMATCH` | 409 | no | Request for a carrier other than the session's. |
| `DBB_SESSION_CLOSED` | 410 | no | Session ended. The message is `session terminated: <category>`; `normal` means both directions finished. |
| `DBB_BACKEND_LOST` | 410 | no | The database connection failed (reset or read error other than EOF). A normal EOF from the database is a FIN, not this code. |
| `DBB_RESUME_EXPIRED` | 410 | no | Recovery grace elapsed. |
| `DBB_QUOTA_EXCEEDED` | 429 | yes | A quota or budget is exhausted. The message names it: `peer session quota`, `session quota`, `session metadata budget`, `peer idempotency quota`, `peer creation concurrency`, `creation concurrency`, `peer probe budget`, `probe budget`, `peer precheck budget`, `peer recovery budget`, `recovery budget`, `request concurrency`, `request metadata`, `control metadata budget`. Usually with `Retry-After`. |
| `DBB_DIAL_FAILED` | 502 | no | The egress could not resolve or connect to the database. The result is cached for the create request ID. |
| `DBB_BUFFER_EXHAUSTED` | 503 (410 as a terminal code) | yes | Buffer or codec budget exhausted (`codec budget`); as terminal code, the session was ended for it. |
| `DBB_DRAINING` | 503 | yes | The egress is stopping. |
| `DBB_CREATE_PENDING` | 202 | yes | The same create is still in progress; retry with the same ID. |
| `DBB_RESUME_PENDING` | 202 | yes | The same resume is waiting for the write barrier; retry with the same ID. |
| `DBB_OFFSET_GAP`, `DBB_INTERNAL_ERROR`, `DBB_WS_HANDSHAKE_FAILED`, `DBB_WS_PROTOCOL_ERROR` | — | — | Reserved; not sent by this release. |

When the egress ends a session itself, it also sends the terminal code on the session's carrier as a final `error` control message ([Transport §4](transport.md#4-failure-detection-failover-and-recovery)).

## 8. Environment variables

| Variable | Used for |
|---|---|
| Names in `token_env` / `token_envs` | Tokens, read at start (and from the unchanged process environment on reload). |
| `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` (and lower-case forms) | Peer connections, only with `transport.use_environment_proxy: true`. |
| `NOTIFY_SOCKET` | systemd readiness notification (set by systemd). |
| `SOURCE_DATE_EPOCH`, `DBB_VERSION`, `DBB_COMMIT`, `DBB_PLATFORMS` | `scripts/build-release.sh` ([Installation](installation.md#3-building-from-source)). |
| `DBB_MYSQL_DSN`, `DBB_MARIADB_DSN` | Database integration tests (`go test -tags=database ./integration`). |

## 9. Configuration error codes

| Code | Meaning | Exit status |
|---|---|---|
| `DBB-CFG-001` | Configuration file unreadable, too large, not valid YAML, more than one document, or schema violation (unknown key, wrong type). | 78 |
| `DBB-CFG-002` | Invalid value or cross reference. | 78 |
| `DBB-CFG-003` | Credentials: credentials file, token file or token variable missing, unreadable or invalid; snapshot constraint (ACL target, quota sum, `credential_ref`, token shared by two identities). | 78 |
| `DBB-CFG-004` | TLS: `http.tls` certificate/key or `peers[].tls.ca_file` cannot be loaded. | 78 |
| `DBB-CFG-005` | Listen addresses invalid or conflicting. | 78 |
| `DBB-CFG-006` | Service credential environment: `--env-file` cannot be loaded, or a token variable needed by `install` is missing or malformed. | 78 |
| `DBB-CFG-010` | Node failed to start (for example an address already in use). | 1 |
| `DBB-CFG-011` | Fatal failure while running. | 1 |
| `DBB-CFG-020` | Credential reload rejected; previous credentials kept (log only; the node keeps running). The cause is in `cause_code`. | — |
| `DBB-CFG-030` | Service install/uninstall failed for another reason (systemd/SCM, permissions, existing service). | 1 |

## 10. Capacity planning

**Concurrent sessions** are limited by `limits.max_sessions`, per identity by `inbound_peers[].max_sessions`, and by the metadata budget ([Configuration §14](configuration.md#14-session-capacity-and-the-metadata-budget)). Each session uses two file descriptors on the egress (carrier and database) or ingress (client and carrier) plus pooled HTTP connections; the generated systemd unit sets `LimitNOFILE=max(8192, 4 × max_sessions + 1024)`.

**Memory.** The worst case is bounded by `global_buffer_limit` (payload) + `codec_buffer_limit` + `metadata_limit`, 320 MiB with defaults, plus runtime overhead. Per session, `buffer_per_direction` (4 MiB) in each direction is the ceiling, not a reservation.

**Rate of new sessions.** Session creation is idempotent: the egress keeps a record of every create request ID, and a tombstone for every closed session, for `idempotency_tombstone_ttl` (at least 10 minutes). These records live in a share of the metadata budget:

```
egress metadata = usable metadata (configuration §14), minus 1/8 on a bidirectional node
record budget   = max(egress metadata − remote session slots × per-session metadata,
                      egress metadata / 8)
remote session slots = max_sessions, minus max(1, max_sessions/4) on a bidirectional node
sustained creates/s ≈ record budget / (2 KiB × idempotency_tombstone_ttl in seconds)
```

With defaults (`max_sessions: 200`, `metadata_limit: 32MiB`, TTL 10 min) the record budget is about 11.3 MiB on an egress-only node and 12.3 MiB on a bidirectional node, which allows about **9.7** (egress-only) or **10.5** (bidirectional) new sessions per second sustained; short bursts above it are absorbed until the records fill up. With several enabled identities, each identity gets a share of the records (see [Transport §6](transport.md#6-memory-and-connection-budgets)), so one identity cannot block the others. Above its share an identity gets `429 DBB_QUOTA_EXCEEDED "peer idempotency quota"` (retryable).

Workloads that open many short connections (no client-side connection pool) should:

- raise `transport.metadata_limit` (the ceiling grows roughly linearly with the record budget), or
- lower `limits.max_sessions` to what is really needed (fewer reserved session slots leave more for records), and
- use a connection pool in the database client.

`idempotency_tombstone_ttl` cannot be lowered below 10 minutes.

## 11. Troubleshooting

| Symptom | Likely cause | What to check |
|---|---|---|
| Service exits at once with status 78 | Configuration or credential error | `journalctl -u <name>` / Event Log for `Error [DBB-CFG-0xx]`; run `dbbridge check-config -c <config>` as the service account. |
| systemd restarts the service every 5 s | Start failure (`DBB-CFG-010`), e.g. a port in use | Log `failed to start node`; `ss -ltnp`. |
| `install` fails with `cannot access ...` or `cannot traverse directory ...` | The service account cannot read a file | Permissions ([Security §8](security.md#8-service-account-and-file-permissions)). |
| Client connection closes immediately | Database unreachable from the egress, ACL, token | Ingress log `session terminated reason=...`; on the egress `dbb_dial_failed_total`, `allowed_targets`, `allowed_cidrs`. |
| `reason=unauthorized` | Token mismatch | Token files on both sides; reload state (`dbb_auth_credentials_stale`). |
| `reason=forbidden` | Target not allowed or revoked | `allowed_targets` of the identity on the egress; listener `target`. |
| Every session uses SSE although WebSocket is wanted | Proxy refuses or alters the upgrade | Proxy WebSocket settings ([Transport §7](transport.md#7-reverse-proxies-and-gateways)); `dbb_session_events_total{event="fallback"}`. |
| `dbb_endpoint_state{state="MISMATCHED"}` | Wrong node behind the URL, protocol violation, TLS failure | `expected_node_id`, certificate host name, proxy that rewrites WebSocket messages. |
| `429 DBB_QUOTA_EXCEEDED` in logs | Quota or budget exhausted (message names it) | §7 messages; `dbb_resource_in_use/limit`; §10. |
| `/readyz` 503 | Starting, draining, or fatal failure | Log around the time; `DBB-CFG-011`. |
| Sessions break during long idle periods | Proxy idle timeout below heartbeat, or keep-alive below 60 s | Proxy timeouts ([Transport §7](transport.md#7-reverse-proxies-and-gateways)). |
| Token change not applied | Reload rejected, or token from an environment variable | Log `DBB-CFG-020`; `dbb_auth_credentials_stale`; [Security §3](security.md#3-tokens-and-credential-sources). |
| `check-config` passes but the service rejects reloads | File not visible or readable for the service (sandbox, permissions), or a new variable name | Keep credential files next to the configuration; permissions; reinstall for new variable names. |
| `limits.max_sessions ... exceeds the process metadata budget` | Too many sessions for `metadata_limit` | Apply the suggested value ([Configuration §14](configuration.md#14-session-capacity-and-the-metadata-budget)). |
| `suspended_buffer_limit ... must be < global_buffer_limit` | Both set explicitly to conflicting values | Remove the explicit `suspended_buffer_limit` to use the derived default. |
| Stop takes long | Sessions draining | `transport.shutdown_timeout`; a second `SIGTERM` forces exit (130). |

**Backpressure diagnostics.** The egress answer to `/ack` always carries `data_tx_blocked`, `data_rx_blocked` and `backpressure_reason`: `none`, `local_socket_write` (the database socket is not accepting data fast enough), `data_channel_write` (the carrier towards the ingress is slow) or `global_buffer` (the process payload budget is full). Together with `dbb_backpressure_total` and `dbb_resource_wait_seconds` they show which side is slow.
