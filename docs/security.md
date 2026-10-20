**English** | [简体中文](security.zh-CN.md)

# Security

[README](../README.md) · [Installation](installation.md) · [Configuration](configuration.md) · [Transport](transport.md) · **Security** · [Operations](operations.md) · [Examples](examples.md)

- [1. Security model](#1-security-model)
- [2. Hardening checklist](#2-hardening-checklist)
- [3. Tokens and credential sources](#3-tokens-and-credential-sources)
- [4. Token rotation](#4-token-rotation)
- [5. Emergency revocation](#5-emergency-revocation)
- [6. Target ACLs and `allowed_cidrs`](#6-target-acls-and-allowed_cidrs)
- [7. Admin endpoint exposure](#7-admin-endpoint-exposure)
- [8. Service account and file permissions](#8-service-account-and-file-permissions)

## 1. Security model

- **Peer authentication.** Every request from an ingress to an egress carries a bearer token (`Authorization: Bearer ...`). The egress maps the token to an inbound identity (`auth.inbound_peers[].id`); the token alone decides the identity. Requests on an existing session also carry a per-session token issued by the egress at creation.
- **Node identity.** The ingress checks the egress's `node.id` against `peers[].expected_node_id` on every probe, and binds every session to the egress process instance. A session never continues on a different node or process.
- **Authorization.** An identity may only open sessions to the target IDs in its `allowed_targets`. Callers name a target ID; the database address is configured only on the egress (`targets`), optionally restricted by `allowed_cidrs`.
- **Local listeners are unauthenticated.** Anyone who can connect to a listener reaches the remote database with this node's credentials. Database authentication still applies end to end; DBBridge forwards bytes and never parses or rewrites them.
- **Transport confidentiality** comes from TLS (on the node or on a reverse proxy). Plaintext across a network requires explicit opt-in keys.

## 2. Hardening checklist

1. **TLS on every network hop.** Either `http.tls` on the egress, or `http.listen` on loopback behind a local TLS-terminating proxy. Keep `peers[].tls.verify: true` (the default); use `tls.ca_file` for a private CA. Do not set `allow_insecure_http`, `http.allow_insecure_listen`, or `tls.allow_insecure_skip_verify` outside a lab.
2. **One token per direction and identity.** `gen-config` uses one shared token for both directions of a pair for convenience; for production, give each direction its own token (see the production example in [Examples](examples.md#4-production-pair-with-tls-and-a-credentials-file)). Tokens: 32 random bytes (`openssl rand -hex 32`).
3. **Token files, not environment variables**, so that tokens can be rotated without a restart (§3).
4. **Least-privilege ACLs.** List only the needed targets in `allowed_targets`; set `allowed_cidrs` on every target, and always on host-name targets.
5. **Loopback listeners.** Bind `listeners[].listen` to `127.0.0.1` (or firewall the port).
6. **Admin on loopback** (§7).
7. **Dedicated service account** with read-only access to configuration and secrets (§8). Keep secrets at 0600, or 0640 with a service group.
8. **Quotas.** Set `inbound_peers[].max_sessions` per identity so that one peer cannot take every session; keep the inbound sum below `limits.max_sessions` on bidirectional nodes.
9. **Validate before deploying.** Run `dbbridge check-config` on the target host as the service account and treat every `Warning:` as a finding.
10. **Monitor credentials.** Alert on `dbb_auth_credentials_stale == 1` and on log records with `code=DBB-CFG-020` (a rejected reload keeps the previous credentials in effect).

## 3. Tokens and credential sources

| Source | Where | Re-read while running |
|---|---|---|
| Token file in `auth.inbound_peers[].token_files` / `peers[].token_file` (main configuration) | Main configuration | Yes, every `auth.reload_interval`, on `SIGHUP` / `systemctl reload`, and on Windows `paramchange` |
| Token file in a credentials file (`inbound_peers[].token_files`, `outbound_peers[].token_file`) | Credentials file | Yes, as above |
| Environment variable (`token_envs`, `token_env`) | Process environment | No: a running process never sees a changed variable; restart to change it |

Under an installed service, environment-variable tokens come from the service environment snapshot written at install time (`<config dir>/<service-name>.service-env.json`, mode 0600). Changing them means updating that file and restarting; a new variable name needs a reinstall. The snapshot contains the secrets in plain text and is not removed by `uninstall`.

What a reload can change and what needs a restart is listed in [Configuration → What is re-read while running](configuration.md#16-what-is-re-read-while-running).

Sessions are bound to the identity, not to the token text: replacing a token does not interrupt established sessions. After an old token is removed, every later request still carrying it is refused with `401`.

## 4. Token rotation

The procedure keeps every session running. It works for inline credentials (including `gen-config` output) and for a credentials file, as long as the tokens come from files. Wait for the reload on each step: at most `auth.reload_interval` (5 s by default), or trigger it with `systemctl reload <name>` (Linux) / `sc.exe control <name> paramchange` (Windows), and confirm it in the log (`credentials reloaded`).

**Rotating the token that node A uses to reach node B** (B is the egress, its identity for A is `A`):

1. On **B**, create the new token file next to the old one (same owner and mode, for example 0640 `root:dbbridge`), and add it to the identity:
   ```yaml
   inbound_peers:
     - id: A
       token_files: [tokens/a-to-b.token, tokens/a-to-b.next.token]
   ```
   Wait for the reload on B. Both tokens are now accepted.
2. On **A**, copy the new token file and point the outbound credential at it: `peers[].token_file: tokens/a-to-b.next.token` (inline) or `outbound_peers[].token_file` (credentials file). Wait for the reload on A. A uses the new token for every following request, including those of established sessions.
3. On **B**, remove the old file from `token_files` and wait for the reload. Then delete the old token file.

For a `gen-config` pair (one shared token used in both directions), do steps 1–3 on both nodes at the same time with a new shared file: add `new.token` to `auth.inbound_peers[].token_files` on both nodes, then set `peers[].token_file: new.token` on both, then remove `shared.token` from `token_files` on both.

An in-place rewrite of a token file is also picked up on the next reload; use it only when a short window of `401` answers is acceptable, because the two nodes do not reload at the same instant.

**Environment-variable tokens** cannot be rotated without a restart. List both variables (`token_envs: [TOKEN_CURRENT, TOKEN_NEXT]`) on the egress and restart it, switch the ingress variable and restart it, then remove the old variable from the egress and restart it again. Each restart ends the sessions of the restarted node. Prefer moving to token files.

If a reload is rejected (log `credential reload rejected ... code=DBB-CFG-020`, metric `dbb_auth_credentials_stale 1`), the previous credentials stay in effect. Fix the cause named in `err`/`cause_code` and reload again.

## 5. Emergency revocation

Removing a token alone stops new requests with that token, but it does not close sessions that are already established. To cut off an identity completely:

1. On the egress, set the identity to `enabled: false` (or delete the identity) in `auth.inbound_peers` or the credentials file:
   ```yaml
   inbound_peers:
     - id: plant-a
       enabled: false
       token_files: [tokens/plant-a.token]   # not read while disabled
       allowed_targets: [mysql-primary]
   ```
   A disabled identity's token sources are not read, so you may delete or move the compromised token file in the same change.
2. Trigger the reload: `sudo systemctl reload <name>` (Linux), `sc.exe control <name> paramchange` (Windows), or wait `auth.reload_interval`.
3. Verify:
   - the log shows `credentials reloaded`, not `credential reload rejected`;
   - `dbb_auth_credentials_stale` is `0`;
   - the identity's sessions were closed: they end with `DBB_TARGET_FORBIDDEN` (`dbb_session_events_total{role="egress",event="terminate",reason="forbidden"}` increases on the egress), and the ingress closes the matching local client connections (log `session terminated`).
4. If the reload was rejected, the identity is still enabled with its old token. The whole candidate is rejected when any part of it is invalid, including parts unrelated to the revoked identity. Fix the error named in the log and reload again; as a last resort, stop the egress.
5. Afterwards, remove the identity or issue a new token (§4) and review the database accounts that were reachable through it.

Narrower revocations work the same way: removing a target from an identity's `allowed_targets` closes that identity's sessions to that target on reload. Lowering `max_sessions` only limits new sessions; it does not close existing ones.

## 6. Target ACLs and `allowed_cidrs`

- `allowed_targets` lists target IDs per identity; every other target is refused with `403 DBB_TARGET_FORBIDDEN`.
- `allowed_cidrs` restricts the addresses a target may resolve to and be dialed at. Set it on every target. For a host-name target without it, `check-config` warns, because a DNS change (or rebinding attack) could make the egress dial an arbitrary internal address. `gen-config` writes the exact IP (`/32` or `/128`) for IP targets and requires `--a-allow-cidr`/`--b-allow-cidr` for host-name targets.
- A name that resolves only to addresses outside `allowed_cidrs` is refused (`403`); a resolver failure is reported as `502 DBB_DIAL_FAILED`.

## 7. Admin endpoint exposure

The admin server (`/healthz`, `/readyz`, `/metrics`) has no TLS and no authentication. `admin.listen` must be a loopback address unless `admin.allow_remote: true` is set; that produces a warning. `gen-config` and the shipped samples enable admin on loopback. To scrape metrics from another host, prefer a local agent or a TLS-terminating proxy with authentication; if you must bind a non-loopback address, restrict it with a host firewall. The admin server is never served on the peer `http.listen` port.

## 8. Service account and file permissions

**Linux.** `dbbridge install` runs the service as root by default, but with an empty capability bounding set: root cannot bypass file permissions, so the configuration, credentials and their directories must be readable by uid 0 through their mode bits. Prefer a dedicated account:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin dbbridge
sudo chown -R root:dbbridge /etc/dbbridge
sudo chmod 0750 /etc/dbbridge /etc/dbbridge/tokens
sudo chmod 0640 /etc/dbbridge/*.yaml /etc/dbbridge/tokens/*.token /etc/dbbridge/tls/*.key
sudo dbbridge install -c /etc/dbbridge/peer.yaml --service-user dbbridge --dry-run
```

`install` checks with the account's uid and groups that every referenced file is readable and every directory on the way is traversable, and fails with the path if not. The generated unit adds systemd sandboxing (`ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, `NoNewPrivileges`, an empty capability set or only `CAP_NET_BIND_SERVICE` for ports below 1024, a system-call filter and more; see [Installation](installation.md#4-linux-systemd-service)). `ProtectHome` and `PrivateTmp` are computed from the files referenced at install time, so keep every credential file next to the configuration (for example under `/etc/dbbridge`); `check-config` warns about credential files under `/home`, `/root`, `/run/user`, `/tmp` or `/var/tmp`. A token file added later must also be readable by the service account; otherwise the reload is rejected with `DBB-CFG-020`.

**Windows.** The service runs as LocalSystem by default (with a warning). Prefer `--service-user LocalService` (or `NetworkService`) and grant it read access to the configuration, credentials and TLS files, for example `icacls C:\dbbridge\peer.yaml /grant "NT AUTHORITY\LOCAL SERVICE:(R)"`. `install` checks the access and prints the `icacls` command for every file it cannot confirm. Files written by `gen-config` have a protected DACL (creator, Administrators, SYSTEM), so the grant is needed.

**Secrets at rest.** `gen-config` writes token and configuration files 0600 (protected DACL on Windows). The environment snapshot must be a regular file without group or other permissions, owned by the service account or root; otherwise the service refuses to start (exit 78). Never commit `*.token` or `*.service-env.json` files.
