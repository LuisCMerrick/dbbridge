**English** | [简体中文](installation.zh-CN.md)

# Installation, upgrade and removal

[README](../README.md) · **Installation** · [Configuration](configuration.md) · [Transport](transport.md) · [Security](security.md) · [Operations](operations.md) · [Examples](examples.md)

- [1. Supported platforms](#1-supported-platforms)
- [2. Release packages and checksums](#2-release-packages-and-checksums)
- [3. Building from source](#3-building-from-source)
- [4. Linux systemd service](#4-linux-systemd-service)
- [5. Windows service](#5-windows-service)
- [6. WinSW wrapper (alternative)](#6-winsw-wrapper-alternative)
- [7. File layout and permissions](#7-file-layout-and-permissions)
- [8. Upgrade](#8-upgrade)
- [9. Uninstall](#9-uninstall)

## 1. Supported platforms

| Platform | Binary | Service installation |
|---|---|---|
| Linux amd64, arm64 | yes | `dbbridge install` (systemd) |
| Windows amd64 | yes | `dbbridge install` (Windows SCM), or WinSW |

**Databases.** DBBridge forwards TCP byte streams without parsing them, so any TCP database protocol works; it is used with MySQL, MariaDB and Microsoft SQL Server. Notes:

- SQL Server named instances are located through the SQL Browser service over UDP 1434, which DBBridge does not forward: give the instance a fixed TCP port and use it as the target address.
- The database client connects to a local listener (for example `127.0.0.1:13306`). If the client verifies the database's TLS certificate, configure the expected server name in the client (for example the driver's host-name-in-certificate option), because the connection host is the loopback address.

**Protocol.** Nodes speak DBBridge protocol 1.5. How the settings of the two nodes meet is described in [Transport §2](transport.md#2-negotiated-values).

## 2. Release packages and checksums

Each release publishes:

| File | Content |
|---|---|
| `dbbridge-<version>-linux-amd64.tar.gz`, `...-linux-arm64.tar.gz` | Directory `dbbridge-<version>-<os>-<arch>/` with `dbbridge`, `README.md`, `README.zh-CN.md`, `CHANGELOG*`, `docs/`, `configs/`, `deploy/`, `third_party/` (module list and license files of the linked dependencies) and `LICENSE` |
| `dbbridge-<version>-windows-amd64.zip` | Same content with `dbbridge.exe` |
| `dbbridge-linux-amd64`, `dbbridge-linux-arm64`, `dbbridge-windows-amd64.exe` | The bare binaries |
| `BUILD-INFO.txt` | Version, source identity, source hash, build time, `SOURCE_DATE_EPOCH`, Go toolchain |
| `SHA256SUMS` | SHA-256 of every file above, including `BUILD-INFO.txt` |

Releases tagged with a suffix (for example `v1.5.0-rc1`) are marked as pre-releases.

Verify before use:

```bash
sha256sum --check --ignore-missing SHA256SUMS
tar xzf dbbridge-<version>-linux-amd64.tar.gz
./dbbridge-<version>-linux-amd64/dbbridge version
```

On Windows: `Get-FileHash .\dbbridge-<version>-windows-amd64.zip -Algorithm SHA256` and compare with the line in `SHA256SUMS`.

Builds are reproducible: the same source tree, Go toolchain and `SOURCE_DATE_EPOCH` produce identical files and checksums.

## 3. Building from source

Requires Go **1.27.3** or later (from `go.mod`). No cgo is needed.

```bash
go build -trimpath -o bin/dbbridge ./cmd/dbbridge
./bin/dbbridge version
```

Tests and checks:

```bash
go vet ./...
go test ./... -count=1
go test -race ./... -count=1            # needs cgo (a C toolchain)
bash scripts/verify.sh                  # everything CI runs, including sample check-config
DBB_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/dbbridge_test' \
  go test -tags=database ./integration  # optional; uses an isolated test database
```

Release artifacts:

```bash
bash scripts/build-release.sh dist
```

| Variable | Default | Effect |
|---|---|---|
| `DBB_VERSION` | `1.5.<yy.mm.dd>` of the source date (UTC) | Version embedded in the binary and package names |
| `DBB_COMMIT` | `source-<sha256 of the sources>` | Source identity shown by `dbbridge version` |
| `SOURCE_DATE_EPOCH` | commit time of `HEAD` | Build date and archive timestamps |
| `DBB_PLATFORMS` | `linux/amd64 linux/arm64 windows/amd64` | Space-separated `GOOS/GOARCH` list; other targets are unsupported |

CI runs on every push and pull request: formatting, `go vet` for Linux and Windows (amd64), the test suite (also with `-race`) on Linux, the service, file and CLI tests on Windows, and `check-config` for every sample.

## 4. Linux systemd service

### 4.1 Prepare the host

```bash
sudo install -m 0755 dbbridge-<version>-linux-amd64/dbbridge /usr/local/bin/dbbridge
sudo useradd --system --no-create-home --shell /usr/sbin/nologin dbbridge
sudo install -d -m 0750 -o root -g dbbridge /etc/dbbridge /etc/dbbridge/tokens /etc/dbbridge/tls
# copy peer.yaml (and auth.yaml, token files, TLS files) into /etc/dbbridge, then:
sudo chown root:dbbridge /etc/dbbridge/*.yaml /etc/dbbridge/tokens/* /etc/dbbridge/tls/*
sudo chmod 0640 /etc/dbbridge/*.yaml /etc/dbbridge/tokens/* /etc/dbbridge/tls/*
sudo -u dbbridge /usr/local/bin/dbbridge check-config -c /etc/dbbridge/peer.yaml
```

With a `gen-config` pair, copy `peer-a.yaml` and `shared.token` to node A and `peer-b.yaml` and the same `shared.token` to node B. Generated files are 0600 and owned by the user who ran `gen-config`; adjust owner and mode as above.

### 4.2 Install

```bash
sudo dbbridge install -c /etc/dbbridge/peer.yaml --service-user dbbridge --dry-run   # review the plan
sudo dbbridge install -c /etc/dbbridge/peer.yaml --service-user dbbridge --start
systemctl status dbbridge
journalctl -u dbbridge -f
```

`install` (run as root, with systemd as the system manager):

1. Loads and validates the configuration exactly like `check-config` (exit 78 on error).
2. Checks, with the service account's uid and groups, that the configuration, credentials file, token files, TLS and CA files are readable, every directory on the way is traversable, and the executable is executable.
3. If the configuration takes tokens from environment variables, writes them to `<config dir>/<service-name>.service-env.json` (0600, owned by the service account) and passes it with `--env-file`. The variables must be set in the environment of the `install` command (`sudo -E` or `sudo VAR=... dbbridge install ...`). An existing snapshot with different values is refused; update or delete it first.
4. Writes `/etc/systemd/system/<service-name>.service` (marked `# Managed by DBBridge`), runs `daemon-reload` and `enable`, and with `--start` starts it. An existing service of the same name is refused.

The executable written into the unit is the path you invoked (symbolic links are kept, a warning names the link target), or `--exec-path <path>`. Use a stable path such as `/usr/local/bin/dbbridge`.

`--service-name <name>` installs several instances side by side (each with its own configuration directory).

### 4.3 The generated unit

| Setting | Value |
|---|---|
| `Type=notify`, `NotifyAccess=main` | `READY=1` after start, `STOPPING=1` at drain |
| `User` | `--service-user` (default `root`, with a warning) |
| `WorkingDirectory` | directory of the configuration |
| `ExecStart` | `<exe> run --config <abs path> --service-name <name> --log-level <l> --log-format <f> [--env-file <snapshot>]` |
| `ExecReload` | `/bin/kill -HUP $MAINPID` (`systemctl reload` re-reads credentials) |
| `Restart=on-failure`, `RestartSec=5s`, `RestartPreventExitStatus=78` | configuration errors are not retried |
| `StartLimitIntervalSec=300`, `StartLimitBurst=10`, `TimeoutStartSec=30s` | |
| `TimeoutStopSec` | `max(45s, transport.shutdown_timeout + 30s)` — 60 s with defaults |
| `LimitNOFILE` | `max(8192, 4 × limits.max_sessions + 1024)` |
| `CapabilityBoundingSet`, `AmbientCapabilities` | empty; `CAP_NET_BIND_SERVICE` only if `http.listen`, `admin.listen` or a listener uses a port below 1024 |
| Sandboxing | `NoNewPrivileges`, `UMask=0077`, `ProtectSystem=strict`, `ProtectHome=true` (`read-only` if the executable, configuration or a referenced file is under `/home`, `/root` or `/run/user`), `PrivateTmp=true` (`false` if one is under `/tmp` or `/var/tmp`), `PrivateDevices`, `ProtectKernelTunables`, `ProtectKernelModules`, `ProtectKernelLogs`, `ProtectControlGroups`, `ProtectClock`, `ProtectHostname`, `RestrictNamespaces`, `RestrictRealtime`, `RestrictSUIDSGID`, `LockPersonality`, `MemoryDenyWriteExecute`, `SystemCallArchitectures=native`, `SystemCallFilter=@system-service`, `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX` |

All values are computed when you run `install`. After changing `transport.shutdown_timeout`, `limits.max_sessions`, a port below 1024, the location of the executable, configuration or credential files, or the set of token environment variables, run `uninstall` and `install` again.

### 4.4 Operating the service

```bash
sudo systemctl reload dbbridge     # re-read credentials now (also every auth.reload_interval)
sudo systemctl restart dbbridge    # apply any other configuration change; drains first
sudo systemctl stop dbbridge       # drain (shutdown_timeout) and stop
```

### 4.5 Static unit

`deploy/systemd/dbbridge.service` is an equivalent hand-written unit for `User=dbbridge`, `/etc/dbbridge/peer.yaml` and `/usr/local/bin/dbbridge`, with an optional `EnvironmentFile=-/etc/dbbridge/environment` (0600 `root:root`). Copy it to `/etc/systemd/system/`, adjust paths, raise `TimeoutStopSec` together with `transport.shutdown_timeout` and `LimitNOFILE` with `limits.max_sessions`, then `systemctl daemon-reload && systemctl enable --now dbbridge`. `dbbridge uninstall` does not remove a hand-written unit.

## 5. Windows service

From an elevated PowerShell:

```powershell
.\dbbridge.exe check-config -c C:\dbbridge\peer.yaml
icacls C:\dbbridge\peer.yaml    /grant "NT AUTHORITY\LOCAL SERVICE:(R)"
icacls C:\dbbridge\shared.token /grant "NT AUTHORITY\LOCAL SERVICE:(R)"
.\dbbridge.exe install -c C:\dbbridge\peer.yaml --service-user LocalService --dry-run
.\dbbridge.exe install -c C:\dbbridge\peer.yaml --service-user LocalService --start
Get-Service dbbridge
sc.exe control dbbridge paramchange      # re-read credentials now
Restart-Service dbbridge                 # apply other configuration changes
```

- Service `<service-name>`, display name `DBBridge (<service-name>)`, automatic start, depends on `Tcpip`. The account is `LocalSystem` by default (with a warning), or `LocalService`/`NetworkService` via `--service-user`.
- `install` checks the account's read access to every referenced file through its DACL and prints the `icacls` command for each file it cannot confirm. Files written by `gen-config` are readable only by their creator, Administrators and SYSTEM, so grant access as shown. A new environment snapshot is created with read access for the selected service account.
- Logs go to the Application Event Log with the service name as source. `install` refuses a service name whose event source already exists and was not created by DBBridge; `uninstall` removes only a source DBBridge created.
- Stop, system shutdown and pre-shutdown drain the node; the pre-shutdown timeout is `transport.shutdown_timeout + 30 s`.
- A configuration or credential error stops the service with service-specific exit code 78 (`sc.exe query dbbridge` shows `SERVICE_EXIT_CODE : 78`) and an Event Log error; it is not restarted. Fix the configuration and start the service. Other failures are restarted after 5 s (failure count reset after one day).
- `sc.exe control <name> paramchange` triggers a credential reload, like `SIGHUP` on Linux.

## 6. WinSW wrapper (alternative)

`deploy/windows/dbbridge.xml` is a sample for the WinSW service wrapper (download WinSW from its official release and verify its checksum). Differences from the built-in service:

- The sample service ID `DBBridge` and the built-in default `dbbridge` name the same SCM service (names are case-insensitive): use one or the other for a given name.
- Logs are written to `%BASE%\logs`, not the Event Log. Grant the service account Modify on that directory and read access to the configuration and secrets.
- WinSW cannot distinguish exit codes: the sample restarts after 5 s, then after 30 s, then stays stopped (failure count reset after one hour).
- `<stoptimeout>` is fixed in the XML (60 s); raise it with `transport.shutdown_timeout` (at least `shutdown_timeout + 30 s`).
- `paramchange` is not available; credentials are re-read every `auth.reload_interval`.

## 7. File layout and permissions

Recommended Linux layout (Windows: one directory such as `C:\dbbridge`, with read grants for the service account):

| Path | Owner / mode | Notes |
|---|---|---|
| `/usr/local/bin/dbbridge` | `root:root` 0755 | Stable path written into the unit. |
| `/etc/dbbridge/` | `root:dbbridge` 0750 | Configuration directory = working directory = base of relative paths. |
| `/etc/dbbridge/peer.yaml` | `root:dbbridge` 0640 | Main configuration. |
| `/etc/dbbridge/auth.yaml` | `root:dbbridge` 0640 | Credentials file, if used. |
| `/etc/dbbridge/tokens/*.token` | `root:dbbridge` 0640 | Token files. Keep every credential file under the configuration directory. |
| `/etc/dbbridge/tls/*.crt`, `*.key` | `root:dbbridge` 0640 | Server certificate and key; CA bundles. |
| `/etc/dbbridge/<service-name>.service-env.json` | service account, 0600 | Only when tokens come from environment variables. Contains the secrets in plain text; not removed by `uninstall`. |
| `/etc/systemd/system/<service-name>.service` | `root:root` 0644 | Generated unit. |

`gen-config` also leaves no temporary files behind on success; files named `.dbbridge-pair-*` or `.dbbridge-config-*` in an output directory are leftovers of an interrupted run and can be deleted.

## 8. Upgrade

Sessions are bound to the running process. Restarting a node ends the sessions that pass through it; database clients see their connection closed and reconnect. The two sides of a pair are upgraded one at a time.

For each node:

1. Read the release notes in the [CHANGELOG](../CHANGELOG.md).
2. Download the package and verify it against `SHA256SUMS` (§2).
3. Validate the existing configuration with the **new** binary, as the service account:
   ```bash
   sudo -u dbbridge ./dbbridge-<version>-linux-amd64/dbbridge check-config -c /etc/dbbridge/peer.yaml
   ```
4. Replace the binary at its installed path: `sudo install -m 0755 ./dbbridge-<version>-linux-amd64/dbbridge /usr/local/bin/dbbridge` (Windows: `Stop-Service dbbridge`, copy `dbbridge.exe`). If you keep versioned directories behind a symbolic link, switch the link; the unit starts whatever the link points to.
5. Apply it:
   - normally `sudo systemctl restart dbbridge` (Windows: `Start-Service dbbridge`). The node drains for up to `transport.shutdown_timeout`, then starts the new binary;
   - if the release notes mention a change to the generated service definition, or you changed a setting the definition is computed from (§4.3), run `sudo dbbridge uninstall` and `sudo dbbridge install ...` with the same options instead.
6. Verify: `curl -s http://127.0.0.1:<admin port>/readyz` returns `ready`; the log shows `starting dbbridge peer node` without errors; `dbb_endpoint_state` of the peers returns to `HEALTHY`; clients reconnect.
7. Upgrade the other node the same way.

To roll back, put the previous binary back and restart (step 4–5).

## 9. Uninstall

```bash
sudo dbbridge uninstall --service-name dbbridge --dry-run   # check
sudo dbbridge uninstall --service-name dbbridge
```

```powershell
.\dbbridge.exe uninstall --service-name dbbridge
```

`uninstall` stops the service (waiting for the drain), disables and removes it, and on Windows removes the Event Log source. Only services installed by `dbbridge install` are removed. The configuration, credentials, TLS files and the service environment snapshot are kept; delete them yourself once you have no further use for them — the snapshot (`<service-name>.service-env.json`) contains secrets. Remove the binary and the `dbbridge` account last.
