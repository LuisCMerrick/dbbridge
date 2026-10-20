**English** | [简体中文](README.zh-CN.md)

# DBBridge

DBBridge carries database connections across restrictive network boundaries — plant networks, cloud VPCs, branch offices, mixed leased-line and internet links — over plain HTTPS. A database client connects to a local TCP port; DBBridge forwards the byte stream to a peer node over **WebSocket** or **HTTP POST + Server-Sent Events**, and the peer connects to the real database. Traffic passes reverse proxies, gateways and firewalls that allow only HTTP(S).

DBBridge is protocol-transparent: it forwards bytes exactly, never parses or rewrites SQL and never caches results, so transactions, prepared statements and driver features behave as on a direct connection. It is used with MySQL, MariaDB and Microsoft SQL Server, and works with any TCP database protocol.

## Features

- **One binary, three roles** — ingress (local listeners), egress (inbound HTTP server and databases) or both, decided by the configuration.
- **Two carriers** — WebSocket by default; automatic fallback to HTTP POST + SSE on the same endpoint when a proxy cannot carry WebSocket.
- **Failover** — several endpoints per peer with priorities, background and on-demand health probes, cooldown of failed paths, failback for new sessions.
- **Session recovery** — a broken carrier is replaced without closing the database connection: both sides resume from the last acknowledged offsets within a fixed grace period. Without recovery, or when it fails, the client connection is closed immediately so the application can reconnect.
- **Security** — TLS, bearer-token identities, node identity check, per-identity target ACLs, address allow-lists for targets, per-identity quotas and budget shares.
- **Credential hot reload** — token files and credential settings are re-read on an interval, on `SIGHUP` / `systemctl reload`, or on a Windows service parameter-change request; rotation and revocation need no restart.
- **Bounded resources** — memory, session metadata, connections and concurrency are budgeted; overload is answered with explicit, retryable errors.
- **Operations** — `gen-config` for matched configuration pairs, `check-config` with stable error codes, service installation for systemd and Windows, loopback admin endpoint with health, readiness and Prometheus metrics.

## Architecture

```
 database client                                                     database
 (mysql, sqlcmd, app)                                       (MySQL / MariaDB / SQL Server)
        │ TCP 127.0.0.1:13306                                              ▲ TCP
        ▼                                                                  │
 ┌──────────────────────┐    HTTPS: WebSocket  or  POST + SSE    ┌─────────────────────┐
 │ ingress node         │ ─────────────────────────────────────▶ │ egress node         │
 │  listeners           │   /v1/peer/probe   /v1/sessions        │  http (listen, TLS) │
 │  peers / endpoints   │   /v1/transport/ws /v1/sessions/{id}/… │  auth (identities,  │
 │  failover, recovery  │   bearer token per request             │   ACLs, quotas)     │
 └──────────────────────┘                                        │  targets            │
                                                                 └─────────────────────┘
```

- **Ingress** accepts a client connection on a listener, picks the best usable endpoint of the peer (probing it and checking the peer's node ID), creates a session for the listener's target, and attaches a carrier.
- **Egress** authenticates the request by its bearer token, checks the identity's target ACL and quota, dials the database (`targets`, optionally restricted by `allowed_cidrs`), and relays data.
- **Carriers.** With `ws`, one WebSocket per session carries data in both directions. With `http-sse`, upstream frames are POSTed with a SHA-256 digest and downstream data arrives on an SSE stream. Control requests (acknowledgements, FIN, resume, close) use a separate connection pool, so they get through while the data path is congested.
- **Sessions and recovery.** Data in both directions is acknowledged by offset. When a carrier fails, a session with recovery enabled is suspended on the egress and resumed by the ingress through any usable endpoint and carrier within `transport.recovery.grace`, starting from the last acknowledged offsets — no byte is lost or duplicated. Sessions never move to another process: after a restart, clients reconnect.

Details: [Transport](docs/transport.md).

## Supported platforms

| | Binary | Service install |
|---|---|---|
| Linux amd64 / arm64 | ✓ | systemd |
| Windows amd64 | ✓ | Windows service (built in), or WinSW |

Building from source requires Go 1.27.2 or later. Nodes speak DBBridge protocol 1.4.

## Quick start

Try a bidirectional pair on one machine (each node also offers `127.0.0.1:3306` to the other):

```bash
dbbridge gen-config -o ./pair                  # peer-a.yaml, peer-b.yaml, shared.token
dbbridge check-config -c ./pair/peer-a.yaml
dbbridge check-config -c ./pair/peer-b.yaml
dbbridge run -c ./pair/peer-b.yaml &           # node B
dbbridge run -c ./pair/peer-a.yaml &           # node A
mysql -h 127.0.0.1 -P 13306 -u app -p          # via A to the database on B's side
curl -s http://127.0.0.1:18090/readyz          # A's admin endpoint
```

For two hosts, pass the real addresses (`--a-peer-url https://b.example.com:8443 --b-peer-url https://a.example.com:8443`, TLS certificates with `--a-tls-cert`/`--a-tls-key`, ...), copy `peer-a.yaml` + `shared.token` to A and `peer-b.yaml` + the same `shared.token` to B, then install the service:

```bash
sudo dbbridge install -c /etc/dbbridge/peer-a.yaml --service-user dbbridge --start
```

See [Examples](docs/examples.md) for complete deployments and [Installation](docs/installation.md) for the service setup.

## Documentation

| Document | Content |
|---|---|
| [Installation](docs/installation.md) | Release packages and checksums, building, systemd and Windows services, WinSW, file layout, upgrade, uninstall |
| [Configuration](docs/configuration.md) | Every configuration key, credentials file, tokens, derived defaults, constraints, what reloads |
| [Transport](docs/transport.md) | Carriers and fallback, negotiation, flow control, recovery, timeouts, budgets, reverse proxies, TLS |
| [Security](docs/security.md) | Hardening, token rotation, emergency revocation, ACLs, admin exposure, service account |
| [Operations](docs/operations.md) | CLI, exit codes, signals, reload, admin endpoints, metrics, logs, error codes, capacity, troubleshooting |
| [Examples](docs/examples.md) | MySQL, SQL Server, bidirectional `gen-config` pair, production pair with TLS |
| [CHANGELOG](CHANGELOG.md) | Release notes |

Sample configurations are in `configs/`, service definitions in `deploy/`.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

