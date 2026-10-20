**English** | [简体中文](examples.zh-CN.md)

# Example deployments

[README](../README.md) · [Installation](installation.md) · [Configuration](configuration.md) · [Transport](transport.md) · [Security](security.md) · [Operations](operations.md) · **Examples**

Every configuration on this page passes `dbbridge check-config` with the token files, variables and certificates the example names. Host names use the reserved `example.com` domain; replace them with your own.

- [1. One-way MySQL: plant → headquarters](#1-one-way-mysql-plant--headquarters)
- [2. Microsoft SQL Server with node TLS and token files](#2-microsoft-sql-server-with-node-tls-and-token-files)
- [3. Bidirectional pair with `gen-config`](#3-bidirectional-pair-with-gen-config)
- [4. Production pair with TLS and a credentials file](#4-production-pair-with-tls-and-a-credentials-file)

## 1. One-way MySQL: plant → headquarters

<!-- example:standalone-pair -->

Node `plant-a` (ingress) gives local applications `127.0.0.1:13306`; node `hq-b` (egress) forwards to MySQL on its own host. `hq-b` listens on loopback behind a local TLS-terminating reverse proxy on `https://hq.example.com` (see [Transport §7](transport.md#7-reverse-proxies-and-gateways) for the proxy configuration). This minimal example takes the token from an environment variable; for tokens that can be rotated without a restart use token files as in examples 2–4.

```yaml
# peer-b.yaml (hq-b, egress)
node:
  id: hq-b

http:
  enabled: true
  listen: "127.0.0.1:18080"        # the TLS proxy forwards https://hq.example.com here

admin:
  enabled: true
  listen: "127.0.0.1:18090"

targets:
  mysql-primary:
    address: "127.0.0.1:3306"
    allowed_cidrs: ["127.0.0.1/32"]

auth:
  inbound_peers:
    - id: plant-a
      token_envs: [DBB_TOKEN_SECRET]
      allowed_targets: [mysql-primary]
      max_sessions: 100
```

```yaml
# peer-a.yaml (plant-a, ingress)
node:
  id: plant-a

admin:
  enabled: true
  listen: "127.0.0.1:18090"

peers:
  - id: hq-b
    expected_node_id: hq-b
    credential_ref: hq-b
    token_env: DBB_TOKEN_SECRET
    endpoints:
      - id: primary
        url: "https://hq.example.com"   # certificate must match this name
        priority: 1

listeners:
  - id: mysql-local
    listen: "127.0.0.1:13306"
    peer: hq-b
    target: mysql-primary
```

<!-- /example:standalone-pair -->

Run it:

```bash
# generate one token and give the same value to both nodes through a secure channel
export DBB_TOKEN_SECRET="$(openssl rand -hex 32)"

dbbridge check-config -c peer-b.yaml      # on hq-b
dbbridge run -c peer-b.yaml

dbbridge check-config -c peer-a.yaml      # on plant-a
dbbridge run -c peer-a.yaml

mysql -h 127.0.0.1 -P 13306 -u app -p     # on plant-a
```

## 2. Microsoft SQL Server with node TLS and token files

The egress terminates TLS itself on port 8443 and forwards to a SQL Server instance on its network. The ingress listens on `127.0.0.1:11433` so it does not collide with a local SQL Server. Use a fixed TCP port for the SQL Server instance (the SQL Browser service is not forwarded).

Files on `hq-b`: `/etc/dbbridge/peer.yaml`, `/etc/dbbridge/tls/hq.crt`, `/etc/dbbridge/tls/hq.key`, `/etc/dbbridge/tokens/plant-a.token`. Files on `plant-a`: `/etc/dbbridge/peer.yaml`, `/etc/dbbridge/tokens/plant-a.token` (same content).

```yaml
# hq-b/peer.yaml (egress)
node:
  id: hq-b

http:
  enabled: true
  listen: "0.0.0.0:8443"
  tls:
    enabled: true
    cert_file: tls/hq.crt           # relative to this file's directory
    key_file: tls/hq.key

admin:
  enabled: true
  listen: "127.0.0.1:18090"

targets:
  mssql-primary:
    address: "192.168.1.200:1433"
    allowed_cidrs: ["192.168.1.200/32"]

auth:
  inbound_peers:
    - id: plant-a
      token_files: [tokens/plant-a.token]
      allowed_targets: [mssql-primary]
      max_sessions: 100
```

```yaml
# plant-a/peer.yaml (ingress)
node:
  id: plant-a

admin:
  enabled: true
  listen: "127.0.0.1:18090"

peers:
  - id: hq-b
    expected_node_id: hq-b
    credential_ref: hq-b
    token_file: tokens/plant-a.token
    endpoints:
      - id: primary
        url: "https://hq.example.com:8443"
        priority: 1
    # tls:
    #   ca_file: tls/company-ca.pem   # when hq.crt is issued by a private CA

listeners:
  - id: mssql-local
    listen: "127.0.0.1:11433"
    peer: hq-b
    target: mssql-primary
```

```bash
openssl rand -hex 32 > tokens/plant-a.token && chmod 0640 tokens/plant-a.token   # copy to both nodes
dbbridge check-config -c /etc/dbbridge/peer.yaml
sqlcmd -S 127.0.0.1,11433 -U app                                                # on plant-a
```

## 3. Bidirectional pair with `gen-config`

Each node offers its own database to the other and reaches the other node's database through a local listener. `gen-config` writes a matched pair and a shared token in one step. Here A serves SQL Server and B serves MySQL; both terminate TLS on port 8443 with certificates that already exist at `/etc/dbbridge/tls/`.

```bash
dbbridge gen-config -o ./pair \
  --a-http-listen 0.0.0.0:8443 --b-http-listen 0.0.0.0:8443 \
  --a-tls-cert /etc/dbbridge/tls/a.crt --a-tls-key /etc/dbbridge/tls/a.key \
  --b-tls-cert /etc/dbbridge/tls/b.crt --b-tls-key /etc/dbbridge/tls/b.key \
  --a-peer-url https://b.example.com:8443 \
  --b-peer-url https://a.example.com:8443 \
  --a-target-id mssql --a-target-address 127.0.0.1:1433 \
  --b-target-id mysql --b-target-address 127.0.0.1:3306
dbbridge check-config -c ./pair/peer-a.yaml
dbbridge check-config -c ./pair/peer-b.yaml
```

Result:

| | Node A (`peer-a.yaml`) | Node B (`peer-b.yaml`) |
|---|---|---|
| Local listener | `127.0.0.1:13306` → B's `mysql` | `127.0.0.1:13307` → A's `mssql` |
| Inbound HTTPS | `0.0.0.0:8443` | `0.0.0.0:8443` |
| Admin | `127.0.0.1:18090` | `127.0.0.1:18091` |
| Database | `mssql` = `127.0.0.1:1433` | `mysql` = `127.0.0.1:3306` |
| Inbound quota | 100 of 200 sessions | 100 of 200 sessions |

Deploy `peer-a.yaml` and `shared.token` to A, `peer-b.yaml` and the same `shared.token` to B, each in the same directory. TLS paths are written as absolute paths; keep the certificate and key at those paths on each node. Repeat `--a-peer-url`/`--b-peer-url` to add failover endpoints. Without the TLS and URL options the pair runs on one machine on loopback, for a quick test.

The pair uses one token for both directions and inline credentials; token files are re-read on reload, so the token can be rotated without a restart ([Security §4](security.md#4-token-rotation)). For separate tokens per direction, use the layout of example 4.

## 4. Production pair with TLS and a credentials file

The layout of `configs/production/`: each node terminates TLS on 8443, has several endpoints for the other node, keeps credentials in a separate file with **one token per direction**, and serves its database to the other node. Files under `/etc/dbbridge/` (`root:dbbridge`, 0640; directories 0750):

| Node A | Node B |
|---|---|
| `peer-a.yaml`, `auth-a.yaml` | `peer-b.yaml`, `auth-b.yaml` |
| `tls/a.crt`, `tls/a.key` | `tls/b.crt`, `tls/b.key` |
| `tokens/a-to-b.token`, `tokens/b-to-a.token` | `tokens/a-to-b.token`, `tokens/b-to-a.token` |

`a-to-b.token` is what A sends to B; `b-to-a.token` is what B sends to A. Both nodes have both files.

```yaml
# peer-a.yaml
node:
  id: A
http:
  enabled: true
  listen: "0.0.0.0:8443"
  tls:
    enabled: true
    cert_file: /etc/dbbridge/tls/a.crt
    key_file: /etc/dbbridge/tls/a.key
admin:
  enabled: true
  listen: "127.0.0.1:9080"
auth:
  credentials_file: auth-a.yaml
  reload_interval: 5s
peers:
  - id: B
    expected_node_id: B
    credential_ref: B
    endpoints:
      - {id: primary, url: "https://b-primary.example.com:8443", priority: 1}
      - {id: secondary, url: "https://b-secondary.example.com:8443", priority: 2}
    tls: {verify: true}
    failover: {enabled: true, strategy: priority, failback: new_sessions}
listeners:
  - {id: mysql-at-b, listen: "127.0.0.1:13306", peer: B, target: mysql}
targets:
  mssql: {network: tcp, address: "127.0.0.1:1433", allowed_cidrs: ["127.0.0.1/32"]}
transport:
  protocol: auto
  protocol_version: "1.4"
  recovery: {enabled: true, grace: 30s, barrier_timeout: 5s}
```

```yaml
# auth-a.yaml
version: 1
inbound_peers:
  - id: B
    token_files: [tokens/b-to-a.token]
    allowed_targets: [mssql]
    max_sessions: 100        # half of limits.max_sessions: local listeners share the budget
outbound_peers:
  - {id: B, token_file: tokens/a-to-b.token}
```

Node B mirrors it: `node.id: B`, `auth-b.yaml` with `inbound_peers: [{id: A, token_files: [tokens/a-to-b.token], allowed_targets: [mysql]}]` and `outbound_peers: [{id: A, token_file: tokens/b-to-a.token}]`, the target `mysql`, and a listener to A's `mssql` (see `configs/production/peer-b.yaml` and `auth-b.yaml`).

Bring it up on each node:

```bash
sudo install -d -m 0750 -o root -g dbbridge /etc/dbbridge/tokens /etc/dbbridge/tls
openssl rand -hex 32 | sudo tee /etc/dbbridge/tokens/a-to-b.token >/dev/null   # once; copy to the other node
openssl rand -hex 32 | sudo tee /etc/dbbridge/tokens/b-to-a.token >/dev/null   # once; copy to the other node
sudo chown root:dbbridge /etc/dbbridge/tokens/*.token && sudo chmod 0640 /etc/dbbridge/tokens/*.token
sudo -u dbbridge dbbridge check-config -c /etc/dbbridge/peer-a.yaml
sudo dbbridge install -c /etc/dbbridge/peer-a.yaml --service-user dbbridge --start
curl -s http://127.0.0.1:9080/readyz
```

Before the certificates are deployed, `dbbridge check-config --offline -c peer-a.yaml` checks structure, references and listen addresses without reading TLS files or tokens.
