[English](examples.md) | **简体中文**

# 部署示例

[README](../README.zh-CN.md) · [安装](installation.zh-CN.md) · [配置](configuration.zh-CN.md) · [传输](transport.zh-CN.md) · [安全](security.zh-CN.md) · [运维](operations.zh-CN.md) · **示例**

本页的每份配置，在提供示例中提到的令牌文件、环境变量和证书后，都能通过 `dbbridge check-config`。主机名使用保留域名 `example.com`，请替换为实际名称。

- [1. 单向 MySQL：厂区 → 总部](#1-单向-mysql厂区--总部)
- [2. 使用节点 TLS 与令牌文件的 Microsoft SQL Server](#2-使用节点-tls-与令牌文件的-microsoft-sql-server)
- [3. 用 `gen-config` 生成双向节点对](#3-用-gen-config-生成双向节点对)
- [4. 使用 TLS 与凭据文件的生产节点对](#4-使用-tls-与凭据文件的生产节点对)

## 1. 单向 MySQL：厂区 → 总部

节点 `plant-a`（Ingress）为本地应用提供 `127.0.0.1:13306`；节点 `hq-b`（Egress）转发到本机的 MySQL。`hq-b` 只监听回环地址，前面由本机的 TLS 终结反向代理对外提供 `https://hq.example.com`（代理配置见[传输 §7](transport.zh-CN.md#7-反向代理与网关)）。这个最小示例从环境变量读取令牌；如果希望不重启就能轮换令牌，请像示例 2–4 那样使用令牌文件。

```yaml
# peer-b.yaml（hq-b，Egress）
node:
  id: hq-b

http:
  enabled: true
  listen: "127.0.0.1:18080"        # TLS 代理把 https://hq.example.com 转发到这里

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
# peer-a.yaml（plant-a，Ingress）
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
        url: "https://hq.example.com"   # 证书必须与该名称匹配
        priority: 1

listeners:
  - id: mysql-local
    listen: "127.0.0.1:13306"
    peer: hq-b
    target: mysql-primary
```

运行：

```bash
# 生成一个令牌，通过安全渠道把同一个值交给两个节点
export DBB_TOKEN_SECRET="$(openssl rand -hex 32)"

dbbridge check-config -c peer-b.yaml      # 在 hq-b 上
dbbridge run -c peer-b.yaml

dbbridge check-config -c peer-a.yaml      # 在 plant-a 上
dbbridge run -c peer-a.yaml

mysql -h 127.0.0.1 -P 13306 -u app -p     # 在 plant-a 上
```

## 2. 使用节点 TLS 与令牌文件的 Microsoft SQL Server

Egress 自己在 8443 端口终结 TLS，并转发到其所在网络中的一个 SQL Server 实例。Ingress 监听 `127.0.0.1:11433`，以免与本机的 SQL Server 冲突。请为 SQL Server 实例使用固定的 TCP 端口（SQL Browser 服务不会被转发）。

`hq-b` 上的文件：`/etc/dbbridge/peer.yaml`、`/etc/dbbridge/tls/hq.crt`、`/etc/dbbridge/tls/hq.key`、`/etc/dbbridge/tokens/plant-a.token`。`plant-a` 上的文件：`/etc/dbbridge/peer.yaml`、`/etc/dbbridge/tokens/plant-a.token`（内容相同）。

```yaml
# hq-b/peer.yaml（Egress）
node:
  id: hq-b

http:
  enabled: true
  listen: "0.0.0.0:8443"
  tls:
    enabled: true
    cert_file: tls/hq.crt           # 相对于本文件所在目录
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
# plant-a/peer.yaml（Ingress）
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
    #   ca_file: tls/company-ca.pem   # hq.crt 由私有 CA 签发时使用

listeners:
  - id: mssql-local
    listen: "127.0.0.1:11433"
    peer: hq-b
    target: mssql-primary
```

```bash
openssl rand -hex 32 > tokens/plant-a.token && chmod 0640 tokens/plant-a.token   # 复制到两个节点
dbbridge check-config -c /etc/dbbridge/peer.yaml
sqlcmd -S 127.0.0.1,11433 -U app                                                # 在 plant-a 上
```

## 3. 用 `gen-config` 生成双向节点对

每个节点都把自己的数据库提供给对方，并通过本地 listener 访问对方的数据库。`gen-config` 一步生成一对匹配的配置和一个共享令牌。本例中 A 提供 SQL Server，B 提供 MySQL；两者都在 8443 端口终结 TLS，证书已存放在 `/etc/dbbridge/tls/`。

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

结果：

| | 节点 A（`peer-a.yaml`） | 节点 B（`peer-b.yaml`） |
|---|---|---|
| 本地 listener | `127.0.0.1:13306` → B 的 `mysql` | `127.0.0.1:13307` → A 的 `mssql` |
| 入站 HTTPS | `0.0.0.0:8443` | `0.0.0.0:8443` |
| Admin | `127.0.0.1:18090` | `127.0.0.1:18091` |
| 数据库 | `mssql` = `127.0.0.1:1433` | `mysql` = `127.0.0.1:3306` |
| 入站配额 | 200 个会话中的 100 个 | 200 个会话中的 100 个 |

把 `peer-a.yaml` 和 `shared.token` 部署到 A，把 `peer-b.yaml` 和同一个 `shared.token` 部署到 B，各自放在同一个目录中。TLS 路径以绝对路径写入；请在每个节点上把证书和私钥放在这些路径下。重复 `--a-peer-url`/`--b-peer-url` 可以添加故障切换 endpoint。不带 TLS 和 URL 选项时，这对节点在同一台机器的回环地址上运行，适合快速试用。

这对节点两个方向使用同一个令牌，并使用内联凭据；令牌文件在重载时重新读取，因此无需重启即可轮换令牌（[安全 §4](security.zh-CN.md#4-令牌轮换)）。如需每个方向使用独立的令牌，请采用示例 4 的布局。

## 4. 使用 TLS 与凭据文件的生产节点对

即 `configs/production/` 的布局：每个节点在 8443 端口终结 TLS，为对方配置多个 endpoint，把凭据放在独立文件中且**每个方向一个令牌**，并把自己的数据库提供给对方。`/etc/dbbridge/` 下的文件（`root:dbbridge`，0640；目录 0750）：

| 节点 A | 节点 B |
|---|---|
| `peer-a.yaml`、`auth-a.yaml` | `peer-b.yaml`、`auth-b.yaml` |
| `tls/a.crt`、`tls/a.key` | `tls/b.crt`、`tls/b.key` |
| `tokens/a-to-b.token`、`tokens/b-to-a.token` | `tokens/a-to-b.token`、`tokens/b-to-a.token` |

`a-to-b.token` 是 A 发给 B 的令牌，`b-to-a.token` 是 B 发给 A 的令牌。两个节点都持有这两个文件。

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
    max_sessions: 100        # limits.max_sessions 的一半：本地 listener 共用会话预算
outbound_peers:
  - {id: B, token_file: tokens/a-to-b.token}
```

节点 B 与之对称：`node.id: B`；`auth-b.yaml` 中为 `inbound_peers: [{id: A, token_files: [tokens/a-to-b.token], allowed_targets: [mysql]}]` 和 `outbound_peers: [{id: A, token_file: tokens/b-to-a.token}]`；目标为 `mysql`；listener 指向 A 的 `mssql`（见 `configs/production/peer-b.yaml` 和 `auth-b.yaml`）。

在每个节点上启动：

```bash
sudo install -d -m 0750 -o root -g dbbridge /etc/dbbridge/tokens /etc/dbbridge/tls
openssl rand -hex 32 | sudo tee /etc/dbbridge/tokens/a-to-b.token >/dev/null   # 只生成一次，复制到另一个节点
openssl rand -hex 32 | sudo tee /etc/dbbridge/tokens/b-to-a.token >/dev/null   # 只生成一次，复制到另一个节点
sudo chown root:dbbridge /etc/dbbridge/tokens/*.token && sudo chmod 0640 /etc/dbbridge/tokens/*.token
sudo -u dbbridge dbbridge check-config -c /etc/dbbridge/peer-a.yaml
sudo dbbridge install -c /etc/dbbridge/peer-a.yaml --service-user dbbridge --start
curl -s http://127.0.0.1:9080/readyz
```

证书部署之前，可以用 `dbbridge check-config --offline -c peer-a.yaml` 检查结构、引用和监听地址，而不读取 TLS 文件和令牌。
