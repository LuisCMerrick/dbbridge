[English](README.md) | **简体中文**

# DBBridge

DBBridge 通过普通的 HTTPS，让数据库连接穿越受限的网络边界——厂区网络、云 VPC、分支机构、专线与公网混合链路。数据库客户端连接本地 TCP 端口；DBBridge 通过 **WebSocket** 或 **HTTP POST + Server-Sent Events** 把字节流转发给对端节点，再由对端连接真实的数据库。流量可以穿过只放行 HTTP(S) 的反向代理、网关和防火墙。

DBBridge 对协议完全透明：逐字节原样转发，从不解析或改写 SQL，也不缓存结果，因此事务、预处理语句和驱动特性的表现与直连相同。它常用于 MySQL、MariaDB 和 Microsoft SQL Server，并适用于任何基于 TCP 的数据库协议。

## 特性

- **一个二进制，三种角色**：Ingress（本地 listener）、Egress（入站 HTTP 服务与数据库），或两者兼有，由配置决定。
- **两种载体**：默认使用 WebSocket；代理无法承载 WebSocket 时，在同一 endpoint 上自动回退为 HTTP POST + SSE。
- **故障切换**：每个对端可配置多个带优先级的 endpoint，支持后台与按需健康探测、故障路径冷却，以及新会话回切。
- **会话恢复**：载体断开时无需关闭数据库连接即可更换载体：两端在固定的宽限期内从最后确认的偏移继续传输。未开启恢复或恢复失败时，立即关闭客户端连接，让应用自行重连。
- **安全**：TLS、基于 Bearer 令牌的身份、节点身份校验、按身份的目标 ACL、目标地址白名单、按身份的配额和预算分额。
- **凭据热重载**：令牌文件和凭据设置会定期重新读取，也可通过 `SIGHUP` / `systemctl reload` 或 Windows 服务的参数变更请求立即重新读取；轮换和吊销无需重启。
- **资源有界**：内存、会话元数据、连接和并发都有预算；过载时返回明确的、可重试的错误。
- **运维**：`gen-config` 生成匹配的配置对，`check-config` 输出稳定的错误码，支持 systemd 和 Windows 服务安装，回环 admin 端点提供健康检查、就绪检查和 Prometheus 指标。

## 架构

```
 数据库客户端                                                         数据库
 (mysql、sqlcmd、应用)                                       (MySQL / MariaDB / SQL Server)
        │ TCP 127.0.0.1:13306                                              ▲ TCP
        ▼                                                                  │
 ┌──────────────────────┐    HTTPS：WebSocket 或 POST + SSE      ┌─────────────────────┐
 │ Ingress 节点         │ ─────────────────────────────────────▶ │ Egress 节点         │
 │  listeners           │   /v1/peer/probe   /v1/sessions        │  http（监听、TLS）  │
 │  peers / endpoints   │   /v1/transport/ws /v1/sessions/{id}/… │  auth（身份、ACL、  │
 │  故障切换、会话恢复  │   每个请求携带 Bearer 令牌             │   配额）            │
 └──────────────────────┘                                        │  targets            │
                                                                 └─────────────────────┘
```

- **Ingress** 在 listener 上接受客户端连接，选出对端最优的可用 endpoint（探测并核对对端节点 ID），为该 listener 的目标创建会话，并接入一个载体。
- **Egress** 按 Bearer 令牌认证请求，检查该身份的目标 ACL 和配额，拨号数据库（`targets`，可用 `allowed_cidrs` 限制），然后中继数据。
- **载体。** 使用 `ws` 时，每个会话一条 WebSocket，双向传输数据。使用 `http-sse` 时，上行帧附带 SHA-256 摘要以 POST 发送，下行数据通过 SSE 流到达。控制请求（确认、FIN、恢复、关闭）使用单独的连接池，因此数据通道拥塞时仍能送达。
- **会话与恢复。** 两个方向的数据都按偏移确认。载体故障时，开启恢复的会话在 Egress 上挂起，由 Ingress 在 `transport.recovery.grace` 内通过任一可用的 endpoint 和载体，从最后确认的偏移继续——不丢失也不重复任何字节。会话不会迁移到其他进程：进程重启后客户端需要重连。

详见[传输](docs/transport.zh-CN.md)。

## 支持的平台

| | 二进制 | 服务安装 |
|---|---|---|
| Linux amd64 / arm64 | ✓ | systemd |
| Windows amd64 | ✓ | Windows 服务（内置），或 WinSW |

从源码构建需要 Go 1.27.2 或更新版本。节点使用协议 1.4；任意两个支持协议 1.4 的版本之间都能互通。

## 快速开始

在一台机器上试用双向节点对（两个节点都把 `127.0.0.1:3306` 提供给对方）：

```bash
dbbridge gen-config -o ./pair                  # peer-a.yaml、peer-b.yaml、shared.token
dbbridge check-config -c ./pair/peer-a.yaml
dbbridge check-config -c ./pair/peer-b.yaml
dbbridge run -c ./pair/peer-b.yaml &           # 节点 B
dbbridge run -c ./pair/peer-a.yaml &           # 节点 A
mysql -h 127.0.0.1 -P 13306 -u app -p          # 经 A 访问 B 一侧的数据库
curl -s http://127.0.0.1:18090/readyz          # A 的 admin 端点
```

部署到两台主机时，传入真实地址（`--a-peer-url https://b.example.com:8443 --b-peer-url https://a.example.com:8443`，用 `--a-tls-cert`/`--a-tls-key` 指定 TLS 证书等），把 `peer-a.yaml` 和 `shared.token` 复制到 A，把 `peer-b.yaml` 和同一个 `shared.token` 复制到 B，然后安装服务：

```bash
sudo dbbridge install -c /etc/dbbridge/peer-a.yaml --service-user dbbridge --start
```

完整部署见[示例](docs/examples.zh-CN.md)，服务配置见[安装](docs/installation.zh-CN.md)。

## 文档

| 文档 | 内容 |
|---|---|
| [安装](docs/installation.zh-CN.md) | 发布包与校验和、构建、systemd 与 Windows 服务、WinSW、文件布局、升级、卸载 |
| [配置](docs/configuration.zh-CN.md) | 所有配置项、凭据文件、令牌、派生默认值、约束、哪些内容会重载 |
| [传输](docs/transport.zh-CN.md) | 载体与回退、协商、流控、会话恢复、超时、预算、反向代理、TLS |
| [安全](docs/security.zh-CN.md) | 加固、令牌轮换、紧急吊销、ACL、admin 暴露、服务账户 |
| [运维](docs/operations.zh-CN.md) | CLI、退出码、信号、重载、admin 端点、指标、日志、错误码、容量、故障排查 |
| [示例](docs/examples.zh-CN.md) | MySQL、SQL Server、用 `gen-config` 生成的双向节点对、使用 TLS 的生产节点对 |
| [CHANGELOG](CHANGELOG.zh-CN.md) | 各版本变更与升级说明 |

样例配置位于 `configs/`，服务定义位于 `deploy/`。

## 开源协议

本项目基于 [Apache License, Version 2.0](LICENSE) 许可协议开源。

