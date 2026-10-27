[English](transport.md) | **简体中文**

# 传输、调优、反向代理与 TLS

[README](../README.zh-CN.md) · [安装](installation.zh-CN.md) · [配置](configuration.zh-CN.md) · **传输** · [安全](security.zh-CN.md) · [运维](operations.zh-CN.md) · [示例](examples.zh-CN.md)

本文提到的每个配置项，其类型、默认值和约束都列在[配置参考](configuration.zh-CN.md#11-transport)中。本文说明这些值的作用、何时需要调整，以及两个节点之间的网络路径需要满足哪些条件。

- [1. 载体：WebSocket 与 HTTP/SSE](#1-载体websocket-与-httpsse)
- [2. 协商值](#2-协商值)
- [3. 帧、窗口与确认](#3-帧窗口与确认)
- [4. 故障检测、故障切换与会话恢复](#4-故障检测故障切换与会话恢复)
- [5. 超时](#5-超时)
- [6. 内存与连接预算](#6-内存与连接预算)
- [7. 反向代理与网关](#7-反向代理与网关)
- [8. TLS](#8-tls)

## 1. 载体：WebSocket 与 HTTP/SSE

一个会话（即一条数据库客户端连接）通过一个**载体**传输：

- **`ws`**：每个会话一条 WebSocket，子协议为 `dbbridge.v1.5`，双向传输二进制数据帧，另有文本控制消息。
- **`http-sse`**：上行数据以 HTTP `POST /v1/sessions/{id}/up` 请求发送（每个请求一帧，附带 SHA-256 摘要头）；下行数据和控制事件通过 Server-Sent Events 流（`GET /v1/sessions/{id}/events`）接收。确认和 FIN 使用单独的控制请求。

控制操作（probe、create、ack、fin、resume、delete）始终使用独立的 HTTP 连接池，因此数据通道饱和时控制操作仍然畅通。

`transport.protocol`（或针对单个 peer 的 `peers[].transport.protocol`）决定使用哪种载体：

| 取值 | 行为 |
|---|---|
| `auto`（默认） | 优先使用 WebSocket。在某个 endpoint 上建立第一个 WebSocket 会话之前，Ingress 先发送一次 WebSocket 预检。如果该路径无法承载 WebSocket，会话改在同一 endpoint 上使用 SSE。 |
| `ws` | 只用 WebSocket。 |
| `http-sse` | 只用 SSE。 |

Egress 在探测应答中告知自己接受的载体（`http.allowed_transports`）；Ingress 不会提议 Egress 不接受的载体。

**`auto` 何时回退到 SSE。** 预检收到的任何普通 HTTP 应答，只要不是经过验证的 DBBridge 应答、也不是 `401`，都表示“该路径不支持 WebSocket”，例如重定向、`400`、不带 DBBridge 响应体的 `403`、`404`、`405`、`407`、`410`、`413`、`414`、`426`、`429`、`431`、`501`、一个 `200` 页面、网关返回的 `5xx`，以及升级过程中连接被重置或超时。此时只有该 endpoint 的 WebSocket 载体进入冷却（`failover.cooldown`）；Ingress 重新探测该 endpoint 后在其上使用 SSE。经过验证的 `DBB_TRANSPORT_UNSUPPORTED` 应答效果相同。

**何时不回退。** `401`（无论是否经过验证）或经过验证的 `403` 属于认证或授权结论：会话失败，不回退。升级之后出现的协议违规（子协议缺失或不符、hello 中的协议版本错误、控制 JSON 格式错误、非法 WebSocket 帧）、TLS 失败，或节点/实例/协议不匹配，会把该 endpoint 标记为 `MISMATCHED`，Ingress 不会在该 endpoint 上改用 SSE；如果故障切换允许，就转向其他 endpoint。

预检结果按 endpoint 缓存 30 s；临时性应答（`408`、`425`、`429`、`5xx`）不缓存。某个 endpoint 上正在使用 WebSocket 时（最近 2 分钟内有会话用过），健康检查会在成功结果过期前续期；成功结果过期后 5 分钟内仍会被使用，同时在后台重新预检，所以新会话不必等待预检。如果期间 WebSocket 路径已经失效，该会话的升级失败，载体按上文冷却并回退。每个成功创建的会话最多计一次回退，指标为 `dbb_session_events_total{event="fallback",from="ws",to="http-sse"}`。

Egress 按已认证身份限制 WebSocket 预检：最多 2 个并发，每秒 4 次，突发 4 次。超出时返回 `429 DBB_QUOTA_EXCEEDED` 并带 `Retry-After`，不占用数据库会话。

**建立连接的开销。** 对端处于热状态时，一个新的本地连接在第一个字节到达数据库之前需要与 Egress 往返三次：create、载体（WebSocket 升级或 SSE 事件流）以及数据本身。

- 新会话复用最近一次成功的健康检查探测结果（不超过 1.5 × `healthcheck_interval`，且不超过 15 s），不再先探测。如果 Egress 期间重启过，create 会被 `DBB_INSTANCE_MISMATCH` 拒绝，会话随即重新探测；create 的其他失败也会丢弃缓存结果；create 迟迟不应答时会并行探测（见“静默断路”）。健康检查在启动时立即执行一次（正向对端；反向对端在一个间隔后才首次探测，此时隧道通常已建立）。
- 每次建立 WebSocket 会话后，Ingress 会向该 endpoint 预先建立一条备用连接（TCP 与 TLS），4 s 内的下一个会话直接在它上面发送升级请求。未使用的备用连接 4 s 后关闭且不续建，空闲的对端不会多占连接。使用环境变量代理时不保留备用连接。使用前会检测服务端是否已关闭备用连接；备用连接在收到任何应答前失败时，改用一次全新的拨号。
- Egress 在 WebSocket 升级和 SSE 事件流的应答中带上 `X-DBB-Implicit-Ready: 1`：新会话的数据面会在第一个标明该流的上行帧或 FIN 到达时激活（只有 `ready` 消息携带流 ID），所以 Ingress 发送数据时不必等待 ready ACK 返回，ACK 仍会立即发出。没有该响应头时（旧版 Egress，或代理丢弃了它），Ingress 照旧等待 ready ACK。恢复中的会话始终等待 ready ACK。
- 新会话发送数据前不再等待第一个心跳 ping，nonce 由控制通道随后回传。恢复时仍先确认 nonce。

在 HTTP/1.1 上每条 SSE 事件流都要独占一个连接，所以没有 HTTP/2 直达 Egress 时，`http-sse` 在连接池没有空闲连接的情况下多一次往返（带 TLS 时两次）。

## 2. 协商值

两个节点的 `transport` 段不必完全相同。创建会话时，Ingress 发送自己的限制值，Egress 在应答中给出该会话实际采用的值：

| 值 | 规则 |
|---|---|
| `max_frame_size`、`up_max_inflight_frames`、`up_window_bytes`、`buffer_per_direction`（作为下行缓冲） | 取两端中较小的值。 |
| 会话恢复 | 由 Ingress 请求（`transport.recovery.enabled` / `peers[].recovery.enabled`）。如果 Egress 关闭了恢复，创建以 `409 DBB_RECOVERY_UNSUPPORTED` 失败：要么在 Ingress 上对该 peer 关闭恢复，要么在 Egress 上开启。 |
| 恢复宽限期 | 取两端 `recovery.grace` 中较小的值。 |
| 首次接入 | Ingress 必须在 Egress 的 `unattached_timeout` 内接入载体；超时后 Egress 丢弃该会话，Ingress 以 `timeout` 原因结束会话。 |
| 载体 | 必须在 Egress 的 `http.allowed_transports` 中，否则返回 `409 DBB_TRANSPORT_UNSUPPORTED`。 |
| 协议版本 | 两端都必须为 `1.5`，否则返回 `409 DBB_PROTOCOL_MISMATCH`。 |

两端 `transport` 段保持一致可以让行为更容易预期，但并非必需。

### 流控上限

每个建连应答都携带 Egress 的流控上限：`limits.up_receipt_limit`（即其 `receipt_limit_per_session`）和 `limits.down_max_frames`（512）。通告值超过 1,048,576 时按 1,048,576 处理；缺少这两个字段的应答视为协议错误。当任一上限用掉一半时，Egress 会在 `ack_up` 控制消息中设置 `ack_requested: true`，Ingress 随即发送确认。此外，当未确认的上行帧达到 `min(up_max_inflight_frames + 4, up_receipt_limit / 2)`，或下行帧达到 `min(32, down_max_frames / 4)` 时，Ingress 也会立即确认。

## 3. 帧、窗口与确认

| 配置项 | 作用 | 何时调整 |
|---|---|---|
| `frame_size` | 从客户端读取的上行帧的目标大小。 | 很少需要。 |
| `max_frame_size` | 双方接受的最大帧（≤ 1 MiB）。更大的 SSE 上行帧以 `413 DBB_FRAME_TOO_LARGE` 拒绝。 | 代理把请求体限制在 256 KiB 以下时调小。 |
| `chunk_size` | 套接字读取大小以及缓冲记账粒度。 | 很少需要。 |
| `buffer_per_direction` | 每个会话每个方向的缓冲。写满后节点停止从源套接字读取（背压）。 | 高带宽、高延迟链路上调大；它按会话放大内存占用。 |
| `up_window_bytes`、`up_max_inflight_frames` | 收到确认之前可以在途的上行字节数和帧数。 | 长肥链路上与 `buffer_per_direction` 一起调大 `up_window_bytes`。 |
| `receipt_limit_per_session` | Egress 为去重保留的上行回执数。 | 只在调大 `up_max_inflight_frames` 时一起调大。 |
| `ack_batch_bytes`、`ack_max_delay` | 下行累计达到该字节数或等待达到该时间（先到者为准）即发送确认。Ingress 实际使用 `min(ack_batch_bytes, 协商后的下行缓冲 / 2)`。 | 很少需要。 |
| `up_ack_max_delay` | 上行确认的最大延迟。 | 很少需要。 |

缓冲写满时，会话最多等待 `backpressure_timeout` 让出空间，之后以 `timeout` 原因结束。会话因等待恢复而挂起期间，数据库一侧不会因背压被关闭，而是由 `recovery.grace` 和 `suspended_buffer_limit` 约束。

## 4. 故障检测、故障切换与会话恢复

**故障检测。** 双方每隔 `heartbeat` 发送心跳。载体在 `failure_detect_timeout` 内没有任何进展即判定为断开。进展指会话流量（绑定到该会话的事件或 WebSocket 消息，或仍在到达的数据帧）；SSE 注释和解析器忽略的字段（例如代理插入的保活注释）不算进展。使用 HTTP/2 时，节点还会每隔 `heartbeat` 发送 PING，5 s 无应答即关闭连接。

**故障切换。** 每个 peer 的 endpoint 按 `priority` 排序。Ingress 每隔 `failover.healthcheck_interval` 探测一次；连续失败 `unhealthy_threshold` 次后 endpoint 变为 `UNHEALTHY`，连续成功 `healthy_threshold` 次后恢复可用。新会话使用最优的可用 endpoint；优先 endpoint 恢复后，只有新会话会切过去（`failback: new_sessions`）。

**静默断路。** 路径开始静默丢包（防火墙规则、链路中断）时，Ingress 约在两次探测时长内发现，而不是等 `control_request_timeout`。守护超时为该 endpoint 最近一次探测耗时的 4 倍，至少 1 s，至多 `healthcheck_timeout`。基于缓存探测结果的新会话，如果 create 在最近探测耗时的 2 倍（至少 0.5 s）内还没应答，就并行探测该 endpoint；任何发往该 endpoint 的请求等待响应头超过守护超时也会触发同样的检查。探测在守护超时内没有任何应答时（HTTP 错误应答不算，说明路径是通的），Ingress 关闭该 endpoint 在检查期间没有收到任何数据的连接，所有仍在等待的请求立即失败，已建立的会话由恢复机制迁走；有其他可用 endpoint 时该地址还会进入 `failover.cooldown`，新会话直接改用下一个 endpoint。日志为 `peer endpoint not answering; closed its stalled connections`。实测（两个 endpoint，主路径防火墙 DROP，RTT 84 ms）：断路后第一个新会话 10.8 s → 1.6–1.8 s，已建立会话 13.7 s → 2.9 s。

**冷却。** 载体在某个 endpoint 上第一次失败，就让该 endpoint/载体组合冷却 `failover.cooldown`；整条路径失败（例如 TCP 连接被拒）则让该地址冷却。冷却不会因探测成功而缩短。所有候选都在冷却时，Ingress 等待最早结束的那个，但不超过 `create_retry_timeout`（建连）或恢复截止时间（恢复）。纯本地原因永远不会导致冷却：本地建连期限、恢复截止时间、客户端关闭或重置了套接字、节点停机，以及本地预算耗尽。

**会话恢复。** 开启恢复后，载体断开不会结束会话。Egress 挂起会话并保留未确认的数据；Ingress 通过任一可用的 endpoint 和载体重新接入，并从两个方向最后确认的偏移继续传输。恢复完成之前，旧载体上已经开始的写入最多可以继续 `recovery.barrier_timeout`；如果无法到达这一写入屏障，会话以 `barrier` 原因结束。整个恢复必须在 `recovery.grace` 内完成，该期限从故障时刻起算，不会延长。所有挂起会话合计最多保留 `suspended_buffer_limit` 字节；超出时，从保留了数据的最早挂起会话开始结束。

**未开启恢复**或恢复失败时，会话终止，Ingress 立即关闭本地客户端连接，数据库客户端看到连接已关闭，可以自行重连。恢复永远不会跨越进程重启：新实例会返回 `409 DBB_INSTANCE_MISMATCH`，客户端需要重连。

**终止通知。** 当 Egress 自己结束一个会话（数据库连接丢失、授权被撤销、缓冲耗尽、帧冲突）时，会在当前载体上发送最后一条 `error` 控制消息（WebSocket 随后发送 Close 帧，代码 1000，原因字段为错误码）。Ingress 随即关闭本地客户端，不会尝试恢复。

**无法成功的恢复。** 强制使用单一载体（`ws` 或 `http-sse`）且只有一个可用 endpoint 时，失败的载体会冷却 `failover.cooldown`；如果 `recovery.grace` 不大于 `cooldown + barrier_timeout`，恢复不可能成功。`check-config` 会对此给出警告；当与 Egress 协商出的宽限期降到这一水平时，Ingress 也会记录警告日志。

## 5. 超时

| 配置项 | 所在端 | 限制对象 |
|---|---|---|
| `dial_timeout` | Egress | 到数据库的 DNS 解析与 TCP 建连。 |
| `peer_dial_timeout` | Ingress | 到对端 endpoint 的 TCP 建连。 |
| `ws.handshake_timeout` | Ingress | WebSocket 升级（TCP、TLS、`101`）以及为其等待连接名额的时间。之后在 `control_request_timeout` 内等待第一条 `ready` 消息。 |
| `control_request_timeout` | Ingress | ack、fin、delete、resume、SSE 订阅的响应头，以及为它们等待连接名额的时间。 |
| `up_request_timeout` | Ingress、Egress | 单个 SSE 上行 POST，含等待连接名额的时间。在 Egress 上，它还是 WebSocket 帧体到达时间的上限（5 s + 每 16 KiB 帧长 1 s）；更慢的帧会导致载体断开。 |
| `create_retry_timeout` | Ingress | 一个会话的整个建连过程，跨所有重试和 endpoint。 |
| `unattached_timeout` | Egress | 从建连到第一个载体接入的时间。 |
| `socket_write_timeout` | 两端 | 写本地客户端或数据库时无进展的时间。 |
| `ws.write_idle_timeout`、`sse_write_idle_timeout` | 两端 | 载体写入无进展的时间。 |
| `backpressure_timeout` | 两端 | 等待缓冲空间的时间。 |
| `failure_detect_timeout` | 两端 | 载体无进展的时间。 |
| `shutdown_timeout` | 两端 | 停止时的排空阶段；之后的收尾最多再用 15 s。 |
| `failover.healthcheck_timeout` | Ingress | 单次探测（后台和按需）。 |

固定值：TLS 握手 5 s；Ingress 的 HTTP 客户端在 50 s 后丢弃空闲 keep-alive 连接；Egress 服务端保留空闲连接 90 s；超过 2 s 的 `Retry-After` 按 2 s 处理。从不跟随重定向。

## 6. 内存与连接预算

| 预算 | 配置项 | 说明 |
|---|---|---|
| 负载 | `global_buffer_limit` | 进程内所有会话缓冲的数据（`dbb_resource_in_use{resource="payload"}`）。 |
| 恢复保留 | `suspended_buffer_limit` | 为挂起会话保留的数据，属于负载预算的一部分。 |
| 编解码 | `codec_buffer_limit` | 编码和解码缓冲。一个 WebSocket 下行帧约占 `2 × 大小 + 56` 字节，一个 SSE 帧约占 `6 × 大小 + 16 KiB`；SSE 上行行按其声明的长度预留。 |
| 元数据 | `metadata_limit` | 会话状态、回执、建连记录与墓碑、请求元数据。其中 `2 × min(limit/4, 1 MiB)` 留给控制和探测请求。 |
| 会话 | `limits.max_sessions` | |

**Egress 上的按身份分额。** 启用的入站身份多于一个时，每个进程级 Egress 预算都会被划分，避免某个身份挤占其他身份：有 *n* 个启用身份时，单个身份最多占用预算 *B* 的 `(B − B/(2n)) / (n − 1)`，因此每个身份始终至少能拿到 `B/(2n)`。这适用于并发建连（16）、探测（16）和恢复（16）、请求元数据、编解码预算以及建连记录。超出分额的请求得到 `429 DBB_QUOTA_EXCEEDED`（消息中写明预算名称，例如 `peer idempotency quota`），编解码预算则返回 `503 DBB_BUFFER_EXHAUSTED "codec budget"`；这些应答都可重试。每次凭据重载后，分额按新的启用身份集合重新计算。

**双向节点**（同时配置了 `http.enabled` 和 `listeners`）为本地客户端保留一部分远端 peer 无法占用的预算：`max(1, max_sessions/4)` 个会话、1/8 的元数据预算，以及 `max(codec_buffer_limit/8, max_frame_size + 56)` 的编解码预算。

**连接。**

| 预算 | 大小 |
|---|---|
| 入站连接（Egress） | `4 × min(max_sessions, 8192) + 64`，每条连接另占 16 KiB 连接元数据 |
| 出站数据连接 / 请求（Ingress） | `2 × min(max_sessions, 8192) + 32`；每个 peer 另外最多 `2 × max_sessions + 32` |
| 出站控制连接 / 请求（Ingress） | `min(min(max_sessions, 8192) + 32, 256)` |

等待连接名额的请求，最长等待到它自身的期限（见 §5）。等待用尽时以 `local connection budget exhausted` 失败；这属于本地背压，因此不会判定载体故障或触发冷却，上行和 FIN 请求会在同一载体上重试。

每个预算的当前占用和上限以 `dbb_resource_in_use{resource}` / `dbb_resource_limit{resource}` 导出（[运维 → 指标](operations.zh-CN.md#5-指标)）。可持续的新建会话速率受建连记录预算限制，见[运维 → 容量规划](operations.zh-CN.md#10-容量规划)。

## 7. 反向代理与网关

Egress 的 `http.listen` 前面可以放置反向代理、负载均衡或 API 网关，但必须满足以下要求：

**路径**：原样转发 `/v1/` 下的所有请求：

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/v1/peer/probe` | 探测：核对节点、实例和协议 |
| POST | `/v1/sessions` | 创建会话 |
| GET + Upgrade | `/v1/transport/ws` | WebSocket 载体与预检 |
| GET | `/v1/sessions/{id}/events` | SSE 下行流 |
| POST | `/v1/sessions/{id}/up` | SSE 上行帧 |
| POST | `/v1/sessions/{id}/ack` | 确认 |
| POST | `/v1/sessions/{id}/fin` | 半关闭 |
| POST | `/v1/sessions/{id}/resume` | 恢复 |
| DELETE | `/v1/sessions/{id}?epoch=N` | 关闭 |

**请求头**：原样透传 `Authorization`（Bearer 令牌）、`X-DBB-Session-ID`、`X-DBB-Session-Token`、`X-DBB-Instance-ID`、`X-DBB-Epoch`、`X-DBB-Stream-ID`、`X-DBB-Offset`、`X-DBB-Length`、`X-DBB-SHA256`、`X-DBB-Nonce`、`X-DBB-Confirm-Up-Offset`、`X-DBB-Ack-Down-Offset`、`X-DBB-WS-Purpose`、`X-DBB-Features`、`Content-Length`。请求头总大小必须保持在 16 KiB 以内。

**响应头**：透传 `X-DBB-Instance-ID`（Ingress 用它确认错误应答确实来自对端）、`X-DBB-Implicit-Ready`（每个新会话省一次往返，见第 1 节）和 `Retry-After`。不要用代理自己的错误页替换 DBBridge 的错误响应体。

**WebSocket**：转发升级请求所需的 `Upgrade`、`Connection`、`Sec-WebSocket-Key`、`Sec-WebSocket-Version`、`Sec-WebSocket-Protocol: dbbridge.v1.5`（以及应答中的 `Sec-WebSocket-Protocol`）。代理做不到时，使用 `auto`（会话回退到 SSE）或 `http-sse`。

**SSE**：不要缓冲 `GET /v1/sessions/{id}/events` 的响应（nginx：`proxy_buffering off`），也不要缓冲上行 POST 的请求体。`/up` 请求体最大为 `max_frame_size`（默认 256 KiB）。

**超时与 keep-alive**：代理的空闲/读超时必须大于 `heartbeat`（15 s），建议 60 s 以上。Ingress 在 50 s 后丢弃空闲的客户端连接，因此代理应让空闲的客户端连接保持**至少 60 s**（nginx 默认的 `keepalive_timeout 75s` 即可）。

**不重定向、不转发到其他后端重试**：Ingress 不跟随重定向；同一会话的所有请求必须到达同一个 Egress 进程。

nginx 配置示意（请按实际环境调整）：

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}
server {
    listen 443 ssl;
    server_name hq.example.com;
    ssl_certificate     /etc/nginx/tls/hq.crt;
    ssl_certificate_key /etc/nginx/tls/hq.key;
    keepalive_timeout 75s;

    location /v1/ {
        proxy_pass http://127.0.0.1:18080;     # Egress 的 http.listen
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_set_header Host $host;
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_read_timeout 120s;
        proxy_send_timeout 120s;
        client_max_body_size 2m;
    }
}
```

由代理终结 TLS 时，Egress 的 `http.listen` 保持在回环地址上（无需 `http.tls`），Ingress 的 endpoint 指向 `https://hq.example.com`（端口 443，URL 中不写端口）。

### 7.1 经代理的反向隧道

反向模式下（[配置 §7.1](configuration.zh-CN.md#71-反向模式peersmode-与-reverse)），代理位于**公网侧** `http.listen` 之前，所有请求都由数据库侧发起。上面的 `location /v1/` 已经覆盖；反向路径如下：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET + Upgrade | `/v1/reverse` | WebSocket 隧道（`Sec-WebSocket-Protocol: dbbridge.reverse.v1.5`） |
| POST | `/v1/reverse/{tunnel}/open` | 建立 SSE 或长轮询隧道 |
| POST | `/v1/reverse/{tunnel}/up` | 数据库侧 → 公网侧的字节（≤ 128 KiB） |
| GET | `/v1/reverse/{tunnel}/events` | 公网侧 → 数据库侧的字节，SSE |
| GET | `/v1/reverse/{tunnel}/poll` | 同上，长轮询（约 25 秒内返回） |
| DELETE | `/v1/reverse/{tunnel}` | 关闭隧道 |

请原样转发 `Authorization`、`X-DBB-Node-ID`、`X-DBB-Expected-Node-ID`、`X-DBB-Instance-ID`、`X-DBB-Tunnel-ID`、`X-DBB-Tunnel-Priority`、`X-DBB-Reverse-Protocol`、`X-DBB-Tunnel-Transport`、`X-DBB-Offset`、`X-DBB-SHA256`、`X-DBB-Confirm-Up-Offset`、`X-DBB-Ack-Down-Offset`，以及响应头 `X-DBB-Node-ID`、`X-DBB-Instance-ID` 和 `Retry-After`。WebSocket 隧道是一条承载所有会话的长连接，代理的空闲超时必须大于 `heartbeat`，与会话相同；长轮询要求读超时大于 30 秒（上面的 120 秒即可）。同一数据库侧节点的所有隧道必须到达同一个公网侧进程：不要在没有会话保持的情况下把 `/v1/reverse/` 分散到多个后端。只有公网侧需要入站端口；数据库侧只需要出站 HTTPS（开启 `transport.use_environment_proxy` 时遵循 `HTTPS_PROXY`）。

## 8. TLS

**服务端（Egress）。** 设置 `http.tls.enabled: true` 及 `cert_file`、`key_file` 后，节点直接以 TLS 1.2 及以上提供 HTTPS/WSS。证书和私钥在 `check-config` 和启动时加载；更换证书后需重启才生效。私钥只允许服务账户读取（0600，或配合服务组使用 0640）。

**客户端（Ingress）。** `https://` endpoint 使用系统信任库或 `peers[].tls.ca_file` 校验，包括 endpoint URL 中的主机名。请使用 ASCII 主机名（国际化域名请转换为 `xn--` 形式），并使用恰好为这些名称签发的证书。`tls.verify: false` 对回环 endpoint 可以直接使用；对其他主机还需要 `tls.allow_insecure_skip_verify: true`，且只应在测试环境中使用。

**明文。** 非回环 `http.listen` 不启用 TLS 需要 `http.allow_insecure_listen: true`；非回环 `http://` endpoint 需要 `peers[].allow_insecure_http: true`。两者都会让 Bearer 令牌和数据库流量在网络上明文传输。请优先在节点上启用 TLS，或使用本机的 TLS 终结代理。

**出站 HTTP 代理。** 除非设置 `transport.use_environment_proxy: true`，对端连接会忽略 `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY`。这些变量每个进程只读取一次。
