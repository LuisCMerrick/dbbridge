[English](CHANGELOG.md) | **简体中文**

# 变更日志

构建的版本号在构建时确定（`dbbridge version`）。

## [Unreleased] - 协议 1.5，反向模式

协议 1.5。**与 1.4 不兼容：**两端必须同时升级；1.4 节点会收到 `DBB_PROTOCOL_MISMATCH`，WebSocket 子协议改为 `dbbridge.v1.5`。

- **反向模式配置。** 新增 `peers[].mode`（`forward`，默认；或 `reverse`）和顶层 `reverse` 段（`enabled`、`peer_node_id`、`token_file`/`token_env`、`transport` `auto|ws|http-sse|long-poll`、`tunnels_per_endpoint`、`endpoints`、`reconnect.min_backoff`/`max_backoff`、`tls`、`allow_insecure_http`），含默认值与校验。一个节点可以同时有正向和反向 peer。`check-config` 新增稳定错误码 `DBB-CFG-007`（peer 模式）和 `DBB-CFG-008`（`reverse` 段），退出码均为 78；`recovery.grace` 容不下隧道重连时给出警告。
- **WebSocket 反向隧道。** 数据库侧节点拨 `GET /v1/reverse`（子协议 `dbbridge.reverse.v1.5`），每个 endpoint 按优先级保持 `tunnels_per_endpoint` 条隧道，断线后立即重连，之后按指数退避加抖动。隧道内用 smux v2 多路复用（新依赖 `github.com/xtaci/smux` v1.5.57）；每个流就是到 Egress HTTP 服务的一条连接，会话、探测、故障切换与恢复都不变，数据库侧不需要监听端口。两端都校验节点身份；公网侧以常量时间比较令牌，拒绝冒充（`403 DBB_NODE_MISMATCH`）、重复隧道 ID 和第二个存活实例（新错误码 `409 DBB_TUNNEL_CONFLICT`），凭据重载轮换或吊销令牌后关闭用旧令牌建立的隧道。隧道流共享入站连接预算，按隧道而不是按 IP 计数；隧道接收缓冲计入编解码预算。
- **SSE 与长轮询反向隧道。** `reverse.transport: http-sse` 与 `long-poll` 用数据库侧发起的普通 HTTPS 请求承载 smux 字节流：`POST /v1/reverse/{tunnel}/open`、`POST …/up`（偏移加 SHA-256，可重传去重），以及 `GET …/events`（SSE）或 `GET …/poll`（长轮询，约 25 秒）。两端保留未确认的字节并重发，HTTP 连接被断开既不丢也不重，也不会断开隧道。请求绑定到建立隧道时的令牌和实例；公网侧已不存在的隧道返回新错误码 `404 DBB_TUNNEL_NOT_FOUND`。`auto` 现在按 endpoint 依次回退 WebSocket → SSE → 长轮询（遇到重定向、404、426、网关错误等非 DBBridge 应答，或 SSE 首个事件未在 `transport.ws.handshake_timeout` 内到达时回退；401、DBBridge 给出的 403 和网络错误不回退），10 分钟后重新尝试 WebSocket。到达公网侧时已丢失升级头的 WebSocket 握手现在返回 `426 DBB_TRANSPORT_UNSUPPORTED`（原为 `400 DBB_BAD_FRAME`）。
- **反向模式工具与可观测性。** `dbbridge gen-config --reverse`（配合 `--mode a-to-b` 或 `b-to-a`）生成匹配的反向配置对：有监听器的节点成为公网侧，有数据库的节点主动拨入且不开任何端口。新增指标：公网侧 `dbb_reverse_hub_tunnels{peer,transport}`、`dbb_reverse_hub_accepted_total`、`dbb_reverse_hub_rejected_total{code}`；数据库侧 `dbb_reverse_tunnels{endpoint,transport}`、`dbb_reverse_tunnel_established_total`、`dbb_reverse_tunnel_lost_total`、`dbb_reverse_tunnel_dial_failures_total`、`dbb_reverse_tunnel_fallback_total{from,to}`。只通过隧道提供服务的数据库侧，在隧道建立前 `/readyz` 返回 `503`；公网侧保持就绪（负载均衡不能摘掉隧道要拨入的地址），并在响应体中列出没有隧道的反向 peer。文档：反向代理（传输 §7.1）、安全模型（安全 §9）、指标、错误码与故障排查（运维）、部署示例 5 和 6。
- **修复。** 隧道连接断开后，公网侧要等到 smux keepalive 超时才发现（smux 在读错误时不会关闭会话），期间仍在失效隧道上开流并把它报告为存活。现在连接一断即结束该隧道。
- **修复。** Egress WebSocket 读取端收到 gorilla 的 `close sent` 错误（本端已发出 close 帧后对端的 close 帧才到达）时，原先当作帧违规并以 `DBB_FRAME_CONFLICT` 结束会话，现在按载体中断处理，会话可以恢复。
- **修复。** WebSocket 会话读取在连接结束时遇到被截断的 TLS 记录（裸 `unexpected EOF`，反向隧道流结束时即如此）被误判为帧违规并结束会话，而不是进入恢复。完整的 WebSocket 消息但其中 DBBridge 帧不完整，仍按违规处理。
- **修复。** 隧道流现在具有精确的写入语义：smux 在写超时时报告未写出，但排队的帧仍会发出，调用方重试剩余部分会在流上产生重复字节。现在写入会复制后无超时地交给 smux，超时只限制等待上一块完成的时间。
- **内部结构。** 对端连接改由 `PeerDialer` 打开（正向 peer 为 TCP，反向 peer 为隧道流），per-peer 与全局连接预算对两者同样生效；Egress HTTP 服务同时在基于隧道的虚拟 `net.Listener` 上提供服务。

## [1.4.26.10.23] - 2026-10-23

协议 1.4。维护与协议加固版本。

- **SSE 与载体活性检测。** SSE 注释行（如反向代理保活注释）不再计入会话进度或事件行数上限。`http-sse` 上行帧在等待确认期间与 WebSocket 一致地执行 `failure_detect_timeout` 与远端背压超时检测。
- **连接池与流控。** 出站 SSE 订阅按每个端点最多 `transport.h2_pool_size`（1–64）个 HTTP 客户端分片。Ingress 本地短读会压缩底层切片并归还多余的负载与恢复保留配额，动态帧大小在窗口尾端自动钳制到剩余发送窗口。
- **预算与准入控制。** 双向节点的 `codec_buffer_limit` 下限与本地 Ingress 编解码预留量同时覆盖 SSE 帧解码与 Egress 最大帧开销。Egress 会话路由在读取请求体前拒绝 HTTP/1.1 请求时显式关闭连接。
- **配置与凭据热重载。** 内联凭据重载拒绝新增或重复的 `peers` 条目（增删对端需重启）。入站配额求和采用饱和加法防止整数溢出。服务安装预检不再要求已禁用内联身份的令牌文件存在。`gen-config` 在目录同步或配对发布失败时回滚新建文件。
- **可观测性与服务管理。** 新增 `dbb_ingress_faults_total{source,reason}` 指标，补齐确定性协议拒绝码的致命判定与原因分类。在 HTTP 101 升级切换窗口内关闭的 WebSocket 会话仍会在关闭载体前下发终态 `error` 通知。Windows 服务安装为事件日志源写入并校验归属标记。
- **发布打包与工具链。** `scripts/build-release.sh` 在发布归档中包含所链接依赖的许可证文件与模块清单（`third_party/MODULES.txt`）。最低 Go 工具链版本升至 1.27.3（修复标准库 `GO-2026-6809`）。

## [1.4.26.10.20] - 2026-10-20

首个版本。协议 1.4。

- **角色。** 同一个程序按配置充当 Ingress（本地 TCP 监听）、Egress（数据库前的 HTTP 服务）或两者兼任。
- **载体。** WebSocket，或 HTTP POST + Server-Sent Events；`auto` 在代理无法承载 WebSocket 时于同一端点回退到 SSE。
- **故障切换与恢复。** 每个 peer 可配置多个带优先级的端点，支持健康探测和故障路径冷却。启用恢复的会话在 `recovery.grace` 内可跨越载体故障，不丢失也不重复任何字节；否则立即关闭客户端连接。
- **安全。** TLS、bearer token 身份、对端节点身份校验、按身份的目标 ACL 与配额、目标地址 CIDR 白名单、按身份的预算份额。
- **凭据重载。** 每隔 `auth.reload_interval`、收到 `SIGHUP` / `systemctl reload` 或 `sc.exe control <name> paramchange` 时重新读取 token 文件和凭据设置；停用或删除身份会关闭其会话。被拒绝的重载保留之前的凭据，并记录日志、暴露为指标。
- **资源有界。** 内存、会话元数据、连接和并发均有预算；过载时返回明确的可重试错误。
- **运维。** `gen-config` 生成配对配置，`check-config` 输出稳定错误码（`DBB-CFG-001` … `DBB-CFG-030`），配置错误退出码为 78；支持 systemd 和 Windows 服务安装；回环管理端点提供 `/healthz`、`/readyz` 和 Prometheus `/metrics`。
- **发布。** 提供 Linux amd64/arm64 和 Windows amd64 安装包（附所链接依赖的许可证文件和模块清单）及 `SHA256SUMS`；基于 `SOURCE_DATE_EPOCH` 的可重现构建。
