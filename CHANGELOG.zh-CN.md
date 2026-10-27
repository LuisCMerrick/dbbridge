[English](CHANGELOG.md) | **简体中文**

# 变更日志

构建的版本号在构建时确定（`dbbridge version`）。

## [1.5.26.10.27.2] - 2026-10-27

协议 1.5，与 1.5.26.10.25 / 1.5.26.10.27 节点线上兼容（新旧节点可任意混用，新增行为靠响应头协商）。新连接首包延迟优化。

- **新连接从约 7–8 个往返降到 3 个。** 以前每个新的本地连接在第一个字节到达数据库前要与 Egress 往返：probe、create、TCP（+TLS）握手、WebSocket 升级、ready ACK、ping ACK，再加数据本身。100 ms RTT 的模拟链路上，热状态下一个短 HTTP 请求从 712 ms 降到 306 ms（ws）/ 406 ms（http-sse，HTTP/1.1）。
  - 新会话复用健康检查最近一次成功的探测结果（≤ 1.5 × `healthcheck_interval`，≤ 15 s），健康检查改为启动时立即执行一次。Egress 重启导致实例不符时，create 被拒后重新探测再建，不会失败。
  - 每次 WebSocket 会话拨号后预建一条备用连接（TCP+TLS），4 s 内的下一个会话直接用它升级；不用则 4 s 后关闭、不续建。被服务端关掉的备用连接会被识别，失败时自动改用全新拨号。
  - Egress 在 WS 升级 / SSE 事件流应答中声明 `X-DBB-Implicit-Ready: 1`，首个上行帧或 FIN 即可激活新会话的数据面，Ingress 不再等 ready ACK 返回才发数据。旧版 Egress 不带该头，Ingress 照旧等待。
  - 新会话不再等待第一个心跳 ping 及其确认才开始传数据（只在恢复时需要）。
  - WS 预检成功结果在使用期间由健康检查提前续期，过期 5 分钟内先用旧结果、后台重检，安静一段时间后的第一个连接不再等预检。
- **测试。** 新增 `TestWarmSessionRoundTrips`（延迟注入代理，按 RTT 数断言）、`TestReusedProbeInstanceMismatchRecovers`、`TestWSSpareDroppedByServer`、`TestActivateImplicit`。

## [1.5.26.10.27] - 2026-10-27

协议 1.5，与 1.5.26.10.25 节点线上兼容。优化评审中的数据面性能改进，以及一个卡顿修复。

- **修复：服务端先关闭后卡顿 5 秒。** 后端先关闭连接时（HTTP `Connection: close`、短连接），Egress 可能在 Ingress 收到上行 FIN 确认之前就关闭载体；Ingress 把这次正常关闭当成载体故障，让该 endpoint 的传输冷却 `failover.cooldown`（5 秒），下一个新会话约等待 5 秒。环回测试中约三分之一的短连接受影响。现在只要任一 FIN 确认尚未到达，Ingress 都会通过控制通道再确认一次关闭（以前只覆盖下行 FIN 确认缺失的情况）。
- **上行摘要只校验一次。** 上行帧在 Egress 上最多做三次 SHA-256（WebSocket 解码、传输处理函数、会话内持锁再算一次）。现在每种传输只校验一次，且在会话锁之外；会话直接接管传输层的缓冲区，不再复制。
- **下行帧只复制一次。** Egress 发送端以会话窗口视图的形式读取帧，直接编码进 WebSocket 帧或 SSE 事件（SHA-256 与 Base64 按分段流式计算），不再先把每帧从窗口复制出来。SSE 事件改为一次构造成字节缓冲，不再使用 `fmt` 和字符串拼接。
- **Ingress 缓冲区。** 本地套接字读缓冲按 2 的幂分档复用（HTTP 发送失败过的缓冲永不复用）；WebSocket 帧以头部加负载通过同一个消息写入器写出，不再先拼接；SSE 帧行按声明的长度一次分配，不再逐步扩容。
- **Egress 后端读取** 被全局负载预算阻塞时，改为等待预算的释放通知（另有 250 ms 兜底轮询），不再每 10 ms 轮询一次。
- **基准测试。** 新增 `BenchmarkByteWindowDownstream`、`BenchmarkIngestUpFrame` 和环回端到端 `BenchmarkForward`（ws、http-sse）。环回、2 vCPU arm64，优化前 → 后：http-sse 140 → 250 MB/s，分配字节减少 54%；ws 吞吐不变（约 280 MB/s，瓶颈在 SHA-256 与系统调用），分配字节减少 45%；64 KiB 上行帧入队 140 → 65 µs。
- **文档。** 容量规划一节说明何时以及如何设置 `GOMEMLIMIT`（默认不设置）。

编解码预算的计费口径不变（保守）：新路径实际占用的内存少于其预留。

## [1.5.26.10.25] - 2026-10-25

协议 1.5，与 1.5.26.10.24 节点兼容。修复 1.5.26.10.24 审计问题，以及停止流程检查。

- **安全修复（审计 AUD-01）。** 在凭据重载轮换令牌之前刚通过认证的反向隧道握手，仍可能在重载检查之后完成登记。现在登记时在 Hub 锁内重新校验令牌，此类请求被拒绝（`401`）。
- **修复（AUD-02、AUD-08）。** 隧道内存改为按打开的流计费（每个流一个接收窗口：codec_buffer_limit/128，256 KiB–1 MiB），不再为每条空闲隧道预留整块接收缓冲；smux 会话总接收额度按 1024 个满窗口设置。空闲隧道不会再耗尽较小的编解码预算（`codec_buffer_limit: 2MiB` 下两条隧道无法承载第一个会话），隧道占用不会挤占本地 ingress 预留，慢数据流也不会再阻塞同一隧道上的控制流。数据库侧 `check-config` 要求预算能容纳两个流窗口（`reverse.transport` 不是 `ws` 时再加一个 HTTP 载体）。
- **修复（AUD-03）。** `/v1/reverse/...` 请求纳入准入：握手、open 和关闭占控制请求槽，上传占数据请求槽，均计入请求元数据并按反向 peer 公平分配；上传请求体计入编解码预算。预算耗尽时返回可重试的 `429`/`503`。
- **修复（AUD-04）。** `install` 现在会快照 `reverse.token_env`，并把 `reverse.token_file`、`reverse.tls.ca_file` 纳入服务文件读取检查（及 systemd 沙箱路径推导）。
- **修复（AUD-05）。** 共用令牌的多个反向 peer 改为按隧道提供的节点 ID 区分（此前取 ID 排序后最后一个匹配者，返回 `403 DBB_NODE_MISMATCH`）；HTTP 隧道请求绑定到隧道自身的令牌。反向 peer 的 `expected_node_id` 现在必须互不相同。
- **修复（AUD-07）。** 关闭反向 WSS 隧道时先关闭底层 socket，黑洞链路不会再因 TLS close_notify 阻塞重连和清理 5 秒。
- **修复（AUD-09）。** HTTP 隧道上传的偏移加长度溢出时按偏移缺口拒绝，不再当作空重传。
- **User-Agent 可配置。** 新增 `transport.user_agent`；发往对端的每个请求和 WebSocket 握手（含反向隧道）都使用它，不再是 Go 默认的 `Go-http-client/1.1`/`2.0`。默认 `DBBridge/1.5`。仅可打印 ASCII，最长 256 字节。
- **停止进度。** 停止时等待已打开会话（`transport.shutdown_timeout`，默认 30 秒；连接池中的数据库连接通常会让它等满）会立即并每 5 秒记录正在等待的内容、剩余时间，并提示再按一次 Ctrl-C 可强制退出；等待超时时记录关闭了多少会话。以前第一次 Ctrl-C 看起来像卡死。
- **修复。** 健康检查探测如果根本没有离开本节点（等待本地连接或请求预算超时），会被记为 endpoint 失败，本地资源争用可能把健康的 endpoint 标为 UNHEALTHY 并进入冷却。本地资源错误不再改变 endpoint 健康状态。
- **测试。** `TestLargeBidirectionalByteStream` 改为按数据块限制无进展时间，不再要求 50 MiB 双向传输在 15 秒内完成，较慢机器上的 race 构建不会再因吞吐而让正确性测试失败。

## [1.5.26.10.24] - 2026-10-24

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
