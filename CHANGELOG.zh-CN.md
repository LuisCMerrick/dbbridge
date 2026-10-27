[English](CHANGELOG.md) | **简体中文**

# 变更日志

构建的版本号在构建时确定（`dbbridge version`）。

## [1.4.26.10.27.2] - 2026-10-27

协议 1.4，与 1.4.26.10.25 / 1.4.26.10.27 节点线上兼容（新旧节点可任意混用，新增行为靠响应头协商）。新连接首包延迟优化。

- **新连接从约 7–8 个往返降到 3 个。** 以前每个新的本地连接在第一个字节到达数据库前要与 Egress 往返：probe、create、TCP（+TLS）握手、WebSocket 升级、ready ACK、ping ACK，再加数据本身。100 ms RTT 的模拟链路上，热状态下一个短 HTTP 请求从 712 ms 降到 306 ms（ws）/ 406 ms（http-sse，HTTP/1.1）。
  - 新会话复用健康检查最近一次成功的探测结果（≤ 1.5 × `healthcheck_interval`，≤ 15 s），健康检查改为启动时立即执行一次。Egress 重启导致实例不符时，create 被拒后重新探测再建，不会失败。
  - 每次 WebSocket 会话拨号后预建一条备用连接（TCP+TLS），4 s 内的下一个会话直接用它升级；不用则 4 s 后关闭、不续建。被服务端关掉的备用连接会被识别，失败时自动改用全新拨号。
  - Egress 在 WS 升级 / SSE 事件流应答中声明 `X-DBB-Implicit-Ready: 1`，首个上行帧或 FIN 即可激活新会话的数据面，Ingress 不再等 ready ACK 返回才发数据。旧版 Egress 不带该头，Ingress 照旧等待。
  - 新会话不再等待第一个心跳 ping 及其确认才开始传数据（只在恢复时需要）。
  - WS 预检成功结果在使用期间由健康检查提前续期，过期 5 分钟内先用旧结果、后台重检，安静一段时间后的第一个连接不再等预检。
- **测试。** 新增 `TestWarmSessionRoundTrips`（延迟注入代理，按 RTT 数断言）、`TestReusedProbeInstanceMismatchRecovers`、`TestWSSpareDroppedByServer`、`TestActivateImplicit`。

## [1.4.26.10.27] - 2026-10-27

协议 1.4，与 1.4.26.10.25 节点线上兼容。优化评审中的数据面性能改进，以及一个卡顿修复。

- **修复：服务端先关闭后卡顿 5 秒。** 后端先关闭连接时（HTTP `Connection: close`、短连接），Egress 可能在 Ingress 收到上行 FIN 确认之前就关闭载体；Ingress 把这次正常关闭当成载体故障，让该 endpoint 的传输冷却 `failover.cooldown`（5 秒），下一个新会话约等待 5 秒。环回测试中约三分之一的短连接受影响。现在只要任一 FIN 确认尚未到达，Ingress 都会通过控制通道再确认一次关闭（以前只覆盖下行 FIN 确认缺失的情况）。
- **上行摘要只校验一次。** 上行帧在 Egress 上最多做三次 SHA-256（WebSocket 解码、传输处理函数、会话内持锁再算一次）。现在每种传输只校验一次，且在会话锁之外；会话直接接管传输层的缓冲区，不再复制。
- **下行帧只复制一次。** Egress 发送端以会话窗口视图的形式读取帧，直接编码进 WebSocket 帧或 SSE 事件（SHA-256 与 Base64 按分段流式计算），不再先把每帧从窗口复制出来。SSE 事件改为一次构造成字节缓冲，不再使用 `fmt` 和字符串拼接。
- **Ingress 缓冲区。** 本地套接字读缓冲按 2 的幂分档复用（HTTP 发送失败过的缓冲永不复用）；WebSocket 帧以头部加负载通过同一个消息写入器写出，不再先拼接；SSE 帧行按声明的长度一次分配，不再逐步扩容。
- **Egress 后端读取** 被全局负载预算阻塞时，改为等待预算的释放通知（另有 250 ms 兜底轮询），不再每 10 ms 轮询一次。
- **基准测试。** 新增 `BenchmarkByteWindowDownstream`、`BenchmarkIngestUpFrame` 和环回端到端 `BenchmarkForward`（ws、http-sse）。环回、2 vCPU arm64，优化前 → 后：http-sse 140 → 250 MB/s，分配字节减少 54%；ws 吞吐不变（约 280 MB/s，瓶颈在 SHA-256 与系统调用），分配字节减少 45%；64 KiB 上行帧入队 140 → 65 µs。
- **文档。** 容量规划一节说明何时以及如何设置 `GOMEMLIMIT`（默认不设置）。

编解码预算的计费口径不变（保守）：新路径实际占用的内存少于其预留。

## [1.4.26.10.25] - 2026-10-25

协议 1.4，与 1.4.26.10.23 节点兼容。停止流程检查。

- **User-Agent 可配置。** 新增 `transport.user_agent`；发往对端的每个请求和 WebSocket 握手都使用它，不再是 Go 默认的 `Go-http-client/1.1`/`2.0`。默认 `DBBridge/1.4`。仅可打印 ASCII，最长 256 字节。
- **停止进度。** 停止时等待已打开会话（`transport.shutdown_timeout`，默认 30 秒；连接池中的数据库连接通常会让它等满）会立即并每 5 秒记录正在等待的内容、剩余时间，并提示再按一次 Ctrl-C 可强制退出；等待超时时记录关闭了多少会话。以前第一次 Ctrl-C 看起来像卡死。
- **修复。** 健康检查探测如果根本没有离开本节点（等待本地连接或请求预算超时），会被记为 endpoint 失败，本地资源争用可能把健康的 endpoint 标为 UNHEALTHY 并进入冷却。本地资源错误不再改变 endpoint 健康状态。
- **测试。** `TestLargeBidirectionalByteStream` 改为按数据块限制无进展时间，不再要求 50 MiB 双向传输在 15 秒内完成，较慢机器上的 race 构建不会再因吞吐而让正确性测试失败。

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
