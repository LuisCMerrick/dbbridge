[English](CHANGELOG.md) | **简体中文**

# 变更日志

构建的版本号在构建时确定（`dbbridge version`）。

## [1.4.26.10.23] - 2026-10-23

协议 1.4。维护与协议加固版本。

- **SSE 与载体活性检测。** SSE 注释行（如反向代理保活注释）不再计入会话进度或事件行数上限。`http-sse` 上行帧在等待确认期间与 WebSocket 一致地执行 `failure_detect_timeout` 与远端背压超时检测。
- **连接池与流控。** 出站 SSE 订阅按每个端点最多 `transport.h2_pool_size`（1–64）个 HTTP 客户端分片。Ingress 本地短读会压缩底层切片并归还多余的负载与恢复保留配额，动态帧大小在窗口尾端自动钳制到剩余发送窗口。
- **预算与准入控制。** 双向节点的 `codec_buffer_limit` 下限与本地 Ingress 编解码预留量同时覆盖 SSE 帧解码与 Egress 最大帧开销。Egress 会话路由在读取请求体前拒绝 HTTP/1.1 请求时显式关闭连接。
- **配置与凭据热重载。** 内联凭据重载拒绝新增或重复的 `peers` 条目（增删对端需重启）。入站配额求和采用饱和加法防止整数溢出。服务安装预检不再要求已禁用内联身份的令牌文件存在。`gen-config` 在目录同步或配对发布失败时回滚新建文件。
- **可观测性与服务管理。** 新增 `dbb_ingress_faults_total{source,reason}` 指标，补齐确定性协议拒绝码的致命判定与原因分类。Windows 服务安装为事件日志源写入并校验归属标记。
- **发布打包。** `scripts/build-release.sh` 在发布归档中包含所链接依赖的许可证文件与模块清单（`third_party/MODULES.txt`）。

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
