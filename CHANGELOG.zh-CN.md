[English](CHANGELOG.md) | **简体中文**

# 变更日志

构建的版本号在构建时确定（`dbbridge version`）。

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
