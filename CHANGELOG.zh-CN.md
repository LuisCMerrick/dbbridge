[English](CHANGELOG.md) | **简体中文**

# 变更日志

构建的版本号在构建时确定（见 `dbbridge version`）；在打出发布标签之前，变更说明都记录在 `[Unreleased]` 下。

## [Unreleased]

### 安全

- **一步完成吊销。** 把入站身份设为 `enabled: false`（或删除该身份）并重载，就会关闭它的所有会话，即使在同一次修改中删除了它的令牌文件或变量也是如此：停用身份的令牌来源不会被读取。
- **被拒绝的凭据重载清晰可见。** 以 ERROR 级别记录（`credential reload rejected ...`，`code=DBB-CFG-020`，附 `cause_code`），问题持续期间每 5 分钟重复一次，并通过 `dbb_auth_credentials_stale` 和 `dbb_auth_reload_last_success_timestamp_seconds` 指标暴露。
- **内联凭据无需重启即可重载。** 对于不使用凭据文件的配置（包括所有 `gen-config` 生成的配置），`auth.inbound_peers`、`peers[].token_file`/`token_env` 以及每个令牌文件都会每隔 `auth.reload_interval` 重新读取。`SIGHUP`、`systemctl reload <name>` 和 `sc.exe control <name> paramchange` 会立即触发重载。来自环境变量的令牌仍需重启；节点会在日志中列出受影响的变量。
- **Egress 上按身份分配预算份额**，涵盖请求元数据、编解码内存和建连记录：单个对端无法为所有其他对端耗尽这些预算。在双向节点上，远端对端无法使用为本地 listener 保留的元数据和编解码内存。
- `gen-config` 的 `--allow-insecure-http` 写入 `peers[].allow_insecure_http`，而不是范围更大、已弃用的 `transport.allow_insecure_remote_http`。
- 安装时的环境快照包含配置可能解析的每一个令牌变量。
- `check-config`、`run` 和 `install` 会对已安装服务看不到的、位于 `/home`、`/root`、`/run/user`、`/tmp` 或 `/var/tmp` 下的凭据文件给出警告。

### 修复

- **代理后的 WebSocket。** WebSocket 预检收到任何普通的代理或网关应答（重定向、407、413、429、网关 5xx 等）时，`auto` 会在同一 endpoint 上使用 SSE；只有该 endpoint 的 WebSocket 载体进入冷却，endpoint 不会被标记为 `MISMATCHED`。
- 第一条较慢的 WebSocket `ready` 消息在 `control_request_timeout` 内等待；`ws.handshake_timeout` 只限制升级过程。
- 出站连接预算：每个对端的数据连接上限为 `2 × max_sessions + 32`；等待连接名额的时间受请求自身超时的约束；本地预算耗尽按本地背压处理（重试，永不判定载体故障或触发冷却）。
- 编解码内存紧张时，小的 SSE 帧仍能正常读取。
- flow-limits 通告值超过 1,048,576 时被截断，而不会导致建连失败；仅在协商了该扩展的会话上响应 `ack_requested`。
- 关闭恢复时同样会对失败的载体进行冷却；本地原因（客户端重置、停机、本地期限和预算）永远不会让载体或地址进入冷却。
- 停机：在停机期限内并发停止会话；主 endpoint 不可用时，远端清理会经由备用 endpoint 完成。
- 恢复截止时间从故障时刻起算；首次接入受 Egress 的 `unattached_timeout` 约束。
- 载体故障时释放早期数据占用的编解码内存。
- Egress 结束会话时（数据库丢失、ACL 撤销、缓冲耗尽、帧冲突）会在载体上发送最后一条 `error` 消息，让 Ingress 立即关闭客户端，而不是做徒劳的恢复。
- 目标的 DNS 解析失败报告为 `502 DBB_DIAL_FAILED`（计入 `dbb_dial_failed_total`）；被停机打断的建连得到 `503 DBB_DRAINING`。
- 格式错误的 WebSocket 升级请求得到 `400 DBB_BAD_FRAME`，会话仍可接入。
- 过大的 SSE 上行帧得到 `413 DBB_FRAME_TOO_LARGE`。
- FIN 和数据冲突以 `DBB_FRAME_CONFLICT` 关闭会话；授权竞态以 `DBB_TARGET_FORBIDDEN` 关闭会话。
- 对进行中或已失败的建连的重试，直接从幂等表应答（`202 DBB_CREATE_PENDING` 或缓存的应答），而不是返回临时性的配额错误。
- 对端已确认 FIN 之后最后一次 FIN 写入失败的会话按正常关闭结束；声称已收到从未发送过的 FIN 的恢复请求会被拒绝。
- 上行数据被数据库写入阻塞时，`/ack` 报告 `backpressure_reason: local_socket_write`。
- Windows：配置错误会以服务特定退出码 78 停止内置服务，且不会重启；用户注销不会停止在控制台中运行的节点。
- `install` 遇到配置和凭据错误时退出码为 78，与 `check-config` 一致。
- `install` 写入调用时使用的可执行文件路径（保留符号链接）或 `--exec-path`；`uninstall` 会等待完整排空。
- 所有指标族都带有 `# HELP` 行且顺序稳定。回退按每个创建的会话计一次；恢复耗时按每次恢复尝试和 endpoint 记录；`dbb_ingress_bound_sessions` 只统计有活动载体的会话。

### 变更

- **派生默认值：** `ack_batch_bytes = min(1MiB, buffer_per_direction/4)`、`max_frame_size = min(256KiB, up_window_bytes)`、`frame_size = min(32KiB, max_frame_size)`、`suspended_buffer_limit = min(64MiB, global_buffer_limit/4)`、`recovery.barrier_timeout = min(5s, grace/2)`、`failover.healthcheck_timeout = min(3s, healthcheck_interval/2)`。不修改其他配置项时，所有默认值与之前相同。约束错误会标注默认值（`..., default`）。
- **稳定的错误码** `DBB-CFG-001` … `DBB-CFG-030`，出现在 CLI 错误（`Error [DBB-CFG-002]: ...`）和日志记录（`code=`）中。退出码：0、78（配置）、130（强制退出）、1（其他）。
- `gen-config` 在回环地址上启用 admin 端点（A 为 `127.0.0.1:18090`，B 为 `127.0.0.1:18091`；选项 `--a-admin-listen`、`--b-admin-listen`）。
- 新增 `install --exec-path`。每个命令有自己的帮助（`run --help`、`check-config --help`、`install --help`）。
- Ingress 在 50 s 后丢弃空闲的对端连接；反向代理应让空闲的客户端连接保持至少 60 s。
- 只允许 WebSocket 的纯 Egress 节点所需的最小 `codec_buffer_limit` 更小（`max(2 × max_frame_size + 56, 16 KiB)`）。
- 新增终止原因 `shutdown`、`local_socket`、`dial_failed`（Egress 无法连到数据库）和 `draining`（Egress 正在停机）；清理事件报告 `not_needed` 和 `skipped`。未开始恢复时，`session terminated` 日志中的 `deadline` 为空。正常关闭和停机时的关闭不计为失败；`dbb_terminated_direct_total` 统计未开启恢复时失败的会话，`dbb_terminated_after_resume_fail_total` 统计开启恢复时失败的会话。
- 新增指标 `dbb_auth_credentials_stale`、`dbb_auth_reload_last_success_timestamp_seconds`、`dbb_ingress_recovering_sessions`。`dbb_auth_reload_success_total` 统计启动后有变化的快照（不含首次加载）。
- SSE 上行帧因编解码预算被拒绝时，消息为 `codec budget`。
- 对强制单一载体且只有一个 endpoint 的对端，如果 `recovery.grace` 不大于 `failover.cooldown + barrier_timeout`，恢复不可能成功，`check-config` 会对此给出警告。
- 样例：所有样例都在回环地址上启用 admin（`configs/peer-b*.yaml` 使用 18090）；生产样例使用 `tokens/` 下的令牌文件；`configs/peer-b.yaml` 只列出一个令牌变量。
- 生成的和样例中的 systemd 单元带有 `ExecReload`。WinSW 样例重启两次后保持停止。
- 发布：按平台提供发布包（`.tar.gz`，Windows 为 `.zip`），包含二进制、文档、样例和服务文件；`SHA256SUMS` 覆盖包括 `BUILD-INFO.txt` 在内的所有产物；基于 `SOURCE_DATE_EPOCH` 可复现构建；带后缀的标签发布为预发布版本。每次推送和拉取请求都运行 CI，包括 Windows 任务。
- 文档提供英文和简体中文两种语言；`docs/configuration-transport.md` 由 `docs/transport.md` 取代，并新增 `docs/installation.md` 和 `docs/security.md`。

### 升级说明

- 本版本的节点可以与此前支持协议 1.4 的版本互通；请逐个升级节点（[安装 §8](docs/installation.zh-CN.md#8-升级)）。
- **重新安装已安装的服务**（先 `dbbridge uninstall`，再用相同选项 `dbbridge install`），以便在 systemd 单元中获得 `ExecReload`，并让 Windows 服务在配置错误时不再重启。在此之前，服务保持安装时的定义。
- 依赖 `install` 在配置错误时返回 1 的自动化脚本，需要改为接受 78。
- 对主配置文件中 `auth.inbound_peers` 的修改会在下一次重载时（`auth.reload_interval` 内）生效，无需重启。保存修改前请先用 `check-config` 校验。
- 如果反向代理对空闲客户端连接的 keep-alive 低于 60 s，请调大。
- 看板与告警：加入 `shutdown`、`local_socket`、`dial_failed`、`draining`、`not_needed` 和 `skipped` 原因；复核基于 `dbb_terminated_direct_total` / `dbb_terminated_after_resume_fail_total`、`dbb_auth_reload_success_total` 以及 `peer codec budget` 消息的告警。已弃用的 `dbb_resume_success`、`dbb_resume_failed` 和 `dbb_client_close_count` 仍会输出；请改用 `_total` 名称。
- 如果只设置了 `buffer_per_direction` 而没有设置 `ack_batch_bytes`，派生的 `ack_batch_bytes` 为其四分之一；如需其他值，请显式设置 `ack_batch_bytes`。
- 可持续的新建会话速率受建连记录预算限制，默认约每秒 10 个；大量短连接的负载应调大 `transport.metadata_limit`（[运维 §10](docs/operations.zh-CN.md#10-容量规划)）。
