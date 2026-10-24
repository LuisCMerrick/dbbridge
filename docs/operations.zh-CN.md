[English](operations.md) | **简体中文**

# 运维

[README](../README.zh-CN.md) · [安装](installation.zh-CN.md) · [配置](configuration.zh-CN.md) · [传输](transport.zh-CN.md) · [安全](security.zh-CN.md) · **运维** · [示例](examples.zh-CN.md)

- [1. CLI 参考](#1-cli-参考)
- [2. 退出码、信号与生命周期](#2-退出码信号与生命周期)
- [3. 重载语义](#3-重载语义)
- [4. Admin 端点](#4-admin-端点)
- [5. 指标](#5-指标)
- [6. 日志与事件](#6-日志与事件)
- [7. 对端协议错误码](#7-对端协议错误码)
- [8. 环境变量](#8-环境变量)
- [9. 配置错误码](#9-配置错误码)
- [10. 容量规划](#10-容量规划)
- [11. 故障排查](#11-故障排查)

## 1. CLI 参考

```
dbbridge <command> [options]
```

| 命令 | 用途 |
|---|---|
| `run` | 在前台或由服务管理器运行节点。 |
| `check-config` | 校验配置后退出。 |
| `gen-config` | 生成一对相互匹配的配置和共享令牌。 |
| `install` | 安装为 systemd 服务（Linux）或 Windows 服务。 |
| `uninstall` | 停止并删除由 `dbbridge install` 安装的服务。 |
| `version`（`-v`、`--version`） | 输出版本信息。 |
| `help`（`-h`、`--help`） | 输出帮助信息。 |

选项采用 Go flag 语法：一个或两个短横线，`--flag=value` 或 `--flag value`。不接受位置参数。每个命令只接受它自己的选项。不带参数时输出帮助信息，退出码为 1。`<command> --help` 把该命令的选项输出到 stderr，退出码为 0。

### `run`、`check-config`、`install`

| 选项 | 适用命令 | 默认值 | 说明 |
|---|---|---|---|
| `--config`、`-c` | 三者 | — | 必填。其所在目录是相对路径的基准，也是已安装服务的工作目录。 |
| `--log-level` | 三者 | `info` | `debug`、`info`、`warn`、`error`。`install` 会把它写入服务定义。 |
| `--log-format` | 三者 | `text` | `text` 或 `json`（生产环境推荐）。 |
| `--env-file` | 三者 | — | 服务凭据快照：最大 1 MiB 的 JSON 对象 `{"NAME": "value"}`，由 `install` 写入并自动传给服务。必须是普通文件，没有组/其他用户权限，属主为进程用户或 root。保留的变量名（例如 `PATH`、`HOME`、代理变量、`NOTIFY_SOCKET`、`LISTEN_*`、Go 运行时变量、`SSL_CERT_*`、`TZ`、`LD_*`）会被拒绝。环境中已设置且非空的同名变量优先于快照（两者不同时记录警告）。 |
| `--offline` | `check-config` | 关 | 校验结构、默认值、引用、配额和监听地址，但不读取令牌文件、环境变量、TLS 证书、私钥和 CA 文件。凭据文件本身仍会读取。 |
| `--service-name` | `run`、`install` | `dbbridge` | `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`，不带 `.service` 后缀。可在同一主机上安装多个实例。`run` 将其用作 Windows 事件日志的事件源。 |
| `--service-user` | `install` | Linux 为 `root`，Windows 为 `LocalSystem` | Linux 账户名或 uid；Windows 上为 `LocalSystem`、`LocalService` 或 `NetworkService`。 |
| `--exec-path` | `install` | 调用 dbbridge 时使用的路径 | 原样写入服务定义的可执行文件路径（保留符号链接）。必须是普通文件或指向普通文件的链接。 |
| `--start` | `install` | 关 | 安装后启动服务。 |
| `--dry-run` | `install` | 关 | 以 JSON 输出安装计划（含完整的 systemd 单元），不做任何修改。 |

输出：`check-config` 输出 `Config <path> is valid. Node ID: <id>`（使用 `--offline` 时为 `... is structurally valid (offline: ...)`），每条警告以 `Warning: ...` 输出到 stderr。错误以 `Error [DBB-CFG-0xx]: <message>` 输出，命令行错误则为 `Error: <message>`。

### `gen-config`

在输出目录中写入 `peer-a.yaml`、`peer-b.yaml` 和 `shared.token`。两份配置在写入前都会经过校验。

| 选项 | 默认值 | 说明 |
|---|---|---|
| `-o`、`--output` | `.` | 输出目录。 |
| `--mode` | `bidirectional` | `bidirectional`、`a-to-b`（A 有 listener，B 有数据库）或 `b-to-a`。总是同时生成两个节点。 |
| `--token-file` | `<output>/shared.token` | 共享令牌路径，相对于当前目录；配置中以相对于配置所在目录的路径引用它。已存在的令牌文件会被复用。 |
| `--protocol` | `auto` | `auto`、`ws`、`http-sse`。 |
| `--recovery` | `true` | `--recovery=false` 在两个节点上关闭会话恢复。 |
| `--allow-insecure-http` | 关 | 允许指向非回环主机的明文 `http://` 对端 URL（写入 `peers[].allow_insecure_http: true`）。 |
| `--force` | 关 | 替换已有的配置（从不替换令牌）。Linux 上被替换的文件保留原有权限位和属主；Windows 上新文件使用受保护 DACL，需要重新授权。 |
| `--reverse` | 关 | 反向模式，需配合 `--mode a-to-b` 或 `b-to-a`。有监听器的节点成为公网侧（一个 `mode: reverse` 的 peer，以及供隧道接入的 HTTP 监听，可使用 `--x-tls-cert`/`--x-tls-key`）；有数据库的节点成为数据库侧：不开监听端口，生成 `reverse` 段，其 endpoints 取自它的 `--x-peer-url`（默认是公网侧的 HTTP 监听地址），`reverse.transport` 取自 `--protocol`。 |
| `--max-sessions` | `200` | 两个节点的 `limits.max_sessions`。双向模式下入站身份取其一半。需要时会写入足够的 `transport.metadata_limit`。 |
| `--a-node-id`、`--b-node-id` | `A`、`B` | |
| `--a-listen`、`--b-listen` | `127.0.0.1:13306`、`127.0.0.1:13307` | 各节点的本地 listener。 |
| `--a-http-listen`、`--b-http-listen` | `127.0.0.1:18080`、`127.0.0.1:18081` | 入站 HTTP 监听地址。 |
| `--a-admin-listen`、`--b-admin-listen` | `127.0.0.1:18090`、`127.0.0.1:18091` | 回环 admin 端点（已启用）。 |
| `--a-target-id`、`--b-target-id` | `mysql`、`mysql` | Target ID；对方节点的 listener 引用它。 |
| `--a-target-address`、`--b-target-address` | `127.0.0.1:3306`、`127.0.0.1:3306` | 数据库地址。 |
| `--a-allow-cidr`、`--b-allow-cidr` | 目标的精确 IP（`/32` 或 `/128`） | 可重复。主机名目标必须指定。 |
| `--a-peer-url`、`--b-peer-url` | 对方节点的 HTTP 监听地址 | 设在发起连接的节点上：Ingress，使用 `--reverse` 时为数据库侧。可重复（故障切换 endpoint `link-1`、`link-2`……，优先级依次为 1、2……）。对方有证书时为 `https`；通配监听地址转换为回环地址。远程部署请显式指定。 |
| `--a-tls-cert`/`--a-tls-key`、`--b-tls-cert`/`--b-tls-key` | — | 节点入站 HTTPS 使用的现有证书和私钥；必须成对提供。 |

生成的配置使用带令牌文件的内联凭据（重载时重新读取）、两个方向使用同一令牌、`tls.verify: true`、admin 在回环地址上，listener ID 为 `database-local`。令牌为 32 个随机字节的十六进制编码，权限 0600（Windows 上为受保护 DACL），其值从不输出。

### `uninstall`

| 选项 | 默认值 | 说明 |
|---|---|---|
| `--service-name` | `dbbridge` | |
| `--dry-run` | 关 | 检查服务是否存在、是否由 DBBridge 安装，不做任何修改。 |

只删除带有 DBBridge 标记的服务。配置、凭据和服务环境快照都会保留。在 Linux 上，`uninstall` 为排空等待单元的停止超时再加 30 s（至少 2 分钟）。

### `version`

```
dbbridge version <version> (commit: <commit>, built: <date>, runtime: <go version>, protocol: 1.5)
```

发布构建带有发布版本号、源码标识和构建日期。直接 `go build` 时版本显示为 `1.5-dev`，提交和日期取自 Go 构建信息（无则为 `unknown`）。

## 2. 退出码、信号与生命周期

| 退出码 | 含义 |
|---|---|
| `0` | 成功，包括收到终止请求后正常停止。 |
| `78` | `run`、`check-config` 或 `install` 遇到配置或凭据错误（错误码 `DBB-CFG-001` 至 `DBB-CFG-008`），包括 `--env-file` 无法使用。 |
| `130` | 排空期间再次收到终止请求，强制退出。 |
| `1` | 其他所有情况：命令行错误（未知命令、选项值无效）、启动失败（例如地址已被占用，`DBB-CFG-010`）、运行中出现致命故障（`DBB-CFG-011`）、非配置原因导致的服务安装/卸载失败（`DBB-CFG-030`）。 |

**服务管理器。** 生成的 systemd 单元设置了 `Restart=on-failure`、`RestartSec=5s`、`RestartPreventExitStatus=78`，以及 300 s 内最多启动 10 次的启动限制：配置错误不会重试，其他失败会重试。内置的 Windows 服务把配置错误报告为服务特定退出码 78（`sc.exe query <name>` 显示 `SERVICE_EXIT_CODE 78`），写入一条事件日志错误，且不会重启；其他失败在 5 s 后重启（失败计数一天后清零）。

**启动。** 节点依次启动 admin 服务、入站 HTTP 服务和 listener，然后报告就绪（向 systemd 发送 `READY=1`，向 Windows SCM 报告 *Running*），`/readyz` 返回 200。

**停止。** 收到 `SIGINT`/`SIGTERM`（Linux）、Windows 控制台中的 Ctrl-C/Ctrl-Break，或 Windows SCM 的 Stop/Shutdown/PreShutdown 时：

1. `/readyz` 返回 503，向 systemd 发送 `STOPPING=1`，停止接受新的客户端连接和会话（新建连请求得到 `503 DBB_DRAINING`）。
2. 已建立的会话可以在 `transport.shutdown_timeout`（默认 30 s）内结束。
3. 收尾阶段并发关闭剩余会话（主 endpoint 不可达时通过备用 endpoint 做远端清理），再关闭 HTTP 和 admin 服务。收尾最多 15 s，因此整个停止过程不超过 `shutdown_timeout + 15 s`。
4. 排空期间再次收到终止请求，立即以退出码 130 退出。

**重载。** `SIGHUP`（Linux；`systemctl reload <name>` 会发送它）和 `sc.exe control <name> paramchange`（Windows 服务）会立即重新读取凭据，且永远不会停止节点。见 §3。

**致命故障。** 运行中某个组件失败（例如入站服务停止提供服务）时，`/readyz` 返回 503，节点排空后以退出码 1 退出（`DBB-CFG-011`），由服务管理器重启。

## 3. 重载语义

| | |
|---|---|
| 触发时机 | 每隔 `auth.reload_interval`（默认 5 s）；`SIGHUP` / `systemctl reload <name>`；Windows `sc.exe control <name> paramchange`。 |
| 读取内容 | 内联：从主配置读取 `auth.inbound_peers` 和已有 peer 的 `token_file`/`token_env`，以及其中引用的每个令牌文件。凭据文件：整个文件及其中引用的每个令牌文件。环境变量取自不变的进程环境。 |
| 生效方式 | 新快照按与启动时相同的规则校验（ACL 目标、配额、`credential_ref`、重复令牌）后原子替换。被停用或删除的身份的会话，以及其 `allowed_targets` 中被移除的目标上的会话，会以 `DBB_TARGET_FORBIDDEN` 关闭。按身份分配的预算份额随新的启用身份集合调整。 |
| 不生效的内容 | 其他所有设置，见[配置 → 运行中重新读取的内容](configuration.zh-CN.md#16-运行中重新读取的内容)。增删 peer 或新增 `auth.credentials_file` 会让重载以 “... requires a restart” 失败。 |
| 候选被拒绝 | 之前的凭据继续生效，包括候选中停用或删除的身份。日志 `ERROR credential reload rejected; ...`，附带 `code=DBB-CFG-020`、`cause_code=<DBB-CFG-0xx>`、`trigger=interval` 或 `request` 以及 `err`。同一原因只记录一次，问题持续期间每 5 分钟重复一次，每次显式请求也会记录。`dbb_auth_credentials_stale` 为 1，`dbb_auth_reload_failed_total` 增加。之后重载成功时记录 `INFO credential reload succeeded; the earlier reload error is resolved`。 |
| 日志 | `credentials reloaded`（有变化）、`credentials re-read; no change`（显式请求且无变化）、`SIGHUP received; re-reading credentials`。 |

## 4. Admin 端点

`admin.enabled: true` 时在 `admin.listen` 上提供服务，永远不会出现在对端端口上。没有 TLS，也没有认证。

| 路径 | 应答 |
|---|---|
| `/healthz` | 进程运行时返回 `200 ok`。 |
| `/readyz` | 启动成功后返回 `200 ready`；启动前、排空期间和致命故障后返回 `503 not ready`。反向模式：开启 `reverse.enabled` 且未开启 `http.enabled` 的数据库侧，在没有存活隧道时返回 `503`（响应体附 `reverse: no tunnel to <peer_node_id>`）；开启了 `http.enabled` 时保持 `200`，只附加这一行。公网侧不会因缺少隧道变为未就绪，因为负载均衡一旦把它摘除，数据库侧也就无法接入；它只附加 `reverse peers without a tunnel: <ids>`。 |
| `/metrics` | Prometheus 文本格式 0.0.4，渲染结果缓存 1 s。 |

限制：最多 16 个并发连接（多余的直接关闭），读请求头超时 5 s，读/写超时 10 s，空闲超时 30 s，请求头 ≤ 8 KiB。

## 5. 指标

所有指标族都有 `# HELP` 和 `# TYPE` 行，输出顺序稳定。计数端：**E** = 节点作为 Egress 时计数（入站 HTTP、数据库一侧），**I** = 作为 Ingress 时计数（listener、客户端一侧），**两端** = 均可。在单向节点上，由另一端计数的指标始终为 0；例如纯 Ingress 节点上的 `dbb_up_bytes_total` 始终为 0。

### 会话与流量

| 指标 | 类型 | 计数端 | 含义 |
|---|---|---|---|
| `dbb_active_sessions` | gauge | E | 未挂起的 Egress 会话数。 |
| `dbb_suspended_sessions` | gauge | E | 为等待恢复而挂起的 Egress 会话数。 |
| `dbb_up_bytes_total` | counter | E | 写入数据库的字节数。 |
| `dbb_down_bytes_total` | counter | I | 写入本地客户端的字节数。 |
| `dbb_post_count_total` | counter | E | 收到的 SSE 上行 POST 数。 |
| `dbb_piggyback_ack_total` | counter | E | 随数据消息携带的确认数。 |
| `dbb_explicit_ack_total` | counter | E | 收到的 `/ack` 请求数。 |
| `dbb_up_fin_applied_total` | counter | E | 已应用到数据库连接的上行 FIN 数。 |
| `dbb_down_fin_acked_total` | counter | E | 已被对端确认的下行 FIN 数。 |
| `dbb_dial_total` | counter | E | 数据库拨号次数。 |
| `dbb_dial_failed_total` | counter | E | 数据库拨号失败次数，含 DNS 失败。 |
| `dbb_egress_carrier_sessions{transport}` | gauge | E | 按载体（`ws`、`http-sse`）统计的 Egress 会话数。 |
| `dbb_egress_state_<STATE>` | gauge | E | 各状态的 Egress 会话数（`CREATING`、`ATTACHING`、`ACTIVE`、`DRAINING`、`SUSPENDED`、`RESUMING`、`CLOSED`）。仅在至少有一个该状态的会话时出现。 |
| `dbb_egress_up_pending_frames`、`dbb_egress_down_retained_frames`、`dbb_egress_digest_receipts`、`dbb_egress_down_unconfirmed_bytes` | gauge | E | 所有 Egress 会话的合计值。仅在至少有一个 Egress 会话时出现。 |
| `dbb_ingress_bound_sessions{peer,endpoint,transport,recovery}` | gauge | I | 有活动载体的 Ingress 会话数。 |
| `dbb_ingress_recovering_sessions` | gauge | I | 处于恢复窗口内、没有活动载体的 Ingress 会话数。 |
| `dbb_ingress_up_pending_bytes` | gauge | I | 已发送但尚未确认的上行字节数。 |
| `dbb_ingress_retained_frames` | gauge | I | 为重放而保留的上行帧数。 |
| `dbb_ingress_connections` | gauge | I | 本地客户端连接数；至少有一条连接时出现。 |
| `dbb_ingress_payload_bytes`、`dbb_ingress_codec_bytes` | gauge | I | 负载和编解码预算的占用量（进程级）。 |
| `dbb_ingress_accept_rejected_total` | counter | I | 因本地预算耗尽而拒绝的本地客户端连接数。 |

### 会话结果与恢复

| 指标 | 类型 | 计数端 | 含义 |
|---|---|---|---|
| `dbb_resume_success_total` | counter | E | 完成的恢复次数。 |
| `dbb_resume_failed_total` | counter | I | 在恢复窗口内结束的会话数。 |
| `dbb_terminated_direct_total` | counter | I | 未开启恢复时失败的会话数。 |
| `dbb_terminated_after_resume_fail_total` | counter | I | 开启恢复时失败的会话数。 |
| `dbb_client_close_count_total` | counter | I | 由 DBBridge 关闭的本地客户端连接数。 |
| `dbb_client_close_duration_sum_ms` | counter | I | 从做出终止决定到关闭本地连接的时间总和（毫秒）。 |
| `dbb_remote_cleanup_failed_total` | counter | I | 失败的远端 DELETE 次数。 |
| `dbb_remote_cleanup_unknown_total` | counter | I | 结果未知的远端 DELETE 次数。 |
| `dbb_session_events_total{role,event,reason,from,to}` | counter | 两端 | 生命周期事件；标签取值见 §6。 |
| `dbb_ingress_faults_total{source,reason}` | counter | I | 按检测位置统计的载体故障：`data`（读取 WS 或 SSE 载体）、`upstream`（发送上行帧或等待其确认）、`control`（控制 API）、`liveness`（`failure_detect_timeout` 内没有会话流量）。 |
| `dbb_operation_duration_seconds{role,phase}` | histogram | 两端 | `barrier`、`local_close`、`recovery`、`cleanup`、`probe`、`precheck`、`attach`、`control` 的耗时；分桶 0.001 … 60 s。 |
| `dbb_endpoint_state{peer,endpoint,state}` | gauge | I | endpoint 当前状态为 1：`UNKNOWN`、`HEALTHY`、`UNHEALTHY`、`MISMATCHED`。 |
| `dbb_phase_duration_seconds{peer,endpoint,phase,success}` | summary | I | 按 endpoint 统计 `probe`、`precheck`、`attach`、`control` 以及每次恢复尝试（`recovery`）的耗时。 |

正常关闭和节点停机时的关闭不计为失败。

### 预算、凭据与进程

| 指标 | 类型 | 计数端 | 含义 |
|---|---|---|---|
| `dbb_buffer_memory_bytes` | gauge | 两端 | 负载预算的占用量。 |
| `dbb_suspended_mem_bytes` | gauge | E | 挂起会话占用的内存。 |
| `dbb_recovery_retained_bytes`、`dbb_recovery_retained_limit` | gauge | 两端 | 为恢复保留的数据及其上限（`suspended_buffer_limit`）。 |
| `dbb_resource_in_use{resource}`、`dbb_resource_limit{resource}` | gauge | 两端 | 预算占用与上限；`resource` ∈ `payload`、`codec`、`metadata`、`control_metadata`、`probe_metadata`、`sessions`、`probes`、`recovery`、`cleanup`、`creates`、`inbound_connections`、`conn_metadata`、`data_connections`、`control_connections`、`data_requests`、`control_requests`。 |
| `dbb_resource_wait_seconds{resource}` | summary | 两端 | 等待预算所花的时间。 |
| `dbb_backpressure_total` | counter | 两端 | 会话等待缓冲空间的次数。 |
| `dbb_admission_rejected_total` | counter | E | 被准入控制拒绝的入站连接和请求数。 |
| `dbb_auth_reload_success_total` | counter | 两端 | 安装了有变化的凭据快照的重载次数（不含首次加载）。 |
| `dbb_auth_reload_failed_total` | counter | 两端 | 被拒绝的凭据加载和重载次数。 |
| `dbb_auth_credentials_stale` | gauge | 两端 | 上一次重载被拒绝、之前的凭据仍在生效时为 1。 |
| `dbb_auth_reload_last_success_timestamp_seconds` | gauge | 两端 | 最近一次成功加载凭据的 Unix 时间。 |
| `dbb_unbalanced_release_total` | counter | 两端 | 记账自检，应保持为 0。非零值表示存在值得报告的缺陷。 |

### 反向隧道

仅在使用反向模式的节点上输出：有 `mode: reverse` peer 的节点输出**公网侧**指标，开启 `reverse.enabled` 的节点输出**数据库侧**指标。

| 指标 | 类型 | 侧 | 含义 |
|---|---|---|---|
| `dbb_reverse_hub_tunnels{peer,transport}` | gauge | 公网侧 | 每个反向 peer 当前存活的隧道数；`transport` ∈ `ws`、`http-sse`、`long-poll`。 |
| `dbb_reverse_hub_accepted_total` | counter | 公网侧 | 已接受并注册的隧道数。 |
| `dbb_reverse_hub_rejected_total{code}` | counter | 公网侧 | 被拒绝的隧道握手和 HTTP 隧道请求，按 DBBridge 错误码（`DBB_UNAUTHORIZED`、`DBB_NODE_MISMATCH`、`DBB_TUNNEL_CONFLICT`、`DBB_TUNNEL_NOT_FOUND` 等）。某个错误码首次出现后才有对应序列。 |
| `dbb_reverse_tunnels{endpoint,transport}` | gauge | 数据库侧 | 到每个 `reverse.endpoints` 条目的存活隧道数。 |
| `dbb_reverse_tunnel_established_total` | counter | 数据库侧 | 已建立的隧道数。 |
| `dbb_reverse_tunnel_lost_total` | counter | 数据库侧 | 节点运行期间断开的已建立隧道数。 |
| `dbb_reverse_tunnel_dial_failures_total` | counter | 数据库侧 | 隧道拨号失败次数（含回退后仍失败）。 |
| `dbb_reverse_tunnel_fallback_total{from,to}` | counter | 数据库侧 | `auto` 的载体回退次数（`ws`→`http-sse`、`http-sse`→`long-poll`）。 |

**建议的告警。** `dbb_auth_credentials_stale == 1`；`/readyz` 不是 200；`rate(dbb_dial_failed_total[5m]) > 0`；`dbb_endpoint_state{state="MISMATCHED"} == 1`；`sessions`、`metadata`、`payload` 的 `dbb_resource_in_use / dbb_resource_limit > 0.9`；`dbb_terminated_*` 或 `dbb_resume_failed_total` 持续上升。反向模式：公网侧 `sum(dbb_reverse_hub_tunnels{peer="..."}) == 0` 持续超过一分钟；`rate(dbb_reverse_tunnel_lost_total[5m]) > 0`；`dbb_reverse_hub_rejected_total{code=~"DBB_UNAUTHORIZED|DBB_NODE_MISMATCH"}` 有任何增长（有人用错误的令牌或身份拨入）；`dbb_reverse_tunnels{transport="long-poll"}` 非零（代理迫使隧道使用最慢的载体）。

## 6. 日志与事件

运行日志输出到 stdout（`--log-format text` 或 `json`），命令行错误与警告输出到 stderr：在 systemd 下均进入 journal（`journalctl -u <name>`），在内置 Windows 服务下进入“应用程序”事件日志、事件源为服务名，在 WinSW 下写入其日志目录。与配置和启动相关的记录带有 `code=DBB-CFG-0xx`。

| 消息 | 级别 | 含义 |
|---|---|---|
| `starting dbbridge peer node` / `admin server started` | INFO | 启动。 |
| `configuration warning` | WARN | 启动时输出的 `check-config` 警告。 |
| `failed to load configuration` | ERROR | 退出码 78。 |
| `failed to start node` | ERROR | `DBB-CFG-010`，退出码 1。 |
| `received termination request, initiating graceful shutdown ...` / `node exited cleanly` | INFO | 停止。 |
| `fatal background failure; stopping the node` | ERROR | `DBB-CFG-011`，退出码 1。 |
| `shutdown teardown exceeded its budget; abandoning remaining cleanup` | ERROR | 收尾未能在 15 s 内完成。 |
| `credentials reloaded` / `credential reload rejected ...` / `credential reload succeeded; the earlier reload error is resolved` | INFO / ERROR / INFO | 见 §3。 |
| `tokens from environment variables are read once per process; ...` | INFO | 列出需要重启才能更换的变量。 |
| `session terminated` | INFO | 一个 Ingress 会话结束（字段见下）。 |
| `session recovered` | INFO | 一次恢复完成。 |
| `session cleanup` / `session cleanup skipped` | INFO / WARN | 终止后的远端 DELETE（通过备用 endpoint 发送时带 `via_backup=true`）。 |
| `recovery grace does not exceed the failover cooldown of the only usable carrier; ...` | WARN | 见[传输 §4](transport.zh-CN.md#4-故障检测故障切换与会话恢复)。 |
| `ingress connection rejected: local budget exhausted` | WARN | 一个本地客户端被拒绝。 |
| `ingress listener is bound to a non-loopback address; ...` | WARN | |

**`session terminated` 的字段：**`session`、`role=ingress`、`peer`、`endpoint`、`owner`（Egress 实例）、`epoch`、`transport`、`recovery_enabled`、`attempts`、`deadline`（恢复截止时间；未开始恢复时为空）、`outcome_unknown`、`reason`、`fault_source` 和 `fault_reason`（最近一个故障周期的首个故障；会话未经故障结束时为空）、`local_close_ms`、`fault_time`、`terminal_time`、`local_closed_time`。会话创建之前结束时，`session`、`endpoint` 和 `transport` 为空。

**`reason` 取值**（日志字段及 `dbb_session_events_total{reason}`）：

| 原因 | 含义 |
|---|---|
| `normal` | 两个方向都已结束（FIN 已应用并已确认）。 |
| `shutdown` | 本节点正在停止。 |
| `local_socket` | 本地客户端套接字出错（读、写或半关闭错误）。 |
| `timeout` | 某个期限到期：建连、首次接入、恢复宽限期、背压。 |
| `network` | 对端不可达或载体断开，且没有更具体的原因。 |
| `protocol` | 协议违规或帧冲突。 |
| `unauthorized` / `forbidden` | `401` / `403`（令牌未被接受；目标不允许或授权被撤销）。 |
| `dial_failed` | Egress 无法连到数据库（`502 DBB_DIAL_FAILED`）；请查看 Egress 上的 `dbb_dial_failed_total`。 |
| `draining` | Egress 正在停机（`503 DBB_DRAINING`）。 |
| `backend_lost` | Egress 与数据库的连接异常断开。 |
| `failed` | Egress 报告会话异常关闭。 |
| `offset`、`quota`、`barrier`、`buffer` | 确认偏移被拒绝；超出配额；未能到达恢复写入屏障；缓冲预算耗尽。 |
| `success`、`failed`、`unknown`、`transport` | `resume`、`cleanup`、`fallback` 以及预检缓存事件的结果。 |
| `instance`、`authentication` | 属于标签集合，本版本不会产生。 |
| `not_needed`、`skipped` | 清理事件：无需远端 DELETE；清理未能执行（例如到达停机截止时间）。 |
| `other` | 集合之外的任何值。 |

**`event` 取值：**`fault`、`terminate`、`resume`、`fallback`、`cleanup`、`cache_hit`、`cache_miss`、`cache_invalidate`、`precheck`、`attach`。`role` 为 `ingress` 或 `egress`；`from`/`to` 为 `ws`、`http-sse`、`none` 或 `other`。在 Egress 上，`terminate` 使用会话的终态类别：`normal`、`backend_lost`、`timeout`、`forbidden`、`buffer`、`protocol`、`failed`。

## 7. 对端协议错误码

这些错误码出现在节点之间的 HTTP 应答和日志中；**数据库客户端永远看不到它们**，它只会看到 TCP 连接被关闭。响应体：`{"code": "...", "message": "...", "retryable": bool, "session_closed": bool}`。

| 错误码 | 状态码 | 可重试 | 含义 / 处理 |
|---|---|---|---|
| `DBB_UNAUTHORIZED` | 401 | 否 | 令牌缺失或未被接受，或会话令牌无效。检查 Ingress 的令牌是否对应 Egress 上某个已启用的身份。 |
| `DBB_TARGET_FORBIDDEN` | 403（作为会话终态码时为 410） | 否 | 目标不在该身份的 `allowed_targets` 中、地址在 `allowed_cidrs` 之外，或授权已被重载撤销。 |
| `DBB_SESSION_NOT_FOUND` | 404 | 否 | 会话不存在或已被清理；客户端需重连。 |
| `DBB_BAD_FRAME` | 400（未知操作为 404，方法错误为 405） | 否 | 请求、请求头或请求体格式错误；也包括格式错误的 WebSocket 升级请求。通常是代理改动了请求。 |
| `DBB_BAD_DIGEST` | 400 | 否 | SSE 帧与 `X-DBB-SHA256` 不符：数据在传输中被改动。 |
| `DBB_BAD_ACK` | 400 | 否 | 确认偏移超出范围。 |
| `DBB_FRAME_TOO_LARGE` | 413 | 否 | SSE 上行帧大于协商的 `max_frame_size`。 |
| `DBB_STALE_EPOCH` | 409 | 否 | 请求针对的是已恢复会话的旧代次。 |
| `DBB_STREAM_EXISTS` | 409 | 否 | 该代次已经接入了载体。 |
| `DBB_IDEMPOTENCY_CONFLICT` | 409 | 否 | 同一请求 ID 携带了不同的参数。 |
| `DBB_RESUME_IN_PROGRESS` | 409 | 是 | 该会话的另一次恢复正在进行。 |
| `DBB_INSTANCE_MISMATCH` | 409 | 否 | Egress 进程已重启，会话无法继续。客户端需重连。 |
| `DBB_NODE_MISMATCH` | 409 | 否 | Egress 的 `node.id` 与 `peers[].expected_node_id` 不符。请修正配置或 endpoint URL。 |
| `DBB_PROTOCOL_MISMATCH` | 409 | 否 | 协议版本不是 1.5。 |
| `DBB_RECOVERY_DISABLED` | 409 | 否 | 对未开启恢复而创建的会话请求恢复。 |
| `DBB_RECOVERY_UNSUPPORTED` | 409 | 否 | 请求了恢复，但 Egress 关闭了恢复。请统一 `recovery.enabled`。 |
| `DBB_FRAME_CONFLICT` | 409 / 410 | 否 | 数据或 FIN 与已收到的内容矛盾；会话被关闭。 |
| `DBB_REPLAY_TOO_OLD` | 409 | 否 | 重放的帧早于保留的回执。 |
| `DBB_WINDOW_EXCEEDED` | 409 | 是 | 在途帧过多。 |
| `DBB_TRANSPORT_UNSUPPORTED` | 409（426） | 否 | 载体不在 `http.allowed_transports` 中（或缺少 WebSocket 子协议）。WebSocket 握手到达节点时已丢失升级头（被代理去掉）则返回 `426`。 |
| `DBB_TUNNEL_CONFLICT` | 409 | 否 | 反向隧道被拒绝：隧道 ID 已注册，或同一数据库侧节点的另一个实例仍有存活隧道（这些隧道消失后即可接入，最迟在 `failure_detect_timeout` 之后）。两个数据库侧节点使用相同 `node.id` 时会一直出现。 |
| `DBB_TUNNEL_NOT_FOUND` | 404 | 否 | 请求的 SSE 或长轮询反向隧道在公网侧已不存在；数据库侧会新建隧道。 |
| `DBB_TRANSPORT_MISMATCH` | 409 | 否 | 请求使用的载体与会话的载体不同。 |
| `DBB_SESSION_CLOSED` | 410 | 否 | 会话已结束。消息为 `session terminated: <category>`；`normal` 表示两个方向都已结束。 |
| `DBB_BACKEND_LOST` | 410 | 否 | 数据库连接出错（重置或 EOF 以外的读错误）。数据库正常 EOF 属于 FIN，不是该错误码。 |
| `DBB_RESUME_EXPIRED` | 410 | 否 | 恢复宽限期已过。 |
| `DBB_QUOTA_EXCEEDED` | 429 | 是 | 某项配额或预算耗尽，消息中写明名称：`peer session quota`、`session quota`、`session metadata budget`、`peer idempotency quota`、`peer creation concurrency`、`creation concurrency`、`peer probe budget`、`probe budget`、`peer precheck budget`、`peer recovery budget`、`recovery budget`、`request concurrency`、`request metadata`、`control metadata budget`。通常带 `Retry-After`。 |
| `DBB_DIAL_FAILED` | 502 | 否 | Egress 无法解析或连接数据库。该结果按建连请求 ID 缓存。 |
| `DBB_BUFFER_EXHAUSTED` | 503（作为终态码时为 410） | 是 | 缓冲或编解码预算耗尽（`codec budget`）；作为终态码时表示会话因此被结束。 |
| `DBB_DRAINING` | 503 | 是 | Egress 正在停止。 |
| `DBB_CREATE_PENDING` | 202 | 是 | 同一建连仍在进行；用同一 ID 重试。 |
| `DBB_RESUME_PENDING` | 202 | 是 | 同一恢复正在等待写入屏障；用同一 ID 重试。 |
| `DBB_OFFSET_GAP`、`DBB_INTERNAL_ERROR`、`DBB_WS_HANDSHAKE_FAILED`、`DBB_WS_PROTOCOL_ERROR` | — | — | 保留；本版本不会发送。 |

Egress 自己结束会话时，还会在该会话的载体上把终态码作为最后一条 `error` 控制消息发送（[传输 §4](transport.zh-CN.md#4-故障检测故障切换与会话恢复)）。

## 8. 环境变量

| 变量 | 用途 |
|---|---|
| `token_env` / `token_envs` 中的变量名 | 令牌，启动时读取（重载时取自不变的进程环境）。 |
| `HTTP_PROXY`、`HTTPS_PROXY`、`NO_PROXY`（含小写形式） | 对端连接，仅在 `transport.use_environment_proxy: true` 时使用。 |
| `NOTIFY_SOCKET` | systemd 就绪通知（由 systemd 设置）。 |
| `SOURCE_DATE_EPOCH`、`DBB_VERSION`、`DBB_COMMIT`、`DBB_PLATFORMS` | `scripts/build-release.sh`（[安装](installation.zh-CN.md#3-从源码构建)）。 |
| `DBB_MYSQL_DSN`、`DBB_MARIADB_DSN` | 数据库集成测试（`go test -tags=database ./integration`）。 |

## 9. 配置错误码

| 错误码 | 含义 | 退出码 |
|---|---|---|
| `DBB-CFG-001` | 配置文件无法读取、过大、不是合法 YAML、包含多个文档，或违反结构约束（未知字段、类型错误）。 | 78 |
| `DBB-CFG-002` | 取值或交叉引用无效。 | 78 |
| `DBB-CFG-003` | 凭据：凭据文件、令牌文件或令牌变量缺失、不可读或无效；快照约束（ACL 目标、配额之和、`credential_ref`、两个身份共用令牌）。 | 78 |
| `DBB-CFG-004` | TLS：`http.tls` 证书/私钥或 `peers[].tls.ca_file` 无法加载。 | 78 |
| `DBB-CFG-005` | 监听地址无效或冲突。 | 78 |
| `DBB-CFG-006` | 服务凭据环境：`--env-file` 无法加载，或 `install` 需要的令牌变量缺失或格式错误。 | 78 |
| `DBB-CFG-007` | Peer 模式：`peers[].mode` 无效、反向 peer 配置了 `endpoints`，或反向 peer 所在节点未开启 `http.enabled`。 | 78 |
| `DBB-CFG-008` | `reverse` 段：缺少 `peer_node_id`、令牌或 endpoint，`reverse.transport` 无效，`tunnels_per_endpoint` 超出范围，非回环明文 endpoint 未显式允许，重连退避无效。 | 78 |
| `DBB-CFG-010` | 节点启动失败（例如地址已被占用）。 | 1 |
| `DBB-CFG-011` | 运行中出现致命故障。 | 1 |
| `DBB-CFG-020` | 凭据重载被拒绝，保留之前的凭据（只写日志，节点继续运行）。原因见 `cause_code`。 | — |
| `DBB-CFG-030` | 服务安装/卸载因其他原因失败（systemd/SCM、权限、服务已存在）。 | 1 |

## 10. 容量规划

**并发会话**受 `limits.max_sessions`、每个身份的 `inbound_peers[].max_sessions` 以及元数据预算（[配置 §14](configuration.zh-CN.md#14-会话容量与元数据预算)）限制。每个会话在 Egress 上（载体和数据库）或 Ingress 上（客户端和载体）占用两个文件描述符，另有连接池中的 HTTP 连接；生成的 systemd 单元设置 `LimitNOFILE=max(8192, 4 × max_sessions + 1024)`。

**内存。** 最坏情况下受 `global_buffer_limit`（负载）+ `codec_buffer_limit` + `metadata_limit` 限制，默认共 320 MiB，另加运行时开销。每个会话每个方向的 `buffer_per_direction`（4 MiB）是上限，而不是预先占用。

**新建会话速率。** 会话创建是幂等的：Egress 为每个建连请求 ID 保留一条记录，为每个已关闭会话保留一条墓碑，保留 `idempotency_tombstone_ttl`（至少 10 分钟）。这些记录存放在元数据预算的一个份额中：

```
Egress 元数据   = 可用元数据（配置 §14），双向节点再减去 1/8
记录预算        = max(Egress 元数据 − 远端会话名额 × 每会话元数据,
                      Egress 元数据 / 8)
远端会话名额    = max_sessions，双向节点再减去 max(1, max_sessions/4)
可持续建连速率 ≈ 记录预算 / (2 KiB × idempotency_tombstone_ttl 秒数)
```

默认值（`max_sessions: 200`、`metadata_limit: 32MiB`、TTL 10 分钟）下，纯 Egress 节点的记录预算约 11.3 MiB，双向节点约 12.3 MiB，可持续的新建会话速率约为每秒 **9.7** 个（纯 Egress）或 **10.5** 个（双向）；短时突发可以在记录填满之前被吸收。启用多个身份时，每个身份分得一部分记录（见[传输 §6](transport.zh-CN.md#6-内存与连接预算)），因此一个身份无法阻塞其他身份。超出分额的身份会得到 `429 DBB_QUOTA_EXCEEDED "peer idempotency quota"`（可重试）。

大量短连接（客户端没有连接池）的负载应当：

- 调大 `transport.metadata_limit`（上限大致随记录预算线性增长），或
- 把 `limits.max_sessions` 降到实际需要的值（预留的会话名额越少，留给记录的就越多），并且
- 在数据库客户端中使用连接池。

`idempotency_tombstone_ttl` 不能低于 10 分钟。

## 11. 故障排查

| 现象 | 可能原因 | 检查内容 |
|---|---|---|
| 服务立即以退出码 78 退出 | 配置或凭据错误 | `journalctl -u <name>` / 事件日志中的 `Error [DBB-CFG-0xx]`；以服务账户运行 `dbbridge check-config -c <config>`。 |
| systemd 每 5 s 重启一次服务 | 启动失败（`DBB-CFG-010`），例如端口被占用 | 日志 `failed to start node`；`ss -ltnp`。 |
| `install` 报 `cannot access ...` 或 `cannot traverse directory ...` | 服务账户无法读取某个文件 | 文件权限（[安全 §8](security.zh-CN.md#8-服务账户与文件权限)）。 |
| 客户端连接立即被关闭 | Egress 连不到数据库、ACL、令牌 | Ingress 日志 `session terminated reason=...`；Egress 上的 `dbb_dial_failed_total`、`allowed_targets`、`allowed_cidrs`。 |
| `reason=unauthorized` | 令牌不匹配 | 两端的令牌文件；重载状态（`dbb_auth_credentials_stale`）。 |
| `reason=forbidden` | 目标不允许或已撤销 | Egress 上该身份的 `allowed_targets`；listener 的 `target`。 |
| 希望用 WebSocket，但所有会话都在用 SSE | 代理拒绝或改动了升级请求 | 代理的 WebSocket 设置（[传输 §7](transport.zh-CN.md#7-反向代理与网关)）；`dbb_session_events_total{event="fallback"}`。 |
| `dbb_endpoint_state{state="MISMATCHED"}` | URL 背后是错误的节点、协议违规、TLS 失败 | `expected_node_id`、证书主机名、会改写 WebSocket 消息的代理。 |
| 日志中出现 `429 DBB_QUOTA_EXCEEDED` | 配额或预算耗尽（消息中写明名称） | §7 中的消息；`dbb_resource_in_use/limit`；§10。 |
| `/readyz` 返回 503 | 正在启动、正在排空或出现致命故障；只靠隧道的数据库侧没有存活隧道 | 对应时间点的日志；`DBB-CFG-011`；响应体中的 `reverse:` 行。 |
| 反向 peer：连到公网侧的客户端立即被断开 | 数据库侧没有建立隧道 | `dbb_reverse_hub_tunnels`；数据库侧日志 `reverse tunnel dial failed`；数据库侧的出站防火墙或代理。 |
| `reverse tunnel dial failed ... 401` / `dbb_reverse_hub_rejected_total{code="DBB_UNAUTHORIZED"}` | `reverse.token_file` 与公网侧该反向 peer 的令牌不一致 | 两侧令牌文件；重载状态。 |
| `... 403 DBB_NODE_MISMATCH` 或 `tunnel peer node mismatch` | `reverse.peer_node_id` ≠ 公网侧 `node.id`，或数据库侧 `node.id` ≠ `peers[].expected_node_id` | 两侧节点 ID；endpoint URL 是否指向正确节点。 |
| `409 DBB_TUNNEL_CONFLICT` 反复出现 | 两个数据库侧进程使用相同 `node.id` | 停掉重复的进程；正常重启后会在 `failure_detect_timeout` 内消失。 |
| 反向隧道使用 `long-poll` | 代理拒绝 WebSocket 升级且缓冲 SSE | `dbb_reverse_tunnel_fallback_total`；代理设置（[传输 §7.1](transport.zh-CN.md#71-经代理的反向隧道)）。 |
| 长时间空闲后会话中断 | 代理空闲超时小于心跳间隔，或 keep-alive 低于 60 s | 代理超时设置（[传输 §7](transport.zh-CN.md#7-反向代理与网关)）。 |
| 更换令牌未生效 | 重载被拒绝，或令牌来自环境变量 | 日志 `DBB-CFG-020`；`dbb_auth_credentials_stale`；[安全 §3](security.zh-CN.md#3-令牌与凭据来源)。 |
| `check-config` 通过，但服务拒绝重载 | 文件对服务不可见或不可读（沙箱、权限），或使用了新的变量名 | 把凭据文件放在配置文件旁边；检查权限；新变量名需重新安装。 |
| `limits.max_sessions ... exceeds the process metadata budget` | 会话数相对 `metadata_limit` 过多 | 采用错误信息中的建议值（[配置 §14](configuration.zh-CN.md#14-会话容量与元数据预算)）。 |
| `suspended_buffer_limit ... must be < global_buffer_limit` | 两者都被显式设置且相互冲突 | 删除显式的 `suspended_buffer_limit`，使用派生默认值。 |
| 停止耗时很长 | 会话正在排空 | `transport.shutdown_timeout`；再发一次 `SIGTERM` 可强制退出（130）。 |

**背压诊断。** Egress 对 `/ack` 的应答始终携带 `data_tx_blocked`、`data_rx_blocked` 和 `backpressure_reason`：`none`、`local_socket_write`（数据库套接字接收数据不够快）、`data_channel_write`（通往 Ingress 的载体较慢）或 `global_buffer`（进程负载预算已满）。结合 `dbb_backpressure_total` 和 `dbb_resource_wait_seconds`，可以判断哪一侧较慢。
