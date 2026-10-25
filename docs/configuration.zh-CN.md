[English](configuration.md) | **简体中文**

# 配置参考

[README](../README.zh-CN.md) · [安装](installation.zh-CN.md) · **配置** · [传输](transport.zh-CN.md) · [安全](security.zh-CN.md) · [运维](operations.zh-CN.md) · [示例](examples.zh-CN.md)

本文列出每个配置项的类型、默认值和约束。`transport` 段的调优建议、反向代理要求和 TLS 见[传输](transport.zh-CN.md)；令牌轮换与吊销见[安全](security.zh-CN.md)。

- [1. 文件格式与取值语法](#1-文件格式与取值语法)
- [2. 顶层结构](#2-顶层结构)
- [3. `node`](#3-node)
- [4. `http`](#4-http)
- [5. `admin`](#5-admin)
- [6. `auth` 与凭据](#6-auth-与凭据)
- [7. `peers`](#7-peers)
- [8. `listeners`](#8-listeners)
- [9. `targets`](#9-targets)
- [10. `limits`](#10-limits)
- [11. `transport`](#11-transport)
- [12. 派生默认值](#12-派生默认值)
- [13. 跨字段约束](#13-跨字段约束)
- [14. 会话容量与元数据预算](#14-会话容量与元数据预算)
- [15. 警告](#15-警告)
- [16. 运行中重新读取的内容](#16-运行中重新读取的内容)

## 1. 文件格式与取值语法

- 配置文件是单个 YAML 文档，最大 4 MiB；凭据文件最大 1 MiB，CA 证书包最大 1 MiB。YAML 嵌套不超过 32 层。
- 未知字段会被拒绝（`field "x" not found in config.http`）。
- 类型严格校验：字符串必须是 YAML 字符串（不加引号的 `protocol_version: 1.4` 会被拒绝，应写 `"1.4"`），布尔值必须是 `true`/`false`，整数必须是整数。
- 显式 `null`（只写 `key:` 不给值，或写 `~`）会被拒绝。可选字段要使用默认值，请直接省略。
- **尺寸**取值为整数字节数，或形如 `<数字>[ ]<单位>` 的字符串，单位为 `B`、`K`/`KB`/`KiB`、`M`/`MB`/`MiB`、`G`/`GB`/`GiB`（不区分大小写）。所有单位均按 1024 进制计算（`1MB` = `1MiB` = 1,048,576 字节）。不支持小数（`1.5MiB`）和 `T`。
- **时长**取值为 Go 时长字符串（`200ms`、`1m30s`；单位 `h`、`m`、`s`、`ms`、`us`、`ns`，不支持 `d`），或表示秒数的 YAML 数值（`5`，`0.5` 即 500 ms）。负数、NaN、无穷大和溢出值会被拒绝。
- 尺寸或时长写空字符串表示“使用默认值”。
- 以下整数字段如果出现，必须为正数（写 0 会被拒绝，而不是当作默认值）：`limits.max_sessions`、`limits.max_endpoints_per_peer`、`transport.up_max_inflight_frames`、`transport.h2_pool_size`、`transport.receipt_limit_per_session`、`peers[].failover.healthy_threshold`、`peers[].failover.unhealthy_threshold`、`peers[].failover.max_concurrent_probes`、`inbound_peers[].max_sessions`。
- **相对路径**：主配置中的相对路径（`auth.credentials_file`、`peers[].token_file`、`auth.inbound_peers[].token_files`、`http.tls.cert_file`、`http.tls.key_file`、`peers[].tls.ca_file`）相对于主配置文件所在目录解析；凭据文件中的相对令牌路径相对于凭据文件所在目录解析。
- 各类 ID（`node.id`、peer、endpoint、listener、target 以及入站身份的 ID）建议满足 `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`。其他非空 ID 可以加载，但会产生警告，因为 ID 会出现在日志、指标标签和对端配置中。
- 校验遇到第一个错误即停止。每个错误都带有稳定的错误码（`Error [DBB-CFG-002]: ...`），见[运维 → 配置错误码](operations.zh-CN.md#9-配置错误码)。

## 2. 顶层结构

```yaml
node:        # 本节点身份
http:        # 入站对端服务（Egress 角色）：HTTP、WebSocket、SSE
admin:       # 回环地址上的 /healthz、/readyz、/metrics
auth:        # 入站身份、目标 ACL、凭据文件
peers:       # 出站对端（Ingress 角色）：endpoint、故障切换、TLS、令牌
listeners:   # 供数据库客户端连接的本地 TCP 入口
targets:     # 本节点可以拨号的数据库（Egress 角色）
limits:      # 会话与 endpoint 上限
transport:   # 载体、流控、会话恢复、预算、超时
```

节点有 `listeners` 和 `peers` 时是 **Ingress**；`http.enabled: true` 且有 `targets` 时是 **Egress**；两者兼有时为**双向**节点。所有角色使用同一个二进制程序。

## 3. `node`

| 配置项 | 类型 | 默认值 | 约束 |
|---|---|---|---|
| `node.id` | string | —（必填） | 非空。对端用 `peers[].expected_node_id` 核对该值。 |

每次启动还会生成一个随机实例 ID（`pi_<32 位十六进制>`），不可配置。会话绑定到实例，因此进程重启后会话不会延续。

## 4. `http`

供其他节点连接的入站服务（Egress 角色）。

| 配置项 | 类型 | 默认值 | 约束 / 行为 |
|---|---|---|---|
| `http.enabled` | bool | `false` | 为 `false` 时 `targets`、入站凭据和 `http.tls` 都不生效（会给出警告）。 |
| `http.listen` | `host:port` | `127.0.0.1:18080` | 仅在启用时校验。允许空主机、`0.0.0.0` 和 `::`。非回环地址（回环指 `localhost`、`127.0.0.0/8`、`::1`）必须设置 `http.tls.enabled: true` 或 `http.allow_insecure_listen: true`（后者会产生警告）。 |
| `http.allow_insecure_listen` | bool | `false` | 允许在非回环 `http.listen` 上使用明文 HTTP。只影响本服务。 |
| `http.allowed_transports` | 字符串列表 | `["ws", "http-sse"]` | 每项为 `ws` 或 `http-sse`，不得重复；显式写 `[]` 会被拒绝。会在探测应答中告知对端。 |
| `http.tls.enabled` | bool | `false` | 仅在 `http.enabled` 时生效。TLS 1.2 及以上。 |
| `http.tls.cert_file` | 路径 | — | 启用 TLS 时必填。`check-config` 和启动时会连同私钥一起加载。证书只在启动时读取一次，更换证书后需重启才生效。 |
| `http.tls.key_file` | 路径 | — | 启用 TLS 时必填。（Unix）文件组可写或其他用户可访问时给出警告。 |

该服务的固定限制：请求头总大小 ≤ 16 KiB，读请求头超时 10 s，空闲 keep-alive 90 s。尚未完成任何已认证请求的连接在 5 s 后关闭（含 TLS 握手、读请求头和空闲时间）；同一时刻未认证连接最多占入站连接上限的一半，单个来源 IP 最多占其中 `max(8, 这一半 / 4)`。认证前即被拒绝的请求（400、401、404、405）在应答后关闭连接。

## 5. `admin`

| 配置项 | 类型 | 默认值 | 约束 |
|---|---|---|---|
| `admin.enabled` | bool | `false` | `gen-config` 生成的配置和所有随附样例都在回环地址上设为 `true`。 |
| `admin.listen` | `host:port` | `127.0.0.1:18090` | 仅在启用时校验。必须是回环地址，除非设置 `admin.allow_remote: true`。参与监听冲突检测。 |
| `admin.allow_remote` | bool | `false` | 允许非回环 `admin.listen`，会产生警告。该端点没有 TLS，也没有认证。 |

端点和限制见[运维 → Admin 端点](operations.zh-CN.md#4-admin-端点)。

## 6. `auth` 与凭据

### 6.1 `auth`

| 配置项 | 类型 | 默认值 | 约束 |
|---|---|---|---|
| `auth.credentials_file` | 路径 | — | 独立凭据文件（§6.3）。不能与 `auth.inbound_peers`，也不能与 `peers[].token_env`/`token_file` 同时使用。 |
| `auth.reload_interval` | 时长 | `5s` | 重新读取凭据的间隔（§16）。省略或写 `0` 表示 5 s；负数以及 0 到 1 s 之间的值会被拒绝。 |
| `auth.inbound_peers` | 列表 | `[]` | 内联入站身份（§6.2）。 |

凭据有两种来源：一是主配置本身（内联的 `auth.inbound_peers` 加上 `peers[].token_file`/`token_env`），二是一个独立凭据文件（同时包含入站和出站凭据）。两种形式都会每隔 `reload_interval` 重新读取，收到 `SIGHUP`（Linux）或 `sc.exe control <name> paramchange`（Windows 服务）时也会立即重新读取。

### 6.2 入站身份（`auth.inbound_peers[]`，或凭据文件中的 `inbound_peers[]`）

| 配置项 | 类型 | 默认值 | 约束 |
|---|---|---|---|
| `id` | string | — | 必填，唯一。本节点对某个来访节点的称呼。请求只凭其 Bearer 令牌归属到身份；按惯例取来访节点的 `node.id`。会出现在日志和指标中。 |
| `enabled` | bool | `true` | 停用的身份无需令牌，也不会读取其 `token_files`/`token_envs`，因此它们可以指向已删除的文件。其 `allowed_targets` 仍做语法检查。通过重载停用某个身份会关闭它的所有存量会话。 |
| `token_files` | 路径列表 | — | 令牌文件。同一身份内不能与 `token_envs` 混用。 |
| `token_envs` | 变量名列表 | — | 存放令牌的环境变量。 |
| `allowed_targets` | target ID 列表 | — | 启用的身份必填。不得有空项或重复项；每项都必须存在于主配置的 `targets` 中。对其他目标的建连一律返回 `403 DBB_TARGET_FORBIDDEN`。 |
| `max_sessions` | int | `200` | 正数。该身份的并发会话上限，超出时返回 `429 DBB_QUOTA_EXCEEDED`。 |

启用的身份至少需要一个令牌和一个允许的目标。启用身份列出的每个来源都必须能解析：列出的文件必须存在，列出的变量必须已设置且非空，不存在“可选”的占位。

配额规则：

- 启用的身份多于一个时，它们的 `max_sessions` 之和（省略的配额按 200 计）不得超过 `limits.max_sessions`，否则报错。
- 单个身份的配额超过 `limits.max_sessions` 时给出警告，实际以全局上限为准。
- 节点同时配置了 `listeners` 时，入站配额之和 ≥ `limits.max_sessions` 会给出警告，因为本地客户端共用同一会话预算。因此 `gen-config` 在双向模式下把入站身份的配额设为 `--max-sessions` 的一半。

### 6.3 凭据文件

```yaml
version: 1                       # 必填，必须为 1
inbound_peers:                   # 条目格式同 §6.2
  - id: B
    token_files: [tokens/b-to-a.token]
    allowed_targets: [mssql]
    max_sessions: 100
outbound_peers:
  - id: B                        # 由 peers[].credential_ref 引用
    token_file: tokens/a-to-b.token   # token_file 与 token_env 二选一
```

| 配置项 | 类型 | 约束 |
|---|---|---|
| `version` | int | 必填，`1`。 |
| `inbound_peers` | 列表 | 见 §6.2。 |
| `outbound_peers[].id` | string | 必填，唯一。每个 `peers[].credential_ref` 都必须有对应条目，启动和每次重载时都会检查。 |
| `outbound_peers[].token_file` | 路径 | 相对于凭据文件。 |
| `outbound_peers[].token_env` | 变量名 | 环境变量。 |

每个 `outbound_peers` 条目都会被解析，包括尚未被任何 peer 引用的条目。

### 6.4 令牌

- 令牌长 1–8192 字节，不含空白和控制字符。令牌文件末尾可以有一个 `\n` 或 `\r\n`；有其他内容（例如多出一行）则拒绝。
- 短于 16 字节的令牌可以加载，但会产生警告。`gen-config` 生成 32 字节随机数的十六进制编码（与 `openssl rand -hex 32` 格式相同）。
- 同一身份重复列出同一个令牌（例如准备轮换时）只计一次。两个不同身份使用同一令牌则被拒绝：`inbound peers A and C use the same token; every inbound identity needs its own token`。
- 错误信息会指明来源（`inbound_peers[plant-a]: token_env DBB_TOKEN_X is unset or empty`、`token_file /path: ...`），但从不输出令牌本身。
- 在 Unix 上，令牌文件或 TLS 私钥组可写或其他用户可访问时给出警告（凭据文件本身不含密钥，不做此检查）。推荐用服务组只读的方式与服务账户共享文件（0640 `root:dbbridge`）。
- **令牌文件在每次重载时都会重新读取；环境变量则取自进程环境，而进程运行期间环境不会变化。** 因此来自环境变量的令牌只能通过重启更换；首次加载时节点会在日志中列出从哪些变量读取了令牌。

## 7. `peers`

出站对端（Ingress 角色）。每个 peer 代表另一个节点，可通过一个或多个 endpoint 访问。

| 配置项 | 类型 | 默认值 | 约束 |
|---|---|---|---|
| `id` | string | — | 必填，唯一。本节点对另一节点的称呼，供 `listeners[].peer`、日志和指标标签使用。按惯例取对方的 `node.id`。 |
| `expected_node_id` | string | — | 必填，不得等于本节点的 `node.id`。每次探测都会核对对方的 `node.id`。 |
| `credential_ref` | string | — | 必填，直接配置令牌时也必须填写。使用 `auth.credentials_file` 时用于选择 `outbound_peers[]` 条目。 |
| `token_file` | 路径 | — | 发往该 peer 的 Bearer 令牌。未使用 `auth.credentials_file` 时，`token_file`/`token_env` 必须且只能设置一个（使用凭据文件时两者都不允许）。 |
| `token_env` | 变量名 | — | 同上，取自环境变量。 |
| `endpoints` | 列表 | — | 1 到 `limits.max_endpoints_per_peer` 项。 |
| `endpoints[].id` | string | — | 必填，在该 peer 内唯一。 |
| `endpoints[].url` | string | — | 只能是 `http://` 或 `https://` 源（origin）：不含用户信息、路径（`/` 也不行）、查询串和片段；如写端口则为 1–65535。主机名会转为小写、默认端口会被去掉，规范化后重复的地址会被拒绝。指向非回环主机的明文 `http://` 需要 `allow_insecure_http: true`。 |
| `endpoints[].priority` | int | `0` | ≥ 0，数值越小越优先。请为每个 endpoint 显式填写：省略时为 0，比显式写 1 的优先级更高。 |
| `failover.enabled` | bool | 多于一个 endpoint 时为 `true`，否则 `false` | 为 `false` 时只使用优先级最高的 endpoint。 |
| `failover.strategy` | string | `priority` | 只支持 `priority`。 |
| `failover.failback` | string | `new_sessions` | 只支持 `new_sessions`：优先 endpoint 恢复后，新会话改用它，已建立的会话保持不动。 |
| `failover.healthcheck_interval` | 时长 | `10s` | > 0。后台探测间隔。 |
| `failover.healthcheck_timeout` | 时长 | `min(3s, healthcheck_interval/2)` | > 0 且 < `healthcheck_interval`。也是按需探测的超时。 |
| `failover.unhealthy_threshold` | int | `2` | 正数。连续失败多少次后 endpoint 变为 `UNHEALTHY`。 |
| `failover.healthy_threshold` | int | `2` | 正数。连续成功多少次后结束 `UNHEALTHY` 状态。 |
| `failover.cooldown` | 时长 | `5s` | ≥ 0。故障 endpoint（或故障的 endpoint/载体组合）被避开的时长；`0` 表示不冷却。探测成功不会提前结束正在进行的冷却。 |
| `failover.max_concurrent_probes` | int | `2` | 正数。该 peer 的并发探测数；进程级探测预算为 16。 |
| `recovery.enabled` | bool | 继承 `transport.recovery.enabled` | `false` 只对该 peer 关闭会话恢复。`transport.recovery.enabled: false` 时设为 `true` 会报错。 |
| `transport.protocol` | string | 继承 `transport.protocol` | 为该 peer 单独指定 `auto`、`ws` 或 `http-sse`。 |
| `tls.verify` | bool | `true` | 校验对端证书及主机名。对非回环 `https` endpoint 设为 `false` 时还需要 `tls.allow_insecure_skip_verify: true`。不能与 `tls.ca_file` 同时使用。 |
| `tls.allow_insecure_skip_verify` | bool | `false` | 对非回环主机使用 `verify: false` 的显式确认。 |
| `tls.ca_file` | 路径 | 系统信任库 | 至少包含一张证书的 PEM 证书包（≤ 1 MiB）。`check-config --offline` 不读取。 |
| `allow_insecure_http` | bool | `false` | 允许该 peer 使用指向非回环主机的明文 `http://` endpoint。只影响出站连接。 |

两端设置如何汇合，见[传输 → 协商值](transport.zh-CN.md#2-协商值)。

## 8. `listeners`

本地 TCP 入口。数据库客户端连接到这里，节点把连接转发到 `peer` 上的 `target`。

| 配置项 | 类型 | 默认值 | 约束 |
|---|---|---|---|
| `id` | string | — | 必填，唯一。 |
| `listen` | `host:port` | — | 必填。端口 0–65535（0 表示任选空闲端口）。**listener 没有认证**：任何能连到它的主机都会以本节点的凭据访问远端。非回环地址会产生警告。 |
| `peer` | string | — | 必填，取某个 `peers[].id`。 |
| `target` | string | — | 必填。本地不做检查：它必须存在于对端的 `targets` 中，并在对端为本节点配置的 `allowed_targets` 里。 |

**监听冲突**（在 `http.listen`、`admin.listen` 和所有 listener 之间检查；端口 0 除外）：端口相同且主机重叠即冲突。`localhost` 代表 `127.0.0.1` 和 `::1`；`0.0.0.0` 与所有 IPv4 地址及所有主机名重叠；`::` 或空主机与一切重叠；其他主机名只与同名主机及通配地址重叠。

## 9. `targets`

从 target ID 到本节点可拨号数据库的映射（Egress 角色）。调用方只能指定 target ID，永远不能提交地址。

| 配置项 | 类型 | 默认值 | 约束 |
|---|---|---|---|
| `targets.<id>` | 对象 | — | ID 非空。 |
| `.network` | string | `tcp` | `tcp`、`tcp4` 或 `tcp6`。 |
| `.address` | `host:port` | — | 必填；主机必填；端口 1–65535。 |
| `.allowed_cidrs` | CIDR 列表 | 无（不限制） | 目标可以解析到的地址。主机名只解析到列表之外的地址时，以 `403 DBB_TARGET_FORBIDDEN` 拒绝。主机名目标未配置 `allowed_cidrs` 会产生警告（DNS 重绑定风险）。 |

DNS 解析出错或超时与 TCP 连接被拒一样，以 `502 DBB_DIAL_FAILED` 告知对端。

## 10. `limits`

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `limits.max_sessions` | int | `200` | 正数。节点的并发会话数（Egress 会话加本地 listener 会话）。必须放得进元数据预算（§14）。双向节点为本地 listener 预留 `max(1, max_sessions/4)` 个会话（上限为 1 时不预留）和 1/8 的元数据预算。`dbbridge install` 据此设置 `LimitNOFILE`。 |
| `limits.max_endpoints_per_peer` | int | `16` | 正数。 |

## 11. `transport`

各项的作用以及何时需要调整，见[传输](transport.zh-CN.md)。

| 配置项 | 类型 | 默认值 | 约束 |
|---|---|---|---|
| `protocol` | string | `auto` | `auto`、`ws`、`http-sse`。 |
| `protocol_version` | string | `"1.4"` | 必须是字符串 `"1.4"`。 |
| `ws.handshake_timeout` | 时长 | `5s` | > 0。限制 WebSocket 升级（TCP、TLS、`101`）。 |
| `ws.write_idle_timeout` | 时长 | `60s` | > 0。 |
| `frame_size` | 尺寸 | `min(32KiB, max_frame_size)` | > 0，≤ `max_frame_size`。 |
| `max_frame_size` | 尺寸 | `min(256KiB, up_window_bytes)` | > 0，≤ 1 MiB，≤ `up_window_bytes`。 |
| `up_max_inflight_frames` | int | `8` | 正数；≤ `metadata_limit / 192`。 |
| `up_window_bytes` | 尺寸 | `min(2MiB, buffer_per_direction)` | ≥ `max_frame_size`，≤ `buffer_per_direction`。 |
| `receipt_limit_per_session` | int | `max(64, 2 × up_max_inflight_frames + 8)` | ≥ `2 × up_max_inflight_frames + 8`；≤ `metadata_limit / 160`。 |
| `h2_pool_size` | int | `4` | 1–64。每个端点用于 SSE 下行订阅的 HTTP 客户端分片数；分片按订阅需要逐个创建。 |
| `heartbeat` | 时长 | `15s` | > 0。载体保活心跳；也是 HTTP/2 PING 间隔（PING 超时 5 s）。 |
| `failure_detect_timeout` | 时长 | `max(45s, 3 × heartbeat)` | ≥ `3 × heartbeat`。 |
| `recovery.enabled` | bool | `true` | |
| `recovery.grace` | 时长 | `30s` | 开启恢复时 > 0。 |
| `recovery.barrier_timeout` | 时长 | `min(5s, grace/2)` | 开启恢复时 > 0 且 < `grace`。 |
| `unattached_timeout` | 时长 | `30s` | > 0。 |
| `create_retry_timeout` | 时长 | `30s` | > 0。 |
| `ack_batch_bytes` | 尺寸 | `max(1, min(1MiB, buffer_per_direction/4))` | > 0，< `buffer_per_direction`。 |
| `ack_max_delay` | 时长 | `200ms` | > 0。 |
| `up_ack_max_delay` | 时长 | `10ms` | > 0。 |
| `chunk_size` | 尺寸 | `32KiB` | > 0。 |
| `buffer_per_direction` | 尺寸 | `4MiB` | > 0。 |
| `global_buffer_limit` | 尺寸 | `256MiB` | > 0，> `suspended_buffer_limit`。 |
| `suspended_buffer_limit` | 尺寸 | `max(1, min(64MiB, global_buffer_limit/4))` | > 0，< `global_buffer_limit`。 |
| `codec_buffer_limit` | 尺寸 | `32MiB` | 可能使用 SSE 时 ≥ `6 × max_frame_size + 16KiB`（见 §13）；只允许 `ws` 的纯 Egress 节点 ≥ `max(2 × max_frame_size + 56, 16KiB)`。 |
| `metadata_limit` | 尺寸 | `32MiB` | ≥ 512 KiB；必须容纳 `limits.max_sessions` 个会话（§14）。 |
| `idempotency_tombstone_ttl` | 时长 | `10m` | ≥ `10m`。 |
| `dial_timeout` | 时长 | `5s` | > 0。Egress → 数据库（DNS 与 TCP）。 |
| `peer_dial_timeout` | 时长 | `dial_timeout` | > 0。Ingress → 对端 endpoint，仅限 TCP 建连。 |
| `control_request_timeout` | 时长 | `10s` | > 0。 |
| `up_request_timeout` | 时长 | `30s` | > 0。 |
| `socket_write_timeout` | 时长 | `60s` | > 0。 |
| `sse_write_idle_timeout` | 时长 | `60s` | > 0。 |
| `backpressure_timeout` | 时长 | `120s` | > 0。 |
| `shutdown_timeout` | 时长 | `30s` | > 0。停止（第一次 Ctrl-C、SIGTERM、服务停止）时等待已打开会话自行结束的最长时间，超时后关闭它们；数据库客户端通常保持连接池中的连接，因此停止往往会等满这段时间。等待期间每 5 秒记录一次进度；再按一次 Ctrl-C（非 Windows 上也可再发 SIGTERM）立即强制退出，退出码 130。 |
| `use_environment_proxy` | bool | `false` | 对端连接使用 `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY`（含小写形式）。每个进程只读取一次。 |
| `user_agent` | 字符串 | `DBBridge/<协议版本>` | 本节点发往对端的每个请求和 WebSocket 握手使用的 `User-Agent`，替代 Go 默认的 `Go-http-client/1.1`/`2.0`。仅可打印 ASCII，最长 256 字节，首尾不能有空格。 |
| `sql_replay` | bool | `false` | 必须为 `false`。 |

## 12. 派生默认值

部分默认值会跟随相邻配置项变化，这样修改一个配置项时，不会与你没有设置的配置项发生约束冲突。显式设置的值始终保留。

| 配置项 | 未设置时的默认值 | 其他项均为默认时 |
|---|---|---|
| `transport.failure_detect_timeout` | `max(45s, 3 × heartbeat)` | 45 s |
| `transport.up_window_bytes` | `min(2MiB, buffer_per_direction)` | 2 MiB |
| `transport.ack_batch_bytes` | `max(1, min(1MiB, buffer_per_direction/4))` | 1 MiB |
| `transport.max_frame_size` | `min(256KiB, up_window_bytes)` | 256 KiB |
| `transport.frame_size` | `min(32KiB, max_frame_size)` | 32 KiB |
| `transport.suspended_buffer_limit` | `max(1, min(64MiB, global_buffer_limit/4))` | 64 MiB |
| `transport.recovery.barrier_timeout` | `min(5s, grace/2)`（至少 1 ms） | 5 s |
| `transport.receipt_limit_per_session` | `max(64, 2 × up_max_inflight_frames + 8)` | 64 |
| `transport.peer_dial_timeout` | `dial_timeout` | 5 s |
| `peers[].failover.healthcheck_timeout` | `min(3s, healthcheck_interval/2)`（至少 1 ms） | 3 s |
| `peers[].failover.enabled` | 多于一个 endpoint 时为 `true` | — |

约束与某个默认值冲突时，错误信息会注明，例如 `max_frame_size (524288) must be <= up_window_bytes (262144, default)`。

## 13. 跨字段约束

| 约束 | 适用范围 |
|---|---|
| `frame_size ≤ max_frame_size ≤ 1 MiB` | 始终 |
| `max_frame_size ≤ up_window_bytes ≤ buffer_per_direction` | 始终 |
| `ack_batch_bytes < buffer_per_direction` | 始终 |
| `suspended_buffer_limit < global_buffer_limit` | 始终 |
| `failure_detect_timeout ≥ 3 × heartbeat` | 始终 |
| `receipt_limit_per_session ≥ 2 × up_max_inflight_frames + 8` | 始终 |
| `receipt_limit_per_session ≤ metadata_limit / 160`，`up_max_inflight_frames ≤ metadata_limit / 192` | 始终 |
| `metadata_limit ≥ 512 KiB` 且能容纳 `limits.max_sessions` 个会话 | 始终（§14） |
| `codec_buffer_limit ≥ 6 × max_frame_size + 16 KiB` | 可能使用 SSE 时：`http.allowed_transports` 含 `http-sse`、配置了任何 listener 或 peer，或 `http.enabled: false` |
| `codec_buffer_limit ≥ max(2 × max_frame_size + 56, 16 KiB)` | `http.allowed_transports: [ws]` 的纯 Egress 节点 |
| `0 < barrier_timeout < grace` | 开启恢复时 |
| `peers[].recovery.enabled: true` 仅在 `transport.recovery.enabled: true` 时允许 | 始终 |
| `0 < healthcheck_timeout < healthcheck_interval` | 每个 peer |
| `idempotency_tombstone_ttl ≥ 10m` | 始终 |
| 使用 `credentials_file` 时不得有内联 `inbound_peers` 和 peer 直配令牌 | 始终 |
| 启用的入站身份 `max_sessions` 之和 ≤ `limits.max_sessions` | 启用身份多于一个时 |
| `allowed_targets` 的每项都存在于 `targets` | 始终 |
| 每个 `peers[].credential_ref` 都存在于 `outbound_peers` | 使用 `credentials_file` 时 |

## 14. 会话容量与元数据预算

每个会话都会预先占用一份会话元数据。校验公式为：

```
可用量    = metadata_limit − 2 × min(metadata_limit / 4, 1 MiB)
每会话   ≈ 64 KiB + 160 × receipt_limit_per_session + 192 × up_max_inflight_frames
             + 32 × (buffer_per_direction / chunk_size + 513) + 8 KiB
max_sessions × 每会话 ≤ 可用量
```

全部取默认值时，每个会话约需 103 KiB，可用量为 30 MiB，因此 `metadata_limit: 32MiB` 最多容纳 **296** 个会话。更大的 `limits.max_sessions` 会被拒绝，并给出建议值，例如 `raise transport.metadata_limit to at least 33 MiB or lower limits.max_sessions to 296`。`gen-config --max-sessions N` 会写入足够的 `metadata_limit`（大于 32 MiB 时为 `ceil((N × 112 KiB + 4 MiB) / 1 MiB)` MiB）。

同一预算还存放保证建连幂等的建连记录和墓碑，因此它也限制了可持续的新建会话速率，见[运维 → 容量规划](operations.zh-CN.md#10-容量规划)。

## 15. 警告

`check-config` 把不致命的问题以 `Warning: ...` 输出到 stderr；`run` 在启动时记录这些警告（`configuration warning`），`install` 随安装计划一起输出。

- 令牌短于 16 字节；
- 令牌文件或 TLS 私钥组可写或其他用户可访问（Unix）；
- 凭据文件位于 `/home`、`/root`、`/run/user`、`/tmp` 或 `/var/tmp` 下而配置文件不在（已安装的服务看不到这些文件；请把凭据文件放在配置文件旁边）；
- 主机名目标未配置 `allowed_cidrs`；
- listener 绑定非回环地址；设置了 `allow_remote` 的非回环 admin；启用显式开关后在非回环 `http.listen` 上使用明文；
- 不规范的 ID；
- 入站配额相关警告（§6.2）；
- `http.enabled: false` 却配置了 targets、入站凭据或 `http.tls`；节点无事可做；
- 会话恢复不可能成功：对强制使用单一载体（`ws` 或 `http-sse`）且只有一个可用 endpoint 的 peer，`transport.recovery.grace` 不大于 `failover.cooldown + recovery.barrier_timeout`。请调大 `grace`、调小 `cooldown`、改用 `auto` 或增加 endpoint。

## 16. 运行中重新读取的内容

| 变更 | 生效时机 |
|---|---|
| 正在使用的任一令牌文件的内容（内联或凭据文件中） | 下一次重载 |
| 主配置中的 `auth.inbound_peers`：增删身份、启用/停用、`token_files`、`allowed_targets`、`max_sessions` | 下一次重载 |
| 已有 peer 的 `peers[].token_file` / `token_env` | 下一次重载 |
| 整个凭据文件（入站和出站） | 下一次重载 |
| 存放令牌的环境变量的值 | 仅重启后 |
| 已安装服务中新增一个令牌环境变量名 | 重新安装（服务环境快照在安装时固定）；建议改用令牌文件 |
| 增删 peer、新增 `auth.credentials_file` | 重启（重载会以 “requires a restart” 被拒绝） |
| 其他任何配置项（peers、endpoints、listeners、targets、http、admin、limits、transport、TLS 证书与 CA 文件、`reload_interval`） | 重启 |
| 已安装服务的 `transport.shutdown_timeout`、`limits.max_sessions`、1024 以下端口、文件位置 | 重新安装（`uninstall` 后再 `install`），因为服务定义是据此计算的 |

重载在以下时机发生：每隔 `auth.reload_interval`；收到 `SIGHUP` / 执行 `systemctl reload <name>`（Linux）；执行 `sc.exe control <name> paramchange`（Windows 服务）。被拒绝的候选凭据不会生效，之前的凭据继续有效，见[运维 → 重载语义](operations.zh-CN.md#3-重载语义)。
