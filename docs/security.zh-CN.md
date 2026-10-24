[English](security.md) | **简体中文**

# 安全

[README](../README.zh-CN.md) · [安装](installation.zh-CN.md) · [配置](configuration.zh-CN.md) · [传输](transport.zh-CN.md) · **安全** · [运维](operations.zh-CN.md) · [示例](examples.zh-CN.md)

- [1. 安全模型](#1-安全模型)
- [2. 加固清单](#2-加固清单)
- [3. 令牌与凭据来源](#3-令牌与凭据来源)
- [4. 令牌轮换](#4-令牌轮换)
- [5. 紧急吊销](#5-紧急吊销)
- [6. 目标 ACL 与 `allowed_cidrs`](#6-目标-acl-与-allowed_cidrs)
- [7. Admin 端点暴露](#7-admin-端点暴露)
- [8. 服务账户与文件权限](#8-服务账户与文件权限)
- [9. 反向模式](#9-反向模式)

## 1. 安全模型

- **对端认证。** Ingress 发往 Egress 的每个请求都携带 Bearer 令牌（`Authorization: Bearer ...`）。Egress 把令牌映射到一个入站身份（`auth.inbound_peers[].id`），身份完全由令牌决定。已有会话上的请求还会携带 Egress 在建连时签发的会话令牌。
- **节点身份。** Ingress 每次探测都会用 `peers[].expected_node_id` 核对 Egress 的 `node.id`，并把每个会话绑定到 Egress 的进程实例。会话绝不会在另一个节点或进程上继续。
- **授权。** 一个身份只能对其 `allowed_targets` 中的 target ID 建立会话。调用方只给出 target ID，数据库地址只配置在 Egress 上（`targets`），并可用 `allowed_cidrs` 进一步限制。
- **本地 listener 没有认证。** 任何能连接到 listener 的人，都会以本节点的凭据访问远端数据库。数据库自身的认证仍然端到端生效；DBBridge 只转发字节，从不解析或改写。
- **传输机密性**由 TLS 提供（节点自身或反向代理）。跨网络使用明文必须显式开启专门的配置项。

## 2. 加固清单

1. **每一跳网络都使用 TLS。** 要么在 Egress 上配置 `http.tls`，要么让 `http.listen` 只监听回环地址，前面放一个本机 TLS 终结代理。保持 `peers[].tls.verify: true`（默认值）；私有 CA 使用 `tls.ca_file`。除测试环境外，不要设置 `allow_insecure_http`、`http.allow_insecure_listen`、或 `tls.allow_insecure_skip_verify`。
2. **每个方向、每个身份各用一个令牌。** 为方便起见，`gen-config` 在一对节点的两个方向上共用一个令牌；生产环境请为每个方向单独配置令牌（参见[示例](examples.zh-CN.md#4-使用-tls-与凭据文件的生产节点对)中的生产示例）。令牌使用 32 个随机字节（`openssl rand -hex 32`）。
3. **使用令牌文件而不是环境变量**，这样轮换令牌无需重启（§3）。
4. **最小权限的 ACL。** `allowed_targets` 只列出需要的目标；每个目标都设置 `allowed_cidrs`，主机名目标尤其如此。
5. **listener 绑定回环地址。** 把 `listeners[].listen` 绑定到 `127.0.0.1`（或用防火墙限制该端口）。
6. **Admin 只监听回环地址**（§7）。
7. **专用服务账户**，只读访问配置和密钥（§8）。密钥文件权限为 0600，或配合服务组使用 0640。
8. **配额。** 为每个身份设置 `inbound_peers[].max_sessions`，避免某个对端占满所有会话；双向节点上让入站配额之和低于 `limits.max_sessions`。
9. **部署前校验。** 在目标主机上以服务账户运行 `dbbridge check-config`，把每条 `Warning:` 都当作需要处理的问题。
10. **反向模式。** 数据库侧保持 `reverse.tls.verify: true`，公网侧通过 TLS 提供服务（§9）。
11. **监控凭据状态。** 对 `dbb_auth_credentials_stale == 1` 以及带 `code=DBB-CFG-020` 的日志告警（被拒绝的重载意味着之前的凭据仍在生效）。

## 3. 令牌与凭据来源

| 来源 | 位置 | 运行中是否重新读取 |
|---|---|---|
| `auth.inbound_peers[].token_files` / `peers[].token_file` 中的令牌文件（主配置） | 主配置 | 是：每隔 `auth.reload_interval`、收到 `SIGHUP` / `systemctl reload` 时，以及 Windows 上的 `paramchange` |
| 凭据文件中的令牌文件（`inbound_peers[].token_files`、`outbound_peers[].token_file`） | 凭据文件 | 是，同上 |
| 环境变量（`token_envs`、`token_env`） | 进程环境 | 否：运行中的进程看不到变量的变化，更换需要重启 |

在已安装的服务中，环境变量形式的令牌来自安装时写入的服务环境快照（`<配置目录>/<服务名>.service-env.json`，权限 0600）。更换这些令牌需要更新该文件并重启；新增变量名需要重新安装。快照以明文保存密钥，`uninstall` 不会删除它。

重载能改变什么、哪些需要重启，见[配置 → 运行中重新读取的内容](configuration.zh-CN.md#16-运行中重新读取的内容)。

会话绑定的是身份，而不是令牌文本：更换令牌不会中断已建立的会话。旧令牌被移除后，之后任何仍携带它的请求都会被 `401` 拒绝。

## 4. 令牌轮换

以下步骤可以保持所有会话不中断。只要令牌来自文件，就同样适用于内联凭据（包括 `gen-config` 生成的配置）和凭据文件。每一步都要等待重载完成：最多 `auth.reload_interval`（默认 5 s），也可以用 `systemctl reload <name>`（Linux）或 `sc.exe control <name> paramchange`（Windows）立即触发，并在日志中确认（`credentials reloaded`）。

**轮换节点 A 访问节点 B 所用的令牌**（B 为 Egress，B 上代表 A 的身份为 `A`）：

1. 在 **B** 上，把新令牌文件放在旧文件旁边（属主和权限相同，例如 0640 `root:dbbridge`），并加入该身份：
   ```yaml
   inbound_peers:
     - id: A
       token_files: [tokens/a-to-b.token, tokens/a-to-b.next.token]
   ```
   等待 B 完成重载。此时新旧两个令牌都被接受。
2. 在 **A** 上，复制新令牌文件，并把出站凭据指向它：`peers[].token_file: tokens/a-to-b.next.token`（内联）或 `outbound_peers[].token_file`（凭据文件）。等待 A 完成重载。此后 A 的所有请求（包括已建立会话上的请求）都使用新令牌。
3. 在 **B** 上，从 `token_files` 中移除旧文件并等待重载，然后删除旧令牌文件。

对于 `gen-config` 生成的节点对（两个方向共用一个令牌），在两个节点上同步执行第 1–3 步，使用一个新的共享文件：先在两端的 `auth.inbound_peers[].token_files` 中加入 `new.token`，再把两端的 `peers[].token_file` 设为 `new.token`，最后从两端的 `token_files` 中移除 `shared.token`。

直接原地改写令牌文件也会在下一次重载时生效；但由于两个节点不会在同一时刻重载，只有在可以接受短暂 `401` 应答的情况下才这样做。

**环境变量形式的令牌**无法在不重启的情况下轮换。先在 Egress 上列出两个变量（`token_envs: [TOKEN_CURRENT, TOKEN_NEXT]`）并重启，再切换 Ingress 上的变量并重启，最后从 Egress 上移除旧变量并再次重启。每次重启都会结束被重启节点上的会话。建议改用令牌文件。

如果重载被拒绝（日志 `credential reload rejected ... code=DBB-CFG-020`，指标 `dbb_auth_credentials_stale 1`），之前的凭据继续生效。根据 `err`/`cause_code` 修正问题后再次重载。

## 5. 紧急吊销

只移除令牌可以阻止携带该令牌的新请求，但不会关闭已建立的会话。要彻底切断某个身份：

1. 在 Egress 上，把该身份设为 `enabled: false`（或删除该身份），位置在 `auth.inbound_peers` 或凭据文件中：
   ```yaml
   inbound_peers:
     - id: plant-a
       enabled: false
       token_files: [tokens/plant-a.token]   # 停用期间不读取
       allowed_targets: [mysql-primary]
   ```
   停用身份的令牌来源不会被读取，因此可以在同一次修改中删除或移走泄露的令牌文件。
2. 触发重载：`sudo systemctl reload <name>`（Linux）、`sc.exe control <name> paramchange`（Windows），或等待 `auth.reload_interval`。
3. 确认：
   - 日志中出现 `credentials reloaded`，而不是 `credential reload rejected`；
   - `dbb_auth_credentials_stale` 为 `0`；
   - 该身份的会话已关闭：它们以 `DBB_TARGET_FORBIDDEN` 结束（Egress 上的 `dbb_session_events_total{role="egress",event="terminate",reason="forbidden"}` 增加），Ingress 关闭相应的本地客户端连接（日志 `session terminated`）。
4. 如果重载被拒绝，该身份仍处于启用状态，旧令牌仍然有效。候选凭据中任何一处无效都会导致整体被拒绝，包括与被吊销身份无关的部分。根据日志中的错误修正后再次重载；万不得已时停止 Egress。
5. 事后删除该身份或签发新令牌（§4），并检查通过它可以访问到的数据库账户。

更细粒度的吊销方式相同：从某个身份的 `allowed_targets` 中移除一个目标，重载时会关闭该身份到该目标的会话。调低 `max_sessions` 只限制新建会话，不会关闭已有会话。

## 6. 目标 ACL 与 `allowed_cidrs`

- `allowed_targets` 按身份列出 target ID，其他目标一律以 `403 DBB_TARGET_FORBIDDEN` 拒绝。
- `allowed_cidrs` 限制目标可以解析到、并被拨号的地址。请为每个目标设置。主机名目标未设置时 `check-config` 会给出警告，因为 DNS 变化（或重绑定攻击）可能让 Egress 拨号到任意内网地址。`gen-config` 为 IP 目标写入精确地址（`/32` 或 `/128`），对主机名目标则要求提供 `--a-allow-cidr`/`--b-allow-cidr`。
- 名称只解析到 `allowed_cidrs` 之外的地址时会被拒绝（`403`）；DNS 解析失败以 `502 DBB_DIAL_FAILED` 报告。

## 7. Admin 端点暴露

Admin 服务（`/healthz`、`/readyz`、`/metrics`）没有 TLS，也没有认证。除非设置 `admin.allow_remote: true`（会产生警告），`admin.listen` 必须是回环地址。`gen-config` 生成的配置和随附样例都在回环地址上启用了 admin。需要从其他主机抓取指标时，优先使用本机代理程序，或带认证的 TLS 终结代理；如果必须绑定非回环地址，请用主机防火墙限制访问来源。Admin 永远不会在对端 `http.listen` 端口上提供服务。

## 8. 服务账户与文件权限

**Linux。** `dbbridge install` 默认以 root 运行服务，但能力边界集为空：root 无法绕过文件权限，因此配置、凭据及其所在目录必须通过权限位对 uid 0 可读。建议使用专用账户：

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin dbbridge
sudo chown -R root:dbbridge /etc/dbbridge
sudo chmod 0750 /etc/dbbridge /etc/dbbridge/tokens
sudo chmod 0640 /etc/dbbridge/*.yaml /etc/dbbridge/tokens/*.token /etc/dbbridge/tls/*.key
sudo dbbridge install -c /etc/dbbridge/peer.yaml --service-user dbbridge --dry-run
```

`install` 会用该账户的 uid 和所属组检查每个被引用的文件是否可读、沿途每级目录是否可进入，不满足时报告具体路径并失败。生成的单元还启用了 systemd 沙箱（`ProtectSystem=strict`、`ProtectHome`、`PrivateTmp`、`NoNewPrivileges`、空能力集或在使用 1024 以下端口时仅保留 `CAP_NET_BIND_SERVICE`、系统调用过滤等，见[安装](installation.zh-CN.md#4-linux-systemd-服务)）。`ProtectHome` 和 `PrivateTmp` 根据安装时引用的文件计算，因此请把所有凭据文件放在配置文件旁边（例如 `/etc/dbbridge` 下）；`check-config` 会对位于 `/home`、`/root`、`/run/user`、`/tmp` 或 `/var/tmp` 下的凭据文件给出警告。之后新增的令牌文件同样必须对服务账户可读，否则重载会以 `DBB-CFG-020` 被拒绝。

**Windows。** 服务默认以 LocalSystem 运行（会给出警告）。建议使用 `--service-user LocalService`（或 `NetworkService`），并授予它对配置、凭据和 TLS 文件的读权限，例如 `icacls C:\dbbridge\peer.yaml /grant "NT AUTHORITY\LOCAL SERVICE:(R)"`。`install` 会检查访问权限，对无法确认的每个文件给出对应的 `icacls` 命令。`gen-config` 写出的文件带有受保护的 DACL（仅创建者、Administrators、SYSTEM 可访问），因此需要授权。

**静态密钥。** `gen-config` 以 0600 写出令牌和配置文件（Windows 上为受保护的 DACL）。环境快照必须是普通文件，不得有组或其他用户权限，属主为服务账户或 root；否则服务拒绝启动（退出码 78）。切勿把 `*.token` 或 `*.service-env.json` 文件提交到版本库。

## 9. 反向模式

反向模式下，数据库侧主动连到公网侧，公网侧的 Ingress 再通过隧道把会话送回去。变化如下：

- **隧道令牌等于数据库的钥匙。** 谁能拿着反向 peer 的令牌和正确的 `node.id` 拨入，谁就会收到公网侧发往该 peer 的数据库流量，包括客户端发出的数据库登录信息。因此公网侧以常量时间比较令牌，要求拨入方的 `node.id` 等于 `peers[].expected_node_id`、其 `X-DBB-Expected-Node-ID` 等于自己的 `node.id`，拒绝同一节点的第二个存活实例（`409 DBB_TUNNEL_CONFLICT`），并把每次拒绝计入 `dbb_reverse_hub_rejected_total{code}`。请对其增长告警。
- **数据库侧必须校验公网侧。** 开启 TLS 校验时，数据库侧只会把令牌发给真正的公网节点，并在 TLS 之后核对公网侧返回的 `X-DBB-Node-ID`。`reverse.tls.verify: false` 或明文 `http://`（`reverse.allow_insecure_http`）会让路径上的任何人冒充公网侧，向数据库侧的目标建立会话；除实验环境外请保持关闭。
- **授权仍在数据库侧。** 经隧道到达的会话仍按数据库侧的 `auth.inbound_peers`、`allowed_targets`/`allowed_cidrs` 校验，与正向模式完全相同。公网侧即使被攻破，也只能访问该身份允许的目标。
- **吊销。** 在公网侧轮换或吊销反向 peer 的令牌（凭据重载）会立即关闭用旧令牌建立的隧道；数据库侧每次拨号都重新读取 `reverse.token_file`。紧急吊销（§5）在两侧都与正向模式相同。
- **暴露面。** 除非设置 `http.enabled`，数据库侧不开任何监听端口。只有公网侧的 `http.listen` 可从网络访问；预认证连接限制按来源 IP 作用于隧道连接，已认证隧道内的流按隧道计数。
- **令牌。** 与正向模式一样，建议隧道使用专用令牌（`gen-config --reverse` 为整对配置写一个共享令牌，生产环境请拆分）。

