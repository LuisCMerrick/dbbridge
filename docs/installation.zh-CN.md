[English](installation.md) | **简体中文**

# 安装、升级与卸载

[README](../README.zh-CN.md) · **安装** · [配置](configuration.zh-CN.md) · [传输](transport.zh-CN.md) · [安全](security.zh-CN.md) · [运维](operations.zh-CN.md) · [示例](examples.zh-CN.md)

- [1. 支持的平台](#1-支持的平台)
- [2. 发布包与校验和](#2-发布包与校验和)
- [3. 从源码构建](#3-从源码构建)
- [4. Linux systemd 服务](#4-linux-systemd-服务)
- [5. Windows 服务](#5-windows-服务)
- [6. WinSW 包装器（替代方案）](#6-winsw-包装器替代方案)
- [7. 文件布局与权限](#7-文件布局与权限)
- [8. 升级](#8-升级)
- [9. 卸载](#9-卸载)

## 1. 支持的平台

| 平台 | 二进制 | 服务安装 |
|---|---|---|
| Linux amd64、arm64 | 有 | `dbbridge install`（systemd） |
| Windows amd64 | 有 | `dbbridge install`（Windows SCM），或 WinSW |

**数据库。** DBBridge 转发 TCP 字节流而不解析内容，因此适用于任何基于 TCP 的数据库协议；常用于 MySQL、MariaDB 和 Microsoft SQL Server。注意：

- SQL Server 命名实例通过 SQL Browser 服务（UDP 1434）定位，而 DBBridge 不转发 UDP：请为实例配置固定的 TCP 端口，并把它作为目标地址。
- 数据库客户端连接的是本地 listener（例如 `127.0.0.1:13306`）。如果客户端要校验数据库的 TLS 证书，请在客户端中配置期望的服务器名称（例如驱动的“证书中的主机名”选项），因为连接的主机是回环地址。

**协议。** 节点使用 DBBridge 协议 1.4。任意两个支持协议 1.4 的版本之间都能互通；可选特性按会话协商（[传输 §2](transport.zh-CN.md#2-协商值)）。

## 2. 发布包与校验和

每个版本发布以下文件：

| 文件 | 内容 |
|---|---|
| `dbbridge-<version>-linux-amd64.tar.gz`、`...-linux-arm64.tar.gz` | 目录 `dbbridge-<version>-<os>-<arch>/`，包含 `dbbridge`、`README.md`、`README.zh-CN.md`、`CHANGELOG*`、`docs/`、`configs/`、`deploy/`（如有 `LICENSE`/`NOTICE` 文件也会包含） |
| `dbbridge-<version>-windows-amd64.zip` | 内容相同，可执行文件为 `dbbridge.exe` |
| `dbbridge-linux-amd64`、`dbbridge-linux-arm64`、`dbbridge-windows-amd64.exe` | 单独的二进制文件 |
| `BUILD-INFO.txt` | 版本、源码标识、源码哈希、构建时间、`SOURCE_DATE_EPOCH`、Go 工具链 |
| `SHA256SUMS` | 以上所有文件（含 `BUILD-INFO.txt`）的 SHA-256 |

带后缀的标签（例如 `v1.5.0-rc1`）发布为预发布版本。

使用前先校验：

```bash
sha256sum --check --ignore-missing SHA256SUMS
tar xzf dbbridge-<version>-linux-amd64.tar.gz
./dbbridge-<version>-linux-amd64/dbbridge version
```

Windows 上：执行 `Get-FileHash .\dbbridge-<version>-windows-amd64.zip -Algorithm SHA256`，并与 `SHA256SUMS` 中对应的行比较。

构建是可复现的：相同的源码树、Go 工具链和 `SOURCE_DATE_EPOCH` 会生成完全相同的文件和校验和。

## 3. 从源码构建

需要 Go **1.27.2** 或更新版本（取自 `go.mod`），不需要 cgo。

```bash
go build -trimpath -o bin/dbbridge ./cmd/dbbridge
./bin/dbbridge version
```

测试与检查：

```bash
go vet ./...
go test ./... -count=1
go test -race ./... -count=1            # 需要 cgo（C 工具链）
bash scripts/verify.sh                  # CI 执行的全部检查，包括样例的 check-config
DBB_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/dbbridge_test' \
  go test -tags=database ./integration  # 可选；使用独立的测试数据库
```

发布产物：

```bash
bash scripts/build-release.sh dist
```

| 变量 | 默认值 | 作用 |
|---|---|---|
| `DBB_VERSION` | 源码日期（UTC）对应的 `1.4.<yy.mm.dd>` | 写入二进制和包名的版本号 |
| `DBB_COMMIT` | `source-<源码的 sha256>` | `dbbridge version` 显示的源码标识 |
| `SOURCE_DATE_EPOCH` | `HEAD` 的提交时间 | 构建日期和归档时间戳 |
| `DBB_PLATFORMS` | `linux/amd64 linux/arm64 windows/amd64` | 以空格分隔的 `GOOS/GOARCH` 列表；其他目标不受支持 |

每次推送和拉取请求都会运行 CI：格式检查，Linux 和 Windows（amd64）的 `go vet`，Linux 上的完整测试（含 `-race`），Windows 上的服务、文件和 CLI 测试，以及对每个样例执行 `check-config`。

## 4. Linux systemd 服务

### 4.1 准备主机

```bash
sudo install -m 0755 dbbridge-<version>-linux-amd64/dbbridge /usr/local/bin/dbbridge
sudo useradd --system --no-create-home --shell /usr/sbin/nologin dbbridge
sudo install -d -m 0750 -o root -g dbbridge /etc/dbbridge /etc/dbbridge/tokens /etc/dbbridge/tls
# 把 peer.yaml（以及 auth.yaml、令牌文件、TLS 文件）复制到 /etc/dbbridge，然后：
sudo chown root:dbbridge /etc/dbbridge/*.yaml /etc/dbbridge/tokens/* /etc/dbbridge/tls/*
sudo chmod 0640 /etc/dbbridge/*.yaml /etc/dbbridge/tokens/* /etc/dbbridge/tls/*
sudo -u dbbridge /usr/local/bin/dbbridge check-config -c /etc/dbbridge/peer.yaml
```

使用 `gen-config` 生成的节点对时，把 `peer-a.yaml` 和 `shared.token` 复制到节点 A，把 `peer-b.yaml` 和同一个 `shared.token` 复制到节点 B。生成的文件权限为 0600，属主为运行 `gen-config` 的用户；请按上面的方式调整属主和权限。

### 4.2 安装

```bash
sudo dbbridge install -c /etc/dbbridge/peer.yaml --service-user dbbridge --dry-run   # 先检查安装计划
sudo dbbridge install -c /etc/dbbridge/peer.yaml --service-user dbbridge --start
systemctl status dbbridge
journalctl -u dbbridge -f
```

`install`（需以 root 运行，且 systemd 为系统管理器）会：

1. 与 `check-config` 完全相同地加载并校验配置（出错时退出码 78）。
2. 使用服务账户的 uid 和所属组，检查配置、凭据文件、令牌文件、TLS 和 CA 文件是否可读，沿途每级目录是否可进入，可执行文件是否可执行。
3. 如果配置从环境变量读取令牌，把这些变量写入 `<配置目录>/<服务名>.service-env.json`（0600，属主为服务账户），并通过 `--env-file` 传给服务。这些变量必须存在于执行 `install` 的环境中（`sudo -E` 或 `sudo VAR=... dbbridge install ...`）。已存在且内容不同的快照会被拒绝，请先更新或删除它。
4. 写入 `/etc/systemd/system/<服务名>.service`（带 `# Managed by DBBridge` 标记），执行 `daemon-reload` 和 `enable`，指定 `--start` 时启动服务。已存在同名服务时拒绝安装。

写入单元的可执行文件路径是你调用时使用的路径（保留符号链接，并给出一条指明链接目标的警告），或 `--exec-path <path>` 指定的路径。请使用稳定的路径，例如 `/usr/local/bin/dbbridge`。

`--service-name <name>` 可以并列安装多个实例（各自使用自己的配置目录）。

### 4.3 生成的单元

| 设置 | 值 |
|---|---|
| `Type=notify`、`NotifyAccess=main` | 启动后发送 `READY=1`，排空时发送 `STOPPING=1` |
| `User` | `--service-user`（默认 `root`，会给出警告） |
| `WorkingDirectory` | 配置文件所在目录 |
| `ExecStart` | `<exe> run --config <绝对路径> --service-name <name> --log-level <l> --log-format <f> [--env-file <快照>]` |
| `ExecReload` | `/bin/kill -HUP $MAINPID`（`systemctl reload` 会重新读取凭据） |
| `Restart=on-failure`、`RestartSec=5s`、`RestartPreventExitStatus=78` | 配置错误不重试 |
| `StartLimitIntervalSec=300`、`StartLimitBurst=10`、`TimeoutStartSec=30s` | |
| `TimeoutStopSec` | `max(45s, transport.shutdown_timeout + 30s)`，默认 60 s |
| `LimitNOFILE` | `max(8192, 4 × limits.max_sessions + 1024)` |
| `CapabilityBoundingSet`、`AmbientCapabilities` | 为空；仅当 `http.listen`、`admin.listen` 或某个 listener 使用 1024 以下端口时为 `CAP_NET_BIND_SERVICE` |
| 沙箱 | `NoNewPrivileges`、`UMask=0077`、`ProtectSystem=strict`、`ProtectHome=true`（可执行文件、配置或被引用的文件位于 `/home`、`/root` 或 `/run/user` 下时为 `read-only`）、`PrivateTmp=true`（其中有文件位于 `/tmp` 或 `/var/tmp` 下时为 `false`）、`PrivateDevices`、`ProtectKernelTunables`、`ProtectKernelModules`、`ProtectKernelLogs`、`ProtectControlGroups`、`ProtectClock`、`ProtectHostname`、`RestrictNamespaces`、`RestrictRealtime`、`RestrictSUIDSGID`、`LockPersonality`、`MemoryDenyWriteExecute`、`SystemCallArchitectures=native`、`SystemCallFilter=@system-service`、`RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX` |

所有值都在执行 `install` 时计算。修改 `transport.shutdown_timeout`、`limits.max_sessions`、1024 以下的端口、可执行文件/配置/凭据文件的位置，或令牌环境变量的集合之后，请重新执行 `uninstall` 和 `install`。

### 4.4 日常操作

```bash
sudo systemctl reload dbbridge     # 立即重新读取凭据（也会每隔 auth.reload_interval 自动读取）
sudo systemctl restart dbbridge    # 应用其他配置修改；会先排空
sudo systemctl stop dbbridge       # 排空（shutdown_timeout）后停止
```

### 4.5 静态单元

`deploy/systemd/dbbridge.service` 是一个等效的手写单元，适用于 `User=dbbridge`、`/etc/dbbridge/peer.yaml` 和 `/usr/local/bin/dbbridge`，并带有可选的 `EnvironmentFile=-/etc/dbbridge/environment`（0600 `root:root`）。把它复制到 `/etc/systemd/system/`，调整路径，随 `transport.shutdown_timeout` 调大 `TimeoutStopSec`、随 `limits.max_sessions` 调大 `LimitNOFILE`，然后执行 `systemctl daemon-reload && systemctl enable --now dbbridge`。`dbbridge uninstall` 不会删除手写的单元。

## 5. Windows 服务

在管理员 PowerShell 中：

```powershell
.\dbbridge.exe check-config -c C:\dbbridge\peer.yaml
icacls C:\dbbridge\peer.yaml    /grant "NT AUTHORITY\LOCAL SERVICE:(R)"
icacls C:\dbbridge\shared.token /grant "NT AUTHORITY\LOCAL SERVICE:(R)"
.\dbbridge.exe install -c C:\dbbridge\peer.yaml --service-user LocalService --dry-run
.\dbbridge.exe install -c C:\dbbridge\peer.yaml --service-user LocalService --start
Get-Service dbbridge
sc.exe control dbbridge paramchange      # 立即重新读取凭据
Restart-Service dbbridge                 # 应用其他配置修改
```

- 服务名 `<服务名>`，显示名 `DBBridge (<服务名>)`，自动启动，依赖 `Tcpip`。账户默认为 `LocalSystem`（会给出警告），可通过 `--service-user` 改为 `LocalService`/`NetworkService`。
- `install` 通过 DACL 检查该账户对每个被引用文件的读权限，对无法确认的文件逐一给出 `icacls` 命令。`gen-config` 写出的文件只有创建者、Administrators 和 SYSTEM 可读，因此需要按上面的方式授权。新建的环境快照会为所选服务账户授予读权限。
- 日志写入“应用程序”事件日志，事件源为服务名。
- 停止、系统关机和预关机都会排空节点；预关机超时为 `transport.shutdown_timeout + 30 s`。
- 配置或凭据错误会以服务特定退出码 78 停止服务（`sc.exe query dbbridge` 显示 `SERVICE_EXIT_CODE : 78`），并写入一条事件日志错误，服务不会被重启。修正配置后再启动服务。其他失败在 5 s 后重启（失败计数一天后清零）。
- `sc.exe control <name> paramchange` 触发凭据重载，作用相当于 Linux 上的 `SIGHUP`。

## 6. WinSW 包装器（替代方案）

`deploy/windows/dbbridge.xml` 是 WinSW 服务包装器的样例（请从 WinSW 官方发布页下载，并校验其校验和）。与内置服务的区别：

- 样例中的服务 ID `DBBridge` 与内置安装器的默认名 `dbbridge` 是同一个 SCM 服务（服务名不区分大小写）：同一个服务名只能二选一。
- 日志写入 `%BASE%\logs`，而不是事件日志。请授予服务账户对该目录的修改权限，以及对配置和密钥的读权限。
- WinSW 无法区分退出码：样例在 5 s 后重启一次，30 s 后再重启一次，之后保持停止（失败计数一小时后清零）。
- `<stoptimeout>` 在 XML 中是固定值（60 s）；调大 `transport.shutdown_timeout` 时请同步调大（至少 `shutdown_timeout + 30 s`）。
- 不支持 `paramchange`；凭据每隔 `auth.reload_interval` 重新读取。

## 7. 文件布局与权限

推荐的 Linux 布局（Windows 上使用一个目录，例如 `C:\dbbridge`，并为服务账户授予读权限）：

| 路径 | 属主 / 权限 | 说明 |
|---|---|---|
| `/usr/local/bin/dbbridge` | `root:root` 0755 | 写入单元的稳定路径。 |
| `/etc/dbbridge/` | `root:dbbridge` 0750 | 配置目录 = 工作目录 = 相对路径的基准。 |
| `/etc/dbbridge/peer.yaml` | `root:dbbridge` 0640 | 主配置。 |
| `/etc/dbbridge/auth.yaml` | `root:dbbridge` 0640 | 凭据文件（如使用）。 |
| `/etc/dbbridge/tokens/*.token` | `root:dbbridge` 0640 | 令牌文件。所有凭据文件都放在配置目录下。 |
| `/etc/dbbridge/tls/*.crt`、`*.key` | `root:dbbridge` 0640 | 服务端证书和私钥；CA 证书包。 |
| `/etc/dbbridge/<服务名>.service-env.json` | 服务账户，0600 | 仅在令牌来自环境变量时存在。以明文保存密钥；`uninstall` 不会删除。 |
| `/etc/systemd/system/<服务名>.service` | `root:root` 0644 | 生成的单元。 |

`gen-config` 成功后不会留下临时文件；输出目录中名为 `.dbbridge-pair-*` 或 `.dbbridge-config-*` 的文件是中断运行后的残留，可以删除。

## 8. 升级

会话绑定在运行中的进程上。重启一个节点会结束经过它的会话；数据库客户端看到连接被关闭后重连。不同版本的节点之间可以互通（协议 1.4），因此一对节点的两端可以逐个升级，先后顺序不限。

对每个节点：

1. 阅读 [CHANGELOG](../CHANGELOG.zh-CN.md) 中的升级说明。
2. 下载发布包并按 `SHA256SUMS` 校验（§2）。
3. 以服务账户、用**新的**二进制校验现有配置：
   ```bash
   sudo -u dbbridge ./dbbridge-<version>-linux-amd64/dbbridge check-config -c /etc/dbbridge/peer.yaml
   ```
4. 在原安装路径上替换二进制：`sudo install -m 0755 ./dbbridge-<version>-linux-amd64/dbbridge /usr/local/bin/dbbridge`（Windows：先 `Stop-Service dbbridge`，再复制 `dbbridge.exe`）。如果用符号链接指向按版本存放的目录，切换链接即可；单元启动时运行链接当前指向的文件。
5. 使之生效：
   - 通常执行 `sudo systemctl restart dbbridge`（Windows：`Start-Service dbbridge`）。节点最多排空 `transport.shutdown_timeout`，然后启动新二进制；
   - 如果升级说明提到生成的服务定义有变化，或者你修改了服务定义据以计算的设置（§4.3），改为用相同的选项执行 `sudo dbbridge uninstall` 和 `sudo dbbridge install ...`。
6. 确认：`curl -s http://127.0.0.1:<admin 端口>/readyz` 返回 `ready`；日志中出现 `starting dbbridge peer node` 且没有错误；各 peer 的 `dbb_endpoint_state` 回到 `HEALTHY`；客户端已重连。
7. 用同样的方式升级另一个节点。

回滚时，放回之前的二进制并重启（第 4–5 步）。

## 9. 卸载

```bash
sudo dbbridge uninstall --service-name dbbridge --dry-run   # 检查
sudo dbbridge uninstall --service-name dbbridge
```

```powershell
.\dbbridge.exe uninstall --service-name dbbridge
```

`uninstall` 停止服务（等待排空），禁用并删除服务，在 Windows 上还会删除事件日志源。只删除由 `dbbridge install` 安装的服务。配置、凭据、TLS 文件和服务环境快照都会保留；确认无用后请自行删除，其中快照（`<服务名>.service-env.json`）包含密钥。最后再删除二进制和 `dbbridge` 账户。
