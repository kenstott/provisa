# CLI 参考

`provisa` 命令是通过 pip 安装的嵌入式层级的唯一入口点（REQ-1128）。
它启动运行时、管理许可证、触发元数据发布、部署模型，并控制维护横幅——无需 Docker、Node 或任何外部服务。

使用 `embedded` extra 安装它，该 extra 还会引入离线 DuckDB 扩展
和嵌入式 PostgreSQL 控制平面：

```bash
pip install 'provisa[embedded]'
```

**平台要求。** `provisa run` 需要 Python 3.12 以及具有 pgserver wheel 的平台：
linux x86_64、macOS 或 Windows x86_64。Linux aarch64 既没有 pgserver wheel 也没有源码分发包，
因此嵌入式层无法在该平台上运行。在 aarch64 上请使用容器层级。
[tool-verified: `_require_supported_interpreter()` at cli.py:513-546]

---

## 共享选项

多个子命令会调用 Provisa HTTP API。它们共享三个标志和两个环境
变量。[tool-verified: `_api_call()` at cli.py:345-376; each subcommand's argparse block
at cli.py:653-773]

| 标志 | 默认值 | 环境变量回退 |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _（无）_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300（`maintenance` 为 30） | _（无）_ |

`--api` 是正在运行的 Provisa 实例的基础 URL。在多租户模式下，主机名标识
组织——`https://acme.provisa.org` 路由到 acme 的租户。`--token` 是一个 Bearer 令牌；
当它为空时不发送 `Authorization` 头，这对于未启用身份验证的
部署是正确的行为。[tool-verified: cli.py:314-316, 357-365]

在你的 CI 环境中同时设置这两个变量，以避免每次调用都重复输入：

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

接受这些标志的子命令：`metadata export`、`env deploy`、`env fetch`、
`maintenance on`、`maintenance off`、`maintenance status`。

---

## provisa run

在单个进程中启动嵌入式 Provisa 系统——API 服务器和 UI 静态/代理服务器。
无需 Docker、无需 Node、无需外部服务。[tool-verified: cli.py module docstring lines 11-23;
`_cmd_run()` at cli.py:549-602]

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### 标志

| 标志 | 默认值 | 说明 |
| --- | --- | --- |
| `--demo` | 关闭 | 加载内置演示——通过嵌入式 SQLite 提供的 pet-store 和 shelter 示例域（REQ-414） |
| `--host` | `127.0.0.1` | 两个服务器的绑定地址 |
| `--api-port` | `8000` | API 服务器端口 |
| `--ui-port` | `3000` | UI 静态/代理服务器端口 |
| `--no-browser` | 关闭 | UI 就绪时跳过打开浏览器；仍会打印 URL |
| `--reset` | 关闭 | 在启动前删除并重建嵌入式控制平面存储；在 Provisa 升级后如果启动报告架构不匹配，请使用此选项 |
| `--data-dir` | `~/.provisa/native` | 保存嵌入式 PostgreSQL 集群和 DuckDB 扩展缓存的目录 |

[tool-verified: run subparser at cli.py:609-634]

### 环境变量

`provisa run` 在启动 HTTP 服务器之前会读取若干附加变量。
设置它们可覆盖 `load_profile("native", ...)` 原本会应用的默认值。
[tool-verified: `_apply_embedded_env()` at cli.py:83-116]

| 变量 | 作用 |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | 用客户提供的 Trino 协调器替换嵌入式 DuckDB 引擎（REQ-1129） |
| `PROVISA_ENGINE_URL` | 指向外部联邦引擎的另一种方式 |
| `PROVISA_CONFIG` | 要加载的配置文件；`--demo` 会将其设置为内置演示配置（REQ-1127） |
| `PROVISA_DEMO` | 由 `--demo` 设置为 `1`；将会话标记为演示运行 |
| `PROVISA_DEMO_DIR` | 演示示例数据目录的路径；由 `--demo` 设置 |
| `PROVISA_CONFIG_REPLACE` | 由 `--demo` 设置为 `true`，以允许演示配置覆盖任何现有配置 |
| `PROVISA_DUCKDB_EXT_DIR` | 预先准备好的 DuckDB 扩展目录；如果存在 `provisa-duckdb-ext` 包，会自动从中设置；不存在则 DuckDB 会在首次使用时从网络下载 |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### 启动顺序

1. 平台检查——如果 Python 不受支持或缺少 pgserver，则以明确的信息中止。
2. `--reset`（如果请求）——删除嵌入式 PostgreSQL 集群；它会在下一步重建。
3. 演示配置（如果指定 `--demo`）——设置 `PROVISA_CONFIG` 和 `PROVISA_DEMO_DIR`。
4. 嵌入式环境——启动 PostgreSQL 控制平面，解析其套接字 URL，并在安装了
   `provisa-duckdb-ext` 时预先准备离线 DuckDB 扩展。
5. 架构漂移检查——扫描实时控制平面以查找缺失的列。如果发现任何缺失，
   会打印 `--reset` 提示并以退出码 1 退出。V1 没有迁移；较新版本中新增的列
   需要重置。[tool-verified: `_control_plane_drift()` at cli.py:119-162]
6. 两个服务器并发启动。就绪通知器轮询 `GET /ready`（而不是 `/health`——
   `/ready` 终结点确认存储已连接且引擎已预热），并在其返回 200 时打开
   浏览器。[tool-verified: `_announce_ready()` at cli.py:182-224]

### 退出码

| 代码 | 含义 |
| --- | --- |
| 0 | 正常关闭（Ctrl-C） |
| 1 | 启动错误（平台检查失败、演示配置缺失、检测到架构漂移） |

### 示例

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

离线验证并安装许可证文件（REQ-1139）。该文件是由
provisa.dev 颁发的 `license.json`。[tool-verified: `_cmd_license_apply()` at cli.py:271-280]

```
provisa license apply <file>
```

| 参数 | 说明 |
| --- | --- |
| `file` | 许可证文件的路径；会应用 `~` 展开 |

退出码 0 表示许可证有效且已安装。退出码 1 表示许可证被拒绝；
原因会打印到 stderr。[tool-verified: cli.py:276-280]

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

显示机器 ID、试用状态、经过天数以及许可证有效性（REQ-1139）。
[tool-verified: `_cmd_license_status()` at cli.py:283-299]

```
provisa license status
```

无标志。打印四行——机器 ID、首次发现日期、经过天数、试用状态，以及
许可证状态——然后以退出码 0 退出。[tool-verified: cli.py:293-299]

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

触发正在运行的服务器进行按需元数据发布（REQ-1072/REQ-1074）。向
`POST /admin/metadata-export/publish` 发起请求——与管理选项卡的**立即发布**
按钮所调用的终结点相同，因此两条路径发送的都是同一份完整快照。
[tool-verified: `_cmd_metadata_export()`
at cli.py:302-342]

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| 标志 | 默认值 | 说明 |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`，其次是 `http://127.0.0.1:8000` | 在多租户模式下，主机名标识组织 |
| `--token` | `$PROVISA_API_TOKEN` | 持有 `org_settings` 的身份的 Bearer 令牌；在未启用身份验证的部署中可省略 |
| `--timeout` | `300` | 放弃 HTTP 调用前等待的秒数 |

[tool-verified: cli.py:654-669]

| 退出码 | 含义 |
| --- | --- |
| 0 | 每项资产都已发布 |
| 1 | 部分发布或连接失败；逐项资产的错误会打印到 stderr |

[tool-verified: cli.py:335-342]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

完整的配置参考——提供程序、凭据、`reconcile_cron`，以及快照包含的内容——
参见[元数据导出](metadata-export.md#from-the-command-line)。

---

## provisa env deploy

将某个 git ref 处的模型部署到一个环境中，使该树成为该环境的当前
模型（REQ-1496）。这是部署流水线运行的命令；其规则是部署始终是携带
身份、针对指定控制平面发出的一次调用。[tool-verified:
`_cmd_env_deploy()` at cli.py:395-417]

```
provisa env deploy --org ORG --env ENV --ref REF
                   [--dry-run] [--seed] [--message MSG]
                   [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| 标志 | 是否必需 | 说明 |
| --- | --- | --- |
| `--org` | 是 | 拥有该环境的组织 |
| `--env` | 是 | 将持有已部署模型的环境 |
| `--ref` | 是 | 组织仓库中的分支或提交 SHA |
| `--dry-run` | 否 | 报告将会发生的变更；不应用任何内容 |
| `--seed` | 否 | 同时应用仅创建型的类（角色）；仅当此次部署是首次创建该环境时才正确 |
| `--message` | 否 | 当目标环境受保护时，附加到审批请求上的备注 |
| `--api` | 否 | 参见[共享选项](#shared-options) |
| `--token` | 否 | 参见[共享选项](#shared-options) |
| `--timeout` | 否 | 默认 300 秒 |

[tool-verified: cli.py:677-711]

| 退出码 | 含义 |
| --- | --- |
| 0 | 部署已应用，或 `--dry-run` 已完成 |
| 2 | 环境受保护；部署仅被提出，未被应用 |

退出码 2 是刻意设计的。将一个待审批状态视为已发布部署的流水线是
错误的。[tool-verified: cli.py:416-417]

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

有关环境类、保护规则、合并报告以及审批生命周期的完整说明，
请参见[环境](environments.md#the-env-cli-commands)。

---

## provisa env fetch

将组织的远程分支拉取到其 Provisa 仓库中（REQ-1541）。当你想要指定
`origin/<branch>` 时，请在部署之前运行此命令。[tool-verified: `_cmd_env_fetch()` at
cli.py:420-432]

```
provisa env fetch --org ORG [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| 标志 | 是否必需 | 说明 |
| --- | --- | --- |
| `--org` | 是 | 要拉取其远程仓库的组织 |
| `--api` | 否 | 参见[共享选项](#shared-options) |
| `--token` | 否 | 组织管理员的 Bearer 令牌 |
| `--timeout` | 否 | 默认 300 秒 |

[tool-verified: cli.py:716-733]

每拉取一个分支打印一行——`origin/<name>  <sha12>`。成功时退出码为 0；
在 HTTP 或连接失败时抛出带有错误信息的 `SystemExit`。

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

---

## provisa maintenance on

在部署环境上升起计划维护横幅（REQ-1466）。在进行会使数据平面下线的
计划性工作之前运行此命令——例如在切换
`var.engine_cluster_mode`（这会替换整个引擎集群及其上的每个分片）之前（REQ-1465）。
[tool-verified: `_cmd_maintenance_on()` at cli.py:483-495]

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| 标志 | 说明 |
| --- | --- |
| `--message` | 覆盖部署环境的标准措辞；默认使用服务器的标准信息 |
| `--ends-at` | 预计工作结束的 ISO-8601 时间点，例如 `2026-08-14T22:30:00Z`；默认无预估值 |
| `--api` | 参见[共享选项](#shared-options) |
| `--token` | 持有 `platform_settings` 的身份的 Bearer 令牌 |
| `--timeout` | 默认 30 秒 |

[tool-verified: cli.py:743-773]

打印结果横幅状态并以退出码 0 退出。在 HTTP 或连接失败时抛出 `SystemExit`。

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

在工作完成后清除维护横幅（REQ-1466）。
[tool-verified: `_cmd_maintenance_off()` at cli.py:498-501]

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

打印结果横幅状态（active: false）并以退出码 0 退出。

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

在不改变维护横幅的情况下显示其当前状态（REQ-1466）。
[tool-verified: `_cmd_maintenance_status()` at cli.py:504-507]

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

打印横幅状态并以退出码 0 退出。

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## 快速参考

| 命令 | REQ | 作用 |
| --- | --- | --- |
| `provisa run` | REQ-1128 | 启动嵌入式 API + UI |
| `provisa run --demo` | REQ-414 | 使用 pet-store / shelter 示例数据启动 |
| `provisa run --reset` | REQ-1535 | 在启动前重建控制平面 |
| `provisa license apply <file>` | REQ-1139 | 离线安装许可证文件 |
| `provisa license status` | REQ-1139 | 显示机器 ID 及试用/许可证状态 |
| `provisa metadata export` | REQ-1072 | 按需发布元数据快照 |
| `provisa env fetch --org ORG` | REQ-1541 | 将远程分支拉取到 Provisa 仓库 |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | 将一个 ref 部署到某个环境 |
| `provisa maintenance on` | REQ-1466 | 升起维护横幅 |
| `provisa maintenance off` | REQ-1466 | 清除维护横幅 |
| `provisa maintenance status` | REQ-1466 | 显示当前横幅状态 |

## 另请参阅

- [环境](environments.md) —— 环境模型、受保护环境、部署审批生命周期
- [元数据导出](metadata-export.md) —— 目录提供程序、配置，以及快照包含的内容
- [部署](deployment.md) —— 容器层级和云端部署
