# CLI 参考 {: #cli-reference }

`provisa` 命令是通过 pip 安装的嵌入式层级的唯一入口。它启动运行时、管理许可证、触发元数据发布、部署模型并控制维护横幅。你不需要 Docker、Node 或任何外部服务。

使用 `embedded` extra 安装它，该 extra 还会引入离线 DuckDB 扩展
和嵌入式 PostgreSQL 控制平面：

```bash
pip install 'provisa[embedded]'
```

**平台要求。** `provisa run` 需要 Python 3.12 以及具有 pgserver wheel 的平台：
linux x86_64、macOS 或 Windows x86_64。Linux aarch64 既没有 pgserver wheel 也没有源码分发包，
因此嵌入式层无法在该平台上运行。在 aarch64 上请使用容器层级。

---

## 共享选项 {: #shared-options }

多个子命令会调用 Provisa HTTP API。它们共享三个标志和两个环境
变量。

| 标志 | 默认值 | 环境变量回退 |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _（无）_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300（`maintenance` 为 30） | _（无）_ |

`--api` 是正在运行的 Provisa 实例的基础 URL。在多租户模式下，主机名指定组织，所以 `https://acme.provisa.org` 会路由到 acme 的租户。`--token` 是 Bearer 令牌。它为空时不发送 `Authorization` 请求头，这正是未启用认证的部署所需要的。

在你的 CI 环境中同时设置这两个变量，以避免每次调用都重复输入：

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

接受这些标志的子命令：`metadata export`、每个 `env` 子命令（`list`、`show`、
`create`、`delete`、`diff`、`merge`、`deploy`、`revert`、`redo`、`fetch`、`push`、`pull`）、
`maintenance on`、`maintenance off`、`maintenance status`。

---

## provisa run

在单个进程中启动嵌入式 Provisa 系统：API 服务器，以及提供界面静态文件和代理的服务器。不涉及 Docker、Node 或外部服务。

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### 标志 {: #flags }

| 标志 | 默认值 | 说明 |
| --- | --- | --- |
| `--demo` | 关闭 | 加载内置演示——通过嵌入式 SQLite 提供的 pet-store 和 shelter 示例域 |
| `--host` | `127.0.0.1` | 两个服务器的绑定地址 |
| `--api-port` | `8000` | API 服务器端口 |
| `--ui-port` | `3000` | UI 静态/代理服务器端口 |
| `--no-browser` | 关闭 | UI 就绪时跳过打开浏览器；仍会打印 URL |
| `--reset` | 关闭 | 在启动前删除并重建嵌入式控制平面存储；在 Provisa 升级后如果启动报告架构不匹配，请使用此选项 |
| `--data-dir` | `~/.provisa/native` | 保存嵌入式 PostgreSQL 集群和 DuckDB 扩展缓存的目录 |

### 环境变量 {: #environment-variables }

`provisa run` 在启动 HTTP 服务器之前会读取若干附加变量。
设置它们可覆盖 `load_profile("native", ...)` 原本会应用的默认值。

| 变量 | 作用 |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | 用客户提供的 Trino 协调器替换嵌入式 DuckDB 引擎 |
| `PROVISA_ENGINE_URL` | 指向外部联邦引擎的另一种方式 |
| `PROVISA_CONFIG` | 要加载的配置文件；`--demo` 会将其设置为内置演示配置 |
| `PROVISA_DEMO` | 由 `--demo` 设置为 `1`；将会话标记为演示运行 |
| `PROVISA_DEMO_DIR` | 演示示例数据目录的路径；由 `--demo` 设置 |
| `PROVISA_DUCKDB_EXT_DIR` | 预先准备好的 DuckDB 扩展目录；如果存在 `provisa-duckdb-ext` 包，会自动从中设置；不存在则 DuckDB 会在首次使用时从网络下载 |

### 启动顺序 {: #startup-sequence }

1. 平台检查——如果 Python 不受支持或缺少 pgserver，则以明确的信息中止。
2. `--reset`（如果请求）——删除嵌入式 PostgreSQL 集群；它会在下一步重建。
3. 演示配置（如果指定 `--demo`）——设置 `PROVISA_CONFIG` 和 `PROVISA_DEMO_DIR`。
4. 嵌入式环境——启动 PostgreSQL 控制平面，解析其套接字 URL，并在安装了
   `provisa-duckdb-ext` 时预先准备离线 DuckDB 扩展。
5. 架构漂移检查——扫描实时控制平面以查找缺失的列。如果发现任何缺失，
   会打印 `--reset` 提示并以退出码 1 退出。V1 没有迁移；较新版本中新增的列
   需要重置。
6. 两个服务器并发启动。就绪通知器轮询 `GET /ready`（而不是 `/health`——
   `/ready` 终结点确认存储已连接且引擎已预热），并在其返回 200 时打开
   浏览器。

### 退出码 {: #exit-codes }

| 代码 | 含义 |
| --- | --- |
| 0 | 正常关闭（Ctrl-C） |
| 1 | 启动错误（平台检查失败、演示配置缺失、检测到架构漂移） |

### 示例 {: #example }

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

离线验证并安装许可证文件。该文件是由
provisa.dev 颁发的 `license.json`。

```
provisa license apply <file>
```

| 参数 | 说明 |
| --- | --- |
| `file` | 许可证文件的路径；会应用 `~` 展开 |

退出码 0 表示许可证有效且已安装。退出码 1 表示许可证被拒绝；
原因会打印到 stderr。

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

显示机器 ID、试用状态、经过天数以及许可证有效性。

```
provisa license status
```

没有标志。打印五行（机器 ID、首次记录日期、已过天数、试用状态和许可证状态），并以 0 退出。

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

触发正在运行的服务器按需发布元数据。该命令向 `POST /admin/metadata-export/publish` 发送请求，也就是 Admin 选项卡中 **立即发布** 按钮调用的端点，所以两条路径发送的是同一份完整快照。

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| 标志 | 默认值 | 说明 |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`，其次是 `http://127.0.0.1:8000` | 在多租户模式下，主机名标识组织 |
| `--token` | `$PROVISA_API_TOKEN` | 持有 `org_settings` 的身份的 Bearer 令牌；在未启用身份验证的部署中可省略 |
| `--timeout` | `300` | 放弃 HTTP 调用前等待的秒数 |

| 退出码 | 含义 |
| --- | --- |
| 0 | 每项资产都已发布 |
| 1 | 部分发布或连接失败；逐项资产的错误会打印到 stderr |

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

完整的配置参考见 [元数据导出](metadata-export.md#from-the-command-line)，涵盖提供方、凭据、`reconcile_cron` 以及快照内容。

---

## provisa env

针对正在运行的 Provisa 执行环境生命周期。每个子命令都是界面所发起的同一个管理 API
调用；CLI 不做任何决定。副本携带内容的清单、合并与移除规则、绑定、保护以及调用方的权利
都属于服务器，因此无论以何种方式调用该操作，它们都同样适用。

```
provisa env {list,show,create,delete,diff,merge,deploy,revert,redo,fetch,push,pull} --org ORG ...
```

### 每个子命令都接受的标志 {: #flags-every-subcommand-takes }

| 标志 | 必填 | 说明 |
| --- | --- | --- |
| `--org` | 是 | 环境所属的组织 |
| `--json` | 否 | 以 JSON 打印 API 响应，供脚本使用 |
| `--api` | 否 | 参见[共享选项](#shared-options) |
| `--token` | 否 | 获准执行该操作的身份的 Bearer 令牌 |
| `--timeout` | 否 | 秒；默认 300 |

作用于单个环境的子命令还接受 `--env ENV`（必填）。`list`、`diff`、
`merge` 和 `fetch` 不接受；`diff` 和 `merge` 用 `--from` 和 `--into` 指定它们的环境。

### 退出码 {: #exit-codes_1 }

| 代码 | 含义 |
| --- | --- |
| 0 | 完成，或未应用任何内容的试运行 |
| 1 | 被拒绝。拒绝规则的代码以 `refused (<code>): <detail>` 打印到 stderr；使用 `--json` 时为 stdout 上的 `{"refused": {...}}` |
| 2 | 已提议，未应用：目标环境受保护，变更正在等待批准 |

退出码 2 是有意为之。把等待中的批准当作已发出的部署的流水线
会出错。

### provisa env list

列出组织的环境以及每个环境所在的构建。除共享标志外没有其他标志。

```
provisa env list --org ORG [--json]
```

### provisa env show

显示一个环境及其当前构建。如果
环境不存在，以 1 和 `environments.unknown` 退出。

```
provisa env show --org ORG --env ENV [--json]
```

### provisa env create

创建一个持有另一个环境模型的环境，其数据在创建时选定。由 API
决定调用方可以做出哪些选择。

```
provisa env create --org ORG --env NAME --data-mode MODE
                   [--from SOURCE] [--mutation-handling HANDLING] [--expires-at ISO8601]
```

| 标志 | 必填 | 说明 |
| --- | --- | --- |
| `--env` | 是 | 新环境的名称 |
| `--from` | 否 | 作为起点的环境；默认 `prod` |
| `--data-mode` | 是 | `inherit`、`unbound`、`test_fake` 或 `test_synthetic`：按引用使用数据源连接、在绑定自己的连接之前没有连接，或测试数据 |
| `--mutation-handling` | 否 | `refused`、`reversible` 或 `direct`；默认 `refused` |
| `--expires-at` | 否 | ISO-8601 时刻，此后环境被移除 |

```bash
provisa env create --org acme --env feature-x --from prod --data-mode test_fake
```

### provisa env delete

停用一个环境。只有点名时才删除其分支。

```
provisa env delete --org ORG --env ENV [--delete-branch] [--delete-remote-branch]
```

| 标志 | 说明 |
| --- | --- |
| `--delete-branch` | 同时删除其分支 |
| `--delete-remote-branch` | 同时删除其在远程仓库上的分支 |

### provisa env diff

报告将一个环境合并到另一个环境会做什么。它是 `merge-preview` 的 GET，因此
它不能写入；把它交给不应更改任何内容的运行器。

```
provisa env diff --org ORG --from SOURCE --into TARGET [--removals] [--select PATH ...]
```

| 标志 | 必填 | 说明 |
| --- | --- | --- |
| `--from` | 是 | 合并来源环境 |
| `--into` | 是 | 合并目标环境 |
| `--removals` | 否 | 同时从目标中移除来源已不再持有的内容 |
| `--select PATH` | 否 | 将合并限制为该对象，按其在模型中的路径（`metrics/revenue.yaml`）；重复以添加其他对象。它所需而目标缺少的内容随之带入并列出 |

### provisa env merge

按身份将一个环境合并到另一个环境。

```
provisa env merge --org ORG --from SOURCE --into TARGET
                  [--removals] [--select PATH ...] [--dry-run] [--message MSG]
                  [--retire-source] [--retire-remote]
```

| 标志 | 必填 | 说明 |
| --- | --- | --- |
| `--from` | 是 | 合并来源环境 |
| `--into` | 是 | 合并目标环境 |
| `--removals` | 否 | 同时从目标中移除来源已不再持有的内容 |
| `--select PATH` | 否 | 将合并限制为该对象，按其在模型中的路径（`metrics/revenue.yaml`）；重复以添加其他对象。它所需而目标缺少的内容随之带入并列出 |
| `--dry-run` | 否 | 打印合并报告，不应用任何内容 |
| `--message` | 否 | 合并做了什么；应用时必填 |
| `--retire-source` | 否 | 当来源的工作已进入目标时移除来源 |
| `--retire-remote` | 否 | 配合 `--retire-source`，同时删除来源在远程仓库上的分支 |

### provisa env deploy

将某个 git 引用处的模型部署到环境中，使该树成为环境的当前模型
。这是部署流水线运行的命令；部署始终是
针对指定控制平面、带身份的调用。

```
provisa env deploy --org ORG --env ENV --ref REF [--dry-run] [--message MSG]
```

| 标志 | 必填 | 说明 |
| --- | --- | --- |
| `--env` | 是 | 将持有所部署模型的环境 |
| `--ref` | 是 | 组织仓库中的分支或提交 |
| `--dry-run` | 否 | 报告部署会做什么；不应用任何内容 |
| `--message` | 否 | 目标受保护时随批准请求携带的说明 |

加载到已有环境中时绝不应用角色。`--seed` 已不存在：API 以
`environments.seed_only_at_creation` 拒绝 seed。

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

### provisa env revert

将环境回退到之前的构建。不带 `--to` 时，它在环境自己的历史中
后退一个提交。带 `--to` 时，它就是对该构建的普通部署。

```
provisa env revert --org ORG --env ENV [--to BUILD] [--message MSG]
```

| 标志 | 必填 | 说明 |
| --- | --- | --- |
| `--env` | 是 | 要回退的环境 |
| `--to` | 否 | 要部署的构建；默认为当前构建之前的那个 |
| `--message` | 否 | 随批准请求携带的说明 |

### provisa env redo

再次前进到回退所离开的构建。

```
provisa env redo --org ORG --env ENV
```

### provisa env fetch

将组织的远程分支拉取到其 Provisa 仓库中。当你想指定 `origin/<branch>` 时，
在部署之前运行它。Provisa 从不按计时器运行它。

```
provisa env fetch --org ORG
```

每拉取一个分支打印一行，`origin/<name>  <sha12>`。

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

### provisa env push

将环境的分支发送到组织的远程仓库。没有可发送的内容时
打印 `nothing to send`。

```
provisa env push --org ORG --env ENV
```

### provisa env pull

应用远程仓库为某个环境持有的内容。没有可应用的内容时打印
`<env> already holds what the remote does`。退出码 2 的含义与
deploy 相同。

```
provisa env pull --org ORG --env ENV
```

关于环境类别、保护规则、合并报告以及
批准生命周期的完整说明，请参阅[环境](environments.md#the-env-cli-commands)。

---

## provisa maintenance on

在部署上升起计划维护横幅。在会让数据平面停机的计划工作之前运行它。切换 `var.engine_cluster_mode` 就是一例，因为它会替换引擎集群及其上的每个分片。

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

打印结果横幅状态并以退出码 0 退出。在 HTTP 或连接失败时抛出 `SystemExit`。

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

在工作完成后清除维护横幅。

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

在不改变维护横幅的情况下显示其当前状态。

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

## 快速参考 {: #quick-reference }

| 命令 | 作用 |
|---|---|
| `provisa run` | 启动嵌入式 API + UI |
| `provisa run --demo` | 使用 pet-store / shelter 示例数据启动 |
| `provisa run --reset` | 在启动前重建控制平面 |
| `provisa license apply <file>` | 离线安装许可证文件 |
| `provisa license status` | 显示机器 ID 及试用/许可证状态 |
| `provisa metadata export` | 按需发布元数据快照 |
| `provisa env list --org ORG` | 列出组织的环境及其构建 |
| `provisa env show --org ORG --env ENV` | 显示一个环境 |
| `provisa env create --org ORG --env ENV --data-mode MODE` | 从另一个环境创建环境 |
| `provisa env delete --org ORG --env ENV` | 停用一个环境 |
| `provisa env diff --org ORG --from A --into B` | 预览合并（只读） |
| `provisa env merge --org ORG --from A --into B` | 将一个环境合并到另一个环境 |
| `provisa env deploy --org ORG --env ENV --ref REF` | 将引用部署到环境中 |
| `provisa env revert --org ORG --env ENV [--to BUILD]` | 后退一个提交，或部署某个构建 |
| `provisa env redo --org ORG --env ENV` | 再次前进 |
| `provisa env fetch --org ORG` | 将远程分支拉取到 Provisa 仓库 |
| `provisa env push --org ORG --env ENV` | 将环境的分支发送到远程仓库 |
| `provisa env pull --org ORG --env ENV` | 应用远程仓库持有的内容 |
| `provisa maintenance on` | 升起维护横幅 |
| `provisa maintenance off` | 清除维护横幅 |
| `provisa maintenance status` | 显示当前横幅状态 |

## 另请参阅 {: #see-also }

- [环境](environments.md) —— 环境模型、受保护环境、部署审批生命周期
- [元数据导出](metadata-export.md) —— 目录提供程序、配置，以及快照包含的内容
- [部署](deployment.md) —— 容器层级和云端部署
