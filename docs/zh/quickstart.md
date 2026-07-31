# 开发者快速入门

如果只是评估 Provisa 而不需要从源码构建，请参阅[快速开始](index.md) — 下载 macOS、Windows 或 Linux 安装包并运行 `provisa start`。(REQ-223, REQ-224, REQ-227)

本指南适用于**从代码仓库**运行 Provisa —— 用于主动开发、调试或贡献代码。

---

## 先决条件

- **Docker Desktop**（运行中）
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. 克隆并设置

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` 会创建 `.venv/`，通过 `pip install -e ".[dev]"` 安装所有 Python 依赖，并将 git 钩子配置到 `.githooks/`。[tool-verified: setup.sh lines 5–9]

---

## 2. 启动全部服务

```bash
./start-ui.sh
```

启动完成后你会看到：

```
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**它启动了什么：** [tool-verified: start-ui.sh]

- Docker Compose 核心服务（`docker-compose.core.yml`）——PostgreSQL、PgBouncer、Trino、Redis（REQ-055）
- Docker Compose 开发覆盖层（`docker-compose.dev.yml`）——MinIO、Kafka、MongoDB、Elasticsearch、Neo4j、Fuseki、Debezium、Schema Registry（REQ-055）
- 后端 API，端口 8001（对 `provisa/` 和 `config/` 的更改支持热重载）（REQ-618）
- Vite UI 开发服务器，端口 3000（HMR）
- 端口 `http://localhost:3100` 上的 OpenTelemetry 追踪和 Grafana。可观测性栈是一个可选启用的 docker-compose `observability` profile（OTel Collector、Prometheus、Tempo、Grafana），在平台层面默认不开启；`start-ui.sh` 作为开发脚本的便利功能默认启用它，除非传入 `--no-observability`。（REQ-302, REQ-303, REQ-330）

**Ctrl+C** 会停止所有服务——后端、UI 和所有 Docker 服务——并还原任何配置补丁。（REQ-619）

**Ctrl+R** 仅重启后端（在热重载未能捕获的配置更改后很有用）。（REQ-619）

### 选项

`--no-observability` —— 禁用分布式追踪。默认情况下，`start-ui.sh` 会下载 OpenTelemetry Java agent（如果尚未存在），修补 Trino 的 `jvm.config` 以加载它，并启动 OTel collector、Prometheus、Tempo 和 Grafana。传入 `--no-observability` 可跳过以上全部操作。`jvm.config` 的修补会在 Ctrl+C 时还原。[tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` —— 在 Docker 服务健康后向 Kafka 填充演示数据。默认不运行。[tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` —— 在 Ctrl+C 后保留 Docker Compose 服务运行，而不是调用 `docker compose down`。[tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` —— 清除所有 Docker 卷并以全新状态重启。适用于 Docker 崩溃恢复。[tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` —— 启动额外的演示数据源（PostgreSQL pet-store 架构、OpenAPI petstore mock、SQLite，以及一个 GraphQL remote）。自动填充 petstore 的用户和订单数据。[tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase` —— 启用用于身份验证的身份提供方。不带此标志时，后端在没有身份验证提供方的情况下运行，所有请求都被视为 `admin`。[tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. 连接数据源

Provisa 从 `config/` 读取配置。添加一个数据源文件——例如 `config/sources/my-db.yaml`：

```yaml
sources:
  - id: my-pg
    type: postgresql
    host: localhost
    port: 5432
    database: mydb
    username: myuser
    password: ${MY_DB_PASSWORD}
    tables:
      - id: orders
        publish: true
        columns:
          - name: id
          - name: amount
          - name: region
          - name: customer_id
```

设置环境变量后，后端会在下一次重新加载时读取它：

```bash
export MY_DB_PASSWORD=secret
```

完整的 YAML 参考和所有支持的数据源类型，请参阅 [docs/configuration.md](configuration.md)。

---

## 4. 运行你的第一个查询

```bash
# GraphQL
curl -s -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}' | jq

# SQL — use the /data/sql endpoint
curl -s -X POST http://localhost:8001/data/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT id, amount, region FROM orders LIMIT 5"}' | jq
```

当 `config/provisa.yaml` 中不存在 `auth` 配置段时（开发环境下的默认状态），不需要身份验证。角色默认为 `admin`。[tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. 打开 UI

在浏览器中打开 `http://localhost:3000`。

导航栏有四个顶级菜单：[tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore（浏览）** —— 架构浏览器（`/schema`）、GraphQL 编辑器（`/query`）、Cypher 编辑器（`/graph`）、SQL 编辑器（`/sql`）
- **Model（建模）** —— 视图和命令
- **Security（安全）** —— 行级安全和列脱敏策略（REQ-038, REQ-041）
- **Admin（管理）** —— 概览、域、缓存、计划任务、系统健康状况、可观测性、用户、组织、角色

管理 GraphQL API 位于 `http://localhost:8001/admin/graphql`。[tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## 故障排查

**后端无法启动** —— 检查 `.logs/server.log`。最常见的原因是缺少环境变量或端口 8001 冲突。[tool-verified: start-ui.sh line 202] (REQ-618)

**Docker 服务不健康** —— 运行 `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` 查看哪个服务卡住了。联邦引擎在首次启动时大约需要 30 秒。（REQ-055）

**端口 3000 或 8001 冲突** —— `start-ui.sh` 在启动前会终止占用这些端口的旧进程。如果是其他程序占用了端口，请先手动停止它。[tool-verified: start-ui.sh lines 197–199] (REQ-619)

**全新启动** —— 停止脚本，然后运行 `./start-ui.sh --reset-volumes` 以清除所有卷并重启。[tool-verified: start-ui.sh line 19] (REQ-170)

---

## 后续步骤

| 目标 | 文档 |
|------|-----|
| 完整的 YAML 配置参考 | [configuration.md](configuration.md) |
| 行级安全、列脱敏、身份验证 | [security.md](security.md) |
| 所有支持的数据源类型 | [sources.md](sources.md) |
| 实时订阅 | [subscriptions.md](subscriptions.md) |
| JDBC、BI 工具、Arrow Flight、Apollo Federation | [integrations.md](integrations.md) |
| Python 客户端 | [python-client.md](python-client.md) |
| 生产环境部署 | [deployment.md](deployment.md) |
