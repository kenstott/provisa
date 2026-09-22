# 开发者快速入门

如需在不从源代码构建的情况下评估 Provisa，请参见[快速入门](index.md)——下载 macOS、Windows 或 Linux 安装程序并运行 `provisa start`。(REQ-223, REQ-224, REQ-227)

本指南适用于**从代码仓库**运行 Provisa——用于日常开发、调试或贡献代码。

---

## 前置条件

- **Docker Desktop**（正在运行）
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

## 2. 启动一切

```bash
./start-ui.sh
```

启动完成后，您会看到：

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**它启动的内容：** [tool-verified: start-ui.sh]

- Docker Compose 核心服务（`docker-compose.core.yml`）—— PostgreSQL、PgBouncer、Trino、Redis（REQ-055）
- Docker Compose 开发叠加层（`docker-compose.dev.yml`）—— MinIO、Kafka、MongoDB、Elasticsearch、Neo4j、Fuseki、Debezium、Schema Registry（REQ-055）
- 8001 端口上的后端 API（对 `provisa/` 和 `config/` 的改动支持热重载）（REQ-618）
- 3000 端口上的 Vite UI 开发服务器（支持 HMR）
- `http://localhost:3100` 上的 OpenTelemetry 追踪和 Grafana。可观测性技术栈是一个可选启用的 docker-compose `observability` 配置文件（OTel Collector、Prometheus、Tempo、Grafana），在平台层面默认不启用；`start-ui.sh` 会将其作为开发脚本的便利功能默认启用，除非您传入 `--no-observability`。（REQ-302, REQ-303, REQ-330）

**Ctrl+C** 会停止一切——后端、UI 以及所有 Docker 服务——并还原任何配置补丁。（REQ-619）

**Ctrl+R** 只重启后端（在热重载未能捕获的配置更改之后很有用）。（REQ-619）

### 选项

`--no-observability` —— 禁用分布式追踪。默认情况下，`start-ui.sh` 会在尚未存在时下载 OpenTelemetry Java agent，修补 Trino 的 `jvm.config` 以加载它，并启动 OTel collector、Prometheus、Tempo 和 Grafana。传入 `--no-observability` 可跳过所有这些操作。`jvm.config` 补丁会在 Ctrl+C 时被还原。[tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` —— 在 Docker 服务健康后向 Kafka 播种演示数据。默认不运行。[tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` —— 在 Ctrl+C 之后保持 Docker Compose 服务继续运行，而不调用 `docker compose down`。[tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` —— 清除所有 Docker 卷并以干净状态重新启动。适用于 Docker 崩溃后的恢复。[tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` —— 启动额外的演示数据源（PostgreSQL 宠物商店架构、OpenAPI petstore mock、SQLite，以及一个 GraphQL 远程端点）。自动播种 petstore 的用户和订单数据。[tool-verified: start-ui.sh lines 17, 55–171]

`--source=<name>`（仅 `start-ui-install.sh`，可重复使用）—— 在 `--demo` 之外额外配置一个可选数据源。每个名称都映射到 `demo/sources/<name>/`。启动过程会调用 `demo/sources/provision.py up`，该脚本将该数据源的 `compose.yml` 作为其自己的 Docker Compose 项目（`provisa-demo-<name>`）启动，等待其健康检查通过，并在该数据源具备 `prime.py` 时运行它来播种数据。随后，启动过程会在 `${PROVISA_HOME:-~/.provisa}/demo/provisa-with-sources.yaml` 写入一个封装配置，该配置在基础配置之上包含每个数据源的 `fragment.yaml`，并从该文件启动。[tool-verified: `start-ui-install.sh`（搜索 `SOURCES`）, `demo/sources/provision.py`] (REQ-1669)

UI 端到端测试套件用来搭建这些数据源的正是同一个 `provision.py`（使用 `provisa-e2e-<name>` 项目前缀，运行在自己的端口上），因此演示所展示的种子数据与测试套件所断言的行是同一份定义。[tool-verified: `provisa-ui/e2e/demo-source-containers.ts`] (REQ-1671)

在 Docker 方式启动（未使用 `--demo`/`--native`）下，协调器是一个容器，因此每个数据源都会加入核心技术栈的网络，并以 `<name>:<container port>` 的形式注册；在原生启动方式下，则以 `localhost:<published port>` 的形式注册。如果某个数据源的 `demo/sources/<name>/engine` 文件指定了启动过程未运行的引擎，该数据源会被拒绝。

内置数据源：

| 名称 | 端口 | 说明 |
|------|---------|-------|
| `neo4j` | HTTP 27474, Bolt 27687 | 两张 Cypher 表（`adopter`、`adopter_referral`）；图数据由 `seed.cypher` 播种；表从片段中注册 |
| `mongodb` | 27117 | 数据源已注册；`product_reviews` 集合由 `db/mongo-init.js` 播种；通过“注册表”手动注册表 |
| `redis` | 26379 | 数据源已注册；`support_agent:*` 和 `agent_status:*` 哈希由 `prime.py` 播种；每个前缀通过“注册表”注册为一张表 (REQ-1675) |
| `cassandra` | 29042 | 数据源已注册；`shelter_ops.intake_events` 由 `prime.py` 播种（需要 `cassandra` 附加组件）；该键空间通过“注册表”注册为一个架构 (REQ-1676) |
| `sparql` | 23030 | Apache Jena Fuseki；数据源和一张基于查询的表（`volunteer`）从片段中注册，图数据由 `prime.py` 播种；可通过“注册表”（查询 + 预览）注册更多表 (REQ-1683) |
| `prometheus` | 29090 | 数据源已注册；服务器自我抓取，因此 `up` 和 `prometheus_*` 指标可通过“注册表”注册为表 (REQ-1689) |
| `elasticsearch` | 29200 | 数据源和索引映射已注册；`support_tickets` 索引由 `prime.py` 播种；由原生引擎通过 HTTP 读取（REQ-1672），在 Trino 上则通过连接器读取 |
| `splunk` | mgmt 8089, HEC 8088 | 数据源以令牌鉴权和 `disable_ssl_validation` 方式注册（该容器的证书是自签名的）；一个索引、七个庇护所警报事件以及 `shelter_alerts` 数据模型由 `prime.py` 播种，该脚本同时生成片段中作为 `PROVISA_DEMO_SPLUNK_TOKEN` 读取的 API 令牌。数据模型通过“注册表”注册为表——在 Trino 上通过 `splunk` 目录注册，在其他每种引擎上都通过该引擎所附带的内置 Calcite pgwire 服务器注册 (REQ-1694) |
| `chinook` | 25433 | 一个持有 snake_case Chinook 子集的 Postgres 数据库，该子集是 Hasura 元数据示例所追踪的对象，由 `prime.py` 从 `tests/fixtures/hasura_v2_t1_seed.sql` 播种；数据源从片段中注册，也是 `tests/fixtures/hasura_v2_t1_metadata.json` 的 Hasura v2 导入所落地的数据源 (REQ-1687) |

`--idp=basic|firebase` —— 为身份验证启用一个身份提供程序。不带此标志时，后端在没有身份验证提供程序的情况下运行，所有请求都被视为 `admin`。[tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. 连接一个数据源

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

设置环境变量后，后端会在下一次重新加载时读取到它：

```bash
export MY_DB_PASSWORD=secret
```

完整的 YAML 参考和所有支持的数据源类型，请参见 [docs/configuration.md](configuration.md)。

---

## 4. 运行您的第一条查询

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

当 `config/provisa.yaml` 中不存在 `auth` 配置节时（开发环境的默认情况），不需要任何身份验证。角色默认为 `admin`。[tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. 打开 UI

在浏览器中打开 `http://localhost:3000`。

导航栏包含四个顶级菜单：[tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore（探索）** —— 架构浏览器（`/schema`）、GraphQL 编辑器（`/query`）、Cypher 编辑器（`/graph`）、SQL 编辑器（`/sql`）
- **Model（模型）** —— 视图和命令
- **Security（安全）** —— 行级安全和列脱敏策略（REQ-038, REQ-041）
- **Admin（管理）** —— 总览、域、缓存、计划任务、系统健康状况、可观测性、用户、组织、角色

管理 GraphQL API 位于 `http://localhost:8001/admin/graphql`。[tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## 故障排查

**后端无法启动** —— 检查 `.logs/server.log`。最常见的原因是缺少环境变量或 8001 端口冲突。[tool-verified: start-ui.sh line 202] (REQ-618)

**Docker 服务不健康** —— 运行 `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` 查看哪个服务卡住了。联邦引擎在首次启动时大约需要 30 秒。（REQ-055）

**3000 或 8001 端口冲突** —— `start-ui.sh` 会在启动前终止占用这些端口的残留进程。如果端口被其他程序占用，请先手动停止它。[tool-verified: start-ui.sh lines 197–199] (REQ-619)

**全新启动** —— 停止脚本，然后运行 `./start-ui.sh --reset-volumes` 以清除所有卷并重新启动。[tool-verified: start-ui.sh line 19] (REQ-170)

---

## 后续步骤

| 目标 | 文档 |
| ------ | ----- |
| 完整的 YAML 配置参考 | [configuration.md](configuration.md) |
| 行级安全、列脱敏、身份验证 | [security.md](security.md) |
| 所有支持的数据源类型 | [sources.md](sources.md) |
| 实时订阅 | [subscriptions.md](subscriptions.md) |
| JDBC、BI 工具、Arrow Flight、Apollo Federation | [integrations.md](integrations.md) |
| Python 客户端 | [python-client.md](python-client.md) |
| 生产环境部署 | [deployment.md](deployment.md) |
