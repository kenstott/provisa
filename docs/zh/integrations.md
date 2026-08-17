# 集成

## 选择连接方式

| 客户端类型 | 推荐方式 | 原因 |
| ------------- | ----------------- | ----- |
| BI 工具（Tableau、Power BI、Looker） | JDBC | 通过线路进行 Arrow Flight 列式流式传输；BI 工具内置 JDBC 向导，并可受益于列式高吞吐量传输大型结果集 |
| psql、DBeaver，任何兼容 PG 的工具 | pgwire（原生 PG 驱动） | 零摩擦默认选项 —— 无需自定义驱动；直接使用已有工具 |
| Python 数据栈（pandas、pyarrow） | `provisa-client` 或原生 ADBC | 流式 Arrow 批次；无逐行序列化开销 |
| Spark、DuckDB、高吞吐量管道 | Arrow Flight（ADBC） | 无限制列式流式传输，直接进入 Arrow 内存 |
| 服务到服务（类型化契约） | Protobuf gRPC | 按角色生成的 proto；流式行数据；类型安全 |
| Web 应用、脚本 | HTTP（`/data/graphql`、`/data/sql`） | 无需驱动；标准 HTTP；查询语言可自由选择 |
| REST 客户端（JSON:API 标准） | `GET /data/jsonapi/{table}` | JSON:API v1.0 信封；通过查询参数提供稀疏字段集、分页、过滤；无需驱动 |

---

## pgwire —— 原生 PostgreSQL 驱动

Provisa 实现了 PostgreSQL 线路协议（协议版本 3.0）。任何能识别 PostgreSQL 协议的客户端都可以在无需自定义驱动的情况下连接。

在启动 Provisa 前设置 `PROVISA_PGWIRE_PORT`（例如 `5433`）即可启用此功能。未设置或为 `0` 时禁用。

### 为什么选择 pgwire 而非 JDBC？

JDBC 驱动使用 Arrow Flight 作为传输方式，并需要部署 `provisa-jdbc.jar`。pgwire 则无需任何额外部署 —— 如果你已经拥有 `psql`、DBeaver、SQLAlchemy 或 PG JDBC 驱动，即可直接使用。对于纯 SQL 工作负载而言，这是摩擦最少的方式。

对于内置 JDBC 连接向导、并可受益于 Arrow Flight 列式流式传输处理大型结果集的 BI 工具而言，JDBC 是正确的选择。pgwire 则可对完整发布的架构执行自由 SQL —— 相同的查询，配置成本更低。

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. 新建连接 → PostgreSQL
2. 主机：`localhost`，端口：`5433`
3. 用户名 / 密码按 Provisa 配置
4. 无需下载额外驱动

### SQLAlchemy（Python）

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

或使用 `asyncpg`：

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### 身份验证

启动包的 `password` 字段承载凭据，而凭据*是什么*决定了采用哪种方法：个人访问令牌、OIDC bearer 令牌，或是针对已配置提供程序的密码。在 `basic` 提供程序且 `auth.scram: true` 之下，密码通过 SCRAM-SHA-256 加以证明，而非发送。支持客户端证书。在信任模式（`none`）下，用户名直接映射到角色，密码被忽略。

完整的接口 × 方法对照表见[安全模型](security.md#_16)。不支持 MD5；在不受信任的网络上运行时，请启用 TLS（`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`）。

### 限制

- 仅支持 SQL。通过 pgwire 不接受 GraphQL 及 Cypher。
- 并非只读。`COPY ... FROM STDIN` 会将数据行插入 `postgresql`、`mysql`、`sqlite` 及 `mariadb` 数据源，并支持 DDL（见下文）。
- 支持 DDL（`CREATE`、`ALTER`、`DROP`），并会分派至 Trino 或直接路径；新表会注册到编译上下文中，并可立即查询。`COPY ... TO STDOUT`（导出）及 `COPY ... FROM STDIN`（导入）均支持 `text` 及 `csv` 格式。
- 针对 `information_schema` 及 `pg_catalog` 的查询会被拦截，并由 DuckDB 目录垫片响应 —— 架构发现工具可正常工作。

---

## JDBC 驱动

Provisa 的 JDBC 驱动以 Arrow Flight 作为底层传输方式。对于具备 JDBC 连接向导的 BI 工具而言，这是推荐使用的方式。

### 连接

下载 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（始终为最新版本），并将其添加到你工具的驱动路径中。

JDBC URL：

```yaml
jdbc:provisa://<host>:8815
```

身份验证使用标准 JDBC 的 `user` / `password` 属性。Provisa 会依据已配置的身份验证提供程序验证凭据，并分配角色 —— 客户端不能自行选择角色。

### BI 工具设置

**Tableau**

1. 管理 → 驱动程序 → 安装 Provisa JDBC
2. 连接 → 其他数据库（JDBC）
3. URL：`jdbc:provisa://localhost:8815`
4. 在系统提示时输入用户名及密码

**DBeaver**（JDBC 方式 —— pgwire 方式见上文）

1. 数据库 → 新建连接 → JDBC
2. 驱动：添加 `provisa-jdbc.jar`
3. URL：`jdbc:provisa://localhost:8815`
4. 在“身份验证”选项卡中输入用户名及密码

**Power BI** —— 使用 ODBC 网关配合 Provisa JDBC-ODBC 桥接器（已包含在安装程序中）。

---

## Arrow Flight 客户端

Arrow Flight（端口 8815）是支持此功能的数据工具推荐使用的方式。结果会以 Arrow RecordBatch 形式流式传输，无需在 Provisa 内存中具体化。

### Python（`provisa-client`）

推荐使用的 Python 方式 —— 同时封装 GraphQL 及 Arrow Flight：

```bash
pip install provisa-client
```

```python
from provisa_client import ProvisaClient

client = ProvisaClient("http://localhost:8001", username="alice", password="secret")

# Arrow Flight → pyarrow Table (high-throughput, streaming)
table = client.flight("SELECT id, amount FROM sales.orders")

# Arrow Flight → pandas DataFrame
df = client.flight_df("SELECT id, amount FROM sales.orders")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount } }")
```

完整参考资料（包括 DB-API 2.0、SQLAlchemy 方言及 ADBC）请参阅 [docs/python-client.md](python-client.md)。

### Python（原生 PyArrow）

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight 在 JSON 负载中以 `token` 字段承载凭据——可以是提供程序 bearer 令牌或个人访问令牌。握手与每个票据都接受它，且两者的校验方式相同，因此在握手时已完成认证的客户端在每次 `do_get` 时仍需出示该令牌。与之并列的 `role` 字段是*请求*一个角色；服务器会推导该身份获准的角色集合并替换为已授权的取值，因此票据中的角色字符串绝不是身份本身。(REQ-1263) 参见[安全模型](security.md#_16)。

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

### ADBC

```python
import adbc_driver_flightsql.dbapi as adbc

conn = adbc.connect("grpc://localhost:8815", db_kwargs={"username": "alice", "password": "secret"})
cursor = conn.cursor()
cursor.execute("SELECT id, amount FROM sales.orders")
table = cursor.fetch_arrow_table()
```

### DuckDB

```python
import duckdb, pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT * FROM sales.orders"}')
arrow_table = client.do_get(ticket).read_all()

conn = duckdb.connect()
result = conn.execute("SELECT region, sum(amount) FROM arrow_table GROUP BY 1").df()
```

### Spark（PySpark）

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC（端口 50051）

服务到服务方式。Provisa 在启动时按角色生成 `.proto` —— 每个角色只能看到其有权访问的表和列。

下载你角色的 proto：

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

使用 `grpc_server_reflection` 以编程方式发现架构。

每次 RPC 都必须在 `authorization` 元数据键中携带凭据——提供程序令牌或个人访问令牌。`x-provisa-role` 是从该身份获准的集合中请求一个角色；它不是凭据，而且从来都不是。支持客户端证书。参见[安全模型](security.md#_16)。

流式查询会逐行发出消息；变更操作（mutation）则为一元操作。

---

## 跨协议调用命令

**命令**是在 Provisa 语义层注册的已跟踪函数或 webhook —— 一个可调用元素，具有 `kind`（`query` 或 `mutation`）及描述其运行方式的 `impl_kind`。所有接口都通过单一受治理的执行器（`invoke_tracked_function`）路由调用，统一强制执行 `writable_by` 及治理（REQ-1156）。[tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | 执行内容 | 绑定字段 |
| ------------ | ----------- | --------------- |
| `source_procedure` | 已注册数据源上的存储过程（默认） | `sourceId`、`schemaName`、`functionName` |
| `script` | 服务器端脚本 | `script` |
| `http` | 出站 HTTP 调用 | `url`、`method` |
| `grpc` | 对外部服务器的出站 gRPC 调用 | `target`、`method` |
| `python` | 由 Provisa 托管的 Python 可调用对象（REQ-885） | `callable`（例如 `demo.py_functions:random_dataset`） |

当命令声明 `return_schema`（`type: array, items: object` 的 JSON Schema）时，即为集合返回型 —— 每个接口都会将其投影为类型化的行集。演示命令 `random_python_set`（impl_kind 为 `python`）及 `random_grpc_set`（impl_kind 为 `grpc`）分别演示了一个托管的可调用对象及一个返回随机值行的外部 gRPC 桥接；两者均已在 `config/provisa-install.yaml` 中注册。[tool-verified: `config/provisa-install.yaml:809-856`]

### 协议对照表

| 接口 | 语法 | 示例 |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → Query 字段；`kind=mutation` → Mutation 字段；`domain_prefix: true` 时加上域前缀 | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` 或 `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP（`POST /data/cypher`） | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt（Neo4j Browser / 驱动程序） | `CALL fn(args)` —— 位置参数会对应到已声明的参数名称 | `CALL random_python_set(3, 7)` |
| Provisa gRPC（端口 50051） | 一元 `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

`kind` 字段仅控制在 GraphQL 中的放置位置 —— SQL、Cypher、Bolt 及 gRPC 接口均同样接受 `query` 及 `mutation` 命令。

---

## Apollo Federation

Provisa 可作为 Federation v2 子图，将其已发布的架构暴露给 Apollo Router 或 Apollo Gateway。

### 设置

在 `config.yaml` 中启用联邦：

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa 会自动在主键列上生成 `@key` 指令，并在跨子图关系上生成 `@external`/`@provides`。

### 向 Apollo Router 注册

在你的 `supergraph.yaml` 中：

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

运行 `rover supergraph compose --config supergraph.yaml` 以生成超级图架构。

### 实体

Provisa 会响应 `_entities` 查询，以进行跨子图连接。任何具有主键的表都可自动作为 Federation 实体解析。

---

## Hasura v2 / DDN 导入

有关从 Hasura 迁移到 Provisa，请参阅 [docs/import.md](import.md)。

---

## Kafka

有关将 Kafka 主题配置为只读表及查询结果接收端，请参阅 [docs/sources.md](sources.md#kafka)。

---

## 数据质量检查器（REQ-1443）

Soda Core 和 Great Expectations 连接 Provisa 的方式与任何其他 postgres 客户端相同——通过 pgwire。这就是全部的集成方式：检查器只持有一个 postgres 驱动，并扫描联邦视图，因此 Snowflake 表、Iceberg 表和 Mongo 集合都由同一种合约方言检查，无需针对每个系统单独编写检查器。[tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

扫描运行在一个子解释器中——`python -m provisa.dq.worker`——这是唯一导入 `soda_core` 或 `great_expectations` 的地方。两者都不会链接进服务器进程，检查器崩溃只会拖垮一个子进程，而不会拖垮事件循环。[tool-verified: `provisa/dq/runner.py` `build_command`]

扫描结果落地为普通的数据源行，因此节奏、新鲜度、事件、血缘、治理、RLS、表格和导出全部无需第二套机制即可适用。合约编写、结果信封和派生注册在 [docs/sources.md](sources.md#req-1443) 中有说明。

### 安装检查器

两个库默认都不随附。安装程序会询问你想要哪一个，答案会成为 `~/.provisa/config.yaml` 中的 `dq_checker: none|soda|gx`。在 Docker 层，`scripts/provisa` 会把它转换为 `PROVISA_EXTRAS` 构建参数；在原生层，`first-launch.sh` 会把对应的 pyproject extra 安装进 venv。[tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh` `_native_extras`]

| `dq_checker` | 库 | 许可证 | 托管云平面 |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | 拒绝（`cloud_eligible: false`） |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | 允许 |

Elastic License 2.0 禁止将该软件以托管服务的形式提供给第三方，而在 SaaS 平面内代租户运行 Soda 恰恰就是这种情形。想要使用 Soda 的托管部署应指向运营方自行运行的 Soda 端点。连接密钥参见 [docs/configuration.md](configuration.md#soda-great_expectations)。

---

## Apache Ossie 语义互操作（REQ-1316）

Provisa 通过边界适配器，与 Apache Ossie（规范 0.2.0.dev0，孵化中；前身为 Open Semantic
Interchange）交换语义模型。Provisa 的内部词汇永远不会重命名为 Ossie 的词汇 —— 由于该规范声明
极有可能出现不兼容变更，因此耦合仅限于适配器内部。
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### 导出

规范的导出接口是一个实时 HTTP 端点。它会在每次请求时，从实时状态派生 Ossie 文档 —— 没有
缓存，也没有生成步骤。

```http
GET /admin/ossie
```

响应是一个 YAML 文档，带有 `Content-Disposition: attachment; filename=provisa.ossie.yaml`。
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

Metrics 页面还在 Ossie Interchange 面板中提供了**下载**按钮及可复制的端点 URL，两者均指向同一
端点。
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### 导出内容

适配器按以下方式将 Provisa 对象映射为 Ossie 对象：

| Provisa 对象 | Ossie 对象 | 说明 |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`；主键／唯一键来自列配置及 `UniqueConstraint` |
| `Column` | `field` | `expression` = 列引用（ANSI_SQL 方言）；时间列会获得 `dimension.is_time: true` |
| `Relationship` | `relationship` | 已设置别名时使用别名作为名称；已计算（函数目标）的关系会被跳过 |
| `Metric` | `metric` | `name`、`expression`（ANSI_SQL）、`datatype`、`description`、`ai_context` —— 按设计无损 |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | 仅用于往返；其他工具可忽略 |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

治理、行级安全、血缘及图语义均不会被导出。它们可以在 custom_extensions 的可选 `provisa` 插槽
中流转以维持往返保真度，但互操作过程从不依赖其他工具读取该数据。
[tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

未知的 Provisa 列类型会原样通过；适配器绝不会静默映射为错误的类型。[tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### 类型映射

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Provisa／数据源类型 | Ossie `datatype` |
| --- | --- |
| `varchar`、`text`、`char`、`uuid`、`string` | `string` |
| `int`、`integer`、`bigint`、`smallint`、`int4`、`int8`、`tinyint` | `integer` |
| `numeric`、`decimal`、`float`、`double`、`real` | `number` |
| `bool`、`boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`、`timestamptz`、`datetime` | `timestamp` |
| 其他任何类型 | 原样通过 |

### 导入

导入接受一个 Ossie 文档（YAML 或 JSON），并返回注册提案。系统不会自动注册任何内容 —— 已导入
的定义永远不会绕过审核步骤。

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

服务器使用 `parse_ossie_model` 解析文档，该函数会验证结构，并返回包含建议表、关系及指标（以
纯字典形式呈现）的 `OssieImport` 数据类。任何结构性问题都会返回带有命名路径错误的 `400`
响应，例如 `ossie import: missing semantic_model[0].datasets[1].source`。
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### 审核界面

在用户界面中，**导入**按钮（Metrics 页面 → Ossie Interchange 面板）会打开文件选择器。文档
提交并解析后，会打开一个审核弹窗，列出每个建议的表、关系及指标，并以已勾选项呈现。建模人员
可取消勾选任何项以将其排除。点击**应用**后，已勾选的项目会通过现有的注册变更操作
（mutation）完成注册 —— 先注册表，再注册关系（因其引用表），最后注册指标。
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

存储在由 Provisa 导出的 Ossie 文档中的建模角色及历史记录，会通过导入正确地往返还原。
[tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## 跨协议指标（REQ-1319）

受治理指标的定义 —— 其表达式、描述及 `ai_context` —— 会通过单一编译器扩展，随其值传递到每个
查询接口。这中间不存在任何副本。编译器为 SQL 访问保留 `metrics` 架构；每个协议随后再各自加入
其自身的元数据通道。

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

将任何指标作为 `metrics` 架构中的虚拟关系进行寻址。你选择的维度列会成为 GROUP BY：

```sql
-- Grand total
SELECT value FROM metrics.net_revenue;

-- By region
SELECT region, value FROM metrics.net_revenue GROUP BY region;

-- By region and month, filtered
SELECT region, month, value
FROM metrics.net_revenue
WHERE net_revenue.status = 'completed'
GROUP BY region, month;
```

编译器会在治理执行之前，将 `metrics.<name>` 形式展开为实际的分组聚合。列描述会以
`pg_description` 条目呈现，因此 DBeaver 及 psql 的 `\d+` 均可显示。[tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` 会被拒绝 —— 请明确指定列名。
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

指标会在根字段 `_aggregate` 内以 `metrics` 代码块的形式投影。
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

定义文本（`description`、`ai_context`）会出现在 GraphQL 内省文档中，因此具有架构感知能力的
工具及代码生成会自动获取该内容。
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP（AI 代理）

有两个工具向 MCP 客户端公开指标：

- **`list_metrics`** —— 返回该会话可见的所有受治理指标，包含 `name`、`description` 及
  `ai_context`。
- **`query_metric`** —— 接受一个指标名称及一个维度列表，并调用编译器的语义 SQL 路径，返回
  聚合结果。

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

代理在构建查询之前调用 `list_metrics`，即可按名称选择受治理指标，而非手动编写聚合 SQL。
`ai_context` 字段正是放置引导正确选择之定义文本的位置。

### Arrow Flight

指标可通过返回 Arrow 表的指标飞行描述符（metric flight descriptor）进行寻址。
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

通过标准 Flight SQL 票据路径，使用相同的 `metrics.<name>` SQL 形式。

### Bolt / Cypher（Neo4j Browser）

使用 `provisa.metric()` 过程调用指标：

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

事实表及维度表在联邦图中带有 `:Fact` 及 `:Dimension` 节点标签，因此 Bloom 可自动呈现星形
结构。
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### 自然语言查询

自然语言架构匹配器会将自然语言问题中的指标词汇，直接解析为一个指标及若干维度，然后生成
语义 SQL。[tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

事实表在自然语言提示中标记为 `[fact]`；维度表则标记为 `[dimension]`。匹配器在解析问题时，
会倾向于选择从事实表到维度表的连接路径。
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### 流式处理

将 `view_metrics` 与 `materialize` 及 Kafka 接收端结合，即可利用现有的物化机制，生成变更即
推送（push-on-change）的指标输出。无需任何新管道。
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### 可观测性（OTel）

指标评估会被追踪，并可导出为 OpenTelemetry 指标。
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
