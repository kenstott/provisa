# 集成 {: #integrations }

## 选择连接方式 {: #choosing-a-connection-path }

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

## pgwire —— 原生 PostgreSQL 驱动 {: #pgwire-native-postgresql-driver }

Provisa 实现了 PostgreSQL 线路协议（协议版本 3.0）。任何能识别 PostgreSQL 协议的客户端都可以在无需自定义驱动的情况下连接。

在启动 Provisa 前设置 `PROVISA_PGWIRE_PORT`（例如 `5433`）即可启用此功能。未设置或为 `0` 时禁用。

### 为什么选择 pgwire 而非 JDBC？ {: #why-pgwire-instead-of-jdbc }

JDBC 驱动通过 Arrow Flight 传输查询，使用它需要部署 `provisa-jdbc.jar`。pgwire 不需要任何额外的东西。如果您已经有 `psql`、DBeaver、SQLAlchemy 或 PG JDBC 驱动，就可以直接用，所以只发送 SQL 时，这是更简单的方式。

对于内置 JDBC 连接向导、需要拉取大型结果集的 BI 工具，选择 JDBC，Arrow Flight 的列式流式传输在这时最划算。pgwire 接受针对整个已发布架构的同样的自由 SQL，需要配置的更少。

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. 新建连接 → PostgreSQL
2. 主机：`localhost`，端口：`5433`
3. 用户名 / 密码按 Provisa 配置
4. 无需下载额外驱动

### SQLAlchemy（Python） {: #sqlalchemy-python }

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

或使用 `asyncpg`：

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### 身份验证 {: #authentication }

启动包的 `password` 字段承载凭据，而凭据*是什么*决定了采用哪种方法：个人访问令牌、OIDC bearer 令牌，或是针对已配置提供程序的密码。在 `basic` 提供程序且 `auth.scram: true` 之下，密码通过 SCRAM-SHA-256 加以证明，而非发送。支持客户端证书。在信任模式（`none`）下，用户名直接映射到角色，密码被忽略。

完整的接口 × 方法对照表见[安全模型](security.md#surfaces-and-credentials)。不支持 MD5；在不受信任的网络上运行时，请启用 TLS（`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`）。

### 限制 {: #limitations }

- 仅支持 SQL。通过 pgwire 不接受 GraphQL 及 Cypher。
- 并非只读。已注册表的行通过 `INSERT`、`UPDATE`、`DELETE`、`MERGE`、`TRUNCATE` 和 `COPY ... FROM STDIN` 写入，每一种都由角色的 `write` 权利、其列授权和其行过滤放行；参见[数据写入](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin)。
- 不支持定义：`CREATE`、`ALTER` 和 `DROP` 会以 SQLSTATE 0A000 被拒绝，会话自己的临时表除外；参见[定义](pgwire.md#definitions-create-alter-drop)。`COPY ... TO STDOUT` 以 `text`、`csv` 和 `binary` 导出；`COPY ... FROM STDIN` 读取 `text` 和 `csv`。
- 针对 `information_schema` 及 `pg_catalog` 的查询会被拦截，并由 DuckDB 目录垫片响应，因此架构发现工具可正常工作。

---

## JDBC 驱动 {: #jdbc-driver }

Provisa 的 JDBC 驱动以 Arrow Flight 作为底层传输方式。对于具备 JDBC 连接向导的 BI 工具而言，这是推荐使用的方式。

当打开连接时 Flight 端口有应答，查询就经由 Arrow Flight 进行，结果连同列类型逐批到达。Flight 端口被视为 HTTP 端口旁边的那个端口（8001 旁的 8815）；不是这样时请设置 `flight_port` 属性。若连接时 Flight 端口不可达，连接会将 HTTP API 用于其查询，并在 INFO 级别记录这一点。当连接已经在使用 Flight 时，服务器错误——被拒绝的凭据、用户不持有的角色、失败的语句——会连同服务器的消息传给调用者；它不会再经由 HTTP 重试。

浏览 schema（`getTables`、`getColumns`、主键和外键）使用登录凭据经由同一个 Flight 端口读取：驱动列出它代为执行的角色的目录，因此未授予该角色的表或列不会出现。引用未授予该角色的表或列的键也不会列出。在连接时 Flight 端口不可达的连接上，驱动通过 HTTP（`GET /data/catalog`）读取同一目录：服务器用同一个函数构建两份清单，因此表、列名、类型和键与哪个端口应答无关。

### 连接 {: #connection }

下载 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（始终为最新版本），并将其添加到你工具的驱动路径中。

JDBC URL：

```yaml
jdbc:provisa://<host>:8815
```

身份验证使用标准 JDBC 的 `user` / `password` 属性。驱动用它们换取一个会话令牌，并随每个请求发送；被拒绝的登录会以服务器的状态和原因使连接失败。Provisa 从登录的身份推导角色。可选属性 `role`（或 URL 中的 `?role=`）请求以用户持有的一个角色执行，或以这类角色的逗号分隔集合执行；用户不持有的角色会被拒绝。在没有身份验证提供程序的部署中不存在登录，用户名被当作请求的角色。

### BI 工具设置 {: #bi-tool-setup }

**Tableau**

1. 管理 → 驱动程序 → 安装 Provisa JDBC
2. 连接 → 其他数据库（JDBC）
3. URL：`jdbc:provisa://localhost:8815`
4. 在系统提示时输入用户名及密码

**DBeaver**（JDBC 方式；pgwire 方式见上文）

1. 数据库 → 新建连接 → JDBC
2. 驱动：添加 `provisa-jdbc.jar`
3. URL：`jdbc:provisa://localhost:8815`
4. 在“身份验证”选项卡中输入用户名及密码

**Power BI** —— 使用 ODBC 网关配合 Provisa JDBC-ODBC 桥接器（已包含在安装程序中）。

---

## Arrow Flight 客户端 {: #arrow-flight-clients }

Arrow Flight（端口 8815）是支持此功能的数据工具推荐使用的方式。结果会以 Arrow RecordBatch 形式流式传输，无需在 Provisa 内存中具体化。

### Python（`provisa-client`） {: #python-provisa-client }

推荐使用的 Python 方式同时封装 GraphQL 及 Arrow Flight：

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

### Python（原生 PyArrow） {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight 在 JSON 负载中以 `token` 字段承载凭据，其中可以是提供程序 bearer 令牌或个人访问令牌。握手与每个票据都接受它，且两者的校验方式相同，因此在握手时已完成认证的客户端在每次 `do_get` 时仍需出示该令牌。与之并列的 `role` 字段是*请求*一个角色；服务器会推导该身份获准的角色集合并替换为已授权的取值，因此票据中的角色字符串绝不是身份本身。 参见[安全模型](security.md#surfaces-and-credentials)。

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

浏览同样需要凭据。`list_flights`、`get_flight_info` 和 `get_schema` 不携带 ticket，因此它们从调用的 `authorization: Bearer <token>` 标头读取凭据，从 `x-provisa-role`（一个角色，或已持有角色的逗号分隔集合）读取所请求的角色。启用身份验证时，没有凭据的浏览调用会被拒绝，它返回的目录是已授权角色的目录：授予该角色的表、列和命令，仅此而已。目录 ticket（不带 `query` 的 ticket）以同样方式，根据其自身的 `token` 和 `role` 应答。因此客户端必须在浏览之前发送凭据；`provisa-client` 在 `list_tables()` 中这样做。在没有身份验证提供程序的部署中，这些调用列出整个目录，且不接受凭据。

```python
options = flight.FlightCallOptions(headers=[
    (b"authorization", b"Bearer provisa_pat_..."),
    (b"x-provisa-role", b"analyst"),
])
for info in client.list_flights(b"", options):
    print(info.descriptor.path)
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

### Spark（PySpark） {: #spark-pyspark }

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC（端口 50051） {: #protobuf-grpc-port-50051 }

服务到服务方式。Provisa 按角色生成 `.proto`，并在模型或角色变化时重新生成，每个角色只能看到其有权访问的表和列。服务为角色架构根部的每张表提供一个 RPC；关联表与 GraphQL 中一样，通过其关系访问。

下载你角色的 proto：

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

使用 `grpc_server_reflection` 以编程方式发现架构。反射描述的是调用所充当角色的服务，与上面下载的文件相同。未启用认证时，反射调用与其他调用一样指明其角色：

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

每次 RPC 都必须在 `authorization` 元数据键中携带凭据，可以是提供程序令牌或个人访问令牌。`x-provisa-role` 是从该身份获准的集合中请求一个角色；它不是凭据，而且从来都不是。支持客户端证书。参见[安全模型](security.md#surfaces-and-credentials)。

流式查询会逐行发出消息；变更操作（mutation）则为一元操作。

---

## 跨协议调用命令 {: #invoking-commands-across-protocols }

**命令**是在 Provisa 模型注册的已跟踪函数或 webhook，即一个可调用元素，具有 `kind`（`query` 或 `mutation`）及描述其运行方式的 `impl_kind`。所有接口都通过单一受治理的执行器（`invoke_tracked_function`）路由调用，统一强制执行 命令的分配（`visible_to`）、变更所需的 `write` 权利 及治理。

| `impl_kind` | 执行内容 | 绑定字段 |
| ------------ | ----------- | --------------- |
| `source_procedure` | 已注册数据源上的存储过程（默认） | `sourceId`、`schemaName`、`functionName` |
| `script` | 服务器端脚本 | `script` |
| `http` | 出站 HTTP 调用 | `url`、`method` |
| `grpc` | 对外部服务器的出站 gRPC 调用 | `target`、`method` |
| `python` | 由 Provisa 托管的 Python 可调用对象 | `callable`（例如 `demo.py_functions:random_dataset`） |

当命令声明 `return_schema`（`type: array, items: object` 的 JSON Schema）时，即为集合返回型，每个接口都会将其投影为类型化的行集。演示命令 `random_python_set`（impl_kind 为 `python`）及 `random_grpc_set`（impl_kind 为 `grpc`）分别演示了一个托管的可调用对象及一个返回随机值行的外部 gRPC 桥接；两者均已在 `config/provisa-install.yaml` 中注册。

### 协议对照表 {: #protocol-matrix }

| 接口 | 语法 | 示例 |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → Query 字段；`kind=mutation` → Mutation 字段；`domain_prefix: true` 时加上域前缀 | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` 或 `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP（`POST /data/cypher`） | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt（Neo4j Browser / 驱动程序） | `CALL fn(args)` —— 位置参数会对应到已声明的参数名称 | `CALL random_python_set(3, 7)` |
| Provisa gRPC（端口 50051） | 一元 `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

`kind` 字段仅控制在 GraphQL 中的放置位置。SQL、Cypher、Bolt 及 gRPC 接口均同样接受 `query` 及 `mutation` 命令。

---

## Apollo Federation

Provisa 可作为 Federation v2 子图，将其已发布的架构暴露给 Apollo Router 或 Apollo Gateway。

### 设置 {: #setup }

在 `config.yaml` 中启用联邦：

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa 会自动在主键列上生成 `@key` 指令，并在跨子图关系上生成 `@external`/`@provides`。

### 向 Apollo Router 注册 {: #register-with-apollo-router }

在你的 `supergraph.yaml` 中：

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

运行 `rover supergraph compose --config supergraph.yaml` 以生成超级图架构。

### 实体 {: #entities }

Provisa 会响应 `_entities` 查询，以进行跨子图连接。任何具有主键的表都可自动作为 Federation 实体解析。

---

## Hasura v2 / DDN 导入 {: #hasura-v2-ddn-import }

有关从 Hasura 迁移到 Provisa，请参阅 [docs/import.md](import.md)。

---

## Kafka

有关将 Kafka 主题配置为只读表及查询结果接收端，请参阅 [docs/sources.md](sources.md#kafka-sources)。

---

## 数据质量检查器 {: #data-quality-checkers }

Soda Core 和 Great Expectations 像任何其他 postgres 客户端一样，通过 pgwire 连接 Provisa。这就是全部的集成方式：检查器只持有一个 postgres 驱动，并扫描联邦视图，因此 Snowflake 表、Iceberg 表和 Mongo 集合都由同一种合约方言检查，无需针对每个系统单独编写检查器。

扫描运行在一个子解释器中——`python -m provisa.dq.worker`——这是唯一导入 `soda_core` 或 `great_expectations` 的地方。两者都不会链接进服务器进程，检查器崩溃只会拖垮一个子进程，而不会拖垮事件循环。

扫描结果落地为普通的数据源行，因此节奏、新鲜度、事件、血缘、治理、RLS、表格和导出全部无需第二套机制即可适用。合约编写、结果信封和派生注册在 [docs/sources.md](sources.md#data-quality-checkers) 中有说明。

### 安装检查器 {: #installing-a-checker }

两个库默认都不随附。安装程序会询问你想要哪一个，答案会成为 `~/.provisa/config.yaml` 中的 `dq_checker: none|soda|gx`。在 Docker 层，`scripts/provisa` 会把它转换为 `PROVISA_EXTRAS` 构建参数；在原生层，`first-launch.sh` 会把对应的 pyproject extra 安装进 venv。

| `dq_checker` | 库 | 许可证 | 托管云平面 |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | 拒绝（`cloud_eligible: false`） |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | 允许 |

Elastic License 2.0 禁止将该软件以托管服务的形式提供给第三方，而在 SaaS 平面内代租户运行 Soda 恰恰就是这种情形。想要使用 Soda 的托管部署应指向运营方自行运行的 Soda 端点。连接密钥参见 [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations)。

---

## Apache Ossie 语义互操作 {: #apache-ossie-semantic-interchange }

Provisa 通过一个边界适配器与 Apache Ossie（规范 0.2.0.dev0，孵化中；前身为 Open Semantic Interchange）交换语义模型。Provisa 的内部词汇从不改名为 Ossie 的词汇。该规范声明可能发生破坏性变更，因此耦合被限制在适配器内。

草案规范曾在不改变版本字符串的情况下改变形态。适配器遵循
`apache/ossie` 提交 `642cb894`（2026-10-10）时的规范：每个文档一个模型，
文档根部包含 `version`、`name`、`datasets`、`relationships` 和 `metrics`，以及
枚举的 `datatype`。导出在单元测试中依据该提交的
`core-spec/ossie-schema.json` 进行校验。

### 导出 {: #export }

规范的导出界面是一个实时 HTTP 端点。它在每次请求时从实时状态推导出 Ossie 文档，没有缓存，也没有生成步骤。

```http
GET /admin/ossie
```

响应是一个 YAML 文档，带有 `Content-Disposition: attachment; filename=provisa.ossie.yaml`。

指标页面在 Ossie Interchange 面板上还提供一个**下载**按钮和可复制的端点 URL，
两者都指向同一个端点。

#### 导出的内容 {: #what-is-exported }

适配器将 Provisa 对象映射为 Ossie 对象如下：

| Provisa 对象 | Ossie 对象 | 说明 |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`；主键/唯一键来自列配置和 `UniqueConstraint` |
| `Column` | `field` | `expression` = 列引用（ANSI_SQL 方言）；时间列获得 `dimension.is_time: true` |
| `Relationship` | `relationship` | 设置了别名时用作名称；计算关系（以函数为目标）被跳过 |
| `Metric` | `metric` | `name`、`expression`（ANSI_SQL）、`datatype`（按列的方式映射）、`description`、`ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | 仅用于往返；其他工具可以忽略 |

跨多列的关系在 `from_columns` 和 `to_columns` 中列出每一列。
没有表的模型以 `422` 被拒绝：Ossie 文档至少有一个 dataset。

治理、行级安全、血缘和图语义不会导出。为了往返保真，它们可以放在可选的
`provisa` custom_extensions 槽位中传递，但互操作从不依赖其他工具是否读取它。

Ossie 词汇之外的列类型写作 `Opaque`，并在字段的 `provisa` 扩展中
写入其自身的类型名，这正是规范对其没有成员的已知类型所规定的做法。
没有记录类型的列不指定 `datatype`。适配器从不映射到列并不具有的类型。

#### 类型映射 {: #type-mapping }

| Provisa / 数据源类型 | Ossie `datatype` |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `String` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `Integer` |
| `numeric`, `decimal` | `Decimal` |
| `float`, `double`, `real` | `Float` |
| `bool`, `boolean` | `Boolean` |
| `date` | `Date` |
| `time` | `Time` |
| `timestamp`, `datetime` | `DateTime` |
| `timestamptz`, `timestamp with time zone` | `DateTimeTz` |
| 其他任何类型 | `Opaque`，类型名写在 `provisa` 扩展中 |

Ossie 的 `Decimal` 不携带精度和标度，因此 `numeric(10,2)` 导出为 `Decimal`，
从导入返回时为 `decimal`。

### 导入 {: #import }

导入接受一个 Ossie 文档（YAML 或 JSON）并返回注册建议。不会自动注册任何内容，导入的定义也从不绕过审查步骤。

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

服务器用 `parse_ossie_model` 解析文档，它校验结构并返回一个
`OssieImport` dataclass，其中建议的表、关系和指标以普通字典表示。
任何结构问题都是一个 `400`，其错误按路径指明，例如
`ossie import: missing $.datasets[1].source`。

导入器只读取文档的扁平形态。三种情况会被点名拒绝：

- 将其模型包裹在 `semantic_model` 中的文档，这是草案规范在
  2026 年 9 月之前的形态。错误会说明该文档早于扁平形态，以及如何转换。
- 不是 `0.2.0.dev0` 的 `version`。
- 不属于 Ossie 词汇成员的 `datatype`；错误会指明路径并列出
  成员。

建议的列或指标获得其 Ossie datatype 所代表的类型（`Integer` 建议
`bigint`，`DateTime` 建议 `timestamp`）。当文档来自 Provisa 时，`Opaque` 建议 `provisa` 扩展所指明的类型，
否则不建议类型。以对象形式给出的 `ai_context` 按其 `instructions` 读取。

仅当字段的表达式是单个数据源列时，它才被建议为列。表达式为计算的字段
不会被建议；审查屏幕会在其表下将它列为未导入。重命名其列的字段会被建议为该列，
并以字段名作为列的别名。dataset 的 `source` 必须由三部分组成，
即 `source.schema.table`；查询或更短的名称会被拒绝，错误会如此说明。

#### 来自 dbt、Cube、Snowflake 及其他语义层 {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

Apache Ossie 项目维护着 Ossie 与其他工具模型格式之间的转换器。运行
你所用工具的转换器，然后把它的输出发送到导入端点，或在下面的审查屏幕上
选择该文件。Provisa 读取 Ossie 文档；它不读取其他工具的文件。

| 来源 | 转换器命令 | 它读取什么 |
|---|---|---|
| dbt（语义层） | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | dbt 为 MetricFlow 写出的 `semantic_manifest.json` |
| Cube | `ossie-cube import -i model/ -o model.yaml` | Cube 模型目录，或单独的 cube 和 view 文件 |

同一仓库还有面向 Snowflake、Databricks、Microsoft、Salesforce、
ThoughtSpot、Sigma、Omni、Hex、GoodData 等的转换器；每个都有自己的 README。

转换之前值得了解的两项限制：

- dbt 转换器只读取语义层清单。没有语义
  模型的 dbt 项目没有可供它转换的内容。它会丢弃转化指标、私有指标和
  自然键实体，并为每一项打印警告。

- 转换器的输出遵循该转换器自身提交时的规范。Provisa 读取
  `apache/ossie` 提交 `642cb894` 的扁平形态；早于 2026 年 9 月的转换器
  会写出 `semantic_model` 包装，Provisa 会点名拒绝它。

两条路径都于 2026-10-10 运行，所用转换器安装自 `apache/ossie` 提交 `642cb894`
（它们不以软件包发布；从仓库的 `converters/dbt` 和
`converters/cube` 目录安装，需要 Python 3.11 或更高版本）。一个由
两张表、一个关系和一个指标组成的模型从 Provisa 导出，转换为 dbt 语义清单
和 Cube 模型，再用 `ossie-dbt msi-to-ossie` 和
`ossie-cube import` 转换回 Ossie，并由 `parse_ossie_model` 读取：

| 路径 | 结果 |
|---|---|
| Cube 模型 → Ossie → Provisa | 已读取。表、键、列类型、关系和指标都回来了；关系名称是转换器的名称（`orders_to_customers`）。 |
| dbt 语义清单 → Ossie → Provisa | 已读取。表、主键和指标回来了。列类型没有（转换器的输出不指定它们），而关系以转换器从 dbt 实体推断出的形式回来，并非已注册的那个。应用之前请检查关系。 |

当任何表具有跨多列的唯一约束时，dbt 转换器会拒绝该模型
（“MetricFlow entities cannot represent composite keys losslessly”）。

#### 审查屏幕 {: #the-review-screen }

在界面中，**导入**按钮（指标页面 → Ossie Interchange 面板）会打开文件选择器。文档提交并解析后，会打开一个审查对话框，其中每个建议的表、关系和指标都以已勾选的条目列出。建模者可以取消勾选任何内容以将其排除。 **应用**会通过现有的注册变更注册已勾选的条目：先注册表，再注册关系（它们引用表），然后注册指标。

保存在 Provisa 导出的 Ossie 文档中的建模角色和历史，在导入时能正确往返。

### dbt sources 文件 {: #dbt-sources-file }

对于不使用语义层的 dbt 项目，Provisa 直接写出 dbt 的 `sources.yml`。
它在每次请求时从实时模型派生。

```http
GET /admin/dbt/sources?role=<role id>
```

响应是 YAML，带有 `Content-Disposition: attachment; filename=provisa.sources.yml`。
调用者需要 `org_settings`，且 `role` 必须是其持有的角色；持有
`access_config` 者可以指定任何角色。指标页面上的互操作面板为当前使用的角色
提供相同的下载。

该文件在每个数据源和 schema 对应的 dbt source 下列出角色可获得的每张表（以
数据源 id 命名，当数据源的表分布在多个 schema 时为 `<source>_<schema>`），
附带表的描述、可获得列的描述，以及以下测试：

| 模型指定 | 文件写出 |
|---|---|
| 单列主键 | 该列上的 `unique` 和 `not_null` |
| 多列主键 | 每一列上的 `not_null` |
| 单列唯一约束 | 该列上的 `unique` |
| 两张均对该角色可获得的表之间的关系 | 源列上的 `relationships`，指向 `source('<source>', '<table>')` |

角色无法获得的列不出现，没有可获得列的表也不出现。跨
多列的键没有标准 dbt 测试；文件的开头注释会指明每个这样的键，且
不会为其写测试。模型不记录可空性，因此 `not_null` 只出现在键列上。
该文件不携带指标、行级安全、脱敏、角色或血缘，其开头注释
也如此说明。

由两张表和一个关系的模型生成的文件，已由 dbt-core 1.12.5 配合
dbt-duckdb 1.11.0 解析：两个 source、五个测试，无警告。dbt-core 1.9.11 无法读取
其 `relationships` 测试，这些测试把参数嵌套在 `arguments` 之下；中间版本
未经测试。

---

## 跨协议指标 {: #metrics-across-protocols }

受治理指标的定义 —— 其表达式、描述及 `ai_context` —— 会通过单一编译器扩展，随其值传递到每个
查询接口。这中间不存在任何副本。编译器为 SQL 访问保留 `metrics` 架构；每个协议随后再各自加入
其自身的元数据通道。

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
`pg_description` 条目呈现，因此 DBeaver 及 psql 的 `\d+` 均可显示。

`SELECT *` 会被拒绝，因此请明确指定所需的列名。

### GraphQL

指标会在根字段 `_aggregate` 内以 `metrics` 代码块的形式投影。

定义文本（`description`、`ai_context`）会出现在 GraphQL 内省文档中，因此具有架构感知能力的
工具及代码生成会自动获取该内容。

### MCP（AI 代理） {: #mcp-ai-agents }

有两个工具向 MCP 客户端公开指标：

- **`list_metrics`** —— 返回该会话可见的所有受治理指标，包含 `name`、`description` 及
  `ai_context`。
- **`query_metric`** —— 接受一个指标名称及一个维度列表，并调用编译器的语义 SQL 路径，返回
  聚合结果。

代理在构建查询之前调用 `list_metrics`，即可按名称选择受治理指标，而非手动编写聚合 SQL。
`ai_context` 字段正是放置引导正确选择之定义文本的位置。

### Arrow Flight {: #arrow-flight }

指标可通过返回 Arrow 表的指标飞行描述符（metric flight descriptor）进行寻址。

通过标准 Flight SQL 票据路径，使用相同的 `metrics.<name>` SQL 形式。

### Bolt / Cypher（Neo4j Browser） {: #bolt-cypher-neo4j-browser }

使用 `provisa.metric()` 过程调用指标：

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

事实表及维度表在联邦图中带有 `:Fact` 及 `:Dimension` 节点标签，因此 Bloom 可自动呈现星形
结构。

### 自然语言查询 {: #natural-language-queries }

自然语言架构匹配器会将自然语言问题中的指标词汇，直接解析为一个指标及若干维度，然后生成
语义 SQL。

事实表在自然语言提示中标记为 `[fact]`；维度表则标记为 `[dimension]`。匹配器在解析问题时，
会倾向于选择从事实表到维度表的连接路径。

### 流式处理 {: #streaming }

将 `view_metrics` 与 `materialize` 结合，即可利用现有的物化机制，在输入变化时让指标视图保持最新。
无需任何新管道。

### 可观测性（OTel） {: #observability-otel }

指标评估会被追踪，并可导出为 OpenTelemetry 指标。
