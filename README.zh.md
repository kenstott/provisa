# Provisa

**连接您的数据库。5 分钟内即可用 GraphQL、gRPC、SQL 或 MCP 查询它们，通过任意 API 或协议。**

Provisa 在您的全部数据之上提供一个 API。把它指向您的数据库和业务应用，描述它们如何相互关联，然后通过 REST、GraphQL、SQL、gRPC、MCP、JSON:API、Arrow Flight、pgwire 或 Bolt 查询连接后的结果。

这之所以可行，是因为一切都来自同一个模型：您的域、表之间的关系，以及作用在它们之上的策略，覆盖所有数据源。Provisa 依据这个模型运行，也依据它实施治理，所以没有任何需要同步的东西。注册过的关系是查询唯一可以进行的连接，访问策略会编译进每一个查询计划。由这一个模型带来四件事：

- **联邦。** 73 个连接器可以连到 85 种以上的数据源，其中包括 Salesforce、ServiceNow 和 Stripe。支撑您业务运行的 SaaS 系统会作为表出现在您的数据库旁边，就地读取，不需要先建管道把数据送进数仓。用 SQL、GraphQL 或 Cypher 查询它们，可经由九种协议。
- **指标。** 指标是有名称、受治理的聚合。只需定义一次，任何工具、任何语言来问，得到的都是同一个定义。
- **本体。** 业务术语表是一张由概念、概念定义以及概念之间关系构成的图。Provisa 把它映射到保存这些值的表和列上，因此能够回答关于您数据的问题。一个概念在直接或经由其他概念连到数据之前，始终是草稿。术语表说明一个概念是什么；指标说明一个数字怎么算。
- **治理。** 六层检查会编译进每个查询计划，覆盖每种语言、每种协议；每个请求都以同样的方式记录：谁查询了什么、以什么角色、依据什么策略。追踪、指标和日志也作为可查询的表，注册在您的业务数据旁边。

您可以用 **GraphQL、Cypher 或 SQL** 提问，通过 **pgwire、Bolt、gRPC、REST、Arrow Flight 或 JDBC** 连接。每种查询语言都会先降为同一种中间表示，治理就在那里施加，只施加一次，因此策略不会在不同语言之间出现偏差。输出时，这种形式会被翻译成各数据源自己的方言。新增一种查询语言，就是在同一个内核上新增一个前端。

读和写都经过它。跨数据源的查询通过联邦层分发。写入，以及只涉及单一数据源的读取，会直接交给该数据源的驱动，治理方式相同，通常在 100 毫秒内返回。内置 Arrow Flight 列式流传输。

模型的词汇很少：域、关系、角色和策略。这让它易读、易审计，读一遍策略集，就知道它做什么。Provisa 本身是一个轻量的查询编译器。它从不作为运行时挡在您的数据路径中间。它把请求转成原生查询，路由出去，然后让开，所以整套数据始终保持高速。

它有两种用法，可以并用：

- **用于现代化改造。** 为您的数据建模，让 Provisa 为每个数据源生成原生 SQL，再把这些 SQL 拿到目标系统中采用。Provisa 是过渡期的桥梁，之后可以不再使用。
- **用于长期治理。** 把它保留为每个查询都要经过的路径，让定义、执行和审计在数据存在的整个期间始终在一起。

## 试用演示 {: #try-a-demo }

最快的方式是在浏览器中打开在线演示，无需安装任何东西：[provisa.dev/demos](https://provisa.dev/demos)。每个演示都会打开一个带导览的私有沙箱。

在本仓库的检出副本中，你可以在自己的机器上运行任何演示：

```bash
./start-ui-install.sh --list-demos     # the demos you can run, and which are published
./start-ui-install.sh --demo weblogs   # start one, with its tour
```

已发布的演示以现成的包拉取；其他演示从 `demo/named/<name>/` 运行。所有选项见 [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md)。

## 联邦模型 {: #the-federation-model }

整个设计靠四个想法支撑。每个数据源都归结为表。每个查询都归结为一种类似 SQL 的形式。可达性决定哪些内容实时查询、哪些先复制。新鲜度策略则让每份副本和每个派生数据集保持在需要的新鲜程度。

先说数据源。每个数据源都表示为一组建立在同一套共享类型系统上的二维表，这是数据源加入整套数据所要满足的唯一契约。MySQL 或 PostgreSQL 的表本来就是这样的表。GraphQL 查询结果展平之后也是。SPARQL 三元组存储和 Neo4j 看起来完全不像表，但您提供一个结果为表格的查询，这个查询就成了适配器。无论是什么数据源，系统看到的都是行、列和类型。接入一种新的数据源，就是满足这一个契约，有时需要一点人工帮助。没有人需要另写一套定制集成。

查询也同样处理。SQL 无论有多少方言，都是在表上做分析的语言，所以类似 SQL 的形式是天然的共同目标。因此，每个请求无论用什么语言到来，都会先降为这种形式。SQL 和 GraphQL 降起来很容易。Cypher 的路径和图语义需要真正下功夫，但也能做到。由于一切在其他处理之前都先经过同一种形式，无论请求用什么语言，治理都只在一个地方施加。

有了表格化的数据源和单一的查询形式，联邦就能同时覆盖实时查询和数据仓库：既包括 Trino 这样的实时查询引擎能做的，也包括这类引擎所依赖的物化。**可达性**把两者连在一起。引擎能就地查询某个数据源吗，还是必须先把它的数据复制到可查询的地方？答案把整套数据分成实时读取的部分和复制的部分。

大多数数据库已经有某种实时链接：DuckDB 的 `ATTACH`、PostgreSQL 的 `postgres_fdw`、Databricks 的外部链接。所以大多数数据库在一定程度上都能充当联邦引擎。但没有一个能覆盖一切。每个都只能连到特定的一组数据源，其余的就复制，而且没有一个会清楚说明哪些属于哪一类。模型把可达性明确下来，作为每个数据源一组确定的方法，说明引擎能实时连到什么，以及反过来需要复制什么。

剩下的是新鲜度。对于引擎连不到的每个数据源，它的副本需要多新？实际上答案只有几种：按需、按计划、按变更信号（CDC、水位线、快照），或固定不变。为每个数据源选一种，就是全部的新鲜度策略。

派生数据集也套用同一种形式：派生表、聚合，以及转换的输出。它们用同一种形式编写，所以血缘自然就有了。从每个源系统到最终输出的路径，就是生成它的那个定义，从头到尾都能读懂。构建它们时，新鲜度问题会再往前走一步：数据集是按计划刷新、只在前置条件满足时刷新、持续刷新，还是作为固定的历史快照？可选项和数据源副本是同一小组，所以派生数据集的构建策略用的是同一套词汇。

维度模型可以直接由此得出。星型模型中的事实和维度，和其他任何派生数据集一样。维度是一致化、去重后的投影，事实表是归约到某个粒度的连接加聚合，各自都有自己的构建和新鲜度策略。缓慢变化维度不需要任何特殊机制：固定快照就是第 2 类历史，按计划重建就是第 1 类。由于模式定义在模型中，并不绑定在某个数据仓库的表上，同样的事实和维度可以在 Oracle 或 Databricks 中构建，也可以在 MPP 引擎上保持为虚拟的，无需重新建模。

Data Vault 也以同样的方式适配，只是早一层。中心表（hub）是去重后的业务键数据集，链接（link）是它们之间注册的关系，卫星（satellite）是只插入、带时间戳、保存历史的属性数据集。卫星就是采用变更信号策略的派生数据集：加载日期加 hashdiff，就是作用在描述性属性上的变更捕获，而只插入的历史就是固定快照策略。时间点表和桥接表则是为查询速度而建的更多派生数据集。因此，原始保险库是模型中的一组派生数据集，星型模型是从它投影出来的，两者都是生成的，都能在引擎之间迁移。方法论仍然由您决定：什么成为中心表、卫星的粒度、怎么拆分。一旦决定，它就存在于模型中。没有任何 ETL 把它焊死在某个数据仓库上。

这两种模式都用**两个快捷声明**来声明，而不用手写视图。它们是所有星型模型和所有 Data Vault 的构件，不会把您绑定在其中任何一种方法上：

- **`entity`** 是数据源上带键、去重、可以保留历史的投影。声明键、属性和历史模式，Provisa 就会构建一个物化视图。如果要求保留历史，它会改为构建一个**双时态 MV**（`scd2` → delta，`snapshot` → snapshot）。一个构造就能得到 Kimball 的**维度**（SCD1 或 SCD2）或 Data Vault 的**中心表加卫星**。
- **`fact`** 连接到实体键，归约到声明的粒度，并聚合度量。Provisa 构建一个聚合 MV，以及到各实体的注册关系。一个构造就能得到星型的**事实表**或 Data Vault 的**链接**；没有度量的事实就是纯键集合链接。

每个 `entity` 或 `fact` 都会准确地变成建模者原本要手写的物化视图、双时态设置和关系，所以数据仓库始终留在模型中，可以在引擎之间迁移而无需重新建模。可以在管理界面（**Model** 表单）或通过管理 API（`registerEntity` / `registerFact`）声明。模型会为您生成 Kimball 星型或 Data Vault，而不会强加其中任何一种。

### 时间旅行 {: #time-travel }

时间旅行的意思是保留一行的每个版本而不是覆盖它，这样您就能问数据在过去任意时刻是什么样子。不同引擎做这件事的效率差别很大，所以 Provisa 把它作为物化视图**定义**的一部分，与存储引擎无关。声明一次，就能在任何能存放物化视图的存储上使用。

让它可以迁移的，是它**只追加**。一个版本一旦写入，就不会再更新或删除。常见的双时态技巧是回写一个“有效至”日期来关闭一行，但这需要 UPDATE，而很多引擎在联邦存储上做不到廉价的更新，甚至根本做不到。所以每次刷新都改为**追加**，某一时刻生效的是哪个版本，在读取时从日志中推算出来。追加有两种方式：

- **Snapshot** 追加整个最新的数据集，并打上本次刷新的系统时间戳。不需要比较差异，在每个引擎上都正确，但每次刷新存储都会增加一整份副本。
- **Delta** 只追加发生变化的内容，再加上已删除键的墓碑。差异由引擎自己用 `INSERT … SELECT` 中的反连接算出来。Provisa 从不逐行合并。它更小，但需要实体键。

Provisa 用这种方式管理系统时间，也就是它记录一个版本的时间。有效时间，也就是一个事实在业务上成立的时间，来自视图自身的 SELECT，并被保留下来。能做得更好的引擎，例如原生 Iceberg 快照或改动更少行的 MERGE，可以放在同一个声明背后使用；只追加的路径是在任何地方都正确的底线。

读取不需要任何特殊语法。对双时态 MV 的普通查询会返回由日志重建的**当前**状态。要回到过去，发送 `X-Provisa-As-Of: <timestamp>` 请求头，整个查询就会按当时的数据状态作答，在每种存储上都一样。再加上 `X-Provisa-As-Recorded`，就能按 Provisa 在某一时刻所记录的样子读取数据，这样之后记录的更正就不会改变对过去的重放。可以在管理界面（**Time Travel** 控件：关闭、snapshot 或 delta，再加一个实体键）或通过管理 API 为任意物化视图开启它。

可达性和新鲜度合在一起，构成一个通用的联邦模型：定义哪些是实时的、哪些是复制的、每份副本保持多新，而不依赖于任何一个引擎能连到什么。模型是可迁移的，所以您的数据不会被绑定在今天恰好能连到最多数据源的那家供应商身上。

## 功能特性 {: #features }

### 查询接口 {: #query-interfaces }

这些是你用来编写查询的语言和结构化 API。每一种都有自己的语法和语义；治理（RLS、脱敏、列可见性、关系强制执行）在所有这些接口上统一应用，与承载它们的线协议无关。

- **GraphQL** —— 按角色区分的 schema，具备字段级可见性、过滤、基于游标的分页和聚合查询（`count`、`sum`、`avg`、`min`、`max`）。schema 只提供已注册的关系，所以每个查询在构造上就结构有效，GraphQL 也是写出正确简单查询的最快路径。内置 Apollo APQ：查询在服务端被哈希并注册，后续调用只需通过 HTTP GET 发送哈希值，响应无需改动客户端即可被 CDN 缓存。行数低于可配置阈值的查找表以枚举类型呈现。
- **SQL** —— 完整的联邦数据 SQL，不受约束，比 GraphQL 更具表达力。编写标准 SQL，包括相关子查询，它都能原样跨数据源运行。单源查询会完全跳过联邦层（低于 100ms）。
- **Cypher** —— 基于同一联邦 schema 的图查询语言。将关系作为图的边进行遍历；联合数据源；支持变长路径。治理与 GraphQL 和 SQL 完全相同。
- **gRPC 模型 API** —— 从已注册的 schema 生成 `.proto`，在角色 schema 的根部为每张表提供带类型的查询和插入 RPC，并以流式返回结果。它和 GraphQL 一样由 schema 驱动：注册模型就是契约，protobuf 只是线上编码。它是完整的逐表查询接口，而 Arrow Flight 是列式流式传输。
- **JSON:API** —— 位于 `/data/jsonapi/{table}` 的结构化查询 API，设计上仅支持 HTTP。支持 JSON:API 1.1：稀疏字段集（`fields[table]=col1,col2`）、过滤表达式（`filter[field][op]=value`）、复合文档（`include=relation`）以及排序。它一次查询一张表，使用标准化的过滤语法，所以需要通用查询语言时请使用 SQL 或 GraphQL。
- **查询语言浏览器** —— 编写一个 GraphQL 查询，在侧边面板中实时查看对应的**语义化 SQL** 和 **Cypher** 翻译；可以复制其中任意一个，或直接跳转到 SQL 或图编辑器。一种实用的工作流是先在 GraphQL 中勾勒查询片段，再把生成的 SQL 拼接进复杂的视图或报表中。

该浏览器将一个 GraphQL 查询与其实时的 SQL 和 Cypher 翻译并排展示：

![Query Language Explorer](docs/images/query-explorer.png)

同一个联邦模式也以实时图的形式呈现，包含域和节点标签、关系类型以及可变长度遍历：

![Graph Visualization](docs/images/graph-view.png)

### 查询组合工具 {: #query-composition-tools }

这些工具帮助您用上述语言编写查询。它们本身不是查询语言。

- **自然语言查询** —— 运行在 Claude 上的 NL→SQL/Cypher/GraphQL 流水线。用日常语言描述您想要的内容，流水线会用您选择的语言写出查询，并在执行前提供交互式校验循环。

![Natural Language Query](docs/images/natural-language.png)

### 线协议 {: #wire-protocols }

这些是连接协议。SQL、GraphQL 和 Cypher 都运行在它们之上，选用哪种协议既不改变查询接口，也不改变治理行为。

- **pgwire** —— 任何 PostgreSQL 客户端（psql、DBeaver、DataGrip、asyncpg、SQLAlchemy、pandas `read_sql`）都可以连接到 5439 端口，就好像它是一个 Postgres 服务器一样。仅接受 SQL。完整的治理流水线适用。`pg_catalog` 和 `information_schema` 由内存中的目录回答，因此 schema 浏览器无需一次联邦往返即可工作。TLS 可选。
- **Bolt（Neo4j）** —— 任何 Neo4j 客户端（Neo4j Browser、Bloom、官方驱动）都可以通过 Bolt 协议连接，并对联邦图运行 Cypher。用户所持有的每个角色都会呈现为一个 `provisa_<role>` 数据库。治理方式与其他每种传输方式相同。TLS 可选。
- **Arrow Flight** —— 基于 gRPC 的高吞吐列式流式传输；接受 GraphQL 或 SQL 作为查询输入。无界结果集，无需服务端物化，无需额外的独立基础设施。
- **JDBC** —— 集成 BI 工具（Tableau、Power BI、DBeaver）。
- **WebSocket / SSE** —— 订阅：近实时变更事件；后端：PG 原生、MongoDB 原生、CDC、轮询。同时也通过 Kafka 暴露。

### 数据源 {: #data-sources }

- **73 个连接器，85 种以上数据源** —— 较大的数字把托管的 PostgreSQL 服务（RDS、Aurora、Cloud SQL、AlloyDB、Azure、Supabase、Neon）和 File Crawler 的各种文件格式逐一计算在内。PostgreSQL、MySQL、MongoDB、Cassandra、Elasticsearch、Neo4j、SPARQL 三元组存储、Kafka、Google Sheets、Kaggle、Salesforce、ServiceNow、Stripe 等都位于同一个 API 之后。图和 RDF 数据源与关系型数据源地位相同。
- **智能路由** —— 单源查询会跳过联邦（低于 100ms）。多源查询经由联邦层执行，可以用您自己的集群，也可以用内置的工作节点。
- **API 数据源** —— 把 REST、GraphQL、gRPC、WebSocket 或 RSS 端点注册为可查询的表，并附带 SPARQL 辅助工具。API 数据源与关系型数据源之间的联邦连接无需额外设置。
- **远程 schema 内省** —— 指向任意 GraphQL、OpenAPI 或 gRPC 端点；已记录的操作会自动暴露为可查询的表、图节点和边，并在其上完整应用治理
- **文件数据源** —— CSV、Parquet 和 SQLite 文件作为可查询的表；支持本地路径和远程对象存储（`s3://`、`ftp://`、`sftp://`）
- **Kaggle 数据集** —— 通过令牌认证的实时搜索，检索 Kaggle 的公开数据集目录并注册某个数据集，无需手动下载；注册为一个单一的文件数据源，其表会被自动发现，仅支持 CSV/Parquet 压缩包
- **Kafka 集成** —— 主题作为只读表；查询结果作为 Kafka 接收端
- **计划触发器** —— Cron 和间隔触发器（APScheduler），可触发 webhook、mutation 或 Kafka 接收端发布
- **联邦性能提示** —— SQL 注释形式的路由提示可覆盖自动路由决策

![Data Sources](docs/images/data-sources.png)

数据源、文件和远程端点通过 UI 注册为被治理的表：

![Table Registration](docs/images/table-registration.png)

### 安全与治理 {: #security-governance }

- **行级安全** —— 按表、按角色注入 WHERE 子句
- **列脱敏** —— 按列脱敏（正则、常量、截断），支持基于角色的绕过
- **列预设** —— 在插入/更新时注入的服务端静态值或会话变量值；不会暴露在 mutation 输入类型中
- **写权限** —— 按列的 mutation 访问控制（`writable_by`）
- **角色继承** —— 角色递归地从父角色继承 RLS、可见性和脱敏规则
- **被跟踪的函数与 webhook** —— 数据库函数和出站 webhook 被暴露为具有带类型返回形态的 GraphQL mutation
- **ABAC 审批钩子** —— 执行前授权钩子；webhook、gRPC 或 unix_socket 传输；可作用于单表、单数据源或全局范围；可配置的回退策略
- **可插拔认证** —— Firebase、Keycloak、OAuth 2.0、simple（测试用）

![Security Roles](docs/images/security-roles.png)

### 交付与性能 {: #delivery-performance }

- **作为已记录转换的物化视图** —— MV 会记录生成它的转换：它的连接形态或 SQL、构建时所依据的各数据源输入信号（Iceberg 快照、RDB 水位线），以及注册时的确定性检查。由于转换已被记录，Provisa 可以把查询或其中一部分改写到新鲜的 MV 上，无需您修改查询。匹配基于连接模式，并接受部分匹配，因此只覆盖部分连接的 MV 依然适用，其余连接会被保留。
- **热表内联** —— 频繁被联接的小型查找表被内联为查询计划中直接的 VALUES CTE，消除维度数据的跨源往返
- **查询缓存** —— 按角色+RLS 分区的 Redis 结果缓存；包含 APQ 哈希缓存
- **作为数据的可观测性** —— 分布式追踪、指标和日志通过 OpenTelemetry 采集，压缩存入 S3 上的 Iceberg，并在联邦模式中注册为可查询的表（`traces`、`metrics`、`logs`、`queries`）。您可以用 SQL、GraphQL 或 Cypher 把它们和业务数据放在一起查询。例如，把 `customers` 表与 `queries` 连接，就能看到谁运行了什么、耗时多久。

### 管理与集成 {: #administration-integration }

- **管理 API** —— 位于 `/admin/graphql` 的 GraphQL；配置的上传/下载、关系编辑、查询审批
- **报表查看器** —— `/admin/reports` 列出内置的运维域管理视图以及任何已注册的自定义报表；需要 `observability` 能力
- **表预览** —— 每张已注册的表都有一个支持下推过滤、多级分组和 CSV 导出的服务端分页治理数据查看器
- **GraphQL Voyager** —— 交互式的、按角色划定范围的 schema 可视化，呈现为实体关系图
- **LLM 关系发现** —— 由 Claude 推荐外键候选。
- **Python 客户端** —— `pip install provisa-client`；GraphQL/SQL → DataFrame，Arrow Flight → pyarrow Table，SQLAlchemy 方言，ADBC 支持
- **数据摄取** —— 用于将 JSON 事件数据推送进平台的 HTTP 端点
- **Hasura v2 / DDN 导入** —— 将 Hasura v2 元数据或 DDN supergraph YAML 转换为 Provisa 配置
- **Apollo Federation** —— 将 Provisa 暴露为一个 Apollo Federation v2 子图

按角色划定范围的 schema 被可视化为实体关系图（GraphQL Voyager）：

![Schema Voyager](docs/images/schema-voyager.png)

关系被注册、审批，并作为唯一合法的 JOIN 路径被强制执行：

![Relationships](docs/images/relationships.png)

## 安全模型 {: #security-model }

“就在每个查询本来就要走的路径上”这句话，要在这里经受检验。Provisa 对每种查询语言（GraphQL、SQL、Cypher）和每种传输方式（REST、gRPC、Arrow Flight、JDBC、pgwire、Bolt、WebSocket）应用同一套分层安全模型，没有任何查询路径能绕过它。覆盖来自设计本身，所以没人需要记住什么：添加一个数据源、一列或一个关系，每一层都会立刻作用于它，不需要额外注册。

各层按顺序应用，请求必须通过一层，才会检查下一层。

### 第 0 层 —— 内省过滤 {: #layer-0-introspection-filtering }

一个角色看到的模式和目录，只包含其 `domain_access` 列表中的表，以及通过各自 `visible_to` 规则的列。角色访问范围之外的一切从一开始就不可见：既不能查询，也不会出现在自动补全中，甚至无法推断它存在。这适用于 GraphQL 模式、SQL 目录和查询编辑器的模式浏览器。

### 第 1 层 —— 公开访问 {: #layer-1-public-access }

没有 `domain_access` 限制的域中的表，对所有已认证身份可见，无需任何额外配置。真正公开的数据不费任何功夫。

### 第 2 层 —— 域访问 {: #layer-2-domain-access }

每个角色带有一个由域 ID 组成的 `domain_access` 列表，触及这些域之外的表的查询会在执行前被拒绝。这是粗粒度的归属边界：无论 SQL 怎么写，人力资源角色都碰不到财务表。

### 第 3 层 —— 行级安全 {: #layer-3-row-level-security }

确认域访问之后，按表、按角色的 `WHERE` 谓词会在执行时加到每个 `SELECT` 上，并针对原始数据求值。查询共享订单表的区域经理只能看到自己区域的行，即使用的是 `SELECT *`。

### 第 4 层 —— 列可见性与脱敏 {: #layer-4-column-visibility-and-masking }

`visible_to` 列表不包含请求角色的列，会从输出中移除。带有脱敏规则的列，在结果离开服务器之前，其值会被正则遮盖、常量替换或截断。脱敏适用于所有查询语言和所有输出格式。

### 第 5 层 —— 谓词守卫 {: #layer-5-predicate-guard }

脱敏列在 `WHERE` 和 `HAVING` 子句中会被拒绝。否则，即使输出已脱敏，调用者也能在过滤条件里用二分查找推出真实值。拒绝发生在查询解析时，任何东西运行之前。

### 关系治理 {: #relationship-governance }

在 SQL 中，JOIN 条件必须与表之间已注册并获批准的关系相符，未获批准的联接会被拒绝。每个关系都带有易读的原因和描述，向人和自主代理说明这条路径为什么存在。这是治理策略，不是硬性的安全边界。无论联接是什么形状，第 2 至第 5 层都照样生效，所以刻意绕行也不会暴露角色用两个独立查询拿不到的任何东西，而且每次尝试都会记入日志以供审计。

无效的联接永远不会在您的数据上运行。角色默认受治理，要退出，就得有意把 `ignore_relationships` 权限授予您信任的角色。代理的 OAuth 令牌映射到一个角色，所以代理得到的层与人相同。带上下文层的数据目录告诉代理它应该做什么；Provisa 强制规定它能做什么。

---

这些层可以叠加。一个拥有域访问、RLS 和脱敏列的角色，所有层同时生效，因为每一层都单独配置，并作用于任何触及受治理对象的查询。

## 快速开始 {: #quick-start }

### macOS

1. 下载 [Provisa-macOS.dmg](https://provisa.dev/dl/macos)（始终是最新版本）
2. 将 **Provisa.app** 拖到 `/Applications`，双击启动
3. 首次启动会完成一次性设置（约 2 分钟，无需联网）
4. 打开终端：

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. 下载 [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux)（始终是最新版本）
2. 赋予可执行权限并运行。首次启动会完成一次性设置（无需联网）：

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. 下载 [Provisa-windows-x64.exe](https://provisa.dev/dl/windows)（始终是最新版本）
2. 运行安装程序，它不需要管理员权限。
3. 从开始菜单打开 **Provisa First Launch**，它会完成一次性设置（约 5 分钟，无需联网）。
4. 打开一个新终端：

```bash
provisa start
```

### 第一次查询 {: #first-query }

在本地开发环境（`PROVISA_MODE=test`）中不需要凭据。在生产环境中，您使用 Bearer 令牌进行认证，Provisa 会从中读取角色。

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC（Tableau、DBeaver、Power BI） {: #jdbc-tableau-dbeaver-power-bi }

下载 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（始终是最新版本），并将其加入你的 BI 工具的驱动路径。

```text
jdbc:provisa://localhost:8815
```

使用您的 Provisa 用户名和密码进行认证，服务器会为您分配角色。

Tableau 和 Power BI 的设置步骤参见 [docs/integrations.md](docs/integrations.md)。

### PostgreSQL 线协议（pgwire） {: #postgresql-wire-protocol-pgwire }

Provisa 在 5439 端口上使用 PostgreSQL 线协议。任何能连接 Postgres 的客户端都能连接 Provisa，无需驱动、无需适配器，也无需改动您现有的工具。

**PostgreSQL 用户名用于选择 Provisa 角色。** 在 `provider: none`（信任模式）下，密码会被忽略，任何已配置的角色名都会被接受为用户名 —— 以 `analyst`、`admin` 或任意角色连接，即可看到该角色所治理的数据视图。在 `provider: simple` 下，密码会经过 bcrypt 验证。其他认证提供方（`firebase`、`keycloak`、`oauth`）不支持通过 pgwire 使用。

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

每个查询都经过完整的治理流水线，所以域访问、RLS、脱敏和谓词保护的作用方式与 GraphQL 和 REST 完全相同。模式浏览器（DBeaver、DataGrip、pgAdmin）无需任何设置即可使用。Provisa 从一个按角色域访问权限限定的内存目录回答它们的 `pg_catalog` 和 `information_schema` 查询，因此用户只能看到他们有权查询的表和列。

DataGrip 通过 pgwire 浏览受治理的模式及其外键关系图，无需驱动，也无需适配器：

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

通过设置 `PROVISA_PGWIRE_CERT` 和 `PROVISA_PGWIRE_KEY` 启用 TLS。端口可通过 `PROVISA_PGWIRE_PORT` 配置（默认 `5439`）。

### Bolt（Neo4j 线协议） {: #bolt-neo4j-wire-protocol }

Provisa 也支持 Neo4j 的 **Bolt** 协议，所以图原生工具可以直接连接，并对联邦图运行 Cypher。无需导出任何数据，也不需要单独的图数据库。把 **Neo4j Browser** 或 **Bloom** 指向 Provisa，即可在相同的治理（域访问、RLS、脱敏）下跨数据源遍历关系。

这里 Neo4j Browser 正在对 Provisa 运行 Cypher，节点标签、关系类型和属性键都直接来自已注册的 schema：

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

设置 `PROVISA_BOLT_PORT` 即可启用它（Neo4j 的默认端口是 `7687`），并通过 `PROVISA_BOLT_CERT` 和 `PROVISA_BOLT_KEY` 开启 TLS。已认证用户拥有的每个 Provisa 角色都显示为一个可选择的 `provisa_<role>` 数据库（即上图中的 `provisa_admin` 选择器）。选择其中一个会把会话收窄到该角色的域权限，用户永远无法超出自己拥有的角色。

### Python 客户端 {: #python-client }

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role URL parameter
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

完整参考请参见 [docs/python-client.md](docs/python-client.md)。

## 文档 {: #documentation }

| 主题 | 文档 |
| --- | --- |
| 开发者快速上手（从源代码运行） | [docs/quickstart.md](docs/quickstart.md) |
| 完整的 YAML 配置参考 | [docs/configuration.md](docs/configuration.md) |
| 端点参考（GraphQL、REST、Flight、gRPC） | [docs/api-reference.md](docs/api-reference.md) |
| 系统设计与组件地图 | [docs/architecture.md](docs/architecture.md) |
| 安全模型（RLS、脱敏、认证） | [docs/security.md](docs/security.md) |
| 密钥存储与 `${secret:NAME}` 引用 | [docs/secrets.md](docs/secrets.md) |
| 业务术语表与术语管理 | [docs/glossary.md](docs/glossary.md) |
| 环境（开发 / 预发布 / 生产） | [docs/environments.md](docs/environments.md) |
| 构建测试环境（画像、伪造数据、合成数据） | [docs/test-data.md](docs/test-data.md) |
| 伪造数据方法参考 | [docs/fake-methods.md](docs/fake-methods.md) |
| 在笔记本电脑上运行和构建演示（售前工程师） | [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) |
| 支持的数据源类型 | [docs/sources.md](docs/sources.md) |
| SSE 订阅 | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC、BI 工具、Arrow Flight 客户端、Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python 客户端（`provisa-client`） | [docs/python-client.md](docs/python-client.md) |
| 管理 API | [docs/admin.md](docs/admin.md) |
| 部署（Docker Compose、Kubernetes、macOS） | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN 导入 | [docs/import.md](docs/import.md) |
| 发布流程（alpha/beta/stable 标签） | [docs/releasing.md](docs/releasing.md) |

## 规模规划 {: #sizing }

Provisa 内置一个用于多源查询的联邦引擎。首次启动时你会选择一个内存预算；Provisa 会自动推导本地联邦 worker 的数量。

| 主机内存 | Worker 数量 | 典型负载 |
| --- | --- | --- |
| < 24 GB | 0 | 开发环境、单源查询、小型团队 |
| 24–47 GB | 1 | 小型团队、适度的跨源查询 |
| 48–95 GB | 2 | 部门级部署，BI + notebook 混合使用 |
| 96 GB 以上 | 4 | 大型部门，高并发联邦查询 |

可以随时通过编辑 `~/.provisa/config.yaml`（`federation_workers: N`）并运行 `provisa restart` 来更改 worker 数量。设为 `0` 则仅运行协调节点（单节点）。

### 超越单机的扩展 {: #scaling-beyond-a-single-box }

**水平扩展** —— 在负载均衡器后面运行多个 Provisa 实例。每个实例都是完整的系统。所有实例必须指向同一个配置数据库（在从属机器上设置 `CONFIG_DB_HOST`），并可共享一个 Redis 实例（`REDIS_URL`）作为统一缓存。大多数查询无需改动即可分散执行。非常大的跨源连接可能超出单个实例的承受范围，这时需要更大的机器或外部联邦集群。

**共享 Redis** —— 在每个实例上设置 `REDIS_URL` 指向一个外部 Redis。共享 Redis 意味着某一个实例产生的缓存条目对所有实例可用，从而提升整个集群的命中率。

**自带联邦集群** —— 让 Provisa 指向一个已有的外部联邦集群，而不是使用内置的 worker。推荐用于大规模或云端部署；配置方式参见 [docs/deployment.md](docs/deployment.md)。

## 许可证 {: #license }

Business Source License 1.1（未经修改，遵循 MariaDB 的 Licensor 承诺）。每个
已发布版本会在其公开发布的第 4 个周年日转换为 Change License（GPL v2.0 或更高版本）；
当前及近期代码保持在 BSL 之下。超出 Additional Use Grant 阈值（少于 100
名员工/承包商且上一年度营收低于 100 万美元）的生产使用需要商业
许可证。参见 [LICENSE](LICENSE)。

Licensor 不同意将本作品用于 AI/ML 训练。参见
[NOTICE](NOTICE)、[ai.txt](ai.txt) 和 [robots.txt](robots.txt)。如需商业
或 AI 训练许可证：<kennethstott@gmail.com>
