# Provisa

**连接您的数据库。使用 GraphQL、gRPC、SQL 或 MCP——通过任意 API 或协议——在 5 分钟内完成查询。**

Provisa 在跨数据源的联合结果之上提供每一个 API 层（REST、GraphQL、SQL、gRPC、MCP 等）。它之所以能做到这一点，是因为它是一个**主动语义层（active semantic layer）**：一份对您整个数据资产的单一定义——涵盖每个域、关系和策略，横跨您的所有数据源，唯一排除的是数据源系统本身——它既运行该资产，又治理该资产。这份定义不是引擎可能会参考的文档，它*就是*引擎本身。已注册的域和关系是唯一合法的连接（join）路径，访问策略被编译进每一个查询计划。一个模型，三项职责：

- **定义（Define）**——域、列和关系只需声明一次。该声明就是每个消费者所看到的架构（schema），也是任何查询可以采用的唯一连接路径集合。
- **强制执行（Enforce）**——行级安全、列脱敏、列可见性和查询审批都在执行路径上内联应用。没有任何查询能够绕过它们直接触达数据，因此覆盖范围因结构设计而天然完整，而非依赖人工尽职。
- **审计（Audit）**——由于每个请求都经过同一条被治理的路径，谁在何种角色下、依据何种策略查询了什么，都会被统一记录。分布式追踪、指标和日志本身也被注册为可查询的表，与您的业务数据并存。

一个被治理的核心为每种语言和传输方式提供服务。使用 **GraphQL、Cypher 或 SQL** 查询；通过 **pgwire、Bolt、gRPC、REST、Arrow Flight 或 JDBC** 消费。每种查询语言都会降阶（lower）为单一的中间表示（IR），治理逻辑只在此处注入一次——因此策略不会在各语言之间产生偏移——该 IR 在输出时会重新定向到每个数据源的原生方言。新增一种语言只是在共享核心之上新增一个前端，而不是新建一个引擎。

该数据资产同时具备分析型和事务型特性。跨数据源的读取通过联合查询层扇出；写入和单一数据源的读取则直接路由至数据源驱动——治理方式完全相同，但具备事务性且延迟低于 100ms。内置 Arrow Flight 列式流式传输。

整个模型由少量的基本要素构建而成——域、关系、角色和策略。词汇量小，因此定义易于理解，也便于评估和审计：您可以直接阅读策略集，了解它做了什么。Provisa 是一个轻量级查询编译器，而不是一个驻留在数据路径中的运行时。它将请求转换为原生查询、路由该查询，然后不再介入——这正是该数据资产性能优异的原因。

这样的设计支持两种互不排斥的使用方式：

- **作为现代化改造的脚手架**——对您的数据资产建模，让 Provisa 为每个数据源生成原生 SQL，然后捕获该 SQL 并直接在目标系统中采用。Provisa 是过渡层，而不是永久依赖。
- **作为永久性的策略强制执行基础设施**——将其长期保留为每个查询都必须经过的治理路径，使定义、强制执行和审计在该数据资产存续期间始终保持统一。

## 联合查询模型

整个模型可以归结为两个契约和两项策略：数据源归约为基于单一类型系统的二维表，查询归约为一种类 SQL 的中间表示（IR），可达性（reachability）决定了哪些数据是实时查询、哪些是物化的，而新鲜度（freshness）策略则治理每一份物化副本和派生数据集。数据形态输入，查询形态输入，在连接（join）处进行治理，原生查询输出。本节接下来将逐一说明每个部分。

该模型建立在一个归约之上：每个数据源都被表达为基于单一通用类型系统的一组二维表。这就是数据源要加入该数据资产必须满足的契约，对所有数据源都是同一契约。一些数据源已经天然符合——MySQL 或 PostgreSQL 的表本身*就是*一个带类型的二维关系。一些数据源经过投影后即可符合：GraphQL 查询结果一旦被展平，就是一张表。一些数据源与这种形态格格不入——SPARQL 三元组存储、Neo4j——但仍然可以运作，因为用户提供的查询其结果集是表格化的；这个查询本身就是适配器。无论数据源是什么，该数据资产看到的只有行、列和通用类型，别无其他。接入一种新类型的数据源，就是满足这一份契约，有时需要一步人工介入，而不是编写定制化集成。

这个归约在查询侧也有一个对应版本。SQL——无论其方言和特性有多少差异——本质上是面向二维数据集分析的语言，这使得类 SQL 形式成为查询的天然统一目标。因此，无论请求以何种语言到达，第一步都会先降阶为该中间表示。有些语言降阶得很干净——SQL 本身，甚至 GraphQL 也是；有些则很难——Cypher 的路径和图语义需要付出真正的工作量——但都是可行的。将每个请求都先汇入同一种 IR，才使治理能够只在一个地方、以一种形式应用，与请求到达时所用的语言无关。

在这两种统一形态之上——表格化的数据源和单一的查询形式——联合查询在这里同时意味着实时查询和数据仓库化——正是像 Trino 这样的实时查询引擎所覆盖的范围，再加上此类引擎所依赖的物化能力。将两者统一起来的概念是**可达性（reachability）**：对任意数据源而言，引擎能否原地查询它，还是必须先将其数据物化到某个可查询的位置？可达性将该数据资产划分为实时查询的部分和先复制再查询的部分。

大多数数据库已经具备某种实时链接的概念——DuckDB 的 `ATTACH`、PostgreSQL 的 `postgres_fdw`、Databricks 的外部链接。因此大多数数据库在一定程度上都能充当联合查询引擎。但没有一个是全面的：每一个都能触达特定的一组数据源，其余则需要物化，且没有统一的说明来区分哪些属于哪一类。该模型通过让可达性变得显式来弥合这一差距——针对每个数据源，定义一组明确的方法，说明引擎能够实时触达什么，并通过排除法得出必须被物化的部分。

剩下的问题是新鲜度：对于每个不可达的数据源，其物化副本必须多新？在实践中，这归结为一小组策略——按需、按计划、按变更信号（CDC、水位线、快照）或固定不变。为每个数据源选择一种策略，就是整个新鲜度策略。

分析型数据集——派生表、聚合结果、转换的输出——也归入同一形态。它们同样必须以 IR 表达，正因为如此，血缘不再是需要单独维护的系统：从每个源系统到最终输出的路径*就是*生成它的那份 IR，端到端可读。构建这些数据集会进一步引出新鲜度问题——该数据集是按计划刷新，还是仅在其前置条件满足后才刷新，或是以近实时方式持续刷新，还是作为固定的历史快照？表达如何以及何时构建数据集的方式，与数据源副本所用的是同一小组、可枚举的方式，因此派生数据集所携带的构建策略与数据源副本使用的是完全相同的词汇。

维度模型是一个直接的应用场景。星型架构的事实表和维度表与其他分析型数据集并无二致——维度是一个经过一致性处理、去重的投影；事实表是连接和聚合归约到某个粒度的结果——各自携带自己的构建和新鲜度策略。缓慢变化维度不需要任何特殊机制：固定快照就是 Type 2 历史，按计划重建就是 Type 1。而且由于该架构是在 IR 中定义的，而不是物理绑定到某一个数据仓库的表，同样的事实表和维度表定义可以重新定向——在 Oracle 中物化，在 Databricks 中物化，或者在某个 MPP 引擎上保持虚拟——而无需重新建模。该模型生成星型架构，而不会将其锁定到某个引擎上。

Data Vault 也以同样的方式契合，只是早一个层次。它的中心表（hub）是去重后的业务键数据集，它的链接表（link）是这些数据集之间已注册的关系，它的卫星表（satellite）是仅追加、带时间戳的属性数据集——即历史记录。卫星表其实就是采用变更信号新鲜度策略的派生数据集：装载日期加哈希差异（hashdiff）就是应用于描述性属性的 CDC，仅追加历史就是固定快照策略。时点表（point-in-time）和桥接表（bridge）是为查询性能而构建的进一步派生数据集。因此，一个原始的 Data Vault 就是 IR 中的一组分析型数据集，而星型架构则是其上的一个投影——两者都是生成的，都可以跨引擎移植。该模型不会替您决定方法论：什么应该成为中心表、卫星表的粒度、拆分策略。这些仍然是建模层面的选择；一旦确定，它们就以可移植的 IR 形式存在，而不是焊死在某一个数据仓库上的 ETL。

这两种模式都通过**两个一等公民的快捷方式**来声明，而不是手写视图——它们是所有星型架构和 Data Vault 都由之构建的基本要素，且保持方法论中立：

- **`entity`**——数据源的一个带键、去重、可选历史化的投影。声明一个实体键、若干属性和一种历史模式；Provisa 会将其降阶为一个物化视图，如果请求了历史记录，则降阶为一个**双时态物化视图（bitemporal MV）**（`scd2` → 增量，`snapshot` → 快照）。一个构造同时服务于 Kimball 的**维度**（SCD1/SCD2）和 Data Vault 的**中心表 + 卫星表**。
- **`fact`**——对实体键的一次连接，归约到一个声明的粒度，并带有聚合度量。Provisa 会将其降阶为一个聚合物化视图，加上到各实体的已注册关系。一个构造同时服务于星型架构的**事实表**和 Data Vault 的**链接表**（一个不带度量的事实就是一个纯键集链接表）。

由于该降阶过程是纯粹的——一份 `entity`/`fact` 规格会精确地变成建模者原本需要手写的物化视图、双时态和关系定义——数据仓库因而彻头彻尾地是 IR，可以跨引擎重新定向而无需重新建模。可以在管理界面（用于实体和事实的 **Model** 表单）中声明数据仓库，也可以通过管理 API（`registerEntity` / `registerFact`）声明；该模型*生成*了 Kimball 星型架构或 Data Vault，而不是强加某一种。

### 时间旅行

时间旅行是一个简单的想法——保留每一行的每个版本而不是覆盖它，这样您就可以查询数据在过去任意时刻*曾经*是什么样子。真正有差异的是每个引擎实现这一点的效率，这正是为什么 Provisa 将其做成物化视图**定义**的一个属性，而不是存储引擎的属性（REQ-1162）。只需声明一次；它就能在任何具备物化能力的后端上工作。

保持其可移植性的规则是**仅追加（append-only）**：一个版本一旦写入，就永远不会被更新或删除。通过写回一个“有效截止”日期来废弃一行——常见的双时态技巧——需要一次 UPDATE，而许多引擎无法在联合数据存储上廉价地（甚至根本无法）执行 UPDATE，所以 Provisa 不这么做。相反，每次刷新都是**追加**，"在时刻 T 哪个版本生效"是在读取时从不可变日志中派生出来的。追加的方式恰好只有两种：

- **快照（Snapshot）**——追加整份最新数据集，并标记本次刷新的系统时间。无需差异比较；在任何引擎上都正确；存储量会随每次刷新增加一整份副本。
- **增量（Delta）**——只追加发生变化的部分，加上已删除键的墓碑标记。增量是**由引擎计算的**（在 `INSERT … SELECT` 内部进行反连接），从不由 Provisa 逐行折叠计算。体积更小，并且需要一个实体键。

系统时间（Provisa 记录某个版本的时刻）以此方式管理；有效时间（某个事实在业务上成立的时刻）由视图自身的 SELECT 提供并予以保留。提供更多能力的引擎——原生 Iceberg 快照、维护更少行数的 MERGE——可以在同一份声明背后被作为效率优化的目标；仅追加路径是在所有引擎上都保证正确的下限。

读取是透明的。对双时态物化视图的普通查询默认会从追加日志中重建**当前**状态；要进行时间旅行，发送一个 `X-Provisa-As-Of: <timestamp>` 请求头，整个查询就会按照该数据资产在那一时刻的状态来回答——在任何底层存储上语义都完全一致。可以在管理界面（一个**时间旅行**控件：关闭 / 快照 / 增量，以及一个实体键）中为任意物化视图开启该功能，也可以通过管理 API 开启。

可达性加新鲜度构成了数据联合查询的一个通用模型：一份定义说明了什么是实时的、什么是物化的、每份副本保持多新——独立于任何一个引擎自身的触达能力。其结果是摆脱专有厂商锁定。该模型是可移植的；该数据资产不会被困于当前触达数据源最多的那家厂商的联合查询能力。

## 功能特性

### 查询接口

这些是您用来编写查询的语言和结构化 API。每种都有自己的语法和语义；治理（RLS、脱敏、列可见性、关系强制执行）在所有这些接口上统一应用，与传递它们的具体协议无关。

- **GraphQL**——按角色划分的架构，具备字段级可见性、过滤、基于游标的分页，以及聚合查询（`count`、`sum`、`avg`、`min`、`max`）。架构被约束在已注册的关系之内——因结构而天然有效，是编写正确简单查询最快的路径。内置 Apollo APQ：查询会被哈希并在服务端注册；后续调用只需通过 HTTP GET 发送哈希值，使响应可被 CDN 缓存，客户端无需任何改动。低于可配置行数阈值的查找表会以枚举类型的形式暴露。
- **SQL**——针对联合数据的完整 SQL 支持；不受约束，比 GraphQL 表达能力更强。编写标准 SQL——包括关联子查询在内——它可以在多个数据源上原样运行。单数据源查询完全绕过联合查询层（延迟低于 100ms）。
- **Cypher**——基于同一联合架构的图查询语言。将关系作为图的边进行遍历；对多个数据源做并集；支持变长路径。治理方式与 GraphQL 和 SQL 完全相同。
- **gRPC 模型 API**——根据已注册的架构自动生成 `.proto`；为每个表提供带类型的查询和插入 RPC，支持流式响应。与 GraphQL 一样是架构驱动的——注册模型即契约，protobuf 只是线路编码方式。与 Arrow Flight（一种列式流式传输协议）不同，这是一个完整的按表查询接口。
- **JSON:API**——位于 `/data/jsonapi/{table}` 的结构化查询 API，专为 HTTP 设计。支持 JSON:API 1.1：稀疏字段集（`fields[table]=col1,col2`）、过滤表达式（`filter[field][op]=value`）、复合文档（`include=relation`）以及排序。不是通用查询语言——一次只查询一张表，使用标准化的过滤语法而非临时拼凑的查询字符串。
- **查询语言浏览器（Query Language Explorer）**——编写一条 GraphQL 查询，即可在侧边面板中实时看到对应的**语义化 SQL** 和 **Cypher** 翻译；可以复制任意一种，或直接跳转到 SQL 或图编辑器中。实用的工作流程是先在 GraphQL 中勾勒查询片段，再将生成的 SQL 拼接到复杂的视图或报表中。

查询语言浏览器展示一条 GraphQL 查询及其实时的 SQL 和 Cypher 翻译：

![Query Language Explorer](docs/images/query-explorer.png)

同一个联合架构也可以作为一个实时图来探索——域和节点标签、关系类型，以及变长遍历：

![Graph Visualization](docs/images/graph-view.png)

### 查询组合工具

这些工具帮助您用上述语言编写查询——它们本身不是查询语言。

- **自然语言查询**——由 Claude 驱动的自然语言到 SQL/Cypher/GraphQL 管道。用简单的英语描述您想要的内容；该管道会生成您所选语言的查询，并在执行前提供交互式验证环节。

![Natural Language Query](docs/images/natural-language.png)

### 传输协议

这些是连接协议。SQL、GraphQL 和 Cypher 都承载在这些协议之上——选择哪种传输协议不会改变查询接口或治理行为。

- **pgwire**——任何 PostgreSQL 客户端（psql、DBeaver、DataGrip、asyncpg、SQLAlchemy、pandas 的 `read_sql`）都可以在 5439 端口上连接，就像连接的是一台 Postgres 服务器一样。仅接受 SQL。完整的治理流水线均适用。`pg_catalog` 和 `information_schema` 由内存目录直接应答，因此架构浏览器无需一次联合查询往返即可工作。TLS 可选。
- **Bolt（Neo4j）**——任何 Neo4j 客户端（Neo4j Browser、Bloom、官方驱动）都可以通过 Bolt 协议连接，并针对联合图执行 Cypher。用户所持有的每个角色都会呈现为一个 `provisa_<role>` 数据库。与其他所有传输方式使用相同的治理机制。TLS 可选。
- **Arrow Flight**——基于 gRPC 的高吞吐列式流式传输；接受 GraphQL 或 SQL 作为查询输入。结果集无边界，无需服务端物化，无需额外基础设施。
- **JDBC**——以 `approved` 或 `catalog` 模式集成 BI 工具（Tableau、Power BI、DBeaver）。
- **WebSocket / SSE**——订阅：近实时变更事件；支持的后端包括 PG 原生、MongoDB 原生、CDC、轮询。同时也通过 Kafka 暴露。

### 数据源

- **52 种数据源类型**——PostgreSQL、MySQL、MongoDB、Cassandra、Elasticsearch、Neo4j、SPARQL 三元组存储、Kafka、Google Sheets 等，均通过单一 API 接入；图数据源和 RDF 数据源是一等公民，而非适配器
- **智能路由**——单数据源查询绕过联合查询层（延迟低于 100ms）；多数据源查询通过联合查询层路由——可以自带集群，也可以使用内置的工作节点
- **API 数据源**——将 REST、GraphQL、gRPC、WebSocket 或 RSS 端点注册为可查询的表；内置 SPARQL 辅助工具；跨 API 数据源和关系型数据源的联合连接可透明工作
- **远程架构自省**——指向任意 GraphQL、OpenAPI 或 gRPC 端点；已记录的操作会自动呈现为可查询的表、图节点和边，并在其上完整应用治理
- **文件数据源**——CSV、Parquet 和 SQLite 文件均可作为可查询的表；支持本地路径和远程对象存储（`s3://`、`ftp://`、`sftp://`）
- **Kafka 集成**——主题（topic）作为只读表；查询结果可作为 Kafka 汇出目标（sink）
- **计划触发器**——基于 Cron 和间隔的触发器（APScheduler），可触发 webhook、变更操作或 Kafka 汇出发布
- **联合查询性能提示**——通过 SQL 注释形式的路由提示覆盖自动路由决策

![Data Sources](docs/images/data-sources.png)

数据源、文件和远程端点均可从界面中注册为受治理的表：

![Table Registration](docs/images/table-registration.png)

### 安全与治理

- **行级安全**——按表、按角色注入 WHERE 子句
- **列脱敏**——按列脱敏（正则、常量、截断），支持基于角色的旁路
- **列预设值**——在插入/更新时注入服务端静态值或会话变量值；不会暴露在变更操作的输入类型中
- **写权限**——按列的变更操作访问控制（`writable_by`）
- **继承角色**——角色可递归地从父角色继承 RLS、可见性和脱敏规则
- **受追踪的函数与 Webhook**——数据库函数和出站 webhook 以带类型返回结构的 GraphQL 变更操作形式暴露
- **ABAC 审批钩子**——执行前授权钩子；支持 webhook、gRPC 或 unix_socket 传输方式；作用域可为按表、按数据源或全局；可配置回退策略
- **可插拔身份验证**——Firebase、Keycloak、OAuth 2.0、simple（测试用）

![Security Roles](docs/images/security-roles.png)

### 交付与性能

- **作为已记录变换的物化视图**——一个物化视图（MV）记录了产生它的变换：它的连接形态或 SQL、构建它所依据的各数据源输入信号（Iceberg 快照、关系型数据库水位线），以及注册时的确定性检查。由于该变换被记录了下来，查询（或其子表达式）会被透明地重写到一个最新的物化视图上——采用带部分匹配支持的结构化连接模式匹配，因此即使某个物化视图只覆盖了部分连接，仍然可以应用，其余连接则被保留
- **热表内联**——被频繁连接的小型查找表会作为 VALUES CTE 直接内联到查询计划中，从而消除维度数据的跨数据源往返
- **查询缓存**——按角色和 RLS 分区的 Redis 结果缓存；包含 APQ 哈希缓存
- **作为数据的可观测性**——分布式追踪、指标和日志通过 OpenTelemetry 收集，压缩后存入 S3 上的 Iceberg，并自动注册为联合架构中可查询的表（`traces`、`metrics`、`logs`、`queries`）；可以使用 SQL、GraphQL 或 Cypher 与业务数据一起查询它们——将 `customers` 表连接到 `queries` 表，即可看到谁运行了什么查询、耗时多久

### 管理与集成

- **管理 API**——位于 `/admin/graphql` 的 GraphQL 接口；支持配置上传/下载、关系编辑、查询审批
- **报表查看器**——`/admin/reports` 列出内置的 ops 域管理视图以及所有已注册的自定义报表；需要 `observability` 能力
- **表预览**——每张已注册的表都配有服务端分页的受治理数据查看器，支持下推过滤、多级分组与 CSV 导出
- **GraphQL Voyager**——以实体关系图形式呈现的交互式、按角色划分的架构可视化
- **LLM 关系发现**——由 Claude 驱动的外键候选建议
- **Python 客户端**——`pip install provisa-client`；支持 GraphQL/SQL → DataFrame、Arrow Flight → pyarrow Table、SQLAlchemy 方言、ADBC
- **数据摄取**——用于将 JSON 事件数据推送到平台的 HTTP 端点
- **Hasura v2 / DDN 导入**——将 Hasura v2 元数据或 DDN supergraph YAML 转换为 Provisa 配置
- **Apollo Federation**——将 Provisa 暴露为 Apollo Federation v2 子图

以实体关系图形式可视化的按角色划分架构（GraphQL Voyager）：

![Schema Voyager](docs/images/schema-voyager.png)

关系经过注册、审批，并作为唯一合法的 JOIN 路径被强制执行：

![Relationships](docs/images/relationships.png)

## 安全模型

正是在这里，"每个查询本就必经的路径"不再只是一句口号。Provisa 在每种查询语言（GraphQL、SQL、Cypher）和每种传输方式（REST、gRPC、Arrow Flight、JDBC、pgwire、Bolt、WebSocket）上都强制执行一套多层安全模型。治理被统一应用——不存在能够绕过它的查询路径。覆盖范围因结构设计而天然完整，而非依赖人工尽职：添加一个数据源、列或关系后，每一层都会自动应用于其上，无需记得手动注册任何内容。

各层依序应用。一个请求必须先通过前一层，才会进入下一层的评估。

### 第 0 层——自省过滤

呈现给某个角色的架构和目录，只包含该角色 `domain_access` 列表中的表，以及通过按列 `visible_to` 规则的列。角色权限之外的对象在发现阶段就不可见——无法被查询、自动补全，也无法被推断存在。这适用于 GraphQL 架构、SQL 目录以及查询编辑器的架构浏览器。

### 第 1 层——公开访问

不带 `domain_access` 限制的域中的表，对所有已认证身份可见，无需任何额外配置。对于真正公开的数据，零摩擦。

### 第 2 层——域访问

每个角色携带一份域 ID 的 `domain_access` 列表。触及这些域之外的表的查询会在执行前被拒绝。这是粗粒度的所有权边界——无论 SQL 怎么写，HR 角色都无法触及财务表。

### 第 3 层——行级安全

在确认域访问权限之后，按表、按角色的 `WHERE` 谓词会在执行时被注入到每个 `SELECT` 中。这些谓词针对原始数据求值。一个地区经理查询共享的订单表时，即便是 `SELECT *`，也只能看到自己所在地区的行。

### 第 4 层——列可见性与脱敏

带有排除请求角色的 `visible_to` 列表的列会从查询输出中剥离。带有脱敏规则的列，其值会在结果离开服务器之前被替换——正则编辑、常量替换或截断。脱敏适用于所有查询语言和输出格式。

### 第 5 层——谓词守卫

被脱敏的列会被拒绝出现在 `WHERE` 和 `HAVING` 子句中。如果没有这一层，调用方可以通过在过滤条件中对其进行二分查找来推断出未脱敏的值，即便输出本身是被脱敏的。该拒绝在查询解析阶段就被强制执行，早于执行阶段。

### 关系治理

SQL 中的 JOIN 条件必须匹配表之间一个已注册、已审批的关系。未经审批的连接会被拒绝。每个关系都携带一段人类可读的原因和描述——为用户和自主智能体说明为何存在该遍历路径。这是治理策略，而不是一道硬性的安全边界：无论连接结构如何，第 2 至 5 层始终有效，因此蓄意规避不会暴露该角色本无法通过两次独立查询获取的数据。规避尝试会被记录并可审计。

---

这些层是可以叠加组合的。一个同时具有域访问、RLS 和脱敏列的角色，会同时激活全部五项约束。新增一个数据源、列或关系，不需要更新每一条规则——每一层都是独立配置的，并会自动应用于任何触及受治理对象的查询。

### macOS

1. 下载 [Provisa-macOS.dmg](https://provisa.dev/dl/macos)（始终是最新版本）
2. 将 **Provisa.app** 拖入 `/Applications`，双击启动
3. 首次启动会完成一次性设置（约 2 分钟，无需联网）
4. 打开终端：

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. 下载 [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux)（始终是最新版本）
2. 赋予其可执行权限并运行——首次启动会完成一次性设置（无需联网）：

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. 下载 [Provisa-windows-x64.exe](https://provisa.dev/dl/windows)（始终是最新版本）
2. 运行安装程序——无需管理员权限
3. 从开始菜单打开 **Provisa First Launch**——完成一次性设置（约 5 分钟，无需联网）
4. 打开新终端：

```bash
provisa start
```

### 第一次查询

在本地开发环境中（`PROVISA_MODE=test`），无需任何凭据。在生产环境中，使用 Bearer 令牌进行身份验证——角色会自动从令牌中提取。

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

### JDBC（Tableau、DBeaver、Power BI）

下载 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（始终是最新版本），并将其添加到您的 BI 工具的驱动路径中。

```text
jdbc:provisa://localhost:8815
```

使用您的 Provisa 用户名和密码进行身份验证——服务器会分配您的角色。

- **`catalog` 模式**——完整架构可见；用于目录类工具（Collibra、Atlan、DBeaver）

有关 Tableau 和 Power BI 的设置步骤，请参见 [docs/integrations.md](docs/integrations.md)。

### PostgreSQL 线路协议（pgwire）

Provisa 在 5439 端口上使用 PostgreSQL 线路协议。任何能够连接 Postgres 的客户端都可以连接 Provisa——无需驱动，无需适配器，现有工具链无需任何改动。

**PostgreSQL 用户名用于选择 Provisa 角色。** 在 `provider: none`（信任模式）下，密码会被忽略，任何已配置的角色名都可以作为用户名被接受——以 `analyst`、`admin` 或任意角色连接，即可看到该角色治理下的数据视图。在 `provider: simple` 下，密码会通过 bcrypt 校验。其他提供方（`firebase`、`keycloak`、`oauth`）不支持通过 pgwire 使用。

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

所有查询都会经过完整的治理流水线——域访问、RLS、脱敏和谓词守卫的适用方式，与 GraphQL 和 REST 完全一致。架构浏览器（DBeaver、DataGrip、pgAdmin）开箱即用：`pg_catalog` 和 `information_schema` 查询由一个限定在该角色域访问范围内的内存目录直接应答，因此用户只能看到自己有权查询的表和列。

DataGrip 通过 pgwire 浏览受治理的架构及其外键关系图——无需驱动，无需适配器：

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

通过设置 `PROVISA_PGWIRE_CERT` 和 `PROVISA_PGWIRE_KEY` 启用 TLS。端口可通过 `PROVISA_PGWIRE_PORT` 配置（默认为 `5439`）。

### Bolt（Neo4j 线路协议）

Provisa 还支持 Neo4j 的 **Bolt** 协议，因此图原生工具可以直接连接，并针对联合图执行 Cypher——无需导出，无需单独的图数据库。将 **Neo4j Browser** 或 **Bloom** 指向 Provisa，即可在跨数据源遍历关系的同时应用相同的治理机制（域访问、RLS、脱敏）。

Neo4j Browser 针对 Provisa 运行 Cypher——节点标签、关系类型和属性键均直接来自已注册的架构：

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

通过设置 `PROVISA_BOLT_PORT` 启用（Neo4j 的默认端口为 `7687`）。通过 `PROVISA_BOLT_CERT` 和 `PROVISA_BOLT_KEY` 启用 TLS。已认证用户所持有的每个 Provisa 角色都会呈现为一个可选的 `provisa_<role>` 数据库（上图中的 `provisa_admin` 选择器）——选择其中一个会将会话范围限定在该角色的域权限之内；用户永远无法超出自己所持有的角色。

### Python 客户端

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

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

完整参考请参见 [docs/python-client.md](docs/python-client.md)。

## 文档

| 主题 | 文档 |
| --- | --- |
| 开发者快速入门（从源码运行） | [docs/quickstart.md](docs/quickstart.md) |
| 完整的 YAML 配置参考 | [docs/configuration.md](docs/configuration.md) |
| 端点参考（GraphQL、REST、Flight、gRPC） | [docs/api-reference.md](docs/api-reference.md) |
| 系统设计与组件地图 | [docs/architecture.md](docs/architecture.md) |
| 安全模型（RLS、脱敏、身份验证） | [docs/security.md](docs/security.md) |
| 支持的数据源类型 | [docs/sources.md](docs/sources.md) |
| SSE 订阅 | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC、BI 工具、Arrow Flight 客户端、Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python 客户端（`provisa-client`） | [docs/python-client.md](docs/python-client.md) |
| 管理 API | [docs/admin.md](docs/admin.md) |
| 部署（Docker Compose、Kubernetes、macOS） | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN 导入 | [docs/import.md](docs/import.md) |
| 发布工作流（alpha/beta/stable 标签） | [docs/releasing.md](docs/releasing.md) |

## 规模评估

Provisa 内置了用于多数据源查询的联合查询引擎。首次启动时，您需要选择一个内存预算，Provisa 会自动推导本地联合查询工作节点的数量。

| 主机内存 | 工作节点数 | 典型工作负载 |
| --- | --- | --- |
| < 24 GB | 0 | 开发环境、单数据源查询、小型团队 |
| 24–47 GB | 1 | 小型团队、中等程度的跨数据源查询 |
| 48–95 GB | 2 | 部门级部署、BI 与笔记本混合使用 |
| 96 GB+ | 4 | 大型部门、高并发联合查询 |

可以随时通过编辑 `~/.provisa/config.yaml`（`federation_workers: N`）并运行 `provisa restart` 来更改工作节点数量。设为 `0` 表示仅协调（单节点）运行。

### 超越单机的扩展

**水平横向扩展**——在负载均衡器后运行多个 Provisa 实例。每个实例都是一个完整运作的系统。所有实例必须指向同一个配置数据库（在从属机器上设置 `CONFIG_DB_HOST`），并可选地指向一个共享的 Redis 实例（`REDIS_URL`）以实现统一缓存。大多数查询都能透明地分布式处理；非常大的跨数据源连接可能超出单个实例的资源上限，需要更大的机器或外部联合查询集群。

**共享 Redis**——在每个实例上设置 `REDIS_URL` 指向一个外部 Redis。共享 Redis 意味着某个实例的缓存条目对所有实例都可用，从而提升整个集群的命中率。

**自带联合查询集群**——将 Provisa 指向一个已有的外部联合查询集群，而不是使用内置的工作节点。推荐用于大规模或云端部署；配置方式请参见 [docs/deployment.md](docs/deployment.md)。

## 许可证

Business Source License 1.1（未经修改，遵循 MariaDB 的 Licensor 承诺）。每个
已发布的版本会在其公开发布满 4 周年时转换为 Change License（GPL v2.0 或更高版本）；
当前及近期代码保持在 BSL 之下。
超出额外使用许可（Additional Use Grant）阈值（员工/承包商少于 100 人，且
上一年度营收低于 100 万美元）的生产使用需要商业许可证。详见 [LICENSE](LICENSE)。

Licensor 不同意将本作品用于 AI/ML 训练。详见
[NOTICE](NOTICE)、[ai.txt](ai.txt) 和 [robots.txt](robots.txt)。如需商业许可证
或 AI 训练许可证：<kennethstott@gmail.com>
</content>
