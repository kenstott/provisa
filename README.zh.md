# Provisa

**连接你的数据库。使用 GraphQL、gRPC、SQL 或 MCP —— 通过任意 API 或协议 —— 5 分钟内完成。**

Provisa 在你所有数据源的联接结果之上服务每一个 API 表面（REST、GraphQL、SQL、gRPC、MCP 等等）。它之所以能做到这一点，是因为它是一个**主动语义层**：一份关于你数据资产的单一定义 —— 涵盖所有数据源上的每个域、每个关系和每条策略，唯独不包括源系统本身 —— 既运营这份资产，也治理这份资产。这份定义不是引擎可以参考的文档；它*就是*引擎。已注册的域和关系是唯一合法的联接路径，访问策略被编译进每个查询计划。一份模型，三项职责：

- **定义** —— 域、列和关系只声明一次。该声明就是每个消费者所看到的 schema，也是任何查询可以采用的唯一联接路径集合。
- **强制执行** —— 行级安全、列脱敏、列可见性和查询审批在执行路径上被内联应用。没有任何查询可以在不经过它们的情况下触达数据，因此覆盖率由构造保证是全面的，而不是靠人为的严谨。
- **审计** —— 由于每个请求都经过同一条被治理的路径，谁在什么角色下查询了什么、依据哪条策略，都被统一记录下来。分布式追踪、指标和日志本身也作为可查询的表与你的业务数据一起注册。

一个被治理的核心为每种语言和每种传输方式提供服务。使用 **GraphQL、Cypher 或 SQL** 查询；通过 **pgwire、Bolt、gRPC、REST、Arrow Flight 或 JDBC** 消费。每种查询语言都会降级为一份统一的中间表示，治理逻辑只在此处注入一次 —— 因此策略不可能在不同语言之间产生漂移 —— 该 IR 在输出时再重新定向为每个数据源的原生方言。新增一种语言只是在共享核心之上新增一个前端，而不是新增一个引擎。

这份数据资产兼具分析型和事务型特性。跨源读取通过联邦层扇出；写入和单源读取直接路由到数据源驱动 —— 治理方式完全相同，但具备事务性且延迟在亚 100ms 级别。内置 Arrow Flight 列式流式传输。

整个模型由为数不多的几个原语构建而成 —— 域、关系、角色和策略。词汇量小，因此这份定义易于理解，也便于评估和审计：你可以直接读懂策略集合，知道它做了什么。Provisa 是一个轻量级的查询编译器，而不是一个坐在数据路径上的运行时。它把一个请求转换为原生查询，路由它们，然后让开。这正是这份数据资产能保持高性能的原因。

这种设计支持两种使用方式，二者并不互斥：

- **作为现代化改造的脚手架** —— 对你的数据资产建模，让 Provisa 为每个数据源生成原生 SQL，然后捕获该 SQL 并直接在目标系统中采用它。Provisa 是过渡层，而不是永久性依赖。
- **作为永久性的策略强制执行基础设施** —— 将其保留为每次查询都必须经过的被治理路径，使定义、强制执行和审计在这份数据资产存在期间始终保持统一。

## 联邦模型

整个模型可以归结为两个契约和两条策略：数据源归约为单一类型系统上的二维表，查询归约为一种类 SQL 的 IR，可达性决定了哪些数据是被实时查询的、哪些是被物化的，而新鲜度策略则治理每一份物化副本和派生数据集。数据形态进，查询形态进，治理在联接处，原生查询出。本节的其余部分将逐一介绍每个部分。

该模型建立在一个归约之上：每个数据源都被表达为单一通用类型系统之上的一组二维表集合。这就是一个数据源要加入这份数据资产必须满足的契约，且对所有数据源都是同一份契约。有些数据源天生就符合 —— MySQL 或 PostgreSQL 的一张表*本身*就是一个带类型的二维关系。有些经过一次投影就能符合：一个 GraphQL 查询结果，一旦被展平，就是一张表。有些天生不符合这种形态 —— SPARQL 三元组存储、Neo4j —— 但仍然可用，因为用户提供的查询本身产生的是表格状结果集；查询本身就是适配器。无论数据源是什么，这份数据资产看到的只是行、列和通用类型，别无其他。接入一种新的数据源类型，就是满足这一个契约，有时需要一步人工介入，而不是编写一次定制集成。

这一归约在查询侧也有对应物。SQL —— 跨越其所有方言和怪癖 —— 本质上就是二维数据集分析的语言，这使得类 SQL 的形态成为查询的天然通用目标。因此，无论以何种语言到达，每个请求的第一步都是先降级为这份中间表示。有些语言降级得很干净 —— SQL 本身，甚至 GraphQL；有些则很难 —— Cypher 的路径和图语义需要真正的工作量 —— 但都是可行的。在任何其他处理发生之前，先把每个请求都汇入同一份 IR，这正是治理能够只在一个地方、以一种形态、不论来源语言为何而统一应用的原因。

在这两种统一形态 —— 表格化的数据源和单一的查询形态 —— 之上，此处的联邦既指实时查询也指数据仓库化：这正是像 Trino 这样的实时查询引擎所覆盖的范围，再加上此类引擎所依赖的物化能力。将两者统一起来的概念是**可达性**：对于任意数据源，引擎能否就地查询它，还是必须先将其数据物化到某个可查询的地方？可达性把这份数据资产划分为被实时查询的部分和先被复制的部分。

大多数数据库已经具备某种实时链接的概念 —— DuckDB 的 `ATTACH`、PostgreSQL 的 `postgres_fdw`、Databricks 的外部链接。因此大多数数据库都能在某种程度上充当联邦引擎。但没有一个是全面的：每一个都能触达特定的一组数据源，其余部分则需要物化，而且没有一份统一的说明来区分哪些是哪些。该模型通过让可达性变得显式来弥合这一差距 —— 针对每个数据源，定义一组方法，说明引擎能实时触达什么，以及（通过排除法）什么必须被物化。

剩下的问题是新鲜度：对于每个不可达的数据源，其物化副本必须多新？在实践中，这可以归约为一小组策略 —— 按需、按计划、按变更信号（CDC、水位线、快照），或固定不变。为每个数据源选择其中一种，就是全部的新鲜度策略。

分析型数据集 —— 派生表、聚合结果、转换的输出 —— 归入同一种形态。它们同样必须被表达为该 IR，正因如此，血缘不需要作为一个独立系统来维护：从每个源系统到最终输出的路径，*就是*产生该输出的那份 IR，可以端到端读懂。构建这些数据集把新鲜度问题向后推了一步 —— 该数据集是按计划刷新，还是仅在其前置条件满足后才刷新，还是以近实时的方式持续刷新，或者作为一个固定不变的历史快照？表达如何以及何时构建一个数据集的方式，与前面那一小组、可枚举的方式相同，因此一个派生数据集所携带的构建策略，与一份数据源副本使用完全相同的词汇。

维度模型是一个直接的应用。星型模式的事实表和维度表和其他任何分析型数据集一样 —— 一个维度是一份经过一致化、去重的投影；一张事实表是一次联接和聚合归约到某个粒度的结果 —— 每一个都携带自己的构建策略和新鲜度策略。渐变维度不需要任何特殊机制：固定不变的快照就是 Type 2 历史，按计划的重建就是 Type 1。而且由于该 schema 是在 IR 中定义的，而不是物理绑定到某一个数据仓库的表上，同样的事实表和维度表定义可以重新定向 —— 物化到 Oracle、Databricks，或者在某个 MPP 引擎上保持虚拟 —— 而无需重新建模。该模型生成星型模式；它不会把星型模式锁死在某个引擎上。

Data Vault 也以同样的方式契合，只是提前了一层。它的 hub 是去重的业务键数据集，它的 link 是它们之间已注册的关系，它的 satellite 是仅追加、带时间戳的属性数据集 —— 即历史记录。satellite 只是一个采用变更信号新鲜度策略的派生数据集：load-date 加 hashdiff 就是应用于描述性属性上的 CDC，仅追加的历史就是固定快照策略。时点表（point-in-time）和桥接表（bridge table）是为查询性能而构建的进一步派生数据集。因此，一个原始 vault 就是 IR 中的一组分析型数据集，而星型模式是从它投影出来的 —— 两者都是生成的，都能跨引擎迁移。该模型不会替你决定方法论 —— 什么应该成为 hub、satellite 的粒度、拆分策略。这些仍然是建模选择；一旦做出选择，它们就以可迁移的 IR 形式存在，而不是焊死在某一个数据仓库上的 ETL。

这两种模式都通过**两个一等的快捷方式**声明，而不是手写视图 —— 它们是每一个星型模式和 Data Vault 都由其构建而成的原语，且保持方法论中立：

- **`entity`（实体）** —— 一个数据源的、带键、去重、可选历史化的投影。声明一个实体键、一组属性和一种历史模式；Provisa 将其降级为一个物化视图，当请求历史化时降级为一个**双时态 MV**（`scd2` → 增量，`snapshot` → 快照）。一个构造同时服务于 Kimball 的**维度**（SCD1/SCD2）和 Data Vault 的 **hub + satellite**。
- **`fact`（事实）** —— 一次到实体键的联接，归约到一个声明的粒度，带聚合度量。Provisa 将其降级为一个聚合 MV，外加到各实体的已注册关系。一个构造同时服务于星型模式的**事实表**和 Data Vault 的 **link**（一个没有度量的事实就是一个纯粹的键集 link）。

由于这种降级是纯粹的 —— 一份 `entity`/`fact` 规格会精确地变成建模者原本需要手写的 MV、双时态和关系定义 —— 数据仓库自始至终都是 IR，可以跨引擎重新定向而无需重新建模。可以在管理界面（针对实体和事实的**建模**表单）中声明一个数据仓库，也可以通过管理 API（`registerEntity` / `registerFact`）声明；该模型*生成* Kimball 星型模式或 Data Vault，而不是强加某一种。

### 时间旅行

时间旅行是一个简单的想法 —— 保留一行数据的每一个版本而不是覆盖它，这样你就可以询问数据在过去任意时刻*曾经是什么样子*。不同之处在于每个引擎能以多高的效率做到这一点，这正是 Provisa 把它做成物化视图**定义**的一个属性、而不是存储引擎的一个属性的原因（REQ-1162）。声明一次；它就能在任何具备物化能力的后端上工作。

保持其可迁移性的规则是**仅追加**：一个版本一旦写入，就永远不会被更新或删除。通过写回一个"有效截止"日期来退役一行 —— 这是常见的双时态技巧 —— 需要一次 UPDATE，而许多引擎无法在联邦存储之上廉价地（甚至根本无法）做到这一点，所以 Provisa 不这样做。相反，每次刷新都**追加**，"哪个版本在时刻 T 生效"是在读取时从这份不可变的日志中推导出来的。追加的方式恰好只有两种：

- **快照** —— 追加整个全新的数据集，标记为本次刷新的系统时间。无需差异比对；在任何引擎上都正确；每次刷新存储量增长一份完整副本。
- **增量** —— 只追加发生变化的部分，加上被移除键的墓碑标记。该增量**由引擎计算**（在一次 `INSERT … SELECT` 内部的反联接），从不在 Provisa 中逐行折叠。更小，且需要一个实体键。

系统时间（Provisa 记录某个版本的时刻）以这种方式管理；有效时间（某个事实在业务上成立的时刻）由视图自身的 SELECT 提供并被保留。提供更多能力的引擎 —— 原生的 Iceberg 快照、维护更少行的 MERGE —— 可以在同一份声明背后被定向以获得效率；仅追加路径是在任何地方都正确的底线。

读取是透明的。对一个双时态 MV 的普通查询默认从追加日志中重建**当前**状态；要进行时间旅行，发送一个 `X-Provisa-As-Of: <timestamp>` 请求头，整个查询就会以该时刻这份数据资产的状态来回答 —— 在任何底层存储上语义都相同。可以在管理界面中为任意物化视图开启该功能（一个**时间旅行**控件：关闭 / 快照 / 增量，外加一个实体键），也可以通过管理 API 开启。

可达性加新鲜度构成了一个通用的数据联邦模型：一份定义说明什么是实时的、什么是物化的、每份副本保持多新 —— 与任何一个引擎自身的触达能力无关。其结果是摆脱专有厂商锁定。该模型是可迁移的；这份数据资产不会被困在当下触达数据源最多的那家厂商的联邦能力里。

## 功能特性

### 查询接口

这些是你用来编写查询的语言和结构化 API。每一种都有自己的语法和语义；治理（RLS、脱敏、列可见性、关系强制执行）在所有这些接口上统一应用，与承载它们的线协议无关。

- **GraphQL** —— 按角色区分的 schema，具备字段级可见性、过滤、基于游标的分页和聚合查询（`count`、`sum`、`avg`、`min`、`max`）。schema 被约束在已注册的关系之内 —— 由构造保证结构有效，是编写正确的简单查询的最快路径。内置 Apollo APQ：查询在服务端被哈希并注册；后续调用只需通过 HTTP GET 发送哈希值，使响应可被 CDN 缓存，且无需任何客户端改动。低于可配置行数阈值的查找表被暴露为枚举类型。
- **SQL** —— 完整的联邦数据 SQL 支持；不受约束，比 GraphQL 更具表达力。编写标准 SQL —— 相关子查询等等 —— 它可以原样跨数据源运行。单源查询完全绕过联邦层（亚 100ms）。
- **Cypher** —— 基于同一联邦 schema 的图查询语言。将关系作为图的边进行遍历；联合数据源；支持变长路径。治理与 GraphQL 和 SQL 完全相同。
- **gRPC 模型 API** —— 从已注册的 schema 自动生成 `.proto`；为每张表提供带类型的查询和插入 RPC，流式返回结果。与 GraphQL 同样是由 schema 驱动的 —— 注册模型就是契约，protobuf 只是线上编码方式。与 Arrow Flight（一种列式流式传输协议）不同，这是一个完整的逐表查询接口。
- **JSON:API** —— 位于 `/data/jsonapi/{table}` 的结构化查询 API，设计上仅支持 HTTP。支持 JSON:API 1.1：稀疏字段集（`fields[table]=col1,col2`）、过滤表达式（`filter[field][op]=value`）、复合文档（`include=relation`）以及排序。不是通用查询语言 —— 一次查询一张表，使用标准化的过滤语法，而非临时拼凑的查询字符串。
- **查询语言浏览器** —— 编写一个 GraphQL 查询，在侧边面板中实时查看对应的**语义化 SQL** 和 **Cypher** 翻译；可以复制其中任意一个，或直接跳转到 SQL 或图编辑器。一种实用的工作流是先在 GraphQL 中勾勒查询片段，再把生成的 SQL 拼接进复杂的视图或报表中。

该浏览器将一个 GraphQL 查询与其实时的 SQL 和 Cypher 翻译并排展示：

![Query Language Explorer](docs/images/query-explorer.png)

同一份联邦 schema 也可以作为一个实时图来探索 —— 域和节点标签、关系类型，以及变长遍历：

![Graph Visualization](docs/images/graph-view.png)

### 查询组合工具

这些工具帮助你用上述语言编写查询 —— 它们本身不是查询语言。

- **自然语言查询** —— 由 Claude 驱动的 NL→SQL/Cypher/GraphQL 流水线。用简单的英语描述你想要什么；该流水线会用你选择的语言生成一条查询，并在执行前提供一个交互式验证循环。

![Natural Language Query](docs/images/natural-language.png)

### 线协议

这些是连接协议。SQL、GraphQL 和 Cypher 都承载在它们之上 —— 选择哪种线协议不会改变查询接口或治理行为。

- **pgwire** —— 任何 PostgreSQL 客户端（psql、DBeaver、DataGrip、asyncpg、SQLAlchemy、pandas `read_sql`）都可以连接到 5439 端口，就好像它是一个 Postgres 服务器一样。仅接受 SQL。完整的治理流水线适用。`pg_catalog` 和 `information_schema` 由内存中的目录回答，因此 schema 浏览器无需一次联邦往返即可工作。TLS 可选。
- **Bolt（Neo4j）** —— 任何 Neo4j 客户端（Neo4j Browser、Bloom、官方驱动）都可以通过 Bolt 协议连接，并对联邦图运行 Cypher。用户所持有的每个角色都会呈现为一个 `provisa_<role>` 数据库。治理方式与其他每种传输方式相同。TLS 可选。
- **Arrow Flight** —— 基于 gRPC 的高吞吐列式流式传输；接受 GraphQL 或 SQL 作为查询输入。无界结果集，无需服务端物化，无需额外的独立基础设施。
- **JDBC** —— 以 `approved` 或 `catalog` 模式集成 BI 工具（Tableau、Power BI、DBeaver）。
- **WebSocket / SSE** —— 订阅：近实时变更事件；后端：PG 原生、MongoDB 原生、CDC、轮询。同时也通过 Kafka 暴露。

### 数据源

- **54 种数据源类型** —— PostgreSQL、MySQL、MongoDB、Cassandra、Elasticsearch、Neo4j、SPARQL 三元组存储、Kafka、Google Sheets、Kaggle 等等，全部通过单一 API 访问；图和 RDF 数据源是一等公民，而非适配器
- **智能路由** —— 单源查询绕过联邦（亚 100ms）；多源查询通过联邦层路由 —— 可自带集群，也可使用内置的 worker
- **API 数据源** —— 将 REST、GraphQL、gRPC、WebSocket 或 RSS 端点注册为可查询的表；内置 SPARQL 辅助工具；跨 API 数据源和关系型数据源的联邦联接透明工作
- **远程 schema 内省** —— 指向任意 GraphQL、OpenAPI 或 gRPC 端点；已记录的操作会自动暴露为可查询的表、图节点和边，并在其上完整应用治理
- **文件数据源** —— CSV、Parquet 和 SQLite 文件作为可查询的表；支持本地路径和远程对象存储（`s3://`、`ftp://`、`sftp://`）
- **Kaggle 数据集** —— 通过令牌认证的实时搜索，检索 Kaggle 的公开数据集目录并注册某个数据集，无需手动下载；注册为一个单一的文件数据源，其表会被自动发现，仅支持 CSV/Parquet 压缩包
- **Kafka 集成** —— 主题作为只读表；查询结果作为 Kafka 接收端
- **计划触发器** —— Cron 和间隔触发器（APScheduler），可触发 webhook、mutation 或 Kafka 接收端发布
- **联邦性能提示** —— SQL 注释形式的路由提示可覆盖自动路由决策

![Data Sources](docs/images/data-sources.png)

数据源、文件和远程端点通过 UI 注册为被治理的表：

![Table Registration](docs/images/table-registration.png)

### 安全与治理

- **行级安全** —— 按表、按角色注入 WHERE 子句
- **列脱敏** —— 按列脱敏（正则、常量、截断），支持基于角色的绕过
- **列预设** —— 在插入/更新时注入的服务端静态值或会话变量值；不会暴露在 mutation 输入类型中
- **写权限** —— 按列的 mutation 访问控制（`writable_by`）
- **角色继承** —— 角色递归地从父角色继承 RLS、可见性和脱敏规则
- **被跟踪的函数与 webhook** —— 数据库函数和出站 webhook 被暴露为具有带类型返回形态的 GraphQL mutation
- **ABAC 审批钩子** —— 执行前授权钩子；webhook、gRPC 或 unix_socket 传输；可作用于单表、单数据源或全局范围；可配置的回退策略
- **可插拔认证** —— Firebase、Keycloak、OAuth 2.0、simple（测试用）

![Security Roles](docs/images/security-roles.png)

### 交付与性能

- **作为已记录转换的物化视图** —— 一个 MV 会捕获产生它的那次转换：它的联接形态或 SQL、构建它所依据的各数据源输入信号（Iceberg 快照、关系型数据库水位线），以及注册时的一次确定性检查。由于该转换被记录了下来，查询（或子表达式）会被透明地重写到一个新鲜的 MV 上 —— 采用支持部分匹配的结构化联接模式匹配，因此即使一个 MV 只覆盖了部分联接，它仍然适用，其余联接则被保留
- **热表内联** —— 频繁被联接的小型查找表被内联为查询计划中直接的 VALUES CTE，消除维度数据的跨源往返
- **查询缓存** —— 按角色+RLS 分区的 Redis 结果缓存；包含 APQ 哈希缓存
- **作为数据的可观测性** —— 分布式追踪、指标和日志通过 OpenTelemetry 收集，压缩进 S3 上的 Iceberg，并自动注册为联邦 schema 中可查询的表（`traces`、`metrics`、`logs`、`queries`）；可以用 SQL、GraphQL 或 Cypher 与你的业务数据一起查询它们 —— 将一张 `customers` 表联接到 `queries` 表，看看是谁运行了什么查询、耗时多久

### 管理与集成

- **管理 API** —— 位于 `/admin/graphql` 的 GraphQL；配置的上传/下载、关系编辑、查询审批
- **报表查看器** —— `/admin/reports` 列出内置的运维域管理视图以及任何已注册的自定义报表；需要 `observability` 能力
- **表预览** —— 每张已注册的表都有一个支持下推过滤、多级分组和 CSV 导出的服务端分页治理数据查看器
- **GraphQL Voyager** —— 交互式的、按角色划定范围的 schema 可视化，呈现为实体关系图
- **LLM 关系发现** —— 由 Claude 驱动的外键候选建议
- **Python 客户端** —— `pip install provisa-client`；GraphQL/SQL → DataFrame，Arrow Flight → pyarrow Table，SQLAlchemy 方言，ADBC 支持
- **数据摄取** —— 用于将 JSON 事件数据推送进平台的 HTTP 端点
- **Hasura v2 / DDN 导入** —— 将 Hasura v2 元数据或 DDN supergraph YAML 转换为 Provisa 配置
- **Apollo Federation** —— 将 Provisa 暴露为一个 Apollo Federation v2 子图

按角色划定范围的 schema 被可视化为实体关系图（GraphQL Voyager）：

![Schema Voyager](docs/images/schema-voyager.png)

关系被注册、审批，并作为唯一合法的 JOIN 路径被强制执行：

![Relationships](docs/images/relationships.png)

## 安全模型

正是在这里，"每次查询本就要走的路径"不再只是一句口号。Provisa 在每一种查询语言（GraphQL、SQL、Cypher）和每一种传输方式（REST、gRPC、Arrow Flight、JDBC、pgwire、Bolt、WebSocket）之上强制执行一个多层次的安全模型。治理被统一应用 —— 不存在可以绕过它的查询路径。覆盖率由构造保证是全面的，而不是靠人为的严谨：新增一个数据源、一列或一个关系，每一层都会自动应用到它上面，没有任何需要人工记得去注册的东西。

各层按顺序应用。一个请求必须先通过每一层，才能进入下一层的评估。

### 第 0 层 —— 内省过滤

呈现给某个角色的 schema 和目录只包含其 `domain_access` 列表中的表，以及通过了逐列 `visible_to` 规则的列。角色访问权限之外的对象在发现阶段就是不可见的 —— 它们无法被查询、自动补全，也无法被推断出其存在。这适用于 GraphQL schema、SQL 目录以及查询编辑器的 schema 浏览器。

### 第 1 层 —— 公开访问

没有 `domain_access` 限制的域中的表，对所有已认证身份可见，无需任何额外配置。对于真正公开的数据，零摩擦。

### 第 2 层 —— 域访问

每个角色都携带一个域 ID 列表 `domain_access`。一个触及这些域之外某张表的查询会在执行之前被拒绝。这是粗粒度的所有权边界 —— 无论 SQL 怎么写，一个 HR 角色都无法触达财务表。

### 第 3 层 —— 行级安全

在确认域访问权限之后，按表、按角色的 `WHERE` 谓词会在执行时被注入到每一条 `SELECT` 中。这些谓词直接作用于原始数据。一位区域经理查询一张共享的订单表时，即使执行的是 `SELECT *`，也只能看到自己区域的行。

### 第 4 层 —— 列可见性与脱敏

`visible_to` 列表中排除了请求角色的列会从查询输出中被剥离。带有脱敏规则的列，其值会在结果离开服务器之前被替换 —— 正则脱敏、常量替换或截断。脱敏适用于所有查询语言和所有输出格式。

### 第 5 层 —— 谓词守卫

被脱敏的列会被禁止出现在 `WHERE` 和 `HAVING` 子句中。如果没有这一层，调用方即使看到的是被脱敏的输出，仍可以通过在过滤条件中对其进行二分搜索来推断出未脱敏的真实值。这一拒绝在查询解析阶段、在执行之前就被强制执行。

### 关系治理

SQL 中的 JOIN 条件必须匹配表之间一个已注册、已审批的关系。未经审批的联接会被拒绝。每个关系都携带一段人类可读的理由和描述 —— 为用户和自主代理提供该遍历路径为何存在的指引。这是一项治理策略，而不是一道硬性的安全边界：无论联接结构如何，第 2 到第 5 层始终有效，因此一次蓄意的规避不会暴露该角色本无法通过两次独立查询获取的数据。规避尝试会被记录并可审计。

---

这些层是可组合的。一个同时具有域访问、RLS 和脱敏列限制的角色，五重约束同时生效。新增一个数据源、一列或一个关系，不需要更新每一条规则 —— 每一层都被独立配置，并自动适用于任何触及被治理对象的查询。

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
2. 赋予可执行权限并运行 —— 首次启动会完成一次性设置（无需联网）：

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. 下载 [Provisa-windows-x64.exe](https://provisa.dev/dl/windows)（始终是最新版本）
2. 运行安装程序 —— 无需管理员权限
3. 从开始菜单打开 **Provisa First Launch** —— 完成一次性设置（约 5 分钟，无需联网）
4. 打开一个新终端：

```bash
provisa start
```

### 第一次查询

在本地开发环境中（`PROVISA_MODE=test`），无需任何凭据。在生产环境中，使用 Bearer 令牌进行认证 —— 角色会自动从中提取。

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

下载 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（始终是最新版本），并将其加入你的 BI 工具的驱动路径。

```text
jdbc:provisa://localhost:8815
```

使用你的 Provisa 用户名和密码进行认证 —— 服务器会分配你的角色。

- **`catalog` 模式** —— 完整 schema 可见；配合目录工具使用（Collibra、Atlan、DBeaver）

Tableau 和 Power BI 的设置步骤参见 [docs/integrations.md](docs/integrations.md)。

### PostgreSQL 线协议（pgwire）

Provisa 在 5439 端口上讲 PostgreSQL 线协议。任何能连接 Postgres 的客户端都能连接 Provisa —— 无需驱动，无需适配器，现有工具无需任何改动。

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

所有查询都经过完整的治理流水线 —— 域访问、RLS、脱敏和谓词守卫的应用方式与 GraphQL 和 REST 完全相同。schema 浏览器（DBeaver、DataGrip、pgAdmin）开箱即用：`pg_catalog` 和 `information_schema` 查询由一个限定在该角色域访问范围内的内存目录回答，因此用户只能看到自己被允许查询的表和列。

DataGrip 通过 pgwire 浏览被治理的 schema 及其外键关系图 —— 无需驱动，无需适配器：

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

通过设置 `PROVISA_PGWIRE_CERT` 和 `PROVISA_PGWIRE_KEY` 启用 TLS。端口可通过 `PROVISA_PGWIRE_PORT` 配置（默认 `5439`）。

### Bolt（Neo4j 线协议）

Provisa 还讲 Neo4j 的 **Bolt** 协议，因此图原生工具可以直接连接，并对联邦图运行 Cypher —— 无需导出，无需独立的图数据库。将 **Neo4j Browser** 或 **Bloom** 指向 Provisa，即可跨数据源遍历关系，并应用相同的治理（域访问、RLS、脱敏）。

Neo4j Browser 对 Provisa 运行 Cypher —— 节点标签、关系类型和属性键都直接来自已注册的 schema：

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

通过设置 `PROVISA_BOLT_PORT`（Neo4j 的默认值为 `7687`）启用它。TLS 通过 `PROVISA_BOLT_CERT` 和 `PROVISA_BOLT_KEY` 启用。已认证用户所持有的每个 Provisa 角色都会呈现为一个可选择的 `provisa_<role>` 数据库（上图中的 `provisa_admin` 选择器）—— 选择其中一个会将会话限定在该角色的域权限范围内；用户永远无法超出自己所持有的角色。

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
| 开发者快速上手（从源代码运行） | [docs/quickstart.md](docs/quickstart.md) |
| 完整的 YAML 配置参考 | [docs/configuration.md](docs/configuration.md) |
| 端点参考（GraphQL、REST、Flight、gRPC） | [docs/api-reference.md](docs/api-reference.md) |
| 系统设计与组件地图 | [docs/architecture.md](docs/architecture.md) |
| 安全模型（RLS、脱敏、认证） | [docs/security.md](docs/security.md) |
| 密钥存储与 `${secret:NAME}` 引用 | [docs/secrets.md](docs/secrets.md) |
| 业务术语表与术语管理 | [docs/glossary.md](docs/glossary.md) |
| 环境（开发 / 预发布 / 生产） | [docs/environments.md](docs/environments.md) |
| 支持的数据源类型 | [docs/sources.md](docs/sources.md) |
| SSE 订阅 | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC、BI 工具、Arrow Flight 客户端、Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python 客户端（`provisa-client`） | [docs/python-client.md](docs/python-client.md) |
| 管理 API | [docs/admin.md](docs/admin.md) |
| 部署（Docker Compose、Kubernetes、macOS） | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN 导入 | [docs/import.md](docs/import.md) |
| 发布流程（alpha/beta/stable 标签） | [docs/releasing.md](docs/releasing.md) |

## 规模规划

Provisa 内置一个用于多源查询的联邦引擎。首次启动时你会选择一个内存预算；Provisa 会自动推导本地联邦 worker 的数量。

| 主机内存 | Worker 数量 | 典型负载 |
| --- | --- | --- |
| < 24 GB | 0 | 开发环境、单源查询、小型团队 |
| 24–47 GB | 1 | 小型团队、适度的跨源查询 |
| 48–95 GB | 2 | 部门级部署，BI + notebook 混合使用 |
| 96 GB 以上 | 4 | 大型部门，高并发联邦查询 |

可以随时通过编辑 `~/.provisa/config.yaml`（`federation_workers: N`）并运行 `provisa restart` 来更改 worker 数量。设为 `0` 则仅运行协调节点（单节点）。

### 超越单机的扩展

**水平扩展** —— 在负载均衡器后面运行多个 Provisa 实例。每个实例都是一个功能完整的系统。所有实例必须指向同一个配置数据库（在从属机器上设置 `CONFIG_DB_HOST`），并可以选择共享一个 Redis 实例（`REDIS_URL`）以实现统一缓存。大多数查询会透明地分布式执行；非常大的跨源联接可能超出单个实例的资源，需要更大的机器或外部联邦集群。

**共享 Redis** —— 在每个实例上设置 `REDIS_URL` 指向一个外部 Redis。共享 Redis 意味着某一个实例产生的缓存条目对所有实例可用，从而提升整个集群的命中率。

**自带联邦集群** —— 让 Provisa 指向一个已有的外部联邦集群，而不是使用内置的 worker。推荐用于大规模或云端部署；配置方式参见 [docs/deployment.md](docs/deployment.md)。

## 许可证

Business Source License 1.1（未经修改，遵循 MariaDB 的 Licensor 承诺）。每个
已发布版本会在其公开发布的第 4 个周年日转换为 Change License（GPL v2.0 或更高版本）；
当前及近期代码保持在 BSL 之下。超出 Additional Use Grant 阈值（少于 100
名员工/承包商且上一年度营收低于 100 万美元）的生产使用需要商业
许可证。参见 [LICENSE](LICENSE)。

Licensor 不同意将本作品用于 AI/ML 训练。参见
[NOTICE](NOTICE)、[ai.txt](ai.txt) 和 [robots.txt](robots.txt)。如需商业
或 AI 训练许可证：<kennethstott@gmail.com>
