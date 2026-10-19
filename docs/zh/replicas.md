# 副本 {: #replicas }

副本是数据源表的一份拷贝，Provisa 将它保存在引擎的物化存储中，并读取它来代替数据源。本页介绍副本按什么建立索引、副本应存放在何处，以及副本有多新鲜。一张表是否被复制，由 [`replicate`](configuration.md#the-replicate-setting) 以及引擎能够访问到的内容决定；参见 [What an engine changes](engines.md#what-an-engine-changes)。

## 副本带有索引 {: #replicas-are-indexed }

Provisa 按模型使用其表的方式为副本建立索引。

- **声明的主键**，作为键强制执行。
- **声明的关系所连接的列**：
  - 以该表为源的每个关系的外键；
  - 以该表为目标的每个关系的目标列，除非该列已经是键；
  - 关系所经过的联结表的两个外键。

以下面这个关系为例：

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

`orders` 的副本得到它的键，以及 `customer_id` 上的索引。`customers` 的副本得到它的键，如果 `id` 就是该键，则无需更多。这样，在使用索引进行联接的存储中，这两张表的联接两侧都会读取索引（参见[各存储构建什么](#what-each-store-builds)）。建立在副本不含的计算列上的关系不会得到索引。

在列上用 `is_primary_key: true` 声明键。没有声明键的副本只得到其联接索引。

### 先可读，后索引 {: #readable-first-indexed-after }

落地从不等待索引。副本从换入的那一刻起就可读取；随后副本运行器在单独的一轮中，在每个副本的索引锁之下，逐个构建它的索引。 落地之后最初的几次查询会在没有这些索引的情况下运行。

每次落地都会按名称记录它所请求的索引、已构建的索引和失败的索引，记录在控制平面中该副本的记录上（`replica_state.index_state`）。

- **索引失败绝不会使落地失败。** 它会连同原因写入日志，并在下一次落地时再次请求。日志行形如 `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`。
- **与已声明的键重复的行是独立的失败**，代码为 `replica.index_duplicate_key`，并指明键的列。数据源中存在模型声明为唯一的列集合的重复值。重试无法修复：请在数据源中更正数据，或更正声明的键。
- **在构建过程中替换表的落地**会丢下之前的构建；它们会停止且不记录任何内容，新表则得到自己的构建。在 PostgreSQL 存储上，替换会取消仍在被替换的表上运行的构建。
- **计划在每次落地时遵循模型。** 之后新增的关系会在副本的下一次落地时得到其索引。

管理界面不显示索引记录。请查看服务器日志，或副本所在行的 `index_state` 列。

### 各存储构建什么 {: #what-each-store-builds }

| 存储 | 构建的内容 | 方式 |
| --- | --- | --- |
| PostgreSQL：`pg` 引擎自己的数据库、Trino 或 DuckDB 的 PostgreSQL 存储、基于 PostgreSQL 的 `sqlalchemy` 引擎 | 键和联接索引 | 在其自己的连接上执行 `CREATE INDEX CONCURRENTLY`，因此不会阻塞读取方和写入方。键先构建为唯一索引，再通过 `ADD CONSTRAINT ... PRIMARY KEY USING INDEX` 成为主键。被取消的构建留下的无效索引会被删除并重新构建。 |
| 关系型引擎背后的其他数据库（`mysql`、`oracle`、`mssql` 等） | 键和联接索引 | 该方言自己的 DDL。拒绝该 DDL 的数据库会把该索引记录为失败。 |
| 嵌入式 DuckDB 文件、MotherDuck | 键和联接索引 | 通过存储代理：`ALTER TABLE ... ADD PRIMARY KEY`、`CREATE INDEX IF NOT EXISTS`。 |
| ClickHouse | 仅排序键 | ClickHouse 副本按其声明的键排序（`ORDER BY`），在落地时设定。ClickHouse 没有可构建的二级 B-tree 索引。 |
| Snowflake、BigQuery、Databricks、Fabric、Synapse | 无 | 数据仓库自行组织表。 |

索引只对在索引所在之处运行的联接有帮助。有两种引擎读取其存储时不会使用存储的索引来做联接：

- **使用嵌入式文件的 DuckDB。** 引擎从不挂载该文件。每条语句会读取它所涉及的每张存储表的本地拷贝，存储每次发生变化后都会重新生成该拷贝。联接在该拷贝上以哈希联接运行，因此联接索引不会加速它们；文件的索引负责强制执行键，并为就地读取该文件的任何操作提供服务。
- **使用 PostgreSQL 存储的 DuckDB。** DuckDB 通过其 `postgres` 扩展读取存储，并关闭了过滤下推（上游缺陷 duckdb/duckdb-postgres#564），因此它会扫描副本，并在 DuckDB 中过滤和联接。存储的索引已构建，但这些读取不使用它们。

MotherDuck 存储是挂载并就地读取的。

## 当引擎在关系型数据库中运行 SQL 时，用该数据库作为副本存储 {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

当引擎在关系型数据库内部运行联接时，请把副本保存在那里。这样联接就会读取上述索引。有三种配置可以做到：

- **`pg` 引擎。** 副本落地到引擎自己的 PostgreSQL 数据库中，每条语句都在那里通过它们的索引运行。
- **关系型引擎**（`mysql`、`oracle`、`mssql`、兜底的 `sqlalchemy` 等）。每个数据源都落地到该数据库中，并在那里查询。
- **使用其 PostgreSQL 存储的 Trino。** PostgreSQL 是 Trino 支持的唯一存储；Trino 通过其 PostgreSQL 连接器读取副本。 Trino 下推到 PostgreSQL 的过滤或联接会使用这些索引；Trino 自己完成的工作则不会。

用 `PROVISA_MATERIALIZE_URL` 或 `materialize_store_url` 将引擎指向某个存储；组织自己注册的存储优先于这两者。

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

这是一般性建议，而不是规则。是否划算取决于引擎、数据源、存储和表的大小。请权衡以下几点：

- **每次查询的访问路径。** 引擎通过网络访问的存储，会给每次读取增加一次往返和一次传输。嵌入式 DuckDB 文件是本地的、列式的，扫描很快。
- **落地成本。** 把副本写入关系型数据库再构建其索引，比写入本地文件的成本更高。
- **数据库自身的负载。** 副本读取、落地和索引构建都在该数据库上运行。请按它们来确定其规模。
- **大规模扫描。** 对读取大表大部分内容的查询，索引没有任何作用。列式存储扫描它比行式存储更快。

## 选择副本存放的位置 {: #choosing-where-replicas-live }

存储只是若干手段之一，而且它们相互重叠。先确定其他手段；存储只决定它们留下的部分。

| 手段 | 作用 | 适用场景 |
| --- | --- | --- |
| Hot tables | 小型查找表保存在 Redis 中，并作为内联的 `VALUES` CTE（公用表表达式）放入每个读取它的查询。 | 在任何存储上与小型查找表的联接：行随查询一起传递。多对一关系的目标表，或行数不超过 `hot_tables.auto_threshold`（默认 1,000）的表，会在首次读取时变为 hot。`hot: true` 强制放入，`hot: false` 将其排除。 |
| 你定义的物化视图 | 带有 `materialize: true` 的视图会被存储并刷新。读取该视图的查询，在视图新鲜且读取方看到的是完整内容时，由存储的行应答：没有任何输入带有比“全部”更窄的行规则。 | 大表上的聚合。哪些视图存在由你决定；不会替你创建任何视图。参见 [Views](configuration.md#views-governed-computed-datasets)。 |
| 关系 `materialize: true` | 跨数据源关系会得到一个存储的、预先联接的父子视图。联接完全或部分匹配该模式的请求，无论以 SQL 形式到达还是由编译器构建，都会被改写为读取它。 | 在不构建联接索引的存储上的联接。参见 [Auto-Materialization](configuration.md#auto-materialization)。 |
| 放置 | 引擎能够实时读取的数据源实时读取；其余的则被复制。最终存储中有什么，取决于你选择的引擎。 | 选择引擎决定了存储需要容纳的大部分内容。 |

关系物化和带索引的联接以两种方式满足同一需求。在列式或数据仓库存储（DuckDB 文件、ClickHouse、Snowflake、BigQuery、Databricks、Fabric、Synapse）上不构建联接索引，因此预先联接的视图就是让频繁联接变得低成本的办法。在关系型存储上，联接列已经有索引，同样的联接无需物化也能快速运行。

仍取决于存储的是：对没有任何物化视图覆盖、也没有 hot 表服务的大表做的临时扫描和联接。这类操作在关系型数据库中依靠存储的索引运行，在列式存储中则依靠其扫描速度。

结果因工作负载而异。修改前后请用[容量规划工具](sizing.md)测量；它的[实时或副本运行](sizing.md#live-or-replica-reads-and-the-ttl)会给出在你的部署上副本读取与实时读取的成本对比。

## 新鲜度 {: #freshness }

把新鲜度与速度一并纳入决策。答案可以有多旧，以及保持其最新对数据源意味着多大成本，决定了一张表是实时读取还是从副本读取，从而决定存储需要容纳什么。

企业数据源很少支持实时。数据通过批量加载或夜间 ETL 到达，变更流存在滞后，许多系统无法承受查询量级的实时读取。从每张表所需的新鲜度出发，再选择满足它的最便宜的策略。运维方一侧的规则见 [Freshness and load: who decides](configuration.md#freshness-and-load-who-decides)。

| 策略 | 设置方式 | 保证什么 | 对数据源的成本 |
| --- | --- | --- | --- |
| 实时读取 | `replicate: -1` | 截至查询时是最新的 | 每个查询都读取数据源 |
| TTL | `change_signal: ttl`, `cache_ttl` | 副本不比 `cache_ttl` 更旧 | 每个 `cache_ttl` 重建一次；有 `watermark_column` 时，只重建超过水位线的行 |
| 变更探测 | `change_signal: probe` 或 `ttl_probe`，`cache_ttl` | 仅在探测报告有变更后才重建；探测每个 `cache_ttl` 运行一次 | 每个间隔一次探测，仅在有变更时重建 |
| 推送变更流 | Kafka 或 WebSocket 数据源；带 `cdc` 块的数据源上的 `change_signal: debezium`；PostgreSQL、MongoDB 或 Cosmos DB 上的 `change_signal: native` | 新鲜度在变更流滞后与防抖范围之内 | Kafka、WebSocket 和 Debezium：首次构建后无，变更流自带行。PostgreSQL、MongoDB 和 Cosmos DB：每一批变更重建一次 |
| 外部信号 | `change_signal: signal` | 每当 `POST /data/sources/{source_id}/signal` 表示数据源已变更时重建；从不按时钟重建 | 每个信号重建一次 |
| 按角色 TTL | 表上的 `role_ttl` | 列出的角色接受不超过其自身 TTL 的副本；表的 `cache_ttl` 仍是下限 | 重建更少：宽容角色的读取从不请求重建 |
| 负载保护 | `load_protected: true`，配合 `cache_ttl`、探测或 `off_peak_window` | 查询从不读取数据源；仅由运行器刷新副本，有非高峰窗口时仅在窗口打开期间刷新 | 仅计划内的刷新 |
| 新鲜度闸门 | 数据源上的 `freshness_gate: true` | 每次读取都先对照数据源的 `change_signal` 和 `cache_ttl` 检查；判定为过期则在读取前先刷新 | 过期时在读取路径中进行一次刷新 |
| 永不过期 | 数据源或表上的 `cache_ttl: -1` | 只加载一次；每次读取都由副本应答，读取尚未加载的表会被按名拒绝 | 仅此一次加载：所有表都已加载的数据源不再被连接、探测或读取，也不需要凭据 |

没有可比较内容的探测（没有水位线、哨兵值或探测查询）会退化为 TTL。

### 来自变更流的近实时 {: #near-real-time-from-a-change-feed }

当数据源无法承受实时查询负载，却已经发布其变更时，变更流可以在查询时不触碰数据源的情况下提供近实时数据。查询读取本地副本，既快又不给数据源带来负载，而事件循环在变更到达时应用它们。数据只滞后于变更流，通常为几秒。

仅有变更流还不够。Provisa 只会为接入了变更流的表跟踪它：

- **Kafka 数据源。** 表指明其主题，每个变更按主键应用到副本，包括删除。

  ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **WebSocket 数据源。** 其数据流以同样方式应用。
- **带 `change_signal: debezium` 的表**（在表或其数据源上），其数据源的 `cdc` 块指明 Kafka 传输和连接器的 topic 前缀。首次读取构建副本；之后 topic `{topic_prefix}.{schema}.{table}` 上的每个变更事件都按主键应用，包括删除，且仅在变更落地后才提交 offset。
- **此类数据源上带 `change_signal: kafka` 的表**。其变更从其 `live.kafka.topic` 指定的增量 topic 读取，使用同一传输，并以同样方式应用；使用此信号却未指定 topic 的表，在保存该表或其数据源以及加载配置时都会被点名拒绝。
- **带 `change_signal: native` 的 MongoDB 或 Cosmos DB 表**（在表或其数据源上）。变更流上的每一批变更都会请求重建副本。
- **带 `change_signal: native` 的 PostgreSQL 表**（在表或其数据源上）。Provisa 从自己的 slot `provisa_<source id>` 读取数据库的逻辑复制，所用的同名发布列出该数据源的原生表。不涉及 Kafka。表上的每一批变更都会请求重建其副本。数据库需要的条件与 Debezium 相同：`wal_level` 设为 `logical`、具有复制权限且可创建该发布的角色，以及主库而非只读副本；缺少其中任何一项的数据源在保存和加载配置时都会被点名拒绝。第一张表跟随时创建 slot 和发布，最后一张停止跟随或数据源被删除时将其删除。 服务器启动时会检查每个已注册的 PostgreSQL 数据源，并删除没有任何原生表跟随的 Provisa slot，因此在没有服务器运行时修改配置所遗留的 slot 不会继续占用数据源的 WAL。

被推送的表需要声明主键；没有主键的表会被跳过并写入日志。

带有推送变更信号的表在读取路径上视为新鲜，因为它的变更流就是它的新鲜度检查。 因此，无人接收的推送信号会被拒绝：除 Kafka、WebSocket、ingest、PostgreSQL、MongoDB 或 Cosmos DB 以外任何类型上的 `change_signal: native`，在保存数据源或表以及加载配置时都会被拒绝；没有 `cdc` 块的数据源上的 `debezium` 同样如此。
