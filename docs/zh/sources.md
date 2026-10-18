# 数据源类型 {: #source-types }

## 执行模型 {: #execution-model }

每个查询最终都通过联邦引擎执行，联邦引擎提供跨所有数据源的联邦能力。数据源根据其连接方式分为三类。[tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| 类别 | 拥有直连驱动 | 拥有联邦连接器 | 示例 |
| --- | --- | --- | --- |
| **直连型** | 是 | 是 | PostgreSQL、MySQL、MariaDB、SingleStore、SQL Server、Oracle、DuckDB |
| **仅联邦型** | 否 | 是 | Redshift、Druid、Exasol、Hive、Iceberg、Delta Lake、Hive（S3 支持） |
| **直读型（副本）** | 是 | 是 | Snowflake、Databricks、ClickHouse — 驱动读取数据并落地一份副本；查询在活动引擎中针对该副本执行 |
| **直连（无跨源连接器）** | 是 | 否 | Athena — 单数据源语句在 Athena 中整体执行；跨源联接会落地完整副本 |
| **物化 → 联邦** | 否 | 否 | REST/OpenAPI、远程 GraphQL、gRPC、Neo4j Cypher、SPARQL、WebSocket、RSS、CSV、SQLite、Parquet、Ingest（推送接收器）、GovData、SharePoint、Salesforce、ServiceNow、云资源清单、Splunk、Qdrant、Azure Cosmos DB、Logs、Google Workspace、Microsoft 365 |

**直连型**数据源通过其原生驱动执行单数据源查询（亚 100 毫秒），绕过联邦引擎（REQ-027、REQ-229）。它们保留完整的连接器支持，并在与其他数据源联接时参与联邦（REQ-028）。

**仅联邦型**数据源始终通过联邦层查询。不存在直连驱动（REQ-229）。

**直读型（副本）**数据源拥有一个 DirectDriver，可原生读取仓库数据（在可用的情况下采用 Arrow 原生方式），将副本落地到活动引擎的物化存储中，随后查询针对该副本执行。参见[作为命名数据源的仓库](#warehouses-as-named-sources)。

**直连（无跨源连接器）**数据源拥有一个 DirectDriver，它在数据源内整体执行单数据源语句 — 所有过滤条件、列和限制都会下推。当语句将该数据源与任何其他数据源联接时，驱动会读取数据并落地一份副本；联接在活动引擎中针对该副本执行。这些数据源不存在 Trino 连接器。

**物化**数据源没有联邦连接器。Provisa 获取其数据（在启动时或查询时），并将其缓存为 S3 或 PostgreSQL 中的 Parquet，使联邦引擎可以进行跨数据源查询（REQ-309）。

---

## 所有数据源 {: #all-sources }

Provisa 注册了 **73** 种数据源类型，即 Sources 表单所提供的类型。下表覆盖全部 73 种；索引即为计数。[tool-verified: `provisa-ui/src/pages/sources/constants.ts` `SOURCE_TYPES`，其中 `graphql` 和 `grpc` 即 `graphql_remote` 和 `grpc_remote`；Kaggle、Wikipedia、GitHub、GitLab 和 Stripe 虽然内部通过 `files`、`graphql_remote` 和 `openapi` 连接器注册，但仍作为独立数据源计数]

| # | 分组 | 数据源类型 |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`、`mysql`、`mariadb`、`singlestore`、`sqlserver`、`oracle`、`duckdb`、`cockroachdb`、`yugabytedb`、`greenplum`、`tidb`、`firebird`、`airport`、`saphana`、`hiveserver2` |
| 16–24 | [云数据仓库](#cloud-data-warehouses) | `snowflake`、`bigquery`、`databricks`、`redshift`, `athena`、`fabric`、`synapse`、`trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analytics / OLAP](#analytics-olap) | `clickhouse`、`druid`、`exasol`、`elasticsearch`、`pinot` |
| 30–33 | [数据湖 / 开放表格式](#data-lake-open-table-formats) | `iceberg`、`delta_lake`、`hive`、`hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`、`cosmosdb`、`qdrant`、`cassandra`、`redis` |
| 39–40 | [代码源](arch/code-sources.md) | `node`, `python` |
| 41–43 | [流式处理](#streaming) | `kafka`、`websocket`、`rss` |
| 44 | [推送接收器](#push-receiver) | `ingest` |
| 45–46 | [图与语义](#graph-semantic) | `neo4j`、`sparql` |
| 47–51 | [基于文件](#file-based) | `sqlite`、`csv`、`parquet`、`files`、`logs` |
| 52–53 | [可观测性及其他](#observability-other) | `google_sheets`、`prometheus` |
| 54–61 | [企业级 SaaS](#enterprise-saas-connectors) | `sharepoint`、`salesforce`、`servicenow`、`jira`、`cloudops`、`splunk`、`google_workspace`, `microsoft_365` |
| 62–67 | [API 数据源](#api-sources) | `openapi`、`graphql_remote`、`grpc_remote`、GitHub、GitLab、Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [数据质量检查器](#data-quality-checkers-req-1443) | `soda`、`great_expectations` |
| 71 | [数据剖析器](#data-profiler) | `data_profiler` |
| 72 | [Kaggle 数据集](#kaggle-datasets) | Kaggle（通过 Sources 表单暂存；注册为 `files` 数据源 — 参见 [Kaggle 数据集](#kaggle-datasets)） |
| 73 | [Wikipedia](#wikipedia) | Wikipedia（注册为抓取所指定页面的 `files` 数据源 — 参见 [Wikipedia](#wikipedia)） |

Provisa 支持的每种数据源类型的参考。"Direct driver"（直连驱动）表示单数据源查询直接针对该数据源原生执行（亚 100 毫秒）（REQ-027）。"Connector Name"（连接器名称）是当该数据源参与多数据源 JOIN 时所使用的联邦连接器（REQ-028）。[tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`；`provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS {: #rdbms }

| Source Type | Direct Driver | Connector Name | Dialect | Mutations |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Yes |
| `mysql` | aiomysql | mysql | mysql | Yes |
| `mariadb` | aiomysql | mariadb | mysql | Yes |
| `singlestore` | — | singlestore | singlestore | Federated |
| `sqlserver` | aioodbc | sqlserver | tsql | Yes |
| `oracle` | oracledb | oracle | oracle | Yes |
| `duckdb` | duckdb | memory | duckdb | Yes |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | Yes |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | Yes |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | Yes |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | Yes |
| `firebird` | — | — (DuckDB extension) | — | No |
| `airport` | — | — (DuckDB extension) | — | No |
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | No |
| `hiveserver2` | impyla (HiveServer2 over Thrift) | — | hive | No |

线协议兼容的数据库复用基础线协议的 JDBC 驱动、原生异步驱动和方言 — CockroachDB、YugabyteDB 和 Greenplum 复用 PostgreSQL 线协议；TiDB 复用 MySQL 线协议。它们只需要注册表条目，无需新的连接器代码。[tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`、`_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird`（Firebird 3/4/5）和 `airport`（Arrow Flight 服务器）是已注册的数据源类型，当 DuckDB 为活动引擎时，通过 DuckDB 社区扩展就地访问 — 没有直连驱动，也没有联邦连接器。[tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

#### `saphana`

SAP HANA 通过其自有驱动读取，并复制到活动引擎的存储中，因此在每个引擎上都可访问。表单需要主机、端口（默认 39015）、用户名、密码和数据库；在多租户系统中，数据库指定租户。Register Table 列出用户可读取的 schema，不包括 HANA 的系统 schema。[tool-verified: `provisa/executor/drivers/registry.py` `_SQLALCHEMY_FALLBACK`；`provisa/api/admin/introspect.py` `_SAPHANA_SYSTEM_SCHEMAS`] (REQ-1753)

#### `hiveserver2`

通过 Thrift 直接读取的 HiveServer2 端点，适用于只有该端点可达的 Hive 部署。它不同于 `hive` 和 `hive_s3`：后两者读取仓库的文件和 metastore，从不经过 HiveServer2。数据行通过该端点读取，并复制到活动引擎的存储中。表单需要主机、端口（默认 10000）、用户名、密码和数据库；`federation_hints.auth_mechanism` 选择身份验证方式（默认 `PLAIN`，例如 Kerberos 使用 `GSSAPI`）。[tool-verified: `provisa/executor/drivers/hive.py` `HiveDriver`；`provisa/core/models.py` `SourceType.hiveserver2`] (REQ-1731)

### Cloud Data Warehouses {: #cloud-data-warehouses }

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Source Type | Direct Driver | Connector Name | Dialect | Mutations | Notes |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federated | 通过 snowflake-connector-python 读取；落地副本；`federation_hints` 中的 `account`/`warehouse`/`role`（REQ-988） |
| `bigquery` | — | bigquery | bigquery | Federated | 没有 DirectDriver；通过联邦引擎或 BigQuery 引擎 ATTACH 访问 |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federated | 通过 databricks-sql-connector 读取（Cloud Fetch、Arrow）；落地副本；`federation_hints` 中必须提供 `http_path`（REQ-987） |
| `redshift` | — | redshift | redshift | Federated | — |
| `athena` | AthenaDriver | — | athena | 部分（INSERT 始终可用；UPDATE/DELETE/MERGE 仅限 Iceberg 表） | 无服务器 S3 查询；单数据源语句完整在 Athena 中执行；跨源联接落地副本；`federation_hints` 中必填 `region`（REQ-1981） |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federated | Microsoft Fabric Warehouse；基于 TDS 的 T-SQL，Azure AD 身份验证；落地副本（REQ-995） |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federated | Azure Synapse SQL；基于 TDS 的 T-SQL，Azure AD 身份验证；落地副本（REQ-995） |
| `trino` | SQLAlchemyDriver | — | — | Federated | 通过 SQLAlchemy trino 方言读取远程 Trino/Presto 协调器；在任意引擎上落地副本（REQ-994） |

#### `athena`

Amazon Athena 是一项针对 S3 数据的无服务器查询服务，按扫描字节计费。仅涉及 Athena 表的语句直接路由到 Athena 执行：过滤条件、列列表和 `LIMIT` 全部下推，因此只有 Athena 满足该语句所需的字节才会被扫描和计费。将 Athena 表与其他源联接的语句会先从 Athena 落地完整副本——读取整张表并计费——然后联接针对该副本执行。源的 Replicate 和负载保护设置控制落地频率；文档中的说明是唯一的规则。（REQ-1981）

**发现。** Register Table 调用 Athena 的元数据 API——`ListDatabases`、`ListTableMetadata`、`GetTableMetadata`。这些调用不执行任何查询，因此列出数据库和表以及读取表的列不产生任何字节费用。仅在选择某张表时才读取其列。分区键跟在普通列之后。类型通过 `provisa/core/ir_types.py` 中的 `athena` 条目映射：`array<…>`、`map<…>` 和 `struct<…>` 映射为 `text`；未知类型被拒绝，并指明其名称。[tool-verified: `provisa/federation/athena.py` `databases`、`tables`、`columns`；`provisa/core/ir_types.py` `"athena"` 条目]

**源字段。**

| 字段 | 位置 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `database` | 源 | 是 | — | 默认查询的 Glue 数据库 |
| `username` | 源 | 否 | — | AWS 访问密钥 ID；可为 `${secret:…}` 或 `${env:…}` |
| `password` | 源 | 否 | — | AWS 访问密钥 Secret；可为 `${secret:…}` 或 `${env:…}` |
| `region` | `federation_hints` | 是 | — | AWS 区域，例如 `us-east-1` |
| `workgroup` | `federation_hints` | 否 | `primary` | Athena 工作组 |
| `s3_staging_dir` | `federation_hints` | 否 | 工作组默认值 | Athena 写入查询结果的位置；工作组未设置时必填 |
| `catalog` | `federation_hints` | 否 | `AwsDataCatalog` | Athena 数据目录 |
| `role_arn` | `federation_hints` | 否 | — | 每次调用时承担的 IAM 角色 ARN |

`username` 和 `password` 均为空时，使用 boto3 的凭证链：实例角色、环境变量（`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`）或命名 profile。只填其中一个将被拒绝。[tool-verified: `provisa/executor/drivers/athena.py` `AthenaDriver.connect`；`provisa/federation/athena.py` `athena_client`]

**写入。** 每张注册表均接受 `INSERT`。`UPDATE`、`DELETE` 和 `MERGE` 仅在 Iceberg 表上可用；格式在注册时记录（`registered_tables.table_format`）。对 Hive 表请求 `UPDATE`、`DELETE` 或 `MERGE` 的写入将被拒绝，并指明表名和操作。Athena 不返回已写入的行——`RETURNING` 子句被拒绝，GraphQL 不为这些表提供 mutation 字段；`INSERT`、`UPDATE`、`DELETE` 和 `COPY` 是写入路径。`INSERT … ON CONFLICT`、`UPDATE … FROM`、`DELETE … USING` 和 `TRUNCATE` 被拒绝，并指明形式。[tool-verified: `provisa/executor/write_capability.py` `table_write_ops`；`provisa/executor/writable.py` `_TYPE_RETURNS_NO_ROWS`、`_TYPE_REFUSED_FORMS`]

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  username: ${env:ATHENA_ACCESS_KEY_ID}
  password: ${env:ATHENA_SECRET_ACCESS_KEY}
  federation_hints:
    region: us-east-1
    workgroup: primary
    s3_staging_dir: s3://my-bucket/athena-results/
```

每次调用时承担角色（基础会话使用凭证链）：

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  federation_hints:
    region: us-east-1
    role_arn: arn:aws:iam::123456789012:role/AthenaQueryRole
    s3_staging_dir: s3://my-bucket/athena-results/
```

### MotherDuck

`motherduck`（REQ-1998）是托管在 MotherDuck 上的 DuckDB 数据库。在 DuckDB 引擎上它被就地附加；在 Postgres 引擎上它仍是 `pg_duckdb` 外部数据包装器（foreign data wrapper）连接器（REQ-1868）。 [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBMotherDuckConnector`, `PgDuckdbMotherDuckConnector`]

**连接。** `database` 指定 MotherDuck 数据库，或按共享地址指定一个共享。`password` 是 MotherDuck 令牌，与任何数据源密钥一样解析。 [tool-verified: `DuckDBMotherDuckConnector.details`]

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

DuckDB 引擎执行 `ATTACH 'md:<database>'`；对于共享，`database` 是共享地址（`_share/...`），因此附加的是 `md:_share/...`。 [tool-verified: `details` builds `"md:" + database`; `tests/integration/test_motherduck_duckdb_e2e.py` attaches MotherDuck's public sample-data share this way]

- **令牌处理。** 引擎以 `SET GLOBAL motherduck_token = ?` 登录，令牌作为绑定值传入。它不出现在任何语句的文本中，因此任何日志、追踪或错误都不包含它。 [tool-verified: `provisa/federation/duckdb_runtime.py` `_sign_in_motherduck`]
- **无令牌。** 没有 `password` 的数据源在附加任何内容之前即被按名拒绝(`a MotherDuck source signs in with its token (the password field); none is set`)。没有这项检查，扩展会启动交互式登录并等待约一分钟。没有 `database` 的数据源同样会被拒绝。 [tool-verified: `details`]
- **每个引擎一个账户。** 一个引擎登录一个 MotherDuck 账户。令牌不同的第二个 MotherDuck 数据源会被按名拒绝："one DuckDB engine reads one MotherDuck account"。 [tool-verified: `_sign_in_motherduck` compares a SHA-256 of the token]
- **写入**经由引擎进行，与 Postgres 引擎相同。 [tool-verified: `DuckDBMotherDuckConnector.capability` `write=True`]

**混合（双重）执行**无需任何设置。在 DuckDB 引擎上，所有数据源都附加在同一个连接上，因此将 DuckDB 文件数据源的表与 MotherDuck 表联接的语句作为一条 DuckDB 语句执行，由 MotherDuck 决定每个部分在哪里执行。 [tool-verified: `DuckDBMotherDuckConnector` docstring; REQ-1998 notes record a live join showing dual-execution operators]

> **数据移动。** 在双重执行下，MotherDuck 可能会把本地行移到它那一侧，用于它执行的部分。因此这样的语句可能把本地文件中的数据发送到组织的 MotherDuck 账户。除非可以接受把数据发送到该账户，否则不要将存有必须留在节点上的数据的文件与 MotherDuck 数据源联接。 [tool-verified: `DuckDBMotherDuckConnector` docstring: "which may move local rows to its side"]

MotherDuck 数据源只由 DuckDB 和 Postgres 引擎提供服务。其他引擎都没有它的连接器，也没有任何机制把它的表作为副本落地到那里。 [tool-verified: `provisa/federation/engine.py` registers `DuckDBMotherDuckConnector` and `PgDuckdbMotherDuckConnector` and no other MotherDuck connector]

### Analytics / OLAP {: #analytics-olap }

[tool-verified: `executor/drivers/clickhouse.py`]

| Source Type | Direct Driver | Connector Name | Dialect | Mutations | Notes |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federated | 通过 clickhouse-connect（HTTP）读取；启用 TLS 时在 `federation_hints` 中设置 `secure: "true"`（REQ-986） |
| `druid` | — | druid | druid | No | — |
| `exasol` | — | exasol | exasol | No | — |
| `elasticsearch` | HTTP (native engines) | elasticsearch (Trino) | — | No | 在 Trino 上，连接器读取该数据源，其属性来自该类型的映射 DSL [tool-verified: `trino_connectors.py:309`]；在其他所有引擎上，Provisa 通过 HTTP 读取索引（Register Table 使用索引和映射，落地时使用滚动读取）并落地这些行 [tool-verified: `provisa/elasticsearch/fetch.py`, `provisa/events/source_loader.py` `make_elasticsearch_loader`] (REQ-1672) |
| `pinot` | — | pinot | — | No | Trino `pinot` 连接器；`pinot.controller-urls` = Pinot 控制器的 host:port [tool-verified: `trino_connectors.py:199`] |

### Data Lake / Open Table Formats {: #data-lake-open-table-formats }

这些数据源类型仅支持联邦，没有直连驱动，也没有方言。[tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Source Type | Connector Name | Time Travel | Notes |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Yes (`as_of` argument, REQ-372) | — |
| `delta_lake` | delta_lake | Yes (`as_of` argument, REQ-372) | — |
| `hive` | hive | No | — |
| `hudi` | — (ClickHouse `Hudi` engine, zero-copy — REQ-1178) | No | 没有联邦连接器；当 ClickHouse 为活动引擎时就地访问 |
| `hive_s3` | hive | No | S3 支持的 Hive |

### NoSQL {: #nosql }

`mongodb`、`cassandra` 和 `redis` 都拥有 Trino 连接器（`redis` 的属性由该类型的映射 DSL 构建）。[tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Source Type | Connector Name | Mutations |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | No |
| `cassandra` | cassandra（Trino）；在其他所有引擎上通过 cassandra-driver 进行 CQL 读取 | No | Keyspace 即为 schema；Register Table 列出某个 keyspace 的表，并根据集群的 schema 元数据为列定型（分区键作为主键）；`cassandra` extra 安装该驱动 [tool-verified: `provisa/cassandra/fetch.py`] (REQ-1676) |
| `redis` | redis（Trino）；在其他所有引擎上通过 redis-py 进行无 HTTP 读取 | No | 键前缀 `<table>:*` 即为一张表，一个 hash 即为一行；Register Table 列出存在的前缀，并根据前缀的 hash 为其列定型（`mapping.tables` 条目可覆盖模式、键列、值类型和列） [tool-verified: `provisa/redis/fetch.py`] (REQ-1675) |

**托管引擎上的 Redis。** 当组织的引擎是托管平台预置的引擎之一时，Redis 不可用。Trino 从目录自身目录中的描述文件读取 Redis 目录的表，而托管引擎在运行期间无法被赋予这样的目录。在那里保存或应用 Redis 数据源会被按名称拒绝（HTTP 422，`sources.redis_needs_engine_files`）；自托管部署按上文所述读取 Redis。[tool-verified: `provisa/core/repositories/source.py` `refuse_on_engine_shard`] (REQ-2073)

### Qdrant

`qdrant`（REQ-1983、REQ-2000）由 Qdrant 自带的客户端 `qdrant-client` 读写。Qdrant **仅限实时**：这是源类型的属性，不是设置。它用自己的向量索引和 Query API 作答，任何副本都无法给出同样的答案，因此其表永远不会被复制。Qdrant 源及其表没有 Replicate 选项；对其设置 `replicate` 值或 `row_materialize` 会被拒绝。[tool-verified: `provisa/core/source_registry.py` `LIVE_ONLY_TYPES`；`provisa/api/admin/_landing_ttl.py` `_live_only_refusal`]

**连接。** `host` 是实例的 URL 及端口（`https://<cluster>.cloud.qdrant.io:6333`，或自托管的 `http://qdrant:6333`）；`password` 是其 API 密钥，是在读取该源时解析的密钥引用，无密钥的实例则留空。`federation_hints.prefer_grpc: true` 改为通过 Qdrant 的 gRPC 端口读取，而非 REST。该源只有一个 schema，即 `default`。[tool-verified: `provisa/qdrant/fetch.py` `QdrantConnection.from_source`；`provisa/qdrant/introspect.py` `SCHEMA`]

**集合表。** 集合（collection）就是一张表。Register Table 提供集合的列：`id`（点 ID，即其键，路径 `@id`）、payload 的字段（已建索引的字段按其索引定型，其他字段按抽样点的提示定型；嵌套字段连同路径展平），以及每个向量一个 `json` 列（路径 `@vector`，命名向量则为 `@vector.<name>`），默认不选中。向量列会指明它所存嵌入所属的已注册嵌入模型，从维度等于该向量大小的模型中选择（在 Register Table 中，以及编辑器的“类型与键”标签页）；选择模型即注册该向量。[tool-verified: `provisa/qdrant/introspect.py` `columns`；`provisa-ui/src/pages/tables/QdrantVectorColumns.tsx`]

**针对单个 Qdrant 源的语句。** 只读取一个集合的语句，在 Qdrant 的 API 能够精确表达它时于 Qdrant 中运行：

- payload 条件转为 Qdrant 过滤器（`=`、`<>`、`IN`、数字和日期的范围、`IS NULL`、`AND`、`OR`、`NOT`）；保持 SQL 的 NULL 语义，因此 `lang <> 'en'` 会跳过没有 `lang` 的点；
- 仅对 `id` 的条件（`=` 或 `IN`）是一次 `retrieve`；其他任何读取是带过滤的 `scroll`；
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` 是对距离为 Cosine 的向量的一次 `query_points`，过滤器在取 top-k 之前应用。文本查询由该向量的模型嵌入。答案带有警告：top-k 是近似的，通过 Qdrant 的索引选出。

其他情况（聚合、`GROUP BY`、计算列或脱敏列、非“最相似优先”的排序）由引擎作答，见下文。[tool-verified: `provisa/qdrant/direct.py` `analyse`、`condition`、`QdrantDriver`]

**由引擎作答的语句。** 当语句把集合与另一个源联接，或对其列做计算时，该集合的那一部分会在该语句执行时从 Qdrant 实时读取，并以行的形式交给引擎。这一部分包括集合的列、语句中仅针对该集合的条件（其行规则也在内），以及在集合于其 `SELECT` 中单独读取时，该 `SELECT` 的相似度排序和 `LIMIT`。Qdrant 无法精确表达的部分会被拒绝，并指明表及原因。没有条件、没有键、没有 top-k 的部分会读取整个集合，同样被拒绝。针对参数（`$1`）的条件留给引擎处理。[tool-verified: `provisa/federation/live_only.py` `cut`、`read`；`provisa/pgwire/_pipeline.py` `_read_live_parts`]

**写入。** 对集合的 `INSERT`、`UPDATE` 和 `DELETE` 在 Qdrant 中运行：

- `INSERT … VALUES` 指明 `id` 列并写入新点。集合中已有的 `id` 会按名称被拒绝。该检查先读取 id，因此与写入不是原子的。
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` 是 upsert。语句未设置的列保持原值。
- `UPDATE` 和 `DELETE` 作用于其 `WHERE` 选出的点，以 Qdrant 过滤器的形式；没有过滤器形式的条件会被拒绝。
- 写入向量列的文本由该列的模型嵌入。写入其中的向量必须具有该模型的维度。

[tool-verified: `provisa/qdrant/direct.py` `_insert`、`_upsert_clause`、`_write_points`、`_update`、`_delete`、`_vector_value`]

**搜索表。** 向量搜索是一张搜索表：其 `query_template` 保存一个 Query API 请求。

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` 始终是参数；`using`、`filter`、`limit`、`score_threshold` 和 `model` 为可选。`using`、`filter`、`limit` 或 `score_threshold` 中恰好为 `"@name"` 的值同样是参数。
- 每个参数都是该表的参数（`query_arg`，REQ-1986），被严格转换为其声明的类型，并作为 JSON 值放入请求，绝不放入其文本。`query` 的参数是 `array(double)`，且必填。
- 调用方像传递任何参数一样传递它们：SQL 中为 `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'`，GraphQL 中为字段参数。
- 未给出的可选参数不做任何过滤：其条件从过滤器中省略。没有 `limit` 时，搜索返回 Qdrant 的默认值，即 10 个点。
- 过滤器由 Qdrant 在选出最近点之前应用，因此是精确的。语句中的其他条件之后才缩小搜索返回的点；不会取回更多。
- `score` 是集合自身针对所搜索向量的度量：对 Cosine 集合而言就是余弦相似度。Register Table 列出集合的向量及其大小和距离。
- `model` 指定一个已注册的嵌入模型（`vector_models`，管理 → AI Models）。此时给定的查询文本由该模型嵌入，给定的向量必须具有该模型的维度（REQ-429、REQ-430）。没有 `model` 时，查询必须是向量。

搜索按每条语句、在其参数下读取，其行在这些参数下保留一个表 TTL 的时间。搜索表是只读的。[tool-verified: `provisa/qdrant/table.py` `check_table`、`plan`、`read`；`provisa/api/data/materialization.py` `_is_native_read`]

### Streaming {: #streaming }

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | 联邦 Kafka 连接器；通过 Confluent Schema Registry（Avro、Protobuf、JSON Schema）、手动定义或样本推断获取 schema（REQ-147, REQ-150） | 否；表的实时输出发布到 Kafka（REQ-565） |
| `websocket` | 外部 WebSocket 数据流 — 连接、订阅、接收事件；结果会被物化（REQ-338） | No |
| `rss` | RSS 2.0 / Atom 订阅源 — 轮询、按 pubDate/updated 设置水位线；结果会被物化（REQ-342, REQ-343） | No |

**托管引擎上的 Kafka。** 在托管平台预置的引擎上，Kafka 数据源必须指定一个模式注册表（`schema_registry_url`）。主题列只由模型声明的数据源在保存或应用时会被按名称拒绝（HTTP 422，`sources.kafka_needs_schema_registry`），因为它的表描述需要一个无法交给运行中引擎的目录。SASL 凭据以内存方式送达这些引擎（参见[安全](security.md#the-federation-engine-trino)）。[tool-verified: `provisa/core/repositories/kafka_source.py` `refuse_on_engine_shard`] (REQ-2073)

### Push Receiver {: #push-receiver }

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `ingest` | 外部服务通过 POST 提交 JSON 事件；结果会被物化（REQ-331, REQ-335） | No |

### Graph & Semantic {: #graph-semantic }

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `neo4j` | 通过 HTTP API 执行 Cypher，结果缓存在 PostgreSQL 中（REQ-295） | No |
| `sparql` | 通过 SPARQL 1.1 POST 请求，结果缓存在 PostgreSQL 中（REQ-297） | No |

### File-Based {: #file-based }

有三种机制覆盖文件类数据源。单文件数据源和文件爬取器使用 `path` 字段而非 `host`/`port`。[tool-verified: `provisa/core/models.py`] (REQ-553) 第三种 [Logs](#logs) 在其 `mapping` 中用通配符（glob）指定文件，并且只读取 Provisa 服务器上的文件。[tool-verified: `provisa/logs/settings.py`]

**文件数据库与单文件数据源** — `sqlite`、`duckdb`、`csv`、`parquet` 将 `path` 指向单个文件。（`duckdb` 数据源把位置保存在表单的 `database` 字段中。） [tool-verified: `provisa-ui/src/pages/sources/FileLocationFields.tsx`]

| Source Type | 位置 | Mutations |
| --- | --- | --- |
| `sqlite` | 本地路径、`https://`, `s3://`, `gs://`, `az://` | 本地文件上为是；远程文件上无 |
| `duckdb` | 本地路径、`https://`, `s3://`, `gs://`, `az://` | 本地文件上为是；远程文件上无 |
| `csv` | 本地路径、`https://`, `s3://`, `gs://`, `az://` （DuckDB 引擎） | 否 |
| `parquet` | 本地路径、`https://`, `s3://`, `gs://`, `az://` （DuckDB 引擎） | 否 |

[tool-verified: `provisa/federation/connector_duckdb.py` `location_kind`, `file_location_details`; `provisa/executor/write_capability.py` `table_write_ops`. The table describes the DuckDB engine, where the connectors above take every location. On other engines, CSV and Parquet over `s3://` or `http(s)://` are better registered through the `files` source. [inferred]] (REQ-1997)

使用其他任何协议（例如 `ftp://`）的路径会被拒绝："DuckDB reads no file at a `ftp://` location"。 [tool-verified: `location_kind` raises `ValueError`]

**远程位置的凭据。** 将其放在数据源的 `federation_hints` 中。每项凭据只作用于该路径自身的存储桶或容器，因此一个数据源的凭据永远不会用于另一个数据源的位置。未提供凭据的数据源按公共位置读取。 [tool-verified: `object_store_secret_ddl`]

| 位置 | `federation_hints` 键 |
| --- | --- |
| `s3://` | `access_key_id`、`secret_access_key`；可选 `region`、`endpoint`（R2 或 MinIO 等 S3 兼容存储） |
| `gs://` | `gcs_access_id`、`gcs_secret_key`（HMAC 密钥） |
| `az://` | `azure_account_name`, `azure_account_key` |

**远程 SQLite 和 DuckDB 文件为只读。** DuckDB 引擎以 `READ_ONLY` 就地附加它们，基于其上的表不提供 insert、update 或 delete。在不附加 SQLite 的引擎上，远程 SQLite 文件通过一个以只读方式附加它的临时 DuckDB 连接读取，其行照常落地。 [tool-verified: `_attach_options`; `table_write_ops`; `provisa/events/source_loader.py` `make_sqlite_loader` `_remote`]

**上传文件。** 在 `sqlite`、`duckdb`、`csv` 或 `parquet` 数据源的表单中，上传文件而不是输入路径。文件以流式写入磁盘，从不保存在内存中，存放到组织文件夹下的 `files/<source id>/<file name>`，数据源的路径随即指向它。 [tool-verified: `provisa/core/source_uploads.py` `receive`, `upload_dir`; `provisa/api/admin/source_files_router.py` `upload_file`]

- **替换。** 再次上传会取代先前的文件，无论其名称为何，并且数据源会重新预配，使下一次读取看到新文件。 [tool-verified: `receive` removes other files in the directory; `upload_file` calls `reprovision_source_after_registration`]
- **删除。** 删除数据源会删除其上传的文件。其他操作都不会删除它。 [tool-verified: `provisa/api/admin/schema_mutation.py` `source_uploads.remove(id)`]
- **权限。** 只有具备 `source_registration` 能力的角色才能上传。 [tool-verified: `upload_file` `require_capability_request(request, "source_registration")`]
- **大小限制。** 设置 `sources.max_upload_bytes`（环境变量 `PROVISA_SOURCES_MAX_UPLOAD_BYTES`）限制文件大小，默认 1 GiB（1073741824 字节），最小为 1，在上传开始时读取。更大的文件会以 HTTP 413 被拒绝并注明限制，且不保留任何部分。 [tool-verified: `provisa/core/settings_catalog.py`; `UploadTooLarge`]
- **路径。** 当路径为该数据源的上传文件时，表单的路径字段为只读。 [tool-verified: `FileLocationFields.tsx` `readOnly={uploaded}`]
- **名称。** 数据源 id 和文件名只能使用字母、数字、`.`、`_` 和 `-`。 [tool-verified: `source_uploads._SEGMENT`]
- **存储。** [inferred from the `source_uploads` docstring] 上传的文件与任何组织文件一样存储：位于组织文件夹下，组织文件夹加密时随之加密，并在共享挂载上对每个节点可见。

**写入本地 SQLite 或 DuckDB 文件，以及锁。** 写入会获取文件锁，且从不等待。当另一个进程或节点持有该文件时，写入会被拒绝，并注明数据源及其文件已被锁定。 [tool-verified: `provisa/federation/file_locks.py` `FileLocked`; REQ-1997 notes quote the maintainer: "writes back to file databases in a multi-node env - just error them out when they can't a lock on it."]

- **SQLite。** 写入设置 `PRAGMA busy_timeout = 0`，因此被锁定的文件会立即失败。 [tool-verified: `provisa/executor/drivers/sqlalchemy_driver.py` `_write_sqlite`]
- **DuckDB。** DuckDB 只在只读打开之间共享文件。因此引擎的附加和数据源自身的连接都以只读方式持有文件，每个节点可同时读取。写入会暂停本进程的持有者，仅为该语句以读写方式打开文件，然后关闭。如果打开时无法获取锁，写入会被拒绝。 [tool-verified: `provisa/executor/drivers/duckdb_driver.py` `_write`: `with file_locks.exclusive(self._database):` then `duckdb.connect(self._database)`; `connector_duckdb._attach_options`]

**共享挂载。** 在挂载由多个节点共享的加密组织文件夹中（设置 `org_files.shared_mount`），对其中 SQLite 或 DuckDB 文件的写入会直接被拒绝，并注明数据源。文件夹加密层（gocryptfs）不会把文件锁传递到其他节点，因此没有任何机制能阻止两个节点同时写入。读取仍然可用。 [tool-verified: `file_locks.refuse_unprotected`, `UnprotectedWrite`; both drivers call it before a write]

未加密共享的文件夹必须以能到达每个节点的锁进行挂载：启用锁定的 NFS 挂载，而不是 `nolock`，也不是 `local_lock`。没有任何探测检查这一点；这是部署要求。 [tool-verified: REQ-1997 description in `docs/arch/requirements.yaml`; no check exists in source [inferred]]

**文件爬取器（`files` 数据源）** — 将 `path` 指向一个通配符（glob），递归遍历该路径，并将该目录注册为一个联邦目录（catalog）中的表集合。它支持多种格式和多种传输方式；下方的集合来自文件连接器（kenstott/calcite fork）。[tool-verified: `provisa/core/catalog.py` `files` 分支及 `provisa/core/models.py` `SOURCE_TO_CONNECTOR`；格式和传输方式列表来自 calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formats | Transports |
| --- | --- |
| CSV、TSV、JSON、YAML、Excel（XLS/XLSX）、Parquet、Arrow，以及转换为表格的文档 — HTML、Markdown、DOCX、PPTX | 本地文件系统、HTTP(S)、`s3://`、`hdfs://`、`ftp://`/`ftps://`、`sftp://`、`iceberg://`、SharePoint（REST 和 Microsoft Graph） |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

任何 `files` 数据源也可以在其 mapping 中声明 `crawl`，设置与 [Wikipedia](#wikipedia) 下列出的相同：此时数据源会先读取网页，并将每个 HTML 数据表和每个链接的数据文件作为表落地到其目录中。[tool-verified: `provisa/file_source/crawl.py` `crawl_operand`]

在 DuckDB 引擎上，`files` 以原生方式读取 — 在解析出的目录下，为每个 `<table>.csv` 提供一个 `read_csv_auto` 扫描视图（REQ-229）[tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]。在没有自带 `files` 连接器的引擎上，数据行通过 sharepoint/splunk 所使用的同一个连接器自带 Calcite pgwire 服务器（`pgwire-file`）落地（REQ-954）— 参见下方的[企业级 SaaS 连接器](#enterprise-saas-connectors)。端到端 UI 覆盖（Sources 表单 → Register Table → SQL 查询）以及 pgwire 落地路径已在 REQ-1694 中得到验证。

#### Logs (REQ-1996) {: #logs }

`logs` 数据源将 Provisa 服务器上的日志文件变成一张表。你用完整路径或通配符（glob）指定文件，并选择其记录所用的格式。每条记录成为一行。格式无法读取的记录会连同原始文本和原因一并保留，绝不丢弃。[tool-verified: `provisa/logs/__init__.py`; `provisa/logs/files.py` `rows_of`]

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

mapping 接受四个键，其他任何键都会按名称被拒绝：`glob` 和 `format`（均为必填），以及仅 `regex` 格式接受的 `pattern` 和 `record_start`。[tool-verified: `provisa/logs/settings.py` `parse`; `provisa/logparse/__init__.py` `format_for`]

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

在 Sources 表单中，Logs 数据源在 File 下显示为“Log files”。**Preview** 按钮会在保存之前显示通配符匹配的文件、格式给出的列，以及最新文件的前 20 行。[tool-verified: `provisa-ui/src/pages/sources/constants.ts`; `provisa-ui/src/pages/sources/LogsFields.tsx`; `provisa/api/admin/logs_router.py` `PREVIEW_ROWS`]

**通配符。** 它必须是完整路径；`~` 会被展开，相对路径会被拒绝（“name the files by their full path”）。匹配使用 Python 的 `glob` 非递归模式，因此 `**` 的行为与 `*` 相同，只在一级目录内匹配。只取常规文件，按名称排序。空文件在有内容之前会被跳过。[tool-verified: `provisa/logs/roots.py` `checked_glob`; `provisa/logs/files.py` `matched`, `identify`] 通配符可以引用环境变量，例如 `${env:LOG_DIR}/*.log`。数据源按原样保存该引用，并在每次读取文件时解析它；未设置的变量会被按名称拒绝。 [tool-verified: `provisa/logs/settings.py` `parse`; `tests/unit/test_logs_source.py` `test_a_glob_naming_a_variable_reads_where_it_points`]

**每一行自带的列。** 这些列排在格式给出的列之前：

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `_file_id` | text | 文件的标识：其前 4096 个字节（解压后）的校验和 |
| `_file_name` | text | 读取该记录时所用的路径 |
| `_line_offset` | bigint | 记录首行在解压后文本中的字节偏移 |
| `_entry` | bigint | 该行是记录中的第几个条目：除批量记录（例如 CloudTrail 文件的 `Records`）外均为 0 |
| `_raw` | text | 记录的原始内容 |
| `_parse_error` | text | 记录或其中某个值无法读取的原因；读取成功时为 NULL |
| `_landed_at` | timestamp | 该行落地的时间 |

`_file_id`、`_line_offset` 和 `_entry` 构成该行的键，因此读取两次的记录仍是同一行。[tool-verified: `provisa/logs/__init__.py` `META_COLUMNS`, `KEY`; `provisa/logs/preview.py` `columns`]

**格式与预设。** 格式下拉框按系列对格式分组；最后一项 `regex` 是你自己的模式。[tool-verified: `provisa/logparse/__init__.py` `BUILT_IN`, `catalog`; `provisa/logparse/presets/__init__.py` `presets`; family labels from `provisa-ui/src/i18n/locales/en/logsFields.json`]

| 系列（表单标签） | 格式名称 |
| --- | --- |
| Common formats | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| Databases and runtimes | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| Containers and Linux | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| Network security monitoring | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| Web servers and proxies | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| Your own | `regex` |

共有 56 种内置格式，外加 `regex`。[tool-verified: `BUILT_IN` enumerated with the project interpreter]

**格式如何产生列。** 格式通过四种方式之一获得列。[tool-verified: `provisa/logparse/formats.py`; `provisa/logparse/presets/_shared.py`; `provisa/logparse/coltypes.py`]

- **固定列。** 大多数格式自行声明列及类型。例如 `rfc3164` 给出 `timestamp`、`facility`、`severity`、`host`、`process`、`pid` 和 `message`；`facility` 和 `severity` 由 `<PRI>` 值计算得出。BSD syslog 行不带年份，因此年份取文件最后写入的年份；若该行的月份晚于最后写入的月份，则取前一年。
- **由记录命名的字段。** `jsonl` 和 `logfmt` 的列取自文件开头若干行中出现的字段（设置项 `logs.preview_lines`，默认 1000）。每列取所见全部值都适用的最窄类型：先 boolean，然后依次为 integer、float、timestamp 和 json，否则为 text。后续记录携带但没有对应列的字段会进入一个 `extra` json 列。
- **由表头命名的字段。** `w3c` 及基于它的预设读取每个文件的 `#Fields:` 行；`date` 和 `time` 合并为一个 `timestamp` 列。`aws_cloudfront`、`ms_iis`、`ms_windows_firewall`、`ms_exchange_tracking` 和 `zeek_tsv` 以同样方式读取 `#` 开头的表头行，`aws_vpc_flow` 则读取以 `version` 开头的表头行。读到表头行的位置会随文件保存，因此之后继续读取的文件保留其字段。
- **`regex`。** 每个命名组（例如 `(?P<level>…)`）是一个 text 类型的列。

Register Table 界面会列出这些列及其类型，并标出键。[tool-verified: `provisa/api/admin/schema_query.py` `_logs_columns`] 读取行时，每个值都会转换为其列在已注册表中的类型，因此在那里更改的类型就是实际应用的类型。列无法容纳的值变为 NULL，原因写入 `_parse_error`。[tool-verified: `provisa/logs/loader.py` `_load`; `provisa/logs/files.py` `_typed`] [inferred: that the screen lets a steward change a suggested type, as REQ-1996 states]

长度超过 `logs.max_line_bytes`（默认 65536）的记录，其开头字节保留在 `_raw` 中，且不会被解析；`_parse_error` 给出其长度。以 UTF-8 或 UTF-16 字节顺序标记开头的文件按该编码读取，其他文件按 UTF-8 读取。[tool-verified: `provisa/core/settings_catalog.py`; `provisa/logs/files.py` `rows_of`, `text_encoding`]

**跨行记录。** 带有记录起始模式的格式，用匹配该模式的一行加上其后所有不匹配的行组成一条记录。堆栈跟踪会与其异常保持在一起，慢查询条目也保持完整。第一个匹配之前的行自成一条记录；格式通常会拒绝它，但该行会被保留。带记录起始的内置格式有 `rfc3164`、`macos_install`、`aws_cloudtrail`、`postgresql_stderr`、`postgresql_csvlog`、`mysql_error`、`mysql_slow`、`log4j2_default`、`logback_default`、`python_logging`、`chrome_net_export`、`ms_sqlserver_errorlog` 和 `azure_monitor`。使用 `regex` 时，请自行设置 `record_start`。[tool-verified: `provisa/logparse/framing.py` `records`; `record_start` of each format enumerated with the project interpreter]

有两种格式自行读取记录，而不是按行读取。`kubernetes_cri` 会把运行时拆分的部分（`P`）行合并为一条记录，`windows_evtx` 读取二进制 `.evtx` 文件。[tool-verified: `provisa/logparse/presets/linux.py` `KubernetesCri`; `frames_bytes` enumerated with the project interpreter]

仍在写入的文件只读到其最后一个完整的行。对于多行格式，则读到其最后一条记录的起点，因为下一行可能延续该记录。文件 2 秒没有写入后，最后一条记录即视为完整。[tool-verified: `provisa/logs/files.py` `QUIET_SECONDS`, `read_file`]

**轮转。** 文件由其前 4096 个字节（解压后）的校验和识别，而不是由名称识别。当 `system.log` 轮转为 `system.log.0`，再压缩为 `system.log.0.gz` 时，所有副本是同一个文件。读取从上次读到的字节继续，因此没有任何一行会被读两次。[tool-verified: `provisa/logs/files.py` module docstring, `identify`; `tests/unit/test_logs_source.py` `test_a_rotated_and_compressed_file_is_read_on_and_no_line_twice`]

- 压缩根据文件的前几个字节识别，绝不看名称：gzip 和 bzip2。压缩文件视为已完整。
- 轮转副本及其压缩孪生文件若都被匹配，则只读一次，取自压缩的那个。
- 发现文件比已读偏移更短，说明它被截断了：它是新文件，从头读取。旧文件的行保留。
- 短于 4096 字节且开头相同的文件通过 inode 区分，因此仍是两个文件。
- 通配符不再匹配的文件会被遗忘；其行作为历史保留。

[tool-verified: `provisa/logs/files.py` `compression`, `Found.complete`, `identify`; `provisa/logs/loader.py` module docstring; `tests/unit/test_logs_source.py`]

**刷新与实时投递。** Logs 表是一份副本。没有任何东西监视这些文件。当数据源的变更信号为 `probe` 或 `ttl_probe` 时，其探测会对每个匹配文件的路径、inode、大小和最后写入时间做摘要；摘要不同时，就会请求刷新。Logs 数据源接受的探测类型是 `hash` 和 `none`。[tool-verified: `provisa/logs/loader.py` `files_token`, `_freshness_probe`; `provisa/events/probes.py` `_LOADER_PROBED`, `probe_capabilities`]

刷新只读取新增内容。每个文件从上一轮读到的位置继续读取，行按键写入，每批 5000 行，然后才保存新位置。在这两步之间中断的一轮，会重新读取那些记录，得到的仍是同样的行。每一轮都在 `_landed_at` 中为其行打上时间戳，实时投递以该列作为水位线。表定义变更会触发整体重建，从每个文件的开头重新读取。[tool-verified: `provisa/logs/loader.py` `_UnitsRead`, `_WholeRead`, `BATCH_ROWS`, `replica_stamp_column`; `provisa/federation/replica_builds.py` `_kept_in_place`] [inferred: that subscriptions deliver a Logs table's new rows by `_landed_at`, as REQ-1996 states; the subscription side was not traced]

**允许的根目录。** 操作员设置 `logs.allowed_roots` 列出通配符可以解析到的目录。默认值为 `["/var/log"]`；它是 Admin → Settings 的 Security 卡片上的一个列表，也可通过 `PROVISA_LOGS_ALLOWED_ROOTS` 设置，更改时会要求确认。每个根目录都在 `~` 展开并解析符号链接之后确定。[tool-verified: `provisa/core/settings_catalog.py` `logs.allowed_roots`; `provisa/logs/roots.py` `allowed_roots`]

适用两项检查。通配符的固定部分（到第一个通配符为止）必须解析到某个根目录之内；这在保存数据源时检查，之后每次预览或读取时也检查。每个匹配的文件在跟随其符号链接后也必须解析到某个根目录之内，因此指向 `/var/log` 之外的符号链接会被拒绝。拒绝信息会给出路径、其解析结果以及允许的根目录：

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

保存时的错误码是 `schema.logs_path_refused`；预览时是带有 `logs.path_refused` 的 403。[tool-verified: `provisa/logs/roots.py` `checked_glob`, `checked_file`; `provisa/api/admin/schema_common.py` `logs_refusal`; `provisa/api/admin/logs_router.py`; `tests/unit/test_logs_source.py` `test_a_symlink_escaping_the_root_is_refused_by_name`]

**托管（SaaS）拒绝。** 承载多个组织的部署不读取自己的任何日志文件。在保存、预览和读取时，所有路径检查都会首先拒绝，并给出“`<path>`: a server that holds many organizations reads no log file of its own”。这些文件属于服务器，而不属于任何一个组织，因此任何租户都不得读取。部署根据其多租户状态判断自己是否承载多个组织。[tool-verified: `provisa/logs/roots.py` `_refuse_on_saas`; `provisa/core/secrets_providers.py` `many_organisations`; `provisa/api/app.py` `set_many_organisations_resolver(lambda: state.multitenancy)`; `tests/unit/test_logs_source.py` `test_a_server_of_many_organizations_reads_no_log_file_of_its_own`]

#### Kaggle 数据集（REQ-1780, REQ-1781, REQ-1782, REQ-1783） {: #kaggle-datasets }

Kaggle 是一个文件下载平台。暂存的 Kaggle 数据集会注册为一个 `files` 类型的 Source，并通过任何其他 `files` 数据源所使用的同一个 pgwire-file 连接器进行查询 — 枚举中并不存在 `kaggle` 这个 SourceType。[tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — 没有 `kaggle` 字面量]

**添加数据集。** 打开 Sources → Subscriptions → Kaggle。该表单运行两个连续的步骤，因为 Kaggle 自身的数据集搜索端点需要身份验证 — 选择器必须在令牌验证通过后才能出现。[tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **令牌** — 输入一个 Kaggle API 令牌并点击"Validate"。验证会向 `POST https://www.kaggle.com/api/v1/datasets/create/new` 发送一个空请求体。`401` 表示无效；任何其他响应都表示有效（Kaggle 自身的负载验证会先于任何数据集的创建触发 — 不会持久化任何内容）。[tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **选择器** — 一个即时搜索字段查询 `GET /api/v1/datasets/list`。每个结果显示数据集标题和描述。选择一个，然后点击"Add Dataset"。

点击"Add Dataset"会从 `GET /api/v1/datasets/download/{owner}/{ref}` 下载数据包，将 CSV 和 Parquet 成员解压到 `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>`，并创建**一个** `files` 类型的 Source，其 `path` 指向该根目录。令牌不会在服务器端存储。[tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

添加数据源后，通过常规的 Register Table 界面注册其表。pgwire-file 连接器的递归目录发现机制会将每个文件列为其自身的表 — 与其他任何 `files` 数据源所使用的机制相同（REQ-1690）。（REQ-1783）

**v1 限制。** 包含 `.sqlite` 或 `.db` 文件的数据包会被直接拒绝并给出明确的错误提示。只有 CSV 和 Parquet 文件会被暂存。[tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**表命名。** 抓取到的表命名为 `<page>__<name>`：先是页面标题，然后是表的标题（caption），表没有标题时则使用表上方最近的标题，两者都采用数据源的列名拼写。例如，页面 `List_of_tallest_buildings` 上标题"Tallest buildings in the world"下的表是 `list_of_tallest_buildings__tallest_buildings_in_the_world`。链接的数据文件保留其自身名称。[tool-verified: a live crawl of that page through the 0.109.0 file bundle]

**刷新。** 要在 Kaggle 发布新版本后重新获取数据集，请调用 `refreshKaggleSource` GraphQL mutation，并提供数据源 ID 和有效令牌。这会就地重新暂存文件，并清除 pgwire-file 端点缓存，使连接器在下一次查询时能获取到任何 schema 变更。创建时存储在 `federation_hints` 中的 `kaggle_owner` 和 `kaggle_ref` 标识了要重新获取的数据集。[tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**没有静态 YAML 配置路径。** Kaggle 数据源仅能通过 Sources 表单创建。导出为 YAML 的 Kaggle 数据源会显示为 `type: files`，并在 `federation_hints` 中带有 `kaggle_owner` 和 `kaggle_ref`。要从 Kaggle 重新下载，需要使用 UI 刷新流程或 `refreshKaggleSource` mutation — 对于隔离网络（air-gapped）环境，替代方案是将 YAML 中的 `path` 指向一个预先暂存好的目录。


#### Wikipedia (REQ-1960) {: #wikipedia }

Wikipedia 会抓取你指定的页面。页面上的每个数据表，以及每个链接的 CSV、TSV、Excel、JSON 或 Parquet 文件，都会成为该数据源的一张表。与 Kaggle 一样，它注册为 mapping 中带有 `crawl` 的 `files` 数据源。添加它不会注册任何表：由数据管理员通过常规的 Register Table 界面注册所需的表。[tool-verified: `provisa/file_source/wikipedia.py` `crawl_settings`、`DATA_FILE_EXTENSIONS`；`provisa-ui/src/pages/sources/constants.ts` `wikipedia: "files"`] (REQ-316)

**设置字段。**

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| Pages to start from | 无（必填） | 每行一个：页面标题，或所选语言版本中某个页面的地址 |
| Language edition | `en` | 语言版本的代码，例如 `en`、`de` 或 `ja` |
| Link depth | `1` | 从起始页面向外读取多少层链接；`0` 只读取起始页面 |
| Page limit | `25` | 一次抓取最多读取的页面数 |
| Contact | 空 | 随每个请求发送，以便 Wikipedia 按其政策要求联系到你 |
| Follow "See also" links | 关 | 关闭时，文章中不包含该章节 |
| What this edition calls its "See also" section | 空 | 仅当上方选项关闭且 Provisa 无法为所选语言版本给出该章节名称时才会询问。填写页面上显示的章节标题，例如 `Voir aussi` |
| Directory for the crawl's files | 空 | 表和数据文件写入的位置。为空时：`<PROVISA_DATA_DIR>/crawl/<source id>` |

[tool-verified: `provisa-ui/src/pages/sources/WikipediaFields.tsx`；`provisa-ui/src/i18n/locales/en/sourceFormFieldsExtended.json` `wp*`；`provisa/file_source/wikipedia.py` `DEFAULTS`；`provisa/file_source/crawl.py` `crawl_landing_directory`]

Provisa 知道这些语言版本的"See also"标题：`en`、`de`、`es`、`it`、`pt`、`nl`、`pl`、`sv`、`ja`、`zh`、`ru`。对于任何其他语言版本，在"See also"关闭时，表单会询问该版本如何称呼这一章节；填写页面上显示的标题（例如 `Voir aussi`）。未填写就保存的数据源会被拒绝（`wikipedia.see_also_heading_needed`）。[tool-verified: `provisa/file_source/wikipedia.py` `SEE_ALSO_HEADING`、`crawl_settings`]

**跟随哪些链接。** 链接的取舍取决于页面自身标记所声明的类型，而不是其地址中的名称：这类名称在每个语言版本中各不相同，也会出现在普通标题里。[tool-verified: `provisa/file_source/wikipedia.py` module docstring]

- 跟随：位于文章正文、列表和数据表中指向其他文章的链接。标记为 `a[rel='mw:WikiLink']`。
- 不跟随：尚未撰写的页面、页面自身、图书来源查询、文件页面、其他 wiki 和外部站点。
- 完全不读取（在收集链接和表之前移除）：导航框、信息框、注释和引用、分类、目录、图片和维护提示。
- 除非开启该选项否则不读取："See also"章节。

只有编辑者标记为数据表的表（`table.wikitable`）才会成为表，行数少于 2 的表不会提供。[tool-verified: `provisa/file_source/wikipedia.py` `LINK_SELECTOR`、`REMOVE_SELECTORS`、`TABLE_SELECTOR`、`DEFAULTS`]

**高级抓取设置。** 表单将这些设置折叠在"Crawl settings"下，并显示各自的默认值；字段留空即保留默认值。在 `files` 的 mapping 中，同样的名称放在 `crawl:` 下，不在此列表中的名称会被拒绝。[tool-verified: `provisa/file_source/crawl.py` `CRAWL_SETTINGS`、`crawl_operand`；`provisa/file_source/wikipedia.py` `crawl_settings`]

| 设置 | Wikipedia 默认值 | 含义 |
| --- | --- | --- |
| `start_urls` | 你指定的页面 | 抓取的起始页面 |
| `max_depth` | `1` | 链接深度 |
| `max_pages` | `25` | 页面上限 |
| `request_delay` | `1 seconds` | 请求之间的延迟 |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`，并附加你的联系方式 | 抓取使用的自我标识名称 |
| `content_selector` | `#mw-content-text .mw-parser-output` | CSS 选择器：只在其内部读取链接和表 |
| `remove_selectors` | 上文所述部分的列表 | 用于排除部分内容的 CSS 选择器。指定后会替换整个列表，包括"See also"规则 |
| `link_selector` | `a[rel='mw:WikiLink']`，并带排除项 | 链接必须匹配才会被跟随的 CSS 选择器 |
| `link_exclude_patterns` | 空 | 正则表达式；地址中匹配其中之一的链接不会被跟随 |
| `follow_external_links` | `false` | 是否跟随指向其他站点的链接 |
| `allowed_domains` | 未设置 | 抓取可以访问的域 |
| `table_selector` | `table.wikitable` | 表必须匹配的 CSS 选择器 |
| `generate_tables_from_html` | 未设置 | HTML 表是否成为表 |
| `html_table_min_rows` | `2` | 表的最少行数 |
| `html_table_max_rows` | 未设置 | 表的最多行数 |
| `allowed_file_extensions` | `csv`、`tsv`、`xlsx`、`xls`、`json`、`parquet` | 会成为表的链接文件类型 |
| `html_cache_ttl` | `1 days` | 抓取结果保留多久 |
| `max_html_size` | 未设置 | 读取的最大页面，例如 `10MB` |
| `max_data_file_size` | 未设置 | 读取的最大数据文件，例如 `100MB` |

**表命名。** 抓取到的表命名为 `<page>__<name>`。名称是表自身的 `id`（若有），否则是其标题（caption），再否则是其上方最近的标题。例如，页面 `List_of_tallest_buildings` 上标题"Tallest buildings"下的表，在名称规范化后成为 `list_of_tallest_buildings__tallest_buildings`。[tool-verified: calcite `file` adapter `HtmlToJsonConverter.java` (`baseFileName + "__" + tableName`), `HtmlTableScanner.java` `getTableName`] [inferred: the exact normalized spelling in the example]

**刷新。** 抓取结果会在缓存期内保留，默认为 `1 days`（`html_cache_ttl`）。只有在该时间过后才会重新获取页面。[tool-verified: `provisa/file_source/wikipedia.py` `DEFAULTS`]


### Observability & Other {: #observability-other }

`prometheus` 拥有一个 Trino 连接器（属性由该类型的映射 DSL 构建）。`google_sheets` 是一种已注册的数据源类型，没有 Trino 连接器，通过 API 缓存管道进行物化。[tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Source Type | Connector Name | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialized) | No |
| `prometheus` | prometheus | No | 一个指标即为一张表，一个样本即为一行（`timestamp`、`value`，每个标签一列）；在没有活动连接器的每个引擎上，Provisa 读取 HTTP API — Register Table 使用指标名称和标签，落地时对该表的范围使用 `query_range` [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689) |

### Enterprise SaaS Connectors {: #enterprise-saas-connectors }

Google Workspace 和 Microsoft 365 由各自的加载器读取；参见 [Google Workspace 和 Microsoft 365](#google-workspace-and-microsoft-365)。SharePoint、Salesforce、ServiceNow、Jira、云资源清单和 Splunk 通过 Apache Calcite 连接器（kenstott/calcite fork）注册。它们都没有直连驱动 — Provisa 启动连接器自带的 Calcite pgwire 服务器（`pgwire-sharepoint`、`pgwire-salesforce`、`pgwire-servicenow`、`pgwire-jira`、`pgwire-cloudops`、`pgwire-splunk`），并将其作为通用 PostgreSQL 端点访问。在 DuckDB 引擎上，该端点通过 postgres 扩展实时挂载：Register Table 从已挂载的目录中列出连接器的表，查询就地读取该连接器，过滤和投影会下推到 Calcite（REQ-1690）[tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]。在其他所有引擎上，数据行都会落地到物化存储中以用于联邦（REQ-954）。这些程序包按操作系统/架构从固定版本的 `kenstott/calcite` release 中获取（`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`；macOS arm64、Linux x86_64、Windows x86_64）[tool-verified: `provisa/runtime_deps/pgwire_bundles.py`] 操作员也可以为每个连接器单独命名一个为该主机构建的程序包文件夹，路径位于 Admin → 设置 → 连接器服务器（`connectors.<connector>.bundle_dir`）；请参阅 [Admin](admin.md)。。五个连接器都始终启用大小写不敏感的名称匹配，以匹配各自产品自身的大小写不敏感语义（REQ-725, REQ-730）。[tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

SharePoint 列表被枚举为 schema 并暴露为可查询的表（REQ-726, REQ-731）。两种身份验证方式：`CLIENT_CREDENTIALS`（默认）和基于证书的 PFX 证书方式（REQ-727）。`mapping` 中的密钥值在到达连接器之前会通过密钥引擎解析（REQ-729）。[tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Source field | Connector property | Notes |
| --- | --- | --- |
| `base_url` or `host` | `site-url` | SharePoint 站点 URL |
| `username` | `client-id` | Azure 应用客户端 ID |
| `password` | `client-secret` | Azure 应用客户端密钥 |
| `database` | `tenant-id` | Azure 租户 UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS`（默认）或 `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | 当 `auth_type: CERTIFICATE` 时的 PFX 路径 — 必须是绝对路径 |
| `mapping.certificate_password` | `certificate-password` | PFX 密码 — 该键必须存在，无密码的 PFX 使用空字符串 |

在非 Trino 引擎上，证书身份验证还带有两条额外规则，均在构建 Calcite pgwire 服务器的 `model.json` operand 时强制执行（REQ-1693）。`certificate_path` 必须是绝对路径：服务器以其程序包目录作为工作目录运行，因此相对路径会在运行时依赖缓存内部解析，导致找不到该 PFX。即使 PFX 没有密码，`certificate_password` 也必须存在于 `mapping` 中，此时其值为空字符串 — Calcite 适配器会直接拒绝 null 密码，缺失该键会被视为配置错误，而不是被静默地当作空密码读取。缺失或相对路径的值会抛出命名该字段的 `MissingConnectorConfig`。[tool-verified: `provisa/federation/pgwire_replica.py` `_sharepoint_operand`]

当连接器不暴露 `information_schema.columns` 时，通过 `registerTable` mutation 使用显式列定义（从 Microsoft Graph API 获取）注册该表（REQ-732）。

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  password: ${env:SP_CLIENT_SECRET}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CLIENT_CREDENTIALS
```

证书身份验证，使用绝对路径和始终存在的密码：

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CERTIFICATE
    certificate_path: /etc/provisa/certs/sharepoint.pfx
    certificate_password: ${env:SP_CERT_PASSWORD}
```

#### `salesforce`

凭据可以查询的每个 sObject 都是数据管理员可以注册的表（`Account`、`Opportunity`、自定义 `*__c` 对象）；不会自动注册任何内容 (REQ-1946)。列和类型来自 sObject 的 describe。过滤、投影、排序和限制以 SOQL 的形式下推到 Salesforce。 [tool-verified: `provisa/federation/pgwire_replica.py` `_salesforce_operand`; `provisa/federation/trino_connectors.py` `TrinoSalesforceConnector`]

Salesforce 数据源需要指定组织的“我的域”登录 URL 和一套完整的凭据，由 `mapping.auth_type` 选择。登录 URL 是完整的 `https://…` URL（例如 `https://acme.my.salesforce.com`）；裸主机或任何其他方案都会被拒绝，并指出字段名称。使用数据源时，不完整的凭据会被拒绝，并指出缺少的值。每个密钥都可以是 `${secret:…}` 或 `${env:…}` 引用。以字面值输入的密钥会存入组织保管库，数据源保留其引用。

| Source field | Connector property | Notes |
| --- | --- | --- |
| `base_url` or `host` | `login-url` | 组织的“我的域”URL（设置，然后是“我的域”）。`login.salesforce.com` 不适用于客户端凭据流程 |
| `username` | `client-id` | 连接的应用程序的消费者密钥 |
| `password` | `client-secret` | 连接的应用程序的消费者密码 |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS`（默认）、`USERNAME_PASSWORD` 或 `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | 仅 `USERNAME_PASSWORD`，与消费者密钥和消费者密码一起使用 |
| `mapping.security_token` | `security-token` | 仅 `USERNAME_PASSWORD`；可选 |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | 仅 `ACCESS_TOKEN`；不需要消费者密钥或消费者密码 |
| `mapping.api_version` | `api-version` | 可选，例如 `v61.0` |

对于 `CLIENT_CREDENTIALS`，连接的应用程序需要启用 *Enable Client Credentials Flow* 并设置 *Run As* 用户；每次读取和写入都以该用户的权限运行。自 Summer '23 起创建的组织默认禁用用户名密码流程。

**读取。** 在 Trino 上，数据源通过只读的 `salesforce` 目录读取。在其他任何引擎上，引擎挂载数据源的 pgwire 服务器并就地读取。

**写入。** 对已注册 sObject 的 `INSERT`、`UPDATE` 和 `DELETE` 是变更操作：与其他任何变更一样受治理、被审计，并按环境处理。在每个引擎上（包括 Trino），写入都在数据源的 pgwire 服务器上运行；在 Trino 上，该服务器只为写入而运行。每条语句都会发送到 Salesforce 并在执行时提交，因此无法回滚。服务器不会返回它所写入的行：带有 `RETURNING` 子句的语句会被拒绝，GraphQL 也不为这些表提供 mutation 字段；SQL 的 `INSERT`、`UPDATE`、`DELETE` 和 `COPY` 是写入路径。一次写入是针对一张表的单一普通语句：`INSERT … ON CONFLICT`、`UPDATE … FROM`、`DELETE … USING`、`MERGE` 和 `TRUNCATE` 均被拒绝，并指出形式。SharePoint 数据源以同样的方式写入。[tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`; `provisa/api/data/pgwire_write.py`]

**启动。** 在开始接受连接之前，数据源的服务器会读取每个 sObject 的列，对于拥有 1,200 个 sObject 的组织大约需要四分钟。这个代价只付一次：describe 结果保存在实例数据目录下该数据源自己的状态目录中。服务器在数据源注册或加载时启动。在 Trino 上，`salesforce` 目录保存自己的 describe 结果，在协调器的 `/data/trino/cache/salesforce-describe` 下每个数据源一个目录，因此当该路径是一个能比容器存活更久的卷时，这里的代价同样只付一次：compose 栈的 `trino_cache` 卷，或 Helm chart 的协调器缓存卷（`trino.coordinator.cache.persistence`）。在它开始接受连接之前，Register Table 会显示连接器正在启动，写入会收到同样的消息而不是等待。[tool-verified: `provisa/federation/pgwire_replica.py` `build_model_json`, `start_endpoint`]

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

用户名密码流程：

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    auth_type: USERNAME_PASSWORD
    sf_username: ops@acme.com
    sf_password: ${env:SF_PASSWORD}
    security_token: ${env:SF_SECURITY_TOKEN}
```

#### `servicenow`

实例的每张表均可供注册；不会自动注册任何内容（REQ-1954）。完整实例可能有数千张表；Register Table 按主题对它们分组——每个主题是 ServiceNow 包的显示名称，当表不属于任何包时显示"(无包)"。每张表以 `name — label` 的形式列出。选择一张表会从实例读取其列；列出表格不会这样做。[tool-verified: `provisa/federation/pgwire_replica.py` `servicenow_table_list`、`servicenow_columns`；`provisa/federation/servicenow.py` `TABLE_LIST`、`COLUMN_LIST`]

ServiceNow 数据源指定实例的完整 `https://…` URL 以及用户名和密码。URL 从 `base_url` 或 `host` 获取；裸主机或其他方案将被拒绝，并指出字段名称（`instanceUrl`）。缺少用户名或密码时将被拒绝，并指出字段名称。每个值都可以是 `${secret:…}` 或 `${env:…}` 引用。以字面值输入的密钥会存入组织保管库，数据源保留其引用。

| 源字段 | 适配器属性 | 说明 |
| --- | --- | --- |
| `base_url` 或 `host` | `instanceUrl` | 实例的完整 `https://…` URL（例如 `https://acme.service-now.com`）。裸主机或其他方案将被拒绝 |
| `username` | `username` | 基本身份验证用户名 |
| `password` | `password` | 基本身份验证密码 |

[tool-verified: `provisa/federation/servicenow.py` `servicenow_instance_url`、`servicenow_settings`]

**读取。** 在 Trino 上，数据源通过只读的 `servicenow` 目录读取。在其他任何引擎上，引擎挂载数据源的 pgwire 服务器并就地读取。实例只接收所选列的请求（`sysparm_fields`）。过滤器仅以适配器已在真实实例上验证过的形式发送到实例（`sysparm_query`）；其他所有过滤器以及每个排序和限制，都应用于已读取的行。[inferred: the adapter's requirements ledger (kenstott/calcite `docs/requirements/servicenow.yaml`, SNOW-022, SNOW-023, SNOW-030)]

引用字段显示为两列：`<field>` 包含被引用记录的 sys_id，`<field>__display` 包含显示值。空字段值为 NULL。适配器无法映射其类型的字段会产生错误，错误中指出表、字段和类型。[inferred from adapter behavior；本次会话中未在 Provisa 源代码中直接验证]

表搜索（自然语言）与每张表的名称、标签和主题匹配。一旦注册了一张表，其列就会加入搜索。[tool-verified: `provisa/discovery/catalog_cache.py` `COLUMNS_PER_TABLE_KINDS` docstring]

**服务器生命周期。** 服务器仅提供从中注册的表。注册或移除一张表会重启服务器并在引擎上重新配置数据源；在该重启期间到达的语句会失败。[tool-verified: `provisa/federation/pgwire_replica.py` `SERVES_REGISTERED_TABLES`；`provisa/api/app_rebuild.py` `serve_registered_tables`]

**写入。** 对已注册表的 `INSERT`、`UPDATE` 和 `DELETE` 是变更操作：与其他任何变更一样受治理、被审计，并按环境处理。在每个引擎上（包括 Trino），写入都在数据源的 pgwire 服务器上运行。服务器会返回每次写入后实例持有的行；因此接受 `RETURNING` 子句，GraphQL 也为这些表提供变更字段。一次写入是针对一张表的单一语句：`INSERT … ON CONFLICT`、`UPDATE … FROM`、`DELETE … USING`、`MERGE` 和 `TRUNCATE` 均被拒绝，并指出形式。[tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`、`PGWIRE_SERVER_RETURNS_ROWS`]

**Import sets（导入集）。** 当一张表存在指向它的活动转换映射时，Register Table 会提供其 import sets，数据管理员可以将表绑定到其中一个（有多个映射的表会逐一命名）。绑定表的插入操作通过实例的 Import Set API 和该映射的转换进行；未绑定表的插入操作以及每次更新和删除都直接写入表中。转换拒绝的行将被拒绝，并指出该行；已写入的行保持不变。绑定在保存表时进行检查：它必须是该表的活动转换映射的 import set，且位于 ServiceNow 数据源上。[tool-verified: `provisa/api/admin/_import_set.py` `table_import_set_refusal`；`provisa/federation/servicenow.py` `servicenow_settings`；`provisa/federation/pgwire_replica.py` `servicenow_transform_maps`]

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

Jira Cloud 站点通过其自有的 Calcite 适配器读取和写入（REQ-2069）。Jira 数据源的一张表是一个**表单**：一个项目和一种 issue 类型，以及其列所读取的字段。Register Table 列出该账户可创建 issue 的每个表单，按项目分组；选择一个表单会从 Jira 读取其字段，列出表单不会这样做。每个表单都有列 `key`（其主键）、`id`、`summary`、`status`、`created` 和 `updated`，之后是每个所选字段一列，按适配器的命名方式命名：字段名称的 snake case 形式（`Story Points` 即 `story_points`）。名称已是某列名称的字段，以其字段 id 命名（`customfield_10099`），因此每个字段都可被选择。用户字段保存账户 id；其显示名称是单独的字段（`assignee.displayName`，列 `assignee_name`）。由 Jira 自行写入的列为只读：`key`、`id`、`created`、`updated`、用户字段的显示名称，以及适配器不认识其形态的字段（适配器将其作为 JSON 文本提供）。注册此类列时，若其没有描述，会在描述中标注“Read only: Jira sets it”；设置该列的写入会被服务器拒绝，并指出该列。[tool-verified: `provisa/federation/jira.py` `FIXED_COLUMNS`、`form_columns`；`provisa/federation/pgwire_replica.py` `jira_form_list`、`jira_fields`；`provisa/api/admin/introspect.py` `complete_jira_form`]

表单的表在其 `arguments` 中指明其项目和 issue 类型（`project`、`issue_type`），每一列读取其 `path` 所指明的字段（字段 id 或字段名称），或与其同名的字段。从管理端注册的表会从 Jira 获得这两者；配置则显式声明它们。固定了其他内容或缺少其中任何一项的 Jira 表将被拒绝，并指出表名。[tool-verified: `provisa/federation/jira.py` `form_of`；`provisa/core/config_loader.py` `_validate_table_arguments`]

| 源字段 | 适配器属性 | 说明 |
| --- | --- | --- |
| `base_url` 或 `host` | `baseUrl` | 站点的完整 `https://…` 地址（例如 `https://acme.atlassian.net`）。裸主机或其他方案将被拒绝 |
| `username` | `email` | Atlassian 账户的电子邮件 |
| `password` | `apiToken` | 该账户的 API 令牌，在 id.atlassian.com 的 Security、API tokens 下创建 |

[tool-verified: `provisa/federation/jira.py` `jira_site_url`、`jira_settings`]

**链接。** 数据源的 `issue_links` 表保存其表单的 issue 之间的链接：`link_id`、`link_type`、`outward_key` 和 `inward_key`，每个链接一次。两个表单之间的关系通过它声明为联结表（junction，REQ-1586），以 `link_type` 为判别列，因此 GraphQL 和 Cypher 按链接类型从 issue 走到被链接的 issue。[inferred: the adapter's design agreed for REQ-2069]

**读取。** 按键查找（`key =`、`key IN`）按键获取，因此未指明任何 issue 的键不会产生行。日期或数字比较会作为 JQL 发送到 Jira。其他所有过滤器以及每个排序，都应用于已读取的行：对文本、选项、状态或用户的相等比较不会发送，因为对于 SQL 返回零行的情况，JQL 会拒绝未指明任何对象的值。多值字段（标签、多选、组件、版本）是保存 JSON 数组的文本，写入方式相同。[inferred: the adapter's behaviour, as the calcite connector reports it for REQ-2069]

在 DuckDB、PostgreSQL 和 ClickHouse 上，引擎挂载数据源的 pgwire 服务器并就地读取。在 Trino 上，数据源没有自己的目录，而是通过其服务器落地。[tool-verified: `provisa/federation/engine.py`；`provisa/federation/strategy.py` `_CONNECTOR_PGWIRE_REPLICA`]

**服务器生命周期。** 服务器仅提供从中注册的表单，以及其表单列表、字段列表和链接。注册、更改或移除表单会重启服务器并在引擎上重新配置数据源。[tool-verified: `provisa/federation/served_tables.py` `SERVES_REGISTERED_TABLES`、`serve_tables`；`provisa/api/app_rebuild.py` `serve_registered_tables`]

**写入。** 对表单的 `INSERT`、`UPDATE` 和 `DELETE` 是变更操作，在每个引擎上都在数据源的 pgwire 服务器上写入。行以每批 50 行的方式插入；更新或删除逐个 issue 按键进行。设置 `status` 会执行到该状态的工作流转换，当 issue 的工作流没有可到达的状态时会被拒绝，并指出可到达的状态。带有子任务的 issue 不会被删除。服务器按键返回每个已写入的行，因此接受 `RETURNING`，GraphQL 也提供变更字段。在 `issue_links` 上，`INSERT` 创建链接，`DELETE` 移除链接；`UPDATE` 被拒绝。[tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`、`PGWIRE_SERVER_RETURNS_ROWS`] [inferred: the batching, transitions and subtask refusal are the adapter's, agreed for REQ-2069]

```yaml
- id: support-jira
  type: jira
  base_url: https://acme.atlassian.net
  username: ${env:JIRA_EMAIL}
  password: ${env:JIRA_API_TOKEN}
tables:
- source_id: support-jira
  table: incident
  arguments: {project: KAN, issue_type: Incident}
  columns:
  - {name: key, data_type: varchar, is_primary_key: true}
  - {name: summary, data_type: varchar}
  - {name: status, data_type: varchar}
  - {name: priority, data_type: varchar}
  - {name: flagged, data_type: varchar, path: customfield_10021}
```

#### `cloudops`

云资源清单把 Azure、AWS 和 GCP 的资源呈现为同一组表：`compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` 和 `compute_security_groups`（将计算资源与其安全组关联）。每张表在三个云上使用同一组列，其 `cloud_provider` 列指明每一行来自哪个云。该数据源只读，其表与其他表一样由数据管理员注册 (REQ-1947)。 [tool-verified: `provisa/federation/cloudops.py` `cloudops_settings`; `provisa/federation/trino_connectors.py` `TrinoCloudopsConnector`]

一个数据源指定一个或多个云。每个云要么完整要么不填：只填了部分必填值的云会被拒绝，并指出缺少的值；没有指定任何云的数据源同样会被拒绝。数据源只读取它指定的云。所有值都放在 `mapping` 中；每个密钥都可以是 `${secret:…}` 或 `${env:…}` 引用。以字面值输入的密钥会存入组织保管库，数据源保留其引用。

| 云 | `mapping` 键 | 连接器属性 | 说明 |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | 必填 |
| Azure | `azure_client_id` | `azure.client-id` | 必填；应用注册的客户端 ID |
| Azure | `azure_client_secret` | `azure.client-secret` | 必填；密钥 |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | 必填; 以逗号分隔 |
| AWS | `aws_access_key_id` | `aws.access-key-id` | 必填 |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | 必填；密钥 |
| AWS | `aws_region` | `aws.region` | 必填，例如 `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | 必填; 以逗号分隔 |
| AWS | `aws_role_arn` | `aws.role-arn` | 可选；为跨账户访问而承担的角色 |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | 必填；服务账号 JSON 密钥文件的绝对路径 |
| GCP | `gcp_project_ids` | `gcp.project-ids` | 必填; 以逗号分隔 |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | 可选；结果在再次查询各云之前被复用的时长（适配器默认值为 5） |

GCP 密钥文件由读取数据源的一方打开。在 Trino 上是 Trino 服务器，因此该路径必须存在于其上。在其他所有引擎上是 Provisa 主机上该数据源的 pgwire 服务器，其工作目录是它自己的状态目录，因此相对路径会被拒绝。

在 Trino 上，数据源通过 `cloudops` 目录读取。在其他任何引擎上，引擎挂载数据源的 pgwire 服务器并就地读取，与 SharePoint 数据源相同。

```yaml
- id: cloud-estate
  type: cloudops
  mapping:
    aws_access_key_id: ${env:CLOUDOPS_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${env:CLOUDOPS_AWS_SECRET_ACCESS_KEY}
    aws_region: us-east-1
    aws_account_ids: "111111111111,222222222222"
    azure_tenant_id: ${env:CLOUDOPS_AZURE_TENANT_ID}
    azure_client_id: ${env:CLOUDOPS_AZURE_CLIENT_ID}
    azure_client_secret: ${env:CLOUDOPS_AZURE_CLIENT_SECRET}
    azure_subscription_ids: "sub-1,sub-2"
```

#### `splunk`

Splunk 的搜索结果可作为表查询（例如 `internal_server`）（REQ-721）。连接器 URL 来自 `base_url`，否则构造为 `https://{host}:{port}`，默认端口为 `8089`（REQ-722）。身份验证：当 `mapping.use_token` 为 `true`（默认值）时，`password` 作为 API 令牌传递；为 `false` 时，`username` 和 `password` 作为独立凭据传递（REQ-723）。[tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Source field | Connector property | Notes |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`，否则为 `https://host:port`（默认端口 8089） |
| `password` | `token` or `password` | 当 `use_token: true` 时为令牌 |
| `username` | `user` | 仅当 `use_token: false` 时 |
| `database` | `app` | 限定到某个 Splunk app |
| `mapping.datamodel_filter` | `datamodel-filter` | 过滤到某个数据模型 |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | 用于自签名证书（REQ-724） |

在 pgwire-replica 路径上（除 Trino 以外的每个引擎），这四个可选设置会成为 Calcite `model.json` operand 键 `app`、`token`/`username`+`password`、`datamodelFilter` 和 `disableSslValidation` — 后两者按 `SplunkSchemaFactory` 转换的类型，分别为字符串和布尔值（REQ-1694）。[tool-verified: `provisa/federation/pgwire_replica.py` `_splunk_operand`]

```yaml
- id: ops-splunk
  type: splunk
  host: splunk
  port: 8089
  password: ${env:SPLUNK_TOKEN}
  mapping:
    use_token: true
    disable_ssl_validation: true
```

#### Google Workspace and Microsoft 365 (REQ-1923, REQ-1994) {: #google-workspace-and-microsoft-365 }

`google_workspace` 和 `microsoft_365` 读取数据源所指定账户的邮件、日历和任务，并将它们落地为副本。它们不是 Calcite 连接器：各自有自己的加载器，调用提供商的 API——一边是 Gmail、Google Calendar 和 Google Tasks，另一边是 Microsoft Graph。表单在 Enterprise 下将它们列为“Google Workspace (Gmail)”和“Microsoft 365 (Outlook, Exchange)”。[tool-verified: `provisa/core/models.py` `SourceType`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`; `provisa-ui/src/pages/sources/constants.ts`]

**表。** 两种类型产生相同的规范表：名称、列类型和顺序都一致。对二者做并集无需转换或重命名；某个提供商没有值的列为 NULL。这些表列在 `default` 模式下，每一行都以读取它的账户开头。数据源提供其所读资源对应的表：[tool-verified: `provisa/core/canonical_mail.py` module docstring, `RESOURCE_TABLES`, `tables_for`; `provisa/google_workspace/loader.py` `SCHEMA`]

| 资源 | 表 | 可由语句写入 |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | 否 |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

各列列于[规范邮件 schema](arch/canonical-mail-schema.md)。`events` 保存单次事件、以 RFC 5545 行表示重复规则的系列主事件，以及例外；它从不保存系列展开后的各次出现。[tool-verified: `provisa/google_workspace/calendar_read.py` module docstring] (REQ-1994)

**注册。** 组织的管理员首先在 Admin → Email 下输入该组织在该平台上的客户端：客户端 ID 和密钥，Microsoft 365 还需要目录（租户）ID 或域名。添加数据源的人看不到也不用输入这些内容。数据源随后采用两种形态之一：[tool-verified: `provisa/core/mail_platforms.py` module docstring; `provisa/microsoft365/__init__.py`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json`]

- **单个邮箱。** 数据源在 `accounts` 中指定一个地址。其所有者在表单上点击 **Connect** 并批准组织的客户端。由此得到的授权保存在组织的状态存储中，而不在数据源的设置里。在 Google Workspace 上，这种形态还需声明 `sign_in: google_account`。
- **组织的各个邮箱。** 数据源将 `mailboxes` 声明为 `everyone: true`、`group: <address or id>` 或 `list: [addresses]` 中恰好一项。它不持有凭据，也没有人去连接它。组织使用自己的访问权限读取，并且只有在管理员打开“Read the organization's mailboxes”之后才可以。在 Google 上，该访问权限是带域范围委派的服务账号密钥，外加用于目录调用的管理员地址。在 Microsoft 上，它是应用注册的客户端凭据授权。

[tool-verified: `provisa/google_workspace/settings.py` `parse`, `_mailboxes`; `provisa/microsoft365/settings.py` `parse`; `provisa/core/mail_platforms.py` `organisation_mailboxes`]

采用第二种形态时，一张表保存所有邮箱的行。除非针对 `account` 列的行规则加以限制，否则可以读取该表的角色能读取每个邮箱。[tool-verified: `provisa-ui/src/i18n/locales/en/googleWorkspaceFields.json` `everyMailbox`]

```yaml
sources:
  - id: team-mail
    type: microsoft_365
    mapping:
      resources: [mail, calendar, tasks]
      mailboxes:
        group: support@acme.com
      mail_since: "2026-01-01"
      calendar_check_days: 30        # required when calendar is read
      calendar_send_updates: false
```

**设置。** 每个模块都会按名称拒绝它不读取的键。[tool-verified: `provisa/google_workspace/settings.py` `_KEYS`; `provisa/microsoft365/settings.py` `_KEYS`]

| `mapping` 键 | 类型 | 含义 |
| --- | --- | --- |
| `resources` | 两者 | 数据源读取 `mail`、`calendar`、`tasks` 中的哪些；至少一个 |
| `accounts` | 两者 | 单个邮箱的地址（单邮箱形态） |
| `mailboxes` | 两者 | `everyone`、`group` 或 `list`（组织形态） |
| `sign_in` | `google_workspace` | `google_account`（单邮箱形态） |
| `mail_since` | 两者 | `YYYY-MM-DD`；更早收到的邮件不保存 |
| `mail_content` | `google_workspace` | `full`，或仅含邮件头和标签的 `headers`（Gmail 的 `gmail.metadata` 范围）。读取邮件时必填 |
| `mail_search` | `google_workspace` | 如在 Gmail 搜索框中键入的搜索 |
| `mail_labels` | `google_workspace` | 标签名称；只读取带有全部这些标签的邮件 |
| `mail_include_spam_trash` | `google_workspace` | 默认为 `false` |
| `calendar_send_updates` | 两者 | 写入时是否通知事件的参与者；默认关闭 |
| `calendar_check_days` | `microsoft_365` | 变更检查查看今天前后各多少天；不小于 1 的整数，读取日历时必填 |

Google 在 `gmail.metadata` 下拒绝搜索，因此将 `mail_content: headers` 与 `mail_search` 或 `mail_since` 一起使用会被拒绝。[tool-verified: `provisa/google_workspace/settings.py` `_mail`]

**范围（scope）。** 登录时只请求已勾选资源所对应的范围：[tool-verified: `provisa/google_workspace/settings.py` `MAIL_SCOPES`, `RESOURCE_SCOPES`; `provisa/google_workspace/sign_in.py` `EMAIL_SCOPE`; `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`, `BASE_SCOPES`; `provisa/microsoft365/directory.py`]

| 资源 | Google Workspace | Microsoft 365（委派） |
| --- | --- | --- |
| `mail` | `gmail.readonly`，仅邮件头时为 `gmail.metadata` | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| 每个数据源 | `userinfo.email` | `offline_access`, `User.Read` |

对于组织形态，Microsoft 需要应用程序权限 `Mail.Read` 和 `User.Read.All`，日历和任务还需要 `Calendars.ReadWrite` 和 `Tasks.ReadWrite.All`。Google 的服务账号被授权使用 `DELEGATED_SCOPES` 中的范围，其中还包括目录用户和群组成员的读取。[tool-verified: `provisa/microsoft365/directory.py`; `provisa/google_workspace/__init__.py` `DELEGATED_SCOPES`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json` `organisationGrant`]

**刷新与新鲜度。** 这些数据源接受的探测类型是 `hash` 和 `none`，变更令牌由其加载器提供：[tool-verified: `provisa/events/probes.py` `_MAIL_TYPES`, `probe_capabilities`]

- **Gmail：** 每个邮箱当前 history id 的摘要，每个邮箱一次 `users.getProfile` 调用。[tool-verified: `provisa/google_workspace/loader.py` `mailbox_token`]
- **Microsoft 邮件：** 沿着保存的每个 delta 链接取一项。[tool-verified: `provisa/microsoft365/loader.py` `unchanged`]
- **Google 日历和任务：** 每个日历的标签和最后变更时间；每个任务列表的标签，以及每个任务的 id 和更新时间。[tool-verified: `provisa/google_workspace/calendar_read.py` `change_token`]
- **Microsoft 日历和任务：** 每个邮箱的变更行，范围是今天前后各 `calendar_check_days` 天。[tool-verified: `provisa/microsoft365/loader.py` `_calendar_token`] [inferred: that a line is an event's id and change key, or a task's update time, from REQ-1994]

无法完成的检查会请求读取；它绝不会报告“unchanged”。日历表和任务表在每次刷新时整体读取。Microsoft 365 或 Google Workspace 数据源的邮件副本就地保存：邮箱的每个部分在到达时写入，可立即查询。[tool-verified: `provisa/core/canonical_mail.py` `RESOURCE_TABLES` comment; `provisa/federation/replica_builds.py` `GROUP_READ_TYPES`; `provisa/federation/mail_units.py`] 通过 `mail_search` 缩小范围的 Google Workspace 数据源在每次刷新时都要重新整体读取，因为只有 Google 才能说明一次搜索匹配了什么。[tool-verified: `provisa/google_workspace/units.py` module docstring]

**写入。** 对 `events`、`event_attendees` 和 `tasks` 的 `INSERT`、`UPDATE` 和 `DELETE` 通过两种类型共用的同一条写入路径发送给提供商。`UPDATE` 或 `DELETE` 通过 `provider`、`account` 和 `id` 指定其行；参与者通过 `event_id` 和 `address` 指定。系列中的某一次出现通过 `series_id` 和 `original_start_at` 指定。设置了表的可写集合之外的列的写入会按名称被拒绝。`calendar_send_updates` 开启时，Google 以 `sendUpdates=all` 写入；关闭时以 `sendUpdates=none` 写入。Microsoft Graph 无法静默地做出这样的变更，因此对会议参与者的变更会被拒绝，并指明该设置。[tool-verified: `provisa/executor/writable.py` `API_WRITTEN`; `provisa/core/canonical_mail.py` `WRITE_KEYS`, `WRITABLE_COLUMNS`; `provisa-ui/src/i18n/locales/en/mailResources.json` `sendUpdatesHelp`] [tool-verified: `provisa/google_workspace/calendar_write.py`]

**排除项。** 邮件只读；没有任何语句会更改邮箱。`calendars`、`task_lists` 和所有邮件表都不接受写入。除非开启 `mail_include_spam_trash`，否则 Gmail 的 Spam 和 Trash 会被排除。Google 的 Tasks API 只保存任务的到期日期，没有提醒。[tool-verified: `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`; `provisa/core/canonical_mail.py` `WRITABLE_COLUMNS`; `provisa/google_workspace/settings.py` `_mail`] [inferred: the Tasks API limit, from REQ-1994]

### API Sources {: #api-sources }

将任意 HTTP 端点注册为可查询的表。[tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| API Type | Discovery | Column Inference |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI 规范解析（REQ-314, REQ-316） | 原始类型 → 原生类型，对象 → JSONB |
| `graphql_remote` | Schema introspection（REQ-307, REQ-308） | 原始类型 → 原生类型，对象 → JSONB |
| `grpc_remote` | Server reflection（REQ-322, REQ-325） | 原始类型 → 原生类型，对象 → JSONB |

API 响应会被获取、缓存在 PostgreSQL 中（TTL 可配置），并暴露为 GraphQL 类型（REQ-309, REQ-318, REQ-327）。缓存的表与其他数据源一样参与联邦查询（REQ-313）。

**JSONB 规则**：以 JSONB 存储的复杂列（对象、数组）不可过滤（REQ-119）。子字段访问在 SQL 中使用 `->>` 提取（REQ-151）。表之间的关系通过标量外键列声明 — JSONB blob 列不能作为连接目标。当需要对嵌套字段进行过滤或连接时，使用 JSONB 提升（promotion）将其转换为原生标量列（REQ-119）。

#### GitHub and GitLab {: #github-and-gitlab }

GitHub 和 GitLab 是由 `graphql_remote` 承载的品牌数据源：每个都以自己的名称出现在 Sources 表单中，并注册为记录其品牌的远程 GraphQL 数据源。表单只需要一个个人访问令牌，别无其他。端点（`https://api.github.com/graphql`、`https://gitlab.com/api/graphql`）和 schema 随 Provisa 一同提供，因此注册时不会对 schema 进行内省。令牌在添加数据源时检查。添加数据源不会注册任何表：schema 提供的每张表都在该数据源的命名空间（`gh`、`gl`）下可用，由数据管理员注册所需的表。令牌无权读取的字段在注册表时会被排除。[tool-verified: `provisa/graphql_remote/brands.py` `BRANDS`] (REQ-1923)

#### Stripe (REQ-1923) {: #stripe }

Stripe 是由 `openapi` 承载的品牌数据源，其构建方式与基于 `graphql_remote` 的 GitHub 和 GitLab 相同。它在 Sources 表单中以 Stripe 之名出现，位于 Enterprise 下。它注册为一个 OpenAPI 数据源，其行在 `federation_hints` 中记录品牌，规范路径为 `brand:stripe`。[tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_CARRIER`; `provisa/openapi/brands.py` `BRANDS`, `SPEC_SCHEME`; `provisa/api/admin/openapi_router.py` `_branded`]

**覆盖范围。** Stripe REST API，通过 Stripe 发布的 OpenAPI 规范（`stripe/openapi`，`spec3.json`）。该规范随 Provisa 一同提供，已固定版本并经 gzip 压缩，注册时不会获取。随附的版本是 API 版本 `2026-09-30.endive`，含 425 个路径。每次调用都带有设为该版本的 `Stripe-Version`，因此无论账户默认使用哪个版本，每个响应都具有规范所描述的形状。Stripe 返回但规范中缺失的字段，从 `stripe.additions.json` 补入。[tool-verified: `provisa/openapi/brands.py` `brand_spec`, `Brand.headers`; `provisa/openapi/_known_specs/`; version and path count read with the project interpreter]

与任何 OpenAPI 数据源一样，每个 `GET` 操作是一张可提供的表，其他每个操作是一条可提供的命令。添加数据源不会注册其中任何一个：由数据管理员注册所需的部分。[tool-verified: `provisa/api/admin/openapi_router.py` `register_openapi_source`; `provisa/openapi/brands.py` module docstring]

**认证。** 表单只要求一样东西：访问令牌（`BRAND_AUTH`：`token`）。请使用 Stripe 的密钥或受限 API 密钥。令牌作为 bearer 令牌发送，并在添加数据源之前通过 `GET /v1/balance` 检查一次。令牌被拒绝时，注册失败并返回 `openapi.credential_rejected`；缺少令牌时返回 `openapi.credential_required`。[tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_AUTH`; `provisa/openapi/brands.py` `Brand.auth`, `verify_path`; `provisa/api/admin/openapi_router.py` `_branded`] [inferred: that a restricted key works, provided it may read `/v1/balance`]

**刷新与新鲜度。** Stripe 表和所有 OpenAPI 表一样是副本。其缓存的响应保持时间为数据源的 `cache_ttl`；除非注册请求另行指定，否则该值设为 300 秒。适用 OpenAPI 的探测类型。[tool-verified: `provisa/api/admin/openapi_router.py` `OpenAPIRegisterRequest.cache_ttl`; `provisa/events/probes.py` `_HTTP_API_TYPES`] REQ-1923 规定预设可以指定刷新时用来只获取变更内容的参数。Stripe 品牌没有指定这样的参数，因此每次刷新都整体读取其表。[tool-verified: `provisa/openapi/brands.py` `Brand` has no such field]

**排除项。** Stripe 已弃用的令牌与 Sources 支付集成未包含在随附的规范中。其列表见[规范构建数据源上的排除项](#exclusions-on-a-specification-built-source)。操作员自己的 `exclusions` 会追加到品牌的排除项之后。[tool-verified: `provisa/openapi/_known_specs/stripe.exclusions.json`; `provisa/openapi/brands.py` `brand_spec`] (REQ-1957)

`stripe` 也出现在 `SourceType` 中，位于被存根化的 Supabase `wrappers` 类型之列。通过表单添加的 Stripe 数据源是 `openapi` 数据源，而不是那个存根。[tool-verified: `provisa/core/models.py` `SourceType`]

#### 规范构建数据源上的排除项 {: #exclusions-on-a-specification-built-source }

已发布的 OpenAPI 规范描述了远程 API 接受的一切，包括其供应商已弃用的内容，以及部署绝不能发送的内容。OpenAPI 数据源可以带有排除项：规范中存在但该数据源不提供的内容。(REQ-1957)

| 被排除项 | 指定方式 | 效果 |
|---|---|---|
| 一个操作 | `method` 和 `path` | 不会列出供注册为表或命令 |
| 一个请求参数 | `name`，针对一个操作（`method` 和 `path`），或两者都省略时针对每个包含它的操作 | 不属于该操作的参数，不显示，从不发送 |

排除项设置在数据源 `mapping` 的 `exclusions` 下：

```yaml
sources:
  - id: shop-api
    type: openapi
    path: https://shop.example.com/openapi.json
    mapping:
      exclusions:
        operations:
          - {method: POST, path: /v1/tokens}
        arguments:
          - {name: source, method: POST, path: /v1/customers}
          - {name: legacy_token}          # on every operation that has it
```

它们在加载规范时应用，因此注册列表、生成的命令和发出的请求所看到的规范中，被排除的内容并不存在。请求体属性以及查询、请求头或 cookie 参数可以被排除；路径参数不能，因为它是操作地址的一部分。

如果排除项指定了规范中不存在的操作或参数，该数据源的规范加载会失败，并列出每个过期条目，因此与较新规范不再匹配的列表会被发现。

品牌数据源在其策划中自带排除项，操作员的排除项会追加到其上。Stripe 数据源排除了 Stripe 已弃用的令牌和来源（source）支付集成：创建 charge、Tokens 和 Sources API、凭令牌向客户添加银行卡或银行账户，以及客户和发票支付写入操作中的 `source`、`card`、`bank_account` 和 `default_card` 参数。收款通过 Payment Intents、Payment Methods 和 Setup Intents 完成，数据源提供这些接口。读取和删除账户已有的支付来源仍然可用。


### GovData {: #govdata }

美国政府开放数据。访问权限按主题分组进行划分。[tool-verified: `provisa/core/models.py` lines 543–609]

为数据源指定一个或多个主题，每个主题会带来下面列出的适配器 schema。一个 schema 可以属于多个主题（`census`）。`ref` 和 `geo` schema 是链接 schema：每个主题的表都通过它们联接的参考数据和地理数据，因此无论指定了哪些主题，每个数据源都会提供它们。该列表在服务器上只声明一次，并由测试保证与固定版本适配器程序包所提供的 schema 一致。[tool-verified: `provisa/core/models.py` `GOVDATA_SUBJECT_SCHEMAS`、`GOVDATA_LINKER_SCHEMAS`；`tests/unit/test_govdata_subject_schemas.py`]

| Subject | Schemas Exposed |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents`, `cftc` |
| `ECONOMY` | `econ`, `econ_reference`, `ag`, `banking`, `fiscal`, `housing`, `transport` |
| `EDUCATION` | `census`, `edu`, `research` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime`, `disasters` |
| `ENVIRONMENT` | `lands`, `environment` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec`, `officials`, `law` |
| `DEMOGRAPHICS` | `census` |
| 每个数据源 | `ref`, `geo` |

`ALL` 是订阅中表示所有主题的简写；它不是 Sources 表单上的选项。

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Yes | — | 唯一标识符 |
| `subject` | Yes | — | 以上主题值之一 |
| `domain_id` | Yes | — | 该数据源所属的域 |
| `description` | No | `""` | 人类可读的描述 |

在管理端创建的 AskAmerica 数据源只需要一个凭据，即 API 密钥。Provisa 向 AskAmerica 的 API 出示该密钥，并获得用于读取数据的短期只读存储凭据；如果密钥未知或已被撤销，会在那里按名称被拒绝。[tool-verified: `provisa/federation/askamerica.py`]

随后该数据源就是一个 Postgres 线协议数据源，与 SharePoint、Salesforce 和云资源清单数据源相同：Provisa 为其启动随附的 `pgwire-govdata` 服务器，并将密钥和这些凭据放入服务器的环境中，每个引擎访问该服务器的方式与访问同类服务器相同。DuckDB、Postgres 和 ClickHouse 挂载它并就地读取其表，因此 AskAmerica 的表可以与其他数据源的表联接、位于视图之下，并像其他表一样交付。Trino 没有针对它的连接器，而是读取通过该服务器落地的副本：即保存在物化存储中的表副本，其新鲜度取决于最近一次刷新（遵循表的缓存和复制设置），而不是在查询时读取。表从其指定的适配器 schema（`sec`、`econ` 等）中读取。[tool-verified: `provisa/federation/pgwire_replica.py`、`provisa/federation/connector_duckdb.py`、`provisa/federation/clickhouse_connectors.py`]

服务器在接受连接之前会从对象存储挂载其 schema，首次启动需要数分钟。它在数据源保存时启动；在就绪之前，Register Table 表单会报告该数据源仍在启动。[tool-verified: `provisa/federation/pgwire_replica.py` `start_when_saved`]

### Where a source's password lives

一个数据源的密码永远不会与其连接设置的其余部分存放在一起。控制平面
`sources` 表的行携带一个 `password_ref` 列，其中存放的是为密码输入的内容：
值本身，或者诸如 `${env:PG_PASSWORD}`、`${secret:SNOWFLAKE_KEY}` 之类的引用。它会在数据源被拨号访问的那一刻，
在发起请求所属的组织内部被解析。[tool-verified: `provisa/core/schema_org.py`,
`provisa/core/repositories/source.py`]

不含 `${…}` 的值按原样使用。`${env:VAR}` 读取部署环境的进程环境变量，不需要任何绑定。`${secret:NAME}`
命名某个组织拥有的密钥，因此它只在该组织自身的操作中解析：管理内省接口
以及各个界面所能触达的查询终端都建立了这种绑定。[tool-verified:
`provisa/core/secrets.py` `resolve_secrets`；`provisa/pgwire/_pipeline.py` `_execute_plan`]

无论数据源是如何注册的，该字段保存的都是你输入的内容：

- **来自配置文件。** 该文件即为记录本身：Provisa 会将值或引用原样复制到
  `password_ref` 中。
- **来自 Sources 表单。** 在密码字段中输入的内容会被原样存储，无论是值还是引用。
  Provisa 不会为此向组织保管库写入任何内容，也不会自行编造密钥名称。若要让凭据不出现在控制平面行中，
  请自行创建密钥（参见 [Secrets](secrets.md)），并在该字段中输入 `${secret:NAME}`。[tool-verified:
  `provisa/api/admin/schema_mutation.py` `create_source`]

以值的形式输入的内容会按输入原样存储在控制平面数据库中。重新输入密码会替换该字段的文本。
删除数据源会移除其行，但不会动保管库中的任何内容：你创建的密钥归你所有。[tool-verified:
`provisa/api/admin/schema_mutation.py` `delete_source`；
`tests/unit/test_no_product_written_secrets.py`]

`password_ref` 不会在环境之间迁移（REQ-1491）。分支环境或复制出的环境
会提供自己的连接值，而引用所指向的保管库属于提供该引用的那个
环境。[tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

### Data Profiler

数据剖析器（data profiler）是一种只保存名称、调度计划和运行默认值的数据源，别无其他。它从不进行内省。其他数据源的表被添加到它之下，每次运行都会对这些表进行度量，并将结果行写入组织的控制平面，在那里像读取其他数据源的表一样读取。创建剖析器并向其添加表的方法参见[测试数据](test-data.md)。[tool-verified: `provisa/core/models.py` `SourceType.data_profiler`；`provisa/federation/strategy.py` `_MATERIALIZE_ONLY`] (REQ-1934)

### Data Quality Checkers (REQ-1443) {: #data-quality-checkers-req-1443 }

数据质量检查器是一种数据源类型，而不是一个子系统。其扫描输出即为数据：一条检查结果就是一次观测，因此它通过普通的数据源路径落地，并从其他任何数据源那里继承节奏、新鲜度、事件、血缘、治理、RLS、网格和导出能力。[tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

支持两种，选择哪一种既是许可证的选择，也是功能的选择。

| Source Type | Contract Dialect | Extra | Licence | Hosted cloud plane |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda contract YAML | `pip install .[soda]`（`soda-postgres`） | Elastic License 2.0 | 拒绝 — 见下文 |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]`（`great-expectations[postgresql]`） | Apache 2.0 | 允许 |

Elastic License 2.0 禁止将该软件以托管或受管服务的形式提供给第三方，而在 SaaS 平面内为某个租户运行 Soda 正是这种情况。`config/capabilities.yaml` 将这一区别体现为 `soda` 选项上的 `cloud_eligible: false`，托管平面会读取该标志。想要使用 Soda 的托管部署，需要连接到由运营方自行运行的运营方提供的 Soda 端点。[tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa 不打包也不链接任何东西。扫描在一个子解释器中运行（`python -m provisa.dq.worker`），这是唯一导入 `soda_core` 或 `great_expectations` 的地方，因此一个源码可用（source-available）的检查器永远不会进入服务器进程，检查器崩溃只会杀死一个子进程，而不会影响事件循环。[tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**该数据源指向的是 Provisa 自身的 pgwire 端点。** 正是这一点使得一个 postgres 驱动就能检查一个由 Snowflake 或 Iceberg 支持的表：检查器扫描的是联邦视图，而不是底层系统。由于策略应用于该连接，扫描身份是显式声明的而非继承而来 — 一个被过滤过的行集绝不能产生一个静默通过的检查。

```yaml
sources:

  - id: dq
    type: soda
    domain_id: sales-analytics
    description: Soda contract scans over the governed estate
    mapping:
      host: localhost
      port: 5439          # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner    # the scan identity, declared explicitly
      password: ${env:PROVISA_DQ_PASSWORD}
```

**每个契约一张结果表，而契约就是全部注册内容。** 该表携带 `dq_contract` — 契约原文本身 — 以及关于其结构的其他任何信息均由此推导。列、水位线和提升（promotion）都是派生出来的。[tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

```yaml
tables:

  - source_id: dq
    schema_name: quality
    table_name: orders_scan
    domain_id: sales-analytics
    change_signal: ttl_probe
    cache_ttl: 3600
    columns:
      - name: scan_id          # declared only to carry visible_to; replaced at parse
        visible_to: [analyst, admin]
    dq_contract: |
      dataset: provisa/sales/orders
      columns:
        - name: customer_id
          checks:
            - missing:
                threshold:
                  metric: percent
                  must_be_less_than: 1
      checks:
        - row_count:
            must_be_greater_than: 0
```

注册过程从该文本中派生出的内容：

- **血缘。** 契约本身已经命名了其目标数据集，因此注册过程会按照 `extract_inputs` 解析 SQL 的方式（REQ-939）解析它，并将其解析到已被治理的表。只有一份定义，不存在可能发生偏差的第二份副本。命名了一个未受治理数据集的契约会在注册时直接报错失败，而不会落地任何无人要求的行。
- **列。** 结果信封（envelope）是检查器自身的，而不是运营方的 — 16 个内置列，从 `scan_id` 到 `diagnostics`。已声明的列仅会被读取其 `visible_to`，该值必须一致，随后会被替换。[tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **水位线。** `scan_time` 成为水位线，这使得落地成为一次追加操作（REQ-982）。扫描历史在没有历史子系统的情况下不断累积。
- **提升。** `freshness_max_timestamp` 和 `dataset_rows_tested` 从 `diagnostics` jsonb 中被提升为有类型的列（REQ-119）。可以像对待其他任何 jsonb 列一样添加更多列。[tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

时机（timing）不引入任何新字段。`change_signal` 加上 `cache_ttl` 给出轮询节奏；`mv_debounce_quiet` 和 `mv_debounce_max_delay` 将上游的一次突发合并为一次扫描（REQ-963）；日历粒度使其成为周期性的（REQ-962）；`expected_events` 会等待其输入在窗口内变新鲜后再执行扫描（REQ-961）。轮询循环就是扫描调度器。

`outcome` 是 `pass`、`fail`、`warn`、`error`、`skipped` 之一。它们都不是一个裁定 — 如果需要强制执行，那是之后一个独立的声明：一个 preflight 或一个建立在落地结果之上的 MV。由于一次落地的观测不承担确定性义务（REQ-964），因此这里可以接受一些在 preflight 门控上永远无法存在的非确定性检查 — 异常分数、尾随窗口变化、相对于当前时刻的新鲜度。

契约在 UI 中编写，位于表编辑界面的数据质量面板中，那里的契约原文始终是事实来源。试运行（dry run）会针对实时表执行该契约并显示结果而不落地它们 — 这正是你捕获一个数据集名称解析到意外位置、否则只会落地全是通过结果的行的契约的方式。

---

## Custom Connectors (REQ-1177)

原生联邦引擎 — Postgres、DuckDB 和 ClickHouse — 在运营方于 `config/custom_connectors.yaml` 中为某个新数据源类型声明连接器时，获得对该类型的可达性。无需任何代码。[tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

连接器的可扩展性本身早已存在。Trino 引擎长期以来在其自身层面上是可扩展的 — 每种数据源类型对应一个通用的、参数化的 JDBC 连接器，每种类型对应一个目录（catalog）`.properties` 主体，以及 Provisa 自己的自定义 Trino 连接器插件（Splunk、SharePoint、Calcite）。[tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 将这种基于配置的可扩展性带给了这两个原生的、无集群的引擎，此前它们只有一套固定的连接器集合。

该配置文件默认为空。内置连接器覆盖开箱即用的可达性；该文件中的一切都是由运营方自行编写的。[tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] 设置 `PROVISA_CUSTOM_CONNECTORS` 以指向一个不同的路径（对测试很有用）。

### Descriptor kinds

| Engine | Kind | Mechanism | What the descriptor supplies |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED（ISO 标准） | `extension`、`server_options`、`user_mapping`、`supports_import`、`table_options`、`remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`、`probe_symbol`、`attach_template`、`remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + 扫描视图 | `extension`、`probe_symbol`、`scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…`（自动暴露每张远程表） | `ch_engine`、`engine_template` |
| `clickhouse` | `clickhouse_table` | 每表一次 `CREATE TABLE ENGINE=…`（列来自注册表） | `ch_engine`、`engine_template`（可携带 `{table}`） |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`，由 ClickHouse 推断 schema | `ch_engine`、`engine_template` |

**Postgres 是通用的。** SQL/MED 是一项 ISO 标准，因此每个符合标准的 FDW 都共享相同的 DDL 形式：`CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`，可选的 `CREATE USER MAPPING`，随后要么是 `IMPORT FOREIGN SCHEMA`（当 `supports_import: true` 时），要么是对每张表执行显式的 `CREATE FOREIGN TABLE`（当为 `false` 时）。一个 `pg_fdw` descriptor 只提供各 FDW 之间的差异部分 — 扩展名称、server option 键、user-mapping 键、import 标志、table option。因此任何符合标准的 FDW 都可以仅凭配置驱动。[tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB 支持两种机制。** 通过 ATTACH 暴露目录的扩展使用 `duckdb_attach`；暴露一个只读 table-function 的扩展使用 `duckdb_scan`。不符合任一模式的扩展不受支持。[tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse 支持三种机制**，各对应一种集成引擎（integration-engine）形态：自动暴露每张远程表的关系型 DATABASE 引擎（`clickhouse_database`，例如 Redis/MySQL）、列由注册表提供的按表引擎（`clickhouse_table`，例如 JDBC/ODBC 桥接 — `engine_template` 可携带一个由运行时绑定的 `{table}` 占位符），以及由 ClickHouse 推断 schema 的文件/湖/URL 引擎（`clickhouse_scan`，例如 HDFS/URL）。SQLite（DATABASE 引擎、文件、无服务器）和 Hudi（湖仓一体、零拷贝）开箱即用。[tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

一个未知的 `kind` 值会在启动时直接报错失败 — 一个 descriptor 拼写错误绝不能默默地使某个数据源类型不可达。[tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Probe gating

可用性在 attach 时针对每个引擎的标准发现目录进行验证：

- **Postgres** — 检查 `pg_extension`，然后是 `pg_available_extensions`。[tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — 运行 `INSTALL`/`LOAD`，并检查 `duckdb_functions()` 中是否存在所声明的 `probe_symbol`。[tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — 检查 `system.table_engines` 中是否存在所声明的 `ch_engine`；构建版本中不存在则直接报错失败。[tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

一个已声明但无法安装的扩展会直接报错失败。没有静默跳过，没有回退。探测失败的连接器在该部署中就是未激活状态。

### Template variables

每个 `server_options` 值、`user_mapping` 值、`attach_template` 和 `scan_template` 都可以使用 `{field}` 占位符。可用字段：[tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`、`{host}`、`{port}`、`{database}`、`{username}`、`{password}`、`{path}`、`{schema_name}`、`{table_name}`，以及来自 `federation_hints` 的任意键。DuckDB 的 attach 模板还会收到 `{alias}` — Provisa 为已挂载数据库分配的内部目录别名。

引用未知字段的模板会在 attach 时直接报错失败，从而在损坏的 DDL 到达引擎之前，就暴露出 descriptor 与数据源之间的不匹配。

### Examples

**Postgres — 通过 `mongo_fdw` 访问 MongoDB（无 schema 导入；按表提供列）**

```yaml
# config/custom_connectors.yaml
connectors:
  - engine: postgres
    source_type: mongodb
    kind: pg_fdw
    extension: mongo_fdw
    mechanism: attach_r
    server_options:
      address: "{host}"
      port: "{port}"
    user_mapping:
      username: "{username}"
      password: "{password}"
    supports_import: false
    table_options:
      database: "{database}"
      collection: "{table_name}"
```

**DuckDB — 通过 `read_xlsx` 访问 Excel 文件（scan table-function）**

```yaml
  - engine: duckdb
    source_type: xlsx
    kind: duckdb_scan
    extension: excel
    install_from_community: false
    probe_symbol: read_xlsx
    scan_template: "read_xlsx('{path}')"
```

[tool-verified: `config/custom_connectors.yaml` commented examples, lines 26–50]

有了任一 descriptor 之后，注册一个具有所声明 `source_type` 的数据源就会通过该自定义连接器路由，前提是探测成功。不需要任何其他配置变更。

---

## Warehouses as Named Sources {: #warehouses-as-named-sources }

Snowflake、Databricks 和 ClickHouse 可以注册为命名数据源，与哪个联邦引擎处于活动状态无关。[tool-verified: `executor/drivers/snowflake.py`（REQ-988）, `executor/drivers/databricks.py`（REQ-987）, `executor/drivers/clickhouse.py`（REQ-986）]

注册后，Provisa 通过该数据源的 DirectDriver 读取仓库数据，并将副本落地到活动引擎的物化存储中。随后查询针对该副本执行。这与传统的直连型路径（asyncpg、aiomysql）不同，后者完全绕过了引擎 — 而这里，引擎仍然执行查询，但针对的是本地副本，而不是每次请求都通过网络连接到仓库。

在仓库支持的情况下，读取采用 Arrow 原生方式：Databricks 使用 Cloud Fetch，Snowflake 使用 `fetch_arrow_table`，ClickHouse 使用原生列式 HTTP 接口。

标准 `host`/`port`/`username`/`password` 字段无法承载的扩展连接参数放在 `federation_hints` 中：

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required — the SQL Warehouse connection detail

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount    # required — Snowflake account identifier
      warehouse: COMPUTE_WH       # optional — virtual warehouse to use
      role: PROVISA_ROLE          # optional — Snowflake role

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    database: analytics
    username: default
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"              # optional — enables TLS on the HTTP interface
```

将某个仓库注册为命名数据源，与是否选择该同一仓库作为联邦引擎相互独立。一个基于 DuckDB 引擎的 Snowflake 数据源会将副本落地到 DuckDB 中，而不是 Snowflake 中。

云端对象/湖数据（S3 / GCS / R2 上的 parquet、csv、iceberg、delta_lake 文件）是一种独立的数据源类型，当活动引擎为该类型拥有 ATTACH 连接器时，会就地挂载。不会落地任何副本 — 引擎直接扫描对象存储。这些数据源的凭据同样放在 `federation_hints` 中：

```yaml
sources:
  - id: r2-events
    type: parquet
    path: s3://my-bucket/events/2026/*.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}     # Cloudflare R2 account (S3-compatible)
```

---

## Source Configuration Fields

所有数据源共享一组公共字段。[tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Yes | — | 唯一标识符；字母数字加连字符/下划线 |
| `type` | Yes | — | 数据源类型（参见上方各表） |
| `host` | No | `""` | 主机名或 IP |
| `port` | No | `0` | 端口号 |
| `database` | No | `""` | 数据库名称 |
| `username` | No | `""` | 用户名 |
| `password` | No | `""` | 密码；使用 `${env:VAR}` 或 `${secret:NAME}` 而非字面值（见下文） |
| `path` | No | `null` | 基于文件及对象/湖类数据源的文件路径或云 URI |
| `base_url` | No | `null` | OpenAPI 数据源的基础 URL |
| `pool_min` | No | `1` | 最小连接池大小（REQ-052） |
| `pool_max` | No | `5` | 最大连接池大小（REQ-052） |
| `use_pgbouncer` | No | `false` | 通过 PgBouncer 路由连接（REQ-053） |
| `pgbouncer_port` | No | `6432` | PgBouncer 端口（REQ-053） |
| `cache_enabled` | No | `true` | 启用 API 响应缓存 |
| `cache_ttl` | No | `null` | 缓存 TTL（秒）；为 null 时继承全局默认值 |
| `cache_catalog` | No | `null` | API 缓存所用的联邦目录；默认为该数据源自身的目录 |
| `cache_schema` | No | `api_cache` | 缓存目录内的 schema |
| `naming_convention` | No | `null` | 覆盖该数据源的全局命名约定（REQ-194） |
| `federation_hints` | No | `{}` | 传递给联邦引擎的会话属性，以及仓库类数据源的扩展连接参数（REQ-278, REQ-281） |
| `mapping` | No | `{}` | NoSQL 和 SaaS 数据源的类型特定连接器设置（例如 SharePoint 的 `auth_type`、Salesforce 的 `auth_type`、Splunk 的 `use_token`）（REQ-251） |
| `allowed_domains` | No | `[]` | 将数据源限制到特定域；空 = 不限制 |
| `description` | No | `""` | 人类可读的描述 |

---

## Kafka Sources

Kafka 主题在 `kafka_sources` 下单独配置，以已注册的 `kafka` 数据源的 `id` 作为键。[tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

```yaml
kafka_sources:

  - id: kafka-support
    topics:

      - id: tickets
        topic: support.tickets
        domain_id: sales-analytics
        description: "Inbound support tickets"
        default_window: 1h
        columns:

          - name: id
          - name: subject
          - name: status
          - name: created_at
```

| Field | Description |
| ------- | ------------- |
| `id` | 必须匹配某个 `type: kafka` 数据源的 `id` |
| `topics[].id` | 该主题在 Provisa 内的逻辑名称 |
| `topics[].topic` | Kafka 主题名称 |
| `topics[].domain_id` | 该主题所属的域 |
| `topics[].description` | 人类可读的描述 |
| `topics[].default_window` | 窗口查询的默认时间窗口（例如 `1h`）（REQ-148） |
| `topics[].columns` | 该主题 schema 的列定义（REQ-150） |

---

## Column Visibility

每列上的 `visible_to` 字段是一个可以看到该列的角色 ID 列表。[tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

从某个角色的 `visible_to` 列表中省略的列不会出现在该角色的 GraphQL schema 中，也无法被查询或在过滤条件中引用（REQ-039）。

---

## Relationships

关系（Relationship）连接两张已注册的表，并作为嵌套字段出现在 GraphQL 中。[tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `id` | Yes | 该关系的唯一标识符 |
| `source_table_id` | Yes | 持有外键的表 |
| `target_table_id` | Yes | 被引用的表；对于计算型关系为空 |
| `source_column` | Yes | 源表上的列 |
| `target_column` | Yes | 目标表上的列；对于计算型关系为空 |
| `cardinality` | Yes | `many-to-one` 或 `one-to-many`（REQ-019） |
| `materialize` | No | 为跨数据源联接自动创建一个物化视图（REQ-158）。在一个由中间表（junction）支持的边上，该视图覆盖的是两跳遍历，而不是一次直接联接（REQ-1586） |
| `refresh_interval` | No | MV 刷新间隔，单位秒（默认：300） |
| `target_function_name` | No | 计算型关系的数据库函数名 |
| `function_arg` | No | 哪个函数参数接收源列的值 |
| `alias` | No | 人类可读的关系类型（例如 `WORKS_FOR`） |
| `graphql_alias` | No | 命名该关系在父类型上暴露的 SDL 字段。缺省时，该名称从目标表的 `field_name` 和关系基数派生而来。[tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | No | 为 `true` 时，将该关系从 Cypher 图边中排除 |
| `source_json_key` | No | 在 JOIN 之前，从源列中提取该键作为一个 JSON 对象 |
| `via_table` | No | 该边所遍历的中间表（junction）的已注册表名。设置该字段会使该边成为由中间表支持的边；留空则使其成为一条外键边（REQ-1586） |
| `via_source_column` | No | 与 `source_column` 配对的中间表列。复合键时使用逗号分隔并按位置对应 |
| `via_target_column` | No | 与 `target_column` 配对的中间表列 |
| `via_type_column` | No | 判别列，当一个中间表携带多种关系类型时使用 |
| `via_type_value` | No | 该边所固定绑定的判别值 |
| `via_label_source` | No | 由哪种提名方式命名该 Cypher 类型：`column`（判别值）、`table`（中间表的表名）或 `fixed`（已声明的别名）。全部转换为大写下划线形式 |

### Junction-backed relationships

一张关联表可以被声明为一个一等公民的 Cypher 关系，而不是一个节点，从而使其
自身的列成为该关系的属性：（REQ-1586）

```yaml
relationships:

  - id: pets-bonded-pair
    source_table_id: pets
    target_table_id: pets
    source_column: id
    target_column: id
    cardinality: one-to-many
    via_table: pet_companions
    via_source_column: pet_id
    via_target_column: companion_pet_id
    via_type_column: relation_type
    via_type_value: bonded pair
    via_label_source: column
```

该中间表是像其他任何表一样的已注册表，必须先注册才能被一个关系
命名。每个判别值声明一次：对 `pet_companions` 表的三行声明会产生
`BONDED_PAIR`、`LITTERMATE` 和 `SHARES_ENCLOSURE` 三个不同的 Cypher 类型，每个都携带
该中间表行的其余列作为边属性。随附的演示配置正是这样声明的。

一条中间表边是一个 Cypher 关系，而不是一个 GraphQL 联接字段：GraphQL 联接生成器
为单一列对构建其 `ON` 子句，没有位置容纳第二跳，因此中间表边会从生成的 SDL 中
以及从 `pg_constraint` 中被排除。[tool-verified: `provisa/compiler/schema_gen.py:304`]
该中间表本身仍可作为自己的根字段被查询，并从 Cypher 图 schema 的节点一侧被移除，
因此它永远不会作为节点标签出现。

在一条中间表边上，`materialize: true` 依然有效，它所物化的是一次遍历，而不是
一次直接的 `pets` 到 `pets` 的联接：该视图保存了源端一跳、中间表一跳、判别值，
以及中间表自身的列与目标端的列并列在一起。由于该中间表是联接的第三条腿，
该边是否跨数据源，要在全部三张表之间判断 — 一个与其所链接的两张表不在同一
数据源中的中间表，即使那两张表一致，也会被物化。一次声明只物化一种
边类型，因此为 `bonded pair` 构建的视图永远无法回答 `littermate` 的遍历。

基数取值 [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]：

- `many-to-one` — 每个源行映射到一个目标行（外键指向主键）
- `one-to-many` — 每个源行映射到多个目标行（上一种的逆关系）

---

## Row-Level Security Rules

RLS 规则在查询时注入 `WHERE` 子句，作用范围限定在某个角色，并可选地限定在某张表或某个域上。[tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

当同一个角色同时存在域级规则和表级规则时，表级规则优先（REQ-403）。

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `table_id` | Conditional | 该规则适用的表；与 `domain_id` 互斥 |
| `domain_id` | Conditional | 该规则适用的域；适用于该域中的所有表（REQ-402） |
| `role_id` | Yes | 该规则适用的角色 |
| `filter` | Yes | 注入到 `WHERE` 中的 SQL 谓词；可以引用会话变量（REQ-041） |

---

## Functions and Webhooks

### DB Functions

追踪一个数据库函数，并将其暴露为一个 GraphQL 查询或 mutation。[tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

数据库数据源还可以从供应商目录（`pg_proc`、`information_schema.routines`，或供应商对应物）中自动发现其存储过程和函数，从而无需手动注册每一个。发现过程读取 `prokind` 和 `provolatile`：不可变/稳定函数注册为参数化关系（过程参数成为查询参数，与 OpenAPI GET 表的形式相同），而 volatile 存储过程注册为 mutation/被追踪函数。被发现的例程与手动注册的例程一样，流经第二阶段治理。[tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

```yaml
functions:

  - name: get_customers_by_region
    source_id: sales-pg
    schema: public
    function_name: get_customers_by_region
    returns: customers
    domain_id: sales-analytics
    description: "Returns customers filtered by region"
    visible_to: [admin, analyst]
    kind: query
    arguments:

      - name: p_region
        type: String
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `name` | Yes | — | GraphQL 字段名 |
| `source_id` | Yes | — | 包含该函数的数据源 |
| `schema` | No | `public` | 数据库 schema |
| `function_name` | Yes | — | 实际的数据库函数名 |
| `returns` | Yes | — | 该函数返回的已注册表 ID（REQ-207） |
| `arguments` | No | `[]` | `{name, type}` 参数定义列表（REQ-211） |
| `visible_to` | 否 | `[]` | 该函数分配给的角色（为空：所有角色）。`mutation` 另外需要 `write` 权利 |
| `domain_id` | No | `""` | 该函数所属的域 |
| `description` | No | `null` | GraphQL 字段描述 |
| `kind` | No | `mutation` | `"query"` 或 `"mutation"`（REQ-205） |

### Webhooks

将一个外部 HTTP 端点暴露为一个 GraphQL 查询或 mutation。[tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

```yaml
webhooks:

  - name: notify_support
    url: http://localhost:9999/notify
    method: POST
    timeout_ms: 3000
    domain_id: sales-analytics
    description: "Send a support notification"
    visible_to: [admin]
    kind: mutation
    arguments:

      - name: message
        type: String
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `name` | Yes | — | GraphQL 字段名 |
| `url` | Yes | — | Webhook 端点 URL |
| `method` | No | `POST` | HTTP 方法 |
| `timeout_ms` | No | `5000` | 请求超时时间，单位毫秒 |
| `returns` | No | `null` | 已注册表 ID，或 null 表示内联类型 |
| `inline_return_type` | No | `[]` | 自定义返回结构的 `{name, type}` 字段列表（REQ-210） |
| `arguments` | No | `[]` | `{name, type}` 参数定义列表 |
| `visible_to` | No | `[]` | 可以调用该 webhook 的角色 |
| `domain_id` | No | `""` | 该 webhook 所属的域 |
| `description` | No | `null` | GraphQL 字段描述 |
| `kind` | No | `mutation` | `"query"` 或 `"mutation"` |

---

## Authentication

身份验证在 `auth` 键下配置。[tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Provider | Description |
| ---------- | ------------- |
| `none` | 不进行身份验证；所有请求都被视为 `default_role` |
| `firebase` | Firebase Authentication；需要 `project_id` 和 `service_account_key`（REQ-121） |
| `keycloak` | Keycloak OIDC（REQ-122） |
| `oauth` | 通用 OAuth 2.0（REQ-123） |
| `simple` | 不依赖外部提供程序的用户名/密码方式（REQ-124） |

```yaml
auth:
  provider: firebase
  assignments_source: provisa   # "claims" or "provisa"
  default_role: analyst
  default_assignments:

    - role_id: analyst
      domain_id: "*"
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT_KEY}
```

`assignments_source: claims` 从 JWT claims 中读取角色分配。`assignments_source: provisa` 从 Provisa 自身的分配存储中读取。[tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Execution Routing

**直接执行** — 单数据源 RDBMS 查询路由到原生驱动，以获得亚 100 毫秒的延迟（REQ-027）。数据源需要同时具备 `SOURCE_TO_DIALECT` 条目和 `SOURCE_TO_CONNECTOR` 条目才能支持该路径（REQ-229）。

**联邦执行** — 多数据源查询以及没有直连驱动的数据源通过联邦引擎路由（REQ-028）。Provisa 内置了一个嵌入式联邦引擎；对于大规模部署，可指向你自己兼容的集群（REQ-226）。

**统计信息** — 在注册时，Provisa 会针对每张已发布的表运行 `ANALYZE`，以初始化基于成本的优化器（行数、null 比例、不同值数量、最小/最大值）。失败会被记录且不会阻止注册（REQ-275）。

---

## Graph & Semantic Sources

### Neo4j

将一个 Neo4j 图数据库注册为一个可查询的数据源。数据管家编写投影标量值的 Cypher 查询；Provisa 缓存结果并将其暴露为 GraphQL 类型（REQ-295）。

Cypher 查询必须在 `RETURN` 子句中使用属性访问器（`RETURN n.id AS id, n.name AS name`）— 返回节点对象会在注册时被拒绝（REQ-296）。

#### Config-file registration (REQ-1668)

在 YAML 中声明一个 `neo4j` 数据源及其表。每张表都需要一个 `query_template`（产生其行数据的 Cypher）以及有类型的列。`query_template` 键在任何其他数据源类型下都是非法的。[tool-verified: `provisa/core/config_loader.py:456-511`]

一个数据源需要 `host`、`port` 和 `database`。[tool-verified: `provisa/core/config_loader.py:456-468`] 行数据通过向 `/db/<database>/tx/commit`（Neo4j HTTP 事务 API）POST `{"statements": [{"statement": <cypher>}]}` 来获取。响应中的 `errors` 列表非空会被视为查询失败，而不是空结果。[tool-verified: `provisa/neo4j/source.py:71-82`, `provisa/api_source/caller.py:344-347`, `provisa/api_source/normalizers.py:49-74`]

每一列都需要 `data_type`。加载器将配置类型映射到查询时使用的 API 列类型 [tool-verified: `provisa/neo4j/persist.py:31-64`]：

| Config `data_type` | API type |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` 和 `decimal(10,2)` 都是可接受的 — 使用括号之前的基础类型。

注册会持久化一条 `api_sources` 行以及每张表一条 `api_endpoints` 行，因此这些表在重启后依然存在，无需重新读取配置文件。`/admin/sources/neo4j` 下的 admin REST 端点写入的是相同的行。[tool-verified: `provisa/neo4j/persist.py:77-120`]

```yaml
sources:
  - id: graph
    type: neo4j
    host: neo4j
    port: 7474
    database: neo4j
    cache_ttl: 300

tables:
  - source_id: graph
    schema: neo4j
    table: person_skills
    query_template: >-
      MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
      RETURN p.name AS name, s.skill AS skill, p.experience AS years
    columns:
      - name: name
        data_type: varchar
      - name: skill
        data_type: varchar
      - name: years
        data_type: integer
```

#### Register Table in the UI (REQ-1670)

一个 neo4j 数据源没有可列出的表，因此 Register Table 表单会要求输入表，而不是提供一个供选择的表。[tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx`（`isNeo4j`）]

1. 选择 neo4j 数据源和一个域。schema 和表选择器、discover 复选框以及水位线选择器都不会出现；该数据源永远不会被内省。
2. 输入一个表名和 Cypher。该 Cypher 必须投影标量值（`RETURN a.name AS name`）；返回节点或列表的投影会被报告为错误。
3. 点击 Preview。该表单通过 `neo4jPreview` GraphQL 查询以 `LIMIT 5` 运行该 Cypher，并根据返回的行填充列列表，类型为 `text`、`integer`、`double`、`boolean` 或 `json`。[tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] 预览失败会将该 Cypher 保留在编辑器中并显示错误信息。
4. 与其他任何表一样调整可见性、别名或脱敏设置，然后注册。在预览为各列定型之前，该表单会拒绝提交，并且服务器会拒绝一张不携带任何 Cypher 的 neo4j 表（`schema.neo4j_query_required`）。[tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

该 Cypher 与表一同以 `queryTemplate` 的形式存储，显示在该表的只读视图中，其持久化方式与通过配置文件注册完全相同：一条 `api_sources` 行和一条下次启动时会被水合的 `api_endpoints` 行。编辑该表会重新持久化经过编辑的 Cypher。

#### Admin REST registration

```bash
# Register via admin API (no YAML config required)
POST /admin/sources/neo4j
{
  "source_id": "graph",
  "host": "neo4j",
  "port": 7474,
  "database": "neo4j"
}

# Register a table (preview + validate before persisting)
POST /admin/sources/neo4j/graph/tables
{
  "table_name": "person_skills",
  "cypher": "MATCH (p:Person)-[:HAS_SKILL]->(s:Skill) RETURN p.name AS name, s.skill AS skill, p.experience AS years",
  "ttl": 300
}
```

预览端点（`POST /admin/sources/neo4j/{id}/preview`）返回样本行，如果该 Cypher 返回节点对象则会阻止注册（REQ-296）。

### SPARQL

将任何符合 SPARQL 1.1 标准的三元组存储（Apache Jena Fuseki、Virtuoso、Stardog 等）注册为一个可查询的数据源（REQ-297）。

查询必须是 `SELECT` 查询。`SELECT` 子句中的变量名会自动成为列名（REQ-297）。

```bash
# Register via admin API
POST /admin/sources/sparql
{
  "source_id": "knowledge-graph",
  "endpoint_url": "http://fuseki:3030/ds/sparql",
  "default_graph_uri": "http://example.org/graph"
}

# Register a table (executes LIMIT 5 probe to validate and infer columns)
POST /admin/sources/sparql/knowledge-graph/tables
{
  "table_name": "product_categories",
  "sparql_query": "SELECT ?product ?label ?category WHERE { ?product a :Product ; rdfs:label ?label ; :hasCategory ?category . }",
  "ttl": 600
}
```

两个连接器都使用 API 数据源缓存管道 — 结果存储在 PostgreSQL 中，TTL 可配置，从而可用于跨数据源的联邦 JOIN（REQ-295, REQ-297, REQ-299）。

---

#### Config-file and UI registration (REQ-1683)

一个 `sparql` 数据源的 `host` 就是其 SPARQL 端点 URL（Sources 表单以同样的方式存储它）。其下的每张表都携带 `query_template`，即一个变量即为列的 SELECT 语句；每个绑定都是 `text`。[tool-verified: `provisa/core/config_loader.py` `_validate_neo4j_sources`, `_handle_sparql_table`]

```yaml
sources:
- id: sparql-demo
  type: sparql
  host: http://localhost:23030/provisa/query
tables:
- source_id: sparql-demo
  domain_id: shelter
  schema: sparql
  table: volunteer
  query_template: >-
    PREFIX s: <http://provisa.dev/shelter#>
    SELECT ?volunteer_id ?name WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name }
  columns:
  - { name: volunteer_id, data_type: text, visible_to: [org_admin] }
  - { name: name, data_type: text, visible_to: [org_admin] }
```

Register Table 的工作方式与 Neo4j 相同：选择数据源，输入一个表名和 SELECT 语句，点击 Preview（`sparqlPreview` 查询以 `LIMIT 5` 运行该语句并填充列列表），然后注册。注册过程会持久化一条 `api_sources` 行和一条 `api_endpoints` 行（对该端点路径进行表单编码的 POST，`sparql_bindings` normalizer），与配置文件注册写入的行相同，原生引擎通过与 Neo4j 相同的获取链路落地这些行。[tool-verified: `provisa/api/admin/_query_api_registration.py`, `provisa/sparql/persist.py`]

## Connection Examples

### PostgreSQL

```yaml
- id: sales-pg
  type: postgresql
  host: postgres
  port: 5432
  database: provisa
  username: provisa
  password: ${env:PG_PASSWORD}
```

### Snowflake

```yaml
- id: analytics-sf
  type: snowflake
  host: org.snowflakecomputing.com
  port: 443
  database: ANALYTICS
  username: svc_provisa
  password: ${env:SNOWFLAKE_PASSWORD}
  federation_hints:
    account: myorg-myaccount
    warehouse: COMPUTE_WH
```

### Databricks

```yaml
- id: lakehouse-db
  type: databricks
  host: my-workspace.azuredatabricks.net
  password: ${env:DATABRICKS_TOKEN}
  federation_hints:
    http_path: /sql/1.0/warehouses/xxxx
```

### MongoDB

```yaml
- id: reviews-mongo
  type: mongodb
  host: mongodb
  port: 27017
  database: provisa
  username: ""
  password: ""
```

### Cross-Source Query

```graphql
{
  orders(where: {region: {eq: "us"}}) {
    id
    amount
    customers {       # PostgreSQL
      name
      email
    }
    productReviews {  # MongoDB (federated)
      rating
      comment
    }
  }
}
```

单数据源部分直接路由（REQ-027）。跨数据源 JOIN 联邦执行，并自动进行类型强制转换（REQ-028, REQ-552）。
