# 数据源类型

## 执行模型

每个查询最终都通过联邦引擎执行，联邦引擎提供跨所有数据源的联邦能力。数据源根据其连接方式分为三类。[tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| 类别 | 拥有直连驱动 | 拥有联邦连接器 | 示例 |
| --- | --- | --- | --- |
| **直连型** | 是 | 是 | PostgreSQL、MySQL、MariaDB、SingleStore、SQL Server、Oracle、DuckDB |
| **仅联邦型** | 否 | 是 | Redshift、Druid、Exasol、Hive、Iceberg、Delta Lake、Hive（S3 支持） |
| **直读型（副本）** | 是 | 是 | Snowflake、Databricks、ClickHouse — 驱动读取数据并落地一份副本；查询在活动引擎中针对该副本执行 |
| **物化 → 联邦** | 否 | 否 | REST/OpenAPI、远程 GraphQL、gRPC、Neo4j Cypher、SPARQL、WebSocket、RSS、CSV、SQLite、Parquet、Ingest（推送接收器）、GovData、SharePoint、Splunk |

**直连型**数据源通过其原生驱动执行单数据源查询（亚 100 毫秒），绕过联邦引擎（REQ-027、REQ-229）。它们保留完整的连接器支持，并在与其他数据源联接时参与联邦（REQ-028）。

**仅联邦型**数据源始终通过联邦层查询。不存在直连驱动（REQ-229）。

**直读型（副本）**数据源拥有一个 DirectDriver，可原生读取仓库数据（在可用的情况下采用 Arrow 原生方式），将副本落地到活动引擎的物化存储中，随后查询针对该副本执行。参见[作为命名数据源的仓库](#warehouses-as-named-sources)。

**物化**数据源没有联邦连接器。Provisa 获取其数据（在启动时或查询时），并将其缓存为 S3 或 PostgreSQL 中的 Parquet，使联邦引擎可以进行跨数据源查询（REQ-309）。

---

## 所有数据源

Provisa 注册了 **54** 种数据源类型。下表覆盖全部 54 种；索引即为计数。[tool-verified: `provisa/core/models.py` `SourceType`；Kaggle 虽然内部通过 `files` 连接器注册，但仍作为独立数据源计数]

| # | 分组 | 数据源类型 |
| --- | --- | --- |
| 1–13 | [RDBMS](#rdbms) | `postgresql`、`mysql`、`mariadb`、`singlestore`、`sqlserver`、`oracle`、`duckdb`、`cockroachdb`、`yugabytedb`、`greenplum`、`tidb`、`firebird`、`airport` |
| 14–20 | [云数据仓库](#cloud-data-warehouses) | `snowflake`、`bigquery`、`databricks`、`redshift`、`fabric`、`synapse`、`trino` |
| 21–25 | [Analytics / OLAP](#analytics-olap) | `clickhouse`、`druid`、`exasol`、`elasticsearch`、`pinot` |
| 26–30 | [数据湖 / 开放表格式](#data-lake-open-table-formats) | `iceberg`、`delta_lake`、`hudi`、`hive`、`hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`、`cassandra`、`redis` |
| 34–36 | [流式处理](#streaming) | `kafka`、`websocket`、`rss` |
| 37 | [推送接收器](#push-receiver) | `ingest` |
| 38–39 | [图与语义](#graph-semantic) | `neo4j`、`sparql` |
| 40–43 | [基于文件](#file-based) | `sqlite`、`csv`、`parquet`、`files` |
| 44–45 | [可观测性及其他](#observability-other) | `google_sheets`、`prometheus` |
| 46–47 | [企业级 SaaS](#enterprise-saas-connectors) | `sharepoint`、`splunk` |
| 48–50 | [API 数据源](#api-sources) | `openapi`、`graphql_remote`、`grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [数据质量检查器](#data-quality-checkers-req-1443) | `soda`、`great_expectations` |
| 54 | [Kaggle 数据集](#kaggle-datasets) | Kaggle（通过 Sources 表单暂存；注册为 `files` 数据源 — 参见 [Kaggle 数据集](#kaggle-datasets)） |

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

线协议兼容的数据库复用基础线协议的 JDBC 驱动、原生异步驱动和方言 — CockroachDB、YugabyteDB 和 Greenplum 复用 PostgreSQL 线协议；TiDB 复用 MySQL 线协议。它们只需要注册表条目，无需新的连接器代码。[tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`、`_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird`（Firebird 3/4/5）和 `airport`（Arrow Flight 服务器）是已注册的数据源类型，当 DuckDB 为活动引擎时，通过 DuckDB 社区扩展就地访问 — 没有直连驱动，也没有联邦连接器。[tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Cloud Data Warehouses {: #cloud-data-warehouses }

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Source Type | Direct Driver | Connector Name | Dialect | Mutations | Notes |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federated | 通过 snowflake-connector-python 读取；落地副本；`federation_hints` 中的 `account`/`warehouse`/`role`（REQ-988） |
| `bigquery` | — | bigquery | bigquery | Federated | 没有 DirectDriver；通过联邦引擎或 BigQuery 引擎 ATTACH 访问 |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federated | 通过 databricks-sql-connector 读取（Cloud Fetch、Arrow）；落地副本；`federation_hints` 中必须提供 `http_path`（REQ-987） |
| `redshift` | — | redshift | redshift | Federated | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federated | Microsoft Fabric Warehouse；基于 TDS 的 T-SQL，Azure AD 身份验证；落地副本（REQ-995） |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federated | Azure Synapse SQL；基于 TDS 的 T-SQL，Azure AD 身份验证；落地副本（REQ-995） |
| `trino` | SQLAlchemyDriver | — | — | Federated | 通过 SQLAlchemy trino 方言读取远程 Trino/Presto 协调器；在任意引擎上落地副本（REQ-994） |

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

### Streaming {: #streaming }

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | 联邦 Kafka 连接器；通过 Confluent Schema Registry（Avro、Protobuf、JSON Schema）、手动定义或样本推断获取 schema（REQ-147, REQ-150） | 仅接收端（REQ-176） |
| `websocket` | 外部 WebSocket 数据流 — 连接、订阅、接收事件；结果会被物化（REQ-338） | No |
| `rss` | RSS 2.0 / Atom 订阅源 — 轮询、按 pubDate/updated 设置水位线；结果会被物化（REQ-342, REQ-343） | No |

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

有两种机制覆盖文件类数据源。两者都使用 `path` 字段而非 `host`/`port`。[tool-verified: `provisa/core/models.py`] (REQ-553)

**单文件数据源** — `sqlite`、`csv`、`parquet` 将 `path` 指向单个文件。

| Source Type | Transports | Mutations |
| --- | --- | --- |
| `sqlite` | local | Yes |
| `csv` | local | No |
| `parquet` | local, `s3://` | No |

私有存储桶需要凭据（来自环境变量的 AWS 区域和密钥）。要在 `s3://` 或 `http(s)://` 上使用 CSV，或一次注册多个文件，请使用 `files` 数据源。[tool-verified: `provisa/file_source/source.py`]

**`files` 数据源** — 将 `path` 指向一个通配符（glob），递归遍历该路径，并将该目录注册为一个联邦目录（catalog）中的表集合。它支持多种格式和多种传输方式；下方的集合来自文件连接器（kenstott/calcite fork）。[tool-verified: `provisa/core/catalog.py` `files` 分支及 `provisa/core/models.py` `SOURCE_TO_CONNECTOR`；格式和传输方式列表来自 calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formats | Transports |
| --- | --- |
| CSV、TSV、JSON、YAML、Excel（XLS/XLSX）、Parquet、Arrow，以及转换为表格的文档 — HTML、Markdown、DOCX、PPTX | 本地文件系统、HTTP(S)、`s3://`、`hdfs://`、`ftp://`/`ftps://`、`sftp://`、`iceberg://`、SharePoint（REST 和 Microsoft Graph） |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

在 DuckDB 引擎上，`files` 以原生方式读取 — 在解析出的目录下，为每个 `<table>.csv` 提供一个 `read_csv_auto` 扫描视图（REQ-229）[tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]。在没有自带 `files` 连接器的引擎上，数据行通过 sharepoint/splunk 所使用的同一个连接器自带 Calcite pgwire 服务器（`pgwire-file`）落地（REQ-954）— 参见下方的[企业级 SaaS 连接器](#enterprise-saas-connectors)。端到端 UI 覆盖（Sources 表单 → Register Table → SQL 查询）以及 pgwire 落地路径已在 REQ-1694 中得到验证。

#### Kaggle 数据集（REQ-1780, REQ-1781, REQ-1782, REQ-1783） {: #kaggle-datasets }

Kaggle 是一个文件下载平台。暂存的 Kaggle 数据集会注册为一个 `files` 类型的 Source，并通过任何其他 `files` 数据源所使用的同一个 pgwire-file 连接器进行查询 — 枚举中并不存在 `kaggle` 这个 SourceType。[tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — 没有 `kaggle` 字面量]

**添加数据集。** 打开 Sources → Subscriptions → Kaggle。该表单运行两个连续的步骤，因为 Kaggle 自身的数据集搜索端点需要身份验证 — 选择器必须在令牌验证通过后才能出现。[tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **令牌** — 输入一个 Kaggle API 令牌并点击"Validate"。验证会向 `POST https://www.kaggle.com/api/v1/datasets/create/new` 发送一个空请求体。`401` 表示无效；任何其他响应都表示有效（Kaggle 自身的负载验证会先于任何数据集的创建触发 — 不会持久化任何内容）。[tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **选择器** — 一个即时搜索字段查询 `GET /api/v1/datasets/list`。每个结果显示数据集标题和描述。选择一个，然后点击"Add Dataset"。

点击"Add Dataset"会从 `GET /api/v1/datasets/download/{owner}/{ref}` 下载数据包，将 CSV 和 Parquet 成员解压到 `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>`，并创建**一个** `files` 类型的 Source，其 `path` 指向该根目录。令牌不会在服务器端存储。[tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

添加数据源后，通过常规的 Register Table 界面注册其表。pgwire-file 连接器的递归目录发现机制会将每个文件列为其自身的表 — 与其他任何 `files` 数据源所使用的机制相同（REQ-1690）。（REQ-1783）

**v1 限制。** 包含 `.sqlite` 或 `.db` 文件的数据包会被直接拒绝并给出明确的错误提示。只有 CSV 和 Parquet 文件会被暂存。[tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**表命名。** 每个文件都会落在数据集根目录下自己的 `<file-stem>/` 子目录中。pgwire-file 连接器在经过 SMART_CASING 规范化后，将生成的表命名为 `<subdir>__<stem>`。例如：`StatewiseTestingDetails.csv` 落地在 `statewise_testing_details/StatewiseTestingDetails.csv`，成为表 `statewise_testing_details__statewise_testing_details`。对于单文件数据集，这种重复的词干是预期行为。多文件数据集会为每个文件生成一对：`orders__orders`、`customers__customers`。（REQ-471）

**刷新。** 要在 Kaggle 发布新版本后重新获取数据集，请调用 `refreshKaggleSource` GraphQL mutation，并提供数据源 ID 和有效令牌。这会就地重新暂存文件，并清除 pgwire-file 端点缓存，使连接器在下一次查询时能获取到任何 schema 变更。创建时存储在 `federation_hints` 中的 `kaggle_owner` 和 `kaggle_ref` 标识了要重新获取的数据集。[tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**没有静态 YAML 配置路径。** Kaggle 数据源仅能通过 Sources 表单创建。导出为 YAML 的 Kaggle 数据源会显示为 `type: files`，并在 `federation_hints` 中带有 `kaggle_owner` 和 `kaggle_ref`。要从 Kaggle 重新下载，需要使用 UI 刷新流程或 `refreshKaggleSource` mutation — 对于隔离网络（air-gapped）环境，替代方案是将 YAML 中的 `path` 指向一个预先暂存好的目录。


### Observability & Other {: #observability-other }

`prometheus` 拥有一个 Trino 连接器（属性由该类型的映射 DSL 构建）。`google_sheets` 是一种已注册的数据源类型，没有 Trino 连接器，通过 API 缓存管道进行物化。[tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Source Type | Connector Name | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialized) | No |
| `prometheus` | prometheus | No | 一个指标即为一张表，一个样本即为一行（`timestamp`、`value`，每个标签一列）；在没有活动连接器的每个引擎上，Provisa 读取 HTTP API — Register Table 使用指标名称和标签，落地时对该表的范围使用 `query_range` [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689) |

### Enterprise SaaS Connectors {: #enterprise-saas-connectors }

SharePoint 和 Splunk 通过 Apache Calcite 连接器（kenstott/calcite fork）注册。两者都没有直连驱动 — Provisa 启动连接器自带的 Calcite pgwire 服务器（`pgwire-sharepoint`、`pgwire-splunk`），并将其作为通用 PostgreSQL 端点访问。在 DuckDB 引擎上，该端点通过 postgres 扩展实时挂载：Register Table 从已挂载的目录中列出连接器的表，查询就地读取该连接器，过滤和投影会下推到 Calcite（REQ-1690）[tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]。在其他所有引擎上，数据行都会落地到物化存储中以用于联邦（REQ-954）。这些程序包按操作系统/架构从固定版本的 `kenstott/calcite` release 中获取（`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`；macOS arm64、Linux x86_64、Windows x86_64）[tool-verified: `provisa/runtime_deps/pgwire_bundles.py`]。两个连接器都始终启用大小写不敏感的名称匹配，以匹配各自产品自身的大小写不敏感语义（REQ-725, REQ-730）。[tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

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

### API Sources {: #api-sources }

将任意 HTTP 端点注册为可查询的表。[tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| API Type | Discovery | Column Inference |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI 规范解析（REQ-314, REQ-316） | 原始类型 → 原生类型，对象 → JSONB |
| `graphql_remote` | Schema introspection（REQ-307, REQ-308） | 原始类型 → 原生类型，对象 → JSONB |
| `grpc_remote` | Server reflection（REQ-322, REQ-325） | 原始类型 → 原生类型，对象 → JSONB |

API 响应会被获取、缓存在 PostgreSQL 中（TTL 可配置），并暴露为 GraphQL 类型（REQ-309, REQ-318, REQ-327）。缓存的表与其他数据源一样参与联邦查询（REQ-313）。

**JSONB 规则**：以 JSONB 存储的复杂列（对象、数组）不可过滤（REQ-119）。子字段访问在 SQL 中使用 `->>` 提取（REQ-151）。表之间的关系通过标量外键列声明 — JSONB blob 列不能作为连接目标。当需要对嵌套字段进行过滤或连接时，使用 JSONB 提升（promotion）将其转换为原生标量列（REQ-119）。

### GovData {: #govdata }

美国政府开放数据。访问权限按主题分组进行划分。[tool-verified: `provisa/core/models.py` lines 543–609]

每个 `govdata` 数据源选择一个主题。该主题决定了暴露哪些 GovData schema。`ref` 和 `geo` schema 始终作为链接 schema 被包含在内 — 它们不按主题单独列出，但始终存在。[tool-verified: `provisa/core/models.py` line 562–563 注释]

| Subject | Schemas Exposed |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`, `fec` |
| `ALL` | 以上所有 schema |

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

### Where a source's password lives

一个数据源的密码永远不会与其连接设置的其余部分存放在一起。控制平面
`sources` 表的行携带一个 `password_ref` 列，其中存放的是一个*引用* —
`${env:PG_PASSWORD}`、`${secret:SNOWFLAKE_KEY}` — 该引用会在数据源被拨号访问的那一刻，
在发起请求所属的组织内部被解析（REQ-1695）。[tool-verified:
`provisa/core/schema_org.py`, `provisa/core/repositories/source.py`]

`${env:VAR}` 读取部署环境的进程环境变量，不需要任何绑定。`${secret:NAME}`
命名某个组织拥有的密钥，因此它只在该组织自身的操作中解析：管理内省接口
以及各个界面所能触达的查询终端都建立了这种绑定。[tool-verified: `provisa/pgwire/_pipeline.py` `_execute_plan`]

引用指向何处取决于该数据源是如何注册的：

- **来自配置文件。** 由你自己编写该引用。`${env:VAR}` 读取部署的进程
  环境变量；`${secret:NAME}` 读取该组织的保管库（参见 [Secrets](secrets.md)）。该
  文件即为记录本身，Provisa 会将该引用原样复制到 `password_ref` 中。
- **来自 Sources 表单。** 在密码字段中输入的引用同样会被原样存储。一个*字面*
  密码会被写入该组织保管库中，键名为
  `source_<id>_password` — 静态加密存储，且永远无法按名称回读 — 该行保留
  引用该密钥的 `${secret:source_<id>_password}`。[tool-verified:
  `provisa/api/admin/schema_common.py` `persist_source_password`]

在已有数据源上重新输入密码，会轮换该唯一保管库条目，而不是创建
第二个。删除该数据源会移除 Provisa 为其铸造的那一个条目，且仅移除那一个：你自己编写的
引用命名的是你出于自身原因所拥有的密钥，因此不会被触碰。
[tool-verified: `provisa/api/admin/schema_mutation.py` `delete_source`]

`password_ref` 不会在环境之间迁移（REQ-1491）。分支环境或复制出的环境
会提供自己的连接值，而引用所指向的保管库属于提供该引用的那个
环境。[tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

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
| `mapping` | No | `{}` | NoSQL 和 SaaS 数据源的类型特定连接器设置（例如 SharePoint 的 `auth_type`、Splunk 的 `use_token`）（REQ-251） |
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
| `visible_to` | No | `[]` | 可以调用该函数的角色 |
| `writable_by` | No | `[]` | 可以将该函数作为 mutation 调用的角色 |
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
