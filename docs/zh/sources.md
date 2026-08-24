# 数据源类型

## 执行模型

每一次查询最终都通过联邦查询引擎执行，该引擎提供跨所有数据源的联邦能力。数据源根据其连接方式分为三类。[tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| 类别 | 是否有直接驱动 | 是否有联邦连接器 | 示例 |
| --- | --- | --- | --- |
| **直连能力型** | 是 | 是 | PostgreSQL、MySQL、MariaDB、SingleStore、SQL Server、Oracle、DuckDB |
| **仅联邦型** | 否 | 是 | Redshift、Druid、Exasol、Hive、Iceberg、Delta Lake、Hive（S3 支持） |
| **直读（副本）型** | 是 | 是 | Snowflake、Databricks、ClickHouse——驱动读取数据并落地一份副本；查询在活动引擎中针对该副本运行 |
| **物化 → 联邦型** | 否 | 否 | REST/OpenAPI、远程 GraphQL、gRPC、Neo4j Cypher、SPARQL、WebSocket、RSS、CSV、SQLite、Parquet、Ingest（推送接收方）、GovData、SharePoint、Splunk |

**直连能力型**数据源通过其原生驱动执行单数据源查询（低于 100 毫秒），绕过联邦查询引擎（REQ-027, REQ-229）。它们保留完整的连接器支持，并在与其他数据源联结时参与联邦查询（REQ-028）。

**仅联邦型**数据源始终通过联邦层查询。不存在直接驱动（REQ-229）。

**直读（副本）型**数据源拥有一个原生读取数据仓库的 DirectDriver（在可用时采用 Arrow 原生方式），将一份副本落地到活动引擎的物化存储中，随后查询针对该副本运行。参见[作为命名数据源的数据仓库](#_15)。

**物化**型数据源没有联邦连接器。Provisa 会（在启动时或查询时）获取其数据，并将其以 Parquet 格式缓存到 S3 或 PostgreSQL 中，使联邦查询引擎能够访问它以进行跨数据源查询（REQ-309）。

---

## 所有数据源

Provisa 注册了 **53** 种数据源类型。下面各表覆盖全部 53 种；序号即为计数。[tool-verified: `provisa/core/models.py` `SourceType`]

| # | 分组 | 数据源类型 |
| --- | --- | --- |
| 1–13 | [关系型数据库（RDBMS）](#rdbms) | `postgresql`、`mysql`、`mariadb`、`singlestore`、`sqlserver`、`oracle`、`duckdb`、`cockroachdb`、`yugabytedb`、`greenplum`、`tidb`、`firebird`、`airport` |
| 14–20 | [云数据仓库](#_4) | `snowflake`、`bigquery`、`databricks`、`redshift`、`fabric`、`synapse`、`trino` |
| 21–25 | [分析型 / OLAP](#olap) | `clickhouse`、`druid`、`exasol`、`elasticsearch`、`pinot` |
| 26–30 | [数据湖 / 开放表格式](#_5) | `iceberg`、`delta_lake`、`hudi`、`hive`、`hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`、`cassandra`、`redis` |
| 34–36 | [流式处理](#_6) | `kafka`、`websocket`、`rss` |
| 37 | [推送接收方](#_7) | `ingest` |
| 38–39 | [图与语义](#_8) | `neo4j`、`sparql` |
| 40–43 | [基于文件](#_9) | `sqlite`、`csv`、`parquet`、`files` |
| 44–45 | [可观测性与其他](#_10) | `google_sheets`、`prometheus` |
| 46–47 | [企业级 SaaS](#saas) | `sharepoint`、`splunk` |
| 48–50 | [API 数据源](#api) | `openapi`、`graphql_remote`、`grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [数据质量检查器](#req-1443) | `soda`、`great_expectations` |

Provisa 支持的每种数据源类型的参考。“直接驱动”表示单数据源查询会针对数据源原生执行（低于 100 毫秒）（REQ-027）。“连接器名称”是该数据源参与多数据源 JOIN 时使用的联邦连接器（REQ-028）。[tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### 关系型数据库（RDBMS）

| 数据源类型 | 直接驱动 | 连接器名称 | 方言 | 变更操作 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | 支持 |
| `mysql` | aiomysql | mysql | mysql | 支持 |
| `mariadb` | aiomysql | mariadb | mysql | 支持 |
| `singlestore` | — | singlestore | singlestore | 联邦 |
| `sqlserver` | aioodbc | sqlserver | tsql | 支持 |
| `oracle` | oracledb | oracle | oracle | 支持 |
| `duckdb` | duckdb | memory | duckdb | 支持 |
| `cockroachdb` | asyncpg（pg wire） | postgresql | postgres | 支持 |
| `yugabytedb` | asyncpg（pg wire） | postgresql | postgres | 支持 |
| `greenplum` | asyncpg（pg wire） | postgresql | postgres | 支持 |
| `tidb` | aiomysql（mysql wire） | mysql | mysql | 支持 |
| `firebird` | — | —（DuckDB 扩展） | — | 不支持 |
| `airport` | — | —（DuckDB 扩展） | — | 不支持 |

线协议兼容的数据库复用某个基础线协议的 JDBC 驱动、原生异步驱动和方言——CockroachDB、YugabyteDB 和 Greenplum 搭乘 PostgreSQL 的线协议；TiDB 搭乘 MySQL 的线协议。它们只需要注册表条目，无需新的连接器代码。[tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird`（Firebird 3/4/5）和 `airport`（Arrow Flight 服务器）是已注册的数据源类型，当 DuckDB 为活动引擎时，通过 DuckDB 社区扩展就地访问——没有直接驱动，也没有联邦连接器。[tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### 云数据仓库

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| 数据源类型 | 直接驱动 | 连接器名称 | 方言 | 变更操作 | 说明 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | 联邦 | 通过 snowflake-connector-python 读取；落地副本；`federation_hints` 中需要 `account`/`warehouse`/`role`（REQ-988） |
| `bigquery` | — | bigquery | bigquery | 联邦 | 无 DirectDriver；通过联邦查询引擎或 BigQuery 引擎 ATTACH 访问 |
| `databricks` | DatabricksDriver | delta_lake | databricks | 联邦 | 通过 databricks-sql-connector（Cloud Fetch、Arrow）读取；落地副本；`federation_hints` 中需要 `http_path`（REQ-987） |
| `redshift` | — | redshift | redshift | 联邦 | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | 联邦 | Microsoft Fabric Warehouse；基于 TDS 的 T-SQL，Azure AD 认证；落地副本（REQ-995） |
| `synapse` | MssqlWarehouseDriver | — | tsql | 联邦 | Azure Synapse SQL；基于 TDS 的 T-SQL，Azure AD 认证；落地副本（REQ-995） |
| `trino` | SQLAlchemyDriver | — | — | 联邦 | 通过 SQLAlchemy trino 方言读取远程 Trino/Presto 协调节点；在任意引擎上落地副本（REQ-994） |

### 分析型 / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| 数据源类型 | 直接驱动 | 连接器名称 | 方言 | 变更操作 | 说明 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | 联邦 | 通过 clickhouse-connect（HTTP）读取；`federation_hints` 中的 `secure: "true"` 用于启用 TLS（REQ-986） |
| `druid` | — | druid | druid | 不支持 | — |
| `exasol` | — | exasol | exasol | 不支持 | — |
| `elasticsearch` | — | elasticsearch | — | 不支持 | 连接器属性来自该类型的映射 DSL [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | 不支持 | Trino `pinot` 连接器；`pinot.controller-urls` = Pinot controller 的 host:port [tool-verified: `trino_connectors.py:199`] |

### 数据湖 / 开放表格式

这些数据源类型仅支持联邦——没有直接驱动，没有方言。[tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| 数据源类型 | 连接器名称 | 时间旅行 | 说明 |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | 支持（`as_of` 参数，REQ-372） | — |
| `delta_lake` | delta_lake | 支持（`as_of` 参数，REQ-372） | — |
| `hive` | hive | 不支持 | — |
| `hudi` | —（ClickHouse `Hudi` 引擎，零拷贝 — REQ-1178） | 不支持 | 无联邦连接器；当 ClickHouse 为活动引擎时就地访问 |
| `hive_s3` | hive | 不支持 | S3 支持的 Hive |

### NoSQL

`mongodb`、`cassandra` 和 `redis` 都有 Trino 连接器（`redis` 的属性由该类型的映射 DSL 构建）。[tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| 数据源类型 | 连接器名称 | 变更操作 |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | 不支持 |
| `cassandra` | cassandra | 不支持 |
| `redis` | redis | 不支持 |

### 流式处理

| 数据源类型 | 机制 | 变更操作 |
| ------------ | ----------- | ----------- |
| `kafka` | 联邦 Kafka 连接器；架构通过 Confluent Schema Registry（Avro、Protobuf、JSON Schema）、手动定义或样本推断获得（REQ-147, REQ-150） | 仅支持写入 sink（REQ-176） |
| `websocket` | 外部 WebSocket 数据流——连接、订阅、接收事件；结果被物化（REQ-338） | 不支持 |
| `rss` | RSS 2.0 / Atom 数据流——轮询，按 pubDate/updated 打水位标记；结果被物化（REQ-342, REQ-343） | 不支持 |

### 推送接收方

| 数据源类型 | 机制 | 变更操作 |
| ------------ | ----------- | ----------- |
| `ingest` | 外部服务通过 POST 发送 JSON 事件；结果被物化（REQ-331, REQ-335） | 不支持 |

### 图与语义

| 数据源类型 | 机制 | 变更操作 |
| ------------ | ----------- | ----------- |
| `neo4j` | 通过 HTTP API 执行 Cypher，结果缓存在 PostgreSQL 中（REQ-295） | 不支持 |
| `sparql` | SPARQL 1.1 POST，结果缓存在 PostgreSQL 中（REQ-297） | 不支持 |

### 基于文件

有两种机制涵盖文件。两者都使用 `path` 字段而不是 `host`/`port`。[tool-verified: `provisa/core/models.py`] (REQ-553)

**单文件数据源** —— `sqlite`、`csv`、`parquet` 将 `path` 指向单个文件。

| 数据源类型 | 传输方式 | 变更操作 |
| --- | --- | --- |
| `sqlite` | 本地 | 支持 |
| `csv` | 本地 | 不支持 |
| `parquet` | 本地、`s3://` | 不支持 |

私有存储桶需要凭据（来自环境的 AWS 区域和密钥）。对于通过 `s3://` 或 `http(s)://` 访问的 CSV，或需要一次注册多个文件时，请使用 `files` 数据源。[tool-verified: `provisa/file_source/source.py`]

**`files` 数据源** —— 将 `path` 指向一个 glob 模式，递归爬取，并将该目录注册为一个联邦表目录。它可以通过多种传输方式读取多种格式；下表列出的能力集来自文件连接器（kenstott/calcite fork）。[tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| 格式 | 传输方式 |
| --- | --- |
| CSV、TSV、JSON、YAML、Excel（XLS/XLSX）、Parquet、Arrow，以及转换为表格的文档——HTML、Markdown、DOCX、PPTX | 本地文件系统、HTTP(S)、`s3://`、`hdfs://`、`ftp://`/`ftps://`、`sftp://`、`iceberg://`、SharePoint（REST 和 Microsoft Graph） |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### 可观测性与其他

`prometheus` 有一个 Trino 连接器（属性由该类型的映射 DSL 构建）。`google_sheets` 是一个已注册的数据源类型，没有 Trino 连接器，通过 API 缓存流水线进行物化。[tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| 数据源类型 | 连接器名称 | 变更操作 |
| ------------ | ----------------- | ----------- |
| `google_sheets` | —（物化） | 不支持 |
| `prometheus` | prometheus | 不支持 |

### 企业级 SaaS 连接器

SharePoint 和 Splunk 通过 Apache Calcite 连接器（kenstott/calcite fork）注册。两者都没有直接驱动——Provisa 通过启动连接器内置的 Calcite pgwire 服务器（`pgwire-sharepoint`、`pgwire-splunk`），以通用 PostgreSQL 终结点方式连接到它，并将行落地到物化存储中以供联邦查询使用（REQ-954）。两个连接器都始终启用不区分大小写的名称匹配，与各自产品自身不区分大小写的语义保持一致（REQ-725, REQ-730）。[tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

SharePoint 列表被枚举为架构，并暴露为可查询的表（REQ-726, REQ-731）。有两种认证方式：`CLIENT_CREDENTIALS`（默认）和基于 PFX 证书的证书认证（REQ-727）。`mapping` 中的密钥值在到达连接器之前会通过密钥引擎解析（REQ-729）。[tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| 数据源字段 | 连接器属性 | 说明 |
| --- | --- | --- |
| `base_url` 或 `host` | `site-url` | SharePoint 站点 URL |
| `username` | `client-id` | Azure 应用客户端 ID |
| `password` | `client-secret` | Azure 应用客户端密钥 |
| `database` | `tenant-id` | Azure 租户 UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS`（默认）或 `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | `auth_type: CERTIFICATE` 时的 PFX 路径 |
| `mapping.certificate_password` | `certificate-password` | PFX 密码 |

当连接器不暴露 `information_schema.columns` 时，通过 `registerTable` 变更操作使用显式列定义（从 Microsoft Graph API 获取）注册该表（REQ-732）。

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

#### `splunk`

Splunk 搜索结果可作为表进行查询（例如 `internal_server`）（REQ-721）。连接器 URL 来自 `base_url`，或按 `https://{host}:{port}` 构造，默认端口为 `8089`（REQ-722）。认证：当 `mapping.use_token` 为 `true`（默认）时，`password` 作为 API 令牌传递；当为 `false` 时，`username` 和 `password` 作为独立凭据传递（REQ-723）。[tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| 数据源字段 | 连接器属性 | 说明 |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | 优先用 `base_url`，否则用 `https://host:port`（端口默认 8089） |
| `password` | `token` 或 `password` | `use_token: true` 时为令牌 |
| `username` | `user` | 仅在 `use_token: false` 时使用 |
| `database` | `app` | 限定到某个 Splunk 应用 |
| `mapping.datamodel_filter` | `datamodel-filter` | 过滤到某个数据模型 |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | 用于自签名证书（REQ-724） |

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

### API 数据源

将任意 HTTP 端点注册为可查询的表。[tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| API 类型 | 发现方式 | 列推断 |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI 规范解析（REQ-314, REQ-316） | 基本类型 → 原生类型，对象 → JSONB |
| `graphql_remote` | 架构自省（REQ-307, REQ-308） | 基本类型 → 原生类型，对象 → JSONB |
| `grpc_remote` | 服务端反射（REQ-322, REQ-325） | 基本类型 → 原生类型，对象 → JSONB |

API 响应被获取、缓存在 PostgreSQL 中（可配置 TTL），并暴露为 GraphQL 类型（REQ-309, REQ-318, REQ-327）。被缓存的表与其他任何数据源一样参与联邦查询（REQ-313）。

**JSONB 规则**：以 JSONB 形式存储的复杂列（对象、数组）不可过滤（REQ-119）。子字段访问在 SQL 中使用 `->>` 提取（REQ-151）。表之间的关系使用标量外键列声明——JSONB 二进制列不能作为联结目标。需要对其进行过滤或联结时，使用 JSONB 提升将嵌套字段转换为原生标量列（REQ-119）。

### GovData

美国政府开放数据。访问权限按主题分组进行分区。[tool-verified: `provisa/core/models.py` lines 543–609]

每个 `govdata` 数据源选择一个主题。该主题决定暴露哪些 GovData 架构。`ref` 和 `geo` 架构始终作为链接架构被包含在内——它们不按主题列出，但始终存在。[tool-verified: `provisa/core/models.py` line 562–563 comment]

| 主题 | 暴露的架构 |
| --------- | ----------------- |
| `COMMERCE` | `sec`、`patents` |
| `ECONOMY` | `econ` |
| `EDUCATION` | `census`、`edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`、`cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`、`fec` |
| `ALL` | 上述全部架构 |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| 字段 | 是否必填 | 默认值 | 说明 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一标识符 |
| `subject` | 是 | — | 上述主题取值之一 |
| `domain_id` | 是 | — | 该数据源所属的域 |
| `description` | 否 | `""` | 人类可读的描述 |

### 数据质量检查器（REQ-1443）

数据质量检查器是一种数据源类型，而不是一个子系统。它的扫描输出就是数据：一个检查结果就是一次观测，因此它会经由普通的数据源路径落地，并从其他任何数据源那里继承节奏、新鲜度、事件、血缘、治理、RLS、表格和导出。[tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

支持两种，选择既是许可证的选择，也是特性的选择。

| 数据源类型 | 合约方言 | Extra | 许可证 | 托管云平面 |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda contract YAML | `pip install .[soda]`（`soda-postgres`） | Elastic License 2.0 | 拒绝——见下文 |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]`（`great-expectations[postgresql]`） | Apache 2.0 | 允许 |

Elastic License 2.0 禁止将该软件以托管或托管式服务的形式提供给第三方，而在 SaaS 平面内代租户运行 Soda 恰恰属于这种情形。`config/capabilities.yaml` 用 `soda` 选项上的 `cloud_eligible: false` 承载了这一区分，托管平面读取该标志。想要使用 Soda 的托管部署会连接运营方自行运行的 Soda 端点。[tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa 不 vendor 也不链接任何东西。扫描运行在一个子解释器中（`python -m provisa.dq.worker`），这是唯一导入 `soda_core` 或 `great_expectations` 的地方，因此一个仅 source-available 的检查器永远不会进入服务器进程，检查器崩溃只会杀死一个子进程，而不会杀死事件循环。[tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**该数据源指向 Provisa 自身的 pgwire 终结点。** 这正是一个 postgres 驱动能够检查 Snowflake 或 Iceberg 支撑的表的原因：检查器扫描的是联邦视图，而非底层系统。由于策略适用于该连接，扫描身份是显式声明的，而非继承而来——一个被过滤掉的行集绝不能悄无声息地产生一个通过的检查。

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

**每个合约对应一张结果表，合约本身就是全部的注册内容。** 该表携带 `dq_contract`——原样保留的合约文本——除此之外不携带任何关于其形状的信息。列、水位和提升字段全部是派生出来的。[tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

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

注册过程从这段文本派生出的内容：

- **血缘。** 合约本身已经命名了其目标数据集，因此注册过程会像 `extract_inputs` 解析 SQL 那样解析它（REQ-939），并将其解析到受治理的表。只有一份定义，没有可能漂移的第二份副本。若合约命名了一个未受治理的数据集，会在注册时立即失败，而不会落地任何无人请求过的行。
- **列。** 结果信封是检查器自己的，而非运维方定义的——从 `scan_id` 到 `diagnostics` 共 16 个内置列。已声明的列仅其 `visible_to` 会被读取（必须一致），随后会被替换。[tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **水位。** `scan_time` 成为水位，这使得落地成为一次追加（REQ-982）。扫描历史得以累积，无需任何历史子系统。
- **提升字段。** `freshness_max_timestamp` 和 `dataset_rows_tested` 会从 `diagnostics` jsonb 中作为类型化列被提升出来（REQ-119）。可以像在任何其他 jsonb 列上那样添加更多。[tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

时机不会引入任何新字段。`change_signal` 加上 `cache_ttl` 给出轮询节奏；`mv_debounce_quiet` 和 `mv_debounce_max_delay` 把上游的一次突发收拢为一次扫描（REQ-963）；日历粒度使其成为周期性的（REQ-962）；`expected_events` 会让扫描等到其输入在窗口内变得新鲜为止（REQ-961）。轮询循环就是扫描调度器。

`outcome` 取值之一为 `pass`、`fail`、`warn`、`error`、`skipped`。它们都不是裁定结果——如果需要强制执行，那是稍后另行声明的事情：一个 preflight，或是一个针对已落地结果的 MV。由于一次已落地的观测不携带任何确定性义务（REQ-964），这里可以接受一些永远无法出现在 preflight 门控上的非确定性检查——异常分数、滚动窗口变化、相对当前时间的新鲜度。

合约在 UI 中编写，位于表编辑界面的数据质量面板中，那里的原始合约文本永远是唯一真相来源。试运行会针对实时表执行该合约，并展示结果而不落地——这正是你发现某个合约的数据集名称解析到了意料之外的地方、否则只会落地一堆全部通过的行的方式。

---

## 自定义连接器（REQ-1177）

当运营人员在 `config/custom_connectors.yaml` 中为某个新数据源类型声明连接器时，原生联邦查询引擎——Postgres、DuckDB 和 ClickHouse——就获得了对该类型的可达性。无需编写代码。[tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

连接器可扩展性本身早已存在。Trino 引擎在其自身层面长期以来就是可扩展的——一个按数据源类型参数化的通用 JDBC 连接器、一个按类型的目录 `.properties` 主体，以及 Provisa 自有的自定义 Trino 连接器插件（Splunk、SharePoint、Calcite）。[tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 将同样的配置驱动可扩展性，带给了此前拥有固定连接器集合的两个原生、无集群引擎。

该配置文件默认为空。内置连接器覆盖开箱即用的可达性；此文件中的一切都由运营人员编写。[tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] 设置 `PROVISA_CUSTOM_CONNECTORS` 可指向不同的路径（对测试很有用）。

### 描述符种类

| 引擎 | 种类 | 机制 | 描述符提供的内容 |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED（ISO 标准） | `extension`、`server_options`、`user_mapping`、`supports_import`、`table_options`、`remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`、`probe_symbol`、`attach_template`、`remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + 扫描视图 | `extension`、`probe_symbol`、`scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…`（自动暴露每张远程表） | `ch_engine`、`engine_template` |
| `clickhouse` | `clickhouse_table` | 按表的 `CREATE TABLE ENGINE=…`（列来自注册表） | `ch_engine`、`engine_template`（可能携带 `{table}`） |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`，由 ClickHouse 推断架构 | `ch_engine`、`engine_template` |

**Postgres 是通用的。** SQL/MED 是一个 ISO 标准，因此每个符合标准的 FDW 都共享相同的 DDL 形式：`CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`，可选的 `CREATE USER MAPPING`，然后是 `IMPORT FOREIGN SCHEMA`（当 `supports_import: true` 时）或按表显式 `CREATE FOREIGN TABLE`（当为 `false` 时）。一个 `pg_fdw` 描述符只提供各 FDW 之间的差异部分——扩展名称、server option 键、user-mapping 键、导入标志、表选项。因此任何符合标准的 FDW 都可以仅凭配置驱动。[tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB 支持两种机制。** 通过 ATTACH 暴露目录的扩展使用 `duckdb_attach`；暴露读取表函数的扩展使用 `duckdb_scan`。不符合这两种模式的扩展不受支持。[tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse 支持三种机制**，每种对应一种集成引擎形式：一种自动暴露每张远程表的关系型 DATABASE 引擎（`clickhouse_database`，例如 Redis/MySQL）、一种由注册表提供列的按表引擎（`clickhouse_table`，例如 JDBC/ODBC 桥接——`engine_template` 可能携带一个由运行时绑定的 `{table}` 占位符），以及一种由 ClickHouse 推断架构的文件/数据湖/URL 引擎（`clickhouse_scan`，例如 HDFS/URL）。SQLite（DATABASE 引擎，文件形式，无服务器）和 Hudi（数据湖仓，零拷贝）开箱即用。[tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

未知的 `kind` 取值会在启动时显式报错——描述符的拼写错误绝不能让某个数据源类型悄无声息地变得不可达。[tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### 探测门控

可用性在 attach 时依据各引擎标准的发现目录进行校验：

- **Postgres** —— 检查 `pg_extension`，然后检查 `pg_available_extensions`。[tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** —— 运行 `INSTALL`/`LOAD`，并在 `duckdb_functions()` 中检查所声明的 `probe_symbol`。[tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** —— 在 `system.table_engines` 中检查所声明的 `ch_engine`；构建中缺失该引擎则显式报错。[tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

无法安装的已声明扩展会显式报错。没有静默跳过，没有回退。探测失败的连接器在该部署环境中就是未激活状态。

### 模板变量

每个 `server_options` 值、`user_mapping` 值、`attach_template` 和 `scan_template` 都可以使用 `{field}` 占位符。可用字段：[tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`、`{host}`、`{port}`、`{database}`、`{username}`、`{password}`、`{path}`、`{schema_name}`、`{table_name}`，以及 `federation_hints` 中的任意键。DuckDB 的 attach 模板还会接收到 `{alias}`——Provisa 为已 attach 的数据库分配的内部目录别名。

引用未知字段的模板会在 attach 时显式报错，在损坏的 DDL 到达引擎之前就暴露出描述符/数据源不匹配的问题。

### 示例

**Postgres —— 通过 `mongo_fdw` 访问 MongoDB（不导入架构；按表提供列）**

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

**DuckDB —— 通过 `read_xlsx` 访问 Excel 文件（扫描表函数）**

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

有了任一描述符后，使用所声明 `source_type` 注册数据源即会通过该自定义连接器路由，前提是探测成功。无需其他配置更改。

---

## 作为命名数据源的数据仓库

Snowflake、Databricks 和 ClickHouse 都可以注册为命名数据源，与当前活动的联邦查询引擎无关。[tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

注册后，Provisa 通过该数据源的 DirectDriver 读取数据仓库，并将一份副本落地到活动引擎的物化存储中。随后查询针对该副本运行。这与传统的直连能力型路径（asyncpg、aiomysql，完全绕过引擎）不同——这里引擎仍然执行查询，但是针对本地副本，而不是在每次请求时都通过网络访问数据仓库。

在数据仓库支持的情况下，读取采用 Arrow 原生方式：Databricks 使用 Cloud Fetch，Snowflake 使用 `fetch_arrow_table`，ClickHouse 使用原生列式 HTTP 接口。

标准的 `host`/`port`/`username`/`password` 字段无法承载的扩展连接参数放在 `federation_hints` 中：

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

作为命名数据源注册，与选择同一数据仓库作为联邦查询引擎是两回事。在 DuckDB 引擎上注册的 Snowflake 数据源会把副本落地到 DuckDB 中，而不是落地到 Snowflake 中。

云对象/数据湖数据（S3 / GCS / R2 上的 parquet、csv、iceberg、delta_lake 文件）是一种独立的数据源类型，当活动引擎拥有该类型的 ATTACH 连接器时，会就地 attach。不会落地任何副本——引擎直接扫描对象存储。这类数据源的凭据同样放在 `federation_hints` 中：

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

## 数据源配置字段

所有数据源共享一组通用字段。[tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| 字段 | 是否必填 | 默认值 | 说明 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一标识符；字母数字加连字符/下划线 |
| `type` | 是 | — | 数据源类型（见上表） |
| `host` | 否 | `""` | 主机名或 IP |
| `port` | 否 | `0` | 端口号 |
| `database` | 否 | `""` | 数据库名称 |
| `username` | 否 | `""` | 用户名 |
| `password` | 否 | `""` | 密码；使用 `${env:VAR}` 进行密钥解析 |
| `path` | 否 | `null` | 基于文件的数据源和对象/数据湖数据源的文件路径或云 URI |
| `base_url` | 否 | `null` | OpenAPI 数据源的基础 URL |
| `pool_min` | 否 | `1` | 连接池最小规模（REQ-052） |
| `pool_max` | 否 | `5` | 连接池最大规模（REQ-052） |
| `use_pgbouncer` | 否 | `false` | 通过 PgBouncer 路由连接（REQ-053） |
| `pgbouncer_port` | 否 | `6432` | PgBouncer 端口（REQ-053） |
| `cache_enabled` | 否 | `true` | 启用 API 响应缓存 |
| `cache_ttl` | 否 | `null` | 缓存 TTL（秒）；为 null 时继承全局默认值 |
| `cache_catalog` | 否 | `null` | API 缓存所用的联邦目录；默认为数据源自身的目录 |
| `cache_schema` | 否 | `api_cache` | 缓存目录内的架构 |
| `naming_convention` | 否 | `null` | 覆盖该数据源的全局命名约定（REQ-194） |
| `federation_hints` | 否 | `{}` | 传递给联邦查询引擎的会话属性，以及数据仓库数据源的扩展连接参数（REQ-278, REQ-281） |
| `mapping` | 否 | `{}` | 针对 NoSQL 和 SaaS 数据源的特定连接器设置（例如 SharePoint 的 `auth_type`、Splunk 的 `use_token`）（REQ-251） |
| `allowed_domains` | 否 | `[]` | 将数据源限制到特定域；空表示不受限 |
| `description` | 否 | `""` | 人类可读的描述 |

---

## Kafka 数据源

Kafka 主题在 `kafka_sources` 下单独配置，通过已注册的 `kafka` 数据源的 `id` 进行索引。[tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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

| 字段 | 说明 |
| ------- | ------------- |
| `id` | 必须匹配某个 `type: kafka` 数据源的 `id` |
| `topics[].id` | 该主题在 Provisa 内的逻辑名称 |
| `topics[].topic` | Kafka 主题名称 |
| `topics[].domain_id` | 该主题所属的域 |
| `topics[].description` | 人类可读的描述 |
| `topics[].default_window` | 窗口查询的默认时间窗口（例如 `1h`）（REQ-148） |
| `topics[].columns` | 该主题架构的列定义（REQ-150） |

---

## 列可见性

每个列上的 `visible_to` 字段是可以看到该列的角色 ID 列表。[tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

从某个角色的 `visible_to` 列表中省略的列不会出现在该角色的 GraphQL 架构中，也无法在查询或过滤条件中被引用（REQ-039）。

---

## 关系

关系连接两张已注册的表，并作为嵌套字段出现在 GraphQL 中。[tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| 字段 | 是否必填 | 说明 |
| ------- | ---------- | ------------- |
| `id` | 是 | 该关系的唯一标识符 |
| `source_table_id` | 是 | 持有外键的表 |
| `target_table_id` | 是 | 被引用的表；对于计算型关系为空 |
| `source_column` | 是 | 源表上的列 |
| `target_column` | 是 | 目标表上的列；对于计算型关系为空 |
| `cardinality` | 是 | `many-to-one` 或 `one-to-many`（REQ-019） |
| `materialize` | 否 | 为跨数据源联结自动创建物化视图（REQ-158） |
| `refresh_interval` | 否 | 物化视图刷新间隔（秒，默认 300） |
| `target_function_name` | 否 | 用于计算型关系的数据库函数名称 |
| `function_arg` | 否 | 哪个函数参数接收源列的值 |
| `alias` | 否 | 人类可读的关系类型（例如 `WORKS_FOR`） |
| `graphql_alias` | 否 | 指定该关系在父类型上暴露的 SDL 字段名称。缺省时，该名称从目标表的 `field_name` 和关系基数派生。[tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | 否 | 当为 `true` 时，将该关系从 Cypher 图边中排除 |
| `source_json_key` | 否 | 在 JOIN 之前，从源列中以 JSON 对象形式提取该键 |

基数取值 [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]：

- `many-to-one` —— 每个源行映射到一个目标行（外键指向主键）
- `one-to-many` —— 每个源行映射到多个目标行（上者的反向关系）

---

## 行级安全规则

RLS 规则在查询时注入 `WHERE` 子句，作用范围限定在某个角色，并可选地限定在某张表或某个域。[tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

当同一角色同时存在域级规则和表级规则时，表级规则优先（REQ-403）。

| 字段 | 是否必填 | 说明 |
| ------- | ---------- | ------------- |
| `table_id` | 条件必填 | 该规则应用到的表；与 `domain_id` 互斥 |
| `domain_id` | 条件必填 | 该规则应用到的域；适用于该域内所有表（REQ-402） |
| `role_id` | 是 | 该规则适用的角色 |
| `filter` | 是 | 注入到 `WHERE` 中的 SQL 谓词；可以引用会话变量（REQ-041） |

---

## 函数与 Webhook

### 数据库函数

跟踪一个数据库函数，并将其暴露为 GraphQL 查询或变更操作。[tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

数据库数据源还可以从供应商目录（`pg_proc`、`information_schema.routines` 或供应商等效物）自动发现其存储过程和函数，从而无需手动逐一注册。发现过程读取 `prokind` 和 `provolatile`：不可变/稳定函数被注册为参数化关系（过程参数变为查询参数，形态与 OpenAPI GET 表相同），易变过程被注册为变更操作/被跟踪函数。被发现的例程与手动注册的例程一样经过 Stage 2 治理流水线。[tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

| 字段 | 是否必填 | 默认值 | 说明 |
| ------- | ---------- | --------- | ------------- |
| `name` | 是 | — | GraphQL 字段名 |
| `source_id` | 是 | — | 包含该函数的数据源 |
| `schema` | 否 | `public` | 数据库架构 |
| `function_name` | 是 | — | 实际的数据库函数名称 |
| `returns` | 是 | — | 该函数返回的已注册表 ID（REQ-207） |
| `arguments` | 否 | `[]` | `{name, type}` 参数定义列表（REQ-211） |
| `visible_to` | 否 | `[]` | 可调用该函数的角色 |
| `writable_by` | 否 | `[]` | 可将其作为变更操作调用的角色 |
| `domain_id` | 否 | `""` | 该函数所属的域 |
| `description` | 否 | `null` | GraphQL 字段描述 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"`（REQ-205） |

### Webhook

将一个外部 HTTP 端点暴露为 GraphQL 查询或变更操作。[tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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

| 字段 | 是否必填 | 默认值 | 说明 |
| ------- | ---------- | --------- | ------------- |
| `name` | 是 | — | GraphQL 字段名 |
| `url` | 是 | — | Webhook 端点 URL |
| `method` | 否 | `POST` | HTTP 方法 |
| `timeout_ms` | 否 | `5000` | 请求超时（毫秒） |
| `returns` | 否 | `null` | 已注册表 ID，或 null 表示内联类型 |
| `inline_return_type` | 否 | `[]` | 用于自定义返回形态的 `{name, type}` 字段列表（REQ-210） |
| `arguments` | 否 | `[]` | `{name, type}` 参数定义列表 |
| `visible_to` | 否 | `[]` | 可调用该 Webhook 的角色 |
| `domain_id` | 否 | `""` | 该 Webhook 所属的域 |
| `description` | 否 | `null` | GraphQL 字段描述 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"` |

---

## 身份验证

身份验证在 `auth` 键下配置。[tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| 提供方 | 说明 |
| ---------- | ------------- |
| `none` | 无身份验证；所有请求都被视为 `default_role` |
| `firebase` | Firebase Authentication；需要 `project_id` 和 `service_account_key`（REQ-121） |
| `keycloak` | Keycloak OIDC（REQ-122） |
| `oauth` | 通用 OAuth 2.0（REQ-123） |
| `simple` | 不依赖外部提供方的用户名/密码认证（REQ-124） |

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

## 执行路由

**直接执行** —— 单数据源的 RDBMS 查询路由到原生驱动，实现低于 100 毫秒的延迟（REQ-027）。数据源需要同时具备 `SOURCE_TO_DIALECT` 条目和 `SOURCE_TO_CONNECTOR` 条目才能支持该路径（REQ-229）。

**联邦执行** —— 多数据源查询以及没有直接驱动的数据源通过联邦查询引擎路由（REQ-028）。Provisa 内置了一个嵌入式联邦查询引擎；大规模部署时可指向您自己兼容的集群（REQ-226）。

**统计信息** —— 在注册时，Provisa 会对每张已发布的表运行 `ANALYZE`，为基于成本的优化器预热（行数、空值比例、去重值、最小/最大值）。失败会被记录，但不会阻塞注册（REQ-275）。

---

## 图与语义数据源

### Neo4j

将 Neo4j 图数据库注册为可查询的数据源。数据管家编写投影标量值的 Cypher 查询；Provisa 缓存结果并将其暴露为 GraphQL 类型（REQ-295）。

Cypher 查询必须在 `RETURN` 子句中使用属性访问器（`RETURN n.id AS id, n.name AS name`）——返回节点对象会在注册时被拒绝（REQ-296）。

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

预览端点（`POST /admin/sources/neo4j/{id}/preview`）返回样本行，若 Cypher 返回节点对象则阻止注册（REQ-296）。

### SPARQL

将任意符合 SPARQL 1.1 规范的三元组存储（Apache Jena Fuseki、Virtuoso、Stardog 等）注册为可查询的数据源（REQ-297）。

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

两个连接器都使用 API 数据源缓存流水线——结果以可配置 TTL 存储在 PostgreSQL 中，使其可用于跨数据源的联邦 JOIN（REQ-295, REQ-297, REQ-299）。

---

## 连接示例

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

### 跨数据源查询

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

单数据源部分直接路由（REQ-027）。跨数据源 JOIN 通过联邦查询并自动进行类型强制转换（REQ-028, REQ-552）。
</content>
