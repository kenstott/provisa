# 配置参考

Provisa 通过一个 YAML 文件进行配置（默认：`config/provisa.yaml`）。(REQ-528)

## 数据源

```yaml
sources:
  - id: sales-pg           # unique identifier
    type: postgresql
    host: postgres
    port: 5432
    database: provisa
    username: provisa
    password: ${env:PG_PASSWORD}  # secret resolution
    pool_min: 1
    pool_max: 5
    use_pgbouncer: false
    pgbouncer_port: 6432
```

所有数据源共享一组通用字段。[tool-verified: `provisa/core/models.py:129-212`]

| 字段 | 默认值 | 说明 |
| ------- | --------- | ------- |
| `id` | 必填 | 字母数字、连字符、下划线 |
| `type` | 必填 | 参见下表 |
| `host` | `""` | 主机名或 IP |
| `port` | `0` | `0` 表示由各个连接器提供自己的默认值——不存在集中式的默认端口映射 |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | 支持 `${env:VAR}` 密钥解析 |
| `path` | `null` | 基于文件的数据源的文件路径或 URI |
| `base_url` | `null` | API 数据源的基础 URL |
| `pool_min` / `pool_max` | `1` / `5` | 连接池上下限 |
| `cache_enabled` | `true` | 切换该数据源中所有表的缓存 |
| `cache_ttl` | `null` | 秒数；`null` 表示继承全局默认值 |
| `federation_hints` | `{}` | 按连接器扩展的参数（dict[str,str]）；见下方类型参考。REQ-281 |
| `mapping` | `{}` | 用于 redis、elasticsearch、prometheus 的映射 DSL。REQ-251 |
| `allowed_domains` | `[]` | 将该数据源限制到特定域 ID；空表示不受限 |
| `description` | `""` | |

### 支持的数据源类型 [tool-verified: `provisa/core/models.py:36-101`]

| 类型 | 连接方式 | 说明 |
| ------ | ----------------- | ------- |
| **关系型数据库（RDBMS）** | | |
| `postgresql` | host/port | Asyncpg 连接池；通过 `use_pgbouncer` 可选启用 PgBouncer |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path`（数据库文件） | DuckDB firebird 社区扩展（REQ-899） |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | 复用 PostgreSQL 驱动/方言（REQ-950） |
| `yugabytedb` | host/port | 复用 PostgreSQL 驱动/方言（REQ-950） |
| `greenplum` | host/port | 复用 PostgreSQL 驱动/方言（REQ-950） |
| `tidb` | host/port | 复用 MySQL 驱动/方言（REQ-950） |
| **云数据仓库** | | |
| `snowflake` | host/port + `federation_hints` | hints 中要求 `account` |
| `bigquery` | `federation_hints` | 要求 `project`；通过 `GOOGLE_APPLICATION_CREDENTIALS` 认证 |
| `databricks` | host + `federation_hints` | hints 中要求 `http_path` |
| `fabric` | 环境变量或 `PROVISA_ENGINE_URL` | 基于 TDS 的 T-SQL，Azure AD 认证 |
| `synapse` | 环境变量或 `PROVISA_ENGINE_URL` | 基于 TDS 的 T-SQL，Azure AD 认证 |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | `secure` hint 切换 TLS；端口默认 8123/8443 |
| `elasticsearch` | host/port + `mapping` DSL | |
| `pinot` | host/port | Controller REST 端点 |
| `druid` | host/port | Broker Avatica 端点 |
| `exasol` | host/port | |
| **数据湖** | | |
| `delta_lake` | `path`（表 URI） | DuckDB `delta_scan`；通过 `federation_hints` 访问对象存储 |
| `iceberg` | `path`（表 URI） | DuckDB `iceberg_scan`；通过 `federation_hints` 访问对象存储 |
| `hudi` | `path`（表 URI） | ClickHouse Hudi 引擎，零拷贝（REQ-1178） |
| `hive` | host/port（metastore） + `mapping.storage` | 存储后端在 `mapping["storage"]` 中：hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port（metastore） + `mapping` S3 键 | 独立类型；始终为 S3 存储（REQ-229） |
| **NoSQL** | | |
| `mongodb` | host/port | 普通连接字段；无映射 DSL |
| `cassandra` | host/port | 普通连接字段；无映射 DSL |
| `redis` | host/port + `mapping` DSL | |
| **流式处理** | | |
| `kafka` | 仅注册 | 真正的配置位于 `kafka_sources[]` 中；参见下方 §Kafka |
| `websocket` | host/port/path + `federation_hints` | 外部 WebSocket 数据流 |
| `rss` | host/port/path + `federation_hints` | RSS 2.0 / Atom 数据源 |
| **图/语义** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **文件** | | |
| `sqlite` | `path` | 始终经引擎路由（无直接连接池） |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path`（目录） | Glob 爬取；将 CSV/Parquet/XLSX/JSON 呈现为表 |
| **API/远程** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port 或 `mapping.url` + `mapping` DSL | |
| `graphql_remote` | `base_url` + 可选 `mapping` | Headers、forward-client-headers、timeout 均在 `mapping` 中 |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url`（Flight 位置） | DuckDB airport 扩展（REQ-899） |
| `ingest` | 推送接收方 | 外部服务通过 POST 发送 JSON 事件 |
| **SaaS** | | |
| `sharepoint` | `base_url` 或 `host` + `mapping` | 通过 `mapping.auth_type` 认证 |
| `splunk` | `host`/`port` 或 `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | 独立的 `GovDataSource` 模型；参见下方 §GovData |

### 数据源类型参考

需要非显而易见配置的类型，下方各有一段简短说明。RDBMS 类型（postgresql、mysql 等）仅使用上述通用字段——无需额外章节。

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

`govdata` 数据源使用一个独立的顶层模型 `GovDataSource`，而不是通用的 `Source`。(REQ-540) 访问权限按主题分组划分。

```yaml
sources:
  - id: federal-data
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    api_key: ${env:GOVDATA_API_KEY}   # optional
    start_year: 2020                   # optional year filter
    end_year: 2024                     # optional year filter
```

每个主题映射到一个或多个 GovData 架构。配置一个带有某主题的 `govdata` 数据源，会自动暴露该主题下的所有架构。(REQ-540)

| 主题 | 架构 |
| --------- | --------- |
| `COMMERCE` | `sec`、`patents` |
| `ECONOMY` | `econ`、`econ_reference` |
| `EDUCATION` | `census`、`edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`、`cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`、`fec` |

`ref` 和 `geo` 架构始终作为链接架构被包含在内——不可配置，也不列在上表中。(REQ-541) 使用主题 `ALL` 可授予对所有架构的访问权限。[tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

`sources:` 中的 `kafka` 行仅用于注册。其连接器的 `details()` 返回 `{}`——真正的配置位于顶层的 `kafka_sources[]` 块中，而不是在某个 `sources:` 行里。Kafka 始终是 VIRTUAL_SOURCE（经引擎路由；无直接连接池）。[tool-verified: `provisa/transpiler/router.py:44-63`]

```yaml
kafka_sources:
  - id: event-stream
    bootstrap_servers: kafka:9092
    schema_registry_url: http://schema-registry:8081  # optional
    topics:
      - id: order-created
        topic: orders.events
        default_window: 1h          # auto-injected time bound
        schema_source: manual       # manual, registry, or sample
        value_format: json
        discriminator:              # filter shared topic by message type
          field: event_type
          value: OrderCreated
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: amount
            type: double
          - name: metadata
            type: varchar           # raw JSON for complex nested data
      - id: order-shipped
        topic: orders.events        # same physical topic
        default_window: 1h
        discriminator:
          field: event_type
          value: OrderShipped
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: shipped_at
            type: timestamp
```

**时间窗口** —— `default_window` 将每个查询限定在一个近期时间段内，防止对高流量主题进行无边界读取。(REQ-148) 格式：`1h`、`30m`、`7d`、`60s`。默认为 `1h`。自动注入为 `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`。客户端可以在 GraphQL 的 `where` 参数中提供自己的 `_timestamp` 过滤条件来覆盖此设置。

**判别器（Discriminator）** —— 多个主题配置可以指向同一个物理 Kafka 主题，但使用不同的 `discriminator` 值，从而生成不同的 GraphQL 类型。(REQ-149) 该判别器会被自动注入为一个 WHERE 子句。

**架构来源（Schema Source）**

| 值 | 行为 |
| ------- | ---------- |
| `registry` | 从 Confluent Schema Registry 获取架构 |
| `manual` | 在配置中内联定义列（无需 Schema Registry） |
| `sample` | 从样本消息中自动发现 |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`federation_hints` 中的 `account` 是必填项。`warehouse`、`role` 和 `schema` 是可选项。

```yaml
sources:
  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    database: MY_DB
    federation_hints:
      account: myorg-myaccount     # required
      warehouse: COMPUTE_WH
      role: PROVISA_ROLE
      schema: PUBLIC               # remote schema override
```

#### Databricks [tool-verified: `provisa/executor/drivers/databricks.py:34-52`]

`federation_hints` 中的 `http_path` 是必填项。`password` 携带个人访问令牌。`catalog` 是可选项（在 SQL/hints 中携带，而非 `database` 字段中）。

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required
      catalog: my_unity_catalog              # optional
```

#### BigQuery [tool-verified: `provisa/federation/connector_duckdb.py:238`]

`federation_hints` 中的 `project` 是必填项。身份验证使用 `GOOGLE_APPLICATION_CREDENTIALS`（服务账号密钥文件的路径）或引擎环境中的应用程序默认凭据。

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

两者都使用基于 TDS 的 T-SQL，并采用 Azure AD 身份验证。使用 `az login`（开发环境）或托管标识（生产环境）进行身份验证——引擎通过 `azure-identity` 的 `DefaultAzureCredential` 读取凭据。连接详情来自环境变量：`FABRIC_SQL_SERVER` / `FABRIC_DATABASE`（Fabric）或 `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE`（Synapse），或通过 `PROVISA_ENGINE_URL`。

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`federation_hints` 中的 `secure` 会在 HTTP 接口上启用 TLS。端口默认为 `8123`（明文）或 `8443`（当 `secure: "true"` 时）。`federation_hints` 中的 `schema` 会覆盖远程架构。[tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

```yaml
sources:
  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"    # uses port 8443; omit to use 8123
      schema: analytics
```

#### Delta Lake / Iceberg [tool-verified: `provisa/federation/connector_duckdb.py:291-327`]

`path` 是表的 URI（S3、GCS、ADLS 或本地）。访问对象存储需要 `federation_hints` 中的凭据。对于 Cloudflare R2，需添加 `account_id`。

```yaml
sources:
  - id: events-delta
    type: delta_lake
    path: s3://my-bucket/data/events
    federation_hints:
      access_key_id: ${env:S3_ACCESS_KEY}
      secret_access_key: ${env:S3_SECRET}

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

#### Hive / Hive S3 [tool-verified: `provisa/federation/trino_connectors.py:244-363`]

`host` 和 `port` 指向 Hive Thrift metastore（默认端口 9083）。对于 `hive`，设置 `mapping["storage"]` 以选择对象存储后端。缺少必需的键会显式报错——没有静默回退。[tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` 是一个独立的类型，始终声明为 S3 存储（REQ-229）；无需 `mapping.storage`。

```yaml
sources:
  - id: hive-s3-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: s3
      endpoint: https://s3.us-east-1.amazonaws.com
      access_key_id: ${env:AWS_ACCESS_KEY_ID}
      secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
      region: us-east-1
      path_style: true           # required for MinIO and non-AWS S3-compatible endpoints

  - id: hive-adls-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: adls
      storage_account: mystorageaccount
      access_key: ${env:ADLS_ACCESS_KEY}
      # sas_token: ${env:ADLS_SAS_TOKEN}   # alternative to access_key
```

`mapping.storage` 接受的取值：`hadoop`（默认）、`hdfs`、`local`、`s3`、`azure`、`adls`。S3 映射键：`endpoint`、`access_key_id`、`secret_access_key`、`region`、`path_style`。ADLS 映射键：`storage_account`、`access_key` 或 `sas_token`。

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

使用 `mapping` DSL。`mongodb` 和 `cassandra` 使用普通连接字段，不使用映射 DSL。

```yaml
sources:
  - id: my-redis
    type: redis
    host: redis.internal
    port: 6379
    password: ${env:REDIS_PASSWORD}
    mapping:
      tables:
        - name: sessions
          key_pattern: "session:*"
          key_column: key           # default "key"
          value_type: hash          # hash | string | zset | list; default hash
          columns:
            - name: user_id
              data_type: VARCHAR
              field: user_id        # Redis hash field name
            - name: expires_at
              data_type: BIGINT
              field: expires_at
```

#### Elasticsearch [tool-verified: `provisa/core/trino_catalog_files.py:78-104`]

```yaml
sources:
  - id: my-es
    type: elasticsearch
    host: es.internal
    port: 9200
    username: elastic
    password: ${env:ES_PASSWORD}
    mapping:
      tls: true
      tables:
        - name: logs
          index: app-logs-*
          discover: false
          columns:
            - name: timestamp
              data_type: TIMESTAMP
              path: "@timestamp"
            - name: level
              data_type: VARCHAR
              path: level
            - name: message
              data_type: VARCHAR
              path: message
```

#### Prometheus [tool-verified: `provisa/core/trino_catalog_files.py:107-124`]

当 `mapping.url` 和 `host:port` 同时存在时，前者优先。

```yaml
sources:
  - id: my-prometheus
    type: prometheus
    mapping:
      url: http://prometheus.internal:9090
      tables:
        - name: http_requests
          metric: http_requests_total
          labels_as_columns: [method, status, handler]
          value_column: value      # default "value"
          default_range: 1h        # default "1h"
```

#### Google Sheets [tool-verified: `provisa/federation/connector_duckdb.py:273-275`]

`federation_hints` 中的 `spreadsheet_id` 是必填项。身份验证使用在 attach 时预配的 DuckDB `gsheet` SECRET。

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### 文件数据源（csv / parquet / sqlite / files）

`path` 为必填项。`files` 会爬取一个目录中的 CSV、Parquet、XLSX 和 JSON 文件，将每个文件呈现为一张表。所有基于文件的数据源都是 VIRTUAL（经引擎路由；无直接连接池）。[tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### API / 远程数据源

**openapi** —— 将 `base_url` 设置为 OpenAPI 基础 URL。架构发现会在启动时读取该 OpenAPI 规范。

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** —— 设置 `base_url`。可选的 `mapping` 键：`headers`（静态请求头字典）、`forward_client_headers`（布尔值）、`timeout_seconds`（整数）。[tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

```yaml
sources:
  - id: orders-gql
    type: graphql_remote
    base_url: https://orders.internal/graphql
    mapping:
      headers:
        X-Api-Key: ${env:ORDERS_API_KEY}
      forward_client_headers: true
      timeout_seconds: 30
```

**airport** —— `base_url` 是 Arrow Flight 服务器的位置。DuckDB airport 扩展（REQ-899）。[tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** —— 使用 `host`、`port`、`path` 和 `federation_hints`。[tool-verified: `provisa/api/data/subscribe.py:85-129`]

```yaml
sources:
  - id: market-feed
    type: websocket
    host: feed.example.com
    port: 443
    path: /ws/v1
    federation_hints:
      use_ssl: "true"
      subscribe_payload: '{"action":"subscribe","channels":["ticker"]}'
      event_path: data

  - id: news-rss
    type: rss
    host: feeds.example.com
    port: 443
    path: /rss/latest
    federation_hints:
      use_ssl: "true"
      poll_interval: "300"      # seconds
      # feed_url: https://...  # overrides host/port/path when set
```

**sharepoint** [tool-verified: `provisa/federation/trino_connectors.py:394-423`]

```yaml
sources:
  - id: my-sharepoint
    type: sharepoint
    base_url: https://myorg.sharepoint.com/sites/data
    username: ${env:SP_CLIENT_ID}
    password: ${env:SP_CLIENT_SECRET}
    database: ${env:SP_TENANT_ID}
    mapping:
      auth_type: CLIENT_CREDENTIALS   # default
      # certificate_path: /path/to/cert.pem
      # certificate_password: ${env:CERT_PASSWORD}
```

**splunk** [tool-verified: `provisa/federation/trino_connectors.py:426-457`]

```yaml
sources:
  - id: my-splunk
    type: splunk
    host: splunk.internal
    port: 8089
    password: ${env:SPLUNK_TOKEN}
    database: search           # Splunk app name (optional)
    mapping:
      use_token: true          # default; false = username/password auth
      datamodel_filter: ""     # optional Splunk Data Model filter
      disable_ssl_validation: false
```

## 域

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## 命名

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### 命名约定

命名权威是面向客户端名称的唯一事实来源；物理后端列名永远不会暴露给客户端。(REQ-194) 每种查询语言都会从 `column.alias`（如果已设置）派生列名，否则通过其配置的约定从物理列名派生。(REQ-194)

GraphQL 约定是三个预设枚举值之一。(REQ-416) 旧的自由格式字符串（`none`、`snake_case`、`camelCase`、`PascalCase`）已被弃用。(REQ-416)

| 预设 | 默认 | 类型名 | 字段名 | 变更操作名 |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | 是 | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

默认的 GraphQL 约定是 `apollo_graphql`，它生成 camelCase 的字段名和变更操作名。(REQ-194, REQ-416) SQL 约定是独立的，默认为 `snake_case`，通过 `apply_sql_name()` 应用；GraphQL 约定通过 `apply_gql_name()` 应用，CQL 名称则从 GraphQL 名称派生。(REQ-194)

`domain_prefix: bool` 是一个正交选项，无论选择哪个预设都会应用。(REQ-416)

显式的 `column.alias` 是规范名称：SQL 会原样使用它，不应用任何约定；GraphQL 会对其应用其约定；CQL 则从 GraphQL 名称派生。(REQ-194)

按数据源覆盖：

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

按表覆盖：

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### 域前缀

当 `domain_prefix: true` 时，所有 GraphQL 字段名和类型名都会用双下划线分隔符加上域 ID 作为前缀：(REQ-154)

| 表 | 域 | 字段名 |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

这可以防止不同域拥有同名表时产生命名冲突，并使查询自解释。

### 命名规则

在生成 GraphQL 字段名时应用于表名的正则规则。在唯一性解析之前按顺序应用。(REQ-542)

## 表

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    alias: purchase_orders     # optional: override GraphQL name
    description: "Customer purchase orders"  # optional: GraphQL description
    columns:
      - name: id
        visible_to: [admin, analyst]
        writable_by: []           # read-only (empty = no writes)
      - name: email
        visible_to: [admin, analyst]
        writable_by: [admin]      # only admin can mutate
        unmasked_to: [admin]      # admin sees raw, analyst sees masked
        mask_type: regex
        mask_pattern: "^(.{2}).*(@.*)$"
        mask_replace: "$1***$2"
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        writable_by: [admin]
        unmasked_to: [admin]
        mask_type: constant
        mask_value: "0"
      - name: created_at
        visible_to: [admin, analyst]
        writable_by: []           # nobody can write
        unmasked_to: [admin]
        mask_type: truncate
        mask_precision: month
    column_presets:               # auto-set values on insert/update
      - column: created_by
        source: header            # from request header
        name: X-User-ID
      - column: updated_at
        source: now               # current timestamp
```

### 别名

表别名和列别名会覆盖默认的 GraphQL 名称。(REQ-155) 适用于：

- 重命名晦涩的数据库名称（例如 `tbl_cust_seg` → `customer_segments`）
- 避免在 API 层出现缩写
- 创建清晰、面向领域的词汇表

### 描述

表描述和列描述会被包含在生成的 GraphQL SDL 中。(REQ-156) 它们会出现在 GraphiQL 的文档浏览器和自省查询中。可以在配置 YAML 中设置，也可以通过管理界面设置。

### 路径（计算型 JSON 提取）

列可以使用点号表示法的 `path` 从 JSON/JSONB 源列中提取值。(REQ-151) 这对 Kafka 消息、MongoDB 文档或 PostgreSQL JSONB 列中的半结构化数据很有用。

```yaml
columns:
  - name: payload
    type: varchar
    visible_to: []            # hide the raw JSON column
  - name: order_id
    type: integer
    path: payload.order_id    # extracts from payload column
    visible_to: [admin, analyst]
  - name: customer_name
    type: varchar
    path: payload.customer.name
    visible_to: [admin, analyst]
```

路径格式为 `source_column.key1.key2...`。编译器会在 SQL 中生成 `json_extract_scalar(source_column, '$.key1.key2')`。(REQ-151)

**对路由的影响：** 路径列使用 PostgreSQL 的 JSON 运算符（`->>`），直接的 PG 路由原生支持它们。(REQ-152) 对于非 PostgreSQL 数据源（MySQL、SQL Server 等），带有路径列的查询会自动通过联合查询引擎路由。(REQ-152) 变更操作不受影响，因为路径列是只读的计算字段。(REQ-153)

### 脱敏类型

| 类型 | 字段 | 说明 |
| ------ | -------- | ------------- |
| `regex` | `pattern`、`replace` | REGEXP_REPLACE（仅字符串列） |
| `constant` | `value` | 字面值替换（NULL、0、MAX、MIN、自定义） |
| `truncate` | `precision` | DATE_TRUNC（仅日期/时间戳列） |

## 关系

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one   # or: one-to-many

  - id: orders-to-reviews
    source_table_id: orders        # sales-pg source
    target_table_id: product_reviews  # reviews-mongo source
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV for this cross-source join
    refresh_interval: 600          # refresh every 10 minutes
```

### 自动物化

在关系上设置 `materialize: true`，可为跨数据源的 JOIN 自动生成物化视图。(REQ-158) 这通过预先计算 JOIN 结果来避免昂贵的联合查询。

- 只有跨数据源的关系才会生成物化视图（同数据源的 JOIN 本身已经很快）(REQ-159)
- 物化视图初始状态为陈旧，由后台刷新循环填充 (REQ-160)
- 对任一数据源表的变更操作都会将该物化视图标记为陈旧，等待重新刷新 (REQ-543)
- `refresh_interval` 默认值为 300 秒（5 分钟） (REQ-543)

## 角色

```yaml
roles:
  - id: admin
    capabilities:
      - source_registration
      - table_registration
      - relationship_registration
      - security_config
      - query_development
      - full_results
      - admin
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

带有 `parent_role_id` 的角色会从父角色继承能力和域访问权限。(REQ-215) 该层级关系在启动时被展平。(REQ-215)

### 能力

| 能力 | 说明 |
| ----------- | ------------- |
| `source_registration` | 注册数据源 |
| `table_registration` | 注册表 |
| `relationship_registration` | 定义关系 |
| `security_config` | 配置 RLS、脱敏 |
| `query_development` | 执行查询 |
| `full_results` | 绕过采样限制 |
| `admin` | 全部能力 |

## RLS 规则

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## 物化视图

```yaml
materialized_views:
  - id: mv-orders-customers
    source_tables: [orders, customers]
    join_pattern:
      left_table: orders
      left_column: customer_id
      right_table: customers
      right_column: id
      join_type: left
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## 视图（受治理的计算数据集）

视图是具有完整列级治理的 SQL 定义计算数据集。(REQ-133) 它们是为语义层添加聚合、转换和派生指标的受治理机制。(REQ-136)

```yaml
views:
  - id: monthly-revenue
    sql: |
      SELECT DATE_TRUNC('month', created_at) AS month,
             region,
             SUM(amount) AS revenue,
             COUNT(*) AS order_count
      FROM orders
      GROUP BY 1, 2
    description: "Monthly revenue by region"
    domain_id: sales-analytics
    materialize: true
    refresh_interval: 3600
    columns:
      - name: month
        visible_to: [admin, analyst]
      - name: region
        visible_to: [admin, analyst]
      - name: revenue
        visible_to: [admin]
      - name: order_count
        visible_to: [admin, analyst]
```

| 字段 | 是否必填 | 说明 |
| ------- | ---------- | ------------- |
| `id` | 是 | 唯一的视图标识符 |
| `sql` | 是 | 定义该视图的 SQL SELECT 语句 |
| `domain_id` | 是 | 用于架构可见性的域 |
| `materialize` | 否 | `true` = 定期 CTAS 刷新，`false` = 实时联合视图 |
| `refresh_interval` | 否 | 每次刷新间隔的秒数（仅物化视图，默认 300） |
| `description` | 否 | 出现在 GraphQL SDL 中 |
| `alias` | 否 | 覆盖 GraphQL 名称 |
| `columns` | 是 | 带可见性、脱敏、描述的列定义 |

### 物化 vs 实时

- **`materialize: true`**：Provisa 通过 CTAS 创建一张表，并按计划刷新它。(REQ-135) 查询更快，但数据可能滞后最多 `refresh_interval` 秒。
- **`materialize: false`**：Provisa 创建一个联合视图。(REQ-135) 查询始终返回实时数据，但在复杂聚合场景下可能较慢。

视图经过与表相同的治理流水线——RLS、脱敏、采样和基于角色的可见性。(REQ-134) 这确保了在没有数据管家监督的情况下，不会有新语义被添加到平台中。(REQ-136)

### 仅查询视图

`materialize: true` 和 `materialize: false` 的视图都将其 GraphQL 类型暴露为仅查询。不会为由 `view_sql` 支持的关系生成插入、upsert、更新或删除变更操作。(REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## 缓存

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### 缓存层级

TTL 解析顺序（越具体优先级越高）：**表** > **数据源** > **全局默认值**。(REQ-544) 使用第一个非空值。

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300              # global fallback: 5 minutes

sources:
  - id: sales-pg
    cache_enabled: true          # toggle caching for all tables in this source
    cache_ttl: 600               # source override: 10 minutes

tables:
  - source_id: sales-pg
    table: orders
    cache_ttl: 60                # table override: 1 minute (frequently changing)
  - source_id: sales-pg
    table: customers
    # no cache_ttl → inherits source TTL (600s)
```

在数据源上设置 `cache_enabled: false` 会禁用该数据源中所有表的缓存，无论表级 TTL 如何设置。(REQ-544) 缓存键始终包含 `role_id` 和 RLS 上下文值，用于安全分区。(REQ-544)

## 身份验证

```yaml
auth:
  provider: simple           # none, firebase, keycloak, oauth, simple
  superuser:
    username: admin
    password: ${env:PROVISA_SUPERUSER_PASSWORD}
  simple:
    allow: true
    jwt_secret: ${env:PROVISA_JWT_SECRET}
    users:
      - username: admin
        password_hash: "$2b$12$..."
        roles: [admin]
  role_mapping:
    - claim: groups
      contains: data-analysts
      provisa_role: analyst
    default_role: analyst
```

### 身份验证提供方类型

| 提供方 | 使用场景 | 令牌验证方式 |
| ---------- | ---------- | ----------------- |
| `simple` | 本地开发/测试。用户在 YAML 中定义。 | 用 `PROVISA_JWT_SECRET` 签名的 JWT |
| `firebase` | Firebase Authentication（所有方式）。 | `firebase-admin` SDK 的 `verify_id_token()` |
| `keycloak` | Keycloak OIDC。租户和客户端角色已映射。 | 基于 JWKS 的 JWT 验证 |
| `oauth` | 通用 OIDC（Okta、Azure AD、Auth0、PingFederate）。 | 来自发现 URL 的 JWKS |

超级用户凭据（`superuser` 块）适用于任何提供方，并始终解析为具有所有能力的 admin 角色。(REQ-125) 用于在配置外部身份验证之前的初始设置。

### 完整身份验证配置示例（已注释）

```yaml
# auth:
#   provider: firebase
#
#   superuser:
#     username: admin
#     password: ${env:PROVISA_SUPERUSER_PASSWORD}
#
#   firebase:
#     project_id: ${env:FIREBASE_PROJECT_ID}
#     service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT}
#
#   # keycloak:
#   #   server_url: https://keycloak.example.com
#   #   # kc-tenant: set to your Keycloak tenant name (e.g. provisa)
#   #   client_id: provisa-app
#   #   client_secret: ${env:KEYCLOAK_CLIENT_SECRET}
#
#   # oauth:
#   #   discovery_url: https://login.example.com/.well-known/openid-configuration
#   #   client_id: provisa
#   #   client_secret: ${env:OAUTH_CLIENT_SECRET}
#   #   role_claim: groups
#   #   audience: provisa-api
#
#   role_mapping:
#     - claim: custom_claims.role
#       value: admin
#       provisa_role: admin
#     - claim: groups
#       contains: data-analysts
#       provisa_role: analyst
#     default_role: analyst
```

## Upsert 变更操作

对于带有主键的表，Provisa 会自动生成 `upsert_<table>` 变更操作字段。(REQ-212) 这些字段会编译为目标方言中的 upsert 语句——在 PostgreSQL 上是 `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`，在 MySQL 上是 `ON DUPLICATE KEY UPDATE`。(REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

冲突列由主键元数据派生而来。(REQ-212) 所有列可见性和写权限规则均适用。

## Distinct On

`distinct_on` 参数为指定列的每个不同取值选取第一行。(REQ-213) 在根查询字段上可用。

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

在 PostgreSQL 中编译为 `SELECT DISTINCT ON (region) ...`。(REQ-213) 对于非 PG 方言，使用窗口函数作为回退方案。(REQ-213)

## 列预设值

在插入/更新时自动向列中注入值。(REQ-214) 按表在配置中定义。

```yaml
tables:
  - source_id: sales-pg
    table: orders
    column_presets:
      - column: created_by
        source: header           # from request header
        name: X-User-ID
      - column: updated_at
        source: now              # current timestamp
      - column: source_system
        source: literal          # constant value
        value: "provisa"
```

| 来源 | 行为 |
| -------- | ---------- |
| `header` | 从指定名称的 HTTP 请求头注入值 |
| `now` | 注入 `NOW()`（当前时间戳） |
| `literal` | 注入一个常量值 |

预设列在 SQL 生成之前的变更操作编译阶段被注入。(REQ-214) 它们不会出现在变更操作的输入类型中。(REQ-214)

## 继承角色

角色可以通过 `parent_role_id` 从父角色继承能力和域访问权限。(REQ-215) 该层级关系在启动时被展平。(REQ-215)

```yaml
roles:
  - id: admin
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

支持多级继承。(REQ-215) 子角色的显式能力和 domain_access 会与父角色的合并。(REQ-215)

## 计划触发器

按计划调用 webhook URL 的基于 Cron 的触发器。(REQ-216) 使用 APScheduler。(REQ-216)

```yaml
scheduled_triggers:
  - name: daily-report
    cron: "0 8 * * *"           # 8:00 AM daily
    webhook_url: https://hooks.example.com/daily-report
    enabled: true
  - name: hourly-sync
    cron: "0 * * * *"           # every hour
    webhook_url: https://hooks.example.com/sync
    enabled: false
```

计划任务可以通过管理界面（启用/禁用开关）或 `toggle_scheduled_task` 管理变更操作进行管理。(REQ-216)

## OrderBy 格式

OrderBy 使用 `{column: direction}` 格式，direction 是一个 6 值枚举：(REQ-200, REQ-201)

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| 方向 | SQL |
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

通过嵌套对象支持关系排序：(REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## 可观测性

```yaml
observability:
  endpoint: "http://localhost:4319"   # OTLP collector; env OTEL_EXPORTER_OTLP_ENDPOINT overrides
  service_name: provisa               # env OTEL_SERVICE_NAME overrides
  sample_rate: 1.0                    # 0.0–1.0; TraceIdRatioBased sampler
  log_level: WARNING                  # env OTEL_LOG_LEVEL overrides
  compact_batch_size: 1000
  telemetry_filter:
    redact_sql_literals: false        # strip literal values from db.statement before export
    redact_attributes: []             # attribute keys dropped entirely before export
  # support_endpoint: ""              # env PROVISA_SUPPORT_OTLP_ENDPOINT; off by default
  support_telemetry_filter:
    redact_sql_literals: true         # default on — strip literals before sending to support
    redact_attributes: []             # additional keys dropped before sending to support
```

### 遥测过滤器 [tool-verified]

Provisa 运行两条独立的 OTLP 导出路径：您的内部收集器和可选的 Provisa 支持端点。(REQ-545) 每条路径都有各自的过滤器。过滤器在 span 离开进程之前，运行在一个包装用的 `_FilteringExporter` 内部——原始 span 对象永远不会被修改。(REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** —— 控制到达您内部收集器的内容。

| 键 | 类型 | 默认值 | 说明 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | 将 `db.statement` 中的字符串和数字字面值替换为 `?` |
| `redact_attributes` | list[str] | `[]` | 从每个 span 中完全丢弃的属性键 |

**`support_telemetry_filter`** —— 控制到达 Provisa 支持端点的内容。此路径上的 SQL 字面值脱敏默认开启为 `true`，因为查询数据归您所有。(REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| 键 | 类型 | 默认值 | 说明 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | 将 `db.statement` 中的字符串和数字字面值替换为 `?` |
| `redact_attributes` | list[str] | `[]` | 从每个 span 中完全丢弃的属性键 |

脱敏后的 `db.statement` 示例——当 `redact_sql_literals: true` 时，这个 span 属性：

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

会变为：

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### 支持端点 [tool-verified]

`support_endpoint`（或环境变量 `PROVISA_SUPPORT_OTLP_ENDPOINT`）将遥测数据转发给 Provisa 支持团队用于诊断。(REQ-548) 未设置时，此路径不会有任何数据离开您的基础设施。(REQ-548) 支持过滤器独立于内部过滤器应用——您可以对两条导出路径都进行 SQL 字面值脱敏，同时仍与支持团队共享 span 时序和错误数据。(REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### 端点协议检测 [tool-verified]

Provisa 根据端点 URL 的方案（scheme）选择 OTLP/HTTP 或 OTLP/gRPC。(REQ-549) 以 `http://` 或 `https://` 开头的 URL 使用 OTLP/HTTP，并自动附加 `/v1/traces`、`/v1/metrics` 和 `/v1/logs`。(REQ-549) 其他任何方案都使用带 `insecure=True` 的 OTLP/gRPC。(REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## 联合查询引擎

配置联合查询引擎是可选的。默认值为 `duckdb`——零配置、进程内运行，无需外部服务（REQ-989）。当您需要 MPP 规模，或想复用现有数据仓库时，可以选择其他引擎。

优先级顺序：`PROVISA_ENGINE` 环境变量 → 持久化的管理界面 `federation_engine` 配置字段 → `duckdb`。更改在服务重启后生效。[tool-verified: `engine.py` `build_engine`]

### 引擎概览 [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| 引擎键 | 标签 | 方言 | MPP | 外部链接机制 | 认证方式 |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | 是 | Trino 目录（广泛的连接器集合） | JDBC 凭据 |
| `trino-byo` | Trino（自带） | Trino SQL | 是 | 与 `trino` 相同；非托管协调节点 | JDBC 凭据 |
| `pg` | PostgreSQL | PostgreSQL | 否 | FDW / pg_duckdb | PostgreSQL 凭据 |
| `duckdb` | DuckDB | DuckDB | 否 | 扩展原生 ATTACH | 无（进程内） |
| `clickhouse` | ClickHouse（嵌入式） | ClickHouse | 是 | S3 / IcebergS3 / DeltaLake 表引擎 | chdb（进程内，无认证） |
| `clickhouse-server` | ClickHouse（服务器 / Cloud） | ClickHouse | 是 | S3 / IcebergS3 / DeltaLake 表引擎 | ClickHouse 凭据 |
| `snowflake` | Snowflake | Snowflake | 是 | 外部 stage + 外部表 | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | 是 | 通过 REST 使用 Unity Catalog 外部表 | `PROVISA_ENGINE_URL`（bearer 令牌 + `http_path`） |
| `bigquery` | BigQuery | BigQuery | 是 | BigQuery 外部表 / BigLake 表 | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | 是 | OneLake shortcuts → OPENROWSET | Azure AD（`az login` 或托管标识） |
| `synapse` | Azure Synapse | T-SQL | 是 | ADLS OPENROWSET / 外部表 | Azure AD |
| `sqlalchemy` | SQLAlchemy（任意关系型数据库） | 按方言 | 否 | 无（仅落地） | 按方言凭据 |

### 引擎参考

#### trino / trino-byo

`trino` 是托管的 Provisa 协调节点；`trino-byo` 连接到您自己的 Trino 集群。两者都使用 Trino SQL，具有最广泛的数据源触达能力。

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

物化存储默认为 `TENANT_DATABASE_URL`（PostgreSQL）。

#### pg

通过 postgres_fdw（SQL/MED）和 pg_duckdb 扩展进行联合查询。单节点；无 MPP。当您的数据已经存放在 PostgreSQL 中，只需连接少量远程数据源时最为适用。

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

物化存储默认为 `TENANT_DATABASE_URL`。

#### duckdb

进程内运行；无外部服务。默认引擎（REQ-989）。`PROVISA_DATA_DIR` 控制嵌入式存储的位置（默认为 `~/.provisa`）。

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

物化存储默认为 `~/.provisa/materialize.duckdb`——唯一一个默认存储不是 PostgreSQL 的引擎。

#### clickhouse（嵌入式） / clickhouse-server

`clickhouse` 使用 chdb（进程内）。`clickhouse-server` 连接到外部 ClickHouse 实例或 ClickHouse Cloud。两者都通过原生 ClickHouse 表引擎直接读取 Delta Lake、Iceberg 和 Hudi。

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

物化存储默认为 `TENANT_DATABASE_URL`。

#### snowflake

引擎即数据仓库：由 Snowflake 运行查询；Provisa 通过外部 stage 推送数据源数据。

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

物化存储默认为 `TENANT_DATABASE_URL`。

#### databricks

Unity Catalog 外部表将 Provisa 管理的数据源桥接到 Databricks SQL。

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

物化存储默认为 `TENANT_DATABASE_URL`。

#### bigquery

BigQuery 外部表和 BigLake 表。项目来自 URL 或 `GOOGLE_CLOUD_PROJECT`；通过服务账号密钥进行身份验证。

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

物化存储默认为 `TENANT_DATABASE_URL`。

#### fabric / synapse

两者都使用基于 TDS 的 T-SQL，并采用 Azure AD 认证（`az login` 或托管标识）。省略 `PROVISA_ENGINE_URL` 可改为从环境变量读取连接详情。

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

物化存储默认为 `TENANT_DATABASE_URL`。

#### sqlalchemy

通用的仅落地关系型数据库引擎（不联合外部数据源）。用于单数据仓库部署或测试。

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

物化存储默认为 `TENANT_DATABASE_URL`。

### 物化存储

当某个数据源无法实时 attach 时（所选引擎没有 ATTACH 连接器），它会落地到该引擎的物化存储中。解析顺序：显式的 `PROVISA_MATERIALIZE_URL` → 引擎声明的默认值 → 硬性报错（无静默回退）。[tool-verified: `engine.py` `materialize_store`]

DuckDB 将其嵌入式文件（`~/.provisa/materialize.duckdb`）声明为默认值。所有其他引擎的默认值均为 `TENANT_DATABASE_URL`（PostgreSQL）。可以用 `PROVISA_MATERIALIZE_URL` 覆盖任意引擎的设置。

### 按数据源的联合查询提示

标准 host/port/user/password 字段无法承载的扩展连接参数，放在数据源的 `federation_hints` 中。各类型的具体提示键参见上方的数据源类型参考。一个综合示例：

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required for Databricks sources

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount
      warehouse: COMPUTE_WH

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"           # enable TLS on the HTTP interface

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

对于 Google Cloud 数据源，将 `GOOGLE_APPLICATION_CREDENTIALS` 设置为服务账号密钥文件的路径。对于 Fabric 和 Synapse，使用 `az login`（开发环境）或托管标识（生产环境）进行身份验证——引擎通过 `azure-identity` 的 `DefaultAzureCredential` 读取凭据。

## 环境变量

| 变量 | 默认值 | 说明 |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 配置文件路径 |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | 控制面存储 URI（SQLAlchemy 异步）；接受 `sqlite+aiosqlite://…` / `duckdb://…` 用于嵌入式桌面存储（REQ-828, REQ-850） |
| `PLATFORM_DATABASE_URL` | — | 平台注册表 URI（租户目录、引擎注册表）；启动时必填，无回退值（REQ-837） |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` 使用嵌入式 fakeredis 而非 Redis 服务器——无需 Docker（REQ-829） |
| `PG_HOST` | `localhost` | PostgreSQL 主机 |
| `PG_PORT` | `5432` | PostgreSQL 端口 |
| `PG_DATABASE` | `provisa` | PostgreSQL 数据库 |
| `PG_USER` | `provisa` | PostgreSQL 用户 |
| `PG_PASSWORD` | `provisa` | PostgreSQL 密码 |
| `PROVISA_ENGINE` | `duckdb` | 联合查询引擎键（REQ-989） |
| `PROVISA_ENGINE_URL` | — | URL 驱动型引擎（Snowflake、Databricks、ClickHouse Server、BigQuery、SQLAlchemy）的连接 URL |
| `PROVISA_MATERIALIZE_URL` | — | 覆盖物化存储 DSN（默认为引擎声明的默认值） |
| `PROVISA_DATA_DIR` | `~/.provisa` | 嵌入式 DuckDB 存储的数据目录（REQ-989） |
| `TRINO_HOST` | `localhost` | Trino 协调节点主机 |
| `TRINO_PORT` | `8080` | Trino 协调节点 HTTP 端口 |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | GCP 服务账号密钥 JSON 的路径（BigQuery 引擎/数据源） |
| `GOOGLE_CLOUD_PROJECT` | — | 默认 GCP 项目（BigQuery；被 URL 覆盖） |
| `FABRIC_SQL_SERVER` | — | Fabric Warehouse SQL 端点（`PROVISA_ENGINE_URL` 的替代方案） |
| `FABRIC_DATABASE` | — | Fabric Warehouse 数据库名称 |
| `SYNAPSE_SQL_SERVER` | — | Synapse 无服务器 SQL 端点 |
| `SYNAPSE_DATABASE` | — | Synapse 数据库名称 |
| `REDIS_URL` | — | Redis 连接 URL |
| `PROVISA_SAMPLE_SIZE` | `10000` | 默认采样上限 |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | 查询未提供显式 `LIMIT` 时的行数上限 |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | 一级读取重试预算（秒）；带全抖动的指数退避（REQ-703） |
| `ZAYCHIK_PORT` | `8480` | Zaychik Flight SQL 代理端口 |
| `FLIGHT_PORT` | `8815` | Provisa Arrow Flight 服务器端口 |
| `GRPC_PORT` | `50051` | Provisa Protobuf gRPC 服务器端口 |
| `PROVISA_REDIRECT_ENABLED` | `false` | 启用服务端阈值重定向 |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | 默认行数阈值 |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | 默认重定向格式 |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | 用于重定向结果的 S3 存储桶 |
| `PROVISA_REDIRECT_ENDPOINT` | — | S3 兼容端点 URL |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | S3 访问密钥 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | S3 密钥 |
| `PROVISA_REDIRECT_TTL` | `3600` | 预签名 URL 的 TTL（秒） |
| `ANTHROPIC_API_KEY` | — | Claude API 密钥（发现功能） |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | 覆盖 `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | 覆盖 `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | 覆盖 `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | 覆盖 `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | 批量 span 处理器的刷新延迟 |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | 覆盖 `observability.support_endpoint` |
</content>
