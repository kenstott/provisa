# 配置参考

Provisa 通过 YAML 文件进行配置（默认：`config/provisa.yaml`）。（REQ-528）

## 包含文件（REQ-1669）

使用 `includes:` 将一份配置拆分到多个文件中。包含文件在此键下列出片段路径；Provisa 会在校验前合并它们，产生与将所有内容写在单个文件中相同的结果。

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

仅包含 `includes:` 的包装文件是合法的。`load_control_plane` 会遍历这些包含项，因此 `control_plane:` 部分取自设置了该项的那个被包含文件。[tool-verified: `provisa/core/config_loader.py:154-166`]

**合并规则** [tool-verified: `provisa/core/config_loader.py:104-146`]

- 路径相对于包含文件解析。绝对路径按原样使用。
- 列表类小节（`sources`、`tables`、`domains`、`relationships`、`roles` 等）采用追加方式——片段中的条目跟在包含文件的条目之后。
- 包含文件未设置的标量或映射键，取自片段。
- 两个文件对同一个键设置了不同的值，视为冲突；加载失败，并指出冲突的键。
- 两个文件中的相同值不视为冲突。
- `includes` 可以嵌套。一个文件——无论是直接还是通过另一个片段——包含自身会被拒绝。
- `includes` 在加载时被消费，永远不会出现在校验后的配置中。


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

所有数据源共享同一组通用字段。[tool-verified: `provisa/core/models.py:129-212`]

| 字段 | 默认值 | 说明 |
| ------- | --------- | ------- |
| `id` | 必填 | 字母数字、连字符、下划线 |
| `type` | 必填 | 参见下表 |
| `host` | `""` | 主机名或 IP |
| `port` | `0` | `0` 表示由每个连接器提供各自的默认值——不存在中心化的默认端口映射表 |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | 支持 `${env:VAR}` 和 `${secret:NAME}` 凭据引用——参见[密钥](secrets.md) |
| `path` | `null` | 基于文件的数据源的文件路径或 URI |
| `base_url` | `null` | API 数据源的基础 URL |
| `pool_min` / `pool_max` | `1` / `5` | 连接池边界 |
| `cache_enabled` | `true` | 切换此数据源下所有表的缓存 |
| `cache_ttl` | `null` | 秒；`null` 表示继承全局默认值 |
| `federation_hints` | `{}` | 每种连接器的扩展参数（dict[str,str]）；参见下方的类型参考。REQ-281 |
| `mapping` | `{}` | 用于 redis、elasticsearch、prometheus 的映射 DSL。REQ-251 |
| `allowed_domains` | `[]` | 将此数据源限制在特定的域 ID 内；为空表示不受限制 |
| `description` | `""` | |

### 支持的数据源类型 [tool-verified: `provisa/core/models.py:36-101`]

| 类型 | 连接方式 | 说明 |
| ------ | ----------------- | ------- |
| **关系型数据库** | | |
| `postgresql` | host/port | Asyncpg 连接池；通过 `use_pgbouncer` 选择性启用 PgBouncer |
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
| `snowflake` | host/port + `federation_hints` | 提示中必须包含 `account` |
| `bigquery` | `federation_hints` | 必须包含 `project`；通过 `GOOGLE_APPLICATION_CREDENTIALS` 认证 |
| `databricks` | host + `federation_hints` | 提示中必须包含 `http_path` |
| `fabric` | 环境变量或 `PROVISA_ENGINE_URL` | 基于 TDS 的 T-SQL，Azure AD 认证 |
| `synapse` | 环境变量或 `PROVISA_ENGINE_URL` | 基于 TDS 的 T-SQL，Azure AD 认证 |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | `secure` 提示切换 TLS；端口默认 8123/8443 |
| `elasticsearch` | host/port + `mapping` DSL | |
| `pinot` | host/port | Controller REST 端点 |
| `druid` | host/port | Broker Avatica 端点 |
| `exasol` | host/port | |
| **数据湖** | | |
| `delta_lake` | `path`（表 URI） | DuckDB `delta_scan`；对象存储访问通过 `federation_hints` |
| `iceberg` | `path`（表 URI） | DuckDB `iceberg_scan`；对象存储访问通过 `federation_hints` |
| `hudi` | `path`（表 URI） | ClickHouse Hudi 引擎，零拷贝（REQ-1178） |
| `hive` | host/port（元存储）+ `mapping.storage` | 存储后端在 `mapping["storage"]` 中：hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port（元存储）+ `mapping` S3 键 | 独立类型；始终使用 S3 存储（REQ-229） |
| **NoSQL** | | |
| `mongodb` | host/port | 普通连接字段；没有映射 DSL |
| `cassandra` | host/port | 普通连接字段；没有映射 DSL |
| `redis` | host/port + `mapping` DSL | |
| **流处理** | | |
| `kafka` | 仅注册 | 实际配置位于 `kafka_sources[]` 中；参见下方的 §Kafka |
| `websocket` | host/port/path + `federation_hints` | 外部 WebSocket 数据源 |
| `rss` | host/port/path + `federation_hints` | RSS 2.0 / Atom 订阅源 |
| **图/语义** | | |
| `neo4j` | [端到端映射未验证] | |
| `sparql` | [端到端映射未验证] | |
| **文件** | | |
| `sqlite` | `path` | 始终通过引擎路由（无直接连接池） |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path`（目录） | 通配爬取；将 CSV/Parquet/XLSX/JSON 作为表呈现 |
| **API/远程** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port 或 `mapping.url` + `mapping` DSL | |
| `graphql_remote` | `base_url` + 可选 `mapping` | 请求头、转发客户端请求头、超时均在 `mapping` 中 |
| `openapi` | `base_url` | |
| `grpc_remote` | [端到端映射未验证] | |
| `airport` | `base_url`（Flight 位置） | DuckDB airport 扩展（REQ-899） |
| `ingest` | 推送接收器 | 外部服务通过 POST 提交 JSON 事件 |
| **SaaS** | | |
| `sharepoint` | `base_url` 或 `host` + `mapping` | 通过 `mapping.auth_type` 认证 |
| `splunk` | `host`/`port` 或 `base_url` + `mapping` | |
| **政府数据** | | |
| `govdata` | subject + `domain_id` | 独立的 `GovDataSource` 模型；参见下方的 §GovData |
| **数据质量** | | |
| `soda` | 指向 Provisa pgwire 的 host/port | 需要 `soda` 扩展；Elastic License 2.0，仅限自托管（REQ-1443） |
| `great_expectations` | 指向 Provisa pgwire 的 host/port | 需要 `gx` 扩展；Apache 2.0（REQ-1443） |

### 数据源类型参考

需要非显而易见配置的类型各自在下方有简短条目。关系型数据库类型（postgresql、mysql 等）仅使用上面的通用字段——无需额外小节。

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

`govdata` 数据源使用独立的顶层模型 `GovDataSource`，而非通用的 `Source`。（REQ-540）访问权限按主题分组进行分区。

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

每个 subject 对应一个或多个 GovData 架构（schema）。配置一个带有 subject 的 `govdata` 数据源会自动暴露该 subject 的所有架构。（REQ-540）

| Subject | 架构（Schema） |
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

`ref` 和 `geo` 架构始终作为关联架构被包含在内——不可配置，也未在上表中列出。（REQ-541）使用 subject `ALL` 可授予对所有架构的访问权限。[tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

`sources:` 中的 `kafka` 行仅用于注册。其连接器的 `details()` 返回 `{}`——真实配置位于顶层的 `kafka_sources[]` 块中，而不在 `sources:` 行中。Kafka 始终是 VIRTUAL_SOURCE（通过引擎路由；无直接连接池）。[tool-verified: `provisa/transpiler/router.py:44-63`]

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

**时间窗口** —— `default_window` 将每次查询限定在一个最近的时间段内，防止对高吞吐量主题进行无边界读取。（REQ-148）格式：`1h`、`30m`、`7d`、`60s`。默认为 `1h`。自动注入为 `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`。客户端可以在 GraphQL 的 `where` 参数中提供自己的 `_timestamp` 过滤条件来覆盖它。

**判别器（Discriminator）** —— 多个主题配置可以指向同一个物理 Kafka 主题，通过不同的 `discriminator` 值生成不同的 GraphQL 类型。（REQ-149）判别器会自动注入为 WHERE 子句。

**架构来源（Schema Source）**

| 值 | 行为 |
| ------- | ---------- |
| `registry` | 从 Confluent Schema Registry 获取架构 |
| `manual` | 在配置中内联定义列（无需 Schema Registry） |
| `sample` | 从样本消息中自动发现 |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`federation_hints` 中的 `account` 为必填项。`warehouse`、`role` 和 `schema` 为可选项。

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

`federation_hints` 中的 `http_path` 为必填项。`password` 携带个人访问令牌。`catalog` 为可选项（携带在 SQL/提示中，而非 `database` 字段）。

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

`federation_hints` 中的 `project` 为必填项。认证使用 `GOOGLE_APPLICATION_CREDENTIALS`（服务账号密钥文件的路径）或引擎环境中的应用默认凭据。

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

两者都使用基于 TDS 的 T-SQL，并采用 Azure AD 认证。使用 `az login`（开发环境）或托管身份（生产环境）进行认证——引擎通过 `azure-identity` 的 `DefaultAzureCredential` 读取凭据。连接详情来自环境变量：`FABRIC_SQL_SERVER` / `FABRIC_DATABASE`（Fabric）或 `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE`（Synapse），或通过 `PROVISA_ENGINE_URL`。

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`federation_hints` 中的 `secure` 用于在 HTTP 接口上启用 TLS。端口默认为 `8123`（明文）或 `8443`（当 `secure: "true"` 时）。`federation_hints` 中的 `schema` 会覆盖远程架构。[tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`path` 是表的 URI（S3、GCS、ADLS 或本地）。对象存储访问需要 `federation_hints` 中的凭据。对于 Cloudflare R2，需添加 `account_id`。

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

`host` 和 `port` 指向 Hive Thrift 元存储（默认端口 9083）。对于 `hive`，设置 `mapping["storage"]` 以选择对象存储后端。缺失必填键会明确失败——没有回退值。[tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` 是一个独立类型，始终声明 S3 存储（REQ-229）；无需 `mapping.storage`。

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

`mapping.storage` 接受的值：`hadoop`（默认）、`hdfs`、`local`、`s3`、`azure`、`adls`。S3 映射键：`endpoint`、`access_key_id`、`secret_access_key`、`region`、`path_style`。ADLS 映射键：`storage_account`、`access_key` 或 `sas_token`。

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
          key_pattern: "sessions:*"
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

当 `host:port` 和 `mapping.url` 同时存在时，`mapping.url` 优先。

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

`federation_hints` 中的 `spreadsheet_id` 为必填项。认证在挂载（attach）时使用 DuckDB 的 `gsheet` SECRET。

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### 文件数据源（csv / parquet / sqlite / files）

`path` 为必填项。`files` 会爬取一个目录中的 CSV、Parquet、XLSX 和 JSON 文件，将每个文件呈现为一张表。所有基于文件的数据源都是 VIRTUAL（通过引擎路由；无直接连接池）。[tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

**Kaggle 数据集**无法通过此文件添加——它们需要实时令牌以及仅在“数据源”表单中提供的数据集选择器（数据源 → 订阅 → Kaggle）。导出为 YAML 的 Kaggle 数据源会显示为 `type: files`，并在 `federation_hints` 中带有 `kaggle_owner` 和 `kaggle_ref`。从 Kaggle 重新下载需要 `refreshKaggleSource` 变更（mutation）或 UI 刷新流程，而不是编辑 YAML。参见数据源类型参考中的 [Kaggle 数据集](sources.md#kaggle-datasets)。

#### API / 远程数据源

**openapi** —— 将 `base_url` 设置为 OpenAPI 的基础 URL。架构发现会在启动时读取 OpenAPI 规范。

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** —— 设置 `base_url`。可选的 `mapping` 键：`headers`（静态请求头的字典）、`forward_client_headers`（布尔值）、`timeout_seconds`（整数）。[tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

#### 数据质量检查器（soda / great_expectations）

[tool-verified: `provisa/dq/registration.py`, `provisa/events/source_loader.py` `make_dq_loader`]

检查器数据源指向 Provisa 自身的 pgwire 端点，因此一个 postgres 驱动就能扫描由 Snowflake 或 Iceberg 支撑的表的联邦视图。扫描所用的身份是显式声明的，而非隐式继承——策略应用于该连接，经过过滤的行集不得产生静默通过的检查。连接键来自 `mapping`：`host`、`port`、`database`、`user`、`password`。

```yaml
sources:
  - id: dq
    type: soda                 # or great_expectations
    domain_id: sales-analytics
    mapping:
      host: localhost
      port: 5439               # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner
      password: ${env:PROVISA_DQ_PASSWORD}
```

每个结果表都携带 `dq_contract`——原样保存的 Soda 契约 YAML 或 Great Expectations 套件 JSON。列、水位线（watermark）和推广（promotion）均由此派生；完整的派生过程参见[数据质量检查器](sources.md#data-quality-checkers-req-1443)。

**安装期选择。**检查器不会被链接进来——扫描运行在一个子解释器中，只有在运维人员指定其名称时才会安装该库。每条安装路径（`install.sh`、`packaging/linux/first-launch.sh`，以及通过 `PROVISA_DQ_CHECKER` 的 macOS 向导）都会将选择写入 `~/.provisa/config.yaml`：

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` 读取该键并导出 `PROVISA_EXTRAS`，`docker-compose.app.yml` 将其作为构建参数传给 `Dockerfile` 的 `ARG PROVISA_EXTRAS`：[tool-verified: `scripts/provisa:69-79`]

| `dq_checker` | `PROVISA_EXTRAS`（Docker 层级） | 原生 venv 安装 |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

安装演示数据集会将 `none` 提升为 `gx` 并说明原因，因为演示配置为 `pet_store.pets` 注册了一个 Great Expectations 套件，否则其数据质量记分卡将无内容可展示。指定 `soda` 时则保持 `soda`。

通过 pip 而非安装程序获取演示环境会跳过该向导步骤，因此 `demo` 扩展会携带相同的检查器：`provisa run --demo` 的扫描所需要的正是 `pip install 'provisa[embedded,demo]'`。若缺少它，扫描会报告 `data-quality checker 'great_expectations' is not installed`，并给出安装命令。

任何其他值都会阻止启动器运行，而不是在缺少运维人员所要求的检查器的情况下启动。`soda` 扩展会拉取 `soda-postgres`；`gx` 会拉取 `great-expectations[postgresql]`。Soda Core 采用 Elastic License 2.0——`config/capabilities.yaml` 将该选项标记为 `cloud_eligible: false`，托管平面会拒绝它。

## 域（Domains）

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

命名权威是面向客户端名称的唯一真实来源；物理后端列名永远不会暴露给客户端。（REQ-194）每种查询语言依据 `column.alias`（如已设置）来推导列名，否则通过其配置的约定从物理列名推导。（REQ-194）

GraphQL 约定是三种预设枚举之一。（REQ-416）旧的自由格式字符串（`none`、`snake_case`、`camelCase`、`PascalCase`）已废弃。（REQ-416）

| 预设 | 默认 | 类型名 | 字段名 | 变更（Mutation）名 |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | 是 | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

默认的 GraphQL 约定是 `apollo_graphql`，它产生 camelCase 的字段名和变更名。（REQ-194、REQ-416）SQL 约定是独立的，默认值为 `snake_case`，通过 `apply_sql_name()` 应用；GraphQL 约定通过 `apply_gql_name()` 应用，CQL 名称由 GraphQL 名称派生。（REQ-194）

`domain_prefix: bool` 是一个与所选预设无关的正交选项。（REQ-416）

显式的 `column.alias` 是权威名称：SQL 会原样使用它，不应用任何约定；GraphQL 会对它应用其约定；CQL 由 GraphQL 名称派生。（REQ-194）

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

当 `domain_prefix: true` 时，所有 GraphQL 字段名和类型名都会以域 ID 为前缀，并用双下划线分隔：（REQ-154）

| 表 | 域 | 字段名 |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

这可以防止不同域中同名表产生名称冲突，并使查询更具自说明性。

### 命名规则

在生成 GraphQL 字段名时对表名应用的正则规则。按顺序应用，在唯一性解析之前执行。（REQ-542）

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

表和列别名会覆盖默认的 GraphQL 名称。（REQ-155）适用场景：

- 重命名晦涩的数据库名称（例如 `tbl_cust_seg` → `customer_segments`）
- 避免在 API 层出现缩写
- 建立清晰的、面向领域的词汇体系

### 描述

表和列的描述会包含在生成的 GraphQL SDL 中。（REQ-156）它们会出现在 GraphiQL 的文档浏览器和内省（introspection）查询中。可以在配置 YAML 中设置，也可以通过管理界面设置。

### Path（计算型 JSON 提取）

列可以使用点号表示法的 `path` 从 JSON/JSONB 源列中提取值。（REQ-151）这对 Kafka 消息、MongoDB 文档或 PostgreSQL JSONB 列中的半结构化数据很有用。

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

path 格式为 `source_column.key1.key2...`。编译器会在 SQL 中生成 `json_extract_scalar(source_column, '$.key1.key2')`。（REQ-151）

**路由影响：**Path 列使用 PostgreSQL 的 JSON 运算符（`->>`），直接 PG 路由天然支持它。（REQ-152）对于非 PostgreSQL 数据源（MySQL、SQL Server 等），带 path 列的查询会自动通过联邦引擎路由。（REQ-152）由于 path 列是只读的计算字段，变更（Mutation）不受影响。（REQ-153）

### 脱敏类型

| 类型 | 字段 | 说明 |
| ------ | -------- | ------------- |
| `regex` | `pattern`、`replace` | REGEXP_REPLACE（仅限字符串列） |
| `constant` | `value` | 字面值替换（NULL、0、MAX、MIN、自定义） |
| `truncate` | `precision` | DATE_TRUNC（仅限日期/时间戳列） |

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

在关系上设置 `materialize: true`，可为跨数据源 JOIN 自动生成物化视图。（REQ-158）这可以避免昂贵的联邦查询，通过预先计算 JOIN 结果实现。

- 只有跨数据源的关系才会生成物化视图（同数据源的 JOIN 本身已经很快）（REQ-159）
- 对于以关联表（junction）为支撑的关系，物化视图覆盖的是两跳遍历——源表这一跳、关联表这一跳、判别器，以及关联表自身的列作为边属性。关联表本身也算一跳，因此只要三张表中任意一张位于不同的数据源，该边就算跨数据源（REQ-1586）
- 物化视图起始状态为过期（stale），由后台刷新循环负责填充（REQ-160）
- 对任一源表的变更（Mutation）会将该物化视图标记为过期，等待重新刷新（REQ-543）
- `refresh_interval` 默认值为 300 秒（5 分钟）（REQ-543）

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

带有 `parent_role_id` 的角色会继承父角色的能力（capabilities）、域访问权限、列和对象授权，以及行级安全（RLS）规则，子角色针对某张表自定义的 RLS 规则优先于父角色的规则。（REQ-215、REQ-1677）继承链在启动时被展平。（REQ-215）

### 能力

| 能力 | 说明 |
| ----------- | ------------- |
| `source_registration` | 注册数据源 |
| `table_registration` | 注册表 |
| `relationship_registration` | 定义关系 |
| `security_config` | 配置行级安全（RLS）、脱敏 |
| `query_development` | 执行查询 |
| `full_results` | 绕过采样限制 |
| `admin` | 全部能力 |

## 行级安全（RLS）规则

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
      # REQ-1586: add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## 视图（受治理的计算数据集）

视图是具有完整列级治理能力的、由 SQL 定义的计算数据集。（REQ-133）它们是向语义层添加聚合、转换和派生指标的受治理机制。（REQ-136）

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

| 字段 | 是否必需 | 说明 |
| ------- | ---------- | ------------- |
| `id` | 是 | 唯一的视图标识符 |
| `sql` | 是 | 定义该视图的 SQL SELECT 语句 |
| `domain_id` | 是 | 架构可见性所属的域 |
| `materialize` | 否 | `true` = 周期性 CTAS 刷新，`false` = 实时联邦视图 |
| `refresh_interval` | 否 | 刷新间隔的秒数（仅物化视图适用，默认 300） |
| `description` | 否 | 出现在 GraphQL SDL 中 |
| `alias` | 否 | 覆盖 GraphQL 名称 |
| `columns` | 是 | 带可见性、脱敏、描述的列定义 |

### 物化 vs 实时

- **`materialize: true`**：Provisa 通过 CTAS 创建一张表，并按计划刷新它。（REQ-135）查询更快，但数据可能会滞后至多 `refresh_interval` 秒。
- **`materialize: false`**：Provisa 创建一个联邦视图。（REQ-135）查询始终返回实时数据，但对于复杂聚合可能较慢。

视图与表经过相同的治理流水线——行级安全（RLS）、脱敏、采样，以及基于角色的可见性。（REQ-134）这确保了在没有数据管家（steward）监督的情况下，不会有新语义被加入平台。（REQ-136）

### 只读查询视图

无论 `materialize: true` 还是 `materialize: false`，视图暴露的 GraphQL 类型都是只读查询的。不会为 `view_sql` 支撑的关系生成 insert、upsert、update 或 delete 变更（Mutation）。（REQ-1157）[tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## 缓存

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### 缓存层级

TTL 解析顺序（最具体者优先）：**表** > **数据源** > **全局默认值**。（REQ-544）取第一个非空值。

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

在数据源上设置 `cache_enabled: false` 会为该数据源下的所有表禁用缓存，无论表级 TTL 如何设置。（REQ-544）缓存键始终包含 `role_id` 以及行级安全（RLS）上下文值，用于安全分区。（REQ-544）

## 认证

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

### 认证提供方类型

| 提供方 | 使用场景 | 令牌校验 |
| ---------- | ---------- | ----------------- |
| `simple` | 本地开发/测试。用户在 YAML 中定义。 | 使用 `PROVISA_JWT_SECRET` 签名的 JWT |
| `firebase` | Firebase 身份认证（所有方式）。 | `firebase-admin` SDK 的 `verify_id_token()` |
| `keycloak` | Keycloak OIDC。映射租户与客户端角色。 | 基于 JWKS 的 JWT 校验 |
| `oauth` | 通用 OIDC（Okta、Azure AD、Auth0、PingFederate）。 | 来自发现 URL 的 JWKS |
| `basic` | 自包含部署。账户存储在 Provisa 自身的存储中。 | bcrypt 密码，或 pgwire 上的 SCRAM-SHA-256 |

超级用户凭据（`superuser` 块）适用于任何提供方，并始终解析为拥有全部能力的 admin 角色。（REQ-125）用于在配置外部认证之前进行初始设置。

### SCRAM-SHA-256（`auth.scram`）

```yaml
auth:
  provider: basic
  scram: true
```

使 pgwire 通过 `SCRAM-SHA-256` 播发 SASL，从而以证明的方式验证密码，而不是以明文发送。（REQ-1394）它仅适用于 `basic` 提供方——没有其他提供方持有 SCRAM 所需的 RFC 5802 验证器——且不提供通道绑定（channel binding）。

验证器无法从现有的 bcrypt 哈希派生。每当密码以明文形式经过一次，就会写入一个验证器，因此每个用户的首次 SCRAM 连接紧随其下一次注册、登录、修改密码或管理员重置之后。在此之前，该用户的连接会回退为通过 TLS 的明文交换；线路上不会暴露谁已经完成了迁移。

### 登录限流（`auth.login_throttle`）

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

默认按图示值开启；该配置块仅用于调整这些值。（REQ-1393）计数器位于凭据校验层，因此通过 HTTP、pgwire 和 Bolt 发生的失败会累计到同一主体上，一次锁定会在每个接口上都生效。它是按进程隔离的：多个 API 工作进程各自都允许最多 `max_attempts` 次尝试。

### 个人访问令牌

个人访问令牌（PAT）不需要任何配置块——它们始终被接受，其存储与其余控制平面架构一同创建。（REQ-1263）可配置的是用户在签发时可以请求的有效期：1 到 366 天，或者选择永不过期。参见[安全模型](security.md#personal-access-tokens)。

### 双向 TLS（Mutual TLS）

客户端证书校验是通过环境变量配置的，而非在 `provisa.yaml` 中，它扩展自 TLS 证书设置所在的同一组环境变量。（REQ-1228）

| 变量 | 默认值 | 含义 |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 未设置 | 允许签发客户端证书的 CA 的 PEM 包。设置它会开启客户端证书校验 |
| `PROVISA_MTLS_MODE` | 一旦设置了 CA 则为 `required` | `required` 或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | 要求证书的通用名称（common name）与连接认证所使用的用户名一致 |

每一项都可以按协议使用与 TLS 设置相同的命名方式进行覆盖。设置了模式却未设置 CA，或者模式取值既非 `required` 也非 `optional`，都会拒绝启动，而不是让运维人员误以为连接已经过验证。

### 通过 TLS 寻址组织

无需配置任何内容。在多组织部署中，pgwire 和 Bolt 会从客户端拨号所用的主机名中读取组织信息，该主机名携带在 TLS ClientHello 中，与 HTTP 从 `Host` 请求头中读取的方式完全一致。（REQ-1234）连接到 `acme.provisa.dev` 的客户端请求的是组织 `acme`；除非已认证的主体是该组织的成员，否则请求会被拒绝。通过 IP 地址连接则不请求任何组织，这在单组织部署中适用于每一条连接。

### 完整认证配置示例（已注释）

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

## Upsert 变更（Mutation）

对于拥有主键的表，Provisa 会自动生成 `upsert_<table>` 变更字段。（REQ-212）它们会编译为目标方言中的 upsert 语句——在 PostgreSQL 上是 `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`，在 MySQL 上是 `ON DUPLICATE KEY UPDATE`。（REQ-212）

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

冲突列由主键元数据派生。（REQ-212）所有列可见性和写权限规则均适用。

## Distinct On

`distinct_on` 参数会为指定列的每个不同取值选取第一行。（REQ-213）在根查询字段上可用。

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

在 PostgreSQL 中编译为 `SELECT DISTINCT ON (region) ...`。（REQ-213）对于非 PG 方言，使用基于窗口函数的回退方案。（REQ-213）

## 列预设

在插入/更新时自动注入列的值。（REQ-214）按表在配置中定义。

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

| Source | 行为 |
| -------- | ---------- |
| `header` | 从指定的 HTTP 请求头注入值 |
| `now` | 注入 `NOW()`（当前时间戳） |
| `literal` | 注入一个常量值 |

预设列在 SQL 生成之前的变更（Mutation）编译阶段被注入。（REQ-214）它们在变更的输入类型中不可见。（REQ-214）

## 继承角色

角色可以通过 `parent_role_id` 继承自一个父角色。（REQ-215）继承链在启动时被展平。（REQ-215）子角色拥有其所有祖先角色能力和域访问权限的并集；授予某个祖先角色的列、指标、函数或 webhook 也会授予该子角色；祖先角色的行级安全（RLS）规则会按表逐一应用于子角色，遵循就近优先原则，子角色针对某张表的自有规则会替换父角色的规则。（REQ-1677）

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

支持多级继承。（REQ-215）子角色显式设置的能力和 domain_access 会与父角色的合并。（REQ-215）父角色必须是已存在的角色，不能是自身，也不能形成循环；任何违反都会在保存时被拒绝。（REQ-1677）

## 计划触发器

按 cron 计划调用 webhook URL 的触发器。（REQ-216）使用 APScheduler。（REQ-216）

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

计划任务通过管理界面（启用/禁用开关）或 `toggle_scheduled_task` 管理变更（Mutation）进行管理。（REQ-216）

## OrderBy 格式

OrderBy 使用 `{column: direction}` 格式，direction 是一个含 6 个取值的枚举：（REQ-200、REQ-201）

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

关系排序通过嵌套对象支持：（REQ-202）

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

Provisa 运行两条独立的 OTLP 导出路径：你自己的内部采集器和可选的 Provisa 支持端点。（REQ-545）每条路径都有各自的过滤器。过滤器在跨度（span）离开进程之前，运行在一个包裹层 `_FilteringExporter` 内部——原始的跨度对象永远不会被修改。（REQ-546）[tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** —— 控制哪些数据到达你的内部采集器。

| 键 | 类型 | 默认值 | 说明 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | 将 `db.statement` 中的字符串和数字字面值替换为 `?` |
| `redact_attributes` | list[str] | `[]` | 从每个跨度中完全删除的属性键 |

**`support_telemetry_filter`** —— 控制哪些数据到达 Provisa 支持端点。在此路径上，SQL 字面值脱敏默认开启为 `true`，因为查询数据归你所有。（REQ-547）[tool-verified: `provisa/api/otel_setup.py` line 240]

| 键 | 类型 | 默认值 | 说明 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | 将 `db.statement` 中的字符串和数字字面值替换为 `?` |
| `redact_attributes` | list[str] | `[]` | 从每个跨度中完全删除的属性键 |

脱敏后的 `db.statement` 示例——当 `redact_sql_literals: true` 时，这个跨度属性：

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

会变为：

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### 支持端点 [tool-verified]

`support_endpoint`（或环境变量 `PROVISA_SUPPORT_OTLP_ENDPOINT`）会将遥测数据转发给 Provisa 支持团队用于诊断。（REQ-548）未设置时，不会有任何数据通过此路径离开你的基础设施。（REQ-548）支持端点的过滤器独立于内部过滤器生效——你可以对两条导出路径都做 SQL 字面值脱敏，同时仍与支持团队共享跨度的时间和错误数据。（REQ-545）[tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### 端点协议探测 [tool-verified]

Provisa 会根据端点 URL 的 scheme 选择 OTLP/HTTP 或 OTLP/gRPC。（REQ-549）以 `http://` 或 `https://` 开头的 URL 使用 OTLP/HTTP，并自动附加 `/v1/traces`、`/v1/metrics`、`/v1/logs`。（REQ-549）其他任何 scheme 都使用 `insecure=True` 的 OTLP/gRPC。（REQ-549）[tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## 联邦引擎

配置联邦引擎是可选的。默认值为 `duckdb`——零配置、进程内运行、无需外部服务（REQ-989）。当你需要 MPP 规模，或想复用现有数据仓库时，可选择其他引擎。

优先级：`PROVISA_ENGINE` 环境变量 → 持久化的管理界面 `federation_engine` 配置字段 → `duckdb`。更改在服务重启后生效。[tool-verified: `engine.py` `build_engine`]

### 引擎概览 [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| 引擎键 | 标签 | 方言 | MPP | 外部链接机制 | 认证 |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa 联邦引擎 | Trino SQL | 是 | Trino 目录（广泛的连接器集合） | JDBC 凭据 |
| `trino-byo` | Trino | Trino SQL | 是 | 与 `trino` 相同；非托管协调器 | JDBC 凭据 |
| `pg` | PostgreSQL | PostgreSQL | 否 | FDW / pg_duckdb | PostgreSQL 凭据 |
| `duckdb` | DuckDB | DuckDB | 否 | 扩展原生 ATTACH | 无（进程内） |
| `clickhouse` | ClickHouse（嵌入式） | ClickHouse | 是 | S3 / IcebergS3 / DeltaLake 表引擎 | chdb（进程内，无需认证） |
| `clickhouse-server` | ClickHouse（服务器/云） | ClickHouse | 是 | S3 / IcebergS3 / DeltaLake 表引擎 | ClickHouse 凭据 |
| `snowflake` | Snowflake | Snowflake | 是 | 外部 stage + 外部表 | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | 是 | 通过 REST 的 Unity Catalog 外部表 | `PROVISA_ENGINE_URL`（承载令牌 + `http_path`） |
| `bigquery` | BigQuery | BigQuery | 是 | BigQuery 外部表/BigLake 表 | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | 是 | OneLake 快捷方式 → OPENROWSET | Azure AD（`az login` 或托管身份） |
| `synapse` | Azure Synapse | T-SQL | 是 | ADLS OPENROWSET / 外部表 | Azure AD |
| `mysql` | MySQL | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `mariadb` | MariaDB | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `oracle` | Oracle Database | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `mssql` | Microsoft SQL Server | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `db2` | IBM Db2 | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `redshift` | Amazon Redshift | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `greenplum` | Greenplum | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `cockroachdb` | CockroachDB | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `yugabytedb` | YugabyteDB | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `opengauss` | openGauss | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `tidb` | TiDB | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `singlestore` | SingleStore | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `vertica` | Vertica | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `exasol` | Exasol | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `teradata` | Teradata Vantage | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `saphana` | SAP HANA | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `sapase` | SAP ASE (Sybase) | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `sqlanywhere` | SAP SQL Anywhere | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `monetdb` | MonetDB | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `firebird` | Firebird | 按方言 | 否 | 无（仅落地） | 按方言凭据 |
| `sqlalchemy` | 其他关系型数据库（按连接 URL） | 按方言 | 否 | 无（仅落地） | 按方言凭据 |

### 引擎参考

#### trino / trino-byo

`trino` 是托管的 Provisa 协调器；`trino-byo` 连接到你自己的 Trino 集群。两者都使用 Trino SQL，拥有最广泛的数据源类型覆盖范围。

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

物化存储默认使用 `TENANT_DATABASE_URL`（PostgreSQL）。

#### pg

通过 postgres_fdw（SQL/MED）和 pg_duckdb 扩展进行联邦。单节点；无 MPP。当你的数据已经存放在 PostgreSQL 中，并且只需连接少量远程数据源时最合适。

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

物化存储默认使用 `TENANT_DATABASE_URL`。

#### duckdb

进程内运行；无需外部服务。默认引擎（REQ-989）。`PROVISA_DATA_DIR` 控制嵌入式存储的位置（默认为 `~/.provisa`）。

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

物化存储默认使用 `~/.provisa/materialize.duckdb`——是唯一默认存储不是 PostgreSQL 的引擎。

#### clickhouse（嵌入式）/ clickhouse-server

`clickhouse` 使用 chdb（进程内）。`clickhouse-server` 连接到外部的 ClickHouse 实例或 ClickHouse Cloud。两者都通过 ClickHouse 原生表引擎直接读取 Delta Lake、Iceberg 和 Hudi。

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

物化存储默认使用 `TENANT_DATABASE_URL`。

#### snowflake

引擎即数据仓库：由 Snowflake 运行查询；Provisa 通过外部 stage 将数据源数据推送过去。

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

物化存储默认使用 `TENANT_DATABASE_URL`。

#### databricks

Unity Catalog 外部表将由 Provisa 管理的数据源桥接到 Databricks SQL。

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

物化存储默认使用 `TENANT_DATABASE_URL`。

#### bigquery

BigQuery 外部表和 BigLake 表。项目来自 URL 或 `GOOGLE_CLOUD_PROJECT`；通过服务账号密钥认证。

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

物化存储默认使用 `TENANT_DATABASE_URL`。

#### fabric / synapse

两者都使用基于 TDS 的 T-SQL，并采用 Azure AD 认证（`az login` 或托管身份）。省略 `PROVISA_ENGINE_URL` 时会改为从环境变量读取连接详情。

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

物化存储默认使用 `TENANT_DATABASE_URL`。

#### 关系型数据库引擎（mysql、mariadb、oracle、mssql、db2、redshift、greenplum、cockroachdb、yugabytedb、opengauss、tidb、singlestore、vertica、exasol、teradata、saphana、sapase、sqlanywhere、monetdb、firebird）以及 `sqlalchemy`

每种可通过网络访问的关系型数据库对应一个键，全部运行在同一个仅落地（land-only）的运行时上（不联邦到外部数据源）：每个数据源都落地到存储中并在那里被查询。该键选择数据库；`PROVISA_ENGINE_URL` 携带其方言所需的 DSN。`sqlalchemy` 是没有专属键的数据库的通用回退项。不提供文件内嵌式存储（SQLite、Access）——服务器必须可通过网络访问。

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

物化存储默认使用 `TENANT_DATABASE_URL`。

### 物化存储

当数据源无法实时挂载（所选引擎没有对应的 ATTACH 连接器）时，它会落地到该引擎的物化存储中。解析顺序：显式的 `PROVISA_MATERIALIZE_URL` → 引擎声明的默认值 → 硬性报错（无静默回退）。[tool-verified: `engine.py` `materialize_store`]

DuckDB 将其嵌入式文件（`~/.provisa/materialize.duckdb`）声明为默认值。所有其他引擎默认使用 `TENANT_DATABASE_URL`（PostgreSQL）。可以用 `PROVISA_MATERIALIZE_URL` 覆盖任意引擎的默认值。

### 按数据源的联邦提示

标准的 host/port/user/password 字段无法承载的扩展连接参数放在数据源的 `federation_hints` 中。各类型的提示键参见上方的数据源类型参考。以下是一个综合示例：

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

对于 Google Cloud 数据源，将 `GOOGLE_APPLICATION_CREDENTIALS` 设置为服务账号密钥文件的路径。对于 Fabric 和 Synapse，使用 `az login`（开发环境）或托管身份（生产环境）进行认证——引擎通过 `azure-identity` 的 `DefaultAzureCredential` 读取凭据。

## 环境变量

| 变量 | 默认值 | 说明 |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 配置文件路径 |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | 控制平面存储 URI（SQLAlchemy 异步）；对于嵌入式桌面存储，可接受 `sqlite+aiosqlite://…` / `duckdb://…`（REQ-828、REQ-850） |
| `PLATFORM_DATABASE_URL` | — | 平台注册表 URI（租户目录、引擎注册表）；启动时必需，无回退（REQ-837） |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` 使用嵌入式 fakeredis 而非 Redis 服务器——无需 Docker（REQ-829） |
| `PG_HOST` | `localhost` | PostgreSQL 主机 |
| `PG_PORT` | `5432` | PostgreSQL 端口 |
| `PG_DATABASE` | `provisa` | PostgreSQL 数据库 |
| `PG_USER` | `provisa` | PostgreSQL 用户 |
| `PG_PASSWORD` | `provisa` | PostgreSQL 密码 |
| `PROVISA_ENGINE` | `duckdb` | 联邦引擎键（REQ-989、REQ-916） |
| `PROVISA_ENGINE_URL` | — | 由 URL 驱动的引擎的连接 URL（Snowflake、Databricks、ClickHouse Server、BigQuery、SQLAlchemy） |
| `PROVISA_MATERIALIZE_URL` | — | 覆盖物化存储的 DSN（默认为引擎声明的默认值） |
| `PROVISA_DATA_DIR` | `~/.provisa` | 嵌入式 DuckDB 存储的数据目录（REQ-989） |
| `TRINO_HOST` | `localhost` | Trino 协调器主机 |
| `TRINO_PORT` | `8080` | Trino 协调器 HTTP 端口 |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | GCP 服务账号密钥 JSON 的路径（BigQuery 引擎/数据源） |
| `GOOGLE_CLOUD_PROJECT` | — | 默认 GCP 项目（BigQuery；会被 URL 覆盖） |
| `FABRIC_SQL_SERVER` | — | Fabric Warehouse SQL 端点（`PROVISA_ENGINE_URL` 的替代方式） |
| `FABRIC_DATABASE` | — | Fabric Warehouse 数据库名称 |
| `SYNAPSE_SQL_SERVER` | — | Synapse 无服务器 SQL 端点 |
| `SYNAPSE_DATABASE` | — | Synapse 数据库名称 |
| `REDIS_URL` | — | Redis 连接 URL |
| `PROVISA_SAMPLE_SIZE` | `10000` | 默认采样上限 |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | 当查询未提供显式 `LIMIT` 时的行数上限 |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | 一级读重试预算（秒）；采用全抖动（full jitter）的指数退避（REQ-703） |
| `ZAYCHIK_PORT` | `8480` | Zaychik Flight SQL 代理端口 |
| `FLIGHT_PORT` | `8815` | Provisa Arrow Flight 服务器端口 |
| `GRPC_PORT` | `50051` | Provisa Protobuf gRPC 服务器端口 |
| `PROVISA_REDIRECT_ENABLED` | `false` | 启用服务端阈值重定向 |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | 默认行数阈值 |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | 默认重定向格式 |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | 用于重定向结果的 S3 存储桶 |
| `PROVISA_REDIRECT_ENDPOINT` | — | 兼容 S3 的端点 URL |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | S3 访问密钥 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | S3 秘密密钥 |
| `PROVISA_REDIRECT_TTL` | `3600` | 预签名 URL 的 TTL（秒） |
| `PROVISA_MTLS_CLIENT_CA` | — | 允许签发客户端证书的 CA 的 PEM 包；设置它会在 pgwire、Bolt、gRPC 和 Flight 上开启客户端证书校验（REQ-1228） |
| `PROVISA_MTLS_MODE` | 一旦设置了 CA 则为 `required` | `required` 或 `optional`；任何其他值都会拒绝启动（REQ-1228） |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | 要求证书的通用名称与认证用户名一致（REQ-1228） |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | 允许从浏览器打开 Bolt WebSocket 的站点，逗号分隔；未设置时拒绝所有浏览器来源（REQ-802） |
| `PROVISA_EXTRAS` | `firebase,vector` | 内置到应用镜像中的 pyproject extras；`scripts/provisa` 根据 `~/.provisa/config.yaml` 中的 `dq_checker` 推导（REQ-1443） |
| `PROVISA_DQ_CHECKER` | `none` | 仅供安装程序使用：`none`/`soda`/`gx`，在非交互模式下由 `first-launch.sh` 读取，并作为 `dq_checker` 写入 `config.yaml`（REQ-1443） |
| `ANTHROPIC_API_KEY` | — | Claude API 密钥（发现用途） |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | 覆盖 `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | 覆盖 `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | 覆盖 `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | 覆盖 `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | 批处理跨度处理器的刷新延迟 |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | 覆盖 `observability.support_endpoint` |
