# 配置参考 {: #configuration-reference }

Provisa 通过 YAML 文件进行配置（默认：`config/provisa.yaml`）。

## 包含文件 {: #includes }

使用 `includes:` 将一份配置拆分到多个文件中。包含文件在此键下列出片段路径；Provisa 会在校验前合并它们，产生与将所有内容写在单个文件中相同的结果。

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

仅包含 `includes:` 的包装文件是合法的。`load_control_plane` 会遍历这些包含项，因此 `control_plane:` 部分取自设置了该项的那个被包含文件。

**合并规则**

- 路径相对于包含文件解析。绝对路径按原样使用。
- 列表类小节（`sources`、`tables`、`domains`、`relationships`、`roles` 等）采用追加方式——片段中的条目跟在包含文件的条目之后。
- 包含文件未设置的标量或映射键，取自片段。
- 两个文件对同一个键设置了不同的值，视为冲突；加载失败，并指出冲突的键。
- 两个文件中的相同值不视为冲突。
- `includes` 可以嵌套。一个文件——无论是直接还是通过另一个片段——包含自身会被拒绝。
- `includes` 在加载时被消费，永远不会出现在校验后的配置中。

## 数据源 {: #sources }

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

所有数据源共享同一组通用字段。

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
| `cache_ttl` | `null` | 秒；`null` 表示继承全局默认值；`-1` 永不过期 |
| `federation_hints` | `{}` | 每种连接器的扩展参数（dict[str,str]）；参见下方的类型参考。 |
| `mapping` | `{}` | 用于 redis、elasticsearch、prometheus 的映射 DSL。 |
| `allowed_domains` | `[]` | 将此数据源限制在特定的域 ID 内；为空表示不受限制 |
| `description` | `""` | |

### 支持的数据源类型 {: #supported-source-types }

| 类型 | 连接方式 | 说明 |
| ------ | ----------------- | ------- |
| **关系型数据库** | | |
| `postgresql` | host/port | Asyncpg 连接池；通过 `use_pgbouncer` 选择性启用 PgBouncer |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path`（数据库文件） | DuckDB firebird 社区扩展 |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | 复用 PostgreSQL 驱动/方言 |
| `yugabytedb` | host/port | 复用 PostgreSQL 驱动/方言 |
| `greenplum` | host/port | 复用 PostgreSQL 驱动/方言 |
| `tidb` | host/port | 复用 MySQL 驱动/方言 |
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
| `hudi` | `path`（表 URI） | ClickHouse Hudi 引擎，零拷贝 |
| `hive` | host/port（元存储）+ `mapping.storage` | 存储后端在 `mapping["storage"]` 中：hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port（元存储）+ `mapping` S3 键 | 独立类型；始终使用 S3 存储 |
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
| `airport` | `base_url`（Flight 位置） | DuckDB airport 扩展 |
| `ingest` | 推送接收器 | 外部服务通过 POST 提交 JSON 事件 |
| **SaaS** | | |
| `sharepoint` | `base_url` 或 `host` + `mapping` | 通过 `mapping.auth_type` 认证 |
| `splunk` | `host`/`port` 或 `base_url` + `mapping` | |
| **政府数据** | | |
| `govdata` | subject + `domain_id` | 独立的 `GovDataSource` 模型；参见下方的 §GovData |
| **数据质量** | | |
| `soda` | 指向 Provisa pgwire 的 host/port | 需要 `soda` 扩展；Elastic License 2.0，仅限自托管 |
| `great_expectations` | 指向 Provisa pgwire 的 host/port | 需要 `gx` 扩展；Apache 2.0 |

### 数据源类型参考 {: #source-type-reference }

需要非显而易见配置的每种类型在下面都有一个简短条目。RDBMS 类型（postgresql、mysql 等）只使用上面的通用字段，不需要单独的小节。

#### GovData

`govdata` 数据源使用自己的顶层模型 `GovDataSource`，而不是通用的 `Source`。访问权限按主题分组划分。

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

每个 subject 对应一个或多个 GovData 架构（schema）。配置一个带有 subject 的 `govdata` 数据源会自动暴露该 subject 的所有架构。

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

`ref` 和 `geo` 模式始终作为关联模式包含在内，无法配置，也不在上面的列表中。使用主题 `ALL` 可授予对所有模式的访问权。

#### Kafka

`sources:` 中的 `kafka` 行只用于注册数据源。其连接器的 `details()` 返回 `{}`；真正的配置在顶层的 `kafka_sources[]` 块中。Kafka 始终是 VIRTUAL_SOURCE：经由引擎路由，没有直连连接池。

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

**时间窗口** —— `default_window` 将每次查询限定在一个最近的时间段内，防止对高吞吐量主题进行无边界读取。格式：`1h`、`30m`、`7d`、`60s`。默认为 `1h`。自动注入为 `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`。客户端可以在 GraphQL 的 `where` 参数中提供自己的 `_timestamp` 过滤条件来覆盖它。

**判别器（Discriminator）** —— 多个主题配置可以指向同一个物理 Kafka 主题，通过不同的 `discriminator` 值生成不同的 GraphQL 类型。判别器会自动注入为 WHERE 子句。

**架构来源（Schema Source）**

| 值 | 行为 |
| ------- | ---------- |
| `registry` | 从 Confluent Schema Registry 获取架构 |
| `manual` | 在配置中内联定义列（无需 Schema Registry） |
| `sample` | 从样本消息中自动发现 |

#### Snowflake

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

#### Databricks

`federation_hints` 中的 `http_path` 是必需的。`password` 承载个人访问令牌。`catalog` 是可选的，放在 SQL 或 hints 中；`database` 字段不存放它。

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

#### BigQuery

`federation_hints` 中的 `project` 为必填项。认证使用 `GOOGLE_APPLICATION_CREDENTIALS`（服务账号密钥文件的路径）或引擎环境中的应用默认凭据。

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

两者都通过 TDS 使用 T-SQL，并采用 Azure AD 认证。用 `az login`（开发）或托管标识（生产）登录；引擎通过 `azure-identity` 的 `DefaultAzureCredential` 读取凭据。连接信息来自环境变量 `FABRIC_SQL_SERVER` / `FABRIC_DATABASE`（Fabric）或 `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE`（Synapse），也可以来自 `PROVISA_ENGINE_URL`。

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`federation_hints` 中的 `secure` 用于在 HTTP 接口上启用 TLS。端口默认为 `8123`（明文）或 `8443`（当 `secure: "true"` 时）。`federation_hints` 中的 `schema` 会覆盖远程架构。

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

#### Delta Lake / Iceberg

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

#### Hive / Hive S3

`host` 和 `port` 指向 Hive Thrift 元数据存储（默认端口 9083）。对于 `hive`，用 `mapping["storage"]` 选择对象存储后端。缺少必需的键会明确报错，没有回退值。

`hive_s3` 是一个独立类型，始终声明 S3 存储；无需 `mapping.storage`。

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

#### Redis

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

#### Elasticsearch

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

#### Prometheus

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

#### Google Sheets

`federation_hints` 中的 `spreadsheet_id` 为必填项。认证在挂载（attach）时使用 DuckDB 的 `gsheet` SECRET。

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### 文件数据源（csv / parquet / sqlite / files） {: #file-sources-csv-parquet-sqlite-files }

`path` 为必填项。`files` 会爬取一个目录中的 CSV、Parquet、XLSX 和 JSON 文件，将每个文件呈现为一张表。所有基于文件的数据源都是 VIRTUAL（通过引擎路由；无直接连接池）。

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

**Kaggle 数据集**不能通过此文件添加，因为它们需要有效令牌以及数据源表单中的数据集选择器（数据源 → 订阅 → Kaggle）。导出为 YAML 的 Kaggle 数据源显示为 `type: files`，并在 `federation_hints` 中带有 `kaggle_owner` 和 `kaggle_ref`。要重新从 Kaggle 下载，需要使用 `refreshKaggleSource` 变更或界面中的刷新；编辑 YAML 做不到。参见数据源类型参考中的 [Kaggle 数据集](sources.md#kaggle-datasets)。

#### API / 远程数据源 {: #api-remote-sources }

**openapi** —— 将 `base_url` 设置为 OpenAPI 的基础 URL。架构发现会在启动时读取 OpenAPI 规范。

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** —— 设置 `base_url`。可选的 `mapping` 键：`headers`（静态请求头的字典）、`forward_client_headers`（布尔值）、`timeout_seconds`（整数）。

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

**airport** —— `base_url` 是 Arrow Flight 服务器的位置。DuckDB airport 扩展。

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** —— 使用 `host`、`port`、`path` 和 `federation_hints`。

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

**sharepoint**

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

**splunk**

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

#### 数据质量检查器（soda / great_expectations） {: #data-quality-checkers-soda-great_expectations }

检查器数据源指向 Provisa 自己的 pgwire 端点，因此一个 postgres 驱动就能扫描背后是 Snowflake 或 Iceberg 的表的联邦视图。扫描身份需要显式声明，从不继承。策略作用于这个连接，被过滤的行集不能让检查悄悄通过。连接键来自 `mapping`：`host`、`port`、`database`、`user`、`password`。

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

每个结果表都带有 `dq_contract`，即原样保存的 Soda 契约 YAML 或 Great Expectations 套件 JSON。列、水位线和提升都由它推导；具体方式见[数据质量检查器](sources.md#data-quality-checkers)。

**安装时选择。** 检查器不会被链接进来：扫描在子解释器中运行，只有运营方指定时才安装相应的库。每条安装路径（`install.sh`、`packaging/linux/first-launch.sh`，以及通过 `PROVISA_DQ_CHECKER` 的 macOS 向导）都会把选择写入 `~/.provisa/config.yaml`：

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` 读取该键并导出 `PROVISA_EXTRAS`，`docker-compose.app.yml` 将其作为构建参数传给 `Dockerfile` 的 `ARG PROVISA_EXTRAS`：

| `dq_checker` | `PROVISA_EXTRAS`（Docker 层级） | 原生 venv 安装 |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

安装演示数据集会将 `none` 提升为 `gx` 并说明原因，因为演示配置为 `pet_store.pets` 注册了一个 Great Expectations 套件，否则其数据质量记分卡将无内容可展示。指定 `soda` 时则保持 `soda`。

如果通过 pip 而不是安装程序进入演示，就会跳过那一步向导，所以 `demo` 额外依赖包含同一个检查器：`provisa run --demo` 需要 `pip install 'provisa[embedded,demo]'` 才能运行扫描。缺少它时，扫描会报告 `data-quality checker 'great_expectations' is not installed`，并给出安装命令。

任何其他值都会让启动器停止，所以它不会在缺少运营方所要求检查器的情况下启动。`soda` 额外依赖会拉取 `soda-postgres`，`gx` 会拉取 `great-expectations[postgresql]`。Soda Core 采用 Elastic License 2.0：`config/capabilities.yaml` 把该选项标为 `cloud_eligible: false`，托管平面会拒绝它。

## 域（Domains） {: #domains }

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## 命名 {: #naming }

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### 命名约定 {: #naming-convention }

命名权威是面向客户端名称的唯一真实来源；物理后端列名永远不会暴露给客户端。每种查询语言依据 `column.alias`（如已设置）来推导列名，否则通过其配置的约定从物理列名推导。

GraphQL 约定是三种预设枚举之一。旧的自由格式字符串（`none`、`snake_case`、`camelCase`、`PascalCase`）已废弃。

| 预设 | 默认 | 类型名 | 字段名 | 变更（Mutation）名 |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | 是 | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

默认的 GraphQL 约定是 `apollo_graphql`，它产生 camelCase 的字段名和变更名。SQL 约定是独立的，默认值为 `snake_case`，通过 `apply_sql_name()` 应用；GraphQL 约定通过 `apply_gql_name()` 应用，CQL 名称由 GraphQL 名称派生。

`domain_prefix: bool` 是一个与所选预设无关的正交选项。

显式的 `column.alias` 是权威名称：SQL 会原样使用它，不应用任何约定；GraphQL 会对它应用其约定；CQL 由 GraphQL 名称派生。

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

### 域前缀 {: #domain-prefix }

当 `domain_prefix: true` 时，所有 GraphQL 字段名和类型名都会以域 ID 为前缀，并用双下划线分隔：

| 表 | 域 | 字段名 |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

这可以防止不同域中同名表产生名称冲突，并使查询更具自说明性。

### 命名规则 {: #naming-rules }

在生成 GraphQL 字段名时对表名应用的正则规则。按顺序应用，在唯一性解析之前执行。

## 表 {: #tables }

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
      - name: email
        visible_to: [admin, analyst]
        masks:                    # first entry the reader holds applies
          - to: [admin]           # admin sees raw
          - to: ["*"]             # everyone else sees it masked
            mask: {type: regex, pattern: "^(.{2}).*(@.*)$", replace: "$1***$2"}
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: constant, value: 0}
      - name: created_by
        visible_to: [admin, analyst]
      - name: created_at
        visible_to: [admin, analyst]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: truncate, precision: month}
    write_grants:                 # who may write this table, per role and operation
      - role: admin
        operation: insert
        columns: [email, amount]
        check: "TRUE"             # any row may be inserted
        presets:                  # set by the grant; the statement may not supply them
          - {column: created_by, source: session, name: user_id}
          - {column: created_at, source: now}
      - role: admin
        operation: update
        columns: [email, amount]
        rows: "TRUE"              # any row may be updated
        check: "TRUE"
```

表默认没有任何写入授权，因此任何角色都不能写入它。一条授权给某个角色表上的一种操作（`insert`、`update` 或 `delete`）。一张表对每个角色和操作最多保存一条授权。

| 授权字段 | 含义 |
| ----------- | ------- |
| `columns` | `INSERT` 可以列出、`UPDATE` 可以设置的列。`delete` 授权不指定任何列。 |
| `rows` | `UPDATE` 或 `DELETE` 可以触及的行：基于表中各列的 SQL 谓词。`insert` 授权没有此项。 |
| `check` | `INSERT` 写入的每一行、或 `UPDATE` 留下的每一行必须满足的条件：SQL 谓词。`delete` 授权没有此项。 |
| `presets` | 授权自己设置的列。语句不得提供这些列，因此预设列不会同时出现在 `columns` 中。 |

省略 `rows` 或 `check` 时，授权采用该角色在这张表上的读取行过滤器：除非授权另有规定，角色写入的就是它能读到的行。`"TRUE"` 表示所有行。旧的列级 `writable_by` 键和表级 `column_presets` 键会按名称被拒绝；在改为 `write_grants` 之前，配置无法加载。

每个界面上的每次写入都按以下顺序准入：角色持有 `write` 权利；角色持有表上该操作的授权；语句写入的每一列都在授权中；授权的预设被添加到语句中；`rows` 谓词以 AND 并入 `UPDATE` 或 `DELETE` 的 `WHERE`；在向数据源发送任何内容之前，对新行评估 `check`。`check` 无法判定的语句会被拒绝。授权的列同时决定 GraphQL 向角色提供哪些变更字段：角色持有授权的每种操作各提供一个字段，且恰好带有这些列。

有些语句需要更宽的授权。`TRUNCATE` 需要覆盖所有行的 `delete` 授权。`MERGE` 需要 `insert` 和 `update` 授权，二者都覆盖所有行。`INSERT ... ON CONFLICT DO UPDATE` 需要覆盖所有行且无预设的 `update` 授权。`COPY ... FROM STDIN` 需要无预设的 `insert` 授权。

授权可以继承。子角色针对每张表和每种操作，采用其链中最近的持有该授权的角色的授权，自己的授权优先。元角色持有其成员授权的折叠结果：成员各列的并集、各 `rows` 和 `check` 谓词的 OR，以及只保留所有成员设置一致的预设。

在管理界面中，表的**写入**部分会列出这些授权并统一编辑。

### 别名 {: #aliases }

表和列别名会覆盖默认的 GraphQL 名称。适用场景：

- 重命名晦涩的数据库名称（例如 `tbl_cust_seg` → `customer_segments`）
- 避免在 API 层出现缩写
- 建立清晰的、面向领域的词汇体系

### 描述 {: #descriptions }

表和列的描述会包含在生成的 GraphQL SDL 中。它们会出现在 GraphiQL 的文档浏览器和内省（introspection）查询中。可以在配置 YAML 中设置，也可以通过管理界面设置。

### Path（计算型 JSON 提取） {: #path-computed-json-extraction }

列可以使用点号表示法的 `path` 从 JSON/JSONB 源列中提取值。这对 Kafka 消息、MongoDB 文档或 PostgreSQL JSONB 列中的半结构化数据很有用。

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

path 格式为 `source_column.key1.key2...`。编译器会在 SQL 中生成 `json_extract_scalar(source_column, '$.key1.key2')`。

**路由影响：**Path 列使用 PostgreSQL 的 JSON 运算符（`->>`），直接 PG 路由天然支持它。对于非 PostgreSQL 数据源（MySQL、SQL Server 等），带 path 列的查询会自动通过联邦引擎路由。由于 path 列是只读的计算字段，变更（Mutation）不受影响。

### 脱敏类型 {: #masking-types }

| 类型 | 字段 | 说明 |
| ------ | -------- | ------------- |
| `regex` | `pattern`、`replace` | REGEXP_REPLACE（仅限字符串列） |
| `constant` | `value` | 字面值替换（NULL、0、MAX、MIN、自定义） |
| `truncate` | `precision` | DATE_TRUNC（仅限日期/时间戳列） |

## 关系 {: #relationships }

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

### 自动物化 {: #auto-materialization }

在关系上设置 `materialize: true`，可为跨数据源 JOIN 自动生成物化视图。这可以避免昂贵的联邦查询，通过预先计算 JOIN 结果实现。

- 只有跨数据源的关系才会生成物化视图（同数据源的 JOIN 本身已经很快）
- 对于以关联表（junction）为支撑的关系，物化视图覆盖的是两跳遍历——源表这一跳、关联表这一跳、判别器，以及关联表自身的列作为边属性。关联表本身也算一跳，因此只要三张表中任意一张位于不同的数据源，该边就算跨数据源
- 物化视图起始状态为过期（stale），由后台刷新循环负责填充
- 对任一源表的变更（Mutation）会将该物化视图标记为过期，等待重新刷新
- `refresh_interval` 默认值为 300 秒（5 分钟）

## 角色 {: #roles }

```yaml
roles:
  - id: admin
    description: Administers sources, tables, relationships and security.
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
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

带有 `parent_role_id` 的角色会继承父角色的能力（capabilities）、域访问权限、列和对象授权，以及行级安全（RLS）规则，子角色针对某张表自定义的 RLS 规则优先于父角色的规则。继承链在启动时被展平。

### 能力 {: #capabilities }

| 能力 | 说明 |
| ----------- | ------------- |
| `source_registration` | 注册数据源 |
| `table_registration` | 注册表 |
| `relationship_registration` | 定义关系 |
| `security_config` | 配置行级安全（RLS）、脱敏 |
| `query_development` | 执行查询 |
| `full_results` | 绕过采样限制 |
| `admin` | 全部能力 |

## 行级安全（RLS）规则 {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

## 物化视图 {: #materialized-views }

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
      # Add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## 视图（受治理的计算数据集） {: #views-governed-computed-datasets }

视图是具有完整列级治理能力的、由 SQL 定义的计算数据集。它们是向模型添加聚合、转换和派生指标的受治理机制。

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

### 物化 vs 实时 {: #materialized-vs-live }

- **`materialize: true`**：Provisa 通过 CTAS 创建一张表，并按计划刷新它。查询更快，但数据可能会滞后至多 `refresh_interval` 秒。
- **`materialize: false`**：Provisa 创建一个联邦视图。查询始终返回实时数据，但对于复杂聚合可能较慢。

视图与表经过同样的治理管道：RLS、脱敏、采样和基于角色的可见性。这样，任何新语义都无法在没有数据管家监督的情况下进入平台。

### 带历史的视图（时间旅行） {: #views-with-history-time-travel }

物化视图可以保存其行的每一个版本，并按过去的某个时刻读取。 它声明如何保存这些版本，以及版本属于哪个键：

```yaml
views:
  - id: customer-history
    sql: SELECT customer_id, tier, updated_at FROM crm.customers
    domain_id: sales
    materialize: true
    bitemporal_mode: delta            # snapshot | delta
    bitemporal_key: [customer_id]     # required
    bitemporal_source_time: updated_at  # optional: the source's change time
    columns:
      - {name: customer_id, visible_to: [analyst]}
      - {name: tier, visible_to: [analyst]}
      - {name: updated_at, visible_to: [analyst]}
```

| 字段 | 必填 | 说明 |
| ------- | ---------- | ------------- |
| `bitemporal_mode` | 是 | `snapshot` 在每次刷新时追加整个结果；`delta` 只追加发生变化的部分 |
| `bitemporal_key` | 是 | 版本所属的列 |
| `bitemporal_source_time` | 否 | 保存每一行在数据源中发生变化的时刻的列：时间旅行据以读取的时钟 |

**选择时钟。** 设置 `bitemporal_source_time` 时，版本按数据源自身的时间排序和读取：“截至上周二”指的是数据源中的上周二。不设置时，版本按 Provisa 落地每次变更的时间排序和读取，这跟随刷新节奏，而不是行在数据源中发生变化的时刻。两种选择都成立。加载时间适合刷新频繁的情况、数据源时间无关紧要的数据，以及本来就关心加载时间的读取，例如审计 Provisa 在什么时候持有什么数据。

一个版本从它自己的时刻起有效，直到下一个版本的时刻。删除没有数据源变更时间，在 Provisa 记录它时生效。

**读取。** 普通读取返回每个键当前有效的版本。标头 `X-Provisa-As-Of` 按某一时刻或某一范围读取，范围以 ISO 8601 区间 `start/end` 给出：

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```

时刻返回每个键在当时有效的版本。范围返回在其中任一时点有效的每个版本，每个版本一行。

**按记录时间。** 之后记录的更正会改变视图对过去的描述：一旦更正被记录，9 月 30 日生效的版本就是更正后的版本。若要按 Provisa 在某个时间点记录的样子读取视图（早于之后的任何更正），请加上 `X-Provisa-As-Recorded`，即一个 ISO 8601 时间：

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

在该时间之后记录的所有版本都会被排除，然后 as-of 选出在其自身时间生效的版本。只使用 `X-Provisa-As-Recorded` 时，读取返回按当时记录各键生效的版本。它只接受一个时间点，不接受范围；范围或格式错误的值会以 HTTP 400（`data.invalid_as_recorded`）被拒绝。

这两个时间也可以不通过请求头传给 Provisa。as-of 时间是 `resolve` 的 `as_of` 参数（GraphQL、gRPC、MCP）、REST 的 `as_of` 查询参数以及 Arrow Flight 票据的 `as_of` 选项；传给 `resolve` 的范围会返回行的 `versions`，而不是它的 `row`。as-recorded 时间是 `resolve` 的 `as_recorded` 参数、REST 的 `as_recorded` 查询参数以及 Arrow Flight 票据的 `as_recorded` 选项。每条语句的审计记录都会保存它读取时使用的两个时间，`as_of` 和 `as_recorded`。权限、掩码和行规则与任何读取一样适用。格式错误的 as-of 值，或结束时间不晚于开始时间的范围，会以 HTTP 400（`data.invalid_as_of`）被拒绝。

每个带历史的视图还会返回三列，其授权方式与它的数据源时间列相同（没有时则与第一个键列相同）：

| 列 | 含义 |
| ------- | ------- |
| `source_from` | 此版本何时生效：其数据源时间，没有时为加载时间 |
| `source_to` | 下一个版本何时生效；此版本仍有效时为空 |
| `recorded_at` | Provisa 何时记录此版本 |

**来自变更数据捕获的数据源时间。** 由变更数据捕获加载的表以 `source_committed_at` 携带其提交时间，以便带历史的视图将其指定为数据源时间：

- Debezium：每个变更事件携带的数据库提交时间（`source.ts_ms`）。由副本的完整构建复制过来的行没有变更事件，因此其 `source_committed_at` 为空，直到该行再次变化，除非数据源是启用了提交时间戳的 PostgreSQL（下一项）。
- 变更信号为 `native` 的 PostgreSQL：PostgreSQL 为每一行保存的提交时间（`pg_xact_commit_timestamp(xmin)`），由数据源自己的驱动在构建副本时读取。PostgreSQL 仅在 `track_commit_timestamp = on` 时保存它。启用它即可在原生信号下获得数据源时间；没有它时 `source_committed_at` 为空。开启或关闭该设置会重建副本。

没有 Debezium 信封的 Kafka 主题不携带提交时间。

### 只读查询视图 {: #query-only-views }

无论 `materialize: true` 还是 `materialize: false`，视图暴露的 GraphQL 类型都是只读查询的。不会为 `view_sql` 支撑的关系生成 insert、upsert、update 或 delete 变更（Mutation）。

## 缓存 {: #cache }

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### 缓存层级 {: #cache-hierarchy }

TTL 解析顺序（最具体者优先）：**表** > **数据源** > **全局默认值**。取第一个非空值。

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

在数据源上设置 `cache_enabled: false` 会为该数据源下的所有表禁用缓存，无论表级 TTL 如何设置。缓存键始终包含 `role_id` 以及行级安全（RLS）上下文值，用于安全分区。

### 按请求选择启用 {: #per-request-opt-in }

对于未请求响应缓存的请求，响应缓存处于关闭状态。请求通过以下方式选择启用：

| 接口 | 启用方式 |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | 在操作上使用 `@cached`，可选 `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | 在语句中加入注释行 `-- @provisa cache=true` 或 `-- @provisa cache_ttl=60` |
| Cypher（`/data/cypher`、Neo4j Query API、Bolt、Arrow Flight） | 在查询中加入注释行 `// @provisa cache=true` 或 `// @provisa cache_ttl=60` |
| gRPC（以及 HTTP gRPC 代理） | 调用元数据（代理上为请求头）`x-provisa-cache: true` 或 `x-provisa-cache-ttl: 60` |
| REST、JSON:API，以及作为文本中提示的替代方式：HTTP 上的 GraphQL、`/data/cypher`、Neo4j Query API | 请求头 `Cache-Control: max-age=60`（或 `max-stale=60`），或 `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

非整数的 `ttl` 会使请求失败。在文本中和请求头中，或在两个请求头中两次声明选择启用且值不同的请求，会以 `400` 失败；不会偏向任何一个。`Cache-Control: no-cache` 或 `no-store` 表示不缓存，与没有请求头相同。

没有该提示时，请求既不读取也不写入缓存。有提示时，仍由上述设置决定：只有当读取的每个数据源都设置了 `cache_enabled: true`，且每张表解析出的 TTL 大于 0 时，结果才会被缓存。若请求给出了 `ttl`，条目的存活时间即为该值；否则为所读取各表中最短的解析 TTL。

该提示只是以接受较旧的数据来换取速度。它绝不会使读取比运营方设置允许的更新：落地与副本的新鲜度（`cache_ttl`、`change_signal`）、`row_materialize`、`load_protected` 快照以及新鲜度检查，对带提示的请求与不带提示的请求完全相同地适用。请参阅[新鲜度与负载：由谁决定](#freshness-and-load-who-decides)。

`@noCache` 和 `-- @provisa no_cache=true` 已不再存在：缓存默认关闭，因此无需绕过。

写入会使写入方组织为其写入的表所持有的所有缓存条目失效；其他组织对同一表的条目不受影响。若失效操作失败，请求会失败，而不会留下过期条目。

## 新鲜度与负载：由谁决定 {: #freshness-and-load-who-decides }

有三方共同决定结果有多新，以及一次查询给其后端系统带来多大负载。

- **上游数据源。** 理想情况下它自行管理背压：连接数上限、语句超时、只读副本。
- **运维方。** 保护平台（即 Provisa 本身）免受背压。上游无法自我保护时，运维方也保护上游。相关设置位于数据源和表上：`load_protected`、`replicate`、数据源的 `federation_hints`、大结果重定向阈值、sink 写入的 Kafka 集群，以及每张表注册的 watermark。
- **最终用户。** 逐个请求地在速度和新鲜度之间取舍。

运维方的设置是下限。请求可以在下限之上移动，走向更旧的数据或更低的负载；选择启用响应缓存就是常见的例子。请求永远不能低于下限。会低于下限的请求提示会被拒绝，错误信息会指明运维方的设置，既不会被悄悄应用，也不会被悄悄丢弃。

| 请求输入 | 允许做什么 | 何时被拒绝 |
| --- | --- | --- |
| `@route(engine: DIRECT)`、`-- @provisa route=direct` | 为单个数据源选择直连驱动 | 语句读取的某张表为 `load_protected` 或 `replicate: 0` |
| `@join`、`@reorder`、`@broadcastSize`、`/*+ ... */` | 设置数据源 `federation_hints` 未设置的引擎会话属性 | 修改了运维方已设置的属性 |
| `X-Provisa-Redirect-Threshold`、`@redirect(threshold:)` | 更早地重定向结果 | 高于运维方的阈值 |
| `@sink(broker:)`、`X-Provisa-Sink` 中的 broker | 重复运维方的 broker | 指定了其他 broker，或未配置 `KAFKA_BOOTSTRAP_SERVERS` |
| 订阅中的 `@watermark` | 重复该表注册的 watermark | 指定了其他列，或该表没有 watermark |
| `@cached`、`-- @provisa cache=true` | 返回更旧的缓存结果 | 从不；缓存条目不会比写入它的那次读取更新 |

拒绝时返回 HTTP 403 及代码 `query.operator_floor`，pgwire 上返回 SQLSTATE `42501`，Flight 和 gRPC 上返回 `PERMISSION_DENIED`。

`load_protected` 或 `replicate: 0` 的表，在任何传输上都不会被查询实时读取。查询读取的是它的副本。只有复制及其刷新会读取数据源：当 `cache_ttl` 或新鲜度检查要求时才刷新；两者都没有时，副本只构建一次，之后仅通过变更流或调度器刷新。 对于带有 `off_peak_window`（在表或其数据源上，按 `off_peak_tz` 解读）的此类表，刷新只在窗口打开时运行：在窗口之外到期或被请求的刷新会等到窗口下一次打开，没有 `cache_ttl` 的窗口每次打开刷新一次。首次构建从不被推迟，因为对该表的读取在等待它。

### `replicate` 设置 {: #the-replicate-setting }

`replicate` 是表上的一个整数。未设置值的表沿用其数据源的值；两者都未设置时，适用全局阈值。副本是数据源表的一份拷贝，保存在引擎的存储中，并按表的时效性设置刷新；它不是持续的变更流。

| 值 | 名称 | 含义 |
| --- | --- | --- |
| 未设置 | 默认 | 适用全局阈值 |
| `-1` | 从不 | 凡存在实时路径之处均实时读取。仅在引擎无法就地读取数据源时才复制 |
| 大于 0 的 `N` | Hot-N | 实时读取，直到该表在每个间隔内被 N 条受治理语句读取，之后由其副本提供服务 |
| `0` | 始终 | 读取来自副本 |

只有“始终”是保证。“从不”和 Hot 值都是尽力而为。`load_protected` 与 `replicate: -1` 同时出现时，会在配置加载和保存时被拒绝。`replicate: 0` 的数据源在任何引擎上都没有实时挂载，因此它的每张表都由其副本提供服务。

下限按语句所读取的表来判断。不读取任何由副本提供服务的表的语句，即使其数据源还有其他被复制的表，也保留直连路由。

副本按其声明的主键，以及其声明的关系所连接的列建立索引，索引在副本落地后构建。这些索引在哪些情况下加速联接、副本应存放何处以及如何选择其新鲜度，参见 [Replicas](replicas.md)。

### Hot 复制 {: #hot-replication }

`replicate` 未设置或为大于 0 的 `N` 的表，在繁忙之前一直实时读取。

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| 设置 | 环境变量 | 默认值 |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

这三项都是运维方设置：管理界面在 Hot Tables 下显示它们，保存的值在下一次评估时生效。

表的计数方式：

- 一条受治理语句对其读取的每张表只计一次，无论读取多少次。
- 由响应缓存应答的语句不计数。它没有给数据源带来负载。
- 被治理拒绝的语句不计数。

表的计数达到其阈值（其自身的 `N`，否则是其数据源的，否则是 `hot_threshold`）时被提升，计数降到阈值的一半以下时被降级。提升会请求构建副本；在构建完成之前，该表仍实时读取，之后从副本读取。降级会立即让读取回到数据源。

表在下列情况下不会被提升：行数超过 `hot_max_rows`；其变更信号为 `ttl` 或 `ttl_probe`，而它和它的数据源都没有声明 `cache_ttl`；或 Redis 热层已经在管理它。管理界面中该表的摘要会说明是哪一种情况。

计数保存在 Redis 中。使用共享 Redis（`cache.redis_url`）时，计数覆盖部署的所有进程。使用内嵌 Redis 时，计数按进程各自统计，因此只有在单个工作进程处理请求时才会提升；有多个工作进程且没有共享 Redis 时不会提升任何表，摘要中会说明这一点。

## 认证 {: #authentication }

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

### 认证提供方类型 {: #auth-provider-types }

| 提供方 | 使用场景 | 令牌校验 |
| ---------- | ---------- | ----------------- |
| `simple` | 本地开发/测试。用户在 YAML 中定义。 | 使用 `PROVISA_JWT_SECRET` 签名的 JWT |
| `firebase` | Firebase 身份认证（所有方式）。 | `firebase-admin` SDK 的 `verify_id_token()` |
| `keycloak` | Keycloak OIDC。映射租户与客户端角色。 | 基于 JWKS 的 JWT 校验 |
| `oauth` | 通用 OIDC（Okta、Azure AD、Auth0、PingFederate）。 | 来自发现 URL 的 JWKS |
| `basic` | 自包含部署。账户存储在 Provisa 自身的存储中。 | bcrypt 密码，或 pgwire 上的 SCRAM-SHA-256 |

超级用户凭据（`superuser` 块）适用于任何提供方，并始终解析为拥有全部能力的 admin 角色。用于在配置外部认证之前进行初始设置。

### SCRAM-SHA-256（`auth.scram`） {: #scram-sha-256-authscram }

```yaml
auth:
  provider: basic
  scram: true
```

让 pgwire 通告带 `SCRAM-SHA-256` 的 SASL，使密码以证明方式校验，从不以明文发送。它只适用于 `basic` 提供方，因为没有其他提供方保存 SCRAM 所需的 RFC 5802 验证数据，并且不提供通道绑定。

验证器无法从现有的 bcrypt 哈希派生。每当密码以明文形式经过一次，就会写入一个验证器，因此每个用户的首次 SCRAM 连接紧随其下一次注册、登录、修改密码或管理员重置之后。在此之前，该用户的连接会回退为通过 TLS 的明文交换；线路上不会暴露谁已经完成了迁移。

### 登录限流（`auth.login_throttle`） {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

默认按图示值开启；该配置块仅用于调整这些值。计数器位于凭据校验层，因此通过 HTTP、pgwire 和 Bolt 发生的失败会累计到同一主体上，一次锁定会在每个接口上都生效。它是按进程隔离的：多个 API 工作进程各自都允许最多 `max_attempts` 次尝试。

### 个人访问令牌 {: #personal-access-tokens }

PAT 不需要配置块。它们始终被接受，其存储会与控制平面的其余模式一起创建。可配置的是用户签发时可申请的有效期：1 到 366 天，或者不设期限，即永不过期的令牌。参见[安全模型](security.md#personal-access-tokens)。

### 双向 TLS（Mutual TLS） {: #mutual-tls }

客户端证书校验通过环境变量配置，与它所扩展的 TLS 证书设置放在一起，不写在 `provisa.yaml` 中。

| 变量 | 默认值 | 含义 |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 未设置 | 允许签发客户端证书的 CA 的 PEM 包。设置它会开启客户端证书校验 |
| `PROVISA_MTLS_MODE` | 一旦设置了 CA 则为 `required` | `required` 或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | 要求证书的通用名称（common name）与连接认证所使用的用户名一致 |

每一项都可以按协议覆盖，命名方式与 TLS 设置相同。设置了模式却没有 CA，或模式既不是 `required` 也不是 `optional`，都会阻止启动，所以服务器永远不会在运营方以为已校验、实际未校验的情况下提供连接。

### 通过 TLS 寻址组织 {: #addressing-an-org-over-tls }

无需配置任何内容。在多组织部署中，pgwire 和 Bolt 会从客户端拨号所用的主机名中读取组织信息，该主机名携带在 TLS ClientHello 中，与 HTTP 从 `Host` 请求头中读取的方式完全一致。连接到 `acme.provisa.dev` 的客户端请求的是组织 `acme`；除非已认证的主体是该组织的成员，否则请求会被拒绝。通过 IP 地址连接则不请求任何组织，这在单组织部署中适用于每一条连接。

### 完整认证配置示例（已注释） {: #full-auth-config-example-commented-out }

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

## Upsert 变更（Mutation） {: #upsert-mutations }

对于有主键的表，Provisa 会生成 `upsert_<table>` 变更字段。它们编译为目标方言的 upsert：PostgreSQL 上是 `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`，MySQL 上是 `ON DUPLICATE KEY UPDATE`。

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

冲突列由主键元数据派生。所有列可见性和写权限规则均适用。

## Distinct On

`distinct_on` 参数会为指定列的每个不同取值选取第一行。在根查询字段上可用。

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

在 PostgreSQL 中编译为 `SELECT DISTINCT ON (region) ...`。对于非 PG 方言，使用基于窗口函数的回退方案。

## 写入授权的预设 {: #write-grant-presets }

写入授权在插入和更新时自行设置列。预设在授权的 `presets` 列表中声明，按角色和操作区分。

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    columns:
      - {name: amount, visible_to: [analyst]}
      - {name: created_by, visible_to: [analyst]}
      - {name: created_at, visible_to: [analyst]}
      - {name: source_system, visible_to: [analyst]}
    write_grants:
      - role: analyst
        operation: insert
        columns: [amount]
        presets:
          - {column: created_by, source: session, name: user_id}   # from the request's session
          - {column: created_at, source: now}                      # time of the write
          - {column: source_system, source: literal, value: provisa}
```

| 来源 | 行为 |
| -------- | ---------- |
| `session` | 注入请求中指定会话变量（`name`）的值 |
| `now` | 注入写入发生的时间 |
| `literal` | 注入一个常量（`value`） |

预设在编译期间、生成 SQL 之前添加到语句中。语句不得提供预设列，预设列也不会出现在变更的输入类型中。

## 继承角色 {: #inherited-roles }

角色可以通过 `parent_role_id` 继承自一个父角色。继承链在启动时被展平。子角色拥有其所有祖先角色能力和域访问权限的并集；授予某个祖先角色的列、指标、函数或 webhook 也会授予该子角色；祖先角色的行级安全（RLS）规则会按表逐一应用于子角色，遵循就近优先原则，子角色针对某张表的自有规则会替换父角色的规则。

```yaml
roles:
  - id: admin
    description: Administers everything.
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    description: An intern, with the junior analyst's access.
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

支持多级继承。子角色显式设置的能力和 domain_access 会与父角色的合并。父角色必须是已存在的角色，不能是自身，也不能形成循环；任何违反都会在保存时被拒绝。

## 计划触发器 {: #scheduled-triggers }

按 cron 计划调用 webhook URL 的触发器。使用 APScheduler。

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

计划任务通过管理界面（启用/禁用开关）或 `toggle_scheduled_task` 管理变更（Mutation）进行管理。

触发器可以在已注册的表上触发，而不是按计划：`on: {table, event}` 以 `source_id.schema.table` 形式指定表，并指定事件 `change`、`error` 或 `quarantine`。`command` 触发器以其 `role` 运行一个已注册的命令，经由该命令自身的受治理调度，并把触发它的内容作为参数传入。变更传入 `table`，以及在变更信号携带时（Kafka、Debezium）作为 `changes` 的已落地批次。`error` 或 `quarantine` 事件传入 `table`、`event`、`reason` 和 `error`。若触发器的命令声明了触发器不提供的参数，保存时会被拒绝。变更在其变更信号报告的每一批变更时触发一次，或在经由 Provisa 写入完成时触发；由副本提供的表在构建落地了与上次不同的行时触发。命令自身的写入也是一次变更，因此触发器可以链式触发。每次运行都会记录时间、结果（`succeeded`、`refused` 或 `failed`）、错误以及触发来源，`scheduledTasks` 管理查询会列出它。只有 prod 的触发器会触发，且不会在任何源数据库中安装任何东西。

```yaml
scheduled_triggers:
  - id: bronze_to_silver
    on: {table: bronze_pg.public.bronze, event: change}
    command: promote_to_silver
    role: loader
  - id: alert_on_quarantine
    on: {table: bronze_pg.public.bronze, event: quarantine}
    command: notify_steward      # declares arguments table, reason
    role: loader
```

## OrderBy 格式 {: #orderby-format }

OrderBy 使用 `{column: direction}` 格式，direction 是一个含 6 个取值的枚举：

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

关系排序通过嵌套对象支持：

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## 可观测性 {: #observability }

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

### 遥测过滤器 {: #telemetry-filters }

Provisa 有两条相互独立的 OTLP 导出路径：您的内部 collector，以及可选的 Provisa 支持端点。每条路径都有自己的过滤器。过滤器在 span 离开进程之前运行，原始的 span 对象永远不会被修改。

**`telemetry_filter`** —— 控制哪些数据到达你的内部采集器。

| 键 | 类型 | 默认值 | 说明 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | 将 `db.statement` 中的字符串和数字字面值替换为 `?` |
| `redact_attributes` | list[str] | `[]` | 从每个跨度中完全删除的属性键 |

**`support_telemetry_filter`** —— 控制哪些数据到达 Provisa 支持端点。在此路径上，SQL 字面值涂黑默认开启为 `true`，因为查询数据归你所有。

| 键 | 类型 | 默认值 | 说明 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | 将 `db.statement` 中的字符串和数字字面值替换为 `?` |
| `redact_attributes` | list[str] | `[]` | 从每个跨度中完全删除的属性键 |

涂黑后的 `db.statement` 示例——当 `redact_sql_literals: true` 时，这个跨度属性：

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

会变为：

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### 支持端点 {: #support-endpoint }

`support_endpoint`（或环境变量 `PROVISA_SUPPORT_OTLP_ENDPOINT`）会把遥测转发给 Provisa 支持团队用于诊断。未设置时，任何数据都不会经由这条路径离开您的基础设施。支持过滤器独立于内部过滤器工作，所以您可以在两路导出中都隐去 SQL 字面量，同时仍与支持团队共享 span 耗时和错误数据。

### 端点协议探测 {: #endpoint-protocol-detection }

Provisa 会根据端点 URL 的 scheme 选择 OTLP/HTTP 或 OTLP/gRPC。以 `http://` 或 `https://` 开头的 URL 使用 OTLP/HTTP，并自动附加 `/v1/traces`、`/v1/metrics`、`/v1/logs`。其他任何 scheme 都使用 `insecure=True` 的 OTLP/gRPC。

## 联邦引擎 {: #federation-engine }

配置联邦引擎是可选的。默认是 `duckdb`：零配置、进程内运行、无需外部服务。需要 MPP 规模，或想复用已有数仓时，再选择其他引擎。

优先级：`PROVISA_ENGINE` 环境变量 → 持久化的管理界面 `federation_engine` 配置字段 → `duckdb`。更改在服务重启后生效。

### 引擎概览 {: #engine-overview }

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

### 引擎参考 {: #engine-reference }

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

进程内运行；无需外部服务。默认引擎。`PROVISA_DATA_DIR` 控制嵌入式存储的位置（默认为 `~/.provisa`）。

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

物化存储默认为 `~/.provisa/materialize.duckdb`。它是唯一默认存储不是 PostgreSQL 的引擎。

#### clickhouse（嵌入式）/ clickhouse-server {: #clickhouse-embedded-clickhouse-server }

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

#### 关系型数据库引擎（mysql、mariadb、oracle、mssql、db2、redshift、greenplum、cockroachdb、yugabytedb、opengauss、tidb、singlestore、vertica、exasol、teradata、saphana、sapase、sqlanywhere、monetdb、firebird）以及 `sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

每种可通过网络访问的关系型数据库对应一个键，全部运行在同一个仅落地（land-only）的运行时上（不联邦到外部数据源）：每个数据源都落地到存储中并在那里被查询。该键选择数据库；`PROVISA_ENGINE_URL` 携带其方言所需的 DSN。`sqlalchemy` 是没有专属键的数据库的通用回退项。不提供文件内嵌式存储（SQLite、Access）——服务器必须可通过网络访问。

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

物化存储默认使用 `TENANT_DATABASE_URL`。

### 物化存储 {: #materialization-store }

当数据源无法实时挂载（所选引擎没有对应的 ATTACH 连接器）时，它会落地到该引擎的物化存储中。解析顺序：显式的 `PROVISA_MATERIALIZE_URL` → 引擎声明的默认值 → 硬性报错（无静默回退）。

DuckDB 将其嵌入式文件（`~/.provisa/materialize.duckdb`）声明为默认值。所有其他引擎默认使用 `TENANT_DATABASE_URL`（PostgreSQL）。可以用 `PROVISA_MATERIALIZE_URL` 覆盖任意引擎的默认值。

存储决定副本的索引对联接起什么作用：PostgreSQL 存储会为副本建立索引，在其中运行 SQL 的引擎会读取这些索引；而 DuckDB 则在每条语句的副本拷贝之上联接其内嵌文件中的副本。参见 [Replicas](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it)。

### 按数据源的联邦提示 {: #per-source-federation-hints }

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

对于 Google Cloud 数据源，将 `GOOGLE_APPLICATION_CREDENTIALS` 设置为服务账号密钥文件的路径。对于 Fabric 和 Synapse，用 `az login`（开发）或托管标识（生产）登录；引擎通过 `azure-identity` 的 `DefaultAzureCredential` 读取凭据。

## 环境变量 {: #environment-variables }

| 变量 | 默认值 | 说明 |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 配置文件路径 |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | 控制平面存储 URI（SQLAlchemy 异步）；对于嵌入式桌面存储，可接受 `sqlite+pysqlite://…` / `duckdb://…` |
| `PLATFORM_DATABASE_URL` | — | 平台注册表 URI（租户目录、引擎注册表）；启动时必需，无回退 |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` 使用嵌入式 fakeredis 而非 Redis 服务器——无需 Docker |
| `PG_HOST` | `localhost` | PostgreSQL 主机 |
| `PG_PORT` | `5432` | PostgreSQL 端口 |
| `PG_DATABASE` | `provisa` | PostgreSQL 数据库 |
| `PG_USER` | `provisa` | PostgreSQL 用户 |
| `PG_PASSWORD` | `provisa` | PostgreSQL 密码 |
| `PROVISA_ENGINE` | `duckdb` | 联邦引擎键 |
| `PROVISA_ENGINE_URL` | — | 由 URL 驱动的引擎的连接 URL（Snowflake、Databricks、ClickHouse Server、BigQuery、SQLAlchemy） |
| `PROVISA_MATERIALIZE_URL` | — | 覆盖物化存储的 DSN（默认为引擎声明的默认值） |
| `PROVISA_DATA_DIR` | `~/.provisa` | 嵌入式 DuckDB 存储的数据目录 |
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
| `PROVISA_RETRY_BUDGET_SECS` | `30` | 一级读重试预算（秒）；采用全抖动（full jitter）的指数退避 |
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
| `PROVISA_MTLS_CLIENT_CA` | — | 允许签发客户端证书的 CA 的 PEM 包；设置它会在 pgwire、Bolt、gRPC 和 Flight 上开启客户端证书校验 |
| `PROVISA_MTLS_MODE` | 一旦设置了 CA 则为 `required` | `required` 或 `optional`；任何其他值都会拒绝启动 |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | 要求证书的通用名称与认证用户名一致 |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | 允许从浏览器打开 Bolt WebSocket 的站点，逗号分隔；未设置时拒绝所有浏览器来源 |
| `PROVISA_EXTRAS` | `firebase,vector` | 内置到应用镜像中的 pyproject extras；`scripts/provisa` 根据 `~/.provisa/config.yaml` 中的 `dq_checker` 推导 |
| `PROVISA_DQ_CHECKER` | `none` | 仅供安装程序使用：`none`/`soda`/`gx`，在非交互模式下由 `first-launch.sh` 读取，并作为 `dq_checker` 写入 `config.yaml` |
| `ANTHROPIC_API_KEY` | — | Claude API 密钥（发现用途） |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | 覆盖 `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | 覆盖 `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | 覆盖 `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | 覆盖 `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | 批处理跨度处理器的刷新延迟 |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | 覆盖 `observability.support_endpoint` |
