# 設定參考

Provisa 透過一個 YAML 檔案進行設定（預設：`config/provisa.yaml`）。(REQ-528)

## 數據來源

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

所有數據來源均共用一組共通欄位。[tool-verified: `provisa/core/models.py:129-212`]

| 欄位 | 預設值 | 備註 |
| ------- | --------- | ------- |
| `id` | 必填 | 英數字元、連字號、底線 |
| `type` | 必填 | 見下表 |
| `host` | `""` | 主機名稱或 IP |
| `port` | `0` | `0` 表示各連接器自行提供其預設值——並無中央預設連接埠對照表 |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | 支援 `${env:VAR}` 密鑰解析 |
| `path` | `null` | 檔案型數據來源的檔案路徑或 URI |
| `base_url` | `null` | API 數據來源的基礎 URL |
| `pool_min` / `pool_max` | `1` / `5` | 連線池上下限 |
| `cache_enabled` | `true` | 切換此數據來源中所有資料表的快取功能 |
| `cache_ttl` | `null` | 秒數；`null` 表示繼承全域預設值 |
| `federation_hints` | `{}` | 各連接器的擴充參數（dict[str,str]）；見下方型別參考。REQ-281 |
| `mapping` | `{}` | 供 redis、elasticsearch、prometheus 使用的對應 DSL。REQ-251 |
| `allowed_domains` | `[]` | 將此數據來源限制於特定領域 ID；留空即不限制 |
| `description` | `""` | |

### 支援的數據來源型別 [tool-verified: `provisa/core/models.py:36-101`]

| 型別 | 連線方式 | 備註 |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Asyncpg 連線池；透過 `use_pgbouncer` 選用啟用 PgBouncer |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path`（資料庫檔案） | DuckDB firebird 社群擴充功能（REQ-899） |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | 重用 PostgreSQL 驅動程式/方言（REQ-950） |
| `yugabytedb` | host/port | 重用 PostgreSQL 驅動程式/方言（REQ-950） |
| `greenplum` | host/port | 重用 PostgreSQL 驅動程式/方言（REQ-950） |
| `tidb` | host/port | 重用 MySQL 驅動程式/方言（REQ-950） |
| **雲端數據倉庫** | | |
| `snowflake` | host/port + `federation_hints` | hints 中必須提供 `account` |
| `bigquery` | `federation_hints` | 必須提供 `project`；透過 `GOOGLE_APPLICATION_CREDENTIALS` 驗證 |
| `databricks` | host + `federation_hints` | hints 中必須提供 `http_path` |
| `fabric` | 環境變數或 `PROVISA_ENGINE_URL` | 透過 TDS 使用 T-SQL，Azure AD 驗證 |
| `synapse` | 環境變數或 `PROVISA_ENGINE_URL` | 透過 TDS 使用 T-SQL，Azure AD 驗證 |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | `secure` hint 切換 TLS；連接埠預設為 8123/8443 |
| `elasticsearch` | host/port + `mapping` DSL | |
| `pinot` | host/port | Controller REST 端點 |
| `druid` | host/port | Broker Avatica 端點 |
| `exasol` | host/port | |
| **數據湖** | | |
| `delta_lake` | `path`（資料表 URI） | DuckDB `delta_scan`；物件儲存存取透過 `federation_hints` |
| `iceberg` | `path`（資料表 URI） | DuckDB `iceberg_scan`；物件儲存存取透過 `federation_hints` |
| `hudi` | `path`（資料表 URI） | ClickHouse Hudi 引擎，零複製（REQ-1178） |
| `hive` | host/port（metastore） + `mapping.storage` | 儲存後端於 `mapping["storage"]` 中設定：hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port（metastore） + `mapping` S3 金鑰 | 獨立型別；恆為 S3 儲存（REQ-229） |
| **NoSQL** | | |
| `mongodb` | host/port | 純連線欄位；無對應 DSL |
| `cassandra` | host/port | 純連線欄位；無對應 DSL |
| `redis` | host/port + `mapping` DSL | |
| **串流** | | |
| `kafka` | 僅供註冊 | 實際設定位於 `kafka_sources[]`；見下方 §Kafka |
| `websocket` | host/port/path + `federation_hints` | 外部 WebSocket 訂閱源 |
| `rss` | host/port/path + `federation_hints` | RSS 2.0 / Atom 訂閱源 |
| **圖形/語意** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **檔案** | | |
| `sqlite` | `path` | 恆經引擎路由（無直接連線池） |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path`（目錄） | Glob 爬取工具；將 CSV/Parquet/XLSX/JSON 呈現為資料表 |
| **API/遠端** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port 或 `mapping.url` + `mapping` DSL | |
| `graphql_remote` | `base_url` + 選用 `mapping` | 標頭、forward-client-headers、逾時設定於 `mapping` 中 |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url`（Flight 位置） | DuckDB airport 擴充功能（REQ-899） |
| `ingest` | 推送接收端 | 外部服務以 POST 方式傳送 JSON 事件 |
| **SaaS** | | |
| `sharepoint` | `base_url` 或 `host` + `mapping` | 驗證方式透過 `mapping.auth_type` |
| `splunk` | `host`/`port` 或 `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | 獨立的 `GovDataSource` 模型；見下方 §GovData |

### 數據來源型別參考

需要非顯而易見設定的型別，均於下方各有簡短說明。RDBMS 型別（postgresql、mysql 等）僅使用上述共通欄位——無須額外章節。

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

`govdata` 數據來源使用獨立的頂層模型 `GovDataSource`，而非通用的 `Source`。(REQ-540) 存取權以主題分組劃分。

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

每個主題均對應一或多個 GovData 結構描述。設定帶有某主題的 `govdata` 數據來源，會自動公開該主題的所有結構描述。(REQ-540)

| 主題 | 結構描述 |
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

`ref` 及 `geo` 結構描述恆作為連結器結構描述包含在內——不可設定，亦未列於上表。(REQ-541) 使用主題 `ALL` 即可授予對所有結構描述的存取權。[tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

`sources:` 中的 `kafka` 列僅供註冊之用。其連接器的 `details()` 回傳 `{}`——實際設定位於頂層的 `kafka_sources[]` 區塊，而非 `sources:` 列中。Kafka 恆為 VIRTUAL_SOURCE（經引擎路由；無直接連線池）。[tool-verified: `provisa/transpiler/router.py:44-63`]

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

**時間視窗**——`default_window` 將每次查詢限制於一段近期時間範圍內，防止對高流量主題進行無限制讀取。(REQ-148) 格式：`1h`、`30m`、`7d`、`60s`。預設為 `1h`。自動注入為 `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`。客戶端可在 GraphQL `where` 引數中以自訂的 `_timestamp` 篩選條件覆寫此設定。

**判別欄位**——多個主題設定可指向同一個實體 Kafka 主題，並以不同的 `discriminator` 值產生各自獨立的 GraphQL 型別。(REQ-149) 判別欄位會自動注入為 WHERE 子句。

**結構描述來源**

| 值 | 行為 |
| ------- | ---------- |
| `registry` | 由 Confluent Schema Registry 擷取結構描述 |
| `manual` | 於設定中內嵌定義欄位（無須 Schema Registry） |
| `sample` | 由樣本訊息自動探索 |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`federation_hints` 中必須提供 `account`。`warehouse`、`role` 及 `schema` 為選用。

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

`federation_hints` 中必須提供 `http_path`。`password` 承載個人存取權杖。`catalog` 為選用（於 SQL/hints 中攜帶，非 `database` 欄位）。

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

`federation_hints` 中必須提供 `project`。驗證方式使用 `GOOGLE_APPLICATION_CREDENTIALS`（服務帳戶金鑰檔案路徑）或引擎環境中的應用程式預設憑證。

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

兩者均透過 TDS 使用 T-SQL，並以 Azure AD 進行驗證。以 `az login`（開發用）或受管理身分（生產環境用）進行驗證——引擎透過 `azure-identity` 的 `DefaultAzureCredential` 讀取憑證。連線詳情來自環境變數：`FABRIC_SQL_SERVER` / `FABRIC_DATABASE`（Fabric）或 `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE`（Synapse），或透過 `PROVISA_ENGINE_URL`。

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`federation_hints` 中的 `secure` 於 HTTP 介面上啟用 TLS。連接埠預設為 `8123`（明文）或 `8443`（當 `secure: "true"` 時）。`federation_hints` 中的 `schema` 會覆寫遠端結構描述。[tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`path` 為資料表 URI（S3、GCS、ADLS 或本機）。物件儲存存取需要 `federation_hints` 憑證。若為 Cloudflare R2，須加入 `account_id`。

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

`host` 及 `port` 指向 Hive Thrift metastore（預設連接埠 9083）。對於 `hive`，設定 `mapping["storage"]` 以選擇物件儲存後端。缺少必要金鑰會直接失敗——無備援機制。[tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` 為獨立型別，恆宣告 S3 儲存（REQ-229）；無須 `mapping.storage`。

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

`mapping.storage` 可接受的值：`hadoop`（預設）、`hdfs`、`local`、`s3`、`azure`、`adls`。S3 對應金鑰：`endpoint`、`access_key_id`、`secret_access_key`、`region`、`path_style`。ADLS 對應金鑰：`storage_account`、`access_key` 或 `sas_token`。

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

使用 `mapping` DSL。`mongodb` 及 `cassandra` 使用純連線欄位，並**不**使用對應 DSL。

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

當 `host:port` 與 `mapping.url` 同時存在時，`mapping.url` 具優先權。

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

`federation_hints` 中必須提供 `spreadsheet_id`。驗證方式使用於連接 (attach) 時佈建的 DuckDB `gsheet` SECRET。

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### 檔案數據來源 (csv / parquet / sqlite / files)

`path` 為必填。`files` 會爬取目錄以尋找 CSV、Parquet、XLSX 及 JSON 檔案，並將各檔案呈現為資料表。所有檔案型數據來源均為 VIRTUAL（經引擎路由；無直接連線池）。[tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### API / 遠端數據來源

**openapi**——將 `base_url` 設為 OpenAPI 基礎 URL。結構描述探索會於啟動時讀取 OpenAPI 規格。

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote**——設定 `base_url`。選用的 `mapping` 金鑰：`headers`（靜態標頭字典）、`forward_client_headers`（布林值）、`timeout_seconds`（整數）。[tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

**airport**——`base_url` 為 Arrow Flight 伺服器位置。DuckDB airport 擴充功能（REQ-899）。[tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss**——使用 `host`、`port`、`path` 及 `federation_hints`。[tool-verified: `provisa/api/data/subscribe.py:85-129`]

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

## 領域

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

### 命名慣例

命名機構是面向客戶端名稱的唯一真確來源；物理後端欄位名稱永不對客戶端公開。(REQ-194) 每種查詢語言會依 `column.alias`（若已設定）衍生欄位名稱，否則透過其所設定的慣例，由物理欄位名稱衍生而來。(REQ-194)

GraphQL 慣例為三種預設列舉值之一。(REQ-416) 舊有的自由格式字串（`none`、`snake_case`、`camelCase`、`PascalCase`）已棄用。(REQ-416)

| 預設值 | 是否為預設 | 型別名稱 | 欄位名稱 | 變異名稱 |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | 是 | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

預設 GraphQL 慣例為 `apollo_graphql`，會產生 camelCase 的欄位及變異名稱。(REQ-194、REQ-416) SQL 慣例是獨立的，預設為 `snake_case`，透過 `apply_sql_name()` 套用；GraphQL 慣例則透過 `apply_gql_name()` 套用，而 CQL 名稱則由 GraphQL 名稱衍生而來。(REQ-194)

`domain_prefix: bool` 是一項與所選預設值無關的獨立選項，無論選用何種預設值均會套用。(REQ-416)

明確設定的 `column.alias` 即為標準名稱：SQL 會逐字使用，不套用任何慣例；GraphQL 會對其套用慣例；而 CQL 則由 GraphQL 名稱衍生而來。(REQ-194)

依數據來源覆寫：

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

依資料表覆寫：

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### 領域前綴

當 `domain_prefix: true` 時，所有 GraphQL 欄位及型別名稱均會以雙底線作為分隔符，加上領域 ID 前綴：(REQ-154)

| 資料表 | 領域 | 欄位名稱 |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

此機制可防止不同領域中同名資料表之間的名稱衝突，並令查詢具備自我說明性。

### 命名規則

於產生 GraphQL 欄位名稱時，套用於資料表名稱的正規表示式規則。於唯一性解析之前依序套用。(REQ-542)

## 資料表

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

### 別名

資料表及欄位別名會覆寫預設的 GraphQL 名稱。(REQ-155) 適用於：

- 重新命名意義不明的資料庫名稱（例如：`tbl_cust_seg` → `customer_segments`）
- 於 API 層避免使用縮寫
- 建立一套簡潔、專屬於該領域的詞彙

### 描述

資料表及欄位描述會納入所產生的 GraphQL SDL 中。(REQ-156) 會出現於 GraphiQL 的文件探索工具及內省查詢中。可於設定 YAML 或透過管理介面設定。

### 路徑（計算所得的 JSON 擷取）

欄位可以點記法 `path` 由 JSON/JSONB 來源欄位擷取值。(REQ-151) 這對於 Kafka 訊息、MongoDB 文件或 PostgreSQL JSONB 欄位中的半結構化數據十分有用。

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

路徑格式為 `source_column.key1.key2...`。編譯器會在 SQL 中產生 `json_extract_scalar(source_column, '$.key1.key2')`。(REQ-151)

**對路由的影響：**路徑欄位使用 PostgreSQL JSON 運算子（`->>`），直接的 PG 路由原生支援此運算子。(REQ-152) 對於非 PostgreSQL 數據來源（MySQL、SQL Server 等），帶有路徑欄位的查詢會自動經聯邦引擎路由。(REQ-152) 由於路徑欄位為唯讀的計算欄位，變異不受影響。(REQ-153)

### 遮罩型別

| 型別 | 欄位 | 描述 |
| ------ | -------- | ------------- |
| `regex` | `pattern`、`replace` | REGEXP_REPLACE（僅限字串欄位） |
| `constant` | `value` | 常值取代（NULL、0、MAX、MIN、自訂值） |
| `truncate` | `precision` | DATE_TRUNC（僅限日期/時間戳記欄位） |

## 關係

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

### 自動具體化

於一項關係上設定 `materialize: true`，即可自動為跨數據來源的 JOIN 產生具體化檢視。(REQ-158) 此舉可透過預先計算 JOIN 結果，避免昂貴的聯邦查詢。

- 僅跨數據來源的關係會產生具體化檢視（同數據來源的 JOIN 本已快速）(REQ-159)
- 該具體化檢視初始為過期狀態，由背景重新整理迴圈填入數據 (REQ-160)
- 對任一來源資料表的變異，均會將該具體化檢視標記為過期，須重新整理 (REQ-543)
- `refresh_interval` 預設為 300 秒（5 分鐘） (REQ-543)

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

帶有 `parent_role_id` 的角色，會由母角色繼承其功能及領域存取權。(REQ-215) 此階層架構於啟動時會被攤平。(REQ-215)

### 功能

| 功能 | 描述 |
| ----------- | ------------- |
| `source_registration` | 註冊數據來源 |
| `table_registration` | 註冊資料表 |
| `relationship_registration` | 定義關係 |
| `security_config` | 設定行級安全、遮罩 |
| `query_development` | 執行查詢 |
| `full_results` | 略過取樣限制 |
| `admin` | 所有功能 |

## 行級安全規則

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## 具體化檢視

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

## 檢視（受治理的計算數據集）

檢視是以 SQL 定義、具備完整欄位級治理的計算數據集。(REQ-133) 它們是為語意層加入彙總、轉換及衍生指標的受治理機制。(REQ-136)

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

| 欄位 | 必填 | 描述 |
| ------- | ---------- | ------------- |
| `id` | 是 | 唯一的檢視識別碼 |
| `sql` | 是 | 定義該檢視的 SQL SELECT 陳述式 |
| `domain_id` | 是 | 供結構描述可見性使用的領域 |
| `materialize` | 否 | `true` = 定期 CTAS 重新整理，`false` = 即時聯邦檢視 |
| `refresh_interval` | 否 | 重新整理之間的秒數（僅適用於具體化檢視，預設 300） |
| `description` | 否 | 出現於 GraphQL SDL 中 |
| `alias` | 否 | 覆寫 GraphQL 名稱 |
| `columns` | 是 | 帶有可見性、遮罩、描述的欄位定義 |

### 具體化與即時之別

- **`materialize: true`**：Provisa 會透過 CTAS 建立資料表，並依排程重新整理。(REQ-135) 查詢較快，但數據可能過期最多達 `refresh_interval` 秒。
- **`materialize: false`**：Provisa 會建立聯邦檢視。(REQ-135) 查詢恆傳回即時數據，但對於複雜的彙總可能較慢。

檢視經由與資料表相同的治理管線——行級安全、遮罩、取樣及依角色的可見性。(REQ-134) 此舉確保平台上不會有新語意能於未經數據管家審核下加入。(REQ-136)

### 僅供查詢的檢視

`materialize: true` 及 `materialize: false` 的檢視，其 GraphQL 型別均公開為僅供查詢。以 `view_sql` 為基礎的關聯不會產生任何插入、upsert、更新或刪除變異。(REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## 快取

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### 快取階層

TTL 解析順序（最具體者優先）：**資料表** > **數據來源** > **全域預設值**。(REQ-544) 採用第一個非空值。

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

於數據來源上設定 `cache_enabled: false`，會停用該數據來源中所有資料表的快取，無論資料表層級的 TTL 為何。(REQ-544) 快取鍵恆包含 `role_id` 及行級安全情境值，以進行安全分割。(REQ-544)

## 驗證

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

### 驗證提供者型別

| 提供者 | 使用情境 | 權杖驗證方式 |
| ---------- | ---------- | ----------------- |
| `simple` | 本機開發/測試。使用者於 YAML 中定義。 | 以 `PROVISA_JWT_SECRET` 簽署的 JWT |
| `firebase` | Firebase Authentication（所有方式）。 | `firebase-admin` SDK 的 `verify_id_token()` |
| `keycloak` | Keycloak OIDC。租用戶及客戶端角色皆有對應。 | 以 JWKS 為基礎的 JWT 驗證 |
| `oauth` | 通用 OIDC（Okta、Azure AD、Auth0、PingFederate）。 | 來自 discovery URL 的 JWKS |

超級使用者憑證（`superuser` 區塊）適用於任何提供者，並恆解析為擁有所有功能的 admin 角色。(REQ-125) 用於設定外部驗證前的初始設置。

### 完整驗證設定範例（已註解）

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

## Upsert 變異

對於帶有主索引鍵的資料表，Provisa 會自動產生 `upsert_<table>` 變異欄位。(REQ-212) 這些會編譯為目標方言中的 upsert——PostgreSQL 上為 `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`，MySQL 上為 `ON DUPLICATE KEY UPDATE`。(REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

衝突欄位由主索引鍵中介資料衍生而來。(REQ-212) 所有欄位可見性及寫入權限規則均適用。

## Distinct On

`distinct_on` 引數會為指定欄位的每個不重複值選出第一列。(REQ-213) 於根查詢欄位上可用。

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

於 PostgreSQL 中編譯為 `SELECT DISTINCT ON (region) ...`。(REQ-213) 對於非 PG 方言，會使用視窗函式作為備援機制。(REQ-213)

## 欄位預設值

於新增/更新時自動注入欄位值。(REQ-214) 依資料表於設定中定義。

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

| 來源 | 行為 |
| -------- | ---------- |
| `header` | 由指定名稱的 HTTP 請求標頭注入值 |
| `now` | 注入 `NOW()`（目前時間戳記） |
| `literal` | 注入一個常值 |

預設欄位值於變異編譯階段、SQL 產生之前注入。(REQ-214) 它們不會出現在變異輸入型別中。(REQ-214)

## 繼承角色

角色可透過 `parent_role_id` 由母角色繼承功能及領域存取權。(REQ-215) 此階層架構於啟動時會被攤平。(REQ-215)

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

支援多層繼承。(REQ-215) 子角色明確設定的功能及 domain_access，會與母角色的設定合併。(REQ-215)

## 排程觸發器

依排程呼叫 webhook URL 的 Cron 型觸發器。(REQ-216) 使用 APScheduler。(REQ-216)

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

排程任務可透過管理介面（啟用/停用切換開關）或 `toggle_scheduled_task` 管理變異進行管理。(REQ-216)

## OrderBy 格式

OrderBy 採用 `{column: direction}` 格式，具備 6 種方向列舉值：(REQ-200、REQ-201)

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

透過巢狀物件支援關係排序：(REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## 可觀測性

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

### 遙測篩選器 [tool-verified]

Provisa 執行兩條獨立的 OTLP 匯出路徑：您的內部收集器，以及選用的 Provisa 支援端點。(REQ-545) 各路徑均有其自身的篩選器。篩選器於 span 離開行程之前，於包覆式的 `_FilteringExporter` 內執行——原始 span 物件永不被修改。(REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`**——控制傳送至您內部收集器的內容。

| 金鑰 | 型別 | 預設值 | 描述 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | 將 `db.statement` 中的字串及數值常值取代為 `?` |
| `redact_attributes` | list[str] | `[]` | 於每個 span 中完全捨棄的屬性金鑰 |

**`support_telemetry_filter`**——控制傳送至 Provisa 支援端點的內容。此路徑上的 SQL 常值遮蔽預設為 `true`，因為查詢數據屬於您所有。(REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| 金鑰 | 型別 | 預設值 | 描述 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | 將 `db.statement` 中的字串及數值常值取代為 `?` |
| `redact_attributes` | list[str] | `[]` | 於每個 span 中完全捨棄的屬性金鑰 |

已遮蔽的 `db.statement` 範例——在 `redact_sql_literals: true` 之下，此 span 屬性：

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

會變為：

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### 支援端點 [tool-verified]

`support_endpoint`（或環境變數 `PROVISA_SUPPORT_OTLP_ENDPOINT`）會將遙測數據轉發至 Provisa 支援團隊以供診斷之用。(REQ-548) 未設定時，此路徑不會有任何數據離開您的基礎設施。(REQ-548) 支援篩選器獨立於內部篩選器運作——您可在兩條匯出路徑中同時遮蔽 SQL 常值，同時仍與支援團隊分享 span 時序及錯誤數據。(REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### 端點通訊協定偵測 [tool-verified]

Provisa 會根據端點 URL 的通訊協定綱要，選擇 OTLP/HTTP 或 OTLP/gRPC。(REQ-549) 以 `http://` 或 `https://` 開頭的 URL 使用 OTLP/HTTP，並自動附加 `/v1/traces`、`/v1/metrics` 及 `/v1/logs`。(REQ-549) 其他任何綱要則使用 OTLP/gRPC，並帶 `insecure=True`。(REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## 聯邦引擎

設定聯邦引擎為選用項目。預設為 `duckdb`——零設定、行程內執行、無須外部服務（REQ-989）。當您需要 MPP 規模，或想重用既有數據倉庫時，可選用其他引擎。

優先順序：`PROVISA_ENGINE` 環境變數 → 已保存的管理介面 `federation_engine` 設定欄位 → `duckdb`。變更於服務重新啟動後生效。[tool-verified: `engine.py` `build_engine`]

### 引擎概覽 [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| 引擎鍵值 | 標籤 | 方言 | MPP | 外部連結機制 | 驗證方式 |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | 是 | Trino catalog（連接器涵蓋範圍廣泛） | JDBC 憑證 |
| `trino-byo` | Trino（自備） | Trino SQL | 是 | 與 `trino` 相同；非受管理協調器 | JDBC 憑證 |
| `pg` | PostgreSQL | PostgreSQL | 否 | FDW / pg_duckdb | PostgreSQL 憑證 |
| `duckdb` | DuckDB | DuckDB | 否 | 擴充功能原生 ATTACH | 無（行程內執行） |
| `clickhouse` | ClickHouse（內嵌） | ClickHouse | 是 | S3 / IcebergS3 / DeltaLake 資料表引擎 | chdb（行程內執行，無須驗證） |
| `clickhouse-server` | ClickHouse（Server / Cloud） | ClickHouse | 是 | S3 / IcebergS3 / DeltaLake 資料表引擎 | ClickHouse 憑證 |
| `snowflake` | Snowflake | Snowflake | 是 | 外部 stage + 外部資料表 | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | 是 | 透過 REST 的 Unity Catalog 外部資料表 | `PROVISA_ENGINE_URL`（Bearer 權杖 + `http_path`） |
| `bigquery` | BigQuery | BigQuery | 是 | BigQuery 外部 / BigLake 資料表 | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | 是 | OneLake 捷徑 → OPENROWSET | Azure AD（`az login` 或受管理身分） |
| `synapse` | Azure Synapse | T-SQL | 是 | ADLS OPENROWSET / 外部資料表 | Azure AD |
| `sqlalchemy` | SQLAlchemy（任何 RDB） | 依方言而定 | 否 | 無（僅供落地） | 依方言憑證 |

### 引擎參考

#### trino / trino-byo

`trino` 為受管理的 Provisa 協調器；`trino-byo` 連接至您自有的 Trino 叢集。兩者均使用 Trino SQL，且具備最廣泛的數據來源觸及範圍。

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

具體化儲存區預設為 `TENANT_DATABASE_URL`（PostgreSQL）。

#### pg

透過 postgres_fdw（SQL/MED）及 pg_duckdb 擴充功能進行聯邦。單一節點；無 MPP。最適合您的數據已存放於 PostgreSQL、且您想連接少數幾個遠端數據來源的情境。

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### duckdb

行程內執行；無外部服務。預設引擎（REQ-989）。`PROVISA_DATA_DIR` 控制內嵌儲存區的存放位置（預設為 `~/.provisa`）。

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

具體化儲存區預設為 `~/.provisa/materialize.duckdb`——是唯一預設儲存區並非 PostgreSQL 的引擎。

#### clickhouse（內嵌） / clickhouse-server

`clickhouse` 使用 chdb（行程內執行）。`clickhouse-server` 連接至外部 ClickHouse 執行個體或 ClickHouse Cloud。兩者均透過原生 ClickHouse 資料表引擎直接讀取 Delta Lake、Iceberg 及 Hudi。

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### snowflake

引擎即數據倉庫：Snowflake 執行查詢；Provisa 透過外部 stage 推送來源數據。

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### databricks

Unity Catalog 外部資料表將 Provisa 管理的數據來源橋接至 Databricks SQL。

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### bigquery

BigQuery 外部及 BigLake 資料表。專案來自 URL 或 `GOOGLE_CLOUD_PROJECT`；透過服務帳戶金鑰驗證。

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### fabric / synapse

兩者均透過 TDS 使用 T-SQL，並以 Azure AD 驗證（`az login` 或受管理身分）。省略 `PROVISA_ENGINE_URL` 即改為由環境變數讀取連線詳情。

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### sqlalchemy

通用的僅供落地 RDBMS 引擎（不聯邦至外部數據來源）。適用於單一數據倉庫部署或測試。

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

### 具體化儲存區

當某數據來源無法即時連接（所選引擎無 ATTACH 連接器）時，其數據會落地至該引擎的具體化儲存區。解析順序：明確設定的 `PROVISA_MATERIALIZE_URL` → 引擎宣告的預設值 → 直接失敗（無沉默備援）。[tool-verified: `engine.py` `materialize_store`]

DuckDB 將其內嵌檔案（`~/.provisa/materialize.duckdb`）宣告為預設值。所有其他引擎預設為 `TENANT_DATABASE_URL`（PostgreSQL）。可透過 `PROVISA_MATERIALIZE_URL` 覆寫任何引擎的設定。

### 依數據來源的聯邦提示

標準 host/port/user/password 欄位無法承載的擴充連線參數，均置於該數據來源的 `federation_hints` 中。各型別的提示金鑰請見上方數據來源型別參考。以下是一個綜合範例：

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

對於 Google Cloud 數據來源，請將 `GOOGLE_APPLICATION_CREDENTIALS` 設為您服務帳戶金鑰檔案的路徑。對於 Fabric 及 Synapse，請以 `az login`（開發用）或受管理身分（生產環境用）驗證——引擎透過 `azure-identity` 的 `DefaultAzureCredential` 讀取憑證。

## 環境變數

| 變數 | 預設值 | 描述 |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 設定檔路徑 |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | 控制平面儲存區 URI（SQLAlchemy 非同步）；接受 `sqlite+aiosqlite://…` / `duckdb://…`，供內嵌桌面儲存區使用（REQ-828、REQ-850） |
| `PLATFORM_DATABASE_URL` | — | 平台註冊表 URI（租用戶目錄、引擎註冊表）；啟動時必須提供，無備援機制（REQ-837） |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` 使用內嵌 fakeredis 而非 Redis 伺服器——無須 Docker（REQ-829） |
| `PG_HOST` | `localhost` | PostgreSQL 主機 |
| `PG_PORT` | `5432` | PostgreSQL 連接埠 |
| `PG_DATABASE` | `provisa` | PostgreSQL 資料庫 |
| `PG_USER` | `provisa` | PostgreSQL 使用者 |
| `PG_PASSWORD` | `provisa` | PostgreSQL 密碼 |
| `PROVISA_ENGINE` | `duckdb` | 聯邦引擎鍵值（REQ-989） |
| `PROVISA_ENGINE_URL` | — | 供以 URL 驅動的引擎使用的連線 URL（Snowflake、Databricks、ClickHouse Server、BigQuery、SQLAlchemy） |
| `PROVISA_MATERIALIZE_URL` | — | 覆寫具體化儲存區 DSN（預設為引擎所宣告的預設值） |
| `PROVISA_DATA_DIR` | `~/.provisa` | 內嵌 DuckDB 儲存區的數據目錄（REQ-989） |
| `TRINO_HOST` | `localhost` | Trino 協調器主機 |
| `TRINO_PORT` | `8080` | Trino 協調器 HTTP 連接埠 |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | GCP 服務帳戶金鑰 JSON 的路徑（BigQuery 引擎/數據來源） |
| `GOOGLE_CLOUD_PROJECT` | — | 預設 GCP 專案（BigQuery；可由 URL 覆寫） |
| `FABRIC_SQL_SERVER` | — | Fabric Warehouse SQL 端點（`PROVISA_ENGINE_URL` 的替代方案） |
| `FABRIC_DATABASE` | — | Fabric Warehouse 資料庫名稱 |
| `SYNAPSE_SQL_SERVER` | — | Synapse 無伺服器 SQL 端點 |
| `SYNAPSE_DATABASE` | — | Synapse 資料庫名稱 |
| `REDIS_URL` | — | Redis 連線 URL |
| `PROVISA_SAMPLE_SIZE` | `10000` | 預設取樣限制 |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | 查詢未提供明確 `LIMIT` 時的列數上限 |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | 第一層讀取重試預算，以秒為單位；指數退避加上完全抖動（REQ-703） |
| `ZAYCHIK_PORT` | `8480` | Zaychik Flight SQL 代理伺服器連接埠 |
| `FLIGHT_PORT` | `8815` | Provisa Arrow Flight 伺服器連接埠 |
| `GRPC_PORT` | `50051` | Provisa Protobuf gRPC 伺服器連接埠 |
| `PROVISA_REDIRECT_ENABLED` | `false` | 啟用伺服端門檻重新導向 |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | 預設列數門檻 |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | 預設重新導向格式 |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | 供重新導向結果使用的 S3 儲存桶 |
| `PROVISA_REDIRECT_ENDPOINT` | — | S3 相容端點 URL |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | S3 存取金鑰 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | S3 密鑰 |
| `PROVISA_REDIRECT_TTL` | `3600` | 預先簽署 URL 的 TTL（秒） |
| `ANTHROPIC_API_KEY` | — | Claude API 金鑰（探索用） |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | 覆寫 `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | 覆寫 `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | 覆寫 `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | 覆寫 `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | 批次 span 處理器的排清延遲 |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | 覆寫 `observability.support_endpoint` |
</content>
