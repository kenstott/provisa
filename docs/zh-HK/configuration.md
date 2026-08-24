# 組態參考

Provisa 透過 YAML 檔案組態（預設：`config/provisa.yaml`）。(REQ-528)

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

所有數據來源共用一組通用欄位。[tool-verified: `provisa/core/models.py:129-212`]

| 欄位 | 預設 | 備註 |
| ------- | --------- | ------- |
| `id` | 必填 | 英數字、連字號、底線 |
| `type` | 必填 | 見下表 |
| `host` | `""` | 主機名稱或 IP |
| `port` | `0` | `0` 表示由各連接器提供自己的預設值 — 沒有集中的預設連接埠對應表 |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | 支援 `${env:VAR}` 與 `${secret:NAME}` 認證參照 — 見[密鑰](secrets.md) |
| `path` | `null` | 檔案型數據來源的檔案路徑或 URI |
| `base_url` | `null` | API 數據來源的基底 URL |
| `pool_min` / `pool_max` | `1` / `5` | 連線集區上下限 |
| `cache_enabled` | `true` | 切換此數據來源所有表的快取 |
| `cache_ttl` | `null` | 秒；`null` 沿用全域預設 |
| `federation_hints` | `{}` | 各連接器的延伸參數（dict[str,str]）；見下方類型參考。REQ-281 |
| `mapping` | `{}` | redis、elasticsearch、prometheus 的對應 DSL。REQ-251 |
| `allowed_domains` | `[]` | 將此數據來源限制於特定網域 ID；空值 = 不限制 |
| `description` | `""` | |

### 支援的數據來源類型 [tool-verified: `provisa/core/models.py:36-101`]

| 類型 | 連線方式 | 備註 |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Asyncpg 集區；透過 `use_pgbouncer` 選用 PgBouncer |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path`（DB 檔案） | DuckDB firebird 社群擴充功能 (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | 沿用 PostgreSQL 驅動程式／方言 (REQ-950) |
| `yugabytedb` | host/port | 沿用 PostgreSQL 驅動程式／方言 (REQ-950) |
| `greenplum` | host/port | 沿用 PostgreSQL 驅動程式／方言 (REQ-950) |
| `tidb` | host/port | 沿用 MySQL 驅動程式／方言 (REQ-950) |
| **Cloud DW** | | |
| `snowflake` | host/port + `federation_hints` | hints 中必須有 `account` |
| `bigquery` | `federation_hints` | 必須有 `project`；透過 `GOOGLE_APPLICATION_CREDENTIALS` 驗證 |
| `databricks` | host + `federation_hints` | hints 中必須有 `http_path` |
| `fabric` | 環境變數或 `PROVISA_ENGINE_URL` | 透過 TDS 的 T-SQL，Azure AD 驗證 |
| `synapse` | 環境變數或 `PROVISA_ENGINE_URL` | 透過 TDS 的 T-SQL，Azure AD 驗證 |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | `secure` 提示切換 TLS；連接埠預設 8123/8443 |
| `elasticsearch` | host/port + `mapping` DSL | |
| `pinot` | host/port | Controller REST 端點 |
| `druid` | host/port | Broker Avatica 端點 |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path`（表 URI） | DuckDB `delta_scan`；物件儲存體存取透過 `federation_hints` |
| `iceberg` | `path`（表 URI） | DuckDB `iceberg_scan`；物件儲存體存取透過 `federation_hints` |
| `hudi` | `path`（表 URI） | ClickHouse Hudi 引擎，零複製 (REQ-1178) |
| `hive` | host/port（metastore） + `mapping.storage` | 儲存體後端位於 `mapping["storage"]`：hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port（metastore） + `mapping` S3 鍵 | 獨立類型；一律使用 S3 儲存體 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | 純連線欄位；不使用對應 DSL |
| `cassandra` | host/port | 純連線欄位；不使用對應 DSL |
| `redis` | host/port + `mapping` DSL | |
| **Streaming** | | |
| `kafka` | 僅供註冊 | 真正的組態位於 `kafka_sources[]`；見下方 §Kafka |
| `websocket` | host/port/path + `federation_hints` | 外部 WebSocket 摘要 |
| `rss` | host/port/path + `federation_hints` | RSS 2.0 / Atom 摘要 |
| **Graph/Semantic** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **File** | | |
| `sqlite` | `path` | 一律經由引擎路由（無直接集區） |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path`（目錄） | Glob 爬取器；將 CSV/Parquet/XLSX/JSON 呈現為表 |
| **API/Remote** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port 或 `mapping.url` + `mapping` DSL | |
| `graphql_remote` | `base_url` + 選用 `mapping` | 標頭、轉送用戶端標頭、逾時皆在 `mapping` 中 |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url`（Flight 位置） | DuckDB airport 擴充功能 (REQ-899) |
| `ingest` | 推送接收器 | 外部服務 POST JSON 事件 |
| **SaaS** | | |
| `sharepoint` | `base_url` 或 `host` + `mapping` | 透過 `mapping.auth_type` 驗證 |
| `splunk` | `host`/`port` 或 `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | 獨立的 `GovDataSource` 模型；見下方 §GovData |
| **Data Quality** | | |
| `soda` | 指向 Provisa pgwire 的 host/port | 需要 `soda` extra；Elastic License 2.0，僅限自行託管 (REQ-1443) |
| `great_expectations` | 指向 Provisa pgwire 的 host/port | 需要 `gx` extra；Apache 2.0 (REQ-1443) |

### 數據來源類型參考

組態不夠直觀的類型各有一段簡短說明。RDBMS 類型（postgresql、mysql 等）只用到上述通用欄位 — 不需額外章節。

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

`govdata` 數據來源使用獨立的頂層模型 `GovDataSource`，而非通用的 `Source`。(REQ-540) 存取按主題分組劃分。

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

每個主題對應一個或多個 GovData 結構描述。以某主題組態 `govdata` 數據來源，會自動公開該主題的所有結構描述。(REQ-540)

| 主題 | 結構描述 |
| --------- | --------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ`, `econ_reference` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec` |

`ref` 與 `geo` 結構描述一律作為連結結構描述納入 — 不可組態，也未列於上表。(REQ-541) 使用主題 `ALL` 可授予所有結構描述的存取權。[tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

`sources:` 中的 `kafka` 項目僅供註冊。其連接器的 `details()` 回傳 `{}` — 真正的組態位於頂層 `kafka_sources[]` 區塊，而非 `sources:` 項目。Kafka 一律是 VIRTUAL_SOURCE（經由引擎路由；無直接集區）。[tool-verified: `provisa/transpiler/router.py:44-63`]

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

**時間窗** — `default_window` 將每個查詢限制在近期的一段時間內，避免對高流量主題進行無邊界讀取。(REQ-148) 格式：`1h`、`30m`、`7d`、`60s`。預設為 `1h`。自動注入為 `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`。用戶端可在 GraphQL `where` 引數中以自己的 `_timestamp` 篩選條件覆寫。

**判別器** — 多個主題組態可指向同一個實體 Kafka 主題但使用不同的 `discriminator` 值，產生各自獨立的 GraphQL 類型。(REQ-149) 判別器會自動注入為 WHERE 子句。

**結構描述來源**

| 值 | 行為 |
| ------- | ---------- |
| `registry` | 從 Confluent Schema Registry 取得結構描述 |
| `manual` | 在組態中內嵌定義欄位（不需 Schema Registry） |
| `sample` | 從樣本訊息自動探索 |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`federation_hints` 中必須有 `account`。`warehouse`、`role` 與 `schema` 為選用。

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

`federation_hints` 中必須有 `http_path`。`password` 攜帶個人存取權杖。`catalog` 為選用（由 SQL/hints 攜帶，不在 `database` 欄位）。

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

`federation_hints` 中必須有 `project`。驗證使用 `GOOGLE_APPLICATION_CREDENTIALS`（服務帳戶金鑰檔案的路徑）或引擎環境中的應用程式預設認證。

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

兩者皆使用透過 TDS 的 T-SQL 搭配 Azure AD 驗證。以 `az login`（開發者）或受控識別（生產環境）進行驗證 — 引擎透過 `azure-identity` 的 `DefaultAzureCredential` 讀取認證。連線詳細資料來自環境變數：`FABRIC_SQL_SERVER` / `FABRIC_DATABASE`（Fabric）或 `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE`（Synapse），或透過 `PROVISA_ENGINE_URL`。

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`federation_hints` 中的 `secure` 會在 HTTP 介面上啟用 TLS。連接埠預設為 `8123`（純文字）或 `8443`（當 `secure: "true"`）。`federation_hints` 中的 `schema` 會覆寫遠端結構描述。[tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`path` 是表的 URI（S3、GCS、ADLS 或本機）。物件儲存體存取需要 `federation_hints` 認證。若使用 Cloudflare R2，請加上 `account_id`。

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

`host` 與 `port` 指向 Hive Thrift metastore（預設連接埠 9083）。對 `hive` 而言，設定 `mapping["storage"]` 以選擇物件儲存體後端。缺少必要的鍵會明確失敗 — 沒有後備值。[tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` 是一個獨立類型，一律宣告 S3 儲存體 (REQ-229)；不需 `mapping.storage`。

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

`mapping.storage` 可接受的值：`hadoop`（預設）、`hdfs`、`local`、`s3`、`azure`、`adls`。S3 對應鍵：`endpoint`、`access_key_id`、`secret_access_key`、`region`、`path_style`。ADLS 對應鍵：`storage_account`、`access_key` 或 `sas_token`。

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

使用 `mapping` DSL。`mongodb` 與 `cassandra` 使用純連線欄位，不使用對應 DSL。

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

當 `mapping.url` 與 `host:port` 同時存在時，前者優先。

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

`federation_hints` 中必須有 `spreadsheet_id`。驗證使用在附加時佈建的 DuckDB `gsheet` SECRET。

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### 檔案數據來源（csv / parquet / sqlite / files）

必須有 `path`。`files` 會爬取目錄中的 CSV、Parquet、XLSX 與 JSON 檔案，將每個檔案呈現為一個表。所有檔案型數據來源都是 VIRTUAL（經由引擎路由；無直接集區）。[tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### API／遠端數據來源

**openapi** — 將 `base_url` 設為 OpenAPI 基底 URL。結構描述探索會在啟動時讀取 OpenAPI 規格。

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — 設定 `base_url`。選用的 `mapping` 鍵：`headers`（靜態標頭字典）、`forward_client_headers`（bool）、`timeout_seconds`（int）。[tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

**airport** — `base_url` 是 Arrow Flight 伺服器位置。DuckDB airport 擴充功能 (REQ-899)。[tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — 使用 `host`、`port`、`path` 與 `federation_hints`。[tool-verified: `provisa/api/data/subscribe.py:85-129`]

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

#### 數據品質檢查器（soda / great_expectations）

[tool-verified: `provisa/dq/registration.py`, `provisa/events/source_loader.py` `make_dq_loader`]

檢查器數據來源指向 Provisa 自己的 pgwire 端點，因此單一個 postgres 驅動程式即可掃描以 Snowflake 或 Iceberg 為底的表的聯邦檢視。掃描身分是宣告出來的，不是繼承來的 — 原則套用於該連線，而經過篩選的資料列集合不得產生一項悄悄通過的檢查。連線鍵來自 `mapping`：`host`、`port`、`database`、`user`、`password`。

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

每個結果表都帶有 `dq_contract` — 逐字保留的 Soda 合約 YAML 或 Great Expectations 套件 JSON。欄位、水位與晉升皆由它衍生；完整衍生方式見[數據品質檢查器](sources.md#data-quality-checkers-req-1443)。

**安裝期選擇。** 檢查器並未連結進來 — 掃描在子解譯器中執行，而程式庫只在操作者指名時才安裝。每條安裝路徑（`install.sh`、`packaging/linux/first-launch.sh`，以及 macOS 精靈透過 `PROVISA_DQ_CHECKER`）都會把選擇寫入 `~/.provisa/config.yaml`：

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` 讀取該鍵並匯出 `PROVISA_EXTRAS`，`docker-compose.app.yml` 再把它當作建置引數傳給 `Dockerfile` 的 `ARG PROVISA_EXTRAS`：[tool-verified: `scripts/provisa:69-79`]

| `dq_checker` | `PROVISA_EXTRAS`（Docker 層） | 原生 venv 安裝 |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

安裝示範數據集會把 `none` 提升為 `gx` 並明說此事，因為示範組態在 `pet_store.pets` 上註冊了一個 Great Expectations 套件，否則其品質計分卡將無內容可顯示。指名 `soda` 則維持 `soda`。

以 pip 而非安裝程式取得示範會跳過該精靈步驟，所以 `demo` extra 帶有同一個檢查器：`pip install 'provisa[embedded,demo]'` 正是 `provisa run --demo` 執行掃描所需。缺少它，掃描會回報 `data-quality checker 'great_expectations' is not installed`，並指出安裝指令。

任何其他值都會讓啟動器停止，而不是在缺少操作者指定的檢查器的情況下啟動。`soda` extra 會拉入 `soda-postgres`；`gx` 拉入 `great-expectations[postgresql]`。Soda Core 採用 Elastic License 2.0 — `config/capabilities.yaml` 將該選項標記為 `cloud_eligible: false`，託管平面會拒絕它。

## 網域

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

命名權威是面向用戶端名稱的單一事實來源；實體後端欄位名稱永不對用戶端公開。(REQ-194) 每種查詢語言皆從欄位的 `column.alias`（若已設定）取得名稱，否則依其組態的慣例從實體欄位名稱衍生。(REQ-194)

GraphQL 慣例是三個預設列舉之一。(REQ-416) 舊有的自由格式字串（`none`、`snake_case`、`camelCase`、`PascalCase`）已淘汰。(REQ-416)

| 預設 | 預設值 | 類型名稱 | 欄位名稱 | 變更操作名稱 |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | 是 | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

預設的 GraphQL 慣例是 `apollo_graphql`，產生 camelCase 的欄位與變更操作名稱。(REQ-194, REQ-416) SQL 慣例是分開的，預設為 `snake_case`，透過 `apply_sql_name()` 套用；GraphQL 慣例透過 `apply_gql_name()` 套用，而 CQL 名稱由 GraphQL 名稱衍生。(REQ-194)

`domain_prefix: bool` 是正交選項，不論選用哪個預設都適用。(REQ-416)

明確的 `column.alias` 是正規名稱：SQL 逐字使用它且不套用任何慣例，GraphQL 對它套用自身慣例，CQL 則由 GraphQL 名稱衍生。(REQ-194)

各數據來源覆寫：

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

各表覆寫：

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### 網域前置詞

當 `domain_prefix: true` 時，所有 GraphQL 欄位與類型名稱都會以網域 ID 加上雙底線分隔符作為前置詞：(REQ-154)

| 表 | 網域 | 欄位名稱 |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

這可避免不同網域中同名的表發生名稱衝突，也讓查詢本身具備說明性。

### 命名規則

產生 GraphQL 欄位名稱時套用於表名稱的正規表示式規則。在唯一性解析之前依序套用。(REQ-542)

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

### 別名

表與欄位別名會覆寫預設的 GraphQL 名稱。(REQ-155) 適用於：

- 重新命名難懂的資料庫名稱（例如 `tbl_cust_seg` → `customer_segments`）
- 避免在 API 層出現縮寫
- 建立乾淨、貼近網域的詞彙

### 描述

表與欄位的描述會納入產生的 GraphQL SDL。(REQ-156) 它們會出現在 GraphiQL 的文件探索工具與自省查詢中。可在組態 YAML 或透過管理員 UI 設定。

### Path（計算式 JSON 擷取）

欄位可使用點記法 `path` 從 JSON/JSONB 來源欄位擷取值。(REQ-151) 這對 Kafka 訊息、MongoDB 文件或 PostgreSQL JSONB 欄位中的半結構化數據很有用。

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

path 格式為 `source_column.key1.key2...`。編譯器會在 SQL 中產生 `json_extract_scalar(source_column, '$.key1.key2')`。(REQ-151)

**路由影響：** path 欄位使用 PostgreSQL JSON 運算子（`->>`），直接 PG 路由原生支援。(REQ-152) 對於非 PostgreSQL 數據來源（MySQL、SQL Server 等），含 path 欄位的查詢會自動經由聯邦引擎路由。(REQ-152) 變更操作不受影響，因為 path 欄位是唯讀的計算欄位。(REQ-153)

### 遮罩類型

| 類型 | 欄位 | 描述 |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE（僅限字串欄位） |
| `constant` | `value` | 字面值取代（NULL、0、MAX、MIN、自訂） |
| `truncate` | `precision` | DATE_TRUNC（僅限日期／時間戳記欄位） |

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

在關係上設定 `materialize: true`，即可為跨數據來源的 JOIN 自動產生具體化檢視。(REQ-158) 這藉由預先計算 JOIN 結果來避免昂貴的聯邦查詢。

- 只有跨數據來源的關係會產生 MV（同一數據來源的 JOIN 本來就很快）(REQ-159)
- MV 起初是過時的，由背景重新整理迴圈填入 (REQ-160)
- 對任一方來源表的變更操作會將 MV 標記為過時以便重新整理 (REQ-543)
- `refresh_interval` 預設為 300 秒（5 分鐘）(REQ-543)

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

具有 `parent_role_id` 的角色會從父角色繼承能力與網域存取權。(REQ-215) 階層在啟動時被攤平。(REQ-215)

### 能力

| 能力 | 描述 |
| ----------- | ------------- |
| `source_registration` | 註冊數據來源 |
| `table_registration` | 註冊表 |
| `relationship_registration` | 定義關係 |
| `security_config` | 組態 RLS、遮罩 |
| `query_development` | 執行查詢 |
| `full_results` | 略過抽樣限制 |
| `admin` | 所有能力 |

## RLS 規則

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

## 檢視（受治理的計算式數據集）

檢視是以 SQL 定義的計算式數據集，具備完整的欄位級治理。(REQ-133) 它們是在語意層加入彙總、轉換與衍生指標的受治理機制。(REQ-136)

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
| `domain_id` | 是 | 結構描述可見性所屬的網域 |
| `materialize` | 否 | `true` = 定期 CTAS 重新整理，`false` = 即時聯邦檢視 |
| `refresh_interval` | 否 | 兩次重新整理之間的秒數（僅具體化檢視，預設 300） |
| `description` | 否 | 出現在 GraphQL SDL 中 |
| `alias` | 否 | 覆寫 GraphQL 名稱 |
| `columns` | 是 | 含可見性、遮罩與描述的欄位定義 |

### 具體化與即時

- **`materialize: true`**：Provisa 透過 CTAS 建立一個表，並依排程重新整理。(REQ-135) 查詢較快，但數據最多可能過時 `refresh_interval` 秒。
- **`materialize: false`**：Provisa 建立一個聯邦檢視。(REQ-135) 查詢一律回傳即時數據，但複雜彙總可能較慢。

檢視與表走同一條治理管線 — RLS、遮罩、抽樣與角色型可見性。(REQ-134) 這確保平台上不會在缺乏數據管家監督的情況下加入新語意。(REQ-136)

### 僅供查詢的檢視

`materialize: true` 與 `materialize: false` 的檢視都以僅供查詢的方式公開其 GraphQL 類型。以 `view_sql` 為底的關聯不會產生 insert、upsert、update 或 delete 變更操作。(REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## 快取

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### 快取階層

TTL 解析順序（最明確者勝出）：**表** > **數據來源** > **全域預設**。(REQ-544) 採用第一個非 null 的值。

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

在數據來源上設定 `cache_enabled: false` 會停用該數據來源所有表的快取，不論表層級的 TTL 為何。(REQ-544) 快取鍵一律包含 `role_id` 加 RLS 內容值，以達成安全分區。(REQ-544)

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

### 驗證提供者類型

| 提供者 | 使用情境 | 權杖驗證 |
| ---------- | ---------- | ----------------- |
| `simple` | 本機開發／測試。使用者定義於 YAML。 | 以 `PROVISA_JWT_SECRET` 簽署的 JWT |
| `firebase` | Firebase Authentication（所有方式）。 | `firebase-admin` SDK `verify_id_token()` |
| `keycloak` | Keycloak OIDC。對應租用戶與用戶端角色。 | 以 JWKS 為基礎的 JWT 驗證 |
| `oauth` | 通用 OIDC（Okta、Azure AD、Auth0、PingFederate）。 | 來自探索 URL 的 JWKS |
| `basic` | 自足式部署。帳戶存放於 Provisa 自己的儲存區。 | bcrypt 密碼，或 pgwire 上的 SCRAM-SHA-256 |

超級使用者認證（`superuser` 區塊）可搭配任何提供者使用，並一律解析為具備所有能力的 admin 角色。(REQ-125) 用於在外部驗證組態完成前的初始設定。

### SCRAM-SHA-256（`auth.scram`）

```yaml
auth:
  provider: basic
  scram: true
```

讓 pgwire 以 `SCRAM-SHA-256` 公告 SASL，因此密碼是被證明而非以明文傳送。(REQ-1394) 它僅適用於 `basic` 提供者 — 其他提供者都不持有 SCRAM 所需的 RFC 5802 驗證子 — 且不提供通道繫結。

驗證子無法從既有的 bcrypt 雜湊衍生。每當密碼以純文字經過時就會寫入一個，因此每位使用者的第一次 SCRAM 連線發生在其下一次註冊、登入、變更密碼或管理員重設之後。在那之前，該使用者的連線會回退為經 TLS 的明文交換；線路上不會顯示誰已遷移。

### 登入節流（`auth.login_throttle`）

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

預設以所示值啟用；此區塊只用來調整。(REQ-1393) 計數器位於認證驗證層，因此經由 HTTP、pgwire 與 Bolt 的失敗會累加到同一個主體上，而鎖定在每個介面上都成立。它是每個處理程序各自計算：多個 API 工作處理程序各自允許至多 `max_attempts` 次。

### 個人存取權杖

PAT 不需組態區塊 — 它們一律被接受，且其儲存區隨控制平面結構描述一併建立。(REQ-1263) 可組態的是使用者在簽發時可要求的到期時間：1 至 366 天，或不設到期的永久權杖。見[安全模型](security.md#personal-access-tokens)。

### 雙向 TLS

用戶端憑證驗證是以環境變數組態，而非在 `provisa.yaml` 中，與它所延伸的 TLS 憑證設定並列。(REQ-1228)

| 變數 | 預設 | 意義 |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 未設定 | 獲准簽署用戶端憑證的 CA 的 PEM 套件。設定它即開啟用戶端憑證驗證 |
| `PROVISA_MTLS_MODE` | 設定 CA 後為 `required` | `required` 或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | 要求憑證的一般名稱等於該連線驗證所用的使用者名稱 |

每一項都可依協定覆寫，命名方式與 TLS 設定相同。設定了模式卻沒有 CA，或模式不是這兩個值之一，都會拒絕啟動，而不是提供操作者以為已驗證的連線。

### 透過 TLS 指向某個組織

無需組態。在多組織部署上，pgwire 與 Bolt 從用戶端撥接的主機名稱讀取組織，該名稱由 TLS ClientHello 攜帶，正如 HTTP 從 `Host` 標頭讀取一樣。(REQ-1234) 連線到 `acme.provisa.dev` 的用戶端即請求組織 `acme`；除非通過驗證的主體是其成員，否則請求會被拒絕。以 IP 位址連線則不請求任何組織，這正是單組織部署上的每一條連線。

### 完整驗證組態範例（已註解）

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

## Upsert 變更操作

對於具有主索引鍵的表，Provisa 會自動產生 `upsert_<table>` 變更操作欄位。(REQ-212) 它們會編譯成目標方言的 upsert — PostgreSQL 上的 `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`，MySQL 上的 `ON DUPLICATE KEY UPDATE`。(REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

衝突欄位由 PK 中繼資料衍生。(REQ-212) 所有欄位可見性與寫入權限規則均適用。

## Distinct On

`distinct_on` 引數會針對指定欄位的每個相異值選出第一列。(REQ-213) 可用於根查詢欄位。

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

在 PostgreSQL 中編譯成 `SELECT DISTINCT ON (region) ...`。(REQ-213) 對非 PG 方言則使用視窗函式的替代做法。(REQ-213)

## 欄位預設值

在插入／更新時自動注入值到欄位。(REQ-214) 於組態中按表定義。

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
| `header` | 從指名的 HTTP 請求標頭注入值 |
| `now` | 注入 `NOW()`（目前時間戳記） |
| `literal` | 注入常數值 |

預設欄位在變更操作編譯期間、SQL 產生之前注入。(REQ-214) 它們不會出現在變更操作的輸入類型中。(REQ-214)

## 繼承角色

角色可透過 `parent_role_id` 從父角色繼承能力與網域存取權。(REQ-215) 階層在啟動時被攤平。(REQ-215)

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

支援多層繼承。(REQ-215) 子角色明確設定的 capabilities 與 domain_access 會與父角色的合併。(REQ-215)

## 排程觸發器

依排程呼叫 webhook URL 的 cron 型觸發器。(REQ-216) 使用 APScheduler。(REQ-216)

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

排程工作透過管理員 UI（啟用／停用切換）或 `toggle_scheduled_task` 管理員變更操作管理。(REQ-216)

## OrderBy 格式

OrderBy 使用 `{column: direction}` 格式，方向為六值列舉：(REQ-200, REQ-201)

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

Provisa 執行兩條獨立的 OTLP 匯出路徑：你的內部收集器，以及選用的 Provisa 支援端點。(REQ-545) 每條路徑各有自己的篩選器。篩選器在跨距離開處理程序之前，於包覆的 `_FilteringExporter` 內執行 — 原始跨距物件永不被變更。(REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — 控制哪些內容到達你的內部收集器。

| 鍵 | 類型 | 預設 | 描述 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | 將 `db.statement` 中的字串與數值字面值取代為 `?` |
| `redact_attributes` | list[str] | `[]` | 從每個跨距中完全移除的屬性鍵 |

**`support_telemetry_filter`** — 控制哪些內容到達 Provisa 支援端點。這條路徑上的 SQL 字面值編修預設為 `true`，因為查詢數據屬於你。(REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| 鍵 | 類型 | 預設 | 描述 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | 將 `db.statement` 中的字串與數值字面值取代為 `?` |
| `redact_attributes` | list[str] | `[]` | 從每個跨距中完全移除的屬性鍵 |

編修後的 `db.statement` 範例 — 在 `redact_sql_literals: true` 之下，這個跨距屬性：

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

會變成：

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### 支援端點 [tool-verified]

`support_endpoint`（或環境變數 `PROVISA_SUPPORT_OTLP_ENDPOINT`）會將遙測轉送給 Provisa 支援以供診斷。(REQ-548) 未設定時，沒有數據會經由這條路徑離開你的基礎架構。(REQ-548) 支援篩選器獨立於內部篩選器運作 — 你可以在兩邊匯出中都編修 SQL 字面值，同時仍與支援分享跨距時間與錯誤數據。(REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### 端點協定偵測 [tool-verified]

Provisa 從端點 URL 的配置選擇 OTLP/HTTP 或 OTLP/gRPC。(REQ-549) 以 `http://` 或 `https://` 開頭的 URL 使用 OTLP/HTTP，並自動附加 `/v1/traces`、`/v1/metrics` 與 `/v1/logs`。(REQ-549) 其他任何配置皆使用 OTLP/gRPC 並帶 `insecure=True`。(REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## 聯邦引擎

組態聯邦引擎是選用的。預設為 `duckdb` — 零組態、行程內、不需外部服務 (REQ-989)。當你需要 MPP 規模，或想沿用既有的數據倉庫時，才選擇其他引擎。

優先順序：`PROVISA_ENGINE` 環境變數 → 持久化的管理員 UI `federation_engine` 組態欄位 → `duckdb`。變更於服務重新啟動時生效。[tool-verified: `engine.py` `build_engine`]

### 引擎總覽 [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| 引擎鍵 | 標籤 | 方言 | MPP | 外部連結機制 | 驗證 |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | 是 | Trino 目錄（廣泛的連接器集合） | JDBC 認證 |
| `trino-byo` | Trino | Trino SQL | 是 | 同 `trino`；非受管協調器 | JDBC 認證 |
| `pg` | PostgreSQL | PostgreSQL | 否 | FDW / pg_duckdb | PostgreSQL 認證 |
| `duckdb` | DuckDB | DuckDB | 否 | 擴充功能原生的 ATTACH | 無（行程內） |
| `clickhouse` | ClickHouse (embedded) | ClickHouse | 是 | S3 / IcebergS3 / DeltaLake 表引擎 | chdb（行程內，無驗證） |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | 是 | S3 / IcebergS3 / DeltaLake 表引擎 | ClickHouse 認證 |
| `snowflake` | Snowflake | Snowflake | 是 | 外部暫存區 + 外部表 | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | 是 | 透過 REST 的 Unity Catalog 外部表 | `PROVISA_ENGINE_URL`（持有人權杖 + `http_path`） |
| `bigquery` | BigQuery | BigQuery | 是 | BigQuery 外部／BigLake 表 | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | 是 | OneLake 捷徑 → OPENROWSET | Azure AD（`az login` 或受控識別） |
| `synapse` | Azure Synapse | T-SQL | 是 | ADLS OPENROWSET／外部表 | Azure AD |
| `mysql` | MySQL | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `mariadb` | MariaDB | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `oracle` | Oracle Database | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `mssql` | Microsoft SQL Server | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `db2` | IBM Db2 | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `redshift` | Amazon Redshift | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `greenplum` | Greenplum | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `cockroachdb` | CockroachDB | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `yugabytedb` | YugabyteDB | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `opengauss` | openGauss | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `tidb` | TiDB | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `singlestore` | SingleStore | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `vertica` | Vertica | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `exasol` | Exasol | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `teradata` | Teradata Vantage | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `saphana` | SAP HANA | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `sapase` | SAP ASE (Sybase) | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `sqlanywhere` | SAP SQL Anywhere | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `monetdb` | MonetDB | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `firebird` | Firebird | 依方言 | 否 | 無（僅落地） | 依方言的認證 |
| `sqlalchemy` | 其他關聯式資料庫（以連線 URL 指定） | 依方言 | 否 | 無（僅落地） | 依方言的認證 |

### 引擎參考

#### trino / trino-byo

`trino` 是受管的 Provisa 協調器；`trino-byo` 連接你自己的 Trino 叢集。兩者皆使用 Trino SQL，且數據來源類型的觸及範圍最廣。

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

具體化儲存區預設為 `TENANT_DATABASE_URL`（PostgreSQL）。

#### pg

透過 postgres_fdw（SQL/MED）與 pg_duckdb 擴充功能進行聯邦。單節點；無 MPP。當你的數據已存放在 PostgreSQL 中，而你想併入少數幾個遠端數據來源時最合適。

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### duckdb

行程內；無外部服務。預設引擎 (REQ-989)。`PROVISA_DATA_DIR` 控制內嵌儲存區的位置（預設 `~/.provisa`）。

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

具體化儲存區預設為 `~/.provisa/materialize.duckdb` — 唯一預設儲存區不是 PostgreSQL 的引擎。

#### clickhouse (embedded) / clickhouse-server

`clickhouse` 使用 chdb（行程內）。`clickhouse-server` 連接外部 ClickHouse 執行個體或 ClickHouse Cloud。兩者皆透過原生 ClickHouse 表引擎直接讀取 Delta Lake、Iceberg 與 Hudi。

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### snowflake

引擎即數據倉庫：由 Snowflake 執行查詢；Provisa 透過外部暫存區把數據來源的數據推送過去。

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### databricks

Unity Catalog 外部表把 Provisa 管理的數據來源橋接進 Databricks SQL。

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### bigquery

BigQuery 外部表與 BigLake 表。專案來自 URL 或 `GOOGLE_CLOUD_PROJECT`；以服務帳戶金鑰驗證。

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### fabric / synapse

兩者皆使用透過 TDS 的 T-SQL 搭配 Azure AD 驗證（`az login` 或受控識別）。省略 `PROVISA_ENGINE_URL` 即改從環境變數讀取連線詳細資料。

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

#### 關聯式資料庫引擎（mysql、mariadb、oracle、mssql、db2、redshift、greenplum、cockroachdb、yugabytedb、opengauss、tidb、singlestore、vertica、exasol、teradata、saphana、sapase、sqlanywhere、monetdb、firebird）與 `sqlalchemy`

每個可透過網路定址的關聯式資料庫各有一個鍵，全都跑在同一套僅落地的執行環境上（不聯邦到外部數據來源）：每個數據來源都落地到儲存區，並在那裡被查詢。鍵用來選擇資料庫；`PROVISA_ENGINE_URL` 攜帶其方言所接受的 DSN。`sqlalchemy` 是沒有專屬鍵的資料庫的萬用選項。不提供檔案內嵌式儲存區（SQLite、Access）— 伺服器必須可透過網路連達。

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

### 具體化儲存區

當某個數據來源無法即時附加（所選引擎沒有對應的 ATTACH 連接器）時，它會落地到該引擎的具體化儲存區。解析順序：明確的 `PROVISA_MATERIALIZE_URL` → 引擎宣告的預設值 → 直接報錯（不會悄悄退回）。[tool-verified: `engine.py` `materialize_store`]

DuckDB 宣告其內嵌檔案（`~/.provisa/materialize.duckdb`）為預設值。其他所有引擎皆預設為 `TENANT_DATABASE_URL`（PostgreSQL）。任一引擎都可用 `PROVISA_MATERIALIZE_URL` 覆寫。

### 各數據來源的聯邦提示

標準 host/port/user/password 欄位無法攜帶的延伸連線參數，放在數據來源的 `federation_hints` 中。各類型的提示鍵見上方的數據來源類型參考。一個整合的範例：

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

若使用 Google Cloud 數據來源，請將 `GOOGLE_APPLICATION_CREDENTIALS` 設為服務帳戶金鑰檔案的路徑。若使用 Fabric 與 Synapse，請以 `az login`（開發者）或受控識別（生產環境）驗證 — 引擎透過 `azure-identity` 的 `DefaultAzureCredential` 讀取認證。

## 環境變數

| 變數 | 預設 | 描述 |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 組態檔路徑 |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | 控制平面儲存區 URI（SQLAlchemy async）；內嵌桌面儲存區接受 `sqlite+aiosqlite://…` / `duckdb://…` (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | 平台登錄 URI（租用戶目錄、引擎登錄）；啟動時必要，無後備值 (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` 改用內嵌的 fakeredis 而非 Redis 伺服器 — 不需 Docker (REQ-829) |
| `PG_HOST` | `localhost` | PostgreSQL 主機 |
| `PG_PORT` | `5432` | PostgreSQL 連接埠 |
| `PG_DATABASE` | `provisa` | PostgreSQL 資料庫 |
| `PG_USER` | `provisa` | PostgreSQL 使用者 |
| `PG_PASSWORD` | `provisa` | PostgreSQL 密碼 |
| `PROVISA_ENGINE` | `duckdb` | 聯邦引擎鍵 (REQ-989, REQ-916) |
| `PROVISA_ENGINE_URL` | — | URL 驅動引擎的連線 URL（Snowflake、Databricks、ClickHouse Server、BigQuery、SQLAlchemy） |
| `PROVISA_MATERIALIZE_URL` | — | 覆寫具體化儲存區 DSN（預設為引擎宣告的預設值） |
| `PROVISA_DATA_DIR` | `~/.provisa` | 內嵌 DuckDB 儲存區的數據目錄 (REQ-989) |
| `TRINO_HOST` | `localhost` | Trino 協調器主機 |
| `TRINO_PORT` | `8080` | Trino 協調器 HTTP 連接埠 |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | GCP 服務帳戶金鑰 JSON 的路徑（BigQuery 引擎／數據來源） |
| `GOOGLE_CLOUD_PROJECT` | — | 預設 GCP 專案（BigQuery；由 URL 覆寫） |
| `FABRIC_SQL_SERVER` | — | Fabric Warehouse SQL 端點（`PROVISA_ENGINE_URL` 的替代方式） |
| `FABRIC_DATABASE` | — | Fabric Warehouse 資料庫名稱 |
| `SYNAPSE_SQL_SERVER` | — | Synapse 無伺服器 SQL 端點 |
| `SYNAPSE_DATABASE` | — | Synapse 資料庫名稱 |
| `REDIS_URL` | — | Redis 連線 URL |
| `PROVISA_SAMPLE_SIZE` | `10000` | 預設抽樣上限 |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | 查詢未提供明確 `LIMIT` 時的資料列上限 |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | 第一層讀取重試預算（秒）；指數輪詢退避搭配完全抖動 (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Zaychik Flight SQL 代理連接埠 |
| `FLIGHT_PORT` | `8815` | Provisa Arrow Flight 伺服器連接埠 |
| `GRPC_PORT` | `50051` | Provisa Protobuf gRPC 伺服器連接埠 |
| `PROVISA_REDIRECT_ENABLED` | `false` | 啟用伺服器端門檻重新導向 |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | 預設資料列數門檻 |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | 預設重新導向格式 |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | 存放重新導向結果的 S3 貯體 |
| `PROVISA_REDIRECT_ENDPOINT` | — | S3 相容端點 URL |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | S3 存取金鑰 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | S3 密鑰 |
| `PROVISA_REDIRECT_TTL` | `3600` | 預先簽署 URL 的 TTL（秒） |
| `PROVISA_MTLS_CLIENT_CA` | — | 獲准簽署用戶端憑證的 CA 的 PEM 套件；設定它會在 pgwire、Bolt、gRPC 與 Flight 上開啟用戶端憑證驗證 (REQ-1228) |
| `PROVISA_MTLS_MODE` | 設定 CA 後為 `required` | `required` 或 `optional`；其他任何值都會拒絕啟動 (REQ-1228) |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | 要求憑證的一般名稱等於進行驗證的使用者名稱 (REQ-1228) |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | 以逗號分隔、獲准從瀏覽器開啟 Bolt WebSocket 的站台；未設定則拒絕每一個瀏覽器來源 (REQ-802) |
| `PROVISA_EXTRAS` | `firebase,vector` | 烘焙進應用程式映像的 pyproject extras；`scripts/provisa` 由 `~/.provisa/config.yaml` 中的 `dq_checker` 推導 (REQ-1443) |
| `PROVISA_DQ_CHECKER` | `none` | 僅供安裝程式使用：`none`/`soda`/`gx`，由 `first-launch.sh` 在非互動模式下讀取，並以 `dq_checker` 寫入 `config.yaml` (REQ-1443) |
| `ANTHROPIC_API_KEY` | — | Claude API 金鑰（探索） |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | 覆寫 `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | 覆寫 `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | 覆寫 `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | 覆寫 `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | 批次跨距處理器的排清延遲 |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | 覆寫 `observability.support_endpoint` |
