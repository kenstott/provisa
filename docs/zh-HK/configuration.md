# 組態參考 {: #configuration-reference }

Provisa 透過 YAML 檔案組態（預設：`config/provisa.yaml`）。

## 包含檔案 {: #includes }

使用 `includes:` 將一份組態拆分到多個檔案中。包含檔案在此鍵下列出片段路徑；Provisa 會在驗證前合併它們，產生與將所有內容寫在單一檔案中相同的結果。

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

僅包含 `includes:` 的包裝檔案是合法的。`load_control_plane` 會遍歷這些包含項，因此 `control_plane:` 部分取自設定了該項的那個被包含檔案。

**合併規則**

- 路徑相對於包含檔案解析。絕對路徑按原樣使用。
- 清單類小節（`sources`、`tables`、`domains`、`relationships`、`roles` 等）採用附加方式——片段中的項目跟在包含檔案的項目之後。
- 包含檔案未設定的純量或對應鍵，取自片段。
- 兩個檔案對同一個鍵設定了不同的值，視為衝突；載入失敗，並指出衝突的鍵。
- 兩個檔案中的相同值不視為衝突。
- `includes` 可以巢狀。一個檔案——無論是直接還是透過另一個片段——包含自身會被拒絕。
- `includes` 在載入時被消耗，永遠不會出現在驗證後的組態中。

## 數據來源 {: #sources }

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

所有數據來源共用一組通用欄位。

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
| `cache_ttl` | `null` | 秒；`null` 沿用全域預設；`-1` 永不過期 |
| `federation_hints` | `{}` | 各連接器的延伸參數（dict[str,str]）；見下方類型參考。 |
| `mapping` | `{}` | redis、elasticsearch、prometheus 的對應 DSL。 |
| `allowed_domains` | `[]` | 將此數據來源限制於特定網域 ID；空值 = 不限制 |
| `description` | `""` | |

### 支援的數據來源類型 {: #supported-source-types }

| 類型 | 連線方式 | 備註 |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Asyncpg 集區；透過 `use_pgbouncer` 選用 PgBouncer |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path`（DB 檔案） | DuckDB firebird 社群擴充功能 |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | 沿用 PostgreSQL 驅動程式／方言 |
| `yugabytedb` | host/port | 沿用 PostgreSQL 驅動程式／方言 |
| `greenplum` | host/port | 沿用 PostgreSQL 驅動程式／方言 |
| `tidb` | host/port | 沿用 MySQL 驅動程式／方言 |
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
| `hudi` | `path`（表 URI） | ClickHouse Hudi 引擎，零複製 |
| `hive` | host/port（metastore） + `mapping.storage` | 儲存體後端位於 `mapping["storage"]`：hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port（metastore） + `mapping` S3 鍵 | 獨立類型；一律使用 S3 儲存體 |
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
| `airport` | `base_url`（Flight 位置） | DuckDB airport 擴充功能 |
| `ingest` | 推送接收器 | 外部服務 POST JSON 事件 |
| **SaaS** | | |
| `sharepoint` | `base_url` 或 `host` + `mapping` | 透過 `mapping.auth_type` 驗證 |
| `splunk` | `host`/`port` 或 `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | 獨立的 `GovDataSource` 模型；見下方 §GovData |
| **Data Quality** | | |
| `soda` | 指向 Provisa pgwire 的 host/port | 需要 `soda` extra；Elastic License 2.0，僅限自行託管 |
| `great_expectations` | 指向 Provisa pgwire 的 host/port | 需要 `gx` extra；Apache 2.0 |

### 數據來源類型參考 {: #source-type-reference }

需要非顯而易見設定的每種類型在下面都有一個簡短條目。RDBMS 類型（postgresql、mysql 等）只使用上面的通用欄位，不需要獨立的小節。

#### GovData

`govdata` 數據來源使用自己的頂層模型 `GovDataSource`，而不是通用的 `Source`。存取權限按主題分組劃分。

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

每個主題對應一個或多個 GovData 結構描述。以某主題組態 `govdata` 數據來源，會自動公開該主題的所有結構描述。

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

`ref` 及 `geo` 結構描述一律作為關聯結構描述包含在內，無法設定，亦不在上面的清單中。使用主題 `ALL` 可授予對所有結構描述的存取權。

#### Kafka

`sources:` 中的 `kafka` 行只用於註冊數據來源。其連接器的 `details()` 傳回 `{}`；真正的設定在頂層的 `kafka_sources[]` 區塊中。Kafka 一律是 VIRTUAL_SOURCE：經由引擎路由，沒有直接連線池。

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

**時間窗** — `default_window` 將每個查詢限制在近期的一段時間內，避免對高流量主題進行無邊界讀取。 格式：`1h`、`30m`、`7d`、`60s`。預設為 `1h`。自動注入為 `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`。用戶端可在 GraphQL `where` 引數中以自己的 `_timestamp` 篩選條件覆寫。

**判別器** — 多個主題組態可指向同一個實體 Kafka 主題但使用不同的 `discriminator` 值，產生各自獨立的 GraphQL 類型。 判別器會自動注入為 WHERE 子句。

**結構描述來源**

| 值 | 行為 |
| ------- | ---------- |
| `registry` | 從 Confluent Schema Registry 取得結構描述 |
| `manual` | 在組態中內嵌定義欄位（不需 Schema Registry） |
| `sample` | 從樣本訊息自動探索 |

#### Snowflake

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

#### Databricks

`federation_hints` 中的 `http_path` 是必需的。`password` 承載個人存取權杖。`catalog` 是可選的，放在 SQL 或 hints 中；`database` 欄位不存放它。

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

`federation_hints` 中必須有 `project`。驗證使用 `GOOGLE_APPLICATION_CREDENTIALS`（服務帳戶金鑰檔案的路徑）或引擎環境中的應用程式預設認證。

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

兩者都經 TDS 使用 T-SQL，並採用 Azure AD 驗證。用 `az login`（開發）或受控識別（生產）登入；引擎經 `azure-identity` 的 `DefaultAzureCredential` 讀取憑證。連線資料來自環境變數 `FABRIC_SQL_SERVER` / `FABRIC_DATABASE`（Fabric）或 `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE`（Synapse），亦可以來自 `PROVISA_ENGINE_URL`。

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`federation_hints` 中的 `secure` 會在 HTTP 介面上啟用 TLS。連接埠預設為 `8123`（純文字）或 `8443`（當 `secure: "true"`）。`federation_hints` 中的 `schema` 會覆寫遠端結構描述。

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

#### Hive / Hive S3

`host` 及 `port` 指向 Hive Thrift 中繼資料存放區（預設連接埠 9083）。對於 `hive`，用 `mapping["storage"]` 選擇物件儲存後端。缺少必需的鍵會明確報錯，沒有後備值。

`hive_s3` 是一個獨立類型，一律宣告 S3 儲存體；不需 `mapping.storage`。

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

#### Redis

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

#### Google Sheets

`federation_hints` 中必須有 `spreadsheet_id`。驗證使用在附加時佈建的 DuckDB `gsheet` SECRET。

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### 檔案數據來源（csv / parquet / sqlite / files） {: #file-sources-csv-parquet-sqlite-files }

必須有 `path`。`files` 會爬取目錄中的 CSV、Parquet、XLSX 與 JSON 檔案，將每個檔案呈現為一個表。所有檔案型數據來源都是 VIRTUAL（經由引擎路由；無直接集區）。

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

**Kaggle 數據集**不能經此檔案加入，因為它們需要有效權杖以及數據來源表單中的數據集選擇器（數據來源 → 訂閱 → Kaggle）。匯出為 YAML 的 Kaggle 數據來源顯示為 `type: files`，並在 `federation_hints` 中帶有 `kaggle_owner` 及 `kaggle_ref`。要重新從 Kaggle 下載，需要使用 `refreshKaggleSource` 變更或介面中的重新整理；編輯 YAML 做不到。參見數據來源類型參考中的 [Kaggle 數據集](sources.md#kaggle-datasets)。

#### API／遠端數據來源 {: #api-remote-sources }

**openapi** — 將 `base_url` 設為 OpenAPI 基底 URL。結構描述探索會在啟動時讀取 OpenAPI 規格。

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — 設定 `base_url`。選用的 `mapping` 鍵：`headers`（靜態標頭字典）、`forward_client_headers`（bool）、`timeout_seconds`（int）。

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

**airport** — `base_url` 是 Arrow Flight 伺服器位置。DuckDB airport 擴充功能。

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — 使用 `host`、`port`、`path` 與 `federation_hints`。

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

#### 數據品質檢查器（soda / great_expectations） {: #data-quality-checkers-soda-great_expectations }

檢查器數據來源指向 Provisa 自己的 pgwire 端點，因此一個 postgres 驅動程式就能掃描背後是 Snowflake 或 Iceberg 的資料表的聯邦檢視。掃描身分需要明確宣告，從不繼承。政策作用於這個連線，被篩選的資料列集不能令檢查悄悄通過。連線鍵來自 `mapping`：`host`、`port`、`database`、`user`、`password`。

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

每個結果資料表都帶有 `dq_contract`，即原樣保存的 Soda 契約 YAML 或 Great Expectations 套件 JSON。資料行、水位及晉升都由它推導；具體方式見[數據品質檢查器](sources.md#data-quality-checkers)。

**安裝時選擇。** 檢查器不會被連結進來：掃描在子直譯器中執行，只有營運者指定時才安裝相應的程式庫。每條安裝路徑（`install.sh`、`packaging/linux/first-launch.sh`，以及經 `PROVISA_DQ_CHECKER` 的 macOS 精靈）都會把選擇寫入 `~/.provisa/config.yaml`：

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` 讀取該鍵並匯出 `PROVISA_EXTRAS`，`docker-compose.app.yml` 再把它當作建置引數傳給 `Dockerfile` 的 `ARG PROVISA_EXTRAS`：

| `dq_checker` | `PROVISA_EXTRAS`（Docker 層） | 原生 venv 安裝 |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

安裝示範數據集會把 `none` 提升為 `gx` 並明說此事，因為示範組態在 `pet_store.pets` 上註冊了一個 Great Expectations 套件，否則其品質計分卡將無內容可顯示。指名 `soda` 則維持 `soda`。

如經 pip 而不是安裝程式進入示範，就會略過那一步精靈，所以 `demo` 額外依賴包含同一個檢查器：`provisa run --demo` 需要 `pip install 'provisa[embedded,demo]'` 才能執行掃描。缺少它時，掃描會報告 `data-quality checker 'great_expectations' is not installed`，並給出安裝命令。

任何其他值都會令啟動器停止，所以它不會在缺少營運者所要求檢查器的情況下啟動。`soda` 額外依賴會拉取 `soda-postgres`，`gx` 會拉取 `great-expectations[postgresql]`。Soda Core 採用 Elastic License 2.0：`config/capabilities.yaml` 把該選項標為 `cloud_eligible: false`，託管平面會拒絕它。

## 網域 {: #domains }

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

### 命名慣例 {: #naming-convention }

命名權威是面向用戶端名稱的單一事實來源；實體後端欄位名稱永不對用戶端公開。 每種查詢語言皆從欄位的 `column.alias`（若已設定）取得名稱，否則依其組態的慣例從實體欄位名稱衍生。

GraphQL 慣例是三個預設列舉之一。 舊有的自由格式字串（`none`、`snake_case`、`camelCase`、`PascalCase`）已淘汰。

| 預設 | 預設值 | 類型名稱 | 欄位名稱 | 變更操作名稱 |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | 是 | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

預設的 GraphQL 慣例是 `apollo_graphql`，產生 camelCase 的欄位與變更操作名稱。 SQL 慣例是分開的，預設為 `snake_case`，透過 `apply_sql_name()` 套用；GraphQL 慣例透過 `apply_gql_name()` 套用，而 CQL 名稱由 GraphQL 名稱衍生。

`domain_prefix: bool` 是正交選項，不論選用哪個預設都適用。

明確的 `column.alias` 是正規名稱：SQL 逐字使用它且不套用任何慣例，GraphQL 對它套用自身慣例，CQL 則由 GraphQL 名稱衍生。

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

### 網域前置詞 {: #domain-prefix }

當 `domain_prefix: true` 時，所有 GraphQL 欄位與類型名稱都會以網域 ID 加上雙底線分隔符作為前置詞：

| 表 | 網域 | 欄位名稱 |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

這可避免不同網域中同名的表發生名稱衝突，也讓查詢本身具備說明性。

### 命名規則 {: #naming-rules }

產生 GraphQL 欄位名稱時套用於表名稱的正規表示式規則。在唯一性解析之前依序套用。

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

資料表預設沒有任何寫入授權，因此任何角色都不能寫入它。一條授權賦予某個角色資料表上的一種操作（`insert`、`update` 或 `delete`）。一張資料表對每個角色和操作最多保存一條授權。

| 授權欄位 | 含義 |
| ----------- | ------- |
| `columns` | `INSERT` 可以列出、`UPDATE` 可以設定的欄位。`delete` 授權不指定任何欄位。 |
| `rows` | `UPDATE` 或 `DELETE` 可以觸及的行：以資料表各欄位構成的 SQL 謂詞。`insert` 授權沒有此項。 |
| `check` | `INSERT` 寫入的每一行、或 `UPDATE` 留下的每一行必須滿足的條件：SQL 謂詞。`delete` 授權沒有此項。 |
| `presets` | 授權自己設定的欄位。陳述式不得提供這些欄位，因此預設欄位不會同時出現在 `columns` 中。 |

省略 `rows` 或 `check` 時，授權採用該角色在這張資料表上的讀取行篩選器：除非授權另有規定，角色寫入的就是它能讀到的行。`"TRUE"` 表示所有行。舊的欄位層級 `writable_by` 鍵和資料表層級 `column_presets` 鍵會按名稱被拒絕；在改為 `write_grants` 之前，設定無法載入。

每個介面上的每次寫入都按以下順序准入：角色持有 `write` 權利；角色持有資料表上該操作的授權；陳述式寫入的每個欄位都在授權中；授權的預設會加入陳述式；`rows` 謂詞以 AND 併入 `UPDATE` 或 `DELETE` 的 `WHERE`；在向數據來源發送任何內容之前，先對新行評估 `check`。`check` 無法判定的陳述式會被拒絕。授權的欄位同時決定 GraphQL 向角色提供哪些變更欄位：角色持有授權的每種操作各提供一個欄位，且恰好帶有這些欄位。

有些陳述式需要更寬的授權。`TRUNCATE` 需要涵蓋所有行的 `delete` 授權。`MERGE` 需要 `insert` 和 `update` 授權，兩者都涵蓋所有行。`INSERT ... ON CONFLICT DO UPDATE` 需要涵蓋所有行且沒有預設的 `update` 授權。`COPY ... FROM STDIN` 需要沒有預設的 `insert` 授權。

授權可以繼承。子角色針對每張資料表和每種操作，採用其鏈中最近的持有該授權的角色的授權，自己的授權優先。元角色持有其成員授權的摺疊結果：成員各欄位的聯集、各 `rows` 和 `check` 謂詞的 OR，以及只保留所有成員設定一致的預設。

在管理介面中，資料表的**寫入**部分會列出這些授權並統一編輯。

### 別名 {: #aliases }

表與欄位別名會覆寫預設的 GraphQL 名稱。 適用於：

- 重新命名難懂的資料庫名稱（例如 `tbl_cust_seg` → `customer_segments`）
- 避免在 API 層出現縮寫
- 建立乾淨、貼近網域的詞彙

### 描述 {: #descriptions }

表與欄位的描述會納入產生的 GraphQL SDL。 它們會出現在 GraphiQL 的文件探索工具與自省查詢中。可在組態 YAML 或透過管理員 UI 設定。

### Path（計算式 JSON 擷取） {: #path-computed-json-extraction }

欄位可使用點記法 `path` 從 JSON/JSONB 來源欄位擷取值。 這對 Kafka 訊息、MongoDB 文件或 PostgreSQL JSONB 欄位中的半結構化數據很有用。

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

path 格式為 `source_column.key1.key2...`。編譯器會在 SQL 中產生 `json_extract_scalar(source_column, '$.key1.key2')`。

**路由影響：** path 欄位使用 PostgreSQL JSON 運算子（`->>`），直接 PG 路由原生支援。 對於非 PostgreSQL 數據來源（MySQL、SQL Server 等），含 path 欄位的查詢會自動經由聯邦引擎路由。 變更操作不受影響，因為 path 欄位是唯讀的計算欄位。

### 遮罩類型 {: #masking-types }

| 類型 | 欄位 | 描述 |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE（僅限字串欄位） |
| `constant` | `value` | 字面值取代（NULL、0、MAX、MIN、自訂） |
| `truncate` | `precision` | DATE_TRUNC（僅限日期／時間戳記欄位） |

## 關係 {: #relationships }

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

### 自動具體化 {: #auto-materialization }

在關係上設定 `materialize: true`，即可為跨數據來源的 JOIN 自動產生具體化檢視。 這藉由預先計算 JOIN 結果來避免昂貴的聯邦查詢。

- 只有跨數據來源的關係會產生 MV（同一數據來源的 JOIN 本來就很快）
- 在由聯結資料表支撐的關係上，MV 涵蓋的是兩跳走訪——來源端跳、聯結跳、判別欄，以及聯結資料表自身的欄位作為邊屬性。聯結資料表算作一條腿，因此只要三張資料表中任何一張位於不同的數據來源，這條邊就是跨數據來源的
- MV 起初是過時的，由背景重新整理迴圈填入
- 對任一方來源表的變更操作會將 MV 標記為過時以便重新整理
- `refresh_interval` 預設為 300 秒（5 分鐘）

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

具有 `parent_role_id` 的角色會從父角色繼承能力、網域存取權、欄位與物件授權，以及 RLS 規則，其中子角色本身對某資料表的 RLS 規則優先。 繼承鏈在啟動時被攤平。

### 能力 {: #capabilities }

| 能力 | 描述 |
| ----------- | ------------- |
| `source_registration` | 註冊數據來源 |
| `table_registration` | 註冊表 |
| `relationship_registration` | 定義關係 |
| `security_config` | 組態 RLS、遮罩 |
| `query_development` | 執行查詢 |
| `full_results` | 略過抽樣限制 |
| `admin` | 所有能力 |

## RLS 規則 {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

## 具體化檢視 {: #materialized-views }

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

## 檢視（受治理的計算式數據集） {: #views-governed-computed-datasets }

檢視是以 SQL 定義的計算式數據集，具備完整的欄位級治理。 它們是在模型加入彙總、轉換與衍生指標的受治理機制。

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

### 具體化與即時 {: #materialized-vs-live }

- **`materialize: true`**：Provisa 透過 CTAS 建立一個表，並依排程重新整理。 查詢較快，但數據最多可能過時 `refresh_interval` 秒。
- **`materialize: false`**：Provisa 建立一個聯邦檢視。 查詢一律回傳即時數據，但複雜彙總可能較慢。

檢視與資料表經過同樣的治理管道：RLS、遮罩、抽樣及基於角色的可見性。這樣，任何新語義都無法在沒有數據管家監督的情況下進入平台。

### 帶歷史的檢視（時間旅行） {: #views-with-history-time-travel }

具體化檢視可以保存其資料列的每一個版本，並按過去某個時刻讀取。 它宣告如何保存這些版本，以及版本屬於哪個鍵：

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
| 欄位 | 必填 | 說明 |
| ------- | ---------- | ------------- |
| `bitemporal_mode` | 是 | `snapshot` 在每次重新整理時附加整個結果；`delta` 只附加有變化的部分 |
| `bitemporal_key` | 是 | 版本所屬的欄位 |
| `bitemporal_source_time` | 否 | 保存每個資料列在數據來源中發生變化的時刻的欄位：時間旅行據以讀取的時鐘 |

**選擇時鐘。** 設定 `bitemporal_source_time` 時，版本按數據來源自身的時間排序及讀取：「截至上週二」指的是數據來源中的上週二。不設定時，版本按 Provisa 落地每次變更的時間排序及讀取，這跟隨重新整理節奏，而不是資料列在數據來源中發生變化的時刻。兩種選擇都成立。載入時間適合重新整理頻繁的情況、數據來源時間無關重要的數據，以及本來就關心載入時間的讀取，例如稽核 Provisa 在甚麼時候持有甚麼數據。

一個版本由其本身的時刻起生效，直至下一個版本的時刻。刪除沒有數據來源的變更時間，在 Provisa 記錄它時生效。

**讀取。** 一般讀取傳回每個鍵目前生效的版本。標頭 `X-Provisa-As-Of` 按某一時刻或某一範圍讀取，範圍以 ISO 8601 區間 `start/end` 表示：

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```
時刻傳回每個鍵在當時生效的版本。範圍傳回在其中任何時點生效的每個版本，每個版本一列。

**按記錄時間。** 之後記錄的更正會改變檢視對過去的描述：一旦更正被記錄，9 月 30 日生效的版本就是更正後的版本。若要按 Provisa 在某個時間點記錄的樣子讀取檢視（早於之後的任何更正），請加上 `X-Provisa-As-Recorded`，即一個 ISO 8601 時間：

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

在該時間之後記錄的所有版本都會被排除，然後 as-of 選出在其自身時間生效的版本。只使用 `X-Provisa-As-Recorded` 時，讀取傳回按當時記錄各鍵生效的版本。它只接受一個時間點，不接受範圍；範圍或格式錯誤的值會以 HTTP 400（`data.invalid_as_recorded`）被拒絕。

這兩個時間亦可以不經請求標頭傳給 Provisa。as-of 時間是 `resolve` 的 `as_of` 參數（GraphQL、gRPC、MCP）、REST 的 `as_of` 查詢參數以及 Arrow Flight 票證的 `as_of` 選項；傳給 `resolve` 的範圍會傳回資料列的 `versions`，而不是它的 `row`。as-recorded 時間是 `resolve` 的 `as_recorded` 參數、REST 的 `as_recorded` 查詢參數以及 Arrow Flight 票證的 `as_recorded` 選項。每條語句的稽核記錄都會保存它讀取時使用的兩個時間，`as_of` 及 `as_recorded`。權限、遮罩及資料列規則與任何讀取一樣適用。格式錯誤的 as-of 值，或結束時間不晚於開始時間的範圍，會以 HTTP 400（`data.invalid_as_of`）被拒絕。

每個帶歷史的檢視還會傳回三個欄位，其授權方式與其數據來源時間欄位相同（沒有時則與第一個鍵欄位相同）：

| 欄位 | 含義 |
| ------- | ------- |
| `source_from` | 此版本何時生效：其數據來源時間，沒有時為載入時間 |
| `source_to` | 下一個版本何時生效；此版本仍然生效時為空 |
| `recorded_at` | Provisa 何時記錄此版本 |

**來自變更數據擷取的數據來源時間。** 由變更數據擷取載入的資料表以 `source_committed_at` 攜帶其提交時間，讓帶歷史的檢視可以把它指定為數據來源時間：

- Debezium：每個變更事件攜帶的資料庫提交時間（`source.ts_ms`）。由副本的完整建構複製過來的資料列沒有變更事件，因此其 `source_committed_at` 為空，直至該資料列再次變化，除非數據來源是啟用了提交時間戳記的 PostgreSQL（下一項）。
- 變更訊號為 `native` 的 PostgreSQL：PostgreSQL 為每個資料列保存的提交時間（`pg_xact_commit_timestamp(xmin)`），由數據來源本身的驅動程式在建構副本時讀取。PostgreSQL 只在 `track_commit_timestamp = on` 時保存它。啟用它便可在原生訊號下取得數據來源時間；沒有它時 `source_committed_at` 為空。開啟或關閉該設定會重建副本。

沒有 Debezium 封裝的 Kafka 主題不攜帶提交時間。

### 僅供查詢的檢視 {: #query-only-views }

`materialize: true` 與 `materialize: false` 的檢視都以僅供查詢的方式公開其 GraphQL 類型。以 `view_sql` 為底的關聯不會產生 insert、upsert、update 或 delete 變更操作。

## 快取 {: #cache }

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### 快取階層 {: #cache-hierarchy }

TTL 解析順序（最明確者勝出）：**表** > **數據來源** > **全域預設**。 採用第一個非 null 的值。

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

在數據來源上設定 `cache_enabled: false` 會停用該數據來源所有表的快取，不論表層級的 TTL 為何。 快取鍵一律包含 `role_id` 加 RLS 內容值，以達成安全分區。

### 按請求選擇啟用 {: #per-request-opt-in }

對於沒有要求回應快取的請求，回應快取處於關閉狀態。 請求以下列方式選擇啟用：

| 介面 | 啟用方式 |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | 在操作上使用 `@cached`，可選 `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | 在語句中加入註解行 `-- @provisa cache=true` 或 `-- @provisa cache_ttl=60` |
| Cypher（`/data/cypher`、Neo4j Query API、Bolt、Arrow Flight） | 在查詢中加入註解行 `// @provisa cache=true` 或 `// @provisa cache_ttl=60` |
| gRPC（以及 HTTP gRPC 代理） | 呼叫元數據（代理上為請求標頭）`x-provisa-cache: true` 或 `x-provisa-cache-ttl: 60` |
| REST、JSON:API，以及作為文字中提示的替代方式：HTTP 上的 GraphQL、`/data/cypher`、Neo4j Query API | 請求標頭 `Cache-Control: max-age=60`（或 `max-stale=60`），或 `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

非整數的 `ttl` 會令請求失敗。在文字中和請求標頭中，或在兩個請求標頭中兩次聲明選擇啟用且值不同的請求，會以 `400` 失敗；不會偏向任何一個。`Cache-Control: no-cache` 或 `no-store` 表示不快取，與沒有請求標頭相同。

沒有該提示時，請求既不讀取也不寫入快取。有提示時，仍由上述設定決定：只有當讀取的每個數據來源都設定了 `cache_enabled: true`，而且每個表解析出的 TTL 大於 0，結果才會被快取。如請求提供了 `ttl`，條目的存活時間就是該值；否則為所讀取各表中最短的解析 TTL。

該提示只是以接受較舊的數據來換取速度。它絕不會令讀取比營運方設定所容許的更新：落地與副本的新鮮度（`cache_ttl`、`change_signal`）、`row_materialize`、`load_protected` 快照以及新鮮度檢查，對帶提示與不帶提示的請求完全一樣適用。請參閱[新鮮度與負載：由誰決定](#freshness-and-load-who-decides)。

`@noCache` 和 `-- @provisa no_cache=true` 已不再存在：快取預設關閉，因此無需繞過。

寫入會令寫入方機構為其寫入的表所持有的所有快取條目失效；其他機構對同一表的條目不受影響。如失效操作失敗，請求會失敗，而不會留下過期條目。

## 新鮮度與負載：由誰決定 {: #freshness-and-load-who-decides }

有三方共同決定結果有多新，以及一次查詢為其後端系統帶來多大負載。

- **上游資料來源。** 理想情況下它自行管理背壓：連線數上限、語句逾時、唯讀副本。
- **營運方。** 保護平台（即 Provisa 本身）免受背壓。上游無法自我保護時，營運方亦保護上游。相關設定位於資料來源和表上：`load_protected`、`replicate`、資料來源的 `federation_hints`、大結果重新導向門檻、sink 寫入的 Kafka 叢集，以及每張表登記的 watermark。
- **最終用戶。** 逐個請求地在速度和新鮮度之間取捨。

營運方的設定是下限。請求可以在下限之上移動，走向較舊的資料或較低的負載；選擇啟用回應快取就是常見的例子。請求永遠不能低於下限。會低於下限的請求提示會被拒絕，錯誤訊息會指明營運方的設定，既不會被悄悄套用，也不會被悄悄捨棄。

| 請求輸入 | 允許做甚麼 | 何時被拒絕 |
| --- | --- | --- |
| `@route(engine: DIRECT)`、`-- @provisa route=direct` | 為單一資料來源選擇直連驅動 | 語句讀取的某張表為 `load_protected` 或 `replicate: 0` |
| `@join`、`@reorder`、`@broadcastSize`、`/*+ ... */` | 設定資料來源 `federation_hints` 未設定的引擎工作階段屬性 | 修改了營運方已設定的屬性 |
| `X-Provisa-Redirect-Threshold`、`@redirect(threshold:)` | 更早地重新導向結果 | 高於營運方的門檻 |
| `@sink(broker:)`、`X-Provisa-Sink` 中的 broker | 重複營運方的 broker | 指定了其他 broker，或未設定 `KAFKA_BOOTSTRAP_SERVERS` |
| 訂閱中的 `@watermark` | 重複該表登記的 watermark | 指定了其他欄位，或該表沒有 watermark |
| `@cached`、`-- @provisa cache=true` | 傳回較舊的快取結果 | 從不；快取項目不會比寫入它的那次讀取更新 |

拒絕時傳回 HTTP 403 及代碼 `query.operator_floor`，pgwire 上傳回 SQLSTATE `42501`，Flight 和 gRPC 上傳回 `PERMISSION_DENIED`。

`load_protected` 或 `replicate: 0` 的表，在任何傳輸上都不會被查詢即時讀取。查詢讀取的是它的複本。只有複製及其刷新會讀取資料來源：當 `cache_ttl` 或新鮮度檢查要求時才刷新；兩者皆無時，複本只建構一次，之後僅透過變更串流或排程器刷新。 對於帶有 `off_peak_window`（在表或其資料來源上，按 `off_peak_tz` 解讀）的此類表，刷新只在視窗開啟時執行：在視窗之外到期或被請求的刷新會等到視窗下一次開啟，沒有 `cache_ttl` 的視窗每次開啟刷新一次。首次建置從不被延後，因為對該表的讀取在等待它。

### `replicate` 設定 {: #the-replicate-setting }

`replicate` 是表上的一個整數。未設定值的表沿用其資料來源的值；兩者皆未設定時，適用全域門檻。複本是資料來源表的一份拷貝，保存在引擎的儲存區中，並按表的時效性設定刷新；它不是持續的變更串流。

| 值 | 名稱 | 含義 |
| --- | --- | --- |
| 未設定 | 預設 | 適用全域門檻 |
| `-1` | 從不 | 凡存在即時路徑之處均即時讀取。僅在引擎無法就地讀取資料來源時才複製 |
| 大於 0 的 `N` | Hot-N | 即時讀取，直至該表在每個間隔內被 N 條受治理語句讀取，之後由其複本提供服務 |
| `0` | 一律 | 讀取來自複本 |

只有「一律」是保證。「從不」和 Hot 值都是盡力而為。`load_protected` 與 `replicate: -1` 同時出現時，會在設定載入和儲存時被拒絕。`replicate: 0` 的資料來源在任何引擎上都沒有即時附加，因此它的每張表都由其複本提供服務。

下限按語句所讀取的表來判斷。不讀取任何由複本提供服務的表的語句，即使其資料來源還有其他被複製的表，也保留直連路由。

複本按其宣告的主索引鍵，以及其宣告的關係所連接的欄位建立索引，索引在複本落地後建構。這些索引在哪些情況下加速聯結、複本應存放何處以及如何選擇其新鮮度，請參閱 [Replicas](replicas.md)。

### Hot 複製 {: #hot-replication }

`replicate` 未設定或為大於 0 的 `N` 的表，在繁忙之前一直即時讀取。

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| 設定 | 環境變數 | 預設值 |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

這三項都是營運方設定：管理介面在 Hot Tables 下顯示它們，儲存的值在下一次評估時生效。

表的計數方式：

- 一條受治理語句對其讀取的每張表只計一次，無論讀取多少次。
- 由回應快取應答的語句不計數。它沒有為資料來源帶來負載。
- 被治理拒絕的語句不計數。

表的計數達到其門檻（其自身的 `N`，否則是其資料來源的，否則是 `hot_threshold`）時被提升，計數降至門檻的一半以下時被降級。提升會請求建構複本；在建構完成之前，該表仍即時讀取，之後從複本讀取。降級會立即讓讀取回到資料來源。

表在下列情況下不會被提升：列數超過 `hot_max_rows`；其變更訊號為 `ttl` 或 `ttl_probe`，而它和它的資料來源都沒有宣告 `cache_ttl`；或 Redis 熱層已經在管理它。管理介面中該表的摘要會說明是哪一種情況。

計數保存在 Redis 中。使用共用 Redis（`cache.redis_url`）時，計數涵蓋部署的所有行程。使用內嵌 Redis 時，計數按行程各自統計，因此只有在單一工作行程處理請求時才會提升；有多個工作行程而沒有共用 Redis 時不會提升任何表，摘要中會說明這一點。

## 驗證 {: #authentication }

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

### 驗證提供者類型 {: #auth-provider-types }

| 提供者 | 使用情境 | 權杖驗證 |
| ---------- | ---------- | ----------------- |
| `simple` | 本機開發／測試。使用者定義於 YAML。 | 以 `PROVISA_JWT_SECRET` 簽署的 JWT |
| `firebase` | Firebase Authentication（所有方式）。 | `firebase-admin` SDK `verify_id_token()` |
| `keycloak` | Keycloak OIDC。對應租用戶與用戶端角色。 | 以 JWKS 為基礎的 JWT 驗證 |
| `oauth` | 通用 OIDC（Okta、Azure AD、Auth0、PingFederate）。 | 來自探索 URL 的 JWKS |
| `basic` | 自足式部署。帳戶存放於 Provisa 自己的儲存區。 | bcrypt 密碼，或 pgwire 上的 SCRAM-SHA-256 |

超級使用者認證（`superuser` 區塊）可搭配任何提供者使用，並一律解析為具備所有能力的 admin 角色。 用於在外部驗證組態完成前的初始設定。

### SCRAM-SHA-256（`auth.scram`） {: #scram-sha-256-authscram }

```yaml
auth:
  provider: basic
  scram: true
```

令 pgwire 通告帶 `SCRAM-SHA-256` 的 SASL，使密碼以證明方式驗證，從不以明文傳送。它只適用於 `basic` 提供者，因為沒有其他提供者保存 SCRAM 所需的 RFC 5802 驗證資料，並且不提供通道繫結。

驗證子無法從既有的 bcrypt 雜湊衍生。每當密碼以純文字經過時就會寫入一個，因此每位使用者的第一次 SCRAM 連線發生在其下一次註冊、登入、變更密碼或管理員重設之後。在那之前，該使用者的連線會回退為經 TLS 的明文交換；線路上不會顯示誰已遷移。

### 登入節流（`auth.login_throttle`） {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

預設以所示值啟用；此區塊只用來調整。 計數器位於認證驗證層，因此經由 HTTP、pgwire 與 Bolt 的失敗會累加到同一個主體上，而鎖定在每個介面上都成立。它是每個處理程序各自計算：多個 API 工作處理程序各自允許至多 `max_attempts` 次。

### 個人存取權杖 {: #personal-access-tokens }

PAT 不需要設定區塊。它們一律被接受，其儲存會與控制平面的其餘結構描述一併建立。可設定的是使用者簽發時可申請的有效期：1 至 366 天，或者不設期限，即永不過期的權杖。參見[安全模型](security.md#personal-access-tokens)。

### 雙向 TLS {: #mutual-tls }

用戶端憑證驗證經環境變數設定，與它所擴充的 TLS 憑證設定放在一起，不寫在 `provisa.yaml` 中。

| 變數 | 預設 | 意義 |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 未設定 | 獲准簽署用戶端憑證的 CA 的 PEM 套件。設定它即開啟用戶端憑證驗證 |
| `PROVISA_MTLS_MODE` | 設定 CA 後為 `required` | `required` 或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | 要求憑證的一般名稱等於該連線驗證所用的使用者名稱 |

每一項都可以按協定覆寫，命名方式與 TLS 設定相同。設定了模式卻沒有 CA，或模式既不是 `required` 亦不是 `optional`，都會阻止啟動，所以伺服器永遠不會在營運者以為已驗證、實際未驗證的情況下提供連線。

### 透過 TLS 指向某個組織 {: #addressing-an-org-over-tls }

無需組態。在多組織部署上，pgwire 與 Bolt 從用戶端撥接的主機名稱讀取組織，該名稱由 TLS ClientHello 攜帶，正如 HTTP 從 `Host` 標頭讀取一樣。 連線到 `acme.provisa.dev` 的用戶端即請求組織 `acme`；除非通過驗證的主體是其成員，否則請求會被拒絕。以 IP 位址連線則不請求任何組織，這正是單組織部署上的每一條連線。

### 完整驗證組態範例（已註解） {: #full-auth-config-example-commented-out }

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

## Upsert 變更操作 {: #upsert-mutations }

對於有主鍵的資料表，Provisa 會產生 `upsert_<table>` 變更欄位。它們編譯為目標方言的 upsert：PostgreSQL 上是 `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`，MySQL 上是 `ON DUPLICATE KEY UPDATE`。

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

衝突欄位由 PK 中繼資料衍生。 所有欄位可見性與寫入權限規則均適用。

## Distinct On

`distinct_on` 引數會針對指定欄位的每個相異值選出第一列。 可用於根查詢欄位。

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

在 PostgreSQL 中編譯成 `SELECT DISTINCT ON (region) ...`。 對非 PG 方言則使用視窗函式的替代做法。

## 寫入授權的預設 {: #write-grant-presets }

寫入授權在插入和更新時自行設定欄位。預設在授權的 `presets` 清單中宣告，按角色和操作區分。

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

| 來源 | 行為 |
| -------- | ---------- |
| `session` | 注入請求中指定工作階段變數（`name`）的值 |
| `now` | 注入寫入發生的時間 |
| `literal` | 注入一個常數（`value`） |

預設在編譯期間、產生 SQL 之前加入陳述式。陳述式不得提供預設欄位，預設欄位也不會出現在變更的輸入類型中。

## 繼承角色 {: #inherited-roles }

角色可透過 `parent_role_id` 從一個父角色繼承。 繼承鏈在啟動時被攤平。 子角色持有其祖先的能力與網域存取權的聯集；授予祖先的欄位、指標、函數或 webhook 亦授予子角色；祖先的 RLS 規則按資料表套用於子角色，最近的角色優先，子角色本身對某資料表的規則會取代父角色的規則。

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

支援多層繼承。 子角色明確設定的 capabilities 與 domain_access 會與父角色的合併。 父角色必須是已存在的角色，不能是角色本身，也不能構成循環；每一種情況都會在儲存時被拒絕。

## 排程觸發器 {: #scheduled-triggers }

依排程呼叫 webhook URL 的 cron 型觸發器。 使用 APScheduler。

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

排程工作透過管理員 UI（啟用／停用切換）或 `toggle_scheduled_task` 管理員變更操作管理。

觸發器可以在已登記的表上觸發，而不是按排程：`on: {table, event}` 以 `source_id.schema.table` 形式指定表，並指定事件 `change`、`error` 或 `quarantine`。`command` 觸發器以其 `role` 執行一個已登記的命令，經由該命令自身的受管治調度，並把觸發它的內容作為參數傳入。變更傳入 `table`，以及在變更信號攜帶時（Kafka、Debezium）作為 `changes` 的已落地批次。`error` 或 `quarantine` 事件傳入 `table`、`event`、`reason` 和 `error`。若觸發器的命令宣告了觸發器不提供的參數，儲存時會被拒絕。變更在其變更信號報告的每一批變更時觸發一次，或在經由 Provisa 寫入完成時觸發；由副本提供的表在建置落地了與上次不同的列時觸發。命令自身的寫入也是一次變更，因此觸發器可以鏈式觸發。每次執行都會記錄時間、結果（`succeeded`、`refused` 或 `failed`）、錯誤以及觸發來源，`scheduledTasks` 管理查詢會列出它。只有 prod 的觸發器會觸發，且不會在任何來源資料庫中安裝任何東西。

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

OrderBy 使用 `{column: direction}` 格式，方向為六值列舉：

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

透過巢狀物件支援關係排序：

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## 可觀測性 {: #observability }

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

### 遙測篩選器 {: #telemetry-filters }

Provisa 有兩條互相獨立的 OTLP 匯出路徑：您的內部 collector，以及可選的 Provisa 支援端點。每條路徑都有自己的篩選器。篩選器在 span 離開程序之前執行，原始的 span 物件永遠不會被修改。

**`telemetry_filter`** — 控制哪些內容到達你的內部收集器。

| 鍵 | 類型 | 預設 | 描述 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | 將 `db.statement` 中的字串與數值字面值取代為 `?` |
| `redact_attributes` | list[str] | `[]` | 從每個跨距中完全移除的屬性鍵 |

**`support_telemetry_filter`** — 控制哪些內容到達 Provisa 支援端點。這條路徑上的 SQL 字面值塗黑預設為 `true`，因為查詢數據屬於你。

| 鍵 | 類型 | 預設 | 描述 |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | 將 `db.statement` 中的字串與數值字面值取代為 `?` |
| `redact_attributes` | list[str] | `[]` | 從每個跨距中完全移除的屬性鍵 |

塗黑後的 `db.statement` 範例 — 在 `redact_sql_literals: true` 之下，這個跨距屬性：

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

會變成：

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### 支援端點 {: #support-endpoint }

`support_endpoint`（或環境變數 `PROVISA_SUPPORT_OTLP_ENDPOINT`）會把遙測轉發給 Provisa 支援團隊作診斷。未設定時，任何數據都不會經由這條路徑離開您的基礎設施。支援篩選器獨立於內部篩選器運作，所以您可以在兩路匯出中都隱去 SQL 字面值，同時仍與支援團隊分享 span 耗時及錯誤數據。

### 端點協定偵測 {: #endpoint-protocol-detection }

Provisa 從端點 URL 的配置選擇 OTLP/HTTP 或 OTLP/gRPC。 以 `http://` 或 `https://` 開頭的 URL 使用 OTLP/HTTP，並自動附加 `/v1/traces`、`/v1/metrics` 與 `/v1/logs`。 其他任何配置皆使用 OTLP/gRPC 並帶 `insecure=True`。

## 聯邦引擎 {: #federation-engine }

設定聯邦引擎是可選的。預設是 `duckdb`：零設定、程序內執行、毋須外部服務。需要 MPP 規模，或想重用現有數據倉庫時，再選擇其他引擎。

優先順序：`PROVISA_ENGINE` 環境變數 → 持久化的管理員 UI `federation_engine` 組態欄位 → `duckdb`。變更於服務重新啟動時生效。

### 引擎總覽 {: #engine-overview }

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

### 引擎參考 {: #engine-reference }

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

行程內；無外部服務。預設引擎。`PROVISA_DATA_DIR` 控制內嵌儲存區的位置（預設 `~/.provisa`）。

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

具體化儲存預設為 `~/.provisa/materialize.duckdb`。它是唯一預設儲存不是 PostgreSQL 的引擎。

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

#### 關聯式資料庫引擎（mysql、mariadb、oracle、mssql、db2、redshift、greenplum、cockroachdb、yugabytedb、opengauss、tidb、singlestore、vertica、exasol、teradata、saphana、sapase、sqlanywhere、monetdb、firebird）與 `sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

每個可透過網路定址的關聯式資料庫各有一個鍵，全都跑在同一套僅落地的執行環境上（不聯邦到外部數據來源）：每個數據來源都落地到儲存區，並在那裡被查詢。鍵用來選擇資料庫；`PROVISA_ENGINE_URL` 攜帶其方言所接受的 DSN。`sqlalchemy` 是沒有專屬鍵的資料庫的萬用選項。不提供檔案內嵌式儲存區（SQLite、Access）— 伺服器必須可透過網路連達。

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

具體化儲存區預設為 `TENANT_DATABASE_URL`。

### 具體化儲存區 {: #materialization-store }

當某個數據來源無法即時附加（所選引擎沒有對應的 ATTACH 連接器）時，它會落地到該引擎的具體化儲存區。解析順序：明確的 `PROVISA_MATERIALIZE_URL` → 引擎宣告的預設值 → 直接報錯（不會悄悄退回）。

DuckDB 宣告其內嵌檔案（`~/.provisa/materialize.duckdb`）為預設值。其他所有引擎皆預設為 `TENANT_DATABASE_URL`（PostgreSQL）。任一引擎都可用 `PROVISA_MATERIALIZE_URL` 覆寫。

儲存區決定複本的索引對聯結起什麼作用：PostgreSQL 儲存區會為複本建立索引，在其中執行 SQL 的引擎會讀取這些索引；而 DuckDB 則在每條語句的複本拷貝之上聯結其內嵌檔案中的複本。請參閱 [Replicas](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it)。

### 各數據來源的聯邦提示 {: #per-source-federation-hints }

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

對於 Google Cloud 數據來源，將 `GOOGLE_APPLICATION_CREDENTIALS` 設定為服務帳戶金鑰檔案的路徑。對於 Fabric 及 Synapse，用 `az login`（開發）或受控識別（生產）登入；引擎經 `azure-identity` 的 `DefaultAzureCredential` 讀取憑證。

## 環境變數 {: #environment-variables }

| 變數 | 預設 | 描述 |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | 組態檔路徑 |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | 控制平面儲存區 URI（SQLAlchemy async）；內嵌桌面儲存區接受 `sqlite+pysqlite://…` / `duckdb://…` |
| `PLATFORM_DATABASE_URL` | — | 平台登錄 URI（租用戶目錄、引擎登錄）；啟動時必要，無後備值 |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` 改用內嵌的 fakeredis 而非 Redis 伺服器 — 不需 Docker |
| `PG_HOST` | `localhost` | PostgreSQL 主機 |
| `PG_PORT` | `5432` | PostgreSQL 連接埠 |
| `PG_DATABASE` | `provisa` | PostgreSQL 資料庫 |
| `PG_USER` | `provisa` | PostgreSQL 使用者 |
| `PG_PASSWORD` | `provisa` | PostgreSQL 密碼 |
| `PROVISA_ENGINE` | `duckdb` | 聯邦引擎鍵 |
| `PROVISA_ENGINE_URL` | — | URL 驅動引擎的連線 URL（Snowflake、Databricks、ClickHouse Server、BigQuery、SQLAlchemy） |
| `PROVISA_MATERIALIZE_URL` | — | 覆寫具體化儲存區 DSN（預設為引擎宣告的預設值） |
| `PROVISA_DATA_DIR` | `~/.provisa` | 內嵌 DuckDB 儲存區的數據目錄 |
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
| `PROVISA_RETRY_BUDGET_SECS` | `30` | 第一層讀取重試預算（秒）；指數輪詢退避搭配完全抖動 |
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
| `PROVISA_MTLS_CLIENT_CA` | — | 獲准簽署用戶端憑證的 CA 的 PEM 套件；設定它會在 pgwire、Bolt、gRPC 與 Flight 上開啟用戶端憑證驗證 |
| `PROVISA_MTLS_MODE` | 設定 CA 後為 `required` | `required` 或 `optional`；其他任何值都會拒絕啟動 |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | 要求憑證的一般名稱等於進行驗證的使用者名稱 |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | 以逗號分隔、獲准從瀏覽器開啟 Bolt WebSocket 的站台；未設定則拒絕每一個瀏覽器來源 |
| `PROVISA_EXTRAS` | `firebase,vector` | 烘焙進應用程式映像的 pyproject extras；`scripts/provisa` 由 `~/.provisa/config.yaml` 中的 `dq_checker` 推導 |
| `PROVISA_DQ_CHECKER` | `none` | 僅供安裝程式使用：`none`/`soda`/`gx`，由 `first-launch.sh` 在非互動模式下讀取，並以 `dq_checker` 寫入 `config.yaml` |
| `ANTHROPIC_API_KEY` | — | Claude API 金鑰（探索） |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | 覆寫 `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | 覆寫 `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | 覆寫 `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | 覆寫 `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | 批次跨距處理器的排清延遲 |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | 覆寫 `observability.support_endpoint` |
