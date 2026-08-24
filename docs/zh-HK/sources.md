# 數據來源類型
## 執行模型
每個查詢最終都經聯邦引擎執行，由它提供跨所有數據來源的聯邦能力。數據來源依其連通性分為三類。 [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| 類別 | 有直接驅動程式 | 有聯邦連接器 | 例子 |
| --- | --- | --- | --- |
| **可直接執行** | 是 | 是 | PostgreSQL、MySQL、MariaDB、SingleStore、SQL Server、Oracle、DuckDB |
| **僅聯邦** | 否 | 是 | Redshift、Druid、Exasol、Hive、Iceberg、Delta Lake、Hive（S3 支援） |
| **直接讀取（副本）** | 是 | 是 | Snowflake、Databricks、ClickHouse——驅動程式讀取數據並落地一份副本；查詢在啟用中的引擎裡針對該副本執行 |
| **具體化 → 聯邦** | 否 | 否 | REST/OpenAPI、遠端 GraphQL、gRPC、Neo4j Cypher、SPARQL、WebSocket、RSS、CSV、SQLite、Parquet、Ingest（推送接收器）、GovData、SharePoint、Splunk |

**可直接執行**的數據來源經其原生驅動程式執行單一來源查詢（低於 100 毫秒），繞過聯邦引擎（REQ-027、REQ-229）。它們保有完整的連接器支援，並在與其他數據來源聯結時參與聯邦（REQ-028）。

**僅聯邦**的數據來源一律經聯邦層查詢。沒有直接驅動程式存在（REQ-229）。

**直接讀取（副本）**的數據來源具備一個 DirectDriver，會原生地（在可行處以 Arrow 原生方式）自倉庫讀取，把一份副本落地到啟用中引擎的具體化儲存區，之後查詢便針對該副本執行。見 [作為具名數據來源的倉庫](#warehouses-as-named-sources)。

**具體化**的數據來源沒有聯邦連接器。Provisa 取得其數據（於啟動時或查詢時），並以 Parquet 快取於 S3 或 PostgreSQL 中，使聯邦引擎能就跨來源查詢觸及它（REQ-309）。

---

## 所有數據來源
Provisa 註冊了 **53** 種數據來源類型。下方表格涵蓋全部 53 種；索引即為計數。 [tool-verified: `provisa/core/models.py` `SourceType`]

| # | 群組 | 數據來源類型 |
| --- | --- | --- |
| 1–13 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport` |
| 14–20 | [雲端數據倉庫](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `fabric`, `synapse`, `trino` |
| 21–25 | [分析／OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 26–30 | [數據湖／開放表格式](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hudi`, `hive`, `hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`, `cassandra`, `redis` |
| 34–36 | [串流](#streaming) | `kafka`, `websocket`, `rss` |
| 37 | [推送接收器](#push-receiver) | `ingest` |
| 38–39 | [圖形與語意](#graph-semantic) | `neo4j`, `sparql` |
| 40–43 | [檔案式](#file-based) | `sqlite`, `csv`, `parquet`, `files` |
| 44–45 | [可觀測性與其他](#observability-other) | `google_sheets`, `prometheus` |
| 46–47 | [企業 SaaS](#enterprise-saas-connectors) | `sharepoint`, `splunk` |
| 48–50 | [API 數據來源](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [數據品質檢查器](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |

Provisa 支援的每一種數據來源類型的參照。「直接驅動程式」指單一來源查詢原生地針對該數據來源執行（低於 100 毫秒）（REQ-027）。「連接器名稱」是該數據來源參與多來源 JOIN 時所用的聯邦連接器（REQ-028）。 [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| 數據來源類型 | 直接驅動程式 | 連接器名稱 | 方言 | 變更操作 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | 是 |
| `mysql` | aiomysql | mysql | mysql | 是 |
| `mariadb` | aiomysql | mariadb | mysql | 是 |
| `singlestore` | — | singlestore | singlestore | 聯邦 |
| `sqlserver` | aioodbc | sqlserver | tsql | 是 |
| `oracle` | oracledb | oracle | oracle | 是 |
| `duckdb` | duckdb | memory | duckdb | 是 |
| `cockroachdb` | asyncpg（pg wire） | postgresql | postgres | 是 |
| `yugabytedb` | asyncpg（pg wire） | postgresql | postgres | 是 |
| `greenplum` | asyncpg（pg wire） | postgresql | postgres | 是 |
| `tidb` | aiomysql（mysql wire） | mysql | mysql | 是 |
| `firebird` | — | —（DuckDB 擴充） | — | 否 |
| `airport` | — | —（DuckDB 擴充） | — | 否 |

wire 相容的資料庫重用基礎 wire 的 JDBC 驅動程式、原生非同步驅動程式與方言——CockroachDB、YugabyteDB 與 Greenplum 走 PostgreSQL wire；TiDB 走 MySQL wire。它們只需要登錄項目，毋須新的連接器程式碼。 [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird`（Firebird 3/4/5）與 `airport`（Arrow Flight 伺服器）是已註冊的數據來源類型，當 DuckDB 為啟用中的引擎時，經 DuckDB 社群擴充就地觸及——沒有直接驅動程式，也沒有聯邦連接器。 [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### 雲端數據倉庫 {#cloud-data-warehouses}
[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| 數據來源類型 | 直接驅動程式 | 連接器名稱 | 方言 | 變更操作 | 備註 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | 聯邦 | 經 snowflake-connector-python 讀取；落地副本；`federation_hints` 中的 `account`／`warehouse`／`role`（REQ-988） |
| `bigquery` | — | bigquery | bigquery | 聯邦 | 無 DirectDriver；經聯邦引擎或 BigQuery 引擎 ATTACH 觸及 |
| `databricks` | DatabricksDriver | delta_lake | databricks | 聯邦 | 經 databricks-sql-connector 讀取（Cloud Fetch、Arrow）；落地副本；`federation_hints` 中必填 `http_path`（REQ-987） |
| `redshift` | — | redshift | redshift | 聯邦 | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | 聯邦 | Microsoft Fabric Warehouse；T-SQL over TDS、Azure AD 驗證；落地副本（REQ-995） |
| `synapse` | MssqlWarehouseDriver | — | tsql | 聯邦 | Azure Synapse SQL；T-SQL over TDS、Azure AD 驗證；落地副本（REQ-995） |
| `trino` | SQLAlchemyDriver | — | — | 聯邦 | 經 SQLAlchemy trino 方言讀取遠端 Trino/Presto 協調器；在任何引擎上落地副本（REQ-994） |

### 分析／OLAP {#analytics-olap}
[tool-verified: `executor/drivers/clickhouse.py`]

| 數據來源類型 | 直接驅動程式 | 連接器名稱 | 方言 | 變更操作 | 備註 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | 聯邦 | 經 clickhouse-connect（HTTP）讀取；TLS 時在 `federation_hints` 中設 `secure: "true"`（REQ-986） |
| `druid` | — | druid | druid | 否 | — |
| `exasol` | — | exasol | exasol | 否 | — |
| `elasticsearch` | — | elasticsearch | — | 否 | 連接器屬性來自該類型的映射 DSL [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | 否 | Trino `pinot` 連接器；`pinot.controller-urls` = Pinot 控制器的 host:port [tool-verified: `trino_connectors.py:199`] |

### 數據湖／開放表格式 {#data-lake-open-table-formats}
這些數據來源類型僅供聯邦使用——沒有直接驅動程式，沒有方言。 [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| 數據來源類型 | 連接器名稱 | 時間旅行 | 備註 |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | 是（`as_of` 引數，REQ-372） | — |
| `delta_lake` | delta_lake | 是（`as_of` 引數，REQ-372） | — |
| `hive` | hive | 否 | — |
| `hudi` | —（ClickHouse `Hudi` 引擎，零複製——REQ-1178） | 否 | 無聯邦連接器；當 ClickHouse 為啟用中的引擎時就地觸及 |
| `hive_s3` | hive | 否 | S3 支援的 Hive |

### NoSQL

`mongodb`、`cassandra` 與 `redis` 有 Trino 連接器（`redis` 自該類型的映射 DSL 建構其屬性）。 [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| 數據來源類型 | 連接器名稱 | 變更操作 |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | 否 |
| `cassandra` | cassandra | 否 |
| `redis` | redis | 否 |

### 串流 {#streaming}
| 數據來源類型 | 機制 | 變更操作 |
| ------------ | ----------- | ----------- |
| `kafka` | 聯邦 Kafka 連接器；結構描述經 Confluent Schema Registry（Avro、Protobuf、JSON Schema）、手動定義或樣本推斷取得（REQ-147、REQ-150） | 僅接收端（REQ-176） |
| `websocket` | 外部 WebSocket 饋送——連線、訂閱、接收事件；結果經具體化（REQ-338） | 否 |
| `rss` | RSS 2.0／Atom 饋送——輪詢，以 pubDate/updated 標記水位；結果經具體化（REQ-342、REQ-343） | 否 |

### 推送接收器 {#push-receiver}
| 數據來源類型 | 機制 | 變更操作 |
| ------------ | ----------- | ----------- |
| `ingest` | 外部服務 POST JSON 事件；結果經具體化（REQ-331、REQ-335） | 否 |

### 圖形與語意 {#graph-semantic}
| 數據來源類型 | 機制 | 變更操作 |
| ------------ | ----------- | ----------- |
| `neo4j` | 經 HTTP API 的 Cypher，結果快取於 PostgreSQL（REQ-295） | 否 |
| `sparql` | SPARQL 1.1 POST，結果快取於 PostgreSQL（REQ-297） | 否 |

### 檔案式 {#file-based}
兩種機制涵蓋檔案。兩者都使用 `path` 欄位而非 `host`／`port`。 [tool-verified: `provisa/core/models.py`] (REQ-553)

**單一檔案數據來源**——`sqlite`、`csv`、`parquet` 把 `path` 指向單一檔案。

| 數據來源類型 | 傳輸 | 變更操作 |
| --- | --- | --- |
| `sqlite` | 本機 | 是 |
| `csv` | 本機 | 否 |
| `parquet` | 本機、`s3://` | 否 |

私有貯體需要憑證（自環境取得的 AWS 區域與金鑰）。若 CSV 走 `s3://` 或 `http(s)://`，或要一次註冊多個檔案，請用 `files` 數據來源。 [tool-verified: `provisa/file_source/source.py`]

**`files` 數據來源**——把 `path` 指向一個 glob，遞迴爬取，並把該目錄註冊為一個由表構成的聯邦目錄。它讀取多種格式、經多種傳輸；下列集合來自檔案連接器（kenstott/calcite fork）。 [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| 格式 | 傳輸 |
| --- | --- |
| CSV、TSV、JSON、YAML、Excel（XLS/XLSX）、Parquet、Arrow，以及轉換成表的文件——HTML、Markdown、DOCX、PPTX | 本機檔案系統、HTTP(S)、`s3://`、`hdfs://`、`ftp://`／`ftps://`、`sftp://`、`iceberg://`、SharePoint（REST 與 Microsoft Graph） |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### 可觀測性與其他 {#observability-other}
`prometheus` 有 Trino 連接器（屬性自該類型的映射 DSL 建構）。`google_sheets` 是已註冊的數據來源類型，沒有 Trino 連接器，經 API 快取管線具體化。 [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| 數據來源類型 | 連接器名稱 | 變更操作 |
| ------------ | ----------------- | ----------- |
| `google_sheets` | —（經具體化） | 否 |
| `prometheus` | prometheus | 否 |

### 企業 SaaS 連接器 {#enterprise-saas-connectors}
SharePoint 與 Splunk 經 Apache Calcite 連接器（kenstott/calcite fork）註冊。兩者都沒有直接驅動程式——Provisa 啟動連接器隨附的 Calcite pgwire 伺服器（`pgwire-sharepoint`、`pgwire-splunk`），以通用 PostgreSQL 端點的身分連線，並把資料列落地到具體化儲存區供聯邦使用（REQ-954）。兩個連接器都一律啟用不分大小寫的名稱比對，以配合各產品自身不分大小寫的語意（REQ-725、REQ-730）。 [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

SharePoint 清單被列舉為結構描述，並以可查詢的表對外呈現（REQ-726、REQ-731）。兩種驗證方式：`CLIENT_CREDENTIALS`（預設）與經 PFX 憑證的憑證式驗證（REQ-727）。`mapping` 中的密鑰值在抵達連接器之前，先經密鑰引擎解析（REQ-729）。 [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| 數據來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` or `host` | `site-url` | SharePoint 網站 URL |
| `username` | `client-id` | Azure 應用程式用戶端 ID |
| `password` | `client-secret` | Azure 應用程式用戶端密鑰 |
| `database` | `tenant-id` | Azure 租用戶 UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS`（預設）或 `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | `auth_type: CERTIFICATE` 時的 PFX 路徑 |
| `mapping.certificate_password` | `certificate-password` | PFX 密碼 |

當連接器不提供 `information_schema.columns` 時，請經 `registerTable` 變更操作以明確的資料行定義（自 Microsoft Graph API 取得）註冊該表（REQ-732）。

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

Splunk 搜尋結果可作為表查詢（例如 `internal_server`）（REQ-721）。連接器 URL 來自 `base_url`，或以 `https://{host}:{port}` 建構，預設連接埠為 `8089`（REQ-722）。驗證：當 `mapping.use_token` 為 `true`（預設）時，`password` 作為 API 權杖傳遞；為 `false` 時，`username` 與 `password` 作為獨立憑證傳遞（REQ-723）。 [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| 數據來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`，否則為 `https://host:port`（連接埠預設 8089） |
| `password` | `token` or `password` | `use_token: true` 時為 token |
| `username` | `user` | 僅在 `use_token: false` 時 |
| `database` | `app` | 限定於某個 Splunk 應用程式 |
| `mapping.datamodel_filter` | `datamodel-filter` | 篩選至某個數據模型 |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | 供自簽憑證使用（REQ-724） |

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

### API 數據來源 {#api-sources}
把任何 HTTP 端點註冊為可查詢的表。 [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| API 類型 | 探索 | 資料行推斷 |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI 規格剖析（REQ-314、REQ-316） | 基本型別 → 原生，物件 → JSONB |
| `graphql_remote` | 結構描述自省（REQ-307、REQ-308） | 基本型別 → 原生，物件 → JSONB |
| `grpc_remote` | 伺服器反射（REQ-322、REQ-325） | 基本型別 → 原生，物件 → JSONB |

API 回應被取得、快取於 PostgreSQL（TTL 可設定），並以 GraphQL 類型對外呈現（REQ-309、REQ-318、REQ-327）。已快取的表與任何其他數據來源一樣參與聯邦查詢（REQ-313）。

**JSONB 規則**：以 JSONB 儲存的複合資料行（物件、陣列）不可篩選（REQ-119）。子欄位存取在 SQL 中使用 `->>` 抽取（REQ-151）。表與表之間的關係以純量 FK 資料行宣告——JSONB blob 資料行不是聯結目標。當需要對巢狀欄位篩選或聯結時，請用 JSONB 晉升把它們轉為原生純量資料行（REQ-119）。

### GovData

美國政府開放數據。存取按主題分組分割。 [tool-verified: `provisa/core/models.py` lines 543–609]

每個 `govdata` 數據來源選定一個主題。該主題決定哪些 GovData 結構描述會對外呈現。`ref` 與 `geo` 結構描述一律作為連結結構描述納入——它們不逐主題列出，但一律存在。 [tool-verified: `provisa/core/models.py` line 562–563 comment]

| 主題 | 對外呈現的結構描述 |
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
| `ALL` | 以上每一個結構描述 |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| 欄位 | 必填 | 預設 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一識別碼 |
| `subject` | 是 | — | 上述主題值之一 |
| `domain_id` | 是 | — | 此數據來源所屬的網域 |
| `description` | 否 | `""` | 供人閱讀的說明 |

### 數據品質檢查器（REQ-1443） {#data-quality-checkers-req-1443}
數據品質檢查器是一種數據來源類型，不是一個子系統。它的掃描輸出就是數據：一項檢查結果是一次觀測，因此它經一般的數據來源路徑落地，並與其他每一個數據來源一樣繼承節奏、鮮度、事件、族系、治理、RLS、格線與匯出。 [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

支援兩種，而這個選擇既是功能選擇，也同樣是授權選擇。

| 數據來源類型 | 契約方言 | 額外項 | 授權 | 託管雲端平面 |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda 契約 YAML | `pip install .[soda]`（`soda-postgres`） | Elastic License 2.0 | 拒絕——見下文 |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]`（`great-expectations[postgresql]`） | Apache 2.0 | 允許 |

Elastic License 2.0 禁止把該軟體以託管或受管服務的形式提供給第三方，而在 SaaS 平面內代租用戶執行 Soda 正正是這件事。`config/capabilities.yaml` 以 `soda` 選項上的 `cloud_eligible: false` 承載這項區分，託管平面會讀取該旗標。想要 Soda 的託管部署，需觸及一個由營運者自行執行、由營運者提供的 Soda 端點。 [tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa 不隨附也不連結任何一者。掃描在子解譯器（`python -m provisa.dq.worker`）中執行，那是唯一匯入 `soda_core` 或 `great_expectations` 的地方，因此一個原始碼可得的檢查器絕不會進入伺服器處理程序，而檢查器當掉時殺掉的是一個子處理程序，不是事件迴圈。 [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**該數據來源指向 Provisa 自己的 pgwire 端點。** 正是這一點讓單一個 postgres 驅動程式得以檢查以 Snowflake 或 Iceberg 為後盾的表：檢查器掃描的是聯邦檢視，不是底層系統。由於政策套用在該連線上，掃描身分是宣告出來的，而非繼承而來——一組被篩選過的資料列，絕不能產生一項悄然通過的檢查。

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

**每份契約一張結果表，而契約就是整份註冊。** 該表承載 `dq_contract`——契約原文逐字——關於其形狀就再無其他。資料行、水位與晉升全部是推導出來的。 [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

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

註冊從那段文字推導出什麼：

- **族系。** 契約本身已指名其目標數據集，因此註冊會以 `extract_inputs` 剖析 SQL 的方式剖析它（REQ-939），並解析到受治理的表。一份定義，沒有第二份可能漂移的副本。指名一個未受治理數據集的契約，會在註冊時大聲失敗，而不是落地一堆沒人要的資料列。
- **資料行。** 結果封套屬於檢查器，不屬於營運者——自 `scan_id` 到 `diagnostics` 共 16 個隨附資料行。已宣告的資料行只被讀取其 `visible_to`（必須一致），之後即被取代。 [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **水位。** `scan_time` 成為水位，這使得落地成為附加（REQ-982）。掃描歷史在沒有歷史子系統的情況下累積。
- **晉升。** `freshness_max_timestamp` 與 `dataset_rows_tested` 自 `diagnostics` jsonb 晉升為具型別的資料行（REQ-119）。要加更多，做法與任何其他 jsonb 資料行相同。 [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

時序不引入新欄位。`change_signal` 加上 `cache_ttl` 給出輪詢節奏；`mv_debounce_quiet` 與 `mv_debounce_max_delay` 把一波上游爆發收攏成一次掃描（REQ-963）；一個日曆粒度使它變成週期性（REQ-962）；`expected_events` 會扣住掃描，直到其輸入在該窗口內達到鮮度（REQ-961）。輪詢迴圈就是掃描排程器。

`outcome` 是 `pass`、`fail`、`warn`、`error`、`skipped` 之一。它們沒有一個是裁決——若想要強制執行，那是稍後的一項獨立宣告：一道預檢，或一個建於已落地結果之上的 MV。由於一次已落地的觀測不帶決定性義務（REQ-964），這裡容許放進絕不可能坐在預檢閘門上的非決定性檢查——異常分數、尾隨窗口變化、對照當下的鮮度。

契約在 UI 中、在表編輯介面的數據品質面板中撰寫，而那裡的契約原文永遠是真相來源。試跑會針對上線中的表執行該契約並顯示結果而不落地——這正是你如何抓到一份數據集名稱解析到意料之外的地方、否則只會落地一堆通過資料列的契約。

---

## 自訂連接器（REQ-1177）
當營運者在 `config/custom_connectors.yaml` 中為某個新數據來源類型宣告一個連接器時，原生聯邦引擎——Postgres、DuckDB 與 ClickHouse——即取得對它的可達性。毋須任何程式碼。 [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

連接器可擴充性本身早於此。Trino 引擎長久以來在它自己那一層就是可擴充的——一個按數據來源類型參數化的通用 JDBC 連接器、一份逐類型的目錄 `.properties` 本體，以及 Provisa 自己的自訂 Trino 連接器外掛（Splunk、SharePoint、Calcite）。 [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 把同樣的設定驅動可擴充性帶到那兩個原生、免叢集的引擎，它們先前只有一組固定的連接器。

該設定檔出貨時是空的。內建連接器已涵蓋開箱即用的觸及範圍；此檔案中的一切都由營運者撰寫。 [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] 設定 `PROVISA_CUSTOM_CONNECTORS` 可指向另一個路徑（供測試使用時很有用）。

### 描述元種類
| 引擎 | 種類 | 機制 | 描述元提供什麼 |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED（ISO 標準） | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + 掃描器檢視 | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…`（自動呈現每一張遠端表） | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | 逐表 `CREATE TABLE ENGINE=…`（資料行來自登錄） | `ch_engine`, `engine_template`（可帶 `{table}`） |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`，由 ClickHouse 推斷結構描述 | `ch_engine`, `engine_template` |

**Postgres 是通用的。** SQL/MED 是 ISO 標準，因此每個符合標準的 FDW 都共用同一套 DDL 形狀：`CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`、選用的 `CREATE USER MAPPING`，然後是 `IMPORT FOREIGN SCHEMA`（當 `supports_import: true`）或逐表明確的 `CREATE FOREIGN TABLE`（當為 `false`）。一份 `pg_fdw` 描述元只提供逐 FDW 的差異——擴充名稱、伺服器選項索引鍵、使用者映射索引鍵、匯入旗標、表選項。因此任何符合標準的 FDW 都能單憑設定驅動。 [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB 支援兩種機制。** 經 ATTACH 呈現目錄的擴充用 `duckdb_attach`；呈現讀取表函式的擴充用 `duckdb_scan`。兩種模式都不符合的擴充不受支援。 [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse 支援三種機制**，各對應一種整合引擎形狀：一個自動呈現每張遠端表的關聯式 DATABASE 引擎（`clickhouse_database`，例如 Redis/MySQL）、一個資料行由登錄提供的逐表引擎（`clickhouse_table`，例如 JDBC/ODBC 橋接——`engine_template` 可帶一個由執行階段繫結的 `{table}` 佔位符），以及一個由 ClickHouse 推斷結構描述的檔案／湖／URL 引擎（`clickhouse_scan`，例如 HDFS/URL）。SQLite（DATABASE 引擎、檔案、無伺服器）與 Hudi（湖倉、零複製）開箱即有。 [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

未知的 `kind` 值會在啟動時大聲失敗——描述元的一個錯字絕不可悄悄讓某個數據來源類型變成不可達。 [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### 探測閘門
可用性在附掛時針對各引擎的標準探索目錄驗證：

- **Postgres**——檢查 `pg_extension`，然後 `pg_available_extensions`。 [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB**——執行 `INSTALL`／`LOAD` 並在 `duckdb_functions()` 中檢查所宣告的 `probe_symbol`。 [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse**——在 `system.table_engines` 中檢查所宣告的 `ch_engine`；建置中不存在即大聲失敗。 [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

一個已宣告但無法安裝的擴充會大聲失敗。沒有默默略過，沒有備援。探測失敗的連接器，在該部署中就是不啟用。

### 樣板變數
每個 `server_options` 值、`user_mapping` 值、`attach_template` 與 `scan_template` 都可使用 `{field}` 佔位符。可用欄位： [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`、`{host}`、`{port}`、`{database}`、`{username}`、`{password}`、`{path}`、`{schema_name}`、`{table_name}`，加上 `federation_hints` 中的任何索引鍵。DuckDB 的 attach 樣板另外收到 `{alias}`——Provisa 指派給所附掛資料庫的內部目錄別名。

引用未知欄位的樣板會在附掛時大聲失敗，讓描述元與數據來源的不相符在損壞的 DDL 抵達引擎之前浮現。

### 例子
**Postgres——經 `mongo_fdw` 的 MongoDB（無結構描述匯入；資料行逐表提供）**

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

**DuckDB——經 `read_xlsx` 的 Excel 檔案（掃描表函式）**

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

任一描述元就位後，註冊帶有所宣告 `source_type` 的數據來源即會經該自訂連接器路由，前提是探測成功。毋須其他設定變更。

---

## 作為具名數據來源的倉庫 {#warehouses-as-named-sources}
Snowflake、Databricks 與 ClickHouse 可註冊為具名數據來源，與哪個聯邦引擎啟用中無關。 [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

註冊後，Provisa 經該數據來源的 DirectDriver 讀取倉庫，並把一份副本落地到啟用中引擎的具體化儲存區。查詢隨後針對該副本執行。這有別於傳統的可直接執行路徑（asyncpg、aiomysql），後者完全繞過引擎——這裡引擎仍然執行查詢，但針對的是一份本地副本，而不是每次請求都經網路連往倉庫。

在倉庫支援之處，讀取是 Arrow 原生的：Databricks 用 Cloud Fetch，Snowflake 用 `fetch_arrow_table`，ClickHouse 用原生資料行式 HTTP 介面。

標準的 `host`／`port`／`username`／`password` 欄位承載不了的延伸連線參數，放進 `federation_hints`：

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

註冊為具名數據來源，與把同一個倉庫選為聯邦引擎是彼此獨立的。在 DuckDB 引擎上的一個 Snowflake 數據來源，把副本落地到 DuckDB，不是落到 Snowflake。

雲端物件／湖數據（S3／GCS／R2 上的 parquet、csv、iceberg、delta_lake 檔案）是另一種數據來源類型，當啟用中的引擎有該類型的 ATTACH 連接器時，就地附掛。不落地副本——引擎直接掃描物件儲存。那些數據來源的憑證同樣放進 `federation_hints`：

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

## 數據來源設定欄位
所有數據來源共用一組共通欄位。 [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| 欄位 | 必填 | 預設 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一識別碼；英數字加連字號／底線 |
| `type` | 是 | — | 數據來源類型（見上方表格） |
| `host` | 否 | `""` | 主機名稱或 IP |
| `port` | 否 | `0` | 連接埠號 |
| `database` | 否 | `""` | 資料庫名稱 |
| `username` | 否 | `""` | 使用者名稱 |
| `password` | 否 | `""` | 密碼；密鑰解析請用 `${env:VAR}` |
| `path` | 否 | `null` | 檔案式與物件／湖數據來源的檔案路徑或雲端 URI |
| `base_url` | 否 | `null` | OpenAPI 數據來源的基底 URL |
| `pool_min` | 否 | `1` | 連線池最小大小（REQ-052） |
| `pool_max` | 否 | `5` | 連線池最大大小（REQ-052） |
| `use_pgbouncer` | 否 | `false` | 經 PgBouncer 路由連線（REQ-053） |
| `pgbouncer_port` | 否 | `6432` | PgBouncer 連接埠（REQ-053） |
| `cache_enabled` | 否 | `true` | 啟用 API 回應快取 |
| `cache_ttl` | 否 | `null` | 快取 TTL（秒）；為 null 時繼承全域預設 |
| `cache_catalog` | 否 | `null` | API 快取所用的聯邦目錄；預設為數據來源自己的目錄 |
| `cache_schema` | 否 | `api_cache` | 快取目錄內的結構描述 |
| `naming_convention` | 否 | `null` | 為此數據來源覆寫全域命名慣例（REQ-194） |
| `federation_hints` | 否 | `{}` | 傳給聯邦引擎的工作階段屬性，以及倉庫數據來源的延伸連線參數（REQ-278、REQ-281） |
| `mapping` | 否 | `{}` | NoSQL 與 SaaS 數據來源的類型專屬連接器設定（例如 SharePoint 的 `auth_type`、Splunk 的 `use_token`）（REQ-251） |
| `allowed_domains` | 否 | `[]` | 把數據來源限制於特定網域；空 = 不受限 |
| `description` | 否 | `""` | 供人閱讀的說明 |

---

## Kafka 數據來源
Kafka 主題另外設定於 `kafka_sources` 之下，以某個已註冊 `kafka` 數據來源的 `id` 為索引鍵。 [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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

| 欄位 | 說明 |
| ------- | ------------- |
| `id` | 必須符合某個 `type: kafka` 數據來源的 `id` |
| `topics[].id` | 此主題在 Provisa 內的邏輯名稱 |
| `topics[].topic` | Kafka 主題名稱 |
| `topics[].domain_id` | 此主題所屬的網域 |
| `topics[].description` | 供人閱讀的說明 |
| `topics[].default_window` | 窗口查詢的預設時間窗口（例如 `1h`）（REQ-148） |
| `topics[].columns` | 該主題結構描述的資料行定義（REQ-150） |

---

## 資料行可見性
每個資料行上的 `visible_to` 欄位，是能看見該資料行的角色 ID 清單。 [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

未列入某角色 `visible_to` 清單的資料行，不會出現在該角色的 GraphQL 結構描述中，也無法被查詢或在篩選中引用（REQ-039）。

---

## 關係
關係連接兩張已註冊的表，並在 GraphQL 中呈現為巢狀欄位。 [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| 欄位 | 必填 | 說明 |
| ------- | ---------- | ------------- |
| `id` | 是 | 此關係的唯一識別碼 |
| `source_table_id` | 是 | 持有外部索引鍵的表 |
| `target_table_id` | 是 | 被引用的表；計算式關係為空 |
| `source_column` | 是 | 來源表上的資料行 |
| `target_column` | 是 | 目標表上的資料行；計算式關係為空 |
| `cardinality` | 是 | `many-to-one` 或 `one-to-many`（REQ-019） |
| `materialize` | 否 | 為跨來源聯結自動建立具體化檢視（REQ-158） |
| `refresh_interval` | 否 | MV 重新整理間隔（秒）（預設：300） |
| `target_function_name` | 否 | 計算式關係的 DB 函式名稱 |
| `function_arg` | 否 | 哪個函式引數接收來源資料行的值 |
| `alias` | 否 | 供人閱讀的關係類型（例如 `WORKS_FOR`） |
| `graphql_alias` | 否 | 為此關係在父類型上呈現的 SDL 欄位命名。未提供時，名稱自目標表的 `field_name` 與關係基數推導。 [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | 否 | 為 `true` 時，把此關係排除在 Cypher 圖形邊之外 |
| `source_json_key` | 否 | JOIN 之前先自來源資料行抽出此索引鍵作為 JSON 物件 |

基數值 [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]：

- `many-to-one`——每一列來源資料列對應一列目標資料列（FK 對 PK）
- `one-to-many`——每一列來源資料列對應多列目標資料列（上者的反向）

---

## 資料列層級安全規則
RLS 規則在查詢時注入 `WHERE` 子句，範圍限定於某個角色，並可選擇限定於某張表或某個網域。 [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

當同一個角色同時存在網域層級與表層級的規則時，表層級規則優先（REQ-403）。

| 欄位 | 必填 | 說明 |
| ------- | ---------- | ------------- |
| `table_id` | 條件式 | 要套用規則的表；與 `domain_id` 互斥 |
| `domain_id` | 條件式 | 要套用規則的網域；套用至該網域內所有表（REQ-402） |
| `role_id` | 是 | 此規則所套用的角色 |
| `filter` | 是 | 注入 `WHERE` 的 SQL 述詞；可引用工作階段變數（REQ-041） |

---

## 函式與 Webhook
### DB 函式
追蹤一個資料庫函式，並把它呈現為 GraphQL 查詢或變更操作。 [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

資料庫數據來源也能自供應商目錄（`pg_proc`、`information_schema.routines` 或供應商對應物）自動探索其預存程序與函式，免去逐一手動註冊。探索會讀取 `prokind` 與 `provolatile`：immutable／stable 的函式註冊為參數化關聯（程序引數變成查詢參數，形狀與 OpenAPI GET 表相同），而 volatile 的程序註冊為變更操作／受追蹤函式。已探索的常式流經 Stage-2 治理的方式，與手動註冊者完全相同。 [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

| 欄位 | 必填 | 預設 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `name` | 是 | — | GraphQL 欄位名稱 |
| `source_id` | 是 | — | 含有該函式的數據來源 |
| `schema` | 否 | `public` | 資料庫結構描述 |
| `function_name` | 是 | — | 實際的資料庫函式名稱 |
| `returns` | 是 | — | 該函式回傳的已註冊表 ID（REQ-207） |
| `arguments` | 否 | `[]` | `{name, type}` 引數定義清單（REQ-211） |
| `visible_to` | 否 | `[]` | 能呼叫此函式的角色 |
| `writable_by` | 否 | `[]` | 能以變更操作呼叫此函式的角色 |
| `domain_id` | 否 | `""` | 此函式所屬的網域 |
| `description` | 否 | `null` | GraphQL 欄位說明 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"`（REQ-205） |

### Webhook
把一個外部 HTTP 端點呈現為 GraphQL 查詢或變更操作。 [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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

| 欄位 | 必填 | 預設 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `name` | 是 | — | GraphQL 欄位名稱 |
| `url` | 是 | — | Webhook 端點 URL |
| `method` | 否 | `POST` | HTTP 方法 |
| `timeout_ms` | 否 | `5000` | 請求逾時（毫秒） |
| `returns` | 否 | `null` | 已註冊表 ID，或 null 表示內嵌類型 |
| `inline_return_type` | 否 | `[]` | 自訂回傳形狀的 `{name, type}` 欄位清單（REQ-210） |
| `arguments` | 否 | `[]` | `{name, type}` 引數定義清單 |
| `visible_to` | 否 | `[]` | 能呼叫此 webhook 的角色 |
| `domain_id` | 否 | `""` | 此 webhook 所屬的網域 |
| `description` | 否 | `null` | GraphQL 欄位說明 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"` |

---

## 驗證
驗證設定於 `auth` 索引鍵之下。 [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| 提供者 | 說明 |
| ---------- | ------------- |
| `none` | 不驗證；所有請求皆視為 `default_role` |
| `firebase` | Firebase Authentication；需要 `project_id` 與 `service_account_key`（REQ-121） |
| `keycloak` | Keycloak OIDC（REQ-122） |
| `oauth` | 通用 OAuth 2.0（REQ-123） |
| `simple` | 不經外部提供者的使用者名稱／密碼（REQ-124） |

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

`assignments_source: claims` 自 JWT 宣告讀取角色指派。`assignments_source: provisa` 自 Provisa 自己的指派儲存區讀取。 [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## 執行路由
**直接執行**——單一來源的 RDBMS 查詢路由到原生驅動程式，取得低於 100 毫秒的延遲（REQ-027）。數據來源需同時具備 `SOURCE_TO_DIALECT` 項目與 `SOURCE_TO_CONNECTOR` 項目才能支援此路徑（REQ-229）。

**聯邦執行**——多來源查詢，以及沒有直接驅動程式的數據來源，經聯邦引擎路由（REQ-028）。Provisa 內含一個內嵌聯邦引擎；大規模部署可指向你自己的相容叢集（REQ-226）。

**統計**——註冊時，Provisa 對每張已發佈的表執行 `ANALYZE`，為成本式最佳化器預熱（資料列數、null 比例、相異值、最小／最大值）。失敗會被記錄，且不會阻擋註冊（REQ-275）。

---

## 圖形與語意數據來源
### Neo4j

把一個 Neo4j 圖形資料庫註冊為可查詢的數據來源。管理員撰寫投影出純量值的 Cypher 查詢；Provisa 快取結果並把它們呈現為 GraphQL 類型（REQ-295）。

Cypher 查詢必須在 `RETURN` 子句中使用屬性存取子（`RETURN n.id AS id, n.name AS name`）——回傳節點物件在註冊時即被拒絕（REQ-296）。

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

預覽端點（`POST /admin/sources/neo4j/{id}/preview`）回傳樣本資料列，並在 Cypher 回傳節點物件時阻擋註冊（REQ-296）。

### SPARQL

把任何符合 SPARQL 1.1 的三元組儲存（Apache Jena Fuseki、Virtuoso、Stardog 等）註冊為可查詢的數據來源（REQ-297）。

查詢必須是 `SELECT` 查詢。`SELECT` 子句中的變數名稱自動成為資料行名稱（REQ-297）。

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

兩個連接器都使用 API 數據來源快取管線——結果以可設定的 TTL 儲存在 PostgreSQL，使它們可用於跨來源的聯邦 JOIN（REQ-295、REQ-297、REQ-299）。

---

## 連線範例
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

### 跨來源查詢
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

單一來源的部分直接路由（REQ-027）。跨來源 JOIN 經聯邦執行，並帶自動型別強制轉換（REQ-028、REQ-552）。
