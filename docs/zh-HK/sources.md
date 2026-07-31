# 數據來源型別

## 執行模型

每一則查詢最終均經由聯邦引擎執行，該引擎為所有數據來源提供聯邦能力。數據來源依其連線方式分為三大類。[tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| 類別 | 具備直接驅動程式 | 具備聯邦連接器 | 範例 |
| --- | --- | --- | --- |
| **可直連型** | 是 | 是 | PostgreSQL、MySQL、MariaDB、SingleStore、SQL Server、Oracle、DuckDB |
| **僅限聯邦型** | 否 | 是 | Redshift、Druid、Exasol、Hive、Iceberg、Delta Lake、Hive（S3 支援） |
| **直讀（複本）型** | 是 | 是 | Snowflake、Databricks、ClickHouse——驅動程式讀取數據並落地為複本；查詢於使用中引擎的複本上執行 |
| **具體化 → 聯邦型** | 否 | 否 | REST/OpenAPI、遠端 GraphQL、gRPC、Neo4j Cypher、SPARQL、WebSocket、RSS、CSV、SQLite、Parquet、Ingest（推送接收端）、GovData、SharePoint、Splunk |

**可直連型**數據來源會經由其原生驅動程式執行單一數據來源查詢（低於 100 毫秒），略過聯邦引擎 (REQ-027、REQ-229)。這類數據來源保留完整的連接器支援，並於與其他數據來源進行 JOIN 時參與聯邦 (REQ-028)。

**僅限聯邦型**數據來源恆經聯邦層查詢。並無直接驅動程式存在 (REQ-229)。

**直讀（複本）型**數據來源具備一個 DirectDriver，會原生讀取該數據倉庫（有支援時採用 Arrow 原生格式），將複本落地至使用中引擎的具體化儲存區，隨後查詢即於該複本上執行。見[作為具名數據來源的數據倉庫](#_15)。

**具體化型**數據來源並無聯邦連接器。Provisa 會擷取其數據（於啟動時或查詢時），並以 Parquet 格式快取於 S3 或 PostgreSQL 中，使聯邦引擎可存取其進行跨數據來源查詢 (REQ-309)。

---

## 所有數據來源

Provisa 支援的每種數據來源型別參考。「直接驅動程式」表示單一數據來源查詢會原生針對該數據來源執行（低於 100 毫秒）(REQ-027)。「連接器名稱」是當該數據來源參與多數據來源 JOIN 時所使用的聯邦連接器 (REQ-028)。[tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| 數據來源型別 | 直接驅動程式 | 連接器名稱 | 方言 | 變異 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | 支援 |
| `mysql` | aiomysql | mysql | mysql | 支援 |
| `mariadb` | aiomysql | mariadb | mysql | 支援 |
| `singlestore` | — | singlestore | singlestore | 聯邦式 |
| `sqlserver` | aioodbc | sqlserver | tsql | 支援 |
| `oracle` | oracledb | oracle | oracle | 支援 |
| `duckdb` | duckdb | memory | duckdb | 支援 |
| `cockroachdb` | asyncpg（PG wire） | postgresql | postgres | 支援 |
| `yugabytedb` | asyncpg（PG wire） | postgresql | postgres | 支援 |
| `greenplum` | asyncpg（PG wire） | postgresql | postgres | 支援 |
| `tidb` | aiomysql（MySQL wire） | mysql | mysql | 支援 |

線路相容的資料庫會重用某基礎線路的 JDBC 驅動程式、原生非同步驅動程式及方言——CockroachDB、YugabyteDB 及 Greenplum 搭乘 PostgreSQL 線路；TiDB 搭乘 MySQL 線路。它們僅需登記項目，無須新增連接器程式碼。[tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird`（Firebird 3/4/5）及 `airport`（Arrow Flight 伺服器）為已註冊的數據來源型別，於 DuckDB 為使用中引擎時，透過 DuckDB 社群擴充功能就地連接——無直接驅動程式，無聯邦連接器。[tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### 雲端數據倉庫

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| 數據來源型別 | 直接驅動程式 | 連接器名稱 | 方言 | 變異 | 備註 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | 聯邦式 | 透過 snowflake-connector-python 讀取；落地複本；`federation_hints` 中須含 `account`/`warehouse`/`role` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | 聯邦式 | 無 DirectDriver；透過聯邦引擎或 BigQuery 引擎 ATTACH 連接 |
| `databricks` | DatabricksDriver | delta_lake | databricks | 聯邦式 | 透過 databricks-sql-connector 讀取（Cloud Fetch、Arrow）；落地複本；`federation_hints` 中須含 `http_path` (REQ-987) |
| `redshift` | — | redshift | redshift | 聯邦式 | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | 聯邦式 | Microsoft Fabric Warehouse；透過 TDS 使用 T-SQL，Azure AD 驗證；落地複本 (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | 聯邦式 | Azure Synapse SQL；透過 TDS 使用 T-SQL，Azure AD 驗證；落地複本 (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | 聯邦式 | 透過 SQLAlchemy trino 方言讀取遠端 Trino/Presto 協調器；於任何引擎上落地複本 (REQ-994) |

### 分析 / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| 數據來源型別 | 直接驅動程式 | 連接器名稱 | 方言 | 變異 | 備註 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | 聯邦式 | 透過 clickhouse-connect（HTTP）讀取；`federation_hints` 中的 `secure: "true"` 用於 TLS (REQ-986) |
| `druid` | — | druid | druid | 不支援 | — |
| `exasol` | — | exasol | exasol | 不支援 | — |
| `elasticsearch` | — | elasticsearch | — | 不支援 | 連接器屬性來自該型別的對應 DSL [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | 不支援 | Trino `pinot` 連接器；`pinot.controller-urls` = Pinot 控制器的 host:port [tool-verified: `trino_connectors.py:199`] |

### 數據湖 / 開放資料表格式

這些數據來源型別為僅限聯邦——無直接驅動程式，無方言。[tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| 數據來源型別 | 連接器名稱 | 時間回溯 | 備註 |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | 支援（`as_of` 引數，REQ-372） | — |
| `delta_lake` | delta_lake | 支援（`as_of` 引數，REQ-372） | — |
| `hive` | hive | 不支援 | — |
| `hive_s3` | hive | 不支援 | S3 支援的 Hive |

### NoSQL

`mongodb`、`cassandra` 及 `redis` 均具備 Trino 連接器（`redis` 依該型別的對應 DSL 建構其屬性）。[tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017、REQ-1097)

| 數據來源型別 | 連接器名稱 | 變異 |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | 不支援 |
| `cassandra` | cassandra | 不支援 |
| `redis` | redis | 不支援 |

### 串流

| 數據來源型別 | 機制 | 變異 |
| ------------ | ----------- | ----------- |
| `kafka` | 聯邦式 Kafka 連接器；結構描述來自 Confluent Schema Registry（Avro、Protobuf、JSON Schema）、手動定義，或樣本推論 (REQ-147、REQ-150) | 僅支援 Sink (REQ-176) |
| `websocket` | 外部 WebSocket 訂閱源——連接、訂閱、接收事件；結果經具體化 (REQ-338) | 不支援 |
| `rss` | RSS 2.0 / Atom 訂閱源——輪詢，以 pubDate/updated 作為水位標記；結果經具體化 (REQ-342、REQ-343) | 不支援 |

### 推送接收端

| 數據來源型別 | 機制 | 變異 |
| ------------ | ----------- | ----------- |
| `ingest` | 外部服務以 POST 方式傳送 JSON 事件；結果經具體化 (REQ-331、REQ-335) | 不支援 |

### 圖形與語意

| 數據來源型別 | 機制 | 變異 |
| ------------ | ----------- | ----------- |
| `neo4j` | 經 HTTP API 執行 Cypher，結果快取於 PostgreSQL (REQ-295) | 不支援 |
| `sparql` | SPARQL 1.1 POST，結果快取於 PostgreSQL (REQ-297) | 不支援 |

### 檔案型

有兩種機制涵蓋檔案。兩者均使用 `path` 欄位取代 `host`/`port`。[tool-verified: `provisa/core/models.py`] (REQ-553)

**單一檔案數據來源**——`sqlite`、`csv`、`parquet` 將 `path` 指向單一檔案。

| 數據來源型別 | 傳輸方式 | 變異 |
| --- | --- | --- |
| `sqlite` | 本機 | 支援 |
| `csv` | 本機 | 不支援 |
| `parquet` | 本機、`s3://` | 不支援 |

私有儲存桶需要憑證（來自環境變數的 AWS 區域及金鑰）。如要透過 `s3://` 或 `http(s)://` 讀取 CSV，或一次註冊多個檔案，請使用 `files` 數據來源。[tool-verified: `provisa/file_source/source.py`]

**`files` 數據來源**——將 `path` 指向一個 glob 模式，遞迴爬取該路徑，並將該目錄註冊為資料表的聯邦目錄。它可透過多種傳輸方式讀取多種格式；下表的內容集合來自檔案連接器（kenstott/calcite fork）。[tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| 格式 | 傳輸方式 |
| --- | --- |
| CSV、TSV、JSON、YAML、Excel（XLS/XLSX）、Parquet、Arrow，以及轉換為資料表的文件——HTML、Markdown、DOCX、PPTX | 本機檔案系統、HTTP(S)、`s3://`、`hdfs://`、`ftp://`/`ftps://`、`sftp://`、`iceberg://`、SharePoint（REST 及 Microsoft Graph） |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### 可觀測性及其他

`prometheus` 具備 Trino 連接器（屬性依該型別的對應 DSL 建構）。`google_sheets` 為已註冊的數據來源型別，無 Trino 連接器，並透過 API 快取管線進行具體化。[tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| 數據來源型別 | 連接器名稱 | 變異 |
| ------------ | ----------------- | ----------- |
| `google_sheets` | —（具體化） | 不支援 |
| `prometheus` | prometheus | 不支援 |

### 企業 SaaS 連接器

SharePoint 及 Splunk 經 Apache Calcite 連接器（kenstott/calcite fork）註冊。兩者均無直接驅動程式——Provisa 透過啟動連接器內建的 Calcite pgwire 伺服器（`pgwire-sharepoint`、`pgwire-splunk`），以通用 PostgreSQL 端點方式連接，並將資料列落地至具體化儲存區以供聯邦使用，藉此具體化其數據 (REQ-954)。兩個連接器均恆啟用不區分大小寫的名稱比對，以符合各自產品本身不區分大小寫的語意 (REQ-725、REQ-730)。[tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

SharePoint 清單會被列舉為結構描述，並公開為可查詢的資料表 (REQ-726、REQ-731)。有兩種驗證方式：`CLIENT_CREDENTIALS`（預設）及透過 PFX 憑證的憑證式驗證 (REQ-727)。`mapping` 中的密鑰值會於送達連接器之前經密鑰引擎解析 (REQ-729)。[tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| 數據來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` 或 `host` | `site-url` | SharePoint 網站 URL |
| `username` | `client-id` | Azure 應用程式客戶端 ID |
| `password` | `client-secret` | Azure 應用程式客戶端密鑰 |
| `database` | `tenant-id` | Azure 租用戶 UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS`（預設）或 `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | 當 `auth_type: CERTIFICATE` 時的 PFX 路徑 |
| `mapping.certificate_password` | `certificate-password` | PFX 密碼 |

當連接器未公開 `information_schema.columns` 時，請透過 `registerTable` 變異，以明確的欄位定義（自 Microsoft Graph API 取得）註冊該資料表 (REQ-732)。

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

Splunk 搜尋結果可作為資料表查詢（例如 `internal_server`）(REQ-721)。連接器 URL 來自 `base_url`，或以預設連接埠 `8089` 建構為 `https://{host}:{port}` (REQ-722)。驗證方式：當 `mapping.use_token` 為 `true`（預設）時，`password` 會作為 API 權杖傳遞；為 `false` 時，`username` 及 `password` 會作為個別憑證傳遞 (REQ-723)。[tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| 數據來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | 使用 `base_url`，否則為 `https://host:port`（連接埠預設為 8089） |
| `password` | `token` 或 `password` | 當 `use_token: true` 時為權杖 |
| `username` | `user` | 僅於 `use_token: false` 時使用 |
| `database` | `app` | 限定於某個 Splunk 應用程式 |
| `mapping.datamodel_filter` | `datamodel-filter` | 篩選至某個數據模型 |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | 供自簽憑證使用 (REQ-724) |

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

### API 數據來源

將任何 HTTP 端點註冊為可查詢的資料表。[tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314、REQ-307、REQ-322)

| API 型別 | 探索方式 | 欄位型別推論 |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI 規格剖析 (REQ-314、REQ-316) | 原始型別 → 原生型別，物件 → JSONB |
| `graphql_remote` | 結構描述內省 (REQ-307、REQ-308) | 原始型別 → 原生型別，物件 → JSONB |
| `grpc_remote` | 伺服端反射 (REQ-322、REQ-325) | 原始型別 → 原生型別，物件 → JSONB |

API 回應會被擷取、快取於 PostgreSQL（TTL 可設定），並公開為 GraphQL 型別 (REQ-309、REQ-318、REQ-327)。已快取的資料表與任何其他數據來源相同，均可參與聯邦查詢 (REQ-313)。

**JSONB 規則**：以 JSONB 儲存的複雜欄位（物件、陣列）不可篩選 (REQ-119)。子欄位存取於 SQL 中使用 `->>` 擷取 (REQ-151)。關係是使用純量外部索引鍵欄位於資料表之間宣告——JSONB blob 欄位不能作為 JOIN 目標。若需篩選或 JOIN 巢狀欄位，請使用 JSONB 提升，將其轉換為原生純量欄位 (REQ-119)。

### GovData

美國政府開放數據。存取權以主題分組劃分。[tool-verified: `provisa/core/models.py` lines 543–609]

每個 `govdata` 數據來源均選擇一個主題。該主題決定了會公開哪些 GovData 結構描述。`ref` 及 `geo` 結構描述恆作為連結器結構描述包含在內——它們並未於各主題下逐一列出，但恆為存在。[tool-verified: `provisa/core/models.py` line 562–563 comment]

| 主題 | 公開的結構描述 |
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
| `ALL` | 以上所有結構描述 |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| 欄位 | 必填 | 預設值 | 描述 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一識別碼 |
| `subject` | 是 | — | 上表主題值之一 |
| `domain_id` | 是 | — | 此數據來源所屬的領域 |
| `description` | 否 | `""` | 易於閱讀的描述 |

---

## 自訂連接器 (REQ-1177)

當操作人員於 `config/custom_connectors.yaml` 中為某個新數據來源型別宣告連接器時，原生聯邦引擎——Postgres、DuckDB 及 ClickHouse——便會取得對該型別的連線能力。無須撰寫任何程式碼。[tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

連接器可擴充性本身早於此功能存在。Trino 引擎長久以來在其自身層級即可擴充——每種數據來源型別均以一個通用 JDBC 連接器參數化，每種型別各有一份 catalog `.properties` 主體，以及 Provisa 自有的自訂 Trino 連接器外掛（Splunk、SharePoint、Calcite）。[tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 將此種以設定驅動的可擴充性，帶入以往連接器組合固定的兩個原生、無叢集引擎。

該設定檔預設為空。內建連接器已涵蓋開箱即用的連線範圍；此檔案中的所有內容均由操作人員自行撰寫。[tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] 可設定 `PROVISA_CUSTOM_CONNECTORS` 以指向不同的路徑（供測試使用）。

### 描述器種類

| 引擎 | 種類 | 機制 | 描述器提供的內容 |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED（ISO 標準） | `extension`、`server_options`、`user_mapping`、`supports_import`、`table_options`、`remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`、`probe_symbol`、`attach_template`、`remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + 掃描器檢視 | `extension`、`probe_symbol`、`scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…`（自動公開每個遠端資料表） | `ch_engine`、`engine_template` |
| `clickhouse` | `clickhouse_table` | 逐資料表的 `CREATE TABLE ENGINE=…`（欄位來自登記表） | `ch_engine`、`engine_template`（可能帶有 `{table}`） |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`，由 ClickHouse 推論結構描述 | `ch_engine`、`engine_template` |

**Postgres 為通用機制。**SQL/MED 為 ISO 標準，因此每個符合規範的 FDW 均共用相同的 DDL 形狀：`CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`，選用的 `CREATE USER MAPPING`，接著為 `IMPORT FOREIGN SCHEMA`（當 `supports_import: true` 時）或逐資料表明確的 `CREATE FOREIGN TABLE`（當為 `false` 時）。`pg_fdw` 描述器僅供每個 FDW 各異的部分——擴充功能名稱、伺服器選項鍵、使用者對應鍵、匯入旗標、資料表選項。因此，任何符合標準規範的 FDW 均可單憑設定即可驅動。[tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB 支援兩種機制。**經 ATTACH 公開目錄的擴充功能使用 `duckdb_attach`；公開讀取用資料表函式的擴充功能使用 `duckdb_scan`。若某擴充功能不符合任一模式，則不受支援。[tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse 支援三種機制**，各對應一種整合引擎形狀：一種自動公開每個遠端資料表的關聯式 DATABASE 引擎（`clickhouse_database`，例如 Redis/MySQL）、一種欄位由登記表提供的逐資料表引擎（`clickhouse_table`，例如 JDBC/ODBC 橋接器——`engine_template` 可帶有一個由執行階段綁定的 `{table}` 佔位符），以及一種由 ClickHouse 推論其結構描述的檔案/數據湖/URL 引擎（`clickhouse_scan`，例如 HDFS/URL）。SQLite（DATABASE 引擎、檔案型、無伺服器）及 Hudi（湖倉一體、零複製）為開箱即用內建項目。[tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

未知的 `kind` 值會於啟動時直接失敗——描述器的拼字錯誤絕不能在無聲之下使某個數據來源型別無法連接。[tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### 探測閘控

可用性會於連接（attach）時，依各引擎的標準探索目錄進行驗證：

- **Postgres**——檢查 `pg_extension`，其次為 `pg_available_extensions`。[tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB**——執行 `INSTALL`/`LOAD`，並於 `duckdb_functions()` 中檢查所宣告的 `probe_symbol`。[tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse**——於 `system.table_engines` 中檢查所宣告的 `ch_engine`；建置中缺少該項目即直接失敗。[tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

無法安裝的已宣告擴充功能會直接失敗。無無聲略過，無備援機制。探測失敗的連接器，對該部署而言即視為未啟用。

### 範本變數

每個 `server_options` 值、`user_mapping` 值、`attach_template` 及 `scan_template` 均可使用 `{field}` 佔位符。可用欄位：[tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`、`{host}`、`{port}`、`{database}`、`{username}`、`{password}`、`{path}`、`{schema_name}`、`{table_name}`，以及 `federation_hints` 中的任何鍵。DuckDB attach 範本亦會取得 `{alias}`——即 Provisa 為已連接資料庫指派的內部目錄別名。

參照未知欄位的範本會於連接（attach）時直接失敗，在錯誤的 DDL 到達引擎之前，即揭露描述器/數據來源不符的情況。

### 範例

**Postgres——經 `mongo_fdw` 連接 MongoDB（無結構描述匯入；欄位逐資料表提供）**

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

**DuckDB——經 `read_xlsx` 讀取 Excel 檔案（掃描資料表函式）**

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

在任一描述器就緒後，以所宣告的 `source_type` 註冊一個數據來源，即會經由該自訂連接器路由，惟須通過探測。無須其他設定變更。

---

## 作為具名數據來源的數據倉庫

無論目前啟用哪個聯邦引擎，Snowflake、Databricks 及 ClickHouse 均可獨立註冊為具名數據來源。[tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

註冊後，Provisa 會透過該數據來源的 DirectDriver 讀取該數據倉庫，並將複本落地至使用中引擎的具體化儲存區。查詢隨後即於該複本上執行。這與傳統的可直連路徑（asyncpg、aiomysql）不同——傳統路徑完全略過引擎；此處引擎仍會執行查詢，但是針對本機複本，而非每次要求皆透過線路連往數據倉庫。

於數據倉庫支援之處，讀取採用 Arrow 原生格式：Databricks 使用 Cloud Fetch，Snowflake 使用 `fetch_arrow_table`，ClickHouse 使用原生欄式 HTTP 介面。

標準的 `host`/`port`/`username`/`password` 欄位無法承載的擴充連線參數，均置於 `federation_hints` 中：

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

以具名數據來源身分註冊，與選用相同的數據倉庫作為聯邦引擎兩者互不相干。以 DuckDB 引擎搭配 Snowflake 數據來源，複本會落地至 DuckDB，而非 Snowflake。

雲端物件/數據湖數據（S3 / GCS / R2 上的 parquet、csv、iceberg、delta_lake 檔案）是獨立的一種數據來源型別，當使用中引擎具備該型別的 ATTACH 連接器時會就地連接。不會落地任何複本——引擎會直接掃描物件儲存。此類數據來源的憑證同樣置於 `federation_hints` 中：

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

所有數據來源均共用一組共通欄位。[tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| 欄位 | 必填 | 預設值 | 描述 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一識別碼；英數字元加連字號/底線 |
| `type` | 是 | — | 數據來源型別（見上表） |
| `host` | 否 | `""` | 主機名稱或 IP |
| `port` | 否 | `0` | 連接埠號碼 |
| `database` | 否 | `""` | 資料庫名稱 |
| `username` | 否 | `""` | 使用者名稱 |
| `password` | 否 | `""` | 密碼；使用 `${env:VAR}` 進行密鑰解析 |
| `path` | 否 | `null` | 供檔案型及物件/數據湖數據來源使用的檔案路徑或雲端 URI |
| `base_url` | 否 | `null` | OpenAPI 數據來源的基礎 URL |
| `pool_min` | 否 | `1` | 連線池最小值 (REQ-052) |
| `pool_max` | 否 | `5` | 連線池最大值 (REQ-052) |
| `use_pgbouncer` | 否 | `false` | 透過 PgBouncer 路由連線 (REQ-053) |
| `pgbouncer_port` | 否 | `6432` | PgBouncer 連接埠 (REQ-053) |
| `cache_enabled` | 否 | `true` | 啟用 API 回應快取 |
| `cache_ttl` | 否 | `null` | 快取 TTL（秒）；為 null 時繼承全域預設值 |
| `cache_catalog` | 否 | `null` | 供 API 快取使用的聯邦 catalog；預設為該數據來源自身的 catalog |
| `cache_schema` | 否 | `api_cache` | 快取 catalog 內的結構描述 |
| `naming_convention` | 否 | `null` | 為此數據來源覆寫全域命名慣例 (REQ-194) |
| `federation_hints` | 否 | `{}` | 傳遞給聯邦引擎的階段屬性，以及數據倉庫數據來源的擴充連線參數 (REQ-278、REQ-281) |
| `mapping` | 否 | `{}` | 供 NoSQL 及 SaaS 數據來源使用的型別專屬連接器設定（例如 SharePoint 的 `auth_type`、Splunk 的 `use_token`） (REQ-251) |
| `allowed_domains` | 否 | `[]` | 將數據來源限制於特定領域；留空即不限制 |
| `description` | 否 | `""` | 易於閱讀的描述 |

---

## Kafka 數據來源

Kafka 主題會於 `kafka_sources` 下另行設定，以已註冊 `kafka` 數據來源的 `id` 作為鍵值。[tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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

| 欄位 | 描述 |
| ------- | ------------- |
| `id` | 須與型別為 `kafka` 之數據來源的 `id` 相符 |
| `topics[].id` | 此主題於 Provisa 中的邏輯名稱 |
| `topics[].topic` | Kafka 主題名稱 |
| `topics[].domain_id` | 此主題所屬的領域 |
| `topics[].description` | 易於閱讀的描述 |
| `topics[].default_window` | 視窗化查詢的預設時間視窗（例如 `1h`） (REQ-148) |
| `topics[].columns` | 該主題結構描述的欄位定義 (REQ-150) |

---

## 欄位可見性

每個欄位上的 `visible_to` 欄位，是可見到該欄位的角色 ID 清單。[tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

未列於某角色 `visible_to` 清單中的欄位，不會出現於該角色的 GraphQL 結構描述中，亦不可於查詢或篩選條件中被查詢或參照 (REQ-039)。

---

## 關係

關係連接兩個已註冊的資料表，並於 GraphQL 中以巢狀欄位形式呈現。[tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| 欄位 | 必填 | 描述 |
| ------- | ---------- | ------------- |
| `id` | 是 | 此關係的唯一識別碼 |
| `source_table_id` | 是 | 持有外部索引鍵的資料表 |
| `target_table_id` | 是 | 被參照的資料表；計算關係時留空 |
| `source_column` | 是 | 來源資料表上的欄位 |
| `target_column` | 是 | 目標資料表上的欄位；計算關係時留空 |
| `cardinality` | 是 | `many-to-one` 或 `one-to-many` (REQ-019) |
| `materialize` | 否 | 自動為跨數據來源 JOIN 建立具體化檢視 (REQ-158) |
| `refresh_interval` | 否 | 具體化檢視重新整理間隔（秒）（預設：300） |
| `target_function_name` | 否 | 供計算關係使用的資料庫函式名稱 |
| `function_arg` | 否 | 哪個函式引數接收來源欄位值 |
| `alias` | 否 | 易於閱讀的關係型別（例如 `WORKS_FOR`） |
| `graphql_alias` | 否 | 為此關係於母型別上公開的 SDL 欄位命名。缺省時，名稱衍生自目標資料表的 `field_name` 及關係基數。[tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | 否 | 設為 `true` 時，將此關係排除於 Cypher 圖形邊之外 |
| `source_json_key` | 否 | 於 JOIN 之前，從來源欄位中以此鍵擷取為一個 JSON 物件 |

基數值 [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]：

- `many-to-one`——每一來源列對應至一列目標列（外部索引鍵對應至主索引鍵）
- `one-to-many`——每一來源列對應至多列目標列（與上述相反）

---

## 行級安全規則

行級安全規則會於查詢時注入 `WHERE` 子句，可限定於某個角色，並可選擇性地限定於某個資料表或領域。[tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

當同一角色同時存在領域層級及資料表層級的規則時，資料表層級規則具優先權 (REQ-403)。

| 欄位 | 必填 | 描述 |
| ------- | ---------- | ------------- |
| `table_id` | 視情況而定 | 套用此規則的資料表；與 `domain_id` 互斥 |
| `domain_id` | 視情況而定 | 套用此規則的領域；適用於該領域內所有資料表 (REQ-402) |
| `role_id` | 是 | 此規則適用的角色 |
| `filter` | 是 | 注入 `WHERE` 中的 SQL 述詞；可參照階段變數 (REQ-041) |

---

## 函式與 Webhook

### 資料庫函式

追蹤一個資料庫函式，並將其公開為 GraphQL 查詢或變異。[tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

資料庫數據來源亦可從供應商目錄（`pg_proc`、`information_schema.routines`，或供應商對應項目）自動探索其預存程序及函式，免除逐一手動註冊的需要。探索過程會讀取 `prokind` 及 `provolatile`：不可變/穩定的函式會註冊為參數化關係（程序引數成為查詢參數，與 OpenAPI GET 資料表相同形狀），而易變的程序則會註冊為變異/已追蹤函式。已探索到的常式，均與手動註冊者一樣經過第二階段治理。[tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

| 欄位 | 必填 | 預設值 | 描述 |
| ------- | ---------- | --------- | ------------- |
| `name` | 是 | — | GraphQL 欄位名稱 |
| `source_id` | 是 | — | 包含此函式的數據來源 |
| `schema` | 否 | `public` | 資料庫結構描述 |
| `function_name` | 是 | — | 實際的資料庫函式名稱 |
| `returns` | 是 | — | 此函式所回傳的已註冊資料表 ID (REQ-207) |
| `arguments` | 否 | `[]` | `{name, type}` 引數定義的清單 (REQ-211) |
| `visible_to` | 否 | `[]` | 可呼叫此函式的角色 |
| `writable_by` | 否 | `[]` | 可以變異方式呼叫此函式的角色 |
| `domain_id` | 否 | `""` | 此函式所屬的領域 |
| `description` | 否 | `null` | GraphQL 欄位描述 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"` (REQ-205) |

### Webhook

將一個外部 HTTP 端點公開為 GraphQL 查詢或變異。[tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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

| 欄位 | 必填 | 預設值 | 描述 |
| ------- | ---------- | --------- | ------------- |
| `name` | 是 | — | GraphQL 欄位名稱 |
| `url` | 是 | — | Webhook 端點 URL |
| `method` | 否 | `POST` | HTTP 方法 |
| `timeout_ms` | 否 | `5000` | 要求逾時（毫秒） |
| `returns` | 否 | `null` | 已註冊的資料表 ID，或內嵌型別時為 null |
| `inline_return_type` | 否 | `[]` | 供自訂回傳形狀使用的 `{name, type}` 欄位清單 (REQ-210) |
| `arguments` | 否 | `[]` | `{name, type}` 引數定義的清單 |
| `visible_to` | 否 | `[]` | 可呼叫此 webhook 的角色 |
| `domain_id` | 否 | `""` | 此 webhook 所屬的領域 |
| `description` | 否 | `null` | GraphQL 欄位描述 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"` |

---

## 驗證

驗證設定於 `auth` 鍵下。[tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| 提供者 | 描述 |
| ---------- | ------------- |
| `none` | 無驗證；所有要求均視為 `default_role` |
| `firebase` | Firebase Authentication；需要 `project_id` 及 `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | 通用 OAuth 2.0 (REQ-123) |
| `simple` | 無外部提供者的使用者名稱/密碼驗證 (REQ-124) |

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

`assignments_source: claims` 由 JWT claims 讀取角色指派。`assignments_source: provisa` 則由 Provisa 自身的指派儲存區讀取。[tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## 執行路由

**直接執行**——單一數據來源的 RDBMS 查詢會路由至原生驅動程式，以取得低於 100 毫秒的延遲 (REQ-027)。數據來源須同時具備 `SOURCE_TO_DIALECT` 項目及 `SOURCE_TO_CONNECTOR` 項目，方可支援此路徑 (REQ-229)。

**聯邦執行**——多數據來源查詢，以及無直接驅動程式的數據來源，均經聯邦引擎路由 (REQ-028)。Provisa 內建一個嵌入式聯邦引擎；如需大規模部署，可指向您自有的相容叢集 (REQ-226)。

**統計資料**——於註冊時，Provisa 會對每個已公開的資料表執行 `ANALYZE`，以為成本導向最佳化器預備數據（列數、空值比率、相異值、最小/最大值）。失敗會被記錄，但不會阻擋註冊 (REQ-275)。

---

## 圖形與語意數據來源

### Neo4j

將 Neo4j 圖形資料庫註冊為可查詢的數據來源。數據管家可撰寫投射純量值的 Cypher 查詢；Provisa 會快取結果並將其公開為 GraphQL 型別 (REQ-295)。

Cypher 查詢的 `RETURN` 子句必須使用屬性存取子（`RETURN n.id AS id, n.name AS name`）——回傳節點物件會於註冊時被拒絕 (REQ-296)。

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

預覽端點（`POST /admin/sources/neo4j/{id}/preview`）會回傳樣本列，並在 Cypher 回傳節點物件時阻擋註冊 (REQ-296)。

### SPARQL

將任何符合 SPARQL 1.1 規範的三元組存儲（Apache Jena Fuseki、Virtuoso、Stardog 等）註冊為可查詢的數據來源 (REQ-297)。

查詢必須為 `SELECT` 查詢。`SELECT` 子句中的變數名稱會自動成為欄位名稱 (REQ-297)。

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

兩個連接器均使用 API 數據來源快取管線——結果會以可設定的 TTL 存放於 PostgreSQL，使其可用於跨數據來源的聯邦 JOIN (REQ-295、REQ-297、REQ-299)。

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

### 跨數據來源查詢

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

單一數據來源的部分直接路由 (REQ-027)。跨數據來源的 JOIN 會自動進行型別強制轉換以進行聯邦 (REQ-028、REQ-552)。
</content>
