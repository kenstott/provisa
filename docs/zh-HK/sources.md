# 來源類型 (Source Types)

## 執行模型

每一項查詢最終都是透過聯邦引擎執行，該引擎為所有來源提供聯邦能力。來源依其連線方式分為三個類別。[tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| 類別 | 具備直接驅動程式 | 具備聯邦連接器 | 範例 |
| --- | --- | --- | --- |
| **可直連 (Direct-capable)** | 是 | 是 | PostgreSQL、MySQL、MariaDB、SingleStore、SQL Server、Oracle、DuckDB |
| **僅限聯邦 (Federation only)** | 否 | 是 | Redshift、Druid、Exasol、Hive、Iceberg、Delta Lake、Hive（以 S3 為後端） |
| **直讀 (複本) (Direct-read (replica))** | 是 | 是 | Snowflake、Databricks、ClickHouse——驅動程式讀取數據並落地為一份複本；查詢針對現用引擎中的複本執行 |
| **具體化 → 聯邦** | 否 | 否 | REST/OpenAPI、遠端 GraphQL、gRPC、Neo4j Cypher、SPARQL、WebSocket、RSS、CSV、SQLite、Parquet、Ingest（推送接收器）、GovData、SharePoint、Splunk |

**可直連**來源透過其原生驅動程式執行單一來源查詢（延遲低於 100 毫秒），繞過聯邦引擎（REQ-027、REQ-229）。它們保留完整的連接器支援，並在與其他來源 join 時參與聯邦（REQ-028）。

**僅限聯邦**來源永遠透過聯邦層查詢。不存在直接驅動程式（REQ-229）。

**直讀 (複本)**來源具備一個 DirectDriver，以原生方式讀取數據倉庫（在支援之處採用 Arrow 原生方式），將一份複本落地至現用引擎的具體化儲存區，之後查詢便針對該複本執行。詳見[作為具名來源的數據倉庫](#warehouses-as-named-sources)。

**具體化**來源沒有聯邦連接器。Provisa 會擷取其數據（於啟動時或查詢時），並以 Parquet 格式快取於 S3 或 PostgreSQL 中，使其可供聯邦引擎用於跨來源查詢（REQ-309）。

---

## 所有來源

Provisa 所支援每一種來源類型的參考資料。「直接驅動程式」意指單一來源查詢以原生方式（延遲低於 100 毫秒）針對該來源執行（REQ-027）。「連接器名稱」是該來源參與多來源 JOIN 時所使用的聯邦連接器（REQ-028）。[tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### 關聯式資料庫管理系統 (RDBMS)

| 來源類型 | 直接驅動程式 | 連接器名稱 | 方言 | Mutation |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | 支援 |
| `mysql` | aiomysql | mysql | mysql | 支援 |
| `mariadb` | aiomysql | mariadb | mysql | 支援 |
| `singlestore` | — | singlestore | singlestore | 聯邦式 |
| `sqlserver` | aioodbc | sqlserver | tsql | 支援 |
| `oracle` | oracledb | oracle | oracle | 支援 |
| `duckdb` | duckdb | memory | duckdb | 支援 |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | 支援 |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | 支援 |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | 支援 |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | 支援 |

線路相容的資料庫，重用某個基礎線路的 JDBC 驅動程式、原生非同步驅動程式及方言——CockroachDB、YugabyteDB 及 Greenplum 搭乘 PostgreSQL 線路；TiDB 搭乘 MySQL 線路。它們只需要登記項目，不需要新的連接器程式碼。[tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird`（Firebird 3/4/5）及 `airport`（Arrow Flight 伺服器）是已登記的來源類型，當 DuckDB 為現用引擎時，透過 DuckDB 社群擴充功能就地觸及——沒有直接驅動程式，也沒有聯邦連接器。[tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### 雲端數據倉庫

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| 來源類型 | 直接驅動程式 | 連接器名稱 | 方言 | Mutation | 備註 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | 聯邦式 | 透過 snowflake-connector-python 讀取；落地複本；`account`/`warehouse`/`role` 位於 `federation_hints` 中（REQ-988） |
| `bigquery` | — | bigquery | bigquery | 聯邦式 | 沒有 DirectDriver；透過聯邦引擎或 BigQuery 引擎 ATTACH 觸及 |
| `databricks` | DatabricksDriver | delta_lake | databricks | 聯邦式 | 透過 databricks-sql-connector 讀取（Cloud Fetch、Arrow）；落地複本；`federation_hints` 中必須提供 `http_path`（REQ-987） |
| `redshift` | — | redshift | redshift | 聯邦式 | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | 聯邦式 | Microsoft Fabric Warehouse；經 TDS 的 T-SQL、Azure AD 身分驗證；落地複本（REQ-995） |
| `synapse` | MssqlWarehouseDriver | — | tsql | 聯邦式 | Azure Synapse SQL；經 TDS 的 T-SQL、Azure AD 身分驗證；落地複本（REQ-995） |
| `trino` | SQLAlchemyDriver | — | — | 聯邦式 | 經 SQLAlchemy trino 方言讀取遠端 Trino/Presto 協調器；於任一引擎上落地複本（REQ-994） |

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| 來源類型 | 直接驅動程式 | 連接器名稱 | 方言 | Mutation | 備註 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | 聯邦式 | 透過 clickhouse-connect（HTTP）讀取；`federation_hints` 中的 `secure: "true"` 用於啟用 TLS（REQ-986） |
| `druid` | — | druid | druid | 不支援 | — |
| `exasol` | — | exasol | exasol | 不支援 | — |
| `elasticsearch` | — | elasticsearch | — | 不支援 | 連接器屬性來自該類型的對應 DSL [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | 不支援 | Trino `pinot` 連接器；`pinot.controller-urls` = Pinot 控制器的 host:port [tool-verified: `trino_connectors.py:199`] |

### 數據湖 / 開放式資料表格式

這些來源類型僅限聯邦——沒有直接驅動程式，也沒有方言。[tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| 來源類型 | 連接器名稱 | 時光回溯 | 備註 |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | 支援（`as_of` 引數，REQ-372） | — |
| `delta_lake` | delta_lake | 支援（`as_of` 引數，REQ-372） | — |
| `hive` | hive | 不支援 | — |
| `hive_s3` | hive | 不支援 | 以 S3 為後端的 Hive |

### NoSQL

`mongodb`、`cassandra` 及 `redis` 均有 Trino 連接器（`redis` 由該類型的對應 DSL 建構其屬性）。[tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| 來源類型 | 連接器名稱 | Mutation |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | 不支援 |
| `cassandra` | cassandra | 不支援 |
| `redis` | redis | 不支援 |

### 串流 (Streaming)

| 來源類型 | 機制 | Mutation |
| ------------ | ----------- | ----------- |
| `kafka` | 聯邦式 Kafka 連接器；結構描述來自 Confluent Schema Registry（Avro、Protobuf、JSON Schema）、人手定義，或樣本推斷（REQ-147、REQ-150） | 僅限 sink（REQ-176） |
| `websocket` | 外部 WebSocket 饋送——連線、訂閱、接收事件；結果會被具體化（REQ-338） | 不支援 |
| `rss` | RSS 2.0 / Atom 饋送——輪詢，依 pubDate/updated 設定水位標記；結果會被具體化（REQ-342、REQ-343） | 不支援 |

### 推送接收器 (Push Receiver)

| 來源類型 | 機制 | Mutation |
| ------------ | ----------- | ----------- |
| `ingest` | 外部服務以 POST 方式送入 JSON 事件；結果會被具體化（REQ-331、REQ-335） | 不支援 |

### 圖形與語義

| 來源類型 | 機制 | Mutation |
| ------------ | ----------- | ----------- |
| `neo4j` | 經由 HTTP API 的 Cypher，結果快取於 PostgreSQL（REQ-295） | 不支援 |
| `sparql` | SPARQL 1.1 POST，結果快取於 PostgreSQL（REQ-297） | 不支援 |

### 以檔案為基礎

有兩種機制涵蓋檔案。兩者都使用 `path` 欄位，而非 `host`/`port`。[tool-verified: `provisa/core/models.py`] (REQ-553)

**單一檔案來源**——`sqlite`、`csv`、`parquet` 會將 `path` 指向單一檔案。

| 來源類型 | 傳輸方式 | Mutation |
| --- | --- | --- |
| `sqlite` | 本機 | 支援 |
| `csv` | 本機 | 不支援 |
| `parquet` | 本機、`s3://` | 不支援 |

私有的儲存桶 (bucket) 需要憑證（來自環境變數的 AWS 區域及金鑰）。若要透過 `s3://` 或 `http(s)://` 使用 CSV，或要一次登記多個檔案，請改用 `files` 來源。[tool-verified: `provisa/file_source/source.py`]

**`files` 來源**——將 `path` 指向一個萬用字元 (glob) 樣式，遞迴地爬取，並將該目錄登記為一個聯邦目錄下的一組資料表。它能透過多種傳輸方式讀取多種格式；下方的集合來自該檔案連接器（kenstott/calcite 分支版本）。[tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| 格式 | 傳輸方式 |
| --- | --- |
| CSV、TSV、JSON、YAML、Excel（XLS/XLSX）、Parquet、Arrow，以及轉換為資料表的文件——HTML、Markdown、DOCX、PPTX | 本機檔案系統、HTTP(S)、`s3://`、`hdfs://`、`ftp://`/`ftps://`、`sftp://`、`iceberg://`、SharePoint（REST 及 Microsoft Graph） |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### 可觀測性及其他

`prometheus` 具備一個 Trino 連接器（屬性由該類型的對應 DSL 建構）。`google_sheets` 是一個已登記的來源類型，沒有 Trino 連接器，並透過 API 快取管線進行具體化。[tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| 來源類型 | 連接器名稱 | Mutation |
| ------------ | ----------------- | ----------- |
| `google_sheets` | —（已具體化） | 不支援 |
| `prometheus` | prometheus | 不支援 |

### 企業級 SaaS 連接器

SharePoint 及 Splunk 透過 Apache Calcite 連接器（kenstott/calcite 分支版本）進行登記。兩者皆無直接驅動程式——Provisa 藉由啟動該連接器內建的 Calcite pgwire 伺服器（`pgwire-sharepoint`、`pgwire-splunk`），以一般 PostgreSQL 端點的方式連線至它，並將資料列落地至具體化儲存區以供聯邦使用，藉此完成資料的具體化（REQ-954）。兩個連接器均一律啟用不分大小寫的名稱比對，與各自產品本身不分大小寫的語義一致（REQ-725、REQ-730）。[tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

SharePoint 清單會被列舉為結構描述，並公開為可查詢的資料表（REQ-726、REQ-731）。有兩種身分驗證方式：`CLIENT_CREDENTIALS`（預設）及以 PFX 憑證為基礎的憑證式驗證（REQ-727）。`mapping` 中的機密值，會在送達連接器之前先經由機密引擎解析（REQ-729）。[tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| 來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` 或 `host` | `site-url` | SharePoint 網站網址 |
| `username` | `client-id` | Azure 應用程式用戶端 ID |
| `password` | `client-secret` | Azure 應用程式用戶端密鑰 |
| `database` | `tenant-id` | Azure 租用戶 UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS`（預設）或 `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | 當 `auth_type: CERTIFICATE` 時的 PFX 路徑 |
| `mapping.certificate_password` | `certificate-password` | PFX 密碼 |

當連接器未公開 `information_schema.columns` 時，請透過 `registerTable` mutation，以明確的欄位定義（取自 Microsoft Graph API）登記該資料表（REQ-732）。

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

Splunk 搜尋結果可作為資料表查詢（例如 `internal_server`）（REQ-721）。連接器網址來自 `base_url`，否則會以 `https://{host}:{port}` 建構，預設連接埠為 `8089`（REQ-722）。身分驗證：當 `mapping.use_token` 為 `true`（預設值）時，`password` 會作為 API 權杖傳遞；當為 `false` 時，`username` 及 `password` 會作為個別憑證傳遞（REQ-723）。[tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| 來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`，否則為 `https://host:port`（連接埠預設 8089） |
| `password` | `token` 或 `password` | 當 `use_token: true` 時為 token |
| `username` | `user` | 僅當 `use_token: false` 時 |
| `database` | `app` | 限定於某個 Splunk app |
| `mapping.datamodel_filter` | `datamodel-filter` | 篩選至某個資料模型 |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | 用於自簽憑證（REQ-724） |

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

### API 來源

將任何 HTTP 端點登記為可查詢的資料表。[tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| API 類型 | 探索方式 | 欄位推斷 |
| --------- | ----------- | ----------------- |
| `openapi` | 剖析 OpenAPI 規格（REQ-314、REQ-316） | 基本型別 → 原生型別，物件 → JSONB |
| `graphql_remote` | 結構描述內省 (introspection)（REQ-307、REQ-308） | 基本型別 → 原生型別，物件 → JSONB |
| `grpc_remote` | 伺服器反射 (server reflection)（REQ-322、REQ-325） | 基本型別 → 原生型別，物件 → JSONB |

API 回應會被擷取、快取於 PostgreSQL（TTL 可設定），並公開為 GraphQL 型別（REQ-309、REQ-318、REQ-327）。已快取的資料表與任何其他來源一樣參與聯邦查詢（REQ-313）。

**JSONB 規則**：以 JSONB 儲存的複合欄位（物件、陣列）不可篩選（REQ-119）。子欄位存取在 SQL 中使用 `->>` 擷取（REQ-151）。關聯是使用純量 FK 欄位在資料表之間宣告——JSONB blob 欄位不可作為 join 目標。如需在巢狀欄位上進行篩選或 join，請使用 JSONB 提升 (promotion) 將其轉換為原生純量欄位（REQ-119）。

### GovData

美國政府開放數據。存取權按主題分組劃分。[tool-verified: `provisa/core/models.py` lines 543–609]

每個 `govdata` 來源選擇一個主題。該主題決定會公開哪些 GovData 結構描述。`ref` 及 `geo` 結構描述一律作為連結用結構描述被納入——它們不會按主題個別列出，但一律存在。[tool-verified: `provisa/core/models.py` line 562–563 comment]

| 主題 | 所公開的結構描述 |
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

| 欄位 | 必要 | 預設值 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一識別碼 |
| `subject` | 是 | — | 上述主題值之一 |
| `domain_id` | 是 | — | 此來源所屬的網域 |
| `description` | 否 | `""` | 人類可讀的描述 |

### 數據品質檢查器 (REQ-1443)

數據品質檢查器是一種來源類型，而非一個子系統。它的掃描輸出即是數據：一個檢查結果即是一項觀測值，因此它會經由一般的來源路徑落地，並從其他每一種來源繼承節奏 (cadence)、新鮮度、事件、血緣、治理、RLS、資料表格檢視及匯出功能。[tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

支援兩種，而選擇它們，既是授權條款上的選擇，也是功能上的選擇。

| 來源類型 | 合約方言 | 額外套件 | 授權條款 | 託管雲端層 |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda 合約 YAML | `pip install .[soda]`（`soda-postgres`） | Elastic License 2.0 | 拒絕使用——見下文 |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]`（`great-expectations[postgresql]`） | Apache 2.0 | 允許使用 |

Elastic License 2.0 禁止將該軟體以託管或代管服務的形式提供給第三方，而在 SaaS 層中代表某個租用戶執行 Soda，正正就是這種行為。`config/capabilities.yaml` 以 `soda` 選項上的 `cloud_eligible: false` 標記這項區分，託管層會讀取該旗標。若某個託管部署想要使用 Soda，會改為觸及一個由營運方自行執行、營運方所提供的 Soda 端點。[tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa 不隨附也不連結任何相關套件。掃描在一個子解譯器 (`python -m provisa.dq.worker`) 中執行，那是唯一匯入 `soda_core` 或 `great_expectations` 的地方，因此一個以原始碼形式提供的檢查器永遠不會觸及伺服器行程，而檢查器發生崩潰時，被終止的只是一個子行程 (subprocess)，而非事件迴圈。[tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**該來源指向 Provisa 自身的 pgwire 端點。**正是這一點，讓單一個 postgres 驅動程式得以檢查一個以 Snowflake 或 Iceberg 為後端的資料表：檢查器掃描的是聯邦檢視，而非底層系統。因為政策適用於該連線，掃描身分是明確宣告的，而非繼承而來——一個經過篩選的資料列集合，絕不能讓某項檢查悄悄地通過。

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

**每份合約對應一個結果資料表，而該合約即是全部的登記內容。**該資料表帶有 `dq_contract`——合約文字的逐字內容——除此之外沒有其他任何關於其形狀的資訊。欄位、水位標記及提升 (promotion) 全部都是推導而來的。[tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

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

登記程序從該文字推導出的內容：

- **血緣。**該合約已經指名其目標數據集，因此登記程序會以剖析 SQL 的方式（`extract_inputs`，REQ-939）剖析它，並將其解析至受治理的資料表。單一定義，不存在可能失步的第二份副本。若合約指名了一個未受治理的數據集，會在登記時立即失敗，而不會落地任何沒人要求的資料列。
- **欄位。**結果封套 (envelope) 是檢查器的，而非操作人員的——從 `scan_id` 到 `diagnostics` 共 16 個隨附欄位。已宣告的欄位只會被讀取其 `visible_to`（必須全體一致），之後便會被取代。[tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **水位標記。**`scan_time` 成為水位標記，這使得落地成為一種附加操作 (append)（REQ-982）。掃描歷史會不斷累積，不需要任何歷史子系統。
- **提升 (Promotions)。**`freshness_max_timestamp` 及 `dataset_rows_tested` 會從 `diagnostics` 的 jsonb 中被提升為具型別欄位（REQ-119）。可以像在任何其他 jsonb 欄位上一樣新增更多。[tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

時序機制不引入任何新欄位。`change_signal` 加上 `cache_ttl` 給出輪詢節奏；`mv_debounce_quiet` 及 `mv_debounce_max_delay` 會把上游的一連串爆發式變更收合為一次掃描（REQ-963）；一個日曆粒度會使其變成週期性（REQ-962）；`expected_events` 會讓掃描保持等待，直到其輸入在該時間窗內已是最新（REQ-961）。輪詢迴圈本身就是掃描排程器。

`outcome` 為 `pass`、`fail`、`warn`、`error`、`skipped` 之一。這些都不是一項裁決——若需要強制執行，那是稍後另外的一項宣告：一個 preflight，或是一個建立在已落地結果之上的 MV。由於一個已落地的觀測值不帶有決定性 (determinism) 義務（REQ-964），因此這裡可以容許一些永遠不能放在 preflight 閘門上的非決定性檢查——異常分數、追蹤視窗 (trailing-window) 變化、相對於現在的新鮮度。

該合約是在使用者介面的資料表編輯介面之數據品質面板中撰寫的，而該處的原始合約文字始終是唯一真實來源。一次試跑 (dry run) 會針對現用資料表執行該合約，並顯示結果而不落地——這正是你發現某份合約的資料集名稱解析到了意料之外的地方，而原本除了通過的資料列之外什麼也不會落地的方式。

---

## 自訂連接器 (REQ-1177)

當操作人員在 `config/custom_connectors.yaml` 中為某個新的來源類型宣告一個連接器時，原生的聯邦引擎——Postgres、DuckDB 及 ClickHouse——即取得對該類型的可達性。不需要撰寫任何程式碼。[tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

連接器可擴充性本身早於此功能便已存在。Trino 引擎在其自身層面上早已具備可擴充性——一個依來源類型參數化的通用 JDBC 連接器、一份逐類型的目錄 `.properties` 內容，以及 Provisa 自身的訂製 Trino 連接器外掛程式（Splunk、SharePoint、Calcite）。[tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 把同樣以設定為驅動的可擴充性，帶到這兩個原生、不需叢集的引擎，它們過去只有一組固定的連接器集合。

該設定檔預設為空。內建連接器已涵蓋開箱即用的觸及範圍；此檔案中的一切內容皆由操作人員自行撰寫。[tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] 設定 `PROVISA_CUSTOM_CONNECTORS` 可指向另一個路徑（適用於測試）。

### 描述子種類

| 引擎 | 種類 | 機制 | 描述子所提供的內容 |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED（ISO 標準） | `extension`、`server_options`、`user_mapping`、`supports_import`、`table_options`、`remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`、`probe_symbol`、`attach_template`、`remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + 掃描器檢視 | `extension`、`probe_symbol`、`scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…`（自動公開每一個遠端資料表） | `ch_engine`、`engine_template` |
| `clickhouse` | `clickhouse_table` | 逐資料表的 `CREATE TABLE ENGINE=…`（欄位來自登記冊） | `ch_engine`、`engine_template`（可能帶有 `{table}`） |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`，由 ClickHouse 推斷結構描述 | `ch_engine`、`engine_template` |

**Postgres 是通用的。**SQL/MED 是一個 ISO 標準，因此每一個符合規範的 FDW 都共用相同的 DDL 形狀：`CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`，選擇性的 `CREATE USER MAPPING`，接著是 `IMPORT FOREIGN SCHEMA`（當 `supports_import: true` 時），或是逐資料表明確的 `CREATE FOREIGN TABLE`（當為 `false` 時）。一個 `pg_fdw` 描述子只需提供逐 FDW 而異的部分——擴充功能名稱、伺服器選項鍵、user-mapping 鍵、匯入旗標、資料表選項。因此，任何符合標準的 FDW 都能單憑設定即可驅動。[tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB 支援兩種機制。**透過 ATTACH 公開目錄的擴充功能使用 `duckdb_attach`；公開一個可讀資料表函式的擴充功能則使用 `duckdb_scan`。不符合這兩種模式的擴充功能則不受支援。[tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse 支援三種機制**，各對應一種整合引擎形狀：一個自動公開每個遠端資料表的關聯式 DATABASE 引擎（`clickhouse_database`，例如 Redis/MySQL）、一個由登記冊提供欄位的逐資料表引擎（`clickhouse_table`，例如 JDBC/ODBC 橋接器——`engine_template` 可帶有一個由執行期綁定的 `{table}` 佔位符），以及一個由 ClickHouse 推斷結構描述的檔案/lake/URL 引擎（`clickhouse_scan`，例如 HDFS/URL）。SQLite（DATABASE 引擎、檔案、無需伺服器）及 Hudi（lakehouse、零複製）為開箱即用支援。[tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

未知的 `kind` 值會在啟動時立即失敗——描述子中的錯字絕不能悄悄地讓某個來源類型變得無法觸及。[tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### 探測閘控 (Probe gating)

可用性是在 attach 時，依各引擎標準的探索目錄進行驗證：

- **Postgres**——先檢查 `pg_extension`，再檢查 `pg_available_extensions`。[tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB**——執行 `INSTALL`/`LOAD`，並在 `duckdb_functions()` 中檢查已宣告的 `probe_symbol`。[tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse**——在 `system.table_engines` 中檢查已宣告的 `ch_engine`；若在建置中缺席，會立即失敗。[tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

一個已宣告但無法安裝的擴充功能會立即失敗。不會悄悄略過，也不會有任何回退機制。探測失敗的連接器，對該部署而言就是未啟用。

### 樣板 (Template) 變數

每一個 `server_options` 值、`user_mapping` 值、`attach_template` 及 `scan_template`，都可以使用 `{field}` 佔位符。可用的欄位：[tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`、`{host}`、`{port}`、`{database}`、`{username}`、`{password}`、`{path}`、`{schema_name}`、`{table_name}`，以及來自 `federation_hints` 的任何鍵。DuckDB 的 attach 樣板還會另外收到 `{alias}`——Provisa 指派給已附加資料庫的內部目錄別名。

樣板中若參照了一個未知的欄位，會在 attach 時立即失敗，在損壞的 DDL 送達引擎之前，先揭露出描述子與來源之間的不一致。

### 範例

**Postgres——經由 `mongo_fdw` 存取 MongoDB（不匯入結構描述；欄位逐資料表提供）**

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

**DuckDB——經由 `read_xlsx` 存取 Excel 檔案（掃描資料表函式）**

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

有了上述任一描述子後，以已宣告的 `source_type` 登記一個來源，即會（在探測成功的前提下）路由經由該自訂連接器。不需要任何其他設定變更。

---

## 作為具名來源的數據倉庫 {#warehouses-as-named-sources}

Snowflake、Databricks 及 ClickHouse 可以登記為具名來源，與哪一個聯邦引擎為現用引擎無關。[tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

登記後，Provisa 會透過該來源的 DirectDriver 讀取該數據倉庫，並將一份複本落地至現用引擎的具體化儲存區。查詢隨後便針對該複本執行。這與傳統的可直連路徑（asyncpg、aiomysql）不同——在傳統路徑中，引擎完全被繞過——在這裡，引擎依然執行該查詢，但針對的是一份本機複本，而不是每次請求都經由線路連往數據倉庫。

在數據倉庫支援之處，讀取採 Arrow 原生方式：Databricks 使用 Cloud Fetch，Snowflake 使用 `fetch_arrow_table`，而 ClickHouse 使用原生欄式 HTTP 介面。

標準 `host`/`port`/`username`/`password` 欄位無法承載的擴充連線參數，會放在 `federation_hints` 中：

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

以具名來源登記，與選擇同一個數據倉庫作為聯邦引擎，兩者互不相干。一個位於 DuckDB 引擎上的 Snowflake 來源，會把複本落地至 DuckDB，而不是 Snowflake。

雲端物件/數據湖數據（位於 S3 / GCS / R2 上的 parquet、csv、iceberg、delta_lake 檔案）是一種獨立的來源類型，當現用引擎具備該類型的 ATTACH 連接器時，會就地附加。不會落地任何複本——引擎直接掃描物件儲存。這些來源的憑證同樣放在 `federation_hints` 中：

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

## 來源設定欄位

所有來源共用一組共通欄位。[tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| 欄位 | 必要 | 預設值 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一識別碼；英數字加連字號/底線 |
| `type` | 是 | — | 來源類型（見上方各表） |
| `host` | 否 | `""` | 主機名稱或 IP |
| `port` | 否 | `0` | 連接埠號 |
| `database` | 否 | `""` | 資料庫名稱 |
| `username` | 否 | `""` | 使用者名稱 |
| `password` | 否 | `""` | 密碼；使用 `${env:VAR}` 進行機密解析 |
| `path` | 否 | `null` | 檔案型及物件/數據湖來源的檔案路徑或雲端 URI |
| `base_url` | 否 | `null` | OpenAPI 來源的基底網址 |
| `pool_min` | 否 | `1` | 連線池最小值（REQ-052） |
| `pool_max` | 否 | `5` | 連線池最大值（REQ-052） |
| `use_pgbouncer` | 否 | `false` | 是否透過 PgBouncer 路由連線（REQ-053） |
| `pgbouncer_port` | 否 | `6432` | PgBouncer 連接埠（REQ-053） |
| `cache_enabled` | 否 | `true` | 是否啟用 API 回應快取 |
| `cache_ttl` | 否 | `null` | 快取 TTL（秒）；為 null 時繼承全域預設值 |
| `cache_catalog` | 否 | `null` | API 快取所用的聯邦目錄；預設為該來源自身的目錄 |
| `cache_schema` | 否 | `api_cache` | 快取目錄中的結構描述 |
| `naming_convention` | 否 | `null` | 為此來源覆寫全域命名慣例（REQ-194） |
| `federation_hints` | 否 | `{}` | 傳遞給聯邦引擎的工作階段屬性，以及數據倉庫來源的擴充連線參數（REQ-278、REQ-281） |
| `mapping` | 否 | `{}` | NoSQL 及 SaaS 來源的特定類型連接器設定（例如 SharePoint 的 `auth_type`、Splunk 的 `use_token`）（REQ-251） |
| `allowed_domains` | 否 | `[]` | 將來源限制於特定網域；空值 = 不受限制 |
| `description` | 否 | `""` | 人類可讀的描述 |

---

## Kafka 來源

Kafka 主題 (topic) 是在 `kafka_sources` 之下另行設定的，以已登記的 `kafka` 來源之 `id` 為索引鍵。[tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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
| `id` | 必須與某個 `type: kafka` 來源的 `id` 相符 |
| `topics[].id` | 此主題在 Provisa 內部的邏輯名稱 |
| `topics[].topic` | Kafka 主題名稱 |
| `topics[].domain_id` | 此主題所屬的網域 |
| `topics[].description` | 人類可讀的描述 |
| `topics[].default_window` | 視窗式查詢的預設時間視窗（例如 `1h`）（REQ-148） |
| `topics[].columns` | 該主題結構描述的欄位定義（REQ-150） |

---

## 欄位可視性

每個欄位上的 `visible_to` 欄位，是一份可以看見該欄位的角色 ID 清單。[tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

未列於某個角色 `visible_to` 清單中的欄位，不會出現在該角色的 GraphQL 結構描述中，也無法在篩選條件中被查詢或參照（REQ-039）。

---

## 關聯 (Relationships)

關聯連接兩個已登記的資料表，並在 GraphQL 中以巢狀欄位的形式呈現。[tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| 欄位 | 必要 | 說明 |
| ------- | ---------- | ------------- |
| `id` | 是 | 此關聯的唯一識別碼 |
| `source_table_id` | 是 | 持有外部索引鍵的資料表 |
| `target_table_id` | 是 | 被參照的資料表；對於計算式關聯則為空 |
| `source_column` | 是 | 來源資料表上的欄位 |
| `target_column` | 是 | 目標資料表上的欄位；對於計算式關聯則為空 |
| `cardinality` | 是 | `many-to-one` 或 `one-to-many`（REQ-019） |
| `materialize` | 否 | 為跨來源 join 自動建立一個具體化檢視（REQ-158） |
| `refresh_interval` | 否 | MV 重新整理間隔（秒）（預設：300） |
| `target_function_name` | 否 | 計算式關聯所用的資料庫函式名稱 |
| `function_arg` | 否 | 哪一個函式引數接收來源欄位的值 |
| `alias` | 否 | 人類可讀的關聯類型（例如 `WORKS_FOR`） |
| `graphql_alias` | 否 | 命名此關聯在父型別上公開的 SDL 欄位。若未提供，該名稱會由目標資料表的 `field_name` 及關聯基數推導而得。[tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | 否 | 當為 `true` 時，將此關聯排除於 Cypher 圖形邊之外 |
| `source_json_key` | 否 | 在 JOIN 之前，從來源欄位中擷取此鍵作為一個 JSON 物件 |

基數 (Cardinality) 值 [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]：

- `many-to-one`——每一列來源資料列對應一列目標資料列（FK 對應至 PK）
- `one-to-many`——每一列來源資料列對應多列目標資料列（上述關係的反向）

---

## 行級安全規則

RLS 規則會在查詢時注入 `WHERE` 子句，範圍限定於某個角色，並可選擇性地限定於某個資料表或網域。[tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

當同一個角色同時存在網域層級及資料表層級的規則時，資料表層級的規則優先（REQ-403）。

| 欄位 | 必要 | 說明 |
| ------- | ---------- | ------------- |
| `table_id` | 條件性 | 此規則所套用的資料表；與 `domain_id` 互斥 |
| `domain_id` | 條件性 | 此規則所套用的網域；適用於該網域內所有資料表（REQ-402） |
| `role_id` | 是 | 此規則所適用的角色 |
| `filter` | 是 | 注入 `WHERE` 的 SQL 判斷式；可參照工作階段變數（REQ-041） |

---

## 函式與 Webhook

### 資料庫函式

追蹤一個資料庫函式，並將其公開為一個 GraphQL 查詢或 mutation。[tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

資料庫來源也可以從供應商目錄（`pg_proc`、`information_schema.routines`，或對等的供應商目錄）自動探索其儲存程序及函式，省去逐一手動登記的需要。探索過程會讀取 `prokind` 及 `provolatile`：不可變/穩定的函式會登記為參數化的關聯（程序引數會成為查詢參數，形狀與 OpenAPI GET 資料表相同），而易變的程序則會登記為 mutation/已追蹤函式。已探索到的常式，與手動登記的常式一樣，同樣流經第二階段治理。[tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

| 欄位 | 必要 | 預設值 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `name` | 是 | — | GraphQL 欄位名稱 |
| `source_id` | 是 | — | 包含該函式的來源 |
| `schema` | 否 | `public` | 資料庫結構描述 |
| `function_name` | 是 | — | 實際的資料庫函式名稱 |
| `returns` | 是 | — | 該函式所傳回、已登記的資料表 ID（REQ-207） |
| `arguments` | 否 | `[]` | 一組 `{name, type}` 引數定義（REQ-211） |
| `visible_to` | 否 | `[]` | 可呼叫此函式的角色 |
| `writable_by` | 否 | `[]` | 可以 mutation 形式呼叫此函式的角色 |
| `domain_id` | 否 | `""` | 此函式所屬的網域 |
| `description` | 否 | `null` | GraphQL 欄位描述 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"`（REQ-205） |

### Webhook

將一個外部 HTTP 端點公開為一個 GraphQL 查詢或 mutation。[tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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

| 欄位 | 必要 | 預設值 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `name` | 是 | — | GraphQL 欄位名稱 |
| `url` | 是 | — | Webhook 端點網址 |
| `method` | 否 | `POST` | HTTP 方法 |
| `timeout_ms` | 否 | `5000` | 請求逾時時間（毫秒） |
| `returns` | 否 | `null` | 已登記的資料表 ID，或 null 表示內嵌型別 |
| `inline_return_type` | 否 | `[]` | 一組 `{name, type}` 欄位，用於自訂回傳形狀（REQ-210） |
| `arguments` | 否 | `[]` | 一組 `{name, type}` 引數定義 |
| `visible_to` | 否 | `[]` | 可呼叫此 webhook 的角色 |
| `domain_id` | 否 | `""` | 此 webhook 所屬的網域 |
| `description` | 否 | `null` | GraphQL 欄位描述 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"` |

---

## 身分驗證

身分驗證設定於 `auth` 鍵之下。[tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| 提供者 | 說明 |
| ---------- | ------------- |
| `none` | 無身分驗證；所有請求均視為 `default_role` |
| `firebase` | Firebase Authentication；需要 `project_id` 及 `service_account_key`（REQ-121） |
| `keycloak` | Keycloak OIDC（REQ-122） |
| `oauth` | 通用 OAuth 2.0（REQ-123） |
| `simple` | 不透過外部提供者的使用者名稱/密碼（REQ-124） |

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

`assignments_source: claims` 從 JWT 聲明中讀取角色指派。`assignments_source: provisa` 則從 Provisa 自身的指派儲存區讀取。[tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## 執行路由

**直接執行**——單一來源的關聯式資料庫查詢，會路由至原生驅動程式，以取得低於 100 毫秒的延遲（REQ-027）。來源需要同時具備 `SOURCE_TO_DIALECT` 項目及 `SOURCE_TO_CONNECTOR` 項目，才能支援此路徑（REQ-229）。

**聯邦式執行**——多來源查詢，以及沒有直接驅動程式的來源，會路由經由聯邦引擎（REQ-028）。Provisa 內含一個內嵌的聯邦引擎；大規模部署時可指向你自己相容的叢集（REQ-226）。

**統計資料**——在登記時，Provisa 會針對每個已發佈的資料表執行 `ANALYZE`，以初始化成本導向優化器（資料列數、null 比例、相異值數量、最小/最大值）。失敗會被記錄，且不會阻擋登記（REQ-275）。

---

## 圖形與語義來源

### Neo4j

登記一個 Neo4j 圖形資料庫作為可查詢的來源。數據管家撰寫用來投影純量值的 Cypher 查詢；Provisa 會快取結果，並將其公開為 GraphQL 型別（REQ-295）。

Cypher 查詢必須在 `RETURN` 子句中使用屬性存取子（`RETURN n.id AS id, n.name AS name`）——若傳回節點物件，會在登記時被拒絕（REQ-296）。

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

預覽端點（`POST /admin/sources/neo4j/{id}/preview`）會傳回範例資料列，並在該 Cypher 傳回節點物件時阻擋登記（REQ-296）。

### SPARQL

登記任何符合 SPARQL 1.1 規範的三元組儲存庫（Apache Jena Fuseki、Virtuoso、Stardog 等）作為可查詢的來源（REQ-297）。

查詢必須是 `SELECT` 查詢。`SELECT` 子句中的變數名稱會自動成為欄位名稱（REQ-297）。

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

兩個連接器都使用 API 來源快取管線——結果儲存於 PostgreSQL 中，TTL 可設定，使其可用於跨來源的聯邦 JOIN（REQ-295、REQ-297、REQ-299）。

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

單一來源的部分會直接路由（REQ-027）。跨來源 JOIN 會以自動型別轉換進行聯邦處理（REQ-028、REQ-552）。
