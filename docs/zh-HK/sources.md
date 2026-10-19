# 數據來源類型 {: #source-types }
## 執行模型 {: #execution-model }

每個查詢最終都要經過聯邦引擎，正是它令任何數據來源都能與其他數據來源聯結。數據來源怎樣到達那裏，取決於 Provisa 怎樣觸及它，據此每個數據來源歸入五組之一。

| 類別 | 有直接驅動程式 | 有聯邦連接器 | 例子 |
| --- | --- | --- | --- |
| **可直接執行** | 是 | 是 | PostgreSQL、MySQL、MariaDB、SingleStore、SQL Server、Oracle、DuckDB |
| **僅聯邦** | 否 | 是 | Redshift、Druid、Exasol、Hive、Iceberg、Delta Lake、Hive（S3 支援） |
| **直接讀取（副本）** | 是 | 是 | Snowflake、Databricks、ClickHouse——驅動程式讀取數據並落地一份副本；查詢在啟用中的引擎裡針對該副本執行 |
| **直接（無跨來源連接器）** | 是 | 否 | Athena——單一來源語句完整在 Athena 中執行；跨來源聯結會落地完整副本 |
| **具體化 → 聯邦** | 否 | 否 | REST/OpenAPI、遠端 GraphQL、gRPC、Neo4j Cypher、SPARQL、WebSocket、RSS、CSV、SQLite、Parquet、Ingest（推送接收器）、GovData、SharePoint、Salesforce、ServiceNow、雲端資源清單、Splunk、Qdrant、Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

**可直接執行**的數據來源經自己的驅動程式回答只涉及它們的查詢，耗時低於 100 毫秒，亦不經過聯邦引擎。它們保有完整的連接器，所以仍然經引擎與其他數據來源聯結。

**僅聯邦**的數據來源沒有直接驅動程式。每個查詢都經聯邦層到達它們。

**直接讀取（副本）**的數據來源具備一個 DirectDriver，原生地自倉庫讀取（可行時以 Arrow 原生方式），並把一份副本落地到啟用中引擎的具體化儲存區。之後查詢針對該副本執行。見 [作為具名數據來源的倉庫](#warehouses-as-named-sources)。

**直接（無跨來源連接器）**的數據來源具備一個 DirectDriver，在數據來源內完整執行單一來源語句，每個篩選條件、資料行與限制都會下推。它們沒有 Trino 連接器，所以當語句把其中一個與其他數據來源聯結時，驅動程式會讀取數據並落地一份副本，聯結在啟用中的引擎裏針對該副本執行。

**具體化**的數據來源同樣沒有聯邦連接器。Provisa 於啟動時或查詢時取得其數據，並以 Parquet 快取於 S3 或 PostgreSQL 中。聯邦引擎從那裏觸及它們，進行跨來源查詢。

---

## 所有數據來源 {: #all-sources }
Provisa 註冊了 **73** 種數據來源類型，即 Sources 表單所提供的類型。下方表格涵蓋全部 73 種；索引即為計數。

| # | 群組 | 數據來源類型 |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [雲端數據倉庫](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [分析／OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [數據湖／開放表格式](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [程式碼來源](code-sources.md) | `node`, `python` |
| 41–43 | [串流](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [推送接收器](#push-receiver) | `ingest` |
| 45–46 | [圖形與語意](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [檔案式](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [可觀測性與其他](#observability-other) | `google_sheets`, `prometheus` |
| 54–61 | [企業 SaaS](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `jira`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 62–67 | [API 數據來源](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [數據品質檢查器](#data-quality-checkers) | `soda`, `great_expectations` |
| 71 | [數據剖析器](#data-profiler) | `data_profiler` |
| 72 | [Kaggle 數據集](#kaggle-datasets) | Kaggle（經 Sources 表單暫存；註冊為 `files` 數據來源——見 [Kaggle 數據集](#kaggle-datasets)） |
| 73 | [Wikipedia](#wikipedia) | Wikipedia（註冊為爬取所指定頁面的 `files` 數據來源——見 [Wikipedia](#wikipedia)） |

Provisa 支援的每一種數據來源類型的參照。「直接驅動程式」指單一來源查詢原生地針對該數據來源執行（低於 100 毫秒）。「連接器名稱」是該數據來源參與多來源 JOIN 時所用的聯邦連接器。

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
| `saphana` | SQLAlchemy（`hana+hdbcli`） | — | — | 否 |
| `hiveserver2` | impyla（HiveServer2 over Thrift） | — | hive | 否 |

協定相容的資料庫重用某個基礎協定的 JDBC 驅動程式、原生非同步驅動程式及方言。CockroachDB、YugabyteDB 及 Greenplum 使用 PostgreSQL 協定，TiDB 使用 MySQL 協定。它們只需要登錄項目，不需要新的連接器程式碼。

`firebird`（Firebird 3/4/5）及 `airport`（Arrow Flight 伺服器）是已登記的數據來源類型，當 DuckDB 是啟用中的引擎時，由 DuckDB 社群擴充功能就地觸及。它們沒有直接驅動程式，亦沒有聯邦連接器。
#### `saphana`

SAP HANA 經它自己的驅動程式讀取，並複製到啟用中引擎的儲存區，因此在每個引擎上都可觸及。表單需要主機、連接埠（預設 39015）、使用者名稱、密碼與資料庫；在多租用戶系統中，資料庫指定租用戶。Register Table 列出使用者可讀取的結構描述，不含 HANA 的系統結構描述。

#### `hiveserver2`

經 Thrift 直接讀取的 HiveServer2 端點，適用於只有該端點可達的 Hive 部署。它有別於 `hive` 與 `hive_s3`：後兩者讀取倉庫的檔案與 metastore，從不經過 HiveServer2。資料列經該端點讀取，並複製到啟用中引擎的儲存區。表單需要主機、連接埠（預設 10000）、使用者名稱、密碼與資料庫；`federation_hints.auth_mechanism` 選擇驗證方式（預設 `PLAIN`，例如 Kerberos 用 `GSSAPI`）。

### 雲端數據倉庫 {: #cloud-data-warehouses }

| 數據來源類型 | 直接驅動程式 | 連接器名稱 | 方言 | 變更操作 | 備註 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | 聯邦 | 經 snowflake-connector-python 讀取；落地副本；`federation_hints` 中的 `account`／`warehouse`／`role` |
| `bigquery` | — | bigquery | bigquery | 聯邦 | 無 DirectDriver；經聯邦引擎或 BigQuery 引擎 ATTACH 觸及 |
| `databricks` | DatabricksDriver | delta_lake | databricks | 聯邦 | 經 databricks-sql-connector 讀取（Cloud Fetch、Arrow）；落地副本；`federation_hints` 中必填 `http_path` |
| `redshift` | — | redshift | redshift | 聯邦 | — |
| `athena` | AthenaDriver | — | athena | 部分（INSERT 始終可用；UPDATE/DELETE/MERGE 僅限 Iceberg 表） | 無伺服器 S3 查詢；單資料來源語句完整在 Athena 中執行；跨源聯結落地副本；`federation_hints` 中必填 `region` |
| `fabric` | MssqlWarehouseDriver | — | tsql | 聯邦 | Microsoft Fabric Warehouse；T-SQL over TDS、Azure AD 驗證；落地副本 |
| `synapse` | MssqlWarehouseDriver | — | tsql | 聯邦 | Azure Synapse SQL；T-SQL over TDS、Azure AD 驗證；落地副本 |
| `trino` | SQLAlchemyDriver | — | — | 聯邦 | 經 SQLAlchemy trino 方言讀取遠端 Trino/Presto 協調器；在任何引擎上落地副本 |

#### `athena`

Amazon Athena 是建基於 S3 數據的無伺服器查詢服務，按掃描的位元組收費。只涉及 Athena 資料表的語句直接交給 Athena，篩選條件、資料行清單及 `LIMIT` 都會下推，所以 Athena 只掃描並收取該語句所需位元組的費用。把 Athena 資料表與其他任何數據來源聯結的語句，會先從 Athena 落地一份完整副本。這會讀取整個資料表，Athena 會為此收費；之後聯結針對該副本執行。數據來源的 Replicate 及負載保護設定決定落地的頻率，而本說明是關於這項費用的唯一提醒。

**探索。** Register Table 呼叫 Athena 的中繼資料 API：`ListDatabases`、`ListTableMetadata` 及 `GetTableMetadata`。這些呼叫不執行任何查詢，所以列出資料庫及資料表、讀取資料表的資料行都不產生按位元組計的費用。只有在您選取某個資料表時才會取得它的資料行，分區鍵排在一般資料行之後。`array<…>`、`map<…>` 及 `struct<…>` 會變成 `text`，未知類型會被拒絕並指明其名稱。

**來源欄位。**

| 欄位 | 位置 | 必填 | 預設值 | 說明 |
| --- | --- | --- | --- | --- |
| `database` | 來源 | 是 | — | 預設查詢的 Glue 資料庫 |
| `username` | 來源 | 否 | — | AWS 存取金鑰 ID；可為 `${secret:…}` 或 `${env:…}` |
| `password` | 來源 | 否 | — | AWS 存取金鑰 Secret；可為 `${secret:…}` 或 `${env:…}` |
| `region` | `federation_hints` | 是 | — | AWS 區域，例如 `us-east-1` |
| `workgroup` | `federation_hints` | 否 | `primary` | Athena 工作群組 |
| `s3_staging_dir` | `federation_hints` | 否 | 工作群組預設值 | Athena 寫入查詢結果的位置；工作群組未設定時必填 |
| `catalog` | `federation_hints` | 否 | `AwsDataCatalog` | Athena 資料目錄 |
| `role_arn` | `federation_hints` | 否 | — | 每次呼叫時承擔的 IAM 角色 ARN |

`username` 和 `password` 均為空時，使用 boto3 的憑證鏈：執行個體角色、環境變數（`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`）或命名 profile。只填其中一個將被拒絕。

**寫入。** 每個已註冊的資料表都接受 `INSERT`。`UPDATE`、`DELETE` 及 `MERGE` 只適用於 Iceberg 資料表，格式在註冊時記錄（`registered_tables.table_format`）。在 Hive 資料表上，要求 `UPDATE`、`DELETE` 或 `MERGE` 的寫入會被拒絕，並指明資料表及操作。Athena 不傳回寫入的資料列，所以 `RETURNING` 子句會被拒絕，GraphQL 亦不為這些資料表提供變更欄位。寫入途徑是 `INSERT`、`UPDATE`、`DELETE` 及 `COPY`。`INSERT … ON CONFLICT`、`UPDATE … FROM`、`DELETE … USING` 及 `TRUNCATE` 會被拒絕，並指明其形式。

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  username: ${env:ATHENA_ACCESS_KEY_ID}
  password: ${env:ATHENA_SECRET_ACCESS_KEY}
  federation_hints:
    region: us-east-1
    workgroup: primary
    s3_staging_dir: s3://my-bucket/athena-results/
```

每次呼叫時承擔角色（基礎工作階段使用憑證鏈）：

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  federation_hints:
    region: us-east-1
    role_arn: arn:aws:iam::123456789012:role/AthenaQueryRole
    s3_staging_dir: s3://my-bucket/athena-results/
```

### MotherDuck

`motherduck`是託管在 MotherDuck 上的 DuckDB 數據庫。在 DuckDB 引擎上它會就地附加；在 Postgres 引擎上它仍是 `pg_duckdb` 外部數據包裝器（foreign data wrapper）連接器。

**連接。** `database` 指定 MotherDuck 數據庫，或按共享地址指定一個共享。`password` 是 MotherDuck 權杖，與任何數據來源密鑰一樣解析。

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

DuckDB 引擎執行 `ATTACH 'md:<database>'`；對於共享，`database` 是共享地址（`_share/...`），因此附加的是 `md:_share/...`。

- **權杖處理。** 引擎以 `SET GLOBAL motherduck_token = ?` 登入，權杖作為綁定值傳入。它不會出現在任何語句的文字中，因此任何日誌、追蹤或錯誤都不包含它。
- **沒有權杖。** 沒有 `password` 的數據來源在附加任何內容之前即被按名拒絕(`a MotherDuck source signs in with its token (the password field); none is set`)。若沒有這項檢查，擴充功能會啟動互動式登入並等待約一分鐘。沒有 `database` 的數據來源同樣會被拒絕。
- **每個引擎一個帳戶。** 一個引擎登入一個 MotherDuck 帳戶。權杖不同的第二個 MotherDuck 數據來源會被按名拒絕："one DuckDB engine reads one MotherDuck account"。
- **寫入**經由引擎進行，與 Postgres 引擎相同。

**混合（雙重）執行**毋須任何設定。在 DuckDB 引擎上，所有數據來源都附加在同一個連線上，因此將 DuckDB 檔案數據來源的表與 MotherDuck 表聯結的語句會作為一條 DuckDB 語句執行，由 MotherDuck 決定每個部分在哪裡執行。

> **數據移動。** 在雙重執行下，MotherDuck 可能會把本地列移到它那一側，用於它執行的部分。因此這樣的語句可能把本地檔案中的數據傳送到機構的 MotherDuck 帳戶。除非可以接受把數據傳送到該帳戶，否則不要將存有必須留在節點上的數據的檔案與 MotherDuck 數據來源聯結。

MotherDuck 數據來源只由 DuckDB 和 Postgres 引擎提供服務。其他引擎都沒有它的連接器，也沒有任何機制把它的表作為副本落地到那裡。

### 分析／OLAP {: #analytics-olap }

| 數據來源類型 | 直接驅動程式 | 連接器名稱 | 方言 | 變更操作 | 備註 |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | 聯邦 | 經 clickhouse-connect（HTTP）讀取；TLS 時在 `federation_hints` 中設 `secure: "true"` |
| `druid` | — | druid | druid | 否 | — |
| `exasol` | — | exasol | exasol | 否 | — |
| `elasticsearch` | HTTP（原生引擎） | elasticsearch (Trino) | — | 否 | 在 Trino 上由連接器讀取，其屬性來自該類型的映射 DSL；在其他每個引擎上，Provisa 經 HTTP 讀取索引（Register Table 取得索引與映射，落地時用 scroll 讀取）並落地資料列 |
| `pinot` | — | pinot | — | 否 | Trino `pinot` 連接器；`pinot.controller-urls` = Pinot 控制器的 host:port |

### 數據湖／開放表格式 {: #data-lake-open-table-formats }
這些數據來源類型僅供聯邦使用——沒有直接驅動程式，沒有方言。

| 數據來源類型 | 連接器名稱 | 時間旅行 | 備註 |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | 是（`as_of` 引數） | — |
| `delta_lake` | delta_lake | 是（`as_of` 引數） | — |
| `hive` | hive | 否 | — |
| `hive_s3` | hive | 否 | S3 支援的 Hive |

### NoSQL

`mongodb`、`cassandra` 與 `redis` 有 Trino 連接器（`redis` 自該類型的映射 DSL 建構其屬性）。

| 數據來源類型 | 連接器名稱 | 變更操作 |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | 否 |
| `cassandra` | cassandra (Trino)；在其他每個引擎上經 cassandra-driver 做 CQL 讀取 | 否 | Keyspace 即結構描述；Register Table 列出 keyspace 的資料表，並依叢集的結構描述中繼資料為資料行定型（分區鍵作為主鍵）；`cassandra` extra 會安裝驅動程式 |
| `redis` | redis (Trino)；在其他每個引擎上以不經 HTTP 的 redis-py 讀取 | 否 | 鍵前綴 `<table>:*` 是一張表，一個 hash 是一列；Register Table 列出現有的前綴，並依前綴的 hash 為資料行定型（`mapping.tables` 條目可覆寫模式、鍵資料行、值類型與資料行） |

**託管引擎上的 Redis。** 當組織的引擎是託管平台預置的引擎之一時，Redis 不可用。Trino 從目錄自身資料夾中的描述檔案讀取 Redis 目錄的資料表，而託管引擎在執行期間無法被賦予這樣的資料夾。在那裡儲存或套用 Redis 數據來源會被按名稱拒絕（HTTP 422，`sources.redis_needs_engine_files`）；自行託管的部署按上文所述讀取 Redis。

### Qdrant

`qdrant`由 Qdrant 自家的用戶端 `qdrant-client` 讀寫。Qdrant **僅限即時**：這是來源類型的屬性，不是設定。它以自己的向量索引和 Query API 作答，任何副本都無法給出同樣的答案，因此其表格永遠不會被複製。Qdrant 來源及其表格沒有 Replicate 選項；對其設定 `replicate` 值或 `row_materialize` 會被拒絕。

**連線。** `host` 是實例的 URL 及埠（`https://<cluster>.cloud.qdrant.io:6333`，或自行託管的 `http://qdrant:6333`）；`password` 是其 API 金鑰，是在讀取該來源時解析的密鑰參照，沒有金鑰的實例則留空。`federation_hints.prefer_grpc: true` 改為透過 Qdrant 的 gRPC 埠讀取，而非 REST。該來源只有一個 schema，即 `default`。

**集合表格。** 集合（collection）就是一張表格。Register Table 提供集合的欄位：`id`（點 ID，即其鍵，路徑 `@id`）、payload 的欄位（已建立索引的欄位按其索引定型，其他欄位按抽樣點的提示定型；巢狀欄位連同路徑展平），以及每個向量一個 `json` 欄位（路徑 `@vector`，命名向量則為 `@vector.<name>`），預設不選取。向量欄位會指明它所存嵌入所屬的已登記嵌入模型，從維度等於該向量大小的模型中選擇（在 Register Table 中，以及編輯器的「類型與鍵」分頁）；選擇模型即登記該向量。

**針對單一 Qdrant 來源的語句。** 只讀取一個集合的語句，在 Qdrant 的 API 能夠精確表達它時於 Qdrant 中執行：

- payload 條件轉為 Qdrant 篩選器（`=`、`<>`、`IN`、數字和日期的範圍、`IS NULL`、`AND`、`OR`、`NOT`）；保持 SQL 的 NULL 語義，因此 `lang <> 'en'` 會略過沒有 `lang` 的點；
- 僅對 `id` 的條件（`=` 或 `IN`）是一次 `retrieve`；其他任何讀取是帶篩選的 `scroll`；
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` 是對距離為 Cosine 的向量的一次 `query_points`，篩選器在取 top-k 之前套用。文字查詢由該向量的模型嵌入。答案附有警告：top-k 是近似的，透過 Qdrant 的索引選出。

其他情況（聚合、`GROUP BY`、計算欄位或遮罩欄位、非「最相似優先」的排序）由引擎作答，見下文。

**由引擎作答的語句。** 當語句把集合與另一個來源聯結，或對其欄位做計算時，該集合的那一部分會在該語句執行時從 Qdrant 即時讀取，並以列的形式交給引擎。這一部分包括集合的欄位、語句中僅針對該集合的條件（其列規則也在內），以及在集合於其 `SELECT` 中單獨讀取時，該 `SELECT` 的相似度排序和 `LIMIT`。Qdrant 無法精確表達的部分會被拒絕，並指明表格及原因。沒有條件、沒有鍵、沒有 top-k 的部分會讀取整個集合，同樣被拒絕。針對參數（`$1`）的條件留給引擎處理。

**寫入。** 對集合的 `INSERT`、`UPDATE` 和 `DELETE` 在 Qdrant 中執行：

- `INSERT … VALUES` 指明 `id` 欄位並寫入新點。集合中已有的 `id` 會按名稱被拒絕。該檢查先讀取 id，因此與寫入不是原子的。
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` 是 upsert。語句未設定的欄位保持原值。
- `UPDATE` 和 `DELETE` 作用於其 `WHERE` 選出的點，以 Qdrant 篩選器的形式；沒有篩選器形式的條件會被拒絕。
- 寫入向量欄位的文字由該欄位的模型嵌入。寫入其中的向量必須具有該模型的維度。

**搜尋表格。** 向量搜尋是一張搜尋表格：其 `query_template` 保存一個 Query API 請求。

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` 一律是參數；`using`、`filter`、`limit`、`score_threshold` 和 `model` 為可選。`using`、`filter`、`limit` 或 `score_threshold` 中恰好為 `"@name"` 的值同樣是參數。
- 每個參數都是該表格的參數（`query_arg`），被嚴格轉換為其宣告的類型，並作為 JSON 值放入請求，絕不放入其文字。`query` 的參數是 `array(double)`，且必填。
- 呼叫方像傳遞任何參數一樣傳遞它們：SQL 中為 `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'`，GraphQL 中為欄位參數。
- 未給出的可選參數不做任何篩選：其條件從篩選器中省略。沒有 `limit` 時，搜尋傳回 Qdrant 的預設值，即 10 個點。
- 篩選器由 Qdrant 在選出最近點之前套用，因此是精確的。語句中的其他條件之後才縮小搜尋傳回的點；不會取回更多。
- `score` 是集合自身針對所搜尋向量的度量：對 Cosine 集合而言就是餘弦相似度。Register Table 會列出集合的向量及其大小和距離。
- `model` 指定一個已註冊的嵌入模型（`vector_models`，管理 → AI Models）。此時給定的查詢文字由該模型嵌入，給定的向量必須具有該模型的維度。沒有 `model` 時，查詢必須是向量。

搜尋按每條語句、在其參數下讀取，其列在這些參數下保留一個表格 TTL 的時間。搜尋表格是唯讀的。

### 串流 {: #streaming }
| 數據來源類型 | 機制 | 變更操作 |
| ------------ | ----------- | ----------- |
| `kafka` | 聯邦 Kafka 連接器；結構描述經 Confluent Schema Registry（Avro、Protobuf、JSON Schema）、手動定義或樣本推斷取得 | 否；資料表的即時輸出會發佈至 Kafka |
| `websocket` | 外部 WebSocket 饋送——連線、訂閱、接收事件；結果經具體化 | 否 |
| `rss` | RSS 2.0／Atom 饋送——輪詢，以 pubDate/updated 標記水位；結果經具體化 | 否 |

**託管引擎上的 Kafka。** 在託管平台預置的引擎上，Kafka 數據來源必須指定一個結構描述登錄（`schema_registry_url`）。主題欄位只由模型宣告的數據來源在儲存或套用時會被按名稱拒絕（HTTP 422，`sources.kafka_needs_schema_registry`），因為它的資料表描述需要一個無法交給執行中引擎的資料夾。SASL 憑證以記憶體方式送達這些引擎（參見[安全](security.md#the-federation-engine-trino)）。

### 推送接收器 {: #push-receiver }
| 數據來源類型 | 機制 | 變更操作 |
| ------------ | ----------- | ----------- |
| `ingest` | 外部服務 POST JSON 事件；結果經具體化 | 否 |

### 圖形與語意 {: #graph-semantic }
| 數據來源類型 | 機制 | 變更操作 |
| ------------ | ----------- | ----------- |
| `neo4j` | 經 HTTP API 的 Cypher，結果快取於 PostgreSQL | 否 |
| `sparql` | SPARQL 1.1 POST，結果快取於 PostgreSQL | 否 |

### 檔案式 {: #file-based }
三種機制涵蓋檔案。單一檔案數據來源與檔案爬取器使用 `path` 欄位而非 `host`／`port`。 第三種 [Logs](#logs) 在其 `mapping` 中以萬用字元（glob）指定檔案，而且只讀取 Provisa 伺服器上的檔案。

**檔案數據庫與單一檔案數據來源。** `sqlite`、`duckdb`、`csv` 或 `parquet` 數據來源把 `path` 指向單一檔案。（`duckdb` 數據來源把位置保存在表單的 `database` 欄位中。）

| 數據來源類型 | 位置 | 變更操作 |
| --- | --- | --- |
| `sqlite` | 本機路徑、`https://`, `s3://`, `gs://`, `az://` | 本機檔案上為是；遠端檔案上無 |
| `duckdb` | 本機路徑、`https://`, `s3://`, `gs://`, `az://` | 本機檔案上為是；遠端檔案上無 |
| `csv` | 本機路徑、`https://`, `s3://`, `gs://`, `az://` （DuckDB 引擎） | 否 |
| `parquet` | 本機路徑、`https://`, `s3://`, `gs://`, `az://` （DuckDB 引擎） | 否 |

使用其他任何協定（例如 `ftp://`）的路徑會被拒絕："DuckDB reads no file at a `ftp://` location"。

**遠端位置的憑證。** 把它們放在數據來源的 `federation_hints` 中。每項憑證只作用於該路徑自身的貯體或容器，因此一個數據來源的憑證永遠不會用於另一個數據來源的位置。沒有提供憑證的數據來源按公開位置讀取。

| 位置 | `federation_hints` 鍵 |
| --- | --- |
| `s3://` | `access_key_id`、`secret_access_key`；可選 `region`、`endpoint`（R2 或 MinIO 等 S3 相容儲存） |
| `gs://` | `gcs_access_id`、`gcs_secret_key`（HMAC 金鑰） |
| `az://` | `azure_account_name`, `azure_account_key` |

**遠端 SQLite 與 DuckDB 檔案為唯讀。** DuckDB 引擎以 `READ_ONLY` 就地附加它們，建於其上的表不提供 insert、update 或 delete。在不附加 SQLite 的引擎上，遠端 SQLite 檔案經由一個以唯讀方式附加它的臨時 DuckDB 連線讀取，其資料列照常落地。

**上載檔案。** 在 `sqlite`、`duckdb`、`csv` 或 `parquet` 數據來源的表單中，上載檔案而不是輸入路徑。檔案以串流方式寫入磁碟，從不保存在記憶體中，存放到機構資料夾下的 `files/<source id>/<file name>`，數據來源的路徑隨即指向它。

- **取代。** 再次上載會取代先前的檔案，無論其名稱為何，而且數據來源會重新佈建，使下一次讀取看到新檔案。
- **移除。** 移除數據來源會移除其上載的檔案。其他操作都不會移除它。
- **權限。** 只有具備 `source_registration` 能力的角色才可上載。
- **大小上限。** 設定 `sources.max_upload_bytes`（環境變數 `PROVISA_SOURCES_MAX_UPLOAD_BYTES`）限制檔案大小，預設 1 GiB（1073741824 位元組），最小為 1，於上載開始時讀取。較大的檔案會以 HTTP 413 被拒絕並註明上限，且不保留任何部分。
- **路徑。** 當路徑為該數據來源的上載檔案時，表單的路徑欄位為唯讀。
- **名稱。** 數據來源 id 與檔案名稱只能使用字母、數字、`.`、`_` 與 `-`。
- **儲存。** 上載的檔案與任何機構檔案一樣儲存：位於機構資料夾下，機構資料夾加密時隨之加密，並在共用掛載上對每個節點可見。

**寫入本機 SQLite 或 DuckDB 檔案，以及鎖。** 寫入會取得檔案鎖，而且從不等待。當另一個程序或節點持有該檔案時，寫入會被拒絕，並註明數據來源及其檔案已被鎖定。

- **SQLite。** 寫入設定 `PRAGMA busy_timeout = 0`，因此被鎖定的檔案會立即失敗。
- **DuckDB。** DuckDB 只在唯讀開啟之間共用檔案。因此引擎的附加與數據來源自身的連線都以唯讀方式持有檔案，每個節點可同時讀取。寫入會暫停本程序的持有者，僅為該語句以讀寫方式開啟檔案，然後關閉。若開啟時無法取得鎖，寫入會被拒絕。

**共用掛載。** 在掛載由多個節點共用的加密機構資料夾中（設定 `org_files.shared_mount`），對其中 SQLite 或 DuckDB 檔案的寫入會直接被拒絕，並註明數據來源。資料夾加密層（gocryptfs）不會把檔案鎖傳遞到其他節點，因此沒有任何機制能阻止兩個節點同時寫入。讀取仍然可用。

未加密共用的資料夾必須以能到達每個節點的鎖進行掛載：啟用鎖定的 NFS 掛載，而不是 `nolock`，也不是 `local_lock`。沒有任何探測檢查這一點；這是部署要求。

**檔案爬取器（`files` 數據來源）。** 它把 `path` 指向一個 glob，遞迴爬取，並把該目錄註冊為一個聯邦資料表目錄。它經多種傳輸方式讀取多種格式：

| 格式 | 傳輸 |
| --- | --- |
| CSV、TSV、JSON、YAML、Excel（XLS/XLSX）、Parquet、Arrow，以及轉換成表的文件——HTML、Markdown、DOCX、PPTX | 本機檔案系統、HTTP(S)、`s3://`、`hdfs://`、`ftp://`／`ftps://`、`sftp://`、`iceberg://`、SharePoint（REST 與 Microsoft Graph） |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

任何 `files` 數據來源也可以在其 mapping 中宣告 `crawl`，設定與 [Wikipedia](#wikipedia) 下列出的相同：此時數據來源會先讀取網頁，並把每個 HTML 數據表與每個連結的數據檔案作為表落地到其目錄中。

在 DuckDB 引擎上，`files` 以原生方式讀取，解析出的目錄下每個 `<table>.csv` 對應一個 `read_csv_auto` 掃描器檢視。在沒有自帶 `files` 連接器的引擎上，資料列經 sharepoint 及 splunk 所用、隨連接器提供的同一個 Calcite pgwire 伺服器（`pgwire-file`）落地。見下方的[企業 SaaS 連接器](#enterprise-saas-connectors)。
#### Logs {: #logs }

`logs` 數據來源把 Provisa 伺服器上的日誌檔案變成一張表。你以完整路徑或萬用字元（glob）指定檔案，並選擇其記錄所用的格式。每條記錄成為一個資料列。格式無法讀取的記錄會連同原始文字與原因一併保留，絕不丟棄。

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

mapping 接受四個鍵，其他任何鍵都會依名稱被拒絕：`glob` 與 `format`（皆為必填），以及只有 `regex` 格式接受的 `pattern` 與 `record_start`。

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

在 Sources 表單中，Logs 數據來源在 File 之下顯示為「Log files」。**Preview** 按鈕會在儲存之前顯示萬用字元所匹配的檔案、格式給出的資料行，以及最新檔案的前 20 個資料列。

**萬用字元。** 它必須是完整路徑；`~` 會被展開，相對路徑會被拒絕（「name the files by their full path」）。匹配使用 Python 的 `glob` 非遞迴模式，因此 `**` 的行為與 `*` 相同，只在一層目錄內匹配。只取一般檔案，依名稱排序。空檔案在有內容之前會被略過。 萬用字元可以引用環境變數，例如 `${env:LOG_DIR}/*.log`。資料來源按原樣保存該引用，並在每次讀取檔案時解析它；未設定的變數會被按名稱拒絕。

**每個資料列自帶的資料行。** 這些資料行排在格式給出的資料行之前：

| 資料行 | 類型 | 內容 |
| --- | --- | --- |
| `_file_id` | text | 檔案的身分：其前 4096 個位元組（解壓縮後）的校驗和 |
| `_file_name` | text | 讀取該記錄時所用的路徑 |
| `_line_offset` | bigint | 記錄第一行在解壓縮後文字中的位元組偏移 |
| `_entry` | bigint | 該資料列是記錄中的第幾個項目：除批次記錄（例如 CloudTrail 檔案的 `Records`）外皆為 0 |
| `_raw` | text | 記錄的原始內容 |
| `_parse_error` | text | 記錄或其中某個值無法讀取的原因；讀取成功時為 NULL |
| `_landed_at` | timestamp | 該資料列落地的時間 |

`_file_id`、`_line_offset` 與 `_entry` 構成該資料列的鍵，因此讀取兩次的記錄仍是同一個資料列。

**格式與預設。** 格式下拉選單依系列把格式分組；最後一項 `regex` 是你自己的樣式。

| 系列（表單標籤） | 格式名稱 |
| --- | --- |
| Common formats | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| Databases and runtimes | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| Containers and Linux | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| Network security monitoring | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| Web servers and proxies | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| Your own | `regex` |

共有 56 種內建格式，外加 `regex`。

**格式如何產生資料行。** 格式以四種方式之一取得資料行。

- **固定資料行。** 大多數格式自行宣告資料行與類型。例如 `rfc3164` 給出 `timestamp`、`facility`、`severity`、`host`、`process`、`pid` 與 `message`；`facility` 與 `severity` 由 `<PRI>` 值計算得出。BSD syslog 行不帶年份，因此年份取檔案最後寫入的年份；若該行的月份晚於最後寫入的月份，則取前一年。
- **由記錄命名的欄位。** `jsonl` 與 `logfmt` 的資料行取自檔案開頭若干行中出現的欄位（設定項 `logs.preview_lines`，預設 1000）。每個資料行取所見全部值都適用的最窄類型：先 boolean，然後依次為 integer、float、timestamp 與 json，否則為 text。後續記錄帶有但沒有對應資料行的欄位，會進入一個 `extra` json 資料行。
- **由標頭命名的欄位。** `w3c` 及以它為基礎的預設讀取每個檔案的 `#Fields:` 行；`date` 與 `time` 合併為一個 `timestamp` 資料行。`aws_cloudfront`、`ms_iis`、`ms_windows_firewall`、`ms_exchange_tracking` 與 `zeek_tsv` 以同樣方式讀取以 `#` 開頭的標頭行，`aws_vpc_flow` 則讀取以 `version` 開頭的標頭行。讀到標頭行的位置會隨檔案保存，因此之後繼續讀取的檔案保留其欄位。
- **`regex`。** 每個具名群組（例如 `(?P<level>…)`）是一個 text 類型的資料行。

Register Table 畫面會列出這些資料行及其類型，並標出鍵。 讀取資料列時，每個值都會轉換為其資料行在已註冊表中的類型，因此在那裡更改的類型就是實際套用的類型。資料行無法容納的值變為 NULL，原因寫入 `_parse_error`。

長度超過 `logs.max_line_bytes`（預設 65536）的記錄，其開頭位元組保留在 `_raw` 中，且不會被剖析；`_parse_error` 給出其長度。以 UTF-8 或 UTF-16 位元組順序標記開頭的檔案依該編碼讀取，其他檔案依 UTF-8 讀取。

**跨行記錄。** 帶有記錄起始樣式的格式，以匹配該樣式的一行加上其後所有不匹配的行組成一條記錄。堆疊追蹤會與其例外保持在一起，慢查詢項目也保持完整。第一個匹配之前的行自成一條記錄；格式通常會拒絕它，但該資料列會被保留。帶記錄起始的內建格式有 `rfc3164`、`macos_install`、`aws_cloudtrail`、`postgresql_stderr`、`postgresql_csvlog`、`mysql_error`、`mysql_slow`、`log4j2_default`、`logback_default`、`python_logging`、`chrome_net_export`、`ms_sqlserver_errorlog` 與 `azure_monitor`。使用 `regex` 時，請自行設定 `record_start`。

有兩種格式自行讀取記錄，而不是逐行讀取。`kubernetes_cri` 會把執行環境拆開的部分（`P`）行合併為一條記錄，`windows_evtx` 讀取二進位的 `.evtx` 檔案。

仍在寫入的檔案只讀到其最後一個完整的行。對於多行格式，則讀到其最後一條記錄的起點，因為下一行可能延續該記錄。檔案 2 秒沒有寫入後，最後一條記錄即視為完整。

**輪替。** 檔案由其前 4096 個位元組（解壓縮後）的校驗和識別，而不是由名稱識別。當 `system.log` 輪替為 `system.log.0`，再壓縮為 `system.log.0.gz` 時，所有副本是同一個檔案。讀取從上次讀到的位元組繼續，因此沒有任何一行會被讀兩次。

- 壓縮根據檔案的前幾個位元組識別，絕不看名稱：gzip 與 bzip2。壓縮檔案視為已完整。
- 輪替副本及其壓縮孿生檔案若都被匹配，則只讀一次，取自壓縮的那個。
- 發現檔案比已讀偏移更短，表示它被截斷了：它是新檔案，從頭讀取。舊檔案的資料列保留。
- 短於 4096 位元組且開頭相同的檔案以 inode 區分，因此仍是兩個檔案。
- 萬用字元不再匹配的檔案會被遺忘；其資料列作為歷史保留。

**重新整理與即時遞送。** Logs 表是一份副本。沒有任何東西監看這些檔案。當數據來源的變更訊號為 `probe` 或 `ttl_probe` 時，其探測會對每個匹配檔案的路徑、inode、大小與最後寫入時間做摘要；摘要不同時，就會要求重新整理。Logs 數據來源接受的探測類型是 `hash` 與 `none`。

重新整理只讀取新增內容。每個檔案從上一輪讀到的位置繼續讀取，資料列依鍵寫入，每批 5000 列，然後才保存新位置。在這兩步之間中斷的一輪，會重新讀取那些記錄，得到的仍是同樣的資料列。每一輪都在 `_landed_at` 中為其資料列打上時間戳，即時遞送以該資料行作為浮水位。表定義變更會觸發整體重建，從每個檔案的開頭重新讀取。

**允許的根目錄。** 營運者設定 `logs.allowed_roots` 列出萬用字元可以解析到的目錄。預設值為 `["/var/log"]`；它是 Admin → Settings 的 Security 卡片上的一個清單，也可透過 `PROVISA_LOGS_ALLOWED_ROOTS` 設定，更改時會要求確認。每個根目錄都在 `~` 展開並解析符號連結之後確定。

適用兩項檢查。萬用字元的固定部分（到第一個萬用字元為止）必須解析到某個根目錄之內；這在儲存數據來源時檢查，之後每次預覽或讀取時也檢查。每個匹配的檔案在跟隨其符號連結後也必須解析到某個根目錄之內，因此指向 `/var/log` 之外的符號連結會被拒絕。拒絕訊息會給出路徑、其解析結果以及允許的根目錄：

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

儲存時的錯誤碼是 `schema.logs_path_refused`；預覽時是帶有 `logs.path_refused` 的 403。

**託管（SaaS）拒絕。** 承載多個組織的部署不讀取自己的任何日誌檔案。在儲存、預覽與讀取時，所有路徑檢查都會首先拒絕，並給出「`<path>`: a server that holds many organizations reads no log file of its own」。這些檔案屬於伺服器，而不屬於任何一個組織，因此任何租戶都不得讀取。部署根據其多租戶狀態判斷自己是否承載多個組織。

#### Kaggle 數據集 {: #kaggle-datasets }

Kaggle 是下載數據檔案的平台。您加入的 Kaggle 數據集會註冊為 `files` 數據來源，查詢方式與其他 `files` 數據來源相同。

**加入數據集。** 打開 Sources → Subscriptions → Kaggle。表單分兩步進行，因為 Kaggle 自己的數據集搜尋需要權杖，所以要等您的權杖通過驗證後才會出現選擇器。

1. **權杖。** 輸入 Kaggle API 權杖並點擊「Validate」。驗證會以空白請求主體呼叫 `POST https://www.kaggle.com/api/v1/datasets/create/new`。`401` 表示權杖無效，其他任何回應都表示有效。Kaggle 在建立任何東西之前就會檢查空白請求主體，所以不會儲存任何內容。
2. **選擇器。** 搜尋欄會在您輸入時查詢 `GET /api/v1/datasets/list`，每項結果顯示數據集的標題及描述。選取一個，然後點擊「Add Dataset」。

「Add Dataset」從 `GET /api/v1/datasets/download/{owner}/{ref}` 下載套件，把其中的 CSV 及 Parquet 檔案解壓到 `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>`，並建立**一個** `files` 數據來源，其 `path` 就是該根目錄。伺服器從不儲存權杖。

然後在一般的 Register Table 畫面註冊您需要的資料表。每個檔案都列為一個獨立的資料表，與其他 `files` 數據來源一樣。

**只限 CSV 及 Parquet。** 包含 `.sqlite` 或 `.db` 檔案的套件會被拒絕，並給出說明原因的錯誤。只會暫存 CSV 及 Parquet 檔案。

**重新整理。** Kaggle 發佈數據集新版本後，以數據來源 ID 及有效權杖呼叫 GraphQL 變更 `refreshKaggleSource`。它會就地重新暫存檔案並清除檔案端點的快取，讓下一次查詢取得任何結構變化。建立數據來源時保存在 `federation_hints` 中的 `kaggle_owner` 及 `kaggle_ref` 指明要取得哪個數據集。

**沒有專屬的 YAML。** Kaggle 數據來源只能透過數據來源表單建立。匯出為 YAML 時，它顯示為 `type: files`，並在 `federation_hints` 中帶有 `kaggle_owner` 及 `kaggle_ref`。要重新從 Kaggle 取得，需要使用介面中的重新整理或 `refreshKaggleSource` 變更。在隔離環境中，把 YAML 的 `path` 指向您自行暫存的目錄。

#### Wikipedia {: #wikipedia }

Wikipedia 會爬取你指定的頁面。頁面上的每個數據表，以及每個連結的 CSV、TSV、Excel、JSON 或 Parquet 檔案，都會成為該數據來源的一張表。與 Kaggle 一樣，它註冊為 mapping 中帶有 `crawl` 的 `files` 數據來源。新增它不會註冊任何表：由數據管理員經一般的 Register Table 畫面註冊所需的表。

**設定欄位。**

| 欄位 | 預設 | 意義 |
| --- | --- | --- |
| Pages to start from | 無（必填） | 每行一個：頁面標題，或所選語言版本中某個頁面的位址 |
| Language edition | `en` | 語言版本的代碼，例如 `en`、`de` 或 `ja` |
| Link depth | `1` | 從起始頁面向外讀取多少層連結；`0` 只讀取起始頁面 |
| Page limit | `25` | 一次爬取最多讀取的頁面數 |
| Contact | 空 | 隨每個請求送出，讓 Wikipedia 能依其政策要求聯絡到你 |
| Follow "See also" links | 關 | 關閉時，文章中不含該章節 |
| What this edition calls its "See also" section | 空 | 僅在上方選項關閉、且 Provisa 無法為所選語言版本給出該章節名稱時才會詢問。填入頁面上顯示的章節標題，例如 `Voir aussi` |
| Directory for the crawl's files | 空 | 表與數據檔案寫入的位置。為空時：`<PROVISA_DATA_DIR>/crawl/<source id>` |

Provisa 知道這些語言版本的「See also」標題：`en`、`de`、`es`、`it`、`pt`、`nl`、`pl`、`sv`、`ja`、`zh`、`ru`。對於任何其他語言版本，在「See also」關閉時，表單會詢問該版本如何稱呼這個章節；填入頁面上顯示的標題（例如 `Voir aussi`）。未填就儲存的數據來源會被拒絕（`wikipedia.see_also_heading_needed`）。

**跟隨哪些連結。** 連結的取捨取決於頁面自身標記所宣告的類型，而不是其位址中的名稱：這類名稱在每個語言版本中各不相同，也會出現在一般標題裡。

- 跟隨：位於文章正文、清單與數據表中指向其他文章的連結。標記為 `a[rel='mw:WikiLink']`。
- 不跟隨：尚未撰寫的頁面、頁面本身、圖書來源查詢、檔案頁面、其他 wiki 與外部網站。
- 完全不讀取（在收集連結與表之前移除）：導覽框、資訊框、註解與引用、分類、目錄、圖片與維護提示。
- 除非開啟該選項否則不讀取：「See also」章節。

只有編輯者標記為數據表的表（`table.wikitable`）才會成為表，列數少於 2 的表不會提供。

**進階爬取設定。** 表單把這些設定摺疊在「Crawl settings」之下，並顯示各自的預設值；欄位留空即保留預設值。在 `files` 的 mapping 中，同樣的名稱放在 `crawl:` 之下，不在此清單中的名稱會被拒絕。

| 設定 | Wikipedia 預設 | 意義 |
| --- | --- | --- |
| `start_urls` | 你指定的頁面 | 爬取的起始頁面 |
| `max_depth` | `1` | 連結深度 |
| `max_pages` | `25` | 頁面上限 |
| `request_delay` | `1 seconds` | 請求之間的延遲 |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`，並附上你的聯絡資料 | 爬取用以自我標示的名稱 |
| `content_selector` | `#mw-content-text .mw-parser-output` | CSS 選擇器：只在其內部讀取連結與表 |
| `remove_selectors` | 上文所述各部分的清單 | 用於排除部分內容的 CSS 選擇器。指定後會取代整份清單，包括「See also」規則 |
| `link_selector` | `a[rel='mw:WikiLink']`，並帶排除項 | 連結必須符合才會被跟隨的 CSS 選擇器 |
| `link_exclude_patterns` | 空 | 正規表示式；位址中符合其中之一的連結不會被跟隨 |
| `follow_external_links` | `false` | 是否跟隨指向其他網站的連結 |
| `allowed_domains` | 未設定 | 爬取可以造訪的網域 |
| `table_selector` | `table.wikitable` | 表必須符合的 CSS 選擇器 |
| `generate_tables_from_html` | 未設定 | HTML 表是否成為表 |
| `html_table_min_rows` | `2` | 表的最少列數 |
| `html_table_max_rows` | 未設定 | 表的最多列數 |
| `allowed_file_extensions` | `csv`、`tsv`、`xlsx`、`xls`、`json`、`parquet` | 會成為表的連結檔案類型 |
| `html_cache_ttl` | `1 days` | 爬取結果保留多久 |
| `max_html_size` | 未設定 | 讀取的最大頁面，例如 `10MB` |
| `max_data_file_size` | 未設定 | 讀取的最大數據檔案，例如 `100MB` |

**資料表命名。** 爬取到的資料表命名為 `<page>__<name>`。名稱取資料表的標題（caption）；沒有標題時，取它上方最近的一個小節標題；兩者都沒有的資料表命名為 `table_1`、`table_2`，如此類推。兩部分都轉為小寫，其他字元組成的每一段都換成 `_`。例如，頁面 `List_of_tallest_buildings` 上「Tallest buildings」小節下的資料表會成為 `list_of_tallest_buildings__tallest_buildings`。連結的數據檔案保留自己的名稱。

**重新整理。** 爬取結果會在快取期內保留，預設為 `1 days`（`html_cache_ttl`）。只有在該時間過後才會重新取得頁面。

### 可觀測性與其他 {: #observability-other }
`prometheus` 有 Trino 連接器（屬性自該類型的映射 DSL 建構）。`google_sheets` 是已註冊的數據來源類型，沒有 Trino 連接器，經 API 快取管線具體化。

| 數據來源類型 | 連接器名稱 | 變更操作 |
| ------------ | ----------------- | ----------- |
| `google_sheets` | —（經具體化） | 否 |
| `prometheus` | prometheus | 否 | 一個指標即為一張表，一個樣本即為一列（`timestamp`、`value`，每個標籤一個資料行）；在沒有即時連接器的每個引擎上，Provisa 讀取 HTTP API——Register Table 取得指標名稱與標籤，落地時對該表的範圍使用 `query_range`

Google Workspace 及 Microsoft 365 由各自的載入器讀取；見 [Google Workspace 及 Microsoft 365](#google-workspace-and-microsoft-365)。SharePoint、Salesforce、ServiceNow、Jira、雲端資源清單及 Splunk 經以獨立程序執行、使用 PostgreSQL 協定通訊的驅動程式讀取（`pgwire-sharepoint`、`pgwire-salesforce`、`pgwire-servicenow`、`pgwire-jira`、`pgwire-cloudops`、`pgwire-splunk`），所以它們都沒有直接驅動程式。在 DuckDB 引擎上，驅動程式的端點透過 postgres 擴充功能即時掛載：Register Table 從已掛載的目錄中列出連接器的資料表，查詢就地讀取連接器，篩選及投影會下推給它。其他所有引擎都把資料列落地到具體化儲存區以用於聯邦。驅動程式套件按作業系統及架構從固定版本的 release 中取得（`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`；macOS arm64、Linux x86_64、Windows x86_64）。營運者亦可以在 Admin → 設定 → 連接器伺服器中，按連接器指定一個為本主機建構的套件資料夾（`connectors.<connector>.bundle_dir`）；見 [Admin](admin.md)。五個 Trino 連接器（Jira 沒有）一律以不分大小寫的方式比對名稱，與各產品自身一致。
#### `sharepoint`

SharePoint 清單被列舉為結構描述，並以可查詢的表對外呈現。兩種驗證方式：`CLIENT_CREDENTIALS`（預設）與經 PFX 憑證的憑證式驗證。`mapping` 中的密鑰值在抵達連接器之前，先經密鑰引擎解析。

| 數據來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` or `host` | `site-url` | SharePoint 網站 URL |
| `username` | `client-id` | Azure 應用程式用戶端 ID |
| `password` | `client-secret` | Azure 應用程式用戶端密鑰 |
| `database` | `tenant-id` | Azure 租用戶 UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS`（預設）或 `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | `auth_type: CERTIFICATE` 時的 PFX 路徑——必須是絕對路徑 |
| `mapping.certificate_password` | `certificate-password` | PFX 密碼——該索引鍵必須存在，沒有密碼的 PFX 用空字串 |

在非 Trino 引擎上，憑證驗證還帶有兩條額外規則，兩者都在建構 Calcite pgwire 伺服器的 `model.json` operand 時強制執行。`certificate_path` 必須是絕對路徑：伺服器以其套件目錄作為工作目錄執行，因此相對路徑會在執行階段依賴快取內解析，導致找不到該 PFX。即使 PFX 沒有密碼，`certificate_password` 也必須存在於 `mapping` 中，此時其值為空字串——Calcite 配接器會直接拒絕 null 密碼，缺少該索引鍵會被視為設定錯誤，而不是被默默當作空密碼讀取。缺少或為相對路徑的值會拋出指明該欄位的 `MissingConnectorConfig`。

當連接器不提供 `information_schema.columns` 時，請經 `registerTable` 變更操作以明確的資料行定義（自 Microsoft Graph API 取得）註冊該表。

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

憑證驗證，使用絕對路徑與一律存在的密碼：

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CERTIFICATE
    certificate_path: /etc/provisa/certs/sharepoint.pfx
    certificate_password: ${env:SP_CERT_PASSWORD}
```

#### `salesforce`

憑證可以查詢的每個 sObject 都是數據管理員可以註冊的表格（`Account`、`Opportunity`、自訂 `*__c` 物件）；不會自動註冊任何內容。欄位和類型來自 sObject 的 describe。篩選、投影、排序和限制會以 SOQL 的形式下推到 Salesforce。

Salesforce 數據來源需要指定機構的「我的網域」登入 URL 和一套完整的憑證，由 `mapping.auth_type` 選擇。登入 URL 是完整的 `https://…` URL（例如 `https://acme.my.salesforce.com`）；裸主機或任何其他配置（scheme）都會被拒絕，並指出欄位名稱。使用數據來源時，不完整的憑證會被拒絕，並指出缺少的值。每個密鑰都可以是 `${secret:…}` 或 `${env:…}` 參照。以字面值輸入的密鑰會存入機構的保管庫，數據來源保留其參照。

| 數據來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` or `host` | `login-url` | 機構的「我的網域」URL（設定，然後是「我的網域」）。`login.salesforce.com` 不適用於用戶端憑證流程 |
| `username` | `client-id` | 已連線應用程式的用戶金鑰 |
| `password` | `client-secret` | 已連線應用程式的用戶密碼 |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS`（預設）、`USERNAME_PASSWORD` 或 `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | 僅限 `USERNAME_PASSWORD`，與用戶金鑰和用戶密碼一併使用 |
| `mapping.security_token` | `security-token` | 僅限 `USERNAME_PASSWORD`；選填 |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | 僅限 `ACCESS_TOKEN`；不需要用戶金鑰或用戶密碼 |
| `mapping.api_version` | `api-version` | 選填，例如 `v61.0` |

對於 `CLIENT_CREDENTIALS`，已連線應用程式需要啟用 *Enable Client Credentials Flow* 並設定 *Run As* 使用者；每次讀取和寫入都以該使用者的權限執行。自 Summer '23 起建立的機構預設會封鎖使用者名稱密碼流程。

**讀取。** 在 Trino 上，數據來源透過唯讀的 `salesforce` 目錄讀取。在其他任何引擎上，引擎會掛載數據來源的 pgwire 伺服器並就地讀取。

**寫入。** 對已註冊 sObject 的 `INSERT`、`UPDATE` 和 `DELETE` 屬於變更操作：與其他任何變更一樣受治理、經審計，並按環境處理。在每個引擎上（包括 Trino），寫入都在數據來源的 pgwire 伺服器上執行；在 Trino 上，該伺服器只為寫入而運行。每條陳述式都會傳送到 Salesforce 並在執行時提交，因此無法回滾。伺服器不會傳回它所寫入的資料列：帶有 `RETURNING` 子句的陳述式會被拒絕，GraphQL 也不為這些表提供 mutation 欄位；SQL 的 `INSERT`、`UPDATE`、`DELETE` 和 `COPY` 是寫入路徑。一次寫入是針對一張表的單一普通陳述式：`INSERT … ON CONFLICT`、`UPDATE … FROM`、`DELETE … USING`、`MERGE` 和 `TRUNCATE` 均被拒絕，並指出形式。SharePoint 數據來源以同樣的方式寫入。

**啟動。** 在開始接受連線之前，數據來源的伺服器會讀取每個 sObject 的欄位，對於擁有 1,200 個 sObject 的機構大約需要四分鐘。這個代價只付一次：describe 結果儲存在執行個體數據目錄下該數據來源自己的狀態目錄中。伺服器在數據來源註冊或載入時啟動。在 Trino 上，`salesforce` 目錄保存自己的 describe 結果，在協調器的 `/data/trino/cache/salesforce-describe` 之下每個數據來源一個目錄，因此當該路徑是能比容器存活更久的磁碟區時，這裡的代價同樣只付一次：compose 堆疊的 `trino_cache` 磁碟區，或 Helm chart 的協調器快取磁碟區（`trino.coordinator.cache.persistence`）。在它開始接受連線之前，Register Table 會顯示連接器正在啟動，寫入會收到相同的訊息而不是等待。

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

使用者名稱密碼流程：

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    auth_type: USERNAME_PASSWORD
    sf_username: ops@acme.com
    sf_password: ${env:SF_PASSWORD}
    security_token: ${env:SF_SECURITY_TOKEN}
```

#### `servicenow`

實例中的每個資料表都可以註冊，不會自動註冊任何資料表。繁忙的實例有成千上萬個資料表，所以 Register Table 按主題分組。每個主題是一個 ServiceNow 套件的顯示名稱；不屬於任何套件的資料表歸入「(no package)」。每個資料表顯示為 `name — label`。選取一個資料表時才會從實例讀取它的資料行，列出資料表時不會。

ServiceNow 數據來源指定執行個體的完整 `https://…` URL 以及使用者名稱和密碼。URL 從 `base_url` 或 `host` 取得；裸主機或其他配置（scheme）將被拒絕，並指出欄位名稱（`instanceUrl`）。缺少使用者名稱或密碼時將被拒絕，並指出欄位名稱。每個值都可以是 `${secret:…}` 或 `${env:…}` 參考。以字面值輸入的密鑰會存入機構的保管庫，數據來源保留其參考。

| 來源欄位 | 配接器屬性 | 說明 |
| --- | --- | --- |
| `base_url` 或 `host` | `instanceUrl` | 執行個體的完整 `https://…` URL（例如 `https://acme.service-now.com`）。裸主機或其他配置將被拒絕 |
| `username` | `username` | 基本驗證使用者名稱 |
| `password` | `password` | 基本驗證密碼 |

**讀取。** 在 Trino 上，數據來源透過唯讀的 `servicenow` 目錄讀取。在其他任何引擎上，引擎會掛載數據來源的 pgwire 伺服器並就地讀取。執行個體只接收所選欄位的請求（`sysparm_fields`）。篩選器僅以配接器已在真實執行個體上驗證過的形式傳送至執行個體（`sysparm_query`）；其他所有篩選器以及每個排序和限制，都套用於已讀取的資料列。

參考欄位顯示為兩欄：`<field>` 包含被參考記錄的 sys_id，`<field>__display` 包含顯示值。空欄位值為 NULL。配接器無法映射其類型的欄位會產生錯誤，錯誤中指出資料表、欄位和類型。

資料表搜尋（自然語言）與每張資料表的名稱、標籤和主題匹配。一旦註冊了一張資料表，其欄位就會加入搜尋。

**伺服器生命週期。** 伺服器僅提供從中註冊的資料表。註冊或移除一張資料表會重新啟動伺服器並在引擎上重新佈建數據來源；在該重新啟動期間到達的陳述式會失敗。

**寫入。** 對已註冊資料表的 `INSERT`、`UPDATE` 和 `DELETE` 屬於變更操作：與其他任何變更一樣受治理、經審計，並按環境處理。在每個引擎上（包括 Trino），寫入都在數據來源的 pgwire 伺服器上執行。伺服器會傳回每次寫入後執行個體持有的資料列；因此接受 `RETURNING` 子句，GraphQL 也為這些資料表提供變更欄位。一次寫入是針對一張資料表的單一陳述式：`INSERT … ON CONFLICT`、`UPDATE … FROM`、`DELETE … USING`、`MERGE` 和 `TRUNCATE` 均被拒絕，並指出形式。

**Import sets（匯入集）。** 當一張資料表存在指向它的活動轉換對應時，Register Table 會提供其 import sets，數據管理員可以將資料表綁定到其中一個（有多個對應的資料表會逐一命名）。已綁定資料表的插入操作透過執行個體的 Import Set API 及該對應的轉換進行；未綁定資料表的插入操作以及每次更新和刪除都直接寫入資料表中。轉換拒絕的資料列將被拒絕，並指出該列；已寫入的資料列保持不變。綁定在儲存資料表時進行檢查：它必須是該資料表的活動轉換對應的 import set，且位於 ServiceNow 數據來源上。

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

Jira Cloud 網站透過其自有的 Calcite 配接器讀取和寫入。Jira 數據來源的一張資料表是一個**表單**：一個專案和一種 issue 類型，以及其欄所讀取的欄位。Register Table 列出該帳戶可建立 issue 的每個表單，按專案分組；選取一個表單會從 Jira 讀取其欄位，列出表單不會這樣做。每個表單都有欄 `key`（其主鍵）、`id`、`summary`、`status`、`created` 和 `updated`，之後是每個所選欄位一欄，按配接器的命名方式命名：欄位名稱的 snake case 形式（`Story Points` 即 `story_points`）。名稱已是某欄名稱的欄位，以其欄位 id 命名（`customfield_10099`），因此每個欄位都可被選取。使用者欄位保存帳戶 id；其顯示名稱是單獨的欄位（`assignee.displayName`，欄 `assignee_name`）。由 Jira 自行寫入的欄為唯讀：`key`、`id`、`created`、`updated`、使用者欄位的顯示名稱，以及配接器不認識其形態的欄位（配接器將其作為 JSON 文字提供）。註冊此類欄時，若其沒有說明，會在說明中標註「Read only: Jira sets it」；設定該欄的寫入會被伺服器拒絕，並指出該欄。

表單的資料表在其 `arguments` 中指明其專案和 issue 類型（`project`、`issue_type`），每一欄讀取其 `path` 所指明的欄位（欄位 id 或欄位名稱），或與其同名的欄位。從管理端註冊的資料表會從 Jira 取得這兩者；配置則明確宣告它們。固定了其他內容或缺少其中任何一項的 Jira 資料表將被拒絕，並指出資料表名稱。

| 來源欄位 | 配接器屬性 | 說明 |
| --- | --- | --- |
| `base_url` 或 `host` | `baseUrl` | 網站的完整 `https://…` 位址（例如 `https://acme.atlassian.net`）。裸主機或其他配置將被拒絕 |
| `username` | `email` | Atlassian 帳戶的電子郵件 |
| `password` | `apiToken` | 該帳戶的 API 權杖，在 id.atlassian.com 的 Security、API tokens 下建立 |

**連結。** 數據來源的 `issue_links` 資料表保存其表單的 issue 之間的連結：`link_id`、`link_type`、`outward_key` 和 `inward_key`，每個連結一次。兩個表單之間的關係透過它宣告為聯結表（junction），以 `link_type` 為判別欄，因此 GraphQL 和 Cypher 按連結類型從 issue 走到被連結的 issue。

**讀取。** 按鍵查找（`key =`、`key IN`）按鍵擷取，因此未指明任何 issue 的鍵不會產生資料列。日期或數字比較會作為 JQL 傳送至 Jira。其他所有篩選器以及每個排序，都套用於已讀取的資料列：對文字、選項、狀態或使用者的相等比較不會傳送，因為對於 SQL 傳回零列的情況，JQL 會拒絕未指明任何對象的值。多值欄位（標籤、多選、元件、版本）是保存 JSON 陣列的文字，寫入方式相同。

在 DuckDB、PostgreSQL 和 ClickHouse 上，引擎會掛載數據來源的 pgwire 伺服器並就地讀取。在 Trino 上，數據來源沒有自己的目錄，而是透過其伺服器落地。

**伺服器生命週期。** 伺服器僅提供從中註冊的表單，以及其表單清單、欄位清單和連結。註冊、變更或移除表單會重新啟動伺服器並在引擎上重新佈建數據來源。

**寫入。** 對表單的 `INSERT`、`UPDATE` 和 `DELETE` 屬於變更操作，在每個引擎上都在數據來源的 pgwire 伺服器上寫入。資料列以每批 50 列的方式插入；更新或刪除逐個 issue 按鍵進行。設定 `status` 會執行到該狀態的工作流程轉換，當 issue 的工作流程沒有可到達的狀態時會被拒絕，並指出可到達的狀態。帶有子任務的 issue 不會被刪除。伺服器按鍵傳回每個已寫入的資料列，因此接受 `RETURNING`，GraphQL 也提供變更欄位。在 `issue_links` 上，`INSERT` 建立連結，`DELETE` 移除連結；`UPDATE` 被拒絕。

```yaml
- id: support-jira
  type: jira
  base_url: https://acme.atlassian.net
  username: ${env:JIRA_EMAIL}
  password: ${env:JIRA_API_TOKEN}
tables:
- source_id: support-jira
  table: incident
  arguments: {project: KAN, issue_type: Incident}
  columns:
  - {name: key, data_type: varchar, is_primary_key: true}
  - {name: summary, data_type: varchar}
  - {name: status, data_type: varchar}
  - {name: priority, data_type: varchar}
  - {name: flagged, data_type: varchar, path: customfield_10021}
```

#### `cloudops`

雲端資源清單把 Azure、AWS 和 GCP 的資源呈現為同一組表格：`compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` 與 `compute_security_groups`（把運算資源與其安全群組關聯起來）。每張表格在三個雲端上使用同一組欄位，其 `cloud_provider` 欄位指明每一列來自哪個雲端。此數據來源只讀，其表格與其他表格一樣由數據管理員註冊。

一個數據來源指定一個或多個雲端。每個雲端要麼完整要麼不填：只填了部分必填值的雲端會被拒絕，並指出缺少的值；沒有指定任何雲端的數據來源同樣會被拒絕。數據來源只讀取它指定的雲端。所有值都放在 `mapping` 中；每個密鑰都可以是 `${secret:…}` 或 `${env:…}` 參照。以字面值輸入的密鑰會存入機構的保管庫，數據來源保留其參照。

| 雲端 | `mapping` 鍵 | 連接器屬性 | 備註 |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | 必填 |
| Azure | `azure_client_id` | `azure.client-id` | 必填；應用程式註冊的用戶端 ID |
| Azure | `azure_client_secret` | `azure.client-secret` | 必填；密鑰 |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | 必填; 以逗號分隔 |
| AWS | `aws_access_key_id` | `aws.access-key-id` | 必填 |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | 必填；密鑰 |
| AWS | `aws_region` | `aws.region` | 必填，例如 `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | 必填; 以逗號分隔 |
| AWS | `aws_role_arn` | `aws.role-arn` | 選填；為跨帳戶存取而承擔的角色 |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | 必填；服務帳戶 JSON 金鑰檔案的絕對路徑 |
| GCP | `gcp_project_ids` | `gcp.project-ids` | 必填; 以逗號分隔 |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | 選填；結果在再次查詢各雲端之前被重用的時間（配接器預設為 5） |

GCP 金鑰檔案由讀取數據來源的一方開啟。在 Trino 上是 Trino 伺服器，因此該路徑必須存在於其上。在其他每個引擎上是 Provisa 主機上該數據來源的 pgwire 伺服器，其工作目錄是它自己的狀態目錄，因此相對路徑會被拒絕。

在 Trino 上，數據來源透過 `cloudops` 目錄讀取。在其他任何引擎上，引擎會掛載數據來源的 pgwire 伺服器並就地讀取，與 SharePoint 數據來源相同。

```yaml
- id: cloud-estate
  type: cloudops
  mapping:
    aws_access_key_id: ${env:CLOUDOPS_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${env:CLOUDOPS_AWS_SECRET_ACCESS_KEY}
    aws_region: us-east-1
    aws_account_ids: "111111111111,222222222222"
    azure_tenant_id: ${env:CLOUDOPS_AZURE_TENANT_ID}
    azure_client_id: ${env:CLOUDOPS_AZURE_CLIENT_ID}
    azure_client_secret: ${env:CLOUDOPS_AZURE_CLIENT_SECRET}
    azure_subscription_ids: "sub-1,sub-2"
```

#### `splunk`

Splunk 搜尋結果可作為表查詢（例如 `internal_server`）。連接器 URL 來自 `base_url`，或以 `https://{host}:{port}` 建構，預設連接埠為 `8089`。驗證：當 `mapping.use_token` 為 `true`（預設）時，`password` 作為 API 權杖傳遞；為 `false` 時，`username` 與 `password` 作為獨立憑證傳遞。

| 數據來源欄位 | 連接器屬性 | 備註 |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`，否則為 `https://host:port`（連接埠預設 8089） |
| `password` | `token` or `password` | `use_token: true` 時為 token |
| `username` | `user` | 僅在 `use_token: false` 時 |
| `database` | `app` | 限定於某個 Splunk 應用程式 |
| `mapping.datamodel_filter` | `datamodel-filter` | 篩選至某個數據模型 |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | 供自簽憑證使用 |

在 pgwire 副本路徑上（除 Trino 外的所有引擎），同樣這四項可選設定會成為 Calcite `model.json` 的運算元鍵 `app`、`token`/`username`+`password`、`datamodelFilter` 及 `disableSslValidation`。後兩項採用 `SplunkSchemaFactory` 轉換後的類型，即字串及布林值。

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

#### Google Workspace 及 Microsoft 365 {: #google-workspace-and-microsoft-365 }

`google_workspace` 與 `microsoft_365` 讀取數據來源所指定帳戶的郵件、日曆與工作，並把它們落地為副本。它們不是 Calcite 連接器：各自有自己的載入器，呼叫供應商的 API——一邊是 Gmail、Google Calendar 與 Google Tasks，另一邊是 Microsoft Graph。表單在 Enterprise 之下把它們列為「Google Workspace (Gmail)」與「Microsoft 365 (Outlook, Exchange)」。

**表。** 兩種類型產生相同的規範表：名稱、資料行類型與順序都一致。對二者做聯集無需轉型或改名；某個供應商沒有值的資料行為 NULL。這些表列在 `default` 結構描述下，每個資料列都以讀取它的帳戶開頭。數據來源提供其所讀資源對應的表：

| 資源 | 表 | 可由語句寫入 |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | 否 |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

各資料行列於[規範郵件結構描述](arch/canonical-mail-schema.md)。`events` 保存單次事件、以 RFC 5545 行表示重複規則的系列主事件，以及例外；它從不保存系列展開後的各次出現。

**註冊。** 組織的管理員首先在 Admin → Email 之下輸入該組織在該平台上的用戶端：用戶端 ID 與密鑰，Microsoft 365 還需要目錄（租戶）ID 或網域。新增數據來源的人看不到也不用輸入這些內容。數據來源隨後採用兩種形態之一：

- **單一郵箱。** 數據來源在 `accounts` 中指定一個地址。其擁有者在表單上按 **Connect** 並核准組織的用戶端。由此得到的核准保存在組織的狀態儲存區中，而不在數據來源的設定裡。在 Google Workspace 上，這種形態還需宣告 `sign_in: google_account`。
- **組織的各個郵箱。** 數據來源把 `mailboxes` 宣告為 `everyone: true`、`group: <address or id>` 或 `list: [addresses]` 中恰好一項。它不持有憑證，也沒有人去連接它。組織以自己的存取權限讀取，而且只有在管理員開啟「Read the organization's mailboxes」之後才可以。在 Google 上，該存取權限是帶網域範圍委派的服務帳戶金鑰，外加用於目錄呼叫的管理員地址。在 Microsoft 上，它是應用程式註冊的用戶端憑證授權。

採用第二種形態時，一張表保存所有郵箱的資料列。除非針對 `account` 資料行的列規則加以限制，否則可以讀取該表的角色能讀取每個郵箱。

```yaml
sources:
  - id: team-mail
    type: microsoft_365
    mapping:
      resources: [mail, calendar, tasks]
      mailboxes:
        group: support@acme.com
      mail_since: "2026-01-01"
      calendar_check_days: 30        # required when calendar is read
      calendar_send_updates: false
```

**設定。** 每個模組都會依名稱拒絕它不讀取的鍵。

| `mapping` 鍵 | 類型 | 含義 |
| --- | --- | --- |
| `resources` | 兩者 | 數據來源讀取 `mail`、`calendar`、`tasks` 中的哪些；至少一個 |
| `accounts` | 兩者 | 單一郵箱的地址（單一郵箱形態） |
| `mailboxes` | 兩者 | `everyone`、`group` 或 `list`（組織形態） |
| `sign_in` | `google_workspace` | `google_account`（單一郵箱形態） |
| `mail_since` | 兩者 | `YYYY-MM-DD`；更早收到的郵件不保存 |
| `mail_content` | `google_workspace` | `full`，或僅含郵件標頭與標籤的 `headers`（Gmail 的 `gmail.metadata` 範圍）。讀取郵件時必填 |
| `mail_search` | `google_workspace` | 如在 Gmail 搜尋框中鍵入的搜尋 |
| `mail_labels` | `google_workspace` | 標籤名稱；只讀取帶有全部這些標籤的郵件 |
| `mail_include_spam_trash` | `google_workspace` | 預設為 `false` |
| `calendar_send_updates` | 兩者 | 寫入時是否通知事件的參與者；預設關閉 |
| `calendar_check_days` | `microsoft_365` | 變更檢查查看今天前後各多少天；不小於 1 的整數，讀取日曆時必填 |

Google 在 `gmail.metadata` 之下拒絕搜尋，因此把 `mail_content: headers` 與 `mail_search` 或 `mail_since` 一起使用會被拒絕。

**範圍（scope）。** 登入時只要求已勾選資源所對應的範圍：

| 資源 | Google Workspace | Microsoft 365（委派） |
| --- | --- | --- |
| `mail` | `gmail.readonly`，僅郵件標頭時為 `gmail.metadata` | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| 每個數據來源 | `userinfo.email` | `offline_access`, `User.Read` |

對於組織形態，Microsoft 需要應用程式權限 `Mail.Read` 與 `User.Read.All`，日曆與工作還需要 `Calendars.ReadWrite` 與 `Tasks.ReadWrite.All`。Google 的服務帳戶被授權使用 `DELEGATED_SCOPES` 中的範圍，其中還包括目錄使用者與群組成員的讀取。

**重新整理與新鮮度。** 這些數據來源接受的探測類型是 `hash` 與 `none`，變更權杖由其載入器提供：

- **Gmail：** 每個郵箱目前 history id 的摘要，每個郵箱一次 `users.getProfile` 呼叫。
- **Microsoft 郵件：** 沿著保存的每個 delta 連結取一項。
- **Google 日曆與工作：** 每個日曆的標籤與最後變更時間；每個工作清單的標籤，以及每個工作的 id 與更新時間。
- **Microsoft 日曆與工作：** 每個郵箱的變更行，範圍是今天前後各 `calendar_check_days` 天。

無法完成的檢查會要求讀取；它絕不會回報「unchanged」。日曆表與工作表在每次重新整理時整體讀取。Microsoft 365 或 Google Workspace 數據來源的郵件副本就地保存：郵箱的每個部分在到達時寫入，可立即查詢。 以 `mail_search` 縮小範圍的 Google Workspace 數據來源在每次重新整理時都要重新整體讀取，因為只有 Google 才能說明一次搜尋匹配了什麼。

**寫入。** 對 `events`、`event_attendees` 與 `tasks` 的 `INSERT`、`UPDATE` 與 `DELETE` 經由兩種類型共用的同一條寫入路徑送給供應商。`UPDATE` 或 `DELETE` 以 `provider`、`account` 與 `id` 指定其資料列；參與者以 `event_id` 與 `address` 指定。系列中的某一次出現以 `series_id` 與 `original_start_at` 指定。設定了表的可寫集合之外的資料行的寫入會依名稱被拒絕。`calendar_send_updates` 開啟時，Google 以 `sendUpdates=all` 寫入；關閉時以 `sendUpdates=none` 寫入。Microsoft Graph 無法靜默地做出這樣的變更，因此對會議參與者的變更會被拒絕，並指明該設定。

**排除項。** 郵件只讀；沒有任何語句會更改郵箱。`calendars`、`task_lists` 與所有郵件表都不接受寫入。除非開啟 `mail_include_spam_trash`，否則 Gmail 的 Spam 與 Trash 會被排除。Google 的 Tasks API 只保存工作的到期日期，沒有提醒。

### API 數據來源 {: #api-sources }
把任何 HTTP 端點註冊為可查詢的表。

| API 類型 | 探索 | 資料行推斷 |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI 規格剖析 | 基本型別 → 原生，物件 → JSONB |
| `graphql_remote` | 結構描述自省 | 基本型別 → 原生，物件 → JSONB |
| `grpc_remote` | 伺服器反射 | 基本型別 → 原生，物件 → JSONB |

API 回應被取得、快取於 PostgreSQL（TTL 可設定），並以 GraphQL 類型對外呈現。已快取的表與任何其他數據來源一樣參與聯邦查詢。

**JSONB 規則**：以 JSONB 儲存的複合資料行（物件、陣列）不可篩選。子欄位存取在 SQL 中使用 `->>` 抽取。表與表之間的關係以純量 FK 資料行宣告——JSONB blob 資料行不是聯結目標。當需要對巢狀欄位篩選或聯結時，請用 JSONB 晉升把它們轉為原生純量資料行。

#### GitHub 與 GitLab {: #github-and-gitlab }

GitHub 與 GitLab 是由 `graphql_remote` 承載的品牌數據來源：各自以自己的名稱出現在 Sources 表單中，並註冊為記錄其品牌的遠端 GraphQL 數據來源。表單只需要一個個人存取權杖，別無其他。端點（`https://api.github.com/graphql`、`https://gitlab.com/api/graphql`）與結構描述隨 Provisa 一併提供，因此註冊時不會自省結構描述。權杖在新增數據來源時檢查。新增數據來源不會註冊任何表：結構描述提供的每張表都在該數據來源的命名空間（`gh`、`gl`）下可用，由數據管理員註冊所需的表。權杖無權讀取的欄位，在註冊表時會被排除。

#### Stripe {: #stripe }

Stripe 是由 `openapi` 承載的品牌數據來源，其建構方式與基於 `graphql_remote` 的 GitHub 和 GitLab 相同。它在 Sources 表單中以 Stripe 之名出現，位於 Enterprise 之下。它註冊為一個 OpenAPI 數據來源，其資料列在 `federation_hints` 中記錄品牌，規格路徑為 `brand:stripe`。

**涵蓋範圍。** Stripe REST API，透過 Stripe 發佈的 OpenAPI 規格（`stripe/openapi`，`spec3.json`）。該規格隨 Provisa 一併提供，已固定版本並經 gzip 壓縮，註冊時不會取得。隨附的版本是 API 版本 `2026-09-30.endive`，含 425 個路徑。每次呼叫都帶有設為該版本的 `Stripe-Version`，因此無論帳戶預設使用哪個版本，每個回應都具有規格所描述的形狀。Stripe 回傳但規格中缺少的欄位，從 `stripe.additions.json` 補入。

與任何 OpenAPI 數據來源一樣，每個 `GET` 操作是一張可提供的表，其他每個操作是一條可提供的指令。新增數據來源不會註冊其中任何一個：由數據管理員註冊所需的部分。

**驗證。** 表單只要求一樣東西：存取權杖（`BRAND_AUTH`：`token`）。請使用 Stripe 的密鑰或受限 API 金鑰。權杖作為 bearer 權杖送出，並在新增數據來源之前透過 `GET /v1/balance` 檢查一次。權杖被拒絕時，註冊失敗並回傳 `openapi.credential_rejected`；缺少權杖時回傳 `openapi.credential_required`。

**重新整理與新鮮度。** Stripe 表和所有 OpenAPI 表一樣是副本。其快取的回應保持時間為數據來源的 `cache_ttl`；除非註冊請求另行指定，否則該值設為 300 秒。適用 OpenAPI 的探測類型。預設可以指定重新整理時用來只取得變更內容的參數。Stripe 品牌沒有指定這樣的參數，因此每次重新整理都整體讀取其表。

**排除項。** Stripe 已棄用的權杖與 Sources 支付整合未包含在隨附的規格中。其清單見[以規格建構的數據來源上的排除項](#exclusions-on-a-specification-built-source)。營運者自己的 `exclusions` 會追加到品牌的排除項之後。

`stripe` 也出現在 `SourceType` 中，位於被存根化的 Supabase `wrappers` 類型之列。透過表單新增的 Stripe 數據來源是 `openapi` 數據來源，而不是那個存根。

#### 以規格建構的數據來源上的排除項 {: #exclusions-on-a-specification-built-source }

已發佈的 OpenAPI 規格描述了遠端 API 接受的一切，包括其供應商已棄用的內容，以及部署絕不能送出的內容。OpenAPI 數據來源可以帶有排除項：規格中存在但該數據來源不提供的內容。

| 被排除項 | 指定方式 | 效果 |
|---|---|---|
| 一個操作 | `method` 與 `path` | 不會列出供註冊為表或命令 |
| 一個請求引數 | `name`，針對一個操作（`method` 與 `path`），或兩者都省略時針對每個含有它的操作 | 不屬於該操作的引數，不顯示，絕不送出 |

排除項設定在數據來源 `mapping` 的 `exclusions` 之下：

```yaml
sources:
  - id: shop-api
    type: openapi
    path: https://shop.example.com/openapi.json
    mapping:
      exclusions:
        operations:
          - {method: POST, path: /v1/tokens}
        arguments:
          - {name: source, method: POST, path: /v1/customers}
          - {name: legacy_token}          # on every operation that has it
```

它們在載入規格時套用，因此註冊清單、產生的命令與送出的請求所看到的規格中，被排除的內容並不存在。請求本文屬性以及查詢、標頭或 cookie 參數可以被排除；路徑參數不能，因為它是操作位址的一部分。

若排除項指明了規格中沒有的操作或引數，該數據來源的規格載入會失敗，並列出每個過時的項目，因此與較新規格不再相符的清單不會被忽略。

品牌數據來源在其策劃中自帶排除項，營運者的排除項會加在其上。Stripe 數據來源排除了 Stripe 已棄用的權杖與來源（source）付款整合：建立 charge、Tokens 與 Sources API、憑權杖為客戶新增信用卡或銀行帳戶，以及客戶與發票付款寫入操作中的 `source`、`card`、`bank_account` 與 `default_card` 引數。收款經 Payment Intents、Payment Methods 與 Setup Intents 進行，數據來源提供這些介面。讀取與刪除帳戶已有的付款來源仍然可用。

### GovData

美國政府開放數據。存取按主題分組分割。

為數據來源指定一個或多個主題，每個主題會帶來下方列出的配接器結構描述。一個結構描述可以屬於多個主題（`census`）。`ref` 與 `geo` 結構描述是連結結構描述：每個主題的表都經由它們聯結的參考數據與地理數據，因此無論指定了哪些主題，每個數據來源都會提供它們。該清單在伺服器上只宣告一次，並由測試確保與固定版本配接器套件所提供的結構描述一致。

| 主題 | 對外呈現的結構描述 |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents`, `cftc` |
| `ECONOMY` | `econ`, `econ_reference`, `ag`, `banking`, `fiscal`, `housing`, `transport` |
| `EDUCATION` | `census`, `edu`, `research` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime`, `disasters` |
| `ENVIRONMENT` | `lands`, `environment` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec`, `officials`, `law` |
| `DEMOGRAPHICS` | `census` |
| 每個數據來源 | `ref`, `geo` |

`ALL` 是訂閱中代表所有主題的簡寫；它不是 Sources 表單上的選項。

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

在管理端建立的 AskAmerica 數據來源只需要一個憑證，即 API 金鑰。Provisa 向 AskAmerica 的 API 出示該金鑰，並取得用於讀取數據的短效、唯讀儲存憑證；若金鑰未知或已被撤銷，會在那裡按名稱被拒絕。

隨後該數據來源就是一個 Postgres wire 數據來源，與 SharePoint、Salesforce 及雲端資源清單數據來源相同：Provisa 為它啟動隨附的 `pgwire-govdata` 伺服器，並把金鑰與那些憑證放進伺服器的環境，每個引擎觸及該伺服器的方式，與觸及同類伺服器相同。DuckDB、Postgres 與 ClickHouse 附掛它並就地讀取其表，因此 AskAmerica 的表可以與其他數據來源的表聯結、位於檢視之下，並像其他表一樣交付。Trino 沒有針對它的連接器，而是讀取經該伺服器落地的副本：即保存在具體化儲存區中的表副本，其鮮度取決於最近一次重新整理（依表的快取與複寫設定），而不是在查詢時讀取。表從其指定的配接器結構描述（`sec`、`econ` 等）讀取。

伺服器在接受連線之前，會從物件儲存掛載其結構描述，首次啟動需時數分鐘。它在數據來源儲存時啟動；在就緒之前，Register Table 表單會回報該數據來源仍在啟動。

### 數據來源的密碼存放在哪裡 {: #where-a-sources-password-lives }

數據來源的密碼絕不會與其連線設定的其餘部分存放在一起。控制平面的 `sources` 資料列帶有一個 `password_ref` 欄位，存放為密碼所輸入的內容：值本身，或是諸如 `${env:PG_PASSWORD}`、`${secret:SNOWFLAKE_KEY}` 之類的參照。它會在數據來源被撥接的那一刻，在發出請求所屬的機構內部解析。

不含 `${…}` 的值按原樣使用。`${env:VAR}` 讀取部署的行程環境變數，不需要任何繫結。`${secret:NAME}` 指名某個機構所擁有的密鑰，因此它只在該機構自己的操作中解析：管理自省接縫，以及每個介面所觸及的查詢終端，都會建立這項繫結。

無論數據來源是如何註冊的，該欄位存放的都是你輸入的內容：

- **來自設定檔。** 該檔案即為記錄：Provisa 會把值或參照原樣複製到 `password_ref`。
- **來自 Sources 表單。** 在密碼欄位輸入的內容會被原樣儲存，無論是值或參照。Provisa 不會為此向機構的保管庫寫入任何內容，也不會自行編造密鑰名稱。若要讓憑證不出現在控制平面資料列中，請自行建立密鑰（見 [Secrets](secrets.md)），並在該欄位輸入 `${secret:NAME}`。

以值的形式輸入的內容，會按輸入原樣儲存在控制平面資料庫中。重新輸入密碼會取代該欄位的文字。刪除數據來源會移除它的資料列，但不會動保管庫中的任何內容：你建立的密鑰歸你所有。

`password_ref` 不會在環境之間遷移。分支或複製出的環境會提供自己的連線值，而參照所指的保管庫屬於提供該參照的那個環境。

### 數據剖析器 {: #data-profiler }

數據剖析器是一種只保存名稱、排程與執行預設值的數據來源，別無其他。它從不被自省。其他數據來源的表被加到它之下，每次執行都會度量這些表，並把結果資料列寫入機構的控制平面，在那裡像讀取其他數據來源的表一樣讀取。建立剖析器並加入表的方法見[測試數據](test-data.md)。

### 數據品質檢查器 {: #data-quality-checkers }

數據質素檢查器只是另一種數據來源類型。它的掃描輸出就是數據：每個檢查結果都是一次觀測，所以它經由一般的數據來源路徑落地，並獲得其他數據來源都有的一切，由節奏、新鮮度、事件及族系，到治理、RLS、表格檢視及匯出。

支援兩種檢查器，在兩者之間作選擇，授權與功能同樣重要。

| 數據來源類型 | 契約方言 | 額外項 | 授權 | 託管雲端平面 |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda 契約 YAML | `pip install .[soda]`（`soda-postgres`） | Elastic License 2.0 | 拒絕——見下文 |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]`（`great-expectations[postgresql]`） | Apache 2.0 | 允許 |

Elastic License 2.0 禁止把軟件作為託管或受管服務提供給第三方，而代表租戶在 SaaS 平面內執行 Soda 正是這種情況。`config/capabilities.yaml` 在 `soda` 選項上把這一點記為 `cloud_eligible: false`，託管平面會讀取這個旗標。需要 Soda 的託管部署指向營運者自行執行的 Soda 端點。

Provisa 不內置亦不連結任何東西。掃描在一個子直譯器（`python -m provisa.dq.worker`）中執行，這是唯一匯入 `soda_core` 或 `great_expectations` 的地方。因此原始碼可用的檢查器永遠不會進入伺服器程序；檢查器當機時，只會帶走一個子程序，事件迴圈不受影響。

**數據來源指向 Provisa 自己的 pgwire 端點。** 正因如此，一個 postgres 驅動程式就能檢查背後是 Snowflake 或 Iceberg 的資料表：檢查器掃描的是聯邦檢視，從不掃描底層系統。政策作用於這個連線，所以掃描身分要明確宣告。如果它是繼承而來的，一組被篩選的資料列可能令檢查悄悄通過，而沒人察覺。

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

**每份契約一個結果資料表，契約就是全部註冊內容。** 這個資料表帶有 `dq_contract`，即契約原文，此外不帶任何關於其結構的資訊。資料行、水位及晉升都由它推導而來。

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

註冊會從這段文字推導出四樣東西：

- **族系。** 契約已指明目標數據集，所以註冊會以 `extract_inputs` 剖析 SQL 的方式剖析它，並解析到受治理的資料表。只有一份定義，沒有可能漂移的第二份副本。指向未受治理數據集的契約會在註冊時明確報錯，因此永遠不會落地沒人要求的資料列。
- **資料行。** 結果封套屬於檢查器：16 個隨附資料行，由 `scan_id` 到 `diagnostics`。已宣告的資料行只讀取其 `visible_to`，所有資料行上的值必須一致，之後這些資料行會被取代。
- **水位。** `scan_time` 成為水位，這使落地成為附加。掃描歷史不斷累積，無需歷史子系統。
- **晉升。** `freshness_max_timestamp` 與 `dataset_rows_tested` 自 `diagnostics` jsonb 晉升為具型別的資料行。您可以像處理其他 jsonb 資料行一樣加入更多。

時間安排不需要新欄位。`change_signal` 加上 `cache_ttl` 決定輪詢頻率，`mv_debounce_quiet` 與 `mv_debounce_max_delay` 把上游的一陣變更合併為一次掃描，日曆粒度令它週期性執行，`expected_events` 則把掃描延後到其輸入在整個視窗內都已新鮮為止。輪詢迴圈就是掃描排程器。

`outcome` 取 `pass`、`fail`、`warn`、`error` 或 `skipped` 之一。它們本身都不強制執行任何東西。如需強制執行，請另行宣告：一個 preflight，或一個建基於已落地結果的 MV。已落地的觀測不必是確定性的，所以這裏可以放那些永遠無法作為 preflight 關卡的檢查，例如異常分數、滑動視窗內的變化，或相對於當前時刻的新鮮度。

契約在介面中編寫，位於資料表編輯頁面的數據質素面板裏，那裏的原始契約文字始終是唯一依據。試行會針對即時資料表執行契約並顯示結果，但不落地。用它來發現數據集名稱解析到意外位置的契約，否則這種契約只會落地全部通過的資料列。

---

## 自訂連接器 {: #custom-connectors }

營運者可以在 `config/custom_connectors.yaml` 中為新的數據來源類型宣告一個連接器，令原生聯邦引擎（Postgres、DuckDB 及 ClickHouse）能夠觸及它。無需編寫程式碼。

這令無需叢集即可執行的原生引擎，獲得與 Trino 引擎在其自身層面相同的設定驅動擴充能力。在 Trino 那裏，一個通用 JDBC 連接器按數據來源類型參數化，每種類型有一個目錄 `.properties` 主體，Provisa 亦提供自己的 Trino 連接器外掛（Splunk、SharePoint、Calcite）。

該檔案出廠時為空。內建連接器涵蓋開箱即用的部分，此檔案中的一切都由營運者編寫。如需使用其他路徑（例如在測試中），請設定 `PROVISA_CUSTOM_CONNECTORS`。

### 描述元種類 {: #descriptor-kinds }
| 引擎 | 種類 | 機制 | 描述元提供什麼 |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED（ISO 標準） | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + 掃描器檢視 | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…`（自動呈現每一張遠端表） | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | 逐表 `CREATE TABLE ENGINE=…`（資料行來自登錄） | `ch_engine`, `engine_template`（可帶 `{table}`） |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`，由 ClickHouse 推斷結構描述 | `ch_engine`, `engine_template` |

**Postgres 是通用的。** SQL/MED 是 ISO 標準，因此每個符合標準的 FDW 都共用同一套 DDL 形狀：`CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`、選用的 `CREATE USER MAPPING`，然後是 `IMPORT FOREIGN SCHEMA`（當 `supports_import: true`）或逐表明確的 `CREATE FOREIGN TABLE`（當為 `false`）。一份 `pg_fdw` 描述元只提供逐 FDW 的差異——擴充名稱、伺服器選項索引鍵、使用者映射索引鍵、匯入旗標、表選項。因此任何符合標準的 FDW 都能單憑設定驅動。

**DuckDB 支援兩種機制。** 經 ATTACH 呈現目錄的擴充用 `duckdb_attach`；呈現讀取表函式的擴充用 `duckdb_scan`。兩種模式都不符合的擴充不受支援。

**ClickHouse 支援三種機制**，各對應一種整合引擎形狀：一個自動呈現每張遠端表的關聯式 DATABASE 引擎（`clickhouse_database`，例如 Redis/MySQL）、一個資料行由登錄提供的逐表引擎（`clickhouse_table`，例如 JDBC/ODBC 橋接——`engine_template` 可帶一個由執行階段繫結的 `{table}` 佔位符），以及一個由 ClickHouse 推斷結構描述的檔案／湖／URL 引擎（`clickhouse_scan`，例如 HDFS/URL）。SQLite（DATABASE 引擎、檔案、無伺服器）與 Hudi（湖倉、零複製）開箱即有。

未知的 `kind` 值會在啟動時明確報錯，所以描述元裏的拼寫錯誤永遠不會令某種數據來源類型悄悄變得無法觸及。
### 探測閘門 {: #probe-gating }
可用性在附掛時針對各引擎的標準探索目錄驗證：

- **Postgres**——檢查 `pg_extension`，然後 `pg_available_extensions`。
- **DuckDB**——執行 `INSTALL`／`LOAD` 並在 `duckdb_functions()` 中檢查所宣告的 `probe_symbol`。
- **ClickHouse**——在 `system.table_engines` 中檢查所宣告的 `ch_engine`；建置中不存在即大聲失敗。

一個已宣告但無法安裝的擴充會大聲失敗。沒有默默略過，沒有備援。探測失敗的連接器，在該部署中就是不啟用。

### 樣板變數 {: #template-variables }
每個 `server_options` 值、`user_mapping` 值、`attach_template` 與 `scan_template` 都可使用 `{field}` 佔位符。可用欄位：

`{id}`、`{host}`、`{port}`、`{database}`、`{username}`、`{password}`、`{path}`、`{schema_name}`、`{table_name}`，以及 `federation_hints` 中的任何鍵。DuckDB 的 attach 範本亦會收到 `{alias}`，即 Provisa 為掛載的資料庫指派的內部目錄別名。

引用未知欄位的樣板會在附掛時大聲失敗，讓描述元與數據來源的不相符在損壞的 DDL 抵達引擎之前浮現。

### 例子 {: #examples }
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

任一描述元就位後，註冊帶有所宣告 `source_type` 的數據來源即會經該自訂連接器路由，前提是探測成功。毋須其他設定變更。

---

## 作為具名數據來源的倉庫 {: #warehouses-as-named-sources }
Snowflake、Databricks 與 ClickHouse 可註冊為具名數據來源，與哪個聯邦引擎啟用中無關。

註冊後，倉庫經數據來源的 DirectDriver 讀取，它把副本落地到啟用中引擎的具體化儲存區，查詢針對該副本執行。在一般的直接路徑上（asyncpg、aiomysql），引擎被完全略過。而這裏仍由引擎執行查詢，只是針對本機副本，而不是每次請求都經網絡存取倉庫。

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

雲端物件與數據湖數據（S3、GCS 或 R2 上的 parquet、csv、iceberg 及 delta_lake 檔案）是一種獨立的數據來源類型。當啟用中的引擎有該類型的 ATTACH 連接器時，它就地掛載，因此不落地副本，引擎直接掃描物件儲存。這些數據來源的憑證同樣放在 `federation_hints` 中：

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

## 數據來源設定欄位 {: #source-configuration-fields }
所有數據來源共用一組共通欄位。

| 欄位 | 必填 | 預設 | 說明 |
| ------- | ---------- | --------- | ------------- |
| `id` | 是 | — | 唯一識別碼；英數字加連字號／底線 |
| `type` | 是 | — | 數據來源類型（見上方表格） |
| `host` | 否 | `""` | 主機名稱或 IP |
| `port` | 否 | `0` | 連接埠號 |
| `database` | 否 | `""` | 資料庫名稱 |
| `username` | 否 | `""` | 使用者名稱 |
| `password` | 否 | `""` | 密碼；請用 `${env:VAR}` 或 `${secret:NAME}` 而非字面值（見下文） |
| `path` | 否 | `null` | 檔案式與物件／湖數據來源的檔案路徑或雲端 URI |
| `base_url` | 否 | `null` | OpenAPI 數據來源的基底 URL |
| `pool_min` | 否 | `1` | 連線池最小大小 |
| `pool_max` | 否 | `5` | 連線池最大大小 |
| `use_pgbouncer` | 否 | `false` | 經 PgBouncer 路由連線 |
| `pgbouncer_port` | 否 | `6432` | PgBouncer 連接埠 |
| `cache_enabled` | 否 | `true` | 啟用 API 回應快取 |
| `cache_ttl` | 否 | `null` | 快取 TTL（秒）；為 null 時繼承全域預設 |
| `cache_catalog` | 否 | `null` | API 快取所用的聯邦目錄；預設為數據來源自己的目錄 |
| `cache_schema` | 否 | `api_cache` | 快取目錄內的結構描述 |
| `naming_convention` | 否 | `null` | 為此數據來源覆寫全域命名慣例 |
| `federation_hints` | 否 | `{}` | 傳給聯邦引擎的工作階段屬性，以及倉庫數據來源的延伸連線參數 |
| `mapping` | 否 | `{}` | NoSQL 與 SaaS 數據來源的類型專屬連接器設定（例如 SharePoint 的 `auth_type`、Salesforce 的 `auth_type`、Splunk 的 `use_token`） |
| `allowed_domains` | 否 | `[]` | 把數據來源限制於特定網域；空 = 不受限 |
| `description` | 否 | `""` | 供人閱讀的說明 |

---

## Kafka 數據來源 {: #kafka-sources }
Kafka 主題另外設定於 `kafka_sources` 之下，以某個已註冊 `kafka` 數據來源的 `id` 為索引鍵。

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
| `topics[].default_window` | 窗口查詢的預設時間窗口（例如 `1h`） |
| `topics[].columns` | 該主題結構描述的資料行定義 |

---

## 資料行可見性 {: #column-visibility }
每個資料行上的 `visible_to` 欄位，是能看見該資料行的角色 ID 清單。

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

未列入某角色 `visible_to` 清單的資料行，不會出現在該角色的 GraphQL 結構描述中，也無法被查詢或在篩選中引用。

---

## 關係 {: #relationships }
關係連接兩張已註冊的表，並在 GraphQL 中呈現為巢狀欄位。

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
| `cardinality` | 是 | `many-to-one` 或 `one-to-many` |
| `materialize` | 否 | 為跨來源聯結自動建立具體化檢視。在由聯結資料表支撐的邊上，該檢視涵蓋的是兩跳走訪，而不是一次直接 join |
| `refresh_interval` | 否 | MV 重新整理間隔（秒）（預設：300） |
| `target_function_name` | 否 | 計算式關係的 DB 函式名稱 |
| `function_arg` | 否 | 哪個函式引數接收來源資料行的值 |
| `alias` | 否 | 供人閱讀的關係類型（例如 `WORKS_FOR`） |
| `graphql_alias` | 否 | 為此關係在父類型上呈現的 SDL 欄位命名。未提供時，名稱自目標表的 `field_name` 與關係基數推導。 |
| `disable_cypher` | 否 | 為 `true` 時，把此關係排除在 Cypher 圖形邊之外 |
| `source_json_key` | 否 | JOIN 之前先自來源資料行抽出此索引鍵作為 JSON 物件 |
| `via_table` | 否 | 這條邊所穿過的聯結資料表的已註冊資料表名稱。設定它會使該邊成為由聯結資料表支撐的邊；留空則它仍是外部索引鍵邊 |
| `via_source_column` | 否 | 與 `source_column` 配對的聯結資料表欄位。複合索引鍵時以逗號分隔並按位置對應 |
| `via_target_column` | 否 | 與 `target_column` 配對的聯結資料表欄位 |
| `via_type_column` | 否 | 判別欄，用於一張聯結資料表承載多種關係類型的情形 |
| `via_type_value` | 否 | 這條邊所釘定的判別值 |
| `via_label_source` | 否 | 由哪一項來命名 Cypher 類型：`column`（判別值）、`table`（聯結資料表的名稱）或 `fixed`（宣告的別名）。三者都會轉成大寫蛇形 |

### 由聯結資料表支撐的關係 {: #junction-backed-relationships }

您可以把關聯表宣告為一個獨立的 Cypher 關係，而不是節點。這樣它的資料行就成為該關係的屬性：

```yaml
relationships:

  - id: pets-bonded-pair
    source_table_id: pets
    target_table_id: pets
    source_column: id
    target_column: id
    cardinality: one-to-many
    via_table: pet_companions
    via_source_column: pet_id
    via_target_column: companion_pet_id
    via_type_column: relation_type
    via_type_value: bonded pair
    via_label_source: column
```

關聯表與其他資料表一樣是已註冊的資料表，關係要引用它，它必須先註冊。每個判別值宣告一個關係。基於 `pet_companions` 的三行會產生 `BONDED_PAIR`、`LITTERMATE` 及 `SHARES_ENCLOSURE` 三種不同的 Cypher 類型，每種都把關聯列的其餘資料行作為邊屬性。隨附的示範設定正是這樣宣告的。

經由關聯表的邊只存在於 Cypher 中。GraphQL 聯結產生器只為一對資料行建構 `ON` 子句，沒有地方放第二跳，所以這類邊不會出現在產生的 SDL 及 `pg_constraint` 中。關聯表本身仍可作為獨立的根欄位查詢。它會從 Cypher 圖結構描述的節點一側移除，因此永遠不會作為節點標籤出現。

`materialize: true` 同樣適用於這類邊。具體化的是整條遍歷：檢視包含來源一跳、關聯表一跳、判別值，以及關聯表自身的資料行與目標的資料行。關聯表是聯結的第三條腿，所以邊是否跨數據來源，要看全部三個資料表。如果關聯表所在的數據來源與它連接的兩個資料表不同，即使那兩個資料表同屬一個數據來源，亦會具體化。每個宣告具體化一種邊類型，所以為 `bonded pair` 建構的檢視永遠不會回答 `littermate` 遍歷。

基數取值：

- `many-to-one`：每個來源列對應一個目標列（FK 指向 PK）
- `one-to-many`：每個來源列對應多個目標列（與上相反）

---

## 資料列層級安全規則 {: #row-level-security-rules }
RLS 規則在查詢時注入 `WHERE` 子句，範圍限定於某個角色，並可選擇限定於某張表或某個網域。

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.

  - domain_id: sales-analytics  # applies to every table in domain
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
    description: Analysts read only their own tenant's rows.
```

當同一個角色同時存在網域層級與表層級的規則時，表層級規則優先。

| 欄位 | 必填 | 說明 |
| ------- | ---------- | ------------- |
| `table_id` | 條件式 | 要套用規則的表；與 `domain_id` 互斥 |
| `domain_id` | 條件式 | 要套用規則的網域；套用至該網域內所有表 |
| `role_id` | 是 | 此規則所套用的角色 |
| `filter` | 是 | 注入 `WHERE` 的 SQL 述詞；可引用工作階段變數 |
| `description` | 是 | 規則的用途，即存取審查所讀到的內容 |

---

## 函式與 Webhook {: #functions-and-webhooks }
### DB 函式 {: #db-functions }
追蹤一個資料庫函式，並把它呈現為 GraphQL 查詢或變更操作。

資料庫數據來源也能自供應商目錄（`pg_proc`、`information_schema.routines` 或供應商對應物）自動探索其預存程序與函式，免去逐一手動註冊。探索會讀取 `prokind` 與 `provolatile`：immutable／stable 的函式註冊為參數化關聯（程序引數變成查詢參數，形狀與 OpenAPI GET 表相同），而 volatile 的程序註冊為變更操作／受追蹤函式。已探索的常式流經 Stage-2 治理的方式，與手動註冊者完全相同。

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
| `returns` | 是 | — | 該函式回傳的已註冊表 ID |
| `arguments` | 否 | `[]` | `{name, type}` 引數定義清單 |
| `visible_to` | 否 | `[]` | 該函式分配給的角色（為空：所有角色）。`mutation` 另外需要 `write` 權利 |
| `domain_id` | 否 | `""` | 此函式所屬的網域 |
| `description` | 否 | `null` | GraphQL 欄位說明 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"` |

### Webhook {: #webhooks }
把一個外部 HTTP 端點呈現為 GraphQL 查詢或變更操作。

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
| `inline_return_type` | 否 | `[]` | 自訂回傳形狀的 `{name, type}` 欄位清單 |
| `arguments` | 否 | `[]` | `{name, type}` 引數定義清單 |
| `visible_to` | 否 | `[]` | 能呼叫此 webhook 的角色 |
| `domain_id` | 否 | `""` | 此 webhook 所屬的網域 |
| `description` | 否 | `null` | GraphQL 欄位說明 |
| `kind` | 否 | `mutation` | `"query"` 或 `"mutation"` |

---

## 驗證 {: #authentication }
驗證設定於 `auth` 索引鍵之下。

| 提供者 | 說明 |
| ---------- | ------------- |
| `none` | 不驗證；所有請求皆視為 `default_role` |
| `firebase` | Firebase Authentication；需要 `project_id` 與 `service_account_key` |
| `keycloak` | Keycloak OIDC |
| `oauth` | 通用 OAuth 2.0 |
| `simple` | 不經外部提供者的使用者名稱／密碼 |

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

`assignments_source: claims` 自 JWT 宣告讀取角色指派。`assignments_source: provisa` 自 Provisa 自己的指派儲存區讀取。

---

## 執行路由 {: #execution-routing }

**直接執行。** 只涉及一個關聯式數據來源的查詢直接交給該數據來源自己的驅動程式，耗時低於 100 毫秒。當 Provisa 同時知道某種數據來源類型的 SQL 方言及連接器時，它就走這條路徑。

**聯邦執行。** 跨數據來源的查詢，或針對沒有直接驅動程式的數據來源的查詢，會經過聯邦引擎。Provisa 附帶一個內嵌引擎；大型部署可以把它指向您自己的相容叢集。

**統計資料。** 註冊資料表時，Provisa 會對每個已發佈的資料表執行 `ANALYZE`，讓以成本為基礎的最佳化器取得資料列數、空值比例、相異值數量及最小/最大值。如果失敗，失敗會記入日誌，註冊照常進行。

---

## 圖形與語意數據來源 {: #graph-semantic-sources }
### Neo4j

把一個 Neo4j 圖形資料庫註冊為可查詢的數據來源。管理員撰寫投影出純量值的 Cypher 查詢；Provisa 快取結果並把它們呈現為 GraphQL 類型。

Cypher 查詢必須在 `RETURN` 子句中使用屬性存取（`RETURN n.id AS id, n.name AS name`）。傳回節點物件的查詢會在註冊時被拒絕。
#### 以設定檔註冊 {: #config-file-registration }

在 YAML 中宣告一個 `neo4j` 數據來源及其表。每張表需要一個 `query_template`（產生其資料列的 Cypher）與具型別的資料行。`query_template` 索引鍵在任何其他數據來源類型下都無效。

數據來源需要 `host`、`port` 與 `database`。 資料列的取得方式，是向 `/db/<database>/tx/commit`（Neo4j HTTP 交易 API）POST `{"statements": [{"statement": <cypher>}]}`。`errors` 清單非空的回應會被視為失敗的查詢，而非空結果。

每個資料行都需要 `data_type`。載入器會把設定類型映射為查詢時使用的 API 資料行類型：

| 設定 `data_type` | API 類型 |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

接受 `varchar(N)` 及 `decimal(10,2)`，使用括號前的基礎類型。

註冊會持久化一列 `api_sources` 與每張表一列 `api_endpoints`，因此這些表在重新啟動後仍在，毋須重新讀取該檔案。`/admin/sources/neo4j` 之下的 admin REST 端點會寫入相同的資料列。

```yaml
sources:
  - id: graph
    type: neo4j
    host: neo4j
    port: 7474
    database: neo4j
    cache_ttl: 300

tables:
  - source_id: graph
    schema: neo4j
    table: person_skills
    query_template: >-
      MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
      RETURN p.name AS name, s.skill AS skill, p.experience AS years
    columns:
      - name: name
        data_type: varchar
      - name: skill
        data_type: varchar
      - name: years
        data_type: integer
```

#### 在 UI 中註冊資料表 {: #register-table-in-the-ui }

neo4j 數據來源沒有可列出的表，因此 Register Table 表單會要求你輸入表，而不是提供選項。

1. 選擇 neo4j 數據來源與網域。結構描述與表的選取器、探索核取方塊與水位選取器都不會出現；該數據來源從不被自省。
2. 輸入表名稱與 Cypher。Cypher 必須投影純量（`RETURN a.name AS name`）；回傳節點或清單的投影會被回報為錯誤。
3. 按 Preview。表單經 `neo4jPreview` GraphQL 查詢以 `LIMIT 5` 執行該 Cypher，並依回傳的資料列填入資料行清單，類型為 `text`、`integer`、`double`、`boolean` 或 `json`。 預覽失敗時，Cypher 會留在編輯器中並顯示訊息。
4. 像處理任何表一樣調整可見性、別名或遮罩，然後註冊。在預覽為資料行定型之前，表單拒絕提交，而伺服器會拒絕不帶 Cypher 的 neo4j 表（`schema.neo4j_query_required`）。

Cypher 隨表一併儲存為 `queryTemplate`，顯示在表的唯讀檢視上，並以與設定檔註冊完全相同的方式持久化：一列 `api_sources` 與一列 `api_endpoints`，於下次啟動時載入。編輯表會重新持久化編輯後的 Cypher。

#### 以 admin REST 註冊 {: #admin-rest-registration }

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

預覽端點（`POST /admin/sources/neo4j/{id}/preview`）回傳樣本資料列，並在 Cypher 回傳節點物件時阻擋註冊。

### SPARQL

把任何符合 SPARQL 1.1 的三元組儲存（Apache Jena Fuseki、Virtuoso、Stardog 等）註冊為可查詢的數據來源。

查詢必須是 `SELECT` 查詢。`SELECT` 子句中的變數名稱自動成為資料行名稱。

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

兩個連接器都使用 API 數據來源快取管線——結果以可設定的 TTL 儲存在 PostgreSQL，使它們可用於跨來源的聯邦 JOIN。

---

#### 以設定檔及 UI 註冊 {: #config-file-and-ui-registration }

`sparql` 數據來源的 `host` 是它的 SPARQL 端點 URL（Sources 表單以相同方式儲存）。其下的每張表帶有 `query_template`，是一個 SELECT，其變數即為資料行；每個繫結都是 `text`。

```yaml
sources:
- id: sparql-demo
  type: sparql
  host: http://localhost:23030/provisa/query
tables:
- source_id: sparql-demo
  domain_id: shelter
  schema: sparql
  table: volunteer
  query_template: >-
    PREFIX s: <http://provisa.dev/shelter#>
    SELECT ?volunteer_id ?name WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name }
  columns:
  - { name: volunteer_id, data_type: text, visible_to: [org_admin] }
  - { name: name, data_type: text, visible_to: [org_admin] }
```

Register Table 的運作方式與 Neo4j 相同：選擇數據來源，輸入表名稱與 SELECT，按 Preview（`sparqlPreview` 查詢以 `LIMIT 5` 執行它並填入資料行清單），然後註冊。註冊會持久化一列 `api_sources` 與一列 `api_endpoints`（以表單編碼 POST 到端點路徑，`sparql_bindings` 正規化器），與設定檔註冊寫入的資料列相同，而原生引擎經與 Neo4j 相同的取得鏈落地資料列。

## 連線範例 {: #connection-examples }
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

### 跨來源查詢 {: #cross-source-query }
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

單一來源的部分直接路由。跨來源 JOIN 經聯邦執行，並帶自動型別強制轉換。
