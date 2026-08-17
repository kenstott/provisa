# 整合

## 選擇連線路徑

| 用戶端類型 | 建議路徑 | 原因 |
| ------------- | ----------------- | ----- |
| BI 工具（Tableau、Power BI、Looker） | JDBC | 透過線路傳輸的 Arrow Flight 欄式串流；BI 工具內建 JDBC 精靈，並能受惠於大型結果集的高吞吐量欄式傳遞 |
| psql、DBeaver 及任何相容 PG 的工具 | pgwire（原生 PG 驅動程式） | 零摩擦的預設選項——不需要自訂驅動程式；直接使用你現有的工具 |
| Python 數據堆疊（pandas、pyarrow） | `provisa-client` 或原始 ADBC | 串流傳輸 Arrow 批次；沒有資料列序列化的額外負擔 |
| Spark、DuckDB、高吞吐量管線 | Arrow Flight（ADBC） | 無限制的欄式串流，直接傳送至 Arrow 記憶體 |
| 服務對服務（具型別的合約） | Protobuf gRPC | 依角色產生的 proto；串流資料列；型別安全 |
| 網頁應用程式、指令碼 | HTTP（`/data/graphql`、`/data/sql`） | 不需驅動程式；標準 HTTP；可自由選擇查詢語言 |
| REST 用戶端（JSON:API 標準） | `GET /data/jsonapi/{table}` | JSON:API v1.0 封裝格式；透過查詢參數提供稀疏欄位集、分頁、篩選；不需驅動程式 |

---

## pgwire —— 原生 PostgreSQL 驅動程式

Provisa 實作了 PostgreSQL 線路通訊協定（協定版本 3.0）。任何支援 PostgreSQL 的用戶端都可以無需自訂驅動程式即可連線。

啟動 Provisa 之前，設定 `PROVISA_PGWIRE_PORT`（例如 `5433`）即可啟用。未設定或設為 `0` 時停用。

### 為何選用 pgwire 而非 JDBC？

JDBC 驅動程式以 Arrow Flight 作為其傳輸方式，並需要部署 `provisa-jdbc.jar`。pgwire 則不需要任何額外部署——如果你已經有 `psql`、DBeaver、SQLAlchemy 或 PG JDBC 驅動程式，即可直接使用。對於純 SQL 工作負載而言，這是摩擦較低的路徑。

對於內建 JDBC 連線精靈、並能受惠於 Arrow Flight 欄式串流以處理大型結果集的 BI 工具而言，JDBC 是正確的選擇。pgwire 則可對完整已發佈結構描述執行自由 SQL——相同的查詢，較低的設定成本。

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host：`localhost`，Port：`5433`
3. 使用者名稱／密碼依 Provisa 中的設定
4. 不需要額外下載驅動程式

### SQLAlchemy（Python）

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

或使用 `asyncpg`：

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### 身分驗證

啟動封包中的 `password` 欄位帶有憑證，而該憑證*究竟是什麼*決定了驗證方式：個人存取權杖、OIDC bearer token，或是針對已設定供應商的密碼。在 `basic` 供應商且 `auth.scram: true` 的情況下，密碼會透過 SCRAM-SHA-256 進行驗證，而非直接傳送。支援用戶端憑證。在信任模式（`none`）下，使用者名稱會直接對應至一個角色，密碼則被忽略。

完整的介面 × 驗證方式對照表載於[安全模型](security.md#_16)。不支援 MD5；在不受信任的網絡上運行時，請啟用 TLS（`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`）。

### 限制

- 僅支援 SQL。pgwire 不接受 GraphQL 及 Cypher。
- 並非唯讀。`COPY ... FROM STDIN` 可將資料列插入 `postgresql`、`mysql`、`sqlite` 及 `mariadb` 來源，並且支援 DDL（見下文）。
- 支援 DDL（`CREATE`、`ALTER`、`DROP`），並會被派送至 Trino 或直接路徑；新資料表會註冊至編譯情境中，並可立即被查詢。`COPY ... TO STDOUT`（匯出）及 `COPY ... FROM STDIN`（匯入）支援 `text` 及 `csv` 格式。
- `information_schema` 及 `pg_catalog` 的查詢會被攔截，並由 DuckDB 目錄墊片 (shim) 回應——結構描述探索工具可正常運作。

---

## JDBC 驅動程式

Provisa JDBC 驅動程式以 Arrow Flight 作為其底層傳輸方式。對於具有 JDBC 連線精靈的 BI 工具，這是建議使用的路徑。

### 連線

下載 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（永遠是最新版本），並將其加入你所使用工具的驅動程式路徑。

JDBC 網址：

```yaml
jdbc:provisa://<host>:8815
```

身分驗證使用標準的 JDBC `user` / `password` 屬性。Provisa 會依已設定的身分驗證提供者驗證憑證，並指派角色——用戶端不能自行選擇角色。

### BI 工具設定

**Tableau**

1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL：`jdbc:provisa://localhost:8815`
4. 於提示時輸入你的使用者名稱及密碼

**DBeaver**（JDBC 路徑——pgwire 路徑請見上文）

1. Database → New Connection → JDBC
2. Driver：新增 `provisa-jdbc.jar`
3. URL：`jdbc:provisa://localhost:8815`
4. 於 Authentication 分頁輸入你的使用者名稱及密碼

**Power BI**——使用 ODBC 閘道，並搭配安裝程式內附的 Provisa JDBC-ODBC 橋接器。

---

## Arrow Flight 用戶端

對於支援 Arrow Flight（連接埠 8815）的數據工具而言，這是建議使用的路徑。結果會以 Arrow RecordBatch 形式串流傳輸，不會在 Provisa 記憶體中具體化。

### Python（`provisa-client`）

建議使用的 Python 路徑——同時包裝了 GraphQL 及 Arrow Flight：

```bash
pip install provisa-client
```

```python
from provisa_client import ProvisaClient

client = ProvisaClient("http://localhost:8001", username="alice", password="secret")

# Arrow Flight → pyarrow Table (high-throughput, streaming)
table = client.flight("SELECT id, amount FROM sales.orders")

# Arrow Flight → pandas DataFrame
df = client.flight_df("SELECT id, amount FROM sales.orders")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount } }")
```

完整參考資料（包括 DB-API 2.0、SQLAlchemy 方言及 ADBC）請見 [docs/python-client.md](python-client.md)。

### Python（原始 PyArrow）

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight 會將其憑證帶在 JSON 承載內容中的 `token` 欄位——可以是供應商 bearer token，也可以是個人存取權杖。握手階段及每一張票證都接受它，且兩者的驗證方式相同，因此在握手階段已通過驗證的用戶端，仍需要在每次 `do_get` 時提交該權杖。與之並列的 `role` 欄位是*請求*一個角色；伺服器會推導出該身分獲准使用的角色，並代入經授權的值，因此票證中的角色字串絕不能視為身分本身。（REQ-1263）請參閱[安全模型](security.md#_16)。

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

### ADBC

```python
import adbc_driver_flightsql.dbapi as adbc

conn = adbc.connect("grpc://localhost:8815", db_kwargs={"username": "alice", "password": "secret"})
cursor = conn.cursor()
cursor.execute("SELECT id, amount FROM sales.orders")
table = cursor.fetch_arrow_table()
```

### DuckDB

```python
import duckdb, pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT * FROM sales.orders"}')
arrow_table = client.do_get(ticket).read_all()

conn = duckdb.connect()
result = conn.execute("SELECT region, sum(amount) FROM arrow_table GROUP BY 1").df()
```

### Spark（PySpark）

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC（連接埠 50051）

服務對服務的路徑。Provisa 在啟動時會為每個角色產生一個 `.proto`——每個角色只能看到其有權存取的資料表及欄。

下載你所屬角色的 proto：

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

使用 `grpc_server_reflection` 以程式方式探索結構描述。

每個 RPC 都必須在 `authorization` 中繼資料鍵中帶有憑證——供應商 token 或個人存取權杖。`x-provisa-role` 會從該身分獲准的角色集合中請求一個角色；它不是憑證，也從來都不是。支援用戶端憑證。請參閱[安全模型](security.md#_16)。

串流查詢每筆資料列發出一則訊息；mutation 則為單一（unary）呼叫。

---

## 跨通訊協定呼叫 Command

**Command** 是一個已註冊的追蹤函式或 webhook——一個在 Provisa 語義層中註冊的可呼叫項目，帶有 `kind`（`query` 或 `mutation`）及描述其運行方式的 `impl_kind`。每個介面都會透過單一受治理的執行器（`invoke_tracked_function`）路由呼叫，統一強制執行 `writable_by` 及治理規則（REQ-1156）。[tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | 運行內容 | 綁定欄位 |
| ------------ | ----------- | --------------- |
| `source_procedure` | 已註冊來源上的預存程序（預設） | `sourceId`、`schemaName`、`functionName` |
| `script` | 伺服器端指令碼 | `script` |
| `http` | 對外 HTTP 呼叫 | `url`、`method` |
| `grpc` | 對外呼叫外部伺服器的 gRPC | `target`、`method` |
| `python` | 由 Provisa 代管的 Python 可呼叫項目（REQ-885） | `callable`（例如 `demo.py_functions:random_dataset`） |

當一個 command 聲明了 `return_schema`（`type: array, items: object` 的 JSON Schema）時，即代表它會傳回一個集合——每個介面都會將其投影為具型別的資料列集。示範用的 `random_python_set`（impl_kind 為 `python`）及 `random_grpc_set`（impl_kind 為 `grpc`）示範了代管可呼叫項目及外部 gRPC 橋接兩種傳回隨機值資料列的方式；兩者皆在 `config/provisa-install.yaml` 中註冊。[tool-verified: `config/provisa-install.yaml:809-856`]

### 通訊協定對照表

| 介面 | 語法 | 範例 |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → Query 欄位；`kind=mutation` → Mutation 欄位；當 `domain_prefix: true` 時附加網域前綴 | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` 或 `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP（`POST /data/cypher`） | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt（Neo4j Browser／驅動程式） | `CALL fn(args)`——位置參數對應至已聲明的引數名稱 | `CALL random_python_set(3, 7)` |
| Provisa gRPC（連接埠 50051） | 單一呼叫 `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

`kind` 欄位只控制其在 GraphQL 中的位置——SQL、Cypher、Bolt 及 gRPC 介面對 `query` 與 `mutation` 兩種 command 的接受方式完全相同。

---

## Apollo Federation

Provisa 可作為 Federation v2 子圖，將其已發佈的結構描述公開給 Apollo Router 或 Apollo Gateway。

### 設定

在 `config.yaml` 中啟用 federation：

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa 會自動在主索引鍵欄上產生 `@key` 指令，並在跨子圖關聯上產生 `@external`／`@provides` 指令。

### 向 Apollo Router 註冊

在你的 `supergraph.yaml` 中：

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

執行 `rover supergraph compose --config supergraph.yaml` 以產生 supergraph 結構描述。

### 實體

Provisa 會回應 `_entities` 查詢以進行跨子圖 join。任何具有主索引鍵的資料表，都會自動可被解析為一個 Federation 實體。

---

## Hasura v2 / DDN 匯入

有關從 Hasura 遷移至 Provisa 的內容，請參閱 [docs/import.md](import.md)。

---

## Kafka

有關將 Kafka 主題設定為唯讀資料表及查詢結果接收端 (sink) 的內容，請參閱 [docs/sources.md](sources.md#kafka)。

---

## 數據品質檢查工具（REQ-1443）

Soda Core 及 Great Expectations 連接 Provisa 的方式，與任何其他 postgres 用戶端相同——都是透過 pgwire。這就是整個整合方式的全部：檢查工具只需一個 postgres 驅動程式即可掃描聯邦檢視，因此無論是 Snowflake 資料表、Iceberg 資料表還是 Mongo 集合 (collection)，都由同一套合約方言檢查，不需要為每個系統各自建立檢查工具。[tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

掃描運行於一個子直譯器中——`python -m provisa.dq.worker`——這是唯一匯入 `soda_core` 或 `great_expectations` 的地方。伺服器行程中不會連結任何相關程式庫，檢查工具發生崩潰時只會拖垮一個子行程，而不會影響事件迴圈。[tool-verified: `provisa/dq/runner.py` `build_command`]

掃描結果會以一般來源資料列的形式落地，因此頻率 (cadence)、新鮮度、事件、數據血緣、治理、RLS、資料格線 (grid) 及匯出功能，全部無需第二套機制即可套用。合約撰寫、結果封裝格式及衍生註冊，請見 [docs/sources.md](sources.md#req-1443)。

### 安裝檢查工具

兩個程式庫預設皆不隨附。安裝程式會詢問你想使用哪一個，並將答案寫入 `~/.provisa/config.yaml` 中的 `dq_checker: none|soda|gx`。在 Docker 層，`scripts/provisa` 會將其轉換為 `PROVISA_EXTRAS` 建置引數；在原生層，`first-launch.sh` 會將相符的 pyproject extra 安裝至虛擬環境中。[tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh` `_native_extras`]

| `dq_checker` | 程式庫 | 授權條款 | 代管雲端平面 |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | 不允許（`cloud_eligible: false`） |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | 允許 |

Elastic License 2.0 禁止以代管服務的形式向第三方提供該軟件，而在 SaaS 平面內代表租用戶運行 Soda 正屬於此類情況。若代管部署想使用 Soda，須改為指向由營運方自行運行的 Soda 端點。連線設定鍵請見 [docs/configuration.md](configuration.md#soda-great_expectations)。

---

## Apache Ossie 語義交換（REQ-1316）

Provisa 透過一個邊界轉接器 (adapter)，與 Apache Ossie（規格 0.2.0.dev0，孵化中；前稱 Open Semantic Interchange）交換語義模型。Provisa 內部的詞彙絕不會被重新命名成 Ossie 的詞彙——由於該規格聲明未來很可能出現破壞性變更，因此耦合僅限於此轉接器內部。
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### 匯出

規範的匯出介面是一個即時 HTTP 端點。它會在每次請求時，從即時狀態推導出 Ossie 文件——沒有快取，也沒有產生步驟。

```http
GET /admin/ossie
```

回應內容是一份 YAML 文件，帶有 `Content-Disposition: attachment; filename=provisa.ossie.yaml`。
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

Metrics 頁面的 Ossie Interchange 面板亦提供一個**下載**按鈕及一個可複製的端點網址，兩者都指向同一個端點。
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### 匯出內容

轉接器會依以下方式，將 Provisa 物件對應至 Ossie 物件：

| Provisa 物件 | Ossie 物件 | 備註 |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`；主索引鍵／唯一索引鍵取自欄設定及 `UniqueConstraint` |
| `Column` | `field` | `expression` = 欄參照（ANSI_SQL 方言）；時間欄會加上 `dimension.is_time: true` |
| `Relationship` | `relationship` | 若有設定則使用別名作為名稱；運算型（函式目標）關聯會被略過 |
| `Metric` | `metric` | `name`、`expression`（ANSI_SQL）、`datatype`、`description`、`ai_context`——依設計無損 |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | 僅供往返使用；其他工具可忽略 |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

治理、RLS、數據血緣及圖形語義不會被匯出。它們可以選擇性地存放於 `provisa` custom_extensions 欄位中以供往返保真，但交換過程本身絕不依賴其他工具去讀取它。[tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

未知的 Provisa 欄型別會原樣通過；轉接器絕不會靜默地對應到錯誤的型別。[tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### 型別對應

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Provisa／來源型別 | Ossie `datatype` |
| --- | --- |
| `varchar`、`text`、`char`、`uuid`、`string` | `string` |
| `int`、`integer`、`bigint`、`smallint`、`int4`、`int8`、`tinyint` | `integer` |
| `numeric`、`decimal`、`float`、`double`、`real` | `number` |
| `bool`、`boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`、`timestamptz`、`datetime` | `timestamp` |
| 其他一切 | 原樣通過 |

### 匯入

匯入功能接受一份 Ossie 文件（YAML 或 JSON），並傳回註冊提案。系統不會自動註冊任何內容——匯入的定義絕不會略過審核步驟。

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

伺服器會以 `parse_ossie_model` 解析該文件，此函式會驗證結構，並傳回一個包含提案資料表、關聯及指標（皆以純字典形式呈現）的 `OssieImport` 資料類別。任何結構性問題都會傳回帶有路徑名稱錯誤訊息的 `400`，例如
`ossie import: missing semantic_model[0].datasets[1].source`。
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### 審核畫面

在使用者介面中，**Import** 按鈕（Metrics 頁面 → Ossie Interchange 面板）會開啟一個檔案選取器。文件送出並解析後，會開啟一個審核視窗，列出所有提案的資料表、關聯及指標，並各自附有一個已勾選的項目。建模人員可以取消勾選任何項目以將其排除。按一下**Apply**，會透過既有的註冊 mutation 註冊已勾選的項目——先資料表，再關聯（因為關聯參照資料表），最後才是指標。
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

在 Provisa 匯出的 Ossie 文件中所儲存的建模角色及歷史記錄，經匯入後可正確往返還原。[tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## 跨通訊協定的指標（REQ-1319）

一個受治理指標的定義——其運算式、描述及 `ai_context`——會透過單一編譯器展開，隨著其值一同傳遞至每一個查詢介面。不存在任何副本。編譯器為 SQL 存取保留了 `metrics` 結構描述；每個通訊協定再各自加上自己的中繼資料通道。

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

將任何指標當作 `metrics` 結構描述中的一個虛擬關聯來查詢。你所選取的維度欄會成為 GROUP BY：

```sql
-- Grand total
SELECT value FROM metrics.net_revenue;

-- By region
SELECT region, value FROM metrics.net_revenue GROUP BY region;

-- By region and month, filtered
SELECT region, month, value
FROM metrics.net_revenue
WHERE net_revenue.status = 'completed'
GROUP BY region, month;
```

編譯器會在治理機制運行之前，將 `metrics.<name>` 形式展開為實際的分組聚合。欄描述會以 `pg_description` 項目的形式呈現，因此 DBeaver 及 psql 的 `\d+` 都能顯示它們。[tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` 會被拒絕——請明確指名欄。
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

指標會在 `_aggregate` 根欄位內以 `metrics` 區塊的形式投影。
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

定義文字（`description`、`ai_context`）會出現在 GraphQL introspection 文件中，因此具結構描述感知能力的工具及程式碼產生器可以自動取得這些內容。
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP（AI 代理程式）

有兩個工具向 MCP 用戶端公開指標：

- **`list_metrics`**——傳回該工作階段可見的所有受治理指標，包含 `name`、`description` 及 `ai_context`。
- **`query_metric`**——接受一個指標名稱及一份維度清單，並呼叫編譯器的語義 SQL 路徑，傳回聚合結果。

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

在建構查詢之前先呼叫 `list_metrics` 的代理程式，會依名稱選取一個受治理指標，而非自行手寫聚合 SQL。`ai_context` 欄位正是用來放置引導正確選取的定義文字之處。

### Arrow Flight

指標可作為傳回 Arrow 資料表的指標 flight descriptor 來定址。
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

透過標準 Flight SQL 票證路徑，使用同樣的 `metrics.<name>` SQL 形式。

### Bolt / Cypher（Neo4j Browser）

使用 `provisa.metric()` 程序呼叫一個指標：

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Fact 及 Dimension 資料表在聯邦圖中帶有 `:Fact` 及 `:Dimension` 節點標籤，因此 Bloom 能自動繪製出星形結構。
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### 自然語言查詢

自然語言結構描述比對器 (NL schema matcher) 會將自然語言問題中的指標詞彙，直接解析為一個指標加上維度，然後產生語義 SQL。[tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Fact 資料表在自然語言提示中會標記為 `[fact]`；Dimension 資料表則標記為 `[dimension]`。此比對器在解析問題時，會偏向選擇由 fact 指向 dimension 的 join 路徑。
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### 串流

將 `view_metrics` 與 `materialize` 及一個 Kafka 接收端 (sink) 結合，即可利用既有的具體化機制，產生異動即推送 (push-on-change) 的指標輸出。不需要新的管線。
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### 可觀測性（OTel）

指標評估過程會被追蹤，並可匯出為 OpenTelemetry 指標。
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
