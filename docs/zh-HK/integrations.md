# 整合

## 選擇連接方式

| 客戶端類型 | 建議方式 | 原因 |
|-------------|-----------------|-----|
| BI 工具（Tableau、Power BI、Looker） | JDBC | 透過線路進行 Arrow Flight 欄式串流；BI 工具內建 JDBC 精靈，並可受惠於欄式高吞吐量傳送大型結果集 |
| psql、DBeaver，任何相容 PG 的工具 | pgwire（原生 PG 驅動程式） | 零摩擦預設選項 —— 毋須自訂驅動程式；直接使用現有工具 |
| Python 數據堆疊（pandas、pyarrow） | `provisa-client` 或原生 ADBC | 串流 Arrow 批次；無逐行序列化開銷 |
| Spark、DuckDB、高吞吐量管線 | Arrow Flight（ADBC） | 無限制欄式串流，直接進入 Arrow 記憶體 |
| 服務對服務（型別化合約） | Protobuf gRPC | 按角色生成的 proto；串流資料列；型別安全 |
| Web 應用程式、指令碼 | HTTP（`/data/graphql`、`/data/sql`） | 無需驅動程式；標準 HTTP；查詢語言可自由選擇 |
| REST 客戶端（JSON:API 標準） | `GET /data/jsonapi/{table}` | JSON:API v1.0 封套；透過查詢參數提供稀疏欄位集、分頁、篩選；無需驅動程式 |

---

## pgwire —— 原生 PostgreSQL 驅動程式

Provisa 實作 PostgreSQL 線路協定（協定版本 3.0）。任何能說 PostgreSQL 語言的客戶端均可在毋須自訂驅動程式的情況下連接。

啟動 Provisa 前設定 `PROVISA_PGWIRE_PORT`（例如 `5433`）以啟用此功能。未設定或設為 `0` 時停用。

### 為何選用 pgwire 而非 JDBC？

JDBC 驅動程式以 Arrow Flight 作為傳輸方式，並需要部署 `provisa-jdbc.jar`。pgwire 則毋須任何額外部署 —— 若已擁有 `psql`、DBeaver、SQLAlchemy 或 PG JDBC 驅動程式，即可直接使用。對於純 SQL 工作負載而言，這是摩擦最少的方式。

對於內建 JDBC 連接精靈、並可受惠於 Arrow Flight 欄式串流以處理大型結果集的 BI 工具，JDBC 是正確的選擇。pgwire 則可對完整發佈的結構描述執行自由 SQL —— 相同的查詢，設定成本更低。

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. 新增連接 → PostgreSQL
2. 主機：`localhost`，連接埠：`5433`
3. 用戶名 / 密碼按 Provisa 設定
4. 毋須下載額外驅動程式

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

### 身份驗證

pgwire 使用明文密碼驗證，並連接至 Provisa 設定的身份驗證提供者（`none` 或 `simple`）。在信任模式（`none`）下，用戶名會直接對應至角色 —— 密碼會被忽略。不支援 MD5；在不受信任的網絡上運作時，請啟用 TLS（`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`）。

### 限制

- 僅支援 SQL。透過 pgwire 不接受 GraphQL 及 Cypher。
- 並非唯讀。`COPY ... FROM STDIN` 會將資料列插入 `postgresql`、`mysql`、`sqlite` 及 `mariadb` 數據來源，並支援 DDL（見下文）。
- 支援 DDL（`CREATE`、`ALTER`、`DROP`），並會轉發至 Trino 或直接路徑；新表格會登記於編譯上下文中，並可即時查詢。`COPY ... TO STDOUT`（匯出）及 `COPY ... FROM STDIN`（匯入）均支援 `text` 及 `csv` 格式。
- 針對 `information_schema` 及 `pg_catalog` 的查詢會被攔截，並由 DuckDB 目錄墊片回應 —— 結構描述探索工具可正常運作。

---

## JDBC 驅動程式

Provisa 的 JDBC 驅動程式以 Arrow Flight 作為底層傳輸方式。對於具備 JDBC 連接精靈的 BI 工具而言，這是建議使用的方式。

### 連接

下載 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（永遠是最新版本），並將其加入工具的驅動程式路徑。

JDBC URL：
```
jdbc:provisa://<host>:8815
```

身份驗證使用標準 JDBC 的 `user` / `password` 屬性。Provisa 會依據已設定的身份驗證提供者驗證憑證，並指派角色 —— 客戶端不能自行選擇角色。

### BI 工具設定

**Tableau**
1. 管理 → 驅動程式 → 安裝 Provisa JDBC
2. 連接 → 其他數據庫（JDBC）
3. URL：`jdbc:provisa://localhost:8815`
4. 系統提示時輸入用戶名及密碼

**DBeaver**（JDBC 方式 —— pgwire 方式見上文）
1. 數據庫 → 新增連接 → JDBC
2. 驅動程式：新增 `provisa-jdbc.jar`
3. URL：`jdbc:provisa://localhost:8815`
4. 在「身份驗證」分頁輸入用戶名及密碼

**Power BI** —— 使用 ODBC 閘道連同 Provisa JDBC-ODBC 橋接器（已包含在安裝程式中）。

---

## Arrow Flight 客戶端

Arrow Flight（連接埠 8815）是支援此功能的數據工具建議使用的方式。結果會以 Arrow RecordBatch 形式串流，毋須於 Provisa 記憶體中具體化。

### Python（`provisa-client`）

建議使用的 Python 方式 —— 同時封裝 GraphQL 及 Arrow Flight：

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

完整參考資料（包括 DB-API 2.0、SQLAlchemy 方言及 ADBC）請參閱 [docs/python-client.md](python-client.md)。

### Python（原生 PyArrow）

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

票證不會攜帶任何角色。伺服器會依據已設定的身份驗證提供者指派角色。若容許選擇角色，請於 gRPC 呼叫的元數據中，以 `x-provisa-role` 鍵傳遞（例如 `flight.FlightCallOptions(headers=[(b"x-provisa-role", b"analyst")])`），而非置於票證 JSON 內。

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

服務對服務方式。Provisa 於啟動時按角色生成 `.proto` —— 每個角色只會看到其有權存取的表格及欄位。

下載您角色的 proto：

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

使用 `grpc_server_reflection` 以程式方式探索結構描述。

角色會透過每次 RPC 的 `x-provisa-role` 元數據鍵傳遞。串流查詢會逐列發出訊息；變異則為單元操作。

---

## 跨協定調用命令

**命令**是登記於 Provisa 語意層的已追蹤函式或 webhook —— 一個可調用元素，具有 `kind`（`query` 或 `mutation`）及描述其執行方式的 `impl_kind`。所有介面均透過單一受管治的執行器（`invoke_tracked_function`）路由調用，統一強制執行 `writable_by` 及治理（REQ-1156）。[tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | 執行內容 | 綁定欄位 |
|------------|-----------|---------------|
| `source_procedure` | 已登記數據來源上的儲存程序（預設值） | `sourceId`、`schemaName`、`functionName` |
| `script` | 伺服器端指令碼 | `script` |
| `http` | 對外 HTTP 呼叫 | `url`、`method` |
| `grpc` | 對外部伺服器的 gRPC 呼叫 | `target`、`method` |
| `python` | 由 Provisa 代管的 Python 可調用元素（REQ-885） | `callable`（例如 `demo.py_functions:random_dataset`） |

當命令宣告 `return_schema`（`type: array, items: object` 的 JSON Schema）時，即屬於集合回傳型 —— 所有介面均會將其投影為型別化的資料列集。示範命令 `random_python_set`（impl_kind 為 `python`）及 `random_grpc_set`（impl_kind 為 `grpc`）分別示範了代管的可調用元素及回傳隨機值資料列的外部 gRPC 橋接；兩者均已登記於 `config/provisa-install.yaml`。[tool-verified: `config/provisa-install.yaml:809-856`]

### 協定對照表

| 介面 | 語法 | 範例 |
|---------|--------|---------|
| GraphQL | `kind=query` → Query 欄位；`kind=mutation` → Mutation 欄位；`domain_prefix: true` 時加上網域前綴 | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` 或 `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP（`POST /data/cypher`） | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt（Neo4j Browser / 驅動程式） | `CALL fn(args)` —— 位置引數會對應至已宣告的引數名稱 | `CALL random_python_set(3, 7)` |
| Provisa gRPC（連接埠 50051） | 單元 `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

`kind` 欄位僅控制在 GraphQL 中的擺放位置 —— SQL、Cypher、Bolt 及 gRPC 介面均同樣接受 `query` 及 `mutation` 命令。

---

## Apollo Federation

Provisa 可作為 Federation v2 子圖，將其已發佈的結構描述向 Apollo Router 或 Apollo Gateway 公開。

### 設定

在 `config.yaml` 中啟用聯邦：
```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa 會自動於主索引鍵欄位生成 `@key` 指令，並於跨子圖關聯上生成 `@external`/`@provides`。

### 向 Apollo Router 登記

於您的 `supergraph.yaml` 中：
```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

執行 `rover supergraph compose --config supergraph.yaml` 以生成超級圖結構描述。

### 實體

Provisa 會回應 `_entities` 查詢，以進行跨子圖聯結。任何具有主索引鍵的表格均可自動作為 Federation 實體解析。

---

## Hasura v2 / DDN 匯入

有關由 Hasura 遷移至 Provisa，請參閱 [docs/import.md](import.md)。

---

## Kafka

有關將 Kafka 主題設定為唯讀表格及查詢結果接收端，請參閱 [docs/sources.md](sources.md#kafka)。

---

## Apache Ossie 語意交換（REQ-1316）

Provisa 透過邊界配接器，與 Apache Ossie（規格 0.2.0.dev0，孵化中；前稱 Open Semantic
Interchange）交換語意模型。Provisa 的內部詞彙永不會重新命名為 Ossie 的詞彙 —— 由於規格聲明
極有可能出現破壞性變更，因此耦合僅限於配接器之內。
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### 匯出

規範匯出介面是一個即時 HTTP 端點。它會於每次請求時，從即時狀態衍生 Ossie 文件 —— 無快取、
無生成步驟。

```
GET /admin/ossie
```

回應是一份 YAML 文件，帶有 `Content-Disposition: attachment; filename=provisa.ossie.yaml`。
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

Metrics 頁面亦於 Ossie Interchange 面板提供**下載**按鈕及可複製的端點 URL，兩者均指向同一
端點。
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### 匯出內容

配接器會將 Provisa 物件對應至 Ossie 物件，如下所示：

| Provisa 物件 | Ossie 物件 | 備註 |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`；主索引鍵／唯一索引鍵來自欄位設定及 `UniqueConstraint` |
| `Column` | `field` | `expression` = 欄位參照（ANSI_SQL 方言）；時間欄位會獲得 `dimension.is_time: true` |
| `Relationship` | `relationship` | 已設定別名時使用別名作為名稱；已計算（函式目標）的關聯會被略過 |
| `Metric` | `metric` | `name`、`expression`（ANSI_SQL）、`datatype`、`description`、`ai_context` —— 依設計無損 |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | 僅供往返使用；其他工具可予以忽略 |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

治理、行級安全、數據血緣及圖形語意均不會被匯出。它們可於 custom_extensions 的選用性
`provisa` 插槽中流轉，以維持往返保真度，但交換過程永不依賴其他工具讀取此資料。
[tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

未知的 Provisa 欄位型別會原樣通過；配接器永不會靜默地對應至錯誤型別。[tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### 型別對應

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Provisa／數據來源型別 | Ossie `datatype` |
| --- | --- |
| `varchar`、`text`、`char`、`uuid`、`string` | `string` |
| `int`、`integer`、`bigint`、`smallint`、`int4`、`int8`、`tinyint` | `integer` |
| `numeric`、`decimal`、`float`、`double`、`real` | `number` |
| `bool`、`boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`、`timestamptz`、`datetime` | `timestamp` |
| 其他任何型別 | 原樣通過 |

### 匯入

匯入接受 Ossie 文件（YAML 或 JSON），並回傳登記提案。系統不會自動登記任何內容 —— 已匯入的
定義永不會繞過覆核步驟。

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

伺服器以 `parse_ossie_model` 剖析文件，此函式會驗證結構，並回傳包含建議表格、關聯及指標（以
純字典形式呈現）的 `OssieImport` 資料類別。任何結構性問題均會傳回帶有具名路徑錯誤的
`400`，例如 `ossie import: missing semantic_model[0].datasets[1].source`。
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### 覆核畫面

在使用者介面中，**匯入**按鈕（Metrics 頁面 → Ossie Interchange 面板）會開啟檔案選取器。文件
提交並剖析後，會開啟覆核對話框，列出每項建議的表格、關聯及指標，並以已勾選項目呈現。建模
人員可取消勾選任何項目以將其排除。按一下**套用**後，已勾選的項目會透過現有的登記變異
（mutation）進行登記 —— 先登記表格，再登記關聯（因其參照表格），最後登記指標。
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

儲存於 Provisa 匯出之 Ossie 文件中的建模角色及歷史記錄，會透過匯入正確地往返還原。
[tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## 跨協定指標（REQ-1319）

受管治指標的定義 —— 其表達式、描述及 `ai_context` —— 會透過單一編譯器展開，隨其數值傳遞至
每個查詢介面。當中不存在任何複本。編譯器為 SQL 存取保留 `metrics` 結構描述；每個協定其後
再各自加入其本身的元數據通道。

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

將任何指標視為 `metrics` 結構描述中的虛擬關係加以定址。您所選取的維度欄位會成為 GROUP BY：

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

編譯器會在治理執行之前，將 `metrics.<name>` 形式展開為實際的分組聚合。欄位描述會以
`pg_description` 條目呈現，因此 DBeaver 及 psql 的 `\d+` 均可顯示。[tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` 會被拒絕 —— 請明確指定欄位名稱。
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

指標會於根欄位 `_aggregate` 內以 `metrics` 區塊形式投影。
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

定義文字（`description`、`ai_context`）會顯示於 GraphQL 內省文件中，因此具結構描述感知能力的
工具及程式碼生成器均可自動接收。
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP（AI 代理）

有兩個工具向 MCP 客戶端公開指標：

- **`list_metrics`** —— 回傳該工作階段可見的所有受管治指標，包含 `name`、`description` 及
  `ai_context`。
- **`query_metric`** —— 接受一個指標名稱及一個維度清單，並呼叫編譯器的語意 SQL 路徑，回傳
  聚合結果。

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

代理在建構查詢前先呼叫 `list_metrics`，即可按名稱選取受管治指標，而非手動撰寫聚合 SQL。
`ai_context` 欄位正是放置指引正確選取之定義文字的位置。

### Arrow Flight

指標可透過回傳 Arrow 表格的指標飛行描述符（metric flight descriptor）加以定址。
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

透過標準 Flight SQL 票證路徑，使用相同的 `metrics.<name>` SQL 形式。

### Bolt / Cypher（Neo4j Browser）

使用 `provisa.metric()` 程序調用指標：

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

事實表及維度表於聯邦圖中帶有 `:Fact` 及 `:Dimension` 節點標籤，因此 Bloom 可自動呈現星形
結構。
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### 自然語言查詢

自然語言結構描述比對器會將自然語言問題中的指標詞彙，直接解析為指標及維度，然後生成語意
SQL。[tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

事實表於自然語言提示中標記為 `[fact]`；維度表則標記為 `[dimension]`。比對器於解析問題時，會
偏向以事實表至維度表的聯結路徑。
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### 串流

將 `view_metrics` 與 `materialize` 及 Kafka 接收端結合，即可利用現有的具體化機制，產生變更即
推送（push-on-change）的指標輸出。毋須任何新管線。
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### 可觀測性（OTel）

指標評估會被追蹤，並可匯出為 OpenTelemetry 指標。
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
