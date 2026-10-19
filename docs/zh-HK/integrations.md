# 整合 {: #integrations }

## 選擇連線路徑 {: #choosing-a-connection-path }

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

## pgwire —— 原生 PostgreSQL 驅動程式 {: #pgwire-native-postgresql-driver }

Provisa 實作了 PostgreSQL 線路通訊協定（協定版本 3.0）。任何支援 PostgreSQL 的用戶端都可以無需自訂驅動程式即可連線。

啟動 Provisa 之前，設定 `PROVISA_PGWIRE_PORT`（例如 `5433`）即可啟用。未設定或設為 `0` 時停用。

### 為何選用 pgwire 而非 JDBC？ {: #why-pgwire-instead-of-jdbc }

JDBC 驅動程式經 Arrow Flight 傳輸查詢，使用它需要部署 `provisa-jdbc.jar`。pgwire 不需要任何額外的東西。如果您已經有 `psql`、DBeaver、SQLAlchemy 或 PG JDBC 驅動程式，就可以直接使用，所以只傳送 SQL 時，這是較簡單的方式。

對於內建 JDBC 連線精靈、需要擷取大型結果集的 BI 工具，選用 JDBC，Arrow Flight 的欄式串流在這時最划算。pgwire 接受針對整個已發佈結構描述的同樣自由 SQL，需要設定的較少。

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host：`localhost`，Port：`5433`
3. 使用者名稱／密碼依 Provisa 中的設定
4. 不需要額外下載驅動程式

### SQLAlchemy（Python） {: #sqlalchemy-python }

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

或使用 `asyncpg`：

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### 身分驗證 {: #authentication }

啟動封包中的 `password` 欄位帶有憑證，而該憑證*究竟是什麼*決定了驗證方式：個人存取權杖、OIDC bearer token，或是針對已設定供應商的密碼。在 `basic` 供應商且 `auth.scram: true` 的情況下，密碼會透過 SCRAM-SHA-256 進行驗證，而非直接傳送。支援用戶端憑證。在信任模式（`none`）下，使用者名稱會直接對應至一個角色，密碼則被忽略。

完整的介面 × 驗證方式對照表載於[安全模型](security.md#surfaces-and-credentials)。不支援 MD5；在不受信任的網絡上運行時，請啟用 TLS（`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`）。

### 限制 {: #limitations }

- 僅支援 SQL。pgwire 不接受 GraphQL 及 Cypher。
- 並非唯讀。已註冊資料表的資料列透過 `INSERT`、`UPDATE`、`DELETE`、`MERGE`、`TRUNCATE` 及 `COPY ... FROM STDIN` 寫入，每一種都由角色的 `write` 權利、其欄位授權及其資料列過濾放行；請參閱[數據寫入](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin)。
- 不支援定義：`CREATE`、`ALTER` 及 `DROP` 會以 SQLSTATE 0A000 被拒絕，工作階段自己的暫存資料表除外；請參閱[定義](pgwire.md#definitions-create-alter-drop)。`COPY ... TO STDOUT` 以 `text`、`csv` 及 `binary` 匯出；`COPY ... FROM STDIN` 讀取 `text` 及 `csv`。
- `information_schema` 及 `pg_catalog` 的查詢會被攔截，並由 DuckDB 目錄墊片 (shim) 回應，因此結構描述探索工具可正常運作。

---

## JDBC 驅動程式 {: #jdbc-driver }

Provisa JDBC 驅動程式以 Arrow Flight 作為其底層傳輸方式。對於具有 JDBC 連線精靈的 BI 工具，這是建議使用的路徑。

當開啟連線時 Flight 連接埠有回應，查詢便會經由 Arrow Flight 進行，結果連同欄位類型逐批到達。Flight 連接埠被視為 HTTP 連接埠旁邊的那一個（8001 旁的 8815）；並非如此時請設定 `flight_port` 屬性。若連線時 Flight 連接埠無法連上，連線會把 HTTP API 用於其查詢，並在 INFO 層級記錄這一點。當連線已經在使用 Flight 時，伺服器錯誤——被拒絕的憑證、使用者並不持有的角色、失敗的陳述式——會連同伺服器的訊息傳給呼叫者；它不會再經由 HTTP 重試。

瀏覽結構描述（`getTables`、`getColumns`、主鍵及外鍵）以登入憑證經由同一個 Flight 連接埠讀取：驅動程式列出它所代為執行的角色的目錄，因此未授予該角色的資料表或欄位不會出現。參照未授予該角色的資料表或欄位的鍵亦不會列出。在連線時 Flight 連接埠無法連上的連線上，驅動程式經由 HTTP（`GET /data/catalog`）讀取同一個目錄：伺服器以同一個函數建構兩份清單，因此資料表、欄位名稱、類型及鍵與哪個連接埠回應無關。

### 連線 {: #connection }

下載 [provisa-jdbc.jar](https://provisa.dev/dl/jdbc)（永遠是最新版本），並將其加入你所使用工具的驅動程式路徑。

JDBC 網址：

```yaml
jdbc:provisa://<host>:8815
```

身分驗證使用標準的 JDBC `user` / `password` 屬性。驅動程式以它們換取一個工作階段權杖，並隨每個請求傳送；被拒絕的登入會以伺服器的狀態及原因令連線失敗。Provisa 從登入的身份推導角色。可選屬性 `role`（或 URL 中的 `?role=`）要求以使用者持有的一個角色執行，或以這類角色的逗號分隔集合執行；使用者並不持有的角色會被拒絕。在沒有身分驗證提供者的部署中沒有登入，使用者名稱會被當作所要求的角色。

### BI 工具設定 {: #bi-tool-setup }

**Tableau**

1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL：`jdbc:provisa://localhost:8815`
4. 於提示時輸入你的使用者名稱及密碼

**DBeaver**（JDBC 路徑；pgwire 路徑請見上文）

1. Database → New Connection → JDBC
2. Driver：新增 `provisa-jdbc.jar`
3. URL：`jdbc:provisa://localhost:8815`
4. 於 Authentication 分頁輸入你的使用者名稱及密碼

**Power BI**——使用 ODBC 閘道，並搭配安裝程式內附的 Provisa JDBC-ODBC 橋接器。

---

## Arrow Flight 用戶端 {: #arrow-flight-clients }

對於支援 Arrow Flight（連接埠 8815）的數據工具而言，這是建議使用的路徑。結果會以 Arrow RecordBatch 形式串流傳輸，不會在 Provisa 記憶體中具體化。

### Python（`provisa-client`） {: #python-provisa-client }

建議使用的 Python 路徑同時包裝了 GraphQL 及 Arrow Flight：

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

### Python（原始 PyArrow） {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight 會將其憑證帶在 JSON 承載內容中的 `token` 欄位，其中可以是供應商 bearer token，也可以是個人存取權杖。握手階段及每一張票證都接受它，且兩者的驗證方式相同，因此在握手階段已通過驗證的用戶端，仍需要在每次 `do_get` 時提交該權杖。與之並列的 `role` 欄位是*請求*一個角色；伺服器會推導出該身分獲准使用的角色，並代入經授權的值，因此票證中的角色字串絕不能視為身分本身。請參閱[安全模型](security.md#surfaces-and-credentials)。

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

瀏覽同樣需要憑證。`list_flights`、`get_flight_info` 及 `get_schema` 不攜帶 ticket，因此它們從呼叫的 `authorization: Bearer <token>` 標頭讀取憑證，並從 `x-provisa-role`（一個角色，或已持有角色的逗號分隔集合）讀取所要求的角色。啟用身分驗證時，沒有憑證的瀏覽呼叫會被拒絕，它傳回的目錄是已授權角色的目錄：授予該角色的資料表、欄位及命令，僅此而已。目錄 ticket（不帶 `query` 的 ticket）以同樣方式，根據其本身的 `token` 及 `role` 回應。因此用戶端必須在瀏覽之前傳送其憑證；`provisa-client` 在 `list_tables()` 中這樣做。在沒有身分驗證提供者的部署中，這些呼叫會列出整個目錄，且不接受憑證。

```python
options = flight.FlightCallOptions(headers=[
    (b"authorization", b"Bearer provisa_pat_..."),
    (b"x-provisa-role", b"analyst"),
])
for info in client.list_flights(b"", options):
    print(info.descriptor.path)
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

### Spark（PySpark） {: #spark-pyspark }

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC（連接埠 50051） {: #protobuf-grpc-port-50051 }

服務到服務方式。Provisa 按角色產生 `.proto`，並在模型或角色變更時重新產生，每個角色只能看到其有權存取的資料表及欄。服務為角色結構描述根部的每張資料表提供一個 RPC；關聯資料表與 GraphQL 中一樣，透過其關聯存取。

下載你所屬角色的 proto：

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

使用 `grpc_server_reflection` 以程式方式探索結構描述。反射描述的是呼叫所擔任角色的服務，與上面下載的檔案相同。未啟用驗證時，反射呼叫與其他呼叫一樣指明其角色：

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

每個 RPC 都必須在 `authorization` 中繼資料鍵中帶有憑證，可以是供應商 token 或個人存取權杖。`x-provisa-role` 會從該身分獲准的角色集合中請求一個角色；它不是憑證，也從來都不是。支援用戶端憑證。請參閱[安全模型](security.md#surfaces-and-credentials)。

串流查詢每筆資料列發出一則訊息；mutation 則為單一（unary）呼叫。

---

## 跨通訊協定呼叫 Command {: #invoking-commands-across-protocols }

**Command** 是一個已註冊的追蹤函式或 webhook，即一個在 Provisa 模型中註冊的可呼叫項目，帶有 `kind`（`query` 或 `mutation`）及描述其運行方式的 `impl_kind`。每個介面都會透過單一受治理的執行器（`invoke_tracked_function`）路由呼叫，統一強制執行 指令的分配（`visible_to`）、變更所需的 `write` 權利 及治理規則。

| `impl_kind` | 運行內容 | 綁定欄位 |
| ------------ | ----------- | --------------- |
| `source_procedure` | 已註冊來源上的預存程序（預設） | `sourceId`、`schemaName`、`functionName` |
| `script` | 伺服器端指令碼 | `script` |
| `http` | 對外 HTTP 呼叫 | `url`、`method` |
| `grpc` | 對外呼叫外部伺服器的 gRPC | `target`、`method` |
| `python` | 由 Provisa 代管的 Python 可呼叫項目 | `callable`（例如 `demo.py_functions:random_dataset`） |

當一個 command 聲明了 `return_schema`（`type: array, items: object` 的 JSON Schema）時，即代表它會傳回一個集合，每個介面都會將其投影為具型別的資料列集。示範用的 `random_python_set`（impl_kind 為 `python`）及 `random_grpc_set`（impl_kind 為 `grpc`）示範了代管可呼叫項目及外部 gRPC 橋接兩種傳回隨機值資料列的方式；兩者皆在 `config/provisa-install.yaml` 中註冊。

### 通訊協定對照表 {: #protocol-matrix }

| 介面 | 語法 | 範例 |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → Query 欄位；`kind=mutation` → Mutation 欄位；當 `domain_prefix: true` 時附加網域前綴 | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` 或 `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP（`POST /data/cypher`） | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt（Neo4j Browser／驅動程式） | `CALL fn(args)`——位置參數對應至已聲明的引數名稱 | `CALL random_python_set(3, 7)` |
| Provisa gRPC（連接埠 50051） | 單一呼叫 `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

`kind` 欄位只控制其在 GraphQL 中的位置。SQL、Cypher、Bolt 及 gRPC 介面對 `query` 與 `mutation` 兩種 command 的接受方式完全相同。

---

## Apollo Federation

Provisa 可作為 Federation v2 子圖，將其已發佈的結構描述公開給 Apollo Router 或 Apollo Gateway。

### 設定 {: #setup }

在 `config.yaml` 中啟用 federation：

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa 會自動在主索引鍵欄上產生 `@key` 指令，並在跨子圖關聯上產生 `@external`／`@provides` 指令。

### 向 Apollo Router 註冊 {: #register-with-apollo-router }

在你的 `supergraph.yaml` 中：

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

執行 `rover supergraph compose --config supergraph.yaml` 以產生 supergraph 結構描述。

### 實體 {: #entities }

Provisa 會回應 `_entities` 查詢以進行跨子圖 join。任何具有主索引鍵的資料表，都會自動可被解析為一個 Federation 實體。

---

## Hasura v2 / DDN 匯入 {: #hasura-v2-ddn-import }

有關從 Hasura 遷移至 Provisa 的內容，請參閱 [docs/import.md](import.md)。

---

## Kafka

有關將 Kafka 主題設定為唯讀資料表及查詢結果接收端 (sink) 的內容，請參閱 [docs/sources.md](sources.md#kafka-sources)。

---

## 數據品質檢查工具 {: #data-quality-checkers }

Soda Core 及 Great Expectations 像任何其他 postgres 用戶端一樣，透過 pgwire 連接 Provisa。這就是整個整合方式的全部：檢查工具只需一個 postgres 驅動程式即可掃描聯邦檢視，因此無論是 Snowflake 資料表、Iceberg 資料表還是 Mongo 集合 (collection)，都由同一套合約方言檢查，不需要為每個系統各自建立檢查工具。

掃描運行於一個子直譯器中——`python -m provisa.dq.worker`——這是唯一匯入 `soda_core` 或 `great_expectations` 的地方。伺服器行程中不會連結任何相關程式庫，檢查工具發生崩潰時只會拖垮一個子行程，而不會影響事件迴圈。

掃描結果會以一般來源資料列的形式落地，因此頻率 (cadence)、新鮮度、事件、數據血緣、治理、RLS、資料格線 (grid) 及匯出功能，全部無需第二套機制即可套用。合約撰寫、結果封裝格式及衍生註冊，請見 [docs/sources.md](sources.md#data-quality-checkers)。

### 安裝檢查工具 {: #installing-a-checker }

兩個程式庫預設皆不隨附。安裝程式會詢問你想使用哪一個，並將答案寫入 `~/.provisa/config.yaml` 中的 `dq_checker: none|soda|gx`。在 Docker 層，`scripts/provisa` 會將其轉換為 `PROVISA_EXTRAS` 建置引數；在原生層，`first-launch.sh` 會將相符的 pyproject extra 安裝至虛擬環境中。

| `dq_checker` | 程式庫 | 授權條款 | 代管雲端平面 |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | 不允許（`cloud_eligible: false`） |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | 允許 |

Elastic License 2.0 禁止以代管服務的形式向第三方提供該軟件，而在 SaaS 平面內代表租用戶運行 Soda 正屬於此類情況。若代管部署想使用 Soda，須改為指向由營運方自行運行的 Soda 端點。連線設定鍵請見 [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations)。

---

## Apache Ossie 語義交換 {: #apache-ossie-semantic-interchange }

Provisa 透過一個邊界轉接器與 Apache Ossie（規格 0.2.0.dev0，孵化中；前稱 Open Semantic Interchange）交換語義模型。Provisa 的內部詞彙從不改名為 Ossie 的詞彙。該規格聲明可能出現破壞性變更，因此耦合只限於轉接器之內。

草案規格曾在不更改版本字串的情況下改變形態。轉接器遵循
`apache/ossie` 提交 `642cb894`（2026-10-10）時的規格：每份文件一個模型，
文件根部包含 `version`、`name`、`datasets`、`relationships` 及 `metrics`，以及
列舉的 `datatype`。匯出在單元測試中按該提交的
`core-spec/ossie-schema.json` 驗證。

### 匯出 {: #export }

正式的匯出介面是一個即時 HTTP 端點。它在每次請求時從即時狀態推導出 Ossie 文件，
沒有快取，也沒有產生步驟。

```http
GET /admin/ossie
```

回應是一份 YAML 文件，帶有 `Content-Disposition: attachment; filename=provisa.ossie.yaml`。

指標頁面在 Ossie Interchange 面板上亦提供一個**下載**按鈕及可複製的端點 URL，
兩者都指向同一個端點。

#### 匯出的內容 {: #what-is-exported }

轉接器把 Provisa 物件對映為 Ossie 物件如下：

| Provisa 物件 | Ossie 物件 | 說明 |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`；主鍵/唯一鍵來自欄位設定及 `UniqueConstraint` |
| `Column` | `field` | `expression` = 欄位參照（ANSI_SQL 方言）；時間欄位取得 `dimension.is_time: true` |
| `Relationship` | `relationship` | 設定了別名時用作名稱；計算關係（以函數為目標）會被略過 |
| `Metric` | `metric` | `name`、`expression`（ANSI_SQL）、`datatype`（按欄位的方式對映）、`description`、`ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | 僅供往返；其他工具可以忽略 |

跨多個欄位的關係會在 `from_columns` 及 `to_columns` 中列出每個欄位。
沒有資料表的模型會以 `422` 被拒絕：Ossie 文件至少有一個 dataset。

治理、行層級安全、血緣及圖語義不會匯出。為了往返的保真度，它們可以放在可選的
`provisa` custom_extensions 槽位中傳遞，但交換從不依賴其他工具是否讀取它。

Ossie 詞彙以外的欄位類型寫作 `Opaque`，並在欄位的 `provisa` 擴充中
寫入其本身的類型名稱，這正是規格對沒有對應成員的已知類型所規定的做法。
沒有記錄類型的欄位不指定 `datatype`。轉接器從不對映至欄位並不具有的類型。

#### 類型對映 {: #type-mapping }

| Provisa / 數據來源類型 | Ossie `datatype` |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `String` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `Integer` |
| `numeric`, `decimal` | `Decimal` |
| `float`, `double`, `real` | `Float` |
| `bool`, `boolean` | `Boolean` |
| `date` | `Date` |
| `time` | `Time` |
| `timestamp`, `datetime` | `DateTime` |
| `timestamptz`, `timestamp with time zone` | `DateTimeTz` |
| 任何其他類型 | `Opaque`，類型名稱寫於 `provisa` 擴充中 |

Ossie 的 `Decimal` 不攜帶精度及小數位數，因此 `numeric(10,2)` 匯出為 `Decimal`，
從匯入傳回時為 `decimal`。

### 匯入 {: #import }

匯入接受一份 Ossie 文件（YAML 或 JSON）並傳回註冊建議。不會自動註冊任何內容，匯入的定義亦從不繞過審查步驟。

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

伺服器以 `parse_ossie_model` 剖析文件，它驗證結構並傳回一個
`OssieImport` dataclass，其中建議的資料表、關係及指標以一般字典表示。
任何結構問題都是一個 `400`，其錯誤按路徑指名，例如
`ossie import: missing $.datasets[1].source`。

匯入器只讀取文件的扁平形態。三種情況會被點名拒絕：

- 把其模型包裹於 `semantic_model` 之內的文件，這是草案規格在
  2026 年 9 月之前的形態。錯誤會說明該文件早於扁平形態，以及如何轉換。
- 不是 `0.2.0.dev0` 的 `version`。
- 不屬於 Ossie 詞彙成員的 `datatype`；錯誤會指名路徑並列出
  成員。

建議的欄位或指標取得其 Ossie datatype 所代表的類型（`Integer` 建議
`bigint`，`DateTime` 建議 `timestamp`）。當文件來自 Provisa 時，`Opaque` 建議 `provisa` 擴充所指名的類型，
否則不建議類型。以物件形式提供的 `ai_context` 按其 `instructions` 讀取。

只有當欄位的運算式是單一個數據來源欄位時，它才會被建議為欄位。運算式為計算的欄位
不會被建議；審查畫面會在其資料表之下把它列為未匯入。重新命名其欄位的欄位會被建議為該欄位，
並以欄位名稱作為欄位的別名。dataset 的 `source` 必須由三部分組成，
即 `source.schema.table`；查詢或較短的名稱會被拒絕，錯誤會如此說明。

#### 來自 dbt、Cube、Snowflake 及其他語義層 {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

Apache Ossie 專案維護 Ossie 與其他工具模型格式之間的轉換器。執行
你所用工具的轉換器，然後把它的輸出傳送至匯入端點，或在下方的審查畫面上
選取該檔案。Provisa 讀取 Ossie 文件；它不會讀取其他工具的檔案。

| 來源 | 轉換器命令 | 它讀取甚麼 |
|---|---|---|
| dbt（語義層） | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | dbt 為 MetricFlow 寫出的 `semantic_manifest.json` |
| Cube | `ossie-cube import -i model/ -o model.yaml` | Cube 模型目錄，或個別的 cube 及 view 檔案 |

同一個儲存庫亦有面向 Snowflake、Databricks、Microsoft、Salesforce、
ThoughtSpot、Sigma、Omni、Hex、GoodData 等的轉換器；每個都有自己的 README。

轉換之前值得知道的兩項限制：

- dbt 轉換器只讀取語義層清單。沒有語義
  模型的 dbt 專案沒有可供它轉換的內容。它會捨棄轉化指標、私有指標及
  自然鍵實體，並為每一項印出警告。

- 轉換器的輸出遵循該轉換器本身提交時的規格。Provisa 讀取
  `apache/ossie` 提交 `642cb894` 的扁平形態；早於 2026 年 9 月的轉換器
  會寫出 `semantic_model` 包裝，Provisa 會點名拒絕它。

兩條路徑均於 2026-10-10 執行，所用轉換器安裝自 `apache/ossie` 提交 `642cb894`
（它們不以套件發佈；從儲存庫的 `converters/dbt` 及
`converters/cube` 目錄安裝，需要 Python 3.11 或更新版本）。一個由
兩張資料表、一個關係及一個指標組成的模型從 Provisa 匯出，轉換為 dbt 語義清單
及 Cube 模型，再以 `ossie-dbt msi-to-ossie` 及
`ossie-cube import` 轉換回 Ossie，並由 `parse_ossie_model` 讀取：

| 路徑 | 結果 |
|---|---|
| Cube 模型 → Ossie → Provisa | 已讀取。資料表、鍵、欄位類型、關係及指標都回來了；關係名稱是轉換器的名稱（`orders_to_customers`）。 |
| dbt 語義清單 → Ossie → Provisa | 已讀取。資料表、主鍵及指標回來了。欄位類型沒有（轉換器的輸出不指定它們），而關係以轉換器從 dbt 實體推斷出的形式回來，並非已註冊的那一個。套用之前請檢查關係。 |

當任何資料表具有跨多個欄位的唯一約束時，dbt 轉換器會拒絕該模型
（「MetricFlow entities cannot represent composite keys losslessly」）。

#### 審查畫面 {: #the-review-screen }

在介面中，**匯入**按鈕（指標頁面 → Ossie Interchange 面板）會開啟檔案選擇器。文件提交並剖析後，會開啟一個審查對話框，其中每個建議的資料表、關係及指標都以已勾選的項目列出。建模者可以取消勾選任何內容以將其排除。 **套用**會透過現有的註冊變更註冊已勾選的項目：先註冊資料表，再註冊關係（它們參照資料表），然後註冊指標。

保存在 Provisa 匯出的 Ossie 文件中的建模角色及歷史，在匯入時能正確往返。

### dbt sources 檔案 {: #dbt-sources-file }

對於不使用語義層的 dbt 專案，Provisa 直接寫出 dbt 的 `sources.yml`。
它在每次請求時從即時模型推導。

```http
GET /admin/dbt/sources?role=<role id>
```

回應是 YAML，帶有 `Content-Disposition: attachment; filename=provisa.sources.yml`。
呼叫者需要 `org_settings`，而 `role` 必須是其持有的角色；持有
`access_config` 者可以指名任何角色。指標頁面上的交換面板為正在使用的角色
提供相同的下載。

該檔案在每個數據來源及結構描述所對應的 dbt source 之下，列出角色可取得的每張資料表（以
數據來源 id 命名，當數據來源的資料表分佈於多個結構描述時為 `<source>_<schema>`），
附帶資料表的描述、可取得欄位的描述，以及以下測試：

| 模型指定 | 檔案寫出 |
|---|---|
| 單一欄位主鍵 | 該欄位上的 `unique` 及 `not_null` |
| 多欄位主鍵 | 每個欄位上的 `not_null` |
| 單一欄位唯一約束 | 該欄位上的 `unique` |
| 兩張均可由該角色取得的資料表之間的關係 | 來源欄位上的 `relationships`，指向 `source('<source>', '<table>')` |

角色無法取得的欄位不會出現，沒有可取得欄位的資料表也不會出現。跨
多個欄位的鍵沒有標準 dbt 測試；檔案的開頭註解會指名每個這樣的鍵，並且
不會為其寫入測試。模型不記錄可否為空，因此 `not_null` 只出現在鍵欄位上。
該檔案不攜帶指標、行層級安全、遮罩、角色或血緣，其開頭註解
亦如此說明。

由兩張資料表及一個關係的模型產生的檔案，已由 dbt-core 1.12.5 配合
dbt-duckdb 1.11.0 剖析：兩個 source、五個測試，沒有警告。dbt-core 1.9.11 無法讀取
其 `relationships` 測試，這些測試把引數巢狀於 `arguments` 之下；中間版本
未經測試。

---

## 跨通訊協定的指標 {: #metrics-across-protocols }

一個受治理指標的定義——其運算式、描述及 `ai_context`——會透過單一編譯器展開，隨著其值一同傳遞至每一個查詢介面。不存在任何副本。編譯器為 SQL 存取保留了 `metrics` 結構描述；每個通訊協定再各自加上自己的中繼資料通道。

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

編譯器會在治理機制運行之前，將 `metrics.<name>` 形式展開為實際的分組聚合。欄描述會以 `pg_description` 項目的形式呈現，因此 DBeaver 及 psql 的 `\d+` 都能顯示它們。

`SELECT *` 會被拒絕，因此請明確指名所需的欄。

### GraphQL

指標會在 `_aggregate` 根欄位內以 `metrics` 區塊的形式投影。

定義文字（`description`、`ai_context`）會出現在 GraphQL introspection 文件中，因此具結構描述感知能力的工具及程式碼產生器可以自動取得這些內容。

### MCP（AI 代理程式） {: #mcp-ai-agents }

有兩個工具向 MCP 用戶端公開指標：

- **`list_metrics`**——傳回該工作階段可見的所有受治理指標，包含 `name`、`description` 及 `ai_context`。
- **`query_metric`**——接受一個指標名稱及一份維度清單，並呼叫編譯器的語義 SQL 路徑，傳回聚合結果。

在建構查詢之前先呼叫 `list_metrics` 的代理程式，會依名稱選取一個受治理指標，而非自行手寫聚合 SQL。`ai_context` 欄位正是用來放置引導正確選取的定義文字之處。

### Arrow Flight {: #arrow-flight }

指標可作為傳回 Arrow 資料表的指標 flight descriptor 來定址。

透過標準 Flight SQL 票證路徑，使用同樣的 `metrics.<name>` SQL 形式。

### Bolt / Cypher（Neo4j Browser） {: #bolt-cypher-neo4j-browser }

使用 `provisa.metric()` 程序呼叫一個指標：

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

Fact 及 Dimension 資料表在聯邦圖中帶有 `:Fact` 及 `:Dimension` 節點標籤，因此 Bloom 能自動繪製出星形結構。

### 自然語言查詢 {: #natural-language-queries }

自然語言結構描述比對器 (NL schema matcher) 會將自然語言問題中的指標詞彙，直接解析為一個指標加上維度，然後產生語義 SQL。

Fact 資料表在自然語言提示中會標記為 `[fact]`；Dimension 資料表則標記為 `[dimension]`。此比對器在解析問題時，會偏向選擇由 fact 指向 dimension 的 join 路徑。

### 串流 {: #streaming }

將 `view_metrics` 與 `materialize` 結合，即可利用既有的具體化機制，在輸入變化時令指標視圖保持最新。不需要新的管線。

### 可觀測性（OTel） {: #observability-otel }

指標評估過程會被追蹤，並可匯出為 OpenTelemetry 指標。
