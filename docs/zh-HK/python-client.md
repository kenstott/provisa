# Python 用戶端（`provisa-client`） {: #python-client-provisa-client }

Provisa 的 Python 用戶端。提供四種介面：

| 介面 | 使用場景 |
| ----------- | ---------- |
| `ProvisaClient` | GraphQL 查詢、Arrow Flight、DataFrame 輸出 |
| DB-API 2.0（`connect`） | 標準 Python 資料庫介面（PEP 249） |
| SQLAlchemy 方言 | BI 工具、ORM、Pandas `read_sql` |
| ADBC | 經 Flight 的 Arrow 原生欄式串流 |

## 安裝 {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### 快速上手 {: #quick-start }

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` 接收的是憑證。它沒有自己的登入步驟，所以從不索取使用者名稱及密碼。指令碼需要無人值守執行時，請使用個人存取權杖：你在自己的個人資料中簽發它，它有有效期，並且可以在不改動帳戶的情況下撤銷。提供者的 bearer 權杖用法相同。兩者都放在 `token` 中，用戶端會在 HTTP 及 Arrow Flight 兩條路徑上出示它。

若要用密碼換取權杖，向 `/auth/login` 發送 POST 並讀取 `access_token`：

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

DB-API 及 ADBC 入口會替你完成這次交換，詳見下文。

### GraphQL 查詢 {: #graphql-queries }

```python
# Raw response dict
result = client.query("{ orders { id amount region } }")

# With variables
result = client.query(
    "query Q($region: String!) { orders(region: $region) { id amount } }",
    variables={"region": "west"},
)

# pandas DataFrame (first root field is flattened)
df = client.query_df("{ orders { id amount region } }")
```

### 非同步 {: #async }

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight（高吞吐量欄式） {: #arrow-flight-high-throughput-columnar }

大型結果集請使用 Flight。資料以 Arrow 記錄批次的形式串流傳輸，從不在伺服器上具體化。

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight 預設連往連接埠 8815。 可以 `flight_port=` 覆寫：

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### 目錄探索 {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### 連線參考 {: #connection-reference }

| 參數 | 預設值 | 描述 |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Provisa 伺服器基礎 URL |
| `token` | `None` | Bearer 憑證——提供者權杖或個人存取權杖；若採密碼驗證則留空 |
| `role` | `None` | 要代為執行的角色，以 `X-Provisa-Role` 傳送，只有在該角色已指派給使用者時才會生效；省略時，請求以身份本身的角色執行 |
| `flight_port` | `8815` | Arrow Flight gRPC 連接埠 |

### 錯誤處理 {: #error-handling }

`query()` 於 HTTP 錯誤時擲出 `httpx.HTTPStatusError`。
`query_df()` 於回應含有 GraphQL 錯誤時擲出 `RuntimeError`。

---

## 在 Jupyter 中使用 Provisa {: #using-provisa-in-jupyter }

在匯入之前，先安裝到正在執行的核心中。`pandas` 擴充提供 DataFrame 輸出；如果還需要透過方言使用 `pandas.read_sql`，請加上 `sqlalchemy`。

```python
%pip install "provisa-client[pandas]"
```

### 憑證 {: #credentials }

筆記本會把儲存格輸出儲存到 `.ipynb` 檔案中。在儲存格裡輸入的權杖會和你的程式碼一起寫入磁碟。不要這樣做。

`ProvisaClient` 不讀取環境：請明確傳入憑證，取自環境變數。（下文的 `provisa_client.query()` 會替你讀取。）

```python
import os
from provisa_client import ProvisaClient

client = ProvisaClient(
    os.environ["PROVISA_URL"],           # choose your own variable name
    token=os.environ["PROVISA_TOKEN"],   # personal access token
    role=os.environ.get("PROVISA_ROLE"), # optional; omit to run as your own role
    org=os.environ.get("PROVISA_ORG"),   # required on a multi-tenant deployment; None otherwise
)
```

在啟動 Jupyter 之前在 shell 中設定這些變數，或用 `python-dotenv` 載入 `.env` 檔案。無論哪種方式，都不要讓它們出現在筆記本中。

### 一次呼叫：`query()` {: #one-call-query }

`provisa_client.query()` 接受一條 SQL、GraphQL 或 Cypher 陳述式，傳回一個 pandas DataFrame。由伺服器判斷是哪種語言。無需事先建構任何物件：連線來自環境或設定檔。

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` 以該角色執行一次讀取，作為 `X-Provisa-Role` 傳送。伺服器只對你持有的角色生效。讀取經由 Arrow Flight；將 `transport` 設為 `http` 可改為傳送到 `/data/query`。`await provisa_client.aquery(...)` 是可等待的形式。

每項設定從第一個指定它的位置讀取：

1. `provisa_client.sign_in(...)` 的關鍵字引數，它為之後的所有讀取一次性解析連線；
2. 環境變數：`PROVISA_URL`、`PROVISA_TOKEN`、`PROVISA_USERNAME`、`PROVISA_PASSWORD`、`PROVISA_ROLE`、`PROVISA_ORG`、`PROVISA_FLIGHT_PORT`、`PROVISA_TRANSPORT`；
3. `~/.provisa/credentials` 中的一個區段：`[default]`，或 `PROVISA_PROFILE`（或 `sign_in(profile=...)`）指定的那一段。

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

`token` 是個人存取權杖或工作階段權杖。如果改為提供 `username` 及 `password`，用戶端會在 `POST /auth/login` 把它們換成工作階段權杖。這適用於登入提供者接受密碼的部署（basic、simple、LDAP）。把密碼放在 `PROVISA_PASSWORD` 中，不要寫進檔案。登入被拒、缺少 URL，或指定的設定檔不在檔案中，都會報錯並說明原因。Provisa 沒有可由筆記本透過瀏覽器驅動的登入流程，所以單一登入的部署請使用個人存取權杖。

### 儲存格魔法指令 {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`、`%%graphql` 和 `%%cypher` 用 `query()` 回答儲存格。DataFrame 就是儲存格的結果。魔法指令後的名稱還會在筆記本中繫結該結果（上例中的 `eu`）；`--role` 以該角色執行這個儲存格。

JupySQL（`%load_ext sql`）也定義了 `%%sql`。兩者都載入時，`%%sql` 歸最後載入的擴充所有。Provisa 的版本接受 `--role`，並透過 Arrow Flight 讀取。JupySQL 也可以使用，透過 SQLAlchemy 方言：`%sql provisa+http://user:password@host:port` 建立連線，`%sql`/`%%sql` 執行受治理的陳述式，`%sqlcmd tables -s <domain>` 和 `%sqlcmd columns -t <table> -s <domain>` 列出目錄。

### 在瀏覽器中（Pyodide） {: #in-the-browser-pyodide }

在 Pyodide 下，`query()` 以頁面已登入使用者的身分，從提供該頁面的伺服器讀取。主機頁面把該使用者保存在核心的全域範圍中：

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

對於頁面自身的來源，`baseUrl` 為空。用戶端每次請求都會讀取 `provisaAuth`，因此輪換後的權杖無需重新啟動即可生效。載入了 `pyarrow` 時結果以 Arrow 傳回；否則以 JSON 傳回。Cypher 陳述式總是以 JSON 傳回。`query()` 是阻塞呼叫，Web Worker（筆記本核心執行的地方）可以這樣做；在頁面主執行緒上它會拋出錯誤，應改用 `await provisa_client.aquery(...)`。

### 從 GraphQL、SQL 和 Cypher 取得 DataFrame {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` 接受這三種查詢語言，並傳回 pandas DataFrame。

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

GraphQL 回應把資料放在一個根欄位名稱下；`query_df()` 會將其提取出來。SQL 和 Cypher 回應帶有伺服器給出的明確欄位名稱，直接對應。

### 大量讀取用 Arrow Flight {: #arrow-flight-for-large-pulls }

`flight_df()` 以 Arrow 記錄批次串流傳輸資料，而不是反序列化 JSON。對於寬表或超過幾十萬列的結果集，它更快，佔用的記憶體更少。

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow 會保留欄類型。整數仍是整數，時間戳記仍是時間戳記，而 JSON 做不到這一點。在意結構描述時，單憑這一點就該優先選擇 Flight。

Flight 預設連線 8815 連接埠。如果伺服器使用其他連接埠，請向 `ProvisaClient` 建構函式傳入 `flight_port=`。

### 透過 SQLAlchemy 方言使用 `pandas.read_sql` {: #pandasread_sql-through-the-sqlalchemy-dialect }

把 `sqlalchemy` 擴充安裝到核心中：

```python
%pip install "provisa-client[sqlalchemy]"
```

然後用 `provisa+http://` URL 建構一個 engine。從環境變數讀取憑證，這樣它們永遠不會出現在你的筆記本裏。

```python
import os
import pandas as pd
from sqlalchemy import create_engine

host = os.environ["PROVISA_HOST"]       # e.g. "localhost:8001"
user = os.environ["PROVISA_USER"]
pwd  = os.environ["PROVISA_PASSWORD"]

engine = create_engine(f"provisa+http://{user}:{pwd}@{host}")

df = pd.read_sql("SELECT id, amount FROM orders", engine)
```

該方言只把使用者名稱及密碼向 `/auth/login` 傳送一次，並在 engine 的整個生命週期內持有得到的權杖。憑證不會隨每次查詢重複傳送。

---

## DB-API 2.0

標準 [PEP 249](https://peps.python.org/pep-0249/) 介面。 適用於任何接受 DB-API 連線的工具。

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` 把使用者名稱及密碼傳送到 `/auth/login`，並持有傳回的 `access_token`，所以連線攜帶的是真正的憑證。`role` 用於*請求*某個角色，只有當身分被指派了該角色時伺服器才會採納。省略時，連線以登入解析出的角色執行。

### 執行查詢 {: #executing-queries }

游標接受 GraphQL 或 SQL，並自動識別收到的是哪一種。

```python
cur = conn.cursor()

# GraphQL
cur.execute("{ orders { id amount region } }")
rows = cur.fetchall()           # list of tuples
one  = cur.fetchone()           # single tuple or None
many = cur.fetchmany(size=50)   # up to N tuples

# SQL (routed through Stage 2 governance)
cur.execute("SELECT id, amount FROM orders WHERE region = 'west'")
rows = cur.fetchall()
```

### 欄位中繼資料 {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### 具名參數 {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Context Manager {: #context-managers }

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## SQLAlchemy 方言 {: #sqlalchemy-dialect }

```bash
pip install "provisa-client[sqlalchemy]"
```

URL 結構描述：`provisa+http://` 或 `provisa+https://`

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### 搭配 pandas {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### URL 參數 {: #url-parameters }

| 參數 | 描述 | 預設值 |
| ----------- | ------------- | --------- |
| `role` | 要請求的角色；由伺服器端驗證 | 登入所解析出的角色 |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### 結構描述內省 {: #schema-introspection }

方言反映提供給你的角色的目錄，以連線的使用者、角色和組織身分從 `GET /data/catalog` 讀取。每個網域是一個結構描述，每張資料表都位於其中之一：`get_schema_names()` 列出各個網域，`get_table_names(schema="shop")` 按 SQL 名稱列出該網域的資料表，`get_columns("orders", schema="shop")` 給出每個欄位的類型、可空性和描述。未指定結構描述時，`get_table_names()` 不列出任何資料表。未知的資料表會拋出 `NoSuchTableError`；伺服器拒絕的目錄會拋出錯誤。

---

## ADBC

基於 Arrow Flight 的 Arrow Database Connectivity。它直接傳回 `pyarrow.Table`，無須 JSON 反序列化。

```bash
pip install "provisa-client[adbc]"
```

```python
from provisa_client.adbc import adbc_connect

conn = adbc_connect(
    "http://localhost:8001",
    user="alice",
    password="secret",
    role="analyst",   # optional; server validates the requested role
    port=8815,        # Arrow Flight port
)
```

`adbc_connect` 先透過 HTTP 登入，並把得到的權杖放進每張 Flight 票據，所以 Flight 伺服器認證連線的方式與 REST 介面相同。`role` 參數是一個請求，伺服器會依據身分的指派對其驗證。它永遠不會變成身分本身。

### 擷取為 Arrow Table {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### 擷取為 tuple {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### 欄位中繼資料 {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Context Manager {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC 預設連往連接埠 8815 上的 Flight 伺服器。 傳入 `port=` 可連往綁定於非預設連接埠的 Flight 伺服器。
