# Python 用戶端（`provisa-client`）

Provisa 的 Python 用戶端。提供四種介面：

| 介面 | 使用場景 |
| ----------- | ---------- |
| `ProvisaClient` | GraphQL 查詢、Arrow Flight、DataFrame 輸出 |
| DB-API 2.0（`connect`） | 標準 Python 資料庫介面（PEP 249）(REQ-268) |
| SQLAlchemy 方言 | BI 工具、ORM、Pandas `read_sql` (REQ-270) |
| ADBC | 經 Flight 的 Arrow 原生欄式串流 (REQ-271) |

## 安裝

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### 快速上手

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    username="alice",
    password="secret",
)
```

### GraphQL 查詢

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

### 非同步

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight（高吞吐量欄式）

大型結果集請使用 Flight——數據以 Arrow record batch 形式串流，不會於伺服端具體化。(REQ-143、REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight 預設連往連接埠 8815。(REQ-143) 可以 `flight_port=` 覆寫：

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### 目錄探索

```python
tables_df = client.list_tables()
```

### 連線參考

| 參數 | 預設值 | 描述 |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Provisa 伺服器基礎 URL |
| `token` | `None` | 持有者權杖；若採密碼驗證則留空 (REQ-606) |
| `role` | `"admin"` | 隨每個要求傳送的角色 (REQ-273) |
| `flight_port` | `8815` | Arrow Flight gRPC 連接埠 (REQ-143) |

### 錯誤處理

`query()` 於 HTTP 錯誤時擲出 `httpx.HTTPStatusError`。(REQ-607)
`query_df()` 於回應含有 GraphQL 錯誤時擲出 `RuntimeError`。(REQ-607)

---

## DB-API 2.0

標準 [PEP 249](https://peps.python.org/pep-0249/) 介面。(REQ-268) 適用於任何接受 DB-API 連線的工具。

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="admin",       # optional, default "admin"
)
```

### 執行查詢

游標接受 GraphQL 或 SQL——會自動偵測。(REQ-268、REQ-274)

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

### 欄位中繼資料

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### 具名參數

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Context Manager

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## SQLAlchemy 方言

```bash
pip install "provisa-client[sqlalchemy]"
```

URL 結構描述：`provisa+http://` 或 `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### 搭配 pandas

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### URL 參數

| 參數 | 描述 | 預設值 |
| ----------- | ------------- | --------- |
| `role` | Provisa 角色 | `admin` |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### 結構描述內省

該方言實作了 `get_table_names()`、`get_columns()` 及 `has_table()`——目錄工具（DBeaver、SQLAlchemy automap）可藉此檢視結構描述。(REQ-363、REQ-270)

---

## ADBC

以 Arrow Flight 為後盾的 Arrow Database Connectivity。(REQ-271) 直接回傳 `pyarrow.Table`——無需 JSON 反序列化。(REQ-271)

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
    port=8815,        # Arrow Flight port (REQ-711)
)
```

### 擷取為 Arrow Table

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### 擷取為 tuple

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### 欄位中繼資料

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Context Manager

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC 預設連往連接埠 8815 上的 Flight 伺服器。(REQ-143) 傳入 `port=` 可連往綁定於非預設連接埠的 Flight 伺服器。(REQ-711)
