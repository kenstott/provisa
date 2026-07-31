# Python 客户端 (`provisa-client`)

Provisa 的 Python 客户端。提供四种接口：

| 接口 | 使用场景 |
|-----------|----------|
| `ProvisaClient` | GraphQL 查询、Arrow Flight、DataFrame 输出 |
| DB-API 2.0（`connect`） | 标准 Python 数据库接口（PEP 249）（REQ-268） |
| SQLAlchemy 方言 | BI 工具、ORM、Pandas `read_sql`（REQ-270） |
| ADBC | 通过 Flight 实现 Arrow 原生列式流式传输（REQ-271） |

## 安装

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### 快速开始

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    username="alice",
    password="secret",
)
```

### GraphQL 查询

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

### 异步

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight（高吞吐列式传输）

大结果集请使用 Flight——数据以 Arrow record batch 的形式流式传输，无需在服务端物化。（REQ-143, REQ-145）

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight 默认连接到 8815 端口。（REQ-143）可通过 `flight_port=` 覆盖：

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### 目录探索

```python
tables_df = client.list_tables()
```

### 连接参考

| 参数 | 默认值 | 说明 |
|-----------|---------|-------------|
| `url` | `http://localhost:8001` | Provisa 服务端基础 URL |
| `token` | `None` | Bearer 令牌；使用密码认证时省略（REQ-606） |
| `role` | `"admin"` | 随每个请求发送的角色（REQ-273） |
| `flight_port` | `8815` | Arrow Flight gRPC 端口（REQ-143） |

### 错误处理

`query()` 在发生 HTTP 错误时抛出 `httpx.HTTPStatusError`。（REQ-607）
`query_df()` 在响应中包含 GraphQL 错误时抛出 `RuntimeError`。（REQ-607）

---

## DB-API 2.0

标准 [PEP 249](https://peps.python.org/pep-0249/) 接口。（REQ-268）可与任何接受 DB-API 连接的工具配合使用。

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="admin",       # optional, default "admin"
)
```

### 执行查询

游标可接受 GraphQL 或 SQL——自动检测。（REQ-268, REQ-274）

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

### 列元数据

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### 命名参数

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### 上下文管理器

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

URL 方案：`provisa+http://` 或 `provisa+https://`（REQ-270）

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### 配合 pandas 使用

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### URL 参数

| 参数 | 说明 | 默认值 |
|-----------|-------------|---------|
| `role` | Provisa 角色 | `admin` |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### 架构自省

该方言实现了 `get_table_names()`、`get_columns()` 和 `has_table()`——目录工具（DBeaver、SQLAlchemy automap）可借此检视架构。（REQ-363, REQ-270）

---

## ADBC

基于 Arrow Flight 的 Arrow Database Connectivity。（REQ-271）直接返回 `pyarrow.Table`——无需 JSON 反序列化。（REQ-271）

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

### 以 Arrow Table 形式获取

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### 以元组形式获取

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### 列元数据

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### 上下文管理器

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC 默认连接到 8815 端口的 Flight 服务器。（REQ-143）传入 `port=` 可连接到绑定在非默认端口的 Flight 服务器。（REQ-711）
