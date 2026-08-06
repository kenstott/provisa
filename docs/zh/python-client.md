# Python 客户端 (`provisa-client`)

Provisa 的 Python 客户端。提供四种接口：

| 接口 | 使用场景 |
| ----------- | ---------- |
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
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` 接受的是一份凭据，而不是用户名加密码：它自身没有登录步骤。当脚本需要无人值守运行时，个人访问令牌是首选凭据——它由用户自己的个人资料页签发，带有有效期，并且可以在不动账户的情况下吊销。（REQ-1263）提供程序颁发的 bearer 令牌用法完全相同。两者都放在 `token` 中，客户端会在 HTTP 和 Arrow Flight 两条路径上都出示它。

要用密码换取令牌，向 `/auth/login` 发送 POST 并读取 `access_token`：

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

DB-API 和 ADBC 入口会替您完成这一交换——见下文。

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
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Provisa 服务端基础 URL |
| `token` | `None` | Bearer 凭据——提供程序令牌或个人访问令牌；使用密码认证时省略（REQ-606、REQ-1263） |
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
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` 会把用户名和密码 POST 到 `/auth/login`，并保存返回的 `access_token`，因此该连接携带的是一份真实凭据，而不只是一个名字。`role` 是在*请求*某个角色，只有该身份确实被分配了该角色时服务器才会予以满足（REQ-273）；若省略，连接就以登录所解析出的角色运行。

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
| ----------- | ------------- | --------- |
| `role` | 要请求的角色；由服务端校验（REQ-273） | 登录所解析出的角色 |

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

`adbc_connect` 会先通过 HTTP 登录，并把得到的令牌放入每一张 Flight ticket，因此 Flight 服务器对连接的身份验证方式与 REST 接口完全一致。（REQ-1263）`role` 参数是一项请求，由服务器对照该身份的角色分配进行校验——它绝不会变成身份本身。（REQ-273）

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
