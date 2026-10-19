# Python 客户端 (`provisa-client`) {: #python-client-provisa-client }

Provisa 的 Python 客户端。提供四种接口：

| 接口 | 使用场景 |
| ----------- | ---------- |
| `ProvisaClient` | GraphQL 查询、Arrow Flight、DataFrame 输出 |
| DB-API 2.0（`connect`） | 标准 Python 数据库接口（PEP 249） |
| SQLAlchemy 方言 | BI 工具、ORM、Pandas `read_sql` |
| ADBC | 通过 Flight 实现 Arrow 原生列式流式传输 |

## 安装 {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### 快速开始 {: #quick-start }

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` 接受的是一份凭据，而不是用户名加密码：它自身没有登录步骤。当脚本需要无人值守运行时，个人访问令牌是首选凭据——它由用户自己的个人资料页签发，带有有效期，并且可以在不动账户的情况下吊销。提供程序颁发的 bearer 令牌用法完全相同。两者都放在 `token` 中，客户端会在 HTTP 和 Arrow Flight 两条路径上都出示它。

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

### GraphQL 查询 {: #graphql-queries }

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

### 异步 {: #async }

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight（高吞吐列式传输） {: #arrow-flight-high-throughput-columnar }

大结果集请使用 Flight——数据以 Arrow record batch 的形式流式传输，无需在服务端物化。

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight 默认连接到 8815 端口。可通过 `flight_port=` 覆盖：

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### 目录探索 {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### 连接参考 {: #connection-reference }

| 参数 | 默认值 | 说明 |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Provisa 服务端基础 URL |
| `token` | `None` | Bearer 凭据——提供程序令牌或个人访问令牌；使用密码认证时省略 |
| `role` | `None` | 要代为执行的角色，作为 `X-Provisa-Role` 发送，仅当该角色已分配给用户时才生效；省略时，请求以身份自身的角色运行 |
| `flight_port` | `8815` | Arrow Flight gRPC 端口 |

### 错误处理 {: #error-handling }

`query()` 在发生 HTTP 错误时抛出 `httpx.HTTPStatusError`。
`query_df()` 在响应中包含 GraphQL 错误时抛出 `RuntimeError`。

---

## 在 Jupyter 中使用 Provisa {: #using-provisa-in-jupyter }

在导入之前，先安装到正在运行的内核中。`pandas` 扩展提供 DataFrame 输出；如果还需要通过方言使用 `pandas.read_sql`，请加上 `sqlalchemy`。

```python
%pip install "provisa-client[pandas]"
```

### 凭据 {: #credentials }

笔记本会把单元格输出保存到 `.ipynb` 文件中。在单元格里输入的令牌会和你的代码一起写入磁盘。不要这样做。

`ProvisaClient` 不读取环境：请显式传入凭据，取自环境变量。（下文的 `provisa_client.query()` 会替你读取。）

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

在启动 Jupyter 之前在 shell 中设置这些变量，或用 `python-dotenv` 加载 `.env` 文件。无论哪种方式，都不要让它们出现在笔记本中。

### 一次调用：`query()` {: #one-call-query }

`provisa_client.query()` 接受一条 SQL、GraphQL 或 Cypher 语句，返回一个 pandas DataFrame。由服务器判断是哪种语言。无需事先构造任何对象：连接来自环境或配置文件。

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` 以该角色执行一次读取，作为 `X-Provisa-Role` 发送。服务器只对你持有的角色生效。读取经由 Arrow Flight；将 `transport` 设为 `http` 可改为发送到 `/data/query`。`await provisa_client.aquery(...)` 是可等待的形式。

每项设置从第一个指定它的位置读取：

1. `provisa_client.sign_in(...)` 的关键字参数，它为之后的所有读取一次性解析连接；
2. 环境变量：`PROVISA_URL`、`PROVISA_TOKEN`、`PROVISA_USERNAME`、`PROVISA_PASSWORD`、`PROVISA_ROLE`、`PROVISA_ORG`、`PROVISA_FLIGHT_PORT`、`PROVISA_TRANSPORT`；
3. `~/.provisa/credentials` 中的一个节：`[default]`，或 `PROVISA_PROFILE`（或 `sign_in(profile=...)`）指定的那一节。

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

`token` 是个人访问令牌或会话令牌。改为提供 `username` 和 `password` 时，客户端会在 `POST /auth/login` 将其换成会话令牌。这适用于登录提供方接受密码的部署（basic、simple、LDAP）。请把密码放在 `PROVISA_PASSWORD` 中，而不是文件里。登录被拒绝、缺少 URL，或指定的配置节不在文件中时，会抛出带原因的错误。Provisa 没有笔记本可以通过浏览器驱动的登录流程，因此在单点登录部署中请使用个人访问令牌。

### 单元格魔法命令 {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`、`%%graphql` 和 `%%cypher` 用 `query()` 回答单元格。DataFrame 就是单元格的结果。魔法命令后的名称还会在笔记本中绑定该结果（上例中的 `eu`）；`--role` 以该角色运行这个单元格。

JupySQL（`%load_ext sql`）也定义了 `%%sql`。两者都加载时，`%%sql` 归最后加载的扩展所有。Provisa 的版本接受 `--role`，并通过 Arrow Flight 读取。JupySQL 也可以使用，通过 SQLAlchemy 方言：`%sql provisa+http://user:password@host:port` 建立连接，`%sql`/`%%sql` 运行受治理的语句，`%sqlcmd tables -s <domain>` 和 `%sqlcmd columns -t <table> -s <domain>` 列出目录。

### 在浏览器中（Pyodide） {: #in-the-browser-pyodide }

在 Pyodide 下，`query()` 以页面已登录用户的身份，从提供该页面的服务器读取。宿主页面把该用户保存在内核的全局作用域中：

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

对于页面自身的源，`baseUrl` 为空。客户端每次请求都会读取 `provisaAuth`，因此轮换后的令牌无需重启即可生效。加载了 `pyarrow` 时结果以 Arrow 返回；否则以 JSON 返回。Cypher 语句总是以 JSON 返回。`query()` 是阻塞调用，Web Worker（笔记本内核运行的地方）可以这样做；在页面主线程上它会抛出错误，应改用 `await provisa_client.aquery(...)`。

### 从 GraphQL、SQL 和 Cypher 得到 DataFrame {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` 接受这三种查询语言，并返回 pandas DataFrame。

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

GraphQL 响应把数据放在一个根字段名下；`query_df()` 会将其提取出来。SQL 和 Cypher 响应带有服务器给出的明确列名，直接映射。

### 大批量读取用 Arrow Flight {: #arrow-flight-for-large-pulls }

`flight_df()` 以 Arrow 记录批次流式传输数据，而不是反序列化 JSON。对于宽表或超过几十万行的结果集，它更快，占用的内存更少。

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow 保留列类型——整数仍是整数，时间戳仍是时间戳。JSON 做不到。仅凭这一点，在模式重要时就应优先使用 Flight。

Flight 默认连接 8815 端口。如果服务器使用其他端口，请向 `ProvisaClient` 构造函数传入 `flight_port=`。

### 通过 SQLAlchemy 方言使用 `pandas.read_sql` {: #pandasread_sql-through-the-sqlalchemy-dialect }

把 `sqlalchemy` 扩展安装到内核中：

```python
%pip install "provisa-client[sqlalchemy]"
```

然后用 `provisa+http://` URL 创建 engine。把凭据放在环境变量中，而不是 URL 字符串里。

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

方言把用户名和密码发送到 `/auth/login`，并在 engine 的整个生命周期内持有得到的令牌——不会在每次查询时重新传输凭据。

---

## DB-API 2.0

标准 [PEP 249](https://peps.python.org/pep-0249/) 接口。可与任何接受 DB-API 连接的工具配合使用。

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` 会把用户名和密码 POST 到 `/auth/login`，并保存返回的 `access_token`，因此该连接携带的是一份真实凭据，而不只是一个名字。`role` 是在*请求*某个角色，只有该身份确实被分配了该角色时服务器才会予以满足；若省略，连接就以登录所解析出的角色运行。

### 执行查询 {: #executing-queries }

游标可接受 GraphQL 或 SQL——自动检测。

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

### 列元数据 {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### 命名参数 {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### 上下文管理器 {: #context-managers }

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

URL 方案：`provisa+http://` 或 `provisa+https://`

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### 配合 pandas 使用 {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### URL 参数 {: #url-parameters }

| 参数 | 说明 | 默认值 |
| ----------- | ------------- | --------- |
| `role` | 要请求的角色；由服务端校验 | 登录所解析出的角色 |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### 架构自省 {: #schema-introspection }

方言反映提供给你的角色的目录，以连接的用户、角色和组织身份从 `GET /data/catalog` 读取。每个域是一个模式，每张表都位于其中之一：`get_schema_names()` 列出各个域，`get_table_names(schema="shop")` 按 SQL 名称列出该域的表，`get_columns("orders", schema="shop")` 给出每列的类型、可空性和描述。未指定模式时，`get_table_names()` 不列出任何表。未知的表会抛出 `NoSuchTableError`；服务器拒绝的目录会抛出错误。

---

## ADBC

基于 Arrow Flight 的 Arrow Database Connectivity。直接返回 `pyarrow.Table`——无需 JSON 反序列化。

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

`adbc_connect` 会先通过 HTTP 登录，并把得到的令牌放入每一张 Flight ticket，因此 Flight 服务器对连接的身份验证方式与 REST 接口完全一致。`role` 参数是一项请求，由服务器对照该身份的角色分配进行校验——它绝不会变成身份本身。

### 以 Arrow Table 形式获取 {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### 以元组形式获取 {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### 列元数据 {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### 上下文管理器 {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC 默认连接到 8815 端口的 Flight 服务器。传入 `port=` 可连接到绑定在非默认端口的 Flight 服务器。
