# Python Client (`provisa-client`)

Python client for Provisa. Provides four interfaces:

| Interface | Use case |
| ----------- | ---------- |
| `ProvisaClient` | GraphQL queries, Arrow Flight, DataFrame output |
| DB-API 2.0 (`connect`) | Standard Python database interface (PEP 249) |
| SQLAlchemy dialect | BI tools, ORM, Pandas `read_sql` |
| ADBC | Arrow-native columnar streaming via Flight |

## Install

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Quick Start

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` takes a credential. It has no login step of its own, so it never asks for a username and password. When a script needs to run unattended, use a personal access token: you issue it from your own profile, it carries an expiry, and you can revoke it without touching the account. A provider bearer token works the same way. Either one goes in `token`, and the client presents it on both the HTTP and the Arrow Flight path.

To exchange a password for a token, POST to `/auth/login` and read `access_token`:

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

The DB-API and ADBC entry points do this exchange for you, as described below.

### GraphQL Queries

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

### Async

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (high-throughput columnar)

Use Flight for large result sets. Data streams as Arrow record batches and is never materialized on the server.

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight connects to port 8815 by default. Override with `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Catalog Exploration

```python
tables_df = client.list_tables()
```

### Connection Reference

| Parameter | Default | Description |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Provisa server base URL |
| `token` | `None` | Bearer credential — a provider token or a personal access token; omit for password auth |
| `role` | `None` | Role to act as, sent as `X-Provisa-Role` and honored only when assigned to the user; omit to run as the identity's own role |
| `flight_port` | `8815` | Arrow Flight gRPC port |

### Error Handling

`query()` raises `httpx.HTTPStatusError` on HTTP errors.
`query_df()` raises `RuntimeError` if the response contains GraphQL errors.

---

## Using Provisa in Jupyter

Install into the running kernel before importing. The `pandas` extra covers DataFrame output; add `sqlalchemy` if you also need `pandas.read_sql` through the dialect.

```python
%pip install "provisa-client[pandas]"
```

### Credentials

Notebooks persist cell output to the `.ipynb` file. Type a token into a cell and it will be saved to disk alongside your code. Don't do that.

`ProvisaClient` reads no environment: pass its credentials in explicitly, from environment variables. (`provisa_client.query()`, below, reads them for you.)

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

Set those variables in your shell before starting Jupyter, or load a `.env` file with `python-dotenv`. Either way, keep them out of the notebook.

### One call: `query()`

`provisa_client.query()` takes a SQL, GraphQL or Cypher statement and returns a pandas DataFrame. The server works out which language it is. Nothing is constructed first: the connection comes from the environment or a profile file.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` acts as that role for one read, sent as `X-Provisa-Role`. The server honours it only for a role you hold. The read goes over Arrow Flight; set `transport` to `http` to send it to `/data/query` instead. `await provisa_client.aquery(...)` is the awaitable form.

The connection is read from the first place that names each setting:

1. keywords to `provisa_client.sign_in(...)`, which resolves the connection once for every later read;
2. environment variables: `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`, `PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT`;
3. a section of `~/.provisa/credentials`: `[default]`, or the one `PROVISA_PROFILE` (or `sign_in(profile=...)`) names.

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

A `token` is a personal access token or a session token. Give a `username` and `password` instead, and the client exchanges them for a session token at `POST /auth/login`. That works on a deployment whose sign-in provider takes a password (basic, simple, LDAP). Keep the password in `PROVISA_PASSWORD`, out of the file. A refused sign-in, a missing URL or a named profile that isn't in the file raises an error with the reason. Provisa has no sign-in flow a notebook can drive through a browser, so for single sign-on deployments use a personal access token.

### Cell magics

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`, `%%graphql` and `%%cypher` answer the cell with `query()`. The DataFrame is the cell's result. A name after the magic also binds it in the notebook (`eu` above); `--role` acts as that role for the cell.

JupySQL (`%load_ext sql`) defines a `%%sql` as well. Load both and `%%sql` belongs to whichever extension loaded last. Provisa's takes `--role` and reads over Arrow Flight. JupySQL works too, through the SQLAlchemy dialect: `%sql provisa+http://user:password@host:port` connects, `%sql`/`%%sql` run governed statements, and `%sqlcmd tables -s <domain>` and `%sqlcmd columns -t <table> -s <domain>` list the catalog.

### In the browser (Pyodide)

Under Pyodide, `query()` reads from the server that served the page, as the page's signed-in user. The host page keeps that user on the kernel's global scope:

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

`baseUrl` is empty for the page's own origin. The client reads `provisaAuth` on every request, so a rotated token takes effect without a restart. Results arrive as Arrow when `pyarrow` is loaded; without it they come as JSON. A Cypher statement always comes back as JSON. `query()` blocks, which a Web Worker (where a notebook kernel runs) can do; on the page's main thread it raises and `await provisa_client.aquery(...)` is the call to use.

### DataFrames from GraphQL, SQL, and Cypher

`query_df()` accepts all three query languages and returns a pandas DataFrame.

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

GraphQL responses carry the data under a root field name; `query_df()` extracts it. SQL and Cypher responses carry explicit column names from the server and are mapped directly.

### Arrow Flight for large pulls

`flight_df()` streams data as Arrow record batches instead of deserializing JSON. For wide tables or result sets over a few hundred thousand rows, it is faster and uses less memory.

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow preserves column types. Integers stay integers and timestamps stay timestamps, which JSON can't promise. When the schema matters, that alone is reason to prefer Flight.

Flight connects on port 8815 by default. Pass `flight_port=` to the `ProvisaClient` constructor if your server uses a different port.

### `pandas.read_sql` through the SQLAlchemy dialect

Install the `sqlalchemy` extra into the kernel:

```python
%pip install "provisa-client[sqlalchemy]"
```

Then build an engine with a `provisa+http://` URL. Read the credentials from environment variables so they never sit in your notebook.

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

The dialect posts the username and password to `/auth/login` once and holds the resulting token for the life of the engine. The credentials aren't sent again with each query.

---

## DB-API 2.0

Standard [PEP 249](https://peps.python.org/pep-0249/) interface. Works with any tool that accepts a DB-API connection.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` posts the username and password to `/auth/login` and holds the `access_token` it gets back, so the connection carries a real credential. `role` *requests* a role, and the server honours it only if the identity is assigned that role. When you omit it, the connection runs as the role the login resolved.

### Executing queries

The cursor accepts GraphQL or SQL and detects which one it got.

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

### Column metadata

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Named parameters

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Context managers

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## SQLAlchemy Dialect

```bash
pip install "provisa-client[sqlalchemy]"
```

URL scheme: `provisa+http://` or `provisa+https://`

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### With pandas

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### URL parameters

| Parameter | Description | Default |
| ----------- | ------------- | --------- |
| `role` | Role to request; validated server-side | the role the login resolves |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Schema introspection

The dialect reflects the catalog your role is served, read from `GET /data/catalog` as the connection's user, role and org. Each domain is a schema, and every table lives in one: `get_schema_names()` lists the domains, `get_table_names(schema="shop")` lists that domain's tables by their SQL names, and `get_columns("orders", schema="shop")` gives each column's type, nullability and description. With no schema named, `get_table_names()` lists none. An unknown table raises `NoSuchTableError`; a catalog the server refuses raises.

---

## ADBC

Arrow Database Connectivity, backed by Arrow Flight. It returns a `pyarrow.Table` directly, with no JSON deserialization.

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

`adbc_connect` logs in over HTTP first and puts the resulting token in every Flight ticket, so the Flight server authenticates the connection the same way the REST surface does. The `role` argument is a request that the server validates against the identity's assignments. It never becomes the identity.

### Fetch as Arrow Table

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Fetch as tuples

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Column metadata

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Context manager

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC connects to the Flight server on port 8815 by default. Pass `port=` to reach a Flight server bound to a non-default port.
