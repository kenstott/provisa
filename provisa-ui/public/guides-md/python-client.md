# Python Client (`provisa-client`)

Python client for Provisa. Provides four interfaces:

| Interface | Use case |
|-----------|----------|
| `ProvisaClient` | GraphQL queries, Arrow Flight, DataFrame output |
| DB-API 2.0 (`connect`) | Standard Python database interface (PEP 249) (REQ-268) |
| SQLAlchemy dialect | BI tools, ORM, Pandas `read_sql` (REQ-270) |
| ADBC | Arrow-native columnar streaming via Flight (REQ-271) |

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
    username="alice",
    password="secret",
)
```

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

Use Flight for large result sets — data streams as Arrow record batches without materializing on the server. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight connects to port 8815 by default. (REQ-143) Override with `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Catalog Exploration

```python
tables_df = client.list_tables()
```

### Connection Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `url` | `http://localhost:8001` | Provisa server base URL |
| `token` | `None` | Bearer token; omit for password auth (REQ-606) |
| `role` | `"admin"` | Role sent with every request (REQ-273) |
| `flight_port` | `8815` | Arrow Flight gRPC port (REQ-143) |

### Error Handling

`query()` raises `httpx.HTTPStatusError` on HTTP errors. (REQ-607)  
`query_df()` raises `RuntimeError` if the response contains GraphQL errors. (REQ-607)

---

## DB-API 2.0

Standard [PEP 249](https://peps.python.org/pep-0249/) interface. (REQ-268) Works with any tool that accepts a DB-API connection.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="admin",       # optional, default "admin"
)
```

### Executing queries

The cursor accepts either GraphQL or SQL — detected automatically. (REQ-268, REQ-274)

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

URL scheme: `provisa+http://` or `provisa+https://` (REQ-270)

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
|-----------|-------------|---------|
| `role` | Provisa role | `admin` |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Schema introspection

The dialect implements `get_table_names()`, `get_columns()`, and `has_table()` — catalog tools (DBeaver, SQLAlchemy automap) can inspect the schema. (REQ-363, REQ-270)

---

## ADBC

Arrow Database Connectivity backed by Arrow Flight. (REQ-271) Returns `pyarrow.Table` directly — no JSON deserialization. (REQ-271)

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

ADBC connects to the Flight server on port 8815 by default. (REQ-143) Pass `port=` to reach a Flight server bound to a non-default port. (REQ-711)
