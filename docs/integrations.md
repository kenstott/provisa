# Integrations

## Choosing a Connection Path

| Client type | Recommended path | Why |
|-------------|-----------------|-----|
| BI tools (Tableau, Power BI, Looker) | JDBC | Arrow Flight columnar streaming over the wire; BI tools have a built-in JDBC wizard and benefit from high-throughput columnar delivery for large result sets |
| psql, DBeaver, any PG-compatible tool | pgwire (native PG driver) | Zero-friction default — no custom driver needed; use what you already have |
| Python data stack (pandas, pyarrow) | `provisa-client` or raw ADBC | Streaming Arrow batches; no row serialization overhead |
| Spark, DuckDB, high-throughput pipelines | Arrow Flight (ADBC) | Unbounded columnar streaming direct to Arrow memory |
| Service-to-service (typed contracts) | Protobuf gRPC | Per-role generated proto; streaming rows; type safety |
| Web apps, scripting | HTTP (`/data/graphql`, `/data/sql`) | No driver; standard HTTP; full query language choice |
| REST clients (JSON:API standard) | `GET /data/jsonapi/{table}` | JSON:API v1.0 envelope; sparse fieldsets, pagination, filtering via query params; no driver |

---

## pgwire — Native PostgreSQL Driver

Provisa implements the PostgreSQL wire protocol (protocol version 3.0). Any client that speaks PostgreSQL connects without a custom driver.

Enable by setting `PROVISA_PGWIRE_PORT` (e.g. `5433`) before starting Provisa. Disabled when unset or `0`.

### Why pgwire instead of JDBC?

The JDBC driver uses Arrow Flight as its transport and requires deploying the `provisa-jdbc.jar`. pgwire requires nothing — if you already have `psql`, DBeaver, SQLAlchemy, or a PG JDBC driver, you are done. It is the lower-friction path for SQL-only workloads.

JDBC is the right choice for BI tools that have a built-in JDBC connection wizard and benefit from Arrow Flight's columnar streaming for large result sets. pgwire accepts free SQL against the full published schema — the same queries, lower setup cost.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host: `localhost`, Port: `5433`
3. Username / password as configured in Provisa
4. No extra driver download required

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

Or with `asyncpg`:

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### Authentication

pgwire uses cleartext password auth bridged to Provisa's configured auth provider (`none` or `simple`). In trust mode (`none`), the username maps directly to a role — password is ignored. MD5 is not supported; enable TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) when running over an untrusted network.

### Limitations

- SQL only. GraphQL and Cypher are not accepted over pgwire.
- Not read-only. `COPY ... FROM STDIN` inserts rows into `postgresql`, `mysql`, `sqlite`, and `mariadb` sources, and DDL is supported (see below).
- DDL (`CREATE`, `ALTER`, `DROP`) is supported and dispatched to the Trino or direct path; the new table is registered into the compilation context and is immediately queryable. `COPY ... TO STDOUT` (export) and `COPY ... FROM STDIN` (import) are supported in `text` and `csv` formats.
- `information_schema` and `pg_catalog` queries are intercepted and answered from a DuckDB catalog shim — schema discovery tools work correctly.

---

## JDBC Driver

The Provisa JDBC driver uses Arrow Flight as its underlying transport. It is the recommended path for BI tools with a JDBC connection wizard.

### Connection

Download `provisa-jdbc-<version>.jar` from the [releases page](https://github.com/kenstott/provisa/releases/latest) and add it to your tool's driver path.

JDBC URL:
```
jdbc:provisa://<host>:8815
```

Authentication uses standard JDBC `user` / `password` properties. Provisa authenticates the credentials against the configured auth provider and assigns the role — the client does not choose its own role.

### BI Tool Setup

**Tableau**
1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Enter your username and password when prompted

**DBeaver** (JDBC path — for pgwire path see above)
1. Database → New Connection → JDBC
2. Driver: add `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. Enter your username and password in the Authentication tab

**Power BI** — use the ODBC gateway with the Provisa JDBC-ODBC bridge (included in the installer).

---

## Arrow Flight Clients

Arrow Flight (port 8815) is the recommended path for data tools that support it. Results stream as Arrow RecordBatches without materializing in Provisa memory.

### Python (`provisa-client`)

The recommended Python path — wraps both GraphQL and Arrow Flight:

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

See [docs/python-client.md](python-client.md) for the full reference including DB-API 2.0, SQLAlchemy dialect, and ADBC.

### Python (raw PyArrow)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

The ticket carries no role. The server assigns the role from the configured auth provider. Where role selection is allowed, pass it in the gRPC call metadata under the `x-provisa-role` key (for example `flight.FlightCallOptions(headers=[(b"x-provisa-role", b"analyst")])`), not in the ticket JSON.

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

### Spark (PySpark)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC (port 50051)

Service-to-service path. Provisa generates a `.proto` per role at startup — each role sees only the tables and columns it has access to.

Download the proto for your role:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Use `grpc_server_reflection` to discover the schema programmatically.

Role is passed via the `x-provisa-role` metadata key on every RPC. Streaming queries emit one message per row; mutations are unary.

---

## Apollo Federation

Provisa can act as a Federation v2 subgraph, exposing its published schema to an Apollo Router or Apollo Gateway.

### Setup

Enable federation in `config.yaml`:
```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa generates `@key` directives on primary-key columns and `@external`/`@provides` on cross-subgraph relationships automatically.

### Register with Apollo Router

In your `supergraph.yaml`:
```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Run `rover supergraph compose --config supergraph.yaml` to generate the supergraph schema.

### Entities

Provisa responds to `_entities` queries for cross-subgraph joins. Any table with a primary key is automatically resolvable as a Federation entity.

---

## Hasura v2 / DDN Import

See [docs/import.md](import.md) for migrating from Hasura to Provisa.

---

## Kafka

See [docs/sources.md](sources.md#kafka) for Kafka topic configuration as read-only tables and query result sinks.
