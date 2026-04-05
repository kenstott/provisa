# Integrations

## JDBC Driver

The Provisa JDBC driver allows BI tools to connect directly without writing GraphQL. It uses Arrow Flight as the underlying transport.

### Connection

Download `provisa-jdbc-<version>.jar` from the [releases page](https://github.com/kenstott/provisa/releases/latest) and add it to your tool's driver path.

JDBC URL:
```
jdbc:provisa://<host>:8815?mode=approved
```

| Parameter | Values | Description |
|-----------|--------|-------------|
| `mode` | `approved` \| `catalog` | `approved`: only governed queries appear as virtual tables; `catalog`: full schema discovery for Collibra and similar tools |

Authentication uses standard JDBC `user` / `password` properties. Provisa authenticates the credentials against the configured auth provider and assigns the role — the client does not choose its own role.

### BI Tool Setup

**Tableau**
1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815?mode=approved`
4. Enter your username and password when prompted

**DBeaver**
1. Database → New Connection → JDBC
2. Driver: add `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815?mode=catalog`
4. Enter your username and password in the Authentication tab

**Power BI** — use the ODBC gateway with the Provisa JDBC-ODBC bridge (included in the installer).

### Driver Modes

**`approved` mode**: Each approved persisted query appears as a virtual table. Column names, types, and row counts reflect the governed query output. RLS is enforced per the authenticated role.

**`catalog` mode**: All published tables appear for schema discovery. Used by data catalog tools (Collibra, Atlan, Alation) to scan and classify Provisa's logical data model.

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

client = ProvisaClient("http://localhost:8001", role="analyst")

# Arrow Flight → pyarrow Table (high-throughput, streaming)
table = client.flight("{ orders { id amount } }")

# Arrow Flight → pandas DataFrame
df = client.flight_df("{ orders { id amount } }")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount } }")
```

See [docs/python-client.md](python-client.md) for the full reference.

### Python (raw PyArrow)

For tools that manage Flight connections directly:

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "analyst"}')
df = client.do_get(ticket).read_all().to_pandas()
```

### DuckDB

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL arrow; LOAD arrow;")
# Use the PyArrow reader above and pass the table to DuckDB
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
