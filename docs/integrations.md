# Integrations

## Choosing a Connection Path

| Client type | Recommended path | Why |
| ------------- | ----------------- | ----- |
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

The `password` field of the startup packet carries the credential, and what the credential *is* picks the method: a personal access token, an OIDC bearer token, or a password against the configured provider. Under the `basic` provider with `auth.scram: true` the password is proved through SCRAM-SHA-256 instead of sent. Client certificates are supported. In trust mode (`none`) the username maps directly to a role and the password is ignored.

The full surface × method table is in [Security Model](security.md#surfaces-and-credentials). MD5 is not supported; enable TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) when running over an untrusted network.

### Limitations

- SQL only. GraphQL and Cypher are not accepted over pgwire.
- Not read-only. `COPY ... FROM STDIN` inserts rows into `postgresql`, `mysql`, `sqlite`, and `mariadb` sources, and DDL is supported (see below).
- DDL (`CREATE`, `ALTER`, `DROP`) is supported and dispatched to the Trino or direct path; the new table is registered into the compilation context and is immediately queryable. `COPY ... TO STDOUT` (export) and `COPY ... FROM STDIN` (import) are supported in `text` and `csv` formats.
- `information_schema` and `pg_catalog` queries are intercepted and answered from a DuckDB catalog shim — schema discovery tools work correctly.

---

## JDBC Driver

The Provisa JDBC driver uses Arrow Flight as its underlying transport. It is the recommended path for BI tools with a JDBC connection wizard.

Queries run over Arrow Flight whenever the Flight port answers when the connection is opened, and results stream a batch at a time with their column types. The Flight port is taken to be the one beside the HTTP port (8815 beside 8001); set the `flight_port` property when it is not. If the Flight port is unreachable at connect, the connection uses the HTTP API for its queries and logs that at INFO. Once a connection is on Flight, an error from the server — a refused credential, a role the user does not hold, a failed statement — is raised to the caller with the server's message; it is not retried over HTTP. (REQ-293)

Schema browsing (`getTables`, `getColumns`, primary and foreign keys) is read from the same Flight port, with the signed-in credential: the driver lists the catalog of the role it acts as, so a table or column that role is not granted does not appear. A key that refers to a table or column the role is not granted is not listed either. On a connection whose Flight port was unreachable at connect, the driver reads the same catalog over HTTP (`GET /data/catalog`): the server builds both listings with one function, so the tables, column names, types and keys do not depend on which port answered. (REQ-128)

### Connection

Download [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (always the latest release) and add it to your tool's driver path.

JDBC URL:

```yaml
jdbc:provisa://<host>:8815
```

Authentication uses standard JDBC `user` / `password` properties. The driver exchanges them for a session token and sends it on every request; a refused sign-in fails the connection with the server's status and reason. Provisa derives the role from the signed-in identity. The optional `role` property (or `?role=` in the URL) asks to act as one role the user holds, or a comma-separated set of them; a role the user does not hold is refused. On a deployment with no auth provider there is no sign-in, and the user name is taken as the requested role. (REQ-131, REQ-273)

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

Flight carries its credential in the JSON payload, as a `token` field — a provider bearer token or a personal access token. Both the handshake and every ticket accept it, and both validate it the same way, so a client that authenticated at handshake still presents the token on each `do_get`. A `role` field alongside it *requests* a role; the server derives the identity's permitted roles and substitutes the authorized value, so a role string in a ticket is never the identity. (REQ-1263) See [Security Model](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

Browsing needs the credential too. `list_flights`, `get_flight_info` and `get_schema` carry no ticket, so they read the credential from the call's `authorization: Bearer <token>` header and the requested role from `x-provisa-role` (one role, or a comma-separated set of held roles). With authentication on, a browsing call without a credential is refused, and the catalog it returns is the authorized role's: the tables, columns and commands that role is granted, and nothing else. A catalog ticket (a ticket with no `query`) is answered the same way from its own `token` and `role`. A client must therefore send its credential before it browses; `provisa-client` does this in `list_tables()`. On a deployment with no auth provider these calls list the whole catalog and take no credential. (REQ-1263, REQ-127)

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

Every RPC must carry a credential in the `authorization` metadata key — a provider token or a personal access token. `x-provisa-role` requests a role from the identity's permitted set; it is not a credential and never was. Client certificates are supported. See [Security Model](security.md#surfaces-and-credentials).

Streaming queries emit one message per row; mutations are unary.

---

## Invoking Commands Across Protocols

A **command** is a registered tracked function or webhook — a callable registered in the Provisa model with a `kind` (`query` or `mutation`) and an `impl_kind` that describes how it runs. Every surface routes invocations through a single governed executor (`invoke_tracked_function`) that enforces `writable_by` and governance uniformly (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | What runs | Binding fields |
| ------------ | ----------- | --------------- |
| `source_procedure` | Stored procedure on a registered source (default) | `sourceId`, `schemaName`, `functionName` |
| `script` | Server-side script | `script` |
| `http` | Outbound HTTP call | `url`, `method` |
| `grpc` | Outbound gRPC call to an external server | `target`, `method` |
| `python` | Python callable hosted by Provisa (REQ-885) | `callable` (e.g. `demo.py_functions:random_dataset`) |

When a command declares a `return_schema` (JSON Schema with `type: array, items: object`), it is set-returning — every surface projects it as a typed row set. The demo commands `random_python_set` (impl_kind `python`) and `random_grpc_set` (impl_kind `grpc`) illustrate both a hosted callable and an external gRPC bridge returning random-valued rows; both are registered in `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### Protocol matrix

| Surface | Syntax | Example |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → Query field; `kind=mutation` → Mutation field; domain-prefixed when `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` or `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — positional args map to declared argument names | `CALL random_python_set(3, 7)` |
| Provisa gRPC (port 50051) | Unary `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

The `kind` field controls GraphQL placement only — SQL, Cypher, Bolt, and gRPC surfaces accept both `query` and `mutation` commands identically.

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

See [docs/sources.md](sources.md#kafka-sources) for Kafka topic configuration as read-only tables and query result sinks.

---

## Data Quality Checkers (REQ-1443)

Soda Core and Great Expectations connect to Provisa the same way any other postgres client does — through pgwire. That is the whole integration: the checker holds one postgres driver and scans the federated view, so a Snowflake table, an Iceberg table and a Mongo collection are all checked by the same contract dialect with no per-system checker. [tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

The scan runs in a child interpreter — `python -m provisa.dq.worker` — which is the only place `soda_core` or `great_expectations` is imported. Nothing is linked into the server process, and a checker crash takes down a subprocess rather than the event loop. [tool-verified: `provisa/dq/runner.py` `build_command`]

Scan results land as ordinary source rows, so cadence, freshness, events, lineage, governance, RLS, the grid and export all apply without a second mechanism. Contract authoring, the result envelope and the derived registration are covered in [docs/sources.md](sources.md#data-quality-checkers-req-1443).

### Installing a checker

Neither library ships by default. The installer asks which one you want, and the answer becomes `dq_checker: none|soda|gx` in `~/.provisa/config.yaml`. On the Docker tier `scripts/provisa` turns that into the `PROVISA_EXTRAS` build arg; on the native tier `first-launch.sh` installs the matching pyproject extra into the venv. [tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh` `_native_extras`]

| `dq_checker` | Library | Licence | Hosted cloud plane |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Refused (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Allowed |

Elastic License 2.0 bars providing the software to third parties as a hosted service, which is what running Soda inside the SaaS plane on a tenant's behalf would be. A hosted deployment that wants Soda points at a Soda endpoint the operator runs themselves. See [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations) for the connection keys.

---

## Apache Ossie Semantic Interchange (REQ-1316)

Provisa exchanges semantic models with Apache Ossie (spec 0.2.0.dev0, incubating; formerly Open
Semantic Interchange) through a boundary adapter. Provisa's internal vocabulary is never renamed
to Ossie's — the spec declares breaking changes as likely, so coupling is confined to the adapter.
[tool-verified: `provisa/ossie/convert.py` module docstring; `OSSIE_VERSION = "0.2.0.dev0"`]

The draft spec has changed shape without changing its version string. The adapter follows the
spec as it stands at `apache/ossie` commit `642cb894` (2026-10-10): one model per document,
with `version`, `name`, `datasets`, `relationships`, and `metrics` at the document's root, and
an enumerated `datatype`. The export is checked in the unit tests against that commit's
`core-spec/ossie-schema.json`.
[tool-verified: `OSSIE_SPEC_COMMIT`, `provisa/ossie/convert.py`;
`test_the_export_is_valid_against_ossies_own_schema`, `tests/unit/test_ossie_convert.py`]

### Export

The canonical export surface is a live HTTP endpoint. It derives the Ossie document from live state
on every request — no caching, no generation step.

```http
GET /admin/ossie
```

The response is a YAML document with `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

The Metrics page also offers a **Download** button and a copyable endpoint URL in the Ossie
Interchange panel, both pointing to the same endpoint.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### What is exported

The adapter maps Provisa objects to Ossie objects as follows:

| Provisa object | Ossie object | Notes |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; primary/unique keys from column config and `UniqueConstraint` |
| `Column` | `field` | `expression` = column reference (ANSI_SQL dialect); time columns gain `dimension.is_time: true` |
| `Relationship` | `relationship` | Alias used as name when set; computed (function-target) relationships are skipped |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype` (mapped as a column's is), `description`, `ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Round-trip only; other tools may ignore |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py`: "Computed
(function-target) relationships have no dataset target — not representable in Ossie; skipping
is the defined export boundary"]

A relationship over several columns lists each column in `from_columns` and `to_columns`. A
model with no table is refused with a `422`: an Ossie document has at least one dataset.
[tool-verified: `test_a_relationship_over_several_columns_lists_each_column`,
`test_a_model_with_no_table_is_refused_by_name`, `tests/unit/test_ossie_convert.py`]

Governance, RLS, lineage, and graph semantics are not exported. They may travel in the optional
`provisa` custom_extensions slot for round-trip fidelity, but interchange never depends on other
tools reading it. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

A column type outside Ossie's vocabulary is written as `Opaque`, with the type's own name in the
field's `provisa` extension, which is what the spec directs for a known type it has no member
for. A column with no type recorded states no `datatype`. The adapter never maps to a type the
column does not have. [tool-verified: `_map_datatype`, `_typed`, `provisa/ossie/convert.py`]

#### Type mapping

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py`]

| Provisa / source type | Ossie `datatype` |
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
| anything else | `Opaque`, with the type's name in the `provisa` extension |

Ossie's `Decimal` carries no precision or scale, so `numeric(10,2)` is exported as `Decimal`
and comes back from an import as `decimal`.
[tool-verified: `test_parse_round_trips_export`, `tests/unit/test_ossie_convert.py`]

### Import

Import accepts an Ossie document (YAML or JSON) and returns registration proposals. Nothing is
registered automatically — imported definitions never bypass the review step.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

The server parses the document with `parse_ossie_model`, which validates structure and returns an
`OssieImport` dataclass containing proposed tables, relationships, and metrics as plain dicts.
Any structural problem is a `400` with a path-named error, e.g.
`ossie import: missing $.datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py`:
"Nothing is registered here — imported definitions never bypass registration review"]

The importer reads the flat document shape only. Three things are refused by name:

- A document that wraps its model in `semantic_model`, the shape the draft spec had before
  September 2026. The error says the document predates the flat shape and how to move it.
- A `version` other than `0.2.0.dev0`.
- A `datatype` that is not a member of Ossie's vocabulary; the error names the path and lists
  the members.

A proposed column or metric carries the type its Ossie datatype stands for (`Integer` proposes
`bigint`, `DateTime` proposes `timestamp`). `Opaque` proposes the type the `provisa` extension
names when the document came from Provisa, and no type otherwise. An `ai_context` given as an
object is read by its `instructions`.
[tool-verified: `parse_ossie_model`, `_proposed_type`, `_ai_context`,
`provisa/ossie/convert.py`; the `test_*refused*` cases in `tests/unit/test_ossie_convert.py`]

#### Coming from dbt, Cube, Snowflake, and other semantic layers

The Apache Ossie project maintains converters between Ossie and other tools' model formats. Run
the converter for your tool, then post its output to the import endpoint or pick the file in the
review screen below. Provisa reads the Ossie document; it does not read the other tool's files.

| Coming from | Converter command | What it reads |
|---|---|---|
| dbt (semantic layer) | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | The `semantic_manifest.json` that dbt writes for MetricFlow |
| Cube | `ossie-cube import -i model/ -o model.yaml` | A Cube model directory, or individual cube and view files |

[tool-verified: `converters/dbt/README.md` and `converters/cube/README.md` in the
`apache/ossie` repository, read 2026-10-10]

The same repository holds converters for Snowflake, Databricks, Microsoft, Salesforce,
ThoughtSpot, Sigma, Omni, Hex, GoodData, and others; each has its own README.
[tool-verified: directory listing of `converters/` in `apache/ossie`, 2026-10-10]

Two limits to know before converting:

- The dbt converter reads the semantic-layer manifest only. A dbt project without semantic
  models has nothing for it to convert. It drops conversion metrics, private metrics, and
  natural-key entities, and prints a warning for each.
  [tool-verified: `converters/dbt/README.md`, the "MSI → Ossie is lossy" table]
- A converter's output follows the spec as that converter's own commit has it. Provisa reads
  the flat shape of `apache/ossie` commit `642cb894`; a converter older than September 2026
  writes the `semantic_model` wrapper, which Provisa refuses by name.

Both paths were run on 2026-10-10 with the converters installed from `apache/ossie` commit
`642cb894` (they are not published as packages; they install from the repository's
`converters/dbt` and `converters/cube` directories and need Python 3.11 or later). A model of
two tables, one relationship, and one metric was exported from Provisa, converted to a dbt
semantic manifest and to a Cube model, converted back to Ossie by `ossie-dbt msi-to-ossie` and
`ossie-cube import`, and read by `parse_ossie_model`:

| Path | Result |
|---|---|
| Cube model → Ossie → Provisa | Read. Tables, keys, column types, the relationship, and the metric all came back; the relationship's name was the converter's (`orders_to_customers`). |
| dbt semantic manifest → Ossie → Provisa | Read. Tables, primary keys, and the metric came back. Column types did not (the converter's output states none), and the relationship came back as the converter derived it from dbt entities, which was not the registered one. Review the relationships before applying. |

[tool-verified: both runs, 2026-10-10, `apache/ossie` commit `642cb894`, `ossie-dbt` and
`ossie-cube` 0.2.0.dev0, metricflow 0.213.0; inputs were files those converters had written
from a Provisa export, not files authored in dbt or Cube]

The dbt converter refuses a model in which any table has a unique constraint over several
columns ("MetricFlow entities cannot represent composite keys losslessly").
[tool-verified: `ossie-dbt ossie-to-msi` on the same export with such a constraint, 2026-10-10]

#### The review screen

In the UI, the **Import** button (Metrics page → Ossie Interchange panel) opens a file picker.
After the document is posted and parsed, a review modal opens with every proposed table,
relationship, and metric listed as a checked item. The modeler can uncheck anything to exclude it.
Clicking **Apply** registers the checked items through the existing registration mutations — tables
first, then relationships (which reference tables), then metrics.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

The modeling role and history stored in a Provisa-exported Ossie document round-trip correctly
through import. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Metrics Across Protocols (REQ-1319)

A governed metric's definition — its expression, description, and `ai_context` — travels with the
value into every query surface through one compiler expansion. There are no copies. The compiler
reserves the `metrics` schema for SQL access; each protocol then adds its own metadata channel.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

Address any metric as a virtual relation in the `metrics` schema. The dimension columns you select
become the GROUP BY:

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

The compiler expands the `metrics.<name>` form into the real grouped aggregate before governance
runs. Column descriptions are surfaced as `pg_description` entries, so DBeaver and psql `\d+`
show them. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` is rejected — name the columns explicitly.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Metrics project inside the `_aggregate` root field as a `metrics` block.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

The definition text (`description`, `ai_context`) appears in GraphQL introspection docs, so
schema-aware tools and codegen pick it up automatically.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (AI agents)

Two tools expose metrics to MCP clients:

- **`list_metrics`** — returns all governed metrics visible to the session, with `name`,
  `description`, and `ai_context`.
- **`query_metric`** — accepts a metric name plus a dimension list and calls the compiler's
  semantic-SQL path, returning the aggregate result.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Agents that call `list_metrics` before constructing a query select a governed metric by name
rather than writing aggregation SQL by hand. The `ai_context` field is the place to put the
definition text that guides correct selection.

### Arrow Flight

Metrics are addressable as metric flight descriptors returning Arrow tables.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Use the same `metrics.<name>` SQL form via the standard Flight SQL ticket path.

### Bolt / Cypher (Neo4j Browser)

Call a metric using the `provisa.metric()` procedure:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Fact and Dimension tables carry `:Fact` and `:Dimension` node labels in the federated graph, so
Bloom renders the star shape automatically.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Natural language queries

The NL schema matcher resolves metric vocabulary in natural-language questions directly to a metric
plus dimensions, then generates semantic SQL. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Fact tables are tagged `[fact]` in the NL prompt; dimension tables are tagged `[dimension]`. The
matcher biases join paths fact-to-dimension when resolving questions.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

Combine `view_metrics` with `materialize` and a Kafka sink to produce push-on-change metric output
using the existing materialization machinery. No new pipeline is required.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observability (OTel)

Metric evaluations are traced and exportable as OpenTelemetry metrics.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
