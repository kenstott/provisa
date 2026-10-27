# Provisa

**Connect Postgres, Snowflake and Salesforce. Query with SQL, GraphQL or your AI agent. In 5 minutes.**

Governed from the first query.

---

Provisa is a data kernel: a compiler and runtime for building your data control plane. Declare a small set of semantic primitives and the whole governed API follows.

Built the way an operating system is built: source drivers, engine drivers and secrets drivers plug in and swap without reinstalling; command bindings answer other tools' calls; clients connect over standard protocols.

The primitives are [sources](docs/sources.md), [tables](docs/configuration.md#tables), [relationships](docs/configuration.md#relationships), [terms](docs/ontology.md), [roles](docs/access-model.md), [capabilities](docs/access-model.md#capabilities), [grants](docs/access-model.md#grants), [scope](docs/access-model.md#scope), [products](docs/data-products.md), and contracts. Declare them and the whole governed API follows.

Every language compiles to one IR. Governance is applied there and pushed down into the native query your engine runs.

Provisa compiles each request and hands it to your engine. Results stream back. It scales horizontally and vertically to support your workloads, and the heavy compute is offloaded to your engine.

Four things follow from the one model:

- **Cross-source queries.** 73 source drivers reach more than 85 kinds of source, Salesforce, ServiceNow and Stripe among them. The SaaS systems your business runs on read as tables beside your databases, in place, without a pipeline into a warehouse first. Federation is declared in the model; the engine you choose runs it.
- **Metrics.** A metric is a named, governed aggregate. Define it once, and every tool and language that asks for it gets the same definition.
- **Ontology.** The business glossary is a graph of concepts, their definitions and how they relate. Provisa maps it onto the tables and columns that hold the values. A concept stays a draft until it reaches data. The glossary says what a concept is; a metric says how a number is computed.
- **Governance.** Six layers of checks are compiled into every query plan, in every language and over every protocol. Every request is recorded the same way: who queried what, as which role, under which policy. Traces, metrics and logs are registered as queryable tables beside your business data.

You can use it in two ways, and they combine:

- **To modernize.** Model your estate, let Provisa generate the native SQL for each source, then take that SQL and adopt it in the target system. Provisa is the bridge for the transition, and you can drop it afterwards.
- **To govern for good.** Keep it as the path every query takes, so definition, enforcement and audit stay together for as long as the estate exists.

## Try a demo

The quickest look is a live demo in your browser, with nothing to install: [provisa.dev/demos](https://provisa.dev/demos). Each one opens a private sandbox with a guided tour.

From a checkout of this repository you can run any demo on your own machine:

```bash
./start-ui-install.sh --list-demos     # the demos you can run, and which are published
./start-ui-install.sh --demo weblogs   # start one, with its tour
```

A published demo is pulled as a ready-made bundle; the others run from `demo/named/<name>/`. See [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) for every option.

## The model

Four ideas carry the whole design. Every source is reduced to tables. Every query is reduced to one SQL-like form. Reachability decides what is queried live and what is copied first. And a freshness strategy keeps each copy, and each derived dataset, as current as it needs to be.

Start with the sources. Each one is expressed as a set of two-dimensional tables over one shared type system, and that's the only contract a source has to meet to join the estate. A MySQL or PostgreSQL table already is one. A GraphQL query result becomes one when it's flattened. SPARQL triplestores and Neo4j don't look like tables at all, but you supply a query whose result is tabular, and the query becomes the adapter. Whatever the source, the estate sees rows, columns and types. Bringing in a new kind of source means meeting that one contract, sometimes with a little human help. Nobody writes a bespoke integration.

Queries get the same treatment. SQL, for all its dialects, is the language of analysis over tables, which makes a SQL-like form the natural common target. So every request, whatever language it arrives in, is lowered to that form first. SQL and GraphQL lower easily. Cypher's paths and graph semantics take real work, but they get there. Because everything passes through one form before anything else happens, governance applies in one place, whatever language the request came in.

With tabular sources and one query form, the model covers both live querying and warehousing: what a live query engine like Trino does, plus the materialization those engines lean on. **Reachability** ties the two together. For any source, can the engine query it where it is, or does its data have to be copied somewhere queryable first? The answer splits the estate into what's read live and what's copied.

Most databases already have some kind of live link: DuckDB's `ATTACH`, PostgreSQL's `postgres_fdw`, Databricks external links. So most databases can act as a federation engine up to a point. None covers everything. Each reaches a particular set of sources and copies the rest, and none says plainly which is which. The model makes reachability explicit, as a defined set of methods per source that say what the engine reaches live and, by elimination, what has to be copied.

That leaves freshness. For each source the engine can't reach, how current does its copy need to be? In practice there are only a few answers: on demand, on a schedule, on a change signal (CDC, a watermark, a snapshot), or pinned. Choosing one per source is the whole freshness policy.

Derived datasets fit the same shape: derived tables, aggregates, the output of a transform. They're written in the same form, so lineage comes for free. The path from each system of origin to a final output is the definition that produced it, readable from end to end. Building them asks the freshness question one step further on: does the dataset refresh on a schedule, only once its preconditions are met, continuously, or as a pinned historical snapshot? The choices are the same small set as for a source copy, so a derived dataset's build policy uses the same vocabulary.

Dimensional models follow directly. A star schema's facts and dimensions are derived datasets like any other. A dimension is a conformed, deduplicated projection, and a fact table is a join and aggregate reduced to a grain, each with its own build and freshness policy. Slowly changing dimensions need nothing special: a pinned snapshot is Type 2 history, a scheduled rebuild is Type 1. Because the schema is defined in the model and isn't bound to one warehouse's tables, the same facts and dimensions can be built in Oracle, in Databricks, or left virtual over an MPP engine, without remodeling.

Data Vault fits the same way, a layer earlier. Hubs are deduplicated business-key datasets, links are the registered relationships between them, and satellites are insert-only, time-stamped attribute datasets that keep the history. A satellite is simply a derived dataset on the change-signal strategy: load date plus hashdiff is change capture applied to descriptive attributes, and insert-only history is the pinned-snapshot strategy. Point-in-time and bridge tables are more derived datasets, built for query speed. So a raw vault is a set of derived datasets in the model, and a star schema is a projection off it, both generated and both portable across engines. The methodology stays yours to decide: what becomes a hub, the grain of a satellite, how to split. Once you've decided, the decision lives in the model. No ETL welds it to one warehouse.

You declare both patterns with **two shortcuts** instead of hand-written views. They're the building blocks every star schema and Data Vault use, and they don't commit you to either method:

- **`entity`** is a keyed, deduplicated projection of a source that can keep history. Declare the key, the attributes and a history mode, and Provisa builds a materialized view. Ask for history and it builds a **bitemporal MV** instead (`scd2` → delta, `snapshot` → snapshot). One construct gives you a Kimball **dimension** (SCD1 or SCD2) or a Data Vault **hub and satellite**.
- **`fact`** joins to entity keys, reduces to a declared grain and aggregates measures. Provisa builds an aggregate MV plus the registered relationships to the entities. One construct gives you a star **fact table** or a Data Vault **link**; a fact with no measures is a pure key-set link.

Each `entity` or `fact` becomes exactly the materialized views, bitemporal settings and relationships a modeler would otherwise write by hand, so the warehouse stays in the model and moves between engines without remodeling. Declare one in the admin UI (the **Model** form) or over the admin API (`registerEntity` / `registerFact`). The model generates a Kimball star or a Data Vault for you; it doesn't impose either.

### Time travel

Time travel means keeping every version of a row instead of overwriting it, so you can ask what the data was at any past moment. Engines differ a lot in how efficiently they can do that, so Provisa makes it part of the materialized view's **definition**, whatever the storage engine. Declare it once and it works on any store that can hold a materialized view.

What keeps it portable is that it's **append-only**. A version, once written, is never updated or deleted. The usual bitemporal trick of closing a row by writing back a "valid to" date needs an UPDATE, which many engines can't do cheaply, or at all, over a distributed store. So every refresh **appends** instead, and which version was in force at a given time is worked out at read time from the log. There are two ways to append:

- **Snapshot** appends the whole fresh dataset, stamped with the refresh's system time. There's no diffing and it's correct on every engine, but storage grows by a full copy per refresh.
- **Delta** appends only what changed, plus tombstones for removed keys. The engine works out the delta itself, with anti-joins inside an `INSERT … SELECT`. Provisa never folds it row by row. It's smaller, and it needs an entity key.

Provisa manages system time, when it recorded a version, this way. Valid time, when a fact is true in the business, comes from the view's own SELECT and is kept. Engines that can do better, such as native Iceberg snapshots or a MERGE that touches fewer rows, can be used behind the same declaration; the append-only path is the floor that's correct everywhere.

Reading needs no special syntax. A plain query against a bitemporal MV returns the **current** state, rebuilt from the log. To go back in time, send an `X-Provisa-As-Of: <timestamp>` header and the whole query is answered as the estate stood then, the same way on every store. Add `X-Provisa-As-Recorded` to read the data as Provisa had recorded it at a time, so a correction recorded later doesn't change a replay of the past. Turn it on for any materialized view in the admin UI (the **Time Travel** control: off, snapshot or delta, plus an entity key) or over the admin API.

Reachability and freshness together define what is live, what is copied, and how fresh each copy stays, independent of what any one engine can reach. The model is portable, so your estate isn't tied to whichever vendor's reach happens to cover the most sources today.

## Features

### Query Interfaces

These are the languages and structured APIs you write queries in. Each has its own syntax and semantics; governance (RLS, masking, column visibility, relationship enforcement) applies uniformly across all of them regardless of which wire protocol delivers them.

- **GraphQL** — Per-role schemas with field-level visibility, filtering, cursor-based pagination and aggregate queries (`count`, `sum`, `avg`, `min`, `max`). The schema offers only registered relationships, so every query is structurally valid by construction, and GraphQL is the fastest path to a correct simple query. Apollo APQ is included: queries are hashed and registered on the server, later calls send only the hash over HTTP GET, and responses become CDN-cacheable with no client changes. Lookup tables below a configurable row threshold appear as enum types.
- **SQL** — Full SQL across all sources, unconstrained and more expressive than GraphQL. Write standard SQL, correlated subqueries included, and it runs across sources unchanged. A single-source query routes direct to the driver (under 100 ms).
- **Cypher** — Graph query language over the same model. Traverse relationships as graph edges; union sources; variable-length paths. Governance applies identically to GraphQL and SQL.
- **gRPC model API** — A `.proto` generated from the registered schema, with typed query and insert RPCs for each table at the root of a role's schema, and streamed responses. Like GraphQL, it's schema-driven: the registration model is the contract, and protobuf is the wire encoding. It's a full per-table query interface, where Arrow Flight is a columnar streaming transport.
- **JSON:API** — A structured query API at `/data/jsonapi/{table}`, HTTP-only by design. It supports JSON:API 1.1: sparse fieldsets (`fields[table]=col1,col2`), filter expressions (`filter[field][op]=value`), compound documents (`include=relation`) and sort. It queries one table at a time with a standard filter syntax, so use SQL or GraphQL when you need a general-purpose query language.
- **Query Language Explorer** — Write a GraphQL query and see live **Semantic SQL** and **Cypher** translations in side panels; copy either or jump directly into the SQL or Graph editor. A practical workflow is to sketch query fragments in GraphQL, then stitch the resulting SQL into complex views or reports.

The Explorer shows a GraphQL query alongside its live SQL and Cypher translations:

![Query Language Explorer](docs/images/query-explorer.png)

The model also shows as a live graph, with domain and node labels, relationship types and variable-length traversals:

![Graph Visualization](docs/images/graph-view.png)

### Query Composition Tools

These tools help you write queries in the languages above. They aren't query languages themselves.

- **Natural language query** — An NL→SQL/Cypher/GraphQL pipeline that runs on Claude. Describe what you want in plain English, and the pipeline writes a query in the language you choose, with an interactive validation loop before anything runs.

![Natural Language Query](docs/images/natural-language.png)

### Wire Protocols

These are the connection protocols. SQL, GraphQL and Cypher ride over them, and the protocol you pick changes neither the query interface nor how governance behaves.

- **pgwire** — Any PostgreSQL client (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) connects on port 5439 as if it were a Postgres server. Accepts SQL only. Full governance pipeline applies. `pg_catalog` and `information_schema` answered from an in-memory catalog so schema browsers work without a catalog round-trip. TLS optional.
- **Bolt (Neo4j)** — Any Neo4j client (Neo4j Browser, Bloom, official drivers) connects over the Bolt protocol and runs Cypher over the registered schema. Each role the user holds surfaces as a `provisa_<role>` database. Same governance as every other transport. TLS optional.
- **Arrow Flight** — High-throughput columnar streaming over gRPC; accepts GraphQL or SQL as the query input. Unbounded result sets, no server-side materialization, no separate infrastructure required.
- **JDBC** — BI tool integration (Tableau, Power BI, DBeaver).
- **WebSocket / SSE** — Subscriptions: near-real-time change events; backends: PG native, MongoDB native, CDC, polling. Also exposed over Kafka.

### Data Sources

- **73 connectors, 85+ kinds of source** — The larger number counts the hosted PostgreSQL services (RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon) and the File Crawler's file formats one by one. PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, SPARQL triplestores, Kafka, Google Sheets, Kaggle, Salesforce, ServiceNow, Stripe and the rest sit behind a single API. Graph and RDF sources stand on equal terms with relational ones.
- **Smart routing** — A single-source query routes direct to the driver (under 100 ms). A multi-source query routes through the engine, on your own cluster or on the embedded workers.
- **API sources** — Register REST, GraphQL, gRPC, WebSocket or RSS endpoints as queryable tables, with SPARQL helpers included. Cross-source joins between API sources and relational sources need no extra setup.
- **Remote schema introspection** — Point at any GraphQL, OpenAPI, or gRPC endpoint; documented operations are automatically surfaced as queryable tables, graph nodes, and edges with full governance applied on top
- **File sources** — CSV, Parquet, and SQLite files as queryable tables; supports local paths and remote object storage (`s3://`, `ftp://`, `sftp://`)
- **Kaggle datasets** — search Kaggle's public catalog and register a dataset by token-authenticated live search, no manual download; registers as a single file source whose tables are discovered automatically, CSV/Parquet bundles only
- **Kafka integration** — Topics as read-only tables; query results as Kafka sinks
- **Scheduled triggers** — Cron and interval triggers (APScheduler) that fire webhooks, mutations, or Kafka sink publishes
- **Query routing hints** — SQL-comment routing hints override automatic routing decisions

![Data Sources](docs/images/data-sources.png)

Sources, files, and remote endpoints are registered as governed tables from the UI:

![Table Registration](docs/images/table-registration.png)

### Security & Governance

- **Row-level security** — Per-table, per-role WHERE clause injection
- **Column masking** — Per-column masking (regex, constant, truncate) with role-based bypass
- **Column presets** — Server-side static or session-variable values injected on insert/update; not exposed in mutation input types
- **Write permissions** — Per-column mutation access control (`writable_by`)
- **Inherited roles** — Roles inherit RLS, visibility, and masking from a parent role recursively
- **Tracked functions & webhooks** — DB functions and outbound webhooks exposed as GraphQL mutations with typed return shapes
- **ABAC approval hook** — Pre-execution authorization hook; webhook, gRPC, or unix_socket transport; per-table, per-source, or global scope; configurable fallback policy
- **Pluggable auth** — Firebase, Keycloak, OAuth 2.0, simple (testing)

![Security Roles](docs/images/security-roles.png)

### Delivery & Performance

- **Materialized views as recorded transforms** — An MV records the transform that produced it: its join shape or SQL, the per-source input signals it was built from (Iceberg snapshot, RDB watermark), and a determinism check at registration. Because the transform is recorded, Provisa can rewrite a query, or part of one, onto a fresh MV without you changing the query. Matching works on join patterns and accepts partial matches, so an MV that covers a subset of the joins still applies, and the remaining joins are kept.
- **Hot table inlining** — Small frequently-joined lookup tables are inlined as VALUES CTEs directly in the query plan, eliminating cross-source round trips for dimension data
- **Query caching** — Role+RLS-partitioned Redis result cache; APQ hash cache included
- **Observability as data** — Distributed traces, metrics and logs are collected through OpenTelemetry, compacted into Iceberg on S3 and registered as queryable tables (`traces`, `metrics`, `logs`, `queries`) in the schema. Query them with SQL, GraphQL or Cypher beside your business data. Join a `customers` table to `queries`, for instance, to see who ran what and how long it took.

### Administration & Integration

- **Admin API** — GraphQL at `/admin/graphql`; config upload/download, relationship editing, query approval
- **Reports viewer** — `/admin/reports` lists the built-in ops-domain management views and any registered custom reports; requires the `observability` capability
- **Table preview** — every registered table has a server-paged governed data viewer with pushed-down filters, multi-level group-by, and CSV export
- **GraphQL Voyager** — Interactive role-scoped schema visualization as an entity-relationship diagram
- **LLM relationship discovery** — Claude suggests foreign-key candidates.
- **Python client** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → pyarrow Tables, SQLAlchemy dialect, ADBC support
- **Data ingestion** — HTTP endpoints for pushing JSON event data into the platform
- **Hasura v2 / DDN import** — Convert Hasura v2 metadata or DDN supergraph YAML to Provisa config
- **Apollo Federation** — Expose Provisa as an Apollo Federation v2 subgraph

Role-scoped schema visualized as an entity-relationship diagram (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Relationships are registered, approved, and enforced as the only legal JOIN paths:

![Relationships](docs/images/relationships.png)

## Security Model

This is where "on the path every query already takes" has to hold up. Provisa applies one layered security model to every query language (GraphQL, SQL, Cypher) and every transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket), and no query path goes around it. Coverage comes from the design, so nobody has to remember anything: add a source, a column or a relationship, and every layer applies to it at once, with nothing extra to register.

The layers apply in order, and a request must clear each one before the next is checked.

### Layer 0 — Introspection filtering

The schema and catalog a role sees hold only the tables in its `domain_access` list and the columns that pass their `visible_to` rules. Anything outside the role's access is invisible from the start: it can't be queried, autocompleted or even inferred to exist. This covers the GraphQL schema, the SQL catalog and the query editor's schema browser.

### Layer 1 — Public access

Tables in domains with no `domain_access` restriction are visible to every authenticated identity, with nothing more to configure. Data that really is public takes no effort.

### Layer 2 — Domain access

Each role carries a `domain_access` list of domain IDs, and a query that touches a table outside those domains is refused before it runs. This is the broad ownership boundary: an HR role can't reach finance tables, however the SQL is written.

### Layer 3 — Row-level security

Once domain access is confirmed, per-table, per-role `WHERE` predicates are added to every `SELECT` at execution time, and they're evaluated against the raw data. A regional manager querying a shared orders table sees only their region's rows, even with `SELECT *`.

### Layer 4 — Column visibility and masking

Columns whose `visible_to` list leaves out the requesting role are removed from the output. Columns with a masking rule have their values replaced by regex redaction, a constant or truncation before the results leave the server. Masking applies in every query language and every output format.

### Layer 5 — Predicate guard

Masked columns are refused in `WHERE` and `HAVING` clauses. Otherwise a caller could work out the real value by binary-searching it in a filter, even though the output is masked. The refusal happens when the query is parsed, before anything runs.

### Relationship governance

In SQL, a JOIN condition must match a registered, approved relationship between the tables, and an unapproved join is refused. Each relationship carries a readable reason and description, which tell people and autonomous agents why the path exists. This is governance policy and not a hard security boundary. Layers 2 to 5 hold whatever the join looks like, so a deliberate workaround exposes nothing the role couldn't reach with two separate queries, and every attempt is logged for audit.

No invalid join ever runs on your data. Roles are governed by default, and opting out means deliberately granting the `ignore_relationships` right to a role you trust. An agent's OAuth token maps to a role, so an agent gets the same layers a person does. Data catalogs with a context layer tell an agent what it should do; Provisa enforces what it can do.

---

These layers compose. A role with domain access, RLS and masked columns has every layer active at once, because each layer is configured on its own and applies to any query that touches a governed object.

## Quick Start

### macOS

1. Download [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (always the latest release)
2. Drag **Provisa.app** to `/Applications` and double-click to launch
3. First launch completes a one-time setup (~2 min, no internet required)
4. Open Terminal:

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Download [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (always the latest release)
2. Make it executable and run it. First launch completes a one-time setup (no internet required):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Download [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (always the latest release)
2. Run the installer. It needs no admin rights.
3. Open **Provisa First Launch** from the Start Menu. It completes a one-time setup (~5 min, no internet required).
4. Open a new terminal:

```bash
provisa start
```

### First Query

In local development (`PROVISA_MODE=test`), no credentials are required. In production, you authenticate with a Bearer token, and Provisa reads the role from it.

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

Download [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (always the latest release) and add it to your BI tool's driver path.

```text
jdbc:provisa://localhost:8815
```

Authenticate with your Provisa username and password, and the server assigns your role.

See [docs/integrations.md](docs/integrations.md) for Tableau and Power BI setup steps.

### PostgreSQL Wire Protocol (pgwire)

Provisa speaks the PostgreSQL wire protocol on port 5439. Any client that can connect to Postgres connects to Provisa, with no driver, no adapter and no change to your existing tools.

**The PostgreSQL username selects the Provisa role.** With `provider: none` (trust mode), the password is ignored and any configured role name is accepted as the username — connect as `analyst`, `admin`, or any role to see that role's governed view of the data. With `provider: simple`, the password is bcrypt-validated. Other providers (`firebase`, `keycloak`, `oauth`) are not supported over pgwire.

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

Every query runs through the full governance pipeline, so domain access, RLS, masking and the predicate guard apply exactly as they do for GraphQL and REST. Schema browsers (DBeaver, DataGrip, pgAdmin) work with no setup. Provisa answers their `pg_catalog` and `information_schema` queries from an in-memory catalog scoped to the role's domain access, so users see only the tables and columns they may query.

DataGrip browsing the governed schema and its foreign-key diagram over pgwire, with no driver and no adapter:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

TLS is enabled by setting `PROVISA_PGWIRE_CERT` and `PROVISA_PGWIRE_KEY`. The port is configurable via `PROVISA_PGWIRE_PORT` (default `5439`).

### Bolt (Neo4j Wire Protocol)

Provisa also speaks the Neo4j **Bolt** protocol, so graph-native tools connect directly and run Cypher over the registered schema. Nothing is exported, and you need no separate graph database. Point **Neo4j Browser** or **Bloom** at Provisa and traverse relationships across sources under the same governance (domain access, RLS, masking).

Neo4j Browser runs Cypher against Provisa here. Its node labels, relationship types and property keys all come straight from the registered schema:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Enable it by setting `PROVISA_BOLT_PORT` (Neo4j's default is `7687`), and turn on TLS with `PROVISA_BOLT_CERT` and `PROVISA_BOLT_KEY`. Each Provisa role the authenticated user holds appears as a selectable `provisa_<role>` database (the `provisa_admin` selector above). Choosing one narrows the session to that role's domain rights, and the user can never exceed the roles they hold.

### Python Client

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role URL parameter
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

See [docs/python-client.md](docs/python-client.md) for full reference.

## Documentation

| Topic | Doc |
| --- | --- |
| Developer quick start (running from source) | [docs/quickstart.md](docs/quickstart.md) |
| Full YAML configuration reference | [docs/configuration.md](docs/configuration.md) |
| Endpoint reference (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| System design and component map | [docs/architecture.md](docs/architecture.md) |
| Security model (RLS, masking, auth) | [docs/security.md](docs/security.md) |
| Secret storage and `${secret:NAME}` references | [docs/secrets.md](docs/secrets.md) |
| Business glossary and term curation | [docs/ontology.md](docs/ontology.md) |
| Environments (dev / staging / prod) | [docs/environments.md](docs/environments.md) |
| Building test environments (profiling, fakes, synthetic data) | [docs/test-data.md](docs/test-data.md) |
| Fake methods reference | [docs/fake-methods.md](docs/fake-methods.md) |
| Running and building demos on a laptop (sales engineers) | [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) |
| Supported source types | [docs/sources.md](docs/sources.md) |
| SSE subscriptions | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, BI tools, Arrow Flight clients, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python client (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| Admin API | [docs/admin.md](docs/admin.md) |
| Deployment (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN import | [docs/import.md](docs/import.md) |
| Release workflow (alpha/beta/stable tags) | [docs/releasing.md](docs/releasing.md) |

## Sizing

Provisa includes embedded query workers for multi-source queries. At first launch you choose a RAM budget; Provisa derives the worker count automatically.

| Host RAM | Workers | Typical workload |
| --- | --- | --- |
| < 24 GB | 0 | Development, single-source queries, small teams |
| 24–47 GB | 1 | Small team, moderate cross-source queries |
| 48–95 GB | 2 | Departmental deployment, mixed BI + notebook usage |
| 96 GB+ | 4 | Large department, heavy concurrent cross-source workloads |

Worker count can be changed at any time by editing `~/.provisa/config.yaml` (`federation_workers: N`) and running `provisa restart`. Set to `0` to run coordination-only (single-node).

### Scaling beyond a single box

**Horizontal scale-out** — Run several Provisa instances behind a load balancer. Each instance is a complete system. Every instance must point at the same config DB (set `CONFIG_DB_HOST` on the secondary boxes), and they can share one Redis instance (`REDIS_URL`) for a single cache. Most queries spread across instances with no change. A very large cross-source join can outgrow one instance, and then you need a larger box or an external engine cluster.

**Shared Redis** — Set `REDIS_URL` on each instance to point at an external Redis. Shared Redis means cache entries from one instance are available to all, improving hit rates across the cluster.

**Bring your own engine cluster** — Point Provisa at an existing external engine cluster instead of the embedded workers. Recommended for large-scale or cloud deployments; see [docs/deployment.md](docs/deployment.md) for configuration.

## License

Business Source License 1.1 (unmodified, per MariaDB's Licensor covenants). Each
released version converts to the Change License (GPL v2.0 or later) on the 4th
anniversary of its public release; current and recent code stays under BSL.
Production use above the Additional Use Grant thresholds (fewer than 100
employees/contractors and under $1M prior-year revenue) requires a commercial
license. See [LICENSE](LICENSE).

The Licensor does not consent to use of this work for AI/ML training. See
[NOTICE](NOTICE), [ai.txt](ai.txt), and [robots.txt](robots.txt). For commercial
or AI-training licenses: <kennethstott@gmail.com>
