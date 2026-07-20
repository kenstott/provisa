# Provisa

Provisa is an **active semantic layer**: a single definition of your data estate — every domain, relationship, and policy across your sources, excluding only the systems of origin themselves — that both operates the estate and governs it. The definition is not documentation an engine may consult; it *is* the engine. Registered domains and relationships are the only legal join paths, and access policies are compiled into every query plan. One model, three jobs:

- **Define** — Domains, columns, and relationships are declared once. That declaration is the schema every consumer sees and the only set of join paths any query may take.
- **Enforce** — Row-level security, column masking, column visibility, and query approval are applied inline on the execution path. No query reaches data without passing through them, so coverage is total by construction rather than by diligence.
- **Audit** — Because every request travels the same governed path, who queried what, under which role, and against which policy is recorded uniformly. Distributed traces, metrics, and logs are themselves registered as queryable tables alongside your business data.

One governed core serves every language and transport. Query with **GraphQL, Cypher, or SQL**; consume over **pgwire, Bolt, gRPC, REST, Arrow Flight, or JDBC**. Each query language lowers to a single intermediate representation where governance is injected once — so a policy cannot drift between languages — and that IR retargets to each source's native dialect on the way out. Adding a language is a new front-end onto the shared core, not a new engine.

The estate is both analytical and transactional. Cross-source reads fan out through the federation layer; writes and single-source reads route directly to the source driver — governed identically, but transactional and sub-100ms. Arrow Flight columnar streaming is built in.

The whole model is built from a handful of primitives — domains, relationships, roles, and policies. Small vocabulary, so the definition is easy to comprehend and simple to evaluate and audit: you can read the policy set and know what it does. Provisa is a light query compiler, not a runtime that sits in the data path. It converts a request into native queries, routes them, and gets out of the way — which is why the estate performs.

That design supports two ways of using it, and they are not exclusive:

- **As scaffolding for modernization** — Model your estate, let Provisa generate the native SQL for each source, then capture that SQL and adopt it directly in the target system. Provisa is the transition layer, not a permanent dependency.
- **As permanent policy-enforcing infrastructure** — Keep it in place as the governed path every query takes, so definition, enforcement, and audit stay unified for as long as the estate exists.

## The federation model

The whole model comes down to two contracts and two policies: sources reduce to 2-D tables over one type system, queries reduce to one SQL-like IR, reachability decides what is queried live versus materialized, and a freshness strategy governs each materialized copy and derived dataset. Data shape in, query shape in, governance at the join, native queries out. The rest of this section walks each piece.

The model rests on one reduction: every source is expressed as a collection of two-dimensional tables over a single, generalized type system. That is the contract a source must meet to join the estate, and it is the same contract for all of them. Some sources already fit — a MySQL or PostgreSQL table *is* a typed 2-D relation. Some fit with a projection: a GraphQL query result, once flattened, is a table. Some are foreign to the shape — SPARQL triplestores, Neo4j — but remain workable, because the user supplies a query whose result set is tabular; the query is the adapter. Whatever the source, the estate sees rows, columns, and generalized types, and nothing else. Onboarding a new kind of source is meeting that one contract, sometimes with a step of human intervention, not writing a bespoke integration.

That reduction has a twin on the query side. SQL — across all its dialects and quirks — is essentially the language for analysis over 2-D datasets, which makes an SQL-like form the natural universal target for queries. So every request, in whatever language it arrives, is lowered to that intermediate representation as its very first step. Some lower cleanly — SQL itself, and even GraphQL; some are hard — Cypher's path and graph semantics take real work — but all are doable. Funneling every request into one IR before anything else happens is what lets governance apply in exactly one place, on one form, regardless of the language it came in on.

On top of those two uniform shapes — tabular sources and a single query form — federation here means both live query and warehousing — the same span a live query engine like Trino covers, plus the materialization such engines lean on. The concept that unifies them is **reachability**: for any source, can the engine query it in place, or must its data first be materialized somewhere queryable? Reachability partitions the estate into what is queried live and what is copied first.

Most databases already carry some notion of a live link — DuckDB `ATTACH`, PostgreSQL `postgres_fdw`, Databricks external links. So most databases can act as a federation engine to some degree. None is comprehensive: each reaches a particular set of sources and materializes the rest, with no single account of which is which. The model closes that gap by making reachability explicit — a defined set of methods, per source, that state what the engine can reach live and, by elimination, what must be materialized.

What remains is freshness: for each non-reachable source, how current must its materialized copy be? In practice this reduces to a small set of strategies — on demand, on a schedule, on a change signal (CDC, watermark, snapshot), or pinned. Choosing one per source is the whole freshness policy.

Analytical datasets — derived tables, aggregates, the outputs of a transform — fold into the same shape. They too must be expressed in the IR, and because they are, lineage is not a separate system to maintain: the path from each system of origin to a final output *is* the IR that produced it, readable end to end. Building them raises the freshness question one step removed — does the dataset refresh on a schedule, only once its preconditions are met, continuously as near-real-time, or as a pinned historical snapshot? The ways to express how and when to build a dataset are the same small, enumerable set, so a derived dataset carries a build policy in exactly the vocabulary a source copy does.

Dimensional models are a direct application. A star schema's fact and dimension tables are analytical datasets like any other — a dimension is a conformed, deduplicated projection; a fact table is a join and aggregate reduced to a grain — each carrying its own build and freshness policy. Slowly changing dimensions need no special machinery: a pinned snapshot is Type 2 history, a scheduled rebuild is Type 1. And because the schema is defined in the IR rather than physically bound to one warehouse's tables, the same fact and dimension definitions retarget — materialized in Oracle, in Databricks, or left virtual over an MPP engine — without remodeling. The model generates the star schema; it does not lock it to an engine.

Data Vault fits the same way, one layer earlier. Its hubs are deduplicated business-key datasets, its links are the registered relationships between them, and its satellites are insert-only, time-stamped attribute datasets — the historical record. A satellite is just a derived dataset on the change-signal freshness strategy: load-date plus hashdiff is CDC applied to descriptive attributes, and insert-only history is the pinned-snapshot strategy. Point-in-time and bridge tables are further derived datasets built for query performance. So a raw vault is a set of analytical datasets in the IR, and a star schema is a projection off it — both generated, both portable across engines. What the model does not do is decide the methodology: what becomes a hub, the grain of a satellite, the split strategy. Those stay modeling choices; once made, they live as portable IR rather than ETL welded to one warehouse.

Reachability plus freshness is a general model for data federation: a definition that says what is live, what is materialized, and how fresh each copy stays — independent of any one engine's reach. The outcome is freedom from proprietary lock-in. The model is portable; the estate is not captive to whichever vendor's federation happens to reach the most sources today.

## Features

### Query Interfaces

These are the languages and structured APIs you write queries in. Each has its own syntax and semantics; governance (RLS, masking, column visibility, relationship enforcement) applies uniformly across all of them regardless of which wire protocol delivers them.

- **GraphQL** — Per-role schemas with field-level visibility, filtering, cursor-based pagination, and aggregate queries (`count`, `sum`, `avg`, `min`, `max`). Schema-constrained to registered relationships — structurally valid by construction, the fastest path to a correct simple query. Apollo APQ included: queries are hashed and registered server-side; subsequent calls send only the hash over HTTP GET, making responses CDN-cacheable with zero client changes required. Lookup tables below a configurable row threshold are exposed as enum types.
- **SQL** — Full SQL over federated data; unconstrained and more expressive than GraphQL. Write standard SQL — correlated subqueries and all — and it runs across sources unchanged. Single-source queries bypass the federation layer entirely (sub-100ms).
- **Cypher** — Graph query language over the same federated schema. Traverse relationships as graph edges; union sources; variable-length paths. Governance applies identically to GraphQL and SQL.
- **gRPC model API** — Auto-generated `.proto` from the registered schema; typed query and insert RPCs per table, streamed responses. Schema-driven in the same sense as GraphQL — the registration model is the contract, protobuf is the wire encoding. Unlike Arrow Flight (which is a columnar streaming transport), this is a full per-table query interface.
- **JSON:API** — Structured query API at `/data/jsonapi/{table}`, HTTP-only by design. Supports JSON:API 1.1: sparse fieldsets (`fields[table]=col1,col2`), filter expressions (`filter[field][op]=value`), compound documents (`include=relation`), and sort. Not a general-purpose query language — queries one table at a time with standardized filter syntax rather than an ad-hoc query string.
- **Query Language Explorer** — Write a GraphQL query and see live **Semantic SQL** and **Cypher** translations in side panels; copy either or jump directly into the SQL or Graph editor. A practical workflow is to sketch query fragments in GraphQL, then stitch the resulting SQL into complex views or reports.

The Explorer shows a GraphQL query alongside its live SQL and Cypher translations:

![Query Language Explorer](docs/images/query-explorer.png)

The same federated schema is explorable as a live graph — domain and node labels, relationship types, and variable-length traversals:

![Graph Visualization](docs/images/graph-view.png)

### Query Composition Tools

These tools help you write queries in the above languages — they are not query languages themselves.

- **Natural language query** — NL→SQL/Cypher/GraphQL pipeline powered by Claude. Describe what you want in plain English; the pipeline produces a query in your chosen language with an interactive validation loop before execution.

![Natural Language Query](docs/images/natural-language.png)

### Wire Protocols

These are the connection protocols. SQL, GraphQL, and Cypher ride over them — the choice of wire protocol does not change the query interface or governance behaviour.

- **pgwire** — Any PostgreSQL client (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) connects on port 5439 as if it were a Postgres server. Accepts SQL only. Full governance pipeline applies. `pg_catalog` and `information_schema` answered from an in-memory catalog so schema browsers work without a federation round-trip. TLS optional.
- **Bolt (Neo4j)** — Any Neo4j client (Neo4j Browser, Bloom, official drivers) connects over the Bolt protocol and runs Cypher against the federated graph. Each role the user holds surfaces as a `provisa_<role>` database. Same governance as every other transport. TLS optional.
- **Arrow Flight** — High-throughput columnar streaming over gRPC; accepts GraphQL or SQL as the query input. Unbounded result sets, no server-side materialization, no separate infrastructure required.
- **JDBC** — BI tool integration (Tableau, Power BI, DBeaver) in `approved` or `catalog` mode.
- **WebSocket / SSE** — Subscriptions: near-real-time change events; backends: PG native, MongoDB native, CDC, polling. Also exposed over Kafka.

### Data Sources

- **46 source types** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, SPARQL triplestores, Kafka, Google Sheets, and more through a single API; graph and RDF sources are first-class, not adapters
- **Smart routing** — Single-source queries bypass federation (sub-100ms); multi-source queries route through the federation layer — bring your own cluster or use the embedded workers
- **API sources** — Register REST, GraphQL, gRPC, WebSocket, or RSS endpoints as queryable tables; SPARQL helpers included; federated joins across API sources and relational sources work transparently
- **Remote schema introspection** — Point at any GraphQL, OpenAPI, or gRPC endpoint; documented operations are automatically surfaced as queryable tables, graph nodes, and edges with full governance applied on top
- **File sources** — CSV, Parquet, and SQLite files as queryable tables; supports local paths and remote object storage (`s3://`, `ftp://`, `sftp://`)
- **Kafka integration** — Topics as read-only tables; query results as Kafka sinks
- **Scheduled triggers** — Cron and interval triggers (APScheduler) that fire webhooks, mutations, or Kafka sink publishes
- **Federation performance hints** — SQL-comment routing hints override automatic routing decisions

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

- **Materialized views as recorded transforms** — An MV captures the transform that produced it: its join shape or SQL, the per-source input signals (Iceberg snapshot, RDB watermark) it was built from, and a determinism check at registration. Because the transform is recorded, queries (or subexpressions) are transparently rewritten onto a fresh MV — structural join-pattern matching with partial-match support, so an MV covering a subset of joins still applies, with remaining joins preserved
- **Hot table inlining** — Small frequently-joined lookup tables are inlined as VALUES CTEs directly in the query plan, eliminating cross-source round trips for dimension data
- **Query caching** — Role+RLS-partitioned Redis result cache; APQ hash cache included
- **Observability as data** — Distributed traces, metrics, and logs are collected via OpenTelemetry, compacted into Iceberg on S3, and automatically registered as queryable tables (`traces`, `metrics`, `logs`, `queries`) in the federated schema; query them with SQL, GraphQL, or Cypher alongside your business data — join a `customers` table to the `queries` table to see who ran what and how long it took

### Administration & Integration

- **Admin API** — GraphQL at `/admin/graphql`; config upload/download, relationship editing, query approval
- **GraphQL Voyager** — Interactive role-scoped schema visualization as an entity-relationship diagram
- **LLM relationship discovery** — Claude-powered foreign key candidate suggestions
- **Python client** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → pyarrow Tables, SQLAlchemy dialect, ADBC support
- **Data ingestion** — HTTP endpoints for pushing JSON event data into the platform
- **Hasura v2 / DDN import** — Convert Hasura v2 metadata or DDN supergraph YAML to Provisa config
- **Apollo Federation** — Expose Provisa as an Apollo Federation v2 subgraph

Role-scoped schema visualized as an entity-relationship diagram (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Relationships are registered, approved, and enforced as the only legal JOIN paths:

![Relationships](docs/images/relationships.png)

## Security Model

This is where "on the path every query already takes" stops being a slogan. Provisa enforces a multi-layered security model across every query language (GraphQL, SQL, Cypher) and every transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). Governance is applied uniformly — there is no query path that bypasses it. Coverage is total by construction, not by diligence: add a source, column, or relationship and every layer applies to it automatically, with nothing to remember to register.

The layers apply in order. A request must clear each layer before the next is evaluated.

### Layer 0 — Introspection filtering

The schema and catalog presented to a role contain only the tables in its `domain_access` list and the columns that pass per-column `visible_to` rules. Objects outside a role's access are invisible at discovery time — they cannot be queried, autocompleted, or inferred to exist. This applies to the GraphQL schema, SQL catalog, and the query editor's schema browser.

### Layer 1 — Public access

Tables in domains with no `domain_access` restriction are visible to all authenticated identities with no additional configuration. Zero friction for genuinely public data.

### Layer 2 — Domain access

Each role carries a `domain_access` list of domain IDs. A query that touches a table outside those domains is rejected before execution. This is the coarse ownership boundary — an HR role cannot reach finance tables regardless of how the SQL is written.

### Layer 3 — Row-level security

After domain access is confirmed, per-table, per-role `WHERE` predicates are injected into every `SELECT` at execution time. The predicates evaluate against raw data. A regional manager querying a shared orders table sees only their region's rows even on a `SELECT *`.

### Layer 4 — Column visibility and masking

Columns with a `visible_to` list that excludes the requesting role are stripped from query output. Columns with a masking rule have their values replaced — regex redaction, constant replacement, or truncation — before results leave the server. Masking applies in all query languages and output formats.

### Layer 5 — Predicate guard

Masked columns are rejected from `WHERE` and `HAVING` clauses. Without this, a caller could infer the unmasked value by binary-searching it in a filter even though the output is masked. Rejection is enforced at query parse time, before execution.

### Relationship governance

JOIN conditions in SQL must match a registered, approved relationship between tables. Unapproved joins are rejected. Each relationship carries a human-readable reason and description — guidance for both users and autonomous agents about why a traversal path exists. This is governance policy, not a hard security boundary: Layers 2–5 hold regardless of join structure, so a deliberate circumvention does not expose data the role could not reach through two separate queries. Circumvention attempts are logged and auditable.

---

These layers compose. A role with domain access, RLS, and masked columns has all five constraints active simultaneously. Adding a new data source, column, or relationship does not require updating every rule — each layer is configured independently and applies automatically to any query that touches governed objects.

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
2. Make it executable and run it — first launch completes a one-time setup (no internet required):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Download [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (always the latest release)
2. Run the installer — no admin rights required
3. Open **Provisa First Launch** from the Start Menu — completes a one-time setup (~5 min, no internet required)
4. Open a new terminal:

```bash
provisa start
```

### First Query

In local development (`PROVISA_MODE=test`), no credentials are required. In production, authenticate with a Bearer token — the role is extracted from it automatically.

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

Authenticate with your Provisa username and password — the server assigns your role.

- **`catalog` mode** — full schema visible; use with catalog tools (Collibra, Atlan, DBeaver)

See [docs/integrations.md](docs/integrations.md) for Tableau and Power BI setup steps.

### PostgreSQL Wire Protocol (pgwire)

Provisa speaks the PostgreSQL wire protocol on port 5439. Any client that can connect to Postgres connects to Provisa — no driver, no adapter, no changes to existing tooling.

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

All queries run through the full governance pipeline — domain access, RLS, masking, and predicate guard apply exactly as they do for GraphQL and REST. Schema browsers (DBeaver, DataGrip, pgAdmin) work out of the box: `pg_catalog` and `information_schema` queries are answered from an in-memory catalog scoped to the role's domain access, so users see only the tables and columns they are permitted to query.

DataGrip browsing the governed schema and its foreign-key diagram over pgwire — no driver, no adapter:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

TLS is enabled by setting `PROVISA_PGWIRE_CERT` and `PROVISA_PGWIRE_KEY`. The port is configurable via `PROVISA_PGWIRE_PORT` (default `5439`).

### Bolt (Neo4j Wire Protocol)

Provisa also speaks the Neo4j **Bolt** protocol, so graph-native tools connect directly and run Cypher against the federated graph — no export, no separate graph database. Point **Neo4j Browser** or **Bloom** at Provisa and traverse relationships across sources with the same governance (domain access, RLS, masking) applied.

Neo4j Browser running Cypher against Provisa — node labels, relationship types, and property keys come straight from the registered schema:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Enable it by setting `PROVISA_BOLT_PORT` (Neo4j's default is `7687`). TLS is enabled with `PROVISA_BOLT_CERT` and `PROVISA_BOLT_KEY`. Each Provisa role the authenticated user holds surfaces as a selectable `provisa_<role>` database (the `provisa_admin` selector above) — choosing one narrows the session to that role's domain rights; the user can never exceed the roles they hold.

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

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
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
| Supported source types | [docs/sources.md](docs/sources.md) |
| SSE subscriptions | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, BI tools, Arrow Flight clients, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python client (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| Admin API | [docs/admin.md](docs/admin.md) |
| Deployment (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN import | [docs/import.md](docs/import.md) |
| Release workflow (alpha/beta/stable tags) | [docs/releasing.md](docs/releasing.md) |

## Sizing

Provisa includes a built-in federation engine for multi-source queries. At first launch you choose a RAM budget; Provisa derives the number of local federation workers automatically.

| Host RAM | Workers | Typical workload |
| --- | --- | --- |
| < 24 GB | 0 | Development, single-source queries, small teams |
| 24–47 GB | 1 | Small team, moderate cross-source queries |
| 48–95 GB | 2 | Departmental deployment, mixed BI + notebook usage |
| 96 GB+ | 4 | Large department, heavy concurrent federation |

Worker count can be changed at any time by editing `~/.provisa/config.yaml` (`federation_workers: N`) and running `provisa restart`. Set to `0` to run coordination-only (single-node).

### Scaling beyond a single box

**Horizontal scale-out** — Run multiple Provisa instances behind a load balancer. Each instance is a fully functioning system. All instances must point at the same config DB (set `CONFIG_DB_HOST` on secondary boxes) and optionally a shared Redis instance (`REDIS_URL`) for a unified cache. Most queries distribute transparently; very large cross-source joins may exceed the resources of a single instance and require a larger box or an external federation cluster.

**Shared Redis** — Set `REDIS_URL` on each instance to point at an external Redis. Shared Redis means cache entries from one instance are available to all, improving hit rates across the cluster.

**Bring your own federation cluster** — Point Provisa at an existing external federation cluster instead of the embedded workers. Recommended for large-scale or cloud deployments; see [docs/deployment.md](docs/deployment.md) for configuration.

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
