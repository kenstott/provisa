# Provisa

Config-driven data virtualization platform. A single governed API over heterogeneous data sources — query it with **GraphQL, Cypher, Proto or SQL**, consume it over **gRPC, REST, Arrow Flight, or JDBC**. Row-level security, column masking, and query approval apply regardless of query language or transport.

Single-source queries execute directly against the source driver (target: sub-100ms). Multi-source queries federate transparently. Result caching, materialized view rewriting, and Arrow Flight columnar streaming scale with your workload.

## Features

### Query & API
- **GraphQL, Cypher and SQL** — First-class query languages; governance, RLS, and column masking apply to all; auto-detected by all client interfaces
- **Natural language query** — NL→SQL/Cypher/GraphQL pipeline powered by Claude with an interactive validation loop
- **GraphQL API** — Per-role schemas with field-level visibility, filtering, cursor-based pagination, and aggregate queries (`count`, `sum`, `avg`, `min`, `max`)
- **Apollo APQ** — Automatic Persisted Queries; Redis-backed hash→query cache; zero client changes required
- **Enum auto-detection** — Lookup tables below a configurable row threshold are exposed as GraphQL enum types
- **gRPC endpoint** — Auto-generated `.proto` from the registration model; streaming responses
- **REST & JSON:API endpoints** — Auto-generated routes from approved queries; JSON:API includes pagination, relationships, and error objects
- **Subscriptions** — Near-real-time change events over WebSocket, SSE, or Kafka; backends: PG native, MongoDB native, Debezium CDC, polling

### Data Sources
- **30+ source types** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, SPARQL triplestores, Kafka, Google Sheets, and more through a single API
- **Smart routing** — Single-source queries bypass federation (sub-100ms); multi-source queries route through Trino-compatible federation — bring your own cluster or use the embedded workers
- **API sources** — Register REST, GraphQL, gRPC, WebSocket, or RSS endpoints as queryable tables; SPARQL helpers included
- **Remote schema introspection** — Point at any GraphQL/OpenAPI/gRPC endpoint; Provisa introspects, registers, and caches results in Parquet with full governance applied on top
- **File sources** — CSV, Parquet, and SQLite files as queryable tables; supports local paths and remote object storage (`s3://`, `ftp://`, `sftp://`)
- **Kafka integration** — Topics as read-only tables; query results as Kafka sinks
- **Scheduled triggers** — Cron and interval triggers (APScheduler) that fire webhooks, mutations, or Kafka sink publishes
- **Federation performance hints** — SQL-comment routing hints override automatic routing decisions

### Security & Governance
- **Row-level security** — Per-table, per-role WHERE clause injection
- **Column masking** — Per-column masking (regex, constant, truncate) with role-based bypass
- **Column presets** — Server-side static or session-variable values injected on insert/update; not exposed in mutation input types
- **Write permissions** — Per-column mutation access control (`writable_by`)
- **Inherited roles** — Roles inherit RLS, visibility, and masking from a parent role recursively
- **Governed query registry** — Approved named queries with approval workflow, ceiling enforcement, and role-scoped execution. Each approved query is a virtual table: scopeable, joinable, and addressable by `stable_id` via a cacheable GET
- **Tracked functions & webhooks** — DB functions and outbound webhooks exposed as GraphQL mutations with typed return shapes
- **ABAC approval hook** — Pre-execution authorization hook; webhook, gRPC, or unix_socket transport; per-table, per-source, or global scope; configurable fallback policy
- **Pluggable auth** — Firebase, Keycloak, OAuth 2.0, simple (testing)

### Delivery & Performance
- **Output formats** — JSON, NDJSON, CSV, Parquet, Apache Arrow
- **Arrow Flight** — High-throughput columnar streaming over gRPC; unbounded, no server-side materialization
- **Query caching** — Role+RLS-partitioned Redis result cache; APQ hash cache included
- **Materialized views** — Transparent SQL rewriting for JOIN optimization; FRESH/STALE/REFRESHING lifecycle with scheduled refresh
- **Large result redirect** — Threshold-based S3 redirect for oversized result sets
- **OpenTelemetry** — Distributed tracing and metrics across all components; FastAPI, Redis, AsyncPG, gRPC auto-instrumented

### Administration & Integration
- **Admin API** — Strawberry GraphQL at `/admin/graphql`; config upload/download, relationship editing, query approval
- **GraphQL Voyager** — Interactive role-scoped schema visualization as an entity-relationship diagram
- **LLM relationship discovery** — Claude-powered foreign key candidate suggestions
- **JDBC driver** — BI tool integration (Tableau, Power BI, DBeaver) in `approved` or `catalog` mode
- **Python client** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → pyarrow Tables, SQLAlchemy dialect, ADBC support
- **Data ingestion** — HTTP endpoints for pushing JSON event data into the platform
- **Hasura v2 / DDN import** — Convert Hasura v2 metadata or DDN supergraph YAML to Provisa config
- **Apollo Federation** — Expose Provisa as an Apollo Federation v2 subgraph

## Quick Start

### macOS
1. Download the DMG from the [releases page](https://github.com/kenstott/provisa/releases/latest)
2. Drag **Provisa.app** to `/Applications` and double-click to launch
3. First launch completes a one-time setup (~2 min, no internet required)
4. Open Terminal:
```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux
1. Download `Provisa-<version>-linux-x86_64.AppImage` from the [releases page](https://github.com/kenstott/provisa/releases/latest)
2. Make it executable and run it — first launch completes a one-time setup (no internet required):
```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows
1. Download `Provisa-<version>-windows-x64.exe` from the [releases page](https://github.com/kenstott/provisa/releases/latest)
2. Run the installer — no admin rights required
3. Open **Provisa First Launch** from the Start Menu — completes a one-time setup (~5 min, no internet required)
4. Open a new terminal:
```
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

# Execute a governed query by name — GET is cacheable by CDN/proxies
curl "http://localhost:8001/data/graphql?queryId=monthly-revenue-by-region"

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

Download `provisa-jdbc-<version>.jar` from the [releases page](https://github.com/kenstott/provisa/releases/latest) and add it to your BI tool's driver path.

```
jdbc:provisa://localhost:8815
```

Authenticate with your Provisa username and password — the server assigns your role.

- **`approved` mode** — approved queries appear as virtual tables with governed columns and RLS enforced
- **`catalog` mode** — full schema visible; use with catalog tools (Collibra, Atlan, DBeaver)

See [docs/integrations.md](docs/integrations.md) for Tableau and Power BI setup steps.

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

# Execute a governed query by name (via DB-API approved mode)
with connect("http://localhost:8001", username="alice", password="secret",
             mode="approved") as conn:
    cur = conn.cursor()
    cur.execute("SELECT * FROM monthly-revenue-by-region")
    rows = cur.fetchall()

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
|-------|-----|
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
|----------|---------|-----------------|
| < 24 GB  | 0       | Development, single-source queries, small teams |
| 24–47 GB | 1       | Small team, moderate cross-source queries |
| 48–95 GB | 2       | Departmental deployment, mixed BI + notebook usage |
| 96 GB+   | 4       | Large department, heavy concurrent federation |

Worker count can be changed at any time by editing `~/.provisa/config.yaml` (`federation_workers: N`) and running `provisa restart`. Set to `0` to run coordination-only (single-node).

### Scaling beyond a single box

**Horizontal scale-out** — Run multiple Provisa instances behind a load balancer. Each instance is a fully functioning system. All instances must point at the same config DB (set `CONFIG_DB_HOST` on secondary boxes) and optionally a shared Redis instance (`REDIS_URL`) for a unified cache. Most queries distribute transparently; very large cross-source joins may exceed the resources of a single instance and require a larger box or BYO Trino.

**Shared Redis** — Set `REDIS_URL` on each instance to point at an external Redis. Shared Redis means cache entries from one instance are available to all, improving hit rates across the cluster.

**BYO Trino** — Point Provisa at an existing Trino cluster by setting `TRINO_HOST` and `TRINO_PORT`. The embedded workers are not started. Recommended for large-scale or cloud deployments.

## License

Business Source License 1.1
