# Provisa

Config-driven data virtualization platform. A single governed API over heterogeneous data sources — query it with **GraphQL or SQL**, consume it over gRPC, REST, Arrow Flight, or JDBC. Row-level security, column masking, and query approval apply regardless of which query language you use.

**Operational performance** — Single-source queries execute directly against the source driver (target: sub-100ms). Multi-source queries federate transparently. Result caching, materialized view rewriting, and Arrow Flight columnar streaming scale with your workload.

## Features

### Query & API
- **GraphQL and SQL** — Both are first-class query languages; governance, RLS, and column masking apply to both; detected automatically by all client interfaces
- **GraphQL API** — Per-role schemas with field-level visibility, filtering, pagination, relationships
- **Cursor-based pagination** — Relay-style `first`/`after`/`last`/`before` arguments on all list queries; returns `pageInfo` with `hasNextPage`, `hasPreviousPage`, `startCursor`, `endCursor`
- **Aggregate queries** — Auto-generated `{table}_aggregate` types with `count`, `sum`, `avg`, `min`, `max` per numeric column and filtered `nodes` access
- **Apollo APQ** — Apollo Automatic Persisted Queries wire protocol; Redis-backed hash→query cache; Apollo Client gets automatic deduplication via `extensions.persistedQuery` with no code changes
- **Enum auto-detection** — Small lookup tables (≤ configured threshold rows) are automatically exposed as GraphQL enum types rather than string scalars
- **gRPC endpoint** — Auto-generated `.proto` from registration model, streaming responses
- **REST endpoints** — Auto-generated REST routes from approved queries
- **JSON:API endpoints** — Auto-generated JSON:API routes with pagination, relationships, error objects
- **SSE subscriptions** — Real-time push via pluggable providers (change events, polling)

### Data Sources
- **Multi-source federation** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, and more through a single API
- **Smart routing** — Single-source queries execute directly (sub-100ms); multi-source queries federate transparently via Trino-compatible federation — bring your own Trino or Trino-compatible cluster to scale out
- **Federation performance hints** — Query-level routing hints embedded as SQL comments (e.g., `/* @provisa route=trino */`) override automatic routing decisions for performance tuning
- **API sources** — Register REST/GraphQL/gRPC endpoints as queryable tables
- **Kafka integration** — Topics as read-only tables, query results as Kafka sinks
- **Scheduled triggers** — Cron and interval-based triggers (via APScheduler) that fire webhooks, mutations, or Kafka sink publishes; configured via the admin API or YAML config

### Security & Governance
- **Row-level security** — Per-table, per-role WHERE clause injection
- **Column masking** — Per-column data masking (regex, constant, truncate) with role-based bypass
- **Column presets** — Server-side preset values (static or session variable references) applied automatically on insert/update without exposing them in the mutation input type
- **Write permissions** — Per-column mutation access control (`writable_by`)
- **Webhook mutations** — Database function tracking and outbound webhook-backed mutations
- **Governed query registry** — Pre-approved named queries with approval workflow, role-scoped execution, and ceiling enforcement; distinct from Apollo APQ
- **Inherited roles** — Roles can inherit from a parent role, recursively inheriting RLS rules, column visibility, and masking policies; avoids duplicating permission sets across similar roles
- **ABAC approval hook** — Pluggable external authorization hook called before query execution; supports webhook, gRPC, and unix_socket transports; scoped per-table, per-source, or globally; configurable fallback policy when hook is unavailable
- **Pluggable auth** — Firebase, Keycloak, OAuth 2.0, simple (testing)

### Delivery & Performance
- **Output formats** — JSON, NDJSON, CSV, Parquet, Apache Arrow
- **Arrow Flight** — gRPC streaming for high-throughput columnar delivery (unbounded, no materialization)
- **Query caching** — Role+RLS-partitioned result caching
- **Materialized views** — Transparent SQL rewriting for JOIN optimization
- **Large result redirect** — Threshold-based S3 redirect for large result sets

### Administration & Integration
- **Admin API** — Strawberry GraphQL at `/admin/graphql` — config upload/download, relationship editing, AI-assisted FK suggestions, query approval
- **GraphQL Voyager** — Built-in interactive schema visualization accessible from the admin UI; renders the role-scoped schema as an interactive entity relationship diagram
- **LLM relationship discovery** — Claude-powered FK candidate suggestion
- **JDBC driver** — BI tool integration (Tableau, PowerBI, DBeaver): `approved` and `catalog` modes
- **Python client** — `pip install provisa-client`; GraphQL queries → DataFrames, Arrow Flight → pyarrow Tables
- **Hasura v2 import** — Convert Hasura v2 metadata YAML to Provisa config
- **DDN import** — Convert Hasura DDN supergraph metadata to Provisa config
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

```bash
# Ad-hoc GraphQL
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }", "role": "admin"}'

# Ad-hoc SQL
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders", "role": "admin"}'

# Execute a governed query by name (stable_id)
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"queryId": "monthly-revenue-by-region", "role": "analyst"}'
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
| Step-by-step getting started | [docs/quickstart.md](docs/quickstart.md) |
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
