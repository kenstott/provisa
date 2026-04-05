# Provisa

Config-driven data virtualization platform — a semantic layer from small teams to large enterprises. Unified GraphQL/gRPC API over heterogeneous data sources with governance, security, and performance optimization.

GraphQL is used as the universal query language specifically because it can only composite existing semantics. New data can only enter the semantic layer via registered sources or aggregates inside Provisa — nothing bypasses governance.

## Features

### Query & API
- **GraphQL API** — Per-role schemas with field-level visibility, filtering, pagination, relationships
- **gRPC endpoint** — Auto-generated `.proto` from registration model, streaming responses
- **REST endpoints** — Auto-generated REST routes from approved queries
- **JSON:API endpoints** — Auto-generated JSON:API routes with pagination, relationships, error objects
- **SSE subscriptions** — Real-time push via pluggable providers (change events, polling)

### Data Sources
- **Multi-source federation** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, and more through a single API
- **Smart routing** — Single-source queries execute directly (sub-100ms); multi-source queries federate transparently via Trino-compatible federation — bring your own Trino or Trino-compatible cluster to scale out
- **API sources** — Register REST/GraphQL/gRPC endpoints as queryable tables
- **Kafka integration** — Topics as read-only tables, query results as Kafka sinks

### Security & Governance
- **Row-level security** — Per-table, per-role WHERE clause injection
- **Column masking** — Per-column data masking (regex, constant, truncate) with role-based bypass
- **Write permissions** — Per-column mutation access control (`writable_by`)
- **Webhook mutations** — Database function tracking and outbound webhook-backed mutations
- **Persisted query registry** — Approval workflow, governance, ceiling enforcement
- **Pluggable auth** — Firebase, Keycloak, OAuth 2.0, simple (testing)

### Delivery & Performance
- **Output formats** — JSON, NDJSON, CSV, Parquet, Apache Arrow
- **Arrow Flight** — gRPC streaming for high-throughput columnar delivery (unbounded, no materialization)
- **Query caching** — Role+RLS-partitioned result caching
- **Materialized views** — Transparent SQL rewriting for JOIN optimization
- **Large result redirect** — Threshold-based S3 redirect for large result sets

### Administration & Integration
- **Admin API** — Strawberry GraphQL at `/admin/graphql` — config upload/download, relationship editing, AI-assisted FK suggestions, query approval
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
3. First launch runs a one-time setup (~2 min): imports bundled images into a Lima VM, installs the `provisa` CLI — no internet required
4. Open Terminal:
```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux
1. Download `Provisa-<version>-linux-x86_64.AppImage` from the [releases page](https://github.com/kenstott/provisa/releases/latest)
2. Make it executable and run it — first launch sets up bundled services (no internet required):
```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows
1. Download `Provisa-<version>-windows-x64.exe` from the [releases page](https://github.com/kenstott/provisa/releases/latest)
2. Run as Administrator — installs to `C:\Program Files\Provisa\` and adds `provisa` to your PATH
3. Open a new terminal:
```
provisa start
```

### First Query

```bash
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }", "role": "admin"}'
```

### JDBC (Tableau, DBeaver, Power BI)

Download `provisa-jdbc-<version>.jar` from the [releases page](https://github.com/kenstott/provisa/releases/latest) and add it to your BI tool's driver path.

```
jdbc:provisa://localhost:8815?mode=approved&role=analyst
```

- **`approved` mode** — approved queries appear as virtual tables with governed columns and RLS enforced
- **`catalog` mode** — full schema visible; use with catalog tools (Collibra, Atlan, DBeaver)

See [docs/integrations.md](docs/integrations.md) for Tableau and Power BI setup steps.

### Python Client

```bash
pip install provisa-client
```

```python
from provisa_client import ProvisaClient

client = ProvisaClient("http://localhost:8001", role="admin")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount region } }")

# Arrow Flight → pyarrow Table (high-throughput)
table = client.flight("{ orders { id amount region } }")
```

See [docs/quickstart.md](docs/quickstart.md) for a step-by-step walkthrough.

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

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
python -m pytest tests/unit/ -x -q       # unit tests
python -m pytest tests/ -x -q -m e2e     # e2e tests (needs services running)

# Start UI
cd provisa-ui && npm install && npm run dev
```

## License

Business Source License 1.1
