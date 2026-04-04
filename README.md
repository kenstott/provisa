# Provisa

Config-driven data virtualization platform, specifically to power a semantic layer from small teams to large enterprises. Unified GraphQL/gRPC API over heterogeneous data sources with governance, security, and performance optimization.

The semantic layer distinction is important. We use GraphQL as a universal query language for this purpose, specifically because it can only composite existing semantics. To add to the semantic layer you must create new data sources or aggregates within the data virtualization layer. This creates a clean separation, so no new additions to the semantics can be added outside the platform, allowing for true data governance.

Furthermore, Provisa is designed to be highly performant for operational needs and highly scalable for enterprise analytical needs. You can use a single platform for both without sacrificing speed or scalability.

## Features

- **Multi-source federation**: Query PostgreSQL, MySQL, MongoDB, and more through a single API
- **GraphQL API**: Per-role schemas with field-level visibility, filtering, pagination, relationships
- **gRPC endpoint**: Auto-generated .proto files from registration model, streaming responses
- **Smart routing**: Single-source → direct driver (sub-100ms); multi-source → Trino federation
- **Row-level security**: Per-table, per-role WHERE clause injection
- **Column masking**: Per-column data masking (regex, constant, truncate) with role-based bypass
- **Write permissions**: Per-column mutation access control (`writable_by`)
- **Query caching**: Redis-backed with role+RLS-partitioned keys
- **Materialized views**: Transparent SQL rewriting for JOIN optimization
- **Output formats**: JSON, NDJSON, CSV, Parquet, Apache Arrow
- **Persisted query registry**: Approval workflow, governance, ceiling enforcement
- **API sources**: Register REST/GraphQL/gRPC endpoints as queryable tables
- **Kafka integration**: Topics as read-only tables, query results as Kafka sink
- **LLM relationship discovery**: Claude-powered FK candidate suggestion
- **Arrow Flight**: gRPC streaming endpoint for high-throughput Arrow columnar delivery
- **JDBC driver**: BI tool integration (Tableau, PowerBI, DBeaver) with two modes — `approved` (governed queries as views) and `catalog` (schema discovery for Collibra)
- **Pluggable auth**: Firebase, Keycloak, OAuth 2.0, simple (testing)

## Quick Start

```bash
# Start infrastructure
docker compose up -d

# Install
pip install -e ".[dev]"

# Configure
export PG_PASSWORD=provisa

# Run
uvicorn main:app --reload --port 8001

# Query
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }", "role": "admin"}'
```

## Configuration

See [docs/configuration.md](docs/configuration.md) for the full YAML reference.

## API

See [docs/api-reference.md](docs/api-reference.md) for endpoint documentation.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the system design.

## Security

See [docs/security.md](docs/security.md) for the security model.

## Supported Sources

See [docs/sources.md](docs/sources.md) for source type details.

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
python -m pytest tests/unit/ -x -q       # unit tests
python -m pytest tests/ -x -q -m e2e     # e2e tests (needs docker compose)

# Start UI
cd provisa-ui && npm install && npm run dev
```

## License

Business Source License 1.1
