---
name: project-layout
description: Module ownership map, architecture overview, and verification commands. Auto-triggers when navigating or modifying project structure.
---

# Project Layout

## Entry Point
`main.py` → `provisa/api/app.py` (FastAPI factory, lifespan, middleware)

## Backend (`provisa/`)

| Module | Purpose |
|---|---|
| `api/` | FastAPI app, routers, middleware |
| `api/admin/` | Strawberry GraphQL admin API |
| `api/rest/` | Auto-generated REST endpoints |
| `api/jsonapi/` | Auto-generated JSON:API endpoints |
| `api/flight/` | Arrow Flight server (port 8815) |
| `compiler/` | GraphQL → SQL, RLS, masking, sampling, federation |
| `transpiler/` | SQLGlot transpilation, routing |
| `executor/` | Trino/direct execution, output formats, redirect |
| `registry/` | Persisted query store, approval, governance |
| `security/` | Visibility, rights, column masking |
| `cache/` | Redis query result cache |
| `mv/` | Materialized view registry, refresh, rewriter |
| `events/` | Dataset change event dispatch |
| `webhooks/` | Outbound webhook execution |
| `scheduler/` | Background job scheduling |
| `subscriptions/` | SSE subscription state and delivery |
| `discovery/` | LLM relationship discovery |
| `grpc/` | Proto generation, gRPC server |
| `api_source/` | External API sources (REST/GraphQL/gRPC) |
| `kafka/` | Kafka topic sources and sinks |
| `auth/` | Auth providers, middleware, role mapping |
| `core/` | Config, models, DB, repositories, secrets |
| `hasura_v2/` | Hasura v2 metadata converter |
| `ddn/` | Hasura DDN converter |
| `mongodb/` | MongoDB connector |
| `elasticsearch/` | Elasticsearch connector |
| `cassandra/` | Cassandra connector |
| `accumulo/` | Accumulo connector |
| `prometheus/` | Prometheus connector |
| `source_adapters/` | Generic source adapter layer |
| `graphql_remote/` | Remote GraphQL schema connector |
| `neo4j/` | Neo4j Cypher query source |
| `sparql/` | SPARQL 1.1 triplestore source |

## Frontend (`provisa-ui/src/`)

| Module | Purpose |
|---|---|
| `components/` | React components (registration, query builder, approval queue) |
| `api/` | HTTP/GraphQL clients |
| `types/` | TypeScript type definitions |

## Python Client (`provisa-client/`)

Standalone package published to PyPI as `provisa-client`. Independent `pyproject.toml`, tests, and release artifact.

| File | Purpose |
|---|---|
| `provisa_client/client.py` | `ProvisaClient` — GraphQL (sync/async) and Arrow Flight methods |
| `tests/test_client.py` | Unit tests (respx mocks for HTTP; ticket encoding tests for Flight) |

## Dependency Graph
- `api/` → `executor/`, `registry/`, `registration/`, `security/`
- `executor/` → `compiler/`, `transpiler/`, `registry/`
- `compiler/` → `registration/`

## Component Stack
```
GraphQL Request
    → Compiler (GraphQL → PG-style SQL)
    → Transpiler (PG SQL → target dialect via SQLGlot)
    → Router (single-source → direct RDBMS, cross-source → Trino)
    → Executor (RLS injection, column security, execution)
    → Response
```

## Verification
- Server tests: `python -m pytest tests/ -x -q`
- Client tests: `python -m pytest provisa-client/tests/ -x -q`
- Dev server: `uvicorn main:app --reload`
