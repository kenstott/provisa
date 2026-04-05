CRITICAL: Never add fallback values or silent error handling. Caused repeated production issues.
CRITICAL: V1 development. Never add migrations.
CRITICAL: Maximum brevity. No pleasantries. No explanations unless asked. Code and facts only.
CRITICAL: Files must stay under 1000 lines. If a file approaches or exceeds this, split it by separation of concerns. This applies to all languages (Python, TypeScript, etc.).
CRITICAL: "Audit" for UI features must include browser rendering and functionality testing (vitest + Playwright), not just code review.
CRITICAL: All Playwright tests must catch uncaught browser exceptions (pageerror). The base fixture in `provisa-ui/e2e/coverage.ts` handles this automatically — all specs must import `test` from `./coverage`, never directly from `@playwright/test`.
CRITICAL: Test errors must be resolved whether they are preexisting or not. Never skip or ignore failing tests.

# Code Audits
When asked to audit code against a spec, requirements, or standards: spawn parallel Explore subagents (split by phase/module range) to compare implementation against the spec. Gather results, then synthesize into a single report categorized as: completed to spec, not added, added but incomplete, added but not to spec.

# Requirements Tracking
On new requirement/constraint/feature/design decision: spawn background haiku agent to append to `docs/arch/requirements.md`. Agent reads `.claude/agents/requirements-tracker.md` for format, then current file, then appends. Silent. Skip implementation details, bugs, questions.

# Architecture
## Entry point
`main.py` → `provisa/api/app.py` (FastAPI factory, lifespan, middleware)

## Backend (`provisa/`)
| Module | Purpose |
|--------|---------|
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

Patterns: FastAPI async handlers, Pydantic validation.

## Python Client (`provisa-client/`)

Standalone package published to PyPI as `provisa-client`. Independent of the server — its own `pyproject.toml`, tests, and release artifact.

| File | Purpose |
|------|---------|
| `provisa_client/client.py` | `ProvisaClient` — GraphQL (sync/async) and Arrow Flight methods |
| `tests/test_client.py` | Unit tests (respx mocks for HTTP; ticket encoding tests for Flight) |

Verification: `python -m pytest provisa-client/tests/ -x -q`

# Verification
- Tests: `python -m pytest tests/ -x -q`
- Client tests: `python -m pytest provisa-client/tests/ -x -q`
- Server: `uvicorn main:app --reload`

# Module Boundaries
Define as project grows.

# Swarm Mode & Teammate Spawning
@.claude/refs/swarm-mode.md