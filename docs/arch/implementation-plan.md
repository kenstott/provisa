# Plan: Provisa Core Engine — Full Implementation

## Context

Build Provisa end-to-end: config-driven Trino catalog creation, GraphQL schema generation, query compilation, execution with smart routing, security enforcement, persisted query registry, output formats, admin API, and UI. Priority: get queries flowing through Trino first, then layer in routing, security, governance, and UI.

## Architecture

```
config.yaml → Config Loader → PostgreSQL (config DB)
                                    ↓
                    Trino Dynamic Catalog API (auto-create catalogs)
                                    ↓
Trino INFORMATION_SCHEMA → Schema Generator → GraphQL SDL (per role)
                                                    ↓
                           GraphQL Query → Compiler → PG-style SQL
                                                         ↓
                                              SQLGlot → Target dialect SQL
                                                         ↓
                                         Router → Trino or Direct RDBMS
                                                         ↓
                                              Executor → Result rows
                                                         ↓
                                              Serializer → JSON / NDJSON / Parquet / Arrow
```

## Requirement → Phase Map

Every REQ is assigned to a phase. Cross-cutting requirements (REQ-064, REQ-065, REQ-069–071) apply to all phases.

| Phase | Requirements |
|-------|-------------|
| A: Infrastructure | REQ-055, REQ-057 |
| B: Config & Auto-Catalog | REQ-012, REQ-013, REQ-014, REQ-015, REQ-019, REQ-041, REQ-054, REQ-067, REQ-075 |
| C: Schema Generation | REQ-007, REQ-008, REQ-010, REQ-011, REQ-016, REQ-018, REQ-021 |
| D: Query Compilation & Trino Execution | REQ-009, REQ-043, REQ-047, REQ-066 |
| E: Routing & Direct Execution | REQ-027, REQ-028, REQ-030, REQ-052, REQ-068 |
| F: Security Layers | REQ-038, REQ-039, REQ-040, REQ-041, REQ-042 |
| G: Mutations | REQ-031, REQ-032, REQ-033, REQ-034, REQ-035, REQ-036, REQ-037 |
| H: Persisted Query Registry | REQ-001, REQ-002, REQ-003, REQ-004, REQ-005, REQ-006, REQ-015, REQ-020, REQ-022, REQ-023, REQ-024, REQ-025, REQ-026, REQ-046 |
| I: Output Formats & Arrow Flight | REQ-045, REQ-048, REQ-049, REQ-050, REQ-051, REQ-126 |
| J: Large Result Redirect | REQ-006, REQ-029, REQ-044 |
| K: Admin API (Strawberry) | REQ-059, REQ-060 |
| L: Connection Pooling Hardening | REQ-052, REQ-053 |
| M: Production Infrastructure | REQ-056 |
| N: UI | REQ-058, REQ-059, REQ-060, REQ-061, REQ-062, REQ-063, REQ-076 |
| O: Query Result Caching | REQ-077, REQ-078, REQ-079, REQ-080 |
| P: Materialized View Optimization | REQ-081, REQ-082, REQ-083, REQ-084, REQ-085, REQ-086 |
| Q: Column-Level Masking | REQ-087, REQ-088, REQ-089, REQ-090, REQ-091 |
| R: LLM Relationship Discovery | REQ-092, REQ-093, REQ-094, REQ-095, REQ-096 |
| S: gRPC Query Endpoint | REQ-097, REQ-098, REQ-099, REQ-100 |
| T: Documentation | REQ-101, REQ-102, REQ-103 |
| U: API Sources (REST, GraphQL, gRPC) | REQ-104, REQ-105, REQ-106, REQ-107, REQ-108, REQ-109, REQ-110, REQ-111, REQ-112, REQ-113, REQ-119 |
| V: Kafka Sources & Sink | REQ-114, REQ-115, REQ-116, REQ-117 |
| W: Authentication | REQ-120, REQ-121, REQ-122, REQ-123, REQ-124, REQ-125 |
| AA: Quick Wins | REQ-212, REQ-213, REQ-214, REQ-215, REQ-216, REQ-217, REQ-229 |
| AB: Medium-Complexity Parity | REQ-218, REQ-219, REQ-220, REQ-221, REQ-222, REQ-256, REQ-257, REQ-258, REQ-261 (REQ-260 moved to AM) |
| AC: Tracked Functions & Webhooks | REQ-205, REQ-206, REQ-207, REQ-208, REQ-209, REQ-210, REQ-211, REQ-242, REQ-243, REQ-244, REQ-245 |
| AD: Schema Alignment | REQ-194, REQ-195, REQ-196, REQ-197, REQ-198, REQ-199, REQ-200, REQ-201, REQ-202, REQ-230, REQ-231, REQ-232, REQ-233, REQ-234, REQ-235, REQ-236, REQ-237, REQ-238, REQ-239, REQ-240, REQ-241 |
| AE: ABAC Approval Hook | REQ-203, REQ-204, REQ-246, REQ-247 |
| AF: Installer & Packaging | REQ-223, REQ-224, REQ-225, REQ-226, REQ-227, REQ-228, REQ-294 |
| X: JDBC Arrow Flight Transport | REQ-293 |
| AG: Hasura v2 Converter | REQ-182, REQ-184, REQ-185, REQ-186, REQ-187, REQ-188, REQ-190, REQ-192, REQ-193 |
| AH: DDN Converter | REQ-183, REQ-189, REQ-191 |
| AI: NoSQL Source Mapping | REQ-250, REQ-251, REQ-252, REQ-253 |
| AJ: Apollo Federation | REQ-259 |
| AK: Two-Stage Compiler & Multi-Protocol Clients | REQ-262, REQ-263, REQ-264, REQ-265, REQ-266, REQ-267, REQ-268, REQ-269, REQ-270, REQ-271, REQ-272, REQ-273, REQ-274 |
| AL: Federation Performance | REQ-275, REQ-276, REQ-277, REQ-278, REQ-279, REQ-280, REQ-281 |
| AM: Live Query Engine | REQ-260, REQ-282, REQ-283, REQ-284, REQ-285, REQ-286, REQ-287 |
| AN: Automatic Persisted Queries (APQ) | REQ-288, REQ-289, REQ-290, REQ-291, REQ-292 |
| AO: Query-API Sources (Neo4j & SPARQL) | REQ-295, REQ-296, REQ-297, REQ-298, REQ-299 |
| Dropped | REQ-017 (NoSQL Parquet materialization — Trino native connectors handle this) |
| Not implementation | REQ-072, REQ-073, REQ-074 (commercial positioning) |

---

## Phase A: Infrastructure
**Goal:** PG + Trino running in Docker Compose, Trino can query PG.
**REQs:** REQ-055, REQ-057

**Build:**
- `docker-compose.yml` — PG + Trino only (dev mode: Provisa runs locally with native debugger)
- `docker-compose.prod.yml` — override that adds Provisa container (CI, production, full-stack testing)
- `trino/catalog/postgresql.properties` — PG connector pointing at compose PG
- `trino/etc/config.properties`, `jvm.config`, `node.properties` — Trino coordinator config
- `db/init.sql` — seed PG with demo schema (orders, customers, products tables with sample data)
- `pyproject.toml` — all dependencies, pytest markers config
- `Dockerfile` — Provisa container (stateless per REQ-057)
- Test infrastructure: `tests/conftest.py`, `tests/fixtures/sample_config.yaml`, fixture factory modules

**Usage:**
```bash
docker compose up                              # dev: PG + Trino only, run Provisa locally
docker compose -f docker-compose.yml -f docker-compose.prod.yml up  # full stack in Docker
```

**Verify:**
- `docker compose up` — PG + Trino healthy, ports exposed to localhost
- `python -m pytest tests/integration/test_infra.py -x -q` — PG connects, sample data exists, Trino queries PG, Trino INFORMATION_SCHEMA returns column metadata

**Files:**
| File | Action |
|---|---|
| `docker-compose.yml` | Create (PG + Trino, dev mode) |
| `docker-compose.prod.yml` | Create (adds Provisa container) |
| `Dockerfile` | Create |
| `trino/catalog/postgresql.properties` | Create |
| `trino/etc/config.properties` | Create |
| `trino/etc/jvm.config` | Create |
| `trino/etc/node.properties` | Create |
| `db/init.sql` | Create |
| `pyproject.toml` | Create |
| `tests/__init__.py` | Create |
| `tests/conftest.py` | Create |
| `tests/unit/__init__.py` | Create |
| `tests/integration/__init__.py` | Create |
| `tests/e2e/__init__.py` | Create |
| `tests/fixtures/__init__.py` | Create |
| `tests/fixtures/sample_config.yaml` | Create |
| `tests/fixtures/registration_model.py` | Create |
| `tests/fixtures/trino_metadata.py` | Create |
| `tests/fixtures/graphql_queries.py` | Create |
| `tests/fixtures/sql_results.py` | Create |
| `tests/integration/test_infra.py` | Create |

---

## Phase B: Config & Auto-Catalog
**Goal:** YAML config consumed into PG. Source registration auto-creates Trino catalogs via dynamic catalog API. Tables only queryable after explicit registration.
**REQs:** REQ-012, REQ-013, REQ-014, REQ-015, REQ-019, REQ-041, REQ-054, REQ-067

**Build:**
- `provisa/core/models.py` — Pydantic models: Source, Domain, Table, Column, Relationship, Role, RLSRule, NamingRule
- `provisa/core/schema.sql` — PG tables mirroring models (sources, domains, tables, table_columns, relationships, roles, rls_rules, naming_rules)
- `provisa/core/db.py` — asyncpg connection pool factory
- `provisa/core/config_loader.py` — read YAML, upsert into PG, validate referential integrity, resolve secrets via provider
- `provisa/core/secrets.py` — pluggable secrets provider interface. V1: env var provider. Interface supports future Vault, K8s secrets, AWS Secrets Manager (REQ-075)
- `provisa/core/catalog.py` — Trino dynamic catalog API client: POST/DELETE catalogs on source add/remove (REQ-012). Catalog name = source id. No restart required.
- `provisa/core/repositories/` — CRUD repositories: source, domain, table, relationship, role, rls
- `config/provisa.yaml` — example config with demo PG source, domains, tables, roles, RLS rules, naming rules

**Config YAML structure:**
```yaml
sources:
  - id: sales-pg
    type: postgresql
    host: postgres          # docker compose service name
    port: 5432
    database: provisa
    username: provisa
    password: ${env:PG_PASSWORD}    # pluggable: ${env:VAR}, ${vault:path/key}, ${k8s:secret/key}

domains:
  - id: sales-analytics
    description: Sales operational and analytical data

naming:
  rules:
    - pattern: "^prod_pg_"
      replace: ""

tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    governance: pre-approved    # REQ-015
    columns:
      - name: id
        visible_to: [admin, analyst]
      - name: customer_id
        visible_to: [admin, analyst]
      - name: amount
        visible_to: [admin]

relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one    # REQ-019

roles:
  - id: admin
    capabilities: [source_registration, table_registration, relationship_registration,
                    security_config, query_development, query_approval, admin]
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]

rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"  # REQ-041
```

**Key behaviors:**
- Source add → validate connection → call Trino catalog API → catalog available in seconds (REQ-012)
- Source add does NOT expose data — tables must be explicitly registered (REQ-013, REQ-014)
- Config loader is idempotent (safe to re-run)
- Secrets resolved via pluggable provider: env vars initially, extensible to Vault, K8s secrets, etc. (REQ-075). Never stored in DB or config DB.
- Target dialect stored per source (REQ-067)
- Single persistent Trino connection maintained (REQ-054)

**Verify:**
- `python -m pytest tests/unit/test_models.py tests/unit/test_secrets.py -x -q` — Pydantic validation, env var resolution
- `python -m pytest tests/integration/test_config_loader.py -x -q` — YAML → PG round-trips correctly, idempotent
- `python -m pytest tests/integration/test_repositories.py -x -q` — CRUD operations against real PG
- `python -m pytest tests/integration/test_catalog.py -x -q` — Trino catalog created/deleted via API, INFORMATION_SCHEMA accessible

**Files:**
| File | Action |
|---|---|
| `provisa/__init__.py` | Create |
| `provisa/core/__init__.py` | Create |
| `provisa/core/models.py` | Create |
| `provisa/core/schema.sql` | Create |
| `provisa/core/db.py` | Create |
| `provisa/core/config_loader.py` | Create |
| `provisa/core/secrets.py` | Create |
| `provisa/core/catalog.py` | Create |
| `provisa/core/repositories/__init__.py` | Create |
| `provisa/core/repositories/source.py` | Create |
| `provisa/core/repositories/domain.py` | Create |
| `provisa/core/repositories/table.py` | Create |
| `provisa/core/repositories/relationship.py` | Create |
| `provisa/core/repositories/role.py` | Create |
| `provisa/core/repositories/rls.py` | Create |
| `config/provisa.yaml` | Create |
| `tests/unit/test_models.py` | Create |
| `tests/unit/test_secrets.py` | Create |
| `tests/integration/test_config_loader.py` | Create |
| `tests/integration/test_repositories.py` | Create |
| `tests/integration/test_catalog.py` | Create |

---

## Phase C: Schema Generation
**Goal:** Read Trino catalog + registration model → produce valid `graphql-core` schema per role.
**REQs:** REQ-007, REQ-008, REQ-010, REQ-011, REQ-016, REQ-018, REQ-021

**Build:**
- `provisa/compiler/__init__.py`
- `provisa/compiler/introspect.py` — query Trino `INFORMATION_SCHEMA.COLUMNS` for registered tables; query `TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE` for FK inference (REQ-018)
- `provisa/compiler/type_map.py` — Trino type → GraphQL scalar mapping (REQ-010): VARCHAR→String, INTEGER→Int, BOOLEAN→Boolean, TIMESTAMP→DateTime, JSONB→JSON. Nullability preserved.
- `provisa/compiler/schema_gen.py` — builds `graphql-core` schema object from registration model + introspected metadata:
  - Domain-scoped: role sees only tables in accessible domains (REQ-021)
  - Per-role column filtering (REQ-008, REQ-039)
  - Relationship fields from registered relationships (many-to-one → object, one-to-many → list)
  - Root query fields with `where` (typed filter inputs), `order_by`, `limit`, `offset` arguments
  - Auto-generated shortest unique names within domain + regex rules + alias override
  - No third-party GraphQL framework (REQ-007) — uses `graphql-core` directly
- `provisa/compiler/naming.py` — GraphQL name generation: shortest unique within domain → regex rules → alias override

**Key behaviors:**
- Schema generation triggered on table publication (REQ-016)
- Schema cached per role; invalidated on registration model change
- Unregistered tables/columns/relationships rejected at parse time with precise errors (REQ-011)
- FK candidates surfaced for steward confirmation (REQ-018)

**Verify:**
- `python -m pytest tests/unit/test_type_map.py -x -q` — all Trino types map correctly
- `python -m pytest tests/unit/test_naming.py -x -q` — naming edge cases (conflicts, regex transforms, aliases)
- `python -m pytest tests/integration/test_schema_gen.py -x -q`:
  - Given registration model → valid `graphql-core` schema (validates without errors)
  - Object types per registered table with correct fields
  - Relationship fields present with correct types
  - Root query fields with filter/pagination args
  - Different roles see different fields (column visibility)
  - Cross-domain relationships visible only if role has access to both domains
  - Naming: auto-generated, regex rules applied, aliases win
- `python -m pytest tests/integration/test_introspect.py -x -q` — real Trino INFORMATION_SCHEMA queries return expected metadata

**Files:**
| File | Action |
|---|---|
| `provisa/compiler/__init__.py` | Create |
| `provisa/compiler/introspect.py` | Create |
| `provisa/compiler/type_map.py` | Create |
| `provisa/compiler/schema_gen.py` | Create |
| `provisa/compiler/naming.py` | Create |
| `tests/unit/test_type_map.py` | Create |
| `tests/unit/test_naming.py` | Create |
| `tests/integration/test_schema_gen.py` | Create |
| `tests/integration/test_introspect.py` | Create |

---

## Phase D: Query Compilation & Trino Execution
**Goal:** Parse GraphQL → compile to PG SQL → transpile to Trino SQL → execute via Trino → return JSON response. Everything routes through Trino initially.
**REQs:** REQ-009, REQ-043, REQ-047, REQ-066

**Build:**
- `provisa/compiler/parser.py` — parse + validate GraphQL operation against generated schema via `graphql-core`
- `provisa/compiler/sql_gen.py` — walk validated AST, emit PG-style SQL (REQ-009, REQ-066):
  - Field selection → `SELECT` projection
  - `where` args → `WHERE` clause
  - `order_by` → `ORDER BY`
  - `limit`/`offset` → `LIMIT`/`OFFSET`
  - Nested many-to-one → `LEFT JOIN`
  - Nested one-to-many → subquery or lateral join
  - Fragment spreads → inline field expansion
  - Single SQL statement, no resolver chain, no N+1
- `provisa/compiler/params.py` — GraphQL variables → parameterized SQL (`$1`, `$2`). Never interpolates values.
- `provisa/transpiler/__init__.py`
- `provisa/transpiler/transpile.py` — SQLGlot PG SQL → Trino SQL (REQ-066). Initial phase: all queries go to Trino.
- `provisa/executor/__init__.py`
- `provisa/executor/trino.py` — execute transpiled SQL via `trino` Python client. Returns rows + column metadata.
- `provisa/executor/serialize.py` — reconstruct nested GraphQL JSON from JOIN result rows (REQ-047):
  - many-to-one → single nested object
  - one-to-many → array of nested objects
  - Null propagation for nullable relationships
  - Output: `{"data": {...}}`
- `provisa/api/__init__.py`
- `provisa/api/app.py` — FastAPI app factory, startup hooks (config load, schema gen), `/health` endpoint
- `provisa/api/data/__init__.py`
- `provisa/api/data/endpoint.py` — `/data/graphql` endpoint (REQ-043). Pipeline: parse → compile → transpile → execute → serialize. Test mode initially (arbitrary queries).
- `main.py` — updated entry point

**Verify:**
- `python -m pytest tests/unit/test_sql_gen.py -x -q` — fixture-based AST→SQL pairs:
  - Simple selection → `SELECT col1, col2 FROM table`
  - Where → `WHERE col = $1`
  - Nested relationship → `LEFT JOIN` with correct keys
  - Pagination → `LIMIT`/`OFFSET`
  - Variables → parameterized (never interpolated)
- `python -m pytest tests/unit/test_params.py -x -q` — variable binding
- `python -m pytest tests/unit/test_serialize.py -x -q` — JOIN rows → nested JSON from fixture data
- `python -m pytest tests/integration/test_transpile.py -x -q` — PG SQL → Trino SQL via SQLGlot
- `python -m pytest tests/e2e/test_query_pipeline.py -x -q`:
  - POST /data/graphql with field selection → correct JSON
  - POST with `where` filter → filtered results
  - POST with nested relationship → JOINed data
  - GET /health → OK

**Files:**
| File | Action |
|---|---|
| `provisa/compiler/parser.py` | Create |
| `provisa/compiler/sql_gen.py` | Create |
| `provisa/compiler/params.py` | Create |
| `provisa/transpiler/__init__.py` | Create |
| `provisa/transpiler/transpile.py` | Create |
| `provisa/executor/__init__.py` | Create |
| `provisa/executor/trino.py` | Create |
| `provisa/executor/serialize.py` | Create |
| `provisa/api/__init__.py` | Create |
| `provisa/api/app.py` | Create |
| `provisa/api/data/__init__.py` | Create |
| `provisa/api/data/endpoint.py` | Create |
| `main.py` | Modify |
| `tests/unit/test_sql_gen.py` | Create |
| `tests/unit/test_params.py` | Create |
| `tests/unit/test_serialize.py` | Create |
| `tests/integration/test_transpile.py` | Create |
| `tests/e2e/test_query_pipeline.py` | Create |

---

## Phase E: Routing & Direct Execution
**Goal:** Single-source queries bypass Trino and go direct to RDBMS. Multi-source queries route to Trino. SQLGlot transpiles to target dialect.
**REQs:** REQ-027, REQ-028, REQ-030, REQ-052, REQ-068

**Build:**
- `provisa/transpiler/router.py` — inspect compiled SQL metadata (source set from compilation context), decide route:
  - Single RDBMS source + direct connection driver available → direct RDBMS via SQLGlot transpilation (REQ-027, target: sub-100ms to low hundreds)
  - Single NoSQL source → always Trino (NoSQL sources don't support SQL; Trino connector is the only path)
  - Single RDBMS source without direct driver → Trino
  - Multiple sources → Trino (REQ-028, target: 300-500ms)
  - Steward override hint respected (REQ-030)
- `provisa/executor/direct.py` — execute transpiled SQL against source via asyncpg (PostgreSQL) or appropriate async driver
- `provisa/executor/pool.py` — warm connection pool per registered RDBMS source, configurable min/max (REQ-052)
- Update `provisa/transpiler/transpile.py` — add all target dialects: PostgreSQL, MySQL, SQL Server, DuckDB, Snowflake, BigQuery (REQ-068)
- Update `provisa/api/data/endpoint.py` — integrate router into pipeline

**Verify:**
- `python -m pytest tests/unit/test_router.py -x -q`:
  - Single RDBMS source with direct driver → route direct
  - Single NoSQL source → route Trino (no SQL support, Trino connector only path)
  - Single RDBMS source without direct driver → route Trino
  - Multi source → route Trino
  - Steward override → respects hint
- `python -m pytest tests/unit/test_pool.py -x -q` — pool creates/destroys correctly, configurable sizes
- `python -m pytest tests/integration/test_transpile.py -x -q` — PG SQL → MySQL, MSSQL, Snowflake, BigQuery, DuckDB all correct
- `python -m pytest tests/integration/test_direct_exec.py -x -q` — direct PG execution returns correct results
- `python -m pytest tests/e2e/test_routing.py -x -q` — single-source query routes direct, multi-source routes Trino (verify via response timing/logs)

**Files:**
| File | Action |
|---|---|
| `provisa/transpiler/router.py` | Create |
| `provisa/executor/direct.py` | Create |
| `provisa/executor/pool.py` | Create |
| `provisa/transpiler/transpile.py` | Modify (add dialects) |
| `provisa/api/data/endpoint.py` | Modify (integrate router) |
| `tests/unit/test_router.py` | Create |
| `tests/unit/test_pool.py` | Create |
| `tests/integration/test_transpile.py` | Modify (add dialect tests) |
| `tests/integration/test_direct_exec.py` | Create |
| `tests/e2e/test_routing.py` | Create |

---

## Phase F: Security Layers
**Goal:** Three independent enforcement layers: pre-approval (Phase H), schema visibility, SQL enforcement.
**REQs:** REQ-038, REQ-039, REQ-040, REQ-041, REQ-042

**Build:**
- `provisa/compiler/rls.py` — after SQL compilation, inject RLS WHERE clauses per role (REQ-040, REQ-041). Strip unauthorized columns before execution. Applied every request.
- `provisa/security/__init__.py`
- `provisa/security/visibility.py` — schema visibility enforcement: unauthorized tables/columns do not appear in SDL (REQ-039). Already partially in schema_gen; this formalizes and hardens it.
- `provisa/security/rights.py` — distinct rights model (REQ-042): source_registration, table_registration, relationship_definition, security_config, query_development, query_authorization, query_execution — independently configured per role.
- Update `provisa/compiler/schema_gen.py` — integrate visibility layer
- Update `provisa/api/data/endpoint.py` — inject RLS into pipeline, enforce rights checks

**Verify:**
- `python -m pytest tests/unit/test_rls.py -x -q`:
  - RLS WHERE clause injected for role with filter
  - No RLS for role without filter
  - Multiple RLS rules combined correctly
  - Columns stripped when not visible to role
- `python -m pytest tests/unit/test_visibility.py -x -q`:
  - Unauthorized table not in schema
  - Unauthorized column not in schema
  - Compiler rejects reference to invisible table/column
- `python -m pytest tests/unit/test_rights.py -x -q`:
  - Each right independently checked
  - Missing right → rejection with clear error
- `python -m pytest tests/e2e/test_security.py -x -q` — query as analyst role → RLS applied, restricted columns invisible, forbidden operations rejected

**Files:**
| File | Action |
|---|---|
| `provisa/compiler/rls.py` | Create |
| `provisa/security/__init__.py` | Create |
| `provisa/security/visibility.py` | Create |
| `provisa/security/rights.py` | Create |
| `provisa/compiler/schema_gen.py` | Modify |
| `provisa/api/data/endpoint.py` | Modify |
| `tests/unit/test_rls.py` | Create |
| `tests/unit/test_visibility.py` | Create |
| `tests/unit/test_rights.py` | Create |
| `tests/e2e/test_security.py` | Create |

---

## Phase G: Mutations
**Goal:** INSERT/UPDATE/DELETE via GraphQL, always direct RDBMS, never Trino.
**REQs:** REQ-031, REQ-032, REQ-033, REQ-034, REQ-035, REQ-036, REQ-037

**Build:**
- Update `provisa/compiler/schema_gen.py` — generate mutation types (`insert_<table>`, `update_<table>`, `delete_<table>`) for registered RDBMS tables only (REQ-036). Input types reflect only columns user's role is permitted to write (REQ-034). No mutations for NoSQL sources (REQ-037).
- Update `provisa/compiler/sql_gen.py` — compile mutation GraphQL → INSERT/UPDATE/DELETE SQL
- Update `provisa/compiler/rls.py` — inject RLS WHERE into UPDATE/DELETE (REQ-035)
- Update `provisa/transpiler/router.py` — mutations always route direct (REQ-031, REQ-032)
- Update `provisa/executor/direct.py` — handle mutation execution, enforce write rights (REQ-033)

**Verify:**
- `python -m pytest tests/unit/test_mutation_sql.py -x -q` — mutation AST → INSERT/UPDATE/DELETE SQL
- `python -m pytest tests/e2e/test_mutations.py -x -q`:
  - INSERT via GraphQL → row created in PG
  - UPDATE via GraphQL → row updated, RLS filter applied
  - DELETE via GraphQL → row deleted, RLS filter applied
  - Mutation attempt on NoSQL source → rejected
  - Mutation on column user can't write → rejected at parse time
  - Cross-source mutation → rejected (REQ-037)

**Files:**
| File | Action |
|---|---|
| `provisa/compiler/schema_gen.py` | Modify |
| `provisa/compiler/sql_gen.py` | Modify |
| `provisa/compiler/rls.py` | Modify |
| `provisa/transpiler/router.py` | Modify |
| `provisa/executor/direct.py` | Modify |
| `tests/unit/test_mutation_sql.py` | Create |
| `tests/e2e/test_mutations.py` | Create |

---

## Phase H: Persisted Query Registry & Pre-Approval
**Goal:** Production governance — registry-required tables need approved queries; pre-approved tables need only user rights. Test endpoint gated from production.
**REQs:** REQ-001, REQ-002, REQ-003, REQ-004, REQ-005, REQ-006, REQ-015, REQ-020, REQ-022, REQ-023, REQ-024, REQ-025, REQ-026, REQ-046

**Build:**
- `provisa/registry/__init__.py`
- `provisa/registry/store.py` — persisted query storage in PG:
  - Submission: full query text, compiled SQL, target tables, parameter schema, permitted output types, developer identity (REQ-022)
  - Stable identifier on approval (REQ-023)
  - Record: who defined, who approved, when, output types, routing hint, registration model version (REQ-024)
  - Deprecation with replacement pointer (REQ-026) — deprecated queries return clear error directing to replacement
- `provisa/registry/approval.py` — approval workflow:
  - Registration changes flag affected entries for re-review (REQ-025)
  - Relationship changes flag entries using affected join fields (REQ-020)
- `provisa/registry/ceiling.py` — approved query ceiling enforcement (REQ-005): clients may restrict within (fewer columns, additional filters) but cannot exceed
- `provisa/registry/governance.py` — production gate:
  - Registry-required tables: query must match approved registry entry (REQ-001)
  - Pre-approved tables: user rights only, no registry needed (REQ-003)
  - Platform-level enforcement, no privilege override (REQ-002)
  - Pre-approved tables cannot use large result redirect or Arrow (REQ-006)
  - Output type must match approved ceiling (REQ-046)
- Update DB schema: `persisted_queries` table, `approval_log` table
- Update `provisa/api/data/endpoint.py`:
  - Test mode: arbitrary queries with full guards, environment-gated (REQ-004)
  - Production mode: registry validation before execution
  - Query text never transmitted in production — only stable ID (REQ-023)

**Verify:**
- `python -m pytest tests/unit/test_ceiling.py -x -q` — ceiling logic (restrict within, reject exceed)
- `python -m pytest tests/unit/test_governance.py -x -q` — governance mode routing logic
- `python -m pytest tests/integration/test_registry.py -x -q` — store/retrieve/approve/deprecate in PG
- `python -m pytest tests/e2e/test_registry_flow.py -x -q`:
  - Submit query → stored with metadata → approve → stable ID → execute in production
  - Raw query against registry-required table → rejected (REQ-001)
  - Pre-approved table → executes without registry (REQ-003)
  - Client exceeding ceiling → rejected
  - Schema change → affected entries flagged
  - Deprecated query → error with replacement pointer
  - Test endpoint disabled in production env (REQ-004)

**Files:**
| File | Action |
|---|---|
| `provisa/registry/__init__.py` | Create |
| `provisa/registry/store.py` | Create |
| `provisa/registry/approval.py` | Create |
| `provisa/registry/ceiling.py` | Create |
| `provisa/registry/governance.py` | Create |
| `provisa/core/schema.sql` | Modify (add tables) |
| `provisa/api/data/endpoint.py` | Modify |
| `tests/unit/test_ceiling.py` | Create |
| `tests/unit/test_governance.py` | Create |
| `tests/integration/test_registry.py` | Create |
| `tests/e2e/test_registry_flow.py` | Create |

---

## Phase I: Output Formats & Arrow Flight
**Goal:** Multiple output formats beyond JSON. gRPC Arrow Flight for high-throughput. JDBC/Flight SQL connector modes for external tool integration.
**REQs:** REQ-045, REQ-048, REQ-049, REQ-050, REQ-051, REQ-126

**Build:**
- `provisa/executor/formats/__init__.py`
- `provisa/executor/formats/ndjson.py` — NDJSON streaming: one JSON object per line (REQ-048)
- `provisa/executor/formats/tabular.py` — normalized (relational tables with FKs, Parquet/CSV) and denormalized (fully flattened, Parquet/CSV) (REQ-049, REQ-050)
- `provisa/executor/formats/arrow.py` — Arrow buffer serialization (REQ-051)
- `provisa/api/flight/__init__.py`
- `provisa/api/flight/server.py` — gRPC Arrow Flight endpoint (REQ-045). Trino produces Arrow natively for zero-copy delivery.
- Update `provisa/api/data/endpoint.py` — content negotiation for output format
- Flight SQL connector mode parameter (REQ-126):
  - `mode=catalog` — Metadata-only view of the user's visible semantic layer. Domains exposed as JDBC schemas, registered tables/views as JDBC tables, columns with types and descriptions. External tools (e.g. reasoning agents) connect via standard JDBC, introspect `information_schema`, and see the full governed catalog scoped to the authenticated user's role. No query execution — purely for schema discovery and query planning.
  - `mode=approved` — Only persisted approved queries are exposed as available tables. Each approved query appears as a virtual table whose columns match the query's output schema. This is the runtime execution interface for tools that should only run sanctioned queries.
  - Default (no mode) — Current behavior: full query execution through the governance pipeline.
  - The mode is passed as a connection property in the Flight SQL handshake or JDBC connection string (e.g. `jdbc:arrow-flight-sql://host:8815?mode=catalog&role=analyst`).

**Verify:**
- `python -m pytest tests/unit/test_formats.py -x -q` — serialization logic for each format
- `python -m pytest tests/e2e/test_output_formats.py -x -q`:
  - Accept: application/x-ndjson → NDJSON response
  - Normalized tabular → Parquet with FK relationships
  - Denormalized tabular → single flat Parquet
  - Arrow Flight client → receives Arrow buffers
- `python -m pytest tests/unit/test_flight_modes.py -x -q`:
  - `mode=catalog` — `information_schema.schemata` returns domains visible to role, `information_schema.tables` returns registered tables within those domains, `information_schema.columns` returns column metadata with descriptions
  - `mode=approved` — only approved persisted queries appear as tables
  - Default mode — queries execute normally

**Files:**
| File | Action |
|---|---|
| `provisa/executor/formats/__init__.py` | Create |
| `provisa/executor/formats/ndjson.py` | Create |
| `provisa/executor/formats/tabular.py` | Create |
| `provisa/executor/formats/arrow.py` | Create |
| `provisa/api/flight/__init__.py` | Create |
| `provisa/api/flight/server.py` | Create/Modify |
| `provisa/api/flight/catalog.py` | Create — virtual information_schema for catalog mode |
| `provisa/api/data/endpoint.py` | Modify |
| `tests/unit/test_formats.py` | Create |
| `tests/unit/test_flight_modes.py` | Create |
| `tests/e2e/test_output_formats.py` | Create |

---

## Phase J: Large Result Redirect
**Goal:** Results above threshold redirect to blob storage with presigned URL.
**REQs:** REQ-006, REQ-029, REQ-044

**Build:**
- `provisa/executor/redirect.py` — threshold check, upload to blob storage (S3-compatible), return presigned URL with TTL (REQ-029, REQ-044)
- Update pipeline — after execution, check result size → inline response or redirect
- Enforce: pre-approved table queries cannot use redirect (REQ-006)

**Verify:**
- `python -m pytest tests/unit/test_redirect.py -x -q` — threshold logic, pre-approved table restriction
- `python -m pytest tests/integration/test_blob_upload.py -x -q` — S3 upload + presign round-trip
- `python -m pytest tests/e2e/test_large_result.py -x -q`:
  - Small result → inline JSON
  - Large result → presigned URL returned, accessible within TTL
  - Pre-approved table large result → no redirect

**Files:**
| File | Action |
|---|---|
| `provisa/executor/redirect.py` | Create |
| `provisa/api/data/endpoint.py` | Modify |
| `tests/unit/test_redirect.py` | Create |
| `tests/integration/test_blob_upload.py` | Create |
| `tests/e2e/test_large_result.py` | Create |
| `tests/fixtures/large_result_data.sql` | Create |

---

## Phase K: Admin API (Strawberry)
**Goal:** GraphQL CRUD surface for managing config — sources, tables, relationships, roles, RLS rules.
**REQs:** REQ-059, REQ-060

**Build:**
- `provisa/api/admin/__init__.py`
- `provisa/api/admin/types.py` — Strawberry types mirroring Pydantic models
- `provisa/api/admin/schema.py` — queries + mutations for all config entities
- Mount at `/admin/graphql`
- Schema regeneration triggered after table/relationship changes

**Verify:**
- `python -m pytest tests/integration/test_admin_api.py -x -q` — CRUD operations via Strawberry against real PG
- `python -m pytest tests/e2e/test_admin_flow.py -x -q`:
  - Create source → register table → data schema regenerated with new type
  - Update/delete source → cascading effects
  - GraphiQL explorer accessible

**Files:**
| File | Action |
|---|---|
| `provisa/api/admin/__init__.py` | Create |
| `provisa/api/admin/types.py` | Create |
| `provisa/api/admin/schema.py` | Create |
| `provisa/api/app.py` | Modify (mount admin) |
| `tests/integration/test_admin_api.py` | Create |
| `tests/e2e/test_admin_flow.py` | Create |

---

## Phase L: Connection Pooling Hardening
**Goal:** PgBouncer for PostgreSQL sources, driver-level pooling for others.
**REQs:** REQ-052, REQ-053

**Build:**
- Add PgBouncer container to Docker Compose for PG sources (REQ-053)
- Configure driver-level pooling for non-PG RDBMS
- Update `provisa/executor/pool.py` — route PG connections through PgBouncer

**Verify:**
- PG queries route through PgBouncer
- Non-PG queries use driver-level pool
- Pool sizing configurable per source

---

## Phase M: Production Infrastructure
**Goal:** Helm chart for production Kubernetes deployment.
**REQs:** REQ-056

**Build:**
- `helm/` — Helm chart: Provisa deployment, Trino StatefulSet with configurable workers, HPA autoscaling, resource groups
- Horizontal Trino worker scaling
- Ingress, service, configmaps

**Verify:**
- `helm install provisa ./helm` on K8s cluster → all pods running
- Queries execute correctly in K8s environment
- Worker scaling responds to load

---

## Phase N: UI
**Goal:** React-based UI with role-driven rendering. Interactive schema visualization via GraphQL Voyager.
**REQs:** REQ-058, REQ-059, REQ-060, REQ-061, REQ-062, REQ-063, REQ-076

**Build:**
- `provisa-ui/` — React app (Vite + TypeScript)
- Role composition system: rendered surface determined by assembled role set (REQ-058, REQ-059)
- Capability-driven views: Source Registration, Table Registration, Relationship Registration, Security Config, Query Development, Query Approval, Admin (REQ-060)
- Confirmation dialogs for destructive actions with consequence summary (REQ-061)
- Test endpoint UI: shows RLS filters applied, columns excluded, schema scope in result metadata (REQ-062)
- Approval queue: steward-optimized, rejection requires specific actionable reason (REQ-063)
- **GraphQL Voyager integration** (REQ-076):
  - `provisa/api/data/sdl.py` — `/data/sdl` endpoint: returns SDL string for the requesting role's schema via `graphql-core` `print_schema`. Role-aware: each role sees their own schema graph.
  - `provisa-ui/src/pages/SchemaExplorer.tsx` — page embedding `graphql-voyager` React component, pointed at `/data/sdl`
  - Interactive graph visualization of tables, relationships, fields, types per role
  - Accessible from the Query Development capability view

**Verify:**
- `npx playwright test` — Playwright E2E tests:
  - Login with different roles → different UI surfaces rendered
  - Source registration workflow: add source → register table → appears in query builder
  - Query builder → test execution → shows RLS metadata → submit for approval → approve/reject
  - Destructive action → confirmation dialog shown with consequence summary
  - Approval queue: list pending, approve with comment, reject with actionable reason
  - Schema Explorer: Voyager renders interactive graph, role switch changes visible schema

**GraphQL Voyager (REQ-076):**
- Embedded via iframe with React 18 CDN standalone bundle. This avoids MUI v5/React 19 incompatibility and works reliably.
- No fork planned — iframe approach is the accepted solution.

**Files (additions for Voyager):**
| File | Action |
|---|---|
| `provisa/api/data/sdl.py` | Create |
| `provisa/api/app.py` | Modify (mount SDL endpoint, CORS middleware) |
| `provisa-ui/src/pages/SchemaExplorer.tsx` | Create (iframe with CDN bundle) |
| `provisa-ui/src/pages/QueryPage.tsx` | Create (GraphiQL native component) |
| `provisa-ui/package.json` | Modify (add graphiql) |
| `tests/e2e/test_sdl.py` | Create |
| `tests/e2e/test_ui_crud.py` | Create |

---

## Phase O: Query Result Caching
**Goal:** Redis-backed application-layer cache for query results. Steward-controlled TTL per approved query. Security-partitioned by role + RLS context.
**REQs:** REQ-077, REQ-078, REQ-079, REQ-080

**Build:**
- `provisa/cache/__init__.py`
- `provisa/cache/key.py` — cache key generation: `hash(query_id_or_sql, params, role_id, rls_context_values)`. RLS context values extracted from the active security context. Missing any component is a security defect.
- `provisa/cache/store.py` — Redis-backed cache store. Operations: get, set with TTL, invalidate by query ID pattern, invalidate by table ID (registration change).
- `provisa/cache/policy.py` — caching policy per query:
  - `none` — no caching (default for test mode / unapproved queries)
  - `ttl` — cache with steward-specified TTL (seconds)
  - Policy stored as metadata on approved registry entries (extends REQ-024)
- `provisa/cache/middleware.py` — pipeline integration: check cache before execution, store result after execution. Transparent — cache miss executes normally.

**Key behaviors:**
- Cache key MUST include `role_id` + all RLS context values. Two users with different RLS filters get different cache entries (REQ-078).
- Registration model changes (REQ-025) trigger invalidation of affected cache entries by query ID (REQ-079).
- Provisa mutations (INSERT/UPDATE/DELETE via REQ-031) invalidate cache entries that reference the mutated table (REQ-080).
- Staleness is explicit: cached responses include `X-Provisa-Cache: HIT` header with age. No silent stale data (REQ-064).
- Cache stores serialized JSON (post-serialization). Large-result-redirect responses (REQ-029) are NOT cached — the blob is already stored.
- Redis is optional at startup: if `REDIS_URL` is not set, caching is disabled (no-op store). No fallback values (REQ-064).

**Config:**
```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}   # e.g., redis://redis:6379/0
  default_ttl: 300               # 5 minutes, overridden per query
```

**Integration points:**
- `provisa/api/data/endpoint.py` — insert cache check/store around execution
- `provisa/registry/store.py` — add `cache_ttl` field to approved query metadata
- `provisa/registry/approval.py` — invalidate cache on registration changes
- `docker-compose.yml` — add Redis service

**Verify:**
- `python -m pytest tests/unit/test_cache_key.py -x -q`:
  - Same query + role + RLS → same key
  - Different role → different key
  - Different RLS context → different key
  - Missing RLS context raises (never silently omit)
- `python -m pytest tests/unit/test_cache_policy.py -x -q`:
  - Policy `none` → skip cache
  - Policy `ttl` → cache with specified TTL
- `python -m pytest tests/integration/test_cache_store.py -x -q`:
  - Set/get round-trip via Redis
  - TTL expiration
  - Invalidate by query ID pattern
  - Invalidate by table ID
- `python -m pytest tests/e2e/test_caching.py -x -q`:
  - First request → cache MISS, executes query
  - Second identical request → cache HIT, no execution
  - Different role → cache MISS (separate partition)
  - Registration change → cache invalidated, next request re-executes
  - Mutation on cached table → cache invalidated
  - `X-Provisa-Cache` header present with HIT/MISS + age

**Files:**
| File | Action |
|---|---|
| `provisa/cache/__init__.py` | Create |
| `provisa/cache/key.py` | Create |
| `provisa/cache/store.py` | Create |
| `provisa/cache/policy.py` | Create |
| `provisa/cache/middleware.py` | Create |
| `provisa/api/data/endpoint.py` | Modify (cache integration) |
| `provisa/registry/store.py` | Modify (add cache_ttl) |
| `provisa/registry/approval.py` | Modify (cache invalidation) |
| `docker-compose.yml` | Modify (add Redis) |
| `config/provisa.yaml` | Modify (add cache config) |
| `tests/unit/test_cache_key.py` | Create |
| `tests/unit/test_cache_policy.py` | Create |
| `tests/integration/test_cache_store.py` | Create |
| `tests/e2e/test_caching.py` | Create |

---

## Phase P: Materialized View Optimization
**Goal:** Transparent SQL-level optimization. Steward-defined materialized views that are invisible in the GraphQL SDL but automatically used when the SQL compiler detects a matching JOIN pattern.
**REQs:** REQ-081, REQ-082, REQ-083, REQ-084, REQ-085

**Build:**
- `provisa/mv/__init__.py`
- `provisa/mv/models.py` — MV config model. Two modes:
  - **Join-pattern MV** (transparent optimization): defines `join_pattern`, compiler matches and rewrites
  - **Custom SQL MV** (exposed in SDL): defines `sql` with arbitrary SELECT (aggregates, computed columns), optionally exposed as a queryable GraphQL type via `expose_in_sdl` + `sdl_config`
- `provisa/mv/registry.py` — MV registry: stores MV definitions in PG, tracks last refresh time, target table name, row count, status (fresh/stale/refreshing). MV definitions loaded at startup alongside config.
- `provisa/mv/refresh.py` — MV refresh engine:
  - Builds the full SELECT from the MV's source tables + join pattern (using `CompilationContext` for catalog-qualified names)
  - Executes via Trino: `CREATE TABLE IF NOT EXISTS target AS SELECT ...` on first run, `DELETE FROM target; INSERT INTO target SELECT ...` on refresh (atomic within Trino transaction)
  - Runs on schedule (background asyncio task) or on-demand via admin API
  - Tracks refresh duration, row count, last error
  - Mutations on source tables mark affected MVs as stale (REQ-084)
- `provisa/mv/rewriter.py` — SQL rewrite pass (REQ-082):
  - After `compile_query` produces SQL, the rewriter inspects the FROM + JOIN clauses
  - **Pattern matching**: extracts (left_table, join_column, right_table, join_column, join_type) from compiled SQL
  - Compares against registered MV join patterns
  - If match found AND MV is fresh: rewrites SQL to read from MV target table instead. The SELECT projection, WHERE, ORDER BY, LIMIT/OFFSET are preserved — only the FROM/JOIN is replaced.
  - If match found AND MV is stale: executes original SQL (no silent stale data per REQ-064), logs that MV refresh is needed
  - If no match: passes SQL through unchanged
  - **Partial match**: if the query JOINs orders+customers+products, and an MV covers orders+customers, the rewriter can partially apply — rewrite the orders+customers portion to the MV and keep the products JOIN. (REQ-083)
- `provisa/mv/schema.sql` — PG tables for MV metadata: `materialized_views`, `mv_refresh_log`

**Key behaviors:**
- MVs are **invisible in GraphQL SDL by default** — users query the same schema. The optimization is transparent (REQ-081).
- **Optional SDL exposure** (REQ-086): steward can set `expose_in_sdl: true` on an MV definition. When exposed:
  - MV target table is registered in the table registry with its own columns, domain, and visibility rules
  - Appears as a queryable type in the GraphQL schema (e.g., `customer_stats { customer_id, order_count, avg_amount, lifetime_value }`)
  - Useful for MVs that add computed semantics (aggregates, derived columns) not present in source tables
  - Subject to same governance (pre-approved/registry-required), RLS, and column visibility as any registered table
  - Steward defines which columns are visible to which roles, independent of source table visibility
- MV target tables without `expose_in_sdl` are NOT registered — they cannot be queried directly via GraphQL.
- Steward controls MV lifecycle: create, refresh schedule, enable/disable, drop.
- Refresh is **not** triggered by every query — it's on a schedule or manual. Queries that match a stale MV skip the optimization and execute normally.
- MV target tables live in a designated schema (e.g., `mv_cache`) in a fast catalog. For cross-source MVs, the target is always in a RDBMS catalog that Trino can write to.
- RLS is applied after the rewrite — the MV contains the full join result, and RLS WHERE clauses are injected on the rewritten query just like on the original. This is safe because RLS is enforced at the SQL level regardless of the underlying table (REQ-085).

**Config integration:**
```yaml
materialized_views:
  # Transparent optimization — invisible in SDL
  - id: mv-orders-customers
    source_tables: [orders, customers]
    join_pattern:
      left: { table: orders, column: customer_id }
      right: { table: customers, column: id }
      type: left
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true

  # Exposed in SDL — adds computed semantics
  - id: mv-customer-stats
    sql: |
      SELECT c.id AS customer_id, c.name,
             COUNT(o.id) AS order_count,
             AVG(o.amount) AS avg_amount,
             SUM(o.amount) AS lifetime_value
      FROM orders o JOIN customers c ON o.customer_id = c.id
      GROUP BY c.id, c.name
    source_tables: [orders, customers]
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 600
    enabled: true
    expose_in_sdl: true
    sdl_config:
      domain_id: sales-analytics
      governance: pre-approved
      columns:
        - name: customer_id
          visible_to: [admin, analyst]
        - name: name
          visible_to: [admin, analyst]
        - name: order_count
          visible_to: [admin, analyst]
        - name: avg_amount
          visible_to: [admin]
        - name: lifetime_value
          visible_to: [admin]
```

**Verify:**
- `python -m pytest tests/unit/test_mv_rewriter.py -x -q`:
  - SQL with matching JOIN → rewritten to MV target table
  - SQL with matching JOIN but stale MV → not rewritten
  - SQL with no matching JOIN → unchanged
  - SQL with partial match → partially rewritten (MV portion replaced, other JOINs kept)
  - SELECT projection, WHERE, ORDER BY, LIMIT preserved after rewrite
- `python -m pytest tests/unit/test_mv_registry.py -x -q`:
  - Register/deregister MV
  - Mark stale on source table mutation
  - Fresh/stale status tracking
- `python -m pytest tests/integration/test_mv_refresh.py -x -q`:
  - CTAS creates MV target table via Trino
  - Refresh replaces data atomically
  - Row count and duration tracked
- `python -m pytest tests/e2e/test_mv_optimization.py -x -q`:
  - Query orders+customers with MV fresh → fast response (reads from MV)
  - Same query with MV disabled → normal cross-source execution
  - Mutation on orders → MV marked stale → next query executes normally
  - Refresh MV → next query uses MV again
  - RLS applied correctly on MV-backed queries
  - Exposed MV appears as queryable type in SDL (`customer_stats { order_count avg_amount }`)
  - Exposed MV respects column visibility per role
  - Exposed MV routes direct (single-source target table), not through Trino
  - Non-exposed MV does NOT appear in SDL

**Files:**
| File | Action |
|---|---|
| `provisa/mv/__init__.py` | Create |
| `provisa/mv/models.py` | Create |
| `provisa/mv/registry.py` | Create |
| `provisa/mv/refresh.py` | Create |
| `provisa/mv/rewriter.py` | Create |
| `provisa/mv/schema.sql` | Create |
| `provisa/core/schema.sql` | Modify (add MV tables) |
| `provisa/api/data/endpoint.py` | Modify (MV rewrite pass) |
| `provisa/api/app.py` | Modify (load MV config, start refresh scheduler) |
| `config/provisa.yaml` | Modify (add MV config) |
| `tests/unit/test_mv_rewriter.py` | Create |
| `tests/unit/test_mv_registry.py` | Create |
| `tests/integration/test_mv_refresh.py` | Create |
| `tests/e2e/test_mv_optimization.py` | Create |

---

## Phase Q: Column-Level Masking
**Goal:** Per-column, per-role data masking at the SQL level. Masked columns return transformed values — raw data never reaches the client. Regex masking for strings, constant/NULL replacement for numerics.
**REQs:** REQ-087, REQ-088, REQ-089, REQ-090, REQ-091

**Build:**
- `provisa/security/masking.py` — masking engine:
  - Parse masking rules from config per (column, role)
  - Generate SQL expressions to replace column references in SELECT projection:
    - `regex` (string columns): `REGEXP_REPLACE("col", 'pattern', 'replace')` — works in both PG and Trino (REQ-088)
    - `constant` (any type): replace column with literal value. Options: `NULL` (if nullable), `0`, `-1`, custom value, `MAX`, `MIN` (resolved to type bounds at compile time, e.g., integer MAX → 2147483647) (REQ-089)
    - `truncate` (date/timestamp): `DATE_TRUNC('precision', "col")` — e.g., precision=year turns 2025-03-31 → 2025-01-01 (REQ-090)
  - Type validation: regex masking only allowed on string types; numeric types reject regex rules at config load time (REQ-091)
- `provisa/compiler/mask_inject.py` — inject masking into compiled SQL:
  - After compilation, before transpilation
  - Walks the SELECT column list and replaces masked columns with their mask expression
  - Handles aliased columns (JOINs with t0, t1 prefixes)
  - Preserves column aliases so serialization still works
- Masking config stored as inline fields on `ColumnConfig` in `provisa/core/models.py` (`mask_type`, `mask_pattern`, etc.) — no separate `MaskingRule` model. Keeps masking co-located with the column definition it applies to.
- Masking rules loaded from `table_columns` DB rows at startup in `provisa/api/app.py`
- Update `provisa/api/data/endpoint.py` — insert masking step in pipeline

**Config:**
```yaml
tables:
  - source_id: sales-pg
    table: customers
    columns:
      - name: email
        visible_to: [admin, analyst, masked_viewer]
        masking:
          analyst:
            type: regex
            pattern: "^(.{2}).*(@.*)$"
            replace: "$1***$2"           # al***@example.com
          masked_viewer:
            type: constant
            value: "***@***.***"
      - name: name
        visible_to: [admin, analyst]
        masking:
          analyst:
            type: regex
            pattern: "^(.).* (.).*$"
            replace: "$1. $2."           # A. J.
      - name: region
        visible_to: [admin, analyst, masked_viewer]
        # no masking — all roles see raw value
  - source_id: sales-pg
    table: orders
    columns:
      - name: amount
        visible_to: [admin, analyst, masked_viewer]
        masking:
          masked_viewer:
            type: constant
            value: 0                     # numeric → constant
      - name: created_at
        visible_to: [admin, analyst]
        masking:
          analyst:
            type: truncate
            precision: month             # 2025-03-31 → 2025-03-01
```

**Pipeline position:**
```
parse → compile → RLS inject → masking inject → sampling → transpile → execute → serialize
```

**Key behaviors:**
- Masking is per-column, per-role. Same column can have different masks for different roles (REQ-087).
- Admin sees raw data unless explicitly configured with a mask.
- Masking is applied at SQL level — the mask expression replaces the column in the SELECT projection. The DB engine performs the transformation.
- A column can be both visible AND masked — the user sees the column exists and can filter/sort on it, but the returned values are masked.
- Regex masking ONLY for string types (varchar, char, text). Attempting to configure regex on a numeric/boolean/date column raises a validation error at config load time (REQ-091).
- `MAX`/`MIN` constants resolved at compile time from the column's Trino data type:
  - integer → 2147483647 / -2147483648
  - bigint → 9223372036854775807 / -9223372036854775808
  - decimal(p,s) → 10^p - 1 (approx)
  - real/double → 'Infinity' / '-Infinity' (or practical large values)
- `NULL` replacement only allowed on nullable columns — if column is NOT NULL, config validation rejects `value: NULL` (REQ-091).

**Verify:**
- `python -m pytest tests/unit/test_masking.py -x -q`:
  - Regex mask generates correct REGEXP_REPLACE expression
  - Constant mask generates correct literal substitution
  - Truncate mask generates correct DATE_TRUNC expression
  - NULL constant rejected on NOT NULL column
  - Regex rejected on numeric column
  - MAX/MIN resolved to correct bounds per type
  - No masking for role without mask config → raw column
- `python -m pytest tests/unit/test_mask_inject.py -x -q`:
  - SELECT columns replaced with mask expressions
  - Aliased columns (t0, t1) handled correctly
  - Column aliases preserved for serialization
  - Unmasked columns unchanged
  - Multiple masked columns in same query
- `python -m pytest tests/e2e/test_masking.py -x -q`:
  - Query as admin → raw email values
  - Query as analyst → email values masked (regex applied)
  - Query as masked_viewer → email values constant-masked
  - Numeric column masked → returns constant value
  - Date column truncated → returns truncated date
  - Filter on masked column still works (WHERE uses raw value, SELECT uses masked)

**Files:**
| File | Action |
|---|---|
| `provisa/security/masking.py` | Create |
| `provisa/compiler/mask_inject.py` | Create |
| `provisa/core/models.py` | Modify (add inline mask fields to ColumnConfig) |
| `provisa/api/app.py` | Modify (load masking rules at startup) |
| `provisa/api/data/endpoint.py` | Modify (masking step) |
| `config/provisa.yaml` | Modify (add masking examples) |
| `tests/unit/test_masking.py` | Create |
| `tests/unit/test_mask_inject.py` | Create |
| `tests/e2e/test_masking.py` | Create |

---

## Phase R: LLM Relationship Discovery
**Goal:** Use an LLM (Claude API) to analyze table metadata and sample data, suggest candidate FK relationships at table, domain, or cross-domain scope. Steward reviews and approves candidates.
**REQs:** REQ-092, REQ-093, REQ-094, REQ-095, REQ-096

**Build:**
- `provisa/discovery/__init__.py`
- `provisa/discovery/collector.py` — gather metadata for LLM analysis:
  - Column names, types, nullability, sample values (configurable N, default 20)
  - Existing relationships (so the LLM doesn't re-suggest)
  - Scope control: per-table, per-domain, or cross-domain (REQ-092)
    - **Per-table**: analyze one table against all other registered tables in the same domain
    - **Per-domain**: analyze all tables within a domain for internal relationships
    - **Cross-domain** (superdomain): analyze tables across multiple/all domains — discovers cross-source relationships (e.g., orders in sales-pg → product_reviews in reviews-mongo)
- `provisa/discovery/prompt.py` — LLM prompt construction:
  - Structured prompt with table schemas, sample data, existing relationships
  - Requests JSON output: `[{source_table, source_column, target_table, target_column, cardinality, confidence, reasoning}]`
  - Includes type compatibility hints (integer ↔ bigint OK, varchar ↔ integer suspicious)
  - Scope-aware: prompt includes only tables in the requested scope
- `provisa/discovery/analyzer.py` — LLM interaction via Claude API:
  - Send prompt to Claude, parse structured response
  - Validate candidates: both tables exist, columns exist, types are join-compatible
  - Score filtering: configurable minimum confidence threshold (default 0.7)
  - Sample data validation: for high-confidence candidates, run a verification query to check if FK values actually exist in the target table (reduces false positives)
- `provisa/discovery/candidates.py` — candidate storage and lifecycle:
  - Store candidates in PG: `relationship_candidates` table
  - Status: `suggested`, `accepted`, `rejected`, `expired`
  - On accept: create the relationship in the registration model (same as manual registration)
  - On reject: record reason, don't re-suggest in future runs
  - Expiry: candidates older than configurable TTL auto-expire (schema may have changed)
- `provisa/api/admin/discovery.py` — admin API endpoints:
  - `POST /admin/discover/relationships` — trigger discovery with scope parameter:
    - `{"scope": "table", "table_id": 1}` — single table
    - `{"scope": "domain", "domain_id": "sales-analytics"}` — all tables in domain
    - `{"scope": "cross-domain"}` — all registered tables across all domains
    - `{"scope": "cross-domain", "domain_ids": ["sales-analytics", "customer-insights"]}` — specific domains
  - `GET /admin/discover/candidates` — list pending candidates
  - `POST /admin/discover/candidates/{id}/accept` — accept candidate → creates relationship
  - `POST /admin/discover/candidates/{id}/reject` — reject with reason

**DB schema additions:**
```sql
CREATE TABLE IF NOT EXISTS relationship_candidates (
    id              SERIAL PRIMARY KEY,
    source_table_id INTEGER NOT NULL REFERENCES registered_tables(id),
    target_table_id INTEGER NOT NULL REFERENCES registered_tables(id),
    source_column   TEXT NOT NULL,
    target_column   TEXT NOT NULL,
    cardinality     TEXT NOT NULL,
    confidence      REAL NOT NULL,
    reasoning       TEXT,
    status          TEXT NOT NULL DEFAULT 'suggested'
                    CHECK (status IN ('suggested', 'accepted', 'rejected', 'expired')),
    scope           TEXT NOT NULL,   -- 'table', 'domain', 'cross-domain'
    rejection_reason TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_table_id, source_column, target_table_id, target_column)
);
```

**Key behaviors:**
- Discovery is steward-initiated, never automatic (REQ-093). Steward chooses scope and triggers.
- LLM sees only metadata and sample data — never full table contents (REQ-094).
- Sample data query respects RLS if the triggering steward has RLS rules (defense in depth).
- Cross-domain discovery is the most powerful: finds relationships between PG orders and MongoDB product_reviews that a human might miss.
- Previously rejected candidates are excluded from future suggestions (REQ-095).
- Accepted candidates go through the normal relationship registration flow — flagging affected persisted queries (REQ-020/REQ-025).
- Claude API key configured via secrets provider: `${env:ANTHROPIC_API_KEY}` (REQ-096).

**Verify:**
- `python -m pytest tests/unit/test_discovery_prompt.py -x -q`:
  - Prompt includes correct table metadata for each scope
  - Existing relationships excluded from prompt
  - Cross-domain prompt includes tables from multiple sources
- `python -m pytest tests/unit/test_discovery_analyzer.py -x -q`:
  - Valid LLM response parsed correctly
  - Invalid candidates filtered (missing columns, incompatible types)
  - Below-threshold confidence filtered
  - Previously rejected candidates excluded
- `python -m pytest tests/integration/test_discovery_candidates.py -x -q`:
  - Store/retrieve/accept/reject candidates in PG
  - Accept creates relationship in registration model
  - Reject records reason
  - Duplicate candidate handling (UNIQUE constraint)
- `python -m pytest tests/e2e/test_discovery_flow.py -x -q`:
  - Trigger table-scope discovery → candidates returned
  - Trigger domain-scope discovery → candidates across domain tables
  - Accept candidate → relationship appears in GraphQL schema
  - Reject candidate → not re-suggested on next run

**Files:**
| File | Action |
|---|---|
| `provisa/discovery/__init__.py` | Create |
| `provisa/discovery/collector.py` | Create |
| `provisa/discovery/prompt.py` | Create |
| `provisa/discovery/analyzer.py` | Create |
| `provisa/discovery/candidates.py` | Create |
| `provisa/api/admin/discovery.py` | Create |
| `provisa/core/schema.sql` | Modify (add relationship_candidates) |
| `config/provisa.yaml` | Modify (add discovery config) |
| `tests/unit/test_discovery_prompt.py` | Create |
| `tests/unit/test_discovery_analyzer.py` | Create |
| `tests/integration/test_discovery_candidates.py` | Create |
| `tests/e2e/test_discovery_flow.py` | Create |

---

## Phase S: gRPC Query Endpoint
**Goal:** Generate `.proto` schemas from the registration model (per role) and serve a gRPC endpoint. Clients get strongly-typed stubs for queries and mutations — no string-based GraphQL required.
**REQs:** REQ-097, REQ-098, REQ-099, REQ-100

**Build:**
- `provisa/grpc/__init__.py`
- `provisa/grpc/proto_gen.py` — generate `.proto` files from the registration model (REQ-097):
  - Each registered table → a protobuf `message` (e.g., `message Orders { int32 id = 1; float amount = 2; ... }`)
  - Relationships → nested messages (many-to-one → singular field, one-to-many → `repeated` field)
  - Per-role: only visible columns/tables appear in the role's proto
  - Filter inputs → `message OrdersWhere { StringFilter region = 1; IntFilter id = 2; ... }`
  - Root service → `service ProvisaData { rpc QueryOrders(OrdersRequest) returns (stream Orders); ... }`
  - Mutation methods → `rpc InsertOrders(OrdersInsertInput) returns (MutationResponse);`
  - Type mapping: Trino types → protobuf types (integer→int32, bigint→int64, varchar→string, decimal→double, boolean→bool, timestamp→google.protobuf.Timestamp)
  - Proto files written to a configurable output directory (e.g., `generated/proto/`)
  - Regenerated on schema change (table publication, column visibility change)
- `provisa/grpc/schema_gen.py` — compile `.proto` → Python gRPC stubs using `grpcio-tools`:
  - `protoc` compilation at startup or on-demand
  - Generated `_pb2.py` and `_pb2_grpc.py` files
  - Hot-reloadable: schema changes regenerate protos and reload stubs without restart
- `provisa/grpc/server.py` — gRPC server implementation (REQ-098):
  - Implements the generated service interface
  - Each RPC method follows the same pipeline: parse request → compile SQL → RLS → sampling → route → execute → serialize to protobuf
  - Streaming responses: server-side streaming for query results (one message per row)
  - Unary responses: mutations return single `MutationResponse`
  - Role determined from gRPC metadata (`x-provisa-role` header)
  - Full security pipeline: rights check, RLS injection, sampling, governance
- `provisa/grpc/reflection.py` — gRPC server reflection (REQ-099):
  - Enable `grpc_reflection` so clients can discover available services/methods at runtime
  - Equivalent of GraphQL introspection for gRPC
- `provisa/api/app.py` — start gRPC server alongside FastAPI (separate port, default 50051)

**Proto generation example:**
```protobuf
syntax = "proto3";
package provisa.v1;

import "google/protobuf/timestamp.proto";

// Generated for role: admin
message Orders {
  int32 id = 1;
  int32 customer_id = 2;
  double amount = 3;
  string region = 4;
  string status = 5;
  google.protobuf.Timestamp created_at = 6;
  // Relationship: many-to-one
  Customers customers = 7;
}

message Customers {
  int32 id = 1;
  string name = 2;
  string email = 3;
  string region = 4;
}

message IntFilter {
  optional int32 eq = 1;
  optional int32 neq = 2;
  optional int32 gt = 3;
  optional int32 lt = 4;
  repeated int32 in = 5;
  optional bool is_null = 6;
}

message StringFilter {
  optional string eq = 1;
  optional string neq = 2;
  repeated string in = 3;
  optional string like = 4;
  optional bool is_null = 5;
}

message OrdersWhere {
  optional IntFilter id = 1;
  optional StringFilter region = 2;
  // ... all visible filterable columns
}

message OrdersRequest {
  optional OrdersWhere where = 1;
  optional int32 limit = 2;
  optional int32 offset = 3;
}

message MutationResponse {
  int32 affected_rows = 1;
}

message OrdersInsertInput {
  int32 customer_id = 1;
  double amount = 2;
  string region = 3;
}

service ProvisaData {
  rpc QueryOrders(OrdersRequest) returns (stream Orders);
  rpc QueryCustomers(CustomersRequest) returns (stream Customers);
  rpc InsertOrders(OrdersInsertInput) returns (MutationResponse);
  rpc UpdateOrders(OrdersUpdateRequest) returns (MutationResponse);
  rpc DeleteOrders(OrdersDeleteRequest) returns (MutationResponse);
}
```

**Key behaviors:**
- Proto files are **generated artifacts**, not hand-written. Regenerated from the registration model on every schema change (REQ-097).
- Each role gets its own proto file and service — `provisa_admin_v1.proto`, `provisa_analyst_v1.proto`. Only visible tables/columns appear.
- gRPC server runs on a separate port from FastAPI HTTP (default 50051) (REQ-098).
- Full security pipeline applied: rights, RLS, sampling, governance — identical to HTTP/GraphQL path.
- Server reflection enabled by default so clients can use tools like `grpcurl` or `evans` for exploration (REQ-099).
- Proto files also served via HTTP at `/data/proto/{role_id}` for client code generation (REQ-100).
- gRPC streaming delivers results row-by-row — better for large result sets than buffered JSON.

**Verify:**
- `python -m pytest tests/unit/test_proto_gen.py -x -q`:
  - Table → message with correct field types and numbers
  - Relationship → nested message field
  - Per-role: invisible columns excluded
  - Filter input types generated correctly
  - Service with query + mutation RPCs
- `python -m pytest tests/unit/test_grpc_server.py -x -q`:
  - Request → SQL compilation → correct results
  - Streaming response delivers all rows
  - Role from metadata applied
- `python -m pytest tests/integration/test_grpc_reflection.py -x -q`:
  - Server reflection lists services
  - Method descriptors match generated proto
- `python -m pytest tests/e2e/test_grpc_query.py -x -q`:
  - gRPC client sends QueryOrders → receives streamed Orders messages
  - Filter applied → filtered results
  - InsertOrders → row created, MutationResponse returned
  - Wrong role → permission denied
  - Schema change → new proto available

**Files:**
| File | Action |
|---|---|
| `provisa/grpc/__init__.py` | Create |
| `provisa/grpc/proto_gen.py` | Create |
| `provisa/grpc/schema_gen.py` | Create |
| `provisa/grpc/server.py` | Create |
| `provisa/grpc/reflection.py` | Create |
| `provisa/api/app.py` | Modify (start gRPC server) |
| `provisa/api/data/endpoint.py` | Modify (add /data/proto/{role_id} route) |
| `tests/unit/test_proto_gen.py` | Create |
| `tests/unit/test_grpc_server.py` | Create |
| `tests/integration/test_grpc_reflection.py` | Create |
| `tests/e2e/test_grpc_query.py` | Create |

---

## Phase T: Documentation
**Goal:** End-user and developer documentation. README for installation/usage/features, architecture guide explaining components and data flow, detailed config and API references. Break into multiple linked files to keep each under ~200 lines.
**REQs:** REQ-101, REQ-102, REQ-103

**Build:**

**`README.md`** (REQ-101) — end-user focused, ~150 lines:
- What Provisa is (1 paragraph)
- Key features (bulleted: multi-source federation, GraphQL API, per-role schemas, smart routing, security layers, output formats, governance)
- Quick start: `docker compose up`, first query via curl
- Configuration overview (link to `docs/configuration.md`)
- API overview (link to `docs/api-reference.md`)
- Security model summary (link to `docs/security.md`)
- Supported sources table
- Development setup (`./setup.sh`, running tests)

**`docs/architecture.md`** (REQ-102) — technical architecture, ~200 lines:
- System overview (ASCII diagram: config → catalog → schema → query pipeline → result)
- Request pipeline: parse → compile → RLS inject → sampling → route → transpile → execute → serialize
- Module map table: which module owns what responsibility
- Routing decision tree: single RDBMS → direct, NoSQL → Trino, multi-source → Trino
- Cross-source JOIN handling: catalog-qualified SQL, type coercion
- Security enforcement order: rights → schema visibility → RLS → sampling
- Data flow diagrams: query path vs mutation path
- Link to component docs for deep dives

**`docs/configuration.md`** — detailed YAML config reference, ~250 lines:
- `sources` — type, host, port, credentials (secrets provider), supported types table
- `domains` — logical groupings
- `naming` — regex rules for GraphQL name generation
- `tables` — registration, governance level, columns with visibility and masking
- `relationships` — FK definitions, cardinality
- `roles` — capabilities list (all 8 capabilities explained), domain_access
- `rls_rules` — per-table per-role filter expressions
- `materialized_views` — transparent optimization and exposed MVs
- `cache` — Redis config, TTL
- Environment variables reference (all `PROVISA_*` vars)

**`docs/api-reference.md`** — HTTP API reference, ~150 lines:
- `POST /data/graphql` — request body, response formats, content negotiation (Accept header), role header
- `GET /health` — health check
- Arrow Flight endpoint — ticket format, connection
- Error responses — 400/403/500 formats
- curl examples for each format (JSON, NDJSON, CSV, Parquet, Arrow)

**`docs/security.md`** — security model deep dive, ~150 lines:
- Rights model (8 capabilities, admin override)
- Schema visibility (domain access, column visibility)
- RLS (per-table per-role WHERE injection)
- Sampling mode (default on, full_results capability)
- Governance (test mode vs production, registry-required vs pre-approved)
- Column masking (regex, constant, truncate)
- Secrets provider (env vars, extensible to Vault/K8s)

**`docs/sources.md`** — source type reference, ~100 lines:
- Per-source-type table: driver, direct execution support, Trino connector, SQLGlot dialect
- Connection examples for each type
- NoSQL limitations (Trino-only, no mutations)
- Cross-source query behavior

**Verify:** _(Manually testable only — no automated test gate)_
- All internal links resolve (no broken `[text](path)` refs)
- Code examples in docs execute correctly
- curl examples return expected output against running stack
- No stale references to renamed modules or changed APIs

**Files:**
| File | Action |
|---|---|
| `README.md` | Create (replace scaffold) |
| `docs/architecture.md` | Create |
| `docs/configuration.md` | Create |
| `docs/api-reference.md` | Create |
| `docs/security.md` | Create |
| `docs/sources.md` | Create |

---

## Phase U: API Sources (REST, GraphQL, gRPC)
**Goal:** Register API endpoints like databases — auto-discover "tables" from OpenAPI specs, GraphQL introspection, and gRPC reflection. Steward reviews candidates and registers them. Primitives become native PG columns; complex objects stored as JSONB (not filterable, no relationships). Cached in PG with TTL. Single-source returns directly; multi-source joins through Trino.
**REQs:** REQ-104, REQ-105, REQ-106, REQ-107, REQ-108, REQ-109, REQ-110, REQ-111, REQ-112, REQ-113, REQ-119

**Build:**
- `provisa/api_source/__init__.py`
- `provisa/api_source/models.py` — Pydantic config models (REQ-104)
- `provisa/api_source/introspect.py` — spec-driven auto-discovery (REQ-111):
  - **REST/OpenAPI**: fetch spec from URL or accept manual upload. Parse with `openapi-pydantic`. Each GET endpoint → candidate table. Path+query params → filterable columns. Response schema 200 → output columns.
  - **GraphQL**: introspect schema via `__schema` query. Each root query field → candidate table. Field arguments → filterable columns. Return type fields → output columns.
  - **gRPC**: use server reflection to list services + methods. Each unary/server-streaming RPC → candidate table. Request message fields → filterable columns. Response message fields → output columns.
  - **Column type inference**: primitive types (string, integer, number, boolean, date) → native PG columns. Complex types (objects, arrays) → JSONB column (REQ-112).
  - **Complex object rules** (REQ-113): JSONB columns are NOT filterable, CANNOT participate in relationships, queryable via `json_extract` in Trino but no predicate pushdown.
  - Returns list of `ApiEndpointCandidate` for steward review.
- `provisa/api_source/candidates.py` — candidate storage + steward review:
  - Store discovered candidates in PG: `api_endpoint_candidates` table
  - Status: `discovered`, `registered`, `rejected`
  - Steward reviews in UI: sees all discovered endpoints as candidate tables
  - On accept: steward can rename columns, set TTL, mark columns non-filterable, inject constants, define relationships
  - On accept: auto-creates PG cache table DDL from column definitions
- `provisa/api_source/caller.py` — HTTP/gRPC client for API calls (REQ-105):
  - Build request from registered endpoint config + resolved filters
  - `param_type` handling: `query` (URL params), `path` (URL interpolation), `body` (JSON body via dot-path injection from `body_template`), `header`, `variable` (GraphQL vars)
  - Pagination: auto-follow (link_header, cursor, offset, page_number)
  - gRPC: build protobuf request from filter values, call unary/stream RPC
  - Retry with backoff on 429/5xx. Timeout configurable per endpoint.
- `provisa/api_source/flattener.py` — response → PG rows (REQ-106):
  - Navigate to response root via JSONPath
  - Primitive fields → native PG column values
  - Complex fields (objects, arrays) → JSONB values
  - Apply transforms: `from_unix_timestamp`, `cents_to_decimal`, etc.
  - Steward-defined constants injected as extra columns (e.g., `source: "github"`)
- `provisa/api_source/cache.py` — PG cache with TTL (REQ-107):
  - Cache key: `hash(endpoint_id, sorted(resolved_filter_params))`
  - Cache table per endpoint: `api_cache_<source_id>_<endpoint_id>` with native + JSONB columns
  - TTL resolution: endpoint > source > global default (300s)
  - `check_cache(key, ttl)` → rows or None
  - `write_cache(key, rows, ttl)` → async fire-and-forget for single-source, blocking for multi-source
  - Stale-while-revalidate: return stale within `stale_ttl`, background refresh
  - Purge expired rows on schedule
  - ON CONFLICT upsert for race conditions
- `provisa/api_source/router_integration.py` — routing (REQ-108):
  - Single API source, cache hit → direct PG read
  - Single API source, cache miss → call API → return immediately → background PG write
  - Multi-source, cache hit → Trino reads PG cache
  - Multi-source, cache miss → call API → write PG (blocking) → Trino joins
- `provisa/api_source/schema_integration.py` — SDL generation (REQ-109):
  - Registered API endpoints appear as types in the GraphQL schema
  - Primitive columns: in output type + WHERE input (if filterable)
  - JSONB columns: in output type only (as JSON scalar), NOT in WHERE, NOT in relationships
  - Non-filterable primitive in WHERE → compile error
  - Relationships: only between primitive columns across any source type

**Registration flow (like registering a database):**
```
1. Steward registers API source:
   - REST: base_url + OpenAPI spec URL (or manual spec upload for non-OpenAPI endpoints)
   - GraphQL: URL (auto-introspects schema)
   - gRPC: host:port (auto-discovers via reflection)

2. Provisa introspects → discovers candidate "tables":
   - REST/OpenAPI: GET /users → candidate "users" table
   - GraphQL: query { articles(...) } → candidate "articles" table
   - gRPC: rpc ListOrders(OrdersRequest) → candidate "orders" table

3. UI shows all candidates with columns, types, filterability

4. Steward registers candidates:
   - Accepts/rejects each candidate
   - Renames columns, sets TTL, injects constants
   - Marks additional columns as non-filterable
   - Defines relationships to other tables
   - Complex objects automatically marked: non-filterable, no relationships

5. Registered endpoints become queryable tables in SDL
```

**PG cache table schema (hybrid storage):**
```sql
CREATE TABLE api_cache_github_users (
    _cache_hash    TEXT NOT NULL,
    _cached_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _ttl_seconds   INTEGER NOT NULL,
    -- Primitive columns (native, fast, pushdown works)
    id             INTEGER,
    login          TEXT,
    type           TEXT,
    -- Complex columns (JSONB, queryable but no pushdown)
    plan           JSONB,
    organizations  JSONB,
    -- Promoted fields from JSONB (generated columns — auto-maintained by PG)
    plan_name      TEXT GENERATED ALWAYS AS (plan->>'name') STORED,
    plan_amount    INTEGER GENERATED ALWAYS AS ((plan->>'amount')::integer) STORED
);
```

**JSONB field promotion (REQ-119):**
Stewards can "promote" nested JSONB fields into native PG generated columns. This makes them filterable, indexable, and relationship-eligible — while the source JSONB column remains available for ad-hoc access.

- **Mechanism:** PG `GENERATED ALWAYS AS (jsonb_col->>'field') STORED` columns
- **Auto-maintained:** PG updates them on every INSERT/UPDATE — no application logic needed
- **Introspectable:** Generated columns appear in `information_schema.columns`, so Trino picks them up automatically via the PG connector
- **Filterable:** Real columns with pushdown support
- **Relationship-eligible:** Can participate in JOINs like any native column
- **Indexable:** Standard B-tree indexes for query performance
- **Nested access:** Supports dot-path extraction: `plan.billing.currency` → `plan->'billing'->>'currency'`

**Promotion config:**
```yaml
sources:
  - id: github-api
    type: openapi
    spec_url: https://api.github.com/openapi.json
    tables:
      - endpoint: /users
        table_name: users
        promotions:
          - jsonb_column: plan
            field: name           # top-level field in JSONB
            target_column: plan_name
            target_type: TEXT
          - jsonb_column: plan
            field: amount
            target_column: plan_amount
            target_type: INTEGER
          - jsonb_column: plan
            field: billing.currency  # nested dot-path
            target_column: plan_currency
            target_type: TEXT
```

**DDL generation for promotions:**
- On candidate accept, generate `GENERATED ALWAYS AS` clauses from promotion config
- Dot-path `a.b.c` → PG expression `(col->'a'->'b'->>'c')`
- Cast to target type: `::integer`, `::numeric`, `::boolean`, `::timestamptz`
- Auto-create B-tree index on each promoted column

**SDL integration:**
- Promoted columns appear as regular scalar fields in the GraphQL type (filterable, sortable)
- Original JSONB column still appears as JSON scalar (not filterable)
- Promoted columns can participate in relationships and RLS filters

**Query-time flow:**
```
1. Parse GraphQL → detect API source endpoint
2. Extract WHERE filters → validate all are filterable primitives
3. Map filters to API params (query/body/path/header/variable)
4. cache_key = hash(endpoint_id, sorted(resolved_params))
5. Check PG cache (TTL-aware)
   HIT → return from PG (single source: direct, multi: Trino)
   STALE → return stale, background refresh
   MISS → call API, flatten (primitives + JSONB), return immediately
          background: write to PG cache
          multi-source: write PG first (blocking), then Trino joins
6. Response header: X-Provisa-Api-Cache: HIT|MISS|STALE
```

**Verify:**
- `python -m pytest tests/unit/test_api_introspect.py -x -q`:
  - OpenAPI spec → candidate tables with correct columns and filterability
  - GraphQL introspection → candidate tables from root query fields
  - gRPC reflection → candidate tables from RPC methods
  - Primitive types → native columns, complex types → JSONB
- `python -m pytest tests/unit/test_api_caller.py -x -q`:
  - Query/body/path/header param assembly
  - GraphQL variable dot-path assembly
  - Pagination following (cursor, link header)
- `python -m pytest tests/unit/test_api_flattener.py -x -q`:
  - Primitives → native values, objects/arrays → JSONB
  - Constant injection
  - Transform application
- `python -m pytest tests/unit/test_api_cache.py -x -q`:
  - Cache key, TTL resolution, stale detection
  - JSONB column in WHERE → error
  - JSONB column in relationship → error
- `python -m pytest tests/integration/test_api_candidates.py -x -q`:
  - Discover/register/reject candidates in PG
  - Accept creates cache table DDL
- `python -m pytest tests/unit/test_api_promotions.py -x -q`:
  - Dot-path → PG expression generation (`a.b.c` → `col->'a'->'b'->>'c'`)
  - DDL generation with GENERATED ALWAYS AS clause
  - Type cast validation (TEXT, INTEGER, NUMERIC, BOOLEAN, TIMESTAMPTZ)
  - Promoted columns appear as filterable in schema
  - Promoted columns eligible for relationships
- `python -m pytest tests/e2e/test_api_source.py -x -q`:
  - Register OpenAPI source → discover candidates → register → appears in SDL
  - Filterable primitive in WHERE → API called, results returned
  - JSONB column in WHERE → 400 error
  - Cache HIT/MISS/STALE behavior
  - Cross-source join via Trino
  - JSONB field promotion: promoted column filterable in WHERE, joins work
  - Promoted column auto-updated when JSONB value changes (PG generated column)

**Files:**
| File | Action |
|---|---|
| `provisa/api_source/__init__.py` | Create |
| `provisa/api_source/models.py` | Create |
| `provisa/api_source/introspect.py` | Create |
| `provisa/api_source/candidates.py` | Create |
| `provisa/api_source/caller.py` | Create |
| `provisa/api_source/flattener.py` | Create |
| `provisa/api_source/cache.py` | Create |
| `provisa/api_source/router_integration.py` | Create |
| `provisa/api_source/schema_integration.py` | Create |
| `provisa/api_source/transforms.py` | Create |
| `provisa/api_source/promotions.py` | Create (JSONB field promotion DDL generation + index creation) |
| `provisa/api/admin/api_discovery.py` | Create (admin endpoints for discovery/registration) |
| `provisa/core/models.py` | Modify |
| `provisa/compiler/schema_gen.py` | Modify |
| `provisa/transpiler/router.py` | Modify |
| `provisa/api/data/endpoint.py` | Modify |
| `provisa/core/schema.sql` | Modify (add api_endpoint_candidates table) |
| `tests/unit/test_api_introspect.py` | Create |
| `tests/unit/test_api_caller.py` | Create |
| `tests/unit/test_api_flattener.py` | Create |
| `tests/unit/test_api_cache.py` | Create |
| `tests/integration/test_api_candidates.py` | Create |
| `tests/unit/test_api_promotions.py` | Create |
| `tests/e2e/test_api_source.py` | Create |

---

## Phase V: Kafka Sources & Sink
**Goal:** Kafka topics as read-only data sources (each message type → table) and query results publishable to Kafka topics. Trino has a native Kafka connector for reads. For sink, use the Kafka Python producer.
**REQs:** REQ-114, REQ-115, REQ-116, REQ-117

**Build:**
- `provisa/kafka/__init__.py`
- `provisa/kafka/source.py` — Kafka topic registration as data source (REQ-114):
  - Register a Kafka cluster (bootstrap servers, auth)
  - Each topic + schema → registered table. Schema from: Schema Registry (Avro/Protobuf/JSON Schema), manual JSON schema, or sample message inference.
  - Trino Kafka connector handles reads: topic messages → rows with columns from schema
  - Primitives → native columns, complex nested → JSONB (same rules as API sources)
  - Complex objects: NOT filterable, NO relationships
  - Trino Kafka connector config auto-generated: `kafka.table-names`, `kafka.topic-session-properties`
- `provisa/kafka/sink.py` — publish query results to Kafka topics (REQ-115):
  - New output format: `Accept: application/x-kafka` or via approved query config `output: kafka`
  - After query execution, serialize result rows as JSON messages → produce to configured topic
  - Topic + key config per approved query: `{ topic: "order-updates", key_column: "id" }`
  - Async production — don't block response (fire-and-forget with delivery callback)
- `provisa/kafka/schema_registry.py` — Schema Registry integration (REQ-116):
  - Fetch Avro/Protobuf/JSON Schema from Confluent Schema Registry
  - Auto-map schema fields to column definitions (same primitive/JSONB rules)
  - Schema evolution: detect changes, flag affected registered tables for re-review
- Update Trino configmap/catalog for Kafka connector
- Config in YAML too complex for UI → admin UI provides raw YAML editor for Kafka config (REQ-117)

**Config:**
```yaml
kafka_sources:
  - id: event-stream
    bootstrap_servers: kafka:9092
    schema_registry_url: http://schema-registry:8081  # optional
    auth:
      type: sasl_plain
      username: ${env:KAFKA_USER}
      password: ${env:KAFKA_PASS}
    
    topics:
      - id: order-events
        topic: orders.events.v1
        schema_source: registry    # or: manual, sample
        value_format: avro         # or: json, protobuf
        # Columns auto-discovered from schema
        # Complex fields → JSONB, primitives → native columns

kafka_sinks:
  - query_stable_id: "abc-123"    # approved query
    topic: enriched-orders
    key_column: order_id
    value_format: json
```

**Verify:**
- `python -m pytest tests/unit/test_kafka_schema.py` — schema → column mapping
- `python -m pytest tests/unit/test_kafka_sink.py` — result serialization to Kafka message format
- `python -m pytest tests/integration/test_kafka_source.py` — read from Kafka via Trino
- `python -m pytest tests/e2e/test_kafka_flow.py` — query → result published to topic

**Files:**
| File | Action |
|---|---|
| `provisa/kafka/__init__.py` | Create |
| `provisa/kafka/source.py` | Create |
| `provisa/kafka/sink.py` | Create |
| `provisa/kafka/schema_registry.py` | Create |
| `trino/catalog/kafka.properties` | Create |
| `docker-compose.yml` | Modify (add Kafka + Schema Registry) |
| `tests/unit/test_kafka_schema.py` | Create |
| `tests/unit/test_kafka_sink.py` | Create |
| `tests/integration/test_kafka_source.py` | Create |
| `tests/e2e/test_kafka_flow.py` | Create |

---

## Phase W: Authentication
**Goal:** Pluggable authentication supporting Firebase (all methods), Keycloak (OIDC/SAML), generic OAuth 2.0 (PingFederate, Okta, Azure AD, Auth0), and a simple username/password mode for local development/testing. Superuser credentials in config for bootstrap access.
**REQs:** REQ-120, REQ-121, REQ-122, REQ-123, REQ-124, REQ-125

**Architecture:**
- Pluggable auth provider interface — one backend at a time, configured in YAML
- All providers produce a standard `AuthIdentity` (user_id, email, roles, claims) that maps to Provisa roles
- Role resolution: identity claims → Provisa role mapping rules in config
- If no auth configured: assume superuser with admin role + all capabilities (current behavior)

**Build:**
- `provisa/auth/__init__.py`
- `provisa/auth/models.py` — `AuthIdentity` dataclass, `AuthProvider` abstract base (REQ-120):
  - `AuthIdentity`: user_id, email, display_name, roles (list of Provisa role IDs), raw_claims (dict)
  - `AuthProvider`: abstract `authenticate(request) → AuthIdentity`, `validate_token(token) → AuthIdentity`
- `provisa/auth/providers/firebase.py` — Firebase Authentication (REQ-121):
  - Validates Firebase ID tokens via `firebase-admin` SDK
  - Supports all Firebase auth methods: email/password, Google, Apple, GitHub, phone, anonymous, SAML, OIDC
  - Token validation: `firebase_admin.auth.verify_id_token(token)`
  - Custom claims in Firebase → role mapping
  - Config: `project_id`, `service_account_key` (secret)
- `provisa/auth/providers/keycloak.py` — Keycloak OIDC (REQ-122):
  - Validates JWT access tokens from Keycloak
  - OIDC discovery: `{server_url}/realms/{realm}/.well-known/openid-configuration`
  - JWKS-based token validation (no Keycloak SDK dependency — pure JWT)
  - Realm roles + client roles → Provisa role mapping
  - Config: `server_url`, `realm`, `client_id`, `client_secret` (optional for public clients)
- `provisa/auth/providers/oauth.py` — Generic OAuth 2.0 / OIDC (REQ-123):
  - Works with any OIDC-compliant provider: PingFederate, Okta, Azure AD, Auth0, Google Workspace
  - OIDC discovery URL → auto-fetch JWKS, issuer, token endpoint
  - JWT validation with JWKS rotation support (cached with TTL)
  - Configurable claims mapping: which claim holds roles/groups
  - Config: `discovery_url`, `client_id`, `client_secret`, `role_claim` (default: "roles"), `audience`
- `provisa/auth/providers/simple.py` — Simple username/password for testing (REQ-124):
  - Users defined in config YAML: `{username, password_hash, roles}`
  - bcrypt password hashing
  - Issues short-lived JWT signed with `PROVISA_JWT_SECRET` env var
  - Login endpoint: `POST /auth/login` → JWT
  - NOT for production — config flag `allow_simple_auth: true` required
- `provisa/auth/superuser.py` — Superuser bootstrap access (REQ-125):
  - Superuser credentials in config: `superuser: { username: "admin", password: ${env:PROVISA_SUPERUSER_PASSWORD} }`
  - Always has admin role + all capabilities regardless of provider
  - Works with any auth provider — special-cased at identity resolution
  - Can be used for initial setup before configuring external auth
- `provisa/auth/middleware.py` — FastAPI middleware:
  - Extracts `Authorization: Bearer <token>` header
  - Calls active provider's `validate_token()`
  - Resolves identity → Provisa roles via role mapping
  - Sets `request.state.identity` and `request.state.role` (replaces `X-Role` header)
  - If no auth configured: sets default admin identity (backward compatible)
  - 401 on invalid/expired token, 403 on insufficient role
- `provisa/auth/role_mapping.py` — Identity → Provisa role resolution:
  - Mapping rules in config: `{ claim: "department", value: "engineering", role: "analyst" }`
  - Wildcard: `{ claim: "groups", contains: "data-stewards", role: "steward" }`
  - Default role for authenticated users with no matching rule
  - Superuser check before rule evaluation

**Config:**
```yaml
auth:
  provider: firebase  # firebase | keycloak | oauth | simple | none
  
  # Superuser — works with any provider
  superuser:
    username: admin
    password: ${env:PROVISA_SUPERUSER_PASSWORD}
  
  # Firebase (existing project: simpleishard-3d847, Google auth already enabled)
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}  # simpleishard-3d847
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT}  # optional — only for admin SDK server-side ops
  
  # Keycloak
  keycloak:
    server_url: https://keycloak.example.com
    realm: provisa
    client_id: provisa-app
    client_secret: ${env:KEYCLOAK_CLIENT_SECRET}
  
  # Generic OAuth 2.0 / OIDC (PingFed, Okta, Azure AD, Auth0)
  oauth:
    discovery_url: https://login.example.com/.well-known/openid-configuration
    client_id: provisa
    client_secret: ${env:OAUTH_CLIENT_SECRET}
    role_claim: groups     # which JWT claim holds role info
    audience: provisa-api
  
  # Simple (testing only)
  simple:
    allow: true
    jwt_secret: ${env:PROVISA_JWT_SECRET}
    users:
      - username: admin
        password_hash: "$2b$12$..."
        roles: [admin]
      - username: analyst
        password_hash: "$2b$12$..."
        roles: [analyst]

  # Role mapping — maps identity claims to Provisa roles
  role_mapping:
    - claim: custom_claims.role
      value: admin
      provisa_role: admin
    - claim: groups
      contains: data-analysts
      provisa_role: analyst
    - claim: groups
      contains: data-stewards
      provisa_role: steward
    default_role: analyst  # fallback for authenticated users with no match
```

**UI integration:**
- Login page: provider-specific UI (Firebase UI, redirect to Keycloak/OAuth, simple login form)
- Token stored in memory (not localStorage) for security
- Auto-refresh before expiry
- Role displayed in navbar (from resolved identity, not manual selection)
- Manual role selector only available to superuser/admin (for testing other roles' views)

**Verify:**
- `python -m pytest tests/unit/test_auth_models.py -x -q`:
  - AuthIdentity creation with claims
  - Role mapping: exact match, contains, wildcard, default
  - Superuser always resolves to admin
- `python -m pytest tests/unit/test_auth_simple.py -x -q`:
  - Login with valid credentials → JWT
  - Login with wrong password → 401
  - JWT validation + expiry
- `python -m pytest tests/unit/test_auth_middleware.py -x -q`:
  - No auth configured → admin identity (backward compat)
  - Valid token → correct identity + role on request
  - Invalid token → 401
  - Missing required capability → 403
- `python -m pytest tests/integration/test_auth_providers.py -x -q`:
  - Simple provider end-to-end: login → token → authenticated request
  - Role mapping integration: claims → Provisa role → capability check
  - Superuser bypass: superuser creds → admin regardless of mapping

**Files:**
| File | Action |
|---|---|
| `provisa/auth/__init__.py` | Create |
| `provisa/auth/models.py` | Create |
| `provisa/auth/providers/__init__.py` | Create |
| `provisa/auth/providers/firebase.py` | Create |
| `provisa/auth/providers/keycloak.py` | Create |
| `provisa/auth/providers/oauth.py` | Create |
| `provisa/auth/providers/simple.py` | Create |
| `provisa/auth/superuser.py` | Create |
| `provisa/auth/middleware.py` | Create |
| `provisa/auth/role_mapping.py` | Create |
| `provisa/api/app.py` | Modify (add auth middleware) |
| `provisa/api/data/endpoint.py` | Modify (use request.state.role instead of X-Role header) |
| `provisa/api/data/sdl.py` | Modify (use request.state.role) |
| `provisa/core/models.py` | Modify (add auth config models) |
| `config/provisa.yaml` | Modify (add auth section) |
| `provisa-ui/src/context/AuthContext.tsx` | Modify (real auth flow) |
| `provisa-ui/src/pages/LoginPage.tsx` | Create |
| `tests/unit/test_auth_models.py` | Create |
| `tests/unit/test_auth_simple.py` | Create |
| `tests/unit/test_auth_middleware.py` | Create |
| `tests/integration/test_auth_providers.py` | Create |

---

## Phase X: JDBC Driver Arrow Flight Transport
**Goal:** Wire the JDBC driver to use Arrow Flight (`grpc://host:8815`) for streaming query results instead of HTTP + JSON/Arrow file download. The Flight server already exists on the backend (`provisa/api/flight/server.py`) — this phase connects the JDBC client to it.
**REQs:** REQ-293

**Motivation:**
The current JDBC driver executes queries over HTTP, buffering the full response before returning rows. The backend already has a Flight server that streams Arrow record batches with backpressure and zero serialization overhead. Connecting the two eliminates the HTTP round-trip bottleneck and enables true streaming from the first row.

For single-source (direct route) queries, the Flight server executes against the source database and streams Arrow batches. For Trino-routed queries, the Flight server can use the Zaychik Flight SQL proxy for end-to-end Arrow streaming without materializing in Provisa memory.

**Build:**
- Add `org.apache.arrow:arrow-flight` and `org.apache.arrow:flight-core` dependencies to `jdbc-driver/pom.xml`
- `jdbc-driver/src/main/java/io/provisa/jdbc/FlightTransport.java` — Arrow Flight client wrapper:
  - Connects to `grpc://host:8815` (port from connection property or default)
  - Builds Flight ticket as JSON: `{"query": queryText, "role": roleId, "variables": {...}}`
  - Calls `doGet(ticket)` and returns a `FlightStream` wrapping the record batches
  - Connection pooling / channel reuse across statements
- Update `ProvisaConnection.java`:
  - On connect: attempt Flight connection to `grpc://host:8815` (port derived from HTTP port + offset or default)
  - If Flight connects: hold a shared `FlightClient` for the connection lifetime
  - If Flight fails: log and proceed with HTTP-only (silent fallback, no error)
  - Close FlightClient on connection close
- Update `ProvisaStatement.java`:
  - If FlightClient is available: build ticket JSON, call FlightTransport, wrap result in `ArrowStreamResultSet`
  - If FlightClient is null: use existing HTTP path
  - No user configuration — transport is an internal implementation detail
- Update `ProvisaDatabaseMetaData.java`:
  - Metadata operations (getTables, getColumns, PK/FK) still use HTTP/GraphQL — Flight is for query data only
- `FlightStreamResultSet.java` — Wraps `FlightStream` as a JDBC `ResultSet`:
  - Reuses `ArrowStreamResultSet` logic but reads from Flight stream instead of InputStream
  - Batch-by-batch consumption with memory bounded to one batch at a time

**Connection string examples:**
```
jdbc:provisa://localhost:8001?mode=approved
jdbc:provisa://localhost:8001?mode=catalog
```
Flight is used automatically — no transport property needed.

**Verify:**
- `mvn test` — unit tests with mocked FlightClient:
  - Flight ticket JSON construction
  - Automatic fallback when Flight unavailable
  - FlightStreamResultSet batch navigation
- `mvn verify` — integration tests against live backend:
  - Flight transport: connect, execute approved query, stream results
  - Verify row-level streaming (first row available before full result)
  - HTTP fallback when Flight port is unavailable
  - Both modes (approved, catalog) work with Flight transport

**Files:**
| File | Action |
|---|---|
| `jdbc-driver/pom.xml` | Modify (add arrow-flight dependencies) |
| `jdbc-driver/src/main/java/io/provisa/jdbc/FlightTransport.java` | Create |
| `jdbc-driver/src/main/java/io/provisa/jdbc/FlightStreamResultSet.java` | Create |
| `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaConnection.java` | Modify (transport selection, FlightClient lifecycle) |
| `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaStatement.java` | Modify (Flight execution path) |
| `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaDriver.java` | Modify (version bump) |
| `jdbc-driver/src/test/java/io/provisa/jdbc/FlightTransportTest.java` | Create |
| `jdbc-driver/src/test/java/io/provisa/jdbc/FlightTransportIT.java` | Create |

---

## Phase Y: Admin Page Global Features
**Goal:** Expand the admin page with platform-wide management surfaces: MV lifecycle, global cache controls, system health, and scheduled task overview.
**REQs:** REQ-081 (MV admin), REQ-077 (cache admin)

**Build:**
- `provisa-ui/src/components/admin/MVManager.tsx` — MV management panel:
  - List all MVs with status (fresh/stale/refreshing/disabled), last refresh time, row count
  - Enable/disable toggle per MV
  - Manual refresh button (POST `/admin/mv/{id}/refresh`)
  - Last error display for failed refreshes
- `provisa-ui/src/components/admin/CacheManager.tsx` — Global cache controls:
  - Display cache stats: total keys, memory usage, hit/miss ratio (from Redis INFO)
  - Purge all cache button (POST `/admin/cache/purge`)
  - Purge by table button (POST `/admin/cache/purge/{table_id}`)
  - Global default TTL editor (already exists in Platform Settings, link here)
- `provisa-ui/src/components/admin/SystemHealth.tsx` — System health panel:
  - Trino connection status and uptime
  - PG pool stats (active/idle/waiting connections)
  - Redis connection status
  - Background task status (MV refresh loop, scheduled triggers)
- `provisa-ui/src/components/admin/ScheduledTasks.tsx` — Scheduled task overview:
  - List cron-scheduled tasks (triggers, MV refreshes) with next run time
  - Last run status per task
  - Enable/disable toggle
- Backend endpoints:
  - `provisa/api/admin/schema.py` — Add Strawberry mutations: `refreshMV(id)`, `toggleMV(id, enabled)`, `purgeCache()`, `purgeCacheByTable(tableId)`
  - `provisa/api/admin/schema.py` — Add Strawberry queries: `mvList`, `cacheStats`, `systemHealth`, `scheduledTasks`
- `provisa-ui/src/pages/AdminPage.tsx` — Add tabs/sections for MV Manager, Cache Manager, System Health, Scheduled Tasks

**Verify:**
- `npx vitest run` — component unit tests for each admin panel
- `npx playwright test` — E2E:
  - Admin page loads all new sections
  - MV list shows correct status, enable/disable toggles work
  - Manual MV refresh triggers and updates status
  - Cache purge clears entries, stats update
  - System health shows live connection statuses
- `python -m pytest tests/unit/test_admin_mv.py -x -q` — MV admin mutations
- `python -m pytest tests/e2e/test_admin_flow.py -x -q` — full admin workflow

**Files:**
| File | Action |
|---|---|
| `provisa-ui/src/components/admin/MVManager.tsx` | Create |
| `provisa-ui/src/components/admin/CacheManager.tsx` | Create |
| `provisa-ui/src/components/admin/SystemHealth.tsx` | Create |
| `provisa-ui/src/components/admin/ScheduledTasks.tsx` | Create |
| `provisa-ui/src/pages/AdminPage.tsx` | Modify (add new sections) |
| `provisa/api/admin/schema.py` | Modify (add MV/cache/health queries+mutations) |
| `tests/unit/test_admin_mv.py` | Create |

---

## Phase Z: Per-Source & Per-Table Cache Configuration
**Goal:** Granular cache control at the source and table level, exposed in both backend config and UI. Overrides the global default TTL with source-level and table-level specificity.
**REQs:** REQ-077 (cache control), REQ-078 (security partitioning)

**Build:**
- `provisa/core/models.py` — Extend models:
  - `Source`: add `cache_enabled: bool = True`, `cache_ttl: int | None = None` (overrides global default)
  - `Table` (table config): add `cache_ttl: int | None = None` (overrides source-level)
- `provisa/cache/policy.py` — Update `resolve_policy()` to check table TTL → source TTL → global default (most specific wins)
- `provisa/core/schema.sql` — Add `cache_enabled BOOLEAN DEFAULT TRUE` and `cache_ttl INTEGER` columns to `sources` and `tables` tables
- `config/provisa.yaml` — Add cache fields to source and table config examples:
  ```yaml
  sources:
    - id: sales-pg
      cache_enabled: true
      cache_ttl: 600  # 10 min override for this source
  tables:
    - source_id: sales-pg
      table: orders
      cache_ttl: 60   # 1 min for frequently-changing table
  ```
- `provisa-ui/src/pages/SourcesPage.tsx` — Add cache options to source detail/edit:
  - Cache enabled toggle
  - Cache TTL override input (blank = inherit global)
  - Display effective TTL (resolved: table → source → global)
- `provisa-ui/src/pages/TablesPage.tsx` — Add cache options to table detail/edit:
  - Cache TTL override input (blank = inherit source/global)
  - Manual cache invalidation button per table (POST `/admin/cache/purge/{table_id}`)
  - Display effective TTL with inheritance chain shown
- `provisa/api/admin/schema.py` — Add `updateSourceCache(sourceId, enabled, ttl)` and `updateTableCache(tableId, ttl)` mutations
- `provisa/api/app.py` — Load source/table cache config into state during startup

**Key behaviors:**
- TTL resolution order: table-level → source-level → global default. First non-null wins.
- `cache_enabled: false` on a source disables caching for ALL tables in that source regardless of table-level TTL.
- Cache keys still include role_id + RLS context (REQ-078) — this phase only adds TTL granularity.
- UI shows the effective (resolved) TTL alongside any override, so stewards understand the inheritance.

**Verify:**
- `python -m pytest tests/unit/test_cache_policy.py -x -q`:
  - Table TTL overrides source TTL
  - Source TTL overrides global TTL
  - Source cache_enabled=false → no caching regardless of TTLs
  - No overrides → global default used
- `npx vitest run` — component tests for cache UI on source/table pages
- `npx playwright test` — E2E:
  - Set source cache TTL → queries against that source use it
  - Set table cache TTL → overrides source TTL
  - Disable cache on source → no caching for its tables
  - Manual invalidation button clears table cache

**Files:**
| File | Action |
|---|---|
| `provisa/core/models.py` | Modify (add cache fields to Source, Table) |
| `provisa/cache/policy.py` | Modify (hierarchical TTL resolution) |
| `provisa/core/schema.sql` | Modify (add cache columns) |
| `config/provisa.yaml` | Modify (add cache config examples) |
| `provisa-ui/src/pages/SourcesPage.tsx` | Modify (add cache options) |
| `provisa-ui/src/pages/TablesPage.tsx` | Modify (add cache options) |
| `provisa/api/admin/schema.py` | Modify (add cache mutations) |
| `provisa/api/app.py` | Modify (load cache config) |
| `tests/unit/test_cache_policy.py` | Modify (add hierarchical TTL tests) |

---

## Dependencies

```toml
[project]
dependencies = [
    "fastapi",
    "uvicorn",
    "asyncpg",
    "pydantic>=2",
    "strawberry-graphql[fastapi]",
    "pyyaml",
    "graphql-core>=3.2",
    "sqlglot",
    "trino",
    "grpcio",
    "pyarrow",
    "httpx",      # Trino catalog API client
    "redis[hiredis]",  # Query result caching (Phase O)
    "anthropic",       # LLM relationship discovery (Phase R)
    "jsonpath-ng",     # JSONPath extraction for API source flattening (Phase U)
    "grpcio-tools",    # Proto compilation for gRPC endpoint (Phase S)
    "grpcio-reflection",  # gRPC server reflection (Phase S)
    "firebase-admin",     # Firebase auth (Phase W)
    "PyJWT[crypto]",      # JWT validation for Keycloak/OAuth/simple (Phase W)
    "bcrypt",             # Password hashing for simple auth (Phase W)
]

[project.optional-dependencies]
dev = [
    "pytest",
    "pytest-asyncio",
    "ruff",
    "black",
]
```

## Testing Strategy

### Test Tiers
- **Unit** (`tests/unit/`) — no external dependencies, fast, mock everything external. Run on every commit.
- **Integration** (`tests/integration/`) — require Docker Compose stack (PG + Trino). Marked `@pytest.mark.integration`. Test real DB operations, Trino queries, catalog API.
- **E2E API** (`tests/e2e/`) — full HTTP requests against running Provisa app via `httpx.AsyncClient` or `TestClient`. Test complete request→response pipelines. Marked `@pytest.mark.e2e`. Phases D–K.
- **E2E UI** (`provisa-ui/e2e/`) — Playwright browser tests against running Provisa app + UI. Test user-facing workflows through the browser. Phase N only.

### Test Infrastructure (created in Phase A, expanded each phase)

| File | Purpose |
|---|---|
| `tests/__init__.py` | Package init |
| `tests/conftest.py` | Shared fixtures: PG pool, Trino client, test config, app client |
| `tests/fixtures/sample_config.yaml` | Minimal valid config for tests (sources, domains, tables, roles, RLS) |
| `tests/fixtures/registration_model.py` | Factory functions for building registration models (tables, columns, relationships, roles) |
| `tests/fixtures/trino_metadata.py` | Mock Trino INFORMATION_SCHEMA responses (column types, FKs) |
| `tests/fixtures/graphql_queries.py` | Sample GraphQL queries + expected SQL output pairs |
| `tests/fixtures/sql_results.py` | Sample SQL result row sets for serializer tests |

### `conftest.py` Fixtures
```python
# Session-scoped (expensive setup)
@pytest.fixture(scope="session")
def pg_pool():           # asyncpg pool → test PG
@pytest.fixture(scope="session")
def trino_client():      # trino.dbapi.connect → test Trino
@pytest.fixture(scope="session")
def app_client():        # httpx.AsyncClient against FastAPI app

# Function-scoped (clean state)
@pytest.fixture
def sample_config():     # parsed config dict from sample_config.yaml
@pytest.fixture
def registration_model():  # populated registration model in PG
@pytest.fixture
def schema_for_role():   # generated graphql-core schema for a given role
```

### Per-Phase Test Additions

| Phase | Unit Tests | Integration Tests | E2E Tests |
|-------|-----------|-------------------|-----------|
| A | None | `tests/integration/test_infra.py` — PG connects, Trino queries PG, sample data exists | None |
| B | `tests/unit/test_models.py` — Pydantic validation, `test_secrets.py` — env var resolution | `tests/integration/test_config_loader.py` — YAML→PG round-trip, `test_repositories.py` — CRUD against real PG, `test_catalog.py` — Trino catalog API | None |
| C | `tests/unit/test_type_map.py`, `test_naming.py` — pure logic, no DB | `tests/integration/test_schema_gen.py` — schema gen from real Trino metadata, `test_introspect.py` — real INFORMATION_SCHEMA queries | None |
| D | `tests/unit/test_sql_gen.py` — AST→SQL with fixture pairs, `test_params.py`, `test_serialize.py` — rows→JSON | `tests/integration/test_transpile.py` — SQLGlot PG→Trino | `tests/e2e/test_query_pipeline.py` — POST /data/graphql → JSON response, field selection, filters, joins, pagination |
| E | `tests/unit/test_router.py` — routing decision logic | `tests/integration/test_direct_exec.py` — direct PG execution, `test_pool.py` — pool lifecycle | `tests/e2e/test_routing.py` — verify single-source routes direct, multi-source routes Trino |
| F | `tests/unit/test_rls.py`, `test_visibility.py`, `test_rights.py` | None (unit tests cover security logic with mocked models) | `tests/e2e/test_security.py` — query as different roles, verify RLS applied, columns hidden |
| G | `tests/unit/test_mutation_sql.py` — mutation AST→SQL | None | `tests/e2e/test_mutations.py` — INSERT/UPDATE/DELETE via GraphQL, verify DB state |
| H | `tests/unit/test_ceiling.py`, `test_governance.py` — logic tests | `tests/integration/test_registry.py` — store/retrieve/approve/deprecate in PG | `tests/e2e/test_registry_flow.py` — submit→approve→execute→deprecate full flow |
| I | `tests/unit/test_formats.py` — serialization logic | None | `tests/e2e/test_output_formats.py` — request with Accept headers, verify NDJSON/Parquet/Arrow |
| J | `tests/unit/test_redirect.py` — threshold logic | `tests/integration/test_blob_upload.py` — S3 upload/presign | `tests/e2e/test_large_result.py` — large query → presigned URL |
| K | None | `tests/integration/test_admin_api.py` — CRUD via Strawberry | `tests/e2e/test_admin_flow.py` — create source → register table → schema regenerated |
| N | None | None | `provisa-ui/e2e/` — Playwright: role-based rendering, registration workflow, query builder, approval queue, confirmation dialogs, Voyager schema explorer. `tests/e2e/test_sdl.py` — SDL endpoint returns valid SDL per role |
| O | `tests/unit/test_cache_key.py` — key generation with RLS, `test_cache_policy.py` — policy logic | `tests/integration/test_cache_store.py` — Redis round-trip, TTL, invalidation | `tests/e2e/test_caching.py` — HIT/MISS, role partitioning, invalidation on mutation/registration change |
| P | `tests/unit/test_mv_rewriter.py` — pattern matching, rewrite logic, partial match, `test_mv_registry.py` — MV lifecycle | `tests/integration/test_mv_refresh.py` — CTAS via Trino, atomic refresh | `tests/e2e/test_mv_optimization.py` — transparent optimization, stale bypass, RLS on MV |
| Q | `tests/unit/test_masking.py` — expression generation, type validation, `test_mask_inject.py` — SELECT rewriting, alias handling | None | `tests/e2e/test_masking.py` — per-role masking, regex/constant/truncate, filter on masked column |
| R | `tests/unit/test_discovery_prompt.py` — prompt construction per scope, `test_discovery_analyzer.py` — response parsing, validation, filtering | `tests/integration/test_discovery_candidates.py` — PG CRUD, accept→relationship creation | `tests/e2e/test_discovery_flow.py` — trigger discovery, accept/reject candidates |
| S | `tests/unit/test_proto_gen.py` — message/service generation, per-role filtering, `test_grpc_server.py` — request→SQL→result | `tests/integration/test_grpc_reflection.py` — reflection lists services | `tests/e2e/test_grpc_query.py` — streamed queries, mutations, role enforcement |
| U | `tests/unit/test_api_caller.py` — param assembly, body injection, pagination; `test_api_flattener.py` — JSONPath, transforms; `test_api_cache.py` — key gen, TTL, filterable validation | `tests/integration/test_api_cache_pg.py` — PG round-trip, TTL, upsert | `tests/e2e/test_api_source.py` — SDL generation, cache HIT/MISS, cross-source join, non-filterable error |

### Sample Data

| File | Contents |
|---|---|
| `db/init.sql` | Demo schema: `orders` (id, customer_id, amount, region, created_at), `customers` (id, name, email), `products` (id, name, price). 20-50 rows each. FK: orders.customer_id → customers.id |
| `tests/fixtures/sample_config.yaml` | Config registering demo PG source, 2 domains, 3 tables, 2 roles (admin, analyst), 1 RLS rule, 1 relationship, naming rules |
| `tests/fixtures/large_result_data.sql` | INSERT script generating 10K+ rows for large result redirect testing (Phase J) |

### Pytest Markers
```python
# pyproject.toml
[tool.pytest.ini_options]
markers = [
    "integration: requires Docker Compose stack (PG + Trino)",
    "e2e: full HTTP pipeline tests against running app",
    "slow: long-running tests",
]
```

### Running Tests
```bash
python -m pytest tests/unit/ -x -q                       # unit only (fast, no deps)
python -m pytest tests/ -x -q -m integration             # integration (needs docker compose up)
python -m pytest tests/ -x -q -m e2e                     # API e2e (needs docker compose up + app)
python -m pytest tests/ -x -q                            # all backend tests
cd provisa-ui && npx playwright test                     # UI e2e (needs full stack + UI running)
```

---

## Cross-Cutting Requirements (All Phases)

- **REQ-064**: Never add fallback values or silent error handling — all errors explicit, fail-fast
- **REQ-065**: No migrations in v1 — schema.sql is the source of truth
- **REQ-069**: Architecture docs in `docs/arch/` ARE the planning documents
- **REQ-070**: Maximum brevity in communications
- **REQ-071**: Requirements tracked via requirements-tracker agent
- **REQ-075**: Secrets (database passwords, API keys) resolved via pluggable provider interface — env vars for v1, extensible to Vault, K8s secrets, AWS Secrets Manager. Secrets never stored in config DB or committed to source.
- **REQ-126**: Flight SQL/JDBC connector mode parameter — `mode=catalog` exposes the user's visible semantic layer as a read-only JDBC catalog (domains as schemas, tables as tables, columns with descriptions) for external tool integration (e.g. reasoning agents building query plans). `mode=approved` exposes only persisted approved queries as virtual tables. Default mode retains current full-execution behavior. Mode is a connection property in the Flight SQL handshake. The catalog mode maps: JDBC catalog = Provisa instance, JDBC schema = domain, JDBC table = registered table/view. This enables any JDBC-compatible tool to discover Provisa's governed data model without Provisa-specific knowledge.

---

## Hasura v2 Parity & Installer Phases (AA–AH)

### Context

New Provisa features for Hasura v2 parity, installer packaging, then migration converters. Requirements: REQ-182 through REQ-229 (plus REQ-293, REQ-294).

**Priority**: Low-complexity features first (quick wins) -> medium-complexity features -> installer -> v2 converter -> DDN converter.

**Phase naming**: Continues after Phase Z. Former "Phase O–V" in parity plan renamed AA–AH to avoid collision with existing phases.

**Gate rules**: Every phase ends with a verification gate (tests pass, feature works end-to-end) and a documentation gate (docs/configuration.md updated, CHANGELOG entry, API reference if applicable).

---

## Phase AA: Quick Wins -- Low-Complexity Features

Features that are essentially a new arg/flag + small SQL/config extension. Each ~100-200 lines.

### AA1. Direct-Route Dialect Expansion (REQ-229)

Add missing entries to `SOURCE_TO_DIALECT` and `SOURCE_TO_CONNECTOR` in `models.py`:

```python
# New SOURCE_TO_DIALECT entries
"clickhouse": "clickhouse",
"mariadb": "mysql",
"singlestore": "singlestore",
"redshift": "redshift",
"databricks": "databricks",
"hive": "hive",
"druid": "druid",
"exasol": "exasol",
```

Three-part checklist per source (all must be true for direct-route):

| Source | SQLGlot Dialect | Trino Connector | `SOURCE_TO_CONNECTOR` | `SOURCE_TO_DIALECT` |
|--------|----------------|-----------------|----------------------|---------------------|
| clickhouse | `clickhouse` | `clickhouse` | add | add |
| mariadb | `mysql` | `mariadb` | exists | add |
| singlestore | `singlestore` | `singlestore` | add | add |
| redshift | `redshift` | `redshift` | add | add |
| databricks | `databricks` | `delta_lake` | add | add |
| hive | `hive` | `hive` | add | add |
| druid | `druid` | `druid` | add | add |
| exasol | `exasol` | `exasol` | add | add |

Also verify each Trino connector plugin is packaged in Docker Compose / Helm Trino deployment.

**Files**: `provisa/core/models.py`, `docker-compose.yml` or Trino catalog configs
**Effort**: ~20 lines Python + Trino catalog config per source

### AA2. Upsert Mutations (REQ-212)

New `upsert_<table>` mutation field in `mutation_gen.py`. Generates `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`. Conflict columns from PK metadata (already available in TableMeta). SQLGlot transpiles to MySQL/MSSQL syntax.

**Files**: `provisa/compiler/mutation_gen.py`, `provisa/compiler/schema_gen.py`
**Effort**: ~150 lines

### AA3. DISTINCT ON (REQ-213)

Add `distinct_on` arg to root query fields. In `sql_gen.py`, prepend `DISTINCT ON (col)` to SELECT. SQLGlot provides window function fallback for non-PG dialects.

**Files**: `provisa/compiler/sql_gen.py`, `provisa/compiler/schema_gen.py`
**Effort**: ~100 lines

### AA4. Column Presets (REQ-214)

Config per table: `column_presets` list. Middleware in mutation compilation injects values from headers/session/`now()` before SQL generation. Mirrors existing masking middleware pattern.

**Files**: `provisa/core/models.py` (config), `provisa/compiler/mutation_gen.py` (injection)
**Effort**: ~200 lines

### AA5. Inherited Roles (REQ-215)

Add optional `parent_role_id` to Role model. Flatten at startup: recurse and merge capabilities + domain_access. Cache flattened roles in AppState. Lookups remain O(1).

**Files**: `provisa/core/models.py`, `provisa/core/schema.sql`, `provisa/api/app.py` (startup)
**Effort**: ~100 lines

### AA6. Scheduled Triggers (REQ-216)

Add `apscheduler` dependency. Config section `scheduled_triggers` with cron expressions. Wire into FastAPI lifespan. Reuses existing async background task pattern (MV refresh).

**Files**: `provisa/core/models.py`, `provisa/api/app.py`, new `provisa/scheduler/jobs.py`
**New dependency**: `apscheduler`
**Effort**: ~100 lines

### AA7. Document Batch Mutations (REQ-217)

Already works -- GraphQL spec executes multiple mutations sequentially. Just document.

**Effort**: 0 lines (docs only)

### Phase AA Gates

**Verification**:
- `python -m pytest tests/ -x -q` -- all existing + new tests pass
- Test upsert against PG + verify SQLGlot transpiles to MySQL `ON DUPLICATE KEY UPDATE`
- Test distinct_on with ORDER BY constraint
- Test column presets inject from session headers
- Test inherited role flattening with 3-level hierarchy
- Test scheduled trigger fires on cron expression
- Verify each new direct-route source has working Trino catalog config

**Documentation**:
- `docs/configuration.md` updated: new source types, upsert mutations, distinct_on, column_presets, inherited roles, scheduled_triggers
- `CHANGELOG.md` entry for Phase AA
- `docs/api-reference.md` updated: new mutation/query args

---

## Phase AB: Medium-Complexity Hasura v2 Parity

### AB1. Cursor Pagination (REQ-218)

Add `first`, `after`, `last`, `before` args to root query fields alongside existing `limit`/`offset`. Cursor = base64(sort_key_values). Compile to `WHERE id > decoded_cursor LIMIT first+1`. Return `edges[{cursor, node}]` + `pageInfo`. No Relay library needed -- custom implementation with graphql-core.

**Files**: `provisa/compiler/sql_gen.py`, `provisa/compiler/schema_gen.py`, serializer
**Effort**: ~300 lines

### AB2. Subscriptions via SSE (REQ-219)

New `GET /data/subscribe/{table}` endpoint. FastAPI `StreamingResponse` with `text/event-stream`. Pluggable notification providers per source type:

| Source Type | Mechanism | Library | Config Required |
|-------------|-----------|---------|-----------------|
| PostgreSQL | `LISTEN/NOTIFY` via `pg_notify()` | asyncpg `.add_listener()` | None (zero config) |
| MongoDB | Change Streams | motor `collection.watch()` | None (built-in since 3.6+) |
| Polling | Timestamp watermark + soft delete query | asyncpg/driver | Table must have `updated_at` + soft delete column |
| Kafka | Consumer group on topic | aiokafka / confluent-kafka | Requires existing Kafka infrastructure |
| Debezium CDC | Transaction log capture → Kafka | Debezium connector + aiokafka | Requires Kafka Connect cluster (documented, not first-class) |

**Priority**: PG and MongoDB are zero-config and implemented first. Polling is the universal fallback for any RDBMS where the developer controls the schema. Kafka is used when the infrastructure already exists. Debezium is documented as an option but not a core implementation target — enterprise adoption is rare due to operational complexity.

**Polling requirements**: Table must define an `updated_at` timestamp column (monotonic). Poll interval configurable per table. Without `updated_at`, polling is unavailable for that table. Soft deletes (`deleted_at` or `is_deleted` column) are strongly recommended but not required — without soft deletes, DELETE operations are invisible to the polling provider (a warning is emitted at config time). Append-only tables (logs, events, metrics) work correctly without soft deletes since there are no deletions to miss.

Abstract `NotificationProvider` interface with `async watch(table, filter) -> AsyncGenerator[ChangeEvent]`. Subscribe endpoint resolves provider by table's source type. Schema validation ensures table exists and is visible to the requesting role. RLS filtering applied to change events.

**Files**: new `provisa/api/data/subscribe.py`, new `provisa/subscriptions/` module (providers), `provisa/api/app.py` (route)
**Effort**: ~350 lines

### AB3. Database Event Triggers (REQ-220)

PostgreSQL trigger + `pg_notify()` -> asyncpg listener -> HTTP POST to webhook URL. Config per table with operation filter (insert/update/delete) and retry policy. Reuses asyncpg pool.

**Files**: new `provisa/events/triggers.py`, `provisa/core/models.py` (config), `provisa/api/app.py` (startup listeners)
**Effort**: ~200 lines

### AB4. Enum Table Auto-Detection (REQ-221)

Introspect `pg_enum` + `pg_type` at schema build time. Generate `GraphQLEnumType` per PG enum. Map columns with enum type to GraphQL enum instead of String in type_map.

**Files**: `provisa/compiler/introspect.py`, `provisa/compiler/schema_gen.py`, `provisa/compiler/type_map.py`
**Effort**: ~250 lines

### AB5. REST Endpoint Auto-Generation (REQ-222)

For each root query field in compiled schema, generate `GET /data/rest/{table}` FastAPI endpoint. Map `?limit=10&where.id.eq=1` to GraphQL args. Compile and execute via existing pipeline.

**Query string mapping:**
- `?limit=10&offset=20` → pagination
- `?where.amount.gt=100&where.region.eq=US` → WHERE clause
- `?order_by.created_at=desc` → ORDER BY
- `?fields=id,amount,created_at` → field selection (sparse fieldset)

**Files**: new `provisa/api/rest/generator.py`, `provisa/api/app.py` (mount routes)
**Effort**: ~300 lines

### AB6. JSON:API Endpoint Auto-Generation

For each root query field, generate JSON:API compliant `GET /data/jsonapi/{table}` endpoints following the [JSON:API specification](https://jsonapi.org/).

**Response format:**
```json
{
  "data": [
    {
      "type": "orders",
      "id": "42",
      "attributes": {"amount": 99.50, "region": "US", "created_at": "2025-03-31"},
      "relationships": {
        "customer": {"data": {"type": "customers", "id": "7"}}
      }
    }
  ],
  "included": [
    {"type": "customers", "id": "7", "attributes": {"name": "Alice", "email": "alice@example.com"}}
  ],
  "meta": {"total": 1250},
  "links": {"self": "/data/jsonapi/orders?page[number]=1", "next": "/data/jsonapi/orders?page[number]=2"}
}
```

**JSON:API features:**
- Resource objects: `type` (table name), `id` (PK), `attributes` (non-FK columns), `relationships` (FK references)
- Sparse fieldsets: `?fields[orders]=amount,created_at` → only return specified attributes
- Inclusion: `?include=customer` → sideload related resources in `included` array
- Filtering: `?filter[region]=US&filter[amount][gt]=100` → WHERE clause
- Sorting: `?sort=-created_at,amount` → ORDER BY (prefix `-` = desc)
- Pagination: `?page[number]=2&page[size]=25` (offset-based) or `?page[after]=cursor` (cursor-based)
- Compound documents: related resources included once, referenced by type+id
- Error objects: JSON:API error format with `status`, `title`, `detail`, `source.pointer`
- Content negotiation: `Accept: application/vnd.api+json` required, `Content-Type: application/vnd.api+json` on responses

**Implementation:**
- `provisa/api/jsonapi/generator.py` — route generation per table, query string → GraphQL args translation
- `provisa/api/jsonapi/serializer.py` — row data → JSON:API resource objects, relationship extraction, compound document assembly
- `provisa/api/jsonapi/pagination.py` — page number/size to limit/offset, link generation
- `provisa/api/jsonapi/errors.py` — JSON:API error object formatting
- Pipeline: parse JSON:API params → translate to GraphQL args → compile → RLS → masking → execute → serialize to JSON:API format
- Same security as GraphQL — RLS, masking, role-based column visibility all apply

**Files**: new `provisa/api/jsonapi/` module (4 files), `provisa/api/app.py` (mount routes)
**Effort**: ~500 lines

### Phase AB Gates

**Verification**:
- Cursor pagination: forward + backward paging, verify `hasNextPage`/`hasPreviousPage` correctness
- SSE: subscribe to table, insert row, verify event received via `curl` or test client
- Event triggers: insert/update/delete on table, verify webhook called with correct payload
- Enum detection: create PG enum, verify GraphQL schema contains enum type with correct values
- REST: `GET /data/rest/{table}?limit=5` returns same data as equivalent GraphQL query
- JSON:API: `GET /data/jsonapi/orders` returns valid JSON:API document with type/id/attributes
- JSON:API sparse fieldsets: `?fields[orders]=amount` returns only `amount` in attributes
- JSON:API include: `?include=customer` returns customer in `included` array
- JSON:API filtering: `?filter[region]=US` returns only US orders
- JSON:API sorting: `?sort=-created_at` returns newest first
- JSON:API pagination: `?page[number]=2&page[size]=10` returns correct page with `links.next`/`links.prev`
- JSON:API errors: invalid filter returns JSON:API error object with correct structure
- JSON:API content negotiation: request without `Accept: application/vnd.api+json` returns 406

**Documentation**:
- `docs/configuration.md`: event_triggers, scheduled_triggers config
- `docs/api-reference.md`: cursor pagination args, SSE endpoint, REST endpoints, JSON:API endpoints
- `docs/security.md`: SSE auth, REST auth, JSON:API auth (same pipeline as GraphQL)
- `CHANGELOG.md` entry for Phase AB

---

## Phase AC: Tracked Functions & Webhook Mutations (REQ-205-211)

### Config Models

New `Function` and `Webhook` Pydantic models. `functions` and `webhooks` sections in `provisa.yaml`. Function results map to registered table's GraphQL type (full governance pipeline). Webhooks support inline return types.

### Implementation

1. Models: `provisa/core/models.py` -- Function, Webhook, FunctionArgument, InlineType
2. Schema: `provisa/compiler/schema_gen.py` -- mutation fields for functions/webhooks
3. SQL: `provisa/compiler/sql_gen.py` -- `SELECT * FROM schema.func($1, $2)` parameterized calls
4. Webhooks: new `provisa/webhooks/executor.py` -- HTTP POST, timeout, response mapping
5. Repository: new `provisa/core/repositories/function.py`
6. Config loader: persist functions/webhooks

### AC2. Actions UI Page (REQ-242-245)

Admin UI "Actions" page for managing custom mutations.

- **List view**: all registered functions + webhooks, grouped by type. Columns: name, type (DB Function/Webhook), source/URL, domain, exposed_as, governance, return table, arg count.
- **Add form**: type selector (DB Function / Webhook). DB Function: source dropdown, schema, function name, exposed_as toggle, returns (registered table dropdown), argument rows (name + type), visible_to/writable_by role pickers. Webhook: name, URL, method, timeout_ms, returns (table dropdown or inline type builder), argument rows, visible_to.
- **Inline type builder**: dynamic rows for webhook return types — field name + GraphQL type dropdown. Adds/removes rows.
- **Test button**: execute action with sample args, show result + governance metadata (masked columns, RLS applied, role used). Mirrors query test endpoint pattern.
- **Edit/delete**: existing actions editable inline, delete with confirmation.

**Files**: `provisa-ui/src/pages/ActionsPage.tsx` (new), `provisa-ui/src/components/ActionForm.tsx` (new), `provisa-ui/src/components/InlineTypeBuilder.tsx` (new), route in `App.tsx`, admin API mutations for CRUD
**Effort**: ~600 lines TypeScript

### Phase AC Gates

**Verification**:
- Create PG function returning `SETOF orders`, register in config, call via GraphQL mutation
- Verify function result goes through column governance (visibility, masking)
- Webhook mutation: mock HTTP endpoint, verify call with correct args, verify response mapping
- Inline return type: verify custom GraphQL type generated
- Test argument validation: invalid arg type rejected at parse time
- Actions UI: add DB function via form, verify appears in list, test button returns result
- Actions UI: add webhook with inline type, verify schema reflects custom return type
- Actions UI: delete action, verify removed from schema

**Documentation**:
- `docs/configuration.md`: `functions` and `webhooks` config sections with examples
- `docs/api-reference.md`: function/webhook mutation fields
- `docs/security.md`: function result governance pipeline
- `CHANGELOG.md` entry for Phase AC

---

## Phase AD: Schema Alignment (REQ-194-202)

### AD1. Naming Convention (REQ-194-195)

Add `naming.convention` field: `snake_case` (default), `camelCase`, `PascalCase`. Auto-generates aliases at schema build time. Explicit `column.alias` takes precedence.

**Files**: `provisa/core/models.py`, `provisa/compiler/schema_gen.py`

### AD2. OrderBy Alignment (REQ-200-202)

Replace `{field: ENUM, direction: ENUM}` with Hasura v2's `{column: direction}` pattern. 6-value direction enum with null placement. Relationship ordering via JOIN-aware compilation.

**Files**: `provisa/compiler/schema_gen.py`, `provisa/compiler/sql_gen.py`, tests

### AD3. Aggregates (REQ-196-199)

Auto-generate `<table>_aggregate` root fields. Type-aware function selection (sum/avg for numeric, min/max for comparable, count for all). Per-role gating. Optional explicit config override. Aggregate MV routing in compiler.

**Files**: `provisa/core/models.py`, `provisa/compiler/schema_gen.py`, `provisa/compiler/sql_gen.py`

### AD4. MV Lifecycle & Size Guards (REQ-234-235)

- Storage reclamation: on config reload, diff MV registry vs config. Drop backing tables for removed/disabled views via Trino `DROP TABLE`. Background cleanup task for orphaned tables.
- Size guard: before CTAS, run `SELECT COUNT(*)` probe. Skip materialization if exceeds `max_rows` (default 1M). Log warning, fall back to live execution.
- Grace period for orphans: configurable (default 24h) before auto-drop, in case of transient config changes.

**Files**: `provisa/mv/refresh.py` (reclamation + size probe), `provisa/core/models.py` (max_rows config)
**Effort**: ~150 lines

### AD5. Warm Tables -- Local SSD via Trino Cache (REQ-238-241)

Frequently queried RDBMS tables materialized into Iceberg results catalog. Trino's `fs.cache.enabled=true` then caches Parquet files on local SSD automatically.

- Query counter: increment per-table counter on each compiled query (in-memory, persisted to PG periodically)
- Auto-promote: tables exceeding `query_threshold` (default 100/interval) materialized via CTAS into Iceberg
- Auto-demote: tables below threshold have backing Iceberg table dropped on next refresh
- Trino config: ensure `fs.cache.enabled=true`, `fs.cache.directories`, `fs.cache.max-sizes` in Iceberg catalog properties
- Size guard: `max_rows` (default 10M) prevents materializing huge fact tables

**Three-tier hierarchy**: Hot (Redis <1ms) -> Warm (local SSD ~10-50ms) -> Cold (remote 100ms+). Auto-assigned by size + frequency. Manual override via `hot: true` / `warm: true`.

**Files**: new `provisa/cache/warm_tables.py`, `provisa/mv/refresh.py` (CTAS into Iceberg), `provisa/compiler/sql_gen.py` (query counter), Trino catalog config
**Effort**: ~300 lines

### AD6. Hot Tables (REQ-230-233)

Small lookup tables cached in Redis. Config: `hot: true` on table with `max_rows` guard (default 10K). Uses existing `cache.redis_url`.

- Auto-detection: during schema build, `SELECT COUNT(*)` each table. If below `hot_tables.auto_threshold` (default 10K) AND table is target of a many-to-one relationship -> auto-cache. `hot: false` opts out. `hot: true` forces in.
- Load: at startup + on refresh interval, `SELECT *` from source, serialize to Redis hash (keyed by PK) + blob (bulk reads)
- Invalidation: mutation to hot table triggers immediate Redis delete + async reload
- JOIN optimization: compiler detects hot table on one side of a join, fetches rows from Redis, injects as `VALUES`-based CTE into the SQL. DB engine sees constants, not a table reference. Works cross-source (constants travel with the query). SQLGlot transpiles VALUES per dialect.
- Security: Redis stores raw data. Column governance (visibility, masking) applied at read time, same as any other table.

**Files**: new `provisa/cache/hot_tables.py`, `provisa/core/models.py` (config), `provisa/compiler/sql_gen.py` (join rewrite), `provisa/api/app.py` (startup load + refresh loop)
**Effort**: ~400 lines

### Phase AD Gates

**Verification**:
- Naming: set `convention: camelCase`, verify `user_id` -> `userId` in schema, verify explicit alias overrides convention
- OrderBy: `order_by: [{created_at: desc_nulls_last}]` compiles to `ORDER BY created_at DESC NULLS LAST`
- OrderBy: relationship ordering `order_by: [{author: {name: asc}}]` generates correct JOIN + ORDER BY
- Aggregates: `orders_aggregate { aggregate { count sum { amount } } }` returns correct values
- Aggregate MV routing: verify compiler rewrites to MV when pattern matches
- Hot tables: mark table `hot: true`, verify loaded into Redis, verify sub-ms read latency
- Hot tables: table exceeding `max_rows` emits warning and skips cache
- Hot tables: mutate source table, verify Redis invalidated and reloaded
- Hot table JOIN: `orders JOIN countries` where countries is hot -- verify compiled SQL contains VALUES CTE with country rows, not a table reference
- Hot table cross-source: hot table from source A joined with table from source B -- verify constants injected, no cross-source Trino routing needed
- MV reclamation: remove view from config, reload, verify backing table dropped
- MV size guard: view with >max_rows skips materialization, falls back to live query
- MV orphan detection: manually create table in mv_cache schema, verify flagged as orphan
- Warm table auto-promote: query table 100+ times, verify materialized into Iceberg on next refresh
- Warm table auto-demote: stop querying, verify backing table dropped after threshold period
- Warm table size guard: table with >10M rows skips warm promotion
- Three-tier: verify small lookup = hot (Redis), frequent medium table = warm (Iceberg), everything else = cold
- **Breaking change audit**: orderBy schema change breaks existing queries -- document migration path

**Documentation**:
- `docs/configuration.md`: `naming.convention`, `aggregates` config
- `docs/api-reference.md`: new orderBy format, aggregate queries
- `docs/migration-guide.md`: orderBy breaking change migration
- `CHANGELOG.md` entry for Phase AD

---

## Phase AE: ABAC Approval Hook (REQ-203-204, REQ-246-247)

### Scoping

Compiler checks at query time whether any table in the query requires approval:
1. Table-level: `tables[].approval_hook: true`
2. Source-level: `sources[].approval_hook: true` (applies to all tables on that source)
3. Global: `auth.approval_hook.scope: all`

If no table in the query is scoped, skip the hook call entirely — zero overhead.

### Protocols

Abstract `ApprovalHook` interface with three implementations:
- **WebhookApprovalHook**: HTTP POST via httpx. Default.
- **GrpcApprovalHook**: gRPC with persistent channel. Proto definition: `provisa/auth/approval.proto`. Compiled to Python stubs at build time.
- **UnixSocketApprovalHook**: HTTP POST over Unix domain socket (httpx supports `transport=httpx.AsyncHTTPTransport(uds=path)`). For OPA / same-machine sidecars.

### Implementation

1. Add `approval_hook` field to `Source` and `Table` models (optional bool)
2. Add `ApprovalHookConfig` to models with type/url/socket_path/timeout_ms/fallback/scope
3. Abstract interface: `provisa/auth/approval_hook.py` — `ApprovalHook.evaluate(request) -> ApprovalResponse`
4. Webhook impl: httpx POST with timeout
5. gRPC impl: `provisa/auth/approval.proto` + generated stubs, persistent channel
6. Unix socket impl: httpx with UDS transport
7. Hook into query pipeline: after RLS injection, before execution. Check scope, call hook, handle timeout/fallback.
8. Circuit breaker: track consecutive failures, open circuit after N failures, half-open after cooldown.

**Files**: `provisa/core/models.py`, new `provisa/auth/approval_hook.py`, new `provisa/auth/approval.proto`, query pipeline in `provisa/api/data/endpoint.py`

### Phase AE Gates

**Verification**:
- **Scoping**: table with `approval_hook: true` triggers call; table without it does not
- **Scoping**: source with `approval_hook: true` triggers call for all its tables
- **Scoping**: `scope: all` triggers call for every query
- **Zero overhead**: query against unscoped table — verify no HTTP/gRPC call made
- **Webhook**: mock endpoint, verify payload contains user/roles/tables/columns/operation
- **Approve**: query executes normally
- **Deny**: query rejected with reason from hook
- **Timeout + fallback=deny**: query rejected after timeout_ms
- **Timeout + fallback=allow**: query executes after timeout_ms
- **gRPC**: mock gRPC server, verify persistent connection reuse
- **Unix socket**: mock socket listener, verify request received

**Documentation**:
- `docs/configuration.md`: `auth.approval_hook` config, per-table/per-source scoping
- `docs/security.md`: ABAC hook architecture, protocol comparison, request/response format, timeout/fallback behavior
- `CHANGELOG.md` entry for Phase AE

---

## Phase AF: Installer & Packaging (REQ-223-228, REQ-294)

### What Gets Bundled (Hidden from User)
- Python FastAPI server (compiled binary)
- PostgreSQL admin DB (pgserver embedded)
- Trino query engine (Java binary)
- React UI (static build)
- CLI wrapper (`provisa` command)

### What Does NOT Get Bundled
- Source datasets (connect over the wire)
- External auth providers (configured later)
- External Trino clusters (configured later)

### Scaling Model
- **Default**: Vertical scaling on single machine. Increase CPU/RAM as needed.
- **Scale-out**: Point to external Trino cluster via config. Primary horizontal scaling mechanism.

### AF1: Shell Script Installer (Immediate)
- `provisa` CLI wrapper (shell script) that manages Docker Compose
- Stores config/data in `~/.provisa/`
- Commands: `provisa start`, `provisa stop`, `provisa status`, `provisa open`
- Prerequisite: Docker/OrbStack/Colima (auto-detected)
- All service names branded "Provisa" (no docker/trino/pg visible in logs)
- **Effort**: ~500 lines shell script

### AF2: Airgapped Native App Bundle (phased by OS)

Common to all phases:
- All Provisa service images (postgres, trino, redis, pgbouncer, provisa-api, provisa-ui) exported via `docker save` at build time and bundled as `.tar` archives
- On first launch: runtime starts silently, images loaded from bundle — zero network required
- `docker-compose.yml` references images by digest (not tag) to prevent any remote resolution
- `provisa` CLI talks to bundled runtime socket, not system Docker socket
- Fully airgap-capable: no outbound network at install or runtime
- Full app re-download per release (~4 GB) — no delta update requirement

#### AF2a: macOS (immediate — primary dev environment)
- Embed **Lima + containerd** binaries for arm64 and x86_64
- Package as signed + notarized `.dmg`
- Sign via `codesign` + `xcrun notarytool` (requires Apple Developer account)
- **Effort**: ~1 week

#### AF2b: Linux (follow-on)
- Lima or native containerd (no VM needed on Linux)
- Package as `.AppImage` (runs on Ubuntu 20.04+, RHEL 8+ without install)
- **Effort**: ~3–4 days once AF2a build pipeline exists

#### AF2c: Windows (follow-on)
- WSL2 + containerd as the runtime layer
- Package as signed `.exe` installer (NSIS or WiX)
- Sign via `signtool`
- **Effort**: ~1 week (WSL2 automation is the tricky part)

- **Supersedes AF3** — a signed native app bundle provides a better experience than OS package managers and covers the same platforms

### ~~AF3: Native OS Packages~~ (superseded by AF2)
- Superseded by AF2. A bundled native app with embedded runtime is a superior distribution mechanism that covers all target platforms without requiring Docker as a visible dependency.

### Phase AF Gates

**Verification (AF1)**:
- Fresh machine with Docker: `./install.sh && provisa start` brings up all services
- `provisa status` shows all healthy
- `provisa open` opens browser to working UI
- `provisa stop` cleanly shuts down all services
- No "docker", "trino", "postgresql" visible in user-facing output
- `~/.provisa/` contains all state, removable with `provisa uninstall`

**Verification (AF2/AF3):** _(Manually testable only — AF2 artifacts only exist in the GitHub Actions release workflow; not automatable in local checkout)_
- Single binary/package installs without Docker prerequisite
- Services start via native OS service manager
- Upgrade path: `provisa upgrade` pulls new version without data loss

**Documentation**:
- `README.md`: installation instructions (replaces Docker Compose setup)
- `docs/installation.md`: per-platform guide, prerequisites, troubleshooting
- `docs/configuration.md`: how to connect external Trino/auth/PostgreSQL
- `CHANGELOG.md` entry for Phase AF

---

## Phase AG: Hasura v2 Metadata Converter (REQ-182, REQ-184-193)

### v2 -> Provisa Mapping

| Hasura v2 Object | Provisa Section |
|------------------|----------------|
| Database source | `sources[]` |
| Tracked table | `tables[]` |
| `select_permissions[].columns` | column `visible_to` |
| `select_permissions[].filter` | `rls_rules[]` |
| `object_relationships` | `relationships[]` many-to-one |
| `array_relationships` | `relationships[]` one-to-many |
| Roles (collected) | `roles[]` |
| `custom_column_names` | column `alias` |
| `custom_root_fields` | table `alias` |
| `insert/update_permissions` | column `writable_by` |
| `allow_aggregations` | `aggregates` config |
| `naming_convention` | `naming.convention` |
| Auth (env vars) | `auth` via `--auth-env-file` |
| `inherited_roles` | `roles[]` with `parent_role_id` |
| Tracked VOLATILE functions | `functions[]` |
| Actions (DB-backed) | `functions[]` |
| Actions (webhook-backed) | `webhooks[]` |
| `event_triggers` | `event_triggers` config |
| `computed_fields` | `functions[]` exposed_as=query |
| `remote_schemas` | skipped (warning) |
| `cron_triggers` | `scheduled_triggers` config |

### Module Layout

```
provisa/hasura_v2/
    __init__.py, parser.py, models.py, mapper.py, cli.py, __main__.py
provisa/import_shared/
    filters.py (boolean expression -> SQL), warnings.py
```

### CLI

```
python -m provisa.hasura_v2 <metadata-dir> [options]
  -o, --output PATH
  --source-overrides PATH
  --domain-map KEY=VAL
  --governance-default LEVEL
  --auth-env-file PATH
  --dry-run
```

### Phase AG Gates

**Verification**:
- Clone `hasura/3factor-example`, run converter, output passes `ProvisaConfig.model_validate()`
- Tables, relationships, permissions, roles all present in output
- Auth env file -> valid Provisa auth config
- Tracked functions -> `functions[]` entries
- Warnings emitted for event_triggers, remote_schemas (not errors)
- Round-trip: load converted config into running Provisa instance, verify schema builds

**Documentation**:
- `docs/migration/hasura-v2.md`: step-by-step migration guide with examples
- `docs/migration/hasura-v2.md`: feature parity matrix (what converts, what's skipped, what's different)
- CLI `--help` text
- `CHANGELOG.md` entry for Phase AG

---

## Phase AH: DDN (Hasura v3) HML Converter (REQ-183, REQ-189, REQ-191)

### DDN -> Provisa Mapping

| DDN Kind | Provisa Section |
|----------|----------------|
| DataConnectorLink + Connector | `sources[]` |
| Subgraph (non-globals) | `domains[]` |
| ObjectType + Model | `tables[]` |
| TypePermissions | column `visible_to` |
| ModelPermissions | `rls_rules[]` |
| Relationship (Object/Array) | `relationships[]` |
| Roles (collected) | `roles[]` |
| AggregateExpression | `aggregates` config |
| Command (procedure) | `functions[]` mutation |
| Command (function) | `functions[]` query |
| OrderByExpression | skipped (all columns orderable) |
| BooleanExpressionType | skipped (warning) |
| AuthConfig | skipped (warning) |

Key: GraphQL field -> physical column resolution via `ObjectType.dataConnectorTypeMapping[].fieldMapping`.

### Module Layout

```
provisa/ddn/
    __init__.py, parser.py, models.py, mapper.py, cli.py, __main__.py
```

### Phase AH Gates

**Verification**:
- Convert local Chinook DDN project (18 models), output passes `ProvisaConfig.model_validate()`
- Convert enterprise FSI project (228 models), output passes validation
- GraphQL field -> physical column resolution correct (e.g., `artistId` -> `artist_id`)
- Relationships have correct cardinality and physical column names
- AggregateExpressions converted to Provisa aggregate config
- Commands converted to `functions[]` entries
- Warnings for BooleanExpressionType, AuthConfig (not errors)
- Round-trip: load converted config into running Provisa, verify schema builds

**Documentation**:
- `docs/migration/hasura-ddn.md`: step-by-step migration guide
- `docs/migration/hasura-ddn.md`: feature parity matrix
- CLI `--help` text
- `CHANGELOG.md` entry for Phase AH

---

---

## Phase AI: NoSQL Source Mapping & Managed Catalog Generation (REQ-250, REQ-251, REQ-252)

### Goal

All Trino catalog configuration flows through Provisa's config YAML. Each NoSQL/non-relational source type gets a type-specific mapping DSL that defines how its native data structures map to relational tables. Schema inference supported where Trino connectors provide auto-discovery.

### Architecture

Follows the Kafka source pattern (`provisa/kafka/`). Each source type gets its own module implementing a common interface:

```python
class SourceAdapter:
    def generate_catalog_properties(config: SourceConfig) -> dict[str, str]
    def generate_table_definitions(config: SourceConfig) -> list[TableEntry]
    def discover_schema(connection, table_hint: str) -> list[ColumnDefinition]  # optional
    def inject_query_filters(compiled, config) -> compiled  # optional
```

### Source-Specific Mapping DSL

#### AI1. Redis (explicit columns required)

```yaml
sources:
  - id: session-cache
    type: redis
    host: redis
    port: 6379
    tables:
      - name: user_sessions
        key_pattern: "session:*"
        key_column: session_id
        value_type: hash            # hash, string, zset, list
        columns:
          - name: user_id
            type: INTEGER
            field: user_id
          - name: email
            type: VARCHAR
            field: email
```

Provisa generates: `redis.table-description-dir` property + JSON table definition files.

**Files**: new `provisa/redis/__init__.py`, `provisa/redis/source.py`, `provisa/redis/table_gen.py`
**Effort**: ~300 lines

#### AI2. MongoDB (inference + optional override)

```yaml
sources:
  - id: product-mongo
    type: mongodb
    connection_url: mongodb://mongo:27017/
    tables:
      - name: products
        collection: products
        discover: true              # infer schema from _schema collection or sample
        columns:                    # optional overrides
          - name: metadata.category
            alias: category         # flatten nested path
            type: VARCHAR
```

Trino MongoDB connector auto-discovers collections. `discover: true` introspects and generates starting column list. Explicit columns override/supplement inferred ones.

**Files**: new `provisa/mongodb/__init__.py`, `provisa/mongodb/source.py`
**Effort**: ~200 lines

#### AI3. Elasticsearch (inference + nested field flattening)

```yaml
sources:
  - id: logs-es
    type: elasticsearch
    host: elasticsearch
    port: 9200
    tables:
      - name: access_logs
        index: nginx-access-*       # index pattern
        discover: true
        columns:
          - name: timestamp
            type: TIMESTAMP
            path: "@timestamp"       # field path in source doc
          - name: method
            type: VARCHAR
            path: request.method     # nested → flattened
```

Provisa reads ES index mappings for inference. Nested fields exposed via `path` property.

**Files**: new `provisa/elasticsearch/__init__.py`, `provisa/elasticsearch/source.py`
**Effort**: ~250 lines

#### AI4. Cassandra (inference + partition key awareness)

```yaml
sources:
  - id: events-cassandra
    type: cassandra
    contact_points: cassandra
    port: 9042
    tables:
      - name: user_events
        keyspace: analytics
        table: user_events
        discover: true
```

Trino auto-discovers keyspaces/tables. `discover: true` introspects partition and clustering keys to inform query pushdown hints.

**Files**: new `provisa/cassandra/__init__.py`, `provisa/cassandra/source.py`
**Effort**: ~150 lines

#### AI5. Prometheus (metric → table mapping)

```yaml
sources:
  - id: metrics
    type: prometheus
    url: http://prometheus:9090
    tables:
      - name: api_latency
        metric: http_request_duration_seconds
        labels_as_columns: [method, endpoint, status]
        value_column: duration_ms
        default_range: 1h           # query time window
```

Each metric becomes a table. Labels become dimension columns. Value + timestamp are fixed columns. `default_range` injects time window filter at query time (like Kafka windows).

**Files**: new `provisa/prometheus/__init__.py`, `provisa/prometheus/source.py`
**Effort**: ~200 lines

#### AI6. Accumulo (explicit columns required)

```yaml
sources:
  - id: graph-accumulo
    type: accumulo
    instance: accumulo
    zookeepers: zookeeper:2181
    tables:
      - name: edges
        accumulo_table: graph_edges
        columns:
          - name: src_vertex
            type: VARCHAR
            family: edge
            qualifier: src
          - name: dst_vertex
            type: VARCHAR
            family: edge
            qualifier: dst
```

Like Redis — no inference. Column definitions map to Accumulo column families/qualifiers.

**Files**: new `provisa/accumulo/__init__.py`, `provisa/accumulo/source.py`
**Effort**: ~200 lines

#### AI7. Managed Catalog Generation

Replace static `.properties` files with runtime generation for ALL source types (including RDBMS):

- On source registration, Provisa generates catalog properties and calls Trino dynamic catalog API
- NoSQL sources with table definition files: generate files to a managed directory, reference in catalog properties
- On source deletion, remove catalog via API
- Static `.properties` files in `trino/catalog/` become templates/examples only, not the runtime config

**Files**: modify `provisa/core/catalog.py` (extend for NoSQL-specific properties), new `provisa/core/catalog_templates/` directory
**Effort**: ~300 lines

#### AI8. Source Configuration & Table Mapping UI

The SourcesPage add-source form must render type-specific configuration fields for every source type. Each NoSQL/non-relational source needs a table definition builder in addition to connection fields.

**Source connection forms** (SourcesPage.tsx add/edit):
- RDBMS (existing): host, port, database, username, password
- Cloud DW (existing): account URL, credentials, warehouse/project
- API (existing): base URL, auth type, headers
- Kafka (existing): bootstrap servers, auth, schema registry
- **Redis** (new): host, port, password, database index
- **MongoDB** (new): connection URL, default database
- **Elasticsearch** (new): host, port, API key, cloud ID (optional)
- **Cassandra** (new): contact points, port, datacenter, consistency level
- **Prometheus** (new): URL, auth headers
- **Accumulo** (new): instance name, zookeeper hosts, username, password
- Data Lake (existing): metastore URI, S3/ADLS credentials

**Table mapping builder** (per source, accessible from SourcesPage expanded row or TablesPage):
- **Redis**: key pattern input, key column name, value type selector (hash/string/zset/list), column rows (name, type, field mapping)
- **MongoDB**: collection selector (populated from `discover` endpoint), column rows with JSONPath `path` field for nested docs, `discover: true` toggle
- **Elasticsearch**: index pattern input, column rows with dot-path for nested fields, `discover: true` toggle
- **Cassandra**: keyspace/table selector (populated from `discover`), `discover: true` toggle
- **Prometheus**: metric name input, label-to-column checkboxes, value column name, default time range
- **Accumulo**: table name, column rows with family/qualifier fields

**Schema discovery flow** (for sources with `discover: true`):
- "Discover Schema" button per source → calls discovery endpoint
- Shows inferred columns with types in a review table
- Steward selects/deselects columns, overrides types, adds aliases
- "Register" button creates the table registration from curated discovery results

**Files**: new `provisa-ui/src/components/SchemaDiscovery.tsx`, new `provisa-ui/src/components/TableMappingBuilder.tsx`, modify `provisa-ui/src/pages/SourcesPage.tsx`, modify `provisa-ui/src/pages/TablesPage.tsx`
**Effort**: ~800 lines TypeScript

### Phase AI Gates

**Verification**:
- Redis: define table in config, verify Trino can query Redis keys as rows
- MongoDB: `discover: true` on collection, verify inferred columns appear in schema, verify explicit overrides take precedence
- Elasticsearch: `discover: true` on index pattern, verify nested field flattening, verify index pattern matching
- Cassandra: `discover: true` on keyspace, verify tables discovered with correct types
- Prometheus: define metric table, verify labels as columns, verify time window injection
- Accumulo: define table with column family/qualifier mapping, verify query works
- Catalog generation: add source via admin API, verify Trino catalog created without manual `.properties` file
- Source config UI: add Redis source via UI form, verify type-specific fields (host, port, db index) rendered
- Source config UI: add MongoDB source via UI form, verify connection URL field rendered
- Table mapping UI: Redis source → open table mapping builder, define key pattern + columns, register → table queryable
- Table mapping UI: MongoDB source → click Discover, review inferred columns, deselect some, register → schema reflects selections
- Schema discovery UI: click Discover on MongoDB source, verify column candidates shown, register → table appears in schema
- Backward compat: existing RDBMS sources still work (catalog API path unchanged)

**Documentation**:
- `docs/configuration.md`: per-source-type config examples
- `docs/sources.md`: updated with NoSQL mapping details, inference behavior
- `CHANGELOG.md` entry for Phase AI

---

## Phase AJ: Apollo Federation Support

### Goal

Enable Provisa to act as an Apollo Federation subgraph, allowing it to be composed into a federated supergraph via Apollo Gateway or Apollo Router. Tables become federated entity types with `@key` directives, enabling cross-service JOINs at the gateway level.

### Federation v2 Spec

Provisa generates a Federation v2 compliant subgraph schema when federation is enabled:

```yaml
federation:
  enabled: true
  version: 2         # Federation v2 (default)
  service_name: provisa
```

### Schema Additions

For each registered table, the schema generator adds:

```graphql
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key", "@shareable", "@external"])

type Orders @key(fields: "id") {
  id: Int!
  customer_id: Int!
  amount: Float!
  customer: Customers  # relationship
}

type Customers @key(fields: "id") {
  id: Int!
  name: String!
  email: String!
}
```

### Required Federation Fields

- `_service { sdl }` — returns the full SDL for schema composition
- `_entities(representations: [_Any!]!) -> [_Entity]!` — resolves entity references from other subgraphs by primary key
- `_Any` scalar — accepts JSON representations of entities
- `_Entity` union — union of all entity types

### Implementation

1. **Config**: Add `federation` section to `ProvisaConfig` (enabled, version, service_name)

2. **Schema generation** (`provisa/compiler/federation.py`):
   - `build_federation_schema(base_schema, tables, relationships)` — wraps the existing schema with Federation directives
   - Auto-detect `@key` from primary key columns (introspected via Trino metadata)
   - Relationships become regular fields (not `@external`) — Provisa owns these entities
   - Multi-key support: composite PKs become `@key(fields: "col1 col2")`

3. **Entity resolution** (`provisa/api/data/federation.py`):
   - `POST /data/graphql` handles `_entities` queries
   - For each representation, compile `SELECT * FROM table WHERE pk = $1`
   - Execute via existing pipeline (with RLS, masking applied)
   - Batch entity resolution: group by type, single query per type with `WHERE pk IN (...)`

4. **SDL endpoint** — modify `/data/sdl` to return Federation-annotated SDL when federation is enabled

5. **Schema generation toggle** — when `federation.enabled`, `generate_schema()` calls `build_federation_schema()` as a post-processing step

### Gateway Integration

Provisa registers as a subgraph in Apollo Router's `supergraph.yaml`:

```yaml
subgraphs:
  provisa:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/sdl
```

Multiple Provisa instances can be composed (e.g., one per data domain), or Provisa can be composed with hand-written subgraphs for business logic.

### Security

- Entity resolution respects the same RLS, masking, and role-based visibility as regular queries
- The `X-Provisa-Role` header (or auth token) must be forwarded from the gateway
- Apollo Router supports header propagation natively

### Phase AJ Gates

**Verification**:
- Federation SDL: `_service { sdl }` returns valid Federation v2 SDL with `@key` directives
- Entity resolution: `_entities(representations: [{__typename: "Orders", id: 1}])` returns the correct order with RLS applied
- Batch resolution: multiple representations in one query resolved efficiently (single SQL per type)
- Composite key: table with multi-column PK correctly resolves with `@key(fields: "col1 col2")`
- Gateway integration: Apollo Router composes Provisa subgraph, cross-subgraph query works
- Security: entity resolution applies masking and column visibility per role
- Disabled by default: federation features absent when `federation.enabled: false`

**Documentation**:
- `docs/configuration.md`: federation config section
- `docs/api-reference.md`: Federation-specific query fields (`_service`, `_entities`)
- `docs/federation.md`: Apollo Gateway/Router integration guide
- `CHANGELOG.md` entry for Phase AJ

**Files**: new `provisa/compiler/federation.py`, new `provisa/api/data/federation.py`, modify `provisa/compiler/schema_gen.py`, modify `provisa/api/data/sdl.py`, modify `provisa/core/models.py`
**Effort**: ~500 lines

---

## Parity Phase Summary

| Phase | What | Effort | Dependencies |
|-------|------|--------|-------------|
| AA | Quick wins: dialect expansion, upsert, distinct_on, presets, inherited roles, cron | Low | None |
| AB | Cursor pagination, SSE subscriptions, event triggers, enums, REST + JSON:API auto-gen | Medium | None |
| AC | Tracked functions & webhook mutations | Medium | None |
| AD | Naming convention, orderBy alignment, aggregates, MV lifecycle, warm/hot tables | Medium | None |
| AE | ABAC approval hook | Medium | None |
| AF | Installer & packaging (AF1/AF2/AF3) | Low -> High | Phases AA-AE for features |
| AG | Hasura v2 converter | Medium | Phases AA-AD (features must exist) |
| AH | DDN converter | Medium | Phase AG (shared infra) |
| AI | NoSQL source mapping & managed catalog generation | Medium-High | Phase AA1 (dialect expansion) |
| AJ | Apollo Federation v2 subgraph support | Medium | Phase C (schema gen) |

Phases AA-AE are independent and can be parallelized. Phase AF1 (shell script) can start anytime. Phase AI can start after AA1. Phase AJ can start anytime (only depends on core schema generation).

## Sample Hasura v2 Projects for End-to-End Testing

| Project | Repo | Features |
|---------|------|----------|
| 3factor-example (food ordering) | `hasura/3factor-example` | Tables, relationships, event triggers, migrations |
| realtime-poll | `hasura/sample-apps/realtime-poll` | Simpler structure, subscriptions |
| demo-apps | `hasura/demo-apps` | Multiple configs, Docker Compose, roles |
| metadata-api-example (Chinook) | `hasura/metadata-api-example` | Chinook DB, relationships |

---

## Phase AK: Two-Stage Compiler & Multi-Protocol Clients
**Goal:** Refactor the compiler into two explicit stages; expose raw SQL as a first-class entry point; add Python DB-API 2.0, SQLAlchemy dialect, and ADBC interfaces to `provisa-client`; update the JDBC driver to enforce governance in catalog mode.
**REQs:** REQ-262, REQ-263, REQ-264, REQ-265, REQ-266, REQ-267, REQ-268, REQ-269, REQ-270, REQ-271, REQ-272, REQ-273, REQ-274

### AK1: Two-Stage Compiler Refactor

**Build:**
- `provisa/compiler/stage1.py` — extract existing GQL→SQL logic from compiler into Stage 1. Input: validated GraphQL AST + registration model. Output: plain PG-style SQL with all aliases explicit (physical names preserved, AS aliases for every selected column).
- `provisa/compiler/stage2.py` — new standalone SQL governance transformer. Input: SQL string + role + registration model. Output: governed SQL. Uses SQLGlot to parse SQL AST, walk all table references, inject: RLS WHERE predicates, column masking expressions, column visibility (remove/NULL invisible columns), LIMIT ceiling, TABLESAMPLE/random sampling. Handles subqueries, CTEs, JOINs, UNION, SELECT *.
- `provisa/compiler/pipeline.py` — assembles Stage 1 + Stage 2 for the existing GraphQL path. Replaces the current monolithic compiler call sites.
- `provisa/api/data/endpoint.py` — add `POST /data/sql` endpoint. Accepts `{sql, variables}`. Authenticates via existing auth middleware (role from token). Passes SQL directly to Stage 2, then routes and executes.

**Verify:**
- `python -m pytest tests/unit/test_stage2.py -x -q`:
  - RLS WHERE injected for every table reference including subqueries and CTEs
  - Masked columns wrapped in masking expression
  - Invisible columns removed from SELECT list
  - LIMIT injected when ceiling exceeded
  - SELECT * expanded and filtered
  - UNION branches each governed independently
- `python -m pytest tests/e2e/test_sql_endpoint.py -x -q` — `/data/sql` returns governed results matching equivalent GraphQL query

**Files:**
| File | Action |
|------|--------|
| `provisa/compiler/stage1.py` | Create (extract from existing compiler) |
| `provisa/compiler/stage2.py` | Create |
| `provisa/compiler/pipeline.py` | Create |
| `provisa/api/data/endpoint.py` | Modify (add /data/sql, wire stage2) |
| `tests/unit/test_stage2.py` | Create |
| `tests/e2e/test_sql_endpoint.py` | Create |

---

### AK2: JDBC Catalog Mode Governance

**Build:**
- Update `ProvisaConnection.java` — in catalog mode, pass arbitrary SQL through `/data/sql` endpoint (Stage 2 governed) instead of executing directly. Remove the current direct execution path for catalog mode.
- Update `FlightTransport.java` — catalog mode Flight tickets route through Stage 2 on the server side (no client change needed; server-side enforcement).

**Verify:**
- `mvn -f jdbc-driver/pom.xml test` — catalog mode queries return governed results (RLS applied, masked columns masked)
- Integration test: catalog mode query as restricted role does not return rows excluded by RLS

**Files:**
| File | Action |
|------|--------|
| `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaConnection.java` | Modify |
| `jdbc-driver/src/test/java/io/provisa/jdbc/ProvisaDriverIT.java` | Modify |

---

### AK3: Python DB-API 2.0

**Build:**
- `provisa_client/dbapi.py` — PEP 249 `connect()`, `Connection`, `Cursor`. `cursor.execute(query)` detects GraphQL vs SQL by leading `{` or `query`/`mutation` keyword. GraphQL path: calls existing `ProvisaClient.query()`. SQL path: calls `/data/sql`. `fetchall()`, `fetchone()`, `fetchmany()`. `description` attribute returns column metadata. `mode` kwarg on `connect()` (default: `approved`).
- `provisa_client/__init__.py` — export `connect` from `dbapi`.

**Verify:**
- `python -m pytest provisa-client/tests/test_dbapi.py -x -q`:
  - `connect()` authenticates and assigns role
  - `execute()` with GraphQL string returns correct rows
  - `execute()` with SQL string returns correct rows
  - `fetchall()`, `fetchone()`, `fetchmany()` work correctly
  - `description` returns correct column names and types
  - `mode=catalog` exposes registered tables

**Files:**
| File | Action |
|------|--------|
| `provisa_client/dbapi.py` | Create |
| `provisa_client/__init__.py` | Modify (export connect) |
| `provisa-client/tests/test_dbapi.py` | Create |

---

### AK4: SQLAlchemy Dialect

**Build:**
- `provisa_client/sqlalchemy_dialect.py` — SQLAlchemy `CreateEnginePlugin` and dialect class. URL scheme `provisa+http://` and `provisa+https://`. Dialect wraps DB-API 2.0 connection. Implements `get_table_names()` (approved mode: stable IDs; catalog mode: registered table names), `get_columns(table_name)` (introspects via `/admin/graphql`). Registers dialect entry point in `pyproject.toml` as `provisa.dialects = provisa_client.sqlalchemy_dialect:ProvisaDialect`.

**Verify:**
- `python -m pytest provisa-client/tests/test_sqlalchemy.py -x -q`:
  - `create_engine("provisa+http://user:pass@localhost:8001")` connects
  - `pd.read_sql("SELECT * FROM monthly_revenue", engine)` returns DataFrame
  - `inspector.get_table_names()` returns approved query stable IDs
  - `inspector.get_columns("monthly_revenue")` returns correct schema

**Files:**
| File | Action |
|------|--------|
| `provisa_client/sqlalchemy_dialect.py` | Create |
| `provisa-client/pyproject.toml` | Modify (add entry_points for dialect) |
| `provisa-client/tests/test_sqlalchemy.py` | Create |

---

### AK5: ADBC Interface

**Build:**
- `provisa_client/adbc.py` — `adbc_connect(url, user, password, mode="approved")` returns an ADBC-compatible connection backed by Provisa's Arrow Flight endpoint. Authenticates via `/auth/login`, uses returned token as Flight bearer auth. Results stream as Arrow RecordBatches. Compatible with `adbc_driver_manager.AdbcConnection` interface. Optional dependency: `pyarrow`, `adbc-driver-manager`.

**Verify:**
- `python -m pytest provisa-client/tests/test_adbc.py -x -q`:
  - `adbc_connect()` authenticates
  - Query returns Arrow Table with correct schema and data
  - Results stream (no full materialization)

**Files:**
| File | Action |
|------|--------|
| `provisa_client/adbc.py` | Create |
| `provisa-client/pyproject.toml` | Modify (add adbc optional dependency group) |
| `provisa-client/tests/test_adbc.py` | Create |

---

## Phase AL: Federation Performance
**Goal:** Cost-based optimizer always has accurate statistics; stewards and developers can steer execution without exposing engine internals.
**REQs:** REQ-275, REQ-276, REQ-277, REQ-278, REQ-279, REQ-280, REQ-281

**Build:**

### AL1: Automatic ANALYZE on Registration (REQ-275, REQ-280)
- `provisa/core/registration.py` — after dynamic catalog API call succeeds and tables are published, fire `ANALYZE catalog.schema.table` for each registered table via Trino HTTP API. Errors are logged and swallowed (connector may not support ANALYZE) — registration does not fail.
- `provisa/api_source/cache.py` — after `write_cache()` completes a refresh cycle, fire `ANALYZE` on `api_cache_{table_name}` via Trino.
- No config flag required — always on.

### AL2: Refresh Statistics Mutation (REQ-276)
- `provisa/api/admin/` — add `refreshSourceStatistics(source_id: ID!)` mutation. Looks up all tables for the source, fires `ANALYZE` on each. Returns count of tables analyzed and list of any failures.

### AL3: Source-Level Federation Hints (REQ-278)
- `provisa/core/models.py` — add `federation_hints: dict[str, str]` field to `SourceConfig`.
- `provisa/executor/trino.py` — before query execution, collect hints from all sources touched by the query, inject `SET SESSION key = 'value'` statements. Conflicts resolved by per-query hints taking precedence (AL4).

### AL4: Per-Query Session Property Overrides (REQ-277)
- `provisa/core/models.py` — add `federation_hints: dict[str, str]` to `PersistedQuery`.
- `provisa/executor/trino.py` — merge per-query hints on top of source-level hints; inject before execution.

### AL5: Comment Hint Parser (REQ-279, REQ-281)
- `provisa/compiler/hints.py` — `extract_hints(sql: str) -> tuple[str, dict[str, str]]`. Regex-extracts `/*+ ... */` from query text. Returns cleaned SQL and hint dict. Supported tokens:
  - `BROADCAST(<table>)` → `join_distribution_type = 'BROADCAST'`
  - `NO_REORDER` → `join_reordering_strategy = 'NONE'`
  - `BROADCAST_SIZE(<size>)` → `join_max_broadcast_table_size = '<size>'`
- `provisa/executor/trino.py` — call `extract_hints()` on incoming SQL; merge result on top of per-query hints (comment hints win).
- All hint names exposed to users are Provisa-branded. Trino session property names live only inside `hints.py`.

**Verify:**
- `python -m pytest tests/test_federation_hints.py -x -q`:
  - `extract_hints("/*+ BROADCAST(orders) */ SELECT ...")` returns cleaned SQL + correct session property
  - `extract_hints("/*+ NO_REORDER */")` returns correct property
  - Source-level hints injected when source is touched
  - Per-query hints override source-level hints
  - Comment hints override per-query hints
  - `ANALYZE` fires after table registration (mock Trino HTTP)
  - `ANALYZE` fires after API cache refresh (mock Trino HTTP)

**Files:**
| File | Action |
|------|--------|
| `provisa/compiler/hints.py` | Create |
| `provisa/executor/trino.py` | Modify |
| `provisa/core/models.py` | Modify |
| `provisa/core/registration.py` | Modify |
| `provisa/api_source/cache.py` | Modify |
| `provisa/api/admin/` (mutations) | Modify |

---

## Phase AM: Live Query Engine
**Goal:** Unify SSE subscriptions and Kafka sinks under a single poll-based delivery engine. A persisted query (or a non-CDC table) declares `live` config with a `watermark_column`, `poll_interval`, and one or more outputs (`sse_subscription`, `kafka_sink`). One engine, two output paths, independent watermarks per output.
**REQs:** REQ-260, REQ-282, REQ-283, REQ-284, REQ-285, REQ-286, REQ-287
**Depends on:** Phase H (Persisted Query Registry), Phase AB (SSE subscription infrastructure), Phase V (Kafka sink infrastructure)

### Architecture

```
Live Query Config (persisted query or table)
  watermark_column, poll_interval, outputs: [sse_subscription, kafka_sink]
                        ↓
              LiveQueryScheduler (APScheduler)
                        ↓
         Execute query WHERE watermark_col > last_watermark
                        ↓
              ┌─────────────────┐
              ▼                 ▼
     SSE fanout            Kafka producer
   (per connected         (one message per row,
    client stream)         keyed by optional col)
              ↓                 ↓
     Update watermark    Update watermark
     in live_query_state in live_query_state
```

### Config

Persisted query live delivery block (in `provisa.yaml` or admin API):
```yaml
persisted_queries:
  - id: active-orders
    query: "{ orders(where: {status: {_eq: \"active\"}}) { id amount updated_at } }"
    live:
      watermark_column: updated_at
      poll_interval: 30s
      outputs:
        - type: sse_subscription
        - type: kafka_sink
          topic: provisa.active-orders
          key_column: id
```

Table poll delivery (non-CDC source):
```yaml
tables:
  - id: trino_orders
    source_id: trino-federation
    live:
      delivery: poll
      watermark_column: updated_at
      soft_delete_column: deleted_at
      poll_interval: 60s
      outputs:
        - type: sse_subscription
        - type: kafka_sink
          topic: provisa.trino-orders
```

### Build

- `provisa/core/models.py` — add `LiveOutputConfig` (type, topic, key_column), `LiveDeliveryConfig` (watermark_column, poll_interval, delivery: cdc|poll, soft_delete_column, outputs list), attach to `PersistedQueryConfig` and `TableConfig`.
- `provisa/core/schema.sql` — add `live_query_state` table: `(query_id TEXT, output_type TEXT, last_watermark TIMESTAMPTZ, last_polled_at TIMESTAMPTZ, status TEXT, PRIMARY KEY (query_id, output_type))`.
- `provisa/live/__init__.py`
- `provisa/live/engine.py` — `LiveQueryEngine`: APScheduler-backed scheduler. On startup, loads all active live configs from DB, schedules poll jobs. On config reload, diffs and reschedules. Each job: execute query with `WHERE watermark_col > last_watermark ORDER BY watermark_col`, fan out rows to registered outputs, commit new watermark to `live_query_state`.
- `provisa/live/watermark.py` — `WatermarkStore`: read/write `live_query_state`. Handles per-output independence. On restart, resumes from last committed watermark.
- `provisa/live/outputs/sse.py` — `SSEOutput`: maintains a set of connected SSE client queues per live query. `push(rows)` serializes and enqueues to all connected clients. Clients connect via existing `/data/subscribe` endpoint (extended to accept query_id in addition to table name).
- `provisa/live/outputs/kafka.py` — `KafkaOutput`: produces one message per row to configured topic. Uses existing Kafka producer from Phase V. Respects `key_column` for Kafka message key.
- `provisa/live/outputs/base.py` — `LiveOutput` abstract base: `push(rows: list[dict]) -> None`. Both SSEOutput and KafkaOutput implement this.
- `provisa/api/data/subscribe.py` — extend SSE endpoint to accept `query_id` parameter. When `query_id` is provided, subscribes to the live query engine's SSE output instead of a raw table. Full security pipeline (RLS, masking, role auth) still applied per row on delivery.
- `provisa/core/config_loader.py` — on config load, validate all live configs: `watermark_column` present for poll delivery, `delivery: cdc` rejected for non-CDC sources, `poll_interval` parseable. Fail-fast on validation error (REQ-064).

**Key behaviors:**
- Watermarks are per (query_id, output_type) — SSE and Kafka outputs advance independently (REQ-286).
- Process restart resumes from last watermark — no replay, no gap (REQ-287).
- Persisted queries always use `delivery: poll`; config that specifies `delivery: cdc` on a persisted query is a validation error (REQ-284).
- Full governance pipeline (RLS, masking, column visibility) applied to every row emitted, regardless of output type (REQ-038–042).
- `watermark_column` must appear in the query SELECT list for persisted queries (validated at config load time).

**Verify:**
- `python -m pytest tests/unit/test_live_engine.py -x -q`:
  - Poll job executes query with correct `WHERE watermark_col > last_watermark` bound
  - Rows fanned out to all registered outputs
  - Watermark committed after successful delivery
  - SSE and Kafka watermarks advance independently
  - Restart resumes from committed watermark (mock DB, mock outputs)
  - Config with missing `watermark_column` raises validation error
  - Config with `delivery: cdc` on persisted query raises validation error
- `python -m pytest tests/integration/test_live_engine.py -x -q`:
  - Insert rows into source table → SSE client receives events within poll interval
  - Insert rows into source table → Kafka topic receives messages within poll interval
  - Both outputs active simultaneously — both receive same rows
  - Watermark state survives process restart (real PG)

**Files:**
| File | Action |
|------|--------|
| `provisa/core/models.py` | Modify (add LiveOutputConfig, LiveDeliveryConfig) |
| `provisa/core/schema.sql` | Modify (add live_query_state table) |
| `provisa/live/__init__.py` | Create |
| `provisa/live/engine.py` | Create |
| `provisa/live/watermark.py` | Create |
| `provisa/live/outputs/__init__.py` | Create |
| `provisa/live/outputs/base.py` | Create |
| `provisa/live/outputs/sse.py` | Create |
| `provisa/live/outputs/kafka.py` | Create |
| `provisa/api/data/subscribe.py` | Modify (add query_id subscription path) |
| `provisa/core/config_loader.py` | Modify (live config validation) |
| `tests/unit/test_live_engine.py` | Create |
| `tests/integration/test_live_engine.py` | Create |
| `tests/test_federation_hints.py` | Create |

---

## Phase AN: Automatic Persisted Queries (APQ)
**Goal:** Implement the Apollo APQ wire protocol. Clients send a SHA256 hash instead of full query text on repeat calls. Pre-approved table queries are cached automatically on first execution. Governed Query hashes are registered at approval time. Registry-required table queries cannot be cached until approved.
**REQs:** REQ-288, REQ-289, REQ-290, REQ-291, REQ-292
**Depends on:** Phase D (query execution pipeline), Phase H (Governed Query registry), Phase O (Redis cache)

### Wire Protocol

Standard Apollo APQ — no client-side changes required:

```
# First call (hash only — cache miss)
POST /data/graphql
{"extensions": {"persistedQuery": {"version": 1, "sha256Hash": "<hash>"}}}

← {"errors": [{"message": "PersistedQueryNotFound", "extensions": {"code": "PERSISTED_QUERY_NOT_FOUND"}}]}

# Second call (full query + hash — cache store)
POST /data/graphql
{"query": "{ orders { id amount } }", "extensions": {"persistedQuery": {"version": 1, "sha256Hash": "<hash>"}}}

← {"data": {...}}   # stored in APQ cache; hash usable from now on

# Subsequent calls (hash only — cache hit)
POST /data/graphql
{"extensions": {"persistedQuery": {"version": 1, "sha256Hash": "<hash>"}}}

← {"data": {...}}
```

### Governance interaction

```
Hash-only request arrives
        ↓
APQ cache lookup
        ↓
    Cache hit?
   ↙          ↘
  Yes           No
   ↓             ↓
Governance    Return PersistedQueryNotFound
check          (client will resend with query text)
   ↓
Permitted?
   ↓
Execute
```

On full-query submission (hash + query text):
- Governance check runs first
- If rejected (registry-required table, no Governed Query approval) → error returned, hash NOT stored
- If permitted → execute, then store hash → Redis

### Governed Query name-based execution

Governed Queries are executed by developer-chosen name — no hash involved. The endpoint accepts `{"queryId": "ActiveOrders"}` and looks up the approved query text from the registry. APQ and Governed Query execution are separate paths that never intersect.

### Build

- `provisa/api/data/endpoint.py` — detect `extensions.persistedQuery` in request body. Extract hash. Branch: hash-only → APQ lookup → governance check → execute. Hash + query → governance check → execute → APQ store.
- `provisa/apq/__init__.py`
- `provisa/apq/cache.py` — `APQCache`: `get(hash) -> str | None`, `set(hash, query_text, ttl)`, `delete(hash)`. Backed by Redis. TTL configurable via `apq.ttl` (REQ-289).
- `provisa/api/data/endpoint.py` — two distinct execution branches: (1) APQ path: detects `extensions.persistedQuery.sha256Hash`, does cache lookup, governance check, execute, store. (2) Governed Query path: detects `queryId` field, looks up approved query text from registry by name, governance check (approval already granted), execute. Neither path calls into the other.

**Key behaviors:**
- APQ cache miss on a hash-only request always returns `PersistedQueryNotFound` — never a governance error. Client cannot infer governance state from APQ responses.
- Governance check runs before APQ store — a rejected query is never cached (REQ-291).
- Governed Query `queryId` path looks up query text from registry by name; approval already granted so no steward check at execution time — only RLS, masking, and role enforcement.
- APQ and Governed Query are separate execution paths. Neither calls into the other (REQ-292).

**Verify:**
- `python -m pytest tests/unit/test_apq.py -x -q`:
  - Hash-only request with cache miss → `PersistedQueryNotFound`
  - Hash + query, governance passes → executes, hash stored in cache
  - Hash + query, governance fails (registry-required, no approval) → error, hash NOT stored
  - Hash-only after store → executes
  - Governed Query approval → hash auto-registered, hash-only request executes immediately
  - Auto-cached hash respects TTL; Governed Query hash has no TTL

**Files:**
| File | Action |
|------|--------|
| `provisa/apq/__init__.py` | Create |
| `provisa/apq/cache.py` | Create |
| `provisa/apq/cache.py` | Create |
| `provisa/api/data/endpoint.py` | Modify (APQ and queryId branches) |
| `tests/unit/test_apq.py` | Create |

---

## Phase AO: Query-API Sources (Neo4j & SPARQL)
**Goal:** Add Neo4j and SPARQL as first-class non-Trino sources riding the existing API source pipeline (Phase U). Two targeted enhancements to the pipeline — HTTP POST body support and a named response normalizer hook — enable both connectors and establish a pattern for future query-API sources. Neo4j uses Cypher queries POSTed to the Neo4j HTTP API; the steward authors the query to return flat scalar projections and a preview step enforces this. SPARQL supports any SPARQL 1.1 compliant triplestore; the `sparql_bindings` normalizer unwraps the standard bindings envelope transparently.
**REQs:** REQ-295, REQ-296, REQ-297, REQ-298, REQ-299
**Depends on:** Phase U (API Sources — REST, GraphQL, gRPC)

### API Source Pipeline Enhancements (REQ-298, REQ-299)

Prerequisites for both connectors. Build first.

- `provisa/api_source/models.py` — add `response_normalizer: str | None = None` to `ApiEndpoint`
- `provisa/api_source/normalizers.py` — normalizer registry:
  - `NORMALIZERS: dict[str, Callable[[Any], list[dict]]]`
  - `neo4j_tabular(response)`: navigates `response["data"]`, zips `fields[]` with each entry in `values[][]` → `[{field: value, ...}]`
  - `sparql_bindings(response)`: extracts `response["results"]["bindings"]`, maps each `{var: {"type": ..., "value": v}}` → `{var: v}`
  - `get_normalizer(name) -> Callable` — raises `ValueError` on unknown name (caught at registration time)
- `provisa/api_source/flattener.py` — apply normalizer before `response_root` navigation:
  ```python
  if endpoint.response_normalizer:
      raw = get_normalizer(endpoint.response_normalizer)(raw)
  ```
- `provisa/api_source/caller.py` — POST body support (REQ-298):
  - Extend `_build_request()` to transmit body when `method == "POST"`
  - Neo4j: `json={"statement": query_text}` with `Content-Type: application/json`
  - SPARQL: `data={"query": query_text}` with `Content-Type: application/x-www-form-urlencoded`
  - Body type determined by new `body_encoding: Literal["json", "form"] | None` field on `ApiEndpoint`
  - Existing GET endpoints: no change

### Neo4j Connector (REQ-295, REQ-296)

- `provisa/core/models.py` — add `NEO4J = "neo4j"` to `SourceType` enum
- `provisa/neo4j/__init__.py`
- `provisa/neo4j/source.py` — `Neo4jSource`:
  - Builds `ApiSource` config: `type=NEO4J`, `base_url=http(s)://{host}:{port}`
  - Builds `ApiEndpoint` per registered table: `method="POST"`, `path="/db/{database}/query/v2"`, `body_encoding="json"`, `response_normalizer="neo4j_tabular"`
  - Steward-supplied Cypher stored as a `query_template` field on the endpoint config
  - Column definitions: steward-declared, matching `RETURN` aliases in the Cypher
- `provisa/neo4j/preview.py` — query preview (REQ-296):
  - `preview_query(source, cypher) -> list[dict]`: appends `LIMIT 5` to steward's Cypher, calls Neo4j HTTP API, runs normalizer, returns rows
  - `validate_shape(rows, columns)`: checks that returned keys are all scalar types (str/int/float/bool/None); raises `Neo4jNodeObjectError` if any value is a dict/list (indicates node object returned)
  - Registration endpoint calls `preview_query` + `validate_shape` before persisting the endpoint; returns validation error to UI if shape is wrong
- `provisa/api/admin/neo4j_router.py` — admin endpoints:
  - `POST /admin/sources/neo4j` — register Neo4j source
  - `POST /admin/sources/neo4j/{source_id}/preview` — preview Cypher query (returns sample rows or shape error)
  - `POST /admin/sources/neo4j/{source_id}/tables` — register table (runs preview+validate internally)

**Registration flow:**
```
1. Steward registers Neo4j source: host, port, database, auth
2. Steward authors a Cypher query (RETURN scalar projections only)
3. UI calls /preview → shows sample rows; blocks if node objects detected
4. Steward confirms columns (names derived from RETURN aliases) + sets TTL
5. Table registered → appears in GraphQL schema via existing api_source/schema_integration.py
6. Queries: cache hit → direct PG read; cache miss → POST Cypher → neo4j_tabular normalizer → flattener → PG cache write
```

### SPARQL Connector (REQ-297)

- `provisa/core/models.py` — add `SPARQL = "sparql"` to `SourceType` enum
- `provisa/sparql/__init__.py`
- `provisa/sparql/source.py` — `SparqlSource`:
  - Builds `ApiSource` config: `type=SPARQL`, `base_url={endpoint_url}`
  - Builds `ApiEndpoint` per registered table: `method="POST"`, `path=""` (base URL is the SPARQL endpoint), `body_encoding="form"`, `response_normalizer="sparql_bindings"`
  - Steward-supplied SPARQL SELECT stored as `query_template`
  - Optional `default_graph_uri` injected as `default-graph-uri` form parameter per SPARQL 1.1 protocol
  - Column names inferred from SPARQL SELECT variable names at registration time (parsed from `SELECT ?var1 ?var2` clause); steward can override
- `provisa/api/admin/sparql_router.py` — admin endpoints:
  - `POST /admin/sources/sparql` — register SPARQL source
  - `POST /admin/sources/sparql/{source_id}/tables` — register table (executes `SELECT ... LIMIT 5` probe to validate endpoint reachability and infer column names)

**Registration flow:**
```
1. Steward registers SPARQL source: endpoint URL, auth, optional default graph URI
2. Steward authors a SPARQL SELECT query
3. Provisa executes SELECT ... LIMIT 5 probe: validates endpoint + infers column names from SELECT variables
4. Steward confirms columns + sets TTL
5. Table registered → appears in GraphQL schema
6. Queries: cache hit → direct PG read; cache miss → POST SPARQL → sparql_bindings normalizer → flattener → PG cache write
```

**Verify:**
- `python -m pytest tests/unit/test_normalizers.py -x -q`:
  - `neo4j_tabular`: zips fields/values correctly; single row; multi-row; empty values
  - `sparql_bindings`: unwraps bindings envelope; mixed types (literal/uri/bnode → string value); empty bindings
  - Unknown normalizer name → `ValueError` at registration
- `python -m pytest tests/unit/test_neo4j_preview.py -x -q`:
  - Flat scalar Cypher → preview returns rows, validation passes
  - Node object in response → `Neo4jNodeObjectError` raised, registration blocked
  - LIMIT 5 appended even when steward omits it; not doubled if steward includes it
- `python -m pytest tests/integration/test_neo4j_source.py -x -q` (requires Neo4j container):
  - Source registration → table registration → query → cache miss → API call → rows returned
  - Second query → cache hit → no API call
- `python -m pytest tests/integration/test_sparql_source.py -x -q` (requires Fuseki container):
  - Source registration → table registration → query → cache miss → SPARQL POST → rows returned

**Files:**
| File | Action |
|------|--------|
| `provisa/core/models.py` | Modify (add `NEO4J`, `SPARQL` to `SourceType`) |
| `provisa/api_source/models.py` | Modify (add `response_normalizer`, `body_encoding` to `ApiEndpoint`) |
| `provisa/api_source/normalizers.py` | Create |
| `provisa/api_source/flattener.py` | Modify (apply normalizer before root navigation) |
| `provisa/api_source/caller.py` | Modify (POST body support) |
| `provisa/neo4j/__init__.py` | Create |
| `provisa/neo4j/source.py` | Create |
| `provisa/neo4j/preview.py` | Create |
| `provisa/sparql/__init__.py` | Create |
| `provisa/sparql/source.py` | Create |
| `provisa/api/admin/neo4j_router.py` | Create |
| `provisa/api/admin/sparql_router.py` | Create |
| `tests/unit/test_normalizers.py` | Create |
| `tests/unit/test_neo4j_preview.py` | Create |
| `tests/integration/test_neo4j_source.py` | Create |
| `tests/integration/test_sparql_source.py` | Create |
