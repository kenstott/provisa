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
| I: Output Formats & Arrow Flight | REQ-045, REQ-048, REQ-049, REQ-050, REQ-051 |
| J: Large Result Redirect | REQ-006, REQ-029, REQ-044 |
| K: Admin API (Strawberry) | REQ-059, REQ-060 |
| L: Connection Pooling Hardening | REQ-052, REQ-053 |
| M: Production Infrastructure | REQ-056 |
| N: UI | REQ-058, REQ-059, REQ-060, REQ-061, REQ-062, REQ-063 |
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
**Goal:** Multiple output formats beyond JSON. gRPC Arrow Flight for high-throughput.
**REQs:** REQ-045, REQ-048, REQ-049, REQ-050, REQ-051

**Build:**
- `provisa/executor/formats/__init__.py`
- `provisa/executor/formats/ndjson.py` — NDJSON streaming: one JSON object per line (REQ-048)
- `provisa/executor/formats/tabular.py` — normalized (relational tables with FKs, Parquet/CSV) and denormalized (fully flattened, Parquet/CSV) (REQ-049, REQ-050)
- `provisa/executor/formats/arrow.py` — Arrow buffer serialization (REQ-051)
- `provisa/api/flight/__init__.py`
- `provisa/api/flight/server.py` — gRPC Arrow Flight endpoint (REQ-045). Trino produces Arrow natively for zero-copy delivery.
- Update `provisa/api/data/endpoint.py` — content negotiation for output format

**Verify:**
- `python -m pytest tests/unit/test_formats.py -x -q` — serialization logic for each format
- `python -m pytest tests/e2e/test_output_formats.py -x -q`:
  - Accept: application/x-ndjson → NDJSON response
  - Normalized tabular → Parquet with FK relationships
  - Denormalized tabular → single flat Parquet
  - Arrow Flight client → receives Arrow buffers

**Files:**
| File | Action |
|---|---|
| `provisa/executor/formats/__init__.py` | Create |
| `provisa/executor/formats/ndjson.py` | Create |
| `provisa/executor/formats/tabular.py` | Create |
| `provisa/executor/formats/arrow.py` | Create |
| `provisa/api/flight/__init__.py` | Create |
| `provisa/api/flight/server.py` | Create |
| `provisa/api/data/endpoint.py` | Modify |
| `tests/unit/test_formats.py` | Create |
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
**Goal:** React-based UI with role-driven rendering.
**REQs:** REQ-058, REQ-059, REQ-060, REQ-061, REQ-062, REQ-063

**Build:**
- `provisa-ui/` — React app (Vite + TypeScript)
- Role composition system: rendered surface determined by assembled role set (REQ-058, REQ-059)
- Capability-driven views: Source Registration, Table Registration, Relationship Registration, Security Config, Query Development, Query Approval, Admin (REQ-060)
- Confirmation dialogs for destructive actions with consequence summary (REQ-061)
- Test endpoint UI: shows RLS filters applied, columns excluded, schema scope in result metadata (REQ-062)
- Approval queue: steward-optimized, rejection requires specific actionable reason (REQ-063)

**Verify:**
- `npx playwright test` — Playwright E2E tests:
  - Login with different roles → different UI surfaces rendered
  - Source registration workflow: add source → register table → appears in query builder
  - Query builder → test execution → shows RLS metadata → submit for approval → approve/reject
  - Destructive action → confirmation dialog shown with consequence summary
  - Approval queue: list pending, approve with comment, reject with actionable reason

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
| N | None | None | `provisa-ui/e2e/` — Playwright: role-based rendering, registration workflow, query builder, approval queue, confirmation dialogs |

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
