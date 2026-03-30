# Plan: Provisa Core Engine — Config, Compiler, Executor

## Context

Build the full Provisa engine: config infrastructure, GraphQL schema generation from Trino catalog, query compilation (GraphQL→SQL), execution (Trino or direct RDBMS), and response serialization. YAML config seeds a PostgreSQL config DB. Two GraphQL surfaces: Strawberry for admin CRUD, purpose-built compiler for the governed data query surface.

## Architecture

```
config.yaml → Config Loader → PostgreSQL (config DB)
                                    ↑
                              Strawberry Admin API (CRUD)       ← admin surface
                                    ↑
                              Apollo React UI (later)

                              Registration Model (PG)
                                    ↓
Trino INFORMATION_SCHEMA → Schema Generator → GraphQL SDL
                                                    ↓
                           GraphQL Query → Compiler → PG-style SQL
                                                         ↓
                                              SQLGlot → Target dialect SQL
                                                         ↓
                                              Executor → Trino or Direct RDBMS
                                                         ↓
                                              Serializer → GraphQL JSON response
```

Docker Compose runs: PostgreSQL, Trino (with PG connector), Provisa app.

## Phase 1: Infrastructure

### 1.1 Docker Compose (`docker-compose.yml`)
- **postgres** — config DB, port 5432, volume for persistence
- **trino** — coordinator with PG connector, port 8080
- **provisa** — FastAPI app, depends_on postgres + trino

### 1.2 Trino PG Connector (`trino/catalog/postgresql.properties`)
- Points at the Docker Compose PG instance
- Enables Trino to introspect PG schemas via INFORMATION_SCHEMA

### 1.3 Config YAML Schema (`config/provisa.yaml`)
Flat, normalized — each key maps to a PG table. Domains are a governance layer grouping tables across sources.

```yaml
sources:
  - id: sales-pg
    type: postgresql
    host: sales-db.internal
    port: 5432
    database: sales
    username: reader
    password_env: SALES_PG_PASSWORD  # env var reference, not plaintext

  - id: warehouse-sf
    type: snowflake
    account: xy12345
    database: analytics
    username: reader
    password_env: SF_PASSWORD

domains:
  - id: sales-analytics
    description: Sales operational and analytical data
  - id: hr-reporting
    description: HR and workforce data

naming:
  # Auto-generates shortest unique name within each domain by default.
  # Regex rules applied in order after auto-generation. Explicit alias always wins.
  rules:
    - pattern: "^prod_pg_"
      replace: ""
    - pattern: "_view$"
      replace: ""

tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    governance: pre-approved
    # Auto-generated GraphQL name: "orders" (unique in domain, no prefix needed)
    columns:
      - name: id
        visible_to: [admin, analyst, viewer]
      - name: customer_id
        visible_to: [admin, analyst]
      - name: amount
        visible_to: [admin, analyst]
      - name: ssn
        visible_to: [admin]

  - source_id: warehouse-sf
    domain_id: sales-analytics
    schema: reporting
    table: sales_summary
    governance: registry-required
    # Auto-generated: "sales_summary" (unique)

  - source_id: sales-pg
    domain_id: hr-reporting
    schema: hr
    table: employees
    governance: pre-approved
    alias: staff               # explicit override
    # GraphQL name: "staff" (alias wins)

relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one

roles:
  - id: admin
    capabilities: [source_registration, table_registration, relationship_registration, security_config, query_development, query_approval, admin]
    domain_access: ["*"]             # all domains
  - id: sales-analyst
    capabilities: [query_development]
    domain_access: [sales-analytics] # scoped to domain
  - id: hr-viewer
    capabilities: []
    domain_access: [hr-reporting]

rls_rules:
  - table_id: orders
    role_id: sales-analyst
    filter: "region = current_setting('provisa.user_region')"
```

**Naming in Trino:** Each source registration creates a Trino catalog. The catalog name = source id (e.g., `sales_pg`). Trino sees `sales_pg.public.orders` and `sales_pg.hr.employees`. Domains are a Provisa-only concept — Trino doesn't know about them. Domains control which tables a user's role can see in the GraphQL schema and query builder.

## Phase 2: Config DB Schema

### 2.1 PostgreSQL Tables (`provisa/core/schema.sql`)
Tables mirror YAML structure:
- `sources` (id, type, host, port, database, username, password_env, created_at)
- `domains` (id, description, created_at)
- `tables` (id, source_id FK, domain_id FK, schema, table_name, governance, created_at)
- `table_columns` (table_id FK, column_name, visible_to jsonb)
- `relationships` (id, source_table_id FK, target_table_id FK, source_column, target_column, cardinality)
- `roles` (id, capabilities jsonb, domain_access jsonb)
- `rls_rules` (id, table_id FK, role_id FK, filter_expression)

Later additions (not in this phase):
- `persisted_queries` (registry)
- `users` (auth)
- `approval_log` (audit)

### 2.2 Config Loader (`provisa/core/config_loader.py`)
- Reads YAML file
- Connects to PG
- Upserts all entities (idempotent — safe to re-run)
- Validates referential integrity (source_id exists, table_id exists, etc.)
- Resolves `password_env` to actual env var values (never stored in DB)
- Runs on app startup and on explicit reload

## Phase 3: Repository Layer

### 3.1 Repository Classes (`provisa/core/repositories/`)
- `SourceRepository` — CRUD for sources
- `DomainRepository` — CRUD for domains
- `TableRepository` — CRUD for tables + columns
- `RelationshipRepository` — CRUD for relationships
- `RoleRepository` — CRUD for roles + domain access
- `RLSRepository` — CRUD for RLS rules

Each repository:
- Takes an async PG connection pool (asyncpg)
- Returns Pydantic models
- Used by both config loader and Strawberry resolvers

## Phase 4: Strawberry Admin API

### 4.1 Strawberry Types (`provisa/api/admin/types.py`)
Mirror Pydantic models as Strawberry types.

### 4.2 Strawberry Schema (`provisa/api/admin/schema.py`)
- Queries: `sources`, `tables`, `relationships`, `roles`, `rls_rules`
- Mutations: `create_source`, `update_source`, `delete_source`, etc. for each entity
- Mount at `/admin/graphql`

### 4.3 FastAPI Integration (`provisa/api/app.py`)
- Mount Strawberry GraphQL app at `/admin/graphql`
- Health endpoint at `/health`
- Config reload endpoint at `POST /admin/reload-config`

## Phase 5: Schema Introspection & SDL Generation

### 5.1 Trino Introspector (`provisa/compiler/introspect.py`)
- Connects to Trino via trino-python-client
- For each registered table, queries `INFORMATION_SCHEMA.COLUMNS` for column names, types, nullability
- Queries `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE` for FK metadata (candidate relationship inference)
- Returns structured column/type metadata per table
- Runs at startup and on registration model change

### 5.2 Type Mapper (`provisa/compiler/type_map.py`)
Maps Trino SQL types to GraphQL scalar types:
- `VARCHAR/CHAR` → `String`
- `INTEGER/BIGINT/SMALLINT` → `Int`
- `DOUBLE/REAL/DECIMAL` → `Float`
- `BOOLEAN` → `Boolean`
- `TIMESTAMP/DATE/TIME` → custom `DateTime`/`Date`/`Time` scalars
- `JSON/JSONB` → custom `JSON` scalar
- Nullability preserved from column metadata

### 5.3 SDL Generator (`provisa/compiler/schema_gen.py`)
Builds GraphQL SDL from registration model + Trino metadata:
- Schema is domain-scoped: a role with `domain_access: [sales-analytics]` sees only tables in that domain
- Each registered table in accessible domains → GraphQL object type (filtered by role's column visibility)
- Column names → field names with mapped scalar types
- Registered relationships → relationship fields (many-to-one → object field, one-to-many → list field)
- Cross-domain relationships visible only if role has access to both domains
- Root query fields per table with filter/pagination arguments:
  - `where` argument with typed filter input (eq, gt, lt, in, like per column type)
  - `order_by`, `limit`, `offset` arguments
- Mutation types for RDBMS tables: `insert_<table>`, `update_<table>`, `delete_<table>`
- Output: `graphql-core` schema object (not a string — enables validation)

### 5.4 Schema Cache
- Generated schema cached per role (domain access + column visibility = unique schema per role)
- Invalidated on registration model change (table add/remove/modify, relationship change, domain change)
- `role_id` → cached schema object

## Phase 6: Query Compiler (GraphQL → PG-style SQL)

### 6.1 Query Parser (`provisa/compiler/parser.py`)
- Uses `graphql-core` to parse and validate GraphQL operation against generated schema
- Returns validated AST
- Rejects: unregistered tables, excluded columns, undefined relationships, type mismatches
- Precise error messages on validation failure

### 6.2 SQL Compiler (`provisa/compiler/sql_gen.py`)
Walks validated GraphQL AST, produces PG-style SQL:

| GraphQL Construct | SQL Output |
|---|---|
| Field selection | `SELECT` column projection |
| `where` argument | `WHERE` clause |
| `order_by` argument | `ORDER BY` clause |
| `limit`/`offset` | `LIMIT`/`OFFSET` |
| Nested relationship (many-to-one) | `LEFT JOIN` on relationship keys |
| Nested relationship (one-to-many) | Subquery or lateral join |
| Fragment spreads | Inline field expansion |

Produces a single SQL statement — no resolver chain, no N+1.

### 6.3 Parameter Binding
- GraphQL variables → parameterized SQL (`$1`, `$2`, etc.)
- Type validation of variable values against parameter schema
- Never interpolates values into SQL strings

### 6.4 RLS Injection (`provisa/compiler/rls.py`)
- After SQL compilation, injects RLS WHERE clauses from registration model
- Strips columns not visible to requesting user's role
- Applied to every query regardless of source (test endpoint, production, pre-approved)

## Phase 7: Transpilation (PG SQL → Target Dialect)

### 7.1 Transpiler (`provisa/transpiler/transpile.py`)
- Takes PG-style SQL + target dialect (from source registration)
- Uses SQLGlot to transpile:
  - Cross-source queries → Trino SQL
  - Single-source queries → target RDBMS dialect (postgres, mysql, mssql, etc.)
  - Mutations → target RDBMS dialect (always direct, never Trino)

### 7.2 Routing Decision (`provisa/transpiler/router.py`)
- Inspects compiled SQL to count distinct registered sources
- Single source → direct RDBMS path
- Multiple sources → Trino path
- Mutations → always direct RDBMS

## Phase 8: Executor

### 8.1 Trino Executor (`provisa/executor/trino.py`)
- Executes transpiled Trino SQL via trino-python-client
- Returns rows + column metadata
- Handles query cancellation, timeouts

### 8.2 Direct RDBMS Executor (`provisa/executor/direct.py`)
- Executes transpiled SQL against registered source via asyncpg (PostgreSQL) or appropriate async driver
- Uses connection pool from source registration
- Handles mutations (INSERT/UPDATE/DELETE)

### 8.3 Connection Pool Manager (`provisa/executor/pool.py`)
- Maintains warm connection pool per registered RDBMS source
- Pool created at source registration time
- Configurable min/max pool size per source

## Phase 9: Response Serializer

### 9.1 GraphQL Response Builder (`provisa/executor/serialize.py`)
- Maps SQL result rows back to GraphQL response shape
- Reconstructs nested objects from JOIN results:
  - many-to-one → single nested object
  - one-to-many → array of nested objects
- Handles null propagation for nullable relationships
- Output: standard GraphQL JSON response `{"data": {...}}`

### 9.2 Data Query Endpoint (`provisa/api/data/endpoint.py`)
- Mounts at `/data/graphql` (separate from admin surface)
- Accepts GraphQL operations
- Pipeline: parse → compile → RLS inject → transpile → route → execute → serialize
- Test mode: accepts arbitrary queries against registered schema
- Production mode: validates against registry or pre-approved table status

## Files to Create/Modify

### Infrastructure
| File | Action |
|---|---|
| `docker-compose.yml` | Create |
| `trino/catalog/postgresql.properties` | Create |
| `config/provisa.yaml` | Create (example config) |
| `pyproject.toml` | Create (dependencies) |
| `main.py` | Modify (point to new app factory) |

### Core (config DB, models, repositories)
| File | Action |
|---|---|
| `provisa/core/schema.sql` | Create |
| `provisa/core/config_loader.py` | Create |
| `provisa/core/models.py` | Create (Pydantic models) |
| `provisa/core/db.py` | Create (PG connection pool) |
| `provisa/core/repositories/source.py` | Create |
| `provisa/core/repositories/domain.py` | Create |
| `provisa/core/repositories/table.py` | Create |
| `provisa/core/repositories/relationship.py` | Create |
| `provisa/core/repositories/role.py` | Create |
| `provisa/core/repositories/rls.py` | Create |

### Compiler (schema gen, query compilation)
| File | Action |
|---|---|
| `provisa/compiler/introspect.py` | Create (Trino INFORMATION_SCHEMA reader) |
| `provisa/compiler/type_map.py` | Create (Trino→GraphQL type mapping) |
| `provisa/compiler/schema_gen.py` | Create (SDL generator) |
| `provisa/compiler/parser.py` | Create (GraphQL parse + validate) |
| `provisa/compiler/sql_gen.py` | Create (AST→PG SQL compiler) |
| `provisa/compiler/rls.py` | Create (RLS/column injection) |

### Transpiler & Executor
| File | Action |
|---|---|
| `provisa/transpiler/transpile.py` | Create (SQLGlot PG→target dialect) |
| `provisa/transpiler/router.py` | Create (single-source vs Trino routing) |
| `provisa/executor/trino.py` | Create (Trino query execution) |
| `provisa/executor/direct.py` | Create (direct RDBMS execution) |
| `provisa/executor/pool.py` | Create (connection pool manager) |
| `provisa/executor/serialize.py` | Create (SQL rows→GraphQL response) |

### API
| File | Action |
|---|---|
| `provisa/api/app.py` | Create (FastAPI app factory) |
| `provisa/api/admin/types.py` | Create (Strawberry types) |
| `provisa/api/admin/schema.py` | Create (Strawberry schema) |
| `provisa/api/data/endpoint.py` | Create (data query endpoint) |

### Tests
| File | Action |
|---|---|
| `tests/test_config_loader.py` | Create |
| `tests/test_repositories.py` | Create |
| `tests/test_type_map.py` | Create |
| `tests/test_schema_gen.py` | Create |
| `tests/test_sql_gen.py` | Create |
| `tests/test_rls.py` | Create |
| `tests/test_transpile.py` | Create |
| `tests/test_serialize.py` | Create |

## Dependencies

```
fastapi
uvicorn
asyncpg
pydantic>=2
strawberry-graphql[fastapi]
pyyaml
graphql-core>=3.2
sqlglot
trino
```

## Verification

1. `docker compose up` — PG + Trino + Provisa start
2. Config YAML consumed into PG on startup (check tables populated)
3. `curl localhost:8000/health` — returns OK
4. Open `localhost:8000/admin/graphql` — Strawberry GraphiQL, query/mutate sources and tables
5. Open `localhost:8000/data/graphql` — data query surface
6. Submit a GraphQL query against registered tables → get JSON response
7. Verify RLS filters applied in response metadata (test mode)
8. Verify cross-source query routes through Trino
9. Verify single-source query routes directly to RDBMS
10. `python -m pytest tests/ -x -q` — all tests pass

## Phases

Each phase is independently testable. Don't start the next until the current phase's verification passes.

---

### Phase A: Infrastructure
**Goal:** PG + Trino running, Trino can query PG tables.

**Build:**
- `docker-compose.yml` — postgres + trino containers
- `trino/catalog/postgresql.properties` — PG connector config
- Seed PG with a sample table (e.g., `demo.public.orders`) via init script

**Verify:**
- `docker compose up` starts both containers
- `psql` connects to PG, sample table exists
- Trino CLI: `SELECT * FROM postgresql.public.orders` returns rows
- Trino CLI: `SELECT * FROM postgresql.information_schema.columns WHERE table_name = 'orders'` returns column metadata

---

### Phase B: Config Model
**Goal:** YAML config consumed into PG, retrievable via repository layer.

**Build:**
- `provisa/core/models.py` — Pydantic models (Source, Table, Column, Relationship, Role, RLSRule)
- `provisa/core/schema.sql` — PG tables mirroring models
- `provisa/core/db.py` — asyncpg connection pool
- `provisa/core/config_loader.py` — read YAML, upsert into PG
- `provisa/core/repositories/` — CRUD for each entity
- `config/provisa.yaml` — example config referencing the demo PG source
- `pyproject.toml` — dependencies

**Verify:**
- `python -m pytest tests/test_config_loader.py` — YAML parsed, loaded into PG, round-trips correctly
- `python -m pytest tests/test_repositories.py` — CRUD operations work
- Config loader is idempotent (run twice, same result)

---

### Phase C: Schema Generation
**Goal:** Read Trino catalog + registration model → produce GraphQL SDL.

**Build:**
- `provisa/compiler/introspect.py` — query Trino INFORMATION_SCHEMA for registered tables
- `provisa/compiler/type_map.py` — Trino SQL types → GraphQL scalars
- `provisa/compiler/schema_gen.py` — build `graphql-core` schema from registration model + introspected metadata

**Verify:**
- `python -m pytest tests/test_type_map.py` — type mappings correct
- `python -m pytest tests/test_schema_gen.py` — given a registration model with tables/relationships/column visibility, produces valid GraphQL SDL
- Generated schema includes: object types per table, relationship fields, root query fields with filter/pagination args, mutation types
- Schema validates with `graphql-core` (no errors)
- Column visibility filtering works: different roles see different fields

---

### Phase D: Query Compiler
**Goal:** Parse GraphQL query against generated schema → produce PG-style SQL.

**Build:**
- `provisa/compiler/parser.py` — parse + validate GraphQL operation
- `provisa/compiler/sql_gen.py` — walk AST, emit PG SQL (SELECT, WHERE, JOIN, ORDER BY, LIMIT)
- `provisa/compiler/rls.py` — inject RLS WHERE clauses, strip invisible columns

**Verify:**
- `python -m pytest tests/test_sql_gen.py` — fixture-based:
  - Simple field selection → `SELECT col1, col2 FROM table`
  - Where filters → `WHERE col = $1`
  - Nested relationship → `LEFT JOIN` with correct keys
  - Pagination → `LIMIT`/`OFFSET`
  - Variables → parameterized bindings (never interpolated)
- `python -m pytest tests/test_rls.py` — RLS rules injected into WHERE clause per role
- Invalid queries (unregistered table, excluded column, bad type) rejected with precise errors

---

### Phase E: Transpilation & Routing
**Goal:** PG-style SQL → target dialect via SQLGlot, correct routing decision.

**Build:**
- `provisa/transpiler/transpile.py` — SQLGlot PG → target dialect
- `provisa/transpiler/router.py` — inspect SQL, count sources, decide Trino vs direct

**Verify:**
- `python -m pytest tests/test_transpile.py` — PG SQL correctly transpiles to Trino SQL, MySQL, MSSQL, etc.
- `python -m pytest tests/test_router.py` — single-source queries route direct, multi-source route to Trino, mutations always direct
- Edge cases: dialect-specific function mappings, type casting differences

---

### Phase F: Execution & Serialization
**Goal:** Execute transpiled SQL against Trino or direct RDBMS, serialize to GraphQL JSON.

**Build:**
- `provisa/executor/trino.py` — execute via trino-python-client
- `provisa/executor/direct.py` — execute via asyncpg
- `provisa/executor/pool.py` — connection pool per source
- `provisa/executor/serialize.py` — SQL rows → nested GraphQL JSON

**Verify:**
- Against running Docker Compose stack:
  - Single-source query → direct PG execution → correct JSON response
  - Cross-source query → Trino execution → correct JSON response
  - Mutation (INSERT/UPDATE/DELETE) → direct PG execution → correct response
- `python -m pytest tests/test_serialize.py` — JOIN result rows correctly reconstruct nested objects
- Response format matches GraphQL spec: `{"data": {...}}`

---

### Phase G: Data Query Endpoint
**Goal:** Full pipeline wired end-to-end behind a FastAPI endpoint.

**Build:**
- `provisa/api/app.py` — FastAPI app factory, startup hooks (config load, schema gen)
- `provisa/api/data/endpoint.py` — `/data/graphql` endpoint
- `main.py` — updated entry point

**Verify:**
- `docker compose up` → app starts, config loaded, schema generated
- `curl POST /data/graphql` with a query → JSON response with correct data
- RLS applied (test with different role contexts)
- Invalid queries rejected with GraphQL errors
- `curl GET /health` → OK

---

### Phase H: Strawberry Admin API
**Goal:** CRUD surface for managing config via GraphQL.

**Build:**
- `provisa/api/admin/types.py` — Strawberry types mirroring Pydantic models
- `provisa/api/admin/schema.py` — queries + mutations for sources, tables, relationships, roles, RLS rules
- Mount at `/admin/graphql`

**Verify:**
- Open `localhost:8000/admin/graphql` — GraphiQL explorer works
- Query all sources, tables, relationships via GraphQL
- Create/update/delete a source via mutation → DB updated
- Schema regenerated after table registration change (add table via mutation → new type appears in data schema)
