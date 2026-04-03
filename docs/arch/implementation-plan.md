# Implementation Plan

## Context

New Provisa features for Hasura v2 parity, installer packaging, then migration converters. Requirements: REQ-182 through REQ-229.

**Priority**: Low-complexity features first (quick wins) -> medium-complexity features -> installer -> v2 converter -> DDN converter.

**Phase naming**: Continues from existing Provisa phases. New phases: O through V.

**Gate rules**: Every phase ends with a verification gate (tests pass, feature works end-to-end) and a documentation gate (docs/configuration.md updated, CHANGELOG entry, API reference if applicable).

---

## Phase O: Quick Wins -- Low-Complexity Features

Features that are essentially a new arg/flag + small SQL/config extension. Each ~100-200 lines.

### O1. Direct-Route Dialect Expansion (REQ-229)

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

### O2. Upsert Mutations (REQ-212)

New `upsert_<table>` mutation field in `mutation_gen.py`. Generates `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`. Conflict columns from PK metadata (already available in TableMeta). SQLGlot transpiles to MySQL/MSSQL syntax.

**Files**: `provisa/compiler/mutation_gen.py`, `provisa/compiler/schema_gen.py`
**Effort**: ~150 lines

### O3. DISTINCT ON (REQ-213)

Add `distinct_on` arg to root query fields. In `sql_gen.py`, prepend `DISTINCT ON (col)` to SELECT. SQLGlot provides window function fallback for non-PG dialects.

**Files**: `provisa/compiler/sql_gen.py`, `provisa/compiler/schema_gen.py`
**Effort**: ~100 lines

### O4. Column Presets (REQ-214)

Config per table: `column_presets` list. Middleware in mutation compilation injects values from headers/session/`now()` before SQL generation. Mirrors existing masking middleware pattern.

**Files**: `provisa/core/models.py` (config), `provisa/compiler/mutation_gen.py` (injection)
**Effort**: ~200 lines

### O5. Inherited Roles (REQ-215)

Add optional `parent_role_id` to Role model. Flatten at startup: recurse and merge capabilities + domain_access. Cache flattened roles in AppState. Lookups remain O(1).

**Files**: `provisa/core/models.py`, `provisa/core/schema.sql`, `provisa/api/app.py` (startup)
**Effort**: ~100 lines

### O6. Scheduled Triggers (REQ-216)

Add `apscheduler` dependency. Config section `scheduled_triggers` with cron expressions. Wire into FastAPI lifespan. Reuses existing async background task pattern (MV refresh).

**Files**: `provisa/core/models.py`, `provisa/api/app.py`, new `provisa/scheduler/jobs.py`
**New dependency**: `apscheduler`
**Effort**: ~100 lines

### O7. Document Batch Mutations (REQ-217)

Already works -- GraphQL spec executes multiple mutations sequentially. Just document.

**Effort**: 0 lines (docs only)

### Phase O Gates

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
- `CHANGELOG.md` entry for Phase O
- `docs/api-reference.md` updated: new mutation/query args

---

## Phase P: Medium-Complexity Hasura v2 Parity

### P1. Cursor Pagination (REQ-218)

Add `first`, `after`, `last`, `before` args to root query fields alongside existing `limit`/`offset`. Cursor = base64(sort_key_values). Compile to `WHERE id > decoded_cursor LIMIT first+1`. Return `edges[{cursor, node}]` + `pageInfo`. No Relay library needed -- custom implementation with graphql-core.

**Files**: `provisa/compiler/sql_gen.py`, `provisa/compiler/schema_gen.py`, serializer
**Effort**: ~300 lines

### P2. Subscriptions via SSE (REQ-219)

New `GET /data/subscribe/{table}` endpoint. FastAPI `StreamingResponse` with `text/event-stream`. PostgreSQL `LISTEN/NOTIFY` via asyncpg `.add_listener()`. No WebSocket, no new library.

**Files**: new `provisa/api/data/subscribe.py`, `provisa/api/app.py` (route)
**Effort**: ~200 lines

### P3. Database Event Triggers (REQ-220)

PostgreSQL trigger + `pg_notify()` -> asyncpg listener -> HTTP POST to webhook URL. Config per table with operation filter (insert/update/delete) and retry policy. Reuses asyncpg pool.

**Files**: new `provisa/events/triggers.py`, `provisa/core/models.py` (config), `provisa/api/app.py` (startup listeners)
**Effort**: ~200 lines

### P4. Enum Table Auto-Detection (REQ-221)

Introspect `pg_enum` + `pg_type` at schema build time. Generate `GraphQLEnumType` per PG enum. Map columns with enum type to GraphQL enum instead of String in type_map.

**Files**: `provisa/compiler/introspect.py`, `provisa/compiler/schema_gen.py`, `provisa/compiler/type_map.py`
**Effort**: ~250 lines

### P5. REST Endpoint Auto-Generation (REQ-222)

For each root query field in compiled schema, generate `GET /data/rest/{table}` FastAPI endpoint. Map `?limit=10&where.id.eq=1` to GraphQL args. Compile and execute via existing pipeline.

**Files**: new `provisa/api/rest/generator.py`, `provisa/api/app.py` (mount routes)
**Effort**: ~300 lines

### Phase P Gates

**Verification**:
- Cursor pagination: forward + backward paging, verify `hasNextPage`/`hasPreviousPage` correctness
- SSE: subscribe to table, insert row, verify event received via `curl` or test client
- Event triggers: insert/update/delete on table, verify webhook called with correct payload
- Enum detection: create PG enum, verify GraphQL schema contains enum type with correct values
- REST: `GET /data/rest/{table}?limit=5` returns same data as equivalent GraphQL query

**Documentation**:
- `docs/configuration.md`: event_triggers, scheduled_triggers config
- `docs/api-reference.md`: cursor pagination args, SSE endpoint, REST endpoints
- `docs/security.md`: SSE auth, REST auth (same pipeline as GraphQL)
- `CHANGELOG.md` entry for Phase P

---

## Phase Q: Tracked Functions & Webhook Mutations (REQ-205-211)

### Config Models

New `Function` and `Webhook` Pydantic models. `functions` and `webhooks` sections in `provisa.yaml`. Function results map to registered table's GraphQL type (full governance pipeline). Webhooks support inline return types.

### Implementation

1. Models: `provisa/core/models.py` -- Function, Webhook, FunctionArgument, InlineType
2. Schema: `provisa/compiler/schema_gen.py` -- mutation fields for functions/webhooks
3. SQL: `provisa/compiler/sql_gen.py` -- `SELECT * FROM schema.func($1, $2)` parameterized calls
4. Webhooks: new `provisa/webhooks/executor.py` -- HTTP POST, timeout, response mapping
5. Repository: new `provisa/core/repositories/function.py`
6. Config loader: persist functions/webhooks

### Phase Q Gates

**Verification**:
- Create PG function returning `SETOF orders`, register in config, call via GraphQL mutation
- Verify function result goes through column governance (visibility, masking)
- Webhook mutation: mock HTTP endpoint, verify call with correct args, verify response mapping
- Inline return type: verify custom GraphQL type generated
- Test argument validation: invalid arg type rejected at parse time

**Documentation**:
- `docs/configuration.md`: `functions` and `webhooks` config sections with examples
- `docs/api-reference.md`: function/webhook mutation fields
- `docs/security.md`: function result governance pipeline
- `CHANGELOG.md` entry for Phase Q

---

## Phase R: Schema Alignment (REQ-194-202)

### R1. Naming Convention (REQ-194-195)

Add `naming.convention` field: `snake_case` (default), `camelCase`, `PascalCase`. Auto-generates aliases at schema build time. Explicit `column.alias` takes precedence.

**Files**: `provisa/core/models.py`, `provisa/compiler/schema_gen.py`

### R2. OrderBy Alignment (REQ-200-202)

Replace `{field: ENUM, direction: ENUM}` with Hasura v2's `{column: direction}` pattern. 6-value direction enum with null placement. Relationship ordering via JOIN-aware compilation.

**Files**: `provisa/compiler/schema_gen.py`, `provisa/compiler/sql_gen.py`, tests

### R3. Aggregates (REQ-196-199)

Auto-generate `<table>_aggregate` root fields. Type-aware function selection (sum/avg for numeric, min/max for comparable, count for all). Per-role gating. Optional explicit config override. Aggregate MV routing in compiler.

**Files**: `provisa/core/models.py`, `provisa/compiler/schema_gen.py`, `provisa/compiler/sql_gen.py`

### Phase R Gates

**Verification**:
- Naming: set `convention: camelCase`, verify `user_id` -> `userId` in schema, verify explicit alias overrides convention
- OrderBy: `order_by: [{created_at: desc_nulls_last}]` compiles to `ORDER BY created_at DESC NULLS LAST`
- OrderBy: relationship ordering `order_by: [{author: {name: asc}}]` generates correct JOIN + ORDER BY
- Aggregates: `orders_aggregate { aggregate { count sum { amount } } }` returns correct values
- Aggregate MV routing: verify compiler rewrites to MV when pattern matches
- **Breaking change audit**: orderBy schema change breaks existing queries -- document migration path

**Documentation**:
- `docs/configuration.md`: `naming.convention`, `aggregates` config
- `docs/api-reference.md`: new orderBy format, aggregate queries
- `docs/migration-guide.md`: orderBy breaking change migration
- `CHANGELOG.md` entry for Phase R

---

## Phase S: ABAC Approval Hook (REQ-203-204)

Config: `auth.approval_hook` with type/url/timeout_ms/fallback. Abstract interface + webhook implementation. Hook into query pipeline between RLS injection and execution. Circuit breaker for timeout.

**Files**: `provisa/core/models.py`, new `provisa/auth/approval_hook.py`, query pipeline

### Phase S Gates

**Verification**:
- Webhook hook: mock approval endpoint, verify request payload contains user/roles/tables/columns/operation
- Approve: query executes normally
- Deny: query rejected with reason
- Timeout + fallback=deny: verify query rejected after timeout_ms
- Timeout + fallback=allow: verify query executes after timeout_ms
- No hook configured: verify zero overhead (no HTTP call)

**Documentation**:
- `docs/configuration.md`: `auth.approval_hook` config
- `docs/security.md`: ABAC hook architecture, request/response format, timeout behavior
- `CHANGELOG.md` entry for Phase S

---

## Phase T: Installer & Packaging (REQ-223-228)

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

### T1: Shell Script Installer (Immediate)
- `provisa` CLI wrapper (shell script) that manages Docker Compose
- Stores config/data in `~/.provisa/`
- Commands: `provisa start`, `provisa stop`, `provisa status`, `provisa open`
- Prerequisite: Docker/OrbStack/Colima (auto-detected)
- All service names branded "Provisa" (no docker/trino/pg visible in logs)
- **Effort**: ~500 lines shell script

### T2: Embedded Binary (Medium-term)
- Compile Python backend with Nuitka -> native binary
- Embed pgserver for admin DB (pip-installable embedded PostgreSQL)
- Bundle Trino binary (downloaded on first run, cached in `~/.provisa/`)
- Bundle React UI build as static assets
- Docker dependency eliminated except for optional Trino
- **Effort**: Significant (Nuitka build pipeline, pgserver integration)

### T3: Native OS Packages (Long-term)
- macOS: `.pkg` with LaunchAgent for service management
- Linux: `.deb` with systemd unit files
- Windows: `.msi` via WiX with Windows Service registration
- Each bundles all services, uses native OS service lifecycle
- **Effort**: Per-platform installer build

### Phase T Gates

**Verification (T1)**:
- Fresh machine with Docker: `./install.sh && provisa start` brings up all services
- `provisa status` shows all healthy
- `provisa open` opens browser to working UI
- `provisa stop` cleanly shuts down all services
- No "docker", "trino", "postgresql" visible in user-facing output
- `~/.provisa/` contains all state, removable with `provisa uninstall`

**Verification (T2/T3)**:
- Single binary/package installs without Docker prerequisite
- Services start via native OS service manager
- Upgrade path: `provisa upgrade` pulls new version without data loss

**Documentation**:
- `README.md`: installation instructions (replaces Docker Compose setup)
- `docs/installation.md`: per-platform guide, prerequisites, troubleshooting
- `docs/configuration.md`: how to connect external Trino/auth/PostgreSQL
- `CHANGELOG.md` entry for Phase T

---

## Phase U: Hasura v2 Metadata Converter (REQ-182, REQ-184-193)

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

### Phase U Gates

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
- `CHANGELOG.md` entry for Phase U

---

## Phase V: DDN (Hasura v3) HML Converter (REQ-183, REQ-189, REQ-191)

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

### Phase V Gates

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
- `CHANGELOG.md` entry for Phase V

---

## Phase Summary

| Phase | What | Effort | Dependencies |
|-------|------|--------|-------------|
| O | Quick wins: dialect expansion, upsert, distinct_on, presets, inherited roles, cron | Low | None |
| P | Cursor pagination, SSE subscriptions, event triggers, enums, REST auto-gen | Medium | None |
| Q | Tracked functions & webhook mutations | Medium | None |
| R | Naming convention, orderBy alignment, aggregates | Medium | None |
| S | ABAC approval hook | Medium | None |
| T | Installer & packaging (T1/T2/T3) | Low -> High | Phases O-S for features |
| U | Hasura v2 converter | Medium | Phases O-R (features must exist) |
| V | DDN converter | Medium | Phase U (shared infra) |

Phases O-S are independent and can be parallelized. Phase T1 (shell script) can start anytime.

## Sample Hasura v2 Projects for End-to-End Testing

| Project | Repo | Features |
|---------|------|----------|
| 3factor-example (food ordering) | `hasura/3factor-example` | Tables, relationships, event triggers, migrations |
| realtime-poll | `hasura/sample-apps/realtime-poll` | Simpler structure, subscriptions |
| demo-apps | `hasura/demo-apps` | Multiple configs, Docker Compose, roles |
| metadata-api-example (Chinook) | `hasura/metadata-api-example` | Chinook DB, relationships |
