# Changelog

All notable changes to Provisa are documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Phase A: Infrastructure
- Docker Compose setup (PostgreSQL + Trino), dev and prod overlay
- Test infrastructure: conftest.py, fixture factories, sample config
- Dockerfile for stateless Provisa container
- Trino coordinator config, PG connector, demo seed data

### Phase B: Config & Auto-Catalog
- YAML config loader with idempotent PG upsert
- Trino dynamic catalog API (auto-create/delete catalogs on source add/remove)
- Pluggable secrets provider (`${env:VAR}` syntax, extensible to Vault/K8s)
- Pydantic models: Source, Domain, Table, Column, Relationship, Role, RLSRule
- CRUD repositories for all config entities

### Phase C: Schema Generation
- GraphQL SDL generation from registration model + Trino INFORMATION_SCHEMA
- Per-role schema: domain-scoped visibility, column filtering
- Relationship fields (many-to-one as object, one-to-many as list)
- Root query fields with `where`, `order_by`, `limit`, `offset` arguments
- Trino type to GraphQL scalar mapping
- Auto-generated shortest unique names, regex rules, alias override

### Phase D: Query Compilation & Trino Execution
- GraphQL to PG-style SQL compiler (single SQL statement, no N+1)
- SQLGlot transpilation (PG SQL to Trino SQL)
- Nested relationship compilation via LEFT JOIN / subquery
- Parameterized queries (never interpolates values)
- JSON serializer: reconstruct nested GraphQL response from JOIN rows
- FastAPI app factory, `/data/graphql` and `/health` endpoints

### Phase E: Routing & Direct Execution
- Smart routing: single RDBMS source direct, multi-source via Trino
- SQLGlot transpilation to target dialects (PostgreSQL, MySQL, SQL Server, DuckDB, Snowflake, BigQuery)
- Connection pool per registered RDBMS source (configurable min/max)
- Steward route override hint

### Phase F: Security Layers
- RLS WHERE clause injection per role per table
- Schema visibility enforcement (unauthorized tables/columns excluded from SDL)
- Rights model with 8 independent capabilities
- Column stripping for unauthorized columns

### Phase G: Mutations
- INSERT/UPDATE/DELETE via GraphQL (`insert_<table>`, `update_<table>`, `delete_<table>`)
- Mutations always route direct to RDBMS (never Trino)
- RLS injected into UPDATE/DELETE WHERE clauses
- Write permission enforcement per column per role
- NoSQL and cross-source mutations rejected

### Phase H: Persisted Query Registry & Pre-Approval
- Query submission with developer metadata and business purpose
- Stable identifier on approval
- Approval ceiling enforcement (restrict within, reject exceed)
- Governance modes: registry-required vs pre-approved tables
- Deprecation with replacement pointer
- Registration changes flag affected entries for re-review

### Phase I: Output Formats & Arrow Flight
- Content negotiation: JSON, NDJSON, CSV, Parquet, Arrow IPC
- Arrow Flight gRPC endpoint (port 8815) with streaming record batches
- Flight SQL connector modes: `catalog` (metadata discovery), `approved` (persisted queries), default (full execution)
- Zaychik Flight SQL proxy integration for end-to-end Arrow streaming

### Phase J: Large Result Redirect
- S3-compatible blob storage redirect with presigned URLs
- Threshold-based conditional redirect
- Trino CTAS for Parquet/ORC (data never passes through Provisa)
- Multi-root query support (some fields inline, some redirected)
- Pre-approved tables cannot use redirect

### Phase K: Admin API (Strawberry)
- Strawberry GraphQL endpoint at `/admin/graphql`
- CRUD for sources, tables, relationships, roles, RLS rules
- Schema regeneration triggered on config changes
- Config download/upload (`GET/PUT /admin/config`)

### Phase L: Connection Pooling Hardening
- PgBouncer for PostgreSQL sources
- Driver-level pooling for non-PG RDBMS
- Per-source pool sizing configuration

### Phase M: Production Infrastructure
- Helm chart for Kubernetes deployment
- Trino StatefulSet with configurable workers, HPA autoscaling

### Phase N: UI
- React app (Vite + TypeScript) with role-driven rendering
- Capability-driven views: Sources, Tables, Relationships, Security, Query Development, Approval
- GraphiQL query editor with Provisa plugin (submit, compile, redirect controls)
- GraphQL Voyager schema explorer (iframe with CDN bundle)
- Confirmation dialogs for destructive actions
- Approval queue with rejection reasons

### Phase O: Query Result Caching
- Redis-backed query result cache
- Security-partitioned cache keys (role + RLS context)
- Steward-controlled TTL per approved query
- Cache invalidation on registration changes and mutations
- `X-Provisa-Cache: HIT|MISS` response headers with age

### Phase P: Materialized View Optimization
- Transparent SQL-level MV rewrite (invisible in GraphQL SDL)
- Join pattern matching with partial match support
- Scheduled background refresh via Trino CTAS
- Stale MV bypass (no silent stale data)
- Optional SDL exposure for MVs with computed semantics
- RLS applied on MV-backed queries

### Phase Q: Column-Level Masking
- Per-column per-role data masking at SQL level
- Mask types: regex (REGEXP_REPLACE), constant (literal/NULL/MAX/MIN), truncate (DATE_TRUNC)
- Type validation (regex only on strings, NULL only on nullable)
- Masking injected into SELECT projection; WHERE uses raw values

### Phase R: LLM Relationship Discovery
- Claude API-powered FK candidate suggestion
- Scope control: per-table, per-domain, cross-domain
- Candidate lifecycle: suggested, accepted, rejected, expired
- Sample data validation for high-confidence candidates
- Admin endpoints for discovery trigger, review, accept/reject

### Phase S: gRPC Query Endpoint
- Auto-generated `.proto` schemas from registration model (per role)
- gRPC server on port 50051 with server reflection
- Streaming queries (one message per row), unary mutations
- Full security pipeline (RLS, masking, sampling, governance)
- Proto files served at `GET /data/proto/{role_id}`

### Phase T: Documentation
- README with quick start, feature overview, supported sources
- Architecture guide with request pipeline and routing decision tree
- Configuration reference (all YAML sections)
- API reference (all HTTP, Flight, gRPC endpoints)
- Security model deep dive
- Source type reference

### Phase U: API Sources (REST, GraphQL, gRPC)
- Register API endpoints as data sources with auto-discovery
- OpenAPI spec parsing, GraphQL introspection, gRPC reflection
- PG cache with TTL (endpoint > source > global)
- Primitive columns native, complex objects as JSONB
- JSONB field promotion via PG generated columns
- Stale-while-revalidate caching strategy

### Phase V: Kafka Sources & Sink
- Kafka topics as read-only tables via Trino Kafka connector
- Schema Registry integration (Avro, Protobuf, JSON Schema)
- Discriminator-based topic splitting (multiple types per physical topic)
- Default time window auto-injection (`default_window`)
- Query result publishing to Kafka topics (sink)

### Phase W: Authentication
- Pluggable auth providers: Firebase, Keycloak, OAuth 2.0/OIDC, simple (username/password)
- Superuser bootstrap access (works with any provider)
- JWT validation with JWKS rotation support
- Role mapping from identity claims to Provisa roles
- Login page with provider-specific UI

### Phase X: JDBC Arrow Flight Transport
- JDBC driver wired to Arrow Flight (`grpc://host:8815`) for streaming results
- Auto-detection with silent fallback to HTTP
- Batch-by-batch streaming (memory bounded to one record batch)
- Both `approved` and `catalog` modes supported

### Phase Y: Admin Page Global Features
- MV manager: list, enable/disable, manual refresh, error display
- Global cache controls: stats, purge all, purge by table
- System health panel: Trino, PG pool, Redis status
- Scheduled task overview with enable/disable toggles

### Phase Z: Per-Source & Per-Table Cache Configuration
- Hierarchical TTL resolution: table > source > global default
- `cache_enabled` toggle per source (disables all child tables)
- UI display of effective (resolved) TTL with inheritance chain
- Admin mutations: `updateSourceCache`, `updateTableCache`

### Phase AA: Quick Wins (AD1-AD2 only)
- **AD1 - Naming Convention**: `none`, `snake_case`, `camelCase`, `PascalCase` at global, source, and table level; explicit alias always wins
- **AD2 - OrderBy Alignment**: `{column: direction}` format with 6-value enum (`asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`); relationship ordering via JOIN
- Upsert mutations (`upsert_<table>`) via `INSERT ... ON CONFLICT DO UPDATE`
- `distinct_on` argument for root query fields
- Column presets: auto-inject values from headers, `now()`, or literals on insert/update
- Inherited roles with `parent_role_id` and capability flattening
- Scheduled triggers with cron expressions (APScheduler)
- Direct-route dialect expansion (ClickHouse, MariaDB, SingleStore, Redshift, Databricks, Hive, Druid, Exasol)
