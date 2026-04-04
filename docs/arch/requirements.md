# Requirements

## Pre-Approval & Query Governance
- **REQ-001** (2026-03-30): Production read queries against non-pre-approved tables MUST be members of the persisted query registry — unregistered queries rejected with no fallback.
- **REQ-002** (2026-03-30): Pre-approval is platform-level enforcement — no level of user privilege overrides it, including superuser.
- **REQ-003** (2026-03-30): Mutations and queries against pre-approved tables governed by user rights alone, no registry membership required.
- **REQ-004** (2026-03-30): Test endpoint accepts arbitrary queries against registered schema with full guards; MUST NOT be exposed in production.
- **REQ-005** (2026-03-30): Approved query defines a permitted ceiling — clients may restrict within it (fewer columns, additional filters) but cannot exceed it.
- **REQ-006** (2026-03-30): Pre-approved table queries do not support large result redirect or Arrow output — those require registry approval.

## Compiler & Schema
- **REQ-007** (2026-03-30): Compiler is purpose-built with no dependency on PostGraphile, DuckDB, or any third-party GraphQL server framework.
- **REQ-008** (2026-03-30): Schema generation pass runs at registration time; queries Trino INFORMATION_SCHEMA, applies per-role column visibility, incorporates relationships, produces GraphQL SDL.
- **REQ-009** (2026-03-30): Query compilation produces PG-style SQL from validated GraphQL AST — single SQL statement, no resolver chain, no N+1.
- **REQ-010** (2026-03-30): Trino type mapping: VARCHAR→String, INTEGER→Int, BOOLEAN→Boolean, TIMESTAMP→DateTime, JSONB→JSON. Nullability preserved.
- **REQ-011** (2026-03-30): References to unregistered tables, excluded columns, undefined relationships, or type mismatches rejected at compile time with precise errors.

## Registration & Governance
- **REQ-012** (2026-03-30): Source registration is privileged; validates connection, calls Trino dynamic catalog API, no restart required, available within seconds.
- **REQ-013** (2026-03-30): Source registration does not expose data — no table queryable until explicitly registered by a steward.
- **REQ-014** (2026-03-30): Unregistered tables do not exist — cannot be referenced in queries, do not appear in schema browser, cannot be mutation targets.
- **REQ-015** (2026-03-30): Each table has configurable governance mode: pre-approved (direct query with user rights) or registry-required (persisted query registry).
- **REQ-016** (2026-03-30): Table publication triggers schema generation pass; table immediately available in query builder.
- **REQ-017** (2026-03-30): NoSQL sources handled through automatic Parquet materialization; read-only, no mutations.
- **REQ-018** (2026-03-30): Trino FK metadata used to infer candidate intra-source relationships for steward confirmation/rejection.
- **REQ-019** (2026-03-30): Cross-source relationships defined manually by steward with cardinality (one-to-one, many-to-one, one-to-many).
- **REQ-020** (2026-03-30): Relationships owned by defining steward, versioned, flagged for re-review on schema changes affecting join fields.
- **REQ-021** (2026-03-30): GraphQL schema reflects registration model (business intent), not raw database structure.

## Persisted Query Registry
- **REQ-022** (2026-03-30): Submission captures: full query text, compiled SQL, target tables, parameter schema, permitted output types, developer identity.
- **REQ-023** (2026-03-30): Approved queries receive stable identifier; query text never transmitted in production requests.
- **REQ-024** (2026-03-30): Registry entries record: who defined, who approved, when, output types, routing hint, registration model version.
- **REQ-025** (2026-03-30): Registration changes flag affected registry entries for re-review — no silent continuation against changed schema.
- **REQ-026** (2026-03-30): Queries can be deprecated (clear error directing to replacement) but not deleted from history.

## Execution & Routing
- **REQ-027** (2026-03-30): Single-source queries route to direct RDBMS connection; SQLGlot transpiles to target dialect. Target: sub-100ms to low hundreds of ms.
- **REQ-028** (2026-03-30): Cross-source queries route to Trino; SQLGlot transpiles to Trino SQL. Target: 300-500ms.
- **REQ-029** (2026-03-30): Large results above threshold redirect to blob storage with presigned URL and TTL.
- **REQ-030** (2026-03-30): Steward override hint on registry entry for edge cases where default routing is inappropriate.
- **REQ-031** (2026-03-30): Mutations ALWAYS route to direct RDBMS connection — Trino never involved in mutation execution.

## Mutation Execution
- **REQ-032** (2026-03-30): Mutations are single-source by definition, bypass Trino, no routing decision, no registry approval.
- **REQ-033** (2026-03-30): User must have write rights to target table for any mutation.
- **REQ-034** (2026-03-30): Mutation input types reflect only columns user's role is permitted to write; excluded column references rejected at parse time.
- **REQ-035** (2026-03-30): RLS WHERE clauses injected into UPDATE and DELETE before execution.
- **REQ-036** (2026-03-30): Mutations can only target registered tables — compiler does not generate mutation types for unregistered tables.
- **REQ-037** (2026-03-30): Cross-source transactions not supported. NoSQL sources do not support mutations.

## Security
- **REQ-038** (2026-03-30): Three independent enforcement layers: pre-approval, schema visibility, SQL enforcement.
- **REQ-039** (2026-03-30): Schema visibility layer: unauthorized tables/columns do not appear in SDL or query builder; compiler rejects at parse time.
- **REQ-040** (2026-03-30): SQL enforcement layer: executor injects RLS WHERE clauses and strips unauthorized columns before execution, every request.
- **REQ-041** (2026-03-30): RLS rules defined at table registration as PG-style SQL filter expressions mapped to user roles.
- **REQ-042** (2026-03-30): Source registration, table registration, relationship definition, security configuration, query development, query authorization, and query execution rights are distinct and independently configured.

## API & Integration
- **REQ-043** (2026-03-30): GraphQL endpoint is primary entry point for queries and mutations.
- **REQ-044** (2026-03-30): Presigned URL redirect for large result consumers with TTL-bounded access.
- **REQ-045** (2026-03-30): gRPC Arrow Flight endpoint for high-throughput consumers; Trino produces Arrow natively for zero-copy delivery.
- **REQ-046** (2026-03-30): Output type governed by approved ceiling — unapproved output type rejected before execution.

## Output & Delivery
- **REQ-047** (2026-03-30): JSON output preserves native GraphQL nested structure.
- **REQ-048** (2026-03-30): NDJSON streaming variant: one JSON object per line.
- **REQ-049** (2026-03-30): Normalized tabular: flattened to relational tables with FK relationships preserved, Parquet or CSV.
- **REQ-050** (2026-03-30): Denormalized tabular: fully flattened single table, Parquet or CSV, single file or partitioned.
- **REQ-051** (2026-03-30): Arrow buffer via gRPC Arrow Flight endpoint; Trino produces Arrow natively.

## Data & Storage
- **REQ-052** (2026-03-30): Each registered RDBMS source maintains warm connection pool; min pool size configurable per source.
- **REQ-053** (2026-03-30): PostgreSQL sources use PgBouncer; other RDBMS use driver-level pooling.
- **REQ-054** (2026-03-30): Trino read path maintains single persistent connection to coordinator.

## Infrastructure
- **REQ-055** (2026-03-30): Docker Compose for development/small-team: single command, Provisa + Trino coordinator + configurable workers, all connectors pre-loaded.
- **REQ-056** (2026-03-30): Helm chart for production Kubernetes: horizontal Trino worker scaling, resource groups, HPA autoscaling.
- **REQ-057** (2026-03-30): Provisa container is stateless; deployment topology behind Trino endpoint is configuration concern.

## UI & Frontend
- **REQ-058** (2026-03-30): Branded, custom React-based UI — rendered surface determined entirely by user's assembled role set.
- **REQ-059** (2026-03-30): Role composition system: admin assembles capabilities from independently assignable building blocks.
- **REQ-060** (2026-03-30): Capabilities: Source Registration, Table Registration, Relationship Registration, Security Configuration, Query Development, Query Approval (global or table-scoped), Admin.
- **REQ-061** (2026-03-30): Every destructive or consequential action requires explicit confirmation with consequence summary.
- **REQ-062** (2026-03-30): Test endpoint execution shows RLS filters applied, columns excluded, schema scope enforced in result metadata.
- **REQ-063** (2026-03-30): Approval queue designed for steward efficiency; rejection reasons must be specific and actionable.

## Error Handling & Reliability
- **REQ-064** (2026-03-30): Never add fallback values or silent error handling — all errors must be explicit and fail-fast.
- **REQ-065** (2026-03-30): No migrations in version 1 development.

## SQLGlot Transpilation
- **REQ-066** (2026-03-30): Compiler emits PG-style SQL as canonical output; SQLGlot translates to Trino SQL or target RDBMS dialect.
- **REQ-067** (2026-03-30): Target dialect determined by source type captured at table registration time.
- **REQ-068** (2026-03-30): Supported dialects: PostgreSQL, MySQL, SQL Server, Trino, DuckDB, Snowflake, BigQuery.

## Architecture & Design Patterns
- **REQ-069** (2026-03-30): Architecture docs in `docs/arch/` ARE the planning documents — update when requirements change, don't implement without planning.
- **REQ-070** (2026-03-30): Maximum brevity in communications — code and facts only, no pleasantries or explanations unless asked.
- **REQ-071** (2026-03-30): New requirements tracked via requirements-tracker agent appending to `docs/arch/requirements.md`.

## Commercial Positioning
- **REQ-072** (2026-03-30): Core product is open source: Docker Compose, Helm chart, UI, compiler, SQLGlot layer, registry, Trino backend.
- **REQ-073** (2026-03-30): SaaS tier: hosted control plane with customer-hosted data plane option.
- **REQ-074** (2026-03-30): Enterprise tier: SLA guarantees, dedicated support, advanced audit logging, compliance reporting.

## JSONB & API Sources
- **REQ-119** (2026-03-31): Stewards can promote specific nested fields from JSONB columns into native PostgreSQL generated columns (GENERATED ALWAYS AS ... STORED). Promoted columns are filterable, indexable, relationship-eligible, and auto-maintained by PostgreSQL. Supports dot-path extraction for nested fields. Part of Phase U (API Sources).

## Authentication
- **REQ-120** (2026-03-31): Pluggable auth provider interface — abstract AuthProvider producing AuthIdentity (user_id, email, roles, claims) mapped to Provisa roles. One provider at a time configured in YAML.
- **REQ-121** (2026-03-31): Firebase Authentication — validates Firebase ID tokens via firebase-admin SDK. Supports all Firebase auth methods (email/password, Google, Apple, GitHub, phone, anonymous, SAML, OIDC).
- **REQ-122** (2026-03-31): Keycloak OIDC — validates JWT access tokens from Keycloak via OIDC discovery + JWKS. Realm roles + client roles → Provisa role mapping.
- **REQ-123** (2026-03-31): Generic OAuth 2.0 / OIDC — works with any OIDC-compliant provider (PingFederate, Okta, Azure AD, Auth0). OIDC discovery URL → JWKS → JWT validation. Configurable role claim mapping.
- **REQ-124** (2026-03-31): Simple username/password auth for testing — users defined in config YAML with bcrypt hashed passwords. Issues short-lived JWT. NOT for production (requires allow_simple_auth: true flag).
- **REQ-125** (2026-03-31): Superuser bootstrap access — superuser credentials in config (username + password from env secret). Always admin role + all capabilities regardless of auth provider. For initial setup.

## JDBC/ODBC Integration
- **REQ-126** (2026-04-01): JDBC driver that exposes approved persisted queries as virtual tables. Connection authenticates against Provisa, maps user to role.
- **REQ-127** (2026-04-01): `getTables()` returns approved queries visible to the authenticated role. Each approved query appears as a table with the query's stable ID as the table name.
- **REQ-128** (2026-04-01): `getColumns(tableName)` introspects the approved query's output schema — column names and types from the compiled query metadata.
- **REQ-129** (2026-04-01): `executeQuery(sql)` parses the SQL to extract the query ID and optional WHERE filters, executes the approved query via Provisa's HTTP API with Parquet format, deserializes the result into a JDBC ResultSet.
- **REQ-130** (2026-04-01): Full security pipeline (RLS, masking, sampling) applied at query time — not baked into views.
- **REQ-131** (2026-04-01): Connection string format: `jdbc:provisa://host:port`. Authentication via standard JDBC username/password properties.
- **REQ-132** (2026-04-01): The driver is a single JAR with no external dependencies beyond the JDK and Apache Arrow (for Parquet deserialization).
- **REQ-229** (2026-04-03): JDBC driver transport via Arrow Flight — connect to Provisa's existing Flight server (`grpc://host:8815`) for streaming query results. Arrow record batches stream from the first row with backpressure, zero serialization overhead, and no full-result buffering. Flight is used automatically when the server is reachable; falls back to HTTP silently if not. No user configuration required. Both `mode=catalog` and `mode=approved` work over Flight. The Flight ticket carries the GraphQL query + role + variables as JSON.

## Views (Governed Computed Datasets)
- **REQ-133** (2026-04-01): Views are SQL-defined computed datasets registered in the Provisa config with full column-level governance (visibility, masking, descriptions, aliases).
- **REQ-134** (2026-04-01): Views go through the same governance pipeline as tables — RLS, masking, sampling, role-based schema visibility, approval workflow.
- **REQ-135** (2026-04-01): Views with `materialize: true` are backed by a periodically refreshed MV (CTAS). Views without materialization run as live subqueries via Trino.
- **REQ-136** (2026-04-01): Views are the governed mechanism for adding computed semantics (aggregations, transformations) to the platform. This preserves the GraphQL constraint that no new semantics can be added outside the platform.

## Large Result Redirect & CTAS
- **REQ-137** (2026-04-01): Client-controlled redirect via `X-Provisa-Redirect-Format` and `X-Provisa-Redirect-Threshold` headers. Format without threshold implies force redirect.
- **REQ-138** (2026-04-01): Trino-native formats (Parquet, ORC) use CTAS — Trino writes directly to S3 via Iceberg, data never passes through Provisa.
- **REQ-139** (2026-04-01): Non-native formats (JSON, NDJSON, CSV, Arrow IPC) serialized by Provisa and uploaded to S3 via boto3.
- **REQ-140** (2026-04-01): Threshold-based redirect uses LIMIT threshold+1 probe — no COUNT(*), no double execution for inline results.
- **REQ-141** (2026-04-01): S3 data cleanup scheduled after presigned URL TTL expires.
- **REQ-142** (2026-04-01): Default redirect format configurable via `PROVISA_REDIRECT_FORMAT` (default: parquet).

## Arrow Flight
- **REQ-143** (2026-04-01): Arrow Flight server (port 8815) streams record batches via gRPC. Full security pipeline applied.
- **REQ-144** (2026-04-01): Zaychik Arrow Flight SQL proxy translates between Flight SQL clients and Trino JDBC.
- **REQ-145** (2026-04-01): Flight server streams batch-by-batch via GeneratorStream — full result never materialized in Provisa memory. Unbounded result support.
- **REQ-146** (2026-04-01): Falls back to materializing via Trino REST if Zaychik unavailable.

## Kafka Sources
- **REQ-147** (2026-04-01): Kafka topics queryable via Trino Kafka connector. Routed through Trino (TRINO_ONLY source).
- **REQ-148** (2026-04-01): Default time window (`default_window`) auto-injected as WHERE clause on `_timestamp`. Prevents unbounded reads.
- **REQ-149** (2026-04-01): Discriminator filter for multi-type topics — multiple table configs on the same physical topic, each filtered by a discriminator field/value.
- **REQ-150** (2026-04-01): Manual schema definition for topics without Schema Registry.

## Column Path Extraction
- **REQ-151** (2026-04-01): Columns with `path` extract values from JSON source columns using PG `>>` syntax. SQLGlot transpiles to `json_extract_scalar` for Trino.
- **REQ-152** (2026-04-01): Path columns on PostgreSQL sources route direct. Non-PG sources force Trino routing.
- **REQ-153** (2026-04-01): Path columns are read-only computed fields — mutations unaffected.

## Naming & Schema
- **REQ-154** (2026-04-01): Optional `domain_prefix` prepends `domain_id__` (double underscore) to all GraphQL names.
- **REQ-155** (2026-04-01): Table and column `alias` fields override GraphQL names.
- **REQ-156** (2026-04-01): Table and column `description` fields included in GraphQL SDL.
- **REQ-157** (2026-04-01): Order-by enum values preserve original column case (not uppercased).

## Auto-Materialized Relationships
- **REQ-158** (2026-04-01): Cross-source relationships with `materialize: true` auto-generate MV definitions at startup.
- **REQ-159** (2026-04-01): Only cross-source relationships generate MVs. Same-source relationships are already fast via direct routing.
- **REQ-160** (2026-04-01): Auto-MVs start STALE and are populated by the background refresh loop.

## Query Development Tools
- **REQ-161** (2026-04-01): `POST /data/compile` returns compiled SQL with RLS/masking applied, route decision, and params without executing.
- **REQ-162** (2026-04-01): `POST /data/submit` submits a named GraphQL query for steward approval. Requires named operation.
- **REQ-163** (2026-04-01): GraphiQL Provisa plugin with View SQL and Submit for Approval.

## Admin & Configuration
- **REQ-164** (2026-04-01): `GET/PUT /admin/config` for config YAML download/upload with backup and reload.
- **REQ-165** (2026-04-01): `GET/PUT /admin/settings` for runtime platform settings (redirect, sampling, cache).
- **REQ-166** (2026-04-01): Editable relationships page with materialize toggle, delete, and add form.
- **REQ-167** (2026-04-01): AI-suggested relationships via LLM discovery integration on relationships page.
- **REQ-168** (2026-04-01): `approveQuery` mutation and `persistedQueries` query in admin GraphQL API.

## Infrastructure
- **REQ-169** (2026-04-01): Trino 480 with Iceberg results catalog (JDBC on PG, native S3 filesystem).
- **REQ-170** (2026-04-01): `start-ui.sh --reset-volumes` for Docker crash recovery.
- **REQ-171** (2026-04-01): MinIO results bucket auto-created at startup.

## Dataset Change Events
- **REQ-172** (2026-04-01): Mutations emit a dataset change event to a Kafka topic — no row-level detail, just `{table, source, timestamp}`.
- **REQ-173** (2026-04-01): Change events fire on the same mutation hook that invalidates cache and marks MVs stale.
- **REQ-174** (2026-04-01): Producers running complex ETL outside Provisa can signal changes via a trivial mutation (touch operation).
- **REQ-175** (2026-04-01): Change event topic configurable via `PROVISA_CHANGE_EVENT_TOPIC` (default: `provisa.change-events`).

## Kafka Sinks (Approved Query Publishing)
- **REQ-176** (2026-04-01): Approved queries can optionally have a Kafka sink — results published to a topic on trigger.
- **REQ-177** (2026-04-01): Sink triggers: `change_event` (re-run when source table changes), `schedule` (cron/interval), `manual` (on-demand).
- **REQ-178** (2026-04-01): Sinks are opt-in per approved query, configured by the steward.
- **REQ-179** (2026-04-01): Sink request can be included in the query submission — steward approves query and sink together.
- **REQ-180** (2026-04-01): Sinks can also be added to an already-approved query independently.
- **REQ-181** (2026-04-01): Sink output format is JSON (one message per row, keyed by optional column).

## Hasura Migration Converters
- **REQ-182** (2026-04-03): Hasura v2 metadata converter -- CLI tool that reads a Hasura v2 metadata export directory and emits valid Provisa YAML config. Converts tracked tables, relationships, permissions, roles, and auth. Part of Phase AG (Hasura v2 Converter).
- **REQ-183** (2026-04-03): Hasura DDN (v3) HML converter -- CLI tool that reads a DDN supergraph project and emits valid Provisa YAML config. Converts ObjectTypes, Models, Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks. Part of Phase AH (DDN Converter).
- **REQ-184** (2026-04-03): Shared boolean expression-to-SQL converter for Hasura filter expressions. Supports `_eq`, `_neq`, `_gt`, `_gte`, `_lt`, `_lte`, `_in`, `_nin`, `_like`, `_ilike`, `_regex`, `_is_null`, `_and`, `_or`, `_not`. Session variable mapping: `X-Hasura-<Name>` -> `current_setting('provisa.<name>')`. Part of Phase AG (Hasura v2 Converter).
- **REQ-185** (2026-04-03): v2 converter maps `select_permissions[].columns` per role -> Provisa column `visible_to`. `columns: "*"` means all columns visible to that role. Part of Phase AG (Hasura v2 Converter).
- **REQ-186** (2026-04-03): v2 converter maps `insert/update_permissions[].columns` per role -> Provisa column `writable_by`. Part of Phase AG (Hasura v2 Converter).
- **REQ-187** (2026-04-03): v2 converter maps `select_permissions[].filter` -> Provisa `rls_rules[]` via boolean expression-to-SQL conversion. `filter: {}` (empty) means no RLS filter. Part of Phase AG (Hasura v2 Converter).
- **REQ-188** (2026-04-03): v2 converter maps `object_relationships` -> cardinality=many-to-one and `array_relationships` -> cardinality=one-to-many. Physical column names used directly (no GraphQL resolution needed). Part of Phase AG (Hasura v2 Converter).
- **REQ-189** (2026-04-03): DDN converter resolves GraphQL field names to physical column names through `ObjectType.dataConnectorTypeMapping[].fieldMapping` for all field references in relationships, permissions, and column definitions. Part of Phase AH (DDN Converter).
- **REQ-190** (2026-04-03): v2 auth conversion via optional `--auth-env-file` flag. JWT with `jwk_url` -> Provisa `provider: oauth`. JWT `claims_map` -> Provisa `role_mapping[]`. Admin secret -> Provisa `superuser`. Webhook auth emits warning (no Provisa equivalent). Part of Phase AG (Hasura v2 Converter).
- **REQ-191** (2026-04-03): DDN AggregateExpression metadata preserved in sidecar `provisa-aggregates.yaml` and converted to Provisa aggregate config. Part of Phase AH (DDN Converter).
- **REQ-192** (2026-04-03): Converters emit warnings for unmappable features (event_triggers, remote_schemas, cron_triggers, BooleanExpressionType) without failing conversion. v2 Actions and DDN Commands convert to Provisa `functions` config where backed by stored procedures; webhook-backed actions emit warning with handler URL. Part of Phases AG/AH (Hasura Converters).
- **REQ-193** (2026-04-03): Both converters produce output that passes `ProvisaConfig.model_validate()` -- Pydantic-valid config or nothing. Part of Phases AG/AH (Hasura Converters).

## Naming Convention
- **REQ-194** (2026-04-03): `naming.convention` field supporting `snake_case` (default), `camelCase`, and `PascalCase`. Auto-generates column aliases from physical column names (e.g., `user_id` -> `userId` when `camelCase`). Explicit `column.alias` always takes precedence. Part of Phase AD (Schema Alignment).
- **REQ-195** (2026-04-03): Aligns with Hasura v2 `naming_convention` setting: `hasura-default` maps to `snake_case`, `graphql-default` maps to `camelCase`. DDN `namingConvention: graphql` maps to `camelCase`. Part of Phase AD (Schema Alignment).

## Aggregates
- **REQ-196** (2026-04-03): Auto-generated aggregate queries following Hasura v2 pattern. Every table gets a `<table>_aggregate` root field. Numeric columns get sum/avg/stddev/variance. All comparable columns get min/max. All columns get count. No configuration required for default behavior. Part of Phase AD (Schema Alignment).
- **REQ-197** (2026-04-03): Per-role aggregate gating via `allow_aggregations` (matching v2) or per-table `aggregates` config section for explicit override of auto-detected functions and role visibility. Part of Phase AD (Schema Alignment).
- **REQ-198** (2026-04-03): Aggregate MV routing -- when a query requests aggregates over a pattern already materialized in an MV, the compiler rewrites the query to use the MV. Requires aggregate catalog + query rewriter. Part of Phase AD (Schema Alignment).
- **REQ-199** (2026-04-03): View auto-materialization for aggregate optimization -- expensive views auto-materialized and registered in aggregate catalog instead of requiring bespoke `materialized_views` entries. Default TTL configurable globally via `materialized_views.default_ttl` (seconds, default: 3600). Individual views can override with `refresh_interval`. Stale MVs refreshed by background loop; queries against stale MVs fall back to live execution. Part of Phase AD (Schema Alignment).

## Materialized View Lifecycle
- **REQ-234** (2026-04-03): Auto-materialized view storage reclamation -- when a view is removed from config, disabled, or its source table is unregistered, the backing MV table is dropped (Trino `DROP TABLE`). Background cleanup task runs on config reload and periodically (default: daily). Orphaned MV tables (present in target schema but not in MV registry) are flagged and optionally auto-dropped after a grace period. Part of Phase AD (Schema Alignment).
- **REQ-235** (2026-04-03): Auto-materialized aggregate views must have a size guard -- `materialized_views.max_rows` (default: 1,000,000). Views whose source query would produce more rows than the limit skip materialization and fall back to live execution. Size estimated via `SELECT COUNT(*)` probe before CTAS. Configurable per view to override the global default. Part of Phase AD (Schema Alignment).

## Hot Tables (Redis-Cached Lookups)
- **REQ-230** (2026-04-03): Hot tables -- small lookup tables cached entirely in Redis for sub-millisecond reads. Config per table: `hot: true` with optional `max_rows` guard (default: 10,000) and `max_bytes` guard (default: 10MB). Tables exceeding either limit at load time emit warning and skip caching. Uses existing `cache.redis_url` config. Byte size measured after serialization to Redis format. Part of Phase AD (Schema Alignment).
- **REQ-231** (2026-04-03): Hot table refresh follows MV pattern: TTL-based via `refresh_interval` (default: `materialized_views.default_ttl`), background refresh loop, stale fallback to live query. Mutations to the source table trigger immediate invalidation + async reload. Part of Phase AD (Schema Alignment).
- **REQ-232** (2026-04-03): Hot table JOIN optimization -- when a query joins a hot table (e.g., `orders JOIN countries ON country_code`), the compiler injects the hot table data as constants into the SQL via a `VALUES`-based CTE. The DB engine sees literal rows, not a table reference. This eliminates the second table scan and works across sources (constants travel with the query to Trino/Snowflake/ClickHouse). SQLGlot transpiles the VALUES clause per dialect. Part of Phase AD (Schema Alignment).
- **REQ-233** (2026-04-03): Redis storage format: hash per row keyed by PK, plus a sorted set index for range queries. Full table also stored as a single serialized blob for bulk reads. Column governance (visibility, masking) still applied at query time -- Redis caches raw data, security enforced on read. Part of Phase AD (Schema Alignment).

## Warm Tables (Local SSD via Trino File Cache)
- **REQ-238** (2026-04-03): Warm tables -- frequently queried RDBMS tables materialized into the Iceberg results catalog so Trino's built-in file system cache (`fs.cache.enabled=true`) caches the Parquet files on local SSD. Provides ~10-50ms reads vs 100ms+ network round-trip to remote source. Same TTL/refresh pattern as MVs. Part of Phase AD (Schema Alignment).
- **REQ-239** (2026-04-03): Warm table auto-promotion -- track query frequency per table (increment counter on each compiled query). Tables exceeding `warm_tables.query_threshold` (default: 100 queries per refresh interval) are auto-materialized into Iceberg. Tables falling below the threshold are demoted (backing table dropped) on next refresh cycle. Part of Phase AD (Schema Alignment).
- **REQ-240** (2026-04-03): Warm table config: `warm: true` to force, `warm: false` to opt out. Global `warm_tables.query_threshold`, `warm_tables.max_rows` (default: 10,000,000), `warm_tables.refresh_interval` (default: same as `materialized_views.default_ttl`). Trino file cache config (`fs.cache.enabled`, `fs.cache.directories`, `fs.cache.max-sizes`) managed in Trino catalog properties. Part of Phase AD (Schema Alignment).
- **REQ-241** (2026-04-03): Three-tier caching hierarchy: Hot (Redis, <1ms, tiny lookups) -> Warm (local SSD via Trino Iceberg cache, ~10-50ms, medium frequent tables) -> Cold (remote source, 100ms+, everything else). Tables can be in at most one tier. Hot takes precedence over warm. Tier assignment is automatic based on table size and query frequency, with manual override via config. Part of Phase AD (Schema Alignment).

## Hot Table Auto-Detection
- **REQ-236** (2026-04-03): Auto-detect hot table candidates at schema build time. Criteria: (1) row count below `hot_tables.auto_threshold` (default: 10,000), measured via `SELECT COUNT(*)` during introspection, AND (2) table is the target side of at least one many-to-one relationship (i.e., it's a lookup table). Tables meeting both criteria are automatically cached in Redis without explicit `hot: true` config. Part of Phase AD (Schema Alignment).
- **REQ-237** (2026-04-03): Auto-hot tables can be opted out via `hot: false` on the table config. Explicit `hot: true` overrides the auto-detection criteria (forces caching regardless of row count or relationship status). Auto-detection runs on every schema rebuild; tables that grow beyond the threshold are automatically evicted from Redis on next rebuild. Part of Phase AD (Schema Alignment).

## OrderBy Alignment
- **REQ-200** (2026-04-03): GraphQL order_by schema must follow Hasura v2 convention: column-keyed input type `{column_name: direction}` instead of current `{field: ENUM, direction: ENUM}` struct. Part of Phase AD (Schema Alignment).
- **REQ-201** (2026-04-03): OrderBy direction enum must include 6 values: `asc`, `asc_nulls_first`, `asc_nulls_last`, `desc`, `desc_nulls_first`, `desc_nulls_last`. SQL compiler maps to `ORDER BY col ASC NULLS FIRST`, etc. Part of Phase AD (Schema Alignment).
- **REQ-202** (2026-04-03): Relationship ordering -- order by related object fields (e.g., `order_by: {author: {name: asc}}`). Matches Hasura v2 default behavior. Part of Phase AD (Schema Alignment).

## Tracked Functions & Custom Mutations
- **REQ-205** (2026-04-03): Database functions (stored procedures, UDFs) registered in Provisa config and exposed as GraphQL mutations or queries. VOLATILE functions exposed as mutations; STABLE/IMMUTABLE as queries. Follows Hasura v2's `pg_track_function` pattern. Part of Phase AC (Tracked Functions & Webhooks).
- **REQ-206** (2026-04-03): Function config section in `provisa.yaml` (Part of Phase AC):
  ```yaml
  functions:
    - source_id: sales-pg
      schema: public
      name: process_payment
      exposed_as: mutation
      domain_id: sales-analytics
      governance: registry-required
      arguments:
        - name: user_id
          type: integer
        - name: amount
          type: numeric
      returns: orders              # registered table name for result mapping
      visible_to: [admin]
      writable_by: [admin]
  ```
- **REQ-207** (2026-04-03): Function return type MUST reference a registered table. The result set maps back to GraphQL using that table's type definition, column governance (visibility, masking), and RLS rules. This ensures function results go through the same security pipeline as direct queries. Part of Phase AC (Tracked Functions & Webhooks).
- **REQ-208** (2026-04-03): Functions execute via direct DB connection (same as mutations). Never routed through Trino. Part of Phase AC (Tracked Functions & Webhooks).
- **REQ-209** (2026-04-03): Webhook-backed mutations -- external HTTP endpoint called as a GraphQL mutation (Part of Phase AC). Config:
  ```yaml
  webhooks:
    - name: send_notification
      url: https://api.internal/notify
      method: POST
      domain_id: support
      governance: registry-required
      arguments:
        - name: user_id
          type: String!
        - name: message
          type: String!
      returns: notification_result  # registered table or inline type
      visible_to: [admin]
      timeout_ms: 5000
  ```
- **REQ-210** (2026-04-03): Webhook mutations support inline return type definitions (not backed by a registered table) for cases where the webhook returns a custom shape. Inline types define fields with names and GraphQL types. Part of Phase AC (Tracked Functions & Webhooks).
- **REQ-211** (2026-04-03): Function and webhook argument types map to GraphQL input types. Arguments are validated at parse time. SQL injection prevented via parameterized calls for DB functions and JSON serialization for webhooks. Part of Phase AC (Tracked Functions & Webhooks).

## Actions UI
- **REQ-242** (2026-04-03): Admin UI "Actions" page listing all registered functions and webhooks. Grouped by type (DB Function / Webhook). Shows source, domain, exposed_as (mutation/query), governance level, return table, argument count. Part of Phase AC (Tracked Functions & Webhooks).
- **REQ-243** (2026-04-03): Add action form with type selector: DB Function (source, schema, function name, exposed_as, returns registered table, arguments, visible_to/writable_by) or Webhook (name, URL, method, timeout_ms, returns registered table or inline type, arguments, visible_to). Part of Phase AC (Tracked Functions & Webhooks).
- **REQ-244** (2026-04-03): Inline type builder for webhook return types — dynamic rows of field name + GraphQL type. Used when webhook returns a custom shape not backed by a registered table. Part of Phase AC (Tracked Functions & Webhooks).
- **REQ-245** (2026-04-03): Test action button — execute function/webhook with sample arguments, display result + governance pipeline applied (which columns masked, RLS filters, role). Same pattern as query test endpoint. Part of Phase AC (Tracked Functions & Webhooks).

## ABAC Approval Hook
- **REQ-203** (2026-04-03): Pluggable operation approval hook for enterprises with complex ABAC that can't be expressed as static RLS rules. Evaluated at query time. Request: user_id, roles, session_vars, tables, columns, operation. Response: approved/denied + optional additional filter. Position: after RLS injection, before execution. Part of Phase AE (ABAC Approval Hook).
- **REQ-204** (2026-04-03): Approval hook scoping — per-table (`approval_hook: true`), per-source (`approval_hook: true` on source config), or global (`auth.approval_hook`). Compiler checks at query time: if no table in the query has approval_hook enabled (directly, via source, or global), skip the call entirely. Zero overhead for unscoped tables. Part of Phase AE (ABAC Approval Hook).
- **REQ-246** (2026-04-03): Approval hook protocols — three transport options, configured via `auth.approval_hook.type` (Part of Phase AE):
  - `webhook` (default): HTTP POST to URL. ~5-50ms. Simplest, works with any external ABAC service.
  - `grpc`: gRPC with persistent connection + multiplexing. ~1-5ms. Binary protocol for high-volume same-datacenter deployments. Proto service definition shipped with Provisa.
  - `unix_socket`: Unix domain socket for same-machine sidecars (e.g., OPA). <0.5ms. Path configured via `auth.approval_hook.socket_path`.
- **REQ-247** (2026-04-03): Approval hook config (Part of Phase AE):
  ```yaml
  auth:
    approval_hook:
      type: webhook          # webhook, grpc, unix_socket
      url: https://authz.internal/approve   # for webhook/grpc
      socket_path: /var/run/provisa-authz.sock  # for unix_socket
      timeout_ms: 500
      fallback: deny         # deny or allow on timeout
      scope: all             # all, or omit for per-table/per-source scoping
  ```
  Per-table: `tables[].approval_hook: true`. Per-source: `sources[].approval_hook: true`. Global: `auth.approval_hook.scope: all`.

## Direct-Route Dialect Expansion
- **REQ-229** (2026-04-03): For every source type, three things must be true for direct-route capability: (1) Trino connector is packaged in the Trino deployment, (2) SQLGlot has the dialect for transpilation, (3) `SOURCE_TO_DIALECT` and `SOURCE_TO_CONNECTOR` entries exist in Provisa config. New direct-route sources to add: clickhouse, mariadb, singlestore, redshift, databricks, hive, druid, exasol. Each must have all three verified. Part of Phase AA (Quick Wins).

## Hasura v2 Parity: Low-Complexity Features
- **REQ-212** (2026-04-03): Upsert mutations -- `INSERT ... ON CONFLICT ... DO UPDATE`. New `upsert_<table>` mutation field. Conflict columns inferred from primary key metadata. SQLGlot transpiles to dialect-specific syntax (MySQL `ON DUPLICATE KEY UPDATE`, etc.). Part of Phase AA (Quick Wins).
- **REQ-213** (2026-04-03): `DISTINCT ON` query argument -- deduplicate results by specified columns. Added as `distinct_on` arg on root query fields. SQLGlot handles dialect differences (window function fallback for non-PG). Part of Phase AA (Quick Wins).
- **REQ-214** (2026-04-03): Column presets -- auto-set column values on insert/update from session variables or built-in functions. Config per table: `column_presets: [{column: created_by, source: header, name: x_user_id}, {column: updated_at, source: now}]`. Preset columns removed from user input, injected before SQL generation. Part of Phase AA (Quick Wins).
- **REQ-215** (2026-04-03): Inherited roles -- role hierarchy where child role inherits capabilities and domain_access from parent. Config: `parent_role_id` on role definition. Flattened at startup (merge capabilities/domain_access up the chain). Lookups remain O(1). Part of Phase AA (Quick Wins).
- **REQ-216** (2026-04-03): Scheduled triggers -- time-based execution of registered webhooks or internal functions. APScheduler in-process, cron expression syntax. Config per trigger in `provisa.yaml`. Reuses existing async background task pattern. Part of Phase AA (Quick Wins).
- **REQ-217** (2026-04-03): Batch mutations already supported by GraphQL spec -- multiple mutations in one request execute sequentially. Document existing behavior. Part of Phase AA (Quick Wins).

## Hasura v2 Parity: Medium-Complexity Features
- **REQ-218** (2026-04-03): Cursor-based pagination -- `first`, `after`, `last`, `before` args on root query fields. Returns `edges[{cursor, node}]` + `pageInfo{hasNextPage, hasPreviousPage, startCursor, endCursor}`. Cursor encoded as base64 of sort key values. Coexists with existing offset/limit. Part of Phase AB (Medium-Complexity Parity).
- **REQ-219** (2026-04-03): Subscriptions via Server-Sent Events (SSE) -- `GET /data/subscribe/<table>` endpoint using FastAPI StreamingResponse. PostgreSQL LISTEN/NOTIFY via asyncpg `.add_listener()` for change detection. No WebSocket complexity. Streams INSERT/UPDATE/DELETE events. Part of Phase AB (Medium-Complexity Parity).
- **REQ-220** (2026-04-03): Database event triggers -- table changes (insert/update/delete) fire webhooks. PostgreSQL trigger + `pg_notify()` -> asyncpg listener -> HTTP POST to configured URL. Config per table with operation filter and retry policy. Part of Phase AB (Medium-Complexity Parity).
- **REQ-221** (2026-04-03): Enum table auto-detection -- introspect `pg_enum` at schema build time, generate GraphQL enum types for columns using PostgreSQL user-defined enums. Map enum columns to GraphQL enum type instead of String. Part of Phase AB (Medium-Complexity Parity).
- **REQ-222** (2026-04-03): REST endpoint auto-generation -- for each root query field, generate `GET /data/rest/<table>` FastAPI endpoint. Map query args to URL query params (`?limit=10&where.id.eq=1`). Reuses GraphQL compilation pipeline internally. Part of Phase AB (Medium-Complexity Parity).

## Installer & Packaging
- **REQ-223** (2026-04-03): Single-executable installer that bundles the Provisa platform into one download. Underlying technology (Python server, PostgreSQL admin DB, Trino query engine, React UI) is hidden from the user. Source datasets are NOT bundled -- they connect over the wire. Part of Phase AF (Installer & Packaging).
- **REQ-224** (2026-04-03): Installer expands into a hidden directory (`~/.provisa/`) containing all services. User interacts via `provisa start`, `provisa stop`, `provisa status`, `provisa open` (opens browser). Part of Phase AF (Installer & Packaging).
- **REQ-225** (2026-04-03): Default deployment uses embedded PostgreSQL (pgserver) for admin DB and bundled Trino for query federation. Vertical scaling by default -- single machine, increase resources as needed. Part of Phase AF (Installer & Packaging).
- **REQ-226** (2026-04-03): Users can later connect their own Trino cluster, Spark, external auth provider, or external PostgreSQL for the admin DB via config. Pointing to an external Trino instance is the primary scale-out mechanism. Part of Phase AF (Installer & Packaging).
- **REQ-227** (2026-04-03): Cross-platform support: macOS (.pkg with LaunchAgent), Linux (.deb with systemd), Windows (.msi). Each uses native service management to start/stop Provisa services transparently. Part of Phase AF (Installer & Packaging).
- **REQ-228** (2026-04-03): Phase 1 (immediate): Shell script installer that bundles Docker Compose, hides behind `provisa` CLI wrapper, stores state in `~/.provisa/`. Requires Docker/OrbStack/Colima as prerequisite. Phase 2: Embedded pgserver + Nuitka-compiled Python binary, reduce Docker dependency to Trino only. Phase 3: Native OS packages with full service management. Part of Phase AF (Installer & Packaging).

## UI & Design Patterns
- **REQ-248** (2026-04-04): GraphQL Voyager integration uses iframe with React 18 CDN standalone bundle (not a native React component fork). The iframe approach avoids MUI v5/React 19 incompatibility and provides reliable isolation. No component fork is planned.
- **REQ-249** (2026-04-04): Column-level masking configuration stored as inline fields on ColumnConfig in models.py (mask_type, mask_pattern, etc.) — not as a separate MaskingRule Pydantic model or separate database table. Masking rules are loaded from table_columns DB rows at startup in app.py. This co-locates masking config with the column definition it applies to.

## Registration & Governance
- **REQ-250** (2026-04-04): All Trino catalog configuration must flow through Provisa's config YAML — users never manage Trino .properties files directly. Provisa generates catalog properties, table definition files, and auto-registers tables at runtime following the Kafka source config pattern as exemplar. Part of Phase AI (NoSQL Source Mapping).
- **REQ-251** (2026-04-04): Each NoSQL/non-relational source type requires a type-specific mapping DSL in the Provisa config to define how its native data structures map to relational tables. Examples: Redis key patterns → rows, hash fields → columns; Elasticsearch index patterns with nested field paths; Prometheus metric names with label-to-column mapping. Part of Phase AI (NoSQL Source Mapping).

## Compiler & Schema
- **REQ-252** (2026-04-04): Schema inference must be supported where the Trino connector provides auto-discovery (MongoDB, Cassandra, Elasticsearch). Sources with inference support should offer a `discover: true` flag that introspects and generates a starting column list. Explicit column definitions always take precedence over inferred schemas. Sources without inference capability (Redis, Accumulo) require explicit column definitions. Part of Phase AI (NoSQL Source Mapping).
- **REQ-253** (2026-04-04): Naming convention changes (global, source, or table level) reflected immediately in GraphQL schema. Admin mutations trigger _rebuild_schemas() which regenerates in-memory graphql-core schema objects. GraphiQL and Voyager fetch fresh introspection on each page load. No client-side schema caching. Part of Phase AI (NoSQL Source Mapping).

## Testing & Quality
- **REQ-254** (2026-04-04): Integration tests must use Docker — spin up required containers (PG, Trino, Redis, etc.), install dependencies, run tests, and tear down containers. Tests must not assume a pre-existing Docker Compose stack.
- **REQ-255** (2026-04-04): Unit tests must mock all external components (databases, Trino, Redis, HTTP endpoints, file system where applicable). No unit test should require a running external service.

## Compiler & Schema
- **REQ-259** (2026-04-04): Apollo Federation v2 subgraph support — when enabled, Provisa generates a Federation v2 compliant schema with @key directives on entity types (derived from primary keys), _service and _entities root fields, and batch entity resolution. Provisa can be composed into an Apollo Gateway/Router supergraph. Entity resolution respects RLS, masking, and role-based visibility. Disabled by default. Part of Phase AJ (Apollo Federation).

## Subscriptions
- **REQ-260** (2026-04-04): Polling-based subscription provider for sources without native CDC. Table must define an `updated_at` timestamp column (monotonic), table must use soft deletes (a `deleted_at` or `is_deleted` column) to capture deletes, and poll interval is configurable per table. Without these prerequisites, polling subscriptions are not available for the table. Part of Phase AB (Medium-Complexity Parity).
- **REQ-261** (2026-04-04): Debezium CDC subscription provider for non-PG RDBMS sources (MySQL, MariaDB, SQL Server, Oracle). Debezium captures changes from the source's transaction log and publishes to Kafka topics. Provisa's Kafka notification provider consumes these CDC events and streams them as SSE subscriptions. Requires Kafka + Debezium connector infrastructure. Part of Phase AB (Medium-Complexity Parity).

## API & Integration
- **REQ-256** (2026-04-04): Auto-generated plain REST endpoints for every registered table via `GET /data/rest/{table}` with query string mapping to GraphQL args (`?limit=10&where.id.eq=1`). Compiles and executes via existing pipeline (RLS, masking, routing) with same security as GraphQL. Part of Phase AB (Medium-Complexity Parity).
- **REQ-257** (2026-04-04): Auto-generated JSON:API compliant endpoints for every registered table via `GET /data/jsonapi/{table}` following spec (jsonapi.org): resource objects with type/id/attributes/relationships, sparse fieldsets (`?fields[orders]=amount`), inclusion of related resources (`?include=customer`), filtering (`?filter[region]=US`), sorting (`?sort=-created_at`), pagination (`?page[number]=2&page[size]=25`), compound documents. Compiles and executes via existing pipeline. Part of Phase AB (Medium-Complexity Parity).
- **REQ-258** (2026-04-04): SSE subscriptions via `GET /data/subscribe/{table}` with pluggable notification providers per source type. PostgreSQL uses LISTEN/NOTIFY via asyncpg, MongoDB uses Change Streams via motor collection.watch(), Kafka uses consumer groups. Each provider implements a common async watch() interface returning change events. RLS filtering and schema validation apply regardless of provider. Part of Phase AB (Medium-Complexity Parity).
