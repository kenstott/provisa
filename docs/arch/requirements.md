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
