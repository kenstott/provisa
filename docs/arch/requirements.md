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
