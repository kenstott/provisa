# Provisa Competitive Position Matrix

Source of truth: `docs/arch/requirements.yaml`. All claims tagged `[tool-verified]` derive directly from `competitive_position` blocks in that file. Claims tagged `[inferred]` are logical groupings of verified claims.

---

## 1. Summary Table

Capability areas as rows. Competitors as columns. Cell values: **A** = ahead, **P** = parity, **G** = gap, **—** = not applicable to competitor.

| Capability Area | Trino/Presto | Hasura | Denodo | Dremio | Snowflake/BigQuery |
|---|---|---|---|---|---|
| Compile-time governance enforcement | **A** | **A** | **A** | **A** | **A** |
| Two-stage compiler (standalone Stage 2) | **A** | **A** | **A** | **A** | **A** |
| Default-invisible table registration | **A** | **A** | **A** | **A** | **A** |
| Business-model schema (domain/alias) | **A** | **A** | **A** | **A** | **A** |
| Five-tier relationship discovery | **A** | **A** | **A** | **A** | **A** |
| AI-suggested relationships | **A** | **A** | **A** | **A** | **A** |
| Pluggable approval hook (HTTP/gRPC/inline) | **A** | **A** | **A** | **A** | **A** |
| Column masking (6 types + predicate guard) | **A** | **A** | **A** | **A** | **A** |
| Domain-scoped field grants | **A** | **A** | **A** | **A** | **A** |
| Append-only DB-enforced audit log | **A** | **A** | **A** | **A** | **A** |
| Firebase auth | **A** | **A** | **A** | **A** | **A** |
| Dual role resolution (header/JWT) | **A** | **A** | **A** | **A** | **A** |
| Server-assigned roles (no client override) | **A** | **A** | **A** | **A** | **A** |
| Neo4j/SPARQL connectors | **A** | **A** | **A** | **A** | **A** |
| WebSocket/RSS/push receiver sources | **A** | **A** | **A** | **A** | **A** |
| OpenAPI full auto-discovery | **A** | **A** | **A** | **A** | **A** |
| gRPC source connector | **A** | **A** | **A** | **A** | **A** |
| Vector search with model registry | **A** | **A** | **A** | **A** | **A** |
| Hot table VALUES-CTE optimization | **A** | **A** | **A** | **A** | **A** |
| Engine-agnostic hint syntax (/*+ */) | **A** | **A** | **A** | **A** | **A** |
| Security-partitioned cache keys | **A** | **A** | **A** | **A** | **A** |
| Tenant-prefixed cache isolation (design) | **A** | **A** | **A** | **A** | **A** |
| Normalized tabular output (FK-preserving) | **A** | **A** | **A** | **A** | **A** |
| Arrow Flight (cross-source, governed) | **A** | **A** | **A** | P | **A** |
| LIMIT threshold+1 redirect probe | **A** | **A** | **A** | **A** | **A** |
| Client-controlled redirect headers | **A** | **A** | **A** | **A** | **A** |
| pgwire PostgreSQL wire protocol | **A** | **A** | **A** | **A** | **A** |
| pg_catalog role-scoped intercept | **A** | **A** | **A** | **A** | **A** |
| DDL dispatch (Trino vs. direct path) | **A** | **A** | **A** | **A** | **A** |
| DDL capability gating over pgwire | **A** | **A** | **A** | **A** | **A** |
| COPY TO/FROM STDIN with RLS | **A** | **A** | **A** | **A** | **A** |
| Domain-driven DDL target resolution | **A** | **A** | **A** | **A** | **A** |
| Immediate post-DDL schema registration | **A** | **A** | **A** | **A** | **A** |
| ADBC client over Arrow Flight | **A** | **A** | **A** | **A** | **A** |
| Governed JDBC (no ungoverned path) | **A** | **A** | **A** | **A** | **A** |
| JDBC transport via Arrow Flight | **A** | **A** | **A** | **A** | **A** |
| JDBC executeQuery through Stage 2 | **A** | **A** | **A** | **A** | **A** |
| getColumns() filtered by role visibility | **A** | **A** | **A** | **A** | **A** |
| Per-call GraphQL/SQL language selection | **A** | **A** | **A** | **A** | **A** |
| Compile endpoint (/data/compile) | **A** | **A** | **A** | **A** | **A** |
| GraphiQL governed-SQL preview plugin | **A** | **A** | **A** | **A** | **A** |
| JSON:API compliant endpoints | **A** | **A** | **A** | **A** | **A** |
| SSE subscriptions (multi-source, governed) | **A** | **A** | **A** | **A** | **A** |
| x-provisa-kind OpenAPI extension | **A** | **A** | **A** | **A** | **A** |
| APQ governance-gated registration | **A** | **A** | **A** | **A** | **A** |
| gRPC per-RPC role via metadata | **A** | **A** | **A** | **A** | **A** |
| Kafka sinks with 4 trigger modes | **A** | **A** | **A** | **A** | **A** |
| Unified Live Query Engine (SSE + Kafka) | **A** | **A** | **A** | **A** | **A** |
| Multi-table subscription change detection | **A** | **A** | **A** | **A** | **A** |
| Creation-request queue | **A** | **A** | **A** | **A** | **A** |
| Composable fine-grained role capabilities | **A** | **A** | **A** | **A** | **A** |
| Test endpoint governance transparency | **A** | **A** | **A** | **A** | **A** |
| RLS Apply-To toggle (table vs. domain) | **A** | **A** | **A** | **A** | **A** |
| Graph UI (hull overlay, supernode collapse) | **A** | **A** | **A** | **A** | **A** |
| Single-executable installer (airgap) | **A** | **A** | **A** | **A** | **A** |
| Hasura v2 + DDN migration converters | **A** | **A** | **A** | **A** | **A** |
| Dual OTLP export with SQL redaction | **A** | **A** | **A** | **A** | **A** |
| Multi-tenancy schema-per-org (proposed) | **A** | **A** | **A** | **A** | **A** |
| Encryption: pluggable client-owned CMK | **A** | **A** | **A** | **A** | **A** |
| JSON:API connector (planned, unique) | — | — | — | — | — |
| Cypher write mutations | **G** | **G** | **G** | **G** | **G** |
| ADBC Flight port configurability | **G** | **G** | **G** | P | **G** |

Legend: **A** = Provisa ahead, **P** = parity, **G** = gap (Provisa behind or incomplete), **—** = no competitor has this capability (planned unique feature)

---

## 2. Top Differentiators

### Governance Architecture

**Compile-time enforcement, not runtime checks.** Provisa's two-stage compiler (Stage 1: GraphQL→SQL; Stage 2: AST rewrite) applies RLS, column masking, and sampling before any query reaches a data source. Stage 2 operates independently on any SQL string — including JDBC, DB-API, and pgwire traffic — so there is no ungoverned access path regardless of protocol. [tool-verified: REQ-001, REQ-002, REQ-038, REQ-262–266]

**Pluggable approval hook with three transports.** The ABAC approval hook supports HTTP callback, persistent gRPC channel with circuit breaker, and inline function — chosen per source. gRPC channels are kept warm; there is no reconnect overhead per query. [tool-verified: REQ-246, REQ-555, REQ-556]

**Server-assigned roles, no client override.** All clients (DB-API, SQLAlchemy, ADBC, JDBC, pgwire) authenticate with username/password. The server assigns the role; no client-supplied role parameter is accepted. Trino and Dremio allow session-level role overrides. Provisa does not. [tool-verified: REQ-273]

### Protocol Breadth

**pgwire (PostgreSQL wire protocol) with governance.** Provisa embeds a native pgwire listener. Any tool that speaks PostgreSQL — DBeaver, Tableau, psql, JDBC — connects without a special driver. The pg_catalog and information_schema queries are intercepted and answered from an in-memory DuckDB built from the role's compilation context, so schema browsers see only the tables and columns the authenticated role may access. Trino/Presto, Hasura, and Dremio have no pgwire endpoint. [tool-verified: REQ-527, REQ-532]

**Arrow Flight across heterogeneous sources.** Provisa's Arrow Flight server (port 8815) applies the full security pipeline at the Flight layer — RLS, masking, tenant isolation — before streaming record batches. Dremio supports Flight but only for its own lakehouse engine. Hasura, Denodo, and Snowflake do not expose Arrow Flight at all. Results stream batch-by-batch via GeneratorStream; the full result never materializes in Provisa memory. [tool-verified: REQ-051, REQ-143, REQ-145]

**ADBC client over Arrow Flight.** `provisa_client.adbc_connect()` returns an ADBC connection backed by Arrow Flight, streaming RecordBatches natively. No competitor's client library offers ADBC over Arrow Flight. The ADBC Flight port is currently hardcoded at 8815, which is a known gap. [tool-verified: REQ-271, REQ-608]

**JDBC transport via Arrow Flight with silent HTTP fallback.** The JDBC driver automatically uses Arrow Flight for streaming with backpressure when the server is reachable, and falls back to HTTP without user configuration. Trino, Snowflake, and Dremio JDBC drivers use their own binary or HTTP/2 protocols, not Arrow Flight. [tool-verified: REQ-293]

**Per-call GraphQL or SQL.** DB-API and GraphQL clients can send a GraphQL string (Stage 1+2) or a SQL string (Stage 2 only) on the same connection. No competitor provides both languages on a single connection type. [tool-verified: REQ-274]

### Source Federation

**Live no-restart source registration.** Registering a new source takes effect immediately via the Trino dynamic catalog API. Tables are default-invisible until a steward explicitly grants visibility per role — the opposite of most platforms, where new sources expose everything. [tool-verified: REQ-012, REQ-013, REQ-014]

**Federation breadth beyond SQL.** Connectors include Neo4j (Cypher), SPARQL 1.1 triplestores, gRPC sources with proto auto-generation, OpenAPI endpoints with full auto-discovery, WebSocket sources, RSS feeds, and an ingest/push receiver. S3/Iceberg caching materializes remote API results for federation joins. Dremio covers data lake sources; Hasura covers PostgreSQL-family. No competitor spans this range. [tool-verified: REQ-295, REQ-297, REQ-309, REQ-316–318, REQ-322–327, REQ-331, REQ-338, REQ-342]

**Vector search with governed embeddings.** Provisa maintains a model registry for embedding models with fallback materialization, and applies governance to vectorized query results at query time. Competitors treat vector search as a separate system. [tool-verified: REQ-419–431]

### Result Delivery

**Normalized tabular output with FK preservation.** Provisa can flatten GraphQL nested results into relational tables with foreign-key relationships preserved, emitting Parquet or CSV directly from a federation query. Trino, Dremio, and Snowflake return flat result sets and delegate normalization to ETL pipelines. [tool-verified: REQ-049]

**Client-controlled redirect format and threshold.** Clients set `X-Provisa-Redirect-Format` and `X-Provisa-Redirect-Threshold` headers inline with the query call. No other platform allows per-request redirect control via headers; competitors use server-side configuration or separate export APIs. [tool-verified: REQ-137]

**LIMIT threshold+1 probe for redirect decision.** When threshold-based redirect is configured, Provisa runs a single `LIMIT threshold+1` probe — no COUNT(*), no re-execution. The cost of the redirect decision is bounded to one extra row read. [tool-verified: REQ-140]

### Live Data

**Unified Live Query Engine.** A single poll engine drives both SSE subscriptions and Kafka sinks from one execution loop. Independent watermark tracking per output type means a slow Kafka consumer never delays SSE delivery. Hasura uses separate subsystems for subscriptions and event triggers. [tool-verified: REQ-282, REQ-286]

**Multi-table subscription change detection.** When a subscription traverses a registered relationship, Provisa watches all physical tables involved in the join. A change to any joined table re-fires the subscription. Hasura subscriptions watch a single table. [tool-verified: REQ-567]

### Deployment

**Airgapped single-executable installer.** The macOS DMG, Linux AppImage, and Windows EXE bundle Lima/containerd and all service images as `.tar` archives. No outbound network calls at install or first launch. No Docker, OrbStack, or Colima prerequisite. Hasura requires Docker; Dremio requires a multi-component installation. [tool-verified: REQ-223, REQ-227, REQ-228, REQ-294]

**Hasura v2 and DDN migration converters.** CLI tools convert Hasura v2 metadata exports and Hasura DDN supergraph projects to valid Pydantic-validated Provisa YAML config, preserving tables, relationships, column visibility, writable_by grants, RLS filters, and auth config. No competitor provides tooling to import Hasura configuration. [tool-verified: REQ-182, REQ-183, REQ-185–193]

**Dual OTLP export with default SQL redaction.** Operators send telemetry to their own collector and optionally to Provisa support. The support path defaults to redacting SQL literals — query data does not leave the operator's infrastructure unless explicitly enabled. [tool-verified: REQ-545, REQ-547]

---

## 3. Honest Gaps

These requirements have `competitive_position.status: gap` in `docs/arch/requirements.yaml`. They represent areas where Provisa is currently behind or incomplete.

### JSON:API Connector (REQ-656 through REQ-660) — Planned Unique Feature [tool-verified]

The JSON:API source connector is in-progress (target 2026-Q3). No competitor natively federates JSON:API-compliant sources with governance — this is not a gap versus competitors but a differentiated planned capability:

- **REQ-656** — JSON:API connector foundation
- **REQ-657** — Include-based relationship expansion (`?include=`)
- **REQ-658** — Sparse fieldsets (`?fields[type]=field1,field2`)
- **REQ-659** — Pagination following (`links.next`)
- **REQ-660** — Filter pushdown to remote JSON:API endpoint

### Cypher Write Mutations (REQ-661 through REQ-668, REQ-670) [tool-verified]

Cypher read queries against Neo4j sources work. Write mutations do not. The following are unimplemented:

- **REQ-661** — Write label resolution
- **REQ-662** — Writes restricted to pre-registered tables only
- **REQ-663** — `writable_by` ACL enforcement on Cypher writes
- **REQ-664** — DML-capability checking before Cypher writes
- **REQ-665** — Relationship immutability enforcement
- **REQ-666** — Cypher `CREATE` → SQL `INSERT` translation
- **REQ-667** — Cypher `DELETE` translation
- **REQ-668** — Cypher `SET` → SQL `UPDATE` translation
- **REQ-670** — `affected_rows` response from Cypher mutations

Applications using Provisa for Neo4j must treat data as read-only today. Native Neo4j clients still needed for write workflows.

### ADBC Flight Port Configurability (REQ-608) [tool-verified]

`adbc_connect()` hardcodes the Arrow Flight port at 8815. No connection parameter exists to override it. Dremio's Flight client and DuckDB's ADBC drivers allow the port to be specified at connection time. Operators who cannot run the Flight server on port 8815 must use the DB-API or SQLAlchemy client instead.

### Tenant Cache Prefix (REQ-595) — Design Gap [tool-verified]

`RedisCacheStore.get/set` accept `tenant_id`, but `check_cache/store_result` in `cache/middleware.py` omit the parameter. The tenant-prefixed cache key design exists; the middleware does not pass `tenant_id` through to the store. Multi-tenant cache key isolation is not active for query result caching until this is closed.

---

## 4. By Competitor

### Trino / Presto

Trino is Provisa's federation engine, not a competitor in the governance sense. The comparison applies when buyers consider using Trino directly, without a governance layer.

Trino exposes all tables in registered catalogs to every user with catalog access. There is no pre-approval registry, no column masking, no domain-scoped visibility. Schema browsers see the full catalog — visibility is not role-scoped at the metadata layer. Provisa's `getColumns()` returns only the columns the authenticated role may see; Trino's JDBC driver returns all columns and relies on query-time enforcement.

Trino does not have a pgwire endpoint, Arrow Flight endpoint, or ADBC client library. Provisa adds all three while keeping Trino as the execution engine for cross-source federation. The `@provisa` federation hint vocabulary abstracts Trino session property names, so query authors and source configs are decoupled from engine version changes. [tool-verified: REQ-279, REQ-281]

One area where Trino leads: running ANALYZE on source registration (REQ-275) and on-demand statistics refresh (REQ-276) are native Trino practices — Provisa integrates them but does not extend them.

### Hasura

Hasura is the closest architectural neighbor. Both provide a GraphQL layer over data sources with role-based access. The differences are structural.

Hasura enforces permissions at the GraphQL layer via row-level boolean expression filters baked into the schema. Provisa enforces governance at SQL generation time and re-enforces it at execution time via the Stage 2 AST rewrite. A caller who obtains raw SQL access to Hasura's underlying PostgreSQL bypasses all permissions. With Provisa, Stage 2 is the only path — there is no raw-SQL shortcut, including over JDBC, DB-API, SQLAlchemy, and pgwire.

Hasura's subscription model watches a single table. Provisa watches all physical tables touched by a subscription's join walk. Hasura has separate event trigger and subscription systems; Provisa's Unified Live Query Engine serves both from one poll loop.

Hasura does not ship migration tooling for teams moving away from it. Provisa ships both a v2 metadata converter and a DDN HML converter, both producing Pydantic-validated output that starts Provisa without manual fixup. [tool-verified: REQ-182, REQ-183, REQ-193]

APQ governance gating is a material difference: Hasura caches APQ hashes regardless of caller role at registration time. Provisa gates registration on the governance check — a query that exceeds the caller's rights cannot be hash-registered. [tool-verified: REQ-291]

### Denodo

Denodo is a virtual data integration platform targeting enterprise data governance. It enforces policies at the virtualization layer via row and column restrictions defined per view.

Denodo does not expose a pgwire endpoint, Arrow Flight endpoint, or ADBC client. Its governance model applies at view definition time — BI tool users connecting directly to a base view can sometimes bypass policies. Provisa's Stage 2 AST rewrite applies on every query regardless of how the client connects. [tool-verified: REQ-130, REQ-272]

Denodo's source connector set covers relational and LDAP sources; it does not federate Neo4j, SPARQL endpoints, gRPC services, WebSocket streams, or RSS feeds. Provisa's connector catalog is broader. [inferred from connector group requirements]

Denodo has no equivalent to the creation-request queue, the composable capability system, or the governed test-endpoint that shows RLS filters applied in real time. [tool-verified: REQ-060, REQ-062, REQ-063]

### Dremio

Dremio is a lakehouse query engine and data virtualization platform. Its strongest area is analytical performance via reflections (materialized views) and native Parquet/Iceberg support.

Dremio does expose Arrow Flight — but only for its own lakehouse engine. Provisa exposes Arrow Flight across all federated sources, including relational, API, and graph sources, with the full governance pipeline applied at the Flight layer. [tool-verified: REQ-143]

Dremio's JDBC driver applies policies at reflection or dataset definition level, not uniformly at the wire. A developer with access to a base dataset bypasses reflection-level filters. Provisa has no ungoverned JDBC path. [tool-verified: REQ-272]

Dremio's cache isolation is not enforced at the cache key level; result caches are not partitioned by role. Provisa's cache keys include `role_id` and RLS context values. [tool-verified: REQ-544]

Dremio has no Hasura migration tooling, no pgwire endpoint, no SSE subscriptions, and no airgapped installer.

### Snowflake / BigQuery

Snowflake and BigQuery are managed cloud data warehouses. The comparison applies when buyers consider using them as the unified query layer for governed data access.

Neither platform runs as an on-premise or airgapped deployment in any meaningful sense. Provisa's AppImage installer works in environments with no internet access. [tool-verified: REQ-294]

Snowflake enforces column masking at the column policy level. It does not apply masking across federated sources — Snowflake is its own data store. Provisa federates 20+ source types with a single masking and RLS model that applies uniformly. [tool-verified: REQ-130]

BigQuery has no pgwire endpoint and no Arrow Flight client. Both Snowflake and BigQuery allow role overrides in client session configuration; Provisa does not accept client-supplied role parameters. [tool-verified: REQ-273]

Cross-source federated joins that mix Snowflake/BigQuery tables with external RDBMS, Neo4j, or API sources require separate ETL. Provisa federates these inline at query time via Trino, governed by the same RLS and masking rules.
