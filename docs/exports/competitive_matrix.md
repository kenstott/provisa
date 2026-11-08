# Provisa Competitive Position Matrix

Provisa's side of every judgement comes from `docs/arch/requirements.yaml` and the code. A claim tagged `[tool-verified]` was checked against a requirement's status and description, a line of code, or a competitor's public documentation listed in [Sources](#7-sources). A claim tagged `[inferred]` rests on general product knowledge, or on a feature's absence from the documentation we read. Confirm those before quoting them to a buyer.

Last restructure: 2026-11-07. Earlier versions scored Provisa against Trino, Dremio, Snowflake and BigQuery. Trino, Snowflake and BigQuery are engines Provisa runs on, so they moved to [Works with](#3-works-with) and carry no score. Provisa does not run on Dremio, so it left the matrix. The columns now hold what a buyer would buy instead of Provisa. We re-judged every row against them. The per-cell reasons from the honesty pass of the same day carry over wherever they still apply.

---

## 1. Summary Table

Capability areas as rows. Competitors as columns. The Notes column points to the row's entry in [section 2](#2-row-notes), where every cell that is not **A** has its reason.

| Mark | Meaning |
| --- | --- |
| **A** | Provisa has the capability and the competitor does not document an equivalent. |
| P | Parity. The competitor reaches the same outcome, sometimes by a different mechanism. |
| **G** | Gap. The competitor has the capability and Provisa does not, or Provisa is behind. |
| — | Not applicable. The row describes a property of a feature the competitor lacks, or a use case it does not target. |

### The columns

| Column | Products | Why grouped this way |
| --- | --- | --- |
| Denodo | Denodo Platform 9.5 | Data virtualisation. Shares Provisa's premise of governed access without copying data. |
| Starburst | Starburst Galaxy and Starburst Enterprise | Trino sold as a product, with built-in access control, data products, classification, lineage and an MCP server. Bare Trino is an engine and lives in Works with. |
| Immuta | Immuta | A policy layer that writes its rules into the platforms it governs. Its verdicts differ from every other column, so it stands alone. |
| UC / Horizon | Databricks Unity Catalog, Snowflake Horizon Catalog | Both are governance bundled with one platform, and both govern what runs on that platform. Their verdicts agree on almost every row, so one column holds them. Where they differ, the cell is judged against the stronger of the two and the note names it. |
| Catalogs | Collibra, Atlan, Alation | Catalogs moving into governance. They differ in one place: Collibra Protect writes policies into platforms, while Atlan syncs tags and Alation surfaces policies read from Snowflake. The cell is judged against the strongest of the three and the note names it. |
| Semantic layers | dbt Semantic Layer, Cube, AtScale | Metrics and models defined once over a warehouse. Cube carries most of the governance and protocol verdicts, AtScale the MDX and DAX ones. Judged against the strongest, named in the note. |
| Hasura / PostgREST | Hasura v2 and DDN, PostgREST | API layers over databases. Hasura is the stronger product on every row; PostgREST is named where it matters. |

The two Hasura releases are judged together. Where v2 and DDN differ, the note says which one a source describes.

| Capability Area | Denodo | Starburst | Immuta | UC / Horizon | Catalogs | Semantic layers | Hasura / PostgREST | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Compile-time governance enforcement | P | P | P | P | P | P | P | N1 |
| Two-stage compiler (standalone Stage 2) | **A** | **A** | **A** | **A** | **A** | **A** | **A** | N2 |
| Default-invisible table registration | P | P | P | P | — | P | P | N3 |
| Business-model schema (domain/alias) | P | P | — | P | — | P | P | N4 |
| Five-tier relationship discovery | **A** | **A** | **A** | **A** | **A** | **A** | **A** | N5 |
| AI-suggested relationships | P | **A** | — | P | **A** | P | **A** | N6 |
| Pluggable approval hook (HTTP/gRPC/Unix socket) | P | P | P | **A** | **A** | P | **A** | N7 |
| Column masking (4 types + predicate guard) | **A** | P | P | P | P | P | **A** | N8 |
| Domain-scoped field grants | P | P | P | P | P | **A** | **A** | N9 |
| Append-only DB-enforced audit log | **A** | P | P | P | — | **A** | **A** | N10 |
| Firebase auth | — | — | — | — | — | P | P | N11 |
| Dual role resolution (header/JWT) | P | P | P | P | — | P | P | N12 |
| Server-assigned roles (no client override) | P | P | P | P | — | P | P | N13 |
| Neo4j/SPARQL connectors | **A** | **A** | — | **A** | — | **A** | **A** | N14 |
| WebSocket/RSS/push receiver sources | P | **A** | — | P | — | **A** | **A** | N15 |
| OpenAPI full auto-discovery | P | **A** | — | **A** | — | **A** | P | N16 |
| gRPC source connector | **A** | **A** | — | **A** | — | **A** | **A** | N17 |
| Vector search with model registry | P | **A** | — | P | — | **A** | **A** | N18 |
| Hot table VALUES-CTE optimization | P | P | — | — | — | P | — | N19 |
| Security-partitioned cache keys | P | P | — | P | — | P | P | N21 |
| Tenant-prefixed cache isolation | — | P | — | P | — | P | — | N22 |
| Normalized tabular output (FK-preserving) | **A** | **A** | — | **A** | — | **A** | **A** | N23 |
| Arrow Flight (cross-source, governed) | P | **A** | — | P | — | P | **A** | N24 |
| LIMIT threshold+1 redirect probe | **A** | P | — | P | — | **A** | **A** | N25 |
| Client-controlled redirect headers | **A** | P | — | **A** | — | **A** | **A** | N26 |
| pgwire PostgreSQL wire protocol | P | **A** | — | **A** | — | P | **A** | N27 |
| pg_catalog role-scoped intercept | P | — | — | — | — | P | — | N28 |
| Persistent objects governed from birth (no client DDL; temporary tables on every surface) | **A** | **A** | **A** | **A** | **A** | **A** | **A** | N29 |
| COPY TO/FROM STDIN with RLS | **A** | **A** | — | P | — | — | **A** | N31 |
| ADBC client over Arrow Flight | P | **A** | — | P | — | P | **A** | N32 |
| Governed JDBC (no ungoverned path) | P | P | P | P | — | P | — | N33 |
| JDBC transport via Arrow Flight | P | **A** | — | P | — | P | — | N34 |
| JDBC executeQuery through Stage 2 | P | P | P | P | — | P | — | N33 |
| getColumns() filtered by role visibility | P | P | P | P | — | P | P | N35 |
| Per-call GraphQL/SQL language selection | **A** | **A** | — | **A** | — | **A** | **A** | N36 |
| Compile endpoint (/data/compile) | **A** | **A** | — | **A** | — | P | P | N37 |
| GraphiQL governed-SQL preview plugin | **A** | — | — | — | — | P | P | N38 |
| JSON:API compliant endpoints | **A** | **A** | — | **A** | — | **A** | P | N39 |
| SSE subscriptions (multi-source, governed) | **A** | **A** | — | **A** | — | **A** | **A** | N40 |
| x-provisa-kind OpenAPI extension | **A** | — | — | — | — | — | **A** | N41 |
| APQ governance-gated registration | — | — | — | — | — | — | P | N42 |
| gRPC per-RPC role via metadata | **A** | **A** | — | **A** | — | **A** | **A** | N43 |
| Kafka sinks with 4 trigger modes | P | **A** | — | P | — | **A** | **A** | N44 |
| Unified Live Query Engine (SSE + Kafka) | **A** | **A** | — | **A** | — | **A** | **A** | N45 |
| Multi-table subscription change detection | — | — | — | — | — | — | P | N46 |
| Creation-request queue | **A** | **A** | **A** | **A** | **A** | **A** | **A** | N47 |
| Composable fine-grained role capabilities | P | P | P | P | P | **A** | **A** | N48 |
| Test endpoint governance transparency | **A** | **A** | **A** | **A** | **A** | P | P | N49 |
| RLS Apply-To toggle (table vs. domain) | P | P | P | P | P | **A** | **A** | N50 |
| Graph UI (hull overlay, supernode collapse) | **A** | **A** | **A** | **A** | **A** | **A** | **A** | N51 |
| Single-executable installer (airgap) | P | P | **A** | **A** | **A** | P | **A** | N52 |
| Dual OTLP export with SQL redaction | **A** | **A** | — | — | — | **A** | **A** | N54 |
| Multi-tenancy schema-per-org | P | P | P | P | P | P | **A** | N55 |
| Encryption: pluggable client-owned CMK | P | P | — | P | — | P | — | N56 |
| Cypher write mutations | **A** | **A** | — | **A** | — | — | **A** | N58 |
| Column-level lineage across sources | P | P | **A** | P | P | P | **A** | N60 |
| Business ontology grounded to columns | P | **A** | **A** | **A** | P | **A** | **A** | N61 |
| Governance facts published to external catalogs | **A** | **A** | **A** | **A** | — | **A** | **A** | N62 |
| Governed metrics on every query surface | **A** | **A** | — | P | — | **G** | **A** | N63 |
| Data products with lineage and quality | P | P | P | P | P | **A** | **A** | N64 |
| MCP server under the same governance | P | P | P | P | — | P | **A** | N65 |
| Automated sensitive-data classification | — | P | P | P | P | — | — | N66 |
| Governs users who query the platform directly | — | — | — | — | — | — | — | N67 |

Counts across the 441 cells (63 rows, 7 columns):

| Mark | Denodo | Starburst | Immuta | UC / Horizon | Catalogs | Semantic layers | Hasura / PostgREST | Total |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | 26 | 30 | 10 | 23 | 9 | 27 | 39 | 164 |
| P | 31 | 26 | 17 | 31 | 10 | 28 | 15 | 158 |
| G | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 1 |
| — | 6 | 7 | 36 | 9 | 44 | 7 | 9 | 118 |

For comparison, the honesty pass of the same day scored the old engine columns at 140 A, 106 P, 0 G and 49 dashes across 295 cells. The new columns produce the first gap since that pass, in metrics (N63).

---

## 2. Row Notes

Every non-A cell gets a line of reason, and so does the basis for each **A**. Source keys such as [S1] resolve in [section 7](#7-sources).

**N1. Compile-time governance enforcement.** Stage 2 rewrites every statement before it reaches a source. The entry protocol makes no difference. [tool-verified: REQ-001, REQ-263, REQ-266]
- Denodo P: the server enforces row restrictions and global security policies for every client [D1]. [tool-verified]
- Starburst P: a Galaxy policy can grant privileges, apply row filters or apply column masks [S3][S1][S2]. [tool-verified]
- Immuta P: protect policies filter rows and mask columns and cells at query time [I2]. [tool-verified]
- UC / Horizon P: Unity Catalog row filters and column masks apply at query time [U1]; Snowflake masking and row access policies do the same [W1][W2]. [tool-verified]
- Catalogs P (Collibra): Collibra Protect writes masking and row filters into Snowflake, Databricks, BigQuery and Lake Formation, which then enforce them [K6]. Atlan's data policies govern querying inside Atlan only [K2]. [tool-verified: Atlan; inferred: Collibra, whose docs refused our fetch]
- Semantic layers P (Cube): access policies apply member-level restrictions, row-level filters and masking per group [M3]. [tool-verified]
- Hasura / PostgREST P: Hasura compiles row and column permissions into the generated SQL [H1], and DDN declares model, type and command permissions per role [H16]. PostgREST relies on PostgreSQL row-level security [P1]. [tool-verified]

**N2. Two-stage compiler (standalone Stage 2).** Any SQL string plus a role goes in; governed SQL comes out. [tool-verified: REQ-262, REQ-263] No column offers a service that rewrites arbitrary client SQL this way. Immuta's Starburst and Databricks Spark integrations rewrite queries, but inside the engine [I8]. Cube's SQL API and dbt's `compileSql` return SQL for a semantic query [M2], which is a narrower input. Hasura's explain API returns the SQL for one of its own operations [H15]. [inferred]

**N3. Default-invisible table registration.** Until a steward registers a table, nothing can query, browse or write it. [tool-verified: REQ-014]
- Denodo P: data is reachable only through base views someone creates [D4]. [inferred]
- Starburst P: Galaxy grants rights per entity, and only an owner or a holder of the admin option can grant them [S12]. [tool-verified: grant model; inferred: default deny]
- Immuta P: users reach registered data through subscription policies [I8]. Unregistered objects stay under the platform's own privileges. [inferred]
- UC / Horizon P: a new object is visible only to its owner until access is granted. [inferred]
- Catalogs —: a catalog serves no queries, so there is nothing to make invisible.
- Semantic layers P: Cube and dbt expose only modeled cubes, views and metrics. [inferred]
- Hasura / PostgREST P: Hasura exposes a table once tracked, and a non-admin role sees nothing without permissions [H1]. PostgREST exposes only the schemas it is configured for, under the database's grants [P1]. [inferred: tracking behaviour]

**N4. Business-model schema (domain/alias).** Domains and aliases give stewards a business-facing model over physical tables. [tool-verified: REQ-263]
- Denodo P: derived and interface views are Denodo's core use case. [inferred]
- Starburst P: data products curate datasets, grouped by domain [S5]. [tool-verified]
- Immuta —: it governs physical objects and models none.
- UC / Horizon P: Unity Catalog metric views [U5] and Snowflake semantic views [W7] model business entities and metrics. [tool-verified]
- Catalogs —: they carry business names and domains as metadata, but nothing queries through them. The glossary comparison is N61.
- Semantic layers P: modeling a business view over the warehouse is their purpose. [inferred]
- Hasura / PostgREST P: Hasura custom field names and DDN subgraphs shape a business-facing API. [inferred]

**N5. Five-tier relationship discovery.** Discovery is a governed workflow. It runs from raw inventory through unclaimed assets, domain assets and relationships to field grants, each stage gated on the one before. [tool-verified: REQ-612] No column documents anything staged like this. Catalogs crawl and curate, and Immuta's identification tags columns for policy [I1], but neither gates grants on relationship discovery. [inferred]

**N6. AI-suggested relationships.** On the relationships page, an LLM proposes candidates. [tool-verified: REQ-167]
- Denodo P: Denodo Assistant's "Suggest Joins" proposes join conditions between views [D8]. [tool-verified]
- A basis (Starburst): Galaxy's AI enriches data product descriptions [S11]; we found nothing on relationships. [inferred]
- Immuta —: it has no relationship model.
- UC / Horizon P (Horizon): Semantic View Autopilot generates logical tables, relationships and metrics [W8]. [tool-verified]
- A basis (Catalogs): their AI features suggest descriptions and terms; we found no relationship suggestion. [inferred]
- Semantic layers P: dbt and Cube both offer AI help that drafts semantic models, entities included. [inferred]
- A basis (Hasura / PostgREST): no AI suggestions of any kind for relationships. [inferred]

**N7. Pluggable approval hook.** Per-query ABAC travels over an HTTP webhook, a gRPC channel kept open with a circuit breaker, or a Unix socket. [tool-verified: REQ-246, REQ-555, REQ-556; `provisa/auth/approval_hook.py` defines `WEBHOOK = "webhook"`, `GRPC = "grpc"`, `UNIX_SOCKET = "unix_socket"`]
- Denodo P: custom policies can call external logic per query. [inferred]
- Starburst P: Starburst Enterprise is built on Trino, whose Open Policy Agent plugin sends each decision to an external service [T3]. [tool-verified: Trino; inferred: Starburst ships it]
- Immuta P: its Trino plugin asks Immuta for policy on each query [I8]. Immuta is itself the external decision point. [inferred]
- A basis (UC / Horizon, Catalogs): policies are stored and evaluated in the platform, with no per-query call to an outside service. [inferred]
- Semantic layers P (Cube): a query-rewrite hook runs code per query and can call out. [inferred]
- A basis (Hasura / PostgREST): neither calls an external service to authorize each query. [inferred]

**N8. Column masking (4 types + predicate guard).** Four mask types exist: `regex`, `constant`, `truncate` and `fake`. Masked columns are refused in WHERE and HAVING. [tool-verified: REQ-531; `class MaskType` in `provisa/security/masking.py`]
- A basis (Denodo): Denodo masks values but warns that users "may still be able to infer sensitive values by crafting selective WHERE conditions" [D1]. [tool-verified]
- Starburst P: column masks are SQL expressions [S1]. A WHERE clause sees only the masked value. [tool-verified: masks; inferred: predicate behaviour]
- Immuta P: more mask types than Provisa, among them hashing, null, regex, rounding and format-preserving [I3]. [tool-verified]
- UC / Horizon P: Snowflake applies a masking policy "at every location where the column appears" [W1]; Unity Catalog masks are SQL functions [U1]. [tool-verified]
- Catalogs P (Atlan): data policies hash, redact or show the last four characters [K2]. [tool-verified]
- Semantic layers P (Cube): a member can be marked for masking, and masking can depend on a row filter [M3]. [tool-verified]
- A basis (Hasura / PostgREST): Hasura column permissions allow or hide a column; nothing masks it. PostgREST leaves masking to views written by hand. [inferred]

**N9. Domain-scoped field grants.** One grant can cover a whole domain. [tool-verified: REQ-402]
- Denodo P: a global security policy applies to every view or column carrying a tag [D1]. [tool-verified]
- Starburst P: a policy scope can cover all schemas and all tables of a catalog, and attribute tags select entities [S3]. [tool-verified]
- Immuta P: policies target columns by tag, so one rule covers every tagged column [I1]. [tool-verified]
- UC / Horizon P: an ABAC policy attached to a catalog or schema covers everything beneath it [U2]. [tool-verified]
- Catalogs P (Collibra): Protect policies target data by classification. [inferred]
- A basis (Semantic layers): Cube policies are defined on each cube or view [M3]. [tool-verified]
- A basis (Hasura / PostgREST): permissions are set per table and per role. [inferred]

**N10. Append-only DB-enforced audit log.** PostgreSQL rules turn UPDATE and DELETE on `query_audit_log` into no-ops. [tool-verified: REQ-596; `provisa/audit/query_log.py` runs `CREATE RULE no_delete_audit AS ON DELETE TO query_audit_log DO INSTEAD NOTHING;` and the matching `no_update_audit`]
- A basis (Denodo, Semantic layers, Hasura / PostgREST): query logs go to files or event streams, which the database does nothing to protect. [inferred]
- Starburst P: Galaxy's audit log records each change, who made it and when [S7], in a vendor-managed service. [tool-verified: log; inferred: immutability]
- Immuta P: it audits queries run in Snowflake and Databricks and exports the logs to S3 or ADLS [I4]. [tool-verified: audit; inferred: immutability]
- UC / Horizon P: Databricks writes audit events to the `system.access.audit` table [U6]; Snowflake Access History records reads and writes down to the column [W3]. Users cannot alter either. [tool-verified: tables; inferred: immutability]
- Catalogs —: no query path to audit.

**N11. Firebase auth.** The firebase-admin SDK validates Firebase ID tokens. [tool-verified: REQ-121; `provisa/auth/providers/firebase.py` imports `firebase_admin`]
- Denodo, Starburst, Immuta, UC / Horizon, Catalogs —: none targets sign-in for an application's end users.
- Semantic layers P (Cube): it takes a JWT from the embedding application, and Firebase issues JWTs. [inferred]
- Hasura / PostgREST P: Hasura documents a Firebase integration [H3]. [tool-verified]

**N12. Dual role resolution (header/JWT).** The acting role comes from the `X-Provisa-Role` header or from the token. [tool-verified: `provisa/auth/middleware.py`]
- Starburst P: a policy is active only when its role is in the user's active role set [S3], switched with `SET ROLE` [T5]. [tool-verified]
- Denodo, Immuta, UC / Horizon P: roles and attributes map from the identity provider, and Snowflake switches with `USE ROLE`. [inferred]
- Catalogs —: no query path.
- Semantic layers P (Cube): the security context comes from the token. [inferred]
- Hasura / PostgREST P: the `x-hasura-role` header, or the JWT's default role when the header is absent [H2]. [tool-verified]

**N13. Server-assigned roles (no client override).** Clients may ask for a role. The code comment states the rule: "the server honors it only when the authenticated user is actually assigned that role — a bare client-supplied role is never trusted". When no auth provider is configured, the header is taken at face value. [tool-verified: REQ-273; `provisa/auth/middleware.py`]
- Starburst P: `SET ROLE` succeeds only for a role the user holds [T5]. [tool-verified]
- Denodo, Immuta, UC / Horizon P: privileges follow the authenticated user, with no documented way to claim an ungranted role. [inferred]
- Catalogs —: no query path.
- Semantic layers P: groups come from the signed token. [inferred]
- Hasura / PostgREST P: `x-hasura-role` must appear in the token's allowed roles [H2]. [tool-verified]

**N14. Neo4j/SPARQL connectors.** [tool-verified: REQ-295, REQ-297] Neither appears in Trino's connector list [T4], which Starburst inherits. Hasura lists Neo4j as "Coming Soon" [H12]. Denodo has no native connector for either. [tool-verified: Trino, Hasura; inferred: Denodo, Starburst additions] Unity Catalog federates relational databases over JDBC [U4], Snowflake federates nothing at query time, and semantic layers sit on a SQL warehouse. [tool-verified: Databricks; inferred: the rest] Immuta and the catalogs run no queries: —.

**N15. WebSocket/RSS/push receiver sources.** [tool-verified: REQ-331, REQ-338]
- Denodo P: Kafka listeners receive events [D7], and an RSS feed reads fine as an XML source. It has no WebSocket source. [tool-verified: Kafka; inferred: RSS, WebSocket]
- A basis (Starburst): Trino's only streaming source is Kafka [T4]. [tool-verified]
- UC / Horizon P: Snowpipe Streaming and Databricks streaming tables accept pushed rows. [inferred]
- A basis (Semantic layers, Hasura / PostgREST): no streaming or push sources. [inferred]
- Immuta, Catalogs —: no query path.

**N16. OpenAPI full auto-discovery.** [tool-verified: REQ-314, REQ-319]
- Denodo P: JSON sources create base views "From OpenAPI 3 document" [D4]. [tool-verified]
- A basis (Starburst, UC / Horizon, Semantic layers): no OpenAPI source in Trino's connector list [T4] or the others. [tool-verified: Trino; inferred: the rest]
- Hasura / PostgREST P: DDN's OpenAPI Lambda connector [H12]. [tool-verified]
- Immuta, Catalogs —: no query path.

**N17. gRPC source connector.** [tool-verified: REQ-322] Trino has a Thrift connector [T4]. Nobody lists gRPC [H12]. [tool-verified: Trino, Hasura; inferred: the rest] Immuta and the catalogs —.

**N18. Vector search with model registry.** [tool-verified: REQ-419, REQ-420, REQ-426]
- Denodo P: VECTOR types, distance functions and `EMBED_AI()` [D5]. [tool-verified]
- A basis (Starburst, Semantic layers): no model registry that we found. [inferred]
- UC / Horizon P: Snowflake vector embeddings [W6] and Databricks Vector Search. [tool-verified: Snowflake; inferred: Databricks]
- A basis (Hasura / PostgREST): Hasura reaches vector search through a Qdrant connector [H12], with embeddings made elsewhere. [tool-verified]
- Immuta, Catalogs —: no query path.

**N19. Hot table VALUES-CTE optimization.** Small tables sit in Redis and are inlined as a VALUES CTE. [tool-verified: REQ-230]
- Denodo P: the cache module can materialise a view in full. [inferred]
- Starburst P: Warp Speed builds indexes and caches on worker-local storage from the queries it sees [S13]. [tool-verified]
- Semantic layers P: Cube's in-memory cache and pre-aggregations keep hot results close [M10]. [tool-verified]
- Immuta, UC / Horizon, Catalogs, Hasura / PostgREST —: none plans cross-source joins that would need a local copy.

**N21. Security-partitioned cache keys.** The key covers the governed SQL, its bound values and the role. [tool-verified: REQ-544; `provisa/cache/key.py`]
- Denodo P: security rules apply on top of cached views at query time. [inferred]
- Starburst P: "Cache entries are not shared between users." Each user has a separate result cache [S6]. [tool-verified]
- UC / Horizon P: Snowflake reuses a cached result only if "the role accessing the cached results has the required privileges" [W4]. [tool-verified]
- Semantic layers P: Cube keys its cache by security context. [inferred]
- Hasura / PostgREST P: Hasura's cache key hashes "the role and session variables used in the permissions of the query" [H4]. [tool-verified]
- Immuta, Catalogs —: no cache.

**N22. Tenant-prefixed cache isolation.** The cache middleware passes `tenant_id=org_id` to the store. [tool-verified: REQ-595; `provisa/cache/middleware.py` calls `store.get(key, tenant_id=org_id)`]
- Starburst P: each Galaxy account and each user has a separate cache [S6]. [tool-verified]
- UC / Horizon P: each account or workspace keeps its own result cache. [inferred]
- Semantic layers P: Cube's multitenancy separates data sources and models per tenant [M5]. [tool-verified: multitenancy; inferred: cache]
- Denodo, Immuta, Catalogs, Hasura / PostgREST —: none runs a result cache shared across tenants. [inferred]

**N23. Normalized tabular output (FK-preserving).** [tool-verified: REQ-049] Each competitor that answers queries hands back one result per query, flat or nested. Several related tables from a single query is something only Provisa documents. [inferred] Immuta and the catalogs —.

**N24. Arrow Flight (cross-source, governed).** [tool-verified: REQ-143]
- Denodo P: Arrow Flight SQL on port 9994, over every virtualised source [D3]. [tool-verified]
- A basis (Starburst): its client protocol is HTTP, in direct or spooling mode [T9]. [tool-verified]
- UC / Horizon P: Snowflake and Databricks fetch results as Arrow, over HTTPS. Neither uses Flight. [inferred]
- Semantic layers P (dbt): "The Semantic Layer JDBC API utilizes the open-source JDBC driver with ArrowFlight SQL protocol" [M1]. [tool-verified]
- A basis (Hasura / PostgREST): GraphQL and REST only. [inferred]
- Immuta, Catalogs —.

**N25. LIMIT threshold+1 redirect probe.** [tool-verified: REQ-140]
- A basis (Denodo, Semantic layers, Hasura / PostgREST): results stream or return inline, with no automatic redirect to object storage. [inferred]
- Starburst P: Trino's spooling protocol writes large results to object storage for the client to fetch [T9]. [tool-verified]
- UC / Horizon P: large results arrive as chunks from cloud storage. [inferred]
- Immuta, Catalogs —.

**N26. Client-controlled redirect headers.** [tool-verified: REQ-137]
- A basis: outside Starburst, no column lets a single request choose how its results are delivered. [inferred]
- Starburst P: clients "actively request usage of the spooling protocol" [T9]. Provisa adds a per-request threshold and format on top. [tool-verified]
- Immuta, Catalogs —.

**N27. pgwire PostgreSQL wire protocol.** [tool-verified: REQ-527] This row judges a client surface. BI tools and drivers that speak PostgreSQL connect to Provisa directly.
- Denodo P: "The ODBC interface of Denodo follows the same protocol as PostgreSQL", on port 9996 [D2]. [tool-verified]
- Semantic layers P (Cube): the SQL API "delivers data over the Postgres-compatible protocol" [M4]. [tool-verified]
- A basis (Starburst, UC / Horizon, Hasura / PostgREST): no PostgreSQL-protocol endpoint. PostgREST talks to PostgreSQL but serves HTTP. [inferred]
- Immuta, Catalogs —.

**N28. pg_catalog role-scoped intercept.** [tool-verified: REQ-532]
- — wherever there is no PostgreSQL-protocol endpoint. With no pg_catalog, there is nothing to scope.
- Denodo P: its PostgreSQL-protocol interface answers catalog queries within the user's privileges. [inferred]
- Semantic layers P (Cube): member-level policies hide members from the SQL API's catalog. [inferred]

**N29. Persistent objects governed from birth.** Every surface lets a session create its own temporary tables. Persistent objects are defined in the model, or defined in their source and admitted into the model, and reach production by promotion through branches (REQ-1926). CREATE, ALTER and DROP of persistent objects are refused on every surface, and pgwire answers with SQLSTATE 0A000. [tool-verified]

We mark every cell A, for a governed estate. Every persistent object is born governed: owned by a domain, with its relationships, masks and tags, visible in lineage, and created through a reviewed, versioned branch that is promoted like any change. [inferred: the advantage is a design judgment]
- Denodo: creates tables in external databases through remote tables [D6]. [tool-verified]
- Starburst: takes CREATE TABLE, as Trino does [T8]. [tool-verified]
- Immuta: governs an object once it is registered and tagged. Identification can tag it soon after creation [I1], but the object exists first. [inferred]
- UC / Horizon: both take CREATE TABLE. A tag-driven ABAC policy covers a new object once it carries the tag [U2], and classification can add that tag later [U8][W18]. [inferred]
- Catalogs: describe assets after they exist. [inferred]
- Semantic layers: dbt models live in git and reach production through CI, which matches Provisa's promotion for the objects dbt builds. Warehouse users can still create tables outside it. [inferred]
- Hasura / PostgREST: Hasura admins can send DDL through `run_sql` [H14]. [tool-verified]

Some teams will want client DDL anyway; temporary tables on every surface cover scratch work.

**N31. COPY TO/FROM STDIN with RLS.** [tool-verified: REQ-585; `provisa/pgwire/copy_handler.py`]
- A basis (Denodo, Starburst, Hasura / PostgREST): none offers bulk COPY over a client connection. [inferred]
- UC / Horizon P: Snowflake `COPY INTO` loads staged files [W16]. [tool-verified]
- Semantic layers —: read-only.
- Immuta, Catalogs —.

**N32. ADBC client over Arrow Flight.** [tool-verified: REQ-271]
- Denodo P: its docs show `adbc-driver-flightsql` connecting to Virtual DataPort [D3]. [tool-verified]
- A basis (Starburst, Hasura / PostgREST): no ADBC driver. [inferred]
- UC / Horizon P: Apache Arrow publishes `adbc-driver-snowflake` [W15]. It does not use Flight. [tool-verified]
- Semantic layers P (dbt): the generic ADBC Flight SQL driver should work against the Flight SQL endpoint [M1]. [tool-verified: endpoint; inferred: ADBC]
- Immuta, Catalogs —.

**N33. Governed JDBC, and JDBC executeQuery through Stage 2.**
- Denodo, Starburst, UC / Horizon P: each applies its own access policies to JDBC queries [D1][S3][U1][W1]. [tool-verified: policies; inferred: JDBC path]
- Immuta P: queries over the platform's JDBC driver meet the policies Immuta wrote into it [I7]. [inferred]
- Semantic layers P: dbt's JDBC API [M1] and AtScale's JDBC connections [M7] answer under the layer's own security. [tool-verified: JDBC; inferred: governance]
- Catalogs, Hasura / PostgREST —: no JDBC interface.

**N34. JDBC transport via Arrow Flight.** [tool-verified: REQ-293]
- Denodo P: a Flight SQL JDBC driver, URL `jdbc:arrow-flight-sql://`, port 9994 [D3]. [tool-verified]
- A basis (Starburst): its JDBC driver speaks Trino's HTTP protocol [T9]. [tool-verified]
- UC / Horizon P: Snowflake's JDBC driver fetches results in Arrow format over HTTPS. [inferred]
- Semantic layers P (dbt): the JDBC API is Flight SQL [M1]. [tool-verified]
- Immuta, Catalogs, Hasura / PostgREST —.

**N35. getColumns() filtered by role visibility.** [tool-verified: REQ-128]
- Denodo, Starburst, Immuta, UC / Horizon P: catalog listings follow privileges. [inferred]
- Semantic layers P (Cube): member-level policies hide members per group [M3]. [tool-verified]
- Hasura / PostgREST P: each Hasura role's schema shows only the columns it may read [H1]. [inferred]
- Catalogs —: no JDBC metadata surface.

**N36. Per-call GraphQL/SQL language selection.** One connection, either language, chosen per call. [tool-verified: REQ-274] Denodo serves GraphQL and SQL on separate interfaces [D9]. Cube and dbt do the same, with SQL, REST and GraphQL as separate APIs [M2][M4]. Every other column has one language per interface. [inferred] Immuta and the catalogs —.

**N37. Compile endpoint (/data/compile).** Governed SQL back, nothing executed. [tool-verified: REQ-161]
- A basis (Denodo, Starburst, UC / Horizon): EXPLAIN returns an execution plan. It does not hand back the governed SQL. [inferred]
- Semantic layers P (dbt): the `compileSql` mutation returns the SQL for a metric query [M2]. [tool-verified]
- Hasura / PostgREST P: given a query and a role, Hasura's explain API returns the "generated SQL for the operation" [H15]. [tool-verified]
- Immuta, Catalogs —.

**N38. GraphiQL governed-SQL preview plugin.** [tool-verified: REQ-163]
- — for every column without a GraphQL interface.
- A basis (Denodo): it has a GraphQL service [D9] but documents no SQL preview. [inferred]
- Semantic layers P: Cube's Playground and dbt's GraphQL API both show the generated SQL [M2]. [tool-verified: dbt; inferred: Cube]
- Hasura / PostgREST P: the console's GraphiQL can show generated SQL, backed by the explain API [H15]. [tool-verified: API; inferred: console]

**N39. JSON:API compliant endpoints.** [tool-verified: REQ-257]
- A basis: no column but Hasura documents JSON:API output. Denodo publishes REST and OData. [inferred]
- Hasura / PostgREST P (DDN): the v3 engine source carries a `jsonapi` crate [H17]. Hasura v2 has RESTified endpoints with OpenAPI docs [H9], which are not JSON:API. [tool-verified: crate; inferred: endpoint shape]
- Immuta, Catalogs —.

**N40. SSE subscriptions (multi-source, governed).** [tool-verified: REQ-282]
- A basis (Hasura / PostgREST): Hasura's live and streaming subscriptions are governed but run over WebSocket against one database [H5][H6]. [tool-verified: subscriptions; inferred: single database]
- A basis (the rest): nothing pushes live query results to clients across sources. Denodo's Kafka listeners publish results to Kafka topics [D7]. [inferred]
- Immuta, Catalogs —.

**N41. x-provisa-kind OpenAPI extension.** It marks an OpenAPI operation as query or mutation. [tool-verified: REQ-408]
- — for columns with no OpenAPI source.
- A basis (Denodo, Hasura / PostgREST): both import OpenAPI [D4][H12]. Neither documents a per-operation override. [inferred]

**N42. APQ governance-gated registration.** [tool-verified: REQ-291]
- Hasura / PostgREST P: persisted queries are a Hasura Enterprise feature [H8], and role-based allow lists restrict which operations each role may run [H7]. [tool-verified]
- — elsewhere. There is no persisted-query mechanism to gate. [inferred for Denodo's GraphQL service and the semantic layers' GraphQL APIs]

**N43. gRPC per-RPC role via metadata.** [tool-verified: REQ-617] No column generates a per-role gRPC data API. [inferred] Immuta and the catalogs —.

**N44. Kafka sinks with 4 trigger modes.** [tool-verified: REQ-286]
- Denodo P: a Kafka listener runs a query per event and can publish the result to a Kafka topic [D7]. [tool-verified]
- A basis (Starburst, Semantic layers, Hasura / PostgREST): Hasura event triggers call webhooks on row changes [H11]. Trino's Kafka connector [T4] has no trigger concept. [tool-verified: Hasura, Trino; inferred: semantic layers]
- UC / Horizon P (Databricks): structured streaming writes query results to Kafka. [inferred]
- Immuta, Catalogs —.

**N45. Unified Live Query Engine (SSE + Kafka).** A single poll engine drives both outputs, each with its own watermark. [tool-verified: REQ-282, REQ-286]
- A basis (Hasura / PostgREST): live queries and event triggers are separate features [H5][H11]. [tool-verified]
- A basis (the rest): no live delivery to clients. [inferred]
- Immuta, Catalogs —.

**N46. Multi-table subscription change detection.** [tool-verified: REQ-567]
- Hasura / PostgREST P: live queries refetch the whole query on an interval [H5], so a change to any joined table shows up. [tool-verified]
- — elsewhere: no client subscriptions.

**N47. Creation-request queue.** [tool-verified: REQ-063, REQ-434] When a user lacks the authority to create a view or relationship, Provisa queues a request for someone who has it. Immuta's Request app routes requests for access to existing data, data products and masking exceptions [I5]. That is a different request. Collibra's workflows can route requests to create catalog assets, which are metadata. We saw nothing that queues the creation of a queryable view in any column. [tool-verified: Immuta; inferred: the rest]

**N48. Composable fine-grained role capabilities.** [tool-verified: REQ-063]
- Denodo, Immuta P: both grant privileges per object and per role or attribute. [inferred]
- Starburst P: account-level privileges such as Manage security sit beside entity privileges [S3][S12]. [tool-verified]
- UC / Horizon P: Snowflake roles nest in hierarchies and carry privileges as narrow as a single object. [inferred]
- Catalogs P (Atlan): personas and purposes combine metadata and data policies [K2]. [tool-verified]
- A basis (Semantic layers): Cube Cloud has a fixed set of user roles, Viewer among them [M6]. [tool-verified: Viewer role; inferred: fixed set]
- A basis (Hasura / PostgREST): Hasura admin access is all or nothing. [inferred]

**N49. Test endpoint governance transparency.** The response shows which RLS filters applied and which columns were excluded. [tool-verified: REQ-062]
- A basis (Denodo, Starburst, Immuta, UC / Horizon): policies apply silently to a query. [inferred]
- A basis (Catalogs): Alation's Policy Center shows which policies apply to an asset [K4]. That view is per asset. A query's applied filters are not shown. [tool-verified]
- Semantic layers P (Cube): the Access Policies viewer shows every row filter, member restriction and mask per user group, "without … running test queries" [M11]. It works per model where Provisa works per query. [tool-verified]
- Hasura / PostgREST P: the explain API shows the generated SQL, permission filters included [H15]. [tool-verified]

**N50. RLS Apply-To toggle (table vs. domain).** [tool-verified: REQ-402, REQ-404]
- Denodo P: a global policy filters rows in every view carrying a tag [D1]. [tool-verified]
- Starburst P: a policy's scope can cover every table of a catalog [S3]. [tool-verified]
- Immuta P: global row-level policies apply by tag [I2]. [tool-verified]
- UC / Horizon P: an ABAC row filter policy at catalog or schema level applies to every table it matches [U2]. [tool-verified]
- Catalogs P (Collibra): Protect row filters target data by classification. [inferred]
- A basis (Semantic layers, Hasura / PostgREST): row rules attach one cube, view or table at a time [M3][H1]. [tool-verified: Cube; inferred: Hasura]

**N51. Graph UI (hull overlay, supernode collapse).** [tool-verified: REQ-648] Starburst [S4], Unity Catalog [U3] and the catalogs [K5] draw lineage graphs. None groups nodes into hulls or collapses them. [inferred]

**N52. Single-executable installer (airgap).** [tool-verified: REQ-223, REQ-294]
- Denodo P: its installer runs offline. [inferred]
- Starburst P: Starburst Enterprise runs on the customer's own cluster, offline. [inferred]
- A basis (Immuta, Catalogs): SaaS-first, with self-managed options that need Kubernetes. [inferred]
- A basis (UC / Horizon): managed cloud services. [inferred]
- Semantic layers P (AtScale): an on-premises installer. [inferred]
- A basis (Hasura / PostgREST): Hasura deploys as Docker or Kubernetes images [H13]. [tool-verified]

**N54. Dual OTLP export with SQL redaction.** [tool-verified: REQ-545, REQ-547]
- A basis (Starburst, Hasura / PostgREST): each exports OpenTelemetry to one endpoint [T7][H10]. There is no second path and no literal redaction. [tool-verified: export; inferred: no redaction]
- A basis (Denodo, Semantic layers): monitoring runs through JMX, logs or vendor dashboards, without dual OTLP export. [inferred]
- Immuta, UC / Horizon, Catalogs —: the operator does not run the service's telemetry.

**N55. Multi-tenancy schema-per-org.** Each org gets its own `org_<org_id>` schema and PostgreSQL role. [tool-verified: REQ-695, REQ-697, REQ-699]
- Denodo P: virtual databases separate tenants. [inferred]
- Starburst, Immuta, UC / Horizon, Catalogs P: SaaS accounts, workspaces or projects isolate tenants. [inferred]
- Semantic layers P (Cube): "Cube supports multitenancy out of the box, both on database and data model levels" [M5]. [tool-verified]
- A basis (Hasura / PostgREST): one deployment serves one metadata set. [inferred]

**N56. Encryption: pluggable client-owned CMK.** [tool-verified: REQ-684, REQ-694]
- Denodo P: caches live in storage the customer controls and encrypts. [inferred]
- Starburst P: Galaxy encrypts cached results [S6], and Starburst Enterprise runs on storage the customer controls. [tool-verified: encryption; inferred: customer keys]
- UC / Horizon P: Snowflake Tri-Secret Secure [W5]. [tool-verified]
- Semantic layers P: Cube Store keeps pre-aggregations in storage the customer can control. [inferred]
- Immuta, Catalogs, Hasura / PostgREST —: none stores customer data at rest.

**N58. Cypher write mutations.** Cypher CREATE, SET and DELETE translate to SQL INSERT, UPDATE and DELETE on registered tables, returning affected rows. Relationships stay unwritable by design (REQ-665). [tool-verified: REQ-661, REQ-664 to REQ-670; `provisa/cypher/write_translator.py`] No column accepts Cypher at all. [inferred] Immuta and the catalogs —. Semantic layers —: they write nothing.

**N60. Column-level lineage across sources.** One SQL statement yields a column-level lineage graph, and every graph merges into one federation-wide provenance graph, one hop below each registered table to the source's own fields. Every lineage surface answers only what the caller can read. [tool-verified: REQ-1160, REQ-1161, REQ-2116, REQ-2156]
- Denodo P: its data catalog shows how views derive from base views. [inferred]
- Starburst P: Galaxy builds column-level lineage automatically, "in-Galaxy" [S4]. Lineage stops at Galaxy's edge. [tool-verified]
- A basis (Immuta): no lineage graph that we found. [inferred]
- UC / Horizon P: Unity Catalog captures lineage "down to the column level" for queries run on Databricks [U3]. Horizon claims column-level lineage across Snowflake, external databases, BI tools and OpenLineage feeds [W17]. [tool-verified]
- Catalogs P: lineage scanned across every system is their core product [K5]. They reach systems Provisa never queries, BI tools among them. [tool-verified]
- Semantic layers P (dbt): column-level lineage across a dbt project. [inferred]
- A basis (Hasura / PostgREST): no lineage. [inferred]

**N61. Business ontology grounded to columns.** Field names normalize into terms. A term is admitted only when grounded to a table or column, takes its domains from those references, and has an owner and a lifecycle. [tool-verified: REQ-1387, REQ-1591, REQ-1593, REQ-2144]
- Denodo P: its data catalog carries a business glossary linked to views. [inferred]
- A basis (Starburst): Context Studio, for glossary and business rules, is in private preview. [inferred]
- A basis (Immuta, Hasura / PostgREST, Semantic layers): no glossary. Semantic layers carry descriptions on metrics and dimensions. [inferred]
- A basis (UC / Horizon): governed tags, descriptions and semantic views. We found no grounded glossary. [inferred]
- Catalogs P: a business glossary with owners, workflows and links to assets is the catalogs' home ground, and it spans every system. Provisa's terms reach only what is registered. [inferred]

**N62. Governance facts published to external catalogs.** Provisa pushes tables, columns, domains, stewards, approved relationships and compiled lineage to OpenLineage, OpenMetadata, Atlan, Collibra, DataHub and Atlas. It projects which columns are masked, row-restricted or hidden as tags, on each change and on a scheduled reconcile. [tool-verified: REQ-1069, REQ-1070, REQ-1071, REQ-1072]
- A basis: catalogs crawl Denodo, Starburst, Unity Catalog, Snowflake and dbt. None of those pushes the facts of what it enforces. Atlan does reverse-sync tags into Snowflake [K1], which runs the other way. [tool-verified: Atlan; inferred: the rest]
- A basis (Immuta): it links access requests from catalogs [I5] and reads their tags. [tool-verified: request links; inferred: tag reads]
- Catalogs —: they are the target.

**N63. Governed metrics on every query surface.** A Metric is a named aggregate with role visibility, a steward and a version. One compiler expansion projects it into every surface, with its description travelling with the value. [tool-verified: REQ-1317, REQ-1319]
- A basis (Denodo, Starburst, Hasura / PostgREST): aggregates live in views or queries, with no metric object. [inferred]
- UC / Horizon P: Unity Catalog metric views define a metric once and query it from SQL, dashboards and BI tools [U5]. Snowflake semantic views do the same [W7]. [tool-verified]
- Semantic layers **G**: metrics are the product. AtScale serves Excel and Power BI natively through MDX and DAX [M8], which Provisa does not speak. MetricFlow also carries metric types beyond Provisa's builder. [tool-verified: AtScale DAX; inferred: metric types; Provisa: no MDX, DAX or XMLA requirement found]
- Immuta, Catalogs —.

**N64. Data products with lineage and quality.** A data product belongs to one domain, has an owner, shows lineage scoped to its members, and owns quality checks on its inputs and outputs. [tool-verified: REQ-1634, REQ-1640, REQ-2157]
- Denodo P: Data Marketplace lists curated views. [inferred]
- Starburst P: data products are "curated datasets", grouped by domain and defined in a file [S5][S11]. [tool-verified]
- Immuta P: data product managers publish data products in the Request app, where consumers request access [I5]. [tool-verified]
- UC / Horizon P: Snowflake publishes data products as listings, and Provisa itself publishes to Horizon (REQ-1659). [tool-verified: REQ-1659; inferred: Snowflake listings]
- Catalogs P (Atlan): data products with scores and business lineage [K3]. [tool-verified]
- A basis (Semantic layers, Hasura / PostgREST): no data product object. [inferred]

**N65. MCP server under the same governance.** AI agents discover catalog metadata and run SQL and Cypher through the same governed pipeline as every other client. [tool-verified: REQ-1008, REQ-1990]
- Denodo P: the open-source Denodo AI SDK gives AI applications access to Denodo under its security [D10]. We could not confirm an MCP server in it, so parity is inferred. [inferred]
- Starburst P: an MCP server runs read-only SQL under Galaxy roles, with OAuth [S8]. Provisa's server also runs Cypher. [tool-verified]
- Immuta P: agentic data access governs agents acting for a user, with the agent's permissions intersected with the user's entitlements [I6]. [tool-verified]
- UC / Horizon P: Databricks MCPs are Unity Catalog securables [U7]; Snowflake runs a managed MCP server [W20]. [tool-verified]
- Semantic layers P (Cube): an MCP server over OAuth on all plans [M6]. [tool-verified]
- A basis (Hasura / PostgREST): Hasura's AI access is PromptQL, a natural-language layer. [inferred]
- Catalogs —: their MCP servers, where they exist, serve metadata. [inferred]

**N66. Automated sensitive-data classification.** The profiler tests each column's sampled values against classification kinds: the SSN rules, the Luhn check for card numbers, IBAN check digits, national-identifier formats, and lists of cities, countries and currencies. When enough values pass, it raises a suggestion with the evidence, for example "97% of sampled values pass the SSN rules". A steward accepts or rejects it. Accepting a sensitive kind marks the column PII and offers a mask preset such as `ssn_last4`. An org admin can disable any kind. [tool-verified: REQ-2147, in progress; `provisa/profiler/classify.py`, `provisa/api/admin/profiler_classifications_router.py`, Admin → Classification kinds]

The P cells carry one difference. Provisa detects and a steward decides; the competitors below scan and apply tags automatically, and their policies follow the tags.
- Starburst P: data classifier jobs analyze data and metadata, and their tags feed ABAC policies [S9]. [tool-verified]
- Immuta P: identification discovers and tags sensitive data, and policies then target the tags [I1]. [tool-verified]
- UC / Horizon P: Databricks Data Classification "uses an agent to automatically classify and tag tables" [U8]; Snowflake "automatically discovers sensitive data" [W18]. [tool-verified]
- Catalogs P (Collibra): automatic classification and asset association [K7]. [inferred: Collibra docs refused our fetch]
- Denodo, Semantic layers, Hasura / PostgREST —: none classifies data. [inferred]

**N67. Governs users who query the platform directly.** Provisa governs what it serves. It does not own the physical layer, and the platform's own controls govern direct sessions. Every cell is — for that reason: the row compares scopes, and Provisa's scope ends at its own surfaces. [inferred: scope judgment]
- Immuta —: it administers privileges inside the platforms it governs, "Immuta administers Snowflake privileges on data registered in Immuta" [I7]. That is the platform's layer.
- UC / Horizon —: they are the platform's own controls [U1][W1].
- Catalogs —: Collibra Protect writes masking and row filters into Snowflake, Databricks, BigQuery and Lake Formation [K6], again the platform's layer. [inferred: Collibra docs refused our fetch]
- Denodo, Starburst, Semantic layers, Hasura / PostgREST —: each governs the queries it serves, as Provisa does.

### Rows removed in the restructure

- **Engine-agnostic hint syntax (`/*+ */`).** The hints map onto Trino session properties. That describes what Provisa adds over an engine, so it moved to [Works with](#3-works-with) under Trino. [tool-verified: REQ-279]
- **Hasura v2 + DDN migration converters.** It scored — in every column. The converters matter to buyers leaving Hasura, so they now sit in the Hasura entry of [section 6](#6-by-competitor). [tool-verified: REQ-182, REQ-183]
- **JSON:API connector.** No requirement backs it, and the code has no JSON:API source type. Provisa serves JSON:API (N39). It does not consume it. [tool-verified]
- **ADBC Flight port configurability.** `adbc_connect()` takes `port: int = 8815` [tool-verified: REQ-711; `provisa-client/provisa_client/adbc.py`]. A port argument is a setting, so it no longer gets a row.

---

## 3. Works with

Provisa compiles governed SQL and hands it to a federation engine. You pick the engine. These are not scored, because they are what Provisa runs on.

Every engine is declared in `provisa/federation/engine.py`, in the `_ENGINE_BUILDERS` registry. Each one declares its traits, and the planner reads them as inputs. The table shows reach, whether the engine spreads work across a cluster (MPP), and whether its own store is transactional. [tool-verified: REQ-840, REQ-897] A source reaches an engine in one of three ways: live (virtual), scanned in place, or materialized into the engine's store. [tool-verified: `class Strategy` in `provisa/federation/strategy.py`, REQ-826] Pushdown is declared per engine and source type on each connector. [tool-verified: REQ-842]

Every engine below returns Arrow, materialized and streamed, which Provisa serves over Arrow Flight. [tool-verified: each builder's `capabilities` includes `EngineCapability.ARROW_STREAM`]

| Engine | Reach | MPP | Own store transactional | What it adds or limits underneath Provisa |
| --- | --- | --- | --- | --- |
| Trino (embedded or your own coordinator) | Broad | Yes | No | The widest native reach, one Trino catalog per source type. Lakehouse catalogs scan files in place. Replicas land only in PostgreSQL today: `supported_materialize_stores=frozenset({"postgresql"})`. The `@provisa` hint vocabulary hides Trino session property names from query authors. [tool-verified: `build_trino_engine`; REQ-279] |
| DuckDB | Partial | No | Yes | Embedded, in-process, zero configuration. Reads CSV, Parquet and Iceberg in place, and keeps replicas in an embedded DuckDB file per org. Single node. [tool-verified: `build_duckdb_engine`; REQ-989] |
| MotherDuck | Partial | No | Yes | The DuckDB engine with its store in MotherDuck, so every node of a deployment reaches the same replicas. [tool-verified: `build_motherduck_engine`; REQ-1999] |
| PostgreSQL (your own or embedded) | Partial | No | Yes | Federates through foreign data wrappers and pg_duckdb, each probed live and disabled when absent. Single node: cross-server joins materialize locally. PostgreSQL has no cross-database queries, so references are never catalog-qualified. [tool-verified: `build_pg_engine`; REQ-904, REQ-1730] |
| ClickHouse (embedded chdb or a server) | Partial | Yes | No | OLAP execution across shards. Reads S3, Iceberg and Delta in place. Replicas land in ClickHouse itself. No catalog level, so the catalog folds into the schema. [tool-verified: `build_clickhouse_engine`; REQ-909] |
| Snowflake | Partial | Yes | Yes | Links cloud object and lake sources in place through external stages and tables. Every other source lands in Snowflake. Arrow reads through `fetch_arrow_batches`. [tool-verified: `build_snowflake_engine`; REQ-988] |
| Databricks | Partial | Yes | Yes | Links object and lake sources through Unity Catalog external tables; the rest land. Arrow reads through Cloud Fetch. [tool-verified: `build_databricks_engine`; REQ-987] |
| BigQuery | Partial | Yes | No | Links GCS sources, and other clouds through BigLake, as external tables; the rest land in a dataset per source. Arrow reads through the Storage Read API. Replicas default to the platform's own PostgreSQL. [tool-verified: `build_bigquery_engine`] |
| Microsoft Fabric Warehouse | Partial | Yes | Yes | T-SQL. OneLake and ADLS sources attach as zero-copy views over `OPENROWSET`; the rest land in a schema per source. Azure AD sign-in. [tool-verified: `_build_mssql_warehouse_engine`] |
| Azure Synapse serverless SQL | Partial | Yes | No | The Fabric runtime over ADLS. Read-only: `transactional=(name == "fabric")`. [tool-verified: `_build_mssql_warehouse_engine`] |
| Any SQLAlchemy database | Self only | No | Yes | MySQL, MariaDB, Oracle, SQL Server, Db2, Redshift, Greenplum, CockroachDB, YugabyteDB, openGauss, TiDB, SingleStore, Vertica, Exasol, SAP HANA, MonetDB and Firebird. No federation connectors: every source lands in the database. [tool-verified: `build_sqlalchemy_engine`, `_RDB_KINDS`; REQ-905, REQ-1421] |

**Engines that are also competitors' platforms.** Starburst is Trino sold with governance on top, and Unity Catalog and Horizon are the governance of Databricks and Snowflake. Run Provisa on one of them and its own controls keep governing anyone who connects to it directly, since Provisa governs what it serves (N67).

---

## 4. Top Differentiators

The leads below hold against every column, or nearly every one. Evidence is in section 2.

**A standalone governance transformer.** Stage 2 takes any SQL string and a role and returns governed SQL. GraphQL, the SQL endpoint, JDBC, DB-API, SQLAlchemy, pgwire, Flight and MCP all pass through it. Competitors enforce governance inside their own engine or write it into someone else's, and none exposes an equivalent service (N2). [tool-verified: REQ-262, REQ-263, REQ-266]

**The engine is a setting.** Starburst is Trino; Unity Catalog and Horizon govern one platform each; Immuta and Collibra write into the platforms they govern. Provisa runs the same model and policy on any of eleven engines, from embedded DuckDB to Snowflake (section 3). [tool-verified: `_ENGINE_BUILDERS`]

**Sources beyond SQL.** Provisa federates Neo4j, SPARQL triplestores, gRPC services with proto auto-generation, OpenAPI endpoints, WebSocket streams, RSS feeds and an HTTP push receiver. Denodo and Hasura DDN match it on OpenAPI. No column we checked has a graph-database or gRPC source (N14, N17). [tool-verified: REQ-295, REQ-297, REQ-314, REQ-322, REQ-331, REQ-338]

**Many client protocols behind one governance path.** pgwire, Arrow Flight, ADBC, JDBC over Flight, GraphQL, JSON:API, Cypher, MCP and a per-role gRPC API, all through the same governance. Denodo comes nearest with PostgreSQL protocol and Flight SQL; Cube has PostgreSQL protocol and GraphQL; dbt has Flight SQL (N24, N27, N32, N39, N43, N65). [inferred: the combination]

**Live delivery across sources.** SSE subscriptions and Kafka sinks share one poll engine with independent watermarks. Hasura comes closest, with subscriptions against one database and event triggers as a separate system (N40, N45). [tool-verified: REQ-282, REQ-286]

**Enforcement facts flow to your catalog.** Provisa tells Collibra, Atlan, DataHub and the others which columns are masked and which tables are row-restricted, on every change. Catalogs crawl every other column and learn only what exists (N62). [tool-verified: REQ-1071, REQ-1072]

---

### Governed from birth: objects are defined in the model [tool-verified]

A session can create temporary tables on every surface. Persistent tables, views and other objects are defined in the model and promoted through branches (REQ-1926), so a team used to creating tables from a SQL client works in the model instead. See N29.

| Column | How a persistent object comes into being |
| --- | --- |
| Denodo | Remote tables create tables in external databases [D6]. |
| Starburst | CREATE TABLE from any client with the privilege [T8]. |
| Immuta | In the platform, ungoverned until registered and tagged [I1]. |
| UC / Horizon | CREATE TABLE; a tag-driven policy covers it once a tag arrives [U2]. |
| Catalogs | Elsewhere; the catalog learns of it on the next crawl. |
| Semantic layers | dbt models ship through git and CI. Other tables in the warehouse do not. |
| Hasura / PostgREST | `run_sql` sends DDL from the admin console [H14]. |

## 5. Honest Gaps

### Governance on the query path is parity [tool-verified]

Every column that answers queries has row filters, column masking, role resolution and role switching. Starburst and Unity Catalog attach ABAC policies by tag. Immuta ships more mask types than Provisa. Where Provisa does lead, the lead is narrower. It comes from uniform enforcement across protocols and sources, and from the predicate guard that Denodo lacks (N8). See also N1, N12 and N13.

### Classification asks a steward [tool-verified]

The profiler finds sensitive columns from their values and suggests a kind with its evidence, and a steward decides. Starburst, Immuta, Unity Catalog, Snowflake and Collibra apply their tags automatically (N66). Classification is still in progress.

### Scope: direct platform sessions [inferred]

This is a boundary. Provisa governs what it serves and does not own the physical layer, so a session that connects straight to Snowflake or Databricks is governed by the platform's own controls (N67). Immuta, Collibra Protect, Unity Catalog and Horizon work in that layer. If your users keep direct credentials, run one of them beside Provisa.

### Metrics for Excel and Power BI [tool-verified]

AtScale answers Excel and Power BI in MDX and DAX. Provisa's metrics reach every Provisa surface, but neither of those languages is one of them (N63).

### Catalogs see further [inferred]

Collibra, Atlan and Alation scan lineage and glossary across every system, BI tools included. Provisa's lineage and ontology cover what is registered in Provisa (N60, N61). Provisa publishes into those catalogs, and most buyers keep one.

### Denodo matches Arrow Flight and pgwire [tool-verified]

Denodo 9.5 offers Arrow Flight SQL with JDBC and Python ADBC drivers, and its ODBC interface speaks the PostgreSQL protocol. Cube speaks the PostgreSQL protocol too, and dbt's JDBC API is Flight SQL. See N24, N27 and N32.

---

## 6. By Competitor

### Denodo

Denodo is a data virtualization platform. On protocols and governance it is the strongest match for Provisa, with 31 parity cells.

Denodo enforces row restrictions and tag-driven global security policies, masking included [D1]. Its own documentation concedes that masking does not stop inference through WHERE conditions. Provisa refuses masked columns in WHERE and HAVING, which is its clearest governance lead over Denodo. [tool-verified: REQ-531]

Much of the protocol story is shared. Denodo's ODBC interface follows the PostgreSQL protocol [D2], and version 9.5 adds Arrow Flight SQL with JDBC and Python ADBC drivers [D3]. It builds base views from OpenAPI 3 documents [D4]. Vector types with `EMBED_AI()` [D5], LLM-suggested joins [D8] and Kafka listeners that publish query results [D7] round out the parity cells.

Where does Provisa pull ahead? On sources Denodo lacks, Neo4j, SPARQL and gRPC among them, and on SSE subscriptions and normalized output. The governance-transparent test endpoint, governed metrics and MCP are Provisa's too, as is publishing enforcement facts to catalogs. Denodo runs its own engine. Provisa lets you pick one.

We found no verified gap against Denodo.

### Starburst

Starburst sells Trino with governance, a catalog explorer, data products and AI features. Galaxy policies grant privileges, filter rows and mask columns, selected by role and by attribute tags [S3]. Each user gets a separate result cache [S6]. Data classifier jobs tag sensitive data automatically [S9]. Galaxy also builds column-level lineage within its own walls [S4] and runs an MCP server for read-only SQL [S8].

On query-path governance and domain-scoped grants the two are level. Trino's spooling protocol matches Provisa's large-result redirect [T9], and data products and MCP are at parity too.

Provisa leads on every client protocol beyond Trino's HTTP protocol. Flight, ADBC, pgwire, GraphQL, JSON:API, Cypher and gRPC all count. It also leads on non-SQL sources, live delivery, governed metrics and publishing to catalogs.

Classification is level with a difference: Starburst tags automatically, and Provisa asks a steward (N66). We found no verified gap.

Starburst also appears in Works with. Provisa can run on Trino, and Starburst's own governance keeps working underneath.

### Immuta

Immuta sells governance itself. It registers data in Snowflake, Databricks, Starburst, BigQuery, Redshift and others. It writes subscription and protect policies into each platform and audits the queries that run there [I7][I8][I4]. Its Request app handles access requests and data product access [I5]. Its agentic data access governs AI agents acting for a user [I6].

Immuta answers no queries of its own, so 36 of its 63 cells are —.

Masking and row filters are level, and so is tag-driven scope. Immuta's Trino plugin makes a policy decision per query, it audits, and it governs data products and agents.

Provisa is ahead on the standalone compiler and on default invisibility through registration. The test endpoint, lineage, the grounded ontology and governed-from-birth objects are Provisa leads as well.

Classification is level, with Immuta tagging automatically where Provisa asks a steward (N66). Immuta works in the platforms' own layer, which Provisa does not own (N67). If your problem is policy across platforms people already query directly, buy Immuta, and put Provisa beside it.

### Unity Catalog and Snowflake Horizon

These are the governance of Databricks and Snowflake. Unity Catalog has row filters, column masks and tag-driven ABAC policies at catalog or schema level [U1][U2]. Column-level lineage [U3], metric views [U5], an audit system table [U6], MCP servers as securables [U7] and automatic classification [U8] come with it. Horizon brings masking and row access policies [W1][W2], Access History [W3] and semantic views [W7]. Classification [W18], lineage [W19] and a managed MCP server [W20] complete it.

Both match Provisa on query-path governance, domain-scoped policies and audit. Lineage, metrics, data products and MCP are level too.

Provisa reaches every source outside the platform. Unity Catalog federates relational databases read-only [U4], and Snowflake federates nothing at query time. Client protocols, live delivery, the test endpoint and governed-from-birth objects are Provisa leads, and Provisa runs on any engine.

Classification is level, automatic on their side and steward-decided on Provisa's (N66). Both platforms also appear in Works with. Run Provisa on Databricks or Snowflake and their controls stay in force for direct users, while Provisa governs everything else.

### Collibra, Atlan and Alation

Catalogs describe and classify data across every system. Atlan's data policies mask and restrict queries run inside Atlan [K2], and it reverse-syncs tags into Snowflake [K1]. Alation's Policy Center surfaces policies read from Snowflake [K4]. Collibra Protect goes furthest and writes masking and row filters into Snowflake, Databricks, BigQuery and Lake Formation [K6].

Because catalogs answer no queries, 44 of the 63 cells are —.

Where catalogs enforce masking, they match Provisa. Domain-scoped policy, lineage, the glossary, data products and role composition are level as well.

Provisa leads by publishing enforcement facts outward (N62), and with the test endpoint and governed-from-birth objects.

Collibra classifies automatically, where Provisa asks a steward (N66). All three catalogs see lineage and glossary terms in systems Provisa never queries. Provisa publishes to each of them, so most buyers will run both.

### Semantic layers: dbt Semantic Layer, Cube, AtScale

Semantic layers define metrics once over a warehouse. Cube has access policies with row filters, member restrictions and masking [M3]. It also offers a PostgreSQL-protocol SQL API [M4], multitenancy [M5] and an MCP server [M6]. dbt's JDBC API is Arrow Flight SQL [M1], and its GraphQL API compiles metric queries to SQL [M2]. AtScale serves Excel and Power BI in MDX and DAX [M8], with row security objects [M9].

Query-path governance is level. So are pgwire, Flight SQL JDBC, compile-to-SQL, MCP, multitenancy and caching.

Each layer sits on one warehouse, so federation across sources is Provisa's lead. Non-SQL sources, live delivery, Cypher, domain-scoped grants, data products and lineage beyond one project follow from it.

Metrics go to the semantic layers (N63). AtScale reaches Excel and Power BI natively through MDX and DAX. Provisa imports semantic models through Apache Ossie and exports a dbt sources file, so it can sit beside a semantic layer. [tool-verified: REQ-1316, REQ-1967]

### Hasura and PostgREST

Hasura is the closest architectural neighbour. Both put an API with role-based permissions over databases. PostgREST is the minimal version, a REST API over one PostgreSQL database governed by PostgreSQL roles and row-level security [P1].

Five Hasura features match Provisa directly. It compiles row and column permissions into its generated SQL [H1]. It resolves roles from a header or JWT, checked against allowed roles [H2]. Its cache key includes the role and session variables [H4]. Its explain API returns the generated SQL for a given role [H15]. And DDN carries a JSON:API implementation [H17].

Live queries refetch the whole query on an interval, so changes to joined tables are seen [H5]. Event triggers are a separate feature that calls webhooks [H11].

Provisa leads in 39 of 63 cells. Hasura has no pgwire, Flight, ADBC, JDBC, Cypher or gRPC surface, and it does not mask columns. Hasura DDN lists Neo4j as coming soon [H12]. Multi-source subscriptions, the shared SSE and Kafka engine, lineage, metrics and data products are Provisa's alone.

Leaving Hasura? Provisa converts Hasura v2 metadata and DDN configuration into a Provisa model. [tool-verified: REQ-182, REQ-183]

We found no verified gap against Hasura or PostgREST.

---

## 7. Sources

Fetched 2026-11-07.

**Denodo** (Virtual DataPort 9.5; base `https://community.denodo.com/docs/html/browse/latest/en/`)
- [D1] Global security policies. `vdp/administration/databases_users_and_access_rights_in_virtual_dataport/global_security_policies/global_security_policies`
- [D2] Details of the ODBC interface. `vdp/developer/access_through_odbc/details_of_the_odbc_interface/details_of_the_odbc_interface`
- [D3] Access through Arrow Flight SQL. `vdp/developer/access_through_flight_sql/access_through_flight_sql`
- [D4] JSON sources. `vdp/administration/creating_data_sources_and_base_views/json_sources/json_sources`
- [D5] Semantic search using embeddings. `vdp/administration/appendix/semantic_search_using_embeddings/semantic_search_using_embeddings`
- [D6] Remote tables. `vdp/administration/remote_tables/remote_tables`
- [D7] Kafka listeners. `vdp/administration/kafka_listeners/kafka_listeners`
- [D8] Denodo Assistant view features. `platform/administration/denodo_assistant_features/denodo_assistant_large_language_models/view_features/denodo_assistant_view_features`
- [D9] GraphQL service. `vdp/administration/restful_architecture/graphql_service/graphql_service`
- [D10] Denodo AI SDK. https://github.com/denodo/denodo-ai-sdk

**Starburst** (base `https://docs.starburst.io/starburst-galaxy/`)
- [S1] Column masks. `security-and-compliance/manage-data-access/column-masks.html`
- [S2] Row filters. `security-and-compliance/manage-data-access/row-filters.html`
- [S3] Access control policy types. `security-and-compliance/manage-data-access/access-control-policy-types.html`
- [S4] Data lineage. `data-engineering/optimization-performance-and-quality/observability/data-lineage.html`
- [S5] Create data products. `working-with-data/data-products/create-data-products.html`
- [S6] Query result caching. `data-engineering/optimization-performance-and-quality/workload-optimization/query-result-caching.html`
- [S7] Manage access changes (audit log). `security-and-compliance/manage-galaxy-access/monitor-access-changes.html`
- [S8] MCP server. `starburst-ai/mcp-server.html`
- [S9] Data classifier jobs. `security-and-compliance/manage-galaxy-access/manage-users-roles-and-tags/data-classifier-jobs.html`
- [S11] Browse and organize data products; data product enrichment with AI. `working-with-data/data-products/browse-organize-data-products.html`, `starburst-ai/data-product-enrichment-ai.html`
- [S12] Galaxy privileges basics. `reference/access-control/galaxy-privileges-basics.html`
- [S13] Warp Speed-enabled clusters. `data-engineering/optimization-performance-and-quality/workload-optimization/warp-speed-enabled.html`

**Trino** (cited where Starburst inherits Trino behaviour)
- [T3] Open Policy Agent access control. https://trino.io/docs/current/security/opa-access-control.html
- [T4] Connectors. https://trino.io/docs/current/connector.html
- [T5] SET ROLE. https://trino.io/docs/current/sql/set-role.html
- [T7] Observability with OpenTelemetry. https://trino.io/docs/current/admin/opentelemetry.html
- [T8] CREATE TABLE. https://trino.io/docs/current/sql/create-table.html
- [T9] Client protocol. https://trino.io/docs/current/client/client-protocol.html

**Immuta** (base `https://documentation.immuta.com/saas/`)
- [I1] Data identification. `configuration/tags/data-discovery`
- [I2] Data policies reference guide. `govern/authoring-policies-in-secure/data-policies/reference-guides/data-policies`
- [I3] Orchestrated masking policies. `govern/authoring-policies-in-secure/data-policies/reference-guides/masking-matrix-functions/orchestrated-masking-policies`
- [I4] Audit. `govern/detect-your-data/audit`
- [I5] Request app introduction. `request/introduction`
- [I6] Agentic data access introduction. `agentic-data-access/introduction`
- [I7] Protecting data in Snowflake. `configuration/integrations/snowflake/snowflake-overview/protecting-data`
- [I8] Integrations and feature support matrix. `configuration/integrations`
- [I9] Secure introduction. `govern/authoring-policies-in-secure/secure-introduction`

**Databricks Unity Catalog** (base `https://docs.databricks.com/aws/en/`)
- [U1] Row filters and column masks. `tables/row-and-column-filters`
- [U2] Attribute-based access control. `data-governance/unity-catalog/abac/`
- [U3] Lineage in Unity Catalog. `data-governance/unity-catalog/data-lineage`
- [U4] Connect to external databases and catalogs. `query-federation/`
- [U5] Unity Catalog metric views. `metric-views/`
- [U6] Audit log system table. `admin/system-tables/audit-logs`
- [U7] Databricks-provided MCPs. `generative-ai/mcp/managed-mcp`
- [U8] Data Classification. `data-governance/unity-catalog/data-classification`

**Snowflake Horizon**
- [W1] Dynamic data masking. https://docs.snowflake.com/en/user-guide/security-column-ddm-intro
- [W2] Row access policies. https://docs.snowflake.com/en/user-guide/security-row-intro
- [W3] Access History. https://docs.snowflake.com/en/user-guide/access-history
- [W4] Persisted query results. https://docs.snowflake.com/en/user-guide/querying-persisted-results
- [W5] Tri-Secret Secure. https://docs.snowflake.com/en/user-guide/security-encryption-tss
- [W6] Vector embeddings. https://docs.snowflake.com/en/user-guide/snowflake-cortex/vector-embeddings
- [W7] Semantic views. https://docs.snowflake.com/en/user-guide/views-semantic/overview
- [W8] Semantic View Autopilot. https://docs.snowflake.com/en/user-guide/views-semantic/ui
- [W15] ADBC driver for Snowflake. https://pypi.org/project/adbc-driver-snowflake/
- [W16] COPY INTO. https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
- [W17] Horizon Catalog. https://docs.snowflake.com/en/user-guide/snowflake-horizon
- [W18] Sensitive data classification. https://docs.snowflake.com/en/user-guide/classify-intro
- [W19] Data lineage. https://docs.snowflake.com/en/user-guide/ui-snowsight-lineage
- [W20] Snowflake-managed MCP server. https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp

**Catalogs**
- [K1] Atlan, manage Snowflake tags. https://docs.atlan.com/apps/connectors/data-warehouses/snowflake/how-tos/manage-snowflake-tags
- [K2] Atlan, data policy. https://docs.atlan.com/product/capabilities/governance/access-control/data-policy
- [K3] Atlan, what are data products. https://docs.atlan.com/product/capabilities/data-products/concepts/what-are-data-products
- [K4] Alation, Policy Center. https://docs.alation.com/en/latest/steward/PolicyCenter/index.html
- [K5] Alation, discover lineage. https://docs.alation.com/en/latest/analyst/Lineage/index.html
- [K6] Collibra Protect, asset data protection. https://productresources.collibra.com/docs/collibra/latest/Content/Protect/co_asset-data-protection.htm (Cloudflare refused our fetch; claims citing it are tagged inferred)
- [K7] Collibra, data classification. https://productresources.collibra.com/docs/collibra/latest/Content/Catalog/DataClassification/UnifiedDataClassification/co_about-data-classification.htm (same)

**Semantic layers**
- [M1] dbt Semantic Layer JDBC API. https://docs.getdbt.com/docs/dbt-cloud-apis/sl-jdbc
- [M2] dbt Semantic Layer GraphQL API. https://docs.getdbt.com/docs/dbt-cloud-apis/sl-graphql
- [M3] Cube access policies. https://docs.cube.dev/docs/data-modeling/data-access-policies
- [M4] Cube SQL API. https://docs.cube.dev/reference/core-data-apis/sql-api
- [M5] Cube multitenancy. https://docs.cube.dev/embedding/multitenancy
- [M6] Cube MCP server. https://docs.cube.dev/docs/integrations/mcp-server
- [M7] AtScale, connect with BI tools. https://documentation.atscale.com/container/connect-integrate/connect-with-bi-tools
- [M8] AtScale, using DAX tabular with Power BI. https://documentation.atscale.com/container/connect-integrate/connect-with-bi-tools/microsoft-power-bi/using-dax-tabular
- [M9] AtScale, row security objects. https://documentation.atscale.com/container/creating-and-sharing-cubes/creating-cubes/modeling-row-security-objects
- [M10] Cube caching and pre-aggregations. https://docs.cube.dev/docs/pre-aggregations
- [M11] Cube Access Policies viewer. https://docs.cube.dev/docs/data-modeling/access-policies-viewer

**Hasura and PostgREST**
- [H1] Permissions. https://hasura.io/docs/2.0/auth/authorization/permissions/
- [H2] JWT authentication. https://hasura.io/docs/2.0/auth/authentication/jwt/
- [H3] Firebase integration. https://hasura.io/learn/graphql/hasura-authentication/integrations/firebase/
- [H4] Caching, how the cache key is computed. https://hasura.io/docs/2.0/caching/how-it-works/
- [H5] Live queries architecture. https://github.com/hasura/graphql-engine/blob/master/architecture/live-queries.md
- [H6] Streaming subscriptions. https://hasura.io/docs/2.0/subscriptions/postgres/streaming/index/
- [H7] Allow list, role-based. https://hasura.io/docs/2.0/security/allow-list/
- [H8] Issue 273, persisted queries. https://github.com/hasura/graphql-engine/issues/273
- [H9] RESTified endpoints. https://hasura.io/docs/2.0/restified/overview/
- [H10] OpenTelemetry. https://hasura.io/docs/2.0/observability/opentelemetry/
- [H11] Event triggers. https://hasura.io/docs/2.0/event-triggers/overview/
- [H12] Connector Hub. https://hasura.io/connectors
- [H13] Docker deployment. https://hasura.io/docs/2.0/deployment/deployment-guides/docker/
- [H14] run_sql. https://hasura.io/docs/2.0/api-reference/schema-api/run-sql/
- [H15] Explain API. https://hasura.io/docs/2.0/api-reference/explain/
- [H16] DDN permissions. https://hasura.io/docs/3.0/auth/permissions/
- [H17] DDN engine `jsonapi` crate. https://github.com/hasura/graphql-engine/tree/master/v3/crates/jsonapi
- [P1] PostgREST, database authorization. https://raw.githubusercontent.com/PostgREST/postgrest-docs/main/docs/explanations/db_authz.rst (docs.postgrest.org returned HTTP 429)
