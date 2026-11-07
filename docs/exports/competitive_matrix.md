# Provisa Competitive Position Matrix

Provisa's side of every judgement comes from `docs/arch/requirements.yaml` and the code. A claim tagged `[tool-verified]` was checked against a requirement's status and description, a line of code, or a competitor's public documentation listed in [Sources](#6-sources). A claim tagged `[inferred]` rests on general product knowledge, or on a feature's absence from the documentation we read. Confirm those before quoting them to a buyer.

Last honesty pass: 2026-11-07. Every row was re-judged against each competitor. Before that pass, 58 rows claimed a lead over all five. Many of those claims fell apart on contact with the competitors' own documentation. Others described features Provisa has since removed.

---

## 1. Summary Table

Capability areas as rows. Competitors as columns. The Notes column points to the row's entry in [section 2](#2-row-notes), where every cell that is not **A** has its reason.

| Mark | Meaning |
| --- | --- |
| **A** | Provisa has the capability and the competitor does not document an equivalent. |
| P | Parity. The competitor reaches the same outcome, sometimes by a different mechanism. |
| **G** | Gap. The competitor has the capability and Provisa does not, or Provisa is behind. |
| — | Not applicable. The row describes a property of a feature the competitor lacks, or a use case it does not target. |

In the Snowflake/BigQuery column, the verdict is against whichever of the two is stronger on that row.

| Capability Area | Trino/Presto | Hasura | Denodo | Dremio | Snowflake/BigQuery | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Compile-time governance enforcement | P | P | P | P | P | N1 |
| Two-stage compiler (standalone Stage 2) | **A** | **A** | **A** | **A** | **A** | N2 |
| Default-invisible table registration | **A** | P | P | P | P | N3 |
| Business-model schema (domain/alias) | **A** | P | P | P | P | N4 |
| Five-tier relationship discovery | **A** | **A** | **A** | **A** | **A** | N5 |
| AI-suggested relationships | **A** | **A** | P | **A** | P | N6 |
| Pluggable approval hook (HTTP/gRPC/Unix socket) | P | **A** | P | P | **A** | N7 |
| Column masking (4 types + predicate guard) | P | **A** | **A** | P | P | N8 |
| Domain-scoped field grants | P | **A** | P | P | P | N9 |
| Append-only DB-enforced audit log | **A** | **A** | **A** | **A** | P | N10 |
| Firebase auth | P | P | — | — | — | N11 |
| Dual role resolution (header/JWT) | P | P | P | P | P | N12 |
| Server-assigned roles (no client override) | P | P | P | P | P | N13 |
| Neo4j/SPARQL connectors | **A** | **A** | **A** | **A** | **A** | N14 |
| WebSocket/RSS/push receiver sources | **A** | **A** | P | **A** | P | N15 |
| OpenAPI full auto-discovery | **A** | P | P | **A** | **A** | N16 |
| gRPC source connector | **A** | **A** | **A** | **A** | **A** | N17 |
| Vector search with model registry | **A** | **A** | P | **A** | P | N18 |
| Hot table VALUES-CTE optimization | **A** | — | P | P | — | N19 |
| Engine-agnostic hint syntax (/*+ */) | **A** | — | P | **A** | — | N20 |
| Security-partitioned cache keys | — | P | P | P | P | N21 |
| Tenant-prefixed cache isolation | — | — | — | — | P | N22 |
| Normalized tabular output (FK-preserving) | **A** | **A** | **A** | **A** | **A** | N23 |
| Arrow Flight (cross-source, governed) | **A** | **A** | P | P | P | N24 |
| LIMIT threshold+1 redirect probe | P | **A** | **A** | **A** | P | N25 |
| Client-controlled redirect headers | P | **A** | **A** | **A** | **A** | N26 |
| pgwire PostgreSQL wire protocol | **A** | **A** | P | **A** | **A** | N27 |
| pg_catalog role-scoped intercept | — | — | P | — | — | N28 |
| DDL dispatch (Trino vs. direct path) | **G** | **G** | **G** | **G** | **G** | N29 |
| DDL capability gating over pgwire | — | — | — | — | — | N30 |
| COPY TO/FROM STDIN with RLS | **A** | **A** | **A** | P | P | N31 |
| Domain-driven DDL target resolution | — | — | — | — | — | N30 |
| Immediate post-DDL schema registration | — | — | — | — | — | N30 |
| ADBC client over Arrow Flight | **A** | **A** | P | P | P | N32 |
| Governed JDBC (no ungoverned path) | P | — | P | P | P | N33 |
| JDBC transport via Arrow Flight | **A** | — | P | P | P | N34 |
| JDBC executeQuery through Stage 2 | P | — | P | P | P | N33 |
| getColumns() filtered by role visibility | P | P | P | P | P | N35 |
| Per-call GraphQL/SQL language selection | **A** | **A** | **A** | **A** | **A** | N36 |
| Compile endpoint (/data/compile) | **A** | P | **A** | **A** | **A** | N37 |
| GraphiQL governed-SQL preview plugin | — | P | **A** | — | — | N38 |
| JSON:API compliant endpoints | **A** | **A** | **A** | **A** | **A** | N39 |
| SSE subscriptions (multi-source, governed) | **A** | **A** | **A** | **A** | **A** | N40 |
| x-provisa-kind OpenAPI extension | — | **A** | **A** | — | — | N41 |
| APQ governance-gated registration | — | P | — | — | — | N42 |
| gRPC per-RPC role via metadata | **A** | **A** | **A** | **A** | **A** | N43 |
| Kafka sinks with 4 trigger modes | **A** | **A** | P | **A** | P | N44 |
| Unified Live Query Engine (SSE + Kafka) | **A** | **A** | **A** | **A** | **A** | N45 |
| Multi-table subscription change detection | — | P | — | — | — | N46 |
| Creation-request queue | **A** | **A** | **A** | **A** | **A** | N47 |
| Composable fine-grained role capabilities | P | **A** | P | P | P | N48 |
| Test endpoint governance transparency | P | P | **A** | **A** | **A** | N49 |
| RLS Apply-To toggle (table vs. domain) | P | **A** | P | **A** | **A** | N50 |
| Graph UI (hull overlay, supernode collapse) | **A** | **A** | **A** | **A** | **A** | N51 |
| Single-executable installer (airgap) | P | **A** | P | P | **A** | N52 |
| Hasura v2 + DDN migration converters | — | — | — | — | — | N53 |
| Dual OTLP export with SQL redaction | **A** | **A** | **A** | **A** | — | N54 |
| Multi-tenancy schema-per-org | **A** | **A** | P | P | P | N55 |
| Encryption: pluggable client-owned CMK | — | — | P | P | P | N56 |
| JSON:API connector | — | — | — | — | — | N57 |
| Cypher write mutations | **A** | **A** | **A** | **A** | **A** | N58 |
| ADBC Flight port configurability | — | — | P | P | — | N59 |

Counts across the 310 cells:

| Mark | Before 2026-11-07 | After |
| --- | --- | --- |
| A | 294 | 135 |
| P | 2 | 106 |
| G | 9 | 5 |
| — | 5 | 64 |

---

## 2. Row Notes

Every non-A cell gets a line of reason, and so does the basis for each **A**. Source keys such as [T1] resolve in [section 6](#6-sources).

**N1. Compile-time governance enforcement.** Stage 2 rewrites every statement before it reaches a source. The entry protocol makes no difference. [tool-verified: REQ-001, REQ-263, REQ-266]
- Trino P: the engine applies row filters and column masks on every query [T1]. [tool-verified]
- Hasura P: row and column permissions are compiled into the generated SQL [H1]. [tool-verified]
- Denodo P: the server enforces row restrictions and global security policies for every client [D1]. [tool-verified]
- Dremio P: row-access and column-masking policies run as UDFs inside the engine [R4]. [tool-verified]
- Snowflake/BigQuery P: masking and row access policies apply at query runtime [W1][W2][W9][W10]. [tool-verified]

**N2. Two-stage compiler (standalone Stage 2).** Any SQL string plus a role goes in; governed SQL comes out. [tool-verified: REQ-262, REQ-263] No competitor offers a service that rewrites arbitrary client SQL this way. The nearest thing is Hasura's explain API, which returns the SQL Hasura generated for one of its own GraphQL operations [H15]. [inferred]

**N3. Default-invisible table registration.** Until a steward registers a table, nothing can query, browse or write it. [tool-verified: REQ-014]
- A basis (Trino): with no access control configured, Trino runs `allow-all` [T2]. [tool-verified]
- Hasura P: a table is exposed only once tracked, and a non-admin role sees nothing without permissions [H1]. [inferred: tracking behaviour]
- Denodo P: data is reachable only through base views someone creates [D4]. [inferred]
- Dremio P: Enterprise RBAC requires privileges before a user can query a dataset. [inferred]
- Snowflake/BigQuery P: a new object is visible only to its owner until access is granted. [inferred]

**N4. Business-model schema (domain/alias).** Domains and aliases give stewards a business-facing model over physical tables. [tool-verified: REQ-263]
- A basis (Trino): SQL views and schemas, no semantic layer. [inferred] Of all the A cells, this one is weakest. Trino users routinely build the same thing out of views.
- Hasura P: custom field names and DDN subgraphs shape a business-facing API. [inferred]
- Denodo P: derived and interface views are Denodo's core use case. [inferred]
- Dremio P: views organised in spaces, with wikis and tags. [inferred]
- Snowflake/BigQuery P: Snowflake semantic views model business entities, metrics and relationships [W7]. [tool-verified]

**N5. Five-tier relationship discovery.** Discovery is a governed workflow. It runs from raw inventory through unclaimed assets, domain assets and relationships to field grants, each stage gated on the one before. [tool-verified: REQ-612] None of the five documents anything staged like this. [inferred]

**N6. AI-suggested relationships.** On the relationships page, an LLM proposes candidates. [tool-verified: REQ-167]
- A basis (Trino, Hasura): neither offers AI suggestions of any kind for relationships. [inferred]
- A basis (Dremio): its AI agent's listed tasks cover exploration, analysis, visualisation and SQL tuning, with nothing on relationships [R8]. [tool-verified]
- Denodo P: Denodo Assistant's "Suggest Joins" proposes join conditions between views [D8]. [tool-verified]
- Snowflake/BigQuery P: Semantic View Autopilot generates logical tables, relationships and metrics [W8]. [tool-verified]

**N7. Pluggable approval hook.** Per-query ABAC travels over an HTTP webhook, a gRPC channel kept open with a circuit breaker, or a Unix socket. Earlier versions of this matrix listed "inline function" as a transport. No such transport exists in the code. [tool-verified: REQ-246, REQ-555, REQ-556; `provisa/auth/approval_hook.py` defines `WEBHOOK = "webhook"`, `GRPC = "grpc"`, `UNIX_SOCKET = "unix_socket"`]
- A basis (Hasura, Snowflake/BigQuery): neither calls out to an external service to authorize each query. [inferred]
- Trino P: its Open Policy Agent plugin sends each authorization decision to an external policy service [T3]. [tool-verified]
- Denodo P: custom policies can call external logic per query. [inferred]
- Dremio P: Apache Ranger can serve as Dremio's policy manager [R4]. [tool-verified]

**N8. Column masking (4 types + predicate guard).** Four mask types exist: `regex`, `constant`, `truncate` and `fake`. Earlier versions said six. Masked columns are refused in WHERE and HAVING. [tool-verified: REQ-531; `class MaskType` in `provisa/security/masking.py`]
- A basis (Hasura): column permissions allow or hide a column; nothing masks it. [inferred]
- A basis (Denodo): Denodo masks values but warns that users "may still be able to infer sensitive values by crafting selective WHERE conditions" [D1]. [tool-verified]
- Trino P: column masks are arbitrary SQL expressions [T1]. Masks apply wherever the column is read, so a WHERE clause sees only the masked value. [tool-verified: masks; inferred: predicate behaviour]
- Dremio P: column-masking UDF policies, with the same predicate behaviour [R4]. [tool-verified: masks; inferred: predicate behaviour]
- Snowflake/BigQuery P: Snowflake applies a masking policy "at every location where the column appears" [W1]. BigQuery ships more built-in mask rules than Provisa, SHA-256 and email masks among them [W9]. [tool-verified]

**N9. Domain-scoped field grants.** One grant can cover a whole domain. [tool-verified: REQ-402]
- A basis (Hasura): permissions are set per table and per role. [inferred]
- Trino P: rules match catalogs, schemas and tables by regex, so one rule covers a group of tables [T1]. [tool-verified]
- Denodo P: a global security policy applies to every view or column carrying a tag [D1]. [tool-verified]
- Dremio P: privileges can be granted on spaces and folders. [inferred]
- Snowflake/BigQuery P: Snowflake tag-based masking policies cover every tagged column [W1]. [tool-verified]

**N10. Append-only DB-enforced audit log.** PostgreSQL rules turn UPDATE and DELETE on `query_audit_log` into no-ops. [tool-verified: REQ-596; `provisa/audit/query_log.py` runs `CREATE RULE no_delete_audit AS ON DELETE TO query_audit_log DO INSTEAD NOTHING;` and the matching `no_update_audit`]
- A basis (Trino, Hasura, Denodo, Dremio): query logs go to files or event streams, which the database does nothing to protect. [inferred]
- Snowflake/BigQuery P: Snowflake Access History records reads and writes down to the column [W3], in a Snowflake-managed view that users cannot alter. [tool-verified: Access History; inferred: immutability]

**N11. Firebase auth.** The firebase-admin SDK validates Firebase ID tokens. [tool-verified: REQ-121; `provisa/auth/providers/firebase.py` imports `firebase_admin`]
- Trino P: JWT authentication loads keys from any JWKS endpoint [T6], and Firebase publishes one. [tool-verified: JWKS; inferred: Firebase works]
- Hasura P: Hasura documents a Firebase integration [H3]. [tool-verified]
- Denodo, Dremio —: neither targets sign-in for an application's end users. [inferred]
- Snowflake/BigQuery —: a warehouse does not authenticate an application's end users.

**N12. Dual role resolution (header/JWT).** The acting role comes from the `X-Provisa-Role` header or from the token. [tool-verified: `provisa/auth/middleware.py`]
- Trino P: `SET ROLE` enables a role for the session [T5]. [tool-verified]
- Hasura P: the `x-hasura-role` header, or the JWT's `x-hasura-default-role` when the header is absent [H2]. [tool-verified]
- Snowflake/BigQuery P: Snowflake takes a default role from the user and switches with `USE ROLE`. [inferred]
- Denodo and Dremio P: both map roles from the identity provider. [inferred]

**N13. Server-assigned roles (no client override).** Clients may ask for a role. The code comment states the rule: "the server honors it only when the authenticated user is actually assigned that role — a bare client-supplied role is never trusted". When no auth provider is configured, the header is taken at face value. [tool-verified: REQ-273; `provisa/auth/middleware.py`]
- Trino P: `SET ROLE` succeeds only for a role the user holds [T5]. [tool-verified]
- Hasura P: `x-hasura-role` must appear in the token's `x-hasura-allowed-roles` [H2]. [tool-verified]
- Snowflake/BigQuery P: `USE ROLE` fails for a role never granted. [inferred]
- Denodo and Dremio P: privileges follow the authenticated user, with no documented way to claim an ungranted role. [inferred]

**N14. Neo4j/SPARQL connectors.** [tool-verified: REQ-295, REQ-297] Neither appears in Trino's connector list [T4] or Dremio's source list [R5], and Hasura lists Neo4j as "Coming Soon" [H12]. [tool-verified] Denodo has no native connector for either, though a Neo4j JDBC driver would work as a stopgap. [inferred] As for the warehouses, Snowflake federates nothing at query time, and BigQuery federates only Google's own databases [W14]. [tool-verified]

**N15. WebSocket/RSS/push receiver sources.** [tool-verified: REQ-331, REQ-338]
- A basis: Trino's only streaming source is Kafka [T4]. Hasura and Dremio have no streaming or push sources at all. [inferred]
- Denodo P: Kafka listeners receive events [D7], and an RSS feed reads fine as an XML source. It has no WebSocket source. [tool-verified: Kafka; inferred: RSS, WebSocket]
- Snowflake/BigQuery P: Snowpipe Streaming accepts pushed rows, as does the BigQuery Storage Write API. [inferred]

**N16. OpenAPI full auto-discovery.** [tool-verified: REQ-314, REQ-319]
- A basis: Trino's connector list [T4] and Dremio's source list [R5] have no OpenAPI source. The warehouses have none either. [tool-verified: Trino, Dremio; inferred: warehouses]
- Hasura P: DDN's OpenAPI Lambda connector [H12]. [tool-verified]
- Denodo P: JSON sources create base views "From OpenAPI 3 document" [D4]. [tool-verified]

**N17. gRPC source connector.** [tool-verified: REQ-322] Trino has a Thrift connector [T4]. Nobody lists gRPC [H12][R5]. [tool-verified: Trino, Hasura, Dremio; inferred: Denodo, warehouses]

**N18. Vector search with model registry.** [tool-verified: REQ-419, REQ-420, REQ-426]
- A basis: Hasura reaches vector search through a Qdrant connector [H12], with embeddings made elsewhere and no model registry. Trino and Dremio have no model registry either. [tool-verified: Hasura connector; inferred: the rest]
- Denodo P: VECTOR types, distance functions and `EMBED_AI()` [D5]. [tool-verified]
- Snowflake/BigQuery P: Snowflake vector embeddings [W6]; BigQuery `AI.GENERATE_EMBEDDING` with vector search [W12]. [tool-verified]

**N19. Hot table VALUES-CTE optimization.** Small tables sit in Redis and are inlined as a VALUES CTE. [tool-verified: REQ-230]
- A basis (Trino): open-source Trino has no table cache. [inferred]
- Hasura —: it plans no cross-source joins that would need a local copy.
- Denodo P: the cache module can materialise a view in full. [inferred]
- Dremio P: reflections materialise small tables. [inferred]
- Snowflake/BigQuery —: the data already lives in the warehouse.

**N20. Engine-agnostic hint syntax.** `/*+ */` hints map onto Trino session properties. [tool-verified: REQ-279]
- A basis (Trino, Dremio): neither accepts inline optimizer hints; Trino relies on session properties. [inferred]
- Hasura —: no SQL surface to carry a hint.
- Denodo P: VQL takes execution hints in its CONTEXT clause. [inferred]
- Snowflake/BigQuery —: a single-engine warehouse has no federation engine to abstract.

**N21. Security-partitioned cache keys.** The key covers the governed SQL, its bound values and the role. [tool-verified: REQ-544; `provisa/cache/key.py`]
- Trino —: open-source Trino has no result cache. [inferred]
- Hasura P: its cache key hashes "the role and session variables used in the permissions of the query" [H4]. [tool-verified]
- Denodo P: security rules apply on top of cached views at query time. [inferred]
- Dremio P: the results cache skips any query using `QUERY_USER` or `IS_MEMBER` [R6]. Dremio declines to cache what Provisa partitions. Either way, no result leaks between users. [tool-verified]
- Snowflake/BigQuery P: a cached result is reused only if "the role accessing the cached results has the required privileges" [W4]. [tool-verified]

**N22. Tenant-prefixed cache isolation.** Earlier versions called this a design gap. It is wired now: the cache middleware passes `tenant_id=org_id` to the store. [tool-verified: REQ-595 complete; `provisa/cache/middleware.py` calls `store.get(key, tenant_id=org_id)`]
- Trino, Hasura, Denodo, Dremio —: none runs a result cache shared across tenants. [inferred]
- Snowflake/BigQuery P: each account or project keeps its own result cache. [inferred]

**N23. Normalized tabular output (FK-preserving).** [tool-verified: REQ-049] Each competitor hands back one result per query, flat or nested. Several related tables from a single query is something only Provisa documents. [inferred]

**N24. Arrow Flight (cross-source, governed).** [tool-verified: REQ-143]
- A basis (Trino): its client protocol is HTTP, in direct or spooling mode [T9]. [tool-verified]
- A basis (Hasura): GraphQL and REST only. [inferred]
- Denodo P: Arrow Flight SQL on port 9994, over every virtualised source [D3]. [tool-verified]
- Dremio P: a Flight endpoint on port 32010, plus Flight SQL [R1][R2], over all Dremio sources including federated ones. [tool-verified]
- Snowflake/BigQuery P: BigQuery's Storage Read API streams Arrow [W11]. It is not Flight. [tool-verified]

**N25. LIMIT threshold+1 redirect probe.** [tool-verified: REQ-140]
- A basis: Hasura returns every result inline. Denodo and Dremio stream results to the client with no automatic redirect to object storage. [inferred]
- Trino P: its spooling protocol writes large results to object storage for the client to fetch [T9]. [tool-verified]
- Snowflake/BigQuery P: large results arrive as chunks from cloud storage. [inferred]

**N26. Client-controlled redirect headers.** [tool-verified: REQ-137]
- A basis: outside Trino, no competitor lets a single request choose how its results are delivered. [inferred]
- Trino P: clients "actively request usage of the spooling protocol" [T9]. Provisa adds a per-request threshold and format on top. [tool-verified]

**N27. pgwire PostgreSQL wire protocol.** [tool-verified: REQ-527]
- A basis: of the five, only Denodo has a PostgreSQL-protocol endpoint. [inferred for the others]
- Denodo P: "The ODBC interface of Denodo follows the same protocol as PostgreSQL", on port 9996 [D2]. [tool-verified]

**N28. pg_catalog role-scoped intercept.** [tool-verified: REQ-532]
- — wherever there is no PostgreSQL-protocol endpoint. With no pg_catalog, there is nothing to scope.
- Denodo P: its PostgreSQL-protocol interface answers catalog queries within the user's privileges. [inferred]

**N29. DDL dispatch.** Provisa no longer accepts DDL. REQ-1926 says "CREATE, DROP and ALTER are refused on every surface", and pgwire answers with SQLSTATE 0A000. The requirements this row once described, REQ-582 to REQ-584, are superseded. [tool-verified]
- Trino G: `CREATE TABLE` through connectors [T8]. [tool-verified]
- Hasura G: admins can run arbitrary SQL, DDL included, through `run_sql` [H14]. [tool-verified]
- Denodo G: remote tables create tables in an external database [D6]. [tool-verified]
- Dremio G: `CREATE TABLE` [R7]. [tool-verified]
- Snowflake/BigQuery G: native DDL. [inferred]

The refusal is deliberate. Provisa defines objects in the model and does not create them through a query protocol. Expect buyers to notice this gap first.

**N30. DDL capability gating, domain-driven DDL target, post-DDL registration.** Every cell on these three rows is —, because the features left with client DDL. REQ-1926: "There is no `ddl` capability, no `ddl_catalog` and no `ddl_schema`". See N29. [tool-verified]

**N31. COPY TO/FROM STDIN with RLS.** [tool-verified: REQ-585; `provisa/pgwire/copy_handler.py`]
- A basis (Trino, Hasura, Denodo): none offers bulk COPY over a client connection. [inferred]
- Dremio P: `COPY INTO` loads CSV, JSON or Parquet files from a source into an Iceberg table [R9]. The files come from storage. [tool-verified]
- Snowflake/BigQuery P: `COPY INTO` loads staged files [W16]. [tool-verified]

**N32. ADBC client over Arrow Flight.** [tool-verified: REQ-271]
- A basis (Trino, Hasura): neither has an ADBC driver. [inferred]
- Denodo P: its docs show `adbc-driver-flightsql` connecting to Virtual DataPort [D3]. [tool-verified]
- Dremio P: the generic ADBC Flight SQL driver should work against Dremio's Flight SQL endpoint [R2]. [tool-verified: endpoint; inferred: ADBC]
- Snowflake/BigQuery P: Apache Arrow publishes `adbc-driver-snowflake` and `adbc-driver-bigquery` [W15]. Neither uses Flight. [tool-verified]

**N33. Governed JDBC, and JDBC executeQuery through Stage 2.**
- P wherever there is a JDBC driver. Each engine applies its own access policies to JDBC queries [T1][D1][R4][W1]. [tool-verified: policies; inferred: JDBC path]
- Hasura —: no JDBC interface.

**N34. JDBC transport via Arrow Flight.** [tool-verified: REQ-293]
- A basis (Trino): its JDBC driver speaks Trino's HTTP protocol [T9]. [tool-verified]
- Hasura —: no JDBC interface.
- Denodo P: a Flight SQL JDBC driver, URL `jdbc:arrow-flight-sql://`, port 9994 [D3]. [tool-verified]
- Dremio P: the Arrow Flight SQL JDBC driver [R3]. [tool-verified]
- Snowflake/BigQuery P: Snowflake's JDBC driver fetches results in Arrow format over HTTPS. [inferred]

**N35. getColumns() filtered by role visibility.** [tool-verified: REQ-128]
- Trino P: a table shows in SHOW only "if the user has any permissions on the table" [T1]. Column-level filtering of metadata is our inference. [tool-verified: tables; inferred: columns]
- Hasura P: each role's GraphQL schema shows only the columns it may read [H1]. [inferred]
- Snowflake/BigQuery P: INFORMATION_SCHEMA lists only objects the role can access. [inferred]
- Denodo and Dremio P: catalog listings follow privileges. [inferred]

**N36. Per-call GraphQL/SQL language selection.** One connection, either language, chosen per call. [tool-verified: REQ-274] Denodo comes closest by serving GraphQL and SQL on separate interfaces [D9]. Every other competitor has one language per interface. [inferred]

**N37. Compile endpoint (/data/compile).** Governed SQL back, nothing executed. [tool-verified: REQ-161]
- A basis: for SQL engines, EXPLAIN returns an execution plan. It does not hand back the governed SQL. [inferred]
- Hasura P: given a query and a role, the explain API returns the "generated SQL for the operation" [H15]. [tool-verified]

**N38. GraphiQL governed-SQL preview plugin.** [tool-verified: REQ-163]
- — for every competitor without a GraphQL interface.
- Hasura P: the console's GraphiQL can show generated SQL, backed by the explain API [H15]. [tool-verified: API; inferred: console]
- A basis (Denodo): it has a GraphQL service [D9] but documents no SQL preview. [inferred]

**N39. JSON:API compliant endpoints.** [tool-verified: REQ-257] Hasura generates REST endpoints with OpenAPI docs [H9]. That is not JSON:API, and no other competitor documents JSON:API output. [tool-verified: Hasura; inferred: the rest]

**N40. SSE subscriptions (multi-source, governed).** [tool-verified: REQ-282]
- A basis (Hasura): its live and streaming subscriptions are governed but run over WebSocket against one database [H5][H6]. A subscription spanning sources is not documented. [tool-verified: subscriptions; inferred: single database]
- A basis (the rest): nothing pushes live query results to clients. Denodo's Kafka listeners publish results to Kafka topics [D7]. [inferred]

**N41. x-provisa-kind OpenAPI extension.** It marks an OpenAPI operation as query or mutation. [tool-verified: REQ-408]
- — for competitors with no OpenAPI source.
- A basis (Hasura, Denodo): both import OpenAPI [H12][D4]. Neither documents a per-operation override. [inferred]

**N42. APQ governance-gated registration.** [tool-verified: REQ-291]
- Hasura P: persisted queries are an Enterprise feature [H8], and role-based allow lists restrict which operations each role may run [H7]. The old claim that Hasura registers APQ hashes regardless of role had no source, so we dropped it. [tool-verified]
- — elsewhere. There is no persisted-query mechanism to gate. [inferred for Denodo's GraphQL service]

**N43. gRPC per-RPC role via metadata.** [tool-verified: REQ-617] Dremio's Flight endpoint runs on gRPC, which makes it the closest. It still has no per-role service definitions, and no competitor generates a per-role gRPC data API. [inferred]

**N44. Kafka sinks with 4 trigger modes.** [tool-verified: REQ-286]
- A basis: Hasura event triggers call webhooks on row changes [H11]. Trino's Kafka connector [T4] has no trigger concept, and Dremio has neither. [tool-verified: Hasura; inferred: Trino, Dremio]
- Denodo P: a Kafka listener runs a query per event and can publish the result to a Kafka topic [D7]. [tool-verified]
- Snowflake/BigQuery P: BigQuery continuous queries export results to Pub/Sub, which plays the part Kafka plays here. [inferred]

**N45. Unified Live Query Engine (SSE + Kafka).** A single poll engine drives both outputs, each with its own watermark. [tool-verified: REQ-282, REQ-286]
- A basis (Hasura): live queries and event triggers are separate features [H5][H11]. [tool-verified]
- A basis (the rest): no live delivery to clients. [inferred]

**N46. Multi-table subscription change detection.** [tool-verified: REQ-567]
- Hasura P: live queries refetch the whole query on an interval [H5], so a change to any joined table shows up. Earlier versions claimed Hasura watches a single table. Its own architecture document says otherwise. [tool-verified]
- — elsewhere: no client subscriptions.

**N47. Creation-request queue.** [tool-verified: REQ-063, REQ-434] When a user lacks the authority to create a view or relationship, Provisa queues a request for someone who has it. We saw nothing comparable in any of the five. [inferred]

**N48. Composable fine-grained role capabilities.** [tool-verified: REQ-063]
- A basis (Hasura): admin access is all or nothing. [inferred]
- Trino P: roles can be granted to roles, and file rules cover system operations [T1][T5]. [tool-verified]
- Snowflake/BigQuery P: Snowflake roles nest in hierarchies and carry privileges as narrow as a single object. [inferred]
- Denodo and Dremio P: both grant privileges per object and per role. [inferred]

**N49. Test endpoint governance transparency.** The response shows which RLS filters applied and which columns were excluded. [tool-verified: REQ-062]
- A basis: in Denodo, Dremio and the warehouses, policies apply silently. [inferred]
- Trino P: row-filter predicates are inlined in the plan, so EXPLAIN shows them. [inferred]
- Hasura P: the explain API shows the generated SQL, permission filters included [H15]. [tool-verified]

**N50. RLS Apply-To toggle (table vs. domain).** [tool-verified: REQ-402, REQ-404]
- A basis: elsewhere, row policies attach one table at a time. [inferred]
- Trino P: one rule's `filter` applies to every table its regex matches [T1]. [tool-verified]
- Denodo P: a global policy filters rows in every view carrying a tag [D1]. [tool-verified]

**N51. Graph UI (hull overlay, supernode collapse).** [tool-verified: REQ-648] Dremio's lineage view draws a graph, and Hasura DDN's console may too. Neither groups nodes into hulls or collapses them. [inferred]

**N52. Single-executable installer (airgap).** [tool-verified: REQ-223, REQ-294]
- A basis (Hasura): it deploys as Docker or Kubernetes images [H13]. [tool-verified]
- A basis (Snowflake/BigQuery): managed cloud services; no self-hosted option. [inferred]
- Denodo P: its installer runs offline. [inferred]
- Trino and Dremio P: tarballs and RPMs that run offline, given a separately installed JVM. [inferred]

**N53. Hasura v2 + DDN migration converters.** [tool-verified: REQ-182, REQ-183] Every cell is —. The converters import Hasura configuration. Hasura is the system being left, and the other four do not compete for Hasura users.

**N54. Dual OTLP export with SQL redaction.** [tool-verified: REQ-545, REQ-547]
- A basis (Trino, Hasura): each exports OpenTelemetry to one endpoint [T7][H10]. There is no second path and no literal redaction. [tool-verified: export; inferred: no redaction]
- A basis (Denodo, Dremio): monitoring runs through JMX and logs, without OTLP export. [inferred]
- Snowflake/BigQuery —: the operator does not run the service's telemetry.

**N55. Multi-tenancy schema-per-org.** Earlier versions marked this "(proposed)". It is complete: each org gets its own `org_<org_id>` schema and PostgreSQL role. [tool-verified: REQ-695, REQ-697, REQ-699]
- A basis (Trino, Hasura): one deployment serves one metadata set. [inferred]
- Denodo P: virtual databases separate tenants. [inferred]
- Dremio P: Dremio Cloud organisations and projects. [inferred]
- Snowflake/BigQuery P: accounts and projects isolate tenants. [inferred]

**N56. Encryption: pluggable client-owned CMK.** [tool-verified: REQ-684, REQ-694]
- Trino, Hasura —: neither stores customer data at rest.
- Denodo and Dremio P: caches and reflections live in storage the customer controls and encrypts. [inferred]
- Snowflake/BigQuery P: Snowflake Tri-Secret Secure [W5]; BigQuery customer-managed Cloud KMS keys [W13]. [tool-verified]

**N57. JSON:API connector.** Every cell is —, because no requirement backs this row any more. REQ-656 to REQ-660, which earlier versions cited, are gone from `docs/arch/requirements.yaml`, and the code has no JSON:API source type. Provisa serves JSON:API (N39). It does not consume it. [tool-verified]

**N58. Cypher write mutations.** Earlier versions called this a gap. Cypher CREATE, SET and DELETE now translate to SQL INSERT, UPDATE and DELETE on registered tables, returning affected rows. Relationships stay unwritable by design (REQ-665). Writes go to registered SQL tables. [tool-verified: REQ-661 and REQ-664 to REQ-670 complete; `provisa/cypher/write_translator.py`] None of the five accepts Cypher at all. [inferred]

**N59. ADBC Flight port configurability.** Earlier versions called this a gap. `adbc_connect()` now takes `port: int = 8815`. [tool-verified: REQ-711; `provisa-client/provisa_client/adbc.py`]
- — where there is no Flight endpoint.
- Denodo P: the Flight SQL URL takes a port [D3]. [tool-verified]
- Dremio P: the Flight endpoint's port is configurable [R1]. [tool-verified]

---

## 3. Top Differentiators

The leads below hold against every competitor, or nearly every one. Evidence is in section 2.

**A standalone governance transformer.** Stage 2 takes any SQL string and a role and returns governed SQL. GraphQL, the SQL endpoint, JDBC, DB-API, SQLAlchemy, pgwire and Flight all pass through it. Competitors enforce governance inside their own engines, and none exposes an equivalent service (N2). [tool-verified: REQ-262, REQ-263, REQ-266]

**Sources beyond SQL.** Provisa federates Neo4j, SPARQL triplestores, gRPC services with proto auto-generation, OpenAPI endpoints, WebSocket streams, RSS feeds and an HTTP push receiver. Denodo and Hasura DDN match it on OpenAPI. No competitor we checked has a graph-database or gRPC source (N14, N17). [tool-verified: REQ-295, REQ-297, REQ-314, REQ-322, REQ-331, REQ-338]

**Normalized tabular output.** From one federated query, several related tables with foreign keys preserved, written as Parquet or CSV (N23). [tool-verified: REQ-049]

**Live delivery across sources.** SSE subscriptions and Kafka sinks share one poll engine with independent watermarks. Hasura comes closest. Its subscriptions run against one database, and its event triggers are a separate system (N40, N45). [tool-verified: REQ-282, REQ-286]

**Many client protocols behind one governance path.** Provisa speaks pgwire, Arrow Flight, ADBC, JDBC over Flight, GraphQL, JSON:API and a per-role gRPC API, all through the same governance. Denodo is the nearest rival, with a PostgreSQL-protocol interface and Flight SQL. We found no JSON:API output or gRPC API in Denodo (N24, N27, N32, N39, N43). [inferred: the combination]

---

## 4. Honest Gaps

### Client DDL is refused [tool-verified]

Provisa refuses CREATE, ALTER and DROP on every surface (REQ-1926). All five competitors accept DDL from a client in some form. Trino and Dremio take `CREATE TABLE`. Hasura admins can send DDL through `run_sql`, and Denodo creates tables in external databases through remote tables. The warehouses have native DDL.

Provisa defines objects in the model and admits them from sources. A team that wants to create tables from a SQL client will need another tool. See N29.

### Governance is parity [tool-verified]

Every competitor we checked has row filters, column masking, role resolution and role switching. Trino has file-based and OPA access control. Snowflake and BigQuery have masking and row access policies. Denodo has tag-driven global policies, Dremio UDF policies. Where Provisa does lead, the lead is narrower. It comes from uniform enforcement across protocols and sources, and from the predicate guard that Denodo lacks (N8). See also N1, N12 and N13.

### Denodo matches Arrow Flight and pgwire [tool-verified]

Denodo 9.5 offers Arrow Flight SQL with JDBC and Python ADBC drivers, and its ODBC interface speaks the PostgreSQL protocol. Dremio offers Flight and Flight SQL as well. See N24, N27 and N32.

### Earlier gaps now closed [tool-verified]

- **Cypher writes.** Implemented; relationship writes remain refused by design (N58).
- **ADBC Flight port.** `adbc_connect()` takes a `port` argument (N59).
- **Tenant cache prefix.** The cache middleware passes `tenant_id` to the store (N22).

### A row with no requirement [tool-verified]

Nothing backs the JSON:API connector row (N57). It stays until the matrix rewrite decides whether to drop it.

---

## 5. By Competitor

### Trino / Presto

Trino is Provisa's federation engine. The comparison applies when buyers consider using Trino directly.

Its governance is real. File-based rules carry row filters and column masks [T1]. Once rules exist, anything unmatched is denied, and SHOW hides any table the user holds no permission on. An Open Policy Agent plugin moves decisions to an external service [T3]. With nothing configured, though, Trino runs `allow-all` [T2]. That default is why Provisa keeps its lead on default-invisible registration.

Trino speaks only its own HTTP client protocol [T9]. An optional spooling mode parks large results in object storage, which matches Provisa's large-result redirect. The `@provisa` hint vocabulary hides Trino session property names from query authors. [tool-verified: REQ-279]

On DDL, Trino leads. `CREATE TABLE` works through any connector that supports it [T8].

### Hasura

Hasura is the closest architectural neighbour. Both put a GraphQL layer with role-based permissions over data sources.

Four Hasura features match Provisa directly. It compiles row and column permissions into its generated SQL [H1]. It resolves roles from a header or JWT, checked against allowed roles [H2]. Its cache key includes the role and session variables [H4]. And its explain API returns the generated SQL for a given role [H15].

Live queries refetch the whole query on an interval, so changes to joined tables are seen [H5]. Event triggers are a separate feature that calls webhooks [H11]. Provisa's lead here is narrower than earlier versions claimed. It covers multi-source subscriptions and the shared engine for SSE and Kafka.

Provisa clearly leads on client protocols. Hasura has no pgwire, Flight, ADBC or JDBC interface. Provisa also leads on column masking and on non-SQL sources, since Hasura DDN lists Neo4j as coming soon and has no gRPC or SPARQL connector [H12]. Hasura deploys as container images [H13].

### Denodo

Denodo is a data virtualization platform. On protocols and governance, it is the strongest match for Provisa.

Denodo enforces row restrictions and tag-driven global security policies, masking included [D1]. Its documentation concedes that masking does not stop inference through WHERE conditions. Provisa refuses masked columns in WHERE and HAVING, which is its clearest governance lead over Denodo. [tool-verified: REQ-531]

The parity list is long. Denodo's ODBC interface follows the PostgreSQL protocol [D2]. Version 9.5 adds Arrow Flight SQL with JDBC and Python ADBC drivers [D3]. It builds base views from OpenAPI 3 documents [D4], has vector types with `EMBED_AI()` [D5], suggests joins with an LLM [D8], and runs Kafka listeners that can publish query results [D7].

Provisa leads on Neo4j, SPARQL and gRPC sources, SSE subscriptions, normalized output and the governance-transparent test endpoint. Denodo leads on DDL, through remote tables [D6].

### Dremio

Dremio is a lakehouse query engine that also federates relational and NoSQL sources [R5].

Across all its sources, Dremio exposes Arrow Flight on port 32010 and Flight SQL [R1][R2], with an Arrow Flight SQL JDBC driver [R3]. So Flight, ADBC and JDBC over Flight are parity. Row-access and column-masking policies are UDFs, and Apache Ranger can take over as policy manager [R4]. The results cache declines queries that call `QUERY_USER` or `IS_MEMBER` [R6].

Provisa leads on the PostgreSQL protocol, non-SQL sources, subscriptions and AI relationship suggestion, which Dremio's AI agent does not list [R8]. Dremio leads on DDL [R7].

### Snowflake / BigQuery

Snowflake and BigQuery are managed cloud data warehouses. The comparison applies when buyers consider them as the governed query layer.

On governance they match Provisa. Snowflake masking policies apply wherever a column appears [W1], alongside row access policies [W2], column-level access history [W3] and a result cache gated on role privileges [W4]. BigQuery has more built-in mask rules than Provisa [W9], plus row-level security [W10]. It also supports customer-managed keys [W13], and Snowflake Tri-Secret Secure matches Provisa's client-owned CMK [W5]. Both offer vector search with embedding generation [W6][W12], and Snowflake's Semantic View Autopilot proposes relationships with AI [W8].

Federation is Provisa's lead. Snowflake federates nothing at query time, and BigQuery federates only AlloyDB, Spanner and Cloud SQL [W14]. Offline deployment and client subscriptions are Provisa leads too. On Arrow transport the practical result is parity. BigQuery streams Arrow through its Storage Read API [W11], and both have ADBC drivers [W15]. Neither uses Flight.

---

## 6. Sources

Fetched 2026-11-07.

**Trino**
- [T1] File-based access control. https://trino.io/docs/current/security/file-system-access-control.html
- [T2] System access control, `allow-all` default. https://trino.io/docs/current/security/built-in-system-access-control.html
- [T3] Open Policy Agent access control. https://trino.io/docs/current/security/opa-access-control.html
- [T4] Connectors. https://trino.io/docs/current/connector.html
- [T5] SET ROLE. https://trino.io/docs/current/sql/set-role.html
- [T6] JWT authentication. https://trino.io/docs/current/security/jwt.html
- [T7] Observability with OpenTelemetry. https://trino.io/docs/current/admin/opentelemetry.html
- [T8] CREATE TABLE. https://trino.io/docs/current/sql/create-table.html
- [T9] Client protocol. https://trino.io/docs/current/client/client-protocol.html

**Hasura**
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

**Dremio**
- [R1] Arrow Flight. https://docs.dremio.com/current/developer/arrow-flight
- [R2] Arrow Flight SQL. https://docs.dremio.com/current/developer/arrow-flight-sql
- [R3] Arrow Flight SQL JDBC driver. https://docs.dremio.com/current/client-applications/drivers/arrow-flight-sql-jdbc-driver/
- [R4] Row-access and column-masking policies. https://docs.dremio.com/current/data-products/govern/row-column-policies-udf
- [R5] Data sources. https://docs.dremio.com/current/data-sources/
- [R6] Results cache. https://docs.dremio.com/dremio-cloud/admin/performance/results-cache
- [R7] CREATE TABLE. https://docs.dremio.com/current/reference/sql/commands/create-table
- [R8] AI agent. https://docs.dremio.com/current/data-products/ai-agent
- [R9] COPY INTO. https://docs.dremio.com/current/reference/sql/commands/apache-iceberg-tables/copy-into-table

**Snowflake / BigQuery**
- [W1] Snowflake dynamic data masking. https://docs.snowflake.com/en/user-guide/security-column-ddm-intro
- [W2] Snowflake row access policies. https://docs.snowflake.com/en/user-guide/security-row-intro
- [W3] Snowflake Access History. https://docs.snowflake.com/en/user-guide/access-history
- [W4] Snowflake persisted query results. https://docs.snowflake.com/en/user-guide/querying-persisted-results
- [W5] Snowflake Tri-Secret Secure. https://docs.snowflake.com/en/user-guide/security-encryption-tss
- [W6] Snowflake vector embeddings. https://docs.snowflake.com/en/user-guide/snowflake-cortex/vector-embeddings
- [W7] Snowflake semantic views. https://docs.snowflake.com/en/user-guide/views-semantic/overview
- [W8] Snowflake Semantic View Autopilot. https://docs.snowflake.com/en/user-guide/views-semantic/ui
- [W9] BigQuery data masking. https://cloud.google.com/bigquery/docs/column-data-masking-intro
- [W10] BigQuery row-level security. https://cloud.google.com/bigquery/docs/row-level-security-intro
- [W11] BigQuery Storage Read API. https://cloud.google.com/bigquery/docs/reference/storage
- [W12] BigQuery embeddings and vector search. https://cloud.google.com/bigquery/docs/vector-search-intro
- [W13] BigQuery customer-managed Cloud KMS keys. https://cloud.google.com/bigquery/docs/customer-managed-encryption
- [W14] BigQuery federated queries. https://cloud.google.com/bigquery/docs/federated-queries-intro
- [W15] ADBC drivers for Snowflake and BigQuery. https://pypi.org/project/adbc-driver-snowflake/ and https://pypi.org/project/adbc-driver-bigquery/
- [W16] Snowflake COPY INTO. https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
