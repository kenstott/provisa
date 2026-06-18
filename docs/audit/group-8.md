# Audit — Group 8: Client Access & Protocols

Date: 2026-06-18
Scope: **Group 8 — Client Access & Protocols** (REQ-043, REQ-044, REQ-045,
REQ-126–132, REQ-161, REQ-163, REQ-256–258, REQ-268–274, REQ-288–291, REQ-293,
REQ-398, REQ-405–408). Spans `provisa/api/` (rest, jsonapi, flight, data, graph,
admin), `provisa/apq/`, `provisa/grpc/`, `provisa/openapi/`, the Java JDBC driver
in `jdbc-driver/`, the Python client in `provisa-client/`, and the steward UI in
`provisa-ui/`.
Method: read implementation against requirement text with file:line evidence.
Companion to the Group-2 audit ([group-2.md](group-2.md)).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 043 | API & Integration | To spec | `/data/graphql` POST compiles+executes GraphQL queries and mutations `provisa/api/data/endpoint.py:275` |
| 044 | API & Integration | To spec | `upload_and_presign` writes result to S3, returns presigned URL with TTL `ExpiresIn` `provisa/executor/redirect.py:283` |
| 045 | API & Integration | To spec | Flight `do_get` streams Arrow record batches; gRPC servicer streams rows `provisa/api/flight/server.py:106` |
| 256 | API & Integration | To spec | `GET /data/rest/{table}` parses `where.col.op`/`limit`/`offset`, compiles+governs+routes `provisa/api/rest/generator.py:184` |
| 257 | API & Integration | Incomplete | JSON:API route does sparse fields/filter/sort/page but never parses `include=` or populates `included_rows` `provisa/api/jsonapi/generator.py:189` |
| 258 | API & Integration | To spec | `GET /data/subscribe/{table}` SSE with pg/mongo/kafka pluggable `watch()` providers + RLS filtering `provisa/api/data/subscribe.py:139` |
| 398 | API & Integration | To spec | `/data/graph-schema` returns `pk_columns` per node label `provisa/api/rest/cypher_router.py:526` |
| 405 | API & Integration | To spec | SourcesPage dropdown exposes collapsed `graphql`/`grpc`, no `_remote` variants `provisa-ui/src/pages/SourcesPage.tsx:94` |
| 406 | API & Integration | To spec | Radio toggle path/URL vs inline, monospace textarea, sends `spec_content`/`spec_path` `provisa-ui/src/pages/SourcesPage.tsx:1280` |
| 407 | API & Integration | To spec | `spec_content` on both request models, YAML-then-JSON parse, `":inline:"` sentinel path `provisa/api/admin/openapi_router.py:72` |
| 408 | API & Integration | To spec | `x-provisa-kind` override precedence: payload > extension > GET heuristic `provisa/openapi/mapper.py:206` |
| 126 | JDBC/ODBC | To spec | Driver auths via `/auth/login`, maps user→role, exposes registered tables/views `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaConnection.java:36` |
| 127 | JDBC/ODBC | To spec | `getTables()` returns role-scoped registered tables/views by alias name `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaDatabaseMetaData.java:29` |
| 128 | JDBC/ODBC | To spec | `getColumns()` introspects schema from compiled metadata, role-filtered `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaDatabaseMetaData.java:96` |
| 129 | JDBC/ODBC | Not to spec | `executeQuery` deserializes Arrow IPC/JSON, not Parquet — no Parquet path exists `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaStatement.java:40` |
| 130 | JDBC/ODBC | To spec | Query-time governance: catalog SQL via `/data/sql` Stage 2, approved via `/data/graphql` `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaConnection.java:291` |
| 131 | JDBC/ODBC | To spec | `jdbc:provisa://host:port` parsed; user/password from standard JDBC Properties `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaDriver.java:24` |
| 132 | JDBC/ODBC | Incomplete | Shaded fat JAR, but bundles Gson + transitive gRPC/Netty/protobuf via flight-core beyond JDK+Arrow `jdbc-driver/pom.xml:104` |
| 293 | JDBC/ODBC | To spec | Flight transport `grpc://host:8815`, auto-connect with silent HTTP fallback, no user config `jdbc-driver/src/main/java/io/provisa/jdbc/FlightTransport.java:40` |
| 161 | Query Dev Tools | Not to spec | Compile-only logic exists as GraphQL `compileQuery` mutation; no `POST /data/compile` REST route `provisa/api/admin/dev_queries.py:400` |
| 163 | Query Dev Tools | To spec | GraphiQL Provisa-tools plugin "View SQL" previews governed SQL/route/sources/params `provisa-ui/src/plugins/provisa-tools.tsx:16` |
| 268 | SQL & Multi-Protocol | Not to spec | `connect()` adds non-spec `mode` param + `X-Role` header; GraphQL/SQL detection correct `provisa-client/provisa_client/dbapi.py:87` |
| 269 | SQL & Multi-Protocol | Not to spec | Spec forbids connection mode; `connect()` exposes `mode="approved"` and forwards `role` `provisa-client/provisa_client/dbapi.py:93` |
| 270 | SQL & Multi-Protocol | To spec | `ProvisaDialect` (port 8001), entry points, `get_table_names` introspection, DB-API conn `provisa-client/provisa_client/sqlalchemy_dialect.py:28` |
| 271 | SQL & Multi-Protocol | To spec | `adbc_connect` returns ADBC conn over Flight, streams Arrow RecordBatches `provisa-client/provisa_client/adbc.py:38` |
| 272 | SQL & Multi-Protocol | Incomplete | SQL Flight path applies RLS/masking/visibility/LIMIT ceiling but not sampling `provisa/pgwire/_pipeline.py:121` |
| 273 | SQL & Multi-Protocol | Not to spec | Clients accept client-supplied `role`; server trusts ticket/header role rather than auth only `provisa-client/provisa_client/dbapi.py:92` |
| 274 | SQL & Multi-Protocol | To spec | Per-call dispatch by syntax: cypher/SQL(Stage 2)/GraphQL(Stage 1+2) via `detect_target` `provisa/api/flight/server.py:479` |
| 288 | APQ | To spec | Apollo wire: `persistedQuery.sha256Hash` parse, hash-only lookup, `PersistedQueryNotFound` on miss `provisa/api/data/endpoint.py:316` |
| 289 | APQ | Incomplete | Redis cache + TTL bound to `REDIS_URL`/`PROVISA_APQ_TTL` env, not config keys `cache.redis_url`/`apq.ttl` `provisa/api/app.py:2438` |
| 290 | APQ | To spec | Any successful query auto-registered by hash, no steward gate, reusable hash-only `provisa/api/data/endpoint.py:432` |
| 291 | APQ | To spec | Rights `check_capability` + Stage 2 run before response; `set()` only on non-None response `provisa/api/data/endpoint.py:501` |

Counts: 20 To spec, 4 Incomplete, 6 Not to spec, 0 Not added.

## Detail

### API & Integration (REQ-043–045, 256–258, 398, 405–408)

- **REQ-043** — GraphQL is the primary entry point: `/data/graphql` POST parses,
  compiles, and executes both queries and mutations
  `provisa/api/data/endpoint.py:275`.
- **REQ-044** — `upload_and_presign` writes the result to S3 and returns a
  presigned URL with `ExpiresIn` set from the redirect TTL, wired into the data
  endpoint redirect path `provisa/executor/redirect.py:283`.
- **REQ-045** — Arrow Flight `do_get` streams Arrow record batches via
  `RecordBatchStream` `provisa/api/flight/server.py:106`; the gRPC servicer streams
  rows at `provisa/grpc/server.py:70`.
- **REQ-256** — `GET /data/rest/{table}` parses `where.col.op` plus `limit`/`offset`
  and runs the compile→govern→route→execute pipeline
  `provisa/api/rest/generator.py:184`.
- **REQ-257** — *Incomplete.* The JSON:API route supports sparse fieldsets,
  `filter[]`, `sort`, and `page[number]`/`page[size]` with relationship-bearing
  resource objects, but it never parses `include=` nor passes `included_rows` to the
  serializer, so compound documents are not produced (the serializer accepts
  `included_rows`; the route does not supply it)
  `provisa/api/jsonapi/generator.py:189`.
- **REQ-258** — `GET /data/subscribe/{table}` streams SSE through pluggable
  `NotificationProvider.watch()` implementations dispatched by source type
  `provisa/api/data/subscribe.py:139`; MongoDB Change Streams via motor at
  `provisa/subscriptions/mongo_provider.py:36`, PostgreSQL LISTEN/NOTIFY at
  `provisa/subscriptions/pg_provider.py:38`. RLS filtering applies in the event
  stream.
- **REQ-398** — `/data/graph-schema` returns `pk_columns` per node label
  `provisa/api/rest/cypher_router.py:526` (endpoint at :488).
- **REQ-405** — SourcesPage `SOURCE_TYPES` offers only the collapsed `graphql` and
  `grpc` values; no `_remote` variants are exposed
  `provisa-ui/src/pages/SourcesPage.tsx:94`.
- **REQ-406** — Radio toggle "Spec path / URL" vs "Write spec inline" with a
  monospace textarea; inline sends `spec_content`, otherwise `spec_path`
  `provisa-ui/src/pages/SourcesPage.tsx:1280`.
- **REQ-407** — `spec_content` exists on both `OpenAPIRegisterRequest` and
  `OpenAPIPreviewRequest`; parsed YAML-then-JSON and stored with the `":inline:"`
  sentinel path `provisa/api/admin/openapi_router.py:72`.
- **REQ-408** — `x-provisa-kind` override with precedence payload override >
  `x-provisa-kind` > GET heuristic `provisa/openapi/mapper.py:206`.

### JDBC/ODBC Integration (REQ-126–132, 293)

- **REQ-126** — Driver authenticates via `/auth/login`, maps user→role, exposes
  registered tables (catalog mode) and approved-query views
  `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaConnection.java:36`.
- **REQ-127** — `getTables()` returns registered tables/views by alias name,
  role-scoped via `X-Provisa-Role`
  `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaDatabaseMetaData.java:29`.
- **REQ-128** — `getColumns()` introspects column names/types from compiled metadata
  and applies role visibility
  `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaDatabaseMetaData.java:96`.
- **REQ-129** — *Not to spec.* `executeQuery` runs SQL over the Provisa HTTP API and
  deserializes into a ResultSet, but the transport is Arrow IPC stream / JSON — there
  is no Parquet deserialization anywhere, contrary to "executes via Provisa's HTTP
  API with Parquet format"
  `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaStatement.java:40`.
- **REQ-130** — Query-time governance: catalog SQL routes through the `/data/sql`
  Stage 2 endpoint; approved queries execute live via `/data/graphql`, not baked into
  views `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaConnection.java:291`.
- **REQ-131** — `jdbc:provisa://host:port` parsed; user/password read from standard
  JDBC `Properties` `jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaDriver.java:24`.
- **REQ-132** — *Incomplete.* maven-shade builds a single fat JAR, but it bundles
  Gson and ships gRPC/Netty/protobuf transitively via flight-core (added for
  REQ-293), exceeding the "JDK + Apache Arrow only" constraint `jdbc-driver/pom.xml:104`.
- **REQ-293** — Flight transport on `grpc://host:8815`, auto-connects with silent
  HTTP fallback (`tryConnect` returns null), no user config; the ticket carries the
  query + role + variables as JSON and streams Arrow batches
  `jdbc-driver/src/main/java/io/provisa/jdbc/FlightTransport.java:40`. Note: the
  ticket carries GraphQL query text; port derived as `httpPort + delta` (default
  8815).

### Query Development Tools (REQ-161, 163)

- **REQ-161** — *Not to spec.* The compile-only logic (SQL + route + params with
  RLS/masking, no execution) exists, but it is exposed as a GraphQL `compileQuery`
  mutation rather than the `POST /data/compile` REST route the requirement names; no
  such route exists `provisa/api/admin/dev_queries.py:400`.
- **REQ-163** — GraphiQL Provisa-tools plugin renders a "View SQL" governed-SQL
  preview (semantic SQL, route, sources, params) from `compileQuery`
  `provisa-ui/src/plugins/provisa-tools.tsx:16`.

### SQL & Multi-Protocol Client Access (REQ-268–274)

- **REQ-268** — *Not to spec.* `connect()` adds a non-spec `mode` parameter and sends
  an `X-Role` header; the requirement says the server assigns the role. GraphQL/SQL
  detection by leading `{`/keyword is correct
  `provisa-client/provisa_client/dbapi.py:87`.
- **REQ-269** — *Not to spec.* The requirement forbids a connection `mode`, but
  `connect()` exposes `mode: str = "approved"` and forwards `role`
  `provisa-client/provisa_client/dbapi.py:93`.
- **REQ-270** — `ProvisaDialect` (name `provisa`, default port 8001) with
  `sqlalchemy.dialects` entry points, `get_table_names` introspection, and a DB-API
  connection via `import_dbapi`
  `provisa-client/provisa_client/sqlalchemy_dialect.py:28`.
- **REQ-271** — `adbc_connect(url, user, password)` returns an `AdbcConnection` over
  `pyarrow.flight`, streaming Arrow RecordBatches via `do_get`/`fetch_arrow_table`
  `provisa-client/provisa_client/adbc.py:38`.
- **REQ-272** — *Incomplete.* The SQL Flight path routes through `_govern_and_route`
  → `apply_governance`, enforcing RLS, masking, visibility, and a LIMIT ceiling, but
  sampling is not applied on this path `provisa/pgwire/_pipeline.py:121`.
- **REQ-273** — *Not to spec.* Clients accept a client-supplied `role` parameter and
  send it; the server trusts the ticket/header role rather than deriving it solely
  from auth (`role` defaults to `admin`, `effective_role = role` even with a token)
  `provisa-client/provisa_client/dbapi.py:92`.
- **REQ-274** — Per-call dispatch by syntax: Flight `_execute_query` routes
  cypher/SQL (Stage 2)/GraphQL (Stage 1+2) via `detect_target`
  `provisa/api/flight/server.py:479`.

### Automatic Persisted Queries (REQ-288–291)

- **REQ-288** — Apollo APQ wire protocol: `extensions.persistedQuery.sha256Hash`
  parsed, hash-only lookup, `PersistedQueryNotFound` on miss, hash+query validated
  and stored on resend `provisa/api/data/endpoint.py:316`.
- **REQ-289** — *Incomplete.* The Redis cache and TTL exist but are bound to the
  `REDIS_URL` / `PROVISA_APQ_TTL` env vars rather than the config keys
  `cache.redis_url` / `apq.ttl` named in the requirement; cold-start miss is handled
  by the `NoopAPQCache` → `PersistedQueryNotFound` retry flow
  `provisa/api/app.py:2438`.
- **REQ-290** — Any successfully executed query is auto-registered by hash with no
  steward gate and is reusable hash-only via `cache.get`
  `provisa/api/data/endpoint.py:432`.
- **REQ-291** — Rights (`check_capability`) plus Stage 2 `apply_governance` run inside
  `_handle_query` before any response; `set()` runs only on a non-None response, so a
  rejected query is never registered `provisa/api/data/endpoint.py:501`.

## Named tests

| Test file | Status |
| --- | --- |
| `tests/unit/test_rest_generator.py` | Exists (16 tests) |
| `tests/unit/test_jsonapi.py` | Exists (38 tests) |
| `tests/unit/test_drivers.py` | Exists (13 tests) |
| `tests/integration/test_compile_endpoint.py` | Exists (14 tests) |
| `tests/integration/test_adbc.py` | Exists (6 tests) |
| `tests/integration/test_sqlalchemy_dialect.py` | Exists (19 tests) |
| `tests/unit/test_apq.py` | Exists (14 tests) |
| `tests/integration/test_apq_integration.py` | Exists (23 tests) |
| `tests/unit/test_graph_schema.py` | **Missing** — named by REQ-398, not present |

REQ-405/406/407/408 are marked `n/a` for tests in the requirements table; no test
file is named for them.

## Remaining tasks

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 257 | Incomplete | M | Parse `include=` in the JSON:API route and pass `included_rows` to the serializer to emit compound documents `provisa/api/jsonapi/generator.py:189` |
| 2 | 129 | Not to spec | M | Add Parquet result format + deserialization to the JDBC `executeQuery` HTTP path, or amend the requirement to Arrow IPC `jdbc-driver/.../ProvisaStatement.java:40` |
| 3 | 132 | Incomplete | M | Shade/relocate or drop Gson and the transitive gRPC/Netty/protobuf so the JAR depends only on JDK+Arrow, or amend the constraint to include Flight `jdbc-driver/pom.xml:104` |
| 4 | 161 | Not to spec | S | Add a `POST /data/compile` REST route wrapping the existing compile-only logic `provisa/api/admin/dev_queries.py:400` |
| 5 | 268 | Not to spec | M | Remove the `mode` param and `X-Role` from the DB-API `connect()`; rely on server-assigned role `provisa-client/provisa_client/dbapi.py:87` |
| 6 | 269 | Not to spec | M | Drop the connection `mode` parameter; route all SQL uniformly through rights + Stage 2 `provisa-client/provisa_client/dbapi.py:93` |
| 7 | 273 | Not to spec | M | Stop accepting/forwarding a client-supplied `role` across DB-API/SQLAlchemy/ADBC/JDBC; derive role from auth only `provisa-client/provisa_client/dbapi.py:92` |
| 8 | 272 | Incomplete | S | Apply sampling in the SQL Flight governance path alongside RLS/masking/ceilings `provisa/pgwire/_pipeline.py:121` |
| 9 | 289 | Incomplete | S | Bind APQ Redis/TTL to config keys `cache.redis_url` / `apq.ttl` instead of `REDIS_URL` / `PROVISA_APQ_TTL` env vars `provisa/api/app.py:2438` |
| 10 | 398 | Missing test | S | Add `tests/unit/test_graph_schema.py` covering `pk_columns` in the graph-schema response |
