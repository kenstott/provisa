# Remote Schemas

A remote schema source connects an external API — GraphQL (including GitHub), gRPC, or REST (OpenAPI) — to the Provisa semantic layer. Once registered, the external API's operations become first-class Provisa tables and functions. (REQ-308, REQ-316, REQ-325) Every governance rule, query interface, and security layer applies automatically. (REQ-310, REQ-319, REQ-328) The remote service never sees Provisa's governance rules. (REQ-310, REQ-319, REQ-328)

---

## Three source types

### GraphQL remote schema (REQ-307–313)

**How to register.** POST to `/admin/sources/graphql-remote` with the endpoint URL, a namespace, and optional auth. Provisa fires a standard `__schema` introspection query against the remote endpoint. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

Auth options: `none`, `bearer` (Authorization header), `basic` (Base64 username:password). (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**Field overrides.** `field_overrides` is a `{fieldName: "query" | "mutation"}` map applied after introspection. It takes priority over structural classification. Only query-type fields can be reclassified as mutations; mutation-type fields have no override path in GraphQL. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**Relationships at registration time.** `relationships` declares FK/PK join paths between tables at registration time. These are stored as manually declared relationships (no `remote_managed` flag). On refresh, auto-detected relationships (those with `remote_managed: True`) are re-run and may change; manually declared relationships are not touched. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**What gets auto-discovered.** Every field on the remote `Query` type that returns an OBJECT becomes a virtual table. Every field on the remote `Mutation` type becomes a tracked function. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**Table naming.** Tables are named `{namespace}__{field_name}`. With namespace `petstore` and a `pets` query field: table name is `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Relay connections.** Many APIs return lists as Relay connections: an object with `nodes` (or `edges { node }`) beside `pageInfo`. Provisa maps a connection to a table of its nodes, and reads it page by page. (REQ-308, REQ-309) [tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- A root field that returns a connection (`securityAdvisories`) becomes one table of its nodes.
- A connection on the single object a root field returns becomes its own table. The table takes the root field's required arguments. With `repository(owner, name)` and a connection `issues` on `Repository`, the table is `repositoryIssues`, SQL name `gh__repository_issues` under namespace `gh`. Filter it through the `_nf_owner` and `_nf_name` columns: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- A connection is never a column. A row would otherwise carry a read the remote computes per row, for every connection its type has.
- A connection is a table only if its field takes `first` and `after`, so it can be read page by page. One that needs an argument of its own is not a table. Neither is a connection of a union, or any connection under a root field that returns a list.

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

**Type mapping (REQ-308).** Scalar fields map to Provisa types directly. OBJECT fields split into two cases depending on whether the target type is governed (see "Governed tables" below). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| GraphQL type | Provisa type |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (non-governed inline type, e.g. `ContactInfo`) | `jsonb` blob column |
| OBJECT (governed-target type) | excluded from SDL and fetch entirely |
| Any ENUM | `jsonb` |
| Custom scalar | `text` (fallback) |

**Governed tables.** A GQL type is governed when it appears as a root `Query` field in the remote schema. `_collect_queryable_types` collects these during registration, preferring no-required-arg fields so they can be bulk-fetched as join targets. [tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

When an OBJECT-typed column on a governed table points to another governed type, that column is subject to three rules simultaneously [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]:

1. **Excluded from the GQL fetch** — the field is not requested when fetching the parent table's rows.
2. **Excluded from the SDL** — the field does not appear on the parent type in the generated schema.
3. **Accessible only via a declared relationship** — a steward must register a JOIN between the two materialized governed tables. Without one, the field is simply absent; there is no blob fallback.

OBJECT types that are NOT reachable as root Query fields (inline types such as `ContactInfo` or `Address`) follow different rules: they are fetched as `jsonb` blob columns and appear in the SDL as nested-object fields. Sub-fields are accessible via `-->>` extraction in SQL.

**Fields that need an argument are not columns.** A field with a required argument cannot be selected bare, so it is left out of the table's columns and out of nested selections. [tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**Required arguments.** When a root query field has non-null arguments with no default value, those become `native_filter_type: query_param` columns on the table (prefixed `_nf_` at injection time). The executor passes them as GraphQL variables. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Relationships detected automatically.** Provisa scans each table's OBJECT-typed columns. When the referenced GQL type is also registered as a table in the same source, a relationship is emitted. Many-to-one relationships infer source and target columns from naming conventions (`breedName` on the source type → `name` on the `Breed` target type). One-to-many (LIST) fields emit relationships with empty column references — the FK lives on the target side. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutations.** Mutation fields produce tracked functions with argument types mapped from the mutation's args and a `return_schema` derived from the mutation's return type. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**Refresh.** POST to `/admin/sources/graphql-remote/{id}/refresh`. Re-introspects the remote schema and updates table and function registrations. Existing governance rules (RLS, masking) are preserved. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py:217–257`]

**Limitations.**

- Scalar and ENUM root query fields (return type is not OBJECT) become tracked functions, not virtual tables. Their `return_schema` is a single `value` column of the mapped scalar type. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- Object nesting is resolved at registration time up to `graphql_remote.max_object_depth` (default: 5). Both the remote fetch selection and the sub-field metadata are built to that depth; fields beyond the limit are not fetched and are not available for SQL extraction. A type is entered once along any one path: a field whose type is already on the way down is left out, so a schema whose types refer back to each other is walked once per type, not once per depth level. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- LIST-typed nested OBJECT fields (e.g. `breed.awards: [Award]`) are included in the fetch selection up to `graphql_remote.max_list_depth` nesting levels (default: 2). Within that limit, the list is fetched as a `jsonb` array on the parent column. When the list field declares a `first` argument (Relay, PostGraphile, pg_graphql) or a `limit` argument (Hasura), the selection passes it as `first: N` or `limit: N`, where N is `graphql_remote.max_list_items` (default: 100). A list field that declares neither gets no argument, because a remote rejects an argument the field does not declare. Beyond `max_list_depth`, the LIST field is excluded entirely to prevent unbounded data expansion. In SQL, the array is accessed via `json_array_elements(column_name)` or `->>` index extraction. If the list's item type has its own root query, register it as a separate table and create a relationship instead — the join path is more efficient and bypasses the blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- For SQL queries, non-governed OBJECT-typed columns are fetched in full from the remote (all sub-fields up to the configured depth) and cached as `jsonb`. Sub-field access in SQL is handled via `->>`  extraction against the blob; the remote request is not narrowed to only the fields the SQL query selects. When the LIST-item type has no root query and the blob representation is insufficient, write the query in GraphQL SDL directly — Provisa faithfully reproduces the GQL field selection, so the remote sees exactly the fields requested. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- If the remote server rejects an OBJECT-typed field because it requires subfield selection (which should not occur when `gql_selection` is available), the executor retries once with those fields removed so scalar columns are still returned. This applies to tables read off a root field. A connection table does not take this path. [tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**Paged reads.** A connection table is read by cursor. Each page asks for `first: N, after: $pageCursor` with `pageInfo { hasNextPage endCursor }`, and the read follows `endCursor` until the remote reports no next page. (REQ-309) [tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| Setting | Default | Effect |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Rows per page. [tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | The most rows one read of a connection table takes. A read that reaches it stops and logs a warning. [tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Two responses make the executor retry:

- **Page too heavy.** When the remote answers 502 or 504, the same page is asked again at half the size, down to one row. [tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **Rate limit with a wait time.** When the remote answers 403 or 429 with a `Retry-After` of 120 seconds or less, the executor waits that long and sends the request again, up to three attempts. A refusal with no `Retry-After`, or one asking for a longer wait, is raised as an error. This applies to every read, connection or not. [tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

Any other error in the response fails the read, unless the source type declares otherwise (see GitHub below). A connection whose parent came back null has no rows. [tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub (REQ-1923)

GitHub is an ordinary source type. Its API is GraphQL, so its tables behave as described above, including connection tables such as `gh__repository_issues`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**Add the source.**

1. Open Sources and add a source of type **GitHub**.
2. Enter a GitHub access token. Optionally enter a namespace, the prefix of the table names; the default is `gh`.
3. Save. Provisa checks the token against GitHub. A token GitHub rejects fails the add with GitHub's message.

Adding the source registers no tables. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**Register tables.** Open Tables, then Register Table. Pick the GitHub source, pick schema `graphql`, then pick the tables you want. Every table GitHub offers is listed; the registration is your choice of which to expose. [tool-verified: `provisa/api/admin/_graphql_brand_registration.py` `offered_tables`] [inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**Token scopes.** When you register a table, Provisa checks it once against GitHub with your token.

- A field the token's scopes do not cover is left out of the table. The result names each field left out: `Left out, because the source's credential may not read them: projectsV2`. [tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- A table the token cannot read at all is refused, with GitHub's reason: `GitHub does not let this source's credential read gh__repository_issues: ...`. [tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**Rows the token may not see.** GitHub answers `FORBIDDEN` for a field the token may not see on one particular row, such as a repository's collaborators without push access, and `NOT_ORG_OWNED_REPO` for a field that exists only on organization-owned repositories. That field is null in that row, the rest of the read stands, and Provisa logs a warning. An error against the table itself fails the read. [tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**Heavy pages.** When GitHub answers `RESOURCE_LIMITS_EXCEEDED` because a page costs too much to compute, the page is asked again at half the size. [tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**Nested objects.** GitHub tables use their own nesting depth of 0 (`max_object_depth=0` for this source type), not `graphql_remote.max_object_depth`. A nested object column is selected with its own scalar fields only; objects inside it show as `__typename`. [tool-verified: `brands.py`]

**Token storage.** The token goes to the secrets vault and the source row keeps a reference, so a restart reads the source again without re-entering the token. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**How it works (operators).** GitHub's schema ships with Provisa, so adding the source makes no introspection call and a large schema costs nothing at registration. Tables are mapped from it one at a time as you register them. The refresh endpoint refuses this source type; a new GitHub schema arrives with a Provisa release. [tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note] [tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab (REQ-1923)

GitLab is an ordinary source type and is added and registered the same way as GitHub: add a source of type **GitLab** with an access token, then register the tables you want from schema `graphql`. The default table-name prefix is `gl`. The source reaches `gitlab.com`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**Choose columns.** GitLab prices every query and refuses one that costs too much: 200 points for an anonymous caller, 250 with a token. A wide table with every column selected is over that price, so register a GitLab table with the columns you want. [tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- When you register a table, Provisa asks GitLab once whether it will serve the selection at the page size reads use. If GitLab answers that the query is too complex or too large, the table is not registered and the result carries GitLab's message: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.` [tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- What a column costs depends on its kind. A plain value costs about a point; a nested-object column costs many times that. Dropping nested-object columns frees the most. [tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- The page size is part of the price. It is `graphql_remote.max_list_items`. [tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**Token check.** GitLab answers an unrecognized token with an empty result, not an error. Provisa treats that as a rejected token and does not add the source. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### gRPC remote schema (REQ-322–329)

**How to register.** POST to `/admin/grpc-remote/register` with the server address, a path or URL to a `.proto` file, and optional TLS config.

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa fetches the proto, parses it with a pure-text parser (no external proto deps at parse time), compiles Python stubs via `grpc_tools.protoc`, and opens a persistent `grpc.aio.Channel`. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Proto files may also be local paths. Import paths for well-known types (`google/protobuf/timestamp.proto`) are stored at registration time and reused on refresh. (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**What gets auto-discovered.** Every `rpc` method in the proto is classified as a query or mutation using three signals in priority order: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** in the registration payload — `{"MethodName": "query"}` or `{"MethodName": "mutation"}` overrides everything else.
2. **`server_streaming: true`** — the server sends a stream of messages; always a virtual table (unless the output is a scalar).
3. **Output message has a repeated message-type field** — e.g. `ListOrdersResponse { repeated Order items; }` is treated as a list-wrapper and becomes a virtual table. Repeated scalar fields (e.g. `repeated string tags`) do not trigger this — they are array properties on a single entity, not row sources.

Methods that match none of these signals (unary RPC returning a single entity message, or any scalar output) become tracked functions.

**Table naming.** The default name is `{namespace}__{ServiceName}__{MethodName}`. Without a namespace, the service and method names are joined directly. Any registered table can be given an `alias`; when set, the alias is the name used everywhere (queries, SDL, relationships). The auto-generated name is the registration key and never changes. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**Type mapping (REQ-324).** Proto scalar types map to SQL types as follows. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Proto type | SQL type |
| --- | --- |
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| Nested message | `jsonb` |
| Enum | `text` |

**Relationships at registration time.** `relationships` works identically to the GQL adapter — declares FK/PK join paths stored as manually declared relationships (no `remote_managed` flag). On refresh, these are preserved unchanged. (REQ-554) [tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Query methods (REQ-325).** Output message fields become table columns. Input message fields both become GraphQL arguments passed to the remote call *and* are registered as `_nf_`-prefixed columns with `native_filter_type: "grpc_input"` — the same mechanism GQL and OpenAPI use for native filter injection. (REQ-555) [tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**Nested message sub-fields.** For query methods, non-repeated message-typed fields at depth 0 (direct output columns) have their sub-fields resolved one level deep and stored as `object_fields` on the `ColumnDef`. This metadata is used for `jsonb` sub-field extraction in SQL and for schema documentation. Fields nested beyond depth 1 are not recursively expanded. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

Server-streaming methods collect all streamed messages into a list before returning rows. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Mutation methods (REQ-326).** Input message fields become mutation input arguments. The output message schema becomes the `return_schema`. [tool-verified: `provisa/grpc_remote/executor.py:122–143`]

**Channel management.** One `grpc.aio.Channel` per registered source is stored in app state and reused across requests. The old channel is closed before a new one opens on refresh. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Refresh.** POST to `/admin/grpc-remote/refresh/{source_id}`. Re-loads the proto from the stored path, recompiles stubs, and re-registers tables and functions. Alternatively, PUT to `/admin/grpc-remote/{source_id}/proto` with new `proto_text` to update the proto inline. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`, `provisa/api/admin/grpc_remote_router.py:300–358`]

**Limitations.**

- Sub-field object extraction is one level deep. Nested message fields beyond depth 1 are not recursively expanded. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**How to register.** Call `auto_register_openapi_source` with a source ID, a parsed spec, and connection metadata. The spec is loaded from a local file or URL. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/openapi/register.py:249–264`]

**Registration payload.** The `/admin/openapi/register` endpoint accepts two additional fields alongside `source_id`, `spec_path`, etc.:

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**What gets auto-discovered.** Every GET operation in the spec becomes a virtual table, unless its response schema is a scalar type (`string`, `number`, `boolean`, `integer`) — scalar-returning GETs become tracked functions with a single `value` column instead. Every non-GET operation (POST, PUT, PATCH, DELETE) becomes a tracked function. (REQ-316, REQ-317)

Classification priority: `operation_overrides` (payload) overrides `x-provisa-kind` (spec extension) overrides the GET heuristic. `operation_overrides` is the recommended override path; `x-provisa-kind` is for when the spec itself should carry the classification. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**Relationships at registration time.** `relationships` works identically to the other adapters — stored as manually declared relationships, preserved on refresh. (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Table naming.** Tables use the operation's `operationId`. If no `operationId` is defined, Provisa slugifies `{method}_{path}`. An alias is derived by stripping the leading verb segment and singularizing the noun (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**Type mapping.** JSON Schema types map to Provisa types as follows. [tool-verified: `provisa/openapi/register.py:59–70`]

| JSON Schema type | Provisa type |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parameters as native filter columns.** Path and query parameters that are not already response fields become columns with `native_filter_type` set to `path_param` or `query_param`, prefixed `_nf_`. When a parameter name matches a response field name, the parameter metadata is merged into the existing column entry rather than creating a duplicate. (REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**Response schema resolution.** The mapper checks `responses.200`, then `responses.2xx`, then `responses.default`. Array-type responses are unwrapped to their item schema. `$ref` references are resolved one level deep. (REQ-316) [tool-verified: `provisa/openapi/mapper.py:83–101`]

**Object sub-fields.** Response properties with `type: object` and their own `properties` are stored as `object_fields` on the column. These sub-fields are visible in the SDL and used for `jsonb` extraction in queries. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**Response caching (REQ-318).** GET operation results are cached in PostgreSQL by `pg_cache.py`. Each combination of request parameters gets its own `_params_hash` group. Rows for a given hash are replaced when the TTL expires. Path-param endpoints (`/pets/{id}`) skip the initial bulk fetch — the cache table is created empty for schema introspection, then populated per-PK as requests arrive. [tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**Refresh (REQ-321).** Re-parse the spec and call `auto_register_openapi_source` again. Existing governance rules are preserved; registrations are updated with ON CONFLICT upsert. [tool-verified: `provisa/openapi/register.py:249–264`]

**Limitations.**

- Sub-field object extraction is one level deep. Properties nested inside `object_fields` are not recursively expanded. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]
- Header and cookie parameters are ignored; only `path` and `query` parameters are registered. (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- Spec-level `$ref` resolution is one level deep for property schemas; deeply nested component references may not resolve. [tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## Impact of registering a remote table

A table registered from any remote schema source is a first-class Provisa table. Nothing about it is treated differently from a locally connected relational table at runtime. (REQ-308, REQ-313)

**Query interfaces.** The table is immediately queryable via GraphQL, SQL (pgwire or direct), Cypher (GQL), JSON:API, and Arrow Flight. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) Schema generation synthesizes `ColumnMetadata` for remote tables since they have no catalog — type mapping is applied at schema build time. (REQ-602) [tool-verified: `provisa/api/app.py:1367–1386`]

**Security model.** All five governance layers apply:

1. Domain access control — the table's `domain_id` gates which roles can see it. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. Row-level security (RLS) — row filters configured on the table are injected into every query, regardless of interface. (REQ-040, REQ-041)
3. Column visibility — `visible_to` list on each column controls per-role field exposure. (REQ-039)
4. Column masking — masking rules apply in Stage 2 of the governance pipeline. (REQ-040, REQ-263)
5. Predicate guard — masked columns are rejected from WHERE and HAVING clauses. (REQ-603)

Ad-hoc queries against remote tables are allowed under the user's rights alone — access is uniform rights-based (table/column rights + approved relationships), with no per-table governance mode. (REQ-001, REQ-003)

**Relationship governance (V002).** JOIN conditions against remote tables — when queried via SQL or Cypher — must match a registered, approved relationship. (REQ-604) The V002 check is skipped for GraphQL queries because SDL-defined relationships are pre-approved by design. See [docs/security.md](security.md#relationship-governance-v002).

**OBJECT-typed columns.** When a column maps to a non-governed inline GQL OBJECT or OpenAPI object type, its Provisa type is `jsonb`. The column stores the full nested JSON blob. When sub-fields are declared (`gql_object_fields` or `object_fields`), the `gql_object_columns` map is populated at schema build time. The SQL generator uses this map to emit `->>`  extraction expressions for sub-fields when a query selects them. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**Required args as native filter params.** Root query fields with non-null, no-default args inject additional columns onto the registered table. These columns carry `native_filter_type: query_param`. The Cypher translator rewrites `WHERE n.id = $val` to `WHERE n._nf_id = $val`, and the GraphQL executor picks them up as variables to pass to the remote endpoint. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## Impact of creating a covering relationship

When a steward registers a relationship between two remote tables (or between a remote table and a local table), the relationship becomes the join path used at query time.

**How the join wins.** At query compilation, Provisa resolves the join path through the registered relationship. The `source_column` and `target_column` on the relationship become the join condition in generated SQL. The join replaces any per-table remote call that would otherwise be needed for the connected type.

**The raw blob is never exposed in SQL.** The `breed` column on `petstore__pets` is not selectable as a raw jsonb value in SQL queries. When a relationship is registered between `petstore__pets` and `petstore__breeds`, SQL queries traverse the join — `SELECT breed.name FROM petstore__pets` resolves via the FK join, not a blob. When no relationship is registered but the column has declared sub-fields (`gql_object_fields`), SQL sub-field references are rewritten to `->>`  extraction against the stored blob. This path is only available for non-governed inline types — governed-target fields are excluded from the SDL entirely and have no blob to extract from. The raw blob itself is never emitted as a bare column value. [tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

In GraphQL SDL, a non-governed inline OBJECT field is typed as the nested object type. Whether it is served by a join or by blob extraction at execution time is an implementation detail — the SDL shape is identical either way. When the child type is registered as its own table (and becomes governed), all five governance layers apply to it independently: its own RLS rules, column visibility, masking rules, predicate guards, and domain access control. (REQ-039, REQ-040, REQ-041, REQ-263) Blob extraction bypasses this — the child data arrives pre-embedded in the parent row and is governed only by the parent table's rules. Registering the child as a table and creating a relationship is the path to fine-grained governance on the child type.

**`graphql_alias` on the relationship.** The `graphql_alias` field names the SDL field that the relationship exposes on the parent type. When absent, the name is derived from the target table's `field_name` and the relationship's cardinality via `rel_field_name(target.field_name, cardinality)`. (REQ-605) [tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 on the join path.** SQL and Cypher queries that traverse the relationship are subject to V002 relationship governance. The relationship must be registered and approved for the join to be allowed. (REQ-604) GraphQL traversal via the SDL relationship field is always pre-approved. [tool-verified: `docs/security.md:41–54`]

**Remote-managed flag.** Relationships auto-detected during GraphQL remote registration are stored with `remote_managed: True`. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:199`] This is a metadata marker; it does not alter governance behavior.

---

## Type-def-only behavior

Not every type in a remote schema needs to be a queryable table.

When `root_table_ids` is set on a `SchemaInput`, tables whose IDs are absent from that set are excluded from the root query fields in the generated SDL. They remain present as GraphQL types and can be reached via relationship fields on tables that do have root entries. (REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

The same mechanism applies to domain-filtered schema builds: tables in domains the role cannot access are type-defs only — their type definition exists in the SDL for relationship traversal, but no root query field is generated for them. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

A type-def-only table:

- Has no root query field — clients cannot query it directly by name.
- Is reachable via relationship fields on tables that do have root entries.
- Still appears in schema introspection as a named type.
- Still has all governance rules applied when data is accessed through a relationship. (REQ-039, REQ-040)

Full removal from the schema — including the type definition — only happens when the table registration is deleted entirely. Marking a table as type-def-only (by removing its ID from `root_table_ids` or by filtering on domain access) does not remove the type.

This design lets stewards expose navigable object graphs where some types are reachable only by traversal, not by independent query.
