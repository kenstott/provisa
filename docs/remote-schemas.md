# Remote Schemas

A remote schema source connects an external API, whether GraphQL (GitHub included), gRPC or REST (OpenAPI), to the Provisa model. Adding the source registers no table. The source offers tables, and a steward registers each one wanted in the Register Table picker; that registration is the curation step. A registered table is a full Provisa table, so every governance rule, query interface and security layer applies to it automatically. The remote service never sees Provisa's governance rules.

---

## Three source types

### GraphQL remote schema

**How to add the source.** POST to `/admin/sources/graphql-remote` with the endpoint URL, a namespace, and optional auth. Provisa fires a standard `__schema` introspection query against the remote endpoint to confirm the endpoint and credential.

Adding the source registers no table and no command. Every remote source type answers adding and refreshing with the same counts: `tables` (tables registered or brought up to date; 0 on add), `available_tables` (tables on offer), `mutations` (always 0) and `available_mutations` (commands on offer).

**Register tables.** Open Tables, then Register Table, pick the source and schema `graphql`, and choose the tables and columns you want. Through the admin GraphQL API: `availableTables(sourceId, schemaName)` lists the tables on offer, `availableColumns` lists a table's columns, and `registerTable(input: TableInput)` registers one with the columns chosen. A registered table is then governed.

How a registered table is read (root field, row path, required arguments, page arguments) is stored in `sources.mapping["tables"]`, so a restarted process reads it without asking the remote for its schema.

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

Auth options: `none`, `bearer` (Authorization header), `basic` (Base64 username:password).

**Field overrides.** `field_overrides` is a `{fieldName: "query" | "mutation"}` map applied after introspection. It takes priority over structural classification. Only query-type fields can be reclassified as mutations; mutation-type fields have no override path in GraphQL.

**Relationships at registration time.** `relationships` declares FK/PK join paths between tables at registration time. These are stored as manually declared relationships (no `remote_managed` flag). On refresh, auto-detected relationships (those with `remote_managed: True`) are re-run and may change; manually declared relationships are not touched.

**What the source offers.** Every field on the remote `Query` type that returns an object or a list of objects is offered as a table, and so is each Relay connection under a single-object field (see below). Registering an offered table makes it a table. Each field of the remote `Mutation` type is a command on offer, counted in `available_mutations`; adding the source registers none. Register the ones you want as commands; see [A remote source's write operation](commands.md#a-remote-sources-write-operation).

**Table naming.** Tables are named `{namespace}__{field_name}`. With namespace `petstore` and a `pets` query field: table name is `petstore__pets`.

**Relay connections.** Many APIs return lists as Relay connections: an object with `nodes` (or `edges { node }`) beside `pageInfo`. Provisa maps a connection to a table of its nodes, and reads it page by page.

- A root field that returns a connection (`securityAdvisories`) becomes one table of its nodes.
- A connection on the single object a root field returns becomes its own table. The table takes the root field's required arguments. With `repository(owner, name)` and a connection `issues` on `Repository`, the table is `repositoryIssues`, SQL name `gh__repository_issues` under namespace `gh`. Filter it through the `_nf_owner` and `_nf_name` columns: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- A connection is never a column. A row would otherwise carry a read the remote computes per row, for every connection its type has.
- A connection is a table only if its field takes `first` and `after`, so it can be read page by page. One that needs an argument of its own is not a table. Neither is a connection of a union, or any connection under a root field that returns a list.

**Type mapping.** Scalar fields map to Provisa types directly. OBJECT fields split into two cases depending on whether the target type is governed (see "Governed tables" below).

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

**Governed tables.** A GQL type is governed when it appears as a root `Query` field in the remote schema. `_collect_queryable_types` collects these during registration, preferring no-required-arg fields so they can be bulk-fetched as join targets.

When an OBJECT-typed column on a governed table points to another governed type, that column is subject to three rules simultaneously:

1. **Excluded from the GQL fetch** — the field is not requested when fetching the parent table's rows.
2. **Excluded from the SDL** — the field does not appear on the parent type in the generated schema.
3. **Accessible only via a declared relationship** — a steward must register a JOIN between the two materialized governed tables. Without one, the field is simply absent; there is no blob fallback.

OBJECT types that are NOT reachable as root Query fields (inline types such as `ContactInfo` or `Address`) follow different rules: they are fetched as `jsonb` blob columns and appear in the SDL as nested-object fields. Sub-fields are accessible via `->>` extraction in SQL.

**Fields that need an argument are not columns.** A field with a required argument cannot be selected bare, so it is left out of the table's columns and out of nested selections.

**Required arguments.** When a root query field has non-null arguments with no default value, those become `native_filter_type: query_param` columns on the table (prefixed `_nf_` at injection time). The executor passes them as GraphQL variables.

**Relationships detected automatically.** Provisa scans each registered table's OBJECT-typed columns. When the GQL type a column refers to is also a registered table in the same source, and the column the relationship rides on is one of the registered columns, the relationship is stored. A table that isn't registered yet gets none. Many-to-one relationships infer their source and target columns from naming conventions (`breedName` on the source type → `name` on the `Breed` target type). One-to-many (LIST) fields produce relationships with empty column references, because the FK lives on the target side.

**Mutations.** A mutation field is registered as a command of kind `source_operation`, one at a time. Its arguments are each typed `json` and passed to the remote as typed variables; the answer is the JSON the remote returns, with no `return_schema`. See [A remote source's write operation](commands.md#a-remote-sources-write-operation).

**Refresh.** POST to `/admin/sources/graphql-remote/{id}/refresh`. Re-introspects the remote schema and brings the already-registered tables up to date with it. It adds no table and no column: a table or column the schema has gained stays on offer, and a column the schema has lost is dropped. Existing governance rules (RLS, masking) are preserved.

**Limitations.**

- Scalar and ENUM root query fields (return type is not OBJECT) become tracked functions, not virtual tables. Their `return_schema` is a single `value` column of the mapped scalar type.
- Object nesting is resolved at registration time up to `graphql_remote.max_object_depth` (default: 5). Both the remote fetch selection and the sub-field metadata are built to that depth; fields beyond the limit are not fetched and are not available for SQL extraction. A type is entered once along any one path: a field whose type is already on the way down is left out, so a schema whose types refer back to each other is walked once per type, not once per depth level.
- LIST-typed nested OBJECT fields (e.g. `breed.awards: [Award]`) are included in the fetch selection up to `graphql_remote.max_list_depth` nesting levels (default: 2). Within that limit, the list is fetched as a `jsonb` array on the parent column. When the list field declares a `first` argument (Relay, PostGraphile, pg_graphql) or a `limit` argument (Hasura), the selection passes it as `first: N` or `limit: N`, where N is `graphql_remote.max_list_items` (default: 100). A list field that declares neither gets no argument, because a remote rejects an argument the field does not declare. Beyond `max_list_depth`, the LIST field is excluded entirely to prevent unbounded data expansion. In SQL, the array is accessed via `json_array_elements(column_name)` or `->>` index extraction. If the list's item type has its own root query, register it as a separate table and create a relationship instead. The join path is more efficient and skips the blob.
- For SQL queries, non-governed OBJECT-typed columns are fetched in full from the remote (all sub-fields up to the configured depth) and cached as `jsonb`. Sub-field access in SQL is handled via `->>`  extraction against the blob; the remote request is not narrowed to only the fields the SQL query selects. When the LIST-item type has no root query and the blob representation is insufficient, write the query in GraphQL SDL directly. Provisa reproduces the GQL field selection, so the remote sees exactly the fields requested.
- If the remote server rejects an OBJECT-typed field because it requires subfield selection (which should not occur when `gql_selection` is available), the executor retries once with those fields removed so scalar columns are still returned. This applies to tables read off a root field. A connection table does not take this path.

**Paged reads.** A connection table is read by cursor. Each page asks for `first: N, after: $pageCursor` with `pageInfo { hasNextPage endCursor }`, and the read follows `endCursor` until the remote reports no next page.

| Setting | Default | Effect |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Rows per page. |
| `graphql_remote.max_rows` | `10000` | The most rows one read of a connection table takes. A read that reaches it stops and logs a warning. |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Two responses make the executor retry:

- **Page too heavy.** When the remote answers 502 or 504, the same page is asked again at half the size, down to one row.
- **Rate limit with a wait time.** When the remote answers 403 or 429 with a `Retry-After` of 120 seconds or less, the executor waits that long and sends the request again, up to three attempts. A refusal with no `Retry-After`, or one asking for a longer wait, is raised as an error. This applies to every read, connection or not.

Any other error in the response fails the read, unless the source type declares otherwise (see GitHub below). A connection whose parent came back null has no rows.

---

### GitHub

GitHub is an ordinary source type. Its API is GraphQL, so its tables behave as described above, including connection tables such as `gh__repository_issues`.

**Add the source.**

1. Open Sources and add a source of type **GitHub**.
2. Enter a GitHub access token. Optionally enter a namespace, the prefix of the table names; the default is `gh`.
3. Save. Provisa checks the token against GitHub. A token GitHub rejects fails the add with GitHub's message.

Adding the source registers no tables.

**Register tables.** Open Tables, then Register Table. Pick the GitHub source, pick schema `graphql`, then pick the tables you want. Every table GitHub offers is listed; the registration is your choice of which to expose.

**Token scopes.** When you register a table, Provisa checks it once against GitHub with your token.

- A field the token's scopes do not cover is left out of the table. The result names each field left out: `Left out, because the source's credential may not read them: projectsV2`.
- A table the token cannot read at all is refused, with GitHub's reason: `GitHub does not let this source's credential read gh__repository_issues: ...`.

**Rows the token may not see.** GitHub answers `FORBIDDEN` for a field the token may not see on one particular row, such as a repository's collaborators without push access, and `NOT_ORG_OWNED_REPO` for a field that exists only on organization-owned repositories. That field is null in that row, the rest of the read stands, and Provisa logs a warning. An error against the table itself fails the read.

**Heavy pages.** When GitHub answers `RESOURCE_LIMITS_EXCEEDED` because a page costs too much to compute, the page is asked again at half the size.

**Nested objects.** GitHub tables use a nesting depth of their own, 0 (`max_object_depth=0` for this source type), instead of `graphql_remote.max_object_depth`. A nested object column is selected with its own scalar fields only, and objects inside it show as `__typename`.

**Token storage.** The token goes to the secrets vault and the source row keeps a reference, so a restart reads the source again without re-entering the token.

**How it works (operators).** GitHub's schema ships with Provisa, so adding the source makes no introspection call and a large schema costs nothing at registration. Tables are mapped from it one at a time as you register them. The refresh endpoint refuses this source type; a new GitHub schema arrives with a Provisa release.

---

### GitLab

GitLab is an ordinary source type and is added and registered the same way as GitHub: add a source of type **GitLab** with an access token, then register the tables you want from schema `graphql`. The default table-name prefix is `gl`. The source reaches `gitlab.com`.

**Choose columns.** GitLab prices every query and refuses one that costs too much: 200 points for an anonymous caller, 250 with a token. A wide table with every column selected is over that price, so register a GitLab table with the columns you want.

- When you register a table, Provisa asks GitLab once whether it will serve the selection at the page size reads use. If GitLab answers that the query is too complex or too large, the table is not registered and the result carries GitLab's message: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.`
- What a column costs depends on its kind. A plain value costs about a point; a nested-object column costs many times that. Dropping nested-object columns frees the most.
- The page size is part of the price. It is `graphql_remote.max_list_items`.

**Token check.** GitLab answers an unrecognized token with an empty result instead of an error. Provisa treats that as a rejected token and doesn't add the source.

---

### gRPC remote schema

**How to add the source.** POST to `/admin/grpc-remote/register` with the server address, a path or URL to a `.proto` file, and optional TLS config. Adding the source registers no table.

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

Provisa fetches the proto, parses it with a pure-text parser (no external proto deps at parse time), compiles Python stubs via `grpc_tools.protoc`, and opens a persistent `grpc.aio.Channel`.

Proto files may also be local paths. Import paths for well-known types (`google/protobuf/timestamp.proto`) are stored at registration time and reused on refresh.

**What the source offers.** Every `rpc` method in the proto is classified as a query or mutation using three signals in priority order:

1. **`method_overrides`** in the registration payload — `{"MethodName": "query"}` or `{"MethodName": "mutation"}` overrides everything else.
2. **`server_streaming: true`** — the server sends a stream of messages; always a virtual table (unless the output is a scalar).
3. **Output message has a repeated message-type field** — e.g. `ListOrdersResponse { repeated Order items; }` is treated as a list-wrapper and becomes a virtual table. Repeated scalar fields (e.g. `repeated string tags`) do not trigger this, because they are array properties on a single entity, not row sources.

Methods that match none of these signals (unary RPC returning a single entity message, or any scalar output) become tracked functions.

**Register tables.** Each query method is offered as one table, named `{namespace}__{Service}__{Method}`, under picker schema `grpc_remote`. Register the ones you want with the Register Table picker (`availableTables`, `availableColumns`, `registerTable`, as for GraphQL sources), choosing response columns. Request fields become `_nf_*` native-filter columns, and these are always included.

Mutation methods are commands on offer, counted in `available_mutations`; adding the source records none. A gRPC mutation is registered on the Commands page by picking the source and then the method, named `Service.Method`; the command's kind is `source_operation`. See [A remote source's write operation](commands.md#a-remote-sources-write-operation).

**Table naming.** The default name is `{namespace}__{ServiceName}__{MethodName}`. Without a namespace, the service and method names are joined directly. Any registered table can be given an `alias`; when set, the alias is the name used everywhere (queries, SDL, relationships). The auto-generated name is the registration key and never changes.

**Type mapping.** Proto scalar types map to SQL types as follows.

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

**Relationships at registration time.** `relationships` works just as it does for the GQL adapter: it declares FK/PK join paths, stored as manually declared relationships (with no `remote_managed` flag). A refresh leaves them as they are.

**Query methods.** Output message fields become table columns. Input message fields both become GraphQL arguments passed to the remote call *and* are registered as `_nf_`-prefixed columns with `native_filter_type: "grpc_input"`, the same mechanism GQL and OpenAPI use for native filter injection.

**Nested message sub-fields.** For query methods, non-repeated message-typed fields at depth 0 (direct output columns) have their sub-fields resolved one level deep and stored as `object_fields` on the `ColumnDef`. This metadata is used for `jsonb` sub-field extraction in SQL and for schema documentation. Fields nested beyond depth 1 are not recursively expanded.

Server-streaming methods collect all streamed messages into a list before returning rows.

**Mutation methods.** A registered mutation method is a command whose arguments are the input message's fields, each typed `json` and passed through unchanged. The remote's answer comes back as the rows; a refused call is a 422, `functions.remote_refused`. See [A remote source's write operation](commands.md#a-remote-sources-write-operation).

**Channel management.** One `grpc.aio.Channel` per registered source is stored in app state and reused across requests. The old channel is closed before a new one opens on refresh.

**Refresh.** POST to `/admin/grpc-remote/refresh/{source_id}`. Re-loads the proto from the stored path, recompiles stubs, and brings the already-registered tables up to date with the proto, with the columns each was registered with. It registers no new table; a query method added to the proto stays on offer. Alternatively, PUT to `/admin/grpc-remote/{source_id}/proto` with new `proto_text` to update the proto inline.

**Limitations.**

- Sub-field object extraction is one level deep. Nested message fields beyond depth 1 are not recursively expanded.

---

### OpenAPI / REST

**How to add the source.** POST to `/admin/openapi/register` with a source ID and a spec, loaded from a local file or URL. The spec is parsed and kept with the source; no table or command is registered. The response reports `tables: 0` and `mutations: 0`, with the counts on offer in `available_tables` and `available_mutations`.

**Register tables.** Register each GET operation wanted through the Register Table picker (`availableTables`, `availableColumns`, `registerTable`), choosing columns. Register each non-GET operation individually as a command on the Commands page, listed by `availableFunctions`; see [A remote source's write operation](commands.md#a-remote-sources-write-operation). `PUT /admin/openapi/spec/{source_id}` stores a hand-edited spec, registers nothing, and returns `available_tables` and `available_mutations`.

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

**What the source offers.** Every GET operation in the spec is offered as a table, unless its response schema is a scalar type (`string`, `number`, `boolean`, `integer`); a GET that returns a scalar is a function with a single `value` column. Every other operation (POST, PUT, PATCH, DELETE) is offered as a command, named by its `operationId`. Once registered, it takes the operation's path parameters and a `body` argument for the request body, each typed `json`, and any other argument goes on the query string. See [A remote source's write operation](commands.md#a-remote-sources-write-operation).

Classification priority: `operation_overrides` (payload) overrides `x-provisa-kind` (spec extension) overrides the GET heuristic. `operation_overrides` is the recommended override path; `x-provisa-kind` is for when the spec itself should carry the classification.

**Relationships at registration time.** `relationships` works just as it does for the other adapters: stored as manually declared relationships and kept on refresh.

**Table naming.** Tables use the operation's `operationId`. If no `operationId` is defined, Provisa slugifies `{method}_{path}`. An alias is derived by stripping the leading verb segment and singularizing the noun (`findPetsByStatus` → `pet_by_status`).

**Type mapping.** JSON Schema types map to Provisa types as follows.

| JSON Schema type | Provisa type |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parameters as native filter columns.** Path and query parameters that aren't already response fields become columns with `native_filter_type` set to `path_param` or `query_param`, prefixed `_nf_`. When a parameter has the same name as a response field, its metadata is merged into that column's entry, so no duplicate is created.

**Response schema resolution.** The mapper checks `responses.200`, then `responses.2xx`, then `responses.default`. Array-type responses are unwrapped to their item schema. `$ref` references are resolved one level deep.

**Object sub-fields.** Response properties with `type: object` and their own `properties` are stored as `object_fields` on the column. These sub-fields are visible in the SDL and used for `jsonb` extraction in queries.

**Response caching.** GET operation results are cached in PostgreSQL by `pg_cache.py`. Each combination of request parameters gets its own `_params_hash` group. Rows for a given hash are replaced when the TTL expires. Path-param endpoints (`/pets/{id}`) skip the initial bulk fetch. The cache table is created empty for schema introspection, then populated per-PK as requests arrive.

**Refresh.** POST to `/admin/openapi/refresh/{source_id}`. Re-parses the spec through `_load_and_register`, which registers nothing: it adds no table and no column. Existing governance rules are preserved. A registered table keeps its columns; it is read through the refreshed spec's operation.

**Limitations.**

- Sub-field object extraction is one level deep. Properties nested inside `object_fields` are not recursively expanded.
- Header and cookie parameters are ignored; only `path` and `query` parameters are registered.
- Spec-level `$ref` resolution is one level deep for property schemas; deeply nested component references may not resolve.

---

## Impact of registering a remote table

A table registered from any remote schema source is an ordinary Provisa table. Nothing about it is treated differently from a locally connected relational table at runtime.

**Query interfaces.** The table is immediately queryable via GraphQL, SQL (pgwire or direct), Cypher (GQL), JSON:API, and Arrow Flight. Schema generation synthesizes `ColumnMetadata` for remote tables since they have no catalog; type mapping is applied at schema build time.

**Security model.** All five governance layers apply:

1. Domain access control — the table's `domain_id` gates which roles can see it.
2. Row-level security (RLS) — row filters configured on the table are injected into every query, regardless of interface.
3. Column visibility — `visible_to` list on each column controls per-role field exposure.
4. Column masking — masking rules apply in Stage 2 of the governance pipeline.
5. Predicate guard — masked columns are rejected from WHERE and HAVING clauses.

Ad-hoc queries against remote tables are allowed on the user's rights alone. Access is the same rights-based model everywhere (table and column rights plus approved relationships), with no governance mode per table.

**Relationship governance (V002).** A JOIN condition against remote tables, in a SQL or Cypher query, must match a registered, approved relationship. GraphQL queries skip the V002 check, because relationships defined in the SDL are approved by design. See [docs/security.md](security.md#relationship-governance-v002).

**OBJECT-typed columns.** When a column maps to a non-governed inline GQL OBJECT or OpenAPI object type, its Provisa type is `jsonb`. The column stores the full nested JSON blob. When sub-fields are declared (`gql_object_fields` or `object_fields`), the `gql_object_columns` map is populated at schema build time. The SQL generator uses this map to emit `->>`  extraction expressions for sub-fields when a query selects them.

**Required args as native filter params.** Root query fields with non-null, no-default args inject additional columns onto the registered table. These columns carry `native_filter_type: query_param`. The Cypher translator rewrites `WHERE n.id = $val` to `WHERE n._nf_id = $val`, and the GraphQL executor picks them up as variables to pass to the remote endpoint.

---

## Impact of creating a covering relationship

When a steward registers a relationship between two remote tables (or between a remote table and a local table), the relationship becomes the join path used at query time.

**How the join wins.** At query compilation, Provisa resolves the join path through the registered relationship. The `source_column` and `target_column` on the relationship become the join condition in generated SQL. The join replaces any per-table remote call that would otherwise be needed for the connected type.

**The raw blob is never exposed in SQL.** You can't select the `breed` column on `petstore__pets` as a raw jsonb value in a SQL query. Once a relationship is registered between `petstore__pets` and `petstore__breeds`, SQL queries traverse the join: `SELECT breed.name FROM petstore__pets` resolves through the FK join. When no relationship is registered but the column has declared sub-fields (`gql_object_fields`), SQL references to sub-fields are rewritten to `->>` extraction against the stored blob. That path exists only for inline types that aren't governed; fields whose target is governed are left out of the SDL entirely and have no blob to extract from. The raw blob itself is never returned as a bare column value.

In GraphQL SDL, an inline OBJECT field that isn't governed is typed as the nested object type. Whether a join or blob extraction serves it at execution time doesn't change the SDL shape. Once the child type is registered as a table of its own, and so governed, all five governance layers apply to it independently: its own RLS rules, column visibility, masking rules, predicate guards and domain access control. Blob extraction skips all of that, because the child data arrives embedded in the parent row and only the parent table's rules govern it. To govern the child type in detail, register it as a table and create a relationship.

**`graphql_alias` on the relationship.** The `graphql_alias` field names the SDL field that the relationship exposes on the parent type. When absent, the name is derived from the target table's `field_name` and the relationship's cardinality via `rel_field_name(target.field_name, cardinality)`.

**V002 on the join path.** SQL and Cypher queries that traverse the relationship are subject to V002 relationship governance. The relationship must be registered and approved for the join to be allowed. GraphQL traversal via the SDL relationship field is always pre-approved.

**Remote-managed flag.** Relationships auto-detected during GraphQL remote registration are stored with `remote_managed: True`. This is a metadata marker; it does not alter governance behavior.

---

## Type-def-only behavior

Not every type in a remote schema needs to be a queryable table.

When `root_table_ids` is set on a `SchemaInput`, tables whose IDs are absent from that set are excluded from the root query fields in the generated SDL. They remain present as GraphQL types and can be reached via relationship fields on tables that do have root entries.

The same mechanism applies when the schema is built filtered by domain. Tables in domains the role can't access are type-defs only: their type definition exists in the SDL for traversing relationships, but no root query field is generated for them.

A type-def-only table:

- Has no root query field, so clients cannot query it directly by name.
- Is reachable via relationship fields on tables that do have root entries.
- Still appears in schema introspection as a named type.
- Still has all governance rules applied when data is accessed through a relationship.

A table, type definition included, leaves the schema completely only when its registration is deleted. Making a table type-def-only (by removing its ID from `root_table_ids`, or through domain access filtering) doesn't remove the type.

This lets stewards expose navigable object graphs in which some types can only be reached by traversal and can't be queried on their own.
