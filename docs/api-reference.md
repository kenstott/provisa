# API Reference

## Overview

Provisa exposes REST endpoints under two prefixes: `/data` for query execution and schema introspection, and `/admin` for configuration management. Most data endpoints require a role identifier. Admin configuration operations use a Strawberry GraphQL API at `/admin/graphql`.

---

## Authentication

When `auth.provider` is configured in `provisa.yaml`, all endpoints except `/health` and `/setup/status` require an `Authorization: Bearer <token>` header.

Without auth configured, the server runs in dev mode. Any request is treated as the `anonymous` identity, which maps to all configured roles with wildcard domain access.

**Login (`POST /auth/login`)** is provided by the active auth provider when `provider: basic` is configured. Credential format and response depend on the provider.

**Identity introspection:**

```http
GET /auth/me
```

Returns the authenticated user's id, email, display name, org memberships, and role assignments. In dev mode returns `dev_mode: true` with all role IDs listed.

```http
GET /auth/provider-type
```

Returns `{"provider": "<name>"}` or `{"provider": null}` when auth is unconfigured.

---

## Data Endpoints

### `POST /data/graphql`

Execute a GraphQL query or mutation.

**Request body:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

The request runs as its acting role: the authenticated user's role, or in dev mode (no auth) the `X-Provisa-Role` header, `org_admin` when the header is absent. The `role` field in the body does not select a role. If it is present it must equal the acting role; a different value returns `400` with code `data.role_mismatch`, naming both roles. The same rule applies to `POST /data/sql`, `POST /data/query` and `POST /query/nl`.

The `extensions` field supports the Automatic Persisted Query (APQ) protocol:

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Headers:**

- `X-Provisa-Role` — the role to run as (dev mode); with auth, a role request honored only when assigned to the user
- `Accept` — response format (see Content Negotiation)
- `Authorization` — `Bearer <token>` when auth is enabled
- `X-Provisa-Redirect-Format` — MIME type for S3 redirect output
- `X-Provisa-Redirect-Threshold` — row count above which redirect triggers
- `X-Provisa-Redirect` — `true` to force redirect unconditionally

**Response cache:** opt-in per request with the `@cached` operation directive (`query @cached(ttl: 60) { ... }`, `ttl` optional); the same directive applies to GraphQL sent over Arrow Flight. Cypher opts in with a `// @provisa cache=true` or `// @provisa cache_ttl=N` line, gRPC with `x-provisa-cache: true` or `x-provisa-cache-ttl: N` call metadata. Without it the request neither reads nor writes the response cache. The `X-Provisa-Cache` response header reports `HIT` or `MISS`. See [Cache](configuration.md#per-request-opt-in).

#### Response cache request headers

`/data/graphql`, `/data/cypher`, the Neo4j Query API (`/db/{database}/query/v2`), `/data/rest` and `/data/jsonapi` also take the opt-in as a request header. Each form below behaves like `@cached(ttl: N)`:

| Header | Same as |
| --- | --- |
| `Cache-Control: max-age=N` or `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale` (no value) | `@cached` (the operator's TTL) |
| `Cache-Control: no-cache` or `no-store` | no cache, the same as sending nothing |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

A request without one of these headers and without a hint in its text is not cached, although plain HTTP caches by default. The operator's cache settings still apply as for `@cached`: a header never makes a read fresher than they allow, and is never refused for asking for a longer `ttl`. Other `Cache-Control` directives (`no-transform`, `min-fresh`, ...) are ignored, as RFC 9111 requires of a cache.

A request can state the opt-in more than once: in its text and in a header, or in two headers. If they agree, it is cached once as asked. If they disagree (for example `@cached(ttl: 60)` with `Cache-Control: max-age=5`, or `x-provisa-cache-ttl: 10` with `Cache-Control: max-age=20`), the request fails with `400` and code `data.invalid_cache_control`, naming each form it sent. A value of `N` that is not a whole number of seconds fails the same way. Neither form is picked over the other.

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

**Response (JSON inline):**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Response (redirect):**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**Response (multi-root with mixed inline/redirect):**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

Multi-root queries run each root field independently. Fields below the redirect threshold return inline; fields above redirect. The `redirects` key (plural) maps field names to redirect info.

**Cache headers:**

- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>` (on HIT)

**Required capabilities:** `QUERY_DEVELOPMENT` for all requests including introspection.

---

### Content Negotiation

| Accept Header | Format |
| --- | --- |
| `application/json` | JSON (default) |
| `application/x-ndjson` | Newline-delimited JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

---

### Redirect

Results above a configured row threshold (or when `X-Provisa-Redirect: true`) are written to S3 and a presigned URL is returned.

| Redirect Format | Written by | Memory |
| --- | --- | --- |
| `application/vnd.apache.parquet` | federated CTAS | None — data never passes through Provisa |
| `application/x-orc` | federated CTAS | None — data never passes through Provisa |
| `application/json` | Provisa | Memory-bound |
| `application/x-ndjson` | Provisa | Memory-bound |
| `text/csv` | Provisa | Memory-bound |
| `application/vnd.apache.arrow.stream` | Provisa | Memory-bound |

For large analytical exports, use the Parquet or ORC redirect. The federation engine writes straight to S3 in parallel, and no data passes through Provisa.

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Execute raw SQL through the Stage 2 governance pipeline.

**Request body:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params` (optional) carries the values of the statement's `$1…$n` placeholders, in order, apart from its text:

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

The values are bound when the statement runs, by the engine or the source's driver, and are never written into the statement, so a value can't change what the statement is. They go with exactly one statement whose placeholders number them exactly. A batch of statements, a placeholder with no value, or a value no placeholder takes is refused with `400` and code `data.sql_parameters_do_not_fit` before the statement is governed or run. A statement without placeholders is sent without `params`, as before.

**Required capabilities:** `QUERY_DEVELOPMENT`.

Governance violations on `POST /data/sql` return HTTP 403.

**Response:** Same format as `/data/graphql` (JSON rows by default, content-negotiated via `Accept`).

---

### `POST /data/query`

One endpoint for every query language. It accepts GraphQL, SQL or Cypher and detects which one it got.

Cypher queries can also be submitted to the Cypher-only `POST /query/cypher` endpoint.

**Request body:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

Returns `{"data": ...}` for GraphQL, `{"columns": [...], "rows": [...]}` for SQL and Cypher.

---

### `POST /data/sql/explain`

Explain or analyze a SQL statement through the governed pipeline.

The endpoint wraps the **governed** SQL in the dialect's EXPLAIN syntax: the statement that actually runs under the caller's role, after RLS and masking. So the plan you see is for the authorized version of the query, not the raw input.

**Request body:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

Set `analyze: true` to run EXPLAIN ANALYZE. The query executes and the plan carries real row counts and timings. Not every dialect supports ANALYZE; see the table in [Query plans and statistics](engines.md#query-plans-and-statistics).

**Response:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` when the dialect has no EXPLAIN support, or `analyze: true` is requested on a dialect that does not support it (e.g. SQLite).

---

### `GET /data/engine/state`

Return the engine shard's current state without waking it.

The UI polls this endpoint to show a startup banner while the engine is cold-starting. It never wakes the engine, so polling is safe and doesn't count as activity for the idle reaper.

**Response:**

```json
{"state": "ready"}
```

Possible values:

| State | Meaning |
| --- | --- |
| `always-on` | Desktop, self-hosted, or BYO coordinator — no lifecycle management |
| `ready` | Shard is up and accepting queries |
| `starting` | Cold-start in progress |
| `stopped` | Shard is scaled to zero |

---

### `POST /data/engine/prewarm`

Trigger an engine wake without running a query.

Returns `202 Accepted` immediately, and the wake runs in the background. Use it when you want the engine ready before the first query arrives, for example from a scheduler that runs queries a few minutes later.

**Response:** `202 Accepted`, body `{"started": true}`

---

### `GET /data/rest/{domain_id}/{table_name}`

Auto-generated plain REST endpoint for every registered table. The query string maps to GraphQL arguments and the request compiles and executes through the same pipeline (RLS, masking, routing) as GraphQL.

**Query parameters:**

- `limit` — max rows (≥ 1)
- `offset` — skip rows (≥ 0)
- `fields` — comma-separated field names as the GraphQL schema spells them, camelCase under the default naming convention, e.g. `?fields=orderId,amount` (defaults to all scalar fields). `filter` and `orderBy` field names follow the same spelling. JSON:API takes the physical column name instead (`fields[orders]=order_id`).
- `filter` — JSON array of `{"field", "comparator", "value"}` filter objects. A value is read as its column's type, so `"100"` on a number column compares as 100; a value its column's type cannot hold returns `400`.
- `orderBy` — JSON array of `{"field", "direction"}` sort objects
- `params` — the table's parameters as one JSON object keyed by parameter name, e.g. `?params={"region":"EU"}`. A table registered over an API, or a query table, has a parameter for each input its source takes. A read that leaves out a required parameter returns `400` naming it, as does a name the table has no parameter for. Each value is checked against its parameter's type.

Filter values and parameter values travel beside the GraphQL text the endpoint builds, as variables, so a value containing a quote is compared as written.

The authenticated role is required; unauthenticated requests return `401`. An OpenAPI spec for these routes is served at `GET /data/rest/openapi.json` with Swagger UI at `GET /data/rest/docs`.

**Response cache:** opt in with a request header, `Cache-Control: max-age=N` or `x-provisa-cache: true` (see [Response cache request headers](#response-cache-request-headers)). Every response carries `X-Provisa-Cache: HIT|MISS`, and `X-Provisa-Cache-Age` on a hit.

#### OpenAPI / Swagger UI Explorer

The OpenAPI explorer page (`/app/openapi`) embeds the Swagger UI in a sandboxed iframe. The spec is scoped to the role, so only tables and columns the current role can see appear, and the domain selector can narrow it further. The UI switches between light and dark themes by itself.

The page loads the spec HTML with `fetch()` instead of a direct iframe `src`. That way the request carries the session's bearer token, and Swagger UI's own relative requests resolve against the same origin.

Opened from an NL "Open in OpenAPI" link, the page expands the target endpoint, fills in the query parameters from the NL-generated URL (e.g. `aggregate`, `groupBy`) and clicks Execute, polling the DOM so that each step finishes before the next starts.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Auto-generated [JSON:API](https://jsonapi.org)-compliant endpoint for every registered table. Same RLS, masking, and routing as GraphQL.

**`Accept` header:** must include `application/vnd.api+json` (the JSON:API media type) or the request returns `406`.

**Query parameters:**

- `fields[<type>]` — sparse fieldsets, e.g. `?fields[orders]=amount`. Names are the physical column names, e.g. `fields[orders]=order_id`, as are `filter[...]`, `sort` and the emitted attribute keys. REST takes the GraphQL spelling instead (`fields=orderId`).
- `filter[<col>]` / `filter[<col>][<op>]` — e.g. `?filter[region]=US`, `?filter[amount][gt]=100`. A value is read as its column's type; a value the type cannot hold returns `400`.
- `params` — the table's parameters as one JSON object keyed by parameter name, e.g. `?params={"region":"EU"}`, as on the REST endpoint. A missing required parameter returns `400` naming it; an unknown name or a value of the wrong type returns `400` with `source.parameter` set to `params`. Pagination links keep the parameter.
- `sort` — comma-separated, `-` prefix for descending, e.g. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — pagination
- `page[total]` — `true` counts the rows the filter matches and returns `meta.total` and `links.last`. Without it the response carries `links.self`, `links.first`, `links.prev` and `links.next` only, and the database runs no count. Any value other than `true` or `false` returns `400`. Links in a response keep the parameter.
- `aggregate` — comma-separated aggregate functions to run instead of row retrieval: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Use `?aggregate=count,sum` to request a subset. Aggregate responses return `data: null` with results in `meta.aggregate`.
- `groupBy` — comma-separated column names; used with `?aggregate=` to group results. Only columns in the table's `DistinctOnColumn` enum are valid; the server returns `400` for any column the role cannot see.
- `includeNodes` — `true` to include base-table scalar columns (and joined dimension scalars named in `include=`) inside each group row's `nodes` array. Required when an NL group-by query also requests dimension details.

- `include` — comma-separated relationship names, physical spelling, e.g. `?include=order_items,shipment`: each related resource is returned once in `included`, and each primary resource links what it includes under `relationships.<name>.data` (a list of resource identifiers for a to-many relationship, one identifier or `null` for a to-one).

Responses are resource objects with `type`/`id`/`attributes`. A resource's `id` is its table's primary key: the key column's value, or for a composite key its columns' values in key order joined by `:` (order 7's line 2 of a table keyed `(order_id, line)` is `7:2`). A `:` or `\` inside a value is escaped with `\`. A table with no declared primary key uses its `id` column; a table with neither returns `400`, as does an `include` that reaches one. Errors follow the JSON:API error object shape.

**Response cache:** opt in with a request header, `Cache-Control: max-age=N` or `x-provisa-cache: true` (see [Response cache request headers](#response-cache-request-headers)). Every response carries `X-Provisa-Cache: HIT|MISS`, and `X-Provisa-Cache-Age` on a hit. Two forms that disagree return `400` as a JSON:API error object.

#### JSON:API Explorer

The JSON:API explorer page (`/app/jsonapi`) is a browser UI over these endpoints. Select a table from the domain-grouped list, then configure:

- **Fields** — choose which columns to include (sparse fieldset); leave all unchecked to request every column
- **Relationships** — select FK-derived relationship names to sideload via `?include=`
- **Filter** — field, operator (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`), and value
- **Sort** — one field, ascending or descending
- **Aggregate** — pick group-by columns from the server-validated list, then check one or more aggregate functions; when group-by columns are selected, an "Include nodes" checkbox appends base-table scalar columns to each row
- **Page size** — resources per page, with first/prev/next/last navigation

Results render in a formatted summary view (resource cards with clickable relationship anchors) or a raw JSON tab. The live request URL is shown and can be copied. Table selection and page size persist across sessions in `localStorage`.

When navigated from an NL "Open in JSON:API" link, the explorer pre-selects the table and seeds the aggregate picker from the NL-generated query parameters, then auto-runs the request.

---

### `POST /query/nl`

Submit a natural-language question. The service starts an async job and returns `202 Accepted` with a `job_id` immediately. Requires an LLM provider configured under the `ai_models` config section.

**Request body:**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

Returns `{"job_id": "<id>"}`. Exceeding the per-role NL rate limit returns `429` with a `Retry-After` header.

**Retrieve the result:**

- `GET /query/nl/{job_id}` — poll. Returns the job document.
- `GET /query/nl/{job_id}/stream` — SSE. One `branch` event per generation target as it completes, then a `done` event.

Three generation loops (Cypher, GraphQL, SQL) run in parallel, each validated through the compiler and refined on error. The prompt is scoped to the role's visible schema. The result document keys each branch by target:

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

A branch that runs out of iterations returns `query: null`, `result: null` and an `error` string. Every generated query runs under the consumer's rights with Stage 2 governance applied; the service never goes around governance.

#### NL Group-By with Dimension Details

Take an NL group-by query that also projects columns from a joined dimension table, such as "count of inquiries by user with user name and email". The runner derives per-field dot-paths (`dim_paths`) from the dimension columns in the SELECT. Those paths fill the `includeNodes=` parameter in the URLs generated for the JSON:API and OpenAPI panels, so those panels ask for the same joined-dimension fields the SQL and GraphQL branches resolved. Without them, `includeNodes=true` would return only the base aggregate table's own scalar fields.

On the gRPC panel, the generated `{Type}GroupByRequest` carries `include_nodes` (bool) and `include` (repeated string of relationship field names). The returned `{Type}GroupByRow` includes a typed `nodes` field with the dimension-detail rows.

---

### `GET /data/sdl`

Return the GraphQL SDL for a role's schema.

**Headers:** `X-Provisa-Role: <role_id>` (required)

**Query parameters:**

- `domain` — comma-separated domain IDs. When set, the response is filtered to the named domain(s) and tables reachable from them.

**Response:** `text/plain` GraphQL SDL.

---

### `GET /data/introspection`

Return GraphQL introspection JSON, optionally domain-filtered.

**Headers:** `X-Provisa-Role: <role_id>` (required)

**Query parameters:** `domain` — comma-separated domain IDs.

**Response:** `application/json` introspection result.

---

### `GET /data/graph-schema`

Return the graph view of the role's schema: node labels and their relationship types, for Cypher/graph clients. Includes `pk_columns` per node label so callers can determine primary-key columns.

**Response:** `application/json` with `node_labels` (each carrying `pk`/`pk_columns`) and `relationship_types`.

Each relationship type also carries `junction_table_name` and `properties`. On a junction-backed edge, the first names the associative table it traverses, and the second lists that table's columns readable as `r.attr` and filterable in `WHERE`. On a foreign-key-backed edge the name is `null` and the property list is empty, which is how a client tells the two apart. The junction table itself is never a node label. It is the edge, so it has no pill in a graph client and no row in `node_labels`.

---

### `GET /data/domains`

Return domain IDs accessible to the requesting role.

**Headers:** `X-Provisa-Role: <role_id>` (required)

**Response:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Return the current schema version string. Combines a per-boot nonce with a rebuild counter. Clients use this to invalidate schema caches after server restarts.

**Response:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

Return the catalog of the role the request runs as: the tables that role is served, as the Arrow Flight listing gives them.

**Response:** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`. `schema` is the table's Arrow schema in the Arrow IPC encoding: each column under the name SQL reads it by, with its registered type, its description, and its key metadata (`primary_key`, `references`) in the field metadata. Commands and metrics are not listed.

The role is the one the auth layer established (`X-Provisa-Role` may request a held role, or a comma-separated set). A deployment with no auth provider lists the whole catalog, as the Flight listing does. The JDBC driver reads this route when the Flight port is unreachable.

---

### `GET /data/proto/{role_id}`

Return the auto-generated `.proto` file for a role.

**Response:** `text/plain` protobuf schema.

`{role_id}` is one role the caller holds, or a comma-separated set of held roles (`analyst,org_admin`), which is served as the set's meta-role: the union of what its members are granted. A role the caller does not hold is refused with `403 auth.role_not_assigned`, and a meta-role id named directly with `403 auth.meta_role_named`. `?domains=a,b` narrows the proto to those domains; a domain the role does not reach is refused with `403 data.domain_not_accessible`. A role with no data surface answers `404 data.no_proto_for_role`, an unknown role `404 data.no_role`, and a model that has not been built yet `503 data.schema_cache_not_ready`.

Each table in the role's schema produces a proto `message`. Relationships produce nested message fields. The service lists RPCs only for the tables at the root of the role's schema, the same tables its GraphQL `Query` type lists; a related table has its message and is reached through its relationship.

Add `?domains=a,b` to restrict the file to those domains. The service then lists RPCs for the chosen domains' own tables; tables related to them appear as messages.

The file follows the model. It is rebuilt with the role's schema whenever the model or the role changes, a meta-role's included. Type mapping: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`.

---

### `GET /data/subscribe/{table}`

Server-Sent Events stream for real-time change notifications from a table.

Notification delivery uses a pluggable provider chosen per source type: PostgreSQL sources use `LISTEN/NOTIFY` (via asyncpg), MongoDB sources use Change Streams (`collection.watch()`), and Kafka sources use consumer groups. Each provider implements a common async watch interface. RLS filtering and schema validation apply regardless of provider. WebSocket and RSS sources are also supported.

**Header — `X-Provisa-Sink`:** Set to a Kafka target (e.g. `kafka://broker:9092/topic`) to redirect change events to a Kafka sink instead of the SSE response. The server launches a sink consumer and returns `202 Accepted` rather than an open stream.

### `POST /data/sources/{source_id}/signal`

Tells Provisa that a source changed, with no rows: the receiver of the `signal` change signal. Each table of the source whose change signal is `signal` (its own, else its source's) is treated as written, as a write through Provisa is: its cached responses are dropped, the materialized views over it go stale, and a build of its replica is asked for. A load-protected table's build waits for its off-peak window. `?table=schema.table` signals one table.

The caller's role needs the write right and must reach the domain of every table it signals. The answer is `202 Accepted` with the tables signalled:

```json
{"signalled": ["public.orders"]}
```

A source that does not exist, or a `table` it does not have, answers `404`; a source with no table on the `signal` change signal answers `409`, because nothing would be refreshed.

---

## Admin REST Endpoints

### Config

#### `GET /admin/config`

Download the current `provisa.yaml` as `application/x-yaml` with a `Content-Disposition: attachment` header.

#### `PUT /admin/config`

Upload a revised config YAML. The server writes a `.bak` backup, saves the new file, reloads the deployment's settings from it, and applies it to the model store as an explicit one-time seed: what the file declares is added or updated through the model store, and nothing is removed. A later restart reads the file's settings and never applies its model again; the model store alone owns the model after the first start.

**Request body:** Raw YAML content.

**Response:**

```json
{"success": true, "message": "Config uploaded and applied"}
```

On reload failure: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Download the model as it stands, written as a configuration file. It holds the deployment file's settings, with every model section read from the model store: stores, regions, sources, domains, roles, data products, glossary terms, tables and their columns, relationships, metrics, tags and their assignments, row filters, commands, webhooks, scheduled triggers and naming rules. Applied to an empty model store, the file builds the same model. What the deployment seeds into every store (the built-in sources and their tables, the system domains, `org_admin` and `platform_admin`) is left out.

**Query parameter:** `sections` (optional) — a comma-separated list of model sections, e.g. `sources,tables`. Only those sections are written; an unknown name answers 400 `settings.export_unknown_section`.

The endpoint is open in every deployment to a caller with the deployment-settings right; the Configuration File section of the admin page exposes it as **Export Model**.

Returns `application/x-yaml` with `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Returns both sides of the config diff, `original` (the startup baseline) and `current` (the live state), normalized the same way, so the comparison shows only real changes and ignores reordering and comment drift.

**Response:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Generate a unified-diff patch from the baseline to the posted config.

Send the revised YAML as the request body. The response is a `text/x-patch` file (`provisa.config.patch`) that `git apply` or `patch` can apply as it is, which is handy for committing config changes made in the UI through a CI/CD pipeline.

---

### Settings

#### `GET /admin/settings`

Return current platform settings as JSON.

**Response:**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

Updates platform settings at runtime. Every field is optional, and only keys present in the body are changed.

**Request body (partial example):**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

Updatable fields per section:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — writes to config file and triggers schema reload
- `relationships`: `auto_track_fk` — governs foreign-key tracking only. A junction-backed relationship is declared on the table registration and is never inferred, so this setting does not reach it.
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Response:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### AI Models

#### `GET /admin/ai-models`

Return the acting org's AI-model assignments, vector-model registry, and NL rate limit.

**Response:**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

API keys are never sent back: `api_keys_set` says only whether each vendor has a key configured. Changes apply from the next request, with no restart.

#### `PUT /admin/ai-models`

Update the org's AI-model assignments, vector-model registry, or NL rate limit. Takes effect on the next request.

#### `GET /admin/ai-models/vendors/{vendor}/models`

Return the model names a vendor currently serves, for the model picker.

The list is read live from the vendor's own list-models API, with the org's configured key, or with the deployment credential when the org has no key. A model released after this build shipped can be selected the same day the vendor starts serving it.

Returns `400` when the vendor publishes no list-models API (enter the model name directly in that case) or when no key is available.

---

### Federation Engine

#### `GET /admin/federation-engine`

Return the current federation-engine selection, its connection config, and the full selectable-engine registry.

**Response:**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

The `current` key is the engine running right now; `persisted` is what is written to the config file and will load on the next restart. They diverge when the config was changed but the service has not restarted yet.

#### `PUT /admin/federation-engine`

Persist a federation-engine selection.

**Request body:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

The selection is written to the platform config and takes effect after the next service restart, because the engine is chosen once at boot.

---

### Domain Policy

#### `POST /admin/domain-policy`

Change the acting org's domain policy (`use_domains` / `default_domain`).

The switch removes nothing. It is refused with `409` and code `settings.domain_policy_catalog_exists` while the acting org has a catalog: a registered table, a source or a domain the deployment did not seed. The refusal's `params` count each kind (`tables`, `sources`, `domains`). Delete those objects first, each through its own action, then switch. When a config file declares any of that catalog the code is `settings.domain_policy_catalog_in_config`: the next load would restore it, so the policy is set in the config file.

**Request body:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` clears the org's override and falls back to the deployment-level setting. `use_domains: false` requires `default_domain` (the single domain name all tables land in). The catalog rebuild is synchronous; the response returns once schemas are ready.

---

### Observability

#### `GET /admin/traces/recent`

Return up to N recent completed spans from the in-memory span buffer.

**Query parameters:** `limit` (default 50, max 200)

**Response:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Hot-reload a named catalog in the federation engine coordinator via its REST API. Reconnects Provisa's internal connection and re-runs OTel DDL.

**Query parameters:** `catalog` (default `"otel"`)

**Response:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Restart the federation engine container (single-node dev only).

**Query parameters:** `container` (defaults to `QUERY_ENGINE_CONTAINER` env var, then `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Trigger relationship discovery. Always runs FK introspection from the federation engine. Runs LLM inference if `ANTHROPIC_API_KEY` is set.

**Request body:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` must be one of `"table"`, `"domain"`, `"cross-domain"`. For `"table"` scope, `table_id` (integer) is required. For `"domain"` scope, `domain_id` is required.

**Response:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

List pending relationship candidates.

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Accept a candidate and register it as a relationship.

**Request body (optional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Reject a candidate.

**Request body:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Return count of rejected candidates.

#### `DELETE /admin/discover/candidates/rejected`

Delete all rejected candidates.

---

### Source Crawl

#### `POST /admin/sources/crawl`

Crawl a data source to introspect its schema and register tables.

---

### Source Table Search

#### `GET /admin/sources/{source_id}/tables/search`

Search available (not yet registered) tables in a source by name.

---

### Table Profiling

#### `POST /admin/tables/{table_id}/profile`

Runs a column profile on a registered table: cardinality, min/max and null rates.

---

### Source Descriptions

#### `POST /admin/source-meta/db-description`

Generate LLM-assisted descriptions for a source's tables and columns.

---

### Object Storage

#### `GET /admin/org-storage`

Report the acting org's storage footprint against its platform allowance, and whether the org has registered its own store.

When the org has registered its own DSN, its materializations go there and are no longer counted against the allowance. The DSN itself is never returned.

#### `PUT /admin/org-storage`

Register (or clear) the org's own materialization store.

**Request body:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

The DSN is checked against the federation engine before it's accepted, so an unusable DSN fails at registration instead of hours later during a refresh. The value is encrypted at rest and never returned by GET.

Send `storage_url: null` to clear the org's own store and return its materializations to the platform store (and allowance). The org runtime is rebuilt in the same call, so the new store is effective immediately.

---

### Org Encryption

#### `GET /admin/org-encryption`

Return the org's current key status: fingerprint, id, and provenance. Never returns key material.

When the org has set no key, returns `{"configured": false}`. Every org starts in this state and inherits the deployment's key.

#### `PUT /admin/org-encryption`

Set or rotate the org's at-rest encryption key.

**Request body:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Leave out `key_b64` to have Provisa generate a key. That's the safest choice, since the key never shows up in a clipboard or a request log. Supplying `key_b64` brings your own key.

Rotation adds a new active entry to the key ring and retains the old one, so data written under the previous key remains readable. Rotation is not re-encryption. There is no delete endpoint: retiring the last key would make every wrapped payload unreadable.

The live ring is rebound in the same call, so the next encrypted write uses the new key immediately.

---

### Hasura / DDN Import

#### `POST /admin/import/hasura/preview`

Convert a Hasura v2 or DDN project archive into proposed Provisa config without writing anything.

**Request body:**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` is `"auto"` (detected from archive structure), `"hasura_v2"`, or `"ddn"`.

**Response:**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

Nothing is persisted. The preview is not cached server-side; `apply` takes the YAML you supply, so what applies is exactly what was reviewed (and optionally edited).

#### `POST /admin/import/hasura/apply`

Load a previously previewed config into the acting org.

**Request body:**

```json
{"config_yaml": "<yaml string>"}
```

An import is an explicit one-time seed of the acting org's model store, the same apply as `PUT /admin/config`: it adds and updates what the imported config declares through the model store, guards and versions included, and removes nothing it does not mention. There is no `replace` field. The engine catalogs of the sources it declares, the org's pools and its schemas are rebuilt before the response returns.

**Response:** `{"summary": {...}}` — the counts of what the imported config declares.

---

### Apache Ossie Interchange

#### `GET /admin/ossie`

Export the org's governed model as an Apache Ossie (incubating) YAML document.

The document is built from live state on every request and never cached, so it can't be stale. Tables become `dataset` objects, columns become `field` objects, and relationships map to Ossie `relationship` objects.

Returns `text/yaml` with `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

#### `GET /admin/dbt/sources`

Download the governed catalog as a dbt `sources.yml`, as one role is served it.

**Query parameter:** `role` — a role the caller holds, or any role for the holder of `access_config`.

Returns `text/yaml` with `Content-Disposition: attachment; filename=provisa.sources.yml`. An unknown role is a `404` with code `dbt_sources.unknown_role`. See [integrations](integrations.md#dbt-sources-file) for what the file contains.

#### `POST /admin/ossie/import`

Parse an Ossie YAML or JSON document and return proposed table and relationship registrations.

**Request body:** Raw Ossie YAML or JSON. Format is auto-detected.

**Response:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

Nothing is registered. Use the admin UI's review screen to accept or trim proposals before any mutation fires.

---

### Actions (Functions and Webhooks)

All endpoints are under the `/admin/actions` prefix.

Every invocation, whether from GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` or Provisa gRPC, goes through one governed executor. It enforces command assignment (`visible_to`), the `write` right for a mutation, and governance, the same way each time. See [docs/integrations.md](integrations.md#invoking-commands-across-protocols) for the call syntax of each protocol.

#### `GET /admin/actions`

Return all tracked DB functions and webhooks.

**Response:**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

Each webhook object carries an `approved` boolean. A webhook is approved once a steward executes its creation request; config-declared webhooks are auto-approved. An unapproved webhook is registered but not exposed on any surface.

#### `POST /admin/actions/functions`

Register a tracked function (command).

**Key fields:**

| Field | Required | Description |
| --- | --- | --- |
| `name` | Yes | Unique command name |
| `kind` | Yes | `"query"` → GraphQL Query field; `"mutation"` → Mutation field |
| `implKind` | No | How the command runs — see table below (default `source_procedure`) |
| `binding` | No | `implKind`-specific connection details (JSON object) |
| `returnSchema` | No | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — makes the command set-returning on every surface |
| `arguments` | No | `[{name, type}]` argument definitions; positional order matters for SQL and Bolt callers |
| `visibleTo` | No | Role IDs the command is assigned to (empty: every role). A `mutation` also needs the `write` right |
| `domainId` | No | Domain for GraphQL placement and access control |

**`implKind` values:**

| `implKind` | What runs | `binding` fields |
| --- | --- | --- |
| `source_procedure` | Stored procedure on a registered source (default) | `sourceId`, `schemaName`, `functionName` |
| `script` | Server-side script | `script` |
| `http` | Outbound HTTP call | `url`, `method` |
| `grpc` | Outbound gRPC call to an external server | `target`, `method` |
| `python` | Python callable hosted by Provisa | `callable` (e.g. `"demo.py_functions:random_dataset"`) |

The demo commands `random_python_set` (`implKind: python`) and `random_grpc_set` (`implKind: grpc`) show set-returning commands with `returnSchema` in practice; both are in `config/provisa-install.yaml`.

#### `PUT /admin/actions/functions/{name}`

Update a tracked function by name.

#### `DELETE /admin/actions/functions/{name}`

Delete a tracked function by name.

#### `POST /admin/actions/webhooks`

Registers a tracked webhook. Registering or updating a webhook queues a steward approval request, and the webhook becomes active on every surface only once a steward approves it. Webhooks declared in config are approved automatically. **Request body fields:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`.

#### `PUT /admin/actions/webhooks/{name}`

Update a tracked webhook by name. Any edit resets approval to pending until re-approved.

#### `DELETE /admin/actions/webhooks/{name}`

Delete a tracked webhook by name.

#### `POST /admin/actions/test`

Test an action (function or webhook) by name.

---

### Roles

All endpoints are under the `/admin/roles` prefix.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/admin/roles/` | List all roles |
| `POST` | `/admin/roles/` | Create a role |
| `PUT` | `/admin/roles/{role_id}` | Update a role |
| `DELETE` | `/admin/roles/{role_id}` | Delete a role |

---

### Users

All endpoints are under the `/admin/users` prefix.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/admin/users/` | Create a local user |
| `GET` | `/admin/users/` | List local users |
| `GET` | `/admin/users/{user_id}` | Get a user |
| `PUT` | `/admin/users/{user_id}` | Update a user |
| `PATCH` | `/admin/users/{user_id}/password` | Change password |
| `DELETE` | `/admin/users/{user_id}` | Delete a user |
| `GET` | `/admin/users/{user_id}/assignments` | List role assignments |
| `POST` | `/admin/users/{user_id}/assignments` | Add a role assignment |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Remove a role assignment |

---

### Organizations

All endpoints are under `/admin/orgs`.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | List orgs |
| `POST` | `/admin/orgs/` | Create an org |
| `PUT` | `/admin/orgs/{org_id}` | Update an org |
| `DELETE` | `/admin/orgs/{org_id}` | Delete an org |
| `GET` | `/admin/orgs/{org_id}/members` | List members |
| `POST` | `/admin/orgs/{org_id}/members` | Add a member |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Remove a member |

---

### Invites

All endpoints are under `/admin/invites`.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Create an invite |
| `GET` | `/admin/invites/` | List pending invites |
| `DELETE` | `/admin/invites/{token}` | Revoke an invite |

---

### Admin GraphQL

#### `POST /admin/graphql`

Strawberry GraphQL endpoint for all admin operations: source and table CRUD, relationship management, domain configuration, RLS rules, cache control, naming conventions, scheduled task management, and query compilation.

For the full schema reference — every Query field, Mutation field, and input/output type — see [Admin GraphQL API Reference](admin-graphql.md).

**Key mutations:**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

---

### Setup

#### `GET /setup/status`

Return first-run setup status. Always unauthenticated.

#### `POST /setup/`

Complete first-run setup.

---

## Health Check

#### `GET /health` or `HEAD /health`

Returns `{"status": "ok"}`. Always unauthenticated.

---

## Error Responses

| Status | Meaning |
| --- | --- |
| 400 | Invalid query, validation error, or SQL parse error |
| 401 | Missing or invalid auth token |
| 403 | Insufficient capabilities; governance violation |
| 404 | Role, resource, or config file not found |
| 422 | Missing required header (e.g. `X-Provisa-Role`) |
| 503 | Database or source not connected; dependency unavailable |
| 504 | Request timed out |

Governance violations on `POST /data/sql` return HTTP 403 with a structured body:

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

All other errors use: `{"detail": "<message>"}`.

---

## Arrow Flight Endpoint

Port `8815`. Native Arrow columnar transport over gRPC.

Queries and catalog discovery are both available on the same connection. The full governance pipeline (RLS, masking, sampling) is applied to every query.

Catalog discovery (`list_flights`, `get_flight_info`, `get_schema`, and a ticket with no `query`) lists what the caller's role is granted. With authentication on, the three ticketless calls take the credential from the `authorization: Bearer` call header and the role from `x-provisa-role`, and are refused without a credential; see [Integrations](integrations.md).

**Ticket format** (JSON):

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Usage (Python):**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

When the Zaychik Flight SQL proxy is available (port 8480), record batches stream end-to-end without full materialization. Falls back to materializing via the federated query layer if Zaychik is unavailable.

**Warnings.** What an answer says about itself rides the stream as the `app_metadata` of a zero-row batch: `{"provisa_warnings": [{"code", "params", "message"}]}`. Warnings known before the rows are read come in a zero-row batch ahead of them. A warning known only once the rows are read comes in a second zero-row batch, the last of the stream: `statement.rows_cut` says the answer was cut at a row limit and more rows match. Read `app_metadata` on every batch, the last included.

---

## Protobuf gRPC Endpoint

Port `50051` (override with `GRPC_PORT` env var or `server.grpc_port` config).

Pass the role in the `x-provisa-role` gRPC metadata key. If absent, the server aborts with `UNAUTHENTICATED`.

Download the role-specific proto from `GET /data/proto/{role_id}`. Only tables and columns visible to that role appear.

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Each table at the root of the role's schema produces a `Query{TypeName}` streaming RPC. These are the tables the role's GraphQL `Query` type lists. A table the role reaches only through a relationship has no RPC of its own, as it has no root field in GraphQL. `Insert{TypeName}` RPCs exist for schema symmetry but abort with `UNIMPLEMENTED`.

**Parameters.** A table with parameters (a table registered over an API, or a query table) has a `{TypeName}Params` message, one optional field per parameter named without its `_nf_` prefix, and `Query{TypeName}Request` carries it as `params`. Each value set there reaches the source as the parameter, bound into the statement; a name that is not one of the table's parameters is refused with `INVALID_ARGUMENT`, and a read that leaves out a required parameter is refused naming it. The HTTP gRPC proxy takes the same values as the body's `params` object (HTTP 400, code `data.invalid_parameter`, for an unknown name or a value of the wrong JSON type).

A method that is not part of the caller's role's service answers `UNIMPLEMENTED`.

`grpc_reflection.v1alpha` is enabled for service discovery without a pre-compiled proto. Reflection describes each caller the service of the role it is acting as: the same `.proto` that role downloads from `GET /data/proto/{role_id}`. It lists the Provisa service and the reflection service by name, and describes the Provisa service's file.

A reflection call is identified like any other gRPC call. With authentication on, the role comes from the caller's credential. With authentication off, name the role in `x-provisa-role`; on a deployment that serves more than one organisation, also name the organisation in `x-provisa-org`. A call without them is refused as `UNAUTHENTICATED`. On a deployment with authentication off, reflection stays disabled unless `server.grpc_allow_unsecured_reflection` is set.

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

The gRPC server starts when the model loaded at startup defines a gRPC service. Each call is then served from the model as it stands: a table registered or changed while the server is running is served and described by the next call, with no restart.

#### Aggregate and Group-By RPCs

When a table has `enable_aggregates` set, the generated proto includes two additional RPCs alongside `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — returns aggregate scalars for the table (`count`; `sum`, `avg`, `stddev`, `variance` per numeric column; `min`, `max` per comparable column)
- **`Query{TypeName}GroupBy`** — returns one row per group key with aggregate sub-fields and, optionally, base-table scalars and joined-dimension rows in a `nodes` field

Both go through the same compiler aggregate pipeline as GraphQL's `{field}_aggregate` and `{field}_group_by` root fields, so there's no separate aggregate implementation.

**`funcs` field.** The request message accepts a `funcs` repeated-string field. Valid values are `count`, `sum`, `avg`, `stddev`, `variance`, `min`, and `max`. When `funcs` is omitted, every function the schema exposes for that table is requested. When set, only the named functions appear. If none of the named functions apply to the table's column types, the query falls back to `count`.

**`include_nodes` and `include` fields.** `Query{TypeName}GroupBy` requests may set `include_nodes: true` to include base-table scalar columns in each row's `nodes` field. The `include` repeated-string field names many-to-one relationship fields whose scalar columns are also nested inside `nodes`. This matches the JSON:API `?includeNodes=` / `?include=` behavior.

---

## JDBC Driver

The Provisa JDBC driver (`provisa-jdbc-<release>.jar`) exposes the semantic catalog to BI tools (Tableau, PowerBI, DBeaver).

**Connection URL:** `jdbc:provisa://host:port`

Domains map to JDBC schemas. Tables use their registered aliases. Columns use aliases and surface descriptions as `REMARKS`. Standard metadata methods (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) expose semantic relationships as PK/FK metadata.

**SQL support:** `SELECT * FROM <alias> [WHERE col = 'value']`.

The driver requests Arrow IPC redirect by default. Results stream batch-by-batch via `ArrowStreamReader`, bounded to one record batch in memory.

---

## `orderBy` Argument Format

The `order_by` argument uses `{column: direction}` objects with a 6-value direction enum:

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Supported directions: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`.

---

## Subscriptions

SSE subscriptions are available at `GET /data/subscribe/{table}`. Notification delivery uses a pluggable provider selected per source type: PostgreSQL sources use `LISTEN/NOTIFY`, MongoDB sources use Change Streams, and Kafka sources use consumer groups. RLS filtering and schema validation apply regardless of provider. WebSocket and RSS sources are also supported via the same endpoint.

---

## Business Glossary

The business glossary maps physical field names, as they appear in the source databases, onto a shared human vocabulary. Every column registered in the model gets a term automatically, so nobody has to enter anything to fill the glossary. Curators add definitions, relationships and experts on top of what the system derives.

### How Terms Are Derived

When Provisa registers or updates a table's columns, `normalize_term` (`provisa/core/glossary.py`) runs on every column name and produces a canonical phrase.

Normalization applies five rules in sequence:

1. Split on camelCase boundaries and separator characters (`_`, `-`, `.`, `/`, whitespace).
2. Case-fold the result to lowercase.
3. Expand a fixed abbreviation table (e.g. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Strip a trailing **proxy token** (`identifier`, `code`, `index`, or `reference`) — a column named for its key or code is pointing at the underlying concept through a stand-in value, so the term should be the concept itself. The last remaining token is never stripped.
5. Qualify a **too-generic phrase** with the table's concept. When the full normalized phrase is a bare attribute word (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, and similar), the term becomes `<table concept> <phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. One shared `name` term across unrelated tables would merge distinct meanings; qualification connects each column to its enclosing concept instead. The table concept is the table's business name, normalized with a singular head noun (`order_lines` → `order line`).

Native-filter pseudo-columns (`_nf_`-prefixed, or any column carrying `native_filter_type`) are query-parameter machinery, not business fields, and derive no terms.

Because `id`, `key`, `pk`, and `sk` all expand to `identifier` before the proxy check, three physically different column names land on exactly the same term:

| Physical name | After normalization |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

The first three collapse to one term. `transaction amount` keeps both tokens because `amount` isn't a proxy. A bare `id` column, with no tokens before it, can't be stripped, so it normalizes to `identifier` and the term isn't empty.

### Lifecycle

Terms are **derived from semantic-layer membership**; users don't create them on demand. The table repository is the one write path: `sync_table_refs` runs inside every column-set upsert, and `sweep_refless_terms` runs after any deletion path.

**When a column is added:** Provisa looks up the normalized term by name. If it already exists, the column gets a ref to it (and if the term was deprecated, it is revived — `deprecated` is set back to `False`). If no term exists yet, one is created.

**When a column departs** (a schema change or a table removal), its ref is deleted and the term is **settled** under a remove-or-deprecate rule. A rooted term with no refs left is removed outright, with its edges and expert assignments, unless removing it would leave an abstract term with no path through the term graph to any rooted term. In that case the term is **deprecated** (marked `deprecated=True`) instead of deleted, so the abstract term keeps its anchor in the graph.

Abstract terms are never auto-removed; they exist outside the physical lifecycle and are only deleted explicitly via the admin API.

**Revival:** if a deprecated term's normalized name reappears (a column is re-registered), the term is unmarked and its refs resume accumulating.

### Curation Endpoints

All endpoints are under `/admin/glossary`. They require `org_admin` access and a configured org. Every mutation triggers a metadata publish.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | List terms. Query params: `q` (name/definition search), `include_deprecated` (default `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Get term detail: definition, physical refs, typed edges, experts |
| `POST` | `/admin/glossary/terms` | Create an abstract term — user vocabulary with no physical refs |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Rename, set definition, or toggle export exclusion |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Delete a term that has no physical refs |
| `POST` | `/admin/glossary/refs/move` | Move one physical ref to a different term (consolidation) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Add a typed relationship edge between two terms |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Remove an edge (query params: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Tag a user as an expert or author for a term |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Remove a user's expert/author designation |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Draft a definition for one term using the org's AI model — returns text only, nothing persists until saved |
| `POST` | `/admin/glossary/definitions/generate` | Generate and persist definitions for every term that has none — never overwrites human-authored text |
| `POST` | `/admin/glossary/relationships/generate` | Propose and persist typed edges across the whole glossary using the org's AI model |

**`POST /admin/glossary/terms` body:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**`POST /admin/glossary/terms/{term_id}/edges` body:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Valid `rel_type` values: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`.

**`POST /admin/glossary/terms/{term_id}/experts` body:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valid `kind` values: `expert`, `author`.

**`POST /admin/glossary/refs/move` body:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Moving a ref settles the term that lost it under the remove-or-deprecate rule. Use it to merge two terms that normalization kept apart, for example when a source uses a non-standard abbreviation that the expansion table doesn't cover.

Deleting a rooted term (one with physical refs) returns `400 glossary.invalid`. Remove or move all refs first.

**`PATCH /admin/glossary/terms/{term_id}` — `export_excluded` field:**

```json
{"export_excluded": true}
```

Setting `export_excluded` to `true` withholds the term from all metadata export snapshots, regardless of its physical refs or abstract status. Setting it back to `false` restores the term to the snapshot on the next publish. Curation data (definition, edges, experts) is unaffected.

### AI-Assisted Curation

The org's configured AI model can draft definitions and propose relationship edges across the whole glossary in one operation. Both bulk actions require `org_admin` access and a configured org.

**`POST /admin/glossary/definitions/generate`**

Goes through every term in the glossary, skips those that already have a definition, and calls the org's AI model to draft one for each of the rest. Each draft is saved straight away, with no editor step, unlike the per-term draft endpoint (`POST /admin/glossary/terms/{term_id}/definition/generate`). A definition a person wrote is never overwritten, because a term that has one is skipped before any model call. One publish notification covers the whole batch.

Response:

```json
{"generated": 12}
```

`generated` is the count of terms that received a new definition. It is zero when every term already has one.

**`POST /admin/glossary/relationships/generate`**

Sends the full term list to the org's AI model with a prompt that specifies the ten allowed edge types (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) and asks for only confident proposals. The model responds with a JSON array; each entry is validated before any write: unknown term names, self-edges, and edge types outside the closed enum are silently dropped. Valid proposals are upserted idempotently — re-running the action does not duplicate edges. One publish notification covers the batch. The endpoint returns `{"added": 0}` immediately when the glossary contains fewer than two non-deprecated terms.

Response:

```json
{"added": 5}
```

`added` is the number of edges written. An edge that already existed still counts: the upsert succeeds, but the edge's data doesn't change.

### MCP `search_terms` Tool

```
search_terms(query, role=None, limit=25)
```

Searches term names and definitions with a case-insensitive substring match, up to `limit` results. Each result is the full term detail: `name`, `definition`, `is_abstract`, `deprecated`, physical refs (with `source_id`, `schema_name`, `table_name`, `column_name`), typed edges, and expert assignments.

Use `search_terms` before writing SQL to find every physical field that represents a concept by name. For example, searching `"order date"` returns the term and all `order_dt`, `orderDate`, `ORDER_DATE` columns across every registered table.

### MCP Statistics Tools

```
describe(sql, role=None)
correlate(sql, columns, method="pearson", role=None)
regress(sql, target, features, kind="linear", role=None)
compare(sql, value, group, role=None)
forecast(sql, time, value, horizon, seasonal_periods=None, role=None)
```

Each tool takes a dataset defined by `sql` and runs it through the same governed pipeline as `run_sql`, as the caller's role: row rules, masks and redaction apply, SQL the pipeline refuses fails as it fails for `run_sql`, and a role the caller does not hold is refused. Each returns results, never the rows, with the method it used and the `n` it counted.

| Tool | Returns |
| --- | --- |
| `describe` | Per column: `count`, `missing`, `distinct`; for a number `mean`, `sd`, `min`, `max` and `quantiles` (p0, p5, p25, p50, p75, p95, p100); `top_values` (the five most frequent) |
| `correlate` | The `pearson` or `spearman` correlation `matrix` of the numeric `columns`, over the rows complete in all of them, and that `n` |
| `regress` | `kind="linear"`: ordinary least squares `coefficients` (`estimate`, `se`, `p_value`), `r_squared`, `adj_r_squared`. `kind="logistic"`: a two-valued `target`, maximum-likelihood `coefficients`, `pseudo_r_squared`, `auc` and the `positive_value`. A text feature becomes one indicator per value but its first |
| `compare` | Each group's `n`, `mean` and `sd`; for two groups Welch's t-test (`t`, `df`, `p_value`) and Cohen's d; for more, one-way ANOVA (`f`, `df_between`, `df_within`, `p_value`) and eta squared |
| `forecast` | `horizon` points past the last `time`, each with `value`, `lower` and `upper` (95% prediction interval), by exponential smoothing (ETS) with additive error, an additive trend from 10 points, and additive seasonality when `seasonal_periods` is given |

The engine computes what it can over every row the role reads: `describe`'s counts, distinct counts, means, standard deviations, minimums and maximums, and each group's `n`, mean and variance for `compare`. A fitted model, `describe`'s quantiles and top values read at most `mcp.stats_max_rows` rows (Admin → Settings → Limits; 100,000 by default). Every answer that reads rows gives `rows_read`, `row_cap` and `truncated`, and a role's own row limit is reported under `warnings` as it is for `run_sql`.

Stats tools return only results, so the rows behind an analysis never enter the model's context: data minimization by default. Access control is what limits what an agent can learn. To keep identities out of an agent's reach, grant its role only the columns that identify no one; a role that can read a column can aggregate it, and aggregates over small groups can still identify individuals. See [Security](security.md#statistics-over-mcp).

### Metadata Export

The glossary term graph is included in every `MetadataSnapshot` built by `build_snapshot`.

The export applies the same filters as the rest of the snapshot:

- A term marked `export_excluded` is withheld outright — regardless of its physical refs, abstract status, or whether the org's catalog is configured.
- A rooted term publishes only when at least one of its physical refs belongs to a column that passes both the **Data Product** filter (the table's `data_product` flag must be `true`) and the **technical** column filter (columns tagged `technical` are withheld).
- A rooted term whose refs are all withheld by those filters is withheld with them.
- Abstract terms publish unconditionally — they are user vocabulary, not bound to physical columns.
- An edge between two terms publishes only when both endpoint terms publish.

Every vendor adapter publishes the term graph natively, into a glossary container that Provisa owns and creates idempotently, and never into an existing catalog glossary:

| Provider | Container | Terms | Relations | Deprecation |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (glossary API) | glossary terms, definition on `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]` shortDescription marker |
| Atlan | Provisa glossary by stable qualifiedName | `longDescription` (never the human-edited `userDescription`) | same Atlas mapping | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | `glossaryTermInfo` aspect per term | KIND_OF → Inherits, PART_OF → Contains (inverted), RELATED_TO/SYNONYM_OF → related terms | deprecation aspect; renames follow URN succession |
| OpenMetadata | Provisa glossary via `/v1/glossaries` | fqn-keyed PUT, renames PATCH-rebind by stored UUID | KIND_OF → native parent hierarchy, SYNONYM_OF → `synonyms`, others → `relatedTerms` | `entityStatus` |
| Collibra | Glossary-type domain "Provisa Glossary" | Business Term assets via the Import API | native Business Term relation types | asset status |

What counts is the binding, not the name. Each published term's vendor id is recorded in `catalog_bindings` under the term's URN (`provisa://<org>/terms/<name>`), and Provisa changes or deletes a glossary item on the vendor side only when it holds that binding, or when the item lives in the container Provisa created. A glossary item with no Provisa binding came from the external system and is never touched. Updates read and merge, so fields a steward added to Provisa's own terms survive, and nothing is deleted when a term leaves the snapshot. Steward assignments of terms to assets stay with the external system: no adapter writes them (publishing assignments authored in Provisa is a separate follow-on). On Collibra in particular, the Import API replaces what it's sent, and safety rests on containment: the payload names only assets inside the Provisa glossary domain and relations only between Provisa terms, so steward glossaries and their relations are out of reach.

---

## Data Products

A data product groups tables published together for consumption, owned by exactly one domain. Fields follow the ODPS (Open Data Product Standard) vocabulary where Provisa already has the source of truth. The admin UI exposes Data Products under **Admin → Data Products**.

### Capabilities

| Capability | Grants |
| --- | --- |
| `data_product_read` | Read access to the `data_products` query field and the Data Products admin page. Seeded by default to `org_admin`, `analyst`, `developer`, and `modeler`. |
| `data_product_rw` | Create and delete mutations. Enables the New / Edit / Delete controls in the UI. |

### Admin GraphQL

All data product operations go through `POST /admin/graphql`.

**Query:**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

Requires `data_product_read`.

**Create or update:**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` upserts — calling it with an existing `id` updates the record. Requires `data_product_rw`.

**Delete:**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

Deleting a product clears `product_id` from every member table, removing their membership. Requires `data_product_rw`.

### Field schema

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `id` | `String` | Yes | Machine-readable stable identifier, e.g. `customer_360` |
| `domain_id` | `String` | Yes | Owning domain. Member tables must share this `domain_id` — mismatches are rejected at save time |
| `name` | `String` | Yes | Display name |
| `owner_role` | `String` | No | Role accountable for this product; distinct from the domain steward |
| `team_role` | `String` | No | Role whose holders maintain this product day-to-day; resolves to individuals |
| `purpose` | `String` | No | What this product publishes and why |
| `limitations` | `String` | No | Known constraints, caveats, or exclusions |
| `usage` | `String` | No | How to consume this product |
| `version` | `String` | No | e.g. `1.2.0` |
| `status` | `String` | No | e.g. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | No | Service-level commitments; prose — a product spans multiple tables and structured SLA cannot unambiguously name which member it describes |
| `support` | `String` | No | Free-text support guidance |
| `custom_properties` | `JSON` | No | Arbitrary key-value metadata not covered by the standard fields |

Two more fields exist on the model but aren't exposed in `DataProductType` / `DataProductInput`. They are specific to the Snowflake Horizon Catalog:

| Field | Notes |
| --- | --- |
| `support_contact` | Email or URL; required by Horizon Catalog organization listing manifests |
| `publish` | `true` to publish Horizon listings immediately; new listings default to DRAFT |

### Table membership

A table joins a data product when its `product_id` field is set on the table edit form. The picker offers only products whose `domain_id` matches the table's own domain, so a table in domain `marketing` is never offered a product in domain `sales`.

Commands in the same domain may also be assigned as members.

### Metadata export filter

`build_snapshot` applies `data_products_only=True` for every catalog publish. Tables without a `product_id` are withheld from the snapshot, along with their relationship edges, lineage edges, and governance tags. Sources and domains always publish. Glossary terms publish only when at least one of their physical refs belongs to an exported (product-member) table.

A product with no exported members gets no snapshot entry, because a listing with no members would misrepresent the product to the catalog.

### Data product support by catalog target

`MetadataSnapshot.data_products` reaches every adapter, but only adapters whose platform has a native data-product concept publish it as a first-class entity; the rest publish the member tables (already filtered above) without a product grouping.

| Target | Data product representation |
| --- | --- |
| Snowflake Horizon | Each product becomes a `SHARE` over its member tables' physical addresses, wrapped in an internal `CREATE ORGANIZATION LISTING` — a native Horizon Catalog Data Product. `publish=true` takes the listing live immediately; otherwise it lands as DRAFT. |
| BigQuery Dataplex | Each product becomes an Analytics Hub listing via `/v1/dataProducts`. |
| OpenMetadata | Each product becomes a native `DataProduct` entity (`/api/v1/dataProducts`), with domain-derived ownership. |
| DataHub | Each product becomes a native `dataProduct` entity (`urn:li:dataProduct:...`) with its own `dataProductProperties`/ownership aspects. |
| Collibra | Each product becomes an asset of a `Data Product` community type, related to its member tables via a `Data Product groups Table` relation. |
| Atlan | Published as a custom `DataProduct` typedef guess — Atlan has no documented stable type name for this concept, so the mapping is best-effort. |
| Apache Atlas | Published as a custom `provisa_data_product` typedef with a `provisa_data_product_members` relation — Atlas has no native data-product entity type. |
| OpenLineage | Not a first-class entity — member tables carry a `provisa_data_product` custom facet naming the owning product. |
