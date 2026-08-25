# API Reference

## Overview

Provisa exposes REST endpoints under two prefixes: `/data` for query execution and schema introspection, and `/admin` for configuration management. (REQ-043) Most data endpoints require a role identifier. Admin configuration operations use a Strawberry GraphQL API at `/admin/graphql`. (REQ-164)

---

## Authentication

When `auth.provider` is configured in `provisa.yaml`, all endpoints except `/health` and `/setup/status` require an `Authorization: Bearer <token>` header. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Without auth configured, the server runs in dev mode. Any request is treated as the `anonymous` identity, which maps to all configured roles with wildcard domain access. (REQ-535)

**Login (`POST /auth/login`)** is provided by the active auth provider when `provider: basic` is configured. (REQ-124) Credential format and response depend on the provider.

**Identity introspection:**

```http
GET /auth/me
```

Returns the authenticated user's id, email, display name, org memberships, and role assignments. In dev mode returns `dev_mode: true` with all role IDs listed. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Returns `{"provider": "<name>"}` or `{"provider": null}` when auth is unconfigured. [tool-verified: `provisa/api/auth_router.py`]

---

## Data Endpoints

### `POST /data/graphql`

Execute a GraphQL query or mutation. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Request body:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

The `role` field is used only in dev mode (no auth). When auth is active, the authenticated user's role is used and `role` in the body is ignored.

The `extensions` field supports the Automatic Persisted Query (APQ) protocol: (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Headers:**

- `X-Provisa-Role` — override role (dev mode)
- `Accept` — response format (see Content Negotiation)
- `Authorization` — `Bearer <token>` when auth is enabled
- `X-Provisa-Redirect-Format` — MIME type for S3 redirect output (REQ-137)
- `X-Provisa-Redirect-Threshold` — row count above which redirect triggers (REQ-137)
- `X-Provisa-Redirect` — `true` to force redirect unconditionally (REQ-029)

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

Multi-root queries run each root field independently. Fields below the redirect threshold return inline; fields above redirect. The `redirects` key (plural) maps field names to redirect info. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Cache headers:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (on HIT) (REQ-536)

**Required capabilities:** `QUERY_DEVELOPMENT` for all requests including introspection. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Content Negotiation

| Accept Header | Format |
| --- | --- |
| `application/json` | JSON (default) |
| `application/x-ndjson` | Newline-delimited JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirect

Results above a configured row threshold (or when `X-Provisa-Redirect: true`) are written to S3 and a presigned URL is returned. (REQ-029, REQ-044)

| Redirect Format | Written by | Memory |
| --- | --- | --- |
| `application/vnd.apache.parquet` | federated CTAS | None — data never passes through Provisa |
| `application/x-orc` | federated CTAS | None — data never passes through Provisa |
| `application/json` | Provisa | Memory-bound |
| `application/x-ndjson` | Provisa | Memory-bound |
| `text/csv` | Provisa | Memory-bound |
| `application/vnd.apache.arrow.stream` | Provisa | Memory-bound |

For large analytical exports, use Parquet or ORC redirect. The federation engine writes directly to S3 in parallel — no data passes through Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Execute raw SQL through the Stage 2 governance pipeline. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Request body:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Required capabilities:** `QUERY_DEVELOPMENT`.

Governance violations on `POST /data/sql` return HTTP 403. (REQ-002, REQ-266)

**Response:** Same format as `/data/graphql` (JSON rows by default, content-negotiated via `Accept`).

---

### `POST /data/query`

Unified query endpoint. Accepts GraphQL, SQL, or Cypher — syntax is auto-detected. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Cypher queries can also be submitted to the Cypher-only `POST /query/cypher` endpoint. (REQ-345)

**Request body:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Returns `{"data": ...}` for GraphQL, `{"columns": [...], "rows": [...]}` for SQL and Cypher.

---

### `POST /data/sql/explain`

Explain or analyze a SQL statement through the governed pipeline. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

The endpoint wraps the **governed** SQL — the statement that actually runs under the caller's role, after RLS and masking — in the dialect's EXPLAIN syntax. What the plan shows is the authorized version of the query, not the raw input.

**Request body:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Set `analyze: true` to run EXPLAIN ANALYZE. The query executes and the plan carries real row counts and timings. Not every dialect supports ANALYZE; see the table in [Query plans and statistics](engines.md#query-plans-and-statistics).

**Response:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` when the dialect has no EXPLAIN support, or `analyze: true` is requested on a dialect that does not support it (e.g. SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Return the engine shard's current state without waking it. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

The UI polls this endpoint to show a startup banner while the engine is cold-starting. It never triggers a wake — polling is safe and does not count as activity for the idle reaper.

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

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Trigger an engine wake without running a query. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Returns `202 Accepted` immediately. The wake runs in the background. Use this if you want the engine ready before the first query arrives — for example, from a scheduler that runs queries a few minutes later.

**Response:** `202 Accepted`, body `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Auto-generated plain REST endpoint for every registered table. The query string maps to GraphQL arguments and the request compiles and executes through the same pipeline (RLS, masking, routing) as GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Query parameters:**

- `limit` — max rows (≥ 1)
- `offset` — skip rows (≥ 0)
- `fields` — comma-separated column names (defaults to all scalar fields)
- `filter` — JSON array of `{"field", "comparator", "value"}` filter objects
- `orderBy` — JSON array of `{"field", "direction"}` sort objects

The authenticated role is required; unauthenticated requests return `401`. An OpenAPI spec for these routes is served at `GET /data/rest/openapi.json` with Swagger UI at `GET /data/rest/docs`.

#### OpenAPI / Swagger UI Explorer

The OpenAPI explorer page (`/app/openapi`) embeds the Swagger UI in a sandboxed iframe. The spec is role-scoped — only tables and columns visible to the current role appear — and optionally domain-filtered via the domain selector. The UI switches between light and dark themes automatically. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

The page loads the spec HTML via `fetch()` rather than a direct iframe `src`, so the request carries the session's bearer token and Swagger UI's own relative requests resolve correctly against the same origin. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

When navigated from an NL "Open in OpenAPI" link, the page auto-expands the target endpoint, populates query parameters from the NL-generated URL (e.g. `aggregate`, `groupBy`), and clicks Execute — using DOM polling to ensure each step completes before the next fires. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Auto-generated [JSON:API](https://jsonapi.org)-compliant endpoint for every registered table. Same RLS, masking, and routing as GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**`Accept` header:** must include `application/vnd.api+json` (the JSON:API media type) or the request returns `406`.

**Query parameters:**

- `fields[<type>]` — sparse fieldsets, e.g. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — e.g. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — comma-separated, `-` prefix for descending, e.g. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — pagination
- `aggregate` — comma-separated aggregate functions to run instead of row retrieval: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Use `?aggregate=count,sum` to request a subset. Aggregate responses return `data: null` with results in `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — comma-separated column names; used with `?aggregate=` to group results. Only columns in the table's `DistinctOnColumn` enum are valid; the server returns `400` for any column the role cannot see. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` to include base-table scalar columns (and joined dimension scalars named in `include=`) inside each group row's `nodes` array. Required when an NL group-by query also requests dimension details. (REQ-1405)

Responses are resource objects with `type`/`id`/`attributes`. Errors follow the JSON:API error object shape.

#### JSON:API Explorer

The JSON:API explorer page (`/app/jsonapi`) is a browser UI over these endpoints. Select a table from the domain-grouped list, then configure:

- **Fields** — choose which columns to include (sparse fieldset); leave all unchecked to request every column
- **Relationships** — select FK-derived relationship names to sideload via `?include=`
- **Filter** — field, operator (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`), and value
- **Sort** — one field, ascending or descending
- **Aggregate** — pick group-by columns from the server-validated list, then check one or more aggregate functions; when group-by columns are selected, an "Include nodes" checkbox appends base-table scalar columns to each row
- **Page size** — resources per page, with first/prev/next/last navigation

Results render in a formatted summary view (resource cards with clickable relationship anchors) or a raw JSON tab. The live request URL is shown and can be copied. Table selection and page size persist across sessions in `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

When navigated from an NL "Open in JSON:API" link, the explorer pre-selects the table and seeds the aggregate picker from the NL-generated query parameters, then auto-runs the request. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Submit a natural-language question. The service starts an async job and returns `202 Accepted` with a `job_id` immediately. Requires an LLM provider configured under the `ai_models` config section. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Request body:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Returns `{"job_id": "<id>"}`. Exceeding the per-role NL rate limit returns `429` with a `Retry-After` header. (REQ-370)

**Retrieve the result:**

- `GET /query/nl/{job_id}` — poll. Returns the job document.
- `GET /query/nl/{job_id}/stream` — SSE. One `branch` event per generation target as it completes, then a `done` event. (REQ-357, REQ-358)

Three generation loops (Cypher, GraphQL, SQL) run in parallel, each validated through the compiler and refined on error. (REQ-355) The prompt is scoped to the role's visible schema. (REQ-356) The result document keys each branch by target: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

A branch that exhausts its iteration limit returns `query: null`, `result: null`, and an `error` string. Every generated query executes under the consumer's rights with Stage 2 governance applied — the service never bypasses governance. (REQ-359)

#### NL Group-By with Dimension Details (REQ-1405)

When an NL group-by query also projects columns from a joined dimension table — for example, "count of inquiries by user with user name and email" — the runner derives per-field dot-paths (`dim_paths`) from the SELECT-projected dimension columns. These paths populate the `includeNodes=` parameter on the JSON:API and OpenAPI panels' generated URLs, so those panels request the same joined-dimension fields the SQL and GraphQL branches resolved. Without this, `includeNodes=true` would return only the base aggregate table's own scalar fields. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

On the gRPC panel, the generated `{Type}GroupByRequest` carries `include_nodes` (bool) and `include` (repeated string of relationship field names). The returned `{Type}GroupByRow` includes a typed `nodes` field with the dimension-detail rows. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Return the GraphQL SDL for a role's schema. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Headers:** `X-Role: <role_id>` (required)

**Query parameters:**

- `domain` — comma-separated domain IDs. When set, the response is filtered to the named domain(s) and tables reachable from them.

**Response:** `text/plain` GraphQL SDL.

---

### `GET /data/introspection`

Return GraphQL introspection JSON, optionally domain-filtered. [tool-verified: `provisa/api/data/sdl.py:200`]

**Headers:** `X-Provisa-Role: <role_id>` (required)

**Query parameters:** `domain` — comma-separated domain IDs.

**Response:** `application/json` introspection result.

---

### `GET /data/graph-schema`

Return the graph view of the role's schema: node labels and their relationship types, for Cypher/graph clients. Includes `pk_columns` per node label so callers can determine primary-key columns. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Response:** `application/json` with `node_labels` (each carrying `pk`/`pk_columns`) and `relationship_types`.

Each relationship type also carries `junction_table_name` and `properties` (REQ-1586). On a junction-backed edge the first names the associative table it traverses and the second lists the columns of that table readable as `r.attr` and filterable in `WHERE`; on a foreign-key-backed edge the name is `null` and the property list is empty, which is how a client tells the two apart. The junction table itself is never a node label — it is the edge, so it has no pill in a graph client and no row in `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Return domain IDs accessible to the requesting role. [tool-verified: `provisa/api/data/sdl.py:116`]

**Headers:** `X-Role: <role_id>` (required)

**Response:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Return the current schema version string. Combines a per-boot nonce with a rebuild counter. Clients use this to invalidate schema caches after server restarts. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Response:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Return the auto-generated `.proto` file for a role. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Response:** `text/plain` protobuf schema.

Each registered table produces a proto `message`. Relationships produce nested message fields. Type mapping: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Server-Sent Events stream for real-time change notifications from a table. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Notification delivery uses a pluggable provider chosen per source type: PostgreSQL sources use `LISTEN/NOTIFY` (via asyncpg), MongoDB sources use Change Streams (`collection.watch()`), and Kafka sources use consumer groups. Each provider implements a common async watch interface. RLS filtering and schema validation apply regardless of provider. (REQ-258) WebSocket and RSS sources are also supported. (REQ-338, REQ-342)

**Header — `X-Provisa-Sink`:** Set to a Kafka target (e.g. `kafka://broker:9092/topic`) to redirect change events to a Kafka sink instead of the SSE response. The server launches a sink consumer and returns `202 Accepted` rather than an open stream. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Admin REST Endpoints

### Config

#### `GET /admin/config`

Download the current `provisa.yaml` as `application/x-yaml` with a `Content-Disposition: attachment` header. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Upload a revised config YAML. The server writes a `.bak` backup, saves the new file, and reloads all schemas, sources, and materialized views. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Request body:** Raw YAML content.

**Response:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

On reload failure: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Download the **current live config** — the config as Provisa would write it today, reflecting every admin-created table, relationship, domain, role, and RLS rule that has accumulated since startup. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

The file on disk may lag the live state if changes were made through the admin API without a subsequent upload. This endpoint closes that gap: its output is what `PUT /admin/config` would need to receive to make the on-disk file match live state.

Returns `application/x-yaml` with `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Return both sides of the config diff — `original` (the startup baseline) and `current` (live state) — normalized identically, so the comparison shows only genuine changes, not reordering or comment drift. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Response:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Generate a unified-diff patch from the baseline to the posted config. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Send the revised YAML as the request body. The response is a `text/x-patch` file (`provisa.config.patch`) that `git apply` or `patch` can consume directly — useful for committing UI-driven config changes through a CI/CD pipeline.

---

### Settings

#### `GET /admin/settings`

Return current platform settings as JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Update platform settings at runtime. All fields are optional — only keys present in the body are updated. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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
- `naming`: `domain_prefix`, `convention` — writes to config file and triggers schema reload (REQ-253)
- `relationships`: `auto_track_fk` — governs foreign-key tracking only. A junction-backed relationship is declared on the table registration and is never inferred, so this setting does not reach it. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Response:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### AI Models

#### `GET /admin/ai-models`

Return the acting org's AI-model assignments, vector-model registry, and NL rate limit. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

API keys are never echoed back — `api_keys_set` reports only whether each vendor has a key configured. Changes take effect on the next request; no restart is needed. (REQ-1349)

#### `PUT /admin/ai-models`

Update the org's AI-model assignments, vector-model registry, or NL rate limit. Takes effect on the next request. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Return the model names a vendor currently serves, for the model picker. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

The list is read live from the vendor's own list-models API using the org's configured key — or the deployment credential when no org key is set. A model released after this build shipped is selectable the same day the vendor serves it.

Returns `400` when the vendor publishes no list-models API (enter the model name directly in that case) or when no key is available. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Federation Engine

#### `GET /admin/federation-engine`

Return the current federation-engine selection, its connection config, and the full selectable-engine registry. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

Persist a federation-engine selection. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Request body:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

The selection is written to the platform config. It takes effect after the next service restart — the engine is chosen once at boot.

---

### Domain Policy

#### `POST /admin/domain-policy`

Change the acting org's domain policy (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

This is a destructive operation scoped to the acting org. Every registered source, table, domain, and relationship is purged and rebuilt under the new policy. Use it when switching an org from domain-namespaced to flat (or vice versa).

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

Return up to N recent completed spans from the in-memory span buffer. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Query parameters:** `limit` (default 50, max 200)

**Response:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Hot-reload a named catalog in the federation engine coordinator via its REST API. Reconnects Provisa's internal connection and re-runs OTel DDL. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Query parameters:** `catalog` (default `"otel"`)

**Response:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Restart the federation engine container (single-node dev only). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Query parameters:** `container` (defaults to `QUERY_ENGINE_CONTAINER` env var, then `"trino"`)

---

### Discovery

#### `POST /admin/discover/relationships`

Trigger relationship discovery. Always runs FK introspection from the federation engine. (REQ-018) Runs LLM inference if `ANTHROPIC_API_KEY` is set. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

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

List pending relationship candidates. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Accept a candidate and register it as a relationship. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Request body (optional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Reject a candidate. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Request body:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Return count of rejected candidates. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Delete all rejected candidates. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Source Crawl

#### `POST /admin/sources/crawl`

Crawl a data source to introspect its schema and register tables. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Source Table Search

#### `GET /admin/sources/{source_id}/tables/search`

Search available (not yet registered) tables in a source by name. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Table Profiling

#### `POST /admin/tables/{table_id}/profile`

Run a column profile on a registered table — cardinality, min/max, null rates. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Source Descriptions

#### `POST /admin/source-meta/db-description`

Generate LLM-assisted descriptions for a source's tables and columns. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Object Storage (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Report the acting org's storage footprint against its platform allowance, and whether the org has registered its own store. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

When the org has registered its own DSN, its materializations go there and are no longer counted against the allowance. The DSN itself is never returned.

#### `PUT /admin/org-storage`

Register (or clear) the org's own materialization store. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Request body:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

The DSN is validated against the federation engine before being accepted — an unusable DSN fails at registration, not hours later during a refresh. The value is encrypted at rest and never returned by GET.

Send `storage_url: null` to clear the org's own store and return its materializations to the platform store (and allowance). The org runtime is rebuilt in the same call, so the new store is effective immediately. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Org Encryption (REQ-1574)

#### `GET /admin/org-encryption`

Return the org's current key status: fingerprint, id, and provenance. Never returns key material. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

When the org has set no key, returns `{"configured": false}`. Every org starts in this state and inherits the deployment's key.

#### `PUT /admin/org-encryption`

Set or rotate the org's at-rest encryption key. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Request body:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omit `key_b64` to have Provisa generate a key — the safest path, as the key never appears in a clipboard or request log. Supplying `key_b64` brings your own key.

Rotation adds a new active entry to the key ring and retains the old one, so data written under the previous key remains readable. Rotation is not re-encryption. There is no delete endpoint: retiring the last key would make every wrapped payload unreadable. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

The live ring is rebound in the same call, so the next encrypted write uses the new key immediately.

---

### Hasura / DDN Import (REQ-1483)

#### `POST /admin/import/hasura/preview`

Convert a Hasura v2 or DDN project archive into proposed Provisa config without writing anything. [tool-verified: `provisa/api/admin/import_router.py`]

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

Load a previously previewed config into the acting org. [tool-verified: `provisa/api/admin/import_router.py`]

**Request body:**

```json
{"config_yaml": "<yaml string>"}
```

Uses the same hot-reload path as `PUT /admin/config`. The org's catalog, schemas, and pools are rebuilt before the response returns.

---

### Apache Ossie Interchange (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Export the org's governed model as an Apache Ossie (incubating) YAML document. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

The document is derived from live state on every request — never cached — so it cannot be stale. Tables become `dataset` objects, columns become `field` objects, and relationships map to Ossie `relationship` objects.

Returns `text/yaml` with `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

Parse an Ossie YAML or JSON document and return proposed table and relationship registrations. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

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

All endpoints are under the `/admin/actions` prefix. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Every invocation — from GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, and Provisa gRPC — routes through a single governed executor that enforces `writable_by` and governance uniformly. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] See [docs/integrations.md](integrations.md#invoking-commands-across-protocols) for the per-protocol call syntax.

#### `GET /admin/actions`

Return all tracked DB functions and webhooks. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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
      "writableBy": [],
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

Each webhook object carries an `approved` boolean. A webhook is approved once a steward executes its creation request (REQ-209); config-declared webhooks are auto-approved. An unapproved webhook is registered but not exposed on any surface. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Register a tracked function (command). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Key fields:**

| Field | Required | Description |
| --- | --- | --- |
| `name` | Yes | Unique command name |
| `kind` | Yes | `"query"` → GraphQL Query field; `"mutation"` → Mutation field |
| `implKind` | No | How the command runs — see table below (default `source_procedure`) |
| `binding` | No | `implKind`-specific connection details (JSON object) |
| `returnSchema` | No | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — makes the command set-returning on every surface |
| `arguments` | No | `[{name, type}]` argument definitions; positional order matters for SQL and Bolt callers |
| `visibleTo` | No | Role IDs that can call the command |
| `writableBy` | No | Role IDs permitted to invoke it as a mutation |
| `domainId` | No | Domain for GraphQL placement and access control |

**`implKind` values:**

| `implKind` | What runs | `binding` fields |
| --- | --- | --- |
| `source_procedure` | Stored procedure on a registered source (default) | `sourceId`, `schemaName`, `functionName` |
| `script` | Server-side script | `script` |
| `http` | Outbound HTTP call | `url`, `method` |
| `grpc` | Outbound gRPC call to an external server | `target`, `method` |
| `python` | Python callable hosted by Provisa (REQ-885) | `callable` (e.g. `"demo.py_functions:random_dataset"`) |

The demo commands `random_python_set` (`implKind: python`) and `random_grpc_set` (`implKind: grpc`) show set-returning commands with `returnSchema` in practice; both are in `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Update a tracked function by name. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Delete a tracked function by name. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Register a tracked webhook. (REQ-209) Registering or updating a webhook enqueues a steward approval request — the webhook becomes active on all surfaces only after a steward approves it. Config-declared webhooks are auto-approved. **Request body fields:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Update a tracked webhook by name. Any edit resets approval to pending until re-approved. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Delete a tracked webhook by name. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Test an action (function or webhook) by name. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Roles

All endpoints are under the `/admin/roles` prefix. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/admin/roles/` | List all roles |
| `POST` | `/admin/roles/` | Create a role |
| `PUT` | `/admin/roles/{role_id}` | Update a role |
| `DELETE` | `/admin/roles/{role_id}` | Delete a role |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Users

All endpoints are under the `/admin/users` prefix. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

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

All endpoints are under `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

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

All endpoints are under `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Create an invite |
| `GET` | `/admin/invites/` | List pending invites |
| `DELETE` | `/admin/invites/{token}` | Revoke an invite |

---

### Admin GraphQL

#### `POST /admin/graphql`

Strawberry GraphQL endpoint for all admin operations: source and table CRUD, relationship management, domain configuration, RLS rules, cache control, naming conventions, scheduled task management, and query compilation. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### Setup

#### `GET /setup/status`

Return first-run setup status. Always unauthenticated. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Complete first-run setup. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Health Check

#### `GET /health` or `HEAD /health`

Returns `{"status": "ok"}`. Always unauthenticated. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Error Responses

| Status | Meaning |
| --- | --- |
| 400 | Invalid query, validation error, or SQL parse error |
| 401 | Missing or invalid auth token |
| 403 | Insufficient capabilities; governance violation |
| 404 | Role, resource, or config file not found |
| 422 | Missing required header (e.g. `X-Role`) |
| 503 | Database or source not connected; dependency unavailable |
| 504 | Request timed out |

Governance violations on `POST /data/sql` return HTTP 403 with a structured body: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

Port `8815`. Native Arrow columnar transport over gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Queries and catalog discovery are both available on the same connection. The full governance pipeline (RLS, masking, sampling) is applied to every query. (REQ-130, REQ-143)

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

When the Zaychik Flight SQL proxy is available (port 8480), record batches stream end-to-end without full materialization. (REQ-144) Falls back to materializing via the federated query layer if Zaychik is unavailable. (REQ-146)

---

## Protobuf gRPC Endpoint

Port `50051` (override with `GRPC_PORT` env var or `server.grpc_port` config). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Pass the role in the `x-provisa-role` gRPC metadata key. If absent, the server aborts with `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Download the role-specific proto from `GET /data/proto/{role_id}`. Only tables and columns visible to that role appear. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Each table produces a `Query{TypeName}` streaming RPC. `Insert{TypeName}` RPCs exist for schema symmetry but abort with `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` is enabled for service discovery without a pre-compiled proto. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

The gRPC server starts only when a valid proto can be compiled at startup. If schema build fails, the gRPC server does not start. (REQ-529)

#### Aggregate and Group-By RPCs (REQ-1359, REQ-1361, REQ-1405)

When a table has `enable_aggregates` set, the generated proto includes two additional RPCs alongside `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — returns aggregate scalars for the table (`count`; `sum`, `avg`, `stddev`, `variance` per numeric column; `min`, `max` per comparable column)
- **`Query{TypeName}GroupBy`** — returns one row per group key with aggregate sub-fields and, optionally, base-table scalars and joined-dimension rows in a `nodes` field

Both route through the same compiler aggregate pipeline as GraphQL's `{field}_aggregate` and `{field}_group_by` root fields — no separate aggregate implementation. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**`funcs` field (REQ-1361).** The request message accepts a `funcs` repeated-string field. Valid values are `count`, `sum`, `avg`, `stddev`, `variance`, `min`, and `max`. When `funcs` is omitted, every function the schema exposes for that table is requested. When set, only the named functions appear. If none of the named functions apply to the table's column types, the query falls back to `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**`include_nodes` and `include` fields (REQ-1405).** `Query{TypeName}GroupBy` requests may set `include_nodes: true` to include base-table scalar columns in each row's `nodes` field. The `include` repeated-string field names many-to-one relationship fields whose scalar columns are also nested inside `nodes`. This matches the JSON:API `?includeNodes=` / `?include=` behavior. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## JDBC Driver

The Provisa JDBC driver (`provisa-jdbc-0.1.0.jar`) exposes the semantic catalog to BI tools (Tableau, PowerBI, DBeaver). (REQ-126)

**Connection URL:** `jdbc:provisa://host:port` (REQ-131)

Domains map to JDBC schemas. (REQ-127) Tables use their registered aliases. Columns use aliases and surface descriptions as `REMARKS`. (REQ-128) Standard metadata methods (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) expose semantic relationships as PK/FK metadata.

**SQL support:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

The driver requests Arrow IPC redirect by default. Results stream batch-by-batch via `ArrowStreamReader`, bounded to one record batch in memory. (REQ-293)

---

## `orderBy` Argument Format

The `order_by` argument uses `{column: direction}` objects with a 6-value direction enum: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Supported directions: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Subscriptions

SSE subscriptions are available at `GET /data/subscribe/{table}`. (REQ-219, REQ-258) Notification delivery uses a pluggable provider selected per source type: PostgreSQL sources use `LISTEN/NOTIFY`, MongoDB sources use Change Streams, and Kafka sources use consumer groups. RLS filtering and schema validation apply regardless of provider. WebSocket and RSS sources are also supported via the same endpoint. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Business Glossary (REQ-1387)

The business glossary maps physical field names — as they exist in source databases — onto a shared human vocabulary. Every column registered in the semantic layer gets a term automatically. No manual entry is required to populate the glossary; curators add definitions, relationships, and experts on top of what the system derives.

### How Terms Are Derived

When Provisa registers or updates a table's columns, `normalize_term` (`provisa/core/glossary.py`) runs on every column name and produces a canonical phrase. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

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

The first three collapse to one term. `transaction amount` keeps both tokens because `amount` is not a proxy. A bare `id` column — no preceding tokens — cannot be stripped; it normalizes to `identifier` so the term is non-empty. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Lifecycle

Terms are **derived from semantic-layer membership**, not created on demand by users. The table repository is the single write path: `sync_table_refs` runs inside every column-set upsert, and `sweep_refless_terms` runs after any deletion path. [tool-verified: `provisa/core/repositories/glossary.py`]

**When a column is added:** Provisa looks up the normalized term by name. If it already exists, the column gets a ref to it (and if the term was deprecated, it is revived — `deprecated` is set back to `False`). If no term exists yet, one is created.

**When a column departs** (schema change or table removal): its ref is deleted and the term is **settled** under a remove-or-deprecate rule. A rooted term with no remaining refs is removed outright — along with its edges and expert assignments — unless removing it would leave an abstract term disconnected from all rooted terms (no path through the term graph). In that case, the term is **deprecated** (marked `deprecated=True`) rather than deleted, so the abstract term's graph anchor survives.

Abstract terms are never auto-removed; they exist outside the physical lifecycle and are only deleted explicitly via the admin API.

**Revival:** if a deprecated term's normalized name reappears (a column is re-registered), the term is unmarked and its refs resume accumulating.

### Curation Endpoints

All endpoints are under `/admin/glossary`. They require `org_admin` access and a configured org. Every mutation triggers a metadata publish. [tool-verified: `provisa/api/admin/glossary_router.py`]

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

Valid `rel_type` values: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**`POST /admin/glossary/terms/{term_id}/experts` body:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valid `kind` values: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**`POST /admin/glossary/refs/move` body:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Moving a ref settles the losing term under the remove-or-deprecate rule. Use this to consolidate two terms that normalization kept separate — for example, after a source uses a non-standard abbreviation that fell outside the expansion table.

Deleting a rooted term (one with physical refs) returns `400 glossary.invalid`. Remove or move all refs first.

**`PATCH /admin/glossary/terms/{term_id}` — `export_excluded` field:**

```json
{"export_excluded": true}
```

Setting `export_excluded` to `true` withholds the term from all metadata export snapshots, regardless of its physical refs or abstract status. Setting it back to `false` restores the term to the snapshot on the next publish. Curation data (definition, edges, experts) is unaffected. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### AI-Assisted Curation

The org's configured AI model can draft definitions and propose relationship edges across the whole glossary in one operation. Both bulk actions require `org_admin` access and a configured org.

**`POST /admin/glossary/definitions/generate`**

Iterates every term in the glossary, skips any that already have a definition, and calls the org's AI model to draft one for each remaining term. The draft is persisted immediately — unlike the per-term draft endpoint (`POST /admin/glossary/terms/{term_id}/definition/generate`), there is no editor step. Human-authored definitions are never overwritten: the guard is `if summary["definition"]: continue` before any model call. One publish notification covers the entire batch. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Response:

```json
{"generated": 12}
```

`generated` is the count of terms that received a new definition. It is zero when every term already has one.

**`POST /admin/glossary/relationships/generate`**

Sends the full term list to the org's AI model with a prompt that specifies the ten allowed edge types (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) and asks for only confident proposals. The model responds with a JSON array; each entry is validated before any write: unknown term names, self-edges, and edge types outside the closed enum are silently dropped. Valid proposals are upserted idempotently — re-running the action does not duplicate edges. One publish notification covers the batch. The endpoint returns `{"added": 0}` immediately when the glossary contains fewer than two non-deprecated terms. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Response:

```json
{"added": 5}
```

`added` is the count of edges written. An edge that already existed still counts — the upsert succeeds, but the edge data does not change.

### MCP `search_terms` Tool

```
search_terms(query, role=None, limit=25)
```

Searches term names and definitions with a case-insensitive substring match, up to `limit` results. Each result is the full term detail: `name`, `definition`, `is_abstract`, `deprecated`, physical refs (with `source_id`, `schema_name`, `table_name`, `column_name`), typed edges, and expert assignments. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Use `search_terms` before writing SQL to find every physical field that represents a concept by name. For example, searching `"order date"` returns the term and all `order_dt`, `orderDate`, `ORDER_DATE` columns across every registered table.

### Metadata Export

The glossary term graph is included in every `MetadataSnapshot` built by `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

The export applies the same filters as the rest of the snapshot:

- A term marked `export_excluded` is withheld outright — regardless of its physical refs, abstract status, or whether the org's catalog is configured. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- A rooted term publishes only when at least one of its physical refs belongs to a column that passes both the **Data Product** filter (the table's `data_product` flag must be `true`) and the **technical** column filter (columns tagged `technical` are withheld).
- A rooted term whose refs are all withheld by those filters is withheld with them.
- Abstract terms publish unconditionally — they are user vocabulary, not bound to physical columns.
- An edge between two terms publishes only when both endpoint terms publish.

Every vendor adapter publishes the term graph natively, into a Provisa-owned glossary container it creates idempotently — never into an existing catalog glossary:

| Provider | Container | Terms | Relations | Deprecation |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (glossary API) | glossary terms, definition on `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | `[DEPRECATED]` shortDescription marker |
| Atlan | Provisa glossary by stable qualifiedName | `longDescription` (never the human-edited `userDescription`) | same Atlas mapping | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | `glossaryTermInfo` aspect per term | KIND_OF → Inherits, PART_OF → Contains (inverted), RELATED_TO/SYNONYM_OF → related terms | deprecation aspect; renames follow URN succession |
| OpenMetadata | Provisa glossary via `/v1/glossaries` | fqn-keyed PUT, renames PATCH-rebind by stored UUID | KIND_OF → native parent hierarchy, SYNONYM_OF → `synonyms`, others → `relatedTerms` | `entityStatus` |
| Collibra | Glossary-type domain "Provisa Glossary" | Business Term assets via the Import API | native Business Term relation types | asset status |

Ownership is the binding, not the name: each published term's vendor id is captured into `catalog_bindings` under the term's URN (`provisa://<org>/terms/<name>`), and Provisa modifies or deletes a vendor-side glossary item only when it holds that binding (or the item lives in the Provisa-owned container it created). A glossary item with no Provisa binding originated in the external system and is never touched; updates read-merge so steward-added fields on Provisa's own terms survive; nothing is deleted when a term leaves the snapshot. Steward term-to-asset assignments remain external-owned — no adapter writes term-to-asset assignments (Provisa-authored assignment publishing is an explicit follow-on). On Collibra specifically, safety under the Import API's REPLACE semantics rests on containment: the payload mentions only assets inside the Provisa glossary domain and relation instances only between Provisa terms, so steward glossaries and their relations are never reachable. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
