# API Reference

## Overview

Provisa exposes REST endpoints under two prefixes: `/data` for query execution and schema introspection, and `/admin` for configuration management. Most data endpoints require a role identifier. Admin configuration operations use a Strawberry GraphQL API at `/admin/graphql`.

---

## Authentication

When `auth.provider` is configured in `provisa.yaml`, all endpoints except `/health` and `/setup/status` require an `Authorization: Bearer <token>` header. [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Without auth configured, the server runs in dev mode. Any request is treated as the `anonymous` identity, which maps to all configured roles with wildcard domain access.

**Login (`POST /auth/login`)** is provided by the active auth provider when `provider: basic` is configured. Credential format and response depend on the provider.

**Identity introspection:**

```
GET /auth/me
```

Returns the authenticated user's id, email, display name, org memberships, and role assignments. In dev mode returns `dev_mode: true` with all role IDs listed. [tool-verified: `provisa/api/auth_router.py`]

```
GET /auth/provider-type
```

Returns `{"provider": "<name>"}` or `{"provider": null}` when auth is unconfigured. [tool-verified: `provisa/api/auth_router.py`]

---

## Data Endpoints

### `POST /data/graphql`

Execute a GraphQL query or mutation. [tool-verified: `provisa/api/data/endpoint.py:151`]

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

The `extensions` field supports the Automatic Persisted Query (APQ) protocol:
```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Headers:**
- `X-Provisa-Role` — override role (dev mode)
- `Accept` — response format (see Content Negotiation)
- `Authorization` — `Bearer <token>` when auth is enabled
- `X-Provisa-Redirect-Format` — MIME type for S3 redirect output
- `X-Provisa-Redirect-Threshold` — row count above which redirect triggers
- `X-Provisa-Redirect` — `true` to force redirect unconditionally

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

Multi-root queries run each root field independently. Fields below the redirect threshold return inline; fields above redirect. The `redirects` key (plural) maps field names to redirect info. [tool-verified: `provisa/api/data/endpoint.py`]

**Cache headers:**
- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>` (on HIT)

**Required capabilities:** `QUERY_DEVELOPMENT` for all requests including introspection. `AD_HOC_QUERY` for actual data queries. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Content Negotiation

| Accept Header | Format |
|---|---|
| `application/json` | JSON (default) |
| `application/x-ndjson` | Newline-delimited JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

[tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirect

Results above a configured row threshold (or when `X-Provisa-Redirect: true`) are written to S3 and a presigned URL is returned.

| Redirect Format | Written by | Memory |
|---|---|---|
| `application/vnd.apache.parquet` | federated CTAS | None — data never passes through Provisa |
| `application/x-orc` | federated CTAS | None — data never passes through Provisa |
| `application/json` | Provisa | Memory-bound |
| `application/x-ndjson` | Provisa | Memory-bound |
| `text/csv` | Provisa | Memory-bound |
| `application/vnd.apache.arrow.stream` | Provisa | Memory-bound |

For large analytical exports, use Parquet or ORC redirect. The federation engine writes directly to S3 in parallel — no data passes through Provisa.

```
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Execute raw SQL through the Stage 2 governance pipeline. [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Request body:**
```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin",
  "discovery_mode": false
}
```

The `discovery_mode` flag widens the table visibility check to include all tables from all contexts. Only for internal tooling. [tool-verified: `provisa/api/data/endpoint_dev.py:148-152`]

**Required capabilities:** `QUERY_DEVELOPMENT` and `AD_HOC_QUERY`.

**Response:** Same format as `/data/graphql` (JSON rows by default, content-negotiated via `Accept`).

---

### `POST /data/query`

Unified query endpoint. Accepts GraphQL, SQL, or Cypher — syntax is auto-detected. [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

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

### `POST /data/nl-to-sql`

Translate a natural-language question to semantic SQL using Claude. Requires `ANTHROPIC_API_KEY` to be set. [tool-verified: `provisa/api/data/endpoint_dev.py:266`]

**Request body:**
```json
{"question": "How many orders were placed last month?", "role": "admin"}
```

Returns the generated SQL string.

---

### `GET /data/sdl`

Return the GraphQL SDL for a role's schema. [tool-verified: `provisa/api/data/sdl.py:137`]

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

### `GET /data/domains`

Return domain IDs accessible to the requesting role. [tool-verified: `provisa/api/data/sdl.py:116`]

**Headers:** `X-Role: <role_id>` (required)

**Response:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Return the current schema version string. Combines a per-boot nonce with a rebuild counter. Clients use this to invalidate schema caches after server restarts. [tool-verified: `provisa/api/data/sdl.py:102`]

**Response:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Return the auto-generated `.proto` file for a role. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Response:** `text/plain` protobuf schema.

Each registered table produces a proto `message`. Relationships produce nested message fields. Type mapping: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`.

---

### `GET /subscribe/{table}`

Server-Sent Events stream for real-time change notifications from a table. [tool-verified: `provisa/api/data/subscribe.py:239`]

Uses PostgreSQL `LISTEN/NOTIFY` for pre-approved PostgreSQL-backed tables. WebSocket and RSS sources are also supported. Requires the table to have a pg_notify trigger installed (automatic for pre-approved tables).

---

## Admin REST Endpoints

### Config

#### `GET /admin/config`

Download the current `provisa.yaml` as `application/x-yaml` with a `Content-Disposition: attachment` header. [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Upload a revised config YAML. The server writes a `.bak` backup, saves the new file, and reloads all schemas, sources, and materialized views. [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Request body:** Raw YAML content.

**Response:**
```json
{"success": true, "message": "Config uploaded and reloaded"}
```

On reload failure: `{"success": false, "message": "<error>"}`.

---

### Settings

#### `GET /admin/settings`

Return current platform settings as JSON. [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Update platform settings at runtime. All fields are optional — only keys present in the body are updated. [tool-verified: `provisa/api/admin/settings_router.py:100`]

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
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Response:**
```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Observability

#### `GET /admin/traces/recent`

Return up to N recent completed spans from the in-memory span buffer. [tool-verified: `provisa/api/admin/settings_router.py:317`]

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

Trigger relationship discovery. Always runs FK introspection from the federation engine. Runs LLM inference if `ANTHROPIC_API_KEY` is set. [tool-verified: `provisa/api/admin/discovery.py:55`]

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

Crawl a data source to introspect its schema and register tables. [tool-verified: `provisa/api/admin/crawl_router.py:36`]

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

### Actions (Functions and Webhooks)

All endpoints are under the `/admin/actions` prefix. [tool-verified: `provisa/api/admin/actions_router.py:24`]

#### `GET /admin/actions`

Return all tracked DB functions and webhooks.

**Response:**
```json
{
  "functions": [
    {
      "name": "get_account_balance",
      "sourceId": "sales-pg",
      "schemaName": "public",
      "functionName": "get_account_balance",
      "returns": "numeric",
      "arguments": [{"name": "account_id", "type": "integer"}],
      "visibleTo": ["admin", "analyst"],
      "writableBy": [],
      "domainId": "sales",
      "description": null,
      "kind": "mutation"
    }
  ],
  "webhooks": [...]
}
```

#### `POST /admin/actions/functions`

Register a tracked DB function.

**Request body fields:** `name`, `sourceId`, `schemaName`, `functionName`, `returns`, `arguments`, `visibleTo`, `writableBy`, `domainId`, `description`, `kind`, `returnSchema`. [tool-verified: `provisa/api/admin/actions_router.py:117`]

#### `PUT /admin/actions/functions/{name}`

Update a tracked function by name. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Delete a tracked function by name. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Register a tracked webhook. **Request body fields:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`]

#### `PUT /admin/actions/webhooks/{name}`

Update a tracked webhook by name. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Delete a tracked webhook by name. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Test an action (function or webhook) by name. [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Roles

All endpoints are under the `/admin/roles` prefix. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Method | Path | Description |
|---|---|---|
| `GET` | `/admin/roles/` | List all roles |
| `POST` | `/admin/roles/` | Create a role |
| `PUT` | `/admin/roles/{role_id}` | Update a role |
| `DELETE` | `/admin/roles/{role_id}` | Delete a role |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Users

All endpoints are under the `/admin/users` prefix. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Method | Path | Description |
|---|---|---|
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
|---|---|---|
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
|---|---|---|
| `POST` | `/admin/invites/` | Create an invite |
| `GET` | `/admin/invites/` | List pending invites |
| `DELETE` | `/admin/invites/{token}` | Revoke an invite |

---

### Admin GraphQL

#### `POST /admin/graphql`

Strawberry GraphQL endpoint for all admin operations: source and table CRUD, relationship management, domain configuration, RLS rules, cache control, naming conventions, scheduled task management, and query compilation. [tool-verified: `provisa/api/app.py:2171`]

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

Return first-run setup status. Always unauthenticated. [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Complete first-run setup. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Health Check

#### `GET /health` or `HEAD /health`

Returns `{"status": "ok"}`. Always unauthenticated. [tool-verified: `provisa/api/app.py:2258`]

---

## Error Responses

| Status | Meaning |
|---|---|
| 400 | Invalid query, validation error, or SQL parse error |
| 401 | Missing or invalid auth token |
| 403 | Insufficient capabilities; governance violation |
| 404 | Role, resource, or config file not found |
| 422 | Missing required header (e.g. `X-Role`) |
| 503 | Database or source not connected; dependency unavailable |
| 504 | Request timed out |

Governance violations on `POST /data/sql` return HTTP 403 with a structured body: [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

Port `8815`. Native Arrow columnar transport over gRPC. [tool-verified: `provisa/api/flight/server.py`]

Queries and catalog discovery are both available on the same connection. The full governance pipeline (RLS, masking, sampling) is applied to every query.

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

---

## Protobuf gRPC Endpoint

Port `50051` (override with `GRPC_PORT` env var or `server.grpc_port` config). [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Pass the role in the `x-provisa-role` gRPC metadata key. If absent, the server aborts with `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Download the role-specific proto from `GET /data/proto/{role_id}`. Only tables and columns visible to that role appear.

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Each table produces a `Query{TypeName}` streaming RPC. `Insert{TypeName}` RPCs exist for schema symmetry but abort with `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` is enabled for service discovery without a pre-compiled proto. [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

The gRPC server starts only when a valid proto can be compiled at startup. If schema build fails, the gRPC server does not start.

---

## JDBC Driver

The Provisa JDBC driver (`provisa-jdbc-0.1.0.jar`) exposes the semantic catalog to BI tools (Tableau, PowerBI, DBeaver).

**Connection URL:** `jdbc:provisa://host:port`

Domains map to JDBC schemas. Tables use their registered aliases. Columns use aliases and surface descriptions as `REMARKS`. Standard metadata methods (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) expose semantic relationships as PK/FK metadata.

**SQL support:** `SELECT * FROM <alias> [WHERE col = 'value']`.

The driver requests Arrow IPC redirect by default. Results stream batch-by-batch via `ArrowStreamReader`, bounded to one record batch in memory.

---

## Query Approval (Governed Queries)

Governed queries are managed through the admin GraphQL API (`POST /admin/graphql`). A governed query is a pre-approved GraphQL operation stored in the `persisted_queries` table with an approval workflow status.

This is separate from APQ (Automatic Persisted Queries). APQ is a client-side caching protocol using SHA-256 hashes. Governed queries are a data governance control: queries submitted for human review, approved or rejected, and then executable by reference to their `queryId`. [inferred from `provisa/api/data/endpoint.py` APQ handling and `_rebuild_schemas` approved_queries loading]

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

SSE subscriptions are available at `GET /subscribe/{table}` for tables with installed LISTEN/NOTIFY triggers (all pre-approved PostgreSQL-backed tables get these automatically at startup). WebSocket and RSS sources are also supported via the same endpoint. [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/api/app.py` `_rebuild_schemas`]
