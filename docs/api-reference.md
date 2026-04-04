# API Reference

## GraphQL Endpoint

### `POST /data/graphql`

Execute a GraphQL query or mutation.

**Request:**
```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin"
}
```

**Headers:**
- `X-Provisa-Role`: Override role (test mode)
- `Accept`: Inline output format (see Content Negotiation)
- `Authorization`: `Bearer <token>` (when auth enabled)
- `X-Provisa-Redirect-Format`: S3 redirect format (see Redirect)
- `X-Provisa-Redirect-Threshold`: Row count limit for conditional redirect
- `X-Provisa-Redirect`: `true` to force redirect in default format

**Response (JSON inline):**
```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99},
      {"id": 2, "amount": 150.00}
    ]
  }
}
```

**Response (single-field redirect):**
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

**Response (multi-field — some inline, some redirected):**
```json
{
  "data": {
    "orders": [{"id": 1, "amount": 99.99}],
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

Multi-root queries (multiple root fields in one GraphQL operation) are executed independently and merged. Fields below the redirect threshold are returned inline; fields above are redirected. The `redirects` key (plural) maps field names to their redirect info.

**Cache headers:**
- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>` (on HIT)

### Content Negotiation

| Accept Header | Format |
|--------------|--------|
| `application/json` | JSON (default) |
| `application/x-ndjson` | Newline-delimited JSON |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

### Redirect

When a redirect format is specified (or the result exceeds the row threshold), results are written to S3 and a presigned download URL is returned.

| Redirect Format | Written by | Scalability |
|----------------|-----------|-------------|
| `application/vnd.apache.parquet` | Trino (CTAS) | Unlimited — data never passes through Provisa |
| `application/x-orc` | Trino (CTAS) | Unlimited — data never passes through Provisa |
| `application/json` | Provisa | Memory-bound — result must fit in Provisa process memory |
| `application/x-ndjson` | Provisa | Memory-bound |
| `text/csv` | Provisa | Memory-bound |
| `application/vnd.apache.arrow.stream` | Provisa | Memory-bound |

For large analytical exports, always use Parquet or ORC redirect — Trino workers write directly to S3 in parallel without any data passing through Provisa.

**Examples:**

Force redirect to Parquet (all results):
```
X-Provisa-Redirect-Format: application/vnd.apache.parquet
```

Conditional redirect (only if over 1000 rows):
```
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

## Submit Endpoint

### `POST /data/submit`

Submit a named query for approval. The query enters the persisted query registry with status `pending`.

**Headers:** `X-Role: <role_id>` (required)

**Request:**
```json
{
  "query": "query TopOrders { orders(limit: 100) { id amount region } }",
  "operation_name": "TopOrders",
  "developer_id": "analyst@company.com",
  "business_purpose": "Weekly revenue reporting dashboard",
  "use_cases": "json, parquet",
  "data_sensitivity": "internal",
  "refresh_frequency": "daily",
  "expected_row_count": "1K-100K",
  "owner_team": "Data Engineering",
  "sink": {
    "topic": "order-updates",
    "trigger": "change_event",
    "key_column": "region"
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `query` | Yes | Named GraphQL query (`query Name { ... }`) |
| `operation_name` | Yes | Operation name (must match query) |
| `developer_id` | Yes | Who submitted |
| `business_purpose` | No | Why this query is needed |
| `use_cases` | No | Expected consumers (dashboards, APIs) |
| `data_sensitivity` | No | `public`, `internal`, `confidential`, `restricted` |
| `refresh_frequency` | No | `real-time`, `hourly`, `daily`, `weekly`, `ad-hoc` |
| `expected_row_count` | No | `<1K`, `1K-100K`, `100K+` |
| `owner_team` | No | Team responsible |
| `sink` | No | Kafka sink config (topic, trigger, key_column) |

**Response:**
```json
{
  "query_id": 42,
  "operation_name": "TopOrders",
  "message": "Query 'TopOrders' submitted for approval (id=42)."
}
```

## Compile Endpoint

### `POST /data/compile`

Compile a GraphQL query to SQL without executing it. Returns routing decision and SQL for each root field.

**Headers:** `X-Role: <role_id>` (required)

**Request:** `{"query": "{ orders { id amount } }"}`

**Response (single root field):**
```json
{
  "sql": "SELECT \"id\", \"amount\" FROM orders",
  "trino_sql": null,
  "direct_sql": "SELECT \"id\", \"amount\" FROM orders",
  "params": [],
  "route": "direct",
  "route_reason": "single source",
  "sources": ["sales-pg"],
  "root_field": "orders"
}
```

**Response (multiple root fields):**
```json
{
  "queries": [
    {"sql": "...", "root_field": "orders", "route": "direct", ...},
    {"sql": "...", "root_field": "customers", "route": "direct", ...}
  ]
}
```

## SDL Endpoint

### `GET /data/sdl`

Returns the GraphQL SDL for a role.

**Headers:** `X-Role: <role_id>` (required)

**Response:** `text/plain` GraphQL SDL string.

## Proto Endpoint

### `GET /data/proto/{role_id}`

Returns the `.proto` file for gRPC client code generation.

**Response:** `text/plain` protobuf schema.

## Admin API

### `POST /admin/graphql`

Strawberry GraphQL endpoint for admin operations (source/table/relationship CRUD).

### Discovery

- `POST /admin/discover/relationships` — Trigger LLM relationship discovery
- `GET /admin/discover/candidates` — List pending candidates
- `POST /admin/discover/candidates/{id}/accept` — Accept candidate
- `POST /admin/discover/candidates/{id}/reject` — Reject with reason

### Config Management

#### `GET /admin/config`

Download the current `provisa.yaml` configuration file.

**Response:** `application/x-yaml` with `Content-Disposition: attachment`.

#### `PUT /admin/config`

Upload a revised config YAML. The server writes a `.bak` backup of the current config, saves the new file, and reloads all schemas, sources, and MVs.

**Request body:** Raw YAML content.

**Response:**
```json
{"success": true, "message": "Config uploaded and reloaded"}
```

## Health Check

### `GET /health`

Returns `{"status": "ok"}`.

## Error Responses

| Status | Meaning |
|--------|---------|
| 400 | Invalid query, validation error |
| 401 | Invalid/expired auth token |
| 403 | Insufficient capabilities |
| 404 | Role/resource not found |
| 500 | Execution error |
| 503 | Source/service unavailable |

## Arrow Flight Endpoint

Port `8815`. Native Arrow columnar transport over gRPC.

**Ticket format** (JSON):
```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Usage (Python):**
```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
reader = client.do_get(ticket)
# Stream batch-by-batch (no full materialization):
for batch in reader:
    process(batch.data)
# Or read all at once for small results:
table = client.do_get(ticket).read_all()
```

The full security pipeline (RLS, masking, sampling) is applied. When the Zaychik Flight SQL proxy is available (port 8480), Arrow record batches stream end-to-end from Trino through Provisa to the client without materializing the full result in memory.

**Scalability:** Unbounded when Zaychik is available (streaming). Falls back to materializing in memory via Trino REST if Zaychik is unavailable.

## Protobuf gRPC Endpoint

Port `50051`. Server reflection enabled.

- Streaming queries: one message per result row
- Unary mutations: single response with `affected_rows`
- Role from metadata key `x-provisa-role`
- `.proto` schema available at `GET /data/proto/{role_id}`

## JDBC Driver

Provisa includes a JDBC driver (`provisa-jdbc-0.1.0.jar`) that exposes approved persisted queries as virtual tables for BI tools (Tableau, PowerBI, DBeaver, etc.).

**Connection URL:** `jdbc:provisa://host:port[?mode=approved|catalog]`

**Authentication:** Standard JDBC `user`/`password` properties. The driver authenticates against Provisa's auth endpoint and maps the user to a role.

### Connection Modes

| Mode | Schema | Shows | Query Execution |
|------|--------|-------|----------------|
| `approved` (default) | `approved` | Approved queries as views | Yes |
| `catalog` | Domain IDs | Registered tables with aliases | No (metadata only) |

### mode=approved (default)

Each approved query is exposed as one or more views named `{stableId}__{rootField}`. Multi-root queries produce multiple views. The root field name includes the domain prefix.

```java
Connection conn = DriverManager.getConnection("jdbc:provisa://localhost:8001", props);

// List approved query views
ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
// → bf02af78__sales_analytics__orders (VIEW)
// → bf02af78__sales_analytics__customers (VIEW)

// Execute
ResultSet rs = stmt.executeQuery("SELECT * FROM bf02af78__sales_analytics__orders");
```

### mode=catalog

Schema discovery for catalog tools (Collibra, Alation). Tables are registered tables with curated aliases and descriptions. Domains are exposed as JDBC schemas.

```java
Connection conn = DriverManager.getConnection(
    "jdbc:provisa://localhost:8001?mode=catalog", props);

// Tables use aliases, schemas are domains
ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
// → sales (schema) / clients (TABLE) — "Customer accounts"

// Columns use aliases and descriptions
ResultSet cols = conn.getMetaData().getColumns(null, null, "clients", null);
// → cust_id (REMARKS: "FK to customers")
```

### PK/FK Relationships

Both modes expose semantic relationships via standard JDBC metadata:
- `getPrimaryKeys()` — derived from the target side of many-to-one relationships
- `getImportedKeys()` / `getExportedKeys()` / `getCrossReference()` — foreign key metadata

### Column Metadata

- `COLUMN_NAME`: Uses registered column alias when set, falls back to raw name
- `REMARKS`: Column description from registration
- `TYPE_NAME`: Column data type

**SQL support:** `SELECT * FROM <viewName> [WHERE col = 'value']`. Query logic is defined in GraphQL and approved by a steward.

**Streaming:** The driver requests Arrow IPC redirect by default. Results stream batch-by-batch via `ArrowStreamReader` — memory bounded to one record batch. Falls back to JSON if redirect is unavailable.

### End-to-End Example

```
$ # 1. Submit a named query in GraphiQL
query TopOrders {
  orders(limit: 10) { id customer_id amount region status }
}
# → Click "Submit for Approval" in Provisa plugin

$ # 2. Steward approves → stable ID assigned
mutation { approveQuery(queryId: 3) { success message } }
# → "Query approved with stable ID: bf02af78-..."

$ # 3. BI tool connects via JDBC
$ java -cp provisa-jdbc-0.1.0.jar:. JdbcTest

=== Listing approved queries (getTables) ===
  TABLE: bf02af78-...  (Approved query: TopOrders)

=== Querying: bf02af78-... ===
id              | customer_id     | amount          | region          | status
-------------------------------------------------------------------------------
1               | 1               | 19.99           | us-east         | completed
2               | 1               | 99.98           | us-east         | completed
3               | 2               | 29.99           | us-west         | completed
...
10 rows returned.
```

## Authentication

When `auth.provider` is configured:
- All endpoints require `Authorization: Bearer <token>`
- `POST /auth/login` (simple provider): `{username, password}` → `{token}`
- `/health` is unauthenticated
