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
table = client.do_get(ticket).read_all()  # pyarrow.Table
```

The full security pipeline (RLS, masking, sampling) is applied. When Trino Flight SQL is available, data flows as native Arrow end-to-end with no serialization overhead.

**Scalability:** Results are materialized in Provisa process memory as an Arrow Table before streaming to the client. For truly unbounded result sets, use Parquet/ORC redirect via the HTTP endpoint instead.

## Protobuf gRPC Endpoint

Port `50051`. Server reflection enabled.

- Streaming queries: one message per result row
- Unary mutations: single response with `affected_rows`
- Role from metadata key `x-provisa-role`
- `.proto` schema available at `GET /data/proto/{role_id}`

## Authentication

When `auth.provider` is configured:
- All endpoints require `Authorization: Bearer <token>`
- `POST /auth/login` (simple provider): `{username, password}` → `{token}`
- `/health` is unauthenticated
