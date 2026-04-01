# Provisa Architecture

## Overview

Provisa is a config-driven data virtualization platform that provides a unified GraphQL/gRPC/Arrow Flight API over heterogeneous data sources with governance, security, and performance optimization layers.

```
Config YAML → PG Metadata → Trino Catalogs
                               ↓
         Trino INFORMATION_SCHEMA → Schema Generator → GraphQL SDL (per role)
                                     ↓
                     Query → Parser → SQL Compiler → Transpiler
                                     ↓
                             Router (Smart Dispatch)
                         /           |            \
                    Trino       Direct PG      Direct MySQL/etc.
                         \           |            /
                              Executor Pool
                                     ↓
                         ┌───── Inline ─────┐     ┌──── Redirect ────┐
                         │  JSON (HTTP)     │     │  CTAS → S3       │
                         │  Arrow (Flight)  │     │  (Parquet, ORC)  │
                         │  Protobuf (gRPC) │     │  Provisa → S3    │
                         └─────────────────-┘     │  (JSON, CSV, …)  │
                                                  └─────────────────-┘
```

## Query Interfaces

Provisa exposes three query interfaces, each serving a different client type:

| Port | Protocol | Interface | Use case |
|------|----------|-----------|----------|
| 8001 | HTTP/REST | `POST /data/graphql` | Web clients, GraphiQL, REST consumers |
| 8815 | Arrow Flight (gRPC) | `do_get` with JSON ticket | Data tools (Pandas, DuckDB, Spark) |
| 50051 | Protobuf gRPC | Typed service RPCs | Service-to-service with typed contracts |

All three interfaces apply the same security pipeline (RLS, masking, sampling, role checks). Clients never talk to Trino directly.

### HTTP (port 8001)

Standard REST endpoint. Returns JSON inline by default. Supports content negotiation via `Accept` header and S3 redirect for large results.

### Arrow Flight (port 8815)

Native Arrow columnar transport over gRPC. Clients send a JSON ticket:
```json
{"query": "{ customers { name email } }", "role": "analyst"}
```
and receive Arrow RecordBatches. When Trino Flight SQL is available, data flows Arrow-native end-to-end (Trino → Provisa → client) with zero serialization overhead.

### Protobuf gRPC (port 50051)

Auto-generated `.proto` from the data schema. Streaming queries (one message per row), unary mutations. Server reflection enabled. Role via `x-provisa-role` metadata key.

## Request Pipeline

```
parse → compile → RLS inject → masking inject → MV rewrite → sampling
  → cache check → route → transpile → execute → cache store → serialize → format
```

1. **Parse**: Validate GraphQL against role's schema
2. **Compile**: GraphQL AST → single PG-style SQL (no N+1)
3. **RLS Inject**: AND per-table, per-role WHERE clauses
4. **Masking Inject**: Replace SELECT columns with mask expressions
5. **MV Rewrite**: Substitute JOIN patterns with materialized view tables
6. **Sampling**: Cap LIMIT for non-full_results roles
7. **Cache Check**: Look up Redis for identical query+role+RLS key
8. **Route**: Single RDBMS → direct driver; multi-source/NoSQL → Trino
9. **Transpile**: SQLGlot converts PG SQL to target dialect
10. **Execute**: Via direct driver pool, Trino REST, or Trino Flight SQL
11. **Cache Store**: Store result in Redis with TTL
12. **Serialize**: Flat SQL rows → nested GraphQL JSON (or Arrow pass-through)
13. **Format/Redirect**: Inline response or S3 redirect (see below)

## Trino Execution Paths

| Path | Transport | When used |
|------|-----------|-----------|
| REST | `trino` Python client (HTTP) | Default, always available |
| Flight SQL | `adbc-driver-flightsql` (gRPC :8480) | When `TRINO_FLIGHT_PORT` is configured |

Flight SQL returns data as native Arrow Tables, avoiding JSON parsing overhead. When the Arrow Flight server (port 8815) is serving a request via Trino, data flows Arrow-native end-to-end.

## Large Result Redirect

Results exceeding a row threshold are redirected to S3-compatible storage (MinIO) instead of being returned inline.

### Redirect Modes

| Mode | How it works | Data touches Provisa? |
|------|-------------|----------------------|
| **CTAS** (Parquet, ORC) | Trino writes directly to S3 via `CREATE TABLE AS SELECT` | No |
| **Provisa upload** (JSON, NDJSON, CSV, Arrow IPC) | Provisa serializes and uploads via boto3 | Yes |

For Trino-native formats, Provisa never handles the data — Trino workers write files directly to MinIO/S3. This is the preferred path for large analytical exports.

### Redirect Headers

| Header | Effect |
|--------|--------|
| `X-Provisa-Redirect-Format: <mime>` | Redirect in this format (implies force unless threshold set) |
| `X-Provisa-Redirect-Threshold: N` | Only redirect if result exceeds N rows |
| `X-Provisa-Redirect: true` | Force redirect using default format |

**Response:**
```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://minio:9000/provisa-results/results/abc.parquet?...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

### Server Configuration

| Env var | Default | Purpose |
|---------|---------|---------|
| `PROVISA_REDIRECT_ENABLED` | `false` | Enable server-side threshold redirect |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Default row count threshold |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Default redirect format |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3 bucket name |
| `PROVISA_REDIRECT_ENDPOINT` | | S3-compatible endpoint URL |
| `PROVISA_REDIRECT_TTL` | `3600` | Presigned URL TTL (seconds) |

## Routing Decision Tree

```
Multi-source query? → Trino
NoSQL source (MongoDB, Cassandra)? → Trino
Single RDBMS with driver? → Direct (sub-100ms target)
Single RDBMS without driver? → Trino
Steward hint "trino"? → Trino (override)
Steward hint "direct"? → Direct (if possible)
Redirect to Parquet/ORC? → Trino (CTAS, regardless of source count)
```

## Materialized Views

MVs transparently optimize expensive queries by pre-computing and caching results.

### Modes

| Mode | Config | Behavior |
|------|--------|----------|
| **Join-pattern** | `join_pattern` in MV config | Rewrites matching JOINs to read from MV table |
| **Custom SQL** | `sql` in MV config | Arbitrary SELECT, optionally exposed in SDL |
| **Auto-materialized relationship** | `materialize: true` on relationship | Auto-generates a join-pattern MV for cross-source relationships |

### Auto-Materialization

Cross-source JOINs are the most expensive queries (always federated through Trino). Relationships with `materialize: true` automatically generate MV definitions at startup:

```yaml
relationships:
  - id: orders-to-reviews
    source_table_id: orders        # sales-pg
    target_table_id: product_reviews  # reviews-mongo
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV
    refresh_interval: 600          # refresh every 10 minutes
```

Only cross-source relationships generate MVs (same-source JOINs are already fast via direct execution). The MV starts in `STALE` status and is refreshed by the background refresh loop before being used by the query optimizer.

### Refresh Lifecycle

```
STALE → (refresh loop picks up) → REFRESHING → FRESH
  ↑                                                |
  └──── mutation hits source table ────────────────┘
```

The refresh loop runs every 30 seconds, checks `get_due_for_refresh()`, and executes `CREATE TABLE AS SELECT` (first run) or `DELETE + INSERT` (subsequent) against the MV target table via Trino.

## Module Map

| Module | Purpose |
|--------|---------|
| `api/` | FastAPI app, routers, middleware |
| `api/flight/` | Arrow Flight server (gRPC, port 8815) |
| `compiler/` | GraphQL parser, SQL generator, RLS, masking, sampling |
| `transpiler/` | SQLGlot transpilation, routing logic |
| `executor/` | Trino/direct execution, serialization, output formats |
| `executor/trino_flight.py` | ADBC Flight SQL client for Trino |
| `executor/trino_write.py` | CTAS-based redirect (Trino writes to S3) |
| `executor/redirect.py` | S3 redirect logic, Provisa-side upload |
| `registry/` | Persisted query store, approval, governance |
| `security/` | Visibility, rights, column masking |
| `cache/` | Redis-backed query result caching |
| `mv/` | Materialized view registry, refresh, SQL rewriter |
| `discovery/` | LLM relationship discovery (Claude API) |
| `grpc/` | Proto generation, gRPC server, reflection |
| `api_source/` | REST/GraphQL/gRPC API sources with PG cache |
| `kafka/` | Kafka topic sources, sink, Schema Registry |
| `auth/` | Pluggable auth providers, middleware, role mapping |
| `core/` | Config, models, DB, repositories, secrets |

## Security Enforcement Order

1. **Rights**: Check role has `query_development` capability
2. **Schema Visibility**: Per-role schema hides unauthorized tables/columns
3. **RLS**: Per-table per-role WHERE clause injection
4. **Column Masking**: Per-column per-role data transformation
5. **Sampling**: LIMIT cap for non-full_results roles
6. **Governance**: Test mode vs production (registry-required)

All three query interfaces (HTTP, Flight, gRPC) enforce the same security pipeline.

## Scalability Limits

Any path where Provisa serializes data is bounded by Provisa process memory. Only the CTAS redirect path (Parquet/ORC) is truly unbounded — Trino workers write directly to S3 in parallel without data passing through Provisa.

| Path | Memory bound? | Suitable for |
|------|--------------|-------------|
| JSON inline (HTTP) | Yes | Small-medium results |
| Arrow Flight inline (gRPC :8815) | Yes | Medium results, analytical tools |
| Protobuf gRPC inline (:50051) | Yes | Medium results, service-to-service |
| Redirect: Provisa upload (JSON, CSV, NDJSON, Arrow IPC) | Yes | Medium results, file download |
| **Redirect: CTAS (Parquet, ORC)** | **No** | **Large/unbounded results** |

For large analytical exports, always use Parquet or ORC redirect. The data is written by Trino workers directly to S3 — Provisa only returns a presigned URL.
