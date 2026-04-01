# Provisa Architecture

## Overview

Provisa is a config-driven data virtualization platform, specifically designed to power a semantic layer from small teams to large enterprises. It provides a unified GraphQL/gRPC/Arrow Flight API over heterogeneous data sources with governance, security, and performance optimization.

The semantic layer distinction is important. Provisa uses GraphQL as a universal query language specifically because it can only composite existing semantics. To add to the semantic layer you must create new data sources or aggregates within the data virtualization layer. This creates a clean separation — no new additions to the semantics can be made outside the platform, enabling true data governance.

Provisa is designed to be highly performant for operational needs and highly scalable for enterprise analytical needs. A single platform serves both without sacrificing speed or scalability.

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
and receive Arrow RecordBatches streamed lazily. When the Zaychik Flight SQL proxy is available, data flows as a stream of Arrow record batches end-to-end:

```
Client ←(Arrow batches)← Provisa Flight Server ←(Arrow batches)← Zaychik ←(JDBC)← Trino
```

The full result is never materialized in Provisa memory — batches are forwarded as they arrive. This makes Arrow Flight an **unbounded** path suitable for arbitrarily large results.

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

| Path | Transport | Via | When used |
|------|-----------|-----|-----------|
| REST | `trino` Python client (HTTP :8080) | Direct to Trino | Default, always available |
| Flight SQL | `adbc-driver-flightsql` (gRPC :8480) | Zaychik proxy → Trino JDBC | When Zaychik is running |
| CTAS | `trino` Python client (HTTP :8080) | Direct to Trino, writes Iceberg to S3 | Parquet/ORC redirect |

### Zaychik Arrow Flight SQL Proxy

Trino does not natively support the Arrow Flight SQL protocol. [Zaychik](https://github.com/Raiffeisen-DGTL/zaychik-trino-proxy) is a Java proxy that implements the Arrow Flight SQL gRPC interface, translates requests to Trino JDBC queries, and streams results back as Arrow record batches.

```
ADBC client → gRPC :8480 → Zaychik → JDBC :8080 → Trino → results → Arrow batches → client
```

The Provisa Flight server (port 8815) connects to Zaychik as an ADBC client, enabling streaming Arrow end-to-end without materializing results.

### Iceberg Results Catalog

CTAS redirect uses an Iceberg connector (`results` catalog) backed by a JDBC catalog on the existing PostgreSQL instance. Iceberg writes Parquet/ORC files directly to MinIO/S3 via the native S3 filesystem (`fs.native-s3.enabled=true`).

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
Uses path columns on non-PG source? → Trino
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

Provisa is a thin compilation and routing layer — it adds single-digit milliseconds to query latency. However, paths where Provisa serializes result data are bounded by process memory. Two paths are truly unbounded:

| Path | Memory bound? | Suitable for |
|------|--------------|-------------|
| JSON inline (HTTP) | Yes | Small-medium results |
| **Arrow Flight streaming (gRPC :8815)** | **No** | **Unbounded — streaming via Zaychik** |
| Protobuf gRPC inline (:50051) | Yes | Medium results, service-to-service |
| Redirect: Provisa upload (JSON, CSV, NDJSON, Arrow IPC) | Yes | Medium results, file download |
| **Redirect: CTAS (Parquet, ORC)** | **No** | **Unbounded — Trino writes to S3** |

### Threshold Probing

For threshold-based redirect, Provisa injects `LIMIT threshold + 1` into the query as a probe. If the result has fewer rows, it returns inline (complete result, no wasted work). If the result hits the limit, the probe is discarded and the full query is re-executed via CTAS or Provisa upload. This avoids `SELECT COUNT(*)` (which some sources don't optimize) and works on every source.

For large analytical workloads, use either:
- **Arrow Flight** (port 8815) for streaming to data tools — batches flow through Provisa without materializing
- **Parquet/ORC redirect** for file-based exports — Trino writes directly to S3, Provisa returns a presigned URL

## Infrastructure

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| Provisa API | (host process) | 8001 | HTTP/REST endpoint |
| Provisa Flight | (host process) | 8815 | Arrow Flight gRPC server |
| Provisa gRPC | (host process) | 50051 | Protobuf gRPC server |
| Trino | `trinodb/trino:480` | 8080 | Query federation engine |
| Zaychik | `provisa-zaychik` (built from source) | 8480 | Arrow Flight SQL proxy for Trino |
| PostgreSQL | `postgres:16` | 5432 | Config metadata + Iceberg catalog |
| MongoDB | `mongo:7` | 27017 | Demo NoSQL data source |
| MinIO | `minio/minio` | 9000/9001 | S3-compatible object storage |
| Redis | `redis:7-alpine` | 6379 | Query result cache |
| PgBouncer | `edoburu/pgbouncer` | 6432 | Connection pooling for PG |
| Kafka | `confluentinc/cp-kafka:7.6.0` | 9092 | Streaming data sources |
| Schema Registry | `confluentinc/cp-schema-registry:7.6.0` | 8081 | Avro/Protobuf schema management |
