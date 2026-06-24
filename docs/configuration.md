# Configuration Reference

Provisa is configured via a YAML file (default: `config/provisa.yaml`). (REQ-528)

## Sources

```yaml
sources:
  - id: sales-pg           # unique identifier
    type: postgresql        # postgresql, mysql, sqlserver, oracle, duckdb, mongodb, cassandra
    host: postgres
    port: 5432
    database: provisa
    username: provisa
    password: ${env:PG_PASSWORD}  # secret resolution
    pool_min: 1
    pool_max: 5
    use_pgbouncer: false
    pgbouncer_port: 6432
```

Supported source types: `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `snowflake`, `bigquery`, `redshift`, `databricks`, `clickhouse`, `druid`, `exasol`, `hive`, `elasticsearch`, `pinot`, `delta_lake`, `iceberg`, `mongodb`, `cassandra`, `redis`, `kudu`, `accumulo`, `kafka`, `google_sheets`, `prometheus`, `govdata`.

## GovData Sources

Sources of type `govdata` expose U.S. government open data. (REQ-540) Access is partitioned by subject grouping. [tool-verified: `provisa/core/models.py` lines 543–574]

```yaml
sources:
  - id: federal-data
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
```

### Subject Groupings [tool-verified]

Each subject maps to one or more GovData schemas. (REQ-540) Configuring a `govdata` source with a subject exposes all schemas for that subject automatically. (REQ-540)

| Subject | Schemas |
|---------|---------|
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ`, `econ_reference` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`, `fec` |

The `ref` and `geo` schemas are always included as linker schemas — not configurable and not listed above. (REQ-541) Use subject `ALL` to grant access to every schema. [tool-verified: `provisa/core/models.py` lines 561–563]


## Domains

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Naming

```yaml
naming:
  convention: camelCase   # none, snake_case (default), camelCase, PascalCase
  domain_prefix: true     # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Naming Convention

Controls how database column names are auto-aliased in the GraphQL schema. (REQ-194) Configurable at three levels (most specific wins): table → source → global. (REQ-194)

| Convention | DB Column `user_id` | DB Column `created_at` |
|------------|--------------------|-----------------------|
| `none` | `user_id` (no alias) | `created_at` |
| `snake_case` | `user_id` (no alias) | `created_at` |
| `camelCase` | `userId` | `createdAt` |
| `PascalCase` | `UserId` | `CreatedAt` |

Explicit `column.alias` always takes precedence over convention. (REQ-194)

Per-source override:
```yaml
sources:
  - id: legacy-db
    naming_convention: camelCase  # overrides global for this source
```

Per-table override:
```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: PascalCase  # overrides source for this table
```

### Domain Prefix

When `domain_prefix: true`, all GraphQL field and type names are prefixed with the domain ID using a double underscore separator: (REQ-154)

| Table | Domain | Field Name |
|-------|--------|-----------|
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

This prevents name collisions when different domains have tables with the same name, and makes queries self-documenting.

### Naming Rules

Regex rules applied to table names when generating GraphQL field names. Applied in order before uniqueness resolution. (REQ-542)

## Tables

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    alias: purchase_orders     # optional: override GraphQL name
    description: "Customer purchase orders"  # optional: GraphQL description
    governance: pre-approved    # or: registry-required
    columns:
      - name: id
        visible_to: [admin, analyst]
        writable_by: []           # read-only (empty = no writes)
      - name: email
        visible_to: [admin, analyst]
        writable_by: [admin]      # only admin can mutate
        unmasked_to: [admin]      # admin sees raw, analyst sees masked
        mask_type: regex
        mask_pattern: "^(.{2}).*(@.*)$"
        mask_replace: "$1***$2"
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        writable_by: [admin]
        unmasked_to: [admin]
        mask_type: constant
        mask_value: "0"
      - name: created_at
        visible_to: [admin, analyst]
        writable_by: []           # nobody can write
        unmasked_to: [admin]
        mask_type: truncate
        mask_precision: month
    column_presets:               # auto-set values on insert/update
      - column: created_by
        source: header            # from request header
        name: X-User-ID
      - column: updated_at
        source: now               # current timestamp
```

### Aliases

Table and column aliases override the default GraphQL name. (REQ-155) Useful for:
- Renaming cryptic database names (e.g., `tbl_cust_seg` → `customer_segments`)
- Avoiding abbreviations in the API layer
- Creating a clean, domain-specific vocabulary

### Descriptions

Table and column descriptions are included in the generated GraphQL SDL. (REQ-156) They appear in GraphiQL's documentation explorer and introspection queries. Set them in config YAML or via the admin UI.

### Path (Computed JSON Extraction)

Columns can extract values from a JSON/JSONB source column using a dot-notation `path`. (REQ-151) This is useful for semi-structured data in Kafka messages, MongoDB documents, or PostgreSQL JSONB columns.

```yaml
columns:
  - name: payload
    type: varchar
    visible_to: []            # hide the raw JSON column
  - name: order_id
    type: integer
    path: payload.order_id    # extracts from payload column
    visible_to: [admin, analyst]
  - name: customer_name
    type: varchar
    path: payload.customer.name
    visible_to: [admin, analyst]
```

The path format is `source_column.key1.key2...`. The compiler generates `json_extract_scalar(source_column, '$.key1.key2')` in the SQL. (REQ-151)

**Routing impact:** Path columns use PostgreSQL JSON operators (`->>`), which are natively supported by direct PG routing. (REQ-152) For non-PostgreSQL sources (MySQL, SQL Server, etc.), queries with path columns are automatically routed through the federation engine, where SQLGlot transpiles `->>'key'` to `json_extract_scalar`. (REQ-152) Mutations are unaffected since path columns are read-only computed fields. (REQ-153)

### Masking Types

| Type | Fields | Description |
|------|--------|-------------|
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (string columns only) |
| `constant` | `value` | Literal replacement (NULL, 0, MAX, MIN, custom) |
| `truncate` | `precision` | DATE_TRUNC (date/timestamp columns only) |

## Relationships

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one   # or: one-to-many

  - id: orders-to-reviews
    source_table_id: orders        # sales-pg source
    target_table_id: product_reviews  # reviews-mongo source
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV for this cross-source join
    refresh_interval: 600          # refresh every 10 minutes
```

### Auto-Materialization

Set `materialize: true` on a relationship to automatically generate a materialized view for cross-source JOINs. (REQ-158) This avoids expensive federated queries by pre-computing the JOIN result.

- Only cross-source relationships generate MVs (same-source JOINs are already fast) (REQ-159)
- The MV starts stale and is populated by the background refresh loop (REQ-160)
- Mutations to either source table mark the MV as stale for re-refresh (REQ-543)
- `refresh_interval` defaults to 300 seconds (5 minutes) (REQ-543)

## Roles

```yaml
roles:
  - id: admin
    capabilities:
      - source_registration
      - table_registration
      - relationship_registration
      - security_config
      - query_development
      - full_results
      - admin
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

Roles with `parent_role_id` inherit capabilities and domain access from the parent. (REQ-215) The hierarchy is flattened at startup. (REQ-215)

### Capabilities

| Capability | Description |
|-----------|-------------|
| `source_registration` | Register data sources |
| `table_registration` | Register tables |
| `relationship_registration` | Define relationships |
| `security_config` | Configure RLS, masking |
| `query_development` | Execute queries |
| `full_results` | Bypass sampling limits |
| `admin` | All capabilities |

## RLS Rules

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Materialized Views

```yaml
materialized_views:
  - id: mv-orders-customers
    source_tables: [orders, customers]
    join_pattern:
      left_table: orders
      left_column: customer_id
      right_table: customers
      right_column: id
      join_type: left
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Views (Governed Computed Datasets)

Views are SQL-defined computed datasets with full column-level governance. (REQ-133) They are the governed mechanism for adding aggregations, transformations, and derived metrics to the semantic layer. (REQ-136)

```yaml
views:
  - id: monthly-revenue
    sql: |
      SELECT DATE_TRUNC('month', created_at) AS month,
             region,
             SUM(amount) AS revenue,
             COUNT(*) AS order_count
      FROM orders
      GROUP BY 1, 2
    description: "Monthly revenue by region"
    domain_id: sales-analytics
    governance: registry-required
    materialize: true
    refresh_interval: 3600
    columns:
      - name: month
        visible_to: [admin, analyst]
      - name: region
        visible_to: [admin, analyst]
      - name: revenue
        visible_to: [admin]
      - name: order_count
        visible_to: [admin, analyst]
```

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique view identifier |
| `sql` | Yes | SQL SELECT statement defining the view |
| `domain_id` | Yes | Domain for schema visibility |
| `governance` | No | `pre-approved` (default) or `registry-required` |
| `materialize` | No | `true` = periodic CTAS refresh, `false` = live federated view |
| `refresh_interval` | No | Seconds between refreshes (materialized only, default 300) |
| `description` | No | Appears in GraphQL SDL |
| `alias` | No | Override GraphQL name |
| `columns` | Yes | Column definitions with visibility, masking, descriptions |

### Materialized vs Live

- **`materialize: true`**: Provisa creates a table via CTAS and refreshes it on a schedule. (REQ-135) Faster queries but data may be stale by up to `refresh_interval` seconds.
- **`materialize: false`**: Provisa creates a federated view. (REQ-135) Queries always return live data but may be slower for complex aggregations.

Views go through the same governance pipeline as tables — RLS, masking, sampling, and role-based visibility. (REQ-134) This ensures no new semantics can be added to the platform without steward oversight. (REQ-136)

## Kafka Sources

```yaml
kafka_sources:
  - id: event-stream
    bootstrap_servers: kafka:9092
    schema_registry_url: http://schema-registry:8081  # optional
    topics:
      - id: order-created
        topic: orders.events
        default_window: 1h          # auto-injected time bound
        schema_source: manual       # manual, registry, or sample
        value_format: json
        discriminator:              # filter shared topic by message type
          field: event_type
          value: OrderCreated
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: amount
            type: double
          - name: metadata
            type: varchar           # raw JSON for complex nested data
      - id: order-shipped
        topic: orders.events        # same physical topic
        default_window: 1h
        discriminator:
          field: event_type
          value: OrderShipped
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: shipped_at
            type: timestamp
```

### Time Window

`default_window` bounds every query to a recent time period, preventing unbounded reads from high-volume topics. (REQ-148) Format: `1h`, `30m`, `7d`, `60s`. Defaults to `1h`.

The window is auto-injected as `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. (REQ-148) Clients can override with their own `_timestamp` filter in the GraphQL `where` argument.

### Discriminator

Multiple topic configs can point to the same physical Kafka topic with different `discriminator` values, producing separate GraphQL types. (REQ-149) The discriminator is auto-injected as a WHERE clause.

### Schema Source

| Value | Behavior |
|-------|----------|
| `registry` | Fetch schema from Confluent Schema Registry |
| `manual` | Define columns inline in config (no Schema Registry needed) |
| `sample` | Auto-discover from sample messages |

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Cache Hierarchy

TTL resolution order (most specific wins): **table** > **source** > **global default**. (REQ-544) First non-null value is used.

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300              # global fallback: 5 minutes

sources:
  - id: sales-pg
    cache_enabled: true          # toggle caching for all tables in this source
    cache_ttl: 600               # source override: 10 minutes

tables:
  - source_id: sales-pg
    table: orders
    cache_ttl: 60                # table override: 1 minute (frequently changing)
  - source_id: sales-pg
    table: customers
    # no cache_ttl → inherits source TTL (600s)
```

Setting `cache_enabled: false` on a source disables caching for all tables in that source, regardless of table-level TTL. (REQ-544) Cache keys always include `role_id` + RLS context values for security partitioning. (REQ-544)

## Authentication

```yaml
auth:
  provider: simple           # none, firebase, keycloak, oauth, simple
  superuser:
    username: admin
    password: ${env:PROVISA_SUPERUSER_PASSWORD}
  simple:
    allow: true
    jwt_secret: ${env:PROVISA_JWT_SECRET}
    users:
      - username: admin
        password_hash: "$2b$12$..."
        roles: [admin]
  role_mapping:
    - claim: groups
      contains: data-analysts
      provisa_role: analyst
    default_role: analyst
```

### Auth Provider Types

| Provider | Use Case | Token Validation |
|----------|----------|-----------------|
| `simple` | Local dev/testing. Users defined in YAML. | JWT signed with `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (all methods). | `firebase-admin` SDK `verify_id_token()` |
| `keycloak` | Keycloak OIDC. Tenant + client roles mapped. | JWKS-based JWT validation |
| `oauth` | Generic OIDC (Okta, Azure AD, Auth0, PingFederate). | JWKS from discovery URL |

Superuser credentials (`superuser` block) work with any provider and always resolve to admin role with all capabilities. (REQ-125) Used for initial setup before external auth is configured.

### Full Auth Config Example (commented out)

```yaml
# auth:
#   provider: firebase
#
#   superuser:
#     username: admin
#     password: ${env:PROVISA_SUPERUSER_PASSWORD}
#
#   firebase:
#     project_id: ${env:FIREBASE_PROJECT_ID}
#     service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT}
#
#   # keycloak:
#   #   server_url: https://keycloak.example.com
#   #   # kc-tenant: set to your Keycloak tenant name (e.g. provisa)
#   #   client_id: provisa-app
#   #   client_secret: ${env:KEYCLOAK_CLIENT_SECRET}
#
#   # oauth:
#   #   discovery_url: https://login.example.com/.well-known/openid-configuration
#   #   client_id: provisa
#   #   client_secret: ${env:OAUTH_CLIENT_SECRET}
#   #   role_claim: groups
#   #   audience: provisa-api
#
#   role_mapping:
#     - claim: custom_claims.role
#       value: admin
#       provisa_role: admin
#     - claim: groups
#       contains: data-analysts
#       provisa_role: analyst
#     default_role: analyst
```

## Upsert Mutations

For tables with a primary key, Provisa auto-generates `upsert_<table>` mutation fields. (REQ-212) These compile to `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`. (REQ-212) SQLGlot transpiles to the target dialect (e.g., MySQL `ON DUPLICATE KEY UPDATE`). (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Conflict columns are derived from PK metadata. (REQ-212) All column visibility and write permission rules apply.

## Distinct On

The `distinct_on` argument selects the first row for each distinct value of the specified columns. (REQ-213) Available on root query fields.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Compiles to `SELECT DISTINCT ON (region) ...` in PostgreSQL. (REQ-213) For non-PG dialects, SQLGlot provides a window function fallback. (REQ-213)

## Column Presets

Auto-inject values into columns on insert/update. (REQ-214) Defined per table in config.

```yaml
tables:
  - source_id: sales-pg
    table: orders
    column_presets:
      - column: created_by
        source: header           # from request header
        name: X-User-ID
      - column: updated_at
        source: now              # current timestamp
      - column: source_system
        source: literal          # constant value
        value: "provisa"
```

| Source | Behavior |
|--------|----------|
| `header` | Injects value from the named HTTP request header |
| `now` | Injects `NOW()` (current timestamp) |
| `literal` | Injects a constant value |

Preset columns are injected during mutation compilation before SQL generation. (REQ-214) They are not visible in the mutation input type. (REQ-214)

## Inherited Roles

Roles can inherit capabilities and domain access from a parent role via `parent_role_id`. (REQ-215) The hierarchy is flattened at startup. (REQ-215)

```yaml
roles:
  - id: admin
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

Multi-level inheritance is supported. (REQ-215) The child role's explicit capabilities and domain_access are merged with the parent's. (REQ-215)

## Scheduled Triggers

Cron-based triggers that call a webhook URL on schedule. (REQ-216) Uses APScheduler. (REQ-216)

```yaml
scheduled_triggers:
  - name: daily-report
    cron: "0 8 * * *"           # 8:00 AM daily
    webhook_url: https://hooks.example.com/daily-report
    enabled: true
  - name: hourly-sync
    cron: "0 * * * *"           # every hour
    webhook_url: https://hooks.example.com/sync
    enabled: false
```

Scheduled tasks are managed via the admin UI (enable/disable toggle) or the `toggle_scheduled_task` admin mutation. (REQ-216)

## OrderBy Format

OrderBy uses the `{column: direction}` format with a 6-value direction enum: (REQ-200, REQ-201)

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| Direction | SQL |
|-----------|-----|
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

Relationship ordering is supported via nested objects: (REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observability

```yaml
observability:
  endpoint: "http://localhost:4319"   # OTLP collector; env OTEL_EXPORTER_OTLP_ENDPOINT overrides
  service_name: provisa               # env OTEL_SERVICE_NAME overrides
  sample_rate: 1.0                    # 0.0–1.0; TraceIdRatioBased sampler
  log_level: WARNING                  # env OTEL_LOG_LEVEL overrides
  compact_batch_size: 1000
  telemetry_filter:
    redact_sql_literals: false        # strip literal values from db.statement before export
    redact_attributes: []             # attribute keys dropped entirely before export
  # support_endpoint: ""              # env PROVISA_SUPPORT_OTLP_ENDPOINT; off by default
  support_telemetry_filter:
    redact_sql_literals: true         # default on — strip literals before sending to support
    redact_attributes: []             # additional keys dropped before sending to support
```

### Telemetry Filters [tool-verified]

Provisa runs two independent OTLP export paths: your internal collector and the optional Provisa support endpoint. (REQ-545) Each path has its own filter. Filters run inside a wrapping `_FilteringExporter` before spans leave the process — original span objects are never mutated. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — controls what reaches your internal collector.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `false` | Replaces string and numeric literals in `db.statement` with `?` |
| `redact_attributes` | list[str] | `[]` | Attribute keys dropped entirely from every span |

**`support_telemetry_filter`** — controls what reaches the Provisa support endpoint. SQL literal redaction defaults to `true` on this path, since query data belongs to you. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `true` | Replaces string and numeric literals in `db.statement` with `?` |
| `redact_attributes` | list[str] | `[]` | Attribute keys dropped entirely from every span |

Redacted `db.statement` example — with `redact_sql_literals: true`, this span attribute:

```
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

becomes:

```
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Support Endpoint [tool-verified]

`support_endpoint` (or env `PROVISA_SUPPORT_OTLP_ENDPOINT`) forwards telemetry to Provisa support for diagnostics. (REQ-548) When unset, no data leaves your infrastructure via this path. (REQ-548) The support filter applies independently of the internal filter — you can redact SQL literals from both exports while still sharing span timing and error data with support. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### Endpoint Protocol Detection [tool-verified]

Provisa selects OTLP/HTTP or OTLP/gRPC from the endpoint URL scheme. (REQ-549) URLs starting with `http://` or `https://` use OTLP/HTTP, with `/v1/traces`, `/v1/metrics`, and `/v1/logs` appended automatically. (REQ-549) Any other scheme uses OTLP/gRPC with `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROVISA_CONFIG` | `config/provisa.yaml` | Config file path |
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `provisa` | PostgreSQL database |
| `PG_USER` | `provisa` | PostgreSQL user |
| `PG_PASSWORD` | `provisa` | PostgreSQL password |
| `TRINO_HOST` | `localhost` | Federation engine host |
| `TRINO_PORT` | `8080` | Federation engine HTTP port |
| `REDIS_URL` | — | Redis connection URL |
| `PROVISA_SAMPLE_SIZE` | `100` | Default sampling limit |
| `ZAYCHIK_PORT` | `8480` | Zaychik Flight SQL proxy port |
| `FLIGHT_PORT` | `8815` | Provisa Arrow Flight server port |
| `GRPC_PORT` | `50051` | Provisa Protobuf gRPC server port |
| `PROVISA_REDIRECT_ENABLED` | `false` | Enable server-side threshold redirect |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Default row count threshold |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Default redirect format |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3 bucket for redirected results |
| `PROVISA_REDIRECT_ENDPOINT` | — | S3-compatible endpoint URL |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | S3 access key |
| `PROVISA_REDIRECT_SECRET_KEY` | — | S3 secret key |
| `PROVISA_REDIRECT_TTL` | `3600` | Presigned URL TTL (seconds) |
| `ANTHROPIC_API_KEY` | — | Claude API key (discovery) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Overrides `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Overrides `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Overrides `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Overrides `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Batch span processor flush delay |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Overrides `observability.support_endpoint` |
