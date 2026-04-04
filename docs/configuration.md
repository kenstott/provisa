# Configuration Reference

Provisa is configured via a YAML file (default: `config/provisa.yaml`).

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

Supported source types: `postgresql`, `mysql`, `sqlserver`, `oracle`, `duckdb`, `snowflake`, `bigquery`, `mongodb`, `cassandra`.

## Domains

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Naming

```yaml
naming:
  domain_prefix: true  # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Domain Prefix

When `domain_prefix: true`, all GraphQL field and type names are prefixed with the domain ID using a double underscore separator:

| Table | Domain | Field Name |
|-------|--------|-----------|
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

This prevents name collisions when different domains have tables with the same name, and makes queries self-documenting.

### Naming Rules

Regex rules applied to table names when generating GraphQL field names. Applied in order before uniqueness resolution.

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

Table and column aliases override the default GraphQL name. Useful for:
- Renaming cryptic database names (e.g., `tbl_cust_seg` → `customer_segments`)
- Avoiding abbreviations in the API layer
- Creating a clean, domain-specific vocabulary

### Descriptions

Table and column descriptions are included in the generated GraphQL SDL. They appear in GraphiQL's documentation explorer and introspection queries. Set them in config YAML or via the admin UI.

### Path (Computed JSON Extraction)

Columns can extract values from a JSON/JSONB source column using a dot-notation `path`. This is useful for semi-structured data in Kafka messages, MongoDB documents, or PostgreSQL JSONB columns.

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

The path format is `source_column.key1.key2...`. The compiler generates `json_extract_scalar(source_column, '$.key1.key2')` in the SQL.

**Routing impact:** Path columns use PostgreSQL JSON operators (`->>`), which are natively supported by direct PG routing. For non-PostgreSQL sources (MySQL, SQL Server, etc.), queries with path columns are automatically routed through Trino, where SQLGlot transpiles `->>'key'` to `json_extract_scalar`. Mutations are unaffected since path columns are read-only computed fields.

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

Set `materialize: true` on a relationship to automatically generate a materialized view for cross-source JOINs. This avoids expensive federated queries through Trino by pre-computing the JOIN result.

- Only cross-source relationships generate MVs (same-source JOINs are already fast)
- The MV starts stale and is populated by the background refresh loop
- Mutations to either source table mark the MV as stale for re-refresh
- `refresh_interval` defaults to 300 seconds (5 minutes)

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
      - query_approval
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

Roles with `parent_role_id` inherit capabilities and domain access from the parent. The hierarchy is flattened at startup.

### Capabilities

| Capability | Description |
|-----------|-------------|
| `source_registration` | Register data sources |
| `table_registration` | Register tables |
| `relationship_registration` | Define relationships |
| `security_config` | Configure RLS, masking |
| `query_development` | Execute queries |
| `query_approval` | Approve persisted queries |
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

Views are SQL-defined computed datasets with full column-level governance. They are the governed mechanism for adding aggregations, transformations, and derived metrics to the semantic layer.

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
| `materialize` | No | `true` = periodic CTAS refresh, `false` = live Trino view |
| `refresh_interval` | No | Seconds between refreshes (materialized only, default 300) |
| `description` | No | Appears in GraphQL SDL |
| `alias` | No | Override GraphQL name |
| `columns` | Yes | Column definitions with visibility, masking, descriptions |

### Materialized vs Live

- **`materialize: true`**: Provisa creates a table via CTAS and refreshes it on a schedule. Faster queries but data may be stale by up to `refresh_interval` seconds.
- **`materialize: false`**: Provisa creates a Trino view. Queries always return live data but may be slower for complex aggregations.

Views go through the same governance pipeline as tables — RLS, masking, sampling, role-based visibility, approval workflow. This ensures no new semantics can be added to the platform without steward oversight.

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

`default_window` bounds every query to a recent time period, preventing unbounded reads from high-volume topics. Format: `1h`, `30m`, `7d`, `60s`. Defaults to `1h`.

The window is auto-injected as `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Clients can override with their own `_timestamp` filter in the GraphQL `where` argument.

### Discriminator

Multiple topic configs can point to the same physical Kafka topic with different `discriminator` values, producing separate GraphQL types. The discriminator is auto-injected as a WHERE clause.

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

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROVISA_CONFIG` | `config/provisa.yaml` | Config file path |
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `provisa` | PostgreSQL database |
| `PG_USER` | `provisa` | PostgreSQL user |
| `PG_PASSWORD` | `provisa` | PostgreSQL password |
| `TRINO_HOST` | `localhost` | Trino host |
| `TRINO_PORT` | `8080` | Trino HTTP port |
| `REDIS_URL` | — | Redis connection URL |
| `PROVISA_SAMPLE_SIZE` | `100` | Default sampling limit |
| `TRINO_FLIGHT_PORT` | `8480` | Zaychik Flight SQL proxy port |
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
