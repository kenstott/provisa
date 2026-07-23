# Configuration Reference

Provisa is configured via a YAML file (default: `config/provisa.yaml`). (REQ-528)

## Sources

```yaml
sources:
  - id: sales-pg           # unique identifier
    type: postgresql
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

All sources share a common field set. [tool-verified: `provisa/core/models.py:129-212`]

| Field | Default | Notes |
|-------|---------|-------|
| `id` | required | Alphanumeric, hyphens, underscores |
| `type` | required | See table below |
| `host` | `""` | Hostname or IP |
| `port` | `0` | `0` means each connector supplies its own default — there is no central default-port map |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Supports `${env:VAR}` secret resolution |
| `path` | `null` | File path or URI for file-based sources |
| `base_url` | `null` | Base URL for API sources |
| `pool_min` / `pool_max` | `1` / `5` | Connection pool bounds |
| `cache_enabled` | `true` | Toggle caching for all tables in this source |
| `cache_ttl` | `null` | Seconds; `null` inherits the global default |
| `federation_hints` | `{}` | Per-connector extended parameters (dict[str,str]); see type reference below. REQ-281 |
| `mapping` | `{}` | Mapping DSL for redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Restrict this source to specific domain IDs; empty = unrestricted |
| `description` | `""` | |

### Supported source types [tool-verified: `provisa/core/models.py:36-101`]

| Type | Connection style | Notes |
|------|-----------------|-------|
| **RDBMS** | | |
| `postgresql` | host/port | Asyncpg pool; PgBouncer opt-in via `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (DB file) | DuckDB firebird community extension (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Reuses PostgreSQL driver/dialect (REQ-950) |
| `yugabytedb` | host/port | Reuses PostgreSQL driver/dialect (REQ-950) |
| `greenplum` | host/port | Reuses PostgreSQL driver/dialect (REQ-950) |
| `tidb` | host/port | Reuses MySQL driver/dialect (REQ-950) |
| **Cloud DW** | | |
| `snowflake` | host/port + `federation_hints` | `account` required in hints |
| `bigquery` | `federation_hints` | `project` required; auth via `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` required in hints |
| `fabric` | env vars or `PROVISA_ENGINE_URL` | T-SQL over TDS, Azure AD auth |
| `synapse` | env vars or `PROVISA_ENGINE_URL` | T-SQL over TDS, Azure AD auth |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | `secure` hint toggles TLS; port defaults 8123/8443 |
| `elasticsearch` | host/port + `mapping` DSL | |
| `pinot` | host/port | Controller REST endpoint |
| `druid` | host/port | Broker Avatica endpoint |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (table URI) | DuckDB `delta_scan`; object-store access via `federation_hints` |
| `iceberg` | `path` (table URI) | DuckDB `iceberg_scan`; object-store access via `federation_hints` |
| `hudi` | `path` (table URI) | ClickHouse Hudi engine, zero-copy (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | Storage backend in `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + `mapping` S3 keys | Distinct type; always S3 storage (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Plain connection fields; no mapping DSL |
| `cassandra` | host/port | Plain connection fields; no mapping DSL |
| `redis` | host/port + `mapping` DSL | |
| **Streaming** | | |
| `kafka` | registration-only | Real config lives in `kafka_sources[]`; see §Kafka below |
| `websocket` | host/port/path + `federation_hints` | External WebSocket feed |
| `rss` | host/port/path + `federation_hints` | RSS 2.0 / Atom feed |
| **Graph/Semantic** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **File** | | |
| `sqlite` | `path` | Always routes through engine (no direct pool) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (directory) | Glob crawler; surfaces CSV/Parquet/XLSX/JSON as tables |
| **API/Remote** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port or `mapping.url` + `mapping` DSL | |
| `graphql_remote` | `base_url` + optional `mapping` | Headers, forward-client-headers, timeout in `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (Flight location) | DuckDB airport extension (REQ-899) |
| `ingest` | push receiver | External services POST JSON events |
| **SaaS** | | |
| `sharepoint` | `base_url` or `host` + `mapping` | Auth via `mapping.auth_type` |
| `splunk` | `host`/`port` or `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Separate `GovDataSource` model; see §GovData below |

### Source type reference

Types needing non-obvious config each have a short entry below. RDBMS types (postgresql, mysql, etc.) use only the common fields above — no additional section needed.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

`govdata` sources use a separate top-level model, `GovDataSource`, not the generic `Source`. (REQ-540) Access is partitioned by subject grouping.

```yaml
sources:
  - id: federal-data
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    api_key: ${env:GOVDATA_API_KEY}   # optional
    start_year: 2020                   # optional year filter
    end_year: 2024                     # optional year filter
```

Each subject maps to one or more GovData schemas. Configuring a `govdata` source with a subject exposes all schemas for that subject automatically. (REQ-540)

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
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec` |

The `ref` and `geo` schemas are always included as linker schemas — not configurable and not listed above. (REQ-541) Use subject `ALL` to grant access to every schema. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

The `kafka` row in `sources:` is registration-only. Its connector's `details()` returns `{}` — the real configuration lives in the top-level `kafka_sources[]` block, not in a `sources:` row. Kafka is always a VIRTUAL_SOURCE (routes through the engine; no direct pool). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**Time Window** — `default_window` bounds every query to a recent time period, preventing unbounded reads from high-volume topics. (REQ-148) Format: `1h`, `30m`, `7d`, `60s`. Defaults to `1h`. Auto-injected as `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Clients can override with their own `_timestamp` filter in the GraphQL `where` argument.

**Discriminator** — Multiple topic configs can point to the same physical Kafka topic with different `discriminator` values, producing separate GraphQL types. (REQ-149) The discriminator is auto-injected as a WHERE clause.

**Schema Source**

| Value | Behavior |
|-------|----------|
| `registry` | Fetch schema from Confluent Schema Registry |
| `manual` | Define columns inline in config (no Schema Registry needed) |
| `sample` | Auto-discover from sample messages |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` in `federation_hints` is required. `warehouse`, `role`, and `schema` are optional.

```yaml
sources:
  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    database: MY_DB
    federation_hints:
      account: myorg-myaccount     # required
      warehouse: COMPUTE_WH
      role: PROVISA_ROLE
      schema: PUBLIC               # remote schema override
```

#### Databricks [tool-verified: `provisa/executor/drivers/databricks.py:34-52`]

`http_path` in `federation_hints` is required. `password` carries the personal access token. `catalog` is optional (carried in SQL/hints, not the `database` field).

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required
      catalog: my_unity_catalog              # optional
```

#### BigQuery [tool-verified: `provisa/federation/connector_duckdb.py:238`]

`project` in `federation_hints` is required. Authentication uses `GOOGLE_APPLICATION_CREDENTIALS` (path to a service-account key file) or Application Default Credentials in the engine environment.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Both use T-SQL over TDS with Azure AD authentication. Authenticate with `az login` (developer) or a managed identity (production) — the engine reads credentials via `azure-identity`'s `DefaultAzureCredential`. Connection details come from env vars: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) or `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), or via `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` in `federation_hints` enables TLS on the HTTP interface. Port defaults to `8123` (plain) or `8443` (when `secure: "true"`). `schema` in `federation_hints` overrides the remote schema. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

```yaml
sources:
  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"    # uses port 8443; omit to use 8123
      schema: analytics
```

#### Delta Lake / Iceberg [tool-verified: `provisa/federation/connector_duckdb.py:291-327`]

`path` is the table URI (S3, GCS, ADLS, or local). Object-store access needs `federation_hints` credentials. For Cloudflare R2, add `account_id`.

```yaml
sources:
  - id: events-delta
    type: delta_lake
    path: s3://my-bucket/data/events
    federation_hints:
      access_key_id: ${env:S3_ACCESS_KEY}
      secret_access_key: ${env:S3_SECRET}

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

#### Hive / Hive S3 [tool-verified: `provisa/federation/trino_connectors.py:244-363`]

`host` and `port` point to the Hive Thrift metastore (default port 9083). For `hive`, set `mapping["storage"]` to choose the object store backend. Missing required keys fail loud — no fallback. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` is a distinct type that always declares S3 storage (REQ-229); no `mapping.storage` needed.

```yaml
sources:
  - id: hive-s3-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: s3
      endpoint: https://s3.us-east-1.amazonaws.com
      access_key_id: ${env:AWS_ACCESS_KEY_ID}
      secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
      region: us-east-1
      path_style: true           # required for MinIO and non-AWS S3-compatible endpoints

  - id: hive-adls-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: adls
      storage_account: mystorageaccount
      access_key: ${env:ADLS_ACCESS_KEY}
      # sas_token: ${env:ADLS_SAS_TOKEN}   # alternative to access_key
```

`mapping.storage` accepted values: `hadoop` (default), `hdfs`, `local`, `s3`, `azure`, `adls`. S3 mapping keys: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. ADLS mapping keys: `storage_account`, `access_key` or `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Uses the `mapping` DSL. `mongodb` and `cassandra` use plain connection fields and do NOT use the mapping DSL.

```yaml
sources:
  - id: my-redis
    type: redis
    host: redis.internal
    port: 6379
    password: ${env:REDIS_PASSWORD}
    mapping:
      tables:
        - name: sessions
          key_pattern: "session:*"
          key_column: key           # default "key"
          value_type: hash          # hash | string | zset | list; default hash
          columns:
            - name: user_id
              data_type: VARCHAR
              field: user_id        # Redis hash field name
            - name: expires_at
              data_type: BIGINT
              field: expires_at
```

#### Elasticsearch [tool-verified: `provisa/core/trino_catalog_files.py:78-104`]

```yaml
sources:
  - id: my-es
    type: elasticsearch
    host: es.internal
    port: 9200
    username: elastic
    password: ${env:ES_PASSWORD}
    mapping:
      tls: true
      tables:
        - name: logs
          index: app-logs-*
          discover: false
          columns:
            - name: timestamp
              data_type: TIMESTAMP
              path: "@timestamp"
            - name: level
              data_type: VARCHAR
              path: level
            - name: message
              data_type: VARCHAR
              path: message
```

#### Prometheus [tool-verified: `provisa/core/trino_catalog_files.py:107-124`]

`mapping.url` overrides `host:port` when both are present.

```yaml
sources:
  - id: my-prometheus
    type: prometheus
    mapping:
      url: http://prometheus.internal:9090
      tables:
        - name: http_requests
          metric: http_requests_total
          labels_as_columns: [method, status, handler]
          value_column: value      # default "value"
          default_range: 1h        # default "1h"
```

#### Google Sheets [tool-verified: `provisa/federation/connector_duckdb.py:273-275`]

`spreadsheet_id` in `federation_hints` is required. Auth uses a DuckDB `gsheet` SECRET provisioned at attach time.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### File sources (csv / parquet / sqlite / files)

`path` is required. `files` crawls a directory for CSV, Parquet, XLSX, and JSON files, surfacing each as a table. All file-based sources are VIRTUAL (route through the engine; no direct pool). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### API / Remote sources

**openapi** — set `base_url` to the OpenAPI base URL. Schema discovery reads the OpenAPI spec at startup.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — set `base_url`. Optional `mapping` keys: `headers` (dict of static headers), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

```yaml
sources:
  - id: orders-gql
    type: graphql_remote
    base_url: https://orders.internal/graphql
    mapping:
      headers:
        X-Api-Key: ${env:ORDERS_API_KEY}
      forward_client_headers: true
      timeout_seconds: 30
```

**airport** — `base_url` is the Arrow Flight server location. DuckDB airport extension (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — use `host`, `port`, `path`, and `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

```yaml
sources:
  - id: market-feed
    type: websocket
    host: feed.example.com
    port: 443
    path: /ws/v1
    federation_hints:
      use_ssl: "true"
      subscribe_payload: '{"action":"subscribe","channels":["ticker"]}'
      event_path: data

  - id: news-rss
    type: rss
    host: feeds.example.com
    port: 443
    path: /rss/latest
    federation_hints:
      use_ssl: "true"
      poll_interval: "300"      # seconds
      # feed_url: https://...  # overrides host/port/path when set
```

**sharepoint** [tool-verified: `provisa/federation/trino_connectors.py:394-423`]

```yaml
sources:
  - id: my-sharepoint
    type: sharepoint
    base_url: https://myorg.sharepoint.com/sites/data
    username: ${env:SP_CLIENT_ID}
    password: ${env:SP_CLIENT_SECRET}
    database: ${env:SP_TENANT_ID}
    mapping:
      auth_type: CLIENT_CREDENTIALS   # default
      # certificate_path: /path/to/cert.pem
      # certificate_password: ${env:CERT_PASSWORD}
```

**splunk** [tool-verified: `provisa/federation/trino_connectors.py:426-457`]

```yaml
sources:
  - id: my-splunk
    type: splunk
    host: splunk.internal
    port: 8089
    password: ${env:SPLUNK_TOKEN}
    database: search           # Splunk app name (optional)
    mapping:
      use_token: true          # default; false = username/password auth
      datamodel_filter: ""     # optional Splunk Data Model filter
      disable_ssl_validation: false
```

## Domains

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Naming

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Naming Convention

The naming authority is the single source of truth for client-facing names; physical backend column names are never exposed to clients. (REQ-194) Each query language derives a column's name from its `column.alias` if set, otherwise from the physical column name via its configured convention. (REQ-194)

The GraphQL convention is one of three preset enums. (REQ-416) Old free-form strings (`none`, `snake_case`, `camelCase`, `PascalCase`) are deprecated. (REQ-416)

| Preset | Default | Type names | Field names | Mutation names |
|--------|---------|------------|-------------|----------------|
| `apollo_graphql` | yes | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

The default GraphQL convention is `apollo_graphql`, which produces camelCase field and mutation names. (REQ-194, REQ-416) The SQL convention is separate, with default `snake_case`, applied via `apply_sql_name()`; the GraphQL convention is applied via `apply_gql_name()`, and the CQL name is derived from the GraphQL name. (REQ-194)

`domain_prefix: bool` is an orthogonal option that applies regardless of the chosen preset. (REQ-416)

Explicit `column.alias` is the canonical name: SQL uses it verbatim with no convention applied, GraphQL applies its convention to it, and CQL derives from the GraphQL name. (REQ-194)

Per-source override:
```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Per-table override:
```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
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

**Routing impact:** Path columns use PostgreSQL JSON operators (`->>`), which are natively supported by direct PG routing. (REQ-152) For non-PostgreSQL sources (MySQL, SQL Server, etc.), queries with path columns are automatically routed through the federation engine. (REQ-152) Mutations are unaffected since path columns are read-only computed fields. (REQ-153)

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
| `materialize` | No | `true` = periodic CTAS refresh, `false` = live federated view |
| `refresh_interval` | No | Seconds between refreshes (materialized only, default 300) |
| `description` | No | Appears in GraphQL SDL |
| `alias` | No | Override GraphQL name |
| `columns` | Yes | Column definitions with visibility, masking, descriptions |

### Materialized vs Live

- **`materialize: true`**: Provisa creates a table via CTAS and refreshes it on a schedule. (REQ-135) Faster queries but data may be stale by up to `refresh_interval` seconds.
- **`materialize: false`**: Provisa creates a federated view. (REQ-135) Queries always return live data but may be slower for complex aggregations.

Views go through the same governance pipeline as tables — RLS, masking, sampling, and role-based visibility. (REQ-134) This ensures no new semantics can be added to the platform without steward oversight. (REQ-136)

### Query-only views

Both `materialize: true` and `materialize: false` views expose their GraphQL type as query-only. No insert, upsert, update, or delete mutations are generated for `view_sql`-backed relations. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

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

For tables with a primary key, Provisa auto-generates `upsert_<table>` mutation fields. (REQ-212) These compile to an upsert in the target dialect — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` on PostgreSQL, `ON DUPLICATE KEY UPDATE` on MySQL. (REQ-212)

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

Compiles to `SELECT DISTINCT ON (region) ...` in PostgreSQL. (REQ-213) For non-PG dialects, a window-function fallback is used. (REQ-213)

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

## Federation Engine

Configuring a federation engine is optional. The default is `duckdb` — zero-config, in-process, no external service required (REQ-989). Choose another engine when you need MPP scale or want to reuse an existing warehouse.

Precedence: `PROVISA_ENGINE` env var → persisted admin-UI `federation_engine` config field → `duckdb`. Changes take effect on service restart. [tool-verified: `engine.py` `build_engine`]

### Engine overview [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Engine key | Label | Dialect | MPP | External-link mechanism | Auth |
|-----------|-------|---------|-----|------------------------|------|
| `trino` | Provisa Federation Engine | Trino SQL | Yes | Trino catalogs (broad connector set) | JDBC credentials |
| `trino-byo` | Trino (bring-your-own) | Trino SQL | Yes | Same as `trino`; unmanaged coordinator | JDBC credentials |
| `pg` | PostgreSQL | PostgreSQL | No | FDW / pg_duckdb | PostgreSQL credentials |
| `duckdb` | DuckDB | DuckDB | No | Extension-native ATTACH | None (in-process) |
| `clickhouse` | ClickHouse (embedded) | ClickHouse | Yes | S3 / IcebergS3 / DeltaLake table engines | chdb (in-process, no auth) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Yes | S3 / IcebergS3 / DeltaLake table engines | ClickHouse credentials |
| `snowflake` | Snowflake | Snowflake | Yes | External stage + external table | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Yes | Unity Catalog external tables via REST | `PROVISA_ENGINE_URL` (bearer token + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Yes | BigQuery external / BigLake tables | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Yes | OneLake shortcuts → OPENROWSET | Azure AD (`az login` or managed identity) |
| `synapse` | Azure Synapse | T-SQL | Yes | ADLS OPENROWSET / external tables | Azure AD |
| `sqlalchemy` | SQLAlchemy (any RDB) | Per-dialect | No | None (land-only) | Per-dialect credentials |

### Engine reference

#### trino / trino-byo

`trino` is the managed Provisa coordinator; `trino-byo` connects to your own Trino cluster. Both use Trino SQL and have the broadest source type reach.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Materialization store defaults to `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Federates via postgres_fdw (SQL/MED) and pg_duckdb extensions. Single-node; no MPP. Best when your data already lives in PostgreSQL and you want to join in a few remote sources.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Materialization store defaults to `TENANT_DATABASE_URL`.

#### duckdb

In-process; no external service. The default engine (REQ-989). `PROVISA_DATA_DIR` controls where the embedded store lives (`~/.provisa` by default).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Materialization store defaults to `~/.provisa/materialize.duckdb` — the only engine with a non-PostgreSQL default store.

#### clickhouse (embedded) / clickhouse-server

`clickhouse` uses chdb (in-process). `clickhouse-server` connects to an external ClickHouse instance or ClickHouse Cloud. Both read Delta Lake, Iceberg, and Hudi directly via native ClickHouse table engines.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

Materialization store defaults to `TENANT_DATABASE_URL`.

#### snowflake

Engine-as-warehouse: Snowflake runs the queries; Provisa pushes source data through external stages.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Materialization store defaults to `TENANT_DATABASE_URL`.

#### databricks

Unity Catalog external tables bridge Provisa-managed sources into Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Materialization store defaults to `TENANT_DATABASE_URL`.

#### bigquery

BigQuery external and BigLake tables. Project comes from the URL or `GOOGLE_CLOUD_PROJECT`; auth via service-account key.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Materialization store defaults to `TENANT_DATABASE_URL`.

#### fabric / synapse

Both use T-SQL over TDS with Azure AD auth (`az login` or managed identity). Omit `PROVISA_ENGINE_URL` to read connection details from env vars instead.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Materialization store defaults to `TENANT_DATABASE_URL`.

#### sqlalchemy

Generic RDBMS land-only engine (no federation to external sources). Use for single-warehouse deployments or testing.

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

Materialization store defaults to `TENANT_DATABASE_URL`.

### Materialization store

When a source cannot attach live (no ATTACH connector for the selected engine), it lands into the engine's materialization store. Resolution order: explicit `PROVISA_MATERIALIZE_URL` → engine's declared default → hard error (no silent fallback). [tool-verified: `engine.py` `materialize_store`]

DuckDB declares its embedded file (`~/.provisa/materialize.duckdb`) as its default. All other engines default to `TENANT_DATABASE_URL` (PostgreSQL). Override any engine with `PROVISA_MATERIALIZE_URL`.

### Per-source federation hints

Extended connection parameters that standard host/port/user/password fields cannot carry go in `federation_hints` on the source. See the source type reference above for per-type hint keys. A consolidated example:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required for Databricks sources

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount
      warehouse: COMPUTE_WH

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"           # enable TLS on the HTTP interface

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

For Google Cloud sources, set `GOOGLE_APPLICATION_CREDENTIALS` to the path of your service-account key file. For Fabric and Synapse, authenticate with `az login` (developer) or a managed identity (production) — the engine reads credentials via `azure-identity`'s `DefaultAzureCredential`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROVISA_CONFIG` | `config/provisa.yaml` | Config file path |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | Control-plane store URI (SQLAlchemy async); accepts `sqlite+aiosqlite://…` / `duckdb://…` for the embedded desktop store (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | Platform registry URI (tenant directory, engine registry); required at startup, no fallback (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` uses embedded fakeredis instead of a Redis server — no Docker (REQ-829) |
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `provisa` | PostgreSQL database |
| `PG_USER` | `provisa` | PostgreSQL user |
| `PG_PASSWORD` | `provisa` | PostgreSQL password |
| `PROVISA_ENGINE` | `duckdb` | Federation engine key (REQ-989, REQ-916) |
| `PROVISA_ENGINE_URL` | — | Connection URL for URL-driven engines (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Override materialization store DSN (defaults to engine's declared default) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Data directory for the embedded DuckDB store (REQ-989) |
| `TRINO_HOST` | `localhost` | Trino coordinator host |
| `TRINO_PORT` | `8080` | Trino coordinator HTTP port |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to GCP service-account key JSON (BigQuery engine/source) |
| `GOOGLE_CLOUD_PROJECT` | — | Default GCP project (BigQuery; overridden by URL) |
| `FABRIC_SQL_SERVER` | — | Fabric Warehouse SQL endpoint (alternative to `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Fabric Warehouse database name |
| `SYNAPSE_SQL_SERVER` | — | Synapse serverless SQL endpoint |
| `SYNAPSE_DATABASE` | — | Synapse database name |
| `REDIS_URL` | — | Redis connection URL |
| `PROVISA_SAMPLE_SIZE` | `10000` | Default sampling limit |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Row cap when a query supplies no explicit `LIMIT` |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Tier-1 read-retry budget in seconds; exponential backoff with full jitter (REQ-703) |
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
