# Configuration Reference

Provisa is configured via a YAML file (default: `config/provisa.yaml`).

## Includes

Split a config across multiple files with `includes:`. The including file lists fragment paths under this key; Provisa merges them before validation, producing the same result as writing everything in one file.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

A wrapper file containing only `includes:` is valid. `load_control_plane` reads through includes, so the `control_plane:` section is taken from whichever included file sets it.

**Merge rules**

- Paths resolve relative to the including file. Absolute paths are used as-is.
- List sections (`sources`, `tables`, `domains`, `relationships`, `roles`, …) append, and fragment entries follow the including file's entries.
- A scalar or mapping key the including file does not set is taken from the fragment.
- A key both files set to different values is a conflict; the load fails, naming the key.
- Identical values in both files are not a conflict.
- Includes nest. A file that includes itself — directly or through another fragment — is refused.
- `includes` is consumed at load time and never appears in the validated config.

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

All sources share a common field set.

| Field | Default | Notes |
| ------- | --------- | ------- |
| `id` | required | Alphanumeric, hyphens, underscores |
| `type` | required | See table below |
| `host` | `""` | Hostname or IP |
| `port` | `0` | `0` means each connector supplies its own default — there is no central default-port map |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Supports `${env:VAR}` and `${secret:NAME}` credential references — see [Secrets](secrets.md) |
| `path` | `null` | File path or URI for file-based sources |
| `base_url` | `null` | Base URL for API sources |
| `pool_min` / `pool_max` | `1` / `5` | Connection pool bounds |
| `cache_enabled` | `true` | Toggle caching for all tables in this source |
| `cache_ttl` | `null` | Seconds; `null` inherits the global default; `-1` never expires |
| `federation_hints` | `{}` | Per-connector extended parameters (dict[str,str]); see type reference below. |
| `mapping` | `{}` | Mapping DSL for redis, elasticsearch, prometheus. |
| `allowed_domains` | `[]` | Restrict this source to specific domain IDs; empty = unrestricted |
| `description` | `""` | |

### Supported source types

| Type | Connection style | Notes |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Asyncpg pool; PgBouncer opt-in via `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (DB file) | DuckDB firebird community extension |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Reuses PostgreSQL driver/dialect |
| `yugabytedb` | host/port | Reuses PostgreSQL driver/dialect |
| `greenplum` | host/port | Reuses PostgreSQL driver/dialect |
| `tidb` | host/port | Reuses MySQL driver/dialect |
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
| `hudi` | `path` (table URI) | ClickHouse Hudi engine, zero-copy |
| `hive` | host/port (metastore) + `mapping.storage` | Storage backend in `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + `mapping` S3 keys | Distinct type; always S3 storage |
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
| `airport` | `base_url` (Flight location) | DuckDB airport extension |
| `ingest` | push receiver | External services POST JSON events |
| **SaaS** | | |
| `sharepoint` | `base_url` or `host` + `mapping` | Auth via `mapping.auth_type` |
| `splunk` | `host`/`port` or `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Separate `GovDataSource` model; see §GovData below |
| **Data Quality** | | |
| `soda` | host/port aimed at Provisa's pgwire | Needs the `soda` extra; Elastic License 2.0, self-hosted only |
| `great_expectations` | host/port aimed at Provisa's pgwire | Needs the `gx` extra; Apache 2.0 |

### Source type reference

Each type that needs non-obvious config has a short entry below. RDBMS types (postgresql, mysql and so on) use only the common fields above and need no section of their own.

#### GovData

`govdata` sources use a top-level model of their own, `GovDataSource`, instead of the generic `Source`. Access is divided up by subject grouping.

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

Each subject maps to one or more GovData schemas. Configuring a `govdata` source with a subject exposes all schemas for that subject automatically.

| Subject | Schemas |
| --------- | --------- |
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

The `ref` and `geo` schemas are always included as linker schemas. They can't be configured and aren't listed above. Use the subject `ALL` to grant access to every schema.

#### Kafka

The `kafka` row in `sources:` only registers the source. Its connector's `details()` returns `{}`; the real configuration lives in the top-level `kafka_sources[]` block. Kafka is always a VIRTUAL_SOURCE: it routes through the engine and has no direct pool.

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

**Time Window** — `default_window` bounds every query to a recent time period, preventing unbounded reads from high-volume topics. Format: `1h`, `30m`, `7d`, `60s`. Defaults to `1h`. Auto-injected as `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Clients can override with their own `_timestamp` filter in the GraphQL `where` argument.

**Discriminator** — Multiple topic configs can point to the same physical Kafka topic with different `discriminator` values, producing separate GraphQL types. The discriminator is auto-injected as a WHERE clause.

**Schema Source**

| Value | Behavior |
| ------- | ---------- |
| `registry` | Fetch schema from Confluent Schema Registry |
| `manual` | Define columns inline in config (no Schema Registry needed) |
| `sample` | Auto-discover from sample messages |

#### Snowflake

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

#### Databricks

`http_path` in `federation_hints` is required. `password` carries the personal access token. `catalog` is optional and goes in SQL or the hints; the `database` field doesn't hold it.

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

#### BigQuery

`project` in `federation_hints` is required. Authentication uses `GOOGLE_APPLICATION_CREDENTIALS` (path to a service-account key file) or Application Default Credentials in the engine environment.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

Both use T-SQL over TDS with Azure AD authentication. Sign in with `az login` (developer) or a managed identity (production); the engine reads the credentials through `azure-identity`'s `DefaultAzureCredential`. Connection details come from environment variables, `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) or `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), or from `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`secure` in `federation_hints` enables TLS on the HTTP interface. Port defaults to `8123` (plain) or `8443` (when `secure: "true"`). `schema` in `federation_hints` overrides the remote schema.

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

#### Delta Lake / Iceberg

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

#### Hive / Hive S3

`host` and `port` point to the Hive Thrift metastore (default port 9083). For `hive`, set `mapping["storage"]` to choose the object store backend. A missing required key fails loudly, with no fallback.

`hive_s3` is a distinct type that always declares S3 storage; no `mapping.storage` needed.

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

#### Redis

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
          key_pattern: "sessions:*"
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

#### Elasticsearch

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

#### Prometheus

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

#### Google Sheets

`spreadsheet_id` in `federation_hints` is required. Auth uses a DuckDB `gsheet` SECRET provisioned at attach time.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### File sources (csv / parquet / sqlite / files)

`path` is required. `files` crawls a directory for CSV, Parquet, XLSX, and JSON files, surfacing each as a table. All file-based sources are VIRTUAL (route through the engine; no direct pool).

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

**Kaggle datasets** can't be added through this file, because they need a live token and the dataset picker in the Sources form (Sources → Subscriptions → Kaggle). A Kaggle source exported to YAML appears as `type: files` with `kaggle_owner` and `kaggle_ref` in `federation_hints`. Downloading from Kaggle again takes the `refreshKaggleSource` mutation or the refresh in the UI; editing the YAML won't do it. See [Kaggle datasets](sources.md#kaggle-datasets) in the source types reference.

#### API / Remote sources

**openapi** — set `base_url` to the OpenAPI base URL. Schema discovery reads the OpenAPI spec at startup.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — set `base_url`. Optional `mapping` keys: `headers` (dict of static headers), `forward_client_headers` (bool), `timeout_seconds` (int).

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

**airport** — `base_url` is the Arrow Flight server location. DuckDB airport extension.

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — use `host`, `port`, `path`, and `federation_hints`.

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

**sharepoint**

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

**splunk**

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

#### Data quality checkers (soda / great_expectations)

A checker source points at Provisa's own pgwire endpoint, so one postgres driver scans the federated view of a table backed by Snowflake or Iceberg. You declare the scan identity; it's never inherited. Policy applies to that connection, and a filtered set of rows must not let a check pass silently. Connection keys come from `mapping`: `host`, `port`, `database`, `user`, `password`.

```yaml
sources:
  - id: dq
    type: soda                 # or great_expectations
    domain_id: sales-analytics
    mapping:
      host: localhost
      port: 5439               # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner
      password: ${env:PROVISA_DQ_PASSWORD}
```

Each results table carries `dq_contract`, the Soda contract YAML or Great Expectations suite JSON word for word. Columns, watermark and promotions are derived from it; see [Data Quality Checkers](sources.md#data-quality-checkers) for how.

**Install-time selection.** The checker isn't linked in: the scan runs in a child interpreter, and the library is installed only when an operator names it. Every installer path (`install.sh`, `packaging/linux/first-launch.sh`, and the macOS wizard through `PROVISA_DQ_CHECKER`) writes the choice to `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` reads that key and exports `PROVISA_EXTRAS`, which `docker-compose.app.yml` passes as a build arg to the `Dockerfile`'s `ARG PROVISA_EXTRAS`:

| `dq_checker` | `PROVISA_EXTRAS` (Docker tier) | Native venv install |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Installing the demo dataset raises `none` to `gx` and says so, because the demo config registers a Great Expectations suite over `pet_store.pets` and its quality scorecard would otherwise have nothing to show. Naming `soda` keeps `soda`.

If you reach the demo through pip instead of an installer, you skip that wizard step, so the `demo` extra carries the same checker: `provisa run --demo` needs `pip install 'provisa[embedded,demo]'` for its scan to run. Without it the scan reports `data-quality checker 'great_expectations' is not installed` and names the install command.

Any other value stops the launcher, so it never starts without the checker the operator asked for. The `soda` extra pulls in `soda-postgres`, and `gx` pulls in `great-expectations[postgresql]`. Soda Core is under the Elastic License 2.0: `config/capabilities.yaml` marks the option `cloud_eligible: false`, and the hosted plane refuses it.

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

The naming authority is the single source of truth for client-facing names; physical backend column names are never exposed to clients. Each query language derives a column's name from its `column.alias` if set, otherwise from the physical column name via its configured convention.

The GraphQL convention is one of three preset enums. Old free-form strings (`none`, `snake_case`, `camelCase`, `PascalCase`) are deprecated.

| Preset | Default | Type names | Field names | Mutation names |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | yes | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

The default GraphQL convention is `apollo_graphql`, which produces camelCase field and mutation names. The SQL convention is separate, with default `snake_case`, applied via `apply_sql_name()`; the GraphQL convention is applied via `apply_gql_name()`, and the CQL name is derived from the GraphQL name.

`domain_prefix: bool` is an orthogonal option that applies regardless of the chosen preset.

Explicit `column.alias` is the canonical name: SQL uses it verbatim with no convention applied, GraphQL applies its convention to it, and CQL derives from the GraphQL name.

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

When `domain_prefix: true`, all GraphQL field and type names are prefixed with the domain ID using a double underscore separator:

| Table | Domain | Field Name |
| ------- | -------- | ----------- |
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
    columns:
      - name: id
        visible_to: [admin, analyst]
      - name: email
        visible_to: [admin, analyst]
        masks:                    # first entry the reader holds applies
          - to: [admin]           # admin sees raw
          - to: ["*"]             # everyone else sees it masked
            mask: {type: regex, pattern: "^(.{2}).*(@.*)$", replace: "$1***$2"}
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: constant, value: 0}
      - name: created_by
        visible_to: [admin, analyst]
      - name: created_at
        visible_to: [admin, analyst]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: truncate, precision: month}
    write_grants:                 # who may write this table, per role and operation
      - role: admin
        operation: insert
        columns: [email, amount]
        check: "TRUE"             # any row may be inserted
        presets:                  # set by the grant; the statement may not supply them
          - {column: created_by, source: session, name: user_id}
          - {column: created_at, source: now}
      - role: admin
        operation: update
        columns: [email, amount]
        rows: "TRUE"              # any row may be updated
        check: "TRUE"
```

A table holds no write grant by default, so no role can write it. A grant gives one role one operation (`insert`, `update` or `delete`) on the table. A table holds at most one grant per role and operation.

| Grant field | Meaning |
| ----------- | ------- |
| `columns` | The columns an `INSERT` may list or an `UPDATE` may set. A `delete` grant names none. |
| `rows` | The rows an `UPDATE` or `DELETE` may touch: a SQL predicate over the table's columns. An `insert` grant has none. |
| `check` | What every row an `INSERT` writes, or an `UPDATE` leaves behind, must satisfy: a SQL predicate. A `delete` grant has none. |
| `presets` | Columns the grant sets itself. The statement may not supply them, so a preset column is not also in `columns`. |

Leave `rows` or `check` out and the grant takes the role's read row filter on the table: a role writes the rows it reads unless the grant says otherwise. `"TRUE"` means every row. The older column `writable_by` and table `column_presets` keys are refused by name; the configuration fails to load until they are replaced by `write_grants`.

Every write on every surface is admitted in this order: the role holds the `write` right; the role holds a grant for the operation on the table; every column the statement writes is in the grant; the grant's presets are added to the statement; the grant's `rows` predicate is ANDed into the `WHERE` of an `UPDATE` or `DELETE`; and the `check` is evaluated on the new rows before anything is sent to the source. A statement the check cannot decide is refused. The grant's columns also decide which mutation fields GraphQL offers a role: one field per operation it holds a grant for, taking exactly those columns.

Some statements need a wider grant. `TRUNCATE` needs a `delete` grant over every row. `MERGE` needs an `insert` and an `update` grant, both over every row. `INSERT ... ON CONFLICT DO UPDATE` needs an `update` grant over every row with no presets. `COPY ... FROM STDIN` needs an `insert` grant with no presets.

Grants are inherited. A child role takes, per table and operation, the grant of the nearest role in its chain that holds one, its own first. A meta role holds the fold of its members' grants: the union of their columns, the OR of their `rows` and of their `check` predicates, and only the presets every member sets alike.

In the admin UI, a table's **Writes** section lists the grants and edits them together.

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

**Routing impact:** Path columns use PostgreSQL JSON operators (`->>`), which are natively supported by direct PG routing. For non-PostgreSQL sources (MySQL, SQL Server, etc.), queries with path columns are automatically routed through the federation engine. Mutations are unaffected since path columns are read-only computed fields.

### Masking Types

| Type | Fields | Description |
| ------ | -------- | ------------- |
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

Set `materialize: true` on a relationship to automatically generate a materialized view for cross-source JOINs. This avoids expensive federated queries by pre-computing the JOIN result.

- Only cross-source relationships generate MVs (same-source JOINs are already fast)
- On a junction-backed relationship the MV covers the two-hop traversal: source hop, junction hop, discriminator, and the junction's own columns as edge attributes. The junction counts as a leg, so an edge is cross-source when any of the three tables sits in a different source
- The MV starts stale and is populated by the background refresh loop
- Mutations to either source table mark the MV as stale for re-refresh
- `refresh_interval` defaults to 300 seconds (5 minutes)

## Roles

```yaml
roles:
  - id: admin
    description: Administers sources, tables, relationships and security.
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
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

Roles with `parent_role_id` inherit capabilities, domain access, column and object grants, and RLS rules from the parent, with the child's own RLS rule for a table taking precedence. The chain is flattened at startup.

### Capabilities

| Capability | Description |
| ----------- | ------------- |
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
    description: Analysts read only their own region's orders.
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
      # Add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Views (Governed Computed Datasets)

Views are SQL-defined computed datasets with full column-level governance. They are the governed mechanism for adding aggregations, transformations, and derived metrics to the model.

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
| ------- | ---------- | ------------- |
| `id` | Yes | Unique view identifier |
| `sql` | Yes | SQL SELECT statement defining the view |
| `domain_id` | Yes | Domain for schema visibility |
| `materialize` | No | `true` = periodic CTAS refresh, `false` = live federated view |
| `refresh_interval` | No | Seconds between refreshes (materialized only, default 300) |
| `description` | No | Appears in GraphQL SDL |
| `alias` | No | Override GraphQL name |
| `columns` | Yes | Column definitions with visibility, masking, descriptions |

### Materialized vs Live

- **`materialize: true`**: Provisa creates a table via CTAS and refreshes it on a schedule. Faster queries but data may be stale by up to `refresh_interval` seconds.
- **`materialize: false`**: Provisa creates a federated view. Queries always return live data but may be slower for complex aggregations.

Views go through the same governance pipeline as tables: RLS, masking, sampling and role-based visibility. That way nothing can add new semantics to the platform without a steward's oversight.

### Views with history (time travel)

A table or a materialized view can keep every version of its rows and be read as of a past time. You map three of the source's columns, and you declare all three or none:

```yaml
views:
  - id: customer-history
    sql: SELECT customer_id, tier, effective_date, updated_at FROM crm.customers
    domain_id: sales
    materialize: true
    business_key: [customer_id]
    valid_time: effective_date
    recorded_time: updated_at
    columns:
      - {name: customer_id, visible_to: [analyst]}
      - {name: tier, visible_to: [analyst]}
      - {name: effective_date, visible_to: [analyst]}
      - {name: updated_at, visible_to: [analyst]}
```

| Field | Description |
| ------- | ------------- |
| `business_key` | The columns that identify the same thing across versions, such as `transaction_id`. Every version of a record shares this key. |
| `valid_time` | The column holding the date your business gives the record: when it was agreed, booked or took effect. |
| `recorded_time` | The column holding when this version of the record was entered or changed, such as `updated_at`. |

Each one names a column the table or view has. A view that declares them sets `materialize: true`, and Provisa picks how it stores the versions. An entry under `tables:` takes the same three keys.

The two times have other names elsewhere: recorded time, called transaction time in the temporal-database literature and system time in SQL:2011; valid time, called application time in SQL:2011 or effective date.

The two can differ for the same version:

| Example | `valid_time` | `recorded_time` |
| ------- | ------------ | --------------- |
| A payment of 100 for 3 Oct is corrected to 120 on 5 Oct. | The correction keeps 3 Oct. | The correction gets 5 Oct. |
| A field measurement taken 3 Oct 14:10 is keyed in on 6 Oct. A typo in it is fixed on 7 Oct. | 3 Oct 14:10, kept by the fix. | 6 Oct. The fix gets 7 Oct. |
| A trade is booked at execution with trade date 3 Oct. On 4 Oct its quantity is found wrong and amended. | Trade date 3 Oct, kept by the amendment. | 3 Oct 10:02. The amendment gets 4 Oct. |

Read as recorded on 3 Oct, the trade shows the wrong quantity. Read as known now, it shows the corrected one.

A version is in force from its time to the next version's. A delete takes effect when Provisa records it.

**Reading.** A plain read answers the version of each key in force now. The `X-Provisa-As-Of` header reads as of a time, or over a range given as an ISO 8601 interval `start/end`:

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```

A point answers the version of each key in force then. A range answers every version in force at any time within it, one row per version.

**As recorded.** A correction recorded later changes what a view says about the past: the version in force on 30 September is the corrected one once the correction is recorded. To read the view as Provisa had recorded it at a time, before any later correction, add `X-Provisa-As-Recorded`, one ISO 8601 time:

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

Every version recorded after that time is left out, and the as-of then picks the version in force at its own time. With `X-Provisa-As-Recorded` alone, the read answers the version of each key in force as recorded then. It takes one time, not a range; a range or a malformed value is refused with HTTP 400 (`data.invalid_as_recorded`).

Both times also reach Provisa without the headers. The as-of time is the `as_of` argument of `resolve` (GraphQL, gRPC, MCP), the REST `as_of` query parameter and the `as_of` option of an Arrow Flight ticket, and a range given to `resolve` answers the row's `versions` in place of its `row`. The as-recorded time is the `as_recorded` argument of `resolve`, the REST `as_recorded` query parameter and the `as_recorded` option of an Arrow Flight ticket. The audit record of each statement keeps both times it read with, `as_of` and `as_recorded`. Rights, masks and row rules apply as to any read. A malformed as-of value, or a range whose end isn't after its start, is refused with HTTP 400 (`data.invalid_as_of`).

Every view with history answers three more columns, granted as its source-time column (else its first key column) is:

| Column | Meaning |
| ------- | ------- |
| `source_from` | When this version took effect: its source time, or its load time without one |
| `source_to` | When the next version took effect; empty while this one is in force |
| `recorded_at` | When Provisa recorded this version |

**Source time from change data capture.** A table landed by change data capture carries its commit time as `source_committed_at`, for a view with history to name as its source time:

- Debezium: the database commit time each change event carries (`source.ts_ms`). A row copied by a full build of the replica has no change event, so its `source_committed_at` is empty until the row next changes, unless the source is PostgreSQL with commit timestamps on (next item).
- PostgreSQL on the `native` change signal: the commit time PostgreSQL keeps for each row (`pg_xact_commit_timestamp(xmin)`), read by the source's own driver when the replica is built. PostgreSQL keeps it only with `track_commit_timestamp = on`. Enable it to get source time on the native signal; without it `source_committed_at` is empty. Turning the setting on or off rebuilds the replica.

Kafka topics without a Debezium envelope carry no commit time.

### Views on a calendar

A materialized view can take its schedule from a calendar: it is built at the close of each window the calendar gives it, a business day, week, month, quarter or year, and a view that keeps history stamps each version with that close. A config declares its calendars under `calendars:`:

```yaml
calendars:
  - name: bank
    version: v1
    tz: America/New_York
    day_close: "18:00"
    holidays: [2026-11-26, 2026-12-25]

views:
  - id: daily-totals
    sql: |
      SELECT store, business_date, SUM(amount) AS total
      FROM fin.payments
      GROUP BY store, business_date
    domain_id: finance
    materialize: true
    calendar: bank
    grain: daily
    business_day_grain: true
    columns:
      - {name: store, visible_to: [analyst]}
      - {name: business_date, visible_to: [analyst]}
      - {name: total, visible_to: [analyst]}
```

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `name` | Yes | The name a view gives as its `calendar` |
| `version` | Yes | The version label. One name can hold several versions, each with its own holidays. |
| `base_system` | No | `gregorian` (default), `fiscal` or `retail_445` |
| `tz` | No | The IANA time zone the calendar's days are in (default `UTC`) |
| `fiscal_anchor_month`, `fiscal_anchor_day` | No | The month and day a fiscal year starts (default 1 and 1) |
| `retail_anchor` | No | The first date of a reference 4-4-5 year; `retail_445` needs it |
| `week_start` | No | The day a week starts, 0 = Monday to 6 = Sunday (default 0) |
| `holidays` | No | Dates that are not business days |
| `weekend` | No | The weekdays that are not business days, 0 = Monday (default `[5, 6]`) |
| `day_close` | No | The local time a business day closes, `"HH:MM"`. Day D then runs from the close of the day before to D's close. Without it a day ends at midnight. Quote it: YAML reads an unquoted `18:00` as a number. |

A calendar is stored before the tables, so the views that name it find it. An entry the `createCalendar` mutation would refuse is refused with the same `invalid calendar: ...` message, and the config is not loaded. The model export writes every calendar under `calendars:`.

A view names its calendar with these keys. An entry under `tables:` takes them as `mv_calendar`, `mv_grain`, `mv_business_day_grain`, `mv_allowed_lateness` and `mv_expected_events`.

| Field | Description |
| ------- | ------------- |
| `calendar` | The calendar's `name` |
| `grain` | `daily`, `weekly`, `monthly`, `quarterly` or `annual`; an nth weekday such as `3WE` (third Wednesday) or `LFR` (last Friday); or an RFC 5545 rule such as `FREQ=WEEKLY;INTERVAL=2;BYDAY=MO` |
| `business_day_grain` | `true`: a daily window exists only on a business day, so no build runs on a weekend or holiday |
| `allowed_lateness` | Seconds past a window's close the build waits for input committed late (default 0) |
| `expected_events` | The inputs that must be fresh through the window's close. Omitted, every input the view reads; `[]`, none. |

A view naming a calendar that no config or `createCalendar` declared is refused by name when the event loop starts: `unknown calendar 'ledger'; registered: [...]`.

**Building one business day.** The `refreshMv` mutation's `window` argument, a date, builds the view sealed at that business day's close:

```graphql
mutation {
  refreshMv(mvId: "daily-totals", window: "2026-10-02") { success message code }
}
```

What the build appends is stamped with the close of the calendar's window for that date, as the build at the close itself would stamp it, so a read as recorded at that close answers it. A view with history built this way rebuilds the dates its input changed since its last build, so a correction recorded before the next day's build is picked up for its own date. A view that names no calendar is refused (`schema.mv_window_needs_calendar`), and so is a date its calendar has no window on, such as a holiday on a business-day grain.

### Query-only views

Both `materialize: true` and `materialize: false` views expose their GraphQL type as query-only. No insert, upsert, update, or delete mutations are generated for `view_sql`-backed relations.

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Cache Hierarchy

TTL resolution order (most specific wins): **table** > **source** > **global default**. First non-null value is used.

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

Setting `cache_enabled: false` on a source disables caching for all tables in that source, regardless of table-level TTL. Cache keys always include `role_id` + RLS context values for security partitioning.

### Per-request opt-in

The response cache is off for every request that does not ask for it. A request opts in with:

| Surface | Opt-in |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | `@cached` on the operation, optionally `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | a `-- @provisa cache=true` or `-- @provisa cache_ttl=60` comment line in the statement |
| Cypher (`/data/cypher`, the Neo4j Query API, Bolt, Arrow Flight) | a `// @provisa cache=true` or `// @provisa cache_ttl=60` comment line in the query |
| gRPC (and the HTTP gRPC proxy) | call metadata (headers on the proxy) `x-provisa-cache: true` or `x-provisa-cache-ttl: 60` |
| REST, JSON:API, and as an alternative to the hint in the text: GraphQL over HTTP, `/data/cypher`, the Neo4j Query API | the request header `Cache-Control: max-age=60` (or `max-stale=60`), or `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

A `ttl` that is not an integer fails the request. A request that states the opt-in twice (in its text and a header, or in two headers) with different values fails with `400`; neither is picked. `Cache-Control: no-cache` or `no-store` means no cache, the same as no header.

Without the hint the request neither reads nor writes the cache. With it, the settings above still decide: the result is cached only if every source it reads has `cache_enabled: true` and every table's resolved TTL is above 0. The entry lives for the request's `ttl` when it gives one, otherwise for the shortest resolved TTL of the tables it read.

The hint only accepts older data in exchange for speed. It never makes a read fresher than the operator's settings allow: landing and replica freshness (`cache_ttl`, `change_signal`), `row_materialize`, `load_protected` snapshots and freshness gates apply to a hinted request exactly as to an unhinted one. See [Freshness and load: who decides](#freshness-and-load-who-decides).

`@noCache` and `-- @provisa no_cache=true` no longer exist: with caching off by default there is nothing to bypass.

A write invalidates every cached entry the writing org holds for the tables it wrote; another org's entries for the same table are untouched. A failed invalidation fails the request instead of leaving stale entries behind.

## Freshness and load: who decides

Three parties have a say in how fresh a result is and how much load a query puts on the systems behind it.

- **The upstream source.** Ideally it manages its own backpressure: connection limits, statement timeouts, read replicas.
- **The operator.** Protects the platform, Provisa itself, from backpressure. When an upstream cannot protect itself, the operator protects it too. The settings for this live on sources and tables: `load_protected`, `replicate`, source `federation_hints`, the large-result redirect threshold, the Kafka cluster sinks write to, and each table's registered watermark.
- **The end user.** Trades speed against recency, one request at a time.

The operator's settings are the floor. A request can move above it, toward older data or less load; opting into the response cache is the usual example. It can never go below it. A request hint that would is refused with an error that names the operator setting, never applied quietly and never dropped quietly.

| Request input | What it may do | Refused when |
| --- | --- | --- |
| `@route(engine: DIRECT)`, `-- @provisa route=direct` | Pick the direct driver for one source | a table the statement reads is `load_protected` or `replicate: 0` |
| `@join`, `@reorder`, `@broadcastSize`, `/*+ ... */` | Set an engine session property the source's `federation_hints` left open | it changes a property the operator set |
| `X-Provisa-Redirect-Threshold`, `@redirect(threshold:)` | Redirect a result sooner | it is higher than the operator's threshold |
| `@sink(broker:)`, `X-Provisa-Sink` broker | Repeat the operator's broker | it names another broker, or no `KAFKA_BOOTSTRAP_SERVERS` is configured |
| `@watermark` on a subscription | Repeat the table's registered watermark | it names another column, or the table has none |
| `@cached`, `-- @provisa cache=true` | Serve an older cached result | never; a cached entry is never fresher than the read that stored it |

Refusals return HTTP 403 with code `query.operator_floor`, SQLSTATE `42501` over pgwire, and `PERMISSION_DENIED` over Flight and gRPC.

A table that is `load_protected` or `replicate: 0` is never read live by a query on any transport. Queries read its replica. Only the replication and its refreshes read the source: a refresh runs when a `cache_ttl` or freshness check calls for one, and with neither the replica is built once and then refreshed only through a change feed or the scheduler. On such a table with an `off_peak_window` (on the table or its source, read in `off_peak_tz`), a refresh runs only while the window is open: one that falls due or is asked for outside it waits for the window's next opening, and a window with no `cache_ttl` refreshes once per opening. The first build is never held, because reads of the table wait for it.

### The `replicate` setting

`replicate` is an integer on a table. A table with no value takes its source's, and when neither is set the global threshold applies. A replica is a copy of a source table kept in the engine's store and refreshed on the table's timeliness settings; it is not a continuous change stream.

| Value | Name | Meaning |
| --- | --- | --- |
| not set | Default | The global threshold applies |
| `-1` | Never | Read live wherever a live path exists. Replicated only on an engine that cannot read the source in place |
| `N` above 0 | Hot-N | Read live until the table passes N governed statements per interval, then served from its replica |
| `0` | Always | Reads come from the replica |

Only Always is a guarantee. Never and the Hot values are best effort. `load_protected` with `replicate: -1` is refused at config load and at save. A source with `replicate: 0` has no live attach on any engine, so every table of it is served from its replica.

The floor is judged by the tables a statement reads. A statement that reads no replica-served table keeps its direct route even when its source has other replicated tables.

A replica is indexed on its declared primary key and on the columns its declared relationships join on, built after it lands. Where those indexes speed up joins, where to keep replicas and how to choose their freshness are in [Replicas](replicas.md).

### Hot replication

A table whose `replicate` is not set, or is `N` above 0, is read live until it is busy.

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| Setting | Environment variable | Default |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

All three are operator settings: the admin UI shows them under Hot Tables, and a saved value applies at the next evaluation.

How a table is counted:

- A governed statement counts each table it reads once, however many times it reads it.
- A statement answered from the response cache does not count. It put no load on the source.
- A statement governance refused does not count.

A table is promoted when its count reaches its threshold (its own `N`, else its source's, else `hot_threshold`) and demoted when the count falls below half of it. Promotion requests a replica build; the table is read live until that build completes, then from the replica. Demotion returns reads to the source at once.

A table is not promoted when it holds more rows than `hot_max_rows`, when its change signal is `ttl` or `ttl_probe` and neither it nor its source declares a `cache_ttl`, or when the Redis hot tier already manages it. The table's summary in the admin UI states which.

The counts live in Redis. With a shared Redis (`cache.redis_url`) they cover every process of the deployment. With the embedded Redis they are per process, so promotion runs only when one worker serves requests; with several workers and no shared Redis nothing is promoted, and the summary says so.

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
| ---------- | ---------- | ----------------- |
| `simple` | Local dev/testing. Users defined in YAML. | JWT signed with `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (all methods). | `firebase-admin` SDK `verify_id_token()` |
| `keycloak` | Keycloak OIDC. Tenant + client roles mapped. | JWKS-based JWT validation |
| `oauth` | Generic OIDC (Okta, Azure AD, Auth0, PingFederate). | JWKS from discovery URL |
| `basic` | Self-contained deployments. Accounts live in Provisa's own store. | bcrypt password, or SCRAM-SHA-256 on pgwire |

Superuser credentials (`superuser` block) work with any provider and always resolve to admin role with all capabilities. Used for initial setup before external auth is configured.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Makes pgwire advertise SASL with `SCRAM-SHA-256`, so a password is proved and never sent in cleartext. It applies only to the `basic` provider, since no other provider holds the RFC 5802 verifiers SCRAM needs, and channel binding isn't offered.

Verifiers cannot be derived from existing bcrypt hashes. One is written whenever a password passes through in plaintext, so each user's first SCRAM connection follows their next signup, login, password change or admin reset. Until then that user's connections fall back to the cleartext exchange over TLS; the wire does not reveal who has migrated.

### Login throttling (`auth.login_throttle`)

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

On by default with the values shown; the block only tunes them. The counter sits at the credential-validation layer, so failures over HTTP, pgwire and Bolt accumulate against the same subject and a lockout holds on every surface. It is per process: several API workers each allow up to `max_attempts`.

### Personal access tokens

PATs need no configuration block. They're always accepted, and their store is created with the rest of the control-plane schema. What you can configure is the expiry a user may ask for when issuing one: 1 to 366 days, or none for a token that doesn't expire. See [Security Model](security.md#personal-access-tokens).

### Mutual TLS

Client-certificate verification is configured with environment variables, alongside the TLS certificate settings it extends, and has no place in `provisa.yaml`.

| Variable | Default | Meaning |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | unset | PEM bundle of the CA(s) permitted to sign client certificates. Setting it turns client-certificate verification on |
| `PROVISA_MTLS_MODE` | `required` once a CA is set | `required` or `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Require the certificate's common name to equal the username the connection authenticates as |

Each one takes a per-protocol override, named the same way as the TLS settings. A mode set without a CA, or a mode that is neither `required` nor `optional`, stops startup, so the server never serves connections the operator believes are verified when they aren't.

### Addressing an org over TLS

Nothing to configure. On a multi-org deployment, pgwire and Bolt read the org from the hostname the client dialed, carried in the TLS ClientHello, exactly as HTTP reads it from the `Host` header. A client connecting to `acme.provisa.dev` requests org `acme`; the request is refused unless the authenticated principal is a member. Connecting by IP address requests no org, which is every connection on a single-org deployment.

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

For tables with a primary key, Provisa generates `upsert_<table>` mutation fields. These compile to an upsert in the target dialect: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` on PostgreSQL, `ON DUPLICATE KEY UPDATE` on MySQL.

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Conflict columns are derived from PK metadata. All column visibility and write permission rules apply.

## Distinct On

The `distinct_on` argument selects the first row for each distinct value of the specified columns. Available on root query fields.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Compiles to `SELECT DISTINCT ON (region) ...` in PostgreSQL. For non-PG dialects, a window-function fallback is used.

## Write Grant Presets

A write grant sets columns itself on insert and update. Presets are declared in the `presets` list of the grant, per role and operation.

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    columns:
      - {name: amount, visible_to: [analyst]}
      - {name: created_by, visible_to: [analyst]}
      - {name: created_at, visible_to: [analyst]}
      - {name: source_system, visible_to: [analyst]}
    write_grants:
      - role: analyst
        operation: insert
        columns: [amount]
        presets:
          - {column: created_by, source: session, name: user_id}   # from the request's session
          - {column: created_at, source: now}                      # time of the write
          - {column: source_system, source: literal, value: provisa}
```

| Source | Behavior |
| -------- | ---------- |
| `session` | Injects the value of the named session variable (`name`) of the request |
| `now` | Injects the time of the write |
| `literal` | Injects a constant (`value`) |

Presets are added to the statement during compilation, before SQL generation. The statement may not supply a preset column, and a preset column is not in the mutation input type.

## Inherited Roles

Roles can inherit from one parent role via `parent_role_id`. The chain is flattened at startup. A child holds the union of its ancestors' capabilities and domain access; a column, metric, function or webhook granted to an ancestor is granted to the child; and an ancestor's RLS rules apply to the child per table, nearest role first, with the child's own rule for a table replacing the parent's.

```yaml
roles:
  - id: admin
    description: Administers everything.
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    description: An intern, with the junior analyst's access.
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

Multi-level inheritance is supported. The child role's explicit capabilities and domain_access are merged with the parent's. A parent must be an existing role, may not be the role itself and may not close a cycle; each is refused at save.

## Scheduled Triggers

Cron-based triggers that call a webhook URL on schedule. Uses APScheduler.

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

Scheduled tasks are managed via the admin UI (enable/disable toggle) or the `toggle_scheduled_task` admin mutation.

A trigger can fire on a registered table instead of a schedule: `on: {table, event}` names the table as `source_id.schema.table` and the event, `change`, `error` or `quarantine`. A `command` trigger runs a registered command as its `role`, through the command's own governed dispatch, and gives it what fired as arguments. A change gives `table` and, where the change signal carries it (Kafka, Debezium), the landed batch as `changes`. An `error` or `quarantine` event gives `table`, `event`, `reason` and `error`. A trigger whose command declares an argument the trigger does not give is refused at save. A change fires once per burst its change signal reports, or once a write made through Provisa lands; a table served from its replica fires when a build lands rows that differ from the last. A command's own write is a change, so triggers chain. Each run is recorded with its time, its outcome (`succeeded`, `refused` or `failed`), its error and what fired it, and the `scheduledTasks` admin query lists it. Only prod's triggers fire, and nothing is installed in a source database.

```yaml
scheduled_triggers:
  - id: bronze_to_silver
    on: {table: bronze_pg.public.bronze, event: change}
    command: promote_to_silver
    role: loader
  - id: alert_on_quarantine
    on: {table: bronze_pg.public.bronze, event: quarantine}
    command: notify_steward      # declares arguments table, reason
    role: loader
```

## OrderBy Format

OrderBy uses the `{column: direction}` format with a 6-value direction enum:

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
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

Relationship ordering is supported via nested objects:

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

### Telemetry Filters

Provisa runs two independent OTLP export paths: your internal collector and the optional Provisa support endpoint. Each path has its own filter. The filters run before spans leave the process, and the original span objects are never changed.

**`telemetry_filter`** — controls what reaches your internal collector.

| Key | Type | Default | Description |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Replaces string and numeric literals in `db.statement` with `?` |
| `redact_attributes` | list[str] | `[]` | Attribute keys dropped entirely from every span |

**`support_telemetry_filter`** — controls what reaches the Provisa support endpoint. SQL literal redaction defaults to `true` on this path, since query data belongs to you.

| Key | Type | Default | Description |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Replaces string and numeric literals in `db.statement` with `?` |
| `redact_attributes` | list[str] | `[]` | Attribute keys dropped entirely from every span |

A redacted `db.statement`. With `redact_sql_literals: true`, this span attribute:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

becomes:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Support Endpoint

`support_endpoint` (or the environment variable `PROVISA_SUPPORT_OTLP_ENDPOINT`) forwards telemetry to Provisa support for diagnostics. When it's unset, no data leaves your infrastructure this way. The support filter works independently of the internal one, so you can redact SQL literals from both exports and still share span timing and error data with support.

### Endpoint Protocol Detection

Provisa selects OTLP/HTTP or OTLP/gRPC from the endpoint URL scheme. URLs starting with `http://` or `https://` use OTLP/HTTP, with `/v1/traces`, `/v1/metrics`, and `/v1/logs` appended automatically. Any other scheme uses OTLP/gRPC with `insecure=True`.

## Federation Engine

Configuring a federation engine is optional. The default is `duckdb`: zero-config, in-process, with no external service. Choose another engine when you need MPP scale or want to reuse a warehouse you already have.

Precedence: `PROVISA_ENGINE` env var → persisted admin-UI `federation_engine` config field → `duckdb`. Changes take effect on service restart.

### Engine overview

| Engine key | Label | Dialect | MPP | External-link mechanism | Auth |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | Yes | Trino catalogs (broad connector set) | JDBC credentials |
| `trino-byo` | Trino | Trino SQL | Yes | Same as `trino`; unmanaged coordinator | JDBC credentials |
| `pg` | PostgreSQL | PostgreSQL | No | FDW / pg_duckdb | PostgreSQL credentials |
| `duckdb` | DuckDB | DuckDB | No | Extension-native ATTACH | None (in-process) |
| `clickhouse` | ClickHouse (embedded) | ClickHouse | Yes | S3 / IcebergS3 / DeltaLake table engines | chdb (in-process, no auth) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Yes | S3 / IcebergS3 / DeltaLake table engines | ClickHouse credentials |
| `snowflake` | Snowflake | Snowflake | Yes | External stage + external table | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Yes | Unity Catalog external tables via REST | `PROVISA_ENGINE_URL` (bearer token + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Yes | BigQuery external / BigLake tables | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Yes | OneLake shortcuts → OPENROWSET | Azure AD (`az login` or managed identity) |
| `synapse` | Azure Synapse | T-SQL | Yes | ADLS OPENROWSET / external tables | Azure AD |
| `mysql` | MySQL | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `mariadb` | MariaDB | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `oracle` | Oracle Database | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `mssql` | Microsoft SQL Server | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `db2` | IBM Db2 | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `redshift` | Amazon Redshift | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `greenplum` | Greenplum | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `cockroachdb` | CockroachDB | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `yugabytedb` | YugabyteDB | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `opengauss` | openGauss | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `tidb` | TiDB | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `singlestore` | SingleStore | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `vertica` | Vertica | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `exasol` | Exasol | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `teradata` | Teradata Vantage | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `saphana` | SAP HANA | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `sapase` | SAP ASE (Sybase) | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `sqlanywhere` | SAP SQL Anywhere | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `monetdb` | MonetDB | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `firebird` | Firebird | Per-dialect | No | None (land-only) | Per-dialect credentials |
| `sqlalchemy` | Other relational database (by connection URL) | Per-dialect | No | None (land-only) | Per-dialect credentials |

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

In-process; no external service. The default engine. `PROVISA_DATA_DIR` controls where the embedded store lives (`~/.provisa` by default).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

The materialization store defaults to `~/.provisa/materialize.duckdb`. It's the only engine whose default store isn't PostgreSQL.

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

#### Relational-database engines (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) and `sqlalchemy`

One key per network-addressable relational database, all on the same land-only runtime (no federation to external sources): every source lands into the store and is queried there. The key selects the database; `PROVISA_ENGINE_URL` carries the DSN its dialect takes. `sqlalchemy` is the catch-all for a database with no key of its own. File-embedded stores (SQLite, Access) are not offered, because the server has to be reachable over the network.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

Materialization store defaults to `TENANT_DATABASE_URL`.

### Materialization store

When a source cannot attach live (no ATTACH connector for the selected engine), it lands into the engine's materialization store. Resolution order: explicit `PROVISA_MATERIALIZE_URL` → engine's declared default → hard error (no silent fallback).

DuckDB declares its embedded file (`~/.provisa/materialize.duckdb`) as its default. All other engines default to `TENANT_DATABASE_URL` (PostgreSQL). Override any engine with `PROVISA_MATERIALIZE_URL`.

The store decides what a replica's indexes do for joins: a PostgreSQL store indexes replicas and an engine that runs SQL in it reads those indexes, while DuckDB joins its embedded file's replicas over a per-statement copy. See [Replicas](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it).

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

For Google Cloud sources, set `GOOGLE_APPLICATION_CREDENTIALS` to the path of your service-account key file. For Fabric and Synapse, sign in with `az login` (developer) or a managed identity (production); the engine reads the credentials through `azure-identity`'s `DefaultAzureCredential`.

## Environment Variables

| Variable | Default | Description |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Config file path |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | Control-plane store URI (SQLAlchemy); accepts `sqlite+pysqlite://…` / `duckdb://…` for the embedded desktop store |
| `PLATFORM_DATABASE_URL` | — | Platform registry URI (tenant directory, engine registry); required at startup, no fallback |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` uses embedded fakeredis instead of a Redis server — no Docker |
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `provisa` | PostgreSQL database |
| `PG_USER` | `provisa` | PostgreSQL user |
| `PG_PASSWORD` | `provisa` | PostgreSQL password |
| `PROVISA_ENGINE` | `duckdb` | Federation engine key |
| `PROVISA_ENGINE_URL` | — | Connection URL for URL-driven engines (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Override materialization store DSN (defaults to engine's declared default) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Data directory for the embedded DuckDB store |
| `TRINO_HOST` | `localhost` | Trino coordinator host |
| `TRINO_PORT` | `8080` | Trino coordinator HTTP port |
| `PROVISA_ENGINE_FILES_GID` | `1000` | Group Trino reads its secret files as (a Kafka source's SASL settings, a code host's client key) when Provisa runs as neither root nor Trino's user, as a native app beside Trino's container does. Provisa's user must be a member of it, or the file is refused by name. |
| `PROVISA_PG_ENGINE_FILES_GID` | `999` | The same, for a Postgres engine's server reading a code host's client key |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to GCP service-account key JSON (BigQuery engine/source) |
| `GOOGLE_CLOUD_PROJECT` | — | Default GCP project (BigQuery; overridden by URL) |
| `FABRIC_SQL_SERVER` | — | Fabric Warehouse SQL endpoint (alternative to `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Fabric Warehouse database name |
| `SYNAPSE_SQL_SERVER` | — | Synapse serverless SQL endpoint |
| `SYNAPSE_DATABASE` | — | Synapse database name |
| `REDIS_URL` | — | Redis connection URL |
| `PROVISA_SAMPLE_SIZE` | `10000` | Default sampling limit |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Row cap when a query supplies no explicit `LIMIT` |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Tier-1 read-retry budget in seconds; exponential backoff with full jitter |
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
| `PROVISA_MTLS_CLIENT_CA` | — | PEM bundle of the CA(s) permitted to sign client certificates; setting it turns on client-certificate verification on pgwire, Bolt, gRPC and Flight |
| `PROVISA_MTLS_MODE` | `required` once a CA is set | `required` or `optional`; any other value refuses to start |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Require the certificate's common name to equal the authenticating username |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Comma-separated sites permitted to open a Bolt WebSocket from a browser; unset refuses every browser origin |
| `PROVISA_EXTRAS` | `firebase,vector` | Pyproject extras baked into the app image; `scripts/provisa` derives it from `dq_checker` in `~/.provisa/config.yaml` |
| `PROVISA_DQ_CHECKER` | `none` | Installer-only: `none`/`soda`/`gx`, read by `first-launch.sh` in non-interactive mode and written to `config.yaml` as `dq_checker` |
| `ANTHROPIC_API_KEY` | — | Claude API key (discovery) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Overrides `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Overrides `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Overrides `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Overrides `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Batch span processor flush delay |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Overrides `observability.support_endpoint` |
