# Source Types

## Execution Model

Every query ultimately executes through the federation engine, which provides federation across all sources. Sources fall into three categories based on their connectivity. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Category | Has Direct Driver | Has Federated Connector | Examples |
| --- | --- | --- | --- |
| **Direct-capable** | Yes | Yes | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Federation only** | No | Yes | MongoDB, Cassandra, Snowflake, BigQuery, Databricks, Redshift, ClickHouse, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-backed) |
| **Materialize → Federation** | No | No | REST/OpenAPI, remote GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (push receiver), GovData |

**Direct-capable** sources execute single-source queries via their native driver (sub-100ms), bypassing the federation engine (REQ-027, REQ-229). They retain full connector support and participate in federation when joined with other sources (REQ-028).

**Federation only** sources are always queried through the federation layer. No direct driver exists (REQ-229).

**Materialize** sources have no federated connector. Provisa fetches their data (on startup or at query time) and caches it as Parquet in S3 or in PostgreSQL, making it reachable by the federation engine for cross-source queries (REQ-309).

---

## All Sources

Reference for every source type Provisa supports. "Direct driver" means single-source queries execute against the source natively (sub-100ms) (REQ-027). "Connector Name" is the federated connector used when the source participates in multi-source JOINs (REQ-028). [tool-verified: `provisa/core/models.py` `SOURCE_TO_CONNECTOR` and `SOURCE_TO_DIALECT`]

### RDBMS

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Yes |
| `mysql` | aiomysql | mysql | mysql | Yes |
| `mariadb` | aiomysql | mariadb | mysql | Yes |
| `singlestore` | — | singlestore | singlestore | Federated |
| `sqlserver` | aioodbc | sqlserver | tsql | Yes |
| `oracle` | oracledb | oracle | oracle | Yes |
| `duckdb` | duckdb | memory | duckdb | Yes |

### Cloud Data Warehouses

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `snowflake` | — | snowflake | snowflake | Federated |
| `bigquery` | — | bigquery | bigquery | Federated |
| `databricks` | — | delta_lake | databricks | Federated |
| `redshift` | — | redshift | redshift | Federated |

### Analytics / OLAP

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `clickhouse` | — | clickhouse | clickhouse | Federated |
| `druid` | — | druid | druid | No |
| `exasol` | — | exasol | exasol | No |
| `elasticsearch` | — | [inferred: no connector entry in models.py] | — | No |
| `pinot` | — | [inferred: no connector entry in models.py] | — | No |

### Data Lake / Open Table Formats

These source types are federation-only — no direct driver, no SQLGlot dialect. [tool-verified: `TRINO_ONLY_SOURCES` in `provisa/core/models.py` line 129] (REQ-229)

| Source Type | Connector Name | Time Travel | Notes |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Yes (`as_of` argument, REQ-372) | — |
| `delta_lake` | delta_lake | Yes (`as_of` argument, REQ-372) | — |
| `hive` | hive | No | — |
| `hive_s3` | hive | No | S3-backed Hive |

### NoSQL

`mongodb` and `cassandra` have federated connector entries. `redis`, `kudu`, and `accumulo` are registered source types but have no connector entry in `SOURCE_TO_CONNECTOR` — they materialize through the API cache pipeline. [tool-verified: `provisa/core/models.py` lines 84–107] (REQ-017)

| Source Type | Connector Name | Mutations |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | No |
| `cassandra` | cassandra | No |
| `redis` | — (materialized) | No |
| `kudu` | — (materialized) | No |
| `accumulo` | — (materialized) | No |

### Streaming

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | Federated Kafka connector; schema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), manual definition, or sample inference (REQ-147, REQ-150) | Sink only (REQ-176) |
| `websocket` | External WebSocket feed — connect, subscribe, receive events; results materialized (REQ-338) | No |
| `rss` | RSS 2.0 / Atom feed — poll, watermark by pubDate/updated; results materialized (REQ-342, REQ-343) | No |

### Push Receiver

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `ingest` | External services POST JSON events; results materialized (REQ-331, REQ-335) | No |

### Graph & Semantic

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via HTTP API, results cached in PostgreSQL (REQ-295) | No |
| `sparql` | SPARQL 1.1 POST, results cached in PostgreSQL (REQ-297) | No |

### File-Based

Two mechanisms cover files. Both use the `path` field instead of `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Single-file sources** — `sqlite`, `csv`, `parquet` point `path` at one file.

| Source Type | Transports | Mutations |
| --- | --- | --- |
| `sqlite` | local | Yes |
| `csv` | local | No |
| `parquet` | local, `s3://` | No |

Private buckets need credentials (AWS region and keys from the environment). For CSV over `s3://` or `http(s)://`, or to register many files at once, use the `files` source. [tool-verified: `provisa/file_source/source.py`]

**`files` source** — points `path` at a glob, crawls it recursively, and registers the directory as a federated catalog of tables. It reads many formats over many transports; the sets below come from the file connector (kenstott/calcite fork). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formats | Transports |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, and documents converted to tables — HTML, Markdown, DOCX, PPTX | Local filesystem, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST and Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Observability & Other

`google_sheets` and `prometheus` are registered source types but have no connector entry in `SOURCE_TO_CONNECTOR`. [tool-verified: `provisa/core/models.py` lines 61–62]

| Source Type | Connector Name | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialized) | No |
| `prometheus` | — (materialized) | No |

### API Sources

Register any HTTP endpoint as a queryable table. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| API Type | Discovery | Column Inference |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI spec parsing (REQ-314, REQ-316) | Primitives → native, objects → JSONB |
| `graphql_remote` | Schema introspection (REQ-307, REQ-308) | Primitives → native, objects → JSONB |
| `grpc_remote` | Server reflection (REQ-322, REQ-325) | Primitives → native, objects → JSONB |

API responses are fetched, cached in PostgreSQL (configurable TTL), and exposed as GraphQL types (REQ-309, REQ-318, REQ-327). Cached tables participate in federated queries like any other source (REQ-313).

**JSONB rules**: Complex columns (objects, arrays) stored as JSONB are not filterable (REQ-119). Sub-field access uses `->>` extraction in SQL (REQ-151). Relationships are declared between tables using scalar FK columns — JSONB blob columns are not join targets. Use JSONB promotion to convert nested fields into native scalar columns when filtering or joining on them is needed (REQ-119).

### GovData

U.S. government open data. Access is partitioned by subject grouping. [tool-verified: `provisa/core/models.py` lines 543–609]

Each `govdata` source selects one subject. That subject determines which GovData schemas are exposed. The `ref` and `geo` schemas are always included as linker schemas — they are not listed per subject but are always present. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Subject | Schemas Exposed |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`, `fec` |
| `ALL` | Every schema above |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
    governance: pre-approved
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Yes | — | Unique identifier |
| `subject` | Yes | — | One of the subject values above |
| `domain_id` | Yes | — | Domain this source belongs to |
| `description` | No | `""` | Human-readable description |
| `governance` | No | `pre-approved` | Governance level for all tables in this source |

---

## Source Configuration Fields

All sources share a common set of fields. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Yes | — | Unique identifier; alphanumeric with hyphens/underscores |
| `type` | Yes | — | Source type (see tables above) |
| `host` | No | `""` | Hostname or IP |
| `port` | No | `0` | Port number |
| `database` | No | `""` | Database name |
| `username` | No | `""` | Username |
| `password` | No | `""` | Password; use `${env:VAR}` for secret resolution |
| `path` | No | `null` | File path or URL for file-based sources (`csv`, `parquet`, `sqlite`) |
| `base_url` | No | `null` | Base URL for OpenAPI sources |
| `pool_min` | No | `1` | Minimum connection pool size (REQ-052) |
| `pool_max` | No | `5` | Maximum connection pool size (REQ-052) |
| `use_pgbouncer` | No | `false` | Route connections through PgBouncer (REQ-053) |
| `pgbouncer_port` | No | `6432` | PgBouncer port (REQ-053) |
| `cache_enabled` | No | `true` | Enable API response caching |
| `cache_ttl` | No | `null` | Cache TTL in seconds; inherits global default when null |
| `cache_catalog` | No | `null` | Federated catalog for API cache; defaults to source's own catalog |
| `cache_schema` | No | `api_cache` | Schema within the cache catalog |
| `naming_convention` | No | `null` | Override global naming convention for this source (REQ-194) |
| `federation_hints` | No | `{}` | Session properties passed to the federation engine (REQ-278, REQ-281) |
| `allowed_domains` | No | `[]` | Restrict source to specific domains; empty = unrestricted |
| `description` | No | `""` | Human-readable description |

---

## Kafka Sources

Kafka topics are configured separately under `kafka_sources`, keyed by the source `id` of a registered `kafka` source. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

```yaml
kafka_sources:

  - id: kafka-support
    topics:

      - id: tickets
        topic: support.tickets
        domain_id: sales-analytics
        description: "Inbound support tickets"
        default_window: 1h
        columns:

          - name: id
          - name: subject
          - name: status
          - name: created_at
```

| Field | Description |
| ------- | ------------- |
| `id` | Must match the `id` of a source with `type: kafka` |
| `topics[].id` | Logical name for this topic within Provisa |
| `topics[].topic` | Kafka topic name |
| `topics[].domain_id` | Domain this topic belongs to |
| `topics[].description` | Human-readable description |
| `topics[].default_window` | Default time window for windowed queries (e.g. `1h`) (REQ-148) |
| `topics[].columns` | Column definitions for the topic schema (REQ-150) |

---

## Governance

Every registered table has a `governance` field. [tool-verified: `provisa/core/models.py` `GovernanceLevel` enum, lines 73–76]

| Value | Behaviour |
| ------- | ----------- |
| `pre-approved` | Queries run against this table with user rights alone; no registry approval required (REQ-003) |

---

## Column Visibility

The `visible_to` field on each column is a list of role IDs that can see that column. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Columns omitted from a role's `visible_to` list do not appear in that role's GraphQL schema and cannot be queried or referenced in filters (REQ-039).

---

## Relationships

Relationships connect two registered tables and appear as nested fields in GraphQL. [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `id` | Yes | Unique identifier for this relationship |
| `source_table_id` | Yes | Table that holds the foreign key |
| `target_table_id` | Yes | Table being referenced; empty for computed relationships |
| `source_column` | Yes | Column on the source table |
| `target_column` | Yes | Column on the target table; empty for computed relationships |
| `cardinality` | Yes | `many-to-one` or `one-to-many` (REQ-019) |
| `materialize` | No | Auto-create a materialized view for cross-source joins (REQ-158) |
| `refresh_interval` | No | MV refresh interval in seconds (default: 300) |
| `target_function_name` | No | DB function name for computed relationships |
| `function_arg` | No | Which function argument receives the source column value |
| `alias` | No | Human-readable relationship type (e.g. `WORKS_FOR`) |
| `graphql_alias` | No | Names the SDL field this relationship exposes on the parent type. When absent, the name is derived from the target table's `field_name` and relationship cardinality. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | No | When `true`, exclude this relationship from Cypher graph edges |
| `source_json_key` | No | Extract this key from source column as a JSON object before JOIN |

Cardinality values [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — each source row maps to one target row (FK to PK)
- `one-to-many` — each source row maps to multiple target rows (inverse of above)

---

## Row-Level Security Rules

RLS rules inject `WHERE` clauses at query time, scoped to a role and optionally to a table or domain. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

When both a domain-level and a table-level rule exist for the same role, the table-level rule takes precedence (REQ-403).

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `table_id` | Conditional | Table to apply the rule to; mutually exclusive with `domain_id` |
| `domain_id` | Conditional | Domain to apply the rule to; applies to all tables in the domain (REQ-402) |
| `role_id` | Yes | Role this rule applies to |
| `filter` | Yes | SQL predicate injected into `WHERE`; may reference session variables (REQ-041) |

---

## Functions and Webhooks

### DB Functions

Track a database function and expose it as a GraphQL query or mutation. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

```yaml
functions:

  - name: get_customers_by_region
    source_id: sales-pg
    schema: public
    function_name: get_customers_by_region
    returns: customers
    domain_id: sales-analytics
    description: "Returns customers filtered by region"
    visible_to: [admin, analyst]
    kind: query
    arguments:

      - name: p_region
        type: String
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `name` | Yes | — | GraphQL field name |
| `source_id` | Yes | — | Source containing the function |
| `schema` | No | `public` | Database schema |
| `function_name` | Yes | — | Actual database function name |
| `returns` | Yes | — | Registered table ID the function returns (REQ-207) |
| `arguments` | No | `[]` | List of `{name, type}` argument definitions (REQ-211) |
| `visible_to` | No | `[]` | Roles that can call this function |
| `writable_by` | No | `[]` | Roles that can call this as a mutation |
| `domain_id` | No | `""` | Domain this function belongs to |
| `description` | No | `null` | GraphQL field description |
| `kind` | No | `mutation` | `"query"` or `"mutation"` (REQ-205) |

### Webhooks

Expose an external HTTP endpoint as a GraphQL query or mutation. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

```yaml
webhooks:

  - name: notify_support
    url: http://localhost:9999/notify
    method: POST
    timeout_ms: 3000
    domain_id: sales-analytics
    description: "Send a support notification"
    visible_to: [admin]
    kind: mutation
    arguments:

      - name: message
        type: String
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `name` | Yes | — | GraphQL field name |
| `url` | Yes | — | Webhook endpoint URL |
| `method` | No | `POST` | HTTP method |
| `timeout_ms` | No | `5000` | Request timeout in milliseconds |
| `returns` | No | `null` | Registered table ID, or null for inline type |
| `inline_return_type` | No | `[]` | List of `{name, type}` fields for custom return shapes (REQ-210) |
| `arguments` | No | `[]` | List of `{name, type}` argument definitions |
| `visible_to` | No | `[]` | Roles that can call this webhook |
| `domain_id` | No | `""` | Domain this webhook belongs to |
| `description` | No | `null` | GraphQL field description |
| `kind` | No | `mutation` | `"query"` or `"mutation"` |

---

## Authentication

Auth is configured under the `auth` key. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Provider | Description |
| ---------- | ------------- |
| `none` | No authentication; all requests treated as the `default_role` |
| `firebase` | Firebase Authentication; requires `project_id` and `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | Generic OAuth 2.0 (REQ-123) |
| `simple` | Username/password without an external provider (REQ-124) |

```yaml
auth:
  provider: firebase
  assignments_source: provisa   # "claims" or "provisa"
  default_role: analyst
  default_assignments:

    - role_id: analyst
      domain_id: "*"
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT_KEY}
```

`assignments_source: claims` reads role assignments from JWT claims. `assignments_source: provisa` reads them from Provisa's own assignment store. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Execution Routing

**Direct execution** — Single-source RDBMS queries route to the native driver for sub-100ms latency (REQ-027). Sources require both a `SOURCE_TO_DIALECT` entry and a `SOURCE_TO_CONNECTOR` entry to support this path (REQ-229).

**Federated execution** — Multi-source queries and sources without a direct driver route through the federation engine (REQ-028). Provisa includes an embedded federation engine; point to your own compatible cluster for large-scale deployments (REQ-226).

**Statistics** — On registration, Provisa runs `ANALYZE` against each published table to prime the cost-based optimizer (row counts, null fraction, distinct values, min/max). Failures are logged and do not block registration (REQ-275).

---

## Graph & Semantic Sources

### Neo4j

Register a Neo4j graph database as a queryable source. Stewards author Cypher queries that project scalar values; Provisa caches results and exposes them as GraphQL types (REQ-295).

Cypher queries must use property accessors in the `RETURN` clause (`RETURN n.id AS id, n.name AS name`) — returning node objects is rejected at registration time (REQ-296).

```bash
# Register via admin API (no YAML config required)
POST /admin/sources/neo4j
{
  "source_id": "graph",
  "host": "neo4j",
  "port": 7474,
  "database": "neo4j"
}

# Register a table (preview + validate before persisting)
POST /admin/sources/neo4j/graph/tables
{
  "table_name": "person_skills",
  "cypher": "MATCH (p:Person)-[:HAS_SKILL]->(s:Skill) RETURN p.name AS name, s.skill AS skill, p.experience AS years",
  "ttl": 300
}
```

The preview endpoint (`POST /admin/sources/neo4j/{id}/preview`) returns sample rows and blocks registration if the Cypher returns node objects (REQ-296).

### SPARQL

Register any SPARQL 1.1 compliant triplestore (Apache Jena Fuseki, Virtuoso, Stardog, etc.) as a queryable source (REQ-297).

Queries must be `SELECT` queries. Variable names in the `SELECT` clause become column names automatically (REQ-297).

```bash
# Register via admin API
POST /admin/sources/sparql
{
  "source_id": "knowledge-graph",
  "endpoint_url": "http://fuseki:3030/ds/sparql",
  "default_graph_uri": "http://example.org/graph"
}

# Register a table (executes LIMIT 5 probe to validate and infer columns)
POST /admin/sources/sparql/knowledge-graph/tables
{
  "table_name": "product_categories",
  "sparql_query": "SELECT ?product ?label ?category WHERE { ?product a :Product ; rdfs:label ?label ; :hasCategory ?category . }",
  "ttl": 600
}
```

Both connectors use the API source cache pipeline — results are stored in PostgreSQL with configurable TTL, making them available for cross-source federated JOINs (REQ-295, REQ-297, REQ-299).

---

## Connection Examples

### PostgreSQL

```yaml
- id: sales-pg
  type: postgresql
  host: postgres
  port: 5432
  database: provisa
  username: provisa
  password: ${env:PG_PASSWORD}
```

### Snowflake

```yaml
- id: analytics-sf
  type: snowflake
  host: org.snowflakecomputing.com
  port: 443
  database: ANALYTICS
  username: svc_provisa
  password: ${env:SNOWFLAKE_PASSWORD}
```

### MongoDB

```yaml
- id: reviews-mongo
  type: mongodb
  host: mongodb
  port: 27017
  database: provisa
  username: ""
  password: ""
```

### Cross-Source Query

```graphql
{
  orders(where: {region: {eq: "us"}}) {
    id
    amount
    customers {       # PostgreSQL
      name
      email
    }
    productReviews {  # MongoDB (federated)
      rating
      comment
    }
  }
}
```

Single-source portions route directly (REQ-027). Cross-source JOINs federate with automatic type coercion (REQ-028, REQ-552).
