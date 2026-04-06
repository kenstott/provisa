# Source Types

## All Sources

Comprehensive reference for every source type Provisa supports. "Direct driver" means single-source queries execute against the source natively (sub-100ms). "Connector Name" is the federated connector used when the source participates in multi-source JOINs. Both can apply to the same source.

### RDBMS

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `postgresql` | asyncpg | postgresql | postgres | Yes |
| `mysql` | aiomysql | mysql | mysql | Yes |
| `mariadb` | aiomysql | mariadb | mysql | Yes |
| `singlestore` | — | singlestore | singlestore | Federated |
| `sqlserver` | aioodbc | sqlserver | tsql | Yes |
| `oracle` | oracledb | oracle | oracle | Yes |
| `duckdb` | duckdb | memory | duckdb | Yes |

### Cloud Data Warehouses

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `snowflake` | — | snowflake | snowflake | Federated |
| `bigquery` | — | bigquery | bigquery | Federated |
| `databricks` | — | delta_lake | databricks | Federated |
| `redshift` | — | redshift | redshift | Federated |

### Analytics / OLAP

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `clickhouse` | — | clickhouse | clickhouse | Federated |
| `elasticsearch` | — | elasticsearch | — | No |
| `pinot` | — | pinot | — | No |
| `druid` | — | druid | druid | No |
| `exasol` | — | exasol | exasol | No |

### Data Lake / Open Table Formats

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `iceberg` | — | iceberg | — | Federated |
| `delta_lake` | — | delta_lake | — | Federated |
| `hive` | — | hive | hive | No |

### NoSQL

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `mongodb` | — | mongodb | — | No |
| `cassandra` | — | cassandra | — | No |
| `redis` | — | redis | — | No |
| `kudu` | — | kudu | — | No |
| `accumulo` | — | accumulo | — | No |

### Streaming

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `kafka` | — | kafka | — | Sink only |

### Graph & Semantic

| Source Type | Mechanism | Connector Name | Mutations |
|------------|-----------|-----------------|-----------|
| `neo4j` | Cypher via HTTP API, results cached in PG | — | No |
| `sparql` | SPARQL 1.1 POST, results cached in PG | — | No |

### Observability & Other

| Source Type | Direct Driver | Connector Name | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `prometheus` | — | prometheus | — | No |
| `google_sheets` | — | google_sheets | — | No |

### API Sources

Register any HTTP endpoint as a queryable table.

| API Type | Discovery | Column Inference |
|---------|-----------|-----------------|
| `openapi` | OpenAPI spec parsing | Primitives → native, objects → JSONB |
| `graphql_api` | Schema introspection | Primitives → native, objects → JSONB |
| `grpc_api` | Server reflection | Primitives → native, objects → JSONB |

**Execution**: API responses are fetched, cached in PostgreSQL (configurable TTL), and exposed as GraphQL types. Cached tables participate in federated queries like any other source.

**JSONB rules**: Complex columns (objects, arrays) stored as JSONB are not filterable and cannot participate in relationships. Use JSONB promotion to convert nested fields into native columns.

---

**Direct execution** — Single-source RDBMS queries route to the native driver for sub-100ms latency. Sources with a direct driver and SQLGlot dialect support this path.

**Federated execution** — Multi-source queries and sources without a direct driver route through the federation engine. Provisa includes an embedded engine; bring your own compatible cluster for large-scale deployments.

**Statistics** — On registration, Provisa runs `ANALYZE` against each published table to prime the cost-based optimizer (row counts, null fraction, distinct values, min/max). Failures are logged and do not block registration.

## Graph & Semantic Sources

### Neo4j

Register a Neo4j graph database as a queryable source. Stewards author Cypher queries that project scalar values; Provisa caches results and exposes them as GraphQL types.

**Requirements**: Cypher queries must use property accessors in the `RETURN` clause (`RETURN n.id AS id, n.name AS name`) — returning node objects is rejected at registration time.

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

The preview endpoint (`POST /admin/sources/neo4j/{id}/preview`) returns sample rows and blocks registration if the Cypher returns node objects.

### SPARQL

Register any SPARQL 1.1 compliant triplestore (Apache Jena Fuseki, Virtuoso, Stardog, etc.) as a queryable source.

**Requirements**: Queries must be `SELECT` queries. Variable names in the `SELECT` clause become column names automatically.

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

Both connectors use the API source cache pipeline — results are stored in PostgreSQL with configurable TTL, making them available for cross-source federated JOINs.

## Kafka Sources

Kafka topics as read-only tables via the federated Kafka connector.

**Schema sources**: Confluent Schema Registry (Avro, Protobuf, JSON Schema), manual definition, or sample inference.

**Sink**: Approved query results can be published to Kafka topics as JSON messages.

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

Single-source portions route directly; cross-source JOINs federate transparently with automatic type coercion.
