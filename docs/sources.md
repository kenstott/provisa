# Source Types

## Database Sources

| Source Type | Direct Driver | Trino Connector | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `postgresql` | asyncpg | postgresql | postgres | Yes |
| `mysql` | aiomysql | mysql | mysql | Yes |
| `mariadb` | aiomysql | mariadb | mysql | Yes |
| `singlestore` | — | singlestore | singlestore | Via Trino |
| `sqlserver` | aioodbc | sqlserver | tsql | Yes |
| `oracle` | oracledb | oracle | oracle | Yes |
| `duckdb` | duckdb | memory | duckdb | Yes |
| `snowflake` | — | snowflake | snowflake | Via Trino |
| `bigquery` | — | bigquery | bigquery | Via Trino |
| `clickhouse` | — | clickhouse | clickhouse | Via Trino |
| `redshift` | — | redshift | redshift | Via Trino |
| `databricks` | — | delta_lake | databricks | Via Trino |
| `hive` | — | hive | hive | No |
| `druid` | — | druid | druid | No |
| `exasol` | — | exasol | exasol | No |
| `mongodb` | — | mongodb | — | No |
| `cassandra` | — | cassandra | — | No |
| `neo4j` | API cache pipeline | — | — | No |
| `sparql` | API cache pipeline | — | — | No |

**Direct execution**: Single-source RDBMS queries route to the native driver for sub-100ms latency. Sources with a direct driver and SQLGlot dialect support this path.

**Trino federation**: Multi-source queries, NoSQL sources, and cloud warehouses route through Trino for cross-source JOINs.

**NoSQL limitations**: MongoDB and Cassandra are read-only via Trino. No mutations, no direct execution.

**Statistics**: On registration, Provisa runs `ANALYZE` against each published table. This primes the federation engine's cost-based optimizer with row counts and column statistics (null fraction, distinct values, min/max). The optimizer uses these to estimate join cardinality and choose efficient execution plans — broadcast vs. partitioned join, join order, predicate pushdown. If a connector does not support `ANALYZE`, the failure is logged and registration proceeds normally.

## API Sources

Register REST, GraphQL, and gRPC endpoints as queryable tables.

| API Type | Discovery | Column Inference |
|---------|-----------|-----------------|
| `openapi` | OpenAPI spec parsing | Primitives → native, objects → JSONB |
| `graphql_api` | Schema introspection | Primitives → native, objects → JSONB |
| `grpc_api` | Server reflection | Primitives → native, objects → JSONB |

**JSONB rules**: Complex columns (objects, arrays) are stored as JSONB. They are NOT filterable, CANNOT participate in relationships, and appear as JSON scalars in the SDL.

**JSONB promotion**: Stewards can promote nested JSONB fields into native PG generated columns, making them filterable and relationship-eligible.

**Caching**: API responses are cached in PG with configurable TTL. Cache key = `hash(endpoint_id, sorted_params)`.

## Graph & Semantic Sources

### Neo4j

Register a Neo4j graph database as a queryable source. Stewards author Cypher queries that project scalar values; Provisa caches results and exposes them as GraphQL types.

**Requirements**: Cypher queries must use property accessors in the `RETURN` clause (`RETURN n.id AS id, n.name AS name`) — returning node objects is rejected at registration time.

```yaml
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

```yaml
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

Both connectors use the existing API source cache pipeline — results are stored in PostgreSQL with configurable TTL, making them available for cross-source JOINs via Trino federation.

## Kafka Sources

Kafka topics as read-only tables via the Trino Kafka connector.

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
    productReviews {  # MongoDB (via Trino)
      rating
      comment
    }
  }
}
```

Single-source portions route directly; cross-source JOINs federate through Trino with automatic type coercion.
