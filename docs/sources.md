# Source Types

## Database Sources

| Source Type | Direct Driver | Trino Connector | SQLGlot Dialect | Mutations |
|------------|--------------|-----------------|-----------------|-----------|
| `postgresql` | asyncpg | postgresql | postgres | Yes |
| `mysql` | aiomysql | mysql | mysql | Yes |
| `sqlserver` | aioodbc | sqlserver | tsql | Yes |
| `oracle` | oracledb | oracle | oracle | Yes |
| `duckdb` | duckdb | memory | duckdb | Yes |
| `snowflake` | — | snowflake | snowflake | Via Trino |
| `bigquery` | — | bigquery | bigquery | Via Trino |
| `mongodb` | — | mongodb | — | No |
| `cassandra` | — | cassandra | — | No |

**Direct execution**: Single-source RDBMS queries route to the native driver for sub-100ms latency.

**Trino federation**: Multi-source queries and NoSQL sources route through Trino for cross-source JOINs.

**NoSQL limitations**: MongoDB and Cassandra are read-only via Trino. No mutations, no direct execution.

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
