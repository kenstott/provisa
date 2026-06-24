# Admin API

The admin API is a Strawberry GraphQL endpoint at `POST /admin/graphql` (REQ-533). It requires a superuser or admin role (REQ-125, REQ-060) and is separate from the data GraphQL endpoint (REQ-533).

## Authentication

Pass your credentials in the `Authorization` header using the standard Provisa auth provider (REQ-120):
```
Authorization: Bearer <token>
```

Admin access is governed by the `admin` capability assigned to a role (REQ-060, REQ-042).

## Capabilities

### Config Management

Download the current running config (REQ-164):
```
GET /admin/config
```

Returns the full `config.yaml` as a YAML file. Upload a new config (REQ-164):
```
PUT /admin/config
```

Provisa validates the YAML, reloads catalogs, and regenerates schemas (REQ-012, REQ-253). No restart required.

### Relationship Editor

List relationships (REQ-166):
```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

Create a relationship (REQ-019):
```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### AI Relationship Discovery

Trigger Claude-powered FK analysis via REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Returns FK candidates ranked by confidence. Accept a candidate:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Schema Introspection

Browse published tables across all sources (REQ-008):
```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### View Management

Register a materialized view (REQ-133, REQ-135):
```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

Trigger a manual refresh (REQ-135):
```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Graph Source Registration

Neo4j and SPARQL sources are registered via REST endpoints (not the GraphQL admin API) (REQ-295, REQ-297):

**Neo4j:**
```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL:**
```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

Once registered, tables appear in the GraphQL schema and are queryable like any other source (REQ-016).

## GraphiQL

The admin API ships with GraphiQL at `GET /admin/graphql` in the browser (REQ-622). Use it to explore the full admin schema interactively.
