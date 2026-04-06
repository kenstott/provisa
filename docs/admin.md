# Admin API

The admin API is a Strawberry GraphQL endpoint at `POST /admin/graphql`. It requires a superuser or admin role and is separate from the data GraphQL endpoint.

## Authentication

Pass the admin token in the `Authorization` header:
```
Authorization: Bearer <admin-token>
```

Admin tokens are configured via the `admin_token` field in `config.yaml` or the `PROVISA_ADMIN_TOKEN` environment variable.

## Capabilities

### Config Management

Download the current running config:
```graphql
query {
  configDownload
}
```

Returns the full `config.yaml` as a string. Upload a new config:
```graphql
mutation {
  configUpload(yaml: "sources:\n  - id: ...")
}
```

Provisa validates the YAML, reloads catalogs, and regenerates schemas. No restart required.

### Relationship Editor

List relationships:
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

Create a relationship:
```graphql
mutation {
  createRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: MANY_TO_ONE
  }) {
    id
  }
}
```

### AI Relationship Discovery

Trigger Claude-powered FK analysis:
```graphql
mutation {
  discoverRelationships(sourceIds: ["sales-pg", "crm-pg"]) {
    sourceColumn
    targetTable
    targetColumn
    confidence
    rationale
  }
}
```

Returns FK candidates ranked by confidence. Accept a candidate:
```graphql
mutation {
  acceptRelationshipCandidate(candidateId: "cand-42") {
    id
  }
}
```

### Persisted Query Approval

List pending queries:
```graphql
query {
  persistedQueries(status: PENDING) {
    id
    name
    query
    submittedBy
    submittedAt
  }
}
```

Approve or reject:
```graphql
mutation {
  approveQuery(id: "pq-101")
}

mutation {
  rejectQuery(id: "pq-101", reason: "Missing RLS coverage")
}
```

Deprecate (soft-remove without deletion):
```graphql
mutation {
  deprecateQuery(id: "pq-101", message: "Use pq-102 instead")
}
```

### Schema Introspection

Browse published tables across all sources:
```graphql
query {
  tables {
    id
    sourceId
    columns {
      name
      type
      masked
      writableBy
    }
    roles {
      id
      hasRls
    }
  }
}
```

### View Management

Register a materialized view:
```graphql
mutation {
  createView(input: {
    id: "orders-with-customers"
    sql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    refreshInterval: 300
    publish: true
  }) {
    id
  }
}
```

Trigger a manual refresh:
```graphql
mutation {
  refreshView(id: "orders-with-customers")
}
```

### Graph Source Registration

Neo4j and SPARQL sources are registered via REST endpoints (not the GraphQL admin API):

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

Once registered, tables appear in the GraphQL schema and are queryable like any other source.

## GraphiQL

The admin API ships with GraphiQL at `GET /admin/graphql` in the browser. Use it to explore the full admin schema interactively.

