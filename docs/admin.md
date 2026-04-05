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

## GraphiQL

The admin API ships with GraphiQL at `GET /admin/graphql` in the browser. Use it to explore the full admin schema interactively.
