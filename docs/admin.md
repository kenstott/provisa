# Admin API

The admin API is a Strawberry GraphQL endpoint at `POST /admin/graphql`. It requires a superuser or admin role and is separate from the data GraphQL endpoint.

## Authentication

Pass your credentials in the `Authorization` header using the standard Provisa auth provider:

```yaml
Authorization: Bearer <token>
```

Admin access is governed by the `admin` capability assigned to a role.

### Personal access tokens

A personal access token is accepted anywhere a bearer token is, this endpoint included. You issue and revoke your own, because a token is its holder's own credential. That's why it lives on your profile in the admin UI, beside leaving an org and deleting your account, and not under an admin page. An administrator never mints a token on someone else's behalf.

| Route | Effect |
| ------- | -------- |
| `POST /auth/tokens` | Mint a token for the caller. Body: `name`, optional `role_id`, `scopes`, `expires_in_days` (1–366). The response is the only place the secret ever appears |
| `GET /auth/tokens` | The caller's active tokens in this org — display prefix, name, lifecycle timestamps, and the hash that identifies a token for revocation. Never a working credential |
| `DELETE /auth/tokens/{token_hash}` | Revoke one of the caller's tokens. 404 when it is not theirs or already revoked |

Omitting `role_id` leaves the token resolving to whatever role its owner holds; naming one narrows the token below its owner. Revocation also happens implicitly: removing a user's org membership revokes their tokens for that org. See [Security Model](security.md#personal-access-tokens) for the credential itself.

## Capabilities

### Config Management

Download the current running config:

```http
GET /admin/config
```

Returns the full `config.yaml` as a YAML file. Upload a new config:

```http
PUT /admin/config
```

Provisa validates the YAML, reloads catalogs, and regenerates schemas. No restart required.

### Runtime Settings

Read and write runtime platform settings without editing the config file:

```http
GET  /admin/settings
PUT  /admin/settings
```

The settings surface covers large-result redirect, default sampling and row limit, response-cache TTL, naming convention, relationship FK auto-tracking, materialization-store DSN, federation-engine memory (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), and the full OpenTelemetry tracing-pipeline tuning surface. Remote-GraphQL traversal limits and warm-tier/read-cache settings are also exposed.

The **Connector servers** card names, for each connector with a bundled server (File Crawler, SharePoint, Salesforce, ServiceNow, Cloud Inventory, Splunk, AskAmerica), a folder holding a server bundle built for this host (`connectors.<connector>.bundle_dir`, environment variable `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`). Empty, the connector's server is fetched from the pinned `kenstott/calcite` release. Set, the folder is used as it is: its launcher must be at `bin/pgwire-<connector>`, and a folder without it stops that connector's server from starting with an error naming the setting; the release is never fetched in its place. The setting is read each time a connector's server starts.

The security posture, `security.mode` (`standard` | `high`), is applied on restart:

```http
GET  /admin/security
PUT  /admin/security
```

AI model assignments, the embedding and vector-model registry, and the NL rate limit take effect on the next request, with no restart:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

The admin encryption tab derives its provider list live from the encryption registry; unavailable providers appear but are not selectable.

`GET`/`HEAD /health` and `GET /setup/status` never require authentication. They skip the `Authorization: Bearer` requirement even when an auth provider is configured.

### Federation Engine

Read or change which engine the deployment uses:

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` returns the active engine key and the config fields it needs. `PUT` accepts a body with `engine` (the key) and any engine-specific fields; the selection persists to the platform config and binds on the next service restart.

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

Create a relationship. An id that exists is refused; a relationship is changed with `updateRelationship`, which takes the same input:

```graphql
mutation {
  createRelationship(input: {
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

Declare a junction-backed relationship:

```graphql
mutation {
  createRelationship(input: {
    id: "pets-bonded-pair"
    sourceTableId: "pets"
    targetTableId: "pets"
    sourceColumn: "id"
    targetColumn: "id"
    cardinality: "one-to-many"
    viaTable: "pet_companions"
    viaSourceColumn: "pet_id"
    viaTargetColumn: "companion_pet_id"
    viaTypeColumn: "companion_type"
    viaTypeValue: "bonded pair"
    viaLabelSource: "column"
  }) {
    success
  }
}
```

You declare an associative table as an edge; Provisa never discovers one. `viaTable` names a registered table. Its two key columns carry the edge, and every other column becomes an attribute of the relationship, filterable like any field. `viaTypeColumn` / `viaTypeValue` split one junction table into several edge types. For example, rows of `pet_companions` whose `companion_type` is `bonded pair`, `littermate` or `shares enclosure` give three distinct relationships over the same pair of tables.

`viaLabelSource` says where the exposed name comes from, and Cypher gets each form in upper snake case. `column` uses `viaTypeValue` (`BONDED_PAIR`), `table` uses the junction table's own name (`PET_COMPANIONS`), and `fixed` uses the declared `alias`. A junction table declared this way is an edge. Provisa drops it from the node labels, so it never appears as a node pill in the graph UI.

### AI Relationship Discovery

Trigger Claude-powered FK analysis via REST:

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

Browse published tables across all sources:

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### Column dependency check

Before saving a table edit that renames a column's SQL alias or drops a column, ask what else
references it:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Renaming an alias breaks every artifact written against the exposed name: views, MVs, metric expressions, RLS predicates and DQ contracts. Dropping a column breaks those too, plus the artifacts that store the physical `column_name`, which are relationships, glossary bindings and tag assignments. `breaksOn` says which. The Tables page runs this check on save and shows the result as an advisory dialog. [Lineage](lineage.md) explains what the query covers and what it can't.

### View Management

Register a materialized view:

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

Trigger a manual refresh:

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
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

### Hasura / DDN Import

Convert an existing Hasura v2 or Hasura DDN project into Provisa config through the admin UI or API, without anything landing until you approve it.

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**Preview** converts the uploaded archive and returns the proposed `config_yaml`, a list of warnings, and a summary of what was found (source, domain, table, column, role, relationship, and RLS counts). Nothing is written to the tenant database. Request body:

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` is `"auto"` (detected from the archive structure), `"hasura_v2"`, or `"ddn"`.

**Apply** loads the YAML you reviewed, and edited if you chose to, into the acting org. It uses the same hot-reload path as `PUT /admin/config`. Request body: `{"config_yaml": "<yaml string>"}`.

Preview never caches the converted YAML server-side; apply takes the YAML you supply, so what is applied is exactly what was reviewed.

### Apache Ossie Interchange

Provisa interoperates with Apache Ossie (incubating) as an import/export boundary.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Export** (`GET /admin/ossie`) builds the Ossie YAML document from the live governed model on every request. Nothing is cached, so the document is never stale. The response is `text/yaml` with a `Content-Disposition: attachment` header. The document is one model with `version`, `name`, `datasets`, `relationships` and `metrics` at its root. Tables become `dataset` objects, columns become `field` objects, and relationships map to Ossie `relationship` objects. A model with no table is refused with a `422`.

**Import** (`POST /admin/ossie/import`) accepts an Ossie YAML or JSON document and detects the format itself. It parses the document and returns proposed table, relationship and metric registrations as a JSON object. Nothing is registered. It reads only the flat document shape, and a document that wraps its model in `semantic_model` is refused by name. In the admin UI, the review screen lets you accept or trim proposals before anything changes.

### Object Storage

Read or configure the org's materialization storage:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` reports how much of the platform storage allowance the org uses. `PUT` registers the org's own storage DSN (encrypted at rest; never returned by GET). Once set, the org's materializations land in its own bucket and are no longer counted against the platform allowance. Sending `storage_url: null` clears it and moves the org back to the platform store.

### Org Encryption

Set or rotate the org's at-rest encryption key:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` returns the key's fingerprint, id and provenance, and never key material. `PUT` sets or rotates the key. Supply `key_b64` (32 raw bytes, base64-encoded) to bring your own key, or omit it and Provisa generates one. There is no delete, because retiring the last key would leave every payload it wrapped unreadable.

## GraphiQL

The admin API ships with GraphiQL at `GET /admin/graphql` in the browser. Use it to explore the full admin schema interactively.

## Ops-domain management views

Every install seeds eight SQL views into the built-in `ops` domain. They expose the query audit log as governed tables. You query them through SQL (pgwire), GraphQL and Cypher under the same domain access, RLS and masking rules as any business table.

`org_admin` is designated as the ops-domain steward at seed time, so the domain never appears as a governance gap in `stale_metadata`.

| View | What it answers |
| --- | --- |
| `usage_ranking` | Query count and distinct users per registered table; zero-hit tables surface as deprecation candidates |
| `deprecated_usage` | Every access to a table or column carrying the `deprecated` tag — the active consumers blocking safe removal |
| `pii_access` | Every access to a table or column carrying the `pii` tag: who queried it, under which role, over which surface |
| `policy_denials` | All access attempts that governance rejected (HTTP 401/403) |
| `surface_mix` | Daily query count and distinct users per protocol surface (SQL, GraphQL, Cypher, gRPC, etc.) |
| `query_health` | Daily error count and average/max latency per surface |
| `stale_metadata` | Tables and columns missing descriptions; domains missing a steward |
| `join_hotspots` | Table pairs co-queried most often — candidates for materialization or caching |

Granularity stops at the table. The audit log records `table_ids`, and the individual columns a query read aren't recorded. Query text is encrypted and left out of every view here; only the authorised admin decrypt path can read it.

A role needs `ops` domain access before these views are visible. Grant it the same way you grant access to any other domain.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

Because the views are governed tables, you can write the same queries in GraphQL or Cypher.

## Reports viewer

The Reports viewer is at `/admin/reports`. Roles without the `observability` capability cannot reach it.

The left panel lists every registered table in the `ops` domain, sorted by alias. The eight seeded management views appear there automatically. Click any report to load it in the governed data viewer on the right.

**Adding a custom report.** The "Add report" button opens a dialog where you give a name, an optional description and a SELECT statement. Saving registers the view as a governed derived table in the `ops` domain. It's cataloged, access-controlled and queryable through every surface, alongside the seeded views.

**Deleting.** The trash icon appears only for custom reports. Seeded management views cannot be deleted from this interface.

## Table preview

Expand any table row on the Tables page. The **Preview** button opens a 90%-width modal with the table's live governed data.

Tables backed by APIs with required path parameters block preview until those values are supplied. An inline form collects each required parameter before the first query runs; optional query parameters appear in the same form.

## Governed data viewer

The same viewer component powers the preview modal and the Reports viewer. Its behavior is identical in both contexts.

**Server-side paging.** Each page is its own governed `SELECT *` with `LIMIT 101 OFFSET n`. 100 rows appear per page; the 101st signals whether more exist. The full dataset is never loaded into the browser.

**Pushed-down filters and sorts.** Each column header has a filter input. A filter term becomes a `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` predicate, and a sort click produces an `ORDER BY` clause. Both run in the database. A filter on a billion-row table scans the source, and its reach is never limited to the 100-row page in front of you.

**Multi-level group-by.** The Layers icon in any column header toggles that column into the grouping. Group columns lead the `ORDER BY` so group members land on the same page as their header across page boundaries. Primary-key columns are appended as a stable tiebreaker. Group-header rows are collapsible; collapsing hides members without issuing a new query.

**Persistent choices.** Filter, sort, and group-by settings persist to `localStorage` under `provisa.grid.table:<domain>.<table>` and restore on the next visit.

**Export.** Download the current page as CSV, or copy it to the clipboard as tab-separated text. Export covers the visible page only.
