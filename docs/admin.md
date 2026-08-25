# Admin API

The admin API is a Strawberry GraphQL endpoint at `POST /admin/graphql` (REQ-533). It requires a superuser or admin role (REQ-125, REQ-060) and is separate from the data GraphQL endpoint (REQ-533).

## Authentication

Pass your credentials in the `Authorization` header using the standard Provisa auth provider (REQ-120):

```yaml
Authorization: Bearer <token>
```

Admin access is governed by the `admin` capability assigned to a role (REQ-060, REQ-042).

### Personal access tokens

A personal access token is accepted anywhere a bearer token is, including this endpoint. Issuing and revoking one is self-service — it is the token holder's own credential, so it lives on the user's profile in the admin UI rather than under an admin page, beside leaving an org and deleting the account. An administrator does not mint tokens on someone else's behalf. (REQ-1263)

| Route | Effect |
| ------- | -------- |
| `POST /auth/tokens` | Mint a token for the caller. Body: `name`, optional `role_id`, `scopes`, `expires_in_days` (1–366). The response is the only place the secret ever appears |
| `GET /auth/tokens` | The caller's active tokens in this org — display prefix, name, lifecycle timestamps, and the hash that identifies a token for revocation. Never a working credential |
| `DELETE /auth/tokens/{token_hash}` | Revoke one of the caller's tokens. 404 when it is not theirs or already revoked |

Omitting `role_id` leaves the token resolving to whatever role its owner holds; naming one narrows the token below its owner. Revocation also happens implicitly: removing a user's org membership revokes their tokens for that org. See [Security Model](security.md#personal-access-tokens) for the credential itself.

## Capabilities

### Config Management

Download the current running config (REQ-164):

```http
GET /admin/config
```

Returns the full `config.yaml` as a YAML file. Upload a new config (REQ-164):

```http
PUT /admin/config
```

Provisa validates the YAML, reloads catalogs, and regenerates schemas (REQ-012, REQ-253). No restart required.

### Runtime Settings

Read and write runtime platform settings without editing the config file (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

The settings surface covers large-result redirect, default sampling and row limit, response-cache TTL, naming convention, relationship FK auto-tracking, materialization-store DSN, federation-engine memory (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), and the full OpenTelemetry tracing-pipeline tuning surface (REQ-1082). Remote-GraphQL traversal limits and warm-tier/read-cache settings are also exposed (REQ-1081, REQ-1083).

Security posture — `security.mode` (`standard` | `high`) — applied on restart (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

AI model assignments, the embedding/vector-model registry, and the NL rate limit — take effect on the next request, no restart required (REQ-1349): [tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

The admin encryption tab derives its provider list live from the encryption registry; unavailable providers appear but are not selectable (REQ-1091).

`GET`/`HEAD /health` and `GET /setup/status` are always unauthenticated — they bypass the `Authorization: Bearer` requirement even when an auth provider is configured (REQ-539).

### Federation Engine

Read or change which engine the deployment uses (REQ-916):

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` returns the active engine key and the config fields it needs. `PUT` accepts a body with `engine` (the key) and any engine-specific fields; the selection persists to the platform config and binds on the next service restart. [tool-verified: `provisa/api/admin/settings_router.py:730-829`]

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

Declare a junction-backed relationship (REQ-1586):

```graphql
mutation {
  upsertRelationship(input: {
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

An associative table is declared as an edge, never discovered. `viaTable` names a registered table; its two key columns carry the edge, and every remaining column becomes an attribute of the relationship, filterable like any other field. `viaTypeColumn` / `viaTypeValue` split one junction table into several edge types — three rows of `pet_companions` with `companion_type` of `bonded pair`, `littermate`, and `shares enclosure` are three distinct relationships over the same pair of tables.

`viaLabelSource` nominates where the exposed name comes from, and all three forms are upper-snake-cased for Cypher: `column` uses `viaTypeValue` (`BONDED_PAIR`), `table` uses the junction table's own name (`PET_COMPANIONS`), `fixed` uses the declared `alias`. A junction table declared this way is an edge and not an entity — it is dropped from the node labels, so it never appears as a node pill in the graph UI. [tool-verified: `provisa/api/admin/types.py:606-611`, `provisa/api/admin/db_queries.py:47-82`]

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

### Column dependency check (REQ-1484)

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

Renaming an alias breaks every artifact authored against the exposed name — views, MVs, metric
expressions, RLS predicates, DQ contracts. Dropping a column breaks those plus the artifacts that
store the physical `column_name`: relationships, glossary bindings, tag assignments. `breaksOn`
says which. The Tables page runs this on save and shows the result as an advisory dialog. See
[Lineage](lineage.md) for what the query covers and what it cannot.

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

### Hasura / DDN Import (REQ-1483)

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

**Apply** takes the YAML you reviewed (and optionally edited) and loads it into the acting org — the same hot-reload path as `PUT /admin/config`. Request body: `{"config_yaml": "<yaml string>"}`.

Preview never caches the converted YAML server-side; apply takes the YAML you supply, so what is applied is exactly what was reviewed. [tool-verified: `provisa/api/admin/import_router.py`]

### Apache Ossie Interchange (REQ-1316, REQ-1321)

Provisa interoperates with Apache Ossie (incubating) as an import/export boundary.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Export** (`GET /admin/ossie`) derives the Ossie YAML document from the live governed model on every request — it is never cached, so it cannot be stale. The response is `text/yaml` with a `Content-Disposition: attachment` header. Tables become `dataset` objects, columns become `field` objects, and relationships map to Ossie `relationship` objects. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py:download_ossie`]

**Import** (`POST /admin/ossie/import`) accepts an Ossie YAML or JSON document (the format auto-detects). It parses the document and returns proposed table and relationship registrations as a JSON object — nothing is registered. The review screen in the admin UI lets you accept or trim proposals before any mutation fires. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py:import_ossie`]

### Object Storage (REQ-1046, REQ-1048, REQ-1049)

Read or configure the org's materialization storage:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` reports how much of the platform storage allowance the org uses. `PUT` registers the org's own storage DSN (encrypted at rest; never returned by GET). Once set, the org's materializations land in its own bucket and are no longer counted against the platform allowance. Sending `storage_url: null` clears it and moves the org back to the platform store. [tool-verified: `provisa/api/admin/org_storage_router.py`]

### Org Encryption (REQ-1574)

Set or rotate the org's at-rest encryption key:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` returns the key's fingerprint, id, and provenance — never key material. `PUT` sets or rotates the key. Supply `key_b64` (32 raw bytes, base64-encoded) to bring your own key, or omit it to have Provisa generate one. There is no delete: retiring the last key would leave every payload it wrapped unreadable. [tool-verified: `provisa/api/admin/org_encryption_router.py`]

## GraphiQL

The admin API ships with GraphiQL at `GET /admin/graphql` in the browser (REQ-622). Use it to explore the full admin schema interactively.

## Ops-domain management views (REQ-1386)

Eight SQL views are seeded into the built-in `ops` domain on every install. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] They expose the query audit log as governed tables — queryable through SQL (pgwire), GraphQL, and Cypher under the same domain access, RLS, and masking rules as any business table.

`org_admin` is designated as the ops-domain steward at seed time, so the domain never appears as a governance gap in `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

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

Two limits apply today. Granularity is at the table level — the audit log records `table_ids`, not individual columns accessed. Query text is encrypted (REQ-689) and excluded from every view here; it is accessible only through the authorised admin decrypt path. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

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

The same queries run as GraphQL or Cypher over any governed transport — pgwire, Arrow Flight, or Bolt. [inferred from governed-surface design]

## Reports viewer (REQ-1390)

The Reports viewer is at `/admin/reports`. Roles without the `observability` capability cannot reach it.

The left panel lists every registered table in the `ops` domain, sorted by alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] The eight seeded management views appear there automatically. Click any report to load it in the governed data viewer on the right.

**Adding a custom report.** The "Add report" button opens a dialog. Provide a name, an optional description, and a SELECT statement. Saving registers the view as a governed derived table in the `ops` domain — cataloged, access-controlled, and queryable through every surface alongside the seeded views. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Deleting.** The trash icon appears only for custom reports. Seeded management views cannot be deleted from this interface. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Table preview (REQ-1392)

Expand any table row on the Tables page. The **Preview** button opens a 90%-width modal with the table's live governed data. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Tables backed by APIs with required path parameters block preview until those values are supplied. An inline form collects each required parameter before the first query runs; optional query parameters appear in the same form. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Governed data viewer (REQ-1391)

The same viewer component powers the preview modal and the Reports viewer. Its behavior is identical in both contexts.

**Server-side paging.** Each page is its own governed `SELECT *` with `LIMIT 101 OFFSET n`. 100 rows appear per page; the 101st signals whether more exist. The full dataset is never loaded into the browser. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Pushed-down filters and sorts.** Each column header has a filter input. Filter terms become `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` predicates; sort clicks produce `ORDER BY` clauses. Both go to the database — a filter on a billion-row table scans the source, not the 100-row page in front of you. [tool-verified: `nativeParams.ts:53-70`]

**Multi-level group-by.** The Layers icon in any column header toggles that column into the grouping. Group columns lead the `ORDER BY` so group members land on the same page as their header across page boundaries. Primary-key columns are appended as a stable tiebreaker. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Group-header rows are collapsible; collapsing hides members without issuing a new query. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Persistent choices.** Filter, sort, and group-by settings persist to `localStorage` under `provisa.grid.table:<domain>.<table>` and restore on the next visit. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Export.** Download the current page as CSV, or copy it to the clipboard as tab-separated text. Export covers the visible page only. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
