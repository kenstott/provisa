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
| `GET /auth/tokens` | The caller's active tokens in this org: display prefix, name, lifecycle timestamps, and the hash that identifies a token for revocation. Never a working credential |
| `DELETE /auth/tokens/{token_hash}` | Revoke one of the caller's tokens. 404 when it is not theirs or already revoked |

Omitting `role_id` leaves the token resolving to whatever role its owner holds; naming one narrows the token below its owner. Revocation also happens implicitly. Removing a user's org membership revokes their tokens for that org. See [Security Model](security.md#personal-access-tokens) for the credential itself.

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

Ask Claude for FK candidates over REST:

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

An org administrator reads the org's storage footprint:

```http
GET  /admin/org-storage
```

`GET` reports how much of the platform storage allowance the org uses.

A platform administrator assigns an organization's own store:

```http
GET  /admin/platform/orgs/{org_id}/storage
PUT  /admin/platform/orgs/{org_id}/storage
```

`PUT` registers the org's own storage DSN (encrypted at rest; never returned by GET). Once set, the org's materializations land in its own bucket and are no longer counted against the platform allowance. Sending `storage_url: null` clears it and moves the org back to the platform store.

### Org Encryption

Set or rotate the org's at-rest encryption key:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` returns the key's fingerprint, id and provenance, and never key material. `PUT` sets or rotates the key. Supply `key_b64` (32 raw bytes, base64-encoded) to bring your own key, or omit it and Provisa generates one. There is no delete, because retiring the last key would leave every payload it wrapped unreadable.

## GraphiQL

The admin API ships with GraphiQL at `GET /admin/graphql` in the browser. Use it to browse the full admin schema and try calls.

## Ops-domain management views

Every install seeds eight SQL views into the built-in `ops` domain. They expose the query audit log as governed tables. You query them through SQL (pgwire), GraphQL and Cypher under the same domain access, RLS and masking rules as any business table.

`org_admin` is designated as the ops-domain steward at seed time, so the domain never appears as a governance gap in `stale_metadata`.

| View | What it answers |
| --- | --- |
| `usage_ranking` | Query count and distinct users per registered table; zero-hit tables surface as deprecation candidates |
| `deprecated_usage` | Every access to a table or column carrying the `deprecated` tag, which shows the active consumers blocking safe removal |
| `pii_access` | Every access to a table or column carrying the `pii` tag: who queried it, under which role, over which surface |
| `policy_denials` | All access attempts that governance rejected (HTTP 401/403) |
| `surface_mix` | Daily query count and distinct users per protocol surface (SQL, GraphQL, Cypher, gRPC, etc.) |
| `query_health` | Daily error count and average/max latency per surface |
| `stale_metadata` | Tables and columns missing descriptions; domains missing a steward |
| `join_hotspots` | Table pairs co-queried most often, as candidates for materialization or caching |

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

### Tags report

`tags_report` has one row for each tag on each object. It also has a row for each required tag missing from an object of a kind the tag applies to. Polly and MCP clients read the same rows from the meta table `meta.tag_instances`. The report covers tags that someone assigned, tags a column inherits from its glossary term, tags Provisa derives from an object's registered state, and the library sections each of them cites. It does not yet show attestations, gates or the people behind each tag, and it shows the present state only.

Each row carries the object's kind, id, domain and owner. Its name comes from the names table of the last model build, and is empty for kinds that table does not name, such as roles. The row also carries the tag's definition, whether the tag is required, and whether its assignments must cite a library section. An assigned row adds the parameter, reason, expiry, purpose, review date, and who assigned it and when.

The `via` column says how the tag got onto the object. `assigned` means someone put it there. `inherited` means the column's glossary term carries it. `derived` means Provisa computes it from the object's registered state, as it does for `fact`. `missing` marks a required tag the object lacks.

Provisa computes the citation columns and the status each time you read the report and stores none of them. `citations_broken` counts cited sections that no longer exist. `citations_drifted` counts sections whose text changed after they were cited. `citation_missing` is true when the tag requires a citation and the assignment has none. `expired` is true once the expiry date has passed. A row with any of these, a `missing` row, or whose tag definition cites a section that changed, has the status `needs attention`, and `status_reason` lists why. Every other row is `clean`.

You see a row exactly when the tag admin would show you that assignment. A tag on a table, column, relationship, command, data product or row rule needs every domain the object touches. A tag on a source needs one of the source's allowed domains, if it names any. A tag on a glossary term or a library document is read the way the term or document is. A tag on a row rule also needs the `access_config` right. A tag on a role needs `user_management`, unless the role is your own. A tag on a metric needs the metric to be granted to your role.

Group by `base_tag_id` to see each tag's quality, or by `domain_id` or `object_key` to see what needs attention there.

### Tags, library sections and objects

Three meta tables connect the reference library to the model. You query them through SQL, GraphQL or Cypher like any other meta table, and Polly and MCP clients read them the same way.

`meta.tag_citations` has one row per citation of a library document or section. `citer` is `definition` when a tag's definition cites the place, and `assignment` when the tag's assignment to an object cites it. An assignment's row names the object as `tag_instances` does. `state` is `current`, `drifted` when the section's text changed after it was cited, or `broken` when the section or document is gone. `needs_review` is true for any state other than `current`.

`meta.section_tags` has one row per tag on a document or on a section of one. `section_heading` and `section_page` are empty when the tag is on the whole document. The row carries the assignment's reason, expiry, purpose and review date.

`meta.policy_coverage` has one row per path from a document or section to an object. The `link` column says how the path runs:

| `link` | The path |
|---|---|
| `shared tag` | The section carries a tag the object also carries. |
| `tag cites it` | The definition of a tag the object carries cites the section. |
| `assignment cites it` | The object's assignment of the tag cites the section. |

Each coverage row names the object by kind, key, id, name, domain and table, as `tag_instances` does, with `via` saying how the tag got onto the object. A citation's path also carries the citation's state in `citation_state`.

You see a row only where you read its document. A row that names an object also needs you to read the tag on that object, by the rule the tags report uses. A citation of a tag's definition needs the document alone.

To find the columns relevant to document 12:

```sql
SELECT DISTINCT object_name, column_key, link, section_heading
FROM meta.policy_coverage
WHERE document_id = 12 AND object_kind = 'column'
ORDER BY object_name, column_key;
```

To find the tags whose definition cites a section that changed:

```sql
SELECT tag_id, document_title, section_heading, section_page, state
FROM meta.tag_citations
WHERE citer = 'definition' AND state <> 'current';
```

In GraphQL and Cypher, a coverage row and a citation reach their `tag_instances` row, their tag, and the registered table and column the object is or belongs to. A tagged section reaches the objects covered from it. A table, a column and a tag instance reach the paths that cover them. The library documents are not a table of their own, so a row names its document by id and title.

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
