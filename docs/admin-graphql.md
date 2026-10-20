# Admin GraphQL API Reference

The admin GraphQL API is Provisa's configuration plane. It is the API the admin web app calls for every management operation: creating sources, registering tables, defining relationships, configuring RLS rules, and everything else that shapes the model.

**Mount point:** `POST /admin/graphql`

This is not the same API as the data plane at `/data/graphql`. The data plane serves end-user queries over registered domains and is described by the SDL at `/data/sdl`. The admin API configures what that schema looks like and who may see what.

---

## How the UI talks to this API

The admin web app uses Apollo Client, pointed at `${API_BASE}/admin/graphql`.

Every request carries a bearer token (fetched fresh from the auth provider on each call), an `X-Org-Id` header when multi-tenant, and an `X-Env` header when serving a branch environment.

The schema is assembled from two `@strawberry.type` classes — `Query` from `schema_query.py` and `Mutation` from `schema_mutation.py` — and wrapped in a `ModelCommitExtension` that records every mutation against the current environment branch.

---

## Authorization

**Dev mode:** when no auth is configured and every request arrives as an anonymous principal, all capability checks are skipped. This keeps a local install functional without auth setup.

**Capability gates:** production deployments enforce named capabilities. The specific right required by each field is noted inline. Calling a mutation without the required capability raises a `PermissionError`. The platform administrator role bypasses all capability checks.

**Domain gates:** several mutations also check the domain the object belongs to. A caller scoped to `sales` cannot register a table into `finance`, queue an RLS rule for it, or create a relationship whose source table lives in a domain they do not hold. Views are further constrained: every table the view SQL reads must be within the caller's domains, because free-hand SQL otherwise gives a member access to data outside their scope.

**Role inheritance:** a parent role's capabilities are inherited by child roles. `createRole` and `deleteRole` refuse cycles and prevent deleting a role that has heirs.

---

## Common return type

Most mutations return `MutationResult`.

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization
}
```

When a mutation fails, `success` is `false` and `message` carries the English reason. `code` is a stable identifier the UI uses to render a localized message.

---

## Queries

### Sources

#### `sources → [SourceType!]!`

All registered data sources.

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` is a `${secret:NAME}` reference into the org vault, never the literal credential.

#### `source(id: String!) → SourceType`

A single source by ID. Returns `null` when not found.

#### `availableSchemas(sourceId: String!) → [String!]!`

Schemas visible in a source, filtered to exclude Provisa-internal ones. Uses native introspection first; falls through to the engine catalog when the source type has no direct pool.

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tables in one schema of a source, with their comments. For OpenAPI sources, returns GET operations whose response is an array or pagination wrapper. For GraphQL sources, returns query fields returning a list. For gRPC, returns server-streaming RPCs.

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Column names for a table in the engine catalog. For govdata sources, uses a separate resolver.

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Column names with data types, comments, native filter types, and primary-key flags. For OpenAPI sources, derives the shape from the operation's response schema and parameters.

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Non-GET operations for an OpenAPI source (POST, PUT, PATCH, DELETE). Returns an empty list for non-OpenAPI sources.

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Preview what a file-connector crawl would discover — files, tables, and columns — before a source is created. The HTTP-only settings (`simpleLinks`, `sameDomain`, `excludePattern`) are ignored for local, S3, FTP, and SFTP roots.

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Returns the alias to use when registering `tableName` in `domainId` from `sourceId`. Returns a plain snake-case alias when no conflict exists, or a source-prefixed alias (`sqlite_b_orders`) when the effective name is already taken by a different source in the same domain.

---

### Tables

#### `tables → [RegisteredTableType!]!`

All registered tables, each with their full column list. Column visibility in the response respects the caller's `table_registration` capability: `canDeployToDb` is gated on whether the caller holds that right.

Each `RegisteredTableType` exposes computed sub-fields:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — the effective refresh/serving policy as plain text, derived server-side from the same planner resolution the engine uses. Returns `null` during startup.
- **`graphqlFieldName → String`** — the field name this table has in the compiled data-plane schema, so the Data Product panel can build a runnable example without replicating the naming algorithm.
- **`dqDataset → String`** — this table as a data-quality contract dataset, in the form the checker scans.
- **`productId → String`** — the data product this table belongs to. A DQ-checker table inherits the product of the table its contract scans.

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Preview the effective refresh/serving summary for *draft* (unsaved) table knobs, so the top-of-form summary updates as fields change without persisting anything. Same derivation as `refreshPolicySummary` above.

Arguments: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `replicate`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artifacts that a pending alias rename or column drop would break. It is advisory: the admin UI shows this before saving and the administrator decides. Must be called *before* saving, because the dependents were authored against the exposed name the column currently carries.

---

### Relationships

#### `relationships → [RelationshipType!]!`

All user-defined relationships (excludes auto-generated `gql_auto__` entries and the synthetic `meta:%` entries used by the ERD).

#### `allRelationships → [RelationshipType!]!`

Same as `relationships`, but includes `meta:%` synthetic entries. Used by the graph ERD, which needs to show every edge including the implicit `HAS_TABLE` links between data tables and the metadata registry.

Each `RelationshipType` exposes:

- **`autoSuggested → Boolean`** — whether the relationship was suggested by FK analysis (`id` starts with `fk__`).
- **`physicalName → String`** — the relationship's name on the SQL and gRPC planes (the `?include=` parameter). Derived server-side; clients must not transliterate the GraphQL alias.

---

### Domains, roles & users

#### `domains → [DomainType!]!`

All domains in the active org's tenant database. The tenant database is isolated at the schema level, so an org-admin's domain list contains only their org's rows.

#### `roles → [RoleType!]!`

Roles visible to the caller. An admin sees every role; a non-admin sees only roles with no `org_id` or roles belonging to their org.

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Resolve role IDs or user IDs to individual users. Used to expand `DataProduct.ownerRole`, `Domain.steward`, and `Column.visibleTo` into a human-readable list. Unknown refs are echoed back bare so the UI shows the raw ID rather than nothing.

---

### RLS rules

#### `rlsRules → [RLSRuleType!]!`

All row-level security rules. The underlying repository decrypts `filterExpr` at the boundary.

---

### Data products

#### `dataProducts → [DataProductType!]!`

All data products. Requires `data_product_read` capability.

---

### Tags

#### `tags → [TagType!]!`

All tag definitions, including their per-tag permitted parameter values.

#### `tagAssignments → [TagAssignmentType!]!`

All tag assignments across sources, tables, columns, and relationships.

---

### Materialized views

#### `mvList → [MVType!]!`

All materialized views with their runtime status: enabled/disabled, last refresh timestamp, row count, and last error.

---

### Cache

#### `cacheStats → CacheStatsType`

Cache statistics. Returns `storeType: "redis"` with full operational metrics when Redis is configured, `storeType: "memory"` for the embedded fakeredis store, and `storeType: "noop"` when no cache is configured.

#### `cacheTableStats → [CacheTableStatType!]!`

Per-table cached entry counts. Empty when no cache store is configured.

#### `hotTables → [HotTableStatType!]!`

Tables Provisa is keeping a copy of, across two tiers: `hot` (mirrored into the response store for JOIN inlining) and `warm` (landed as an Iceberg copy). A table is in at most one tier.

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identity of the durable materialization store: engine name, store DSN reference, MV count, and whether the store is instance-local (a local file like DuckDB or SQLite, which means each instance behind a load balancer keeps its own copy).

---

### System health

#### `systemHealth → SystemHealthType`

Engine connection status, worker pool counts, metadata DB pool state, cache mode, and liveness of every protocol listener (pgwire, gRPC, Arrow Flight, Bolt).

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### Scheduled tasks

#### `scheduledTasks → [ScheduledTaskType!]!`

Scheduled triggers from config with runtime state. Each entry carries its cron expression, `kind` (`webhook` or `sql`), whether it is currently enabled, the last run timestamp (always `null`; the scheduler tracks it), and the next scheduled run time from APScheduler.

---

### Data quality

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Parse raw contract text into the builder panel's editable rows. Called on every edit; a parse failure comes back as `error` rather than as a GraphQL error, because half-written text is normal while the operator is typing.

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

The checks `checker` offers, scoped to the columns of `dataset`. The dataset is the contract's observed target, resolved the same way the scanner resolves it, so the offered checks match the columns the checker will really see.

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

One check's text from the panel's editors. Server-side because the dialect has one implementation; a builder-made check and a hand-typed one must be indistinguishable.

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serialize edited check rows back into contract text. The inverse of `dqContractParse`. Server-side for the same reason: the panel cannot emit text the checker would refuse.

---

### Source previews

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Preview a Cypher projection on a Neo4j source: up to five rows and the column types the registration will carry. Failures come back as `error`.

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Preview a SPARQL SELECT on a SPARQL source: up to five rows, all columns as text.

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Live check against the Kaggle API. Returns `true` only when the token authenticates. Backs the token-gate step in the Kaggle source form.

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Search Kaggle's full public dataset catalog. Token-gated.

---

### Calendars

#### `calendars → [CalendarType!]!`

All registered snapshot-boundary calendar versions. Feeds the snapshot-schedule config picker and confirms which calendars a periodic MV may reference.

---

### Metrics

#### `metrics → [MetricType!]!`

All governed metric definitions. Fact-derived metrics carry `fromFact`.

---

### Schema version

#### `schemaVersion → String!`

SHA-256 hash of the current schema state (domains, table IDs, relationship IDs). The Apollo client reads this from the `X-Schema-Version` response header and refetches all active queries when it advances.

---

### AI helpers

#### `generateTableDescription(tableId: String!) → String!`

Use the configured LLM to generate a one-to-two sentence description for a registered table. Save the table first; calling this on an unsaved table returns an instructional message.

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Use the configured LLM to generate a one-sentence description for a single column.

---

### Creation requests

#### `creationRequests → [CreationRequestType!]!`

Pending creation requests, visible to callers holding the relevant create capability. Used when a member without `create_relationship` or `create_view` submits a request that a rights-holder must approve. A relationship request is listed for the users whose `create_relationship` right reaches the domain of its source or target table, and for the user who made it.

---

## Mutations

### Sources

#### `createSource(input: SourceInput!) → MutationResult`

Register a new data source. An id that exists is refused (`schema.source_id_taken`) after the definition checks and before anything is staged or written; changing a source is `updateSource`. Validates the connection before persisting. A rejected source leaves no vault entry behind. Stores credentials in the org vault and records the reference; the plaintext never lands in the database. Requires `source_registration` capability.

#### `updateSource(input: SourceInput!) → MutationResult`

Update an existing source's connection details, description, and config, then provision it again on the bound engine, rebuild the schemas and reconcile its landed tables, so a source saved under one engine is served by one swapped to since. Tears down and re-attaches the pgwire endpoint for file/SharePoint sources so a path change takes effect immediately. Requires `source_registration`.

#### `deleteSource(id: String!) → MutationResult`

Remove a source and its vault entry. Drops the engine catalog and rebuilds schemas.

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Rename a source ID. A new ID that another source holds is refused (`schema.source_id_taken`) and nothing is written; renaming a source onto its own ID changes nothing.

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Enable or disable query result caching for a source, and set the TTL in seconds.

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

Set when a source's tables are served from their replicas: `null` Default, `-1` Never, `N` above 0 once a table passes N governed statements per interval, `0` Always. A table with no value of its own inherits it. Refused when it would pair `load_protected` with `-1`, or leave a `ttl` table that it replicates without a `cache_ttl`. Requires `source_registration`.

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Mark a source load-protected (scheduled-refresh-only). Requires at least one gate — off-peak window, cache TTL cadence, or a probing change signal — or the call fails.

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Set the per-source GraphQL naming convention.

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Set which domains may use a source (empty list = unrestricted). Requires `source_registration`.

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Download and unzip a Kaggle dataset onto local disk. Returns the staged directory path; the caller then creates one `files`-type source pointing at it. SQLite-bearing bundles are rejected whole. Requires `source_registration`.

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Re-fetch a Kaggle-derived source's dataset in place. Skips the download if Kaggle has nothing newer than what is on disk. Requires `source_registration`.

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Run `ANALYZE` on all registered tables for a source. Improves join-order and broadcast decisions for federated queries.

---

### Tables

#### `registerTable(input: TableInput!) → MutationResult`

Register a new table (or view) into a domain. Requires `table_registration` capability and membership in the target domain. A caller lacking `create_relationship` who submits a view is queued as a creation request for a rights-holder to approve.

#### `updateTable(input: TableInput!) → MutationResult`

Update an existing table's alias, description, column metadata, MV settings, and live-delivery config. Requires `table_registration`.

#### `deleteTable(id: Int!) → MutationResult`

Delete a registered table. Looks up the table's domain for the domain gate before deleting.

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Override the cache TTL for one table.

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

Set when one table is served from its replica. `null` = inherit the source's value. Same values and refusals as `updateSourceReplicate`. Requires `table_registration`.

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Override load protection for one table. `null` for `loadProtected` inherits the source default. Validates the effective (table → source) gate combination.

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Set the per-table GraphQL naming convention.

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promote a virtual Provisa view to a real database view on its underlying native source.

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Recompute a table's landed rows on demand, bypassing the normal change gate. `reason` is a required audit annotation. Refused for live-federated tables (no landed rows).

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Force a SQLite file-connector table's next access to re-sync from disk.

#### `registerEntity(input: EntityInput!) → MutationResult`

Sugar for registering a dimension/hub entity. Lowers to a (bitemporal, when historized) MV and calls `registerTable`.

#### `registerFact(input: FactInput!) → MutationResult`

Sugar for registering a star-schema fact. Lowers to an aggregate MV, creates dimension relationships, and auto-registers fact measures as governed metrics.

---

### Relationships

#### `createRelationship(input: RelationshipInput!) → MutationResult`

Create a relationship. The gate checks the source table's domain (not the target's). A cross-domain edge is stored with `needsReview: true`. A caller lacking `create_relationship` is queued as a creation request. Junction (many-to-many) edges require matching key-list lengths. Refused by name, before anything is written: an id that exists (`schema.relationship_id_taken`); a table name that resolves to no registered table or to more than one (`schema.relationship_unknown_source`, `_target`, `_junction`, `schema.relationship_ambiguous_table`); a Cypher alias or a GraphQL alias another relationship of the same source table holds (`schema.relationship_alias_taken`, `schema.relationship_graphql_alias_taken`). A table is named by its virtual name: its alias when it has one.

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

Change an existing relationship, with the same checks as `createRelationship`. An id no relationship holds is refused (`schema.relationship_not_found`). A change needs the right itself; it is never queued as a request.

#### `deleteRelationship(id: String!) → MutationResult`

Delete a relationship by ID and rebuild schemas.

---

### Domains

#### `createDomain(input: DomainInput!) → MutationResult`

Create a domain. An id that exists is refused (`schema.domain_id_taken`). Reserved segment words (`tables`, `relationships`, and other URI path segments) are rejected, as is the wildcard literal `*`. Requires `org_settings` capability.

#### `deleteDomain(id: String!) → MutationResult`

Delete a domain. Requires `org_settings`.

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Set the global GraphQL naming convention and rebuild schemas for all roles. Only recognized convention names are accepted.

---

### Roles

#### `createRole(input: RoleInput!) → MutationResult`

Create a role with capabilities, domain access, optional rate limits, and an optional parent role. Validates that the parent exists and that the parent chain is cycle-free. After the definition checks, an id that exists is refused (`schema.role_id_taken`). Requires `user_management`.

#### `updateRole(input: RoleInput!) → MutationResult`

Change an existing role, with the same checks as `createRole`. An id no role holds is refused (`schema.role_not_found`). Redefining a role is an act in every domain it reaches.

#### `deleteRole(id: String!) → MutationResult`

Delete a role. Fails if other roles inherit from it; reparent those first. Requires `user_management`.

---

### RLS rules

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Create or update a row-level security rule. The filter expression is validated at save time against the target table's or domain's columns so a rule the admin cannot query is refused with the reason rather than silently failing at query time. Requires `masking_config`.

Targets are mutually exclusive: set `tableId` for a table-level rule, `domainId` for a domain-level rule, or `actionName` for a tracked function/webhook.

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Delete an RLS rule. Requires `masking_config`.

---

### Data products

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Create a data product. An id that exists is refused (`schema.data_product_id_taken`). Requires `data_product_rw` capability.

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

Change an existing data product. An id no product holds is refused (`schema.data_product_not_found`). Requires `data_product_rw`.

#### `deleteDataProduct(id: String!) → MutationResult`

Delete a data product. Requires `data_product_rw`.

---

### Tags

#### `createTag(input: TagInput!) → MutationResult`

Create a tag definition. An id that exists is refused (`schema.tag_id_taken`). System tags and derived tags cannot be redefined. `appliesTo` must be a non-empty subset of `["source", "table", "column", "relationship", "command"]`.

#### `updateTag(input: TagInput!) → MutationResult`

Change an existing tag definition, with the same checks as `createTag`. An id no tag holds is refused (`schema.tag_not_found`).

#### `deleteTag(id: String!) → MutationResult`

Delete a tag. Refuses system and derived tags.

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Assign a tag to a source, table, column, relationship, or command. Enforces the tag's field policies (`reason_policy`, `expires_policy`) and — for parameterized tags — validates the parameter value against the tag's permitted list.

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Remove a tag assignment.

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Add or re-describe a permitted parameter value for a parameterized tag. The permitted-values list is closed: every assignment must name a value from it.

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Remove a permitted value. Refused while any assignment still carries it, because those assignments would name a type the list no longer admits.

---

### Metrics

#### `createMetric(input: MetricInput!) → MutationResult`

Create a governed metric definition. A name that exists is refused (`schema.metric_name_taken`). The expression must parse under sqlglot and contain at least one aggregate function. Regenerates all metric-composed views that reference this metric. Requires `table_registration`.

#### `updateMetric(input: MetricInput!) → MutationResult`

Replace an existing metric's definition, with the same checks as `createMetric`. A name no metric holds is refused (`schema.metric_not_found`). Saving it needs the domains of the new expression and of the definition it replaces.

#### `deleteMetric(name: String!) → MutationResult`

Delete a governed metric. Rebuilds schemas. Requires `table_registration`.

---

### Calendars

#### `createCalendar(input: CalendarInput!) → MutationResult`

Create or replace a versioned snapshot-boundary calendar. Validated by constructing the in-memory `Calendar` before persisting. It fails on an unknown base system, bad time zone, or bad fiscal anchor.

#### `deleteCalendar(name: String!) → MutationResult`

Delete a calendar (all versions). Refused when any materialized view references it.

---

### Materialized views

#### `refreshMv(mvId: String!) → MutationResult`

Trigger a manual refresh of a materialized view. Coordinates across the fleet when the MV consistency mode is `shared`.

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Enable or disable a materialized view.

---

### Cache

#### `purgeCache → MutationResult`

Purge all cached query results.

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Purge cached results for one table.

---

### Scheduled tasks

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Create a scheduled trigger — either a webhook call or a SQL statement — and register it live in APScheduler. `kind` is `"webhook"` or `"sql"`.

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Remove a scheduled trigger from config and the live scheduler.

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Enable or disable a scheduled task in the config file.

---

### Data quality

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Run a contract against the live table and return outcomes without landing anything. A mutation rather than a query because it costs a real scan. What it proves is whether the dataset identifier resolves to the governed table the operator intends.

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Fire a checker table's poll job immediately. Lands rows the normal way, so results persist and the DQ history shows the new scan.

---

### Schema maintenance

#### `rebuildSchemas → MutationResult`

Rebuild the in-memory schema from database state. Useful after external database changes.

---

### Creation requests

#### `approveCreationRequest(requestId: Int!) → MutationResult`

Approve a creation request: the same rule, refusals and admin-trail entries as `POST /admin/creation-requests/{id}/approve`. The requester cannot approve their own request and one user's approval counts once. The approval that completes the request's count creates what it asks for; `params.status` is `executed` when this one did, otherwise `pending`, and `params.waitingOn` lists the domains a relationship request still waits on.

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Carries out a creation request that has had its approvals — relationship, view, table, source or webhook — through the code the direct mutation uses. Refused with `requests.approvals_incomplete` until the request's required approvals are met (one, or two for a relationship) by users other than the requester; a relationship request is also refused with `requests.waiting_on_domains` while a domain it touches has no approver. Approvals are given through `POST /admin/creation-requests/{id}/approve`, and the approval that completes the count performs the creation, so this mutation serves to retry a creation that failed. A failed creation leaves the request pending and answers with the failure's own code.

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Reject a queued request with an actionable reason. `reason` is required. A relationship request may be rejected by any user who could approve it. The author of a request of any other type may take it back; that is recorded on the admin trail as a withdrawal.

---

### Query compilation

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compile a data-plane GraphQL query against a role's schema and return the full routing decision: semantic SQL, engine SQL, direct SQL, route, enforcement metadata (RLS filters applied, columns excluded, masking applied), and compiled Cypher. Returns one result per root field in the query.

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

`CompileQueryInput` fields:

| Field | Type | Description |
|-------|------|-------------|
| `query` | `String!` | Data-plane GraphQL query to compile |
| `role` | `String!` | Role whose schema to compile against |
| `variables` | `JSON` | Variable bindings |
| `flatSql` | `Boolean` | Return a single flattened SQL string instead of a semantic/engine pair |
| `flatCypher` | `Boolean` | Flatten the Cypher output |
| `nodeOnlyCypher` | `Boolean` | Emit node-only Cypher (no edge patterns) |

---

## Key input types

### `SourceInput`

| Field | Type | Notes |
|-------|------|-------|
| `id` | `String!` | Source identifier |
| `type` | `String!` | Connector type (e.g. `postgres`, `files`, `openapi`) |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | Plaintext or `${secret:NAME}` reference |
| `path` | `String` | File-system path for file/CSV sources |
| `federationHintsJson` | `String` | JSON object for warehouse extras (Snowflake warehouse/role, Databricks http_path) |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` |
| `loadProtected` | `Boolean` | Scheduled-refresh-only |
| `offPeakWindow` | `String` | `HH:MM-HH:MM` maintenance window |
| `offPeakTz` | `String` | IANA time zone |
| `cdc` | `SourceCdcConfigInput` | Kafka CDC transport config, including how its schema registry is reached (see [Schema registry authentication](subscriptions.md#schema-registry-authentication)). `schemaRegistryPassword` and `schemaRegistryToken` are returned as vault references, never as credentials. |

### `TableInput`

The core table registration input. Key fields beyond the basics:

| Field | Notes |
|-------|-------|
| `materialize` | Land a copy in the materialization store |
| `mvRefreshInterval` | Seconds between refreshes |
| `mvPersist` | `replace` \| `append` \| `upsert` |
| `mvIncremental` | Incremental maintenance |
| `businessKey` | Columns that identify the same thing across versions; declared with `validTime` and `recordedTime`, all three or none |
| `validTime` | Column holding the date the business gives the record |
| `recordedTime` | Column holding when this version was entered or changed |
| `mvCalendar` | Snapshot calendar name |
| `mvGrain` | Snapshot grain: `daily`, `weekly`, `monthly`, `annual`, or custom `3WE` / `LFR` |
| `viewSql` | SQL for a derived view |
| `viewMetrics` | Declarative metric-composed view spec — mutually exclusive with `viewSql` |
| `dqContract` | YAML/JSON data quality contract text |
| `queryTemplate` | Cypher for a Neo4j table |
| `live` | Live delivery config for SSE/Kafka push |
| `discover` | Infer columns from the live source at registration time |

### `RelationshipInput`

| Field | Notes |
|-------|-------|
| `id` | Relationship identifier |
| `sourceTableId` | Virtual table name (alias if set, else table name) |
| `targetTableId` | Virtual table name; empty for computed relationships |
| `sourceColumn` | Join column on the source side |
| `targetColumn` | Join column on the target side |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Cypher edge label (e.g. `WORKS_FOR`) |
| `graphqlAlias` | GraphQL field name on the source type |
| `viaTable` | Junction table name for many-to-many edges |
| `recordCandidate` | Also write an `accepted` relationship_candidates row |

---

## Example: registering a table

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## Example: creating an RLS rule

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
