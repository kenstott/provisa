# Admin GraphQL API Reference

The admin GraphQL API is Provisa's configuration plane. It is the API the admin web app calls for every management operation — creating sources, registering tables, defining relationships, configuring RLS rules, and everything else that shapes the model.

**Mount point:** `POST /admin/graphql`

This is not the same API as the data plane at `/data/graphql`. The data plane serves end-user queries over registered domains and is described by the SDL at `/data/sdl`. The admin API configures what that schema looks like and who may see what.

---

## How the UI talks to this API

The admin web app uses Apollo Client, pointed at `${API_BASE}/admin/graphql`. [tool-verified: `provisa-ui/src/apolloClient.ts:19`]

Every request carries a bearer token (fetched fresh from the auth provider on each call), an `X-Org-Id` header when multi-tenant, and an `X-Env` header when serving a branch environment. [tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

The schema is assembled from two `@strawberry.type` classes — `Query` from `schema_query.py` and `Mutation` from `schema_mutation.py` — and wrapped in a `ModelCommitExtension` that records every mutation against the current environment branch (REQ-1524). [tool-verified: `provisa/api/admin/schema.py:44`]

---

## Authorization

**Dev mode:** when no auth is configured and every request arrives as an anonymous principal, all capability checks are skipped. This keeps a local install functional without auth setup. [tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**Capability gates:** production deployments enforce named capabilities. The specific right required by each field is noted inline. Calling a mutation without the required capability raises a `PermissionError`. The platform administrator role bypasses all capability checks (REQ-1297). [tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**Domain gates:** several mutations also check the domain the object belongs to. A caller scoped to `sales` cannot register a table into `finance`, queue an RLS rule for it, or create a relationship whose source table lives in a domain they do not hold (REQ-1530, REQ-1531). Views are further constrained: every table the view SQL reads must be within the caller's domains, because free-hand SQL otherwise gives a member access to data outside their scope. [tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**Role inheritance:** a parent role's capabilities are inherited by child roles (REQ-1677). `createRole` and `deleteRole` refuse cycles and prevent deleting a role that has heirs.

---

## Common return type

Most mutations return `MutationResult`. [tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

When a mutation fails, `success` is `false` and `message` carries the English reason. `code` is a stable identifier the UI uses to render a localized message.

---

## Queries

### Sources

#### `sources → [SourceType!]!`

All registered data sources. [tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` is a `${secret:NAME}` reference into the org vault — never the literal credential. [tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

A single source by ID. Returns `null` when not found. [tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

Schemas visible in a source, filtered to exclude Provisa-internal ones. Uses native introspection first; falls through to the engine catalog when the source type has no direct pool. [tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tables in one schema of a source, with their comments. For OpenAPI sources, returns GET operations whose response is an array or pagination wrapper. For GraphQL sources, returns query fields returning a list. For gRPC, returns server-streaming RPCs. [tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Column names for a table in the engine catalog. For govdata sources, uses a separate resolver. [tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Column names with data types, comments, native filter types, and primary-key flags. For OpenAPI sources, derives the shape from the operation's response schema and parameters. [tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Non-GET operations for an OpenAPI source (POST, PUT, PATCH, DELETE). Returns an empty list for non-OpenAPI sources. [tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Preview what a file-connector crawl would discover — files, tables, and columns — before a source is created. The HTTP-only settings (`simpleLinks`, `sameDomain`, `excludePattern`) are ignored for local, S3, FTP, and SFTP roots. (REQ-1785) [tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Returns the alias to use when registering `tableName` in `domainId` from `sourceId`. Returns a plain snake-case alias when no conflict exists, or a source-prefixed alias (`sqlite_b_orders`) when the effective name is already taken by a different source in the same domain. [tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### Tables

#### `tables → [RegisteredTableType!]!`

All registered tables, each with their full column list. Column visibility in the response respects the caller's `table_registration` capability — `canDeployToDb` is gated on whether the caller holds that right. (REQ-016, REQ-021, REQ-042) [tool-verified: `provisa/api/admin/schema_query.py:502-536`]

Each `RegisteredTableType` exposes computed sub-fields:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — the effective refresh/serving policy as plain text, derived server-side from the same planner resolution the engine uses. Returns `null` during startup. (REQ-1143) [tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** — the field name this table has in the compiled data-plane schema, so the Data Product panel can build a runnable example without replicating the naming algorithm. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** — this table as a data-quality contract dataset, in the form the checker scans. (REQ-1443) [tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** — the data product this table belongs to. A DQ-checker table inherits the product of the table its contract scans. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Preview the effective refresh/serving summary for *draft* (unsaved) table knobs, so the top-of-form summary updates as fields change without persisting anything. Same derivation as `refreshPolicySummary` above. (REQ-1143) [tool-verified: `provisa/api/admin/schema_query.py:947-979`]

Arguments: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `preferMaterialized`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artifacts that a pending alias rename or column drop would break. Advisory — the admin UI shows this before saving and the administrator decides. Must be called *before* saving, because the dependents were authored against the exposed name the column currently carries. (REQ-1484) [tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### Relationships

#### `relationships → [RelationshipType!]!`

All user-defined relationships (excludes auto-generated `gql_auto__` entries and the synthetic `meta:%` entries used by the ERD). [tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

Same as `relationships`, but includes `meta:%` synthetic entries. Used by the graph ERD, which needs to show every edge including the implicit `HAS_TABLE` links between data tables and the metadata registry. [tool-verified: `provisa/api/admin/schema_query.py:572-601`]

Each `RelationshipType` exposes:

- **`autoSuggested → Boolean`** — whether the relationship was suggested by FK analysis (`id` starts with `fk__`). [tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** — the relationship's name on the SQL and gRPC planes (the `?include=` parameter). Derived server-side; clients must not transliterate the GraphQL alias. (REQ-471, REQ-1417) [tool-verified: `provisa/api/admin/types.py:527-536`]

---

### Domains, roles & users

#### `domains → [DomainType!]!`

All domains in the active org's tenant database. The tenant database is isolated at the schema level, so an org-admin's domain list contains only their org's rows. (REQ-021, REQ-042, REQ-1293) [tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

Roles visible to the caller. An admin sees every role; a non-admin sees only roles with no `org_id` or roles belonging to their org. (REQ-042, REQ-059, REQ-060, REQ-215) [tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Resolve role IDs or user IDs to individual users. Used to expand `DataProduct.ownerRole`, `Domain.steward`, and `Column.visibleTo` into a human-readable list. Unknown refs are echoed back bare so the UI shows the raw ID rather than nothing. [tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### RLS rules

#### `rlsRules → [RLSRuleType!]!`

All row-level security rules. The underlying repository decrypts `filterExpr` at the boundary. (REQ-041, REQ-402, REQ-686) [tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### Data products

#### `dataProducts → [DataProductType!]!`

All data products. Requires `data_product_read` capability. (REQ-1634) [tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### Tags

#### `tags → [TagType!]!`

All tag definitions, including their per-tag permitted parameter values. (REQ-1373, REQ-1467) [tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

All tag assignments across sources, tables, columns, and relationships. (REQ-1377) [tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### Materialized views

#### `mvList → [MVType!]!`

All materialized views with their runtime status: enabled/disabled, last refresh timestamp, row count, and last error. [tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### Cache

#### `cacheStats → CacheStatsType`

Cache statistics. Returns `storeType: "redis"` with full operational metrics when Redis is configured, `storeType: "memory"` for the embedded fakeredis store, and `storeType: "noop"` when no cache is configured. [tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

Per-table cached entry counts. Empty when no cache store is configured. [tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Tables Provisa is keeping a copy of, across two tiers: `hot` (mirrored into the response store for JOIN inlining) and `warm` (landed as an Iceberg copy). A table is in at most one tier (REQ-241). [tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identity of the durable materialization store: engine name, store DSN reference, MV count, and whether the store is instance-local (a local file like DuckDB or SQLite, which means each instance behind a load balancer keeps its own copy). [tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### System health

#### `systemHealth → SystemHealthType`

Engine connection status, worker pool counts, metadata DB pool state, cache mode, and liveness of every protocol listener (pgwire, gRPC, Arrow Flight, Bolt). [tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

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

Scheduled triggers from config with runtime state. Each entry carries its cron expression, `kind` (`webhook` or `sql`), whether it is currently enabled, the last run timestamp (always `null` in this release — tracked by the scheduler), and the next scheduled run time from APScheduler. [tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### Data quality

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Parse raw contract text into the builder panel's editable rows. Called on every edit; a parse failure comes back as `error` rather than as a GraphQL error, because half-written text is normal while the operator is typing. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

The checks `checker` offers, scoped to the columns of `dataset`. The dataset is the contract's observed target, resolved the same way the scanner resolves it — so the offered checks match the columns the checker will really see. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

One check's text from the panel's editors. Server-side because the dialect has one implementation; a builder-made check and a hand-typed one must be indistinguishable. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serialize edited check rows back into contract text. The inverse of `dqContractParse`. Server-side for the same reason: the panel cannot emit text the checker would refuse. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### Source previews

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Preview a Cypher projection on a Neo4j source: up to five rows and the column types the registration will carry. Failures come back as `error`. (REQ-1670) [tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Preview a SPARQL SELECT on a SPARQL source: up to five rows, all columns as text. (REQ-1683) [tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Live check against the Kaggle API. Returns `true` only when the token authenticates. Backs the token-gate step in the Kaggle source form. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Search Kaggle's full public dataset catalog. Token-gated. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### Calendars

#### `calendars → [CalendarType!]!`

All registered snapshot-boundary calendar versions. Feeds the snapshot-schedule config picker and confirms which calendars a periodic MV may reference. (REQ-962) [tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### Metrics

#### `metrics → [MetricType!]!`

All governed metric definitions. Fact-derived metrics carry `fromFact`. (REQ-1317, REQ-1320) [tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### Schema version

#### `schemaVersion → String!`

SHA-256 hash of the current schema state (domains, table IDs, relationship IDs). The Apollo client reads this from the `X-Schema-Version` response header and refetches all active queries when it advances. [tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### AI helpers

#### `generateTableDescription(tableId: String!) → String!`

Use the configured LLM to generate a one-to-two sentence description for a registered table. Save the table first; calling this on an unsaved table returns an instructional message. [tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Use the configured LLM to generate a one-sentence description for a single column. [tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### Creation requests

#### `creationRequests → [CreationRequestType!]!`

Pending creation requests, visible to callers holding the relevant create capability. Used when a member without `create_relationship` or `create_view` submits a request that a rights-holder must approve. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## Mutations

### Sources

#### `createSource(input: SourceInput!) → MutationResult`

Register a new data source. Validates the connection before persisting — a rejected source leaves no vault entry behind. Stores credentials in the org vault and records the reference; the plaintext never lands in the database. (REQ-012, REQ-013) Requires `source_registration` capability. [tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

Update an existing source's connection details, description, and config. Tears down and re-attaches the pgwire endpoint for file/SharePoint sources so a path change takes effect immediately. Requires `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

Remove a source and its vault entry. Drops the engine catalog and rebuilds schemas. [tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Rename a source ID. [tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Enable or disable query result caching for a source, and set the TTL in seconds. [tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

Force (or release) materialized federation for all tables on a source. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Mark a source load-protected (scheduled-refresh-only). Requires at least one gate — off-peak window, cache TTL cadence, or a probing change signal — or the call fails. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Set the per-source GraphQL naming convention. [tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Set which domains may use a source (empty list = unrestricted). Requires `source_registration`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Download and unzip a Kaggle dataset onto local disk. Returns the staged directory path; the caller then creates one `files`-type source pointing at it. SQLite-bearing bundles are rejected whole. Requires `source_registration`. (REQ-1780–1782) [tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Re-fetch a Kaggle-derived source's dataset in place. Skips the download if Kaggle has nothing newer than what is on disk. Requires `source_registration`. (REQ-1787) [tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Run `ANALYZE` on all registered tables for a source. Improves join-order and broadcast decisions for federated queries. (REQ-276) [tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### Tables

#### `registerTable(input: TableInput!) → MutationResult`

Register a new table (or view) into a domain. Requires `table_registration` capability and membership in the target domain. A caller lacking `create_relationship` who submits a view is queued as a creation request for a rights-holder to approve. (REQ-013, REQ-016, REQ-252, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

Update an existing table's alias, description, column metadata, MV settings, and live-delivery config. (REQ-016, REQ-020) Requires `table_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

Delete a registered table. Looks up the table's domain for the domain gate before deleting. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Override the cache TTL for one table. [tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

Override materialized federation for one table. `null` = inherit the source default. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Override load protection for one table. `null` for `loadProtected` inherits the source default. Validates the effective (table → source) gate combination. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Set the per-table GraphQL naming convention. [tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promote a virtual Provisa view to a real database view on its underlying native source. [tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Recompute a table's landed rows on demand, bypassing the normal change gate. `reason` is a required audit annotation. Refused for live-federated tables (no landed rows). (REQ-968) [tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Force a SQLite file-connector table's next access to re-sync from disk. [tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

Sugar for registering a dimension/hub entity. Lowers to a (bitemporal, when historized) MV and calls `registerTable`. (REQ-1164) [tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

Sugar for registering a star-schema fact. Lowers to an aggregate MV, creates dimension relationships, and auto-registers fact measures as governed metrics. (REQ-1164, REQ-1320) [tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### Relationships

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

Create or update a relationship. The gate checks the source table's domain (not the target's). A cross-domain edge is stored with `needsReview: true`. A caller lacking `create_relationship` is queued as a creation request. Junction (many-to-many) edges require matching key-list lengths. (REQ-019, REQ-020, REQ-366, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

Delete a relationship by ID and rebuild schemas. [tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### Domains

#### `createDomain(input: DomainInput!) → MutationResult`

Create a domain. Reserved segment words (`tables`, `relationships`, and other URI path segments) are rejected, as is the wildcard literal `*`. Requires `org_settings` capability. (REQ-021, REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

Delete a domain. Requires `org_settings`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Set the global GraphQL naming convention and rebuild schemas for all roles. Only recognized convention names are accepted. (REQ-253, REQ-416) [tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### Roles

#### `createRole(input: RoleInput!) → MutationResult`

Create or replace a role with capabilities, domain access, optional rate limits, and an optional parent role. Validates that the parent exists and that the parent chain is cycle-free. Requires `user_management`. (REQ-042, REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

Delete a role. Fails if other roles inherit from it — reparent those first. Requires `user_management`. (REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### RLS rules

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Create or update a row-level security rule. The filter expression is validated at save time against the target table's or domain's columns so a rule the admin cannot query is refused with the reason rather than silently failing at query time. Requires `masking_config`. (REQ-041, REQ-402, REQ-1531, REQ-1676) [tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

Targets are mutually exclusive: set `tableId` for a table-level rule, `domainId` for a domain-level rule, or `actionName` for a tracked function/webhook. (REQ-1679)

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Delete an RLS rule. Requires `masking_config`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### Data products

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Create or replace a data product. Requires `data_product_rw` capability. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

Delete a data product. Requires `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### Tags

#### `upsertTag(input: TagInput!) → MutationResult`

Create or update a tag definition. System tags and derived tags cannot be redefined. `appliesTo` must be a non-empty subset of `["source", "table", "column", "relationship", "command"]`. (REQ-1373, REQ-1375) [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

Delete a tag. Refuses system and derived tags. (REQ-1373) [tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Assign a tag to a source, table, column, relationship, or command. Enforces the tag's field policies (`reason_policy`, `expires_policy`) and — for parameterized tags — validates the parameter value against the tag's permitted list. (REQ-1376, REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Remove a tag assignment. (REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Add or re-describe a permitted parameter value for a parameterized tag. The permitted-values list is closed: every assignment must name a value from it. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Remove a permitted value. Refused while any assignment still carries it, because those assignments would name a type the list no longer admits. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### Metrics

#### `upsertMetric(input: MetricInput!) → MutationResult`

Create or replace a governed metric definition. The expression must parse under sqlglot and contain at least one aggregate function. Regenerates all metric-composed views that reference this metric. Requires `table_registration`. (REQ-1317, REQ-1318) [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

Delete a governed metric. Rebuilds schemas. Requires `table_registration`. (REQ-1317) [tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### Calendars

#### `createCalendar(input: CalendarInput!) → MutationResult`

Create or replace a versioned snapshot-boundary calendar. Validated by constructing the in-memory `Calendar` before persisting — fails on an unknown base system, bad time zone, or bad fiscal anchor. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

Delete a calendar (all versions). Refused when any materialized view references it. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### Materialized views

#### `refreshMv(mvId: String!) → MutationResult`

Trigger a manual refresh of a materialized view. Coordinates across the fleet when the MV consistency mode is `shared`. (REQ-133, REQ-158, REQ-879) [tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Enable or disable a materialized view. [tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### Cache

#### `purgeCache → MutationResult`

Purge all cached query results. [tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Purge cached results for one table. [tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### Scheduled tasks

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Create a scheduled trigger — either a webhook call or a SQL statement — and register it live in APScheduler. `kind` is `"webhook"` or `"sql"`. (REQ-1003, REQ-1004) [tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Remove a scheduled trigger from config and the live scheduler. (REQ-1003) [tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Enable or disable a scheduled task in the config file. [tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### Data quality

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Run a contract against the live table and return outcomes without landing anything. A mutation rather than a query because it costs a real scan. What it proves is whether the dataset identifier resolves to the governed table the operator intends. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Fire a checker table's poll job immediately. Lands rows the normal way, so results persist and the DQ history shows the new scan. (REQ-1443) [tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### Schema maintenance

#### `rebuildSchemas → MutationResult`

Rebuild the in-memory schema from database state. Useful after external database changes. [tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### Creation requests

#### `executeCreationRequest(requestId: Int!) → MutationResult`

A rights-holder executes a queued creation request — relationship, view, or webhook. Requires the capability the request is waiting on. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Reject a queued request with an actionable reason. `reason` is required. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### Query compilation

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compile a data-plane GraphQL query against a role's schema and return the full routing decision: semantic SQL, engine SQL, direct SQL, route, enforcement metadata (RLS filters applied, columns excluded, masking applied), and compiled Cypher. Returns one result per root field in the query. (REQ-161) [tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

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

[tool-verified: `provisa/api/admin/types.py:590-611`]

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
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` (REQ-929) |
| `loadProtected` | `Boolean` | Scheduled-refresh-only (REQ-1141) |
| `offPeakWindow` | `String` | `HH:MM-HH:MM` maintenance window |
| `offPeakTz` | `String` | IANA time zone |
| `cdc` | `SourceCdcConfigInput` | Kafka CDC transport config (REQ-824) |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

The core table registration input. Key fields beyond the basics:

| Field | Notes |
|-------|-------|
| `materialize` | Land a copy in the materialization store |
| `mvRefreshInterval` | Seconds between refreshes |
| `mvPersist` | `replace` \| `append` \| `upsert` (REQ-965) |
| `mvIncremental` | Incremental maintenance (REQ-969) |
| `mvBitemporalMode` | `snapshot` \| `delta` for bitemporal tables (REQ-1162) |
| `mvCalendar` | Snapshot calendar name (REQ-962) |
| `mvGrain` | Snapshot grain: `daily`, `weekly`, `monthly`, `annual`, or custom `3WE` / `LFR` (REQ-962) |
| `viewSql` | SQL for a derived view |
| `viewMetrics` | Declarative metric-composed view spec — mutually exclusive with `viewSql` (REQ-1318) |
| `dqContract` | YAML/JSON data quality contract text (REQ-1443) |
| `queryTemplate` | Cypher for a Neo4j table (REQ-1670) |
| `live` | Live delivery config for SSE/Kafka push (REQ-565, REQ-813) |
| `discover` | Infer columns from the live source at registration time (REQ-252) |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

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
| `viaTable` | Junction table name for many-to-many edges (REQ-1586) |
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
