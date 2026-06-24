# Dataset Handling Standards

## Principles

1. **Schema = physical grouping from the source.** The schema name shown to users must be the name the data source itself uses to group datasets. For a relational database, that is the schema (or database in MySQL). For a flat/API source with no native grouping concept, use a fixed constant that names the source type. REQ-635

2. **Trino is preferred when a connector exists.** When a Trino connector is configured for a source type (i.e., the type is in `SOURCE_TO_CONNECTOR`), use Trino for schema and table introspection. REQ-636 Trino is the canonical query path; introspection via the same path ensures consistency. Native driver introspection is only used for source types with no Trino connector. REQ-636

3. **No intermediate layers exposed.** PG cache schemas, Trino catalog names, and other implementation-layer namespaces must never appear as schema or table names presented to the user. REQ-637

4. **Single endpoint per operation.** The UI calls one `availableSchemas` endpoint and one `availableTables` endpoint. REQ-638 Backend routing selects the correct introspection strategy per source type internally. REQ-636

5. **Dataset stewardship — one domain owner, one responsible party.** Registering a dataset is a governance act: the registering domain becomes the sole steward of that dataset and is accountable for its governance, access policy, quality, and lifecycle. (REQ-433) A datasource may serve multiple domains, but each physical dataset may be stewarded by exactly one domain. Once claimed, no other domain may register it. (REQ-433) Uniqueness of virtual names (alias or table_name) within each domain is enforced by Provisa. (REQ-432) Source-level uniqueness is inherent to the datasource. Together these guarantees eliminate the need for domain-prefixed aliases — table names are registered under their native source names. (REQ-432)

6. **All source types must be handled.** A source type not listed below must return `[]` for both schemas and tables — never `None`, which triggers a broken Trino fallback for non-Trino sources. REQ-639

---

## Source Type Reference

### Hierarchical sources — schema from Trino

These types are in `SOURCE_TO_CONNECTOR`. Trino is the preferred introspection path. (REQ-012) REQ-636

| Source type | Physical schema concept | Trino catalog schema query |
| --- | --- | --- |
| `postgresql` | Schema | `information_schema.schemata` (filter system schemas) |
| `mysql` / `mariadb` | Database | `information_schema.schemata` (filter system dbs) |
| `sqlserver` | Schema | `information_schema.schemata` (filter system schemas) |
| `duckdb` | Schema | `information_schema.schemata` |
| `snowflake` | Schema | `information_schema.schemata` |
| `bigquery` | Dataset | `information_schema.schemata` |
| `redshift` | Schema | `information_schema.schemata` |
| `databricks` | Schema | `information_schema.schemata` |
| `clickhouse` | Database | `information_schema.schemata` |
| `mongodb` | Database | `information_schema.schemata` |
| `cassandra` | Keyspace | `information_schema.schemata` |
| `iceberg` | Namespace | `information_schema.schemata` |
| `hive` / `hive_s3` | Database | `information_schema.schemata` |
| `delta_lake` | Schema | `information_schema.schemata` |

Tables: `information_schema.tables WHERE table_schema = ?`.

---

### Flat sources — fixed schema constant

These types use native source introspection directly rather than Trino for schema/table discovery.

| Source type | Fixed schema constant | Table discovery method | Native endpoint |
| --- | --- | --- | --- |
| `sqlite` | `"main"` | `sqlite_master` in the `.db` file | Local file via `sqlite3` |
| `openapi` | `"openapi"` | GET operations with non-null response schema | OpenAPI spec (stored in state) |
| `graphql_remote` | `"graphql"` | Query-type fields returning LIST | GraphQL HTTP introspection endpoint |
| `grpc_remote` | `"grpc"` | Server-streaming RPCs + non-streaming RPCs with repeated response | Remote gRPC server via proto reflection or proto file |
| `kafka` | `"kafka"` | Topics from `kafka_topics` table | Provisa config DB |
| `neo4j` | `"neo4j"` | No list introspection — user supplies a Cypher query that must return a JSON array | Bolt connection |
| `sparql` | `"sparql"` | No list introspection — user supplies a SPARQL query that must return a JSON array | SPARQL HTTP endpoint |
| `govdata` | Category names from `sources.database` | `fetch_tables` per category | GovData JDBC adapter |

For `graphql_remote`: introspection connects to the remote GraphQL HTTP endpoint directly. (REQ-307) The PG cache holds previously-executed query results only — it is not the source and must not be used for introspection. (REQ-309)

For `grpc_remote`: introspection connects to the remote gRPC server (via proto reflection or a proto file path/URL in `sources.path`). (REQ-322) PG is not the source.

For `neo4j`: datasets are user-supplied Cypher queries that must return a JSON array. (REQ-295) Neo4j node labels could theoretically be introspected but combining labels is common, so a fixed query model is used instead.

For `sparql`: datasets are user-supplied SPARQL queries that must return a JSON array. (REQ-297) No standard introspection path exists. The schema constant exists only to allow the UI form to complete.

---

## Introspection Routing

```text
native_schemas(source_id, source_type, pool, config_conn)
  ├─ source_type in SOURCE_TO_CONNECTOR → return None  (available_schemas falls through to Trino)
  ├─ "graphql_remote" → ["graphql"]
  ├─ "grpc_remote"    → ["grpc"]
  ├─ "kafka"          → ["kafka"]
  ├─ "sqlite"         → ["main"]
  ├─ "openapi"        → ["openapi"]
  ├─ "neo4j"          → ["neo4j"]
  ├─ "sparql"         → ["sparql"]
  ├─ "govdata"        → [categories from sources.database]
  └─ unknown          → []   (never None — avoids broken Trino fallback)

native_tables(source_id, source_type, schema_name, pool, config_conn, state)
  ├─ "openapi"         → parse spec, filter by schema_name == "openapi"
  ├─ "graphql_remote"  → HTTP introspect remote endpoint, filter list fields
  ├─ "grpc_remote"     → parse proto from remote endpoint, filter streaming/repeated methods
  ├─ "kafka"           → query kafka_topics where source_id = ?
  ├─ "sqlite"          → sqlite_master WHERE type='table', filter by schema_name == "main"
  ├─ "neo4j"           → [] (no introspection)
  ├─ "sparql"          → [] (no introspection)
  ├─ "govdata"         → fetch_tables per category
  ├─ source_type in SOURCE_TO_CONNECTOR → None  (available_tables falls through to Trino)
  └─ unknown           → []
```

---

## Name Normalization

Dataset names may be normalized to a semantic standard (e.g., `snake_case`) and may also carry an alias. Source-level naming conventions differ: OpenAPI uses camelCase operation IDs, SQL sources use whatever the DBA chose, GraphQL fields are camelCase, gRPC methods are PascalCase. (REQ-194)

**Never compare two dataset names with `===`.** Always normalize both sides through the centralized normalization function before comparison. REQ-640

- In the UI (`TablesPage.tsx`): use `toSnakeCase(a) === toSnakeCase(b)` via the shared `toSnakeCase` utility. REQ-640
- In the backend: use `provisa.core.naming.to_snake_case` (or equivalent canonical function). REQ-640

This applies everywhere a source-introspected name is compared against a stored registered name: `isRegistered` checks, alias lookups, duplicate detection, and any cross-source name matching. REQ-640

Inline assumptions about source casing (e.g., `name.toLowerCase()`, hardcoded `.replace(/-/g, "_")`) are forbidden — they are invisible and diverge from the canonical logic.

---

## Dataset Identity

Every registered dataset has two forms of identity:

- **Physical identity**: the native name as returned by the source (`findPetsByStatus`, `orders`, `pets` field in GraphQL, `StreamPets` gRPC method). This is what the query engine uses. REQ-641
- **Semantic identity**: `domainId + tableName` (the alias, normalized). This is what the API, GraphQL schema, and end users see. (REQ-155, REQ-194)

A reference to a dataset may use either form. Any code that needs to determine whether two references point to the same dataset must resolve both through a centralized identity service — never by ad-hoc string comparison. REQ-641

### Resolution rules

1. **Physical → semantic**: look up `registered_tables` by `(sourceId, normalizedPhysicalName)` → returns `(domainId, tableName)`. REQ-641
2. **Semantic → physical**: look up `registered_tables` by `(domainId, tableName)` → returns `(sourceId, physicalName)`. REQ-641
3. **Same-form comparison**: normalize both names with `toSnakeCase` before comparing. Two normalized names that are equal refer to the same dataset within the same source and schema context. REQ-640
4. **Cross-form comparison**: resolve both sides to their physical `(sourceId, normalizedName)` tuple first, then compare tuples. REQ-641

### Query-language representations

A dataset may also be referenced by name inside a query payload. Each query language has a canonical naming convention for dataset names: (REQ-194)

| Language | Canonical form | Example |
| --- | --- | --- |
| SQL | snake_case, qualified as `schema.table` | `pet_store.pet_by_status` |
| Cypher | PascalCase node label or relationship type | `PetByStatus` |
| GraphQL | camelCase field name | `petByStatus` |

The primary control over query-language naming is the **alias** set at registration time (`registered_tables.alias`). (REQ-155) When an alias is present it is the authoritative name for all query languages — it is not a hint, it is the name. (REQ-194) The naming convention (`snake_case`, `camelCase`, `PascalCase`, `none`, `inherit`) is then applied to the alias (or to `tableName` when no alias is set) to produce the correct cased form for each language. (REQ-194, REQ-416)

Resolution priority for the query-language name of a dataset:

1. `alias` (if set) → apply active naming convention per language (REQ-155, REQ-194)
2. `tableName` (normalized) → apply active naming convention per language (REQ-194)

Any name extracted from a SQL, Cypher, or GraphQL query must be resolvable to a physical dataset through the same centralized identity service. Resolution de-normalizes from query-language form → checks alias match first → falls back to tableName match → returns `(sourceId, physicalName)`. REQ-641

### Implementation contract

- Backend: `provisa.core.dataset_identity` module must expose:
  - `resolve(ref, lang=None) -> DatasetIdentity` — accepts physical name, semantic `domain.table`, or query-language name (with `lang` in `{"sql", "cypher", "graphql"}`).
  - `same_dataset(ref_a, ref_b) -> bool` — resolves both sides before comparing.
  - `to_query_name(identity, lang) -> str` — applies the active naming convention to produce the correct form for a given query language.
- Frontend: shared utility in `provisa-ui/src/utils/datasetIdentity.ts` — no per-component name-matching logic.
- All inline normalization (`_normalize_op_id`, ad-hoc `toSnakeCase` comparisons, etc.) must be replaced by calls to these utilities.
- The service reads the active naming convention from `registered_tables.naming_convention` → `sources.naming_convention` → global default, in that priority order. (REQ-194, REQ-416)
