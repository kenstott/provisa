# Dataset Handling Standards

## Principles

1. **Schema = physical grouping from the source.** The schema name shown to users must be the name the data source itself uses to group datasets. For a relational database, that is the schema (or database in MySQL). For a flat/API source with no native grouping concept, use a fixed constant that names the source type.

2. **Trino is preferred when a connector exists.** When a Trino connector is configured for a source type (i.e., the type is in `SOURCE_TO_CONNECTOR`), use Trino for schema and table introspection. Trino is the canonical query path; introspection via the same path ensures consistency. Native driver introspection is only used for source types with no Trino connector.

3. **No intermediate layers exposed.** PG cache schemas, Trino catalog names, and other implementation-layer namespaces must never appear as schema or table names presented to the user.

4. **Single endpoint per operation.** The UI calls one `availableSchemas` endpoint and one `availableTables` endpoint. Backend routing selects the correct introspection strategy per source type internally.

5. **Dataset stewardship — one domain owner, one responsible party.** Registering a dataset is a governance act: the registering domain becomes the sole steward of that dataset and is accountable for its governance, access policy, quality, and lifecycle. A datasource may serve multiple domains, but each physical dataset may be stewarded by exactly one domain. Once claimed, no other domain may register it. Uniqueness of virtual names (alias or table_name) within each domain is enforced by Provisa. Source-level uniqueness is inherent to the datasource. Together these guarantees eliminate the need for domain-prefixed aliases — table names are registered under their native source names.

6. **All source types must be handled.** A source type not listed below must return `[]` for both schemas and tables — never `None`, which triggers a broken Trino fallback for non-Trino sources.

---

## Source Type Reference

### Direct-route RDBMS — Trino connector + dialect

These types are in both `SOURCE_TO_CONNECTOR` and `SOURCE_TO_DIALECT`. They support direct-route execution (single-source queries bypass Trino) and federated execution.

| Source type | Trino connector | dialect | Physical schema concept | Trino catalog schema query |
| --- | --- | --- | --- | --- |
| `postgresql` | `postgresql` | `postgres` | Schema | `information_schema.schemata` (filter system schemas) |
| `mysql` | `mysql` | `mysql` | Database | `information_schema.schemata` (filter system dbs) |
| `mariadb` | `mariadb` | `mysql` | Database | `information_schema.schemata` (filter system dbs) |
| `singlestore` | `singlestore` | `singlestore` | Database | `information_schema.schemata` |
| `sqlserver` | `sqlserver` | `tsql` | Schema | `information_schema.schemata` (filter system schemas) |
| `oracle` | `oracle` | `oracle` | Schema | `information_schema.schemata` |
| `duckdb` | `memory` | `duckdb` | Schema | `information_schema.schemata` |
| `snowflake` | `snowflake` | `snowflake` | Schema | `information_schema.schemata` |
| `bigquery` | `bigquery` | `bigquery` | Dataset | `information_schema.schemata` |
| `clickhouse` | `clickhouse` | `clickhouse` | Database | `information_schema.schemata` |
| `redshift` | `redshift` | `redshift` | Schema | `information_schema.schemata` |
| `databricks` | `delta_lake` | `databricks` | Schema | `information_schema.schemata` |
| `hive` | `hive` | `hive` | Database | `information_schema.schemata` |
| `druid` | `druid` | `druid` | Schema | `information_schema.schemata` |
| `exasol` | `exasol` | `exasol` | Schema | `information_schema.schemata` |

Tables: `information_schema.tables WHERE table_schema = ?`.

---

### TRINO_ONLY lake sources — Trino connector, no direct driver

These types are in `SOURCE_TO_CONNECTOR` and `TRINO_ONLY_SOURCES`. All queries route through Trino. No dialect; no direct driver. Time-travel (`as_of`) is supported on `iceberg` and `delta_lake` (REQ-372).

| Source type | Trino connector | Physical schema concept | Trino catalog schema query |
| --- | --- | --- | --- |
| `iceberg` | `iceberg` | Namespace | `information_schema.schemata` |
| `hive_s3` | `hive` | Database | `information_schema.schemata` |
| `delta_lake` | `delta_lake` | Schema | `information_schema.schemata` |

Tables: `information_schema.tables WHERE table_schema = ?`.

---

### NoSQL / non-relational — Trino connector, no direct driver

These types are in `SOURCE_TO_CONNECTOR` but have no dialect and no direct driver. All queries route through Trino using the mapping DSL (REQ-251). Introspection falls back to Trino.

| Source type | Trino connector | Physical schema concept | Trino catalog schema query |
| --- | --- | --- | --- |
| `mongodb` | `mongodb` | Database | `information_schema.schemata` |
| `cassandra` | `cassandra` | Keyspace | `information_schema.schemata` |
| `redis` | `redis` | Key-pattern namespace | `information_schema.schemata` |
| `elasticsearch` | `elasticsearch` | Index | `information_schema.schemata` |
| `prometheus` | `prometheus` | Metric namespace | `information_schema.schemata` |

Tables: `information_schema.tables WHERE table_schema = ?`.

---

### Calcite-based connectors — Trino connector via Apache Calcite

These types are in `SOURCE_TO_CONNECTOR` using the `kenstott/calcite` Trino plugin. All queries route through Trino.

| Source type | Trino connector | Physical schema concept | Trino catalog schema query |
| --- | --- | --- | --- |
| `sharepoint` | `sharepoint` | Site/list hierarchy | `information_schema.schemata` |
| `splunk` | `splunk` | Splunk index | `information_schema.schemata` |
| `files` | `file` | Directory/path | `information_schema.schemata` |

Tables: `information_schema.tables WHERE table_schema = ?`.

---

### Flat sources — fixed schema constant, native introspection

These types have no Trino connector. `native_schemas` returns a fixed constant; `native_tables` queries the source directly.

| Source type | Fixed schema constant | Table discovery method | Native endpoint |
| --- | --- | --- | --- |
| `kafka` | `"kafka"` | Topics from `kafka_topics` config table | Provisa config DB |
| `openapi` | `"openapi"` | GET operations with non-null response schema | OpenAPI spec (stored in state) |
| `graphql_remote` | `"graphql"` | Query-type fields returning LIST | GraphQL HTTP introspection endpoint |
| `grpc_remote` | `"grpc"` | Server-streaming RPCs + non-streaming RPCs with repeated response | Remote gRPC server via proto reflection or proto file |
| `sqlite` | `"main"` | `sqlite_master WHERE type='table'` | Local file via `sqlite3` |
| `neo4j` | `"neo4j"` | No list introspection — user supplies a Cypher query | Bolt connection |
| `sparql` | `"sparql"` | No list introspection — user supplies a SPARQL query | SPARQL HTTP endpoint |
| `govdata` | Category names from `sources.database` | `fetch_tables` per category | GovData JDBC adapter |

Note: `kafka` is in `SOURCE_TO_CONNECTOR` (Trino `kafka` connector used for execution) but uses native config-DB introspection for schema/table discovery — it does not use Trino for introspection.

For `graphql_remote`: introspection connects to the remote GraphQL HTTP endpoint directly. The PG cache holds previously-executed query results only — it is not the source and must not be used for introspection.

For `grpc_remote`: introspection connects to the remote gRPC server (via proto reflection or a proto file path/URL in `sources.path`). PG is not the source.

For `neo4j`: datasets are user-supplied Cypher queries that must return a JSON array. Neo4j node labels could theoretically be introspected but combining labels is common, so a fixed query model is used instead.

For `sparql`: datasets are user-supplied SPARQL queries that must return a JSON array. No standard introspection path exists. The schema constant exists only to allow the UI form to complete.

---

### Event / embedded / file sources — no introspection (return `[]`)

These types have no Trino connector and no native introspection path. Both `available_schemas` and `available_tables` return `[]`. Table definitions are supplied by config, user query, or file path.

| Source type | Description |
| --- | --- |
| `websocket` | External WebSocket feed — schema is event-driven; no introspection |
| `rss` | RSS 2.0 / Atom feed — schema fixed to feed fields; no introspection |
| `csv` | Local or remote CSV file — schema from file headers at registration |
| `parquet` | Local or remote Parquet file — schema from file metadata at registration |
| `google_sheets` | Google Sheets API — schema from spreadsheet columns at registration |
| `ingest` | HTTP push receiver — schema defined in config at registration (REQ-333) |
| `pinot` | Apache Pinot — Trino `pinot` connector (`pinot.controller-urls`); introspects live via Trino (REQ-1097) |

---

## Introspection Routing

```text
native_schemas(source_id, source_type, pool, config_conn)
  ├─ "graphql_remote"  → ["graphql"]
  ├─ "grpc_remote"     → ["grpc"]
  ├─ "kafka"           → ["kafka"]
  ├─ "neo4j"           → ["neo4j"]
  ├─ "sparql"          → ["sparql"]
  ├─ "openapi"         → ["openapi"]
  ├─ "sqlite"          → ["main"]
  ├─ "govdata"         → [categories from sources.database]
  ├─ source_type in SOURCE_TO_CONNECTOR → None  (available_schemas falls through to Trino)
  └─ unknown           → []   (never None — avoids broken Trino fallback)

native_tables(source_id, source_type, schema_name, pool, config_conn, state)
  ├─ "openapi"         → parse spec, filter by schema_name == "openapi"
  ├─ "graphql_remote"  → HTTP introspect remote endpoint, filter list fields
  ├─ "grpc_remote"     → parse proto from remote endpoint, filter streaming/repeated methods
  ├─ "kafka"           → query kafka_topics where source_id = ?
  ├─ "neo4j"           → []  (no introspection)
  ├─ "sparql"          → []  (no introspection)
  ├─ "sqlite"          → sqlite_master WHERE type='table', filter by schema_name == "main"
  ├─ "govdata"         → fetch_tables per category
  ├─ source_type in SOURCE_TO_CONNECTOR → None  (available_tables falls through to Trino)
  └─ unknown           → []
```

---

## Name Normalization

Dataset names may be normalized to a semantic standard (e.g., `snake_case`) and may also carry an alias. Source-level naming conventions differ: OpenAPI uses camelCase operation IDs, SQL sources use whatever the DBA chose, GraphQL fields are camelCase, gRPC methods are PascalCase.

**Never compare two dataset names with `===`.** Always normalize both sides through the centralized normalization function before comparison.

- In the UI: use `toSnakeCase(a) === toSnakeCase(b)` via the shared utility exported from `provisa-ui/src/naming.ts`.
- In the backend: use the canonical function in `provisa.compiler.naming` (REQ-640).

This applies everywhere a source-introspected name is compared against a stored registered name: `isRegistered` checks, alias lookups, duplicate detection, and any cross-source name matching.

Inline assumptions about source casing (e.g., `name.toLowerCase()`, hardcoded `.replace(/-/g, "_")`) are forbidden — they are invisible and diverge from the canonical logic.

---

## Dataset Identity

Every registered dataset has two forms of identity:

- **Physical identity**: the native name as returned by the source (`findPetsByStatus`, `orders`, `pets` field in GraphQL, `StreamPets` gRPC method). This is what the query engine uses.
- **Semantic identity**: `domainId + tableName` (the alias, normalized). This is what the API, GraphQL schema, and end users see.

A reference to a dataset may use either form. Any code that needs to determine whether two references point to the same dataset must resolve both through a centralized identity service — never by ad-hoc string comparison.

### Resolution rules

1. **Physical → semantic**: look up `registered_tables` by `(sourceId, normalizedPhysicalName)` → returns `(domainId, tableName)`.
2. **Semantic → physical**: look up `registered_tables` by `(domainId, tableName)` → returns `(sourceId, physicalName)`.
3. **Same-form comparison**: normalize both names with `toSnakeCase` before comparing. Two normalized names that are equal refer to the same dataset within the same source and schema context.
4. **Cross-form comparison**: resolve both sides to their physical `(sourceId, normalizedName)` tuple first, then compare tuples.

### Query-language representations

A dataset may also be referenced by name inside a query payload. Each query language has a canonical naming convention for dataset names:

| Language | Canonical form | Example |
| --- | --- | --- |
| SQL | snake_case, qualified as `schema.table` | `pet_store.pet_by_status` |
| Cypher | PascalCase node label or relationship type | `PetByStatus` |
| GraphQL | camelCase field name | `petByStatus` |

The primary control over query-language naming is the **alias** set at registration time (`registered_tables.alias`). When an alias is present it is the authoritative name — it is not a hint, it is the name. The alias is consumed differently per language: **SQL uses the alias verbatim** (no convention applied); **GQL applies its configured convention** to the alias; **Cypher derives from the GQL name** (per REQ-351 the Cypher node label is the PascalCase GraphQL `type_name`), not directly from the alias. When no alias is set, each language derives its name from `tableName` (normalized) via its configured convention.

Resolution priority for the query-language name of a dataset:

1. `alias` (if set) → SQL verbatim; GQL applies convention; Cypher derived from the GQL name
2. `tableName` (normalized) → each language applies its configured convention

Any name extracted from a SQL, Cypher, or GraphQL query must be resolvable to a physical dataset through the same centralized identity service. Resolution de-normalizes from query-language form → checks alias match first → falls back to tableName match → returns `(sourceId, physicalName)`.

### Implementation contract

- Backend: `provisa.core.dataset_identity` module must expose:
  - `resolve(ref, lang=None) -> DatasetIdentity` — accepts physical name, semantic `domain.table`, or query-language name (with `lang` in `{"sql", "cypher", "graphql"}`).
  - `same_dataset(ref_a, ref_b) -> bool` — resolves both sides before comparing.
  - `to_query_name(identity, lang) -> str` — applies the active naming convention to produce the correct form for a given query language.
- Frontend: shared utility in `provisa-ui/src/utils/datasetIdentity.ts` — no per-component name-matching logic.
- All inline normalization (`_normalize_op_id`, ad-hoc `toSnakeCase` comparisons, etc.) must be replaced by calls to these utilities.
- The service reads the active naming convention from `registered_tables.naming_convention` → `sources.naming_convention` → global default, in that priority order (three levels: table, source, global).
