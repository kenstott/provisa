# Migrating from Hasura DDN (v3) to Provisa

## Prerequisites

1. A Hasura DDN project with HML files (`.hml` extension).
   DDN projects typically have a directory structure like:
   ```
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```
2. Python 3.11+ with the `provisa` package installed.

## CLI Usage

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `hml_dir` | Yes | Path to the DDN HML project directory (scanned recursively for `.hml` files) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output FILE` | stdout | Output YAML file path |
| `--source-overrides FILE` | None | YAML file with per-source connection overrides |
| `--domain-map KEY=VAL ...` | None | Subgraph-to-domain mappings (e.g., `app=core analytics=reporting`) |
| `--governance-default` | `pre-approved` | Default governance level: `pre-approved` or `registry-required` |
| `--dry-run` | off | Parse and validate without writing output |

### Source Overrides File

A YAML file keyed by connector name (after ID sanitization: spaces, dots, slashes
become underscores) with connection properties:

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## Feature Parity Matrix

| DDN Kind | Provisa Equivalent | Notes |
|---|---|---|
| **DataConnectorLink** | `sources[]` | Source type inferred from connector URL (postgres, mysql, mssql, mongo, clickhouse, snowflake, bigquery). Connection details default to placeholders; use `--source-overrides` to set actual values. |
| **ObjectType** | Column definitions on `tables[]` | Fields become columns. `dataConnectorTypeMapping.fieldMapping` resolves GraphQL field names to physical column names. |
| **Model** | `tables[]` | Each Model produces one table. `source_id` from connector, `table_name` from collection. `graphql_type_name` becomes `alias`. |
| **Relationship** | `relationships[]` | Object type -> `many-to-one`, Array type -> `one-to-many`. Field mapping resolved through physical column lookup. |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` determines which roles can see each column. |
| **ModelPermissions** | `rls_rules[]` | Filter predicates converted to SQL WHERE clauses. Supports `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`, `_in`, `_nin`, `_like`, `_is_null`, `_and`, `_or`, `_not`. Session variable references preserved as `${x-hasura-...}`. |
| **Command** | `functions[]` | Both functions and procedures mapped. Arguments, return type, and GraphQL root field name preserved. `domain_id` set from subgraph. |
| **AggregateExpression** | Table description annotation | Count, count_distinct, and per-field aggregate functions appended to table description as `[aggregates: ...]`. |
| **BooleanExpressionType** | Skipped (silently) | Used internally by DDN for filtering; no direct Provisa equivalent needed. |
| **AuthConfig** | Skipped (silently) | DDN auth config not mapped; configure Provisa auth separately. |
| **ScalarType** | Skipped | Warning emitted with count. |
| **GraphqlConfig** | Skipped | Warning emitted with count. |
| **CompatibilityConfig** | Skipped | Warning emitted with count. |
| **Other unrecognized Kinds** | Skipped | Warning emitted with count per kind. |

## Key Concept: GraphQL Field to Physical Column Resolution

DDN separates the GraphQL schema (field names) from the physical database schema
(column names) via `dataConnectorTypeMapping` on ObjectTypes. The converter:

1. Reads `fieldMapping` entries from each ObjectType's type mappings.
2. Builds a lookup: `{graphql_field_name -> physical_column_name}`.
3. For fields without an explicit mapping, assumes field name equals column name.
4. Uses this lookup when building columns, relationships, and RLS filter expressions.

This means the output `provisa.yaml` uses **physical column names** for `columns[].name`
and sets `columns[].alias` to the GraphQL field name when they differ.

## Post-Conversion Steps

1. **Review the output YAML.** Verify sources, tables, and column mappings.
2. **Configure source connections.** Connectors only provide a URL hint for type
   detection. Actual host/port/database/credentials must be supplied via
   `--source-overrides` or by editing the output.
3. **Verify domain assignments.** Without `--domain-map`, each subgraph name becomes
   a domain ID directly. Use `--domain-map` to rename them.
4. **Check RLS rules.** DDN filter predicates are converted to SQL approximations.
   Nested boolean logic (`_and`/`_or`/`_not`) is supported but complex
   relationship-traversing filters may need manual review.
5. **Review aggregate annotations.** Aggregate expressions are stored as table
   description text, not as structured config. If you need programmatic access,
   parse the `[aggregates: ...]` annotation or configure aggregates separately.
6. **Review warnings.** The converter prints a summary to stderr listing skipped
   DDN Kinds and any models referencing unknown ObjectTypes.
7. **Test.** Start the Provisa server and verify queries against your data sources.

## Common Issues and Troubleshooting

### Source type detection fails

The connector URL is used heuristically (checking for keywords like "postgres",
"mysql", "mongo"). If the URL does not contain a recognizable keyword, the source
defaults to `postgresql`. Override with `--source-overrides`.

### Missing ObjectType for a Model

If a Model references an ObjectType name that was not found in any `.hml` file,
the table is skipped and a warning is emitted. Ensure all HML files are included
in the scanned directory.

### Subgraph discovery

Subgraphs are discovered from the HML documents themselves (the `subgraph` field
in each Kind definition). The directory structure is not used for subgraph inference.

### Relationship source resolution

Relationships reference a `source_type` (ObjectType name) and `target_model` (Model
name). If no Model uses the given ObjectType, the relationship is skipped silently.

### Column aliases everywhere

If your DDN project uses `fieldMapping` extensively, expect most columns to have
an `alias` in the output. This is correct behavior -- `name` is the physical column,
`alias` is the GraphQL name your application used.

### Aggregate expressions

Aggregate expressions are not a first-class Provisa config section. They are appended
to the table's `description` field as `[aggregates: count, count_distinct, Revenue(sum,avg)]`.
This preserves the information but requires parsing if you need structured access.

## Example: Converting a Chinook DDN Project

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --governance-default pre-approved \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

Output structure:

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    governance: pre-approved
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
    description: "[aggregates: count, Title(min,max)]"
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    governance: pre-approved
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
