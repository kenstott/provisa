# Migrating from Hasura v2 to Provisa

## Prerequisites

1. A running Hasura v2 instance (v2.x) with metadata exported.
2. Export the metadata in either layout:

   **The CLI's metadata directory:**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   This creates a `metadata/` directory. `databases/databases.yaml` lists each source with
   its kind and connection and points at its tables and functions through `!include`
   indexes; `actions.yaml`, `cron_triggers.yaml`, `inherited_roles.yaml`,
   `remote_schemas.yaml` and the other files sit beside it. The older flat layout
   (`tables.yaml` at the root) converts too.

   **The single metadata API document:**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   The bare object and the `{resource_version, metadata}` envelope both work. The admin
   import (Admin, Import Hasura Config) takes this form; the command-line converter below
   reads directories.
3. Python 3.11+ with the `provisa` package installed.

## CLI Usage

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Arguments

| Argument | Required | Description |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Yes | Path to the exported Hasura v2 metadata directory |

### Options

| Option | Default | Description |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Output YAML file path |
| `--source-overrides FILE` | None | YAML file with per-source connection overrides |
| `--domain-map KEY=VAL ...` | None | Schema-to-domain mappings (e.g., `public=core hr=people`) |
| `--auth-env-file FILE` | None | Path to `.env` file with JWT/admin-secret auth configuration |
| `--dry-run` | off | Parse and validate without writing output |

### Source Overrides File

A YAML file keyed by source name with connection properties to override:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Auth Env File

A `.env`-style file holding the Hasura auth configuration to convert. The converter
maps:

- JWT with `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- Webhook auth -> warning emitted (no Provisa equivalent).

## The report

The conversion ends with a report in three parts:

1. **Carried as it is.** The object has a Provisa counterpart and converts without a change of meaning.
2. **Mapped to a better Provisa feature.** Each item says why the Provisa feature does the job better.
3. **Redo by hand.** Each item says what to do. These are the warnings.

The command-line converter prints the report to stderr as `Carried as`, `Mapped to` and `Redo by hand`, each with a count, when anything is left to redo. The admin import returns the three lists with the preview.

## Feature Parity Matrix

| Hasura v2 Feature | Part | Provisa Equivalent | Notes |
| --- | --- | --- | --- |
| **Sources** (postgres, mysql, mssql, bigquery, citus) | carried | `sources[]` | Kind mapped: pg/postgres -> postgresql, mssql -> sqlserver. A literal connection URL parses into host/port/database/username/password. Pool size preserved. A URL read from an environment variable is not guessed (see below). |
| **Tables** (tracked tables) | carried | `tables[]` | Schema + table name preserved. `source_id` links to source. |
| **Custom table names** (`custom_name`, `custom_root_fields.select`) | carried | `tables[].alias` | First non-null of `select`, `select_by_pk`, `custom_name`. |
| **Custom column names** (`column_config`, older `custom_column_names`) | carried | `columns[].alias` | Each column's custom name becomes its alias. |
| **Other root field names** (`select_by_pk`, `select_aggregate`, `insert`, ...) | mapped | The table's one field | Reported by name. A by-key read is a filter on the table, an aggregate its `_aggregate` field, a write its mutation. |
| **Select permissions** (columns, filter) | carried | `columns[].visible_to[]`, `rls_rules[]` | Column lists become `visible_to`; `*` is supported. Filters become SQL; session variables become `current_setting('provisa.<name>')`. |
| **Filters through relationships** | carried | `EXISTS` subqueries in `rls_rules[]` | The rule follows the relationship to its table. |
| **Insert, update and delete permissions** | carried | `tables[].write_grants[]` | One grant per role and operation: columns from `columns`, `rows` from `filter`, `check` from `check`, presets from `set`. Roles gain the `write` right. |
| **Admin role** | carried | `org_admin` | Holds every grant over every column and row, and is named on every imported column. |
| **Object relationships** | carried | `relationships[]` with `cardinality: many-to-one` | Approved. The GraphQL field is the Hasura relationship name. |
| **Array relationships** | carried | `relationships[]` with `cardinality: one-to-many` | Approved. The GraphQL field is the Hasura relationship name. |
| **Inherited roles** (one role) | carried | `roles[].parent_role_id` | The role becomes the parent. |
| **Inherited roles** (several roles) | mapped | A role holding the union of their grants | Provisa roles have one parent, so the union is written out on the role. |
| **Computed fields** | redo | A view column | The export names the function, not its body. The report gives the `SELECT` to add. |
| **Tracked functions** | carried, else redo | `functions[]` commands | The preview reads the signature from the database. Without it, add the function on the Commands page. |
| **Actions** | mapped | Commands calling the handler | `{{VAR}}` in the handler URL is written `${env:VAR}` and resolved when the command is called. |
| **Cron triggers** | carried | `scheduled_triggers[]` | Cron expression, URL, payload and enabled flag. Retries are not carried (warned). |
| **Event triggers** | mapped | `scheduled_triggers[]` on the table's change | Fires per batch of changes on any change. Retries are not carried (warned). |
| **Remote schemas** | mapped | `sources[]` (`graphql_remote`) landed as tables | Name, URL, headers and authentication preserved; root fields become governed tables. |
| **Enum tables** | carried | Table created | `is_enum` flag not carried over (no Provisa equivalent). |
| **Allow lists, query collections, REST endpoints** | redo | None | Reported when present in a consolidated export; not converted. |

### Relationships

- The GraphQL field of each relationship is the Hasura name. When the table already has a field of that name, the relationship takes the name Provisa gives it and the report warns.
- Two relationships declaring the same join merge into one. The report says which merged into which.
- A relationship joining on several column pairs is warned and not created, since a Provisa relationship joins on one. Row rules through it still convert.
- An object relationship declared by foreign key column alone is resolved through the inverse array relationship; without one, it is warned.

### Write grants

A grant states what one role may write to one table by one operation. An empty `filter` or `check` is every row (`TRUE`). A `set` value of `X-Hasura-User-Id` becomes a `session` preset of `user_id`; any other value becomes a `literal` preset. A preset column is not also in the grant's columns.

### Actions, tracked functions and triggers

- An **action** becomes a command calling its handler, visible to the roles its permissions name (to `org_admin` alone when it names none). An asynchronous action is warned: the command waits for the handler's answer.
- A **tracked function** becomes a command when the preview can read its arguments and return table from a PostgreSQL source. Offline, and for any function that is missing, overloaded or returns a table the import does not register, it goes to redo.
- An **event trigger** fires on every change to its table, once per batch, and posts the table and the changed rows rather than one event per row with old and new values. A trigger that listened to fewer than all three operations is warned.

## Post-Conversion Steps

1. **Review the report.** Start with `Redo by hand`.
2. **Configure source connections.** A connection the export spells out converts as written. One read from an environment variable arrives empty. Use `--source-overrides` or edit the output directly.
3. **Verify domain assignments.** Without `--domain-map`, all tables land in `default`.
   Assign schemas to domains with `--domain-map public=core analytics=reporting`.
4. **Check row rules.** Filters convert to SQL. Review complex boolean expressions
   (nested `_and`/`_or`/`_exists`) and filters through relationships.
5. **Redo what is listed.** Computed fields, tracked functions the preview could not read,
   retries on triggers and webhook auth have no automatic conversion.
6. **Set up auth.** If your Hasura instance uses JWT/webhook auth, create an auth env
   file and re-run with `--auth-env-file`.
7. **Test.** Start the Provisa server and verify queries against your data sources.

## Common Issues and Troubleshooting

### Connection comes from an environment variable

If the source `database_url` is an environment variable reference (`{"from_env": "PG_URL"}`),
its value is not in the export. The converter does not guess one: the source has empty
host, database, username and password, and the report lists it under `Redo by hand`.
Enter the connection in the import's source settings (or `--source-overrides`) and convert
again.

### Wildcard columns

When a permission grants `columns: "*"`, the converter creates a single wildcard
column entry. After conversion, you may want to replace it with explicit column
lists by inspecting the actual database schema.

### Event trigger fidelity

An event trigger becomes a trigger on changes to its table, calling the same webhook. It fires
per batch of changes on any change, whether the change came through Provisa or straight to the
database, and posts the table and the changed rows. Hasura's per-row old and new values,
per-operation filtering and retries are not carried; the report warns of each that applies.
Make the handler act on the rows it is meant for, and retry inside it if a missed call matters.

### Missing roles

Roles are collected from table permissions, action permissions and inherited roles. If a role
exists in Hasura but appears in none of them, it will not appear in the output.

### Custom root fields

The `select` and `select_by_pk` root field names give the table alias. The other custom root
fields (`select_aggregate`, `insert`, `update`, `delete`) are reported by name: Provisa serves
a table as one field, with aggregates and writes as its `_aggregate` field and mutations.

## Example

Convert a typical Hasura v2 project with two schemas mapped to domains:

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

Output structure (abridged):

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
