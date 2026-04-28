# Migrating from Hasura v2 to Provisa

## Prerequisites

1. A running Hasura v2 instance (v2.x) with metadata exported.
2. Export metadata using the Hasura CLI:
   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```
   This creates a `metadata/` directory containing `sources.yaml`, `actions.yaml`,
   `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml`, etc.
3. Python 3.11+ with the `provisa` package installed.

## CLI Usage

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `metadata_dir` | Yes | Path to the exported Hasura v2 metadata directory |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output FILE` | stdout | Output YAML file path |
| `--source-overrides FILE` | None | YAML file with per-source connection overrides |
| `--domain-map KEY=VAL ...` | None | Schema-to-domain mappings (e.g., `public=core hr=people`) |
| `--governance-default` | `pre-approved` | Default governance level: `pre-approved` or `registry-required` |
| `--auth-env-file FILE` | None | Path to `.env` file with auth provider configuration |
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

A `.env`-style file defining the auth provider:

```
AUTH_PROVIDER=firebase
FIREBASE_PROJECT_ID=my-project-123
```

Supported providers: `firebase` (requires `FIREBASE_PROJECT_ID`),
`keycloak` (requires `KEYCLOAK_URL`, `KEYCLOAK_REALM`).

## Feature Parity Matrix

| Hasura v2 Feature | Provisa Equivalent | Notes |
|---|---|---|
| **Sources** (postgres, mysql, mssql, bigquery, citus) | `sources[]` | Kind mapped: pg/postgres -> postgresql, mssql -> sqlserver. Connection URL parsed into host/port/database/username/password. Pool settings preserved. |
| **Tables** (tracked tables) | `tables[]` | Schema + table name preserved. `source_id` links to source. |
| **Custom table names** (`custom_name`, `custom_root_fields.select`) | `tables[].alias` | First non-null of `select`, `select_by_pk`, `custom_name`. |
| **Custom column names** | `columns[].alias` | Maps `custom_column_names` dict to column aliases. |
| **Select permissions** (columns, filter) | `columns[].visible_to[]`, `rls_rules[]` | Column lists become `visible_to`. Wildcard (`*`) columns supported. Filters converted to SQL via `bool_expr_to_sql`. |
| **Insert/Update permissions** (columns) | `columns[].writable_by[]` | Column lists become `writable_by`. Roles upgraded to `write` capability. |
| **Delete permissions** | Role capability upgrade | Role gets `write` capability. No per-table delete mapping. |
| **Object relationships** | `relationships[]` with `cardinality: many-to-one` | Column mapping preserved. |
| **Array relationships** | `relationships[]` with `cardinality: one-to-many` | Column mapping preserved. |
| **Computed fields** | `functions[]` | Mapped to Function with `returns` pointing to the parent table ID. |
| **Tracked functions** | `functions[]` | `exposed_as` defaults to mutation. Schema preserved. |
| **Actions** (HTTP handler) | `webhooks[]` | URL, method (POST), arguments, and `visible_to` roles preserved. |
| **Actions** (non-HTTP handler) | `functions[]` | Placeholder function created. Warning emitted. |
| **Cron triggers** | `scheduled_triggers[]` | Cron expression, webhook URL, and enabled state preserved. |
| **Event triggers** | `event_triggers[]` | Operations, webhook URL, retry config (max retries, interval) preserved. Warning about limited fidelity. |
| **Inherited roles** | `roles[].parent_role_id` | First role in `role_set` becomes parent. All child roles created. |
| **Remote schemas** | Skipped | Warning emitted. No Provisa equivalent. |
| **Enum tables** | Table created | `is_enum` flag not carried over (no Provisa equivalent). |
| **Allow lists** | Skipped | Not present in metadata model. |

## Post-Conversion Steps

1. **Review the output YAML.** Check that sources, tables, and roles look correct.
2. **Configure source connections.** The converter parses connection URLs but defaults
   to `localhost` on parse failure. Use `--source-overrides` or edit the output directly.
3. **Verify domain assignments.** Without `--domain-map`, all tables land in `default`.
   Assign schemas to domains with `--domain-map public=core analytics=reporting`.
4. **Check RLS rules.** Filters are converted to SQL approximations. Complex boolean
   expressions (nested `_and`/`_or`/`_exists`) should be reviewed manually.
5. **Review warnings.** The converter prints a warning summary to stderr for features
   with limited conversion fidelity (event triggers, non-HTTP actions, remote schemas).
6. **Set up auth.** If your Hasura instance uses JWT/webhook auth, create an auth env
   file and re-run with `--auth-env-file`.
7. **Test.** Start the Provisa server and verify queries against your data sources.

## Common Issues and Troubleshooting

### Connection URL not parsed

If the source `database_url` is an environment variable reference (`{"from_env": "PG_URL"}`),
the converter cannot resolve it at conversion time. The source will have placeholder
values (`host: localhost`, `database: default`). Fix with `--source-overrides`.

### Wildcard columns

When a permission grants `columns: "*"`, the converter creates a single wildcard
column entry. After conversion, you may want to replace it with explicit column
lists by inspecting the actual database schema.

### Event trigger fidelity

Event triggers are converted with `operations` and `webhook_url` but Hasura-specific
delivery guarantees (exactly-once, redelivery) do not have direct Provisa equivalents.
Review the `event_triggers` section and configure your webhook infrastructure accordingly.

### Missing roles

Roles are collected only from permission entries. If a role exists in Hasura but has
no permissions on any table or action, it will not appear in the output.

### Custom root fields

Only `select` and `select_by_pk` root fields are used for the table alias. Other
custom root fields (`select_aggregate`, `insert`, `update`, `delete`) are not mapped.

## Example

Convert a typical Hasura v2 project with two schemas mapped to domains:

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --governance-default pre-approved \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

Output structure:

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    governance: pre-approved
    columns:
      - name: id
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
