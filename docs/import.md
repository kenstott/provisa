# Importing from Hasura

Provisa can convert existing Hasura metadata into a Provisa `config.yaml`, preserving tracked tables, relationships, permissions, and remote schemas.

## Interactive import (Admin → Import Hasura Config)

The admin surface runs the same converters, so an import needs no shell access and no config file
round-trip. Requires the `org_settings` capability; the import lands in the organization the
session is acting in.

1. **Upload.** Choose a zipped Hasura v2 metadata directory, a zipped DDN project, a consolidated
   metadata export (`.yaml`/`.json`, including the `{resource_version, metadata}` envelope the
   metadata API returns), or a single `.hml`. Leave the format on *Detect automatically* unless the
   upload is ambiguous.
2. **Map domains** (optional). Each pair maps a v2 schema or a DDN subgraph to a Provisa domain;
   anything unmapped keeps its original name.
3. **Convert and preview.** The server converts and returns counts, the conversion report (see
   [The report](#the-report)), and the generated configuration. Nothing is written at this step.
4. **Review and edit.** The configuration is editable in place — connection details, domain names,
   role names. What you apply is what is shown.
5. **Apply.** *Apply to this organization* loads the configuration and rebuilds the organization's schemas.
   An import is always a merge into what the organization has.

Endpoints: `POST /admin/import/hasura/preview` and `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Export Metadata

Both export layouts convert.

**The CLI's metadata directory.** `hasura metadata export` writes a directory. `databases/databases.yaml` lists each source with its kind and connection, and points at the source's tables and functions through `!include` indexes:

```bash
hasura metadata export
```

```yaml
# metadata/databases/databases.yaml
- name: default
  kind: postgres
  configuration:
    connection_info:
      database_url:
        from_env: HASURA_GRAPHQL_DATABASE_URL
  tables: "!include default/tables/tables.yaml"
  functions: "!include default/functions/functions.yaml"
```

The flat layout (`tables.yaml` and `actions.yaml` at the root) converts too.

**The single metadata API document.** The metadata API returns the whole export as one object, with the per-source `configuration` the directory layout has nowhere to put:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

The bare object and the `{resource_version, metadata}` envelope are both accepted. The admin import takes this form (see [Interactive import](#interactive-import-admin--import-hasura-config)); the command-line converter reads directories only.

### Convert

The command-line converter reads a Hasura metadata **directory** (the layout `hasura metadata export` produces, or the flat `tables.yaml` / `actions.yaml` layout) and writes a Provisa config:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omit `-o` to write the config to stdout. When something is left to redo by hand, the report prints to stderr.

Flags:

| Flag | Purpose |
| ------ | --------- |
| `-o`, `--output` | Output YAML path (default: stdout) |
| `--source-overrides` | YAML file with per-source connection overrides (host, port, credentials), keyed by source name |
| `--domain-map` | Schema-to-domain mappings as `SCHEMA=DOMAIN` pairs |
| `--auth-env-file` | `.env` file with auth config; converts JWT/JWK, admin secret, and claims map |
| `--dry-run` | Parse and validate without writing output |

### The report

Every conversion sorts what it found into three parts. Read them in this order.

1. **Carried as it is.** The object has a direct Provisa counterpart and converts unchanged in meaning: sources, tables, approved relationships, select permissions, cron triggers.
2. **Mapped to a better Provisa feature.** Provisa has a feature that does the job better than the Hasura one, so the object becomes that feature. Each item says why. Event triggers, actions, remote schemas, extra root field names and roles inherited from several roles land here.
3. **Redo by hand.** The object has no Provisa equivalent, or the export lacks what the conversion needs. Each item says what to do. These are the warnings.

The preview returns the three parts as lists. The command-line converter prints them to stderr as `Carried as`, `Mapped to` and `Redo by hand`, each with a count. A conversion with nothing in the third part is finished.

### What Gets Converted

| Hasura concept | Part | Provisa result |
| --------------- | ---- | -------------- |
| Source (`kind`, `connection_info`) | carried | `sources[]`. A connection written out in the export converts to host, port, database and credentials. One read from an environment variable is not guessed (see [Connections and domains](#connections-and-domains-on-the-import-tab)). |
| Tracked table | carried | A registered table. Its custom name (or its `select` root field) becomes the table alias. |
| Custom column names (`column_config`, or the older `custom_column_names`) | carried | Column `alias`. |
| Root field names beyond `select` (`select_by_pk`, `select_aggregate`, `insert`, ...) | mapped | Reported by name. Provisa serves a table as one field: a by-key read is a filter on it, an aggregate its `_aggregate` field, a write its mutation. Check that the app's queries take that shape. |
| Object and array relationship | carried | An approved relationship (`many-to-one` for object, `one-to-many` for array) whose GraphQL field is the Hasura relationship name. See below for the cases that differ. |
| Select permission | carried | Column grants (`visible_to`) and a row rule. A session-variable term (`X-Hasura-User-Id`) becomes `current_setting('provisa.hasura_user_id')`, which the request binds at query time from the identity's claims or the member's attributes. A Hasura session variable keeps its own name; `provisa.user_id` stays the signed-in identity. |
| Row rule through a relationship | carried | An `EXISTS` subquery over the related table. |
| Insert, update and delete permission | carried | Write grants on the table: one per role and operation. `columns` become the grant's columns, `filter` its `rows`, `check` its `check`, and `set` its presets. |
| Inherited role of one role | carried | That role as `parent_role_id`. |
| Inherited role of several roles | mapped | A role holding the union of their grants, written out: a Provisa role has one parent. |
| Computed field | redo | A column of a view over the table. |
| Tracked function | carried | A command; see below. Otherwise redo. |
| Action | mapped | A command calling the action's handler. |
| Cron trigger | carried | A scheduled trigger. |
| Event trigger | mapped | A trigger on changes to the table. |
| Remote schema | mapped | A `graphql_remote` source whose root fields land as governed tables. |

Relationship cases:

- **Collision.** When the table already has a field with the relationship's name, the relationship takes the name Provisa gives it, and the report warns. Rename it on the Relationships page if the app needs another name.
- **Same join twice.** A second relationship declaring a join already declared merges into the first. The report says so under carried.
- **Several column pairs.** A Provisa relationship joins on one column pair. A relationship joining on several is warned and not created, but row rules through it are carried.
- **Foreign key column only.** An object relationship declared by `foreign_key_constraint_on: artist_id` names no target in the export. The converter resolves it through the inverse array relationship and warns when there is none.

Write grant details:

- An empty `filter` or `check` is every row, written `TRUE`.
- A `set` value that is a session variable (`X-Hasura-User-Id`) becomes a `session` preset of the matching session variable (`X-Hasura-User-Id` becomes `hasura_user_id`). Any other value becomes a `literal` preset. A preset column is not also in the grant's columns.
- Hasura's implicit admin holds every permission. It becomes the role `org_admin`, which holds an insert, update and delete grant over every column and every row, and is named on every imported column.
- Each role with an insert, update or delete permission also receives the `write` right.

Command details:

- **Tracked function.** The export names the function and the roles that call it, not its arguments or what it returns. The preview reads both from the database's catalog and registers a command whose signature it found. When the source is not reached, is not PostgreSQL, or the function is missing, overloaded, or returns rows of a table the import does not register, the function goes to redo. The command-line converter never reaches a database, so every tracked function it finds goes to redo.
- **Action.** The command calls the action's handler as a `POST`. A `{{VAR}}` in the handler URL is written `${env:VAR}` and resolved when the command is called. The command is visible to the roles the action's permissions name, or to `org_admin` alone when it names none. An asynchronous action is warned: its command waits for the handler's answer. A return type whose fields the export does not carry is warned.
- **Event trigger.** The trigger fires on every change to the table, once per batch of changes, and posts the table and the changed rows. It does not fire per row with old and new values. A Hasura trigger on fewer than all three operations is warned, since the Provisa trigger fires on all of them. Retries are not carried: a Hasura trigger with `num_retries` is warned, because a Provisa trigger calls once. Retry in the handler if a missed call matters.
- **Cron trigger.** The cron expression, URL, payload and enabled flag carry over. Retries are not carried and are warned the same way.

A consolidated document also holds sections the converter has no target for: `query_collections`, `allowlist`, `rest_endpoints`, `network` and `opentelemetry`. When one is present, the report lists it under redo. The directory reader never opens their files.

### Connections and domains on the import tab

A connection the export spells out converts as written. When the export reads a database URL from an environment variable (`database_url: {from_env: HASURA_GRAPHQL_DATABASE_URL}`), the variable's value is not in the export, so the conversion does not guess one. The source is registered with empty connection fields, and the report says to redo it: enter the source's host, port, database, user and password in the import's source settings and convert again. Only the fields you change travel, as source overrides. The domain rows cover every schema, subgraph and remote schema the upload carries; each is a picker over the organization's existing domains that also accepts a typed name, marked "new domain" when it matches none. Apply merges into what the organization already has.

### Types come from the source at preview

A Hasura export names columns without types, and a tracked table with no permission names no columns. The preview runs with the source connections you supply, so it reads each reachable SQL source's `information_schema.columns`: every untyped column gets the source's type mapped to the IR vocabulary, and a table with no columns takes every column the source has, visible to `org_admin` alone, since Hasura exposed it to no other role. A source the preview cannot reach is reported as a `[sources]` warning and its columns stay untyped for you to finish before apply.

### Limitations

- **Computed fields** are redone by hand: the export names the database function but does not define it. Add it as a column of a view over the table, or compute it in the app.
- **Remote schemas** land as tables from the role permission SDLs. A remote schema with no permissions lands nothing, since the export carries no other statement of its shape.
- **Tracked functions** need the database at preview time to read their signature; offline, they are redone on the Commands page.
- **Event and cron trigger retries** are not carried.

---

## Hasura DDN (v3)

### Locate the HML project

The DDN converter reads the DDN project **directory** of `.hml` files directly — no supergraph build step is required. The first directory component under the project root is taken as the subgraph name; files under `globals/` are assigned the `globals` subgraph.

### Convert

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Omit `-o` to write the config to stdout.

Flags:

| Flag | Purpose |
| ------ | --------- |
| `-o`, `--output` | Output YAML path (default: stdout) |
| `--source-overrides` | YAML file with per-source connection overrides |
| `--domain-map` | Subgraph-to-domain mappings as `SUBGRAPH=DOMAIN` pairs |
| `--aggregates-output` | Output path for the aggregate-expressions sidecar (default: `<output>-aggregates.yaml`) |
| `--dry-run` | Parse and validate without writing output |

`AggregateExpression` metadata is preserved in a sidecar `*-aggregates.yaml` file.

### What Gets Converted

| DDN concept | Provisa equivalent |
| ------------ | ------------------- |
| Subgraph model | `tables[]` under a source |
| Relationship | `relationships[]` |
| Permission rule | RLS filter |
| Command | Webhook mutation or view |
| Connector | Source entry with connection details |

### Limitations

- **Lambda connectors** (TypeScript/Python functions) require manual webhook setup
- **Lifecycle plugins** have no direct equivalent
- **DDN auth modes** map to Provisa auth providers but JWT claim paths may need adjustment

---

## After Import

1. Review the generated `config.yaml` and the report — start with the `Redo by hand` items
2. Verify connection credentials. A connection the export read from an environment variable arrives empty; the report names it
3. Start Provisa and confirm tables appear in the Explorer
4. Run your existing GraphQL queries — the schema is compatible for common patterns
5. Submit queries for approval via the Admin API or UI before enabling production governance
