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
3. **Convert and preview.** The server converts and returns counts, converter warnings, and the
   generated configuration. Nothing is written at this step.
4. **Review and edit.** The configuration is editable in place — connection details, domain names,
   role names. What you apply is what is shown.
5. **Apply.** *Replace the existing semantic layer* deletes every source, table, role and rule
   absent from the configuration; left off, the import merges into what the organization has.
   Applying loads the configuration and rebuilds the organization's schemas.

Endpoints: `POST /admin/import/hasura/preview` and `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Export Metadata

From your Hasura console or CLI:

```bash
hasura metadata export --output metadata.yaml
```

Or use the Hasura API:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Convert

The v2 converter reads a Hasura metadata **directory** (the layout produced by `hasura metadata export`, or the flat `tables.yaml` / `actions.yaml` layout) and writes a Provisa config:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omit `-o` to write the config to stdout.

Flags:

| Flag | Purpose |
| ------ | --------- |
| `-o`, `--output` | Output YAML path (default: stdout) |
| `--source-overrides` | YAML file with per-source connection overrides (host, port, credentials) |
| `--domain-map` | Schema-to-domain mappings as `SCHEMA=DOMAIN` pairs |
| `--auth-env-file` | `.env` file with auth config; converts JWT/JWK, admin secret, and claims map |
| `--dry-run` | Parse and validate without writing output |

### What Gets Converted

| Hasura concept | Provisa equivalent |
| --------------- | ------------------- |
| Tracked table | `tables[]` with `publish: true` |
| Object relationship | `relationships[]` with `cardinality: many-to-one`. One declared by FK column alone (`foreign_key_constraint_on: artist_id`) names no target in the export; the converter resolves it through the inverse array relationship, and drops it with a `[relationships]` warning when there is none. (REQ-1680) |
| Array relationship | `relationships[]` with `cardinality: one-to-many` |
| Select permission | Role visibility + RLS filter. A session-variable term (`X-Hasura-User-Id`) becomes `current_setting('provisa.user_id')`, which the request binds from the identity's user id and claims at query time. (REQ-1682) |
| Column permission | `visible_to` / `writable_by` |
| Insert/update/delete permission | Mutation `writable_by` + RLS |
| Remote schema | `graphql_remote` source registration plus one landed table per Query root field the role SDLs expose; a column is visible to every role whose SDL exposes it, a non-null root argument becomes a `_nf_` native-filter column, nested fields are named in a warning. (REQ-1681) |
| Computed field | `functions[]` entry with `kind: query` |

### Connections and domains on the import tab

The export names its databases by environment variable, so after the first conversion the tab lists each SQL source with the connection the conversion guessed. Fill in the host, port, database, username and password, and convert again; only the fields you changed travel, as source overrides. The domain rows cover every schema, subgraph and remote schema the upload carries; each is a picker over the organization's existing domains that also accepts a typed name, marked "new domain" when it matches none. Apply merges into what the organization already has unless the replace checkbox is on. (REQ-1687)

### Types come from the source at preview

A Hasura export names columns without types, and a tracked table with no permission names no columns. The preview runs with the source connections you supply, so it reads each reachable SQL source's `information_schema.columns`: every untyped column gets the source's type mapped to the IR vocabulary, and a table with no columns takes every column the source has, visible to `org_admin` alone, since Hasura exposed it to no other role. A source the preview cannot reach is reported as a `[sources]` warning and its columns stay untyped for you to finish before apply. (REQ-1683, REQ-1684)

### Limitations

- **Actions** convert automatically: HTTP-handler actions become `webhooks[]` mutations; actions with a non-HTTP (database) handler become a `functions[]` placeholder and emit a warning to review the handler
- **Event triggers** convert to per-table `event_triggers` config (operations, webhook URL, retry policy) and emit a warning noting limited fidelity
- **Remote schemas** convert to `graphql_remote` source entries and are landed as tables from the role permission SDLs; a remote schema with no permissions lands nothing, since the export carries no other statement of its shape (REQ-1681)
- **Custom SQL functions** require review — simple cases convert to `functions[]` entries, complex ones need manual work
- **Cron triggers** convert to `scheduler` config entries, preserving the cron expression and enabled flag

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

1. Review the generated `config.yaml` — pay attention to `warnings` from the converter
2. Verify connection credentials (the converter uses placeholder values)
3. Start Provisa and confirm tables appear in the Explorer
4. Run your existing GraphQL queries — the schema is compatible for common patterns
5. Submit queries for approval via the Admin API or UI before enabling production governance
