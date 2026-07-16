# Importing from Hasura

Provisa can convert existing Hasura metadata into a Provisa `config.yaml`, preserving tracked tables, relationships, permissions, and remote schemas.

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
|------|---------|
| `-o`, `--output` | Output YAML path (default: stdout) |
| `--source-overrides` | YAML file with per-source connection overrides (host, port, credentials) |
| `--domain-map` | Schema-to-domain mappings as `SCHEMA=DOMAIN` pairs |
| `--auth-env-file` | `.env` file with auth config; converts JWT/JWK, admin secret, and claims map |
| `--dry-run` | Parse and validate without writing output |

### What Gets Converted

| Hasura concept | Provisa equivalent |
|---------------|-------------------|
| Tracked table | `tables[]` with `publish: true` |
| Object relationship | `relationships[]` with `cardinality: many-to-one` |
| Array relationship | `relationships[]` with `cardinality: one-to-many` |
| Select permission | Role visibility + RLS filter |
| Column permission | `visible_to` / `writable_by` |
| Insert/update/delete permission | Mutation `writable_by` + RLS |
| Remote schema | `graphql_remote` source registration |
| Computed field | `functions[]` entry with `kind: query` |

### Limitations

- **Actions** convert automatically: HTTP-handler actions become `webhooks[]` mutations; actions with a non-HTTP (database) handler become a `functions[]` placeholder and emit a warning to review the handler
- **Event triggers** convert to per-table `event_triggers` config (operations, webhook URL, retry policy) and emit a warning noting limited fidelity
- **Remote schemas** convert to `graphql_remote` source entries
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
|------|---------|
| `-o`, `--output` | Output YAML path (default: stdout) |
| `--source-overrides` | YAML file with per-source connection overrides |
| `--domain-map` | Subgraph-to-domain mappings as `SUBGRAPH=DOMAIN` pairs |
| `--aggregates-output` | Output path for the aggregate-expressions sidecar (default: `<output>-aggregates.yaml`) |
| `--dry-run` | Parse and validate without writing output |

`AggregateExpression` metadata is preserved in a sidecar `*-aggregates.yaml` file.

### What Gets Converted

| DDN concept | Provisa equivalent |
|------------|-------------------|
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
