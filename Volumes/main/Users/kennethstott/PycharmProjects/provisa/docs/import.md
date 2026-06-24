# Importing from Hasura

Provisa can convert existing Hasura metadata into a Provisa `config.yaml`, preserving tracked tables, relationships, permissions, and remote schemas. (REQ-182, REQ-183)

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

```bash
python -m provisa.hasura_v2 <metadata_dir> -o config.yaml
```

### What Gets Converted

| Hasura concept | Provisa equivalent |
|---------------|-------------------|
| Tracked table | `tables[]` with `publish: true` (REQ-182) |
| Object relationship | `relationships[]` with `cardinality: many-to-one` (REQ-188) |
| Array relationship | `relationships[]` with `cardinality: one-to-many` (REQ-188) |
| Select permission | Role visibility + RLS filter (REQ-185, REQ-187) |
| Column permission | `visible_to` / `writable_by` (REQ-185, REQ-186) |
| Insert/update/delete permission | Mutation `writable_by` + RLS (REQ-186) |
| Remote schema | `graphql_remote` source entry (REQ-417) |
| Computed field | `functions[]` entry |
| Cron trigger | `scheduled_triggers[]` entry with cron expression (REQ-216) |
| Event trigger | `event_triggers[]` entry (REQ-220) |

### Limitations

- **Actions** are not automatically converted — create equivalent webhook mutations manually (REQ-192)
- **Event triggers** are converted with limited fidelity; verify webhook URLs after import (REQ-192)
- **Custom SQL functions** require review — simple cases convert to `functions[]`, complex ones need manual work (REQ-192)

---

## Hasura DDN (v3)

### Export Supergraph

```bash
ddn supergraph build local
# Output: .hasura/ HML project directory
```

### Convert

```bash
python -m provisa.ddn <hml_dir> -o config.yaml
```

### What Gets Converted

| DDN concept | Provisa equivalent |
|------------|-------------------|
| Subgraph model | `tables[]` under a source (REQ-183) |
| Relationship | `relationships[]` (REQ-183) |
| Permission rule | RLS filter (REQ-183) |
| Command | `functions[]` tracked function entry (REQ-183) |
| Connector | Source entry with connection details (REQ-183) |

### Limitations

- **Lambda connectors** (TypeScript/Python functions) require manual webhook setup (REQ-192)
- **Lifecycle plugins** have no direct equivalent (REQ-192)
- **DDN auth modes** map to Provisa auth providers but JWT claim paths may need adjustment (REQ-190)

---

## After Import

1. Review the generated `config.yaml` — pay attention to `warnings` from the converter (REQ-192, REQ-193)
2. Verify connection credentials (the converter uses placeholder values) (REQ-621)
3. Start Provisa and confirm tables appear in the Explorer
4. Run your existing GraphQL queries — the schema is compatible for common patterns
