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

```bash
provisa import hasura-v2 --input metadata.yaml --output config.yaml
```

Or via the Admin API:
```graphql
mutation {
  importHasuraV2(yaml: "<contents of metadata.yaml>") {
    config
    warnings {
      field
      message
    }
  }
}
```

### What Gets Converted

| Hasura concept | Provisa equivalent |
|---------------|-------------------|
| Tracked table | `tables[]` with `publish: true` |
| Object relationship | `relationships[]` with `cardinality: many-to-one` |
| Array relationship | `relationships[]` with `cardinality: one-to-many` |
| Select permission | Role visibility + RLS filter |
| Column permission | `visible_to` / `writable_by` |
| Insert/update/delete permission | Mutation `writable_by` + RLS |
| Remote schema | `api_source` entry |
| Computed field | `views[]` entry |

### Limitations

- **Actions** are not automatically converted — create equivalent webhook mutations manually
- **Event triggers** map to Provisa webhook config but require manual URL configuration
- **Custom SQL functions** require review — simple cases convert to `views[]`, complex ones need manual work
- **Scheduled triggers** map to `scheduler` config entries (intervals only, no cron expressions yet)

---

## Hasura DDN (v3)

### Export Supergraph

```bash
ddn supergraph build local
# Output: .hasura/supergraph.json or connector metadata
```

### Convert

```bash
provisa import ddn --input supergraph.json --output config.yaml
```

Or via the Admin API:
```graphql
mutation {
  importDDN(json: "<contents of supergraph.json>") {
    config
    warnings {
      field
      message
    }
  }
}
```

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
