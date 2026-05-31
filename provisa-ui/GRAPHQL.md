# GraphQL Setup & SDL Validation

This project uses externalized GraphQL operations with schema validation via a generated Schema Definition Language (SDL) file.

## Files

- **`schema.graphql`** — Generated server schema for validation (auto-generated via introspection)
- **`src/hooks/admin.graphql`** — Application queries and mutations (externalized from inline gql``)
- **`graphql.config.ts`** — GraphQL configuration for editor support and validation
- **`scripts/generate-schema.ts`** — Script to fetch and regenerate schema from server

## Workflow

### Generate Schema (when server schema changes)

```bash
npm run generate-schema
```

This introspects the running backend (`http://127.0.0.1:8000/admin/graphql`) and generates `schema.graphql`.

### Validation

Once `schema.graphql` exists:
- **GraphQL extensions** (VSCode, JetBrains) read `graphql.config.ts` and validate `.graphql` files against the schema
- **Type safety** — queries/mutations in `src/hooks/admin.graphql` are validated at dev time
- **Autocompletion** — editor provides hints based on the schema

## Setup

1. Start the backend: `./restart.sh` (from provisa root)
2. Generate schema: `npm run generate-schema`
3. Install GraphQL extension in your editor (VSCode: `apollo.apollo-vscode`)
4. Editor will use `graphql.config.ts` for validation and autocompletion

## Adding Operations

1. Add query/mutation to `src/hooks/admin.graphql`
2. Import it in `src/hooks/useAdminQueries.ts`
3. Regenerate schema if the server schema changed: `npm run generate-schema`
4. Editor will validate the new operation
