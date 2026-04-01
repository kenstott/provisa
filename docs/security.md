# Security Model

## Rights Model

8 capabilities, no role hierarchy. `admin` grants all.

| Capability | Description |
|-----------|-------------|
| `source_registration` | Register data sources |
| `table_registration` | Register tables, columns |
| `relationship_registration` | Define FK relationships |
| `security_config` | Configure RLS, masking |
| `query_development` | Execute queries |
| `query_approval` | Approve persisted queries |
| `full_results` | Bypass sampling limits |
| `admin` | Superuser — grants all |

## Schema Visibility

Per-role GraphQL schemas hide unauthorized content:
- **Domain access**: Role sees tables only in its `domain_access` domains (`"*"` = all)
- **Column visibility**: Each column lists which roles can see it via `visible_to`
- Unauthorized tables/columns do not appear in the SDL

## Row-Level Security (RLS)

Per-table, per-role SQL WHERE clause injection. Applied after compilation, before execution.

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

The filter is ANDed into the query's WHERE clause. Works for both queries and mutations (UPDATE/DELETE).

## Column-Level Masking

Per-column, per-role data transformation at the SQL level.

| Mask Type | Supported Types | Example |
|-----------|----------------|---------|
| `regex` | String (varchar, char, text) | `al***@example.com` |
| `constant` | Any | `0`, `NULL`, `MAX` |
| `truncate` | Date/Timestamp | `2025-03-01` (from `2025-03-31`) |

Masking is applied in the SELECT projection. WHERE clauses use raw values (users can filter but not see unmasked data).

## Sampling

All roles see sampled results (default: 100 rows) unless they have `full_results` capability. Controlled via `PROVISA_SAMPLE_SIZE` env var.

## Governance

- **Test mode**: All queries allowed
- **Production mode**:
  - `pre-approved` tables: user rights sufficient
  - `registry-required` tables: query must have an approved `stable_id`
- **Ceiling enforcement**: Client queries cannot exceed approved query scope

## Authentication

Pluggable auth providers:

| Provider | Token Type | Use Case |
|----------|-----------|----------|
| `none` | X-Provisa-Role header | Development |
| `firebase` | Firebase ID token | Production |
| `keycloak` | Keycloak JWT | Enterprise |
| `oauth` | OIDC JWT | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Testing |

Role mapping: identity claims → Provisa role via configurable rules.

## Secrets

Credentials use `${env:VAR_NAME}` syntax, resolved at runtime. Passwords are never stored in the config DB.
