# Security Model

## Rights Model

8 capabilities with optional role hierarchy via `parent_role_id`. `admin` grants all.

| Capability | Description |
|-----------|-------------|
| `source_registration` | Register data sources |
| `table_registration` | Register tables, columns |
| `relationship_registration` | Define FK relationships |
| `security_config` | Configure RLS, masking |
| `query_development` | Execute queries |
| `query_approval` | Approve governed queries |
| `full_results` | Bypass sampling limits |
| `admin` | Superuser — grants all |

### Role Inheritance

Roles can inherit capabilities and domain access from a parent role via `parent_role_id`. The hierarchy is flattened at startup — child roles merge their parent's capabilities and domain access with their own.

```yaml
roles:
  - id: basic_user
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```

## Column Permission Model

Each column has a four-field permission model controlling read, write, and masking access per role.

### Three-Tier Visibility

| Tier | Condition | Result |
|------|-----------|--------|
| **Hidden** | Role not in `visible_to` | Column absent from GraphQL SDL |
| **Masked** | Role in `visible_to`, has masking rule, role not in `unmasked_to` | Column visible but data masked in SQL |
| **Unmasked** | Role in `visible_to` AND role in `unmasked_to` (or no masking rule) | Full read access |

### Write Permissions

| Field | Empty means | Purpose |
|-------|------------|---------|
| `visible_to` | All roles can read | Controls who sees the column (masked or unmasked) |
| `unmasked_to` | No role sees unmasked | Controls who bypasses masking |
| `writable_by` | No role can write | Controls who can mutate (INSERT/UPDATE) |

Write permission is enforced in the mutation pipeline. A role not in `writable_by` receives a 403 error when attempting to write to a restricted column.

### Example

```yaml
columns:
  - name: email
    visible_to: [admin, analyst, viewer]
    writable_by: [admin]
    unmasked_to: [admin]
    mask_type: regex
    mask_pattern: "(.).*@"
    mask_replace: "$1***@"
  - name: salary
    visible_to: [admin, hr]
    writable_by: [hr]
    unmasked_to: [admin, hr]
    mask_type: constant
    mask_value: "0"
  - name: created_at
    visible_to: []           # all can read
    writable_by: []          # nobody can write (auto-set)
```

In this example:
- `email`: admin sees `alice@example.com` and can edit; analyst/viewer see `a***@example.com`
- `salary`: admin and hr see the real value; hr can edit; all other roles don't see the column at all
- `created_at`: everyone can read, nobody can write

## Schema Visibility

Per-role GraphQL schemas hide unauthorized content:
- **Domain access**: Role sees tables only in its `domain_access` domains (`"*"` = all)
- **Column visibility**: Columns not in `visible_to` for a role are omitted from the SDL
- Unauthorized tables/columns do not appear in the schema

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

Masking is defined once per column — it is a property of the column, not the role. The `unmasked_to` field controls which roles bypass it.

| Mask Type | Supported Types | SQL Expression |
|-----------|----------------|----------------|
| `regex` | String (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Any | Literal value (NULL, 0, custom) |
| `truncate` | Date/Timestamp | `DATE_TRUNC(precision, col)` |

Masking is pushed into the SQL SELECT projection — the database returns masked data. Unmasked data never crosses the wire for masked roles. WHERE clauses use raw values (users can filter but not see unmasked data).

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

## ABAC Approval Hook

An optional external policy hook that fires before query execution. When configured, Provisa calls out to your policy engine with the user identity, roles, tables, columns, and operation. The response determines whether the query proceeds.

### Scoping

The hook only fires when the query touches a scoped table or source — zero overhead for everything else.

| Config | Effect |
|--------|--------|
| `auth.approval_hook.scope: all` | Every query triggers the hook |
| `sources[].approval_hook: true` | All tables on that source trigger the hook |
| `tables[].approval_hook: true` | That table triggers the hook |

### Protocols

Three transports are supported:

| Type | Use case | Config field |
|------|----------|-------------|
| `webhook` | Any HTTP-capable policy service (OPA, custom) | `url` |
| `unix_socket` | OPA or policy sidecar on same machine | `socket_path` + `url` |
| `grpc` | High-throughput co-located policy service | `url` (host:port) |

The gRPC transport uses the `provisa.auth.ApprovalService` contract defined in `provisa/auth/approval.proto`. Implement this service in your policy engine:

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

The gRPC channel is persistent — one channel per Provisa instance, reused across all calls to that hook endpoint.

### Request / Response

All three transports carry the same payload:

| Field | Type | Description |
|-------|------|-------------|
| `user` | string | Authenticated user identity |
| `roles` | string[] | User's Provisa roles |
| `tables` | string[] | Table IDs referenced in the query |
| `columns` | string[] | Columns selected in the query |
| `operation` | string | `"query"` or `"mutation"` |

The webhook and Unix socket transports exchange JSON. Response must include `approved` (bool) and optionally `reason` (string).

### Timeout and Fallback

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

On timeout or transport error, the `fallback` policy applies. A circuit breaker (default: open after 5 consecutive failures, half-open after 30s) prevents cascading failures from a slow hook endpoint.

### Configuration Example

```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```

## Secrets

Credentials use `${env:VAR_NAME}` syntax, resolved at runtime. Passwords are never stored in the config DB.
