# Security Model

Provisa enforces a multi-layered security model across every query language (GraphQL, SQL, Cypher) and every transport (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) Governance is applied uniformly — there is no query path that bypasses it. (REQ-002, REQ-266)

The layers apply in order. A request must clear each layer before the next is evaluated.

## Layered Model

### Layer 0 — Introspection filtering

The schema and catalog presented to a role contain only the tables in its `domain_access` list and the columns that pass per-column `visible_to` rules. (REQ-039) Objects outside a role's access are invisible at discovery time — they cannot be queried, autocompleted, or inferred to exist. (REQ-039) This applies to the GraphQL schema, SQL catalog, and the query editor's schema browser. (REQ-039, REQ-363)

See [Schema Visibility](#schema-visibility).

### Layer 1 — Public access

Tables in domains with no `domain_access` restriction are visible to all authenticated identities with no additional configuration. Zero friction for genuinely public data.

### Layer 2 — Domain access

Each role carries a `domain_access` list of domain IDs. A query that touches a table outside those domains is rejected before execution. (REQ-038, REQ-039) This is the coarse ownership boundary — an HR role cannot reach finance tables regardless of how the SQL is written. (REQ-002)

See [Rights Model](#rights-model).

### Layer 3 — Row-level security

After domain access is confirmed, per-table, per-role `WHERE` predicates are injected into every `SELECT` at execution time. (REQ-041, REQ-263) The predicates evaluate against raw data. A regional manager querying a shared orders table sees only their region's rows even on a `SELECT *`. (REQ-264)

See [Row-Level Security (RLS)](#row-level-security-rls).

### Layer 4 — Column visibility and masking

Columns with a `visible_to` list that excludes the requesting role are stripped from query output. (REQ-040, REQ-263) Columns with a masking rule have their values replaced — regex redaction, constant replacement, or truncation — before results leave the server. (REQ-263) Masking applies in all query languages and output formats. (REQ-263)

See [Column Permission Model](#column-permission-model) and [Column-Level Masking](#column-level-masking).

### Layer 5 — Predicate guard

Masked columns are rejected from `WHERE` and `HAVING` clauses. (REQ-263) Without this, a caller could infer the unmasked value by binary-searching it in a filter even though the output is masked. Rejection is enforced at query parse time, before execution. (REQ-531)

### Relationship governance (V002)

JOIN conditions in SQL must match a registered, approved relationship between tables. (REQ-001) Unapproved joins are rejected. Each relationship carries a human-readable reason and description — guidance for both users and autonomous agents about why a traversal path exists. This is governance policy, not a hard security boundary: Layers 2–5 hold regardless of join structure, so a deliberate circumvention does not expose data the role could not reach through two separate queries. Circumvention attempts are logged and auditable.

**Bypass mechanisms** — V002 can be bypassed only when two independent conditions are both true:

1. **Role flag** — `relationship_guard: false` on the role definition (default: `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Per-query opt-out** — the SQL contains the comment `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

Both must be present. The role flag alone does not bypass V002; the comment alone does not bypass V002.

**GraphQL path** — V002 is unconditionally skipped for GraphQL queries. SDL-defined relationships are pre-approved by design; the check is redundant and is not applied. [tool-verified: `provisa/api/data/endpoint.py:468`]

**SQL and Cypher paths** — V002 is active by default. Both `endpoint_dev.py` and `cypher_router.py` apply the two-condition check before calling `validate_sql`. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**pgwire path** — same two-condition check as SQL. The `--relationship-guard=false` comment is stripped from the query before execution; it does not reach the database. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

These layers compose. A role with domain access, RLS, and masked columns has all five constraints active simultaneously. Adding a new data source, column, or relationship does not require updating every rule — each layer is configured independently and applies automatically to any query that touches governed objects.

---

## Rights Model

Independently assigned capabilities with optional role hierarchy via `parent_role_id`. `admin` grants all. (REQ-042)

| Capability | Description |
|-----------|-------------|
| `source_registration` | Register data sources |
| `table_registration` | Register tables, columns |
| `create_relationship` | Define FK relationships |
| `access_config` | Configure RLS, masking |
| `query_development` | Execute queries |
| `write` | Invoke registered mutations (coarse gate; see Mutation Authorization) |
| `full_results` | Bypass sampling limits |
| `ignore_relationships` | Bypass relationship governance (V002) |
| `admin` | Superuser — grants all |

### Role Inheritance

Roles can inherit capabilities and domain access from a parent role via `parent_role_id`. (REQ-215) The hierarchy is flattened at startup — child roles merge their parent's capabilities and domain access with their own. (REQ-215)

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

Each column has a four-field permission model controlling read, write, and masking access per role. (REQ-042, REQ-249)

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

Write permission is enforced in the mutation pipeline. A role not in `writable_by` receives a 403 error when attempting to write to a restricted column. (REQ-033, REQ-034)

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

## Mutation Authorization

Registered mutations (remote GraphQL, OpenAPI, gRPC, Hasura) are gated by two independent checks. (REQ-867, REQ-868) A role may invoke a mutation only if it holds the global `write` capability AND appears in that mutation's `writable_by` list. (REQ-868) An empty `writable_by` is default-deny — no role can invoke it. (REQ-867)

Mutations are classified as writes by contract, not by caller declaration. (REQ-869) A `SELECT` that references a mutation-kind function is promoted to a write and subject to the same two-gate check, so a caller cannot invoke a mutation by disguising it as a read. (REQ-869) Reclassifying a mutation to read-safe requires the `access_config` capability and is recorded as a governance decision; there is no per-request opt-out. (REQ-870)

## Schema Visibility

Per-role GraphQL schemas hide unauthorized content: (REQ-039)

- **Domain access**: Role sees tables only in its `domain_access` domains (`"*"` = all) (REQ-039)
- **Column visibility**: Columns not in `visible_to` for a role are omitted from the SDL (REQ-039)
- Unauthorized tables/columns do not appear in the schema (REQ-039)

## Row-Level Security (RLS)

Per-table, per-role SQL WHERE clause injection. Applied after compilation, before execution. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

The filter is ANDed into the query's WHERE clause. Works for both queries and mutations (UPDATE/DELETE). (REQ-035, REQ-041)

## Column-Level Masking

Masking is defined once per column — it is a property of the column, not the role. The `unmasked_to` field controls which roles bypass it. (REQ-249)

| Mask Type | Supported Types | SQL Expression |
|-----------|----------------|----------------|
| `regex` | String (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Any | Literal value (NULL, 0, custom) |
| `truncate` | Date/Timestamp | `DATE_TRUNC(precision, col)` |

Masking is pushed into the SQL SELECT projection — the database returns masked data. (REQ-263) Unmasked data never crosses the wire for masked roles. (REQ-263) Masked columns are also blocked from `WHERE` and `HAVING` clauses (Layer 5 predicate guard) to prevent inference of the unmasked value through filtering. (REQ-263, REQ-531)

## Sampling

All roles see sampled results (default: 100 rows) unless they have `full_results` capability. (REQ-554) Controlled via `PROVISA_SAMPLE_SIZE` env var. (REQ-554)

## Audit Logging

Every query that touches a domain asset is recorded in the append-only `query_audit_log`. (REQ-596, REQ-613) Each row captures `tenant_id`, `user_id`, `role_id`, a SHA-256 hash of the query text, `table_ids`, `source`, `status_code`, `duration_ms`, and `logged_at`. (REQ-596) The query text is never stored verbatim — only its hash. (REQ-596)

The log is append-only at the database level: PostgreSQL rules block `DELETE` and `UPDATE`. (REQ-596, REQ-613) Two indexes — `(tenant_id, logged_at)` and `(user_id, logged_at)` — support tenant-scoped and per-user time-range compliance queries. (REQ-596, REQ-613)

When encryption is enabled, the query text hash column is stored encrypted and decrypted only on authorized admin reads. (REQ-689)

## Rate Limiting

Per-role rate limits are configured in `provisa.yaml`: max requests per second, max concurrent SSE subscriptions, and max concurrent Arrow Flight streams. (REQ-369) Limits are enforced at the API layer before compilation or execution; requests over the limit are rejected with HTTP 429 and a `Retry-After` header. (REQ-369)

The NL query service (`POST /query/nl`) has an independent limit via `nl.rate_limit` (requests per minute per role). Requests over the limit are rejected before any LLM call is made. (REQ-370)

Rate limit state lives in Redis (`cache.redis_url`) as a sliding-window counter — no per-instance state — so limits hold across all horizontal Provisa instances. (REQ-371)

## Authentication

Pluggable auth providers: (REQ-120)

| Provider | Token Type | Use Case |
|----------|-----------|----------|
| `none` | X-Provisa-Role header | Development |
| `firebase` | Firebase ID token | Production |
| `keycloak` | Keycloak JWT | Enterprise |
| `oauth` | OIDC JWT | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Testing |

Role mapping: identity claims → Provisa role via configurable rules. (REQ-120) The `assignments_source` field controls where role assignments come from: `claims` reads them from JWT token claims (default), `provisa` reads them from Provisa's internal assignment store. (REQ-551)

A superuser configured in `provisa.yaml` (username plus a password from an env secret) always receives the admin role and all capabilities regardless of the configured provider — a bootstrap path for initial setup. (REQ-125)

## ABAC Approval Hook

An optional external policy hook that fires before query execution. (REQ-203) When configured, Provisa calls out to your policy engine with the user identity, roles, tables, columns, and operation. The response determines whether the query proceeds. (REQ-203)

### Scoping

The hook only fires when the query touches a scoped table or source — zero overhead for everything else. (REQ-204)

| Config | Effect |
|--------|--------|
| `auth.approval_hook.scope: all` | Every query triggers the hook |
| `sources[].approval_hook: true` | All tables on that source trigger the hook |
| `tables[].approval_hook: true` | That table triggers the hook |

### Protocols

Three transports are supported: (REQ-246)

| Type | Use case | Config field |
|------|----------|-------------|
| `webhook` | Any HTTP-capable policy service (OPA, custom) | `url` |
| `unix_socket` | OPA or policy sidecar on same machine | `socket_path` + `url` |
| `grpc` | High-throughput co-located policy service | `url` (host:port) |

The gRPC transport uses the `provisa.auth.ApprovalService` contract defined in `provisa/auth/approval.proto`. Implement this service in your policy engine: (REQ-246)

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

The gRPC channel is persistent — one channel per Provisa instance, reused across all calls to that hook endpoint. (REQ-555)

### Request / Response

All three transports carry the same payload: (REQ-246)

| Field | Type | Description |
|-------|------|-------------|
| `user` | string | Authenticated user identity |
| `roles` | string[] | User's Provisa roles |
| `tables` | string[] | Table IDs referenced in the query |
| `columns` | string[] | Columns selected in the query |
| `operation` | string | `"query"` or `"mutation"` |

The webhook and Unix socket transports exchange JSON. Response must include `approved` (bool) and optionally `reason` (string). (REQ-246)

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

On timeout or transport error, the `fallback` policy applies. (REQ-247) A circuit breaker (default: open after 5 consecutive failures, half-open after 30s) prevents cascading failures from a slow hook endpoint. (REQ-556)

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

Credentials use `${env:VAR_NAME}` syntax, resolved at runtime. (REQ-557) Passwords are never stored in the config DB. (REQ-557)
