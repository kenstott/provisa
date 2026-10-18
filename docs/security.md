# Security Model

Provisa enforces a multi-layered security model across every query language (GraphQL, SQL, Cypher) and every transport (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) Governance is applied uniformly — there is no query path that bypasses it. (REQ-002, REQ-266)

The layers apply in order. A request must clear each layer before the next is evaluated.

## Layered Model

### Layer 0 — Introspection filtering

The schema and catalog presented to a role contain only the tables in its `domain_access` list and the columns that pass per-column `visible_to` rules. (REQ-039) Objects outside a role's access are invisible at discovery time — they cannot be queried, autocompleted, or inferred to exist. (REQ-039) This applies to the GraphQL schema, SQL catalog, and the query editor's schema browser. (REQ-039, REQ-363)

See [Schema Visibility](#schema-visibility).

### Layer 1 — Public access

A column published with scope `public` is served to every role of the org, including roles whose `domain_access` does not reach the table's domain; such a role sees the table with its public columns only. Nothing is public unless a table's registrar says so. (REQ-1959) See [Public columns](#public-columns).

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

**Bypass mechanisms** — V002 can be bypassed two ways. The first is a capability: a role holding `ignore_relationships` joins across relations the catalog does not cover. Among the seeded system roles only `modeler` holds it — the discovery role whose job is to determine the model rather than enforce it. (REQ-1297) `analyst` does not.

The second is a two-condition opt-out, where both must be true:

1. **Role flag** — `relationship_guard: false` on the role definition (default: `true`).
2. **Per-query opt-out** — the SQL contains the comment `--relationship-guard=false`.

The role flag alone does not bypass V002; the comment alone does not bypass V002.

**High-security mode pins the guard.** Under `security.mode: high` neither bypass applies: `ignore_relationships` is ignored, `relationship_guard: false` is ignored, and every join must exist in the approved relationship catalog. (REQ-693) This is deliberate redundancy — a production role that was granted the capability by mistake still cannot break out of the model.

**GraphQL path** — V002 is unconditionally skipped for GraphQL queries. SDL-defined relationships are pre-approved by design; the check is redundant and is not applied.

**SQL and Cypher paths** — V002 is active by default. Both `endpoint_dev.py` and `cypher_router.py` apply the two-condition check before calling `validate_sql`.

**pgwire path** — same two-condition check as SQL. The `--relationship-guard=false` comment is stripped from the query before execution; it does not reach the database.

---

These layers compose. A role with domain access, RLS, and masked columns has all five constraints active simultaneously. Adding a new data source, column, or relationship does not require updating every rule — each layer is configured independently and applies automatically to any query that touches governed objects.

---

## Rights Model

Independently assigned capabilities with optional role hierarchy via `parent_role_id`. `admin` grants all. (REQ-042)

| Capability | Description |
| ----------- | ------------- |
| `source_registration` | Register data sources |
| `table_registration` | Register tables, columns |
| `create_relationship` | Define FK relationships |
| `access_config` | Configure RLS, masking |
| `query_development` | Execute queries |
| `write` | Invoke registered mutations (coarse gate; see Mutation Authorization) |
| `full_results` | Bypass sampling limits |
| `ignore_relationships` | Bypass relationship governance (V002). Held by `modeler` only among the system roles, and ignored entirely in high-security mode |
| `admin` | Superuser — grants all |

### Role Inheritance

Roles can inherit from one parent role via `parent_role_id`, set in config or on the Security page as "Inherits from". (REQ-215) The chain is folded in at startup: a child holds the union of its ancestors' capabilities and domain access, every column, metric, function and webhook grant that names an ancestor, and the ancestors' RLS rules per table with the child taking precedence — the nearest role in the chain with a rule for a table supplies that table's predicate, and a rule saved for the child replaces the parent's for that table. (REQ-1677)

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

## Roles: granted, pseudo and meta

Three kinds of role work together in Provisa's access model.

### Granted roles

A granted role is assigned directly to a user. It holds rights: capabilities, domain access, masking entries, row filters. Inheritance (`parent_role_id`) folds an ancestor's rights into the child at startup. Rights only add; there is no subtraction. To restrict a reader, grant to a composite that names both the role and a condition, or place a mask entry before the permissive one. (REQ-215, REQ-1677)

### Pseudo-roles

A pseudo-role is derived from a user's attributes. The org declares which attribute types become roles (`claim_roles`). For each declared type `t` whose value `v` the user's attributes supply, the user holds the parent role `t` and its sub-role `t__v`. No value means neither role.

Attribute resolution matters. The org's own record of a member wins over what the identity provider supplies: both name the same attribute, the org's value is used, the provider's is marked overridden.

Pseudo-roles carry no rights and are never assigned by hand. A grant list names them exactly as it names a granted role; a request names them in `X-Provisa-Role` and acts as them. (REQ-2057)

Think of a JWT: its claims are facts about the person. Pseudo-roles turn chosen facts into roles, so a grant can address them without creating a role per combination.

### Meta-roles

When a request names more than one role, Provisa builds an ephemeral meta-role identified as `meta:<ids sorted, joined by +>`. It is a single child of every role in the named set. The whole pipeline then runs with it as an ordinary single role.

Rights are the union of every member's. Row filters are the OR of each member's resolved filter; if any member that reads a table has no filter, the meta-role has none either — no filter means all rows. A meta-role is never stored, listed, or exported. It is rebuilt from its members when the model changes. (REQ-1620)

Each row's columns are masked and shown according to the roles whose row filter admitted that row: of those roles, the least restrictive reading of the column applies, and a role that did not admit the row contributes nothing to how it is read. Masks apply where the table is read, before any join or aggregate, so an aggregate reads the masked values. Writes are checked the same way: a column is written on a row only where a role granting that column admits the row. (REQ-1620)

The audit record names every acting role, not the meta-role id.

### How a request names roles

A client sends one role, or a comma-separated set, in `X-Provisa-Role`. Each named role must be held. A role the identity is not assigned is refused by name.

Rights and data follow only the roles the request acts as: holding a role is not acting as it. One role is that role; two or more act as their meta-role. An acting role that brought every held role's rights with it would mean nothing, so a request gets least privilege and the audit record names exactly what acted.

Named pseudo-roles enter as qualifiers. A qualifier has no rights, domain access, masks or row filters of its own, so it is no member of the meta-role's rights: as a member with no masks it would unmask every column, and as one reading no table it would end every OR of row filters. It enters only the grants: a grant or mask entry naming `region__eu` reaches a request that names that qualifier alongside a granted role. (REQ-2057)

An MCP agent never acts beyond the roles it was given. A remote agent names only roles its token's principal holds. A local stdio agent names only roles within the set pinned in `mcp.role` (`PROVISA_MCP_ROLE`), and acts as that set when it names none; any other role is refused by name. (REQ-2075)

### What `+` means

`+` appears in two places with different meanings in each.

**In a grant entry.** `analyst+region__eu` in a `visible_to` or `to` list means the reader holds all of them — `analyst` AND `region__eu` simultaneously. An entry that is not a composite is a single role. Across a list's entries the rule is any one: a reader holds the grant if it holds any entry. AND-within, OR-across.

**In a meta-role id.** `meta:analyst+engineer` is the server's identifier for the meta-role of the set {analyst, engineer}. Its rights are the union of both. A client never names the meta-role id directly; it names the roles it holds, and the server builds the meta-role.

### ABAC column rules as plain RBAC

An attribute-based policy asks: "can this user, with these attributes, see this column?" RBAC asks: "which roles can see this column?" Composite grants and pseudo-roles make them the same question.

"Unmask this email only for an analyst in the EU" is a mask entry `{to: [analyst+region__eu], mask: null}`. Any reader holding both the `analyst` role and the `region__eu` pseudo-role reads the column unmasked. No policy language. No role per combination.

The trade-off is computation over enumeration. A small role catalogue keeps maintenance low, but "who can see this EU email?" becomes a computed answer: composite entries are evaluated against each user's assignments and claims. Provisa can answer that in both directions because the model is one definition. The column editor's Check access (`POST /admin/masks/check`) runs that evaluation for a chosen set of roles.

### Benefits

- An ABAC column rule becomes an RBAC grant entry.
- A small role catalogue instead of one role per combination.
- One definition coordinating every region's rules.
- Least privilege and an exact audit record per request.

### Best practices

- Keep the role catalogue as small as the organisation allows.
- Inherit within one axis (seniority, clearance level). Compose across independent axes (role + region, role + department).
- Put the most specific mask entries first — an earlier entry wins even when the reader holds a later one.
- Check effective access with `POST /admin/masks/check` before deploying a combination in production.

## Column Permission Model

Each column has a permission model controlling read, write, and masking access per role. (REQ-042, REQ-249)

### Three-Tier Visibility

| Tier | Condition | Result |
| ------ | ----------- | -------- |
| **Hidden** | Role not in `visible_to` | Column absent from schema and catalog |
| **Masked** | Role in `visible_to`; first mask entry it holds has a `mask` | Column visible, value replaced in output |
| **Unmasked** | Role in `visible_to`; first mask entry it holds has no `mask`, or it holds none | Full read access |

### Public columns

Every column has a `scope`, set in the model file, the admin API or the Tables page. The three scopes are ordered by who they serve: `restricted`, then `domain`, then `public`. A table editor (`table_registration` in the table's domain) may narrow a column's scope. Widening it — `restricted` to `domain` or `public`, `domain` to `public` — serves the column to more roles and takes the right that granting it takes: `column_grant` or `access_config` in the table's domain. A column being registered is compared with `domain`, so registering it `public` is a widening. On a column carrying a sensitive tag, any change of scope takes `sensitive_data`. A refused change names the column and the right. (REQ-1959, REQ-1943, REQ-1944)

| Scope | Served to |
| ------- | ----------- |
| `domain` (the default) | Roles that reach the table's domain, as `visible_to` says. An empty `visible_to` is every such role. |
| `public` | The same roles, and also roles that do not reach the table's domain. |
| `restricted` | Only the roles `visible_to` names. An empty `visible_to` is nobody. |

Publishing a column removes the domain condition for that column and nothing else:

- A role outside the domain is served the table with its public columns only. The table's other columns, and tables that publish nothing, stay out of its schema, its catalog and its SQL.
- The column's own `visible_to` still decides who: a public column granted to one role is public to that role alone.
- Masks, row filters, row limits and the relationship guard apply as they do inside the domain. With no row filter for the role, the published columns are read unfiltered; a row filter on the table or its domain still applies.
- A relationship is served when both of its ends' columns are.
- A parameter column of an API table (a path or query argument) is not data and publishes nothing.
- The `ops` domain needs an explicit grant for every column (REQ-1133); nothing in it is served across domains, whatever its scope.

The rule is the same on every surface, because each one reads the role's compiled schema: GraphQL, SQL (HTTP, pgwire, Flight, JDBC), Cypher and Bolt, gRPC, REST and JSON:API, the Flight and MCP catalogs and the admin listings. A scope that is not one of the three values is refused when a column is saved.

### Write Grants

Reading a column is controlled by `visible_to` (empty means all roles can read). Writing is controlled per table by `write_grants`, not per column. A table holds no grants by default, so no role can write it. (REQ-2074)

A write grant states what one role may write to one table by one operation (`insert`, `update` or `delete`):

| Field | Purpose |
| ------- | --------- |
| `columns` | The columns an `INSERT` may list or an `UPDATE` may set |
| `rows` | The rows an `UPDATE` or `DELETE` may touch (a SQL predicate) |
| `check` | What every row an `INSERT` writes, or an `UPDATE` leaves behind, must satisfy (a SQL predicate) |
| `presets` | Columns the grant sets itself, from a session variable, a literal or the time of the write. The statement may not supply them. |

Leave `rows` or `check` out and the grant takes the role's read row filter on the table, so a role writes the rows it reads. `"TRUE"` means every row.

One admission decides every write on every surface, in this order: the role holds the `write` right; it holds a grant for the operation on the table; each column the statement writes is in the grant; the presets are added; `rows` is ANDed into the `WHERE`; and `check` is evaluated on the new rows before anything is sent to the source. A refusal names the table, role and operation. (REQ-2074)

Wider statements need wider grants. `TRUNCATE` needs a `delete` grant over every row. `MERGE` needs an `insert` and an `update` grant over every row. `INSERT ... ON CONFLICT DO UPDATE` needs an `update` grant over every row with no presets. `COPY` needs an `insert` grant with no presets.

GraphQL offers each role a mutation field for exactly the operations it holds grants for, taking the grant's columns. A child role inherits its parent's grants, its own first. A meta role holds the fold of its members' grants: the union of columns, the OR of `rows` and `check`, and only the presets all members share. In the admin UI, a table's **Writes** section lists and edits its grants.

### Example

```yaml
tables:
  - source_id: hr-pg
    domain_id: people
    schema: public
    table: employees
    columns:
      - name: id
        visible_to: []                # all can read
      - name: email
        visible_to: [admin, analyst, viewer]
        masks:
          - to: [admin]               # admin sees raw value (no mask)
          - to: ["*"]                 # everyone else sees it masked
            mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}
      - name: salary
        visible_to: [admin, hr]
        masks:
          - to: [admin, hr]           # admin and hr see real value
          - to: ["*"]                 # all other visible roles see 0
            mask: {type: constant, value: 0}
      - name: created_at
        visible_to: []                # all can read
    write_grants:
      - role: admin
        operation: update
        columns: [email]
        rows: "TRUE"
      - role: hr
        operation: update
        columns: [salary]
      - role: hr
        operation: insert
        columns: [email, salary]
        presets:
          - {column: created_at, source: now}
```

In this example:

- `email`: admin sees `alice@example.com` and can update it on any row; analyst and viewer see `a***@example.com` and cannot write
- `salary`: admin and hr see the real value; hr can update it on the rows hr can read, because the grant states no `rows`; every other visible role sees `0`
- `created_at`: everyone can read it, no role supplies it, and an hr insert sets it to the time of the write

## Mutation Authorization

Registered mutations (remote GraphQL, OpenAPI, gRPC, Hasura) are gated by two independent checks. (REQ-868) A role may invoke a mutation only if it holds the global `write` capability AND the command is assigned to it by `visible_to` (an empty list assigns it to every role). (REQ-868) A command has no `writable_by` list; the configuration refuses one by name. [tool-verified: `provisa/security/mutation_authz.py` `command_assigned`, `admit_command`; `provisa/core/models.py` `_one_role_list`]

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

A `current_setting('provisa.<name>')` term resolves against the session variables the request binds: `user_id` from the authenticated identity, `role` from the acting role, and every scalar claim of the token under its lower-cased name, dashes as underscores. A Hasura session variable keeps its own name: `X-Hasura-User-Id` is `hasura_user_id`, read from the token's `x-hasura-user-id` claim (top level, or inside the Hasura claims object such as `https://hasura.io/jwt/claims`) or recorded for the member as the attribute `hasura_user_id`. An imported Hasura filter on `X-Hasura-User-Id` therefore reads `provisa.hasura_user_id`. `provisa.user_id` is always the signed-in identity; no claim replaces it. A role's configured `session_vars` are the constants underneath; the request's bindings overlay them. A role inheriting from another takes its ancestors' constants under its own, the nearest role's value for each name. `user_id`, `role`, `region` and `org` are bound by the request or the node and cannot be configured as constants. In unsecured mode a `x-provisa-session-<name>` header binds `<name>`. A variable bound nowhere resolves to NULL and matches no rows. (REQ-1682)

## Column-Level Masking

Masking is defined as an ordered list on each column. The first entry the reader holds decides what it sees. (REQ-2059, REQ-263)

| Mask Type | Supported column types | SQL expression |
| ----------- | ----------------------- | --------------- |
| `regex` | String (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Any | Literal value (`NULL`, `0`, a custom string) |
| `truncate` | Date/Timestamp | `DATE_TRUNC(precision, col)` |

Each entry has a `to` list naming the readers it targets, and an optional `mask`. An entry with no `mask` means unmasked for those readers. `*` in `to` matches every reader.

The mask expression is pushed into the SQL SELECT projection. The database returns the masked value; unmasked data never crosses the wire for masked roles. (REQ-263) Masked columns are also blocked from `WHERE` and `HAVING` clauses (Layer 5 predicate guard) to prevent value inference through filtering. (REQ-263, REQ-531)

**A mask covers every name of the column, not a related column.** A column read by its alias or its physical name is masked alike. A mask does not hide a key that an approved relationship joins to a column the role can see: the join compares the real values, so the related column shows what the masked key holds. Mask the related column too, or don't approve the relationship for that role. (REQ-263, REQ-019)

**No entry held means unmasked.** A reader that holds no entry reads the column unmasked. End a list with a `*` entry when every reader should have some mask applied. The column editor warns when a list lacks a `*` entry (`unlisted_unmasked`).

**Deny by order.** An earlier entry wins even when the reader also holds a later, more permissive one. To ensure contractors always see a masked salary regardless of their other roles, put the contractor entry before the unmasked entry. A granted role never loses what its parent grants through inheritance, but a mask entry placed earlier in the list applies first — that is how restriction works here.

**Meta-roles.** A meta-role's chain includes every member's roles, their ancestors, and any named pseudo-roles. An entry granted to any member reaches the meta-role. An entry before it, held by another member, still applies first.

**Checking access.** `POST /admin/masks/check` accepts a mask list and an optional set of roles, and returns which entry applies for those roles plus any entries no reader can reach. Gated by `masking_config`.

```yaml
# email column: analyst+region__eu sees raw; everyone else sees masked
masks:
  - to: [analyst+region__eu]          # composite: analyst AND in EU region
  - to: ["*"]                         # all others
    mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}

# hire_date column: hr sees raw; all other visible roles see year only
masks:
  - to: [hr]
  - to: ["*"]
    mask: {type: truncate, precision: year}
```

## Governing Action Responses

A tracked function's or webhook's response is governed like a table's rows. (REQ-1679) The rows an action returns are bound as a relation whose columns are the action's declared output contract — `output_columns` for a function, `inline_return_type` for a webhook, or the object properties of a function's array `return_schema` — and that relation runs through the same governance stage every table read runs through, over a VALUES CTE holding the rows. (REQ-1679) Nothing about RLS, masking or visibility is reimplemented for actions.

Three things apply, in the same order as for a table. (REQ-1679)

- A row filter: the role's RLS rule saved against the action by name (`upsertRlsRule` with `actionName`), or, when the action has none of its own, the domain rule of the action's domain. The predicate is validated against the contract at save the way a table rule is. (REQ-1676)
- Column visibility: a contract column that declares `visible_to` is dropped for a role the list does not name; a column that declares none is part of the action's public shape and stays.
- Masks: a contract column carries `masks`, the same ordered mask list a table column carries. (REQ-2059)

Role inheritance resolves the chain for all three. (REQ-1677) A response that returns a column outside the declared contract is refused with a 502 rather than passed through ungoverned. An action that declares no column contract returns a scalar and has nothing to bind; it is returned as is. The admin test-invoke reports the filters, exclusions and masks that applied, and the governed statement is written to the query audit log.

## Sampling

All roles see sampled results (default: 100 rows) unless they have `full_results` capability. (REQ-554) Controlled via `PROVISA_SAMPLE_SIZE` env var. (REQ-554)

## Audit Logging

Every query that touches a domain asset is recorded in `query_audit_log`. (REQ-596, REQ-613) The audit log belongs to the org, not to the platform: the platform's own registry of orgs, users and invitations holds no audit records. (REQ-1922) Each org's `query_audit_log` is a table in that org's own schema, in the store the org names for its record in each region it selects. An org that declares no regions keeps that schema in the deployment's control-plane database, alongside its model.

Each row captures `tenant_id` (the org), `user_id`, `role_id`, a SHA-256 hash of the query text, the query text itself encrypted (REQ-689), `table_ids` and `table_names`, `source`, `status_code`, `duration_ms`, the route and row count, what was enforced on the statement, and `logged_at`. The query text is encrypted with the org's key when the org has set one, otherwise with the deployment's key (REQ-1574). A deployment that configures no encryption provider stores it unencrypted. The text is never exposed in the queryable ops tables, and is decrypted only on authorized admin reads.

The log is append-only at the database level: PostgreSQL rules turn `DELETE` and `UPDATE` on `query_audit_log` into no-ops. (REQ-596, REQ-613) Two indexes, `(tenant_id, logged_at)` and `(user_id, logged_at)`, support org-scoped and per-user time-range compliance queries. (REQ-596, REQ-613)

Retention: rows are kept indefinitely. Provisa has no retention setting and never purges the audit log; because `DELETE` is a no-op, removing old rows means an operator dropping or archiving the table's data outside Provisa.

## Rate Limiting

Per-role rate limits are configured in `provisa.yaml`: max requests per second, max concurrent SSE subscriptions, and max concurrent Arrow Flight streams. (REQ-369) Limits are enforced at the API layer before compilation or execution; requests over the limit are rejected with HTTP 429 and a `Retry-After` header. (REQ-369)

The NL query service (`POST /query/nl`) has an independent limit via `nl.rate_limit` (requests per minute per role). Requests over the limit are rejected before any LLM call is made. (REQ-370)

Rate limit state lives in Redis (`cache.redis_url`) as a sliding-window counter — no per-instance state — so limits hold across all horizontal Provisa instances. (REQ-371)

## Query Complexity Limit

A rate limit caps how many requests a role sends. The complexity limit caps what one statement may ask for. (REQ-1174)

Every statement has a complexity score: the relations it reads, its joins, the columns it selects (a `*` counts as the columns it stands for), and the queries nested inside it. A relation read from a remote API source (OpenAPI, remote GraphQL, remote gRPC) counts as 10, because each read spends a budget the remote system sets for everyone using that source.

The limit is `max_query_complexity` in a role's `rate_limit`. The org-wide setting `limits.max_query_complexity` is a ceiling for every role: a role may set a lower limit, and a higher one has no effect. Neither is set by default.

```yaml
roles:
  - id: analyst
    rate_limit:
      max_query_complexity: 200
```

The score is measured after a request is parsed and before it is governed or run, so one limit holds on every interface a statement can arrive on: GraphQL, SQL over HTTP, pgwire, Arrow Flight, gRPC, Cypher, JSON:API and MCP. A statement over the limit is refused with its score, the limit and what it asked for. HTTP interfaces answer 413. The refusal is recorded as a policy denial.

## Authentication

Pluggable auth providers: (REQ-120)

| Provider | Credential | Use Case |
| ---------- | ----------- | ---------- |
| `none` | X-Provisa-Role header | Development only |
| `basic` | Local accounts (bcrypt) + session JWT | Self-contained deployments |
| `firebase` | Firebase ID token | Production |
| `keycloak` | Keycloak JWT | Enterprise |
| `oauth` / `oidc` | OIDC JWT | PingFed, Okta, Azure AD, Auth0 |
| `ldap` | Directory username and password (LDAP or Active Directory) | Enterprise directories (REQ-1265) |
| `saml` | SAML 2.0 signed assertion, then a session JWT | Enterprise single sign-on (REQ-1265) |
| `simple` | bcrypt + JWT | Testing |

`ldap` signs a user in by search-then-bind: Provisa binds as a service account, finds the one entry the username names, then binds as that entry with the presented password, so the directory checks it. The user's groups become their roles. The account and its groups are read again each time a session token comes back, so a user removed from the directory, or from a group, loses that access before the token expires. `ldaps://` and StartTLS are both supported.

`saml` is SP-initiated Web Browser SSO. The response's XML signature is verified against the identity provider's metadata, and claims are read only from the signed element. Issuer, Audience, the ACS URL, expiry and `InResponseTo` are all checked. Unsolicited (IdP-initiated) responses, encrypted assertions, documents with a DOCTYPE, multiple assertions and SHA-1 signatures are refused. A SAML session is not checked again with the identity provider: a user removed there keeps access until the session token expires.

Role mapping: identity claims → Provisa role via configurable rules. (REQ-120) The `assignments_source` field controls where role assignments come from: `claims` reads them from JWT token claims (default), `provisa` reads them from Provisa's internal assignment store. (REQ-551) A multi-tenant deployment (`multitenancy: true`) grants roles itself, per org, so it must use `assignments_source: provisa`: with `claims` it does not start, and a settings change that would produce that combination is refused. A single-tenant deployment may use either.

A superuser configured in `provisa.yaml` (username plus a password from an env secret) always receives the admin role and all capabilities regardless of the configured provider — a bootstrap path for initial setup. (REQ-125)

### Surfaces and credentials

Every surface authenticates through the same provider contract, so a credential that works on one works on all of them wherever the protocol can carry it. (REQ-124, REQ-1263) This table is the single reference; the per-surface docs do not restate it.

| Surface | Password | Provider token | Personal access token | Client certificate (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | via terminating proxy |
| pgwire | password field (cleartext or SCRAM) | password field, OIDC deployments | password field | yes |
| Bolt | `basic` scheme | `bearer` scheme | `bearer` scheme | yes |
| Arrow Flight | — | `token` in the handshake or ticket payload; `authorization: Bearer` call header on `list_flights`, `get_flight_info` and `get_schema` | same | yes |
| gRPC | — | `authorization` metadata | `authorization` metadata | yes |
| Airport (DuckDB `airport` extension, opt-in) | — | `authorization: Bearer` (the secret's `auth_token`) | same | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | via terminating proxy |

The DuckDB `airport` extension's only authentication option is the bearer `auth_token` of its secret, so a DuckDB client presents a provider token or a personal access token there and acts as the role its identity resolves to. A client that can set call headers may request one held role, or a comma-separated set of them, in `x-provisa-role`. (REQ-1120, REQ-1263)

A refusal says which kind it is on every one of these surfaces. A missing, rejected or unusable credential is an authentication refusal: HTTP 401, gRPC `UNAUTHENTICATED`, Arrow Flight and Airport `Unauthenticated`. A valid credential asking to act as a role its identity does not hold is an authorization refusal, with the role named: HTTP 403, gRPC `PERMISSION_DENIED`, Arrow Flight and Airport `Unauthorized`. (REQ-273, REQ-1263) One exception is in the Arrow client library, not the server: a refusal of Flight's `get_schema` call reaches a Python client as a generic Arrow error carrying the server's reason, without its kind; `list_flights`, `get_flight_info` and `do_get` deliver the kind.

Where a cell reads `—` the protocol carries no username field to pair a password with; the token forms cover it. pgwire is the mirror case: the startup packet has one secret field and no scheme, so what the secret *is* picks the method — a PAT is recognized by its prefix, the secret is read as a bearer token when the configured provider is a token provider, and anything else is a password. The choice is made once — a credential the selected validator refuses is not retried against another.

The matrix is enforced by `tests/unit/test_auth_surface_conformance.py`, which drives each surface's real validation entry point and fails when a new surface is added without a row.

### Personal access tokens

A PAT is a long-lived bearer secret a user mints for a client that cannot complete an interactive login — a script, a BI tool, a driver. (REQ-1263) It carries its own org and role, and every surface resolves it through the same validator, so no surface needs to know what a PAT is.

The wire form is `provisa_pat_` followed by 43 url-safe base64 characters. The prefix is what routes a presented secret to the token store instead of the identity provider, and it makes a leaked token greppable in logs and repositories.

- **Storage** — only the SHA-256 of the secret is kept. The secret itself is shown exactly once, at creation, and cannot be recovered. The listing carries the display prefix and the lifecycle timestamps, never a working credential.
- **Issuance and revocation** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, plus the self-service section on the user's own profile in the admin UI. Minting and revoking a credential is the token holder's act.
- **Attribution** — a validated PAT resolves to its owner's account: user id, email and display name. An audit row or usage report written under a PAT therefore names the person, not the credential. Which of that person's tokens acted is carried separately, in `raw_claims["token_name"]`.
- **Expiry** — a token may carry an expiry; an expired token is refused at validation. Deleting a user's membership revokes their tokens with it.

### SCRAM-SHA-256 on pgwire

Under the `basic` provider, setting `auth.scram: true` makes pgwire advertise SASL (authentication code 10) with the `SCRAM-SHA-256` mechanism, so a password is proved rather than sent. (REQ-1394) Channel binding (`SCRAM-SHA-256-PLUS`) is not offered.

SCRAM needs an RFC 5802 verifier, which cannot be derived from a bcrypt hash. A verifier is written whenever a password passes through in plaintext — signup, login, password change, admin reset — so a deployment that turns SCRAM on collects verifiers as its users next authenticate, and each user's first SCRAM connection follows their next password entry. A user with no verifier yet is answered with a mock exchange indistinguishable from a real one, so the wire does not reveal who has migrated.

### Mutual TLS

Client-certificate verification moves the first check to the TLS handshake: a caller without a certificate signed by the deployment's CA never reaches the credential layer. (REQ-1228) It is available on pgwire, Bolt, gRPC and Arrow Flight — the four transports that terminate their own TLS.

| Variable | Meaning |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | PEM bundle of the CA(s) permitted to sign client certificates |
| `PROVISA_MTLS_MODE` | `required` (the default once a CA is set) or `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | When true, the certificate's common name must equal the username the connection then authenticates as |

Per-protocol overrides follow the same naming as the TLS settings. Nothing is inferred: a mode set without a CA refuses to start, and an unrecognized mode refuses to start rather than being read as the safest neighbour — a deployment that believes it requires client certificates and does not is worse off than one that fails to start.

### TLS versions

Every listener Provisa terminates (the HTTP API and UI, pgwire, Bolt, MCP, gRPC and Arrow Flight) accepts TLS 1.2 and TLS 1.3, and refuses TLS 1.0 and 1.1. Provisa sets no protocol version or cipher list of its own. The HTTP, pgwire and Bolt listeners use Python's `ssl` server defaults (minimum TLS 1.2). gRPC and Arrow Flight use gRPC's defaults (minimum TLS 1.2). The versions are not configurable.

### Login throttling

Password guessing is protocol-independent: the same account can be hammered over HTTP, pgwire and Bolt. The counter therefore lives at the credential-validation layer, not on any one surface, so a lockout earned anywhere is enforced everywhere. (REQ-1393)

It is on by default — five failures in five minutes locks the subject out for fifteen minutes — and is tuned under `auth.login_throttle`. A locked-out subject is refused before the credential is examined at all, and a successful authentication clears that subject's history.

The key is the principal the protocol carries. A bearer-only surface carries no principal, so the key is a digest of the credential itself; what that stops is one bad token being replayed without limit. The store is per process, so a deployment running several API workers allows up to `max_attempts` per worker — the throttle is a brake on guessing, not a distributed quota.

### Role probing

Naming a role the caller does not hold is treated as a detected security event of high severity, on every surface: the HTTP API, MCP, pgwire, Bolt, Arrow Flight, the airport Flight service and gRPC. A legitimate client names the roles it was given; one that names another is either broken or probing. (REQ-2076)

The request is refused. The caller is told only "not permitted", nothing about whether the role exists, and the connection or session ends where there is one: the pgwire connection, the Bolt session, the gRPC call, the MCP session. The one exception is a signed-in session on the HTTP API, which keeps the refusal naming the role.

What else happens depends on the credential and the transport:

| Credential | Transport | Suspended |
|---|---|---|
| Personal access token | any | the token |
| Token signed by an external identity provider | MCP, pgwire, Bolt, Flight, gRPC | the principal's token access to the org |
| Token signed by an external identity provider | the HTTP API | nothing |
| A session or password this deployment checks | any | nothing |
| A local MCP session (no bearer) | MCP | nothing; the event goes to the operator's log |

The HTTP API is the exception because the browser presents the same bearer token an API client would, so the server cannot tell them apart, and a tab still naming a role the user has since lost (after an administrator changes their roles, or an org switch) is routine. MCP and the other transports are never a browser session. There the event is refused, recorded and notified like any other, and only the suspension is withheld.

A suspended token is refused as a revoked one is. A principal whose token access is suspended is refused on MCP, pgwire, Bolt, Flight and gRPC in that org, and can still sign in to the UI. Only an administrator lifts a suspension; nothing lifts it on a timer. Team → Suspended access lists each suspension with the event behind it and lifts it (`GET /admin/orgs/{org}/suspensions`, `POST /admin/orgs/{org}/suspensions/lift`).

Each event is written to the org's admin trail (action `security.role_probe`) with the principal, the credential id, the client address, the transport, the endpoint or tool, the role named, the roles held, the time, and the ids of the principal's most recent audited statements. Their text stays in the query audit log and is read under the `view_governance` right.

The org's administrators are emailed where the deployment sends mail. The event is also posted as JSON to the operator's security webhook when `security.webhook_url` is set. What leaves the org names the principal, credential id, transport, endpoint, role named, time and severity, with a link to the admin view; it carries no statement and no held role. A notice that cannot be delivered is logged and never undoes the refusal or the suspension.

### Naming the org

Under multitenancy every request names the org it is for. Belonging to exactly one org does not name it: a request that names none is refused, and the refusal says what to send. (REQ-1235) Each surface names an org this way:

| Surface | How a client names the org |
| ------- | -------------------------- |
| HTTP (UI, REST, GraphQL) | The org's own hostname, `acme.provisa.dev`, or the `X-Org-Provisa` header on the control-plane host |
| pgwire | The database name (`psql -d acme`, a BI tool's database field), or the org's own hostname dialed over TLS |
| Bolt | The database name, `<org>.provisa_<role>` (or `<org>.provisa_ops_<role>`), or the org's own hostname dialed over TLS |
| Arrow Flight | `"org"` in the ticket |
| gRPC | The `x-provisa-org` metadata header |
| MCP (HTTP transport) | The org's own hostname, or the `X-Org-Provisa` header |
| Python client | `org=` on `ProvisaClient`, `connect()` and `adbc_connect()`; `?org=` in a SQLAlchemy URL |

A pgwire client that dials a hostname over TLS sends it in the TLS ClientHello, and Provisa reads the org from there (REQ-1234). When the hostname and the database name both name an org and the two differ, the connection is refused; neither is preferred. The database name `provisa`, the default, names no org, and a BI tool connected to an org shows
that org as its database. On Bolt, `SHOW DATABASES` lists one `<org>.provisa_<role>` per org the
user belongs to; a bare `provisa_<role>` is refused under multitenancy, and a session stays in the
org it first named.

A name is a request, not a grant. It reaches the same resolver everywhere, which refuses an org the authenticated principal is neither a member of nor holds the cross-org right for.

A personal access token is issued for one org and names it itself. It opens that org only: a request naming a different org is refused, even when the token's owner belongs to both.

A single-tenant deployment has no org to name. No surface reads a hostname, a database name or a header for one there.

## Secrets across organizations

An organization's secrets are read only by that organization. In Provisa's own store each secret is held under its organization. In a central service (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) on a deployment of many organizations, every read is made inside the reading organization's prefix, and a reference that would leave it is refused. (REQ-1557) The prefixes and the steps to move existing secrets under them are in [Secrets](secrets.md#one-organizations-secrets-in-a-central-service).

## Telemetry across organizations

On a deployment shared by many organizations, the ops signals (`"ops"."traces"`, and on the Trino engine the metrics and logs beside it) are one store every organization's work lands in. Each span, metric point and log record records the organization it was written for, kept as `tenant_id`, and every read of the signals answers only the reading organization's rows, whatever the role. A record made outside any organization's work (the server's start, for one) belongs to none and is read by none. The platform administrator holds no data access in any organization and reads none of them through Provisa. (REQ-1349, REQ-1327)

The ops signals are kept indefinitely on both engines, as the audit log is: compaction rewrites the files they are stored in and removes none of their rows. On Trino they are the `otel` catalog's Iceberg tables. On a native engine (DuckDB) the server lands its own trace records as Parquet files in its telemetry folder and the engine reads them under the same name, `"ops"."traces"`, with the refresh lineage view `"ops"."mv_refresh_lineage"` over them; both are read under the same per-organization rule. (REQ-862)

The signals are kept in the clear in their store. A workload that needs more than a per-organization boundary inside one deployment runs on a dedicated deployment; a highly sensitive or regulated one is self-hosted.

## Deployment Hardening

### Store credentials and where the stores listen

The bundled stores have no default credentials. The PostgreSQL control plane, Redis, MinIO, Grafana's admin account and the Trino cluster's shared secret each take a value generated for the deployment:

- The launchers (`provisa start`, the desktop and server installers) generate them once and keep them in `~/.provisa/store-credentials.env` (mode 600). A value already in the environment is used as given, and a secondary node, which uses the primary's stores, refuses to start without the primary's.
- The Terraform modules generate them with `random_password` and give the same values to every node. They are held in the Terraform state, so protect the state as you would the passwords.
- The Helm chart sets none. It requires a Secret you create, named by `credentials.existingSecret`, holding `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` and `trino-shared-secret`.

Redis requires its password, and the PostgreSQL role's password is set again at each start so a volume made earlier follows the deployment's current one. The stores, the federation engine and the telemetry services publish their ports on one address, `PROVISA_STORE_BIND_IP`: `127.0.0.1` on a node of its own, or the primary's private address when secondaries connect to it. Grafana's admin account takes the generated password; the compose stack lets anonymous visitors view dashboards, and the Helm chart admits no anonymous visitor.

### Transport encryption

Every listener Provisa terminates serves TLS (see [TLS versions](#tls-versions)). A server node uses the certificate in `PROVISA_TLS_CERT`/`PROVISA_TLS_KEY`, or generates a self-signed one. The Helm chart serves TLS by default (`tls.enabled: true`) from the `kubernetes.io/tls` Secret `tls.secretName` names.

### The embedded PostgreSQL

The desktop and native tiers run PostgreSQL embedded. On macOS and Linux it listens on a unix socket inside its data directory (mode 700), which only the account that owns the directory reaches. Over TCP (Windows, where it listens on 127.0.0.1), every connection must give a password generated for the data directory and kept in it (`provisa.password`, mode 600). It is never started as root.

### The federation engine (Trino)

- **Between nodes.** A coordinator and its workers authenticate each other with the deployment's generated shared secret (`internal-communication.shared-secret`). Encrypting their traffic is an operator option, off by default: off, it stays inside the deployment's private network or cluster. Turn it on where a compliance or zero-trust requirement asks for encryption inside the deployment. Trino documents it as slowing shuffle-heavy queries (distributed joins, aggregations, window functions) by 10% to over 100%. On a compose deployment it is the setting `engine.trino_internal_tls` (Deployment settings → Engine), with the engine network and the coordinator's pinned address beside it, and it applies at the next start. Under the Helm chart it is the value `trino.internalTls`; the chart generates the CA and certificate the nodes use, keeps them across upgrades, and addresses each node by its pod DNS name. The hosted platform's engines are single-node coordinators, with no traffic between nodes, and refuse it. (REQ-2072)
- **Spooled data.** Fault-tolerant execution encrypts the exchange data it spools with a key generated for each query and discarded when it ends (`fault-tolerant-execution.exchange-encryption-enabled=true`, set explicitly). Legacy spill to disk is not used.
- **Organizations on one engine.** Each organization's catalogs are its own, named for the organization, and each organization's statements run in its own resource group.
- **Catalogs, and the credentials in them, live only in the engine's memory.** Provisa creates every Trino catalog at runtime, and each holds its source's connection properties, passwords included. Every engine Provisa runs keeps them in memory (`catalog.store=memory`): no catalog is written to the engine's disk, and none is mounted from a file. A coordinator that restarts comes back with no catalogs; each Provisa process notices on its next query, and the engine watcher within a minute, and recreates the catalogs it serves. A Kafka source with SASL credentials needs one file: Trino's Kafka connector reads its client settings only from a file, so Provisa writes them, password included, to `kafka-client/<catalog>.properties`. The file is readable by Trino's user alone (mode 0600 in a 0700 directory). In the container stack the directory is a memory-backed volume the API and the engine share. On the hosted platform's engines it is the engine's own Kubernetes Secret, held in the node's memory and readable by Trino's group alone (mode 0440). Either way the file never reaches a disk. On the hosted engines a Kafka source must name a schema registry: one whose topics' columns only the model declares is refused by name when it is saved or applied, since its table descriptions would need a directory the running engine cannot be given. Starting the stack removes the catalog and Kafka client files earlier releases left on disk.

### Sources on a shared deployment

On a deployment that holds many organizations, an organization's sources may reach only public addresses: never the platform's private network, where its control plane, cache and object store are. The check applies when a source is saved, by the UI, the admin API or a configuration apply, and again at every connection and every HTTP redirect. A demo sandbox's sources are further held to the address they were checked at. The deployment's own organization, whose sources the operator configures, is not held.

## Encryption at Rest and the Organization's Own Keys

Encryption under an organization's own key is available, and it is off until the organization turns it on. Nothing is encrypted under a key of the organization's unless an organization admin creates one.

**The organization's encryption key** (REQ-1574) wraps the organization's secrets, its encrypted columns and its audit query text. An organization admin creates it under **Admin → Org Secrets → Org Key**, or with `PUT /admin/org-encryption`: with no body Provisa generates the key, which is never shown, and a `key_b64` body supplies 32 bytes of your own. `GET /admin/org-encryption` reports only its fingerprint. Without an organization key, the organization's data is wrapped with the deployment's key (`encryption.provider` in `provisa.yaml`). A deployment that configures no encryption provider stores encrypted columns and audit query text unencrypted; secrets are never stored in the clear (see [Secrets](secrets.md#provisas-built-in-store)).

**The organization's folder** (REQ-1992) holds its uploaded and landed files, its engine's spill files and its temporary files. An organization admin turns its encryption on under **Admin → Org Secrets → File folder encryption**, or with `PUT /admin/org-files` (`{"encrypted": true, "key_reference": "${secret:NAME}"}`). `POST /admin/org-files/generate-key` answers a new 256-bit key once, to be stored in the vault. The key stays in the organization's own vault (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault or HashiCorp Vault): Provisa stores only the reference, and gocryptfs keeps the folder's files encrypted on disk under the key. This needs FUSE, so a Linux host or container with `/dev/fuse`; on macOS the setting is refused.

On a self-hosted deployment, which typically runs one organization on its own engines, turning both on encrypts that organization's secrets, files, spill files and temporary files under its keys:

- The DuckDB engine an organization runs spills to the organization's folder.
- Rows a row filter fetches and uploaded code are staged in the organization's folder.
- Trino's fault-tolerant execution encrypts spooled exchange data with a key generated for each query and discarded when it ends (`fault-tolerant-execution.exchange-encryption-enabled=true`). Provisa does not turn on Trino's legacy spill to disk.
- Trino keeps the organization's catalogs, with their credentials, in memory and writes none to disk (`catalog.store=memory`); a Kafka source's SASL client settings are in a file only Trino's user reads, in memory in the container stack (see [The federation engine](#the-federation-engine-trino)).
- A PostgreSQL federation engine writes its temporary files where that PostgreSQL server keeps them, under the server's own storage encryption, not the organization's folder.

### Shared and dedicated hosted platforms

The shared hosted platform isolates organizations logically. One deployment serves them all, and each organization's data stays its own through governance, its own schemas, catalogs and resource groups on the engine, its own keys, per-query keys for spooled engine data, and the refusal of its sources at the platform's private addresses. It is recommended for experimentation and small businesses. A dedicated hosted platform is physically separated: its own machines, network and stores, serving one customer. For highly sensitive or regulated workloads, self-host Provisa inside your own boundary, where your own operators hold every key and every host. The hosted platforms do not carry a SOC 2 report today. We are exploring SOC 2 and will provide updates. If you require SOC 2, we recommend self-hosting.

### Python in the browser

Explore's Python and Notebook tabs run Python in the visitor's browser. Every read it makes goes to the server as the signed-in user, with their acting role, organization and environment, and is governed exactly as the same statement from any other client.

## High-Security Mode

`security.mode: high` in `provisa.yaml` asserts one guarantee: the Provisa backend never handles plaintext data. (REQ-693) Every column that matters is encrypted at the source, and only a client holding the decryption key can read it. That guarantee has consequences a deployment must plan for.

**What the mode does:**

- **Data endpoints require proof of client-side decryption.** Everything under `/data/` returns 403 unless the caller presents the `X-Provisa-KMS-Key` header — the marker of a JDBC or Python client configured to decrypt locally. A browser or a plaintext REST consumer carries no such key and is refused. The gate is a default-deny over the whole tree: a route added tomorrow is gated on the day it ships, and an exemption has to be argued for.
- **Schema-metadata endpoints stay open.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` and `/data/compile` return no row data, and a client has to read the schema — including which fields are `@encrypted` — before it can connect at all.
- **gRPC and Arrow Flight keep serving, under the same proof.** They are the transports encrypting clients actually use; closing them would leave a high-security deployment with no wire protocol. A data call on either must carry the same KMS key as call metadata.
- **pgwire, Bolt and MCP do not start.** None of the three has a per-connection handshake that can carry a decryption context: a pgwire row set and a Cypher result are plaintext on the wire, and an MCP tool call hands its results to a model as text. A configured port for any of them is refused at startup rather than served.
- **The relationship guard cannot be bypassed.** `ignore_relationships` and `relationship_guard: false` are both ignored; see [Relationship governance](#relationship-governance-v002).

**Verifying a deployment is in the mode:** the startup log names it, a `/data/sql` request without a KMS key answers 403 with a message naming REQ-693, and the pgwire, Bolt and MCP ports are not listening.

## ABAC Approval Hook

An optional external policy hook that fires before query execution. (REQ-203) When configured, Provisa calls out to your policy engine with the user identity, roles, tables, columns, and operation. The response determines whether the query proceeds. (REQ-203) A command registered with **Requires approval** is put to the same hook before each call; see [Command calls](#command-calls). (REQ-1924)

### Scoping

The hook only fires when the query touches a scoped table or source — zero overhead for everything else. (REQ-204)

| Config | Effect |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Every query triggers the hook |
| `sources[].approval_hook: true` | All tables on that source trigger the hook |
| `tables[].approval_hook: true` | That table triggers the hook |

### Protocols

Three transports are supported: (REQ-246)

| Type | Use case | Config field |
| ------ | ---------- | ------------- |
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
  map<string, string> session_vars = 6;
  string command = 7;         // a command call's name; empty for a query
  string arguments_json = 8;  // a command call's arguments as a JSON object
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
| ------- | ------ | ------------- |
| `user` | string | Authenticated user identity |
| `roles` | string[] | User's Provisa roles |
| `tables` | string[] | Table IDs referenced in the query |
| `columns` | string[] | Columns selected in the query |
| `operation` | string | `"query"` or `"mutation"`; `"command"` for a command call |
| `command` | string | The command's name for a command call; empty for a query |
| `arguments` | object | The arguments a command is called with; empty for a query |

The webhook and Unix socket transports exchange JSON, with `command` and `arguments` as keys. On gRPC the arguments travel as the JSON text `arguments_json`. Response must include `approved` (bool) and optionally `reason` (string). (REQ-246)

### Command calls

A command with `requires_approval` set (the **Requires approval** switch on the command form) goes to the hook before every call, on every surface. It runs only when the hook approves. The request carries `operation: "command"`, the command's name, and its arguments; `tables` and `columns` are empty. (REQ-1924) [tool-verified: `provisa/api/data/action_exec.py` `_require_approval` (`operation="command"`, `tables=[]`, `columns=[]`, `command=fn["name"]`, `arguments=args`)]

```json
{
  "user": "analyst",
  "roles": ["analyst"],
  "tables": [],
  "columns": [],
  "operation": "command",
  "command": "create_issue",
  "arguments": {"input": {"title": "Crash on save"}}
}
```

[inferred: the payload also carries `session_vars`; values shown are placeholders]

Two refusals, both 403:

- `functions.approval_unavailable`: no hook is configured. Unlike a query, a command that needs approval is never let through without one.
- `functions.approval_denied`: the hook answered `approved: false`. The reason is in the message.

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

For the full secrets service — vaults, reference syntax, and providers — see [Secrets](secrets.md).
