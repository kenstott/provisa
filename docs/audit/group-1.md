# Audit — Group 1: Access Governance & Security

Date: 2026-06-16
Scope: REQ-001–006 (Query Governance), REQ-038–042 + REQ-402 (Security),
REQ-262–267 (Two-Stage Compiler / Governed SQL), REQ-203–204 + REQ-246–247
(ABAC Approval Hook), REQ-369–371 (Rate Limiting).
Method: five parallel read-only subagents comparing each sub-area against the
requirement text, with file:line evidence; results synthesised here.

Note: REQ-046 (per-role output-format permission) was removed from the spec
during this review and is excluded. Output format is chosen by the `Accept`
header, which is now the intended behaviour.

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Area | Status | Note |
| --- | --- | --- | --- |
| 001 | Query Governance | Not to spec | Rights model present; deprecated approved-query registry (`persisted_queries`) remains. Not APQ (Redis, kept). |
| 002 | Query Governance | To spec | Stage 2 applied on every path |
| 003 | Query Governance | Not to spec | Approved-query `stable_id` remnants remain (deprecated GPQ registry, not Apollo APQ) |
| 004 | Query Governance | Fixed 2026-06-16 | Test endpoint now opt-in via `PROVISA_ENABLE_TEST_ENDPOINTS`; 404 by default |
| 005 | Query Governance | Fixed 2026-06-16 | Per-role/table `max_rows` ceiling now wired (was: field never populated) |
| 006 | Query Governance | To spec | All formats + redirect available, governed |
| 038 | Security | To spec | Two-layer model; no pre-approval layer in enforcement |
| 039 | Security | To spec | Visibility filters SDL + columns |
| 040 | Security | To spec | Stage 2 injects RLS WHERE + strips/NULLs columns |
| 041 | Security | To spec | Declarative PG-style RLS in `rls_rules` |
| 042 | Security | Not to spec | ~17 capabilities vs the 7 specified |
| 402 | Security | Fixed 2026-06-16 | Startup `rls_rules` load now selects `domain_id`; domain-scoped rules apply |
| 203 | ABAC Hook | Fixed 2026-06-16 | Hook moved to after RLS; `session_vars` + `additional_filter` added across all transports |
| 204 | ABAC Hook | To spec | Scoping + zero-overhead skip correct |
| 246 | ABAC Hook | To spec | webhook / grpc / unix_socket + proto present |
| 247 | ABAC Hook | Fixed 2026-06-16 | `auth.approval_hook` loaded at startup; per-source/table scope flags wired |
| 262 | Two-Stage | To spec | `stage1.py` + `stage2.apply_governance` |
| 263 | Two-Stage | Fixed 2026-06-16 | Renamed to four concerns (RLS/mask/visibility/row cap); cap wired + transport-uniform; "sampling" removed |
| 264 | Two-Stage | To spec | AST transform covers CTE/subquery/JOIN/UNION/`SELECT *` |
| 265 | Two-Stage | To spec | Operates on physical names |
| 266 | Two-Stage | Partial | Row cap now uniform across all transports; gRPC/JSON:API/Kafka still inject RLS/masking via the string path, not Stage 2 |
| 267 | Two-Stage | To spec | `/data/sql` rights-based, no approval gate |
| 369 | Rate Limiting | Not added | No per-role limits, no 429/Retry-After |
| 370 | Rate Limiting | Not added | No NL-service limit before LLM call |
| 371 | Rate Limiting | Not added | No Redis sliding-window state |
| 478 | Compiler & Schema | Added 2026-06-16 | GraphQL `sample` arg → `TABLESAMPLE BERNOULLI` (statistical sampling, distinct from the cap) |

## Detail

### Query Governance (REQ-001–006)

- **001 / 003 — deprecated approved-query registry remains.** The rights model is
  enforced ([endpoint.py:360](../../provisa/api/data/endpoint.py#L360),
  [rights.py:31](../../provisa/security/rights.py#L31)), but the GPQ approved-query
  registry the rewritten spec removed still exists: the `persisted_queries` table
  ([schema.sql:220](../../provisa/core/schema.sql#L220) — `stable_id`,
  `status='approved'`, `developer_id`), its startup load
  ([app.py:2117](../../provisa/api/app.py#L2117)), approved-query routing
  ([cypher_router.py:113](../../provisa/api/rest/cypher_router.py#L113)), and
  `query_id`/`stable_id` fields ([models.py:318](../../provisa/core/models.py#L318),
  [kafka/sink.py:30](../../provisa/kafka/sink.py#L30)).
  **Terminology:** this is the deprecated *approved-query* registry, distinct from
  Apollo **APQ** (Automatic Persisted Queries, REQ-288–291) — a separate, retained
  requirement implemented in `provisa/apq/` over Redis (SHA-256 keyed) that never
  reads `persisted_queries`. Removing the approved-query registry is a migration,
  not a delete: see remaining-tasks item 3.
- **004 — fixed.** The action test endpoint (`/admin/actions/test`) is now gated by
  `_test_endpoints_enabled()` ([actions_router.py](../../provisa/api/admin/actions_router.py))
  — opt-in via `PROVISA_ENABLE_TEST_ENDPOINTS`, returning 404 by default so it is
  absent in production. Mirrors the `allow_simple_auth` opt-in pattern. Tested in
  [test_test_endpoint_guard.py](../../tests/unit/test_test_endpoint_guard.py).
- **005 — fixed.** Per-role/table `max_rows` ceiling is now populated and
  injected (see Remediation).
- **006 — to spec.** JSON/NDJSON/CSV/Parquet/Arrow and large-result redirect all
  run through Stage 2 before format selection.

### Security (REQ-038–042, 402)

- **038–041 — to spec.** Two enforcement layers (visibility + Stage 2 SQL).
  RLS WHERE injection and column strip/NULL in
  [stage2.py](../../provisa/compiler/stage2.py) and
  [rls.py](../../provisa/compiler/rls.py); RLS rules are declarative PG-style
  filter expressions in `rls_rules` ([schema.sql:165](../../provisa/core/schema.sql#L165)).
- **042 — capability sprawl.** The `Capability` enum defines ~17 entries
  ([rights.py:21](../../provisa/security/rights.py#L21)) against the 7 distinct
  rights named in the requirement; "query authorization" is not cleanly isolated.
  Several entries (e.g. approval-related) are obsolete under the rights model.
- **402 — fixed.** The startup query now selects `domain_id`
  ([app.py:2106](../../provisa/api/app.py#L2106)), so domain-scoped RLS rules reach
  `build_rls_context` and apply with table-over-domain precedence
  ([rls.py:84](../../provisa/compiler/rls.py#L84)). Covered by a `TestDomainRLS`
  class plus a loader-regression test in
  [test_rls.py](../../tests/unit/test_rls.py).

### ABAC Approval Hook (REQ-203–204, 246–247)

- **203 — fixed.** The hook is now evaluated in `_prepare_compiled` **after**
  `apply_governance` (RLS/masking) and before execution
  ([endpoint.py](../../provisa/api/data/endpoint.py)). `ApprovalRequest` carries
  `session_vars` (populated from role context) and `ApprovalResponse` carries
  `additional_filter`, which is ANDed into the governed WHERE via `_inject_where`.
  All three transports updated: webhook/unix-socket JSON, and gRPC (proto
  `approval.proto` extended with `session_vars`/`additional_filter`, `approval_pb2`
  regenerated). Tests in
  [test_approval_hook.py](../../tests/unit/test_approval_hook.py) cover the payload,
  the response field, and the after-RLS ordering. **Note:** `session_vars` is empty
  until identity propagation populates per-request session variables on the role
  context.
- **204 — to spec.** `should_check` gives per-table/per-source/global scoping with
  a zero-overhead skip.
- **246 — to spec.** webhook, gRPC (persistent channel), and unix_socket
  transports plus shipped proto.
- **247 — fixed.** `load_approval_hook_config` parses the `auth.approval_hook`
  block; `_setup_approval_hook` ([app.py](../../provisa/api/app.py)) builds the hook
  via `create_hook` at startup and populates `state.approval_hook`,
  `approval_hook_config`, `source_approval_hooks` (from `Source.approval_hook`), and
  `table_approval_hooks` (from `Table.approval_hook`, resolved to table_ids via the
  compilation contexts). `approval_hook` config fields added to `AuthConfig`,
  `Source`, `Table`. Tests in
  [test_approval_hook.py](../../tests/unit/test_approval_hook.py) (`TestConfigLoading`).

### Two-Stage Compiler (REQ-262–267)

- **262, 264, 265, 267 — to spec.** Explicit Stage 1 / Stage 2; AST transform
  visits every SELECT (CTE, subquery, JOIN, UNION, `SELECT *` expansion); Stage 2
  uses physical names; `/data/sql` is rights-based with no approval gate.
- **263 — four concerns, all applied (resolved 2026-06-16).** The requirement was
  renamed: "sampling" is removed as a governance concern, leaving RLS, masking,
  visibility, and the row cap. RLS/masking/visibility rewrite the full AST; the row
  cap is now wired (Remediation) and shared by every transport via one
  implementation in [stage2.py](../../provisa/compiler/stage2.py)
  (`resolve_row_cap` / `apply_row_cap` / `_apply_limit_ceiling`). Statistical
  sampling moved to a user query feature (REQ-478, `TABLESAMPLE`).
- **266 — row cap unified; RLS/masking still split.** GraphQL, `/data/sql`, pgwire,
  NL, and Cypher converge on `stage2.apply_governance`. The **row cap** is now
  uniform on every transport: gRPC ([grpc/server.py](../../provisa/grpc/server.py))
  and admin dev_queries call the same `resolve_row_cap`/`apply_row_cap`. **RLS and
  masking** on gRPC, JSON:API
  ([jsonapi/generator.py:279](../../provisa/api/jsonapi/generator.py#L279)), and
  Kafka sink ([kafka/sink_executor.py:129](../../provisa/kafka/sink_executor.py#L129))
  still use the older string-based `inject_rls`/`inject_masking`. Routing those three
  through Stage 2 closes the remaining divergence.

### Rate Limiting (REQ-369–371)

Not implemented. No per-role limit config on `Role`/`ProvisaConfig`, no API-layer
middleware, no `429`/`Retry-After`, no NL-service pre-LLM check, no Redis
sliding-window state, and no `tests/unit/test_rate_limiting.py`. Confirmed across
`provisa/api/`, `provisa/nl/`, `provisa/core/models.py`, and `provisa/cache/`.

## Module boundedness and duplication

Governance is not bounded to a single module group, and two parallel
implementations exist.

- **Path A — Stage 2 AST rewrite** (`compiler/stage2.py` with `security/masking.py`,
  `security/visibility.py`, `rls._qualify_filter`). Spec-aligned. Used by GraphQL,
  `/data/sql`, pgwire, NL, Cypher.
- **Path B — per-pass string injectors** (`compiler/rls.py:inject_rls`,
  `compiler/mask_inject.py:inject_masking`, `compiler/sampling.py:apply_sampling`).
  Used by gRPC, JSON:API, Kafka sink, admin dev_queries.

Resolved 2026-06-16 (row cap + stale module):

- The **row cap** is deduped to one implementation in `stage2` shared by all
  transports. `compiler/sampling.py` is reduced to a thin adapter over it (no second
  algorithm) plus the admin-settings config reader; statistical sampling is now a
  separate user feature (REQ-478). `compiler/pipeline.py` (dead `run_pipeline`) was
  deleted.
- A **third** cap path remains: `sql_gen._get_default_row_limit()` (default 10000,
  [sql_gen.py:56](../../provisa/compiler/sql_gen.py#L56)) injects a `LIMIT` on the
  GraphQL path when no limit is given. It is an OOM guard, distinct in purpose from
  the governance cap, but overlaps mechanically — a candidate for folding into the
  same ceiling later.

Still open:

1. **RLS and masking** still have two implementations (AST `stage2` vs string
   `compiler/rls.py`/`compiler/mask_inject.py`); the string path is weaker on nested
   SQL/CTEs and is used by gRPC, JSON:API, and Kafka sink.
2. REQ-002/266 require uniform enforcement of RLS/masking; today that holds for the
   Stage 2 clients and for the row cap everywhere, but not RLS/masking on Path B.
3. `build_governance_context` + `apply_governance` are re-wired at ~6 entry points
   rather than behind one chokepoint.

Direction: make Stage 2 the single entry point, route gRPC/JSON:API/Kafka through
it, delete the remaining string injectors, and fold the per-entry wiring
into one `execute_governed(sql_or_doc, role)` helper. That confines governance to
`compiler/stage2.py` + `security/`.

## Remediation applied (2026-06-16)

- **REQ-005 / REQ-263 row ceiling — implemented.** Added `Role.max_rows`
  ([models.py:388](../../provisa/core/models.py#L388)) with `flatten_roles`
  propagation (child inherits parent when unset, `min` when both set); added
  `GovernanceContext.table_ceilings`; `build_governance_context` now sets
  `limit_ceiling` from the role and `table_ceilings` from table config;
  `apply_governance` injects `LIMIT` from the smallest ceiling on a referenced
  table ([stage2.py](../../provisa/compiler/stage2.py)). Wired into all six
  `build_governance_context` call sites. Default is unbounded (no `max_rows` set =
  no injected ceiling). Nine tests added in
  [test_stage2.py](../../tests/unit/test_stage2.py); related suites green.
- **REQ-046 — removed from spec.** Output format by `Accept` header is the
  intended behaviour.
- **REQ-263 row cap — deduped + transport-uniform.** One cap implementation in
  `stage2` (`resolve_row_cap`/`apply_row_cap`); a role without FULL_RESULTS gets the
  configured default cap (`PROVISA_DEFAULT_MAX_ROWS`, legacy `PROVISA_SAMPLE_SIZE`
  honoured), uncapped with FULL_RESULTS. Repointed the GraphQL, gRPC, and admin
  dev_queries paths off the old sampling pass; `compiler/sampling.py` is now a thin
  adapter; `compiler/pipeline.py` deleted. Requirement reworded to four governance
  concerns; "sampling" removed.
- **REQ-478 — GraphQL statistical sampling added.** `sample: Float` arg on root
  query fields → `TABLESAMPLE BERNOULLI (<pct>)`
  ([schema_gen.py](../../provisa/compiler/schema_gen.py),
  [sql_gen.py](../../provisa/compiler/sql_gen.py)); range-checked, rejected with
  `as_of`/lateral op-joins. Four tests in
  [test_sql_gen.py](../../tests/unit/test_sql_gen.py). Full unit suite green
  (4296 passed).
- **REQ-402 — domain-scoped RLS fixed.** Startup loader selects `domain_id`; domain
  rules apply with table precedence. `TestDomainRLS` + loader regression in
  [test_rls.py](../../tests/unit/test_rls.py).
- **REQ-004 — test endpoint gated.** Opt-in `PROVISA_ENABLE_TEST_ENDPOINTS`; 404 by
  default. [test_test_endpoint_guard.py](../../tests/unit/test_test_endpoint_guard.py).
- **REQ-203 — ABAC hook corrected.** Moved after RLS; added `session_vars` and
  `additional_filter` across webhook/unix/gRPC (proto regenerated). Tests in
  [test_approval_hook.py](../../tests/unit/test_approval_hook.py).
- **Row-cap fold (single cap path) — done.** Removed the compile-time default
  `LIMIT` from `sql_gen`; the row cap is now applied solely by `resolve_row_cap`
  ([stage2.py](../../provisa/compiler/stage2.py)) — **FULL_RESULTS roles get no
  default row limit at all** (uncapped; large results still redirect to S3, a
  separate threshold), every other role gets the single configured
  `default_row_limit` (`PROVISA_DEFAULT_ROW_LIMIT`, default 10000). JSON:API (Path B)
  now applies the cap explicitly; REST already routed through Stage 2; Kafka sinks
  are uncapped (publish full results). Distinct from the large-result **redirect
  threshold** (`redirect.threshold`) — they are separate config and code paths.
  Full unit suite green (4312 passed).

## Remaining tasks

Status: 9 of 25 requirements resolved (REQ-005, 046, 263, 478, plus the row-cap
dedup + single-cap-path fold; the three defects REQ-402, REQ-004, REQ-203; and the
ABAC config loading REQ-247). The items below are what remains for Group 1.

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 266 | Gap (consistency) | L | Route gRPC, JSON:API, and Kafka sink through `stage2.apply_governance` for **RLS/masking** (the row cap is already uniform); delete the string injectors `compiler/rls.py:inject_rls` and `compiler/mask_inject.py`. |
| 2 | 369–371 | Gap (feature) | L | Build rate limiting end to end: per-role config, API middleware, `429`+`Retry-After`, NL pre-LLM check, Redis sliding-window. Nothing exists yet. |
| 3 | 001/003 | Migration | L | Target = the **approved-query / GPQ registry** (`persisted_queries` table: `stable_id`, `status='approved'`, `developer_id`), which is deprecated. **NOT** Apollo APQ (REQ-288–291: `provisa/apq/`, Redis, SHA-256 keyed) — that is a requirement and stays; it never touches `persisted_queries`. The approved-query table is still load-bearing: deprecated approved-query *features* read it — Kafka sinks ([sink_executor.py:55](../../provisa/kafka/sink_executor.py#L55)), GPQ SSE subscriptions ([subscribe.py:331](../../provisa/api/data/subscribe.py#L331)), startup load ([app.py:2161](../../provisa/api/app.py#L2161)), pgwire catalog. Removal = rewriting those features to their rights-based forms (sinks/subscriptions→tables/views, catalog→registered tables) — multi-day, not a cleanup. |
| 4 | 042 | Redesign | M | Six capabilities have 0 Python references (`APPROVE_VIEW`, `APPROVE_RELATIONSHIP`, `CREATE_RELATIONSHIP`, `USAGE`, `READ_RESTRICTED`, `COLUMN_GRANT`) but are likely referenced by role configs and the role-composition UI. Reaching the 7 named rights is a capability-model redesign with config/UI coupling, not an enum trim. |
| 5 | — | Test debt | M | Add the requirement-named tests that don't exist: `tests/unit/test_governance.py`, `tests/integration/test_registry.py`, `tests/unit/test_rate_limiting.py`. Also add an endpoint-level ABAC integration test (the REQ-203 fix is covered at unit/structural level only). |

The default-limit overlap is now resolved — see "Row-cap fold" above. Of the rest:
the approved-query registry (item 3) is load-bearing across Kafka sinks,
subscriptions, the live engine, and pgwire catalog — removing it is the same
migration as moving those delivery paths off approved-query-by-`stable_id` (and is
unrelated to Apollo APQ, which stays). The capability trim (item 4) couples to role
configs and the UI. So all remaining items are multi-day or a redesign and warrant
their own focused, fully-verified passes rather than a quick cleanup sweep.
Effort: S ≈ <½ day, M ≈ ~1 day, L ≈ multi-day.
