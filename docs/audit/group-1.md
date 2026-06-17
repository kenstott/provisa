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
| 001 | Query Governance | Fixed 2026-06-16 | Rights-only access; all approved-query/GPQ registry *code* removed (Phase 3). Orphaned `persisted_queries` table drop is follow-on DB cleanup. |
| 002 | Query Governance | To spec | Stage 2 applied on every path |
| 003 | Query Governance | Fixed 2026-06-16 | No registry/approval governs access; GPQ execute-by-id, subscriptions, and sinks removed |
| 004 | Query Governance | Fixed 2026-06-16 | Test endpoint now opt-in via `PROVISA_ENABLE_TEST_ENDPOINTS`; 404 by default |
| 005 | Query Governance | Fixed 2026-06-16 | Per-role/table `max_rows` ceiling now wired (was: field never populated) |
| 006 | Query Governance | To spec | All formats + redirect available, governed |
| 038 | Security | To spec | Two-layer model; no pre-approval layer in enforcement |
| 039 | Security | To spec | Visibility filters SDL + columns |
| 040 | Security | To spec | Stage 2 injects RLS WHERE + strips/NULLs columns |
| 041 | Security | To spec | Declarative PG-style RLS in `rls_rules` |
| 042 | Security | To spec 2026-06-16 | The 7 named rights are distinct, independently-assignable capabilities; the extra entries are finer granularity (e.g. `full_results`), not a violation. Pinned by test_governance.py |
| 402 | Security | Fixed 2026-06-16 | Startup `rls_rules` load now selects `domain_id`; domain-scoped rules apply |
| 203 | ABAC Hook | Fixed 2026-06-16 | Hook moved to after RLS; `session_vars` + `additional_filter` added across all transports |
| 204 | ABAC Hook | To spec | Scoping + zero-overhead skip correct |
| 246 | ABAC Hook | To spec | webhook / grpc / unix_socket + proto present |
| 247 | ABAC Hook | Fixed 2026-06-16 | `auth.approval_hook` loaded at startup; per-source/table scope flags wired |
| 262 | Two-Stage | To spec | `stage1.py` + `stage2.apply_governance` |
| 263 | Two-Stage | Fixed 2026-06-16 | Renamed to four concerns (RLS/mask/visibility/row cap); cap wired + transport-uniform; "sampling" removed |
| 264 | Two-Stage | To spec | AST transform covers CTE/subquery/JOIN/UNION/`SELECT *` |
| 265 | Two-Stage | To spec | Operates on physical names |
| 266 | Two-Stage | Fixed 2026-06-16 | All data-serving query transports (GraphQL, /data/sql, pgwire, NL, Cypher, REST, gRPC, JSON:API, Kafka sink) route through Stage 2; injectors remain only for admin dev-preview + mutation write-RLS |
| 267 | Two-Stage | To spec | `/data/sql` rights-based, no approval gate |
| 369 | Rate Limiting | Fixed 2026-06-16 | Per-role req/sec middleware + SSE & Flight concurrency caps; 429 + Retry-After |
| 370 | Rate Limiting | Fixed 2026-06-16 | NL `nl.rate_limit` (req/min/role) enforced before the LLM call |
| 371 | Rate Limiting | Fixed 2026-06-16 | Redis sliding-window + concurrency gauges; no in-process state |
| 478 | Compiler & Schema | Added 2026-06-16 | GraphQL `sample` arg → `TABLESAMPLE BERNOULLI` (statistical sampling, distinct from the cap) |

## Detail

### Query Governance (REQ-001–006)

- **001 / 003 — fixed (Phase 3, 2026-06-16).** Every code path that read the GPQ
  approved-query registry was removed: GPQ-as-subscription-field generation in the
  schema builder ([schema_gen.py](../../provisa/compiler/schema_gen.py)), the
  startup approved-query load + `_build_and_register_schemas`/`SchemaInput`
  `approved_queries` param ([app.py](../../provisa/api/app.py)), the on-demand SDL
  build ([sdl.py](../../provisa/api/data/sdl.py)), the Cypher execute-by-`query_id`
  path ([cypher_router.py](../../provisa/api/rest/cypher_router.py) → now 410), the
  GPQ SSE subscription path ([subscribe.py](../../provisa/api/data/subscribe.py) →
  now 410), and the GPQ Kafka-sink trigger
  ([sink_executor.py](../../provisa/kafka/sink_executor.py) → no-op). Access is
  rights-only; nothing consults the registry. **Terminology:** this is the
  deprecated *approved-query* registry — distinct from Apollo **APQ** (REQ-288–291,
  `provisa/apq/`, Redis), which is retained and untouched. The physical
  `persisted_queries` table (+ its `scheduled_queries`/stable-id dependents) is now
  orphaned (no reader); dropping it from `schema.sql` is follow-on DB cleanup that
  does not affect the governance requirement.
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
- **042 — to spec (re-assessed 2026-06-16).** The requirement is that the seven
  named rights are "distinct and independently configured", not that *only* seven
  exist. They are: `source_registration`, `table_registration`,
  `create_relationship`, `access_config`, `query_development`, `approve_view`
  (query/artifact authorization), `ad_hoc_query` (query execution) — each a separate,
  independently-assignable `Capability` ([rights.py:21](../../provisa/security/rights.py#L21))
  enforced independently by `check_capability`/`has_capability`. The other entries
  (`full_results`, `masking_config`, `user_management`, `admin`, …) are legitimate
  finer-grained rights and all live in the role-composition UI palette, so they are
  not dead. Nothing approval/registry-specific survived Phase 3. Verified by
  [test_governance.py](../../tests/unit/test_governance.py). (The earlier
  "Not to spec" was a miscount, not a real gap.)
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
- **266 — fixed (Phase 2, 2026-06-16).** gRPC ([grpc/server.py](../../provisa/grpc/server.py)),
  JSON:API ([jsonapi/generator.py](../../provisa/api/jsonapi/generator.py)), and the
  Kafka sink ([kafka/sink_executor.py](../../provisa/kafka/sink_executor.py)) were
  rewritten to compile to semantic SQL then call `_govern_and_route_compiled` +
  `_execute_plan` — the same Stage 2 path as GraphQL/REST — so RLS, masking,
  visibility, and the row cap are applied uniformly. No data-serving query transport
  bypasses Stage 2. The query-side `compiler/rls.py:inject_rls` and
  `compiler/mask_inject.py:inject_masking` are **not** deleted: they remain the admin
  compile-preview compiler (`dev_queries`, admin-only) with their own unit tests, and
  `mutation_gen.inject_rls_into_mutation` (write-RLS, REQ-035) is a separate function
  that stays. 66 transport tests + full unit suite (4322) green.

### Rate Limiting (REQ-369–371) — implemented 2026-06-16 (Phase 1)

- Redis-backed limiter [rate_limit.py](../../provisa/api/rate_limit.py): sliding-window
  `allow()` (req/sec, NL req/min) + concurrency `acquire()/release()` (SSE, Flight).
  No in-process state; no-op when Redis is unconfigured.
- **REQ-369:** `Role.rate_limit` config (`requests_per_second`, `max_sse_subscriptions`,
  `max_flight_streams`). Per-role req/sec enforced by
  [rate_limit_middleware.py](../../provisa/api/middleware/rate_limit_middleware.py)
  (added before `wire_auth` so auth populates `request.state.role` first) → 429 +
  `Retry-After`. SSE concurrency capped in [subscribe.py](../../provisa/api/data/subscribe.py)
  (acquire on connect, release when the stream ends, both return paths); Flight
  concurrency capped in [flight/server.py](../../provisa/api/flight/server.py)
  `do_get` over the query execution window.
- **REQ-370:** `nl.rate_limit` (req/min/role) checked in
  [nl_router.py](../../provisa/api/rest/nl_router.py) before the job/LLM call → 429.
- **REQ-371:** Redis sliding window (sorted set) + concurrency gauge; shared across
  stateless instances.
- Tests: [test_rate_limiting.py](../../tests/unit/test_rate_limiting.py) (limiter
  windows, concurrency, middleware 429) — Redis faked in-memory.

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

Status: 16 of 25 requirements resolved (REQ-005, 046, 263, 478, the row-cap
dedup + single-cap-path fold; the three defects REQ-402, REQ-004, REQ-203; the
ABAC config loading REQ-247; **rate limiting REQ-369/370/371**; the **Stage 2
transport consolidation REQ-266**; the **approved-query registry removal
REQ-001/003**; and the **capability-rights verification REQ-042**). Phased plan order: REQ-369–371 (done) → 266 (done) → 001/003 (done)
→ 042 (done) → test debt.

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | — | Test debt | M | Add the remaining requirement-named test: `tests/integration/test_registry.py` reframed as rights-based (the registry is gone). Also an endpoint-level ABAC integration test (REQ-203 covered at unit/structural level only). `test_rate_limiting.py` and `test_governance.py` are done. |
| 2 | 001/003 | DB cleanup | S | Drop the now-orphaned `persisted_queries` table (+ `scheduled_queries`/stable-id dependents) from `schema.sql`. Code no longer reads it; schema tidy-up, not a governance change. |

Effort: S ≈ <½ day, M ≈ ~1 day, L ≈ multi-day.
