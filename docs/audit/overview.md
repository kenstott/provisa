# Audit — Original Findings (as found)

Date: 2026-06-16
Snapshot: this is the **pristine original audit** of code against requirements, before
any remediation. It is preserved unchanged as the as-found record. The living status
(with fixes applied) is tracked separately in [group-1.md](group-1.md).

Scope audited: **Group 1 — Access Governance & Security** only. Groups 2–12 have not
been audited.
Requirements: REQ-001–006 (Query Governance), REQ-038–042 + REQ-402 (Security),
REQ-262–267 (Two-Stage Compiler / Governed SQL), REQ-203–204 + REQ-246–247 (ABAC
Approval Hook), REQ-369–371 (Rate Limiting), REQ-046 (output-type governance — still
in spec at audit time; removed from spec afterward).
Method: five parallel read-only subagents (audit-workflow skill), one per sub-area,
each comparing implementation to requirement text with file:line evidence; synthesised
here.

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary (as found)

| REQ | Area | Status | Finding |
| --- | --- | --- | --- |
| 001 | Query Governance | Not to spec | Rights model enforced, but orphaned registry plumbing remains (`persisted_queries`, approved-query routing, `query_id`/`stable_id`) |
| 002 | Query Governance | To spec | Stage 2 governance applied on every path |
| 003 | Query Governance | Not to spec | `query_id`/`stable_id` approval remnants remain under the rights model |
| 004 | Query Governance | Not added | `/admin/actions/test` registered unconditionally — no production guard |
| 005 | Query Governance | Incomplete | `_apply_limit_ceiling` exists but `limit_ceiling` never populated; no `max_rows` config |
| 006 | Query Governance | To spec | All formats + large-result redirect available and governed |
| 038 | Security | To spec | Two enforcement layers (visibility + Stage 2 SQL); no pre-approval layer |
| 039 | Security | To spec | Unauthorized tables/columns filtered from SDL + columns |
| 040 | Security | To spec | Stage 2 injects RLS WHERE and strips/NULLs columns each request |
| 041 | Security | To spec | Declarative PG-style RLS filter expressions in `rls_rules` |
| 042 | Security | Not to spec | ~17 capabilities vs the 7 named rights; "query authorization" not isolated |
| 046 | API & Integration | Not added | Output format chosen by `Accept` header only; no per-role format permission |
| 402 | Security | Incomplete | Schema + compiler support domain RLS, but startup load omits `domain_id` → domain rules silently dropped |
| 203 | ABAC Hook | Not to spec | Hook runs before RLS (spec: after); missing `session_vars` request field and `additional_filter` response handling |
| 204 | ABAC Hook | To spec | Per-table/source/global scoping with zero-overhead skip |
| 246 | ABAC Hook | To spec | webhook / gRPC / unix_socket transports + shipped proto |
| 247 | ABAC Hook | Incomplete | `ApprovalHookConfig` exists but `auth.approval_hook` never parsed; `state.approval_hook*` never populated; hook inert |
| 262 | Two-Stage | To spec | Explicit Stage 1 (`stage1.py`) + Stage 2 (`apply_governance`) |
| 263 | Two-Stage | Incomplete | RLS/masking/visibility done; row ceiling unwired; "sampling" is a whole-query `LIMIT`, not `TABLESAMPLE`, applied pre-Stage-2 (GraphQL only) |
| 264 | Two-Stage | To spec | AST transform covers CTE/subquery/JOIN/UNION/`SELECT *` |
| 265 | Two-Stage | To spec | Operates on physical column names |
| 266 | Two-Stage | Not to spec | GraphQL/SQL/pgwire/NL/Cypher use Stage 2; gRPC/JSON:API/Kafka sink use the older string injectors |
| 267 | Two-Stage | To spec | `/data/sql` rights-based, no approval gate |
| 369 | Rate Limiting | Not added | No per-role limits, no `429`/`Retry-After` |
| 370 | Rate Limiting | Not added | No NL-service rate limit before LLM call |
| 371 | Rate Limiting | Not added | No Redis sliding-window state |

## Detail (as found)

### Query Governance (REQ-001–006, 046)

- **001 / 003 — deprecated approved-query registry remains.** Rights enforced at
  [endpoint.py:360](../../provisa/api/data/endpoint.py#L360),
  [rights.py:31](../../provisa/security/rights.py#L31). The GPQ approved-query
  registry the rewritten spec removed still exists: `persisted_queries`
  ([schema.sql:220](../../provisa/core/schema.sql#L220),
  [app.py:2117](../../provisa/api/app.py#L2117)), approved-query routing
  ([cypher_router.py:113](../../provisa/api/rest/cypher_router.py#L113)),
  `query_id`/`stable_id` ([models.py:318](../../provisa/core/models.py#L318),
  [kafka/sink.py:30](../../provisa/kafka/sink.py#L30)). This is the *approved-query*
  registry — distinct from Apollo **APQ** (REQ-288–291, `provisa/apq/` over Redis),
  which is a retained requirement and never touches `persisted_queries`.
- **004 — test endpoint exposed.** `/admin/actions/test`
  ([actions_router.py:386](../../provisa/api/admin/actions_router.py#L386)) registered
  unconditionally ([app.py:2804](../../provisa/api/app.py#L2804)).
- **005 — ceiling unwired.** `GovernanceContext.limit_ceiling` defined
  ([stage2.py:44](../../provisa/compiler/stage2.py#L44)) and consumed by
  `apply_governance`, but `build_governance_context` never sets it; no `max_rows`
  config on roles ([schema.sql:152](../../provisa/core/schema.sql#L152)).
- **006 — to spec.** JSON/NDJSON/CSV/Parquet/Arrow + redirect run through Stage 2.
- **046 — no per-role format permission.** Output format from `Accept` header only
  ([endpoint.py:388](../../provisa/api/data/endpoint.py#L388)); no role-format check.
  (REQ-046 was removed from the spec after the audit.)

### Security (REQ-038–042, 402)

- **038–041 — to spec.** RLS injection + column strip/NULL in
  [stage2.py](../../provisa/compiler/stage2.py) and
  [rls.py](../../provisa/compiler/rls.py); RLS rules declarative in `rls_rules`
  ([schema.sql:165](../../provisa/core/schema.sql#L165)).
- **042 — capability sprawl.** ~17 `Capability` entries
  ([rights.py:21](../../provisa/security/rights.py#L21)) vs the 7 named rights;
  "query authorization" not cleanly isolated; several entries obsolete under the
  rights model.
- **402 — domain RLS dropped at load.** Schema + compiler support domain-scoped
  rules with table precedence ([rls.py:84](../../provisa/compiler/rls.py#L84)), but
  `SELECT table_id, role_id, filter_expr FROM rls_rules`
  ([app.py:2106](../../provisa/api/app.py#L2106)) omits `domain_id`, so domain rules
  are silently lost. Silent-failure defect.

### ABAC Approval Hook (REQ-203–204, 246–247)

- **203 — position and payload.** Hook invoked before RLS injection
  ([endpoint.py:451](../../provisa/api/data/endpoint.py#L451)); spec requires after
  RLS, before execution. `ApprovalRequest`
  ([approval_hook.py:36](../../provisa/auth/approval_hook.py#L36)) omits
  `session_vars`; `additional_filter` response not applied.
- **204 — to spec.** `should_check` gives per-table/source/global scoping with a
  zero-overhead skip.
- **246 — to spec.** Three transports + proto.
- **247 — not loaded.** `ApprovalHookConfig` exists but nothing parses
  `auth.approval_hook`; `state.approval_hook*` never populated; per-table/source
  flags have no persistence.

### Two-Stage Compiler (REQ-262–267)

- **262, 264, 265, 267 — to spec.** Explicit Stage 1/Stage 2; AST transform visits
  every SELECT (CTE, subquery, JOIN, UNION, `SELECT *`); physical names; `/data/sql`
  rights-based, no approval gate.
- **263 — two of five concerns short.** RLS, masking, visibility applied on the full
  AST. Row ceiling unwired (see 005). Sampling is a whole-query `LIMIT`, not
  `TABLESAMPLE`/`random()`, and runs pre-Stage-2 on the GraphQL path only
  ([sampling.py](../../provisa/compiler/sampling.py),
  [endpoint.py:505](../../provisa/api/data/endpoint.py#L505)).
- **266 — uniform enforcement gap.** GraphQL, `/data/sql`, pgwire, NL, Cypher use
  `stage2.apply_governance`. gRPC ([grpc/server.py:157](../../provisa/grpc/server.py#L157)),
  JSON:API ([jsonapi/generator.py:279](../../provisa/api/jsonapi/generator.py#L279)),
  and Kafka sink ([kafka/sink_executor.py:129](../../provisa/kafka/sink_executor.py#L129))
  use the older string-based `inject_rls`/`inject_masking`. Governance can diverge.

### Rate Limiting (REQ-369–371)

Not implemented. No per-role limit config on `Role`/`ProvisaConfig`, no API-layer
middleware, no `429`/`Retry-After`, no NL pre-LLM check, no Redis sliding-window, no
`tests/unit/test_rate_limiting.py`. Confirmed across `provisa/api/`, `provisa/nl/`,
`provisa/core/models.py`, `provisa/cache/`.

## Module boundedness and duplication (as found)

Governance was not bounded to a single module group; two parallel implementations
existed.

- **Path A — Stage 2 AST rewrite** (`compiler/stage2.py` with `security/masking.py`,
  `security/visibility.py`, `rls._qualify_filter`). Spec-aligned. Used by GraphQL,
  `/data/sql`, pgwire, NL, Cypher.
- **Path B — per-pass string injectors** (`compiler/rls.py:inject_rls`,
  `compiler/mask_inject.py:inject_masking`, `compiler/sampling.py:apply_sampling`).
  Used by gRPC, JSON:API, Kafka sink, admin dev_queries.

Consequences as found:

1. RLS, masking, and sampling each had two implementations (AST vs string) that can
   diverge; the string path is weaker on nested SQL/CTEs.
2. REQ-002/266 require uniform enforcement; held only for Path A clients.
3. `compiler/pipeline.py` (`run_pipeline`) imported by nothing — stale.
4. `build_governance_context` + `apply_governance` re-wired at ~6 entry points rather
   than behind one chokepoint.
5. A third cap path — `sql_gen._get_default_row_limit()` (10000) — overlaps
   mechanically with the governance ceiling.

## Named tests missing at audit time

`tests/unit/test_governance.py`, `tests/integration/test_registry.py`,
`tests/unit/test_rate_limiting.py` did not exist.

---

For current status (which of the above have since been fixed, with remediation
detail and remaining-task estimates), see [group-1.md](group-1.md).
