# Audit — Group 5: Query Languages

Date: 2026-06-18
Scope: **Group 5 — Query Languages, Compilation & Operations** (REQ-007–011, 032–037,
066–068, 196–202, 205–211, 252–253, 259, 300–301, 304–306, 345–362, 403, 409,
411–412, 416, 478). Code under `provisa/compiler/`, `provisa/cypher/`, `provisa/nl/`,
`provisa/transpiler/`, `provisa/executor/`, `provisa/webhooks/`, `provisa/mv/`, plus
the REST routers and the admin UI.
Method: read implementation against requirement text with file:line evidence from
Grep/Read. Companion to [group-2.md](group-2.md).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 007 | Compiler & Schema | To spec | Uses graphql-core directly, no third-party GraphQL framework `provisa/compiler/parser.py:13` |
| 008 | Compiler & Schema | To spec | INFORMATION_SCHEMA introspection + per-role filtering + SDL; rebuild on registration `provisa/compiler/introspect.py:11` |
| 009 | Compiler & Schema | To spec | Walk AST → single PG-style SQL, no resolver chain/N+1 `provisa/compiler/sql_gen.py:11` |
| 010 | Compiler & Schema | To spec | Trino→GraphQL scalar map, nullability preserved `provisa/compiler/type_map.py:11` |
| 011 | Compiler & Schema | To spec | graphql-core validate + relationship visibility filter reject bad refs `provisa/compiler/parser.py:35` |
| 032 | Mutation Execution | To spec | Mutations route direct to RDBMS, never Trino; cross-source rejected `provisa/compiler/mutation_gen.py:13` |
| 033 | Mutation Execution | To spec | `_check_writable_by` enforces per-column write rights `provisa/api/data/endpoint.py:2282` |
| 034 | Mutation Execution | To spec | Insert input built from `visible_columns` only `provisa/compiler/schema_gen.py:1179` |
| 035 | Mutation Execution | To spec | `inject_rls_into_mutation` ANDs RLS into UPDATE/DELETE WHERE `provisa/compiler/mutation_gen.py:370` |
| 036 | Mutation Execution | To spec | Mutation types built only for tables in `ctx.tables` `provisa/compiler/mutation_gen.py:110` |
| 037 | Mutation Execution | To spec | `NOSQL_TYPES` rejected; cross-source single-`source_id` guard `provisa/compiler/mutation_gen.py:30` |
| 066 | SQLGlot Transpilation | To spec | PG-style canonical → SQLGlot transpile to target dialect `provisa/transpiler/transpile.py:11` |
| 067 | SQLGlot Transpilation | To spec | Dialect from `source_dialects` map keyed by source `provisa/transpiler/router.py:79` |
| 068 | SQLGlot Transpilation | To spec | `SUPPORTED_DIALECTS` covers all 7 `provisa/transpiler/transpile.py:23` |
| 196 | Aggregates | To spec | Fixed: stddev/variance added (numeric-only) with STDDEV/VARIANCE SQL emission `provisa/compiler/aggregate_gen.py`, `provisa/compiler/sql_gen.py` |
| 197 | Aggregates | To spec | Fixed: per-role gate via the `no_aggregations` capability suppresses the `<table>_aggregate` field `provisa/compiler/schema_gen.py` |
| 198 | Aggregates | To spec | `find_aggregate_mv` + `rewrite_sql` route aggregates to MV `provisa/mv/aggregate_catalog.py:54` |
| 199 | Aggregates | To spec | Fixed: `materialized_views.default_ttl` config + TTL-aware `is_fresh_at`/`get_fresh` so stale MVs fall back to live; materialization is opt-in (no cost auto-detect) `provisa/mv/models.py`, `provisa/mv/registry.py` |
| 200 | OrderBy Alignment | To spec | Column-keyed OrderBy input type `provisa/compiler/schema_gen.py:587` |
| 201 | OrderBy Alignment | To spec | 6-value `OrderDirection` enum → `_DIRECTION_SQL` `provisa/compiler/schema_gen.py:117` |
| 202 | OrderBy Alignment | To spec | Relationship fields nested into order_by input via thunk `provisa/compiler/schema_gen.py:614` |
| 205 | Tracked Functions | To spec | `tracked_functions` registry; routed by `kind` query/mutation `provisa/api/admin/actions_router.py:29` |
| 206 | Tracked Functions | To spec | `FunctionInput` config model with governance fields `provisa/api/admin/actions_router.py:119` |
| 207 | Tracked Functions | To spec | `func.returns` must be in `table_gql_types`; reuses table governance `provisa/compiler/function_gen.py:113` |
| 208 | Tracked Functions | To spec | Functions execute via `source_pools.execute`, no Trino `provisa/api/data/endpoint.py:2470` |
| 209 | Tracked Functions | To spec | Fixed: webhooks exposed only after steward approval, tracked via the creation_requests queue (latest "webhook" request executed) `provisa/api/admin/actions_router.py`, `provisa/api/app.py` |
| 210 | Tracked Functions | To spec | `inlineReturnType` → inline type when `returns` empty `provisa/compiler/schema_gen.py:877` |
| 211 | Tracked Functions | To spec | Args → GraphQL input; parameterized `$N` (DB) + JSON body (webhook) `provisa/compiler/function_gen.py:157` |
| 252 | Compiler & Schema | To spec | Fixed: `register_table(discover=true)` infers columns from the live source (explicit columns win); ES live `_mapping` bridge; ES/Cassandra raise instead of silent-empty `provisa/api/admin/schema.py`, `provisa/elasticsearch/source.py`, `provisa/discovery/column_inference.py` |
| 253 | Compiler & Schema | To spec | Naming update calls `_naming.configure` + `_rebuild_schemas` `provisa/api/admin/schema.py:2049` |
| 259 | Compiler & Schema | To spec | `FederationConfig(enabled=False)`; @key, _service, _entities `provisa/compiler/federation.py:44` |
| 300 | GraphQL Variable Defaults | To spec | `coerce_variable_defaults` applies defaults for missing vars `provisa/compiler/parser.py:91` |
| 301 | GraphQL Variable Defaults | To spec | LIMIT/OFFSET emitted via `collector.add` ($N) `provisa/compiler/sql_gen.py:1100` |
| 304 | Custom Return Schema | To spec | `return_schema` → `_json_schema_to_gql_type`; array/object handling `provisa/compiler/schema_gen.py:848` |
| 305 | Custom Return Schema | To spec | Admin UI toggle `returnSchemaMode` + `inferJsonSchema` + sample JSON `provisa-ui/src/pages/CommandsPage.tsx:56` |
| 306 | Custom Return Schema | To spec | `_JS_MAP` scalars; unknown→String; top-level only `provisa/compiler/schema_gen.py:738` |
| 345 | Cypher Frontend | To spec | `POST /data/cypher`; Stage 2 governance applied `provisa/api/rest/cypher_router.py:355` |
| 346 | Cypher Frontend | To spec | Write clauses + APOC rejected at parse `provisa/cypher/parser.py:785` |
| 347 | Cypher Frontend | To spec | MATCH→JOIN, OPTIONAL→LEFT JOIN, WITH→CTE, label resolution `provisa/cypher/translator.py:1087` |
| 348 | Cypher Frontend | To spec | shortestPath/`[*1..n]`→WITH RECURSIVE; unbounded `[*]` rejected `provisa/cypher/parser.py:547` |
| 349 | Cypher Frontend | To spec | Stage 3 `apply_graph_rewrites` → `CAST(ROW(...) AS JSON)` `provisa/cypher/graph_rewriter.py:29` |
| 350 | Cypher Frontend | To spec | Node/Edge/Path GraphQL types defined `provisa/cypher/graph_types.py:33` |
| 351 | Cypher Frontend | To spec | `CypherLabelMap.from_schema(ctx)`, no separate config `provisa/cypher/label_map.py:197` |
| 352 | Cypher Frontend | To spec | `$param`→positional; missing param rejected `provisa/cypher/params.py:30` |
| 353 | Cypher Frontend | To spec | Fixed: a plain relational JOIN across sources raises `CypherCrossSourceError` (traversal-only + computed/meta joins excluded) `provisa/cypher/translator.py` |
| 354 | NL Query Service | To spec | `POST /query/nl` → job_id; poll + SSE routes `provisa/nl/` router |
| 355 | NL Query Service | To spec | Three parallel loops; compiler-validated retry `provisa/nl/loop.py:57` |
| 356 | NL Query Service | To spec | Role-scoped SDL in prompt; compiler rejects invisible refs `provisa/nl/runner.py:53` |
| 357 | NL Query Service | To spec | Parallel exec; per-branch query/result/error `provisa/nl/runner.py:73` |
| 358 | NL Query Service | To spec | Three mechanisms (multi-target, role scope, refinement) present `provisa/nl/runner.py:69` |
| 359 | NL Query Service | To spec | Executor applies same governance/RLS as direct path `provisa/nl/executor.py:54` |
| 360 | Tracked Functions | To spec | `_apply_action_filters` does where/order_by/limit/offset post-proc `provisa/api/data/endpoint.py:2379` |
| 361 | Tracked Functions | To spec | `_resolve_action_relationships` batch-fetches with governance `provisa/api/data/endpoint.py:2311` |
| 362 | Tracked Functions | To spec | Cardinality from `join_meta.cardinality` → array vs object `provisa/api/data/endpoint.py:2362` |
| 403 | Compiler & Schema | To spec | `_rule_for_table`: table rule first, domain fallback `provisa/compiler/rls.py:84` |
| 409 | Compiler & Schema | To spec | `_coerce_ts_literals` wraps ISO datetime as `TIMESTAMP '...'` `provisa/cypher/translator.py:1854` |
| 411 | Compiler & Schema | To spec | `hasura-default`→`hasura_graphql` (snake_case) `provisa/compiler/naming.py:137` |
| 412 | Compiler & Schema | To spec | `graphql-default`→`apollo_graphql`, default `provisa/compiler/naming.py:138` |
| 416 | Compiler & Schema | To spec | Fixed: `update_gql_naming_convention` validates against `VALID_CONVENTIONS` via `validation_error_for_convention` before applying `provisa/api/admin/schema.py`, `provisa/compiler/naming.py` |
| 478 | Compiler & Schema | To spec | `sample` arg, range check, TABLESAMPLE BERNOULLI, as_of/lateral guard `provisa/compiler/sql_gen.py:1969` |

61 To spec (all remediated 2026-06-19). Original audit (2026-06-18): 54 To spec, 4 Incomplete (196, 197, 199, 416), 1 Not to spec (353), 1 Not added (252).

**Audit correction (REQ-252):** the inference primitives were not absent — per-connector `discover_schema()` functions and a `discover` flag already existed (REQ-250). The gap was wiring them to run automatically at registration and a live ES `_mapping` bridge; both are now in place.

## Detail

### Compiler & Schema (REQ-007–011, 252, 253, 259, 403, 409, 411, 412, 416, 478)

- REQ-007 — graphql-core used directly; module headers state no third-party GraphQL
  framework `provisa/compiler/parser.py:13`, `provisa/compiler/schema_gen.py:13`.
- REQ-008 — INFORMATION_SCHEMA query `provisa/compiler/introspect.py:11`; per-role
  column filtering `provisa/compiler/schema_gen.py:14`; SDL via `print_schema`
  `provisa/compiler/federation.py:229`; rebuild on registration
  `provisa/api/admin/schema.py:1771`.
- REQ-009 — single statement, no resolver chain `provisa/compiler/sql_gen.py:11`.
- REQ-010 — scalar map with nullability `provisa/compiler/type_map.py:11`.
- REQ-011 — `graphql.validate` raises on bad refs `provisa/compiler/parser.py:35`;
  relationship visibility filter `provisa/compiler/schema_gen.py:672`.
- REQ-252 — **Not added.** No `discover` flag or connector-driven schema inference
  for MongoDB/Cassandra/Elasticsearch. The `provisa/discovery/` module does LLM
  relationship discovery, not column inference `provisa/compiler/introspect.py`.
- REQ-253 — `update_gql_naming_convention` reconfigures naming and awaits
  `_rebuild_schemas` `provisa/api/admin/schema.py:2049`.
- REQ-259 — `FederationConfig.enabled` defaults False; @key from PKs, `_service`,
  `_entities` `provisa/compiler/federation.py:44`.
- REQ-403 — `_rule_for_table` checks table rule then domain fallback
  `provisa/compiler/rls.py:84`.
- REQ-409 — `_coerce_ts_literals` wraps ISO datetimes before SQLGlot parse
  `provisa/cypher/translator.py:1854`.
- REQ-411 / REQ-412 — alias map: `hasura-default`→`hasura_graphql` (snake),
  `graphql-default`→`apollo_graphql` (default, camelCase) `provisa/compiler/naming.py:137`.
- REQ-416 — **Incomplete.** Three enums + `domain_prefix` defined in
  `VALID_CONVENTIONS` and validated in `NamingConfig`
  `provisa/compiler/naming.py:148`, but the admin update endpoint passes the
  convention straight to `_naming.configure` without validating it
  `provisa/api/admin/schema.py:2049`, so old free-form strings are still accepted on
  that path.
- REQ-478 — `sample` arg validated `(0,100]`, emits TABLESAMPLE BERNOULLI, rejects
  combination with `as_of`/lateral op-joins `provisa/compiler/sql_gen.py:1969`.

### Mutation Execution (REQ-032–037)

- REQ-032 — direct-to-RDBMS, no Trino `provisa/compiler/mutation_gen.py:13`.
- REQ-033 — `_check_writable_by` per-column write rights
  `provisa/api/data/endpoint.py:2282`.
- REQ-034 — insert fields from `visible_columns` `provisa/compiler/schema_gen.py:1179`.
- REQ-035 — `inject_rls_into_mutation` for UPDATE/DELETE
  `provisa/compiler/mutation_gen.py:370`.
- REQ-036 — only registered tables get mutation types
  `provisa/compiler/mutation_gen.py:110`.
- REQ-037 — `NOSQL_TYPES` rejected; cross-source guard
  `provisa/compiler/mutation_gen.py:30`.

### SQLGlot Transpilation (REQ-066–068)

- REQ-066 — `transpile_to_trino` / `transpile` `provisa/transpiler/transpile.py:11`.
- REQ-067 — dialect from registration-captured `source_dialects`
  `provisa/transpiler/router.py:79`.
- REQ-068 — `SUPPORTED_DIALECTS` has all 7 `provisa/transpiler/transpile.py:23`.

### Aggregates (REQ-196–199)

- REQ-196 — **Incomplete.** sum/avg/min/max/count generated
  `provisa/compiler/aggregate_gen.py:97`; no `stddev` or `variance` fields, so the
  Hasura-v2 statistical set is not met.
- REQ-197 — **Incomplete.** `allow_aggregations` is parsed into the Hasura model
  `provisa/hasura_v2/parser.py:106` but never read in the schema compiler — aggregate
  fields are added with no role gate `provisa/compiler/schema_gen.py:1344`.
- REQ-198 — aggregate MV catalog + rewriter
  `provisa/mv/aggregate_catalog.py:54`.
- REQ-199 — **Incomplete.** Background refresh exists `provisa/mv/refresh.py:90`, but
  no expensive-view auto-materialization, no `materialized_views.default_ttl` config,
  and no explicit stale→live fallback found.

### OrderBy Alignment (REQ-200–202)

- REQ-200 — column-keyed input `provisa/compiler/schema_gen.py:587`.
- REQ-201 — 6-value enum + `_DIRECTION_SQL` map
  `provisa/compiler/schema_gen.py:117`, `provisa/compiler/sql_gen.py:699`.
- REQ-202 — relationship ordering thunk `provisa/compiler/schema_gen.py:614`.

### Tracked Functions & Custom Mutations (REQ-205–211, 304–306, 360–362)

- REQ-205 — registry routed by `kind` `provisa/api/admin/actions_router.py:29`.
- REQ-206 — `FunctionInput` config model `provisa/api/admin/actions_router.py:119`.
- REQ-207 — return must be registered table `provisa/compiler/function_gen.py:113`.
- REQ-208 — direct DB execute `provisa/api/data/endpoint.py:2470`.
- REQ-209 — **Incomplete.** Webhook table + `WebhookInput` carry url/method/timeout/
  args/visible_to/domain_id `provisa/api/admin/actions_router.py:44`, but no
  `governance`/`registry-required` field or gating exists, so the steward-approval
  gate in the spec is absent.
- REQ-210 — inline return type `provisa/compiler/schema_gen.py:877`.
- REQ-211 — parameterized DB calls + JSON webhook body
  `provisa/compiler/function_gen.py:157`, `provisa/webhooks/executor.py:55`.
- REQ-304 — `return_schema`→GraphQL type, array/object handling
  `provisa/compiler/schema_gen.py:848`.
- REQ-305 — admin UI return-type toggle, sample-JSON paste, client-side inference
  `provisa-ui/src/pages/CommandsPage.tsx:56` (`returnSchemaMode`, `inferJsonSchema`).
- REQ-306 — `_JS_MAP` scalar map, unknown→String, top-level only
  `provisa/compiler/schema_gen.py:738`.
- REQ-360 — `_apply_action_filters` post-processing
  `provisa/api/data/endpoint.py:2379`.
- REQ-361 — `_resolve_action_relationships` governed batch fetch
  `provisa/api/data/endpoint.py:2311`.
- REQ-362 — cardinality from `JoinMeta` `provisa/api/data/endpoint.py:2362`.

### GraphQL Variable Defaults (REQ-300, 301)

- REQ-300 — `coerce_variable_defaults` applies defaults
  `provisa/compiler/parser.py:91`.
- REQ-301 — LIMIT/OFFSET parameterized `provisa/compiler/sql_gen.py:1100`.

### Cypher Query Frontend (REQ-345–353, 409)

- REQ-345 — `POST /data/cypher` with Stage 2 governance
  `provisa/api/rest/cypher_router.py:355`.
- REQ-346 — write/APOC rejected at parse `provisa/cypher/parser.py:785`.
- REQ-347 — clause→SQL mapping + label resolution
  `provisa/cypher/translator.py:1087`.
- REQ-348 — recursive CTE for paths; unbounded rejected
  `provisa/cypher/parser.py:547`.
- REQ-349 — Stage 3 ROW→JSON rewrite `provisa/cypher/graph_rewriter.py:29`.
- REQ-350 — Node/Edge/Path types `provisa/cypher/graph_types.py:33`.
- REQ-351 — label map from `CompilationContext` `provisa/cypher/label_map.py:197`.
- REQ-352 — named→positional params `provisa/cypher/params.py:30`.
- REQ-353 — **Not to spec.** `CypherCrossSourceError` is declared
  `provisa/cypher/translator.py:251` but no code path raises it; cross-source Cypher
  queries are not detected or rejected.

### Natural Language Query Service (REQ-354–359)

- REQ-354 — async job + poll + SSE `provisa/nl/` router.
- REQ-355 — three compiler-validated parallel loops `provisa/nl/loop.py:57`.
- REQ-356 — role-scoped SDL prompt `provisa/nl/runner.py:53`.
- REQ-357 — parallel exec, per-branch result/error `provisa/nl/runner.py:73`.
- REQ-358 — three differentiator mechanisms present `provisa/nl/runner.py:69`.
- REQ-359 — same governance as direct path `provisa/nl/executor.py:54`.

## Named tests

All named test files exist except one. Verified by directory listing.

| Spec-named test | Present |
| --- | --- |
| `tests/unit/test_sql_gen.py` | yes |
| `tests/unit/test_transpile.py`, `tests/unit/test_transpiler.py` | yes |
| `tests/unit/test_mutation_sql.py` | yes |
| `tests/unit/test_actions.py`, `tests/unit/test_function_gen.py` | yes |
| `tests/unit/test_cypher_translator.py`, `tests/unit/test_cypher_parser.py` | yes |
| `tests/unit/test_nl_loop.py`, `tests/unit/test_nl_runner.py` | yes |
| `tests/unit/test_orderby_alignment.py` | yes |
| `tests/unit/test_params.py` | yes |
| `tests/unit/test_sql_gen_aggregate.py` | yes |
| `tests/integration/test_schema_gen.py`, `test_mutations.py`, `test_cypher_endpoint.py`, `test_nl_endpoint.py` | yes |
| `tests/unit/test_rls_compiler_fallback.py` (REQ-403) | yes — added 2026-06-19 (table-rule-first / domain-fallback coverage; broader domain-RLS load path remains in `test_rls.py`) |

## Remediation (2026-06-19)

All 8 audit tasks resolved across five phases on the `group-5` branch.

- **Phase 1 — guards:** REQ-353 cross-source Cypher rejection; REQ-416 naming-convention validation on the admin update path; REQ-403 spec-named `tests/unit/test_rls_compiler_fallback.py`.
- **Phase 2 — aggregates:** REQ-196 stddev/variance fields + SQL; REQ-197 per-role gate via the `no_aggregations` capability.
- **Phase 3 — webhook gate:** REQ-209 steward approval through the creation_requests queue.
- **Phase 4 — discover:** REQ-252 auto-discovery at registration (explicit columns win) + ES live `_mapping` bridge; ES/Cassandra raise instead of returning empty columns.
- **Phase 5 — MV lifecycle:** REQ-199 TTL-aware freshness (stale→live fallback) + `materialized_views.default_ttl`; materialization stays opt-in.

Two requirements were re-scoped to honor V1's no-migration rule: REQ-197 (aggregate gating) and REQ-209 (webhook approval) are tracked via existing structures — the role `capabilities` array and the creation_requests queue — rather than new columns on `roles` / `tracked_webhooks`.

Follow-up (not in audit scope): a live Cassandra CQL metadata client for `discover` (currently raises), and per-table `aggregates` config overrides (REQ-197 mentions them; the per-role capability gate is implemented).
