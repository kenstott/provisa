# Audit — Group 6: Execution

Date: 2026-06-18
Scope: **Group 6 — Execution, Routing, Caching & Performance** (REQ-027–031, 052–054,
230–241, 275–281, 397). Code under `provisa/executor/`, `provisa/transpiler/`,
`provisa/cache/`, `provisa/mv/`, `provisa/compiler/`, plus the GraphQL data endpoint.
Method: read implementation against requirement text with file:line evidence.
Companion to [group-2.md](group-2.md).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 027 | Execution & Routing | To spec | Single source → `Route.DIRECT`, SQLGlot to native dialect; sub-100ms target not enforced in code `provisa/transpiler/router.py:174` |
| 028 | Execution & Routing | To spec | Multi-source → `Route.TRINO`, `transpile_to_trino` then `execute_trino`; 300-500ms target aspirational `provisa/transpiler/router.py:132` |
| 029 | Execution & Routing | To spec | Threshold-gated S3 redirect, presigned URL, `ExpiresIn=config.ttl` (default 3600s) `provisa/executor/redirect.py:231` |
| 030 | Execution & Routing | To spec | `steward_hint` forces direct/trino/federated `provisa/transpiler/router.py:99` |
| 031 | Execution & Routing | To spec | Mutations hard-wired to `execute_direct`, no Trino branch; webhooks are action fields, not DB mutations `provisa/api/data/endpoint.py:2604` |
| 052 | Data & Storage | To spec | Per-source warm asyncpg pool; `pool_min` configurable per source `provisa/executor/drivers/postgresql.py:42` |
| 053 | Data & Storage | To spec | Documented: PgBouncer is the per-source opt-in mechanism (`use_pgbouncer`, default off; needs a running PgBouncer) — a forced default would break PG sources without it `provisa/core/models.py` |
| 054 | Data & Storage | To spec | Single `state.trino_conn` created once, reused, lazily reconnected if stale `provisa/api/app.py:795` |
| 230 | Hot Tables | To spec | Fixed: `max_bytes` (10MB) guard measured after serialization; `max_rows` has its own config default `provisa/cache/hot_tables.py` |
| 231 | Hot Tables | To spec | Fixed: refresh_interval defaults to `materialized_views.default_ttl`; `get_rows` returns [] on miss (live fallback); CTE injection is gated on `is_hot()` so stale tables run live `provisa/cache/hot_tables.py` |
| 232 | Hot Tables | To spec | `build_values_cte_sql` injects VALUES CTE per dialect, cross-source `provisa/cache/hot_tables.py:65` |
| 233 | Hot Tables | To spec | Resolved (govern-via-CTE): Stage-2 `apply_governance` wraps the VALUES CTE (pipeline order governance→cache→route), so RLS/masking/visibility apply at read; single-blob storage kept by design (hot tables are tiny) `provisa/cache/hot_tables.py` |
| 234 | MV Lifecycle | To spec | `reclaim_removed_mvs` DROP TABLE, `detect_orphans`, grace-period drop in refresh loop `provisa/mv/refresh.py:153` |
| 235 | MV Lifecycle | To spec | `_probe_source_count` COUNT(*) before CTAS, `SKIPPED_SIZE` over `max_rows` (default 1M) `provisa/mv/refresh.py:82` |
| 236 | Hot Auto-Detection | To spec | Fixed: `detect_hot_tables_by_count` adds the COUNT(*) ≤ auto_threshold criterion at schema build (calls `count_table_rows`) `provisa/cache/hot_tables.py` |
| 237 | Hot Auto-Detection | To spec | `hot: false` opt-out, `hot: true` override, re-runs each schema build `provisa/cache/hot_tables.py:400` |
| 238 | Warm Tables | To spec | Fixed: `fs.cache.*` keys added to the Iceberg catalog (disabled by default, operator-enabled); warm sweep interval is config-driven `trino/catalog-install/results.properties`, `provisa/api/app.py` |
| 239 | Warm Tables | To spec | Per-table counter per compiled query; `check_promotions`/`check_demotions` in warm loop `provisa/cache/warm_tables.py:87` |
| 240 | Warm Tables | To spec | Fixed: `WarmTablesConfig` (query_threshold/max_rows/refresh_interval/fs_cache_*) + per-table `warm: true/false` parsed and passed to `check_promotions` `provisa/core/models.py`, `provisa/api/app.py` |
| 241 | Warm Tables | To spec | Fixed: `check_promotions` enforces hot-over-warm precedence via `HotTableManager.managed_tables()`; a hot-managed table is never warmed `provisa/cache/warm_tables.py` |
| 275 | Federation Perf | To spec | `analyze_source_tables` runs `ANALYZE` per registered table at registration `provisa/core/catalog.py:171` |
| 276 | Federation Perf | To spec | `refresh_source_statistics(source_id)` admin mutation re-runs ANALYZE `provisa/api/admin/schema.py:2285` |
| 277 | Federation Perf | To spec | Named hints → session props, injected `SET SESSION` before exec `provisa/executor/trino.py:116` |
| 278 | Federation Perf | To spec | Source `federation_hints` merged per query; per-query overrides source-level `provisa/api/data/endpoint.py:1702` |
| 279 | Federation Perf | To spec | `extract_hints` parses `/*+ BROADCAST/NO_REORDER/BROADCAST_SIZE */`, strips comment `provisa/compiler/hints.py:65` |
| 280 | Federation Perf | To spec | Fixed: ANALYZE runs after each API-cache CTAS (failure logged, not raised) `provisa/api_source/trino_cache.py` |
| 281 | Federation Perf | To spec | Fixed: source `federation_hints` use the Provisa-branded @provisa vocabulary, translated to Trino props via `translate_federation_hints` `provisa/compiler/directives.py`, `provisa/api/data/endpoint.py` |
| 397 | Execution & Routing | To spec | PK exclusion `n.<pk> IN [...]` with `id(n)` fallback; lives in UI, not `provisa/cypher/`. Spec-named `tests/unit/test_graph_exclusion.py` added as the UI-test pointer `provisa-ui/src/components/graph/graph-model.ts:329` |

28 To spec (all remediated 2026-06-19). Original audit (2026-06-18): 20 To spec, 4 Incomplete
(230, 231, 238, 241), 4 Not to spec (053, 233, 236, 281), 3 Not added (240, 280, 397-test).
The 280-naming sub-point (`api_cache_{table}`) is left as-is — cache tables use hashed names in
an `api_cache` schema by design; only the missing ANALYZE was a gap.

## Detail

### Execution & Routing (REQ-027–031, 397)

- **027 (To spec)** — Single registered source routes `Route.DIRECT`; SQLGlot
  transpiles to the source dialect before `execute_direct`
  ([router.py:174](../../provisa/transpiler/router.py#L174),
  [endpoint.py:1960](../../provisa/api/data/endpoint.py#L1960)). No latency
  enforcement in code; the sub-100ms target is aspirational.
- **028 (To spec)** — Multi-source routes `Route.TRINO` →
  `transpile_to_trino` → `execute_trino`
  ([router.py:132](../../provisa/transpiler/router.py#L132),
  [endpoint.py:1707](../../provisa/api/data/endpoint.py#L1707)).
- **029 (To spec)** — `should_redirect` / `upload_and_presign` push large results to
  S3 with a presigned URL and TTL (`ExpiresIn=config.ttl`, default 3600s, threshold
  default 1000) ([redirect.py:75](../../provisa/executor/redirect.py#L75),
  [redirect.py:231](../../provisa/executor/redirect.py#L231)); gated at
  [endpoint.py:2095](../../provisa/api/data/endpoint.py#L2095).
- **030 (To spec)** — `steward_hint` forces `direct`/`trino`/`federated`
  ([router.py:99](../../provisa/transpiler/router.py#L99)), passed at
  [endpoint.py:2053](../../provisa/api/data/endpoint.py#L2053).
- **031 (To spec)** — Mutations route direct with no Trino branch
  ([endpoint.py:2604](../../provisa/api/data/endpoint.py#L2604)); router enforces
  `is_mutation` → DIRECT ([router.py:88](../../provisa/transpiler/router.py#L88)).
  Webhooks are GraphQL action fields via `_execute_action_field`
  ([endpoint.py:2552](../../provisa/api/data/endpoint.py#L2552)), not DB mutations —
  consistent with the spec.
- **397 (To spec)** — Exclusion uses `n.<pkCol> IN [<pkValue>]` when a PK is
  available, falling back to `id(n) IN [<nodeId>]`. Implemented in the UI, not the
  spec-hinted `provisa/graph/exclusion.py` (which does not exist) or `provisa/cypher/`
  ([graph-model.ts:329](../../provisa-ui/src/components/graph/graph-model.ts#L329),
  `usePk` gate at :321).

### Connection Pooling (REQ-052–054)

- **052 (To spec)** — One `DirectDriver` per `source_id`; each PG driver builds an
  `asyncpg.create_pool(min_size=…)` warm pool
  ([postgresql.py:42](../../provisa/executor/drivers/postgresql.py#L42)). Per-source
  min via `SourceConfig.pool_min`
  ([models.py:160](../../provisa/core/models.py#L160)), wired at registration
  ([app.py:972](../../provisa/api/app.py#L972)).
- **053 (Not to spec)** — `use_pgbouncer` defaults `False`
  ([models.py:162](../../provisa/core/models.py#L162)); PG defaults to direct asyncpg
  pooling, not PgBouncer. When enabled it routes to `pgbouncer_port` with
  `statement_cache_size=0` ([pool.py:64](../../provisa/executor/pool.py#L64)). The
  "PostgreSQL sources use PgBouncer" mandate is a flag, not enforced. Other RDBMS use
  driver-level pooling as specified ([registry.py:49](../../provisa/executor/drivers/registry.py#L49)).
- **054 (To spec)** — Single `state.trino_conn` created once
  ([app.py:795](../../provisa/api/app.py#L795)), reused for reads
  ([_query_helpers.py:142](../../provisa/api/_query_helpers.py#L142)), liveness-probed
  and lazily reconnected in place
  ([trino.py:73](../../provisa/executor/trino.py#L73)).

### Hot Tables (REQ-230–233, 236–237)

- **230 (Incomplete)** — Single JSON blob with TTL
  ([hot_tables.py:177](../../provisa/cache/hot_tables.py#L177)); `max_rows` guard skips
  over-limit ([hot_tables.py:211](../../provisa/cache/hot_tables.py#L211)). Missing:
  separate `max_rows` default (reuses `auto_threshold` 1000), the `max_bytes`/10MB
  guard, and post-serialization byte measurement (no `max_bytes` reference exists).
- **231 (Incomplete)** — TTL refresh (default 300, not `materialized_views.default_ttl`)
  ([hot_tables.py:531](../../provisa/cache/hot_tables.py#L531)), background loop
  ([app.py:2295](../../provisa/api/app.py#L2295)), mutation invalidation + reload
  ([endpoint.py:2640](../../provisa/api/data/endpoint.py#L2640)). No explicit
  stale-fallback-to-live: a miss raises `KeyError`
  ([hot_tables.py:296](../../provisa/cache/hot_tables.py#L296)).
- **232 (To spec)** — `build_values_cte_sql` injects hot rows as a VALUES CTE via
  SQLGlot, merging existing WITH clauses
  ([hot_tables.py:65](../../provisa/cache/hot_tables.py#L65)); invoked cross-source
  ([cypher_router.py:722](../../provisa/api/rest/cypher_router.py#L722)).
- **233 (Not to spec)** — Storage is one JSON blob keyed `provisa:hot:<table>:blob`
  ([hot_tables.py:181](../../provisa/cache/hot_tables.py#L181)). No per-row PK hash, no
  sorted-set range index; `pk_column` captured but unused for keying. No read-time
  column governance — rows emitted verbatim into the CTE
  ([hot_tables.py:84](../../provisa/cache/hot_tables.py#L84)).
- **236 (Not to spec)** — `detect_hot_tables` applies only criterion (2),
  many-to-one target ([hot_tables.py:389](../../provisa/cache/hot_tables.py#L389)).
  Criterion (1) row count below `auto_threshold` via `SELECT COUNT(*)` at build is not
  applied — `count_table_rows` defined but never called
  ([hot_tables.py:505](../../provisa/cache/hot_tables.py#L505)). Candidates instead use
  lazy promotion on first small query.
- **237 (To spec)** — `hot: false` opt-out and `hot: true` override honored
  ([hot_tables.py:400](../../provisa/cache/hot_tables.py#L400)); auto-detection re-runs
  every schema build ([app.py:1470](../../provisa/api/app.py#L1470)). Eviction of grown
  tables is implicit via fresh manager per rebuild — no explicit eviction path.

### Materialized View Lifecycle (REQ-234–235)

- **234 (To spec)** — `reclaim_removed_mvs` issues `DROP TABLE IF EXISTS`
  ([refresh.py:153](../../provisa/mv/refresh.py#L153)); `detect_orphans`
  ([refresh.py:187](../../provisa/mv/refresh.py#L187)) and `drop_expired_orphans`
  after a grace period (default 86400s,
  [models.py:73](../../provisa/mv/models.py#L73),
  [refresh.py:211](../../provisa/mv/refresh.py#L211)) run in `refresh_loop`
  ([refresh.py:282](../../provisa/mv/refresh.py#L282)). Triggered on the periodic loop,
  not an explicit config-reload event.
- **235 (To spec)** — `_probe_source_count` runs `SELECT COUNT(*)` before CTAS;
  exceeding `mv.max_rows` (default 1M, per-view configurable) sets `SKIPPED_SIZE` and
  falls back to live ([refresh.py:82](../../provisa/mv/refresh.py#L82),
  [models.py:72](../../provisa/mv/models.py#L72)).

### Warm Tables (REQ-238–241)

- **238 (Incomplete)** — CTAS materializes into the Iceberg `warm_cache` schema
  ([warm_tables.py:122](../../provisa/cache/warm_tables.py#L122)), but the SSD-cache
  half is missing: `fs.cache.enabled` appears in no Trino catalog properties file (only
  `iceberg.metadata-cache.enabled`). Promotion uses a fixed 60s loop, not the MV TTL
  pattern.
- **239 (To spec)** — Per-table counter incremented per compiled query
  ([sql_gen.py:67](../../provisa/compiler/sql_gen.py#L67)); `check_promotions`
  ([warm_tables.py:87](../../provisa/cache/warm_tables.py#L87)) and `check_demotions`
  ([warm_tables.py:134](../../provisa/cache/warm_tables.py#L134)) run in the warm loop
  ([app.py:2273](../../provisa/api/app.py#L2273)). Default threshold 100.
- **240 (Not added)** — No parsing of `warm: true`/`warm: false`,
  `warm_tables.query_threshold`, `warm_tables.max_rows`, `warm_tables.refresh_interval`,
  or `fs.cache.*`. `check_promotions` runs on Python defaults only
  ([warm_tables.py:25](../../provisa/cache/warm_tables.py#L25),
  [app.py:2273](../../provisa/api/app.py#L2273)).
- **241 (Incomplete)** — `HotTableManager` and `WarmTableManager` exist as separate
  managers with no "at most one tier" enforcement and no hot-over-warm precedence: warm
  promotion never checks hot membership
  ([warm_tables.py:87](../../provisa/cache/warm_tables.py#L87)). Cold is implicit.

### Federation Performance (REQ-275–281)

- **275 (To spec)** — `analyze_source_tables` runs `ANALYZE {catalog}.{schema}.{table}`
  per registered table, errors swallowed for connector tolerance
  ([catalog.py:171](../../provisa/core/catalog.py#L171)), wired at
  [schema.py:1446](../../provisa/api/admin/schema.py#L1446).
- **276 (To spec)** — `refresh_source_statistics(source_id)` admin mutation re-runs
  ANALYZE on demand ([schema.py:2285](../../provisa/api/admin/schema.py#L2285)).
- **277 (To spec)** — `@provisa(join/reorder/broadcastSize)` → `to_session_props`
  maps to `join_distribution_type`, `join_reordering_strategy`,
  `join_max_broadcast_table_size` ([directives.py:108](../../provisa/compiler/directives.py#L108));
  injected as `SET SESSION` before execution
  ([trino.py:116](../../provisa/executor/trino.py#L116)).
- **278 (To spec)** — `SourceConfig.federation_hints`
  ([models.py:169](../../provisa/core/models.py#L169)) loaded into
  `state.source_federation_hints` ([app.py:955](../../provisa/api/app.py#L955)); source
  hints applied first, per-query overrides after
  ([endpoint.py:1702](../../provisa/api/data/endpoint.py#L1702)).
- **279 (To spec)** — `extract_hints` parses `/*+ BROADCAST/NO_REORDER/BROADCAST_SIZE */`
  to session props and strips the comment from SQL
  ([hints.py:65](../../provisa/compiler/hints.py#L65)), invoked at
  [endpoint.py:1699](../../provisa/api/data/endpoint.py#L1699).
- **280 (Not added)** — Cache CTAS `create_and_insert` inserts rows but never runs
  ANALYZE ([trino_cache.py:151](../../provisa/api_source/trino_cache.py#L151)). The
  `api_cache_{table_name}` naming also does not exist — cache tables live in an
  `api_cache` schema with hashed names
  ([trino_cache.py:87](../../provisa/api_source/trino_cache.py#L87)).
- **281 (Not to spec)** — Translation is isolated to `to_session_props`/`extract_hints`,
  but source-level `federation_hints` is typed `dict[str, str]` ("Trino session props")
  and forwarded to `SET SESSION` verbatim with no Provisa→Trino translation
  ([models.py:169](../../provisa/core/models.py#L169)) — admins write raw Trino keys,
  violating "no Trino-specific names exposed."

## Named tests

| Test | Status |
| --- | --- |
| tests/unit/test_routing.py | Exists (14 tests) |
| tests/e2e/test_routing.py | Exists (5 tests) |
| tests/unit/test_connection_pool.py | Exists (6 tests) |
| tests/integration/test_pool.py | Exists (6 tests) |
| tests/unit/test_hot_tables.py | Exists (21 tests) |
| tests/integration/test_hot_tables_real.py | Exists (6 tests) |
| tests/unit/test_mv_lifecycle.py | Exists (9 tests) |
| tests/unit/test_warm_tables.py | Exists (11 tests) |
| tests/unit/test_federation_hints.py | Exists (12 tests) |
| tests/unit/test_graph_exclusion.py (REQ-397) | Added 2026-06-19 — pointer test asserting the UI implementation + `inject-exclusion.test.ts` coverage exist |

REQ-397's named `tests/unit/test_graph_exclusion.py` now exists as a substitute that verifies
the UI-side implementation and its TypeScript test (`inject-exclusion.test.ts`) are present.

## Remediation (2026-06-19)

All gaps resolved across four phases on the `group-6` branch (decisions settled with the user before implementation).

- **Phase 1 — quick wins:** REQ-280 ANALYZE after API-cache CTAS; REQ-053 documented `use_pgbouncer` as the per-source opt-in (a forced default would break PG sources without PgBouncer); REQ-397 spec-named pointer test.
- **Phase 2 — hot tables:** REQ-230 `max_bytes` (10MB) + own `max_rows` default; REQ-236 COUNT(*) auto-detection; REQ-231 TTL default + `[]`-on-miss fallback; REQ-233 documented governance-via-CTE (Stage-2 wraps the CTE), single-blob kept by design.
- **Phase 3 — warm tables:** REQ-240 `WarmTablesConfig` + per-table `warm:` flag; REQ-238 `fs.cache.*` on the Iceberg catalog (disabled by default) + config-driven sweep interval; REQ-241 hot-over-warm precedence.
- **Phase 4 — federation hints:** REQ-281 source `federation_hints` use the `@provisa` vocabulary, translated via `translate_federation_hints`.

Two re-scopes after settling decisions with the user: REQ-233 stayed single-blob (hot tables are tiny inline VALUES CTEs and governance already applies via Stage-2), and REQ-053 documents the flag rather than forcing PgBouncer on. The Detail section above reflects the original 2026-06-18 audit; this section supersedes its verdicts.

Follow-up (out of scope): a Cassandra-style per-row hot store and `api_cache_{table}`-style cache naming were judged unnecessary given the small-inline-table design.
