# Anti-Pattern Refactor Plan

Status tracker for eliminating fallback/silent-error/masking anti-patterns found in the full-codebase review (82k Py + 44k TS).

**Status legend:** `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` skipped (with reason)

## Guiding rule (per CLAUDE.md)
Every mask is replaced by one of three explicit semantics — never a silent substitute:
- **(P) Propagate** — unexpected failure → let it raise / re-raise.
- **(L) Log+degrade** — best-effort paths (teardown, telemetry) → log at warning, continue, but never fabricate a *value*.
- **(S) Distinct sentinel** — "not found" gets its own return type, separate from the error path.

Each fix is **red-first**: write a failing test that asserts the error surfaces (or the correct value flows), then fix.

Phases are dependency-ordered (each raises the floor of "errors are visible" before the next). Phase 5 is orthogonal. Phases 2 and 3 internally parallelize across module owners.

---

## Phase 1 — Governance/security correctness (do first, serially reviewed)
Silently weaken access control. Highest risk; smallest batch.

- [ ] **nl tables** — [nl/runner.py:115,118](provisa/nl/runner.py#L115) + [executor.py:101,137,190](provisa/nl/executor.py#L101): `tables` is NOT an AppState field (verified against app.py:71-140), so `getattr(app_state,"tables",[])` always yields `[]`; `raw_tables` feeds `build_governance_context` and the NL prompt. Fix: find real source (`_fetch_tables(_pg)`/cached dict), thread it in, delete the `getattr(...,[])`. Test: NL governance receives non-empty tables for a role with masked columns; masking actually applies.
- [ ] **pgwire V000** — [pgwire/_pipeline.py:116](provisa/pgwire/_pipeline.py#L116): `except Exception: pass` skips the V000 table-access check (SQL already parsed at :71). Target **(P)**. Test: malformed access-check input → V000 raised, not skipped.
- [ ] **cypher predicate** — [cypher/translator.py:1491](provisa/cypher/translator.py#L1491): parse failure → `exp.true()` (always-true filter). Target **(P)**. Test: bad predicate → translation error, not always-true.
- [ ] **auth provider** — [auth/middleware.py:174-176](provisa/auth/middleware.py#L174): `provider_name` default `"unknown"` for 3 of 5 providers; upsert `except: pass`. Fix: add attr to all 5 providers (no default); upsert **(P or L with log)**.

**Gate:** full auth + governance suite green before Phase 2.

## Phase 2 — Silent data corruption (parallel by module)
Masks that return wrong data as success.

- [ ] [sql_gen.py:69](provisa/compiler/sql_gen.py#L69) — config-load `except` returns hardcoded `100` row limit; comment says `10000`. **(P)** + fix comment.
- [ ] [sql_gen.py:331](provisa/compiler/sql_gen.py#L331) — `_nfc_type` returns `"text"` on unresolvable type. **(P)**.
- [ ] [cache/store.py:218,241](provisa/cache/store.py#L218) — `invalidate_by_pattern`/`by_table` return `0` on Redis error → callers serve stale. **(P)**.
- [ ] [executor/drivers/postgresql.py:77](provisa/executor/drivers/postgresql.py#L77) — empty PgBouncer result → regex-guess columns from SQL. **(P)** (no guess).
- [ ] [executor/trino_write.py:97](provisa/executor/trino_write.py#L97) — fabricated `row_count=0` on missing count row. **(P)**.
- [ ] [subscriptions/polling_provider.py:66](provisa/subscriptions/polling_provider.py#L66) — watermark → `datetime.now()`. **(P)**.
- [ ] [subscriptions/trino_polling_provider.py:121](provisa/subscriptions/trino_polling_provider.py#L121) — watermark → `datetime.now()`. **(P)**.
- [ ] [subscriptions/pg_provider.py:68,113](provisa/subscriptions/pg_provider.py#L68) — `op` default `"unknown"`. **(P)**.
- [ ] [mv/refresh.py:56](provisa/mv/refresh.py#L56) — introspection failure → `SELECT left.*` drops right-table columns. **(P)**.
- [ ] [sources/counts.py:33](provisa/sources/counts.py#L33) — no `raise_for_status`; `errors`-only response → zero counts. Add `raise_for_status`.
- [ ] [kafka/schema_registry.py:84](provisa/kafka/schema_registry.py#L84) — non-200 collapsed to `False`. **(S)** (non-200 raises, distinct from real "incompatible").
- [ ] [hasura_v2/mapper.py:73-103](provisa/hasura_v2/mapper.py#L73) — bad `database_url` → default `localhost:5432/default/postgres`. **(P)**.

## Phase 3 — not-found vs failure conflation (mechanical, parallel)

### Batch A — admin/schema.py cluster (14 sites)
Connection/query failure must raise (P); genuine emptiness returns `[]`. One test harness reused.
- [ ] [schema.py](provisa/api/admin/schema.py) lines 111, 242, 431, 557, 949, 1004, 1053, 1135, 1235, 1262, 1276, 1316 — Trino/OpenAPI/Redis fetches returning `[]`/`{}`/noop on any error.

### Batch B — config/discovery
- [ ] [admin/_config_io.py:22](provisa/api/admin/_config_io.py#L22) — `read_config` returns `{}` for missing file AND malformed YAML. **(S)**.
- [ ] [core/config_loader.py:208](provisa/core/config_loader.py#L208) — `_upsert_sources` `except: pass`; `resolve_secrets` runs before catalog logging.
- [ ] [discovery/catalog_cache.py:140,188](provisa/discovery/catalog_cache.py#L140) — column-fetch/native-tables error → `[]`/`None`, no log.
- [ ] [discovery/table_search.py:144](provisa/discovery/table_search.py#L144) — malformed LLM JSON → `[]`, no log.
- [ ] [admin/introspect.py:243](provisa/api/admin/introspect.py#L243) — introspection error → `[]`.

### Batch C — protocol/misc
- [ ] [bolt/session.py:91,572,657](provisa/bolt/session.py#L91) — app-import failure ≡ user-not-found; `_count` returns 0; edge batch dropped via `except: continue`.
- [ ] [api/rest/cypher_router.py:850](provisa/api/rest/cypher_router.py#L850) — `_run_count` returns `0` on any pipeline failure → corrupts `graph_counts`.
- [ ] [api/data/subscribe.py:109](provisa/api/data/subscribe.py#L109) — JSON parse → `None`, hides parse error.
- [ ] [govdata/schema_import.py:130](provisa/govdata/schema_import.py#L130) — view-definition query error ≡ "no definition".
- [ ] [apq/cache.py:109](provisa/apq/cache.py#L109) — Redis error → `None`, indistinguishable from cache miss.
- [ ] [executor/trino_write.py:57](provisa/executor/trino_write.py#L57) — "schema may already exist" masks real DDL/permission errors (debug-only).

## Phase 4 — transpiler + teardown swallows (low risk)

### Transpiler parse-masks (systemic) — target (L): log before returning original SQL
- [ ] [transpiler/transpile.py:48,75,133,522](provisa/transpiler/transpile.py#L48)
- [ ] [compiler/nf_extractor.py:83,147,156,171,225,258](provisa/compiler/nf_extractor.py#L83)
- [ ] [compiler/sql_gen.py:956](provisa/compiler/sql_gen.py#L956)
- [ ] [pgwire/catalog.py:2667,2918](provisa/pgwire/catalog.py#L2667)

**Open decision:** L (log+continue) vs P (propagate)? Recommend L first (make visible), audit logs, promote to P where original SQL is actually invalid downstream.

### Teardown/telemetry swallows — target (L): add logging, keep continue
- [ ] [api/app.py:1428,1977,3022](provisa/api/app.py#L1428)
- [ ] [live/engine.py:149](provisa/live/engine.py#L149)
- [ ] [events/triggers.py:136](provisa/events/triggers.py#L136)
- [ ] [admin/settings_router.py:168,214,413](provisa/api/admin/settings_router.py#L168)
- [ ] [subscriptions/govdata_provider.py:141](provisa/subscriptions/govdata_provider.py#L141)

## Phase 5 — Structural (separate track, behavior-preserving)

### File splits (12 files >1000 lines) — snapshot/regression tests prove no behavior change
- [ ] Python: [api/app.py](provisa/api/app.py) 3617 · [api/data/endpoint.py](provisa/api/data/endpoint.py) 3382 · [compiler/sql_gen.py](provisa/compiler/sql_gen.py) 3069 · [pgwire/catalog.py](provisa/pgwire/catalog.py) 3057 · [api/admin/schema.py](provisa/api/admin/schema.py) 2790 · [cypher/translator.py](provisa/cypher/translator.py) 2485 · [compiler/schema_gen.py](provisa/compiler/schema_gen.py) 1542 · [api/rest/cypher_router.py](provisa/api/rest/cypher_router.py) 1540 · [cypher/sql_to_cypher.py](provisa/cypher/sql_to_cypher.py) 1288 · [transpiler/transpile.py](provisa/transpiler/transpile.py) 1070
- [ ] TS: SqlPage 3652 · SourcesPage 2669 · SqlModelingModal 2570 · TablesPage 2344 · GraphCanvas 1445 · CommandsPage 1338 · ErdModal 1195 · GraphFrame 1183

### Magic-number constants
- [ ] cache TTL `300` ×6 in [api/app.py](provisa/api/app.py) → named constant
- [ ] `500`/`300` defaults in [api/data/endpoint.py](provisa/api/data/endpoint.py) → named constants
- [ ] `timeout=30.0` ×2 in [subscriptions/pg_provider.py](provisa/subscriptions/pg_provider.py) → named constant

---

## Notes
- No bare `except:`, no mutable default args, no empty JS catches found — those categories are clean.
- Known-intentional (do NOT "fix"): `role or (user if user else None)` in adbc.py/dbapi.py (REQ-AK5).
