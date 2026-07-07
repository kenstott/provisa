# DRY-Cross plan — `provisa/federation` runtime family

**Scope:** `provisa/federation` (25 modules, 4626 LOC).
**Verdict:** one real cross-file duplication cluster — the four `*FederationRuntime`
classes. Backend family (`*_backend.py`) is already correctly factored; no action.
**Mechanism:** shared **free-function helpers** (+ one optional behavioral mixin) —
**not** a base class. See trigger analysis below.

## Not a finding — already earned abstraction (contrast case)

`duckdb_backend.py`, `clickhouse_backend.py`, `pg_backend.py`, `sqlalchemy_backend.py`
already extend `NativeEngineBackend` (native_backend.py:44), which owns the whole
lifecycle; each subclass supplies only `_new_runtime()` and `_attach_errors`. This is
the base/concrete trigger *correctly* satisfied — subtypes share state (`_runtime`,
`_attached`) and lifecycle. Left untouched.

Minor: `from provisa.federation.engine import configured_engine_url` is re-imported
inside `_new_runtime` in clickhouse/pg/sqlalchemy backends (clickhouse_backend.py:27,
pg_backend.py:31, sqlalchemy_backend.py:28). Cosmetic; not worth a move.

## Finding — parallel implementations in the runtime family

Four standalone classes conform to the same runtime protocol but share **no base**:
`DuckDBFederationRuntime`, `ClickHouseFederationRuntime`, `PgFederationRuntime`,
`SqlAlchemyFederationRuntime`. Confirmed duplication (grep-verified):

| # | Duplicated logic | Sites | Concretes |
|---|---|---|---|
| A | async `run` = `run_in_executor(None, lambda: self.run_sync(...))` | sqlalchemy_runtime.py:66-68, pg_runtime.py:104-106, clickhouse_runtime.py:275-277 | 3 (duckdb diverges) |
| B | `run_sync` builds `QueryResult` from a DBAPI result object (`cols=[d[0] for d in X.description] if X.description else []`, `rows=X.fetchall()`) | pg_runtime.py:96-102 (`cur`), sqlalchemy_runtime.py:57-64 (`cur`), duckdb_runtime.py:185-189 (`res`) | 3 (clickhouse excluded — no cursor) |
| C | `introspect_columns` tail `{row[0]: str(row[1]).lower() for row in ...}` | duckdb_runtime.py:156-165, clickhouse_runtime.py:251-260 | 2 |
| D | `execute` = `await self.run(transpile(sql, "<dialect>"))` | duckdb_runtime.py:169-171, clickhouse_runtime.py:264-266 | 2 |

## Base/concrete trigger analysis (why NOT a base class)

- **Count:** 4 concretes → count trigger satisfied.
- **State + lifecycle:** FAILS. Each holds a different connection object
  (SA raw_connection, psycopg2 conn, duckdb conn, `_CHBackend`), different
  constructor signatures, no shared instance fields.
- **Varying axes diverge:** attach model (in-place FDW vs LAND vs self-only no-op),
  namespace (3-part catalog vs flat `db.table`), commit model (sqlalchemy_runtime.py:63
  explicit `commit()` vs pg_runtime.py:36 `autocommit`). A stateful base would fight
  these.
- **Conclusion:** per the rule, prefer **composition / free functions**. The shared
  items are behaviorless. A single-impl stateful base here would be Speculative
  Generality.

## Plan (ordered, behavior-preserving)

New module: `provisa/federation/runtime_support.py` (pure helpers, no state).

**Step 1 — `result_from_dbapi(obj) -> QueryResult`** (finding B)
- What: extract the `cols`/`rows`-from-`.description` + `QueryResult(...)` build. Keyed
  on the DBAPI-result protocol (`.description`, `.fetchall()`) — so it takes the
  cursor *or* result object, covering all three drivers, not just the `cur`-named two.
- Covers **3** sites: pg_runtime.py:99-102 (`cur`), sqlalchemy_runtime.py:60-64
  (`cur`), duckdb_runtime.py:187-189 (`res`) → each passes its object to the helper.
- Excluded: **clickhouse** — clickhouse_runtime.py:270-273 has no cursor/result object;
  it delegates to `self._backend.query(sql)` which already returns `(rows, cols)`. By
  construction it cannot share this helper. Correctly out of scope, not an omission.
- Keep in concretes: the engine-specific parts — sqlalchemy's `self._con.commit()`
  (line 63), each driver's `execute` call, and duckdb's `params`-vs-no-`params` branch
  (line 187) stay put; only the result-shaping tail moves.
- Metric: −~4 LOC ×3; kills the duplicated block.
- Behavior: identical result shape; **commit must remain** in sqlalchemy's `run_sync`;
  duckdb's `fetchall()`-without-`description`-guard is subsumed (the helper guards on
  `.description`, which is behavior-equivalent — DuckDB reports `None` description for
  non-SELECT, yielding `[]` rows, same as today).
- Guarded by: `tests/integration/test_pg_runtime_e2e.py`,
  `tests/integration/test_sqlalchemy_runtime_e2e.py`,
  `tests/integration/test_duckdb_runtime_e2e.py`.
- Risk: none (leaf helper, no imports back into federation). Verify the duckdb
  `.description`-guard equivalence with its e2e (non-SELECT DDL path).

**Step 2 — `columns_from_describe(rows) -> dict[str,str]`** (finding C)
- What: the `{row[0]: str(row[1]).lower() for row in rows}` tail.
- From: duckdb_runtime.py:165 (`res.fetchall()`), clickhouse_runtime.py:260 (`rows`).
- Keep in concretes: `attach_source` + phys-name + the DESCRIBE call (they differ).
- Behavior: identical dict.
- Guarded by: `test_duckdb_runtime_e2e.py`, `test_clickhouse_runtime_e2e.py`.
- Risk: none.

**Step 3 — `run_async(run_sync, sql, params)` helper OR `AsyncRunFromSync` mixin**
(finding A)
- What: the 3 identical async `run` bodies.
- Option 3a (helper): each `run` becomes
  `return await run_async(self.run_sync, sql, params)`.
- Option 3b (mixin): a stateless mixin providing `run` in terms of `self.run_sync`;
  sqlalchemy/pg/clickhouse (and duckdb, after aligning) add it to their bases.
  Prefer 3a unless duckdb is aligned — a one-method mixin is thin.
- duckdb note: duckdb_runtime.py:173-189 inlines a *different* `run` (does not call
  `run_sync`). Optional follow-up: refactor duckdb's `run` to delegate to its own
  `run_sync`, then it joins the pattern. Flag only — behavior-equivalent but a
  separate, reviewable step.
- Guarded by: all four `*_runtime_e2e.py`.
- Risk: none for 3a; 3b touches class bases (low).

**Step 4 (optional) — parameterize `execute` transpile wrapper** (finding D)
- Two 1-line methods differing only by dialect literal. Low value; fold only if
  Step 3 introduces a mixin (add `_DIALECT` class attr + shared `execute`). Otherwise
  leave — extracting a 1-liner is not worth the indirection.

## Verification (per step)

```bash
.venv/bin/python -m pytest tests/integration/test_pg_runtime_e2e.py \
  tests/integration/test_sqlalchemy_runtime_e2e.py \
  tests/integration/test_duckdb_runtime_e2e.py \
  tests/integration/test_clickhouse_runtime_e2e.py
```

## Execution note

Plan-only. Hand to the `refactorer` agent, one step per commit. Do not fold in the
duckdb `run` alignment with the extractions — keep it a separate, flagged step.
