# Row-Level, Query-Driven Materializer — Design

Status: design only, not implemented. Supersedes issue #120's "incremental refresh"
framing for tables that opt in (see "Relationship to `apply_cdc_events` and
`materialize_pending`" below); #120's own proposal (delta-fetch during a stale
whole-table land) is left as the mechanism for tables that do NOT opt in.

Motivating case: `demo/named/perf/fragment.yaml`'s `bench_order_node` (Neo4j
source, ~2M rows, `query_template` is a fixed `MATCH (o:Order) RETURN ...` scan).
A point-lookup query against it pays a 60-120s full re-land (confirmed live,
`provisa/federation/backend.py:297`'s `materialize_pending`) for a handful of rows.

## 0. Hard constraints (given, not re-derived here)

1. Per-table opt-in (a `registered_tables` column), not a global engine mode.
2. Never eager — a row is fetched only when a query's resolved plan names its PK value.
3. Always the full row — no column-subset caching.
4. TTL is per row, not per table.
5. Requires a declared primary key; no design for a PK-less table.
6. A declared PK is trusted, not verified. Provisa never runtime-checks that
   `is_primary_key` columns are actually unique/stable against the live source —
   same trust level as `data_type`, `cardinality`, etc. This design adds no
   PK-validity check anywhere (source-side or store-side).

## 1. Table-registry schema/config addition

### 1a. `provisa/core/schema.sql`

Add to the `registered_tables` migration block (next to the existing
`materialize`/`mv_*` columns, `provisa/core/schema.sql:218-230`), no new migration
file (V1 project rule — this file IS the schema, a fresh install rebuilds from it):

```sql
ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS row_materialize BOOLEAN NOT NULL DEFAULT FALSE;  -- REQ-TBD
```

No new TTL column. Constraint 4 ("TTL is per row, not per table") is about the
freshness CLOCK — each cached row's own `_row_cached_at`/`_row_expires_at`
(section 2a) expiring independently of every other row's — not about the
configured TTL *duration* needing its own field. The duration itself reuses
the table's existing `cache_ttl` (models.py:878, already resolvable per-table
with source-level inheritance, `None` = inherit): `_row_expires_at =
_row_cached_at + resolved cache_ttl`. One setting, tracked per row. Naming
otherwise avoids `materialize` (already the view_sql-CTAS flag) so the two
mechanisms never collide on that one flag's meaning.

### 1b. `provisa/core/schema_org.py`

Mirror in the `registered_tables` `Table(...)` definition (next to
`Column("materialize", Boolean, nullable=False, server_default=false())` at
line 218 of schema.sql / the corresponding block in schema_org.py):

```python
Column("row_materialize", Boolean, nullable=False, server_default=false()),
```

### 1c. `provisa/core/models.py` — `Table` model

Add next to the existing `cache_ttl` / `materialize` fields (~line 878, 929):

```python
# REQ-TBD: row-level, query-driven materialize. Opt-in per table (never global). A row is
# fetched and cached lazily, strictly when a query's resolved plan names its PK value(s) — never
# eagerly, never a background prefetch of a key nothing has asked for yet. The cached unit is
# always the FULL row (never a column subset). Each cached row carries its OWN freshness clock,
# independent of every other row's, driven off the SAME `cache_ttl` duration this table already
# has (no separate row-level TTL field — one setting, tracked per row). Mutually exclusive with
# `materialize` (view_sql CTAS) and with the existing whole-table MATERIALIZED pull path for the
# same table — a table is either whole-table-landed on staleness or row-landed on demand, never
# both. COMPATIBLE with a push change_signal (debezium/kafka/native): an incoming CDC event for a
# row ALREADY in this table's row cache refreshes it in the background (never inserts a key
# nothing has queried yet — see design doc, "Relationship to apply_cdc_events / materialize_pending").
row_materialize: bool = False
```

`Table` already carries `Column.is_primary_key` (models.py:716) — no new PK
representation is needed, only a validator that requires it be non-empty.

### 1d. Validation

Add a `model_validator(mode="after")` on `Table` (alongside the existing
`_validate_view_definition_forms`, models.py ~line 990) enforcing, as hard
errors (never a silent default, per project convention):

- `row_materialize` requires at least one `Column.is_primary_key == True` among
  `self.columns` (constraint 5). No PK → `ValueError`, not a fallback to
  whole-table materialize.
- `row_materialize` requires a resolved `cache_ttl` (constraint 4 needs a
  duration to drive each row's clock). `Table.cache_ttl` alone may be `None`
  ("inherit source"), so this check cannot live in `Table`'s own
  `model_validator` (it doesn't see `Source`) — it belongs where `cache_ttl`
  inheritance is actually resolved today, the same place `resolve_landing_args`
  (`provisa/federation/residency.py`) and the source/table registration
  path already combine `table.cache_ttl or source.cache_ttl`. Add the check
  there: `row_materialize=True` with both `table.cache_ttl` and
  `source.cache_ttl` `None` is a registration-time `ValueError`, not a
  runtime fallback to an undefined TTL.
- `row_materialize and materialize` is a conflict (`materialize` here is the
  view_sql-CTAS flag, models.py:929) — reject, two materialization concepts on
  one table.
- `row_materialize` and the table's *source*-level `prefer_materialized`/
  `load_protected` are compatible in principle (a row-cache can sit in front of
  a load-protected source too).
- `row_materialize` together with a push `change_signal` (`debezium`/`kafka`/
  `native`) is ALLOWED and is in fact the intended combination for a source
  that can push: see section 5 — a CDC event for a row already in the cache
  becomes this mechanism's background-refresh trigger, handled through the
  same per-key path a query-driven fetch uses (section 3d/4), not a second
  competing writer.

This mirrors how `mv_persist == "upsert"` requires `mv_primary_key` today
(models.py ~970) — same pattern, new field.

## 2. Cache storage shape

### 2a. Where it lives

A distinct code path, not an extension of `land_source_table`/`store_writer.land`.
Those two write faces are shaped around *batch* replace/append/CDC land of
`list[dict]` rows already fetched (`provisa/federation/materialize_exec.py`'s
`land_replace`/`land_append`/`apply_cdc`, `provisa/federation/store_writer.py:304`'s
`land`). Row-level materialize reuses the **upsert primitive** those already
call (`Connection.upsert(table, values, index_elements=pk_columns)`,
`store_writer.StoreConn` protocol) but not the batch-land entrypoints — a
row-level fetch is never "land everything currently known", it's "upsert
exactly these N rows".

New function, same module family as the existing land executors
(`provisa/federation/materialize_exec.py`):

```python
async def land_rows(
    conn: StoreConn,
    table: Table,          # sqlalchemy Table, from build_table() — reused as-is
    pk_columns: list[str],
    rows: list[dict],       # full rows, one per requested PK value that the source returned
) -> None:
    """UPSERT exactly these rows by PK (row-level materialize, REQ-TBD) — never a blind append,
    never a bulk insert. Each row may already exist in the cache (a re-fetch of a stale/CDC-
    touched key, section 5) or may be new (a key's first fetch, section 3d); both cases go
    through the identical UPDATE-by-PK-else-INSERT call, so there is no separate insert-only
    branch that could double a row or skip refreshing its stamped columns. Body:

        json_cols = _json_columns(table)
        for row in rows:
            stamped = {**_coerce_json_row(dict(row), json_cols),
                       "_row_cached_at": now, "_row_expires_at": now + resolved_cache_ttl}
            await conn.upsert(table, stamped, index_elements=pk_columns)

    — the exact per-row `conn.upsert(..., index_elements=pk_columns)` shape `apply_cdc`
    (`materialize_exec.py:224`) already uses for its own insert/update case, applied here to
    every row `land_rows` is given, not just a CDC-tagged subset. Never drops or truncates — this
    is never a replace, only ever an upsert of the rows the caller explicitly fetched."""
```

Table shape = the SAME `build_table(schema, table, columns, pk_columns)` DDL
(`materialize_exec.py:62`) used by the whole-table path, **plus two bookkeeping
columns appended by the caller before `CreateTable`**:

```
_row_cached_at   TIMESTAMP WITH TIME ZONE NOT NULL   -- when this row was last fetched from source
_row_expires_at  TIMESTAMP WITH TIME ZONE NOT NULL   -- _row_cached_at + the table's resolved cache_ttl (this row's own clock)
```

Per-row (not per-table) freshness lives IN the row, as literal columns — the
only way to satisfy constraint 4 (independent expiry per row) without a
second per-row bookkeeping table that would double the write volume for every
fetch. `land_rows` UPSERTS both on every call — a repeat fetch of an
already-cached key (a stale re-fetch, or the CDC path in section 5) overwrites
that row's stamps and data columns in place; it is never appended as a second
row or left as a stale duplicate. `pk_columns` being the upsert's
`index_elements` is what makes this an upsert rather than an append —
identical to how `land_append`'s own `pk_columns` branch upserts instead of
blind-inserting (`materialize_exec.py`'s `land_append`, REQ-960).

### 2b. Physical address

Reuses `EngineBackend.landing_target` unchanged (`backend.py`'s
`landing_target`, `store_schema, f"{source_id}__{schema_name}__{table_name}"`)
— the row-cache replica lives at the exact same physical address a whole-table
MATERIALIZED land of the same table would use. This is deliberate: a table is
either row-materialized or whole-table-materialized (validated mutually
exclusive in section 1d), so there is never a collision between the two shapes
at one address, and every other piece of plumbing that resolves "where does
this registered table's replica live" (the engine's `_expose_landed` view,
`reconcile_landed_tables`, `analyze_landed_table`) keeps working unmodified.

## 3. The read path

### 3a. What this mechanism can and cannot serve — explicit design call

A row-level cache is usable **only** when the compiled plan's predicate on the
row-materialized table resolves to a concrete, bounded set of PK equality
values: `pk = $1`, `pk IN ($1,$2,...)`, or an OR-chain of PK equalities
`sqlglot` normalizes to the same shape. It is **not** usable for: a full scan,
a range predicate (`pk > 100`), an aggregate, a predicate on a non-PK column,
or a join whose only predicate is on the other side.

Design call for the unservable case: **fall back to the existing whole-table
`materialize_pending` path for that table**, not DIRECT/live routing and not a
refusal. Reasoning: `row_materialize` is a registered-table property, and a
row-materialized table still has to answer a `SELECT * FROM t` or
`SELECT count(*)` correctly — those aren't errors, they're just not what this
optimization accelerates. Routing them DIRECT would silently change the
table's federation strategy per-query (the table's whole-table strategy,
whatever `prefer_materialized`/engine cost-based promotion decided, is
untouched by this feature) and refusing them would make `row_materialize` an
unsafe flag to set (any query shape other than a point lookup breaks). So: the
table keeps its ordinary whole-table-or-live resolution for every query shape
except the PK-bounded one, which this mechanism intercepts and answers instead.
This requires `row_materialize` to NOT suppress whatever `is_stale`/
`materialize_pending` gate the table would otherwise have — a row-materialized
table still has an (optionally very long, or TTL-gated the same way) whole-
table staleness clock for the fallback path; setting `row_materialize=True`
does not have to (and by default should not) also set `prefer_materialized`.

### 3b. Where the PK value(s) are extracted

Today `_Plan` (`provisa/pgwire/_pipeline.py:61-123`) carries `sql`,
`physical_sql`, `exec_params`, and `sources: frozenset[str]` (source **ids**
only — no table identity, no predicate shape). `ensure_resident(state,
source_ids)` (`provisa/federation/query_residency.py:116`) only ever receives
that `frozenset[str]`, so today's residency-prep call sites (pgwire/server.py:690,
flight/server.py:798/940/1037, airport/query.py:93/171, grpc/server.py:460,
endpoint_executors.py:138/240/407, copy_handler.py:387, `_pipeline.py:1059`)
have no access to which rows a query needs — only which sources it touches.

Extraction point: the compiler already parses to a `sqlglot` AST and walks
`exp.Where` nodes for other purposes (`provisa/compiler/nf_extractor.py:84`,
`:283`; `provisa/compiler/rls.py:225`). Add a new pass,
`provisa/compiler/pk_bounds.py` (new file), run at the same pipeline stage
`nf_extractor` runs at (before/alongside `extract_nf_args` in `_pipeline.py`,
~line 719):

```python
@dataclass(frozen=True)
class PkBound:
    """A row-materialized table this statement can serve from the row cache, and the concrete PK
    value(s) its predicate resolves to."""
    source_id: str
    schema_name: str
    table_name: str
    pk_columns: tuple[str, ...]
    # one tuple of literal values per matched row, in pk_columns order — [] means the predicate
    # did not resolve to a bounded equality set for this table (caller falls back to whole-table).
    values: tuple[tuple[Any, ...], ...]

def extract_pk_bounds(
    ast: exp.Expression, row_materialized_tables: dict[str, Table]
) -> list[PkBound]:
    """Walk WHERE/JOIN-ON equality and IN predicates; for every row-materialized table referenced,
    resolve its PK columns' literal values. A table with no resolvable bound is simply absent from
    the result — never an error (most statements don't touch a row-materialized table at all)."""
```

`_Plan` gains one new field:

```python
pk_bounds: tuple[PkBound, ...] = field(default_factory=tuple)  # REQ-TBD: row-materialize predicate resolution
```

populated at the same point `sources` itself is populated (wherever `_Plan(...,
sources=...)` is currently constructed in `_pipeline.py`'s `_govern_and_route`/
`_govern_and_route_compiled`, e.g. the constructor calls around lines 832, 921,
965) via `extract_pk_bounds(ast, row_materialized_tables_by_name)`.

### 3c. New sibling to `ensure_resident`

`ensure_resident` itself stays as-is (whole-table residency, REQ-1661,
unchanged signature) — a new sibling function, same module
(`provisa/federation/query_residency.py`):

```python
async def ensure_rows_resident(
    state: Any, pk_bounds: Iterable[PkBound], *, force: bool = False
) -> list[tuple[str, str, int]]:
    """Serve exactly the rows pk_bounds names from the row cache, fetching from source only the
    missing/stale ones (REQ-TBD). Returns (source_id, table_name, n_rows_fetched) per bound touched.
    A no-op for a bound with no values (extract_pk_bounds already filters those out) or when the
    named table is not row_materialize (defensive: a stale/mismatched bound is just skipped, since
    the compiler is the sole authority on which tables qualify). ``force=True`` (used only by the
    CDC background-refresh caller, section 5) treats every ALREADY-CACHED key in the bound as
    stale regardless of its `_row_expires_at`, without ever adding a key that isn't already
    cached — the one behavioral difference between a query-driven call and a CDC-driven one."""
```

Called at exactly the call sites `ensure_resident(state, governed.sources)`
is called today, immediately alongside it (not instead of it — a statement can
touch both a row-materialized table via PK lookup AND a plain MATERIALIZED
source in the same join, e.g. the `bench_order_node` ⋈ `bench_customer_node`
case): `await ensure_rows_resident(state, governed.pk_bounds)`.

### 3d. Fetch step

For each `PkBound` with `values`, `ensure_rows_resident`:

1. Reads the row cache table's current `(_row_expires_at, pk...)` for exactly
   those PK values (`SELECT pk_cols..., _row_expires_at FROM <landing_target> WHERE (pk1,pk2) IN ((...))`
   — one bounded query, not a scan).
2. Splits `values` into `fresh` (row present, `_row_expires_at > now`) and
   `missing_or_stale` (absent, or present but expired).
3. For `missing_or_stale`, fetches from the real source **only those keys** —
   this requires a keyed-fetch capability from the loader, which does not
   exist today (`SourceRowLoader.load(source, table)` in
   `provisa/events/source_loader.py:118` always returns the whole table). New
   method:

```python
async def load_keys(
    self, source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
) -> list[dict]:
    """Fetch exactly the rows whose pk_columns match one of keys, full row, from the live
    source — never a scan. Requires the source type to expose a keyed-fetch translation; a type
    without one is a registration-time error for row_materialize=True, not a runtime fallback to
    a full load()."""
```

   For an engine-scannable relational/warehouse source this is
   `SELECT <all data_cols> FROM <physical> WHERE (pk1,...) IN ((...))` run
   through the engine terminal (trivial — `resolve_landing_args`'s already-
   resolved `columns`/`pk_columns` give the column list and key shape). For a
   query-API source whose table is defined by a `query_template`
   (Neo4j/GraphQL, e.g. `bench_order_node`'s Cypher `MATCH (o:Order) RETURN
   ...`), the *table registration itself* must supply a keyed variant of the
   template — this is a real gap this design surfaces rather than papers over:
   **`row_materialize=True` on a `query_template` table requires the template
   author to write it in a form the loader can bound** (e.g. Cypher: insert
   `WHERE o.<pk_property> IN $keys` before `RETURN`). Registration validation
   rejects `row_materialize=True` on a query-API table whose `query_template`
   has no recognizable key-bound insertion point, rather than silently falling
   back to a full fetch per lookup (which would defeat the entire feature).
4. Upserts the fetched rows via `land_rows` (section 2a), stamping
   `_row_cached_at = now`, `_row_expires_at = now + resolved_cache_ttl` (the
   same `table.cache_ttl or source.cache_ttl` resolution `resolve_landing_args`
   already performs for the whole-table path — reused, not reinvented, for
   this per-row clock).
5. Assembly: the caller's physical query is answered by reading the row cache
   table directly (it is a normal landed table at `landing_target`'s address —
   the engine's existing attach/`_expose_landed` view sees it exactly like a
   whole-table MATERIALIZED replica). No separate "assemble fresh + cached"
   merge step is needed in the read path itself: by the time `ensure_rows_
   resident` returns, every key the query names is present and fresh in the
   landed table, and the already-compiled `physical_sql` (which addresses that
   table) reads it normally.

## 4. Concurrency/consistency

`land_lock` (`provisa/events/land_lock.py`) is keyed per physical node
(`schema.table`, one `asyncio.Lock` for the whole table) — correct for a
whole-table REPLACE (two interleaved REPLACEs on one table corrupt each other,
confirmed live per that file's own docstring) but wrong-grained here: locking
the whole row-cache table for every single-row fetch would serialize unrelated
point lookups against millions of independent rows, destroying exactly the
concurrency a row-level cache exists to provide.

Design call: **per-key locks, held only around the fetch-and-upsert of the
SPECIFIC missing/stale keys a call is resolving, keyed on `(node, pk_tuple)`**,
not per-table:

```python
def row_lock(node: str, pk_values: tuple[Any, ...]) -> asyncio.Lock:
    return _row_locks.setdefault((node, pk_values), asyncio.Lock())
```

Two concurrent queries needing the *same* missing key both call
`row_lock(node, key)`; the second waits, then (this is the point) re-checks
`_row_expires_at` after acquiring — sees the first query's fresh write and
skips its own fetch, rather than assuming staleness and refetching. Two
concurrent queries needing *disjoint* keys never contend. This is a plain
`dict`-of-locks the same shape as `land_lock`'s own `_locks: dict[str,
asyncio.Lock]` — same pattern, finer key. Unlike `land_lock`'s per-node locks
(bounded, one per registered table), a per-key lock dict is unbounded by
construction (up to one entry per row ever requested); it needs a bound this
design flags explicitly rather than solving silently: evict a key's lock
entry once its waiters have drained and no task holds it (a simple "delete
after release when the lock has no waiters" — `asyncio.Lock` doesn't expose
waiter count directly, so this needs either a small wrapper counting holders/
waiters, or a periodic sweep of the dict dropping unheld locks). This bound
is a genuine implementation task, not a detail to defer silently — call it out
to the implementer as the one piece of this design with no fully-specified
answer here. It also needs to be the SAME lock a CDC-triggered background
refresh acquires (section 5) — a query-driven fetch and a CDC-driven refresh
of the same key must never interleave either.

## 5. Relationship to `apply_cdc_events` and `materialize_pending`

A **third mode that composes with `apply_cdc_events` rather than excluding
it**, and stays parallel to (never subsumes) `materialize_pending`:

- `materialize_pending` (pull, whole-table, staleness-triggered) — unchanged,
  still what a row-materialized table falls back to for a non-PK-bounded query
  (section 3a). Still what every non-opted-in MATERIALIZED table uses exactly
  as today.
- Row-level query-driven materialize (this document) — pull, bounded by PK,
  triggered by a query's own predicate. This is the mechanism section 3
  describes: `ensure_rows_resident` treats an incoming request for a set of
  keys as "make exactly these rows fresh, fetching from source only what's
  missing or expired."
- `apply_cdc_events` (push, row-level, webhook-driven) becomes, for a
  `row_materialize` table with a push `change_signal`, the **background
  counterpart of that same request shape** — a CDC batch arrives already
  keyed by PK (`events` carry `.row`/PK, `materialize_exec.py:224`'s
  `apply_cdc`), which is exactly a set of keys with fresh data in hand,
  the same shape `ensure_rows_resident` resolves for a read. Concretely:
  `EngineBackend.apply_cdc_events` (`backend.py:410`), when the target table
  is `row_materialize`, does not call the ordinary `apply_cdc` (which upserts
  every event unconditionally — that would be the eager/background-prefetch
  behavior constraint 2 forbids, since a CDC stream may well carry rows never
  queried). Instead it does the SIMPLEST thing that reuses existing machinery
  rather than a bespoke apply-in-place path: it filters `events` down to
  those whose PK is **already present** in the row cache (`SELECT pk FROM
  <landing_target> WHERE (pk...) IN (...)`), then synthesizes a `PkBound`
  (section 3b) out of exactly those already-cached keys. It does NOT call
  `ensure_rows_resident(..., force=True)` inline from the CDC handler —
  section 6 below moves that call out-of-band, off the CDC-ingestion path,
  through the same event-queue substrate (`provisa/events/queue.py`) the
  rest of the event loop already uses. `ensure_rows_resident(state, [bound],
  force=True)` (section 3c) is still the exact call that eventually runs —
  just from a background processor, not synchronously inside
  `apply_cdc_events` — not a separate CDC-specific upsert routine. Two
  deliberate consequences of routing it through the read path instead of
  applying the event's own pushed row in place:
  - The event's payload itself is discarded; `ensure_rows_resident` re-fetches
    the current row from the live source via `load_keys` (section 3d), the
    same as it would for a query-driven miss. This costs one extra round trip
    to the source per touched key compared to trusting the push directly, but
    means there is exactly ONE code path that ever writes a row into the
    cache (fetch-by-key → `land_rows`), not two (a query-driven one and a
    CDC-apply one) that would have to be kept consistent with each other.
  - A key that CDC says changed but the source no longer has (a delete, or a
    row that vanished between the event and the re-fetch) needs no separate
    "delete event" handling: `load_keys` simply returns no row for that key,
    and the shared fetch step (section 3d) tombstones any requested key the
    source doesn't return — one rule ("a key that comes back empty is
    removed from the cache") that serves both a genuine CDC delete and an
    ordinary read racing a source-side deletion.
  An event for a key NOT already cached is dropped before the `PkBound` is
  even built (never triggers a fetch, never inserted) — a row still enters
  the cache only the first time a query names it; after that, CDC keeps it
  warm in the background instead of waiting for `cache_ttl` to lapse and the
  next query to pay a synchronous re-fetch. The per-key `row_lock` (section 4)
  is exactly the same lock a query-driven fetch for that key would take —
  `ensure_rows_resident` doesn't know or care whether its caller is a read
  path or the CDC path, so the two can never interleave on one key.
- Net effect for a table with both flags set: the first read of a PK pays the
  lazy on-demand fetch (section 3); every subsequent source-side change to
  that same row arrives as a background `ensure_rows_resident(..., force=True)`
  call via CDC, so a repeat read usually finds the row already fresh and its
  own `ensure_rows_resident` call does no fetching at all (every key is
  already `fresh`). A table with `row_materialize` and a pull `change_signal`
  (ttl/probe) has no such background refresh and relies purely on `cache_ttl`
  expiry + the next query's lazy re-fetch, exactly as section 3 describes on
  its own.

## 6. Storage lifecycle: tombstone in-path, everything else out-of-band

Two distinct concerns get two distinct treatments — conflating them would put
source-round-trip latency on paths that don't need it.

### 6a. Tombstone — stays synchronous, in-path

A requested key the source no longer has (section 3d's "empty fetch") is
deleted from the row cache **immediately**, inline in whichever call
discovered it (a query's `ensure_rows_resident`, or the CDC-triggered one).
This has to stay synchronous: it's the only thing standing between "the
source deleted this row" and "a query still gets served the stale cached
copy." Nothing here is deferred to a background process.

### 6b. CDC-triggered refresh — moved out-of-band

Per the correction above: `apply_cdc_events` filtering events to already-
cached keys and building a `PkBound` (section 5) stays inline — it's a cheap,
purely local existence check. But actually FETCHING those keys from the
source (`ensure_rows_resident(..., force=True)`, which is a real network
round trip per touched key) does not run inline inside the CDC handler.
Instead, `apply_cdc_events` posts one work item per row-materialized table
touched — reusing the existing event-substrate outbox
(`provisa/events/queue.py`'s `post_event(conn, source_table=..., event_type=
"row_refresh", payload={"keys": [...]})`, in the SAME transaction as
whatever else the CDC apply does, so the post is atomic with the rest of the
CDC batch's effects) instead of calling `ensure_rows_resident` directly. A
dedicated background processor — same claim/heartbeat/complete shape
`queue.py`'s own docstring describes for a "TABLE PROCESSOR" (`claim` a
target table's pending events, drain them in id order, `complete`) — drains
`row_refresh` events for that table and is the one that actually calls
`ensure_rows_resident(state, [bound], force=True)` against the live source.
This keeps the CDC ingestion path (whatever posts these events — Debezium/
Kafka/native webhook consumer) fast and decoupled from source-fetch latency,
at the cost of the refresh landing slightly later than "the instant the CDC
event arrived" — acceptable, since constraint 4's per-row TTL already means a
query never trusts a cached row past its own `_row_expires_at` regardless of
whether a background refresh got to it first.

### 6c. Cold-row reaping — out-of-band, storage hygiene only

Nothing in the read or CDC path ever needs to delete a merely-**expired**
(not source-deleted) row to stay correct — `_row_expires_at` already gates
whether a cached row can be trusted, and an expired-but-still-present row is
simply refetched on its next touch (sections 3d/5). So a row that nobody asks
about again just sits there, accumulating storage. Reaping it is purely a
storage-bound concern, and — same principle as 6b — does not belong in a
request's or a CDC event's own critical path. A periodic out-of-band sweep
(a new scheduled task alongside the event loop's other periodic work, not a
per-request or per-event action) issues, in batches:

```sql
DELETE FROM <landing_target> WHERE _row_expires_at < now() - reap_grace_period
```

`reap_grace_period` is intentionally longer than the table's `cache_ttl` —
this sweep is bounding disk, not enforcing freshness (freshness is already
enforced per-row on every touch). The sweep takes each candidate row's
`row_lock` (section 4) before deleting it, so it can never race a fetch that
just started refreshing that same key — same discipline as any other writer
of a row-cache row, not a special case.

Provisa's obligation here stops at exposing `reap_grace_period` (and the
sweep's own batch size/cadence) as ordinary operator-set config, the same
tier as `cache_ttl` itself — not at picking a default that's "right" for a
given deployment. How large a table's real key space is, how fast it churns
at the source, and how much storage headroom the operator has are all
properties of THAT deployment's upstream source and load, not something this
mechanism can infer or auto-tune for. This is the same posture as constraint
6 (a declared PK is trusted, not verified): Provisa provides the knob and the
correct mechanism behind it; sizing that knob against a specific source's
physical characteristics and load volume is the operator's problem, not a gap
in this design.

## 7. Open questions / trade-offs flagged for the implementer

1. **Per-key lock dict growth bound** (section 4) — no fully-specified
   eviction policy given here; needs one before this ships for a
   high-cardinality table.
2. **Query-API keyed-fetch authoring burden** (section 3d) — a
   `query_template` table opting into `row_materialize` must be rewritten by
   its author to accept a key predicate; this is a registration-time
   requirement, not something the mechanism can infer from a plain scan
   template. Flagging this because it directly affects whether
   `bench_order_node` itself (the motivating example, whose current
   `query_template` has no WHERE) could adopt this feature without a config
   edit.
3. **One `cache_ttl` now drives two different clocks on the same table** — the
   whole-table fallback path's own staleness clock (`is_stale_of`,
   `residency.py`) and each row's independent `_row_expires_at` (section 2a)
   both derive from the same resolved `table.cache_ttl or source.cache_ttl`.
   That's the explicit design call here (constraint 4 is about per-row
   tracking, not a per-row duration setting), but it means the operator has
   no way to give the row cache a shorter/longer horizon than the whole-table
   fallback's reload cadence without changing both at once. If that turns out
   to matter in practice, reintroducing a distinct `row_cache_ttl` is a small,
   backward-compatible addition — flagged here as a deliberately deferred
   knob, not an oversight.
4. **Whether a row-materialized table needs its own separate whole-table
   staleness policy at all**, or whether the fallback path (section 3a) should
   instead always be "no cache, go DIRECT/live" for a table that opted into
   row-level caching specifically because whole-table reload is prohibitively
   expensive (the `bench_order_node` case) — this document chose "keep the
   existing whole-table fallback machinery" for consistency and to avoid a
   second routing special-case, but for a 2M-row Neo4j table a full-scan query
   is exactly as expensive after this feature ships as before it; that may not
   be the intended trade-off.
5. **`reap_grace_period` and sweep cadence are operator-set config, not
   design decisions this document defaults** (section 6c) — the mechanism is
   fully specified (out-of-band batched DELETE, gated by each row's
   `row_lock`); the VALUES depend on a given deployment's real key-space size,
   source churn rate, and storage headroom, none of which this design can
   know in advance. Not a gap — deliberately left as a tunable, the same way
   `cache_ttl` itself already is.
6. **Existence-check cost, `row_refresh` backlog depth, and lock-dict size
   under load are all functions of the upstream source's own physical
   characteristics and the operator's chosen throughput for the background
   processor** (sections 4-6b) — how much CDC churn, how large a key space,
   and how tight a freshness SLA a given row-materialized table can sustain
   is bounded by real properties of that table's actual source and by
   however much processing capacity the operator provisions for the
   background refresh/reap workers, not by anything this mechanism can
   auto-tune. Provisa's obligation is to expose these as measurable,
   configurable (queue depth, batch size, worker concurrency) rather than
   hidden — sizing them for a specific deployment's load volume and source
   constraints is the operator's problem, not a defect in this design.
