# Streaming Uniformity — Conformance Gap & Requirement Amendment

**Status:** proposed · **Supersedes premise of:** REQ-1190, REQ-1218 · **Narrows:** REQ-1214 · **Extends:** REQ-1203/1204

## Objective (invariant)

> 1. There is exactly **ONE path to serve a result.** One terminal, for every query, every route
>    (ENGINE and DIRECT), every engine, emits a lazy `(schema, batch_gen)`. Nothing about the
>    **protocol** or the **route** changes whether a result can OOM the process.
> 2. That path **always streams**, with exactly one exception: the wire-**buffered** transports
>    — **JSON:API, GraphQL, Bolt** — cannot stream incrementally, so they serve via **CTAS**
>    (engine-native materialization off Provisa's heap, threshold-gated: inline below N rows, a
>    CTAS handle above), **never** an in-Provisa `fetchall`.

"Buffered" therefore means *buffered on the engine via CTAS*, never *buffered in Provisa RAM*. Every
other transport (pgwire, Flight SQL, native gRPC, **airport**) drains the one stream to the wire.

Partly stated already: REQ-1185 ("every result-bearing surface must stream and never materialize
unbounded user data"), REQ-1214 ("one governed pipeline"). The defects below are where the
*implementation* and three *requirement premises* (REQ-1190, REQ-1214's DIRECT carve-out, REQ-1218)
fall short of it.

## Transport classification under the objective

- **Streaming (drain the one terminal to the wire):** pgwire, Flight SQL, native gRPC, **airport**.
  Airport is NOT exempt — it must stream (see Defect 5). The **native** gRPC query RPC already
  streams (REQ-1215); the GraphQL-over-gRPC proxy is a GraphQL transport (buffered, below).
- **Buffered → CTAS (never Provisa-RAM):** JSON:API, GraphQL, Bolt. These land a CTAS handle above
  the threshold and inline the body below it.
- **Genuinely bounded (small by construction, materialize freely):** metadata / admin /
  registered-function output.

## Defect 1 — DIRECT single-source passthrough materializes on a false premise

REQ-1190/1214 classify the DIRECT route as "genuinely-bounded (metadata/admin)." That conflates
two different things: *single reachable source* (the router's DIRECT trigger, REQ-826) and
*bounded metadata*. `SELECT * FROM one_big_postgres` is a single reachable source → DIRECT →
`execute_native` → full `.rows` → `rows_to_arrow_table`. A billion-row passthrough to ONE source
materializes — the passthrough that should stream *hardest* streams *worst*.

- `provisa/federation/runtime.py:153` `execute_native` returns a materialized `QueryResult`.
- Consumed materialized at `provisa/api/flight/server.py:697`, `provisa/api/airport/query.py:66`,
  `provisa/pgwire/server.py` DIRECT branch.

**This is a requirement bug, not just a code bug.** REQ-1190's premise ("only ENGINE queries need
streaming") is wrong. Amendment: DIRECT is bounded *only* when it is metadata/admin/registered-fn;
a DIRECT **user data scan** must stream via the source's own server-side cursor / Arrow reader,
identically to ENGINE.

## Defect 2 — Warehouse engines declare ARROW_STREAM but fake laziness

REQ-1216 asserts "All Flight-serving engines (DuckDB, Snowflake, Databricks, BigQuery, ClickHouse,
Fabric/Synapse, Trino) declare `EngineCapability.ARROW_STREAM`," and REQ-1217 requires the terminal
be "genuinely lazy." Three of them materialize the whole table then re-emit its batches:

- `provisa/federation/databricks_runtime.py:257` — `run_arrow` then `.to_batches()`
- `provisa/federation/bigquery_runtime.py:218` — same
- `provisa/federation/mssql_warehouse_runtime.py:271` — same

**Who declares it:** not the runtime — the per-engine **builder** in `provisa/federation/engine.py`
hardcodes `ARROW_STREAM` into the capability `frozenset`, each with a comment naming a lazy driver
primitive the runtime never calls:

- `build_databricks_engine` engine.py:756 — "lazy record-batch streaming via Flight (REQ-987)"
- `build_bigquery_engine` engine.py:786 — "to_arrow_iterable — lazy record batches" (runtime calls
  full `to_arrow`, not `to_arrow_iterable`)
- mssql builder engine.py:818 — "built from the ODBC cursor"

The builder advertises the capability; the runtime fakes it. Declaring a capability the terminal
does not honor is exactly the silent-materialization this project forbids.

**The fix must make the terminal genuinely lazy** — use the server-side chunk API the builder
already names (bigquery `to_arrow_iterable`, databricks Cloud Fetch chunk iteration, mssql cursor
`fetchmany`→Arrow), as Snowflake `fetch_arrow_batches` and DuckDB `fetch_record_batch` do (REQ-1217).
This fixes Defect 3 in the same stroke — a lazy `run_sync` falls out of the same primitive.

**Dropping `ARROW_STREAM` does NOT conform.** `require(ARROW_STREAM)` failing routes to the adapter
path `arrow_batches_from_rows(rt.run_sync(...))` (native_backend.py:298-304), but the adapter is
bounded ONLY when `run_sync` hands it a genuinely streaming `ResultStream`. Warehouse `run_sync`
returns a fully-materialized `QueryResult` (Defect 3), so the adapter would re-batch an in-memory
list whose materialization already OOM'd one call earlier. Dropping the capability relocates the
buffer from `run_arrow_stream` to `run_sync`; it does not remove it. The adapter conforms only for
engines whose `run_sync` already streams (pg / duckdb / sqla) — i.e. not the ones at fault. The only
non-lazy-terminal alternative that conforms is CTAS-land (engine-native materialization off Provisa's
heap, REQ-1194/1195), never the row→Arrow adapter over a materializing `run_sync`.

## Defect 3 — Warehouse `run_sync` materializes on the pgwire ENGINE route

REQ-1186 makes the pgwire ENGINE route stream. But the warehouse runtimes' `run_sync` returns a
fully-materialized `QueryResult`, not a `StreamingQueryResult`:

- snowflake `:117`, databricks `:233`, bigquery `:199`, clickhouse `:370`, mssql `:245`.

So pgwire against a warehouse engine OOMs on a large result despite REQ-1186. (pg / duckdb / sqla
`run_sync` are genuinely streaming — REQ-1222/1223 and the DuckDB private-cursor terminal.)
REQ-1223's use case even *claims* server-side-cursor streaming "on federation engines (Trino,
Snowflake, Databricks)" — but those engines don't use the sqla runtime; their own `run_sync`
materializes. The claim is unbacked for them.

## Defect 4 — No automatic stream↔materialize threshold

REQ-1203/1204 give a redirect/CTAS sink, but it fires only when a caller passes `deliver=` via a
side-channel. The end goal's "*optionally* generate CTAS based on a row-count threshold"
— an automatic policy at the single terminal — does not exist yet. Today materialization is either
caller-requested (redirect) or an accident of route/engine (Defects 1–3).

## Defect 5 — Airport materializes a full scan in Provisa RAM

REQ-1218 declares airport a materializing catalog transport by design: it caches the full governed
scan per (role, schema, table) for byte-stable schema advertisement and derives an `is_rowid`
pseudo-column over the whole table. Under the objective airport is a **streaming** transport, so a
full-scan buffer in Provisa memory violates it. `provisa/api/airport/query.py:60` already calls the
materialized arrow terminal (`execute_engine_arrow`) on the ENGINE route and `.rows` on DIRECT.

Airport must drain the one streaming terminal like Flight SQL. Its two real needs are separable
catalog-identity concerns, not result-path buffering:

- **byte-stable schema** — advertise from the plan's typed output columns (already known
  pre-execution), not by scanning rows. Empty-result retyping (`_retype_null_columns`,
  query.py) already reads column types independent of row data.
- **`is_rowid` for UPDATE/DELETE echo** — derive from the source's key metadata / a streamed rowid
  column, not by holding the whole table. If a genuinely unbounded rowid mapping is needed, land it
  as a CTAS side-table (off-heap), not an in-RAM cache.

REQ-1218 is superseded: airport streams; schema/rowid caching moves off the result path.

## Target design

One terminal, `_execute_plan`, is the single decision point:

1. Govern → obtain the engine's **streaming** `(schema, batch_gen)` — for ENGINE *and* DIRECT
   (DIRECT drives the single source's server-side cursor / Arrow reader). There is no non-streaming
   producer path.
2. The terminal decides delivery by **transport class**, not by route or engine:
   - **streaming transport** (pgwire, Flight SQL, native gRPC, airport) → hand the generator through,
     unbounded, never buffered in Provisa.
   - **buffered transport** (JSON:API, GraphQL, Bolt) → land a CTAS via `run_materialize`
     (REQ-1194/1195) above the row-count/byte threshold and return a handle; inline the body below it.
     Never a Provisa-RAM `fetchall`.
   - **bounded** (metadata/admin/registered-fn) → collect directly; small by construction.
3. Engine capability is honest: no `ARROW_STREAM`/streaming `run_sync` unless the terminal is
   genuinely lazy. A non-streaming engine either gets a genuinely-lazy terminal or serves buffered
   transports straight to CTAS-land — never a streaming-shaped method that secretly materializes,
   and never the row→Arrow adapter over a materializing `run_sync` (that just moves the buffer).

## Remediation sequence (highest leverage first)

1. **DIRECT streaming** (Defect 1) — `execute_native` returns a lazy batch generator; DIRECT user
   scans stream. Amend REQ-1190; the biggest single OOM hole.
2. **Warehouse honesty** (Defects 2–3) — make `run_arrow_stream`/`run_sync` genuinely lazy via the
   driver's server-side chunk API. Delete the three fake `run_arrow_stream`s. No capability-dropping
   fallback (it doesn't conform — Defect 2).
3. **Airport streams** (Defect 5) — route airport through the streaming terminal; move
   schema/`is_rowid` off the result path. Supersede REQ-1218.
4. **Buffered = CTAS** (Defect 4) — JSON:API / GraphQL / Bolt land a threshold-gated CTAS at the one
   terminal instead of an in-Provisa buffer; wire the automatic threshold into `_execute_plan`.

Each step is verifiable by extending `tests/integration/test_streaming_memory_bounded_e2e.py`
(RLIMIT_AS cap) to the new surface/engine: the streaming variant drains under the cap; the buffered
transport lands a CTAS handle rather than busting it; only the genuinely-bounded control materializes.
