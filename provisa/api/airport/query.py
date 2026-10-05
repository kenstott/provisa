# Copyright (c) 2026 Kenneth Stott
# Canary: df19d325-26bd-420e-b2a7-a52723dd4402
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Engine-agnostic governed read for the airport Flight service (REQ-1120).

A do_get for an airport table runs the SAME governed pipeline the existing
ProvisaFlightServer SQL path uses (``_govern_and_route`` → EngineRuntime), so
governance (RLS, masking, column visibility, row cap) is applied and the query
routes through whatever engine is bound (duckdb default, trino, pg, ...). No
Trino-specific path.
"""

from __future__ import annotations

from decimal import Decimal
from itertools import islice
from typing import TYPE_CHECKING, Any, Iterator, NamedTuple

import pyarrow as pa

from provisa.core.connection_loop import run_on_connection_loop

if TYPE_CHECKING:
    from provisa.executor.result import ResultStream

    from provisa.api.app import AppState


def _plan_for_scan(
    state: AppState,
    sql: str,
    role_id: str,
):
    """Govern + route a scan SQL and return the stamped final-statement plan.

    session_vars={} makes RLS current_setting() predicates deny-by-default (NULL) rather than
    reaching an engine that lacks the function — the airport transport has no SET LOCAL channel
    (REQ-1120). govern_batch_final_plan runs any leading statements of a multi-statement batch
    (governed) and returns the final statement's plan. Runs on the airport RPC's handler thread, on
    its own connection loop (REQ-1882, provisa.core.rpc_loop).
    """
    from provisa.audit.context import with_audit_identity
    from provisa.pgwire._pipeline import govern_batch_final_plan, require_governed_plan

    # REQ-074/REQ-1386: the airport transport authenticates with a role token and carries no
    # separate principal — the role IS the acting identity here, bound inside the coroutine.
    plan = run_on_connection_loop(
        with_audit_identity(
            role_id,
            "airport",
            govern_batch_final_plan(sql, role_id, state, session_vars={}, deliver=None),
        )
    )
    require_governed_plan(plan)  # REQ-1176: this Arrow terminal must verify the stamp too
    return plan


class ScanStream(NamedTuple):
    """A governed scan: its schema, its lazily pulled batches, and what the statement's answer
    says about itself (REQ-1350), which the transport sends ahead of the rows."""

    schema: pa.Schema
    batches: Iterator[pa.RecordBatch]
    warnings: list


def governed_table_scan_stream(
    state: AppState,
    sql: str,
    role_id: str,
) -> ScanStream:
    """Stream ``sql`` for ``role_id`` through the ONE governed pipeline (:class:`ScanStream`).

    The airport transport drains the streaming terminal like Flight SQL's _do_get_sql_governed
    (streaming-uniformity Defect 5, superseding REQ-1218) — a large governed scan is never fully
    materialized in Provisa RAM. ENGINE route → the engine's lazy record-batch terminal; DIRECT
    route → the source's server-side cursor (or, for a source with no cursor, the native driver's
    result) adapted to a byte-stable typed Arrow schema and lazily batched.

    The schema is derived from the query's TYPED output columns (DuckDB's result schema on ENGINE;
    the native driver's result-column types on DIRECT), so it is identical whether the scan returns
    0 or N rows — the airport advertises and streams one schema regardless of row content.
    """
    from provisa.transpiler.router import Route

    from provisa.federation.live_concurrency import acquire_plan_permits

    plan = _plan_for_scan(state, sql, role_id)
    permits = None
    try:
        if plan.route == Route.ENGINE:
            assert plan.physical_sql is not None
            # REQ-1661/REQ-1865: this streaming terminal never reaches _execute_plan (see module
            # docstring and _finalize_scan_audit's own comment), so its own residency calls are the
            # ONLY place a MATERIALIZED / row_materialize source this plan reads gets landed before
            # the engine executes — mirrors the identical fixes in provisa/pgwire/server.py,
            # provisa/api/flight/server.py, provisa/api/rest/cypher_router.py, provisa/grpc/server.py.
            from provisa.federation.query_residency import (
                ensure_resident,
                ensure_rows_resident,
                pushdown_row_materialize,
            )

            async def _prep_residency() -> set[str]:
                await ensure_rows_resident(state, plan.pk_bounds, reader_role=plan.role_id)
                return await pushdown_row_materialize(
                    state,
                    plan.physical_sql,
                    state.federation_engine.dialect,
                    plan.exec_params,
                    reader_role=plan.role_id,
                )

            run_on_connection_loop(_prep_residency())
            plan.replicas_read = run_on_connection_loop(
                ensure_resident(
                    state, plan.sources, reader_role=plan.role_id, table_ids=plan.table_ids
                )
            ).replicas_read
            # REQ-1909: the permits ride the batch generator until the scan is fully pulled.
            permits = acquire_plan_permits(state, plan)
            schema, batch_gen = state.federation_engine.execute_engine_stream(plan.physical_sql, [])
            _finalize_scan_audit(plan, 200, state)
            return ScanStream(schema, permits.guard(_audited(plan, batch_gen)), plan.warnings)
        if plan.route == Route.DIRECT:
            permits = acquire_plan_permits(state, plan)  # REQ-1909
            if state.source_pools.has(plan.source_id) and state.source_pools.supports_stream(
                plan.source_id
            ):
                # REQ-1190: single-reachable-source scan streams via the source's server-side cursor.
                stream = state.federation_engine.execute_native_stream(
                    state.source_pools,
                    plan.source_id,
                    plan.sql,
                    plan.exec_params or [],
                    run=run_on_connection_loop,
                )
            else:
                # Source has no server-side cursor — the native driver materializes its own result
                # (bounded by the source's capability); the airport still emits typed row-batches.
                stream = run_on_connection_loop(
                    state.federation_engine.execute_native(
                        state.source_pools,
                        plan.source_id,
                        plan.sql,
                        plan.exec_params or [],
                    )
                )
            typed = _direct_typed_schema(stream.column_names, stream.column_types)
            _finalize_scan_audit(plan, 200, state)
            return ScanStream(
                typed,
                permits.guard(_audited(plan, _typed_batches_from_rows(stream, typed))),
                plan.warnings,
            )
    except Exception:
        if permits is not None:
            permits.release()
        _finalize_scan_audit(plan, 500, state)
        raise
    raise ValueError(f"Route {plan.route!r} is not supported for the airport service")


def _finalize_scan_audit(plan, status_code: int, state: AppState) -> None:
    """Write the governed scan's audit row (REQ-074/REQ-1386).

    The airport drains its own terminal on the RPC's thread and never reaches ``_execute_plan``, so
    the row is written here, once the cursor/reader has opened — the point the scan is served.
    ``finalize_audit`` is idempotent, so no path double-writes.
    """
    from provisa.pgwire._pipeline import finalize_audit

    # A served scan is a stream the caller has not drained: the row is written when the drain
    # ends (``_audited``), with the rows delivered. A failure is written at once.
    run_on_connection_loop(
        finalize_audit(plan, status_code, state, defer_to_drain=status_code == 200)  # noqa: PLR2004
    )


def _audited(plan, batches):
    """The scan's record batches, with its audit row written when the drain ends."""
    from provisa.pgwire._pipeline import audit_on_drain

    return audit_on_drain(plan, batches, rows_in=lambda batch: batch.num_rows)


def governed_table_scan_schema(
    state: AppState,
    sql: str,
    role_id: str,
) -> pa.Schema:
    """The byte-stable Arrow schema of a governed scan, WITHOUT opening a row-streaming cursor.

    flight_info / list_schemas advertise from this; do_get streams data with the identical schema
    (schema/rowid caching lives off the result path now — Defect 5). Derives the schema from the
    query's TYPED output columns and never holds — nor eagerly opens — a data cursor: the ENGINE
    route reads DuckDB's result schema off the lazy reader (closing it before any batch is pulled);
    the DIRECT route reads the native driver's result-column types and closes the server-side cursor
    without a drain, so the pooled connection is released (no leak from a schema-only probe).
    """
    from provisa.transpiler.router import Route

    plan = _plan_for_scan(state, sql, role_id)
    if plan.route == Route.ENGINE:
        assert plan.physical_sql is not None
        # REQ-1661/REQ-1865: a schema-only probe still binds the query against the engine (DuckDB's
        # Binder Error fires at bind/compile time, before any row is fetched), so a MATERIALIZED /
        # row_materialize source not yet landed fails here identically to the streaming path above
        # — same fix required.
        from provisa.federation.query_residency import (
            ensure_resident,
            ensure_rows_resident,
            pushdown_row_materialize,
        )

        async def _prep_residency() -> set[str]:
            await ensure_rows_resident(state, plan.pk_bounds, reader_role=plan.role_id)
            return await pushdown_row_materialize(
                state,
                plan.physical_sql,
                state.federation_engine.dialect,
                plan.exec_params,
                reader_role=plan.role_id,
            )

        run_on_connection_loop(_prep_residency())
        plan.replicas_read = run_on_connection_loop(
            ensure_resident(state, plan.sources, reader_role=plan.role_id, table_ids=plan.table_ids)
        ).replicas_read
        from provisa.federation.live_concurrency import acquire_plan_permits

        # REQ-1909: binding the probe reads the live source too; held only while it opens.
        with acquire_plan_permits(state, plan):
            schema, batch_gen = state.federation_engine.execute_engine_stream(plan.physical_sql, [])
            close = getattr(batch_gen, "close", None)
            if close is not None:
                close()
        return schema
    if plan.route == Route.DIRECT:
        from provisa.federation.live_concurrency import acquire_plan_permits

        # REQ-1909: the probe opens a cursor on the live source; held only while it does.
        with acquire_plan_permits(state, plan):
            if state.source_pools.has(plan.source_id) and state.source_pools.supports_stream(
                plan.source_id
            ):
                stream = state.federation_engine.execute_native_stream(
                    state.source_pools,
                    plan.source_id,
                    plan.sql,
                    plan.exec_params or [],
                    run=run_on_connection_loop,
                )
                typed = _direct_typed_schema(stream.column_names, stream.column_types)
                stream.close()  # release the eagerly-opened server-side cursor; no rows fetched
                return typed
            result = run_on_connection_loop(
                state.federation_engine.execute_native(
                    state.source_pools,
                    plan.source_id,
                    plan.sql,
                    plan.exec_params or [],
                )
            )
        return _direct_typed_schema(result.column_names, result.column_types)
    raise ValueError(f"Route {plan.route!r} is not supported for the airport service")


def _direct_typed_schema(column_names: list[str], column_types: list[str] | None) -> pa.Schema:
    """Byte-stable Arrow schema for a DIRECT scan, from the native driver's own result-column types.

    Row-independent (identical for a 0-row and an N-row result), so the airport advertises and
    streams ONE schema regardless of row content — the empty-scan retyping REQ-1218 patched around
    on the result path is unnecessary once the schema comes from the typed columns (Defect 5).
    """
    from provisa.core.ir_types import to_ir

    if column_types is None or len(column_types) != len(column_names):
        raise ValueError(
            f"airport: DIRECT scan reported {len(column_names)} columns but "
            f"{None if column_types is None else len(column_types)} column types — cannot form a "
            "byte-stable schema"
        )
    return pa.schema(
        [
            pa.field(name, _ir_to_arrow(to_ir(ctype, "postgresql")))
            for name, ctype in zip(column_names, column_types)
        ]
    )


def _typed_batches_from_rows(
    stream: ResultStream,
    typed_schema: pa.Schema,
    *,
    batch_rows: int = 65_536,
) -> Iterator[pa.RecordBatch]:
    """Adapt a lazy row ``ResultStream`` into RecordBatches cast to ``typed_schema``.

    Memory-bounded — only ``batch_rows`` rows are held at once — so a large DIRECT scan streams
    instead of materializing. Draining the generator drains (and closes) the underlying source
    cursor via the stream's own ``finally``.
    """
    names = stream.column_names

    def _conv(v: Any) -> Any:
        return float(v) if isinstance(v, Decimal) else v

    def _to_batch(rows: list[tuple]) -> pa.RecordBatch:
        arrays = [
            pa.array([_conv(r[i]) for r in rows], type=typed_schema.field(i).type)
            for i in range(len(names))
        ]
        return pa.RecordBatch.from_arrays(arrays, schema=typed_schema)

    row_iter = stream.iter_rows()
    while True:
        chunk = list(islice(row_iter, batch_rows))
        if not chunk:
            return
        yield _to_batch(chunk)


# Canonical IR type name → Arrow type, for typing an empty governed scan's columns (REQ-1120).
_IR_TO_ARROW: dict[str, pa.DataType] = {
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "numeric": pa.float64(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "time": pa.time64("us"),
    "bytea": pa.large_binary(),
    "uuid": pa.string(),
    "text": pa.string(),
    "json": pa.string(),
}


def _ir_to_arrow(ir: str) -> pa.DataType:
    arrow_t = _IR_TO_ARROW.get(ir)
    if arrow_t is None:
        raise ValueError(f"airport: IR type {ir!r} has no Arrow mapping for an empty-scan schema")
    return arrow_t


def governed_mutation(
    state: AppState,  # noqa: ARG001  # kept for call-site symmetry with the scan seam
    sql: str,
    role_id: str,
) -> int:
    """Submit a mutation (INSERT/UPDATE/DELETE) through the ONE governed pipeline (REQ-1120).

    Routes the semantic mutation SQL through the SAME ``_compile_govern_execute`` the
    ``/data/sql`` endpoint uses, so governance (writable-column ACL, RLS injection on
    UPDATE/DELETE, domain-access) and the write-routing decision (native → sqlalchemy →
    engine, via writable.py) apply — the airport DML path is NOT a parallel writer. Runs
    on the airport RPC's handler thread, on its own connection loop (REQ-1882).

    Returns the count of rows the driver reported back (RETURNING/affected rows); callers
    that build a fixed-size mutation use their own input count for the airport
    ``total_changed`` metadata when the driver reports none.
    """
    from provisa.api.app import state as _app_state
    from provisa.audit.context import with_audit_identity
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    async def _run():
        _plan = await _govern_and_route(sql, role_id)
        return await _execute_plan(_plan, _app_state)

    # REQ-074/REQ-1386: same role-as-principal binding the scan seam uses; _execute_plan writes
    # the audit row itself, so the mutation needs only the identity bound inside the coroutine.
    result = run_on_connection_loop(with_audit_identity(role_id, "airport", _run()))
    return len(result.rows)
