# Copyright (c) 2026 Kenneth Stott
# Canary: df19d325-26bd-420e-b2a7-a52723dd4402
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Engine-agnostic governed read for the airport Flight service (REQ-1106).

A do_get for an airport table runs the SAME governed pipeline the existing
ProvisaFlightServer SQL path uses (``_govern_and_route`` → EngineRuntime), so
governance (RLS, masking, column visibility, row cap) is applied and the query
routes through whatever engine is bound (duckdb default, trino, pg, ...). No
Trino-specific path.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from provisa.api.app import AppState


def governed_table_scan_arrow(
    state: AppState,
    main_loop: asyncio.AbstractEventLoop,
    sql: str,
    role_id: str,
) -> pa.Table:
    """Execute ``sql`` for ``role_id`` through the governed pipeline, return Arrow.

    Runs on an airport server worker thread; asyncpg-bound coroutines are
    dispatched to the main event loop that owns the pools, mirroring
    ProvisaFlightServer._do_get_sql_governed. Engine route → engine Arrow;
    direct route → native driver rows → Arrow.
    """
    from provisa.compiler.sql_gen import ColumnRef
    from provisa.executor.formats.arrow import rows_to_arrow_table
    from provisa.pgwire._pipeline import _govern_and_route
    from provisa.transpiler.router import Route

    # session_vars={} makes RLS current_setting() predicates deny-by-default (NULL)
    # rather than reaching an engine that lacks the function — the airport transport
    # has no SET LOCAL channel (REQ-1106).
    plan = asyncio.run_coroutine_threadsafe(
        _govern_and_route(sql, role_id, session_vars={}),
        main_loop,
    ).result()

    if plan.route == Route.ENGINE:
        assert plan.physical_sql is not None
        return state.federation_engine.execute_engine_arrow(plan.physical_sql, [])
    if plan.route == Route.DIRECT:
        result = asyncio.run_coroutine_threadsafe(
            state.federation_engine.execute_native(
                state.source_pools,
                plan.source_id,
                plan.sql,
                plan.exec_params or [],
            ),
            main_loop,
        ).result()
        columns = [
            ColumnRef(field_name=c, column=c, alias=None, nested_in=None)
            for c in result.column_names
        ]
        tbl = rows_to_arrow_table(result.rows, columns)
        # An EMPTY governed scan (e.g. a freshly created table, or an RLS-emptied one) yields
        # null-typed Arrow columns from row inference — DuckDB then cannot cast a real value into
        # the advertised NULL type on a later INSERT. Re-type those columns from the source's own
        # result-column types (carried by the native driver even for a zero-row result) so the
        # airport catalog advertises the true column types (REQ-1106).
        if tbl.num_rows == 0 and result.column_types:
            tbl = _retype_null_columns(tbl, result.column_types)
        return tbl
    raise ValueError(f"Route {plan.route!r} is not supported for the airport service")


def _retype_null_columns(tbl: pa.Table, column_types: list[str]) -> pa.Table:
    """Replace null-typed columns of an empty Arrow table with the source's real (mapped) types."""
    from provisa.core.ir_types import to_ir

    if len(column_types) != tbl.num_columns:
        return tbl
    fields: list[pa.Field] = []
    arrays: list[pa.Array] = []
    for i, field in enumerate(tbl.schema):
        if pa.types.is_null(field.type):
            arrow_t = _ir_to_arrow(to_ir(column_types[i], "postgresql"))
            fields.append(pa.field(field.name, arrow_t))
            arrays.append(pa.array([], type=arrow_t))
        else:
            fields.append(field)
            arrays.append(tbl.column(i).combine_chunks())
    return pa.table(arrays, schema=pa.schema(fields))


# Canonical IR type name → Arrow type, for typing an empty governed scan's columns (REQ-1106).
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
    main_loop: asyncio.AbstractEventLoop,
    sql: str,
    role_id: str,
) -> int:
    """Submit a mutation (INSERT/UPDATE/DELETE) through the ONE governed pipeline (REQ-1106).

    Routes the semantic mutation SQL through the SAME ``_compile_govern_execute`` the
    ``/data/sql`` endpoint uses, so governance (writable-column ACL, RLS injection on
    UPDATE/DELETE, domain-access) and the write-routing decision (native → sqlalchemy →
    engine, via writable.py) apply — the airport DML path is NOT a parallel writer. Runs
    on an airport worker thread; the coroutine is dispatched to the main event loop that
    owns the pools.

    Returns the count of rows the driver reported back (RETURNING/affected rows); callers
    that build a fixed-size mutation use their own input count for the airport
    ``total_changed`` metadata when the driver reports none.
    """
    from provisa.api.app import state as _app_state
    from provisa.api.data.endpoint_dev import _compile_govern_execute

    result, _sources, _default, _decision, _phys = asyncio.run_coroutine_threadsafe(
        _compile_govern_execute(sql, role_id, _app_state),
        main_loop,
    ).result()
    return len(result.rows)
