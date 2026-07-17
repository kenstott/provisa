# Copyright (c) 2026 Kenneth Stott
# Canary: df19d325-26bd-420e-b2a7-a52723dd4402
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Engine-agnostic governed read for the airport Flight service (REQ-1098).

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
    # has no SET LOCAL channel (REQ-1098).
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
        return rows_to_arrow_table(result.rows, columns)
    raise ValueError(f"Route {plan.route!r} is not supported for the airport service")
