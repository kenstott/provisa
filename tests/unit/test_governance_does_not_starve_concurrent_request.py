# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2b8a4d-1c9e-4b7a-8d3f-5e2a9c6b1f4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882: governance's CPU-bound work (`apply_governance`, dispatched via `_off_loop`) must
not block the shared event loop, or a slow governed statement starves every other concurrently-
scheduled coroutine on the same loop (live-verified original symptom: a full-table GROUP BY took
a concurrent single-row PK lookup from ~120ms to ~12.1s). Same technique as
`tests/unit/test_api_source_caller.py::TestPaginate::test_json_decode_does_not_block_concurrent_task`:
run the real governance entrypoint with a deliberately slow `apply_governance` concurrently with a
cheap ticker coroutine on the SAME loop, and assert the ticker isn't starved.

This FAILS if `apply_governance`'s `_off_loop` dispatch (provisa/pgwire/_pipeline.py) is ever
reverted to an in-line call — the ticker would then be blocked for the whole synchronous sleep and
accumulate far fewer ticks than expected.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.federation.replica_address import ReplicaRoutes
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta

pytestmark = pytest.mark.asyncio

TABLE_ID = 1
SOURCE_ID = "pg-src"
DOMAIN_ID = "petstore"

_COLUMNS = [{"column_name": "id", "data_type": "integer", "visible_to": ["analyst"]}]

_TABLE_DICT = {
    "id": TABLE_ID,
    "source_id": SOURCE_ID,
    "schema_name": "public",
    "table_name": "pets",
    "domain_id": DOMAIN_ID,
    "columns": _COLUMNS,
}


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "pets": TableMeta(
            table_id=TABLE_ID,
            field_name="pets",
            type_name="Pets",
            source_id=SOURCE_ID,
            catalog_name=SOURCE_ID,
            schema_name="public",
            table_name="pets",
            domain_id=DOMAIN_ID,
        )
    }
    return ctx


def _fake_state():
    return SimpleNamespace(
        model_stamp=1,  # REQ-1914: the stamp the model was built at; audit rows record it
        admin_db=None,  # as AppState without a control plane: no debug-trace settings
        contexts={"analyst": _ctx()},
        rls_contexts={},
        roles={"analyst": {"capabilities": [], "domain_access": ["*"]}},
        masking_rules={},
        tables=[_TABLE_DICT],
        relationships=[],
        source_types={SOURCE_ID: "postgresql"},
        source_catalogs={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        metrics={},
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        # as the schema build publishes it: no table is served from a replica (REQ-826)
        replica_routes=ReplicaRoutes(),
    )


async def test_governance_does_not_starve_concurrent_request(monkeypatch):
    """A slow `apply_governance` call must not block a concurrently-scheduled coroutine on the
    same loop — proof that governance's CPU-bound work stays off the loop (REQ-1882)."""
    import provisa.api.app as app_mod
    from provisa.compiler import stage2 as stage2_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _fake_state(), raising=False)

    real_apply_governance = stage2_mod.apply_governance

    def _slow_apply_governance(sql, gov_ctx, *request):
        time.sleep(0.2)  # simulates a slow governance pass — must run off-loop
        return real_apply_governance(sql, gov_ctx, *request)

    monkeypatch.setattr(stage2_mod, "apply_governance", _slow_apply_governance)

    async def _fake_optimize_and_route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        from provisa.transpiler.router import Route, RouteDecision

        return (
            exec_sql,
            RouteDecision(route=Route.DIRECT, source_id=SOURCE_ID, dialect=None, reason="test"),
            SOURCE_ID,
            False,
            {SOURCE_ID},
            (),
        )

    ticks = 0

    async def ticker():
        nonlocal ticks
        for _ in range(20):
            await asyncio.sleep(0.01)
            ticks += 1

    with patch.object(
        _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_fake_optimize_and_route)
    ):
        ticker_task = asyncio.ensure_future(ticker())
        plan = await _pipeline._govern_and_route("SELECT * FROM pets", "analyst")
        await ticker_task

    assert plan is not None
    # If apply_governance ran in-line on the loop, ticker would have been starved for the whole
    # 0.2s sleep and accumulated far fewer than 20 ticks by the time governance returned.
    assert ticks >= 15
