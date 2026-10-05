# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e1a95-6b2d-4f80-a4e9-2d8b5f1c7e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher read (HTTP and Bolt) runs on its own source's connection, or not at all.

The Cypher direct dispatcher is the terminal Cypher over HTTP and Bolt hand a non-ENGINE plan to.
A plan for a source this node holds no connection for must never run on the org's control-plane
store; the role reading it has no right to that store."""

# Requirements: REQ-825, REQ-1919, REQ-031

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from provisa.api.errors import ApiError

from tests.unit.test_governed_sql_engine_internals import _state as gov_state

pytestmark = pytest.mark.asyncio


class _ControlPlane:
    """The org's control-plane store: records every statement it is asked to run."""

    def __init__(self) -> None:
        self.ran: list[str] = []

    @asynccontextmanager
    async def acquire(self):
        store = self

        class _Conn:
            async def fetch_with_columns(self, sql):
                store.ran.append(sql)
                return ["id"], [(1,)]

        yield _Conn()


class _NoPools:
    """Connections this node holds: none."""

    source_ids: tuple[str, ...] = ()

    def has(self, _source_id: str) -> bool:
        return False


async def test_a_role_without_meta_reading_a_source_with_no_connection_never_reaches_the_control_plane(
    monkeypatch,
):
    import provisa.api.app as app_mod
    from provisa.api.rest.cypher_router import _dispatch_execution_direct
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _govern_and_route_compiled
    from provisa.transpiler.router import Route

    store = _ControlPlane()
    state = gov_state(["sales"])  # analyst: the sales domain only, no meta
    state.source_pools = _NoPools()
    state.tenant_db = store
    state.source_dialects = {"pg": "postgres"}
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))

    # The statement Cypher's translator produces for MATCH (o:Orders) RETURN o.id, governed as
    # the Cypher router governs it.
    plan = await _govern_and_route_compiled(
        'SELECT "o"."id" FROM "sales"."orders" AS "o"',
        "analyst",
        state=state,
        cache_hint=NO_CACHE_HINT,
    )
    assert plan.route != Route.ENGINE and plan.source_id == "pg", plan

    # The Cypher router and the Bolt session hand exactly this to the direct dispatcher.
    with pytest.raises(ApiError) as refused:
        await _dispatch_execution_direct(plan.exec_sql or "", plan.source_id, [], state)
    assert (refused.value.status_code, refused.value.code) == (500, "data.no_direct_route")
    assert store.ran == [], f"the read ran on the control-plane store: {store.ran}"


async def test_the_admin_source_is_served_by_the_model_store_not_the_state_store():
    from provisa.api.rest.cypher_router import _dispatch_execution_direct

    model, tenant = _ControlPlane(), _ControlPlane()
    state = type(
        "_State", (), {"model_db": model, "tenant_db": tenant, "source_pools": _NoPools()}
    )()
    rows = await _dispatch_execution_direct("SELECT 1 AS id", "provisa-admin", [], state)
    assert rows == [{"id": 1}]
    assert (model.ran, tenant.ran) == (["SELECT 1 AS id"], [])
