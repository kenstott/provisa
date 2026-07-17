# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regression tests for admin GraphQL schema wiring (no stack required).

Guards two shipped defects:
  1. refresh_source_statistics (REQ-276) was missing @strawberry.mutation, so it
     never appeared in the schema and was unreachable via GraphQL.
  2. available_schemas called state.federation_engine.reachable(...), but
     federation_engine is an EngineRuntime whose reachable() lives on the wrapped
     FederationEngine at .engine — the call raised AttributeError.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import strawberry
from strawberry.printer import print_schema

from provisa.api.admin.schema_mutation import Mutation
from provisa.api.admin.schema_query import Query


def test_refresh_source_statistics_exposed_in_schema():
    sdl = print_schema(strawberry.Schema(query=Query, mutation=Mutation))
    assert "refreshSourceStatistics" in sdl


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_available_schemas_uses_engine_reachable_on_wrapped_engine():
    # native_schemas returns None -> forces the engine-reachability branch (line 324).
    reachable = SimpleNamespace(reachable=lambda source_type: False)
    fake_state = SimpleNamespace(
        source_types={"warehouse": "snowflake"},
        source_pools=object(),
        federation_engine=SimpleNamespace(engine=reachable),
    )
    fake_pool = SimpleNamespace(acquire=lambda: _FakeAcquire(object()))

    with (
        patch("provisa.api.app.state", fake_state),
        patch("provisa.api.admin.schema_query._get_pool", new=AsyncMock(return_value=fake_pool)),
        patch("provisa.api.admin.introspect.native_schemas", new=AsyncMock(return_value=None)),
    ):
        result = await Query().available_schemas(source_id="warehouse")

    assert result == []  # unreachable -> no engine schemas, and no AttributeError


@pytest.mark.asyncio
async def test_available_schemas_engine_runtime_has_no_direct_reachable():
    # Documents the fix: EngineRuntime does not expose reachable() itself; it is a
    # method of the wrapped FederationEngine reached via .engine.
    from provisa.federation.runtime import EngineRuntime
    from provisa.federation.engine import FederationEngine

    assert not hasattr(EngineRuntime, "reachable")
    assert hasattr(FederationEngine, "reachable")
