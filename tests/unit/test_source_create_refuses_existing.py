# Copyright (c) 2026 Kenneth Stott
# Canary: 2a8d5f13-7c46-4e90-b1e7-5d3c9a0f6e28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""createSource makes a new source: a valid definition whose id exists is refused by name before
anything is staged, connected or written (REQ-012); a refused definition is refused for what it is
(test_landing_ttl_admin). It used to overwrite the source and report it created.
Changing a source is updateSource, which also provisions it again on the engine bound at the call
-- the one it was saved under, or one swapped to since -- then rebuilds the schemas and lands
its replicated tables there (REQ-1730)."""

# Requirements: REQ-012, REQ-1730

from __future__ import annotations

import contextlib
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from provisa.core.schema_org import sources
from tests.unit.test_landing_ttl_admin import _db, _mutation, _source_input

pytestmark = pytest.mark.asyncio


async def _stored(db) -> list[tuple]:
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(sources.c.id, sources.c.host, sources.c.database, sources.c.cache_ttl)
            )
        ).fetchall()
    return [tuple(r) for r in rows]


async def test_a_create_on_an_existing_id_is_refused_and_writes_nothing(control_plane):
    async with _db(control_plane, source_signal="ttl", source_ttl=60) as db:
        before = await _stored(db)
        m, p = _mutation(db)
        m = cast(Any, m)
        add_pool = AsyncMock()
        with (
            p,
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
            patch("provisa.api.admin.schema_mutation._add_source_pool", add_pool),
            patch(
                "provisa.api.admin.schema_mutation._refuse_over_source_limit",
                new=AsyncMock(return_value=None),
            ),
        ):
            source = _source_input(change_signal="ttl", cache_ttl=60)
            source.host = "elsewhere"
            result = await m.create_source(MagicMock(), source)
        assert (result.success, result.code, result.params) == (
            False,
            "schema.source_id_taken",
            {"source": "s"},
        )
        assert await _stored(db) == before
        add_pool.assert_not_awaited()  # not connected to either


async def test_an_update_provisions_the_source_on_the_bound_engine_again(
    control_plane, monkeypatch
):
    from provisa.api.app import state

    async with _db(control_plane, source_signal="ttl", source_ttl=60) as db:
        m, p = _mutation(db)
        m = cast(Any, m)

        @contextlib.asynccontextmanager
        async def _no_vault():
            yield

        engine: Any = MagicMock()
        engine.reconcile_landed_tables = AsyncMock()
        engine.engine.unreachable_reason.return_value = None  # the engine reads the source
        monkeypatch.setattr(state, "federation_engine", engine, raising=False)
        register, rebuild = MagicMock(), AsyncMock()
        with (
            p,
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
            patch("provisa.core.secrets_store.bound_to_request_org", _no_vault),
            patch("provisa.api.admin.schema_mutation._register_source_on_engine", register),
            patch("provisa.api.admin.schema_mutation._rebuild_schemas", rebuild),
        ):
            result = await m.update_source(
                MagicMock(), _source_input(change_signal="ttl", cache_ttl=60)
            )
        assert result.success is True, result.message
        assert register.call_args.args[1].id == "s"
        rebuild.assert_awaited()
        engine.reconcile_landed_tables.assert_awaited_once()
