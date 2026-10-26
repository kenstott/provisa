# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The source form's save is one createSource / updateSource: the GraphQL naming setting
(gql_naming_convention), which also has a mutation of its own (updateSourceNaming), travels in
SourceInput like the source's other settings, so a save rebuilds the schema once (REQ-2109's rule
for a table, applied to a source). Left out, a create inherits the global setting and an update
keeps the stored one."""

# Requirements: REQ-012, REQ-2109

from __future__ import annotations

import contextlib
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from provisa.core.schema_org import sources
from tests.unit.test_landing_ttl_admin import _db, _mutation, _source_input

pytestmark = pytest.mark.asyncio


async def _naming(db, source_id: str) -> str | None:
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(sources.c.gql_naming_convention).where(sources.c.id == source_id)
            )
        ).fetchone()
    assert row is not None, f"source {source_id!r} is not stored"
    return row[0]


async def _set_naming(db, source_id: str, convention: str | None) -> None:
    from sqlalchemy import update

    async with db.acquire() as conn:
        await conn.execute_core(
            update(sources)
            .where(sources.c.id == source_id)
            .values(gql_naming_convention=convention)
        )


@contextlib.contextmanager
def _update_patches(p, rebuild: AsyncMock, monkeypatch):
    from provisa.api.app import state

    @contextlib.asynccontextmanager
    async def _no_vault():
        yield

    engine: Any = MagicMock()
    engine.reconcile_landed_tables = AsyncMock()
    monkeypatch.setattr(state, "federation_engine", engine, raising=False)
    with (
        p,
        patch("provisa.api.admin.capabilities.require_capability", return_value=None),
        patch("provisa.core.secrets_store.bound_to_request_org", _no_vault),
        patch("provisa.api.admin.schema_mutation._register_source_on_engine", MagicMock()),
        patch("provisa.api.admin.schema_mutation._rebuild_schemas", rebuild),
    ):
        yield


async def test_an_update_carrying_the_naming_convention_stores_it_with_one_rebuild(
    control_plane, monkeypatch
):
    async with _db(control_plane, source_signal="ttl", source_ttl=60) as db:
        m, p = _mutation(db)
        rebuild = AsyncMock()
        source = _source_input(change_signal="ttl", cache_ttl=60)
        source.gql_naming_convention = "hasura-default"
        with _update_patches(p, rebuild, monkeypatch):
            result = await cast(Any, m).update_source(MagicMock(), source)
        assert result.success is True, result.message
        from provisa.compiler.naming import stored_convention

        assert await _naming(db, "s") == stored_convention("hasura-default")
        assert rebuild.await_count == 1


async def test_an_update_leaving_the_naming_convention_out_keeps_the_stored_one(
    control_plane, monkeypatch
):
    from provisa.compiler.naming import stored_convention

    async with _db(control_plane, source_signal="ttl", source_ttl=60) as db:
        await _set_naming(db, "s", stored_convention("hasura-default"))
        m, p = _mutation(db)
        with _update_patches(p, AsyncMock(), monkeypatch):
            result = await cast(Any, m).update_source(
                MagicMock(), _source_input(change_signal="ttl", cache_ttl=60)
            )
        assert result.success is True, result.message
        assert await _naming(db, "s") == stored_convention("hasura-default")


async def test_an_update_carrying_null_inherits_the_global_convention(control_plane, monkeypatch):
    from provisa.compiler.naming import stored_convention

    async with _db(control_plane, source_signal="ttl", source_ttl=60) as db:
        await _set_naming(db, "s", stored_convention("hasura-default"))
        m, p = _mutation(db)
        source = _source_input(change_signal="ttl", cache_ttl=60)
        source.gql_naming_convention = None
        with _update_patches(p, AsyncMock(), monkeypatch):
            result = await cast(Any, m).update_source(MagicMock(), source)
        assert result.success is True, result.message
        assert await _naming(db, "s") is None


async def test_an_unknown_naming_convention_is_refused_and_nothing_is_saved(
    control_plane, monkeypatch
):
    async with _db(control_plane, source_signal="ttl", source_ttl=60) as db:
        m, p = _mutation(db)
        rebuild = AsyncMock()
        source = _source_input(change_signal="ttl", cache_ttl=60)
        source.host = "elsewhere"
        source.gql_naming_convention = "shouting"
        with _update_patches(p, rebuild, monkeypatch):
            result = await cast(Any, m).update_source(MagicMock(), source)
        assert (result.success, result.code) == (False, "schema.naming_convention_invalid")
        async with db.acquire() as conn:
            host = (
                await conn.execute_core(select(sources.c.host).where(sources.c.id == "s"))
            ).scalar_one()
        assert host == "h"
        rebuild.assert_not_awaited()


async def test_a_create_refuses_an_unknown_naming_convention_before_writing(control_plane):
    async with _db(control_plane) as db:
        m, p = _mutation(db)
        source = _source_input(change_signal="ttl", cache_ttl=60)
        source.id = "fresh"
        source.gql_naming_convention = "shouting"
        with (
            p,
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
            patch(
                "provisa.api.admin.schema_mutation._refuse_over_source_limit",
                new=AsyncMock(return_value=None),
            ),
        ):
            result = await cast(Any, m).create_source(MagicMock(), source)
        assert (result.success, result.code) == (False, "schema.naming_convention_invalid")
        async with db.acquire() as conn:
            stored = await conn.execute_core(select(sources.c.id).where(sources.c.id == "fresh"))
            assert stored.fetchone() is None
