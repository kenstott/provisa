# Copyright (c) 2026 Kenneth Stott
# Canary: 7cf99366-dfe6-47b3-8941-6078fb60b0e0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An admin edit of a table or a source keeps the region it has (REQ-1921). The forms carry no
region; an edit saved the object as the form rebuilt it and wrote NULL over the stored region —
for a table, moving where its data lives (built in every region again)."""

# Requirements: REQ-1921

from __future__ import annotations

from contextlib import ExitStack
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select, update

from provisa.core.regions import OrgRegion, StoreConfig
from provisa.core.schema_org import registered_tables, sources
from tests.unit.test_landing_ttl_admin import (
    _db,
    _mutation,
    _source_input,
    _table_input,
    _table_patches,
)
from tests.unit.gate_identity import serve_every_column


async def _in_region_eu(db) -> None:
    """The org selects region eu; the source and its table name it (as the config sets them)."""
    from provisa.core.repositories import region as region_repo

    async with db.acquire() as conn:
        for store in (
            StoreConfig(id="eu-pg", url="postgresql://eu/db"),
            StoreConfig(id="eu-trino", url="trino://eu:8080", kind="trino-byo"),
        ):
            await region_repo.upsert_store(conn, store)
        await region_repo.upsert_region(
            conn,
            OrgRegion(
                id="eu",
                engine="eu-trino",
                replicas="eu-pg",
                views="eu-pg",
                cache="eu-pg",
                state="eu-pg",
                record="eu-pg",
            ),
        )
        await conn.execute_core(update(sources).values(region="eu"))
        await conn.execute_core(update(registered_tables).values(region="eu"))


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which ones a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


async def _regions(db) -> tuple[str | None, str | None]:
    async with db.acquire() as conn:
        source = (await conn.execute_core(select(sources.c.region))).scalar_one()
        table = (await conn.execute_core(select(registered_tables.c.region))).scalar_one()
    return source, table


@pytest.mark.asyncio
async def test_an_admin_edit_of_a_table_keeps_its_region(control_plane):
    from provisa.api.admin.schema_mutation import Mutation

    async with _db(control_plane, source_signal="ttl", table_ttl=60) as db:
        await _in_region_eu(db)
        with ExitStack() as stack:
            for p in _table_patches(db):
                stack.enter_context(p)
            result = await Mutation().update_table(MagicMock(), _table_input(description="edited"))
        assert result.success is True, result.message
        assert (await _regions(db))[1] == "eu"


@pytest.mark.asyncio
async def test_an_admin_edit_of_a_source_keeps_its_region(control_plane):
    async with _db(control_plane, source_signal="ttl", source_ttl=60) as db:
        await _in_region_eu(db)
        m, p = _mutation(db)

        @contextlib.asynccontextmanager
        async def _no_vault():
            yield

        with (
            p,
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
            # The save provisions the source on the engine and rebuilds the schemas: this
            # stand-in has neither engine nor vault, and what is checked is the stored region.
            patch("provisa.core.secrets_store.bound_to_request_org", _no_vault),
            patch("provisa.api.admin.schema_mutation._register_source_on_engine"),
            patch("provisa.api.admin.schema_mutation._rebuild_schemas", new=AsyncMock()),
        ):
            result = await m.update_source(
                MagicMock(), _source_input(change_signal="ttl", cache_ttl=60)
            )
        assert result.success is True, result.message
        assert (await _regions(db))[0] == "eu"


@pytest.mark.asyncio
async def test_an_edit_through_update_table_keeps_the_store_only_settings(control_plane):
    """REQ-1919: every setting a configuration can give a table is held by the store, and the
    admin's table form — and the MCP model tools (set_column_fake, set_table_profiler), which save
    through the same updateTable — carry none of the store-only ones. A save keeps each as stored.
    REQ-2000: a column's embedding and its model are the form's (a vector column names its model
    there), so a save stores them as the form gives them: here, none."""
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.core.schema_org import table_columns

    async with _db(control_plane, source_signal="ttl", table_ttl=60) as db:
        promotions = [{"source_path": "a.b", "target_column": "b", "data_type": "text"}]
        async with db.acquire() as conn:
            await conn.execute_core(
                update(registered_tables).values(
                    hot=True, approval_hook=True, relay_pagination=True, promotions=promotions
                )
            )
            table_id = (await conn.execute_core(select(registered_tables.c.id))).scalar_one()
            await conn.execute_core(
                table_columns.insert().values(
                    table_id=table_id,
                    domain_id="",
                    column_name="id",
                    data_type="integer",
                    visible_to=["analyst"],
                    encrypted=True,
                    embedding=True,
                )
            )
        with ExitStack() as stack:
            for p in _table_patches(db):
                stack.enter_context(p)
            result = await Mutation().update_table(MagicMock(), _table_input(description="edited"))
        assert result.success is True, result.message
        async with db.acquire() as conn:
            table = (
                await conn.execute_core(
                    select(
                        registered_tables.c.hot,
                        registered_tables.c.approval_hook,
                        registered_tables.c.relay_pagination,
                        registered_tables.c.promotions,
                    )
                )
            ).one()
            columns = (
                await conn.execute_core(
                    select(
                        table_columns.c.encrypted,
                        table_columns.c.embedding,
                        table_columns.c.embedding_model,
                    )
                )
            ).fetchall()
        assert (table.hot, table.approval_hook, table.relay_pagination) == (True, True, True)
        assert table.promotions == promotions
        assert columns and all(c.encrypted for c in columns)
        assert [(c.embedding, c.embedding_model) for c in columns] == [(False, None)]


def test_the_forms_column_carries_its_embedding_and_model():
    """REQ-2000: a column's embedding and its model are the form's to give."""
    from provisa.api.admin._table_ops import _build_column_models
    from provisa.api.admin.types import ColumnInput

    (column,) = _build_column_models(
        [
            ColumnInput(
                name="body_vec",
                data_type="json",
                visible_to=[],
                embedding=True,
                embedding_model="stub",
            )
        ]
    )
    assert (column.embedding, column.embedding_model) == (True, "stub")
