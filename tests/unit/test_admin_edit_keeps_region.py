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
from unittest.mock import MagicMock, patch

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


async def _in_region_eu(db) -> None:
    """The org selects region eu; the source and its table name it (as the config sets them)."""
    from provisa.core.repositories import region as region_repo

    async with db.acquire() as conn:
        for store in (
            StoreConfig(id="eu-pg", url="postgresql://eu/db"),
            StoreConfig(id="eu-trino", url="trino://eu:8080", kind="trino-byo"),
        ):
            await region_repo.upsert_store(conn, store, origin="config")
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
            origin="config",
        )
        await conn.execute_core(update(sources).values(region="eu"))
        await conn.execute_core(update(registered_tables).values(region="eu"))


async def _regions(db) -> tuple[str | None, str | None]:
    async with db.acquire() as conn:
        source = (await conn.execute_core(select(sources.c.region))).scalar_one()
        table = (await conn.execute_core(select(registered_tables.c.region))).scalar_one()
    return source, table


@pytest.mark.asyncio
async def test_an_admin_edit_of_a_table_keeps_its_region(tmp_path):
    from provisa.api.admin.schema_mutation import Mutation

    async with _db(tmp_path, source_signal="ttl", table_ttl=60) as db:
        await _in_region_eu(db)
        with ExitStack() as stack:
            for p in _table_patches(db):
                stack.enter_context(p)
            result = await Mutation().update_table(MagicMock(), _table_input(description="edited"))
        assert result.success is True, result.message
        assert (await _regions(db))[1] == "eu"


@pytest.mark.asyncio
async def test_an_admin_edit_of_a_source_keeps_its_region(tmp_path):
    async with _db(tmp_path, source_signal="ttl", source_ttl=60) as db:
        await _in_region_eu(db)
        m, p = _mutation(db)
        with (
            p,
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
        ):
            result = await m.update_source(
                MagicMock(), _source_input(change_signal="ttl", cache_ttl=60)
            )
        assert result.success is True, result.message
        assert (await _regions(db))[0] == "eu"
