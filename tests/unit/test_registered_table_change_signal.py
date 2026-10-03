# Copyright (c) 2026 Kenneth Stott
# Canary: 8478e862-1527-4892-9c68-efc157016821
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table registered through the admin carries its own change signal into the registry view
(REQ-929, REQ-1674).

The registry view (``registry_view.registered_tables``) is what residency, the replica refresh
clock and Hot promotion read a table's settings from. A table registered at runtime has no entry
in the config file, so its change signal lives only on its control-plane row — the view must
read it there, or the table is judged by its source's signal instead of its own."""

# Requirements: REQ-929, REQ-1674, REQ-1907, REQ-826

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.federation.registry_view import registered_tables

pytestmark = pytest.mark.unit


@pytest.fixture
def tenant_db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org")
    asyncio.run(init_schema(db, "", org_id="default"))
    yield db
    engine.dispose()


def _table(name: str, **settings) -> Table:
    return Table(
        source_id="ui_pg",
        domain_id="d",
        schema="public",
        table=name,
        columns=[Column(name="id", data_type="integer", visible_to=["*"], is_primary_key=True)],
        **settings,
    )


async def _registered(db: Database, config_tables: list[Table]) -> dict[str, object]:
    state = SimpleNamespace(model_db=db, tenant_db=db, config=SimpleNamespace(tables=config_tables))
    async with db.acquire() as conn:
        rows = await registered_tables(state, conn)
    return {t.table_name: t for t in rows}


def test_a_table_registered_through_the_admin_carries_its_own_change_signal(tenant_db):
    async def _go() -> dict[str, object]:
        async with tenant_db.acquire() as conn:
            # what the admin's registerTable / updateTable saves: no config file entry
            await table_repo.upsert(conn, _table("events", change_signal="probe"), origin="admin")
            await table_repo.upsert(conn, _table("inherits"), origin="admin")
        return await _registered(tenant_db, config_tables=[])

    by_name = asyncio.run(_go())
    assert by_name["events"].change_signal == "probe"
    # a table that sets none still inherits its source's: no value of its own
    assert by_name["inherits"].change_signal is None


def test_a_config_declared_tables_change_signal_is_the_one_its_row_holds(tenant_db):
    """The config loader saves the declared value on the row too; the row is what is read."""
    declared = _table("orders", change_signal="ttl_probe", cache_ttl=30)

    async def _go() -> dict[str, object]:
        async with tenant_db.acquire() as conn:
            await table_repo.upsert(conn, declared, origin="config")
        return await _registered(tenant_db, config_tables=[declared])

    assert asyncio.run(_go())["orders"].change_signal == "ttl_probe"
