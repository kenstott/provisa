# Copyright (c) 2026 Kenneth Stott
# Canary: 6a164ccd-c020-48ce-817f-4ad8ccd30aa9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A files-glob table's declaration round-trips through the control plane (REQ-788).

file_glob and source_file_column are what the read paths (native DuckDB/ClickHouse, the
replicator for Trino/pg) key on, so a table registered or loaded must carry them on its
control-plane row and the registry view must read them back."""

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
        source_id="files",
        domain_id="d",
        schema="main",
        table=name,
        columns=[Column(name="id", data_type="integer", visible_to=["*"], is_primary_key=True)],
        **settings,
    )


async def _registered(db: Database) -> dict[str, object]:
    state = SimpleNamespace(model_db=db, tenant_db=db, config=SimpleNamespace(tables=[]))
    async with db.acquire() as conn:
        rows = await registered_tables(state, conn)
    return {t.table_name: t for t in rows}


def test_file_glob_and_source_file_column_round_trip(tenant_db):
    async def _go() -> dict[str, object]:
        async with tenant_db.acquire() as conn:
            await table_repo.upsert(
                conn,
                _table("orders", file_glob="orders/*.csv", source_file_column="_source_file"),
            )
            await table_repo.upsert(conn, _table("single"))
        return await _registered(tenant_db)

    by_name = asyncio.run(_go())
    assert by_name["orders"].file_glob == "orders/*.csv"
    assert by_name["orders"].source_file_column == "_source_file"
    # a plain single-file table carries neither
    assert by_name["single"].file_glob is None
    assert by_name["single"].source_file_column is None
