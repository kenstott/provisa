# Copyright (c) 2026 Kenneth Stott
# Canary: dcaabc41-56d9-403b-a44d-30b2454d9f01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""table_columns.domain_id mirrors the owning table's registered_tables.domain_id.

The column is populated at the one write path that inserts table_columns rows
(``table_repo.upsert``, REQ-1387) so every column of a registered table carries its
domain without a separate lookup at read time.
"""

from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.sql.dml import Insert

from provisa.core.database import Connection
from provisa.core.models import Column, Table
from provisa.core.schema_org import table_columns


class _FakeConn:
    """Enough of Connection for table_repo.upsert to run, capturing every table_columns insert."""

    def __init__(self):
        self.upsert = AsyncMock()
        self.upsert_returning = AsyncMock(return_value=7)
        self.inserted_columns: list[dict] = []

    async def execute_core(self, stmt, *_a, **_k):
        if isinstance(stmt, Insert) and stmt.table is table_columns:
            self.inserted_columns.append(dict(stmt.compile().params))
        result = MagicMock()
        result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result


@pytest.mark.asyncio
async def test_upsert_sets_table_columns_domain_id_from_the_table():
    from provisa.core.repositories import table as table_repo

    conn = _FakeConn()
    tbl = Table(
        source_id="s",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        columns=[
            Column(name="id", data_type="integer", visible_to=[]),
            Column(name="total", data_type="numeric", visible_to=[]),
        ],
    )

    await table_repo.upsert(cast(Connection, conn), tbl)

    assert len(conn.inserted_columns) == 2
    assert {c["domain_id"] for c in conn.inserted_columns} == {"sales"}


@pytest.mark.asyncio
async def test_upsert_re_registration_moves_columns_to_the_new_domain():
    """A table's domain reassignment (REQ-367/REQ-432) must carry through to its columns —
    otherwise a column stays stamped with its table's stale domain after a move."""
    from provisa.core.repositories import table as table_repo

    conn = _FakeConn()
    tbl = Table(
        source_id="s",
        domain_id="finance",
        schema_name="public",
        table_name="orders",
        columns=[Column(name="id", data_type="integer", visible_to=[])],
    )

    await table_repo.upsert(cast(Connection, conn), tbl)

    assert conn.inserted_columns[-1]["domain_id"] == "finance"
