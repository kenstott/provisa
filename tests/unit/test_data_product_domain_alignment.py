# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2b6f4a-9c1d-4b8e-a3f0-5d6c8e1a9b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1634: a table's product_id must reference a DataProduct in the table's own domain.

Enforced at the last write gate (table_repo.upsert) so every caller — config load, admin
GraphQL, introspection — is covered, not only the picker UI (provisa/core/repositories/table.py).
"""

from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.sql import Select
from sqlalchemy.sql.dml import Insert

from provisa.core.database import Connection
from provisa.core.models import Table
from provisa.core.schema_org import data_products, table_columns


class _FakeConn:
    """Enough of Connection for table_repo.upsert to run against a canned data_products row."""

    def __init__(self, product_row: dict | None):
        self.product_row = product_row
        self.upsert = AsyncMock()
        self.upsert_returning = AsyncMock(return_value=7)
        self.inserted_columns: list[dict] = []

    async def execute_core(self, stmt, *_a, **_k):
        if isinstance(stmt, Select) and stmt.get_final_froms()[0] is data_products:
            result = MagicMock()
            row = MagicMock()
            row._mapping = self.product_row
            result.fetchone.return_value = row if self.product_row is not None else None
            return result
        if isinstance(stmt, Insert) and stmt.table is table_columns:
            self.inserted_columns.append(dict(stmt.compile().params))
        result = MagicMock()
        result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result


def _table(*, domain_id: str, product_id: str | None) -> Table:
    return Table(
        source_id="s",
        domain_id=domain_id,
        schema_name="public",
        table_name="orders",
        product_id=product_id,
        columns=[],
    )


@pytest.mark.asyncio
async def test_upsert_rejects_a_product_in_a_different_domain():
    from provisa.core.repositories import table as table_repo

    conn = _FakeConn(product_row={"id": "checkout", "domain_id": "finance"})
    tbl = _table(domain_id="sales", product_id="checkout")

    with pytest.raises(ValueError, match="sales.*finance|finance.*sales"):
        await table_repo.upsert(cast(Connection, conn), tbl)


@pytest.mark.asyncio
async def test_upsert_rejects_a_nonexistent_product_id():
    from provisa.core.repositories import table as table_repo

    conn = _FakeConn(product_row=None)
    tbl = _table(domain_id="sales", product_id="missing")

    with pytest.raises(ValueError, match="missing"):
        await table_repo.upsert(cast(Connection, conn), tbl)


@pytest.mark.asyncio
async def test_upsert_accepts_a_product_in_the_same_domain():
    from provisa.core.repositories import table as table_repo

    conn = _FakeConn(product_row={"id": "checkout", "domain_id": "sales"})
    tbl = _table(domain_id="sales", product_id="checkout")

    await table_repo.upsert(cast(Connection, conn), tbl)

    conn.upsert_returning.assert_awaited_once()
    values = conn.upsert_returning.await_args.args[1]
    assert values["product_id"] == "checkout"


@pytest.mark.asyncio
async def test_upsert_allows_no_product_id():
    from provisa.core.repositories import table as table_repo

    conn = _FakeConn(product_row=None)
    tbl = _table(domain_id="sales", product_id=None)

    await table_repo.upsert(cast(Connection, conn), tbl)

    conn.upsert_returning.assert_awaited_once()
    values = conn.upsert_returning.await_args.args[1]
    assert values["product_id"] is None
