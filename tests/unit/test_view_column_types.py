# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Regression: a view must never be persisted with a null-typed column.

introspect_tables requires every SQL-catalog column to carry a data_type; a view
registered with a null type permanently bricks backend startup. The admin UI
snapshots view columns by running the SQL, and a column whose type can't be traced
(e.g. it references a source not yet introspected) arrives with data_type=None.
_ensure_view_column_types resolves those — from referenced tables, else varchar.
"""

from unittest.mock import AsyncMock

import pytest

from provisa.api.admin.schema import _ensure_view_column_types
from provisa.core.models import Column


@pytest.mark.asyncio
async def test_fills_null_from_referenced_table():
    conn = AsyncMock()
    conn.fetch.return_value = [
        {"column_name": "careLevel", "data_type": "varchar"},
        {"column_name": "avgLifespanYears", "data_type": "integer"},
    ]
    cols = [
        Column(name="careLevel", data_type=None, visible_to=[]),
        Column(name="avgLifespanYears", data_type=None, visible_to=[]),
    ]
    out = await _ensure_view_column_types(
        conn, "SELECT careLevel, avgLifespanYears FROM shelter__animalBreeds", cols
    )
    assert out[0].data_type == "varchar"
    assert out[1].data_type == "integer"


@pytest.mark.asyncio
async def test_untraceable_column_defaults_to_varchar():
    conn = AsyncMock()
    conn.fetch.return_value = []  # no referenced-table match (e.g. aliased projection)
    cols = [Column(name="user_name", data_type=None, visible_to=[])]
    out = await _ensure_view_column_types(
        conn, "SELECT users.name AS user_name FROM users", cols
    )
    assert out[0].data_type == "varchar"


@pytest.mark.asyncio
async def test_no_nulls_short_circuits_without_db():
    conn = AsyncMock()
    cols = [Column(name="id", data_type="bigint", visible_to=[])]
    out = await _ensure_view_column_types(conn, "SELECT id FROM t", cols)
    assert out[0].data_type == "bigint"
    conn.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_never_returns_null_type():
    conn = AsyncMock()
    conn.fetch.return_value = []
    cols = [Column(name=f"c{i}", data_type=None, visible_to=[]) for i in range(5)]
    out = await _ensure_view_column_types(conn, "SELECT c0,c1,c2,c3,c4 FROM t", cols)
    assert all(c.data_type for c in out)


@pytest.mark.asyncio
async def test_unparseable_sql_still_fills_varchar():
    conn = AsyncMock()
    cols = [Column(name="x", data_type=None, visible_to=[])]
    out = await _ensure_view_column_types(conn, "NOT VALID SQL ((", cols)
    assert out[0].data_type == "varchar"
