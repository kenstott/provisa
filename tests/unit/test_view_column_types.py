# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7ce5d5-8a25-4a53-ae3e-48f2224fa003
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1426: a view is registered fully typed or not at all.

introspect_tables requires every SQL-catalog column to carry a data_type; a view registered with
a null type permanently bricks backend startup. The admin UI snapshots view columns by running the
SQL, and a column whose type can't be traced arrives with data_type=None. _ensure_view_column_types
resolves those by SQLGlot annotation over the referenced tables' stored types — including projected
expressions, which name-matching could never type — and refuses the registration when a column
still cannot be typed. It never stamps a default.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.api.admin.schema import _ensure_view_column_types
from provisa.core.models import Column


def _conn_with_rows(rows):
    """rows: (table_name, column_name, data_type)."""
    conn = AsyncMock()
    result = MagicMock()
    result.fetchall.return_value = [
        MagicMock(table_name=tn, alias=None, column_name=cn, data_type=dt) for tn, cn, dt in rows
    ]
    conn.execute_core = AsyncMock(return_value=result)
    return conn


@pytest.mark.asyncio
async def test_fills_null_from_referenced_table():
    conn = _conn_with_rows(
        [
            ("shelter__animalbreeds", "carelevel", "varchar"),
            ("shelter__animalbreeds", "avglifespanyears", "integer"),
        ]
    )
    cols = [
        Column(name="carelevel", data_type=None, visible_to=[]),
        Column(name="avglifespanyears", data_type=None, visible_to=[]),
    ]
    out, err = await _ensure_view_column_types(
        conn, "SELECT carelevel, avglifespanyears FROM shelter__animalbreeds", cols
    )
    assert err is None
    assert out[0].data_type == "varchar"
    assert out[1].data_type == "integer"  # stored spelling round-trips verbatim


@pytest.mark.asyncio
async def test_aliased_projection_is_typed_from_the_underlying_column():
    conn = _conn_with_rows([("users", "name", "varchar")])
    cols = [Column(name="user_name", data_type=None, visible_to=[])]
    out, err = await _ensure_view_column_types(
        conn, "SELECT users.name AS user_name FROM users", cols
    )
    assert err is None
    assert out[0].data_type == "varchar"


@pytest.mark.asyncio
async def test_expression_columns_are_typed_not_defaulted():
    """count(*)/sum() have no matching source column; annotation still types them."""
    conn = _conn_with_rows([("orders", "amount", "double"), ("orders", "customer_id", "int")])
    cols = [
        Column(name="n", data_type=None, visible_to=[]),
        Column(name="total", data_type=None, visible_to=[]),
    ]
    out, err = await _ensure_view_column_types(
        conn,
        "SELECT count(*) AS n, sum(orders.amount) AS total FROM orders GROUP BY customer_id",
        cols,
    )
    assert err is None
    assert out[0].data_type == "bigint"
    assert out[1].data_type == "double"


@pytest.mark.asyncio
async def test_untypeable_column_is_refused_never_defaulted():
    conn = _conn_with_rows([])  # referenced table has no stored types
    cols = [Column(name="user_name", data_type=None, visible_to=[])]
    out, err = await _ensure_view_column_types(
        conn, "SELECT users.name AS user_name FROM users", cols
    )
    assert out == []
    assert err is not None and err.success is False
    assert err.code == "schema.column_types_unresolved"
    assert "user_name" in err.message


@pytest.mark.asyncio
async def test_no_nulls_short_circuits_without_db():
    conn = AsyncMock()
    conn.execute_core = AsyncMock()
    cols = [Column(name="id", data_type="bigint", visible_to=[])]
    out, err = await _ensure_view_column_types(conn, "SELECT id FROM t", cols)
    assert err is None
    assert out[0].data_type == "bigint"
    conn.execute_core.assert_not_called()


@pytest.mark.asyncio
async def test_never_returns_null_type():
    conn = _conn_with_rows([])
    cols = [Column(name=f"c{i}", data_type=None, visible_to=[]) for i in range(5)]
    out, err = await _ensure_view_column_types(conn, "SELECT c0,c1,c2,c3,c4 FROM t", cols)
    assert err is not None  # nothing typeable → refused
    assert all(c.data_type for c in out)  # and nothing untyped escapes


@pytest.mark.asyncio
async def test_unparseable_sql_is_refused():
    conn = _conn_with_rows([])
    cols = [Column(name="x", data_type=None, visible_to=[])]
    out, err = await _ensure_view_column_types(conn, "NOT VALID SQL ((", cols)
    assert out == []
    assert err is not None and err.code == "schema.column_types_unresolved"
