# Copyright (c) 2026 Kenneth Stott
"""Composite FK reconstruction (pg_constraint) and pg_index population.

A composite FK is stored as N single-column joins with no shared identifier;
_build_fk_constraint_rows regroups them via the FK→PK invariant. pg_index is
populated from PK/UNIQUE constraints so clients that read indkey resolve key
columns.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import duckdb

from provisa.pgwire.catalog_populate import CatalogIndex
from provisa.pgwire.catalog_constraints import (
    _build_fk_constraint_rows,
    _build_pk_constraint_rows,
    _populate_pg_index,
)
from provisa.pgwire.system_tables import _populate_empty_system_tables


def _tm(table_id: int, name: str):
    return SimpleNamespace(table_id=table_id, type_name=name, field_name=name, display_name="")


class _Idx:
    # order_item oid=101, orders oid=100
    table_id_to_oid = {1: 100, 2: 101}
    toid_to_table = {100: ("provisa", "public", "orders"), 101: ("provisa", "public", "order_item")}
    ns_map = {"public": 2200}
    col_attnum = {
        (100, "order_id"): 1,
        (100, "order_line"): 2,
        (101, "order_id"): 1,
        (101, "order_line"): 2,
        # users (single-col PK) for the bridge case
        (200, "id"): 1,
        (300, "user_a"): 1,
        (300, "user_b"): 2,
    }
    attnum_to_col = {v: k for k, v in {}.items()}


def _idx() -> CatalogIndex:
    return cast(CatalogIndex, _Idx())


def _composite_ctx():
    orders = _tm(1, "Orders")
    order_item = _tm(2, "OrderItem")
    jm1 = SimpleNamespace(
        source_column="order_id",
        target_column="order_id",
        cardinality="many-to-one",
        target=orders,
        source_constant=None,
        source_expr=None,
    )
    jm2 = SimpleNamespace(
        source_column="order_line",
        target_column="order_line",
        cardinality="many-to-one",
        target=orders,
        source_constant=None,
        source_expr=None,
    )
    return SimpleNamespace(
        tables={"OrderItem": order_item, "Orders": orders},
        joins={("OrderItem", "order"): jm1, ("OrderItem", "orderLine"): jm2},
        pk_columns={1: ["order_id", "order_line"]},
    )


def test_composite_fk_collapsed_to_one_row():
    rows, next_oid = _build_fk_constraint_rows(_composite_ctx(), _idx(), 40000)
    assert len(rows) == 1
    row = rows[0]
    assert row[3] == "f"  # contype
    assert row[7] == 101  # conrelid (order_item)
    assert row[11] == 100  # confrelid (orders)
    # ordered by target PK order [order_id, order_line]
    assert row[18] == [1, 2]  # conkey (src attnums)
    assert row[19] == [1, 2]  # confkey (tgt attnums)
    assert next_oid == 40001


def test_two_independent_fks_to_same_table_not_merged():
    users = _tm(1, "Users")
    friendship = _tm(2, "Friendship")
    idx = cast(
        CatalogIndex,
        SimpleNamespace(
            table_id_to_oid={1: 200, 2: 300},
            toid_to_table={
                200: ("provisa", "public", "users"),
                300: ("provisa", "public", "friendship"),
            },
            ns_map={"public": 2200},
            col_attnum={(200, "id"): 1, (300, "user_a"): 1, (300, "user_b"): 2},
        ),
    )
    jm_a = SimpleNamespace(
        source_column="user_a",
        target_column="id",
        cardinality="many-to-one",
        target=users,
        source_constant=None,
        source_expr=None,
    )
    jm_b = SimpleNamespace(
        source_column="user_b",
        target_column="id",
        cardinality="many-to-one",
        target=users,
        source_constant=None,
        source_expr=None,
    )
    ctx = SimpleNamespace(
        tables={"Friendship": friendship, "Users": users},
        joins={("Friendship", "userA"): jm_a, ("Friendship", "userB"): jm_b},
        pk_columns={1: ["id"]},  # single-column PK → cannot be composite
    )
    rows, _ = _build_fk_constraint_rows(ctx, idx, 40000)
    assert len(rows) == 2
    assert all(len(r[18]) == 1 for r in rows)  # each conkey single-column
    assert {r[18][0] for r in rows} == {1, 2}


def test_pg_index_populated_from_pk_and_unique():
    db = duckdb.connect(":memory:")
    _populate_empty_system_tables(db)
    ctx = _composite_ctx()
    # add a PK on orders (composite) so _build_pk emits a composite-key constraint
    pk_rows, _ = _build_pk_constraint_rows(ctx, _idx(), 20000)
    _populate_pg_index(db, pk_rows)
    idx_rows = db.execute(
        "SELECT indrelid, indisprimary, indisunique, indkey, indnatts FROM _pg_index"
    ).fetchall()
    assert len(idx_rows) == 1
    indrelid, isprimary, isunique, indkey, indnatts = idx_rows[0]
    assert indrelid == 100  # orders
    assert isprimary is True
    assert isunique is True
    assert list(indkey) == [1, 2]  # composite PK attnums
    assert indnatts == 2


def test_pg_index_empty_when_no_constraints():
    db = duckdb.connect(":memory:")
    _populate_empty_system_tables(db)
    _populate_pg_index(db, [])
    assert db.execute("SELECT count(*) FROM _pg_index").fetchone()[0] == 0
