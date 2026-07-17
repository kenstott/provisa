# Copyright (c) 2026 Kenneth Stott
# Canary: 115449e6-fd23-4e56-b4a5-6b77264c4098
"""REQ-1093: pgwire emits declared UNIQUE constraints as pg_constraint contype 'u'."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from provisa.pgwire.catalog_populate import CatalogIndex
from provisa.pgwire.catalog_constraints import (
    _build_unique_constraint_rows,
    _populate_is_constraints,
)


class _Idx:
    table_id_to_oid = {1: 100}
    toid_to_table = {100: ("provisa", "public", "users")}
    ns_map = {"public": 2200}
    col_attnum = {(100, "tenant_id"): 1, (100, "email"): 2, (100, "sku"): 3}
    attnum_to_col = {(100, 1): "tenant_id", (100, 2): "email", (100, 3): "sku"}


def _idx() -> CatalogIndex:
    # duck-typed stand-in exposing only the attributes the builders read
    return cast(CatalogIndex, _Idx())


def _ctx():
    tm = SimpleNamespace(table_id=1)
    return SimpleNamespace(
        tables={"users": tm},
        unique_constraints={
            1: [("users_tenant_email_key", ["tenant_id", "email"]), ("users_sku_key", ["sku"])]
        },
    )


def test_unique_rows_contype_u_and_ordered_conkey():
    rows, next_oid = _build_unique_constraint_rows(_ctx(), _idx(), 30000)
    assert len(rows) == 2
    composite = next(r for r in rows if r[1] == "users_tenant_email_key")
    assert composite[3] == "u"  # contype
    assert composite[7] == 100  # conrelid
    assert composite[11] == 0  # confrelid (no referenced table)
    assert composite[18] == [1, 2]  # conkey in ordinal order
    single = next(r for r in rows if r[1] == "users_sku_key")
    assert single[18] == [3]
    assert next_oid == 30002


def test_unique_skipped_when_column_missing_from_projection():
    ctx = _ctx()
    ctx.unique_constraints = {1: [("uq_hidden", ["tenant_id", "not_projected"])]}
    rows, _ = _build_unique_constraint_rows(ctx, _idx(), 30000)
    assert rows == []


def test_information_schema_projects_unique_type():
    import duckdb

    db = duckdb.connect(":memory:")
    rows, _ = _build_unique_constraint_rows(_ctx(), _idx(), 30000)
    _populate_is_constraints(db, rows, _idx())
    tc = db.execute(
        "SELECT constraint_name, constraint_type FROM _is_table_constraints ORDER BY constraint_name"
    ).fetchall()
    assert ("users_sku_key", "UNIQUE") in tc
    assert ("users_tenant_email_key", "UNIQUE") in tc
    kcu = db.execute(
        "SELECT column_name, ordinal_position FROM _is_key_column_usage "
        "WHERE constraint_name = 'users_tenant_email_key' ORDER BY ordinal_position"
    ).fetchall()
    assert kcu == [("tenant_id", 1), ("email", 2)]
