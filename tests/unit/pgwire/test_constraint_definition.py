# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg_get_constraintdef answers each constraint's definition as PostgreSQL writes it.

SQLAlchemy's PostgreSQL dialect reflects a table's foreign keys by reading
``pg_get_constraintdef(r.oid, true)`` and matching it against ``FOREIGN KEY (...) REFERENCES
[schema.]table(...)``; Superset reflects every table it shows that way. The catalog answered
NULL, and the reflection failed (``TypeError: expected string or bytes-like object``)."""

# Requirements: REQ-532

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import cast

import duckdb
import pytest

from provisa.pgwire.catalog_constraints import _populate_pg_constraint
from provisa.pgwire.catalog_populate import CatalogIndex
from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb

# SQLAlchemy 1.4's PostgreSQL dialect, get_foreign_keys (dialects/postgresql/base.py), verbatim.
_FK_SQL = """
  SELECT r.conname,
        pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
        n.nspname as conschema
  FROM  pg_catalog.pg_constraint r,
        pg_namespace n,
        pg_class c

  WHERE r.conrelid = {table} AND
        r.contype = 'f' AND
        c.oid = confrelid AND
        n.oid = c.relnamespace
  ORDER BY 1
"""
_FK_REGEX = re.compile(r"FOREIGN KEY \((.*?)\) REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)")


def _tm(table_id: int, name: str):
    return SimpleNamespace(table_id=table_id, type_name=name, field_name=name, display_name="")


@pytest.fixture
def catalog():
    customers, orders = _tm(1, "Customers"), _tm(2, "Orders")
    ctx = SimpleNamespace(
        tables={"Customers": customers, "Orders": orders},
        pk_columns={1: ["id"], 2: ["id"]},
        unique_constraints={1: [("uq_customers_email", ["email"])]},
        joins={
            ("Orders", "customer"): SimpleNamespace(
                source_column="customer_id",
                target_column="id",
                cardinality="many-to-one",
                target=customers,
                source_constant=None,
                source_expr=None,
            )
        },
    )
    idx = cast(
        CatalogIndex,
        SimpleNamespace(
            table_id_to_oid={1: 100, 2: 101},
            toid_to_table={
                100: ("provisa", "shop", "customers"),
                101: ("provisa", "shop", "orders"),
            },
            ns_map={"public": 2200, "shop": 3000},
            col_attnum={
                (100, "id"): 1,
                (100, "email"): 2,
                (101, "id"): 1,
                (101, "customer_id"): 2,
            },
        ),
    )
    db = duckdb.connect()
    db.execute("CREATE TABLE _pg_namespace (oid INTEGER, nspname VARCHAR)")
    db.execute("INSERT INTO _pg_namespace VALUES (2200, 'public'), (3000, 'shop')")
    db.execute("CREATE TABLE _pg_class (oid INTEGER, relname VARCHAR, relnamespace INTEGER)")
    db.execute("INSERT INTO _pg_class VALUES (100, 'customers', 3000), (101, 'orders', 3000)")
    _populate_pg_constraint(db, ctx, idx)
    yield db
    db.close()


def _rows(db, sql: str) -> list[tuple]:
    return db.execute(_rewrite_for_duckdb(sql, "analyst", "provisa")).fetchall()


def test_sqlalchemy_reflects_a_foreign_key_from_its_definition(catalog):
    [(conname, condef, conschema)] = _rows(catalog, _FK_SQL.format(table=101))
    assert condef == "FOREIGN KEY (customer_id) REFERENCES shop.customers(id)"
    assert conschema == "shop"
    m = _FK_REGEX.search(condef)
    assert m is not None and m.groups() == ("customer_id", "shop", "customers", "id")


@pytest.mark.parametrize(
    ("conname", "definition"),
    [("pk_customers", "PRIMARY KEY (id)"), ("uq_customers_email", "UNIQUE (email)")],
)
def test_a_key_constraint_is_written_as_postgresql_writes_it(catalog, conname, definition):
    [(condef,)] = _rows(
        catalog,
        "SELECT pg_catalog.pg_get_constraintdef(r.oid) FROM pg_catalog.pg_constraint r "
        f"WHERE r.conname = '{conname}'",
    )
    assert condef == definition
