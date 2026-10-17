# Copyright (c) 2026 Kenneth Stott
# Canary: 178aa15e-545b-4c8f-95f8-00d0faf8301c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's virtual columns read in SQL as GraphQL reads them.

Every registered table carries ``_name_`` (its domain and table, ``sales.events``) and
``_domain_`` (its domain): the GraphQL compiler answers them as literals (``ctx.virtual_columns``)
and the pgwire catalog lists them among the table's columns. The SQL pipeline sent a reference
to them on to the source, which has no such column, so a client selecting every column it was
shown (Superset, a BI tool) was refused: column "_name_" does not exist."""

# Requirements: REQ-641

from __future__ import annotations

import duckdb
import pytest

from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical
from provisa.compiler.sql_types import CompilationContext, TableMeta

_EVENTS = TableMeta(
    table_id=1,
    field_name="events",
    type_name="Events",
    source_id="pg",
    catalog_name="pg",
    schema_name="public",
    table_name="events",
    domain_id="sales",
)
_USERS = TableMeta(
    table_id=2,
    field_name="users",
    type_name="Users",
    source_id="pg",
    catalog_name="pg",
    schema_name="public",
    table_name="users",
    domain_id="sales",
)


@pytest.fixture
def source():
    db = duckdb.connect()
    db.execute("CREATE SCHEMA IF NOT EXISTS public")
    db.execute("CREATE TABLE public.events (id INTEGER, user_id INTEGER)")
    db.execute("CREATE TABLE public.users (id INTEGER, name VARCHAR)")
    db.execute("INSERT INTO public.events VALUES (1, 1), (2, 1)")
    db.execute("INSERT INTO public.users VALUES (1, 'Ann')")
    return db


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {"events": _EVENTS, "users": _USERS}
    ctx.virtual_columns = {
        1: {"_name_": "sales.events", "_domain_": "sales"},
        2: {"_name_": "sales.users", "_domain_": "sales"},
    }
    return ctx


def _rows(source, sql: str) -> list[tuple]:
    return source.execute(rewrite_semantic_to_physical(sql, _ctx())).fetchall()


def test_a_select_reads_them_as_graphql_does(source):
    assert _rows(source, 'SELECT id, _name_, _domain_ FROM "sales"."events" ORDER BY id') == [
        (1, "sales.events", "sales"),
        (2, "sales.events", "sales"),
    ]


def test_a_selected_virtual_column_keeps_its_name(source):
    cursor = source.execute(
        rewrite_semantic_to_physical('SELECT e._name_ FROM "sales"."events" AS e', _ctx())
    )
    assert [d[0] for d in cursor.description] == ["_name_"]


def test_a_where_on_them_filters_as_its_value_does(source):
    assert _rows(source, 'SELECT id FROM "sales"."events" WHERE _name_ = \'sales.events\'') == [
        (1,),
        (2,),
    ]
    assert _rows(source, 'SELECT id FROM "sales"."events" WHERE _domain_ = \'other\'') == []


def test_each_tables_own_in_a_join(source):
    assert _rows(
        source,
        'SELECT e.id, e._name_, u._name_ FROM "sales"."events" AS e '
        'JOIN "sales"."users" AS u ON u.id = e.user_id ORDER BY e.id',
    ) == [(1, "sales.events", "sales.users"), (2, "sales.events", "sales.users")]
