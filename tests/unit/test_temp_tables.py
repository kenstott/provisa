# Copyright (c) 2026 Kenneth Stott
# Canary: b86fa3e4-fd77-47cb-87e6-269e693ff159
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's own temporary tables (REQ-615, REQ-1926, REQ-1942): what a statement does to one
is read as the governed SELECT giving its rows and what is done with them; every other definition
is nothing of the kind."""

# Requirements: REQ-615, REQ-1926, REQ-1942

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler import temp_tables
from provisa.compiler.definitions import NotAvailableHere
from provisa.compiler.temp_tables import TempSession, TempTable, action_of


@pytest.fixture
def session():
    held = TempSession()
    held.tables["t"] = TempTable("t", [("id", "bigint"), ("region", "varchar")])
    token = temp_tables.bind(held)
    yield held
    temp_tables.unbind(token)


def test_a_temporary_table_needs_a_session():
    with pytest.raises(NotAvailableHere, match="needs a session"):
        action_of("CREATE TEMP TABLE t (id INT)")


def test_create_with_columns_takes_no_rows(session):
    action = action_of("CREATE TEMPORARY TABLE k (id INTEGER, name TEXT)")
    assert (action.kind, action.name) == ("create", "k")
    assert action.columns == [("id", "INT"), ("name", "TEXT")]
    assert "WHERE FALSE" in action.select


def test_create_as_select_takes_the_selects_rows(session):
    action = action_of("CREATE TEMP TABLE k AS SELECT id FROM sales.orders WHERE id < 3")
    assert (action.kind, action.name, action.columns) == ("create", "k", None)
    assert (
        sqlglot.parse_one(action.select, read="postgres").find(sqlglot.exp.Table).name == "orders"
    )


def test_an_existing_name_and_a_qualified_name_are_refused(session):
    with pytest.raises(ValueError, match="already exists in this session"):
        action_of("CREATE TEMP TABLE t (id INT)")
    with pytest.raises(NotAvailableHere, match="with no schema"):
        action_of("CREATE TEMP TABLE sales.k (id INT)")


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE made (id INT)",
        "CREATE TABLE made AS SELECT 1",
        "CREATE VIEW v AS SELECT 1",
        "DROP TABLE orders",
        "ALTER TABLE t ADD COLUMN x INT",
        "INSERT INTO orders VALUES (1, 'x')",
        "UPDATE orders SET region = 'x'",
        "DELETE FROM orders",
        "SELECT * FROM t",
    ],
)
def test_any_other_statement_does_nothing_to_a_temporary_table(session, sql):
    assert action_of(sql) is None


def test_an_insert_takes_its_values_or_its_select(session):
    values = action_of("INSERT INTO t VALUES (1, 'east'), (2, 'west')")
    assert (values.kind, values.name) == ("insert", "t")
    assert [c for c, _ in values.columns] == ["id", "region"]
    assert values.select.startswith("SELECT * FROM (VALUES")
    named = action_of("INSERT INTO t (region) SELECT region FROM sales.orders")
    assert [c for c, _ in named.columns] == ["region"]
    with pytest.raises(ValueError, match="no column 'nope'"):
        action_of("INSERT INTO t (nope) VALUES (1)")


def test_an_update_and_a_delete_are_the_tables_rows_afterwards(session):
    update = action_of("UPDATE t SET region = 'venus' WHERE id = 1")
    assert update.kind == "rewrite" and update.rewrite.endswith("FROM {table}")
    assert (
        "CASE WHEN COALESCE((id = 1), FALSE) THEN ('venus') ELSE \"region\" END" in update.rewrite
    )
    delete = action_of("DELETE FROM t WHERE id = 2")
    assert delete.rewrite == "SELECT * FROM {table} WHERE NOT COALESCE((id = 2), FALSE)"
    assert action_of("DELETE FROM t").rewrite.endswith("NOT COALESCE((TRUE), FALSE)")


def test_drop_of_the_sessions_table_and_reads_of_it(session):
    assert action_of("DROP TABLE t").kind == "drop"
    assert temp_tables.reads(sqlglot.parse_one("SELECT * FROM t JOIN sales.orders o ON TRUE"))
    assert not temp_tables.reads(sqlglot.parse_one("SELECT * FROM sales.t"))
    assert temp_tables.names() == frozenset({"t"})


def test_a_column_is_landed_as_its_own_type_or_refused_by_name():
    """A decimal keeps its precision and scale; a type a temporary table does not take is
    refused naming the column and the type -- never stored as another type."""
    from decimal import Decimal

    from provisa.core.ir_types import to_sqlalchemy
    from provisa.pgwire.temp_exec import _batch, inferred_type, landed_type

    assert landed_type("id", "INT") == "bigint"
    assert landed_type("name", "VARCHAR(40)") == "text"
    assert landed_type("price", "DECIMAL(12, 4)") == "numeric(12,4)"
    assert landed_type("price", "numeric(12,4)") == "numeric(12,4)"
    with pytest.raises(ValueError, match="'price': give DECIMAL its precision and scale"):
        landed_type("price", "DECIMAL")
    with pytest.raises(ValueError, match="column 'doc': type JSONB is not supported"):
        landed_type("doc", "JSONB")
    sized = to_sqlalchemy("numeric(12,4)")
    assert (sized.precision, sized.scale) == (12, 4)

    assert inferred_type("price", [Decimal("1.2345"), None]) == "numeric(5,4)"
    assert inferred_type("n", [1, 2]) == "bigint" and inferred_type("s", ["a"]) == "text"
    with pytest.raises(ValueError, match="'gap' has no value to take its type from: CAST it"):
        inferred_type("gap", [None, None])
    with pytest.raises(ValueError, match="column 'tags': type list<item: int64> is not supported"):
        inferred_type("tags", [[1, 2]])

    batch = _batch([("price", "numeric(12,4)")], [(Decimal("1.2345"),), (None,)])
    assert str(batch.schema.field("price").type) == "decimal128(12, 4)"
    assert batch.column(0).to_pylist() == [Decimal("1.2345"), None]
    with pytest.raises(ValueError, match="column 'id' is bigint; a value given for it is not"):
        _batch([("id", "bigint")], [("not a number",)])


def test_a_statement_is_kept_for_its_session_alone_as_its_tables_stand():
    """The same words in two sessions name two tables: a governed statement, and the address it
    was lowered to, is never shared between them, nor kept across a drop and a create."""
    assert temp_tables.slot_key() == []  # no session
    first, second = TempSession(), TempSession()
    token = temp_tables.bind(first)
    try:
        assert temp_tables.slot_key() == []  # a session with no table shares every statement
        first.tables["mine"] = TempTable("mine", [("id", "bigint")])
        mine = temp_tables.slot_key()
        assert mine == [("provisa.temp_session", f"{first.id}:0")]
        first.generation += 1
        assert temp_tables.slot_key() != mine
    finally:
        temp_tables.unbind(token)
    second.tables["mine"] = TempTable("mine", [("id", "bigint")])
    token = temp_tables.bind(second)
    try:
        assert temp_tables.slot_key() != mine
    finally:
        temp_tables.unbind(token)
