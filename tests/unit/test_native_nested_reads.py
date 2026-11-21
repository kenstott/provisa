# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4b1f7e-2a85-4d36-b0c9-f7e3a6d15b28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1965: every read of a natively array, struct or map column in a physical statement is the
source's own to-JSON expression, on whichever backend the statement is routed to."""

from __future__ import annotations

import json

import duckdb

from provisa.compiler.native_nested import NestedColumn, mark_native_nested_reads
from provisa.transpiler.transpile import transpile

_NESTED = {
    ("shop", "orders"): {
        "tags": NestedColumn("varchar[]", 1, "text"),
        "amounts": NestedColumn("decimal(18,2)[]", 1, "numeric(18,2)"),
        "ship_to": NestedColumn("struct(city varchar, zip varchar)", 0, None),
    }
}


def _on(dialect: str, semantic: str) -> str:
    return transpile(mark_native_nested_reads(semantic, _NESTED), dialect)


def test_a_selected_native_array_is_read_as_json_under_its_own_name():
    sql = 'SELECT "o"."id", "o"."tags" FROM "shop"."orders" AS "o"'
    assert _on("duckdb", sql) == (
        'SELECT "o"."id", TO_JSON("o"."tags") AS "tags" FROM "shop"."orders" AS "o"'
    )
    assert _on("postgres", sql) == (
        'SELECT "o"."id", TO_JSONB("o"."tags") AS "tags" FROM "shop"."orders" AS "o"'
    )
    assert _on("trino", sql) == (
        'SELECT "o"."id", CAST("o"."tags" AS JSON) AS "tags" FROM "shop"."orders" AS "o"'
    )


def test_an_aliased_and_an_unqualified_read_are_both_marked():
    sql = 'SELECT tags AS labels, ship_to FROM "shop"."orders"'
    assert _on("duckdb", sql) == (
        'SELECT TO_JSON(tags) AS labels, TO_JSON(ship_to) AS "ship_to" FROM "shop"."orders"'
    )


def test_a_decimal_array_is_read_with_its_elements_as_strings():
    sql = 'SELECT "o"."amounts" FROM "shop"."orders" AS "o"'
    assert _on("duckdb", sql) == (
        'SELECT TO_JSON(LIST_TRANSFORM("o"."amounts", LAMBDA x1 : CAST(x1 AS TEXT))) AS "amounts" '
        'FROM "shop"."orders" AS "o"'
    )


def test_a_null_test_keeps_the_bare_column():
    sql = 'SELECT "o"."id" FROM "shop"."orders" AS "o" WHERE "o"."tags" IS NOT NULL'
    assert _on("postgres", sql) == (
        'SELECT "o"."id" FROM "shop"."orders" AS "o" WHERE "o"."tags" IS NOT NULL'
    )


def test_a_predicate_reads_the_json():
    sql = 'SELECT "o"."id" FROM "shop"."orders" AS "o" WHERE "o"."tags" ->> 0 = \'a\''
    assert 'TO_JSONB("o"."tags") ->> 0 = \'a\'' in _on("postgres", sql)


def test_a_write_target_is_left_as_written_and_returning_is_read_as_json():
    sql = 'UPDATE "shop"."orders" SET "tags" = $1 WHERE "id" = $2 RETURNING "tags"'
    assert _on("postgres", sql) == (
        'UPDATE "shop"."orders" SET "tags" = $1 WHERE "id" = $2 '
        'RETURNING TO_JSONB("tags") AS "tags"'
    )


def test_marking_is_idempotent_and_leaves_other_tables_untouched():
    sql = 'SELECT "o"."tags" FROM "shop"."orders" AS "o"'
    once = mark_native_nested_reads(sql, _NESTED)
    assert mark_native_nested_reads(once, _NESTED) == once
    other = 'SELECT "c"."tags" FROM "shop"."customers" AS "c"'
    assert mark_native_nested_reads(other, _NESTED) is other
    assert mark_native_nested_reads(sql, {}) is sql


def test_the_statement_runs_on_duckdb_and_returns_json_text():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA shop")
    con.execute(
        "CREATE TABLE shop.orders (id INTEGER, tags VARCHAR[], amounts DECIMAL(18,2)[], "
        "ship_to STRUCT(city VARCHAR, zip VARCHAR))"
    )
    con.execute(
        "INSERT INTO shop.orders VALUES "
        "(1, ['a', 'b'], [1234567890123456.78], {'city': 'Oslo', 'zip': '0150'}), "
        "(2, [], NULL, NULL)"
    )
    sql = _on(
        "duckdb",
        'SELECT "o"."id", "o"."tags", "o"."amounts", "o"."ship_to" FROM "shop"."orders" AS "o" '
        'WHERE "o"."tags" IS NOT NULL ORDER BY "o"."id"',
    )

    rows = con.execute(sql).fetchall()

    assert [d[0] for d in con.description] == ["id", "tags", "amounts", "ship_to"]
    assert all(isinstance(v, str) or v is None for row in rows for v in row[1:])
    assert [json.loads(v) for v in rows[0][1:]] == [
        ["a", "b"],
        ["1234567890123456.78"],
        {"city": "Oslo", "zip": "0150"},
    ]
    assert rows[1] == (2, "[]", None, None)
