# Copyright (c) 2026 Kenneth Stott
# Canary: 78638d94-980f-4a6e-b224-d65842e42ca1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DuckDB statement over an attached PostgreSQL table orders and limits as PostgreSQL does
(#294, REQ-066): DuckDB's postgres scanner answered ORDER BY ... NULLS FIRST LIMIT n with rows
missing, and every DESC was written with NULLS FIRST (PostgreSQL's null order). The null order is
now a sort key of its own, so the scanner never sees NULLS FIRST. Each case is answered by
PostgreSQL itself and by DuckDB over the attached table."""

# Requirements: REQ-066

from typing import LiteralString, cast

import duckdb
import psycopg
import pytest

from provisa.transpiler.transpile import transpile_to_duckdb

_ORDERS = [
    "DESC",
    "DESC NULLS FIRST",
    "DESC NULLS LAST",
    "ASC",
    "ASC NULLS FIRST",
    "ASC NULLS LAST",
]
_LIMITS = ["LIMIT 1", "LIMIT 2", "OFFSET 1 LIMIT 2", ""]


@pytest.fixture(scope="module")
def attached(embedded_postgres):
    pg = embedded_postgres
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("DROP DATABASE IF EXISTS null_order_294")
        conn.execute("CREATE DATABASE null_order_294")
    with psycopg.connect(pg.dsn("null_order_294"), autocommit=True) as conn:
        conn.execute("CREATE TABLE t (d date, v int)")
        conn.execute("CREATE TABLE no_nulls (d date, v int)")
        conn.execute(
            "INSERT INTO t VALUES ('2026-09-30', 1), ('2026-09-01', 2), (NULL, 3), ('2026-09-15', 4)"
        )
        conn.execute("INSERT INTO no_nulls VALUES ('2026-09-30', 1), ('2026-09-01', 2)")
    duck = duckdb.connect()
    duck.execute(f"ATTACH '{pg.dsn('null_order_294')}' AS p (TYPE POSTGRES, READ_ONLY)")
    yield pg, duck
    duck.close()
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("DROP DATABASE null_order_294 WITH (FORCE)")


@pytest.mark.parametrize("table", ["t", "no_nulls"])
@pytest.mark.parametrize("limit", _LIMITS)
@pytest.mark.parametrize("order", _ORDERS)
def test_an_ordered_limited_read_answers_as_postgresql_does(attached, table, order, limit):
    pg, duck = attached
    sql = f'SELECT "v" FROM "p"."public"."{table}" ORDER BY "d" {order} {limit}'
    with psycopg.connect(pg.dsn("null_order_294")) as conn:
        # The same statement on PostgreSQL, the table addressed by its schema.
        expected = conn.execute(cast(LiteralString, sql.replace('"p".', ""))).fetchall()
    assert duck.execute(transpile_to_duckdb(sql)).fetchall() == expected


def test_no_statement_order_is_written_with_a_nulls_clause():
    written = transpile_to_duckdb('SELECT "v" FROM "p"."public"."t" ORDER BY "d" DESC LIMIT 1')
    assert "NULLS" not in written.upper()
    assert written.endswith('ORDER BY ("d" IS NULL) DESC, "d" DESC LIMIT 1')


def test_a_window_and_a_union_keep_their_own_order():
    # A window's ORDER BY is no scan's (and a RANGE frame orders by one key); a UNION's orders
    # the union's rows, which DuckDB orders by its output columns only.
    window = transpile_to_duckdb('SELECT ROW_NUMBER() OVER (ORDER BY "d" DESC) FROM "t"')
    assert 'OVER (ORDER BY "d" DESC NULLS FIRST)' in window
    union = transpile_to_duckdb(
        'SELECT "d" FROM "t" UNION ALL SELECT "d" FROM "u" ORDER BY "d" DESC'
    )
    assert union.endswith('ORDER BY "d" DESC NULLS FIRST')


def test_an_ordinal_key_tests_the_item_it_names():
    written = transpile_to_duckdb('SELECT "d" AS "x", "v" FROM "t" ORDER BY 1 DESC LIMIT 2')
    assert 'ORDER BY ("x" IS NULL) DESC, 1 DESC' in written


def test_a_view_reading_its_latest_row_answers_on_duckdb_as_on_postgresql(embedded_postgres):
    """#294 as a demo met it: groupexposure's exposure view reads the bank's Tier 1 capital as
    (SELECT tier1_eur FROM capital ORDER BY as_of DESC LIMIT 1); on DuckDB that subquery was
    empty, so every borrower's share of Tier 1 was null."""
    import importlib.util
    import sys
    from pathlib import Path

    repo = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "graphql_golden", repo / "scripts" / "graphql_golden.py"
    )
    assert spec is not None and spec.loader is not None
    golden = importlib.util.module_from_spec(spec)
    sys.modules["graphql_golden"] = golden
    spec.loader.exec_module(golden)
    demo = repo / "demo" / "named" / "groupexposure"
    answers = golden.demo_answers(embedded_postgres, demo, "null_order_294_ge")
    by_sector = {
        e["engine"]: {r[0]: r for r in e["rows"]}
        for e in answers
        if e.get("engine") and e["origin"].endswith("2-by-sector.graphql")
    }
    assert float(by_sector["postgres"]["Utilities"][3]) == 8.1
    assert by_sector["duckdb"]["Utilities"][3] == 8.1


def test_an_output_name_a_source_column_shares_is_ordered_as_the_output():
    """The reviews demo's 'customers': SUM(t.customers) AS customers, ordered by customers. Inside
    an expression DuckDB reads a bare name as the source's column, not the output's, so the null
    key tests the output's own expression -- ungrouped t.customers was a binder error."""
    sql = (
        "SELECT o.category, SUM(t.customers) AS customers FROM t JOIN o ON o.id = t.oid "
        "GROUP BY o.category ORDER BY customers DESC"
    )
    written = transpile_to_duckdb(sql)
    assert written.endswith("ORDER BY (SUM(t.customers) IS NULL) DESC, customers DESC"), written
    duck = duckdb.connect()
    duck.execute("CREATE TABLE t (oid int, customers int)")
    duck.execute("CREATE TABLE o (id int, category text)")
    duck.execute("INSERT INTO t VALUES (1, 5), (1, 2), (2, NULL), (3, 1)")
    duck.execute("INSERT INTO o VALUES (1, 'power'), (2, 'cables'), (3, 'cases')")
    # As PostgreSQL orders it: DESC puts the null total first.
    assert duck.execute(written).fetchall() == [("cables", None), ("power", 7), ("cases", 1)]
