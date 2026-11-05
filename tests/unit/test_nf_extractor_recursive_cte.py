# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3c2a91-6d47-4b05-a1e8-c5b7d29e0f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Dropping the UNION branches of a table the statement cannot address (its required parameters
have no value) keeps the statement well-formed when a branch is part of a recursive CTE (#312).

A recursive term's arm that reads the table is dropped like any branch: the walk keeps every row
that never reads it, the rows it would give with the table empty. An anchor arm is dropped only
while another anchor stays. When every anchor reads the table, every row of the walk comes through
it, as a lone table does, so the read is refused naming the table (REQ-1661, REQ-1986) instead of
leaving the recursive term without its base case. Dropping the first arm of any union keeps the
column names it gave, and dropping an arm of the statement's own top-level union keeps its WITH.
Each statement runs on DuckDB, so the rows are checked, not only the SQL."""

# Requirements: REQ-1661, REQ-1986, REQ-751

from __future__ import annotations

import duckdb
import pytest

from provisa.compiler.nf_extractor import (
    apply_dropped_tables,
    drop_union_branches_for_table,
    find_api_table_names,
)
from provisa.core.parameters import ParameterRequired, missing_reason
from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from tests.unit.test_cypher_min_hops import _node, _rel

_REASON = missing_reason(["q"])


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """a, b, x hold edges (id -> nxt): a 1->2, b 2->3 3->4, x 1->5 5->6."""
    con = duckdb.connect()
    for name, rows in {"a": "(1, 2)", "b": "(2, 3), (3, 4)", "x": "(1, 5), (5, 6)"}.items():
        con.execute(f"CREATE TABLE {name} (id INTEGER, nxt INTEGER)")
        con.execute(f"INSERT INTO {name} VALUES {rows}")
    return con


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return sorted(con.execute(sql).fetchall())


def _rows_with_x_empty(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    """What *sql* answers when ``x`` contributes no rows: the oracle for a dropped branch."""
    con.execute("CREATE TEMP TABLE x_keep AS SELECT * FROM x")
    con.execute("DELETE FROM x")
    try:
        return _rows(con, sql)
    finally:
        con.execute("INSERT INTO x SELECT * FROM x_keep")
        con.execute("DROP TABLE x_keep")


def _assert_dropped(con: duckdb.DuckDBPyConnection, sql: str) -> str:
    out = apply_dropped_tables(sql, {"x": _REASON})
    assert "x" not in find_api_table_names(out), out
    assert _rows(con, out) == _rows_with_x_empty(con, sql), out
    return out


def _assert_refused(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    out = drop_union_branches_for_table(sql, "x")
    con.execute(out).fetchall()  # whatever is left is a statement the engine accepts
    with pytest.raises(ParameterRequired) as err:
        apply_dropped_tables(sql, {"x": _REASON})
    assert err.value.table == "x"


_STEP = "SELECT {t}.nxt, w.h + 1 FROM w JOIN {t} ON {t}.id = w.cur WHERE w.h < 3"


def _walk(*arms: str) -> str:
    return f"WITH RECURSIVE w AS ({' UNION ALL '.join(arms)}) SELECT cur, h FROM w"


# --- the anchor ----------------------------------------------------------------------------------


def test_a_walk_whose_only_anchor_reads_the_table_is_refused(con):
    # Before #312 the anchor was dropped, leaving the recursive term alone:
    # "Circular reference to CTE "w", use WITH RECURSIVE to use recursive CTEs".
    _assert_refused(con, _walk("SELECT id AS cur, 1 AS h FROM x", _STEP.format(t="b")))


def test_a_walk_whose_every_anchor_reads_the_table_is_refused(con):
    sql = _walk(
        "SELECT id AS cur, 1 AS h FROM x",
        "SELECT nxt AS cur, 1 AS h FROM x",
        _STEP.format(t="b"),
    )
    _assert_refused(con, sql)


def test_an_anchor_that_reads_the_table_goes_while_another_stays(con):
    # The first anchor names the walk's columns; before #312 its names went with it:
    # 'Values list "w" does not have a column named "cur"'.
    sql = _walk("SELECT id AS cur, 1 AS h FROM x", "SELECT id, 1 FROM a", _STEP.format(t="b"))
    _assert_dropped(con, sql)


# --- the recursive term --------------------------------------------------------------------------


def test_a_recursive_arm_that_reads_the_table_goes_while_another_stays(con):
    # DuckDB admits one recursive term, so the statement is not run with x empty; the rows are
    # the walk over b alone: a 1->2, then b 2->3->4.
    for steps in [("x", "b"), ("b", "x")]:
        sql = _walk("SELECT nxt AS cur, 1 AS h FROM a", *(_STEP.format(t=t) for t in steps))
        out = apply_dropped_tables(sql, {"x": _REASON})
        assert "x" not in find_api_table_names(out), out
        assert _rows(con, out) == [(2, 1), (3, 2), (4, 3)], out


def test_a_walk_whose_every_recursive_arm_reads_the_table_keeps_its_first_hop(con):
    out = _assert_dropped(con, _walk("SELECT id AS cur, 1 AS h FROM a", _STEP.format(t="x")))
    assert "RECURSIVE" not in out.upper(), "nothing in the WITH refers to itself any more"


def test_a_hop_of_the_one_recursive_term_that_reads_the_table_goes(con):
    hops = "SELECT id, nxt FROM x UNION ALL SELECT id, nxt FROM b"
    sql = _walk(
        "SELECT id AS cur, 1 AS h FROM a",
        f"SELECT _hop.nxt, w.h + 1 FROM w JOIN ({hops}) AS _hop ON _hop.id = w.cur WHERE w.h < 3",
    )
    _assert_dropped(con, sql)


# --- the statement around it ---------------------------------------------------------------------


def test_the_top_level_union_keeps_its_with(con):
    # Before #312 the WITH hung off the dropped union: "Table with name w does not exist!"
    sql = (
        "WITH RECURSIVE w AS (SELECT nxt AS cur, 1 AS h FROM a UNION ALL "
        f"{_STEP.format(t='b')}) SELECT cur FROM w UNION ALL SELECT id FROM x ORDER BY 1 LIMIT 2"
    )
    out = _assert_dropped(con, sql)
    assert con.execute(out).fetchall() == [(2,), (3,)]  # the walk 2, 3, 4, ordered and limited


def test_a_parenthesized_arm_left_alone_keeps_the_statement_s_order_and_limit(con):
    sql = (
        "SELECT id FROM x UNION ALL (SELECT id FROM b ORDER BY id DESC LIMIT 1) ORDER BY 1 LIMIT 5"
    )
    out = drop_union_branches_for_table(sql, "x")
    assert "x" not in find_api_table_names(out), out
    assert con.execute(out).fetchall() == [(3,)]


def test_dropping_the_first_arm_keeps_the_union_s_column_names(con):
    out = drop_union_branches_for_table("SELECT id AS k FROM x UNION ALL SELECT nxt FROM a", "x")
    assert [d[0] for d in con.execute(out).description] == ["k"]


# --- a Cypher variable-length walk (the #312 report) ---------------------------------------------

_TERMS = {
    "Terms": _node("Terms", "terms"),
    "Concepts": _node("Concepts", "concepts"),
    "Employees": _node("Employees", "employees"),
    "Schedules": _node("Schedules", "schedules"),
}


@pytest.fixture
def graph() -> duckdb.DuckDBPyConnection:
    """t1 and t2 narrow c1, t1 is related to t2 and t2 to t3; a schedule names an employee."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute(
        "CREATE TABLE pg.public.terms (id INTEGER, name VARCHAR, concept_id INTEGER, "
        "related_id INTEGER)"
    )
    con.execute(
        "INSERT INTO pg.public.terms VALUES (1, 't1', 1, 2), (2, 't2', 1, 3), (3, 't3', NULL, NULL)"
    )
    con.execute("CREATE TABLE pg.public.concepts (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO pg.public.concepts VALUES (1, 'c1')")
    con.execute("CREATE TABLE pg.public.employees (id INTEGER, name VARCHAR)")
    con.execute("CREATE TABLE pg.public.schedules (id INTEGER, name VARCHAR, employee_id INTEGER)")
    return con


def _cypher(cypher: str, rels: dict) -> str:
    lm = CypherLabelMap(nodes=_TERMS, relationships=rels)
    return cypher_to_sql(parse_cypher(cypher), lm, {})[0].sql(dialect="postgres")


def test_a_walk_between_terms_through_an_unaddressable_concepts_is_refused(graph):
    # The #312 report: the walk's base case reads concepts and the rest of it reached employees
    # and schedules, so the base case alone was dropped ("Circular reference to CTE _traverse_n0").
    sql = _cypher(
        "MATCH (n0:Terms)-[*..2]-(n1:Terms) RETURN n0.name, n1.name",
        {
            "BROADER": _rel("BROADER", "Terms", "Concepts", "concept_id"),
            "WORKS": _rel("WORKS", "Schedules", "Employees", "employee_id"),
        },
    )
    graph.execute(drop_union_branches_for_table(sql, "concepts")).fetchall()
    with pytest.raises(ParameterRequired) as err:
        apply_dropped_tables(sql, {"concepts": _REASON})
    assert err.value.table == "concepts"


def test_a_walk_between_terms_drops_only_its_hops_through_concepts(graph):
    sql = _cypher(
        "MATCH (n0:Terms)-[*..2]->(n1:Terms) RETURN n0.name, n1.name",
        {
            "BROADER": _rel("BROADER", "Terms", "Concepts", "concept_id"),
            "RELATED": _rel("RELATED", "Terms", "Terms", "related_id"),
        },
    )
    out = apply_dropped_tables(sql, {"concepts": _REASON})
    assert "concepts" not in find_api_table_names(out)
    assert _rows(graph, out) == [("t1", "t2"), ("t1", "t3"), ("t2", "t3")]
