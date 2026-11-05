# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1e8b47-2c9a-4f36-b0e1-7a4c93d6e215
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An undirected variable-length pattern (and its both-arrow spelling, ``<-[...]->``, which
openCypher matches the same way) walks only the relationships its start can reach within its
bound. A walk between Terms reads no other domain's tables (employees, schedules), and a label
no relationship touches answers no rows rather than an error. A path search needs one end bound
or labelled (REQ-348, ONE END BOUND OR BINDABLE): a variable an earlier clause bound keeps its
label (REQ-347), and the other end may be free -- whatever the walk reaches. Run on DuckDB, so the
rows are checked, not only the SQL."""

# Requirements: REQ-347, REQ-348, REQ-751

import dataclasses

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap, NodeMapping
from provisa.gql.path_walk import UNLABELLED_ENDS
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CypherTranslateError
from tests.unit.test_cypher_min_hops import _node as _min_hops_node
from tests.unit.test_cypher_min_hops import _rel


def _node(label: str, table: str) -> NodeMapping:
    """A node whose name column is its physical column too, as a free end's union reads it."""
    return dataclasses.replace(_min_hops_node(label, table), physical_properties={"name": "name"})


_OTHER_DOMAIN = {
    "Employees": _node("Employees", "employees"),
    "Schedules": _node("Schedules", "schedules"),
}
_WORKS = {"WORKS": _rel("WORKS", "Schedules", "Employees", "employee_id")}
# Ontology is the Terms' domain label, as the Graph page writes it (:Ontology:Terms).
_DOMAINS = {"Ontology": ["Terms"]}


def _isolated_terms() -> CypherLabelMap:
    """Terms, which no relationship touches, beside employees and their schedules."""
    return CypherLabelMap(
        nodes={"Terms": _node("Terms", "terms"), **_OTHER_DOMAIN},
        relationships=_WORKS,
        domains=_DOMAINS,
    )


def _linked_terms() -> CypherLabelMap:
    """Each term narrows a concept (BROADER), a concept has a parent concept (PARENT); employees
    and their schedules are another domain, reachable from no term."""
    return CypherLabelMap(
        nodes={
            "Terms": _node("Terms", "terms"),
            "Concepts": _node("Concepts", "concepts"),
            **_OTHER_DOMAIN,
        },
        relationships={
            "BROADER": _rel("BROADER", "Terms", "Concepts", "concept_id"),
            "PARENT": _rel("PARENT", "Concepts", "Concepts", "parent_id"),
            **_WORKS,
        },
        domains=_DOMAINS,
    )


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """t1 and t2 narrow c1; t3 narrows c2, whose parent is c1. A schedule names an employee."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute("CREATE TABLE pg.public.terms (id INTEGER, name VARCHAR, concept_id INTEGER)")
    con.execute("INSERT INTO pg.public.terms VALUES (1, 't1', 1), (2, 't2', 1), (3, 't3', 2)")
    con.execute("CREATE TABLE pg.public.concepts (id INTEGER, name VARCHAR, parent_id INTEGER)")
    con.execute("INSERT INTO pg.public.concepts VALUES (1, 'c1', NULL), (2, 'c2', 1)")
    con.execute("CREATE TABLE pg.public.employees (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO pg.public.employees VALUES (1, 'e1')")
    con.execute("CREATE TABLE pg.public.schedules (id INTEGER, name VARCHAR, employee_id INTEGER)")
    con.execute("INSERT INTO pg.public.schedules VALUES (1, 's1', 1)")
    return con


def _sql(cypher: str, lm: CypherLabelMap) -> str:
    return cypher_to_sql(parse_cypher(cypher), lm, {})[0].sql(dialect="duckdb")


def _pairs(con: duckdb.DuckDBPyConnection, cypher: str, lm: CypherLabelMap) -> set[tuple]:
    return set(con.execute(_sql(cypher, lm)).fetchall())


_ARROWS = ["-[*..2]-", "<-[*..2]->"]


@pytest.mark.parametrize("arrows", _ARROWS)
def test_a_walk_from_a_label_no_relationship_touches_is_no_rows(con, arrows):
    cypher = f"MATCH (n0:Terms){arrows}(n1:Terms) RETURN n0.name, n1.name"
    assert _pairs(con, cypher, _isolated_terms()) == set()


@pytest.mark.parametrize("arrows", _ARROWS)
def test_a_walk_reads_no_relationship_its_start_cannot_reach(arrows):
    sql = _sql(f"MATCH (n0:Terms){arrows}(n1:Terms) RETURN n0.name, n1.name", _linked_terms())
    assert '"concepts"' in sql
    assert '"employees"' not in sql and '"schedules"' not in sql


@pytest.mark.parametrize("arrows", _ARROWS)
def test_a_walk_between_terms_keeps_its_bound(con, arrows):
    # Two hops: t1 and t2 through c1. t3 is three hops from either (t3-c2-c1-t1), past the bound;
    # no term pairs with itself, as that walk takes its one edge twice.
    two = _pairs(
        con, f"MATCH (n0:Terms){arrows}(n1:Terms) RETURN n0.name, n1.name", _linked_terms()
    )
    assert two == {("t1", "t2"), ("t2", "t1")}
    three = _pairs(
        con,
        f"MATCH (n0:Terms){arrows.replace('..2', '..3')}(n1:Terms) RETURN n0.name, n1.name",
        _linked_terms(),
    )
    assert three == two | {("t1", "t3"), ("t3", "t1"), ("t2", "t3"), ("t3", "t2")}


@pytest.mark.parametrize("lm", [_isolated_terms, _linked_terms])
def test_a_variable_bound_with_a_label_keeps_it_in_a_later_path(con, lm):
    # REQ-347, REQ-348: n0 is a Terms node, bound by the first MATCH; no relationship leads from
    # a term to a term, so each term is kept with mTerms null -- never refused as unlabelled.
    cypher = (
        "MATCH (n0:Terms) OPTIONAL MATCH (n0)-[*..2]->(mTerms:Ontology:Terms) "
        "RETURN n0.name, mTerms.name"
    )
    assert _pairs(con, cypher, lm()) == {("t1", None), ("t2", None), ("t3", None)}


def test_a_walk_from_a_labelled_end_reaches_a_free_one(con):
    # REQ-348: t3 narrows c2 (one hop), whose parent is c1 (two); t3's own edge is not taken back.
    cypher = "MATCH (n0:Terms {name: 't3'})-[*..2]-(m) RETURN m.name"
    assert _pairs(con, cypher, _linked_terms()) == {("c2",), ("c1",)}
    lengths = "MATCH p = (n0:Terms {name: 't3'})-[*..2]-(m) RETURN m.name, length(p)"
    assert _pairs(con, lengths, _linked_terms()) == {("c2", 1), ("c1", 2)}


def test_a_walk_written_from_its_free_end_is_walked_from_the_other(con):
    # Into c1: t1 and t2 narrow it and c2 is its child (one hop); t3 narrows c2 (two hops).
    cypher = "MATCH (m)-[*..2]->(c:Concepts {name: 'c1'}) RETURN m.name"
    assert _pairs(con, cypher, _linked_terms()) == {("t1",), ("t2",), ("c2",), ("t3",)}


def test_a_free_end_bound_by_an_earlier_clause_ties_the_walk_to_it(con):
    cypher = "MATCH (m:Concepts {name: 'c1'}) MATCH (n0:Terms)-[*..2]->(m) RETURN n0.name"
    assert _pairs(con, cypher, _linked_terms()) == {("t1",), ("t2",), ("t3",)}


def test_a_path_search_with_both_ends_free_is_refused_by_name():
    with pytest.raises(CypherTranslateError, match=UNLABELLED_ENDS):
        _sql("MATCH (a)-[*..2]->(b) RETURN a, b", _linked_terms())
