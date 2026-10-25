# Copyright (c) 2026 Kenneth Stott
# Canary: 3d650b04-f2be-46c3-bdb3-7610e2d08a27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship whose far end the statement already holds (#303): the graph back end reads it as
a condition between the two rows held, never as a second copy of the far end's table -- which
shadowed the one held (a wrong answer inside EXISTS) or named one alias twice (refused). Each
statement is run on DuckDB and checked by its answer against the equivalent SQL."""

# Requirements: REQ-2135

import dataclasses

import duckdb
import pytest
import sqlglot.expressions as exp

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap, JunctionMapping
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_cypher_min_hops import _node, _rel
from tests.unit.test_sparql_compiler import BASE, _answer, _model


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """Ann (41) and Bo (35) work at Acme, Cy (29) at Beta, Di (no age) nowhere; Ann knows Bo, Bo
    knows Cy, Cy knows Ann. Ann and Cy belong to the club Chess, Bo to Go."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.public")
    c.execute(
        "CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR, age INTEGER, knows_id INTEGER, "
        "company_id INTEGER)"
    )
    c.execute(
        "INSERT INTO pg.public.persons VALUES (1, 'Ann', 41, 2, 1), (2, 'Bo', 35, 3, 1), "
        "(3, 'Cy', 29, 1, 2), (4, 'Di', NULL, NULL, NULL)"
    )
    c.execute("CREATE TABLE pg.public.companies (id INTEGER, name VARCHAR, parent_id INTEGER)")
    c.execute(
        "INSERT INTO pg.public.companies VALUES (1, 'Acme', 2), (2, 'Beta', 3), (3, 'Corp', NULL)"
    )
    c.execute("CREATE TABLE pg.public.clubs (id INTEGER, name VARCHAR)")
    c.execute("INSERT INTO pg.public.clubs VALUES (1, 'Chess'), (2, 'Go')")
    c.execute("CREATE TABLE pg.public.members (person_id INTEGER, club_id INTEGER)")
    c.execute("INSERT INTO pg.public.members VALUES (1, 1), (3, 1), (2, 2)")
    return c


def _label_map() -> CypherLabelMap:
    lm = _model()
    club = dataclasses.replace(_node("Club", "clubs"), properties={"id": "id", "name": "name"})
    member_of = dataclasses.replace(
        _rel("MEMBER_OF", "Person", "Club", "id"),
        via=JunctionMapping(
            catalog_name="pg",
            schema_name="public",
            table_name="members",
            type_name="Members",
            source_columns=("person_id",),
            target_columns=("club_id",),
        ),
    )
    return CypherLabelMap(
        nodes={**lm.nodes, "Club": club},
        relationships={**lm.relationships, "MEMBER_OF": member_of},
    )


def _rows(con, cypher: str) -> list[tuple]:
    sql, _, _ = cypher_to_sql(parse_cypher(cypher), _label_map(), {})
    return sorted(con.execute(transpile_to_duckdb(sql.sql(dialect="postgres"))).fetchall(), key=str)


def _sql(con, sql: str) -> list[tuple]:
    return sorted(con.execute(sql).fetchall(), key=str)


PAIRS = (
    "SELECT p.name, c.name FROM pg.public.persons p JOIN pg.public.companies c "
    "ON p.company_id = c.id"
)


def test_a_match_between_two_held_nodes_is_their_condition(con):
    rows = _rows(
        con,
        "MATCH (s:Person) MATCH (c:Company) MATCH (s)-[:WORKS_AT]->(c) RETURN s.name, c.name",
    )
    assert rows == _sql(con, PAIRS)


def test_exists_over_two_held_nodes_reads_the_held_far_end(con):
    rows = _rows(
        con,
        "MATCH (s:Person) MATCH (c:Company) WHERE EXISTS { MATCH (s)-[:WORKS_AT]->(c) } "
        "RETURN s.name, c.name",
    )
    want = _sql(con, PAIRS)
    assert rows == want and len(want) == 3


def test_a_match_written_from_its_new_end_to_a_held_one(con):
    rows = _rows(
        con,
        "MATCH (c:Company {name: 'Beta'}) MATCH (s:Person)-[:WORKS_AT]->(c) RETURN s.name, c.name",
    )
    assert rows == [("Cy", "Beta")]


def test_a_cycle_returns_to_the_node_it_started_from(con):
    rows = _rows(
        con,
        "MATCH (s:Person)-[:KNOWS]->(t:Person)-[:KNOWS]->(u:Person)-[:KNOWS]->(s) RETURN s.name",
    )
    assert rows == _sql(
        con,
        "SELECT a.name FROM pg.public.persons a JOIN pg.public.persons b ON a.knows_id = b.id "
        "JOIN pg.public.persons c ON b.knows_id = c.id WHERE c.knows_id = a.id",
    )


def test_an_optional_match_between_two_held_nodes_keeps_every_row(con):
    rows = _rows(
        con,
        "MATCH (s:Person) MATCH (c:Company) OPTIONAL MATCH (s)-[r:WORKS_AT]->(c) "
        "RETURN s.name, c.name, r",
    )
    every = _sql(
        con, "SELECT p.name, c.name FROM pg.public.persons p CROSS JOIN pg.public.companies c"
    )
    assert [(n, c) for n, c, _ in rows] == every
    assert sorted(((n, c) for n, c, r in rows if r is not None), key=str) == _sql(con, PAIRS)


def test_a_junction_relationship_between_two_held_nodes(con):
    rows = _rows(
        con, "MATCH (s:Person) MATCH (k:Club) MATCH (s)-[:MEMBER_OF]->(k) RETURN s.name, k.name"
    )
    assert rows == _sql(
        con,
        "SELECT p.name, k.name FROM pg.public.persons p JOIN pg.public.members m "
        "ON m.person_id = p.id JOIN pg.public.clubs k ON k.id = m.club_id",
    )


def test_exists_over_a_junction_between_two_held_nodes(con):
    rows = _rows(
        con,
        "MATCH (s:Person) MATCH (k:Club) WHERE NOT EXISTS { MATCH (s)-[:MEMBER_OF]->(k) } "
        "RETURN s.name, k.name",
    )
    assert rows == _sql(
        con,
        "SELECT p.name, k.name FROM pg.public.persons p CROSS JOIN pg.public.clubs k WHERE NOT "
        "EXISTS (SELECT 1 FROM pg.public.members m WHERE m.person_id = p.id AND m.club_id = k.id)",
    )


def test_sparql_an_optional_edge_between_two_matched_subjects(con):
    rows = _answer(
        con,
        "SELECT ?n ?cn ?a WHERE { ?s p:name ?n . ?c c:name ?cn "
        "OPTIONAL { ?s rel:WORKS_AT ?c ; p:age ?a } }",
    )
    want = _sql(
        con,
        "SELECT p.name, c.name, CASE WHEN p.company_id = c.id THEN p.age END "
        "FROM pg.public.persons p CROSS JOIN pg.public.companies c",
    )
    assert sorted(((r["n"], r["cn"], r["a"]) for r in rows), key=str) == want


# A node or relationship variable inside an expression (#304): null by its own row, not a column
# of its name.


def test_an_optional_relationship_is_null_where_it_matched_none(con):
    rows = _rows(
        con,
        "MATCH (a:Person) OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) RETURN a.name, r IS NULL",
    )
    assert rows == _sql(con, "SELECT name, knows_id IS NULL FROM pg.public.persons")


def test_an_optional_node_is_null_where_it_matched_none(con):
    rows = _rows(
        con,
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:WORKS_AT]->(c:Company) RETURN a.name, c IS NULL",
    )
    assert rows == _sql(con, "SELECT name, company_id IS NULL FROM pg.public.persons")


def test_a_required_relationship_is_never_null(con):
    rows = _rows(con, "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, r IS NOT NULL")
    assert rows == _sql(con, "SELECT name, true FROM pg.public.persons WHERE knows_id IS NOT NULL")


def test_an_optional_junction_relationship_is_null_where_it_matched_none(con):
    rows = _rows(
        con,
        "MATCH (a:Person) OPTIONAL MATCH (a)-[m:MEMBER_OF]->(k:Club) RETURN a.name, m IS NULL",
    )
    assert rows == _sql(
        con,
        "SELECT p.name, m.club_id IS NULL FROM pg.public.persons p "
        "LEFT JOIN pg.public.members m ON m.person_id = p.id",
    )


def test_an_optional_relationship_between_held_nodes_is_null_where_it_does_not_hold(con):
    rows = _rows(
        con,
        "MATCH (s:Person) MATCH (c:Company) OPTIONAL MATCH (s)-[r:WORKS_AT]->(c) "
        "WHERE r IS NOT NULL RETURN s.name, c.name",
    )
    assert rows == _sql(con, PAIRS)


# Two node or relationship variables compared (#305): by identity -- one table's keys, one
# relationship's type and ends -- never by the rows' contents.


def _compares_aliases(cypher: str) -> bool:
    """Whether the statement compares two bare table aliases (rows, not elements): DuckDB reads
    each as its row's contents, and Trino refuses it."""
    sql, _, _ = cypher_to_sql(parse_cypher(cypher), _label_map(), {})
    return any(
        isinstance(side, exp.Column) and not side.table and side.name in ("a", "c", "r", "s", "x")
        for node in sql.find_all(exp.EQ, exp.NEQ)
        for side in (node.this, node.expression)
    )


def test_two_nodes_are_equal_where_they_are_one_row(con):
    con.execute("INSERT INTO pg.public.persons VALUES (5, 'Ann', 41, 2, 1)")  # Ann's twin
    query = "MATCH (a:Person) MATCH (c:Person) WHERE a = c RETURN a.name, c.name"
    assert _rows(con, query) == _sql(con, "SELECT name, name FROM pg.public.persons")
    assert not _compares_aliases(query)


def test_two_nodes_differ_where_they_are_two_rows(con):
    con.execute("INSERT INTO pg.public.persons VALUES (5, 'Ann', 41, 2, 1)")
    query = "MATCH (a:Person) MATCH (c:Person) WHERE a <> c RETURN a.name, c.name"
    assert not _compares_aliases(query)
    assert _rows(con, query) == _sql(
        con,
        "SELECT a.name, c.name FROM pg.public.persons a JOIN pg.public.persons c ON a.id <> c.id",
    )


def test_nodes_of_two_tables_are_never_equal(con):
    query = "MATCH (a:Person) MATCH (c:Company) RETURN a.name, c.name, a = c"
    assert not _compares_aliases(query)
    rows = _rows(con, query)
    assert {r[2] for r in rows} == {False} and len(rows) == 12


def test_an_unmatched_optional_node_compares_as_null(con):
    rows = _rows(
        con,
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:WORKS_AT]->(c:Company) MATCH (x:Company) "
        "RETURN a.name, x.name, c = x",
    )
    assert rows == _sql(
        con,
        "SELECT p.name, x.name, x.id = p.company_id FROM pg.public.persons p "
        "CROSS JOIN pg.public.companies x",
    )


def test_two_relationships_are_equal_where_they_are_one(con):
    rows = _rows(
        con,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) MATCH (a)-[s:KNOWS]->(c:Person) "
        "RETURN a.name, r = s, r <> s",
    )
    assert rows == _sql(
        con, "SELECT name, true, false FROM pg.public.persons WHERE knows_id IS NOT NULL"
    )


def test_two_relationships_of_two_types_are_never_equal(con):
    rows = _rows(
        con,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) MATCH (a)-[s:WORKS_AT]->(c:Company) "
        "RETURN a.name, r = s",
    )
    assert {r[1] for r in rows} == {False} and rows


def test_nodes_are_not_ordered(con):
    from provisa.gql.translator_types import CypherTranslateError

    with pytest.raises(CypherTranslateError, match="compared by = and <>"):
        _rows(con, "MATCH (a:Person) MATCH (c:Person) WHERE a < c RETURN a.name")
