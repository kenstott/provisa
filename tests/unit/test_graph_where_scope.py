# Copyright (c) 2026 Kenneth Stott
# Canary: 7863691b-7e2b-4108-96a6-5ede17b348ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a condition holds, in Cypher and in SPARQL (REQ-345, REQ-2135), each answered on DuckDB.
Cypher: a WHERE is its own MATCH's, and an OPTIONAL MATCH's WHERE is that match's condition,
never a filter of the rows. SPARQL: a query's FILTER holds of its solutions, its OPTIONALs'
bindings among them; an OPTIONAL nested in an OPTIONAL extends it; a FILTER in an OPTIONAL is its
condition; a triple in an OPTIONAL holds only of a value."""

# Requirements: REQ-345, REQ-2135

import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.compiler import SparqlRefused
from tests.unit import test_sparql_compiler as _sparql_tests
from tests.unit.test_sparql_compiler import (  # noqa: F401 -- fixtures
    COMPANY,
    PERSON,
    _answer,
    _model,
    public_url,
)

con = _sparql_tests.con  # the model's DuckDB fixture


def _cypher(con, cypher: str) -> list[tuple]:
    lm = _model()
    sql, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    return sorted(con.execute(sql.sql(dialect="duckdb")).fetchall(), key=repr)


def _sparql(con, query: str) -> list[tuple]:
    return sorted((tuple(r.values()) for r in _answer(con, query)), key=repr)


def test_a_where_is_its_own_matchs_and_a_later_optional_where_keeps_it(con):
    # (a) Ann (41) and Bo (35) only: Cy (29) and Di (no age) fail the first MATCH's WHERE.
    assert _cypher(
        con,
        "MATCH (s:Person) WHERE s.age > 30 OPTIONAL MATCH (s)-[:WORKS_AT]->(c:Company) "
        "WHERE c.name = 'Beta' RETURN s.name, c.name",
    ) == [("Ann", None), ("Bo", None)]


def test_an_optional_matchs_where_conditions_the_match_never_the_rows(con):
    # (b) Every person is kept; only those over 30 have their company.
    assert _cypher(
        con,
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company) WHERE n.age > 30 "
        "RETURN n.name, c.name",
    ) == [("Ann", "Acme"), ("Bo", "Acme"), ("Cy", None), ("Di", None)]


def test_a_filter_holds_of_what_the_querys_optionals_bind(con):
    # (c) Di alone has no age.
    rows = _sparql(
        con,
        f"SELECT ?n ?a WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s p:age ?a }} "
        "FILTER(!BOUND(?a)) }",
    )
    assert rows == [("Di", None)]


def test_an_optional_nested_in_an_optional_extends_it(con):
    # (d) Each person's company's name, through the inner OPTIONAL.
    rows = _sparql(
        con,
        f"SELECT ?n ?cn WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:WORKS_AT ?c . "
        f"?c a <{COMPANY}> OPTIONAL {{ ?c c:name ?cn }} }} }}",
    )
    assert rows == [("Ann", "Acme"), ("Bo", "Acme"), ("Cy", "Beta"), ("Di", None)]


def test_a_filter_naming_what_an_inner_optional_binds_is_refused():
    with pytest.raises(SparqlRefused, match="an OPTIONAL inside it binds"):
        _answer(
            None,
            f"SELECT ?n WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:WORKS_AT ?c . "
            f"?c a <{COMPANY}> OPTIONAL {{ ?c c:name ?cn }} FILTER(?cn = 'Acme') }} }}",
        )


def test_a_filter_in_an_optional_is_its_condition(con):
    # (e) Cy works at Beta: the OPTIONAL does not hold of him, and he is kept unbound.
    rows = _sparql(
        con,
        f"SELECT ?n ?cn WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:WORKS_AT ?c . "
        f'?c a <{COMPANY}> ; c:name ?cn FILTER(?cn = "Acme") }} }}',
    )
    assert rows == [("Ann", "Acme"), ("Bo", "Acme"), ("Cy", None), ("Di", None)]


def test_a_triple_in_an_optional_holds_only_of_a_value(con):
    # (f) Eve knows Di, who has no age: the OPTIONAL does not hold, so Di is not bound for her.
    con.execute("INSERT INTO pg.public.persons VALUES (5, 'Eve', 50, 4, NULL)")
    rows = _sparql(
        con,
        f"SELECT ?n ?qn WHERE {{ ?s a <{PERSON}> ; p:name ?n . OPTIONAL {{ ?s rel:KNOWS ?q . "
        f"?q a <{PERSON}> ; p:name ?qn ; p:age ?qa }} }}",
    )
    assert ("Eve", None) in rows
    assert ("Ann", "Bo") in rows
