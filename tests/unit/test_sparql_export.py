# Copyright (c) 2026 Kenneth Stott
# Canary: e70ab600-03a8-45fe-8c42-d7936b1c364f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph read exported as SPARQL is exact or refused by name (REQ-2136): each statement and its
SPARQL are both answered on DuckDB over the same model -- the SPARQL read back by the SPARQL front
end -- and give the same rows, in the same order where the statement orders them."""

# Requirements: REQ-2136, REQ-2135

import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.export import SparqlExportRefused, ir_to_sparql
from provisa.sparql_query.vocabulary import build_vocabulary
from tests.unit import test_sparql_compiler as _sparql_tests
from tests.unit.test_sparql_compiler import (  # noqa: F401 -- fixtures
    BASE,
    _GROUNDING,
    _TYPES,
    _answer,
    _model,
    public_url,
)

con = _sparql_tests.con  # the model's DuckDB fixture


def _cypher_rows(con, cypher: str) -> list[tuple]:
    lm = _model()
    sql, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    return [tuple(r) for r in con.execute(sql.sql(dialect="duckdb")).fetchall()]


def _export(cypher: str) -> str:
    lm = _model()
    return ir_to_sparql(parse_cypher(cypher), build_vocabulary(lm, BASE, _TYPES, _GROUNDING), lm)


def _sparql_rows(con, sparql: str) -> list[tuple]:
    # The export carries its own PREFIX lines, after the test's, which it never uses.
    return [tuple(r.values()) for r in _answer(con, sparql)]


@pytest.mark.parametrize(
    "cypher, ordered",
    [
        ("MATCH (p:Person) RETURN p.name, p.age", False),
        ("MATCH (p:Person) WHERE p.age > 30 RETURN p.name", False),
        ("MATCH (p:Person) WHERE p.age IS NULL RETURN p.name", False),
        ("MATCH (p:Person) WHERE p.name IN ['Ann', 'Di'] OR p.age < 30 RETURN p.name", False),
        ("MATCH (p:Person) WHERE p.name STARTS WITH 'B' RETURN p.name", False),
        ("MATCH (p:Person) WHERE NOT p.age >= 35 RETURN p.name", False),
        ("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age", True),
        ("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC", True),
        ("MATCH (p:Person) RETURN p.name ORDER BY p.name SKIP 1 LIMIT 2", True),
        ("MATCH (p:Person) RETURN DISTINCT p.age", False),
        ("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name", False),
        ("MATCH (c:Company)<-[:WORKS_AT]-(p:Person) RETURN c.name, p.name", False),
        (
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name",
            False,
        ),
        (
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company) WHERE c.name = 'Acme' "
            "RETURN p.name, c.name",
            False,
        ),
        (
            "MATCH (p:Person) WHERE p.age > 30 OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) "
            "RETURN p.name, q.name, q.age",
            False,
        ),
        ("MATCH (p:Person) RETURN count(*) AS n", False),
        ("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name, count(*) AS n", False),
        ("MATCH (p:Person) RETURN min(p.age) AS lo, max(p.age) AS hi, count(p.age) AS n", False),
    ],
)
def test_the_export_answers_what_the_statement_does(con, cypher, ordered):
    sparql = _export(cypher)
    expected, got = _cypher_rows(con, cypher), _sparql_rows(con, sparql)
    if not ordered:
        expected, got = sorted(expected, key=repr), sorted(got, key=repr)
    assert expected and got == expected, sparql


def test_a_property_is_bound_where_its_node_is_and_returned_by_its_name():
    sparql = _export(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name"
    )
    assert "SELECT ?name ?name_2" in sparql
    assert "OPTIONAL { ?c companies:name ?name_2 }" in sparql  # inside c's OPTIONAL
    assert sparql.startswith("PREFIX ")


@pytest.mark.parametrize(
    "cypher, why",
    [
        ("MATCH (p:Person) RETURN p", "A whole node"),
        ("MATCH (p:Person) RETURN {name: p.name} AS m", "map"),
        ("MATCH (p:Person) RETURN collect(p.name) AS names", r"collect\(\)"),
        ("MATCH (p:Person) RETURN sum(p.age) AS s", r"sum\(\)"),
        ("MATCH (p:Person) WHERE p.age + 1 > 30 RETURN p.name", "The operator +"),
        ("MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p.name", "=~"),
        ("MATCH (p:Person)-[:KNOWS*1..2]->(q:Person) RETURN q.name", "variable-length"),
        ("MATCH (p:Person)-[:KNOWS]-(q:Person) RETURN q.name", "undirected"),
        ("MATCH (p:Person) WITH p RETURN p.name", "WITH"),
        (
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company) WHERE c.name IS NULL "
            "RETURN p.name",
            "IS NULL in an OPTIONAL MATCH",
        ),
        ("MATCH (p:Person) RETURN p.name UNION MATCH (c:Company) RETURN c.name", "UNION"),
    ],
)
def test_what_has_no_exact_sparql_form_is_refused_by_name(cypher, why):
    with pytest.raises(SparqlExportRefused, match=why):
        _export(cypher)
