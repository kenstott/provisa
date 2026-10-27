# Copyright (c) 2026 Kenneth Stott
# Canary: 7863691b-7e2b-4108-96a6-5ede17b348ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a Cypher condition holds (REQ-345), answered on DuckDB: a WHERE is its own MATCH's, and an
OPTIONAL MATCH's WHERE is that match's condition, never a filter of the rows."""

# Requirements: REQ-345

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from tests.unit import test_sparql_compiler as _sparql_tests
from tests.unit.test_sparql_compiler import _model, public_url  # noqa: F401 -- fixtures

con = _sparql_tests.con  # the model's DuckDB fixture


def _cypher(con, cypher: str) -> list[tuple]:
    lm = _model()
    sql, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    return sorted(con.execute(sql.sql(dialect="duckdb")).fetchall(), key=repr)


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


def test_a_where_after_a_match_filters_the_rows_whatever_it_names(con):
    # The WHERE is the second MATCH's: it filters the rows, though it names the OPTIONAL MATCH's
    # c. Ann and Bo work at Acme, and Cy, at Beta, knows no one: no row is left.
    assert (
        _cypher(
            con,
            "MATCH (n:Person) OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company) "
            "MATCH (n)-[:KNOWS]->(m:Person) WHERE c.name = 'Beta' RETURN n.name, m.name",
        )
        == []
    )
