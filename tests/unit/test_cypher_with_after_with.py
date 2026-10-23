# Copyright (c) 2026 Kenneth Stott
# Canary: 4f27a8b4-f978-4381-8dbc-bb4511f7529d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A WITH right after a WITH goes on from the rows that WITH carries (#288, REQ-345): it
projects, orders, limits, filters and aggregates them as a WITH after a MATCH does. Each case
runs on DuckDB."""

# Requirements: REQ-345

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_cypher_junction_relationships import _term_node


@pytest.fixture
def rows():
    lm = CypherLabelMap(nodes={"GlossaryTerm": _term_node()}, relationships={})
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    con.execute("CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR)")
    con.execute(
        "INSERT INTO provisa_admin.public.glossary_terms VALUES (1, 'ab'), (2, 'cd'), (3, 'ef')"
    )

    def _rows(cypher: str) -> list[tuple]:
        sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
        sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")
        return sorted(con.execute(transpile_to_duckdb(sql)).fetchall())

    return _rows


_TERMS = [("ab",), ("cd",), ("ef",)]


@pytest.mark.parametrize(
    "cypher, expected",
    [
        ("MATCH (t:GlossaryTerm) WITH t.name AS n WITH n RETURN n", _TERMS),
        ("MATCH (t:GlossaryTerm) WITH t WITH t RETURN t.name AS v", _TERMS),
        (
            "MATCH (t:GlossaryTerm) WITH t.name AS n ORDER BY n DESC WITH n LIMIT 2 RETURN n",
            [("cd",), ("ef",)],
        ),
        (
            "MATCH (t:GlossaryTerm) WITH t.name AS n WITH n WHERE n <> 'ab' RETURN n",
            [("cd",), ("ef",)],
        ),
        ("MATCH (t:GlossaryTerm) WITH t.name AS n WITH count(n) AS c RETURN c", [(3,)]),
        (
            "MATCH (t:GlossaryTerm) WITH t.id AS i, t.name AS n WITH n, i * 2 AS d "
            "WITH n, d RETURN n, d",
            [("ab", 2), ("cd", 4), ("ef", 6)],
        ),
        ("MATCH (t:GlossaryTerm) WITH {a: t.name} AS m WITH m RETURN m.a AS v", _TERMS),
    ],
)
def test_a_with_after_a_with_reads_the_rows_it_carries(rows, cypher, expected):
    assert rows(cypher) == expected
