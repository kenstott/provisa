# Copyright (c) 2026 Kenneth Stott
# Canary: 1e16b871-f16b-4907-97af-93fbf0a5d9a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A variable an UNWIND binds is each element of the list (#289, REQ-345): returned bare, in an
expression, grouped, ordered or carried by a WITH, it is the element's value, not the UNNEST's
whole row. Each case runs on DuckDB."""

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
    con.execute("INSERT INTO provisa_admin.public.glossary_terms VALUES (1, 'ab'), (2, 'cd')")

    def _rows(cypher: str) -> list[tuple]:
        sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
        sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")
        return sorted(con.execute(transpile_to_duckdb(sql)).fetchall())

    return _rows


@pytest.mark.parametrize(
    "cypher, expected",
    [
        ("UNWIND [1, 2] AS x RETURN x", [(1,), (2,)]),
        ("UNWIND [1, 2] AS x RETURN x AS y", [(1,), (2,)]),
        ("UNWIND [1, 2] AS x RETURN x + 1 AS y", [(2,), (3,)]),
        ("UNWIND [1, 2, 2] AS x RETURN x, count(*) AS c", [(1, 1), (2, 2)]),
        ("UNWIND [1, 2] AS x WITH x RETURN x", [(1,), (2,)]),
        ("UNWIND [[1, 2], [3]] AS l UNWIND l AS x RETURN x", [(1,), (2,), (3,)]),
        (
            "MATCH (t:GlossaryTerm) WITH collect(t.name) AS ns UNWIND ns AS x RETURN x",
            [("ab",), ("cd",)],
        ),
        (
            "MATCH (t:GlossaryTerm) UNWIND [1] AS x RETURN x, t.name AS n",
            [(1, "ab"), (1, "cd")],
        ),
    ],
)
def test_an_unwind_variable_is_each_element(rows, cypher, expected):
    assert rows(cypher) == expected
