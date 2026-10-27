# Copyright (c) 2026 Kenneth Stott
# Canary: ff304634-5e7f-41e5-bc08-b79112a79ab1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each list function a Cypher read uses answers on the DuckDB engine as Cypher defines it
(REQ-345, REQ-066): an index (0-based, negative from the end), a slice (0-based, the end
excluded), size, head, last, tail, range, the quantifiers (an empty list: any false, all and none
true) and a map's key. The IR names them as Trino does; transpile_to_duckdb gives DuckDB's."""

# Requirements: REQ-345, REQ-066

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_cypher_junction_relationships import _term_node


@pytest.fixture
def run():
    lm = CypherLabelMap(nodes={"GlossaryTerm": _term_node()}, relationships={})
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    con.execute("CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO provisa_admin.public.glossary_terms VALUES (1, 'ab')")

    def _run(expression: str):
        sql_ast, _, _ = cypher_to_sql(
            parse_cypher(f"MATCH (t:GlossaryTerm) RETURN {expression} AS v"), lm, {}
        )
        (row,) = con.execute(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall()
        return row[0]

    return _run


@pytest.mark.parametrize(
    "expression, value",
    [
        ("[t.name, 'x'][0]", "ab"),
        ("[t.name, 'x'][-1]", "x"),
        ("[1, 2, 3][1..2]", [2]),
        ("[1, 2, 3][1..]", [2, 3]),
        ("[1, 2, 3][..2]", [1, 2]),
        ("size([1, 2, 3])", 3),
        ("head([1, 2])", 1),
        ("last([1, 2])", 2),
        ("tail([1, 2, 3])", [2, 3]),
        ("range(1, 3)", [1, 2, 3]),
        ("range(1, 5, 2)", [1, 3, 5]),
        ("any(x IN [1, 2] WHERE x > 1)", True),
        ("any(x IN [] WHERE x > 1)", False),
        ("all(x IN [1, 2] WHERE x > 1)", False),
        ("all(x IN [] WHERE x > 1)", True),
        ("none(x IN [1, 2] WHERE x > 5)", True),
        ("none(x IN [] WHERE x > 5)", True),
        ("single(x IN [1, 2] WHERE x > 1)", True),
        ("[x IN [1, 2, 3] WHERE x > 1]", [2, 3]),
        ("[x IN [1, 2, 3] | x * 2]", [2, 4, 6]),
        ("reduce(s = 0, x IN [1, 2, 3] | s + x)", 6),
        # A map's key read by name is its value, as m.a reads it (openCypher Map2).
        ("{a: 1}['a']", 1),
    ],
)
def test_a_list_function_answers_on_duckdb_as_cypher_defines_it(run, expression, value):
    assert run(expression) == value
