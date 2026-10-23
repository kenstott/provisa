# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A map's key is read with ``.key`` as a node's property is (#287, REQ-345): a map literal's key
is its entry's own value, a key it has not is null, and a map a WITH carries is read after it
-- in a WHERE, a RETURN, and after a later WITH carries it on -- with each value as it is, not
its JSON-encoded copy in the map. Each case runs on DuckDB."""

# Requirements: REQ-345

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
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
        sql_ast, _, _ = cypher_to_sql(parse_cypher(cypher), lm, {})
        return sorted(con.execute(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall())

    return _rows


def test_a_map_literal_s_key_is_its_value(rows):
    assert rows("MATCH (t:GlossaryTerm) WHERE t.name = 'ab' RETURN {a: 1, b: t.name}.a AS v") == [
        (1,)
    ]
    assert rows("MATCH (t:GlossaryTerm) WHERE t.name = 'ab' RETURN {a: 1}.b AS v") == [(None,)]


def test_a_carried_map_s_key_is_its_value_in_a_where_and_a_return(rows):
    assert rows(
        "MATCH (t:GlossaryTerm) WITH {a: t.name, b: t.id} AS m WHERE m.b = 2 "
        "RETURN m.a AS v, m.b AS w"
    ) == [("cd", 2)]
    assert rows("MATCH (t:GlossaryTerm) WITH {a: t.name} AS m RETURN m.z AS v") == [
        (None,),
        (None,),
    ]


def test_a_carried_map_is_read_after_a_later_with_carries_it_on(rows):
    assert rows(
        "MATCH (t:GlossaryTerm) WHERE t.name = 'ab' WITH {a: t.name} AS m "
        "MATCH (u:GlossaryTerm) WITH m, u RETURN m.a AS v, u.name AS w"
    ) == [("ab", "ab"), ("ab", "cd")]


@pytest.fixture
def returned():
    """Each RETURN column's name and value, as a Cypher read answers them (graph rewrites
    included)."""
    from provisa.gql.graph_rewriter import apply_graph_rewrites

    lm = CypherLabelMap(nodes={"GlossaryTerm": _term_node()}, relationships={})
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    con.execute("CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO provisa_admin.public.glossary_terms VALUES (1, 'ab')")

    def _returned(cypher: str) -> list[tuple[str, object]]:
        sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
        sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")
        cur = con.execute(transpile_to_duckdb(sql))
        (row,) = cur.fetchall()
        return list(zip((d[0] for d in cur.description), row, strict=True))

    return _returned


_NODE = '{"id":"GlossaryTerm|1","label":"GlossaryTerm","tableLabel":"GlossaryTerm","name":"ab"}'


@pytest.mark.parametrize(
    "cypher, expected",
    [
        # Only the node: not the map, nor the columns its keys are carried in.
        ("MATCH (t:GlossaryTerm) WITH {a: t.name} AS m, t RETURN t", [("t", _NODE)]),
        ("MATCH (t:GlossaryTerm) WITH {a: t.name} AS m, t RETURN t AS x", [("x", _NODE)]),
        # A node a WITH carries beside a value is the node, and the value is its own column.
        ("MATCH (t:GlossaryTerm) WITH t.name AS k, t RETURN t, k", [("t", _NODE), ("k", "ab")]),
        ("MATCH (t:GlossaryTerm) WITH t RETURN t", [("t", _NODE)]),
    ],
)
def test_a_carried_variable_returns_only_itself(returned, cypher, expected):
    assert returned(cypher) == expected
