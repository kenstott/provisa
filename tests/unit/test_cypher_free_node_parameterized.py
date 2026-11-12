# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3e91b2-4c6a-4f0e-a8d5-2b19c07e6f43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
# ruff: noqa: F811 -- a fixture imported here is redefined by each test that takes it

"""A free node never reads a parameterized table; a label naming one reads it (REQ-2135).

A parameterized table is a function of its arguments (REQ-1986), with no rows of its own an open
pattern could find. Every expansion of a node no label names goes through one rule
(``CypherLabelMap.end_reads``): a bare node, a hop of a type or of none with a free end on either
side or both, undirected, variable-length, optional, in a later pattern, and the explorer's chips.
The maintainer's case was the relationship-type chip on a type the demo's ``assignments_by_breed``
carries beside plain tables: ``MATCH ()-[r:IS_EMPLOYEE]->() RETURN r LIMIT 25`` was refused for
the parameter no one asked to give."""

# Requirements: REQ-2135, REQ-1986

import dataclasses
from unittest.mock import AsyncMock, patch

import duckdb
import pytest

from provisa.core.parameters import ParameterRequired
from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_cypher_find_paths_by_mapping import _node as _bare_node
from tests.unit.test_cypher_find_paths_by_mapping import _rel
from tests.unit.test_table_parameters_required import _STEPS, _openapi_state, _param, _read

#: The parameterized table's name: the REQ-1986 fixture's OpenAPI table, so a read of it is
#: refused there exactly as the pipeline refuses one.
PARAMETERIZED = "orders"


def _node(label: str, table: str):
    """A node whose properties are read by their columns, as a union of node types reads them."""
    return dataclasses.replace(
        _bare_node(label, table), physical_properties={"id": "id", "name": "name"}
    )


@pytest.fixture
def model():
    """T (IS_EMPLOYEE's shape) from P, a parameterized table, and from A, a plain one, each to E;
    P's mapping is the type's first, so a free end inferred from the type's first mapping lands
    on P. U is a type only P carries. Each table has a row a read of P would add."""
    p = dataclasses.replace(
        _node("P", PARAMETERIZED), native_filter_columns={"_nf_region": "varchar"}
    )
    t_p = _rel("P", "E", "e_id", "id")
    t_a = _rel("A", "E", "e_id", "id")
    u_p = dataclasses.replace(_rel("P", "A", "a_id", "id"), rel_type="U")
    lm = CypherLabelMap(
        nodes={"P": p, "A": _node("A", "a"), "E": _node("E", "e")},
        relationships={"T::P→E": t_p, "T::A→E": t_a, "U::P→A": u_p},
    )
    lm.aliases = {"T": [t_p, t_a], "U": [u_p]}
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS cat")
    con.execute("CREATE SCHEMA cat.public")
    con.execute("CREATE TABLE cat.public.a (id INTEGER, name VARCHAR, e_id INTEGER)")
    con.execute("CREATE TABLE cat.public.e (id INTEGER, name VARCHAR)")
    con.execute(
        f"CREATE TABLE cat.public.{PARAMETERIZED} "
        "(id INTEGER, name VARCHAR, e_id INTEGER, a_id INTEGER)"
    )
    con.execute("INSERT INTO cat.public.a VALUES (1, 'a1', 10)")
    con.execute("INSERT INTO cat.public.e VALUES (10, 'e10'), (11, 'e11')")
    con.execute(f"INSERT INTO cat.public.{PARAMETERIZED} VALUES (7, 'p7', 11, 1)")
    return lm, con


def _sql(model, cypher: str) -> str:
    lm, _ = model
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    return apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")


def _rows(model, cypher: str) -> list[tuple]:
    _, con = model
    return con.execute(transpile_to_duckdb(_sql(model, cypher))).fetchall()


def _reads_parameterized(sql: str) -> bool:
    return f'"{PARAMETERIZED}"' in sql


# Each shape a free node is expanded in, and the rows it answers from the plain tables alone.
_FREE = {
    "bare-node": ("MATCH (n) RETURN n.name ORDER BY n.name", [("a1",), ("e10",), ("e11",)]),
    "bare-node-where": (
        "MATCH (n) WHERE n.name IS NOT NULL RETURN n.name ORDER BY n.name",
        [("a1",), ("e10",), ("e11",)],
    ),
    "type-chip": ("MATCH ()-[r:T]->() RETURN r LIMIT 25", 1),
    "typed-both-free-backward": ("MATCH ()<-[r:T]-() RETURN r LIMIT 25", 1),
    "typed-both-free-named": ("MATCH (x)-[r:T]->(y) RETURN x.name, y.name", [("a1", "e10")]),
    "typed-free-left": ("MATCH (x)-[r:T]->(y:E) RETURN x.name, y.name", [("a1", "e10")]),
    "typed-free-right": ("MATCH (y:E)<-[r:T]-(x) RETURN x.name, y.name", [("a1", "e10")]),
    "typed-undirected": ("MATCH (x)-[r:T]-(y) RETURN x.name, y.name ORDER BY x.name", 2),
    "untyped-both-free": ("MATCH ()-[r]->() RETURN r LIMIT 25", 1),
    "untyped-free-left": ("MATCH (x)-[r]->(y:E) RETURN x.name, y.name", [("a1", "e10")]),
    "untyped-free-right": ("MATCH (x:A)-[r]-(y) RETURN x.name, y.name", [("a1", "e10")]),
    "untyped-undirected": ("MATCH (x)-[r]-(y) RETURN x.name, y.name", 2),
    "variable-length": ("MATCH (x:A)-[*1..4]-(y) RETURN x.name, y.name", [("a1", "e10")]),
    "variable-length-typed": ("MATCH (x)-[:T*1..3]-(y:E) RETURN x.name, y.name", [("a1", "e10")]),
    "optional-match": ("OPTIONAL MATCH (x)-[r:T]->(y) RETURN x.name, y.name", [("a1", "e10")]),
    "optional-match-after-label": (
        "MATCH (y:E) OPTIONAL MATCH (x)-[r:T]->(y) RETURN y.name, x.name ORDER BY y.name",
        [("e10", "a1"), ("e11", None)],
    ),
    "later-pattern-free-left": (
        "MATCH (e:E {id: 10}) MATCH (x)-[r:T]->(e) RETURN e.name, x.name",
        [("e10", "a1")],
    ),
    "later-pattern-free-right": (
        "MATCH (e:E {id: 10}) MATCH (e)<-[r:T]-(x) RETURN e.name, x.name",
        [("e10", "a1")],
    ),
    "later-pattern-node": (
        "MATCH (e:E {id: 10}) MATCH (x) RETURN x.name ORDER BY x.name",
        [("a1",), ("e10",), ("e11",)],
    ),
    "property-key-chip": (
        'MATCH (n)\nWHERE n.name IS NOT NULL\nRETURN DISTINCT "node" AS entity, n.name AS name\n'
        "LIMIT 25\nUNION ALL\nMATCH ()-[r]-()\nWHERE r.name IS NOT NULL\n"
        'RETURN DISTINCT "relationship" AS entity, r.name AS name\nLIMIT 25',
        3,
    ),
}


@pytest.mark.parametrize("cypher, answer", _FREE.values(), ids=_FREE.keys())
def test_a_free_node_never_reads_a_parameterized_table(model, cypher, answer):
    assert not _reads_parameterized(_sql(model, cypher))
    rows = _rows(model, cypher)
    assert (len(rows) if isinstance(answer, int) else rows) == answer


# REQ-2135, PATHS SKIP PARAMETERIZED TABLES: a type only parameterized tables carry joins nothing
# a free end may read, so a hop of it between ends no label names to one is no path: no rows.
_ONLY_PARAMETERIZED = {
    "both-free": "MATCH ()-[r:U]->() RETURN r LIMIT 25",
    "free-left": "MATCH (x)-[r:U]->(y:A) RETURN x.name",
    "free-right": "MATCH (y:A)<-[r:U]-(x) RETURN x.name",
    "undirected": "MATCH (x)-[r:U]-(y) RETURN x.name",
    "optional": "MATCH (y:A) OPTIONAL MATCH (x)-[r:U]->(y) RETURN y.name, x.name",
}


@pytest.mark.parametrize("cypher", _ONLY_PARAMETERIZED.values(), ids=_ONLY_PARAMETERIZED.keys())
def test_a_type_only_parameterized_tables_carry_answers_nothing_between_free_ends(model, cypher):
    assert not _reads_parameterized(_sql(model, cypher))
    rows = _rows(model, cypher)
    assert rows in ([], [("a1", None)])


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (x:P) RETURN x.name",
        "MATCH (x:P)-[r:T]->(y) RETURN y.name",
        "MATCH (x:P)-[r:U]->(y) RETURN y.name",
        "MATCH (y)<-[r:T]-(x:P) RETURN y.name",
    ],
    ids=["node", "typed-hop", "type-only-it-carries", "typed-hop-backward"],
)
def test_a_label_naming_a_parameterized_table_reads_it(model, cypher):
    assert _reads_parameterized(_sql(model, cypher))


@pytest.mark.parametrize(
    "cypher", ["MATCH (x:P)-[r:T]->(y) RETURN y.name", "MATCH (x:P) RETURN x.name"]
)
async def test_a_label_naming_a_parameterized_table_still_requires_its_parameters(model, cypher):
    state = _openapi_state(_param("region", "query_param", True))
    with patch(_STEPS["openapi"], new=AsyncMock()) as step:
        with pytest.raises(
            ParameterRequired, match=r"'orders' requires the parameter\(s\) \['region'\]"
        ):
            await _read(state, _sql(model, cypher))
    step.assert_not_awaited()


async def test_the_type_chip_sends_no_parameterized_table_to_be_refused(model):
    state = _openapi_state(_param("region", "query_param", True))
    with patch(_STEPS["openapi"], new=AsyncMock()) as step:
        await _read(state, _sql(model, "MATCH ()-[r:T]->() RETURN r LIMIT 25"))
    step.assert_not_awaited()
