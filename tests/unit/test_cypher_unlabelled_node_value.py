# Copyright (c) 2026 Kenneth Stott
# Canary: c6f94520-ab76-41a2-9b1a-86008b3bfb80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An unlabelled node returned bare is one JSON value per type (REQ-345, REQ-350): each arm of the
union over its types carries its own type's node as one value, not a column for every property of
every type, NULL in all but its own. The statement no longer grows with every property in the
model, and the nodes answered are the same: id, label and each property present."""

# Requirements: REQ-345, REQ-350

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import Node, assemble_rows
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, NodeMapping
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb


def _node(label: str, table: str, table_id: int, props: list[str]) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=table_id,
        source_id="pg",
        id_column="id",
        pk_columns=["id"],
        catalog_name="db",
        schema_name="s",
        table_name=table,
        properties={p: p for p in props},
        physical_properties={p: p for p in props},
    )


_LM = CypherLabelMap(
    nodes={
        "Pet": _node("Pet", "pets", 1, ["id", "name", "age"]),
        "Shop": _node("Shop", "shops", 2, ["id", "city"]),
    },
    relationships={},
    nodes_by_table={"Pet": ["Pet"], "Shop": ["Shop"]},
)


@pytest.fixture
def answer():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS db")
    con.execute("CREATE SCHEMA db.s")
    con.execute("CREATE TABLE db.s.pets (id INTEGER, name VARCHAR, age INTEGER)")
    con.execute("INSERT INTO db.s.pets VALUES (1, 'rex', 3), (2, NULL, 5)")
    con.execute("CREATE TABLE db.s.shops (id INTEGER, city VARCHAR)")
    con.execute("INSERT INTO db.s.shops VALUES (7, 'oslo')")

    def _answer(cypher: str) -> list[dict]:
        sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), _LM, {})
        sql = apply_graph_rewrites(sql_ast, graph_vars, _LM).sql(dialect="postgres")
        cur = con.execute(transpile_to_duckdb(sql))
        names = [d[0] for d in cur.description]
        return assemble_rows([dict(zip(names, r, strict=True)) for r in cur.fetchall()], graph_vars)

    return _answer


def _arms(cypher: str) -> list[exp.Select]:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(cypher), _LM, {})
    tree = sqlglot.parse_one(sql_ast.sql(dialect="postgres"), read="postgres")
    # Each type's arm: a SELECT of one table (a type no read property names is left out).
    arms = [s for s in tree.find_all(exp.Select) if s is not tree and s.find(exp.Table)]
    assert arms
    return arms


def test_each_arm_carries_its_node_as_one_value():
    for arm in _arms("MATCH (n) RETURN n"):
        assert [e.alias for e in arm.expressions] == ["__label", "__id", "__node"]


def test_the_nodes_answered_are_each_type_s_own(answer):
    rows = answer("MATCH (n) RETURN n")
    assert sorted((r["n"] for r in rows), key=lambda n: n.id) == [
        # Every column a property under its own name, its key column ``id`` included.
        Node(
            id="Pet|1",
            label="Pet",
            table_label="",
            properties={"age": "3", "id": "1", "name": "rex"},
        ),
        Node(id="Pet|2", label="Pet", table_label="", properties={"age": "5", "id": "2"}),
        Node(id="Shop|7", label="Shop", table_label="", properties={"city": "oslo", "id": "7"}),
    ]


def test_a_property_the_statement_reads_is_still_its_own_column():
    # Read by name, the property is a column of every arm, as before.
    for arm in _arms("MATCH (n) WHERE n.city = 'oslo' RETURN n"):
        assert "city" in [e.alias for e in arm.expressions]
        assert "__node" not in [e.alias for e in arm.expressions]
