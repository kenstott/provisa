# Copyright (c) 2026 Kenneth Stott
# Canary: dad6998f-5183-4650-ba4d-f3e2590f9eef
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A node matched with no label (``MATCH (n)``) is any table's row: a property every table holding
it holds as one type keeps that type (integers of any width as an integer, integers and floats as
a float), and ``labels(n)`` is the label of the row's own table."""

# Requirements: REQ-345, REQ-572

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, NodeMapping
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb

pytestmark = pytest.mark.unit


def _node(label: str, columns: dict[str, str]) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=hash(label) % 1000,
        source_id="s",
        id_column="id",
        pk_columns=["id"],
        catalog_name="memory",
        schema_name="s",
        table_name=label.lower(),
        properties={c: c for c in columns},
        physical_properties={c: c for c in columns},
        property_types=tuple(columns.items()),
    )


_TABLES = {
    "A": (
        {"id": "integer", "num": "integer", "name": "varchar(20)", "code": "text"},
        "(1, 1, 'a1', 'x'), (2, 10, 'a2', 'y')",
    ),
    "B": ({"id": "integer", "num": "bigint", "name": "text", "code": "integer"}, "(3, 9, 'b1', 7)"),
    "C": ({"id": "integer", "num": "double precision"}, "(4, 2.5)"),
}


def _rows(query: str, labels: tuple[str, ...] = ("A", "B")) -> list[tuple]:
    duck = duckdb.connect()
    duck.execute("CREATE SCHEMA s")
    nodes = {}
    for label in labels:
        columns, values = _TABLES[label]
        ddl = ", ".join(f"{c} {t}" for c, t in columns.items())
        duck.execute(f"CREATE TABLE s.{label.lower()} ({ddl})")
        duck.execute(f"INSERT INTO s.{label.lower()} VALUES {values}")
        nodes[label] = _node(label, columns)
    lm = CypherLabelMap(nodes=nodes, relationships={})
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(query, lm), lm, {})
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, lm)
    return sorted(duck.execute(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall())


def test_a_property_of_one_type_everywhere_keeps_it():
    assert _rows("MATCH (n) RETURN n.name") == [("a1",), ("a2",), ("b1",)]


def test_integers_of_any_width_are_integers():
    assert _rows("MATCH (n) RETURN n.num") == [(1,), (9,), (10,)]


def test_integers_and_floats_are_floats():
    assert _rows("MATCH (n) RETURN n.num", ("A", "C")) == [(1.0,), (2.5,), (10.0,)]


def test_a_property_is_compared_as_its_type():
    # As text, '10' > '9' is false.
    assert _rows("MATCH (n) WHERE n.num > 9 RETURN n.num") == [(10,)]


def test_a_property_of_several_types_is_its_text():
    assert _rows("MATCH (n) RETURN n.code") == [("7",), ("x",), ("y",)]


def test_labels_of_a_node_matched_with_no_label_is_its_own_tables_label():
    assert _rows("MATCH (n) RETURN n.name, labels(n)") == [
        ("a1", ["A"]),
        ("a2", ["A"]),
        ("b1", ["B"]),
    ]
