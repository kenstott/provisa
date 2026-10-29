# Copyright (c) 2026 Kenneth Stott
# Canary: 3b1d6f0e-5c2a-4e7b-9a41-7d8e2f6c0b15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-348: a labelled walk may reuse one relationship type between different labels
(A-[:T]->B-[:T]->C): the relationships a path has used are told apart by mapping (its labels,
type and join), not by type name. Run on DuckDB against the rows the walk must answer."""

# Requirements: REQ-348, REQ-751

import duckdb
import pytest

from provisa.compiler.sql_rewrite import make_semantic_sql  # noqa: F401
from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb


def _node(label: str, table: str) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=1,
        source_id="src",
        id_column="id",
        pk_columns=[],
        catalog_name="cat",
        schema_name="public",
        table_name=table,
        properties={"id": "id", "name": "name"},
    )


def _rel(src: str, tgt: str, src_col: str, tgt_col: str) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type="T",
        source_label=src,
        target_label=tgt,
        join_source_column=src_col,
        join_target_column=tgt_col,
        field_name=f"{src.lower()}_to_{tgt.lower()}",
    )


@pytest.fixture
def chain():
    """T maps A->B and B->C: two mappings, one type name."""
    lm = CypherLabelMap(
        nodes={"A": _node("A", "a"), "B": _node("B", "b"), "C": _node("C", "c")},
        relationships={
            "T::A→B": _rel("A", "B", "b_id", "id"),
            "T::B→C": _rel("B", "C", "c_id", "id"),
        },
    )
    lm.aliases = {"T": list(lm.relationships.values())}
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS cat")
    con.execute("CREATE SCHEMA cat.public")
    con.execute("CREATE TABLE cat.public.a (id INTEGER, name VARCHAR, b_id INTEGER)")
    con.execute("CREATE TABLE cat.public.b (id INTEGER, name VARCHAR, c_id INTEGER)")
    con.execute("CREATE TABLE cat.public.c (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO cat.public.a VALUES (1, 'a1', 10), (2, 'a2', 11)")
    con.execute("INSERT INTO cat.public.b VALUES (10, 'b10', 100), (11, 'b11', 101)")
    con.execute("INSERT INTO cat.public.c VALUES (100, 'c100'), (101, 'c101')")
    return lm, con


def _rows(chain, cypher: str) -> list[tuple]:
    lm, con = chain
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")
    return con.execute(transpile_to_duckdb(sql)).fetchall()


def test_find_paths_walks_one_type_across_two_mappings(chain):
    lm, _ = chain
    paths = lm.find_paths("A", "C", max_hops=3)
    assert [[(r.source_label, r.target_label) for r in p] for p in paths] == [
        [("A", "B"), ("B", "C")]
    ]


def test_find_paths_still_never_reuses_one_mapping():
    # One self-mapping T on A: a path of 2 hops would reuse it, so it is not found.
    lm = CypherLabelMap(
        nodes={"A": _node("A", "a")},
        relationships={"T::A→A": _rel("A", "A", "parent_id", "id")},
    )
    assert [len(p) for p in lm.find_paths("A", "A", max_hops=3)] == [1]


def test_a_variable_length_walk_reuses_a_type_between_labels(chain):
    assert sorted(_rows(chain, "MATCH (a:A)-[:T*1..2]->(c:C) RETURN a.name, c.name")) == [
        ("a1", "c100"),
        ("a2", "c101"),
    ]


def test_a_shortest_path_reuses_a_type_between_labels(chain):
    assert sorted(
        _rows(chain, "MATCH p = shortestPath((a:A)-[:T*..3]->(c:C)) RETURN a.name, c.name")
    ) == [("a1", "c100"), ("a2", "c101")]
