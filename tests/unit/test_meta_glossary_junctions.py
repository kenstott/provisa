# Copyright (c) 2026 Kenneth Stott
# Canary: f31649a8-60b5-46aa-9449-901cd5f989b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The system catalog's glossary associations are edges, not nodes (REQ-1586, REQ-2130): a term's
columns and domains are reached over their junctions (ontology._term_refs, ontology._domains), a
table's class term over registered_tables.class_term_id, each in one hop -- and none of those
associations is a label. The terms are the ontology schema's, the columns, domains and tables
meta's (REQ-2163). Read off the catalog the server registers (meta_model.yaml)."""

# Requirements: REQ-1586, REQ-2130

import importlib.util
import sys
from pathlib import Path

import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.translator import cypher_to_sql

_REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def label_map():
    spec = importlib.util.spec_from_file_location(
        "cypher_golden", _REPO / "scripts" / "cypher_golden.py"
    )
    assert spec is not None and spec.loader is not None
    cypher_golden = importlib.util.module_from_spec(spec)
    sys.modules["cypher_golden"] = cypher_golden
    spec.loader.exec_module(cypher_golden)
    return cypher_golden.model_label_map(
        _REPO / "tests" / "fixtures" / "graphql_golden" / "meta_model.yaml"
    )


@pytest.mark.parametrize("table", ["_term_refs", "_domains", "_term_edges"])
def test_no_glossary_association_is_a_label(label_map, table):
    assert table not in label_map.nodes_by_table
    assert all(node.table_name != table for node in label_map.nodes.values())


@pytest.mark.parametrize(
    ("rel_type", "source", "target", "via"),
    [
        ("REFERENCED_BY", "ONT__Terms", "M__TableColumns", "_term_refs"),
        ("IN_DOMAIN", "ONT__Terms", "M__Domains", "_domains"),
        ("CLASS_TERM", "M__RegisteredTables", "ONT__Terms", None),
    ],
)
def test_a_glossary_association_is_one_direct_edge(label_map, rel_type, source, target, via):
    (edge,) = [r for r in label_map.relationships.values() if r.rel_type == rel_type]
    assert (edge.source_label, edge.target_label) == (source, target)
    assert (edge.via.table_name if edge.via else None) == via
    if via is None:
        assert edge.join_source_column == "class_term_id"
    # One hop in Cypher, translated: the pattern names no association node.
    src, tgt = label_map.nodes[source], label_map.nodes[target]
    cypher = f"MATCH (a:{src.label})-[:{rel_type}]->(b:{tgt.label}) RETURN a, b"
    sql = cypher_to_sql(parse_cypher(cypher), label_map, {})[0].sql(dialect="postgres")
    assert (via or src.table_name) in sql
