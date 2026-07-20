# Copyright (c) 2026 Kenneth Stott
# Canary: 1c6e9a04-7b53-4d28-8f01-3a5d2e6c9b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1161: federation-wide lineage merge, cycle characterization, and focus slicing."""

from __future__ import annotations

import pytest

from provisa.lineage.graph import Edge, LineageGraph, Node, TransformOp
from provisa.lineage.merge import mark_materialized, merge_graphs, slice_graph


def _n(nid, kind="derived", materialized=False):
    rel, col = (nid.rsplit(".", 1)) if "." in nid else (None, nid)
    return Node(id=nid, column=col, relation=rel, kind=kind, materialized=materialized)


def _g(nodes, edges):
    g = LineageGraph()
    for n in nodes:
        g.nodes[n.id] = n
    for s, t in edges:
        g.add_edge(Edge(s, t, "f", (TransformOp("f", "sql_function"),)))
    return g


def test_merge_unions_by_node_id():
    # two statements share node 'mv.total' → one node in the merged graph, edges stitched
    g1 = _g([_n("orders.amt", "source"), _n("mv.total")], [("orders.amt", "mv.total")])
    g2 = _g([_n("mv.total"), _n("report.sum")], [("mv.total", "report.sum")])
    m = merge_graphs([g1, g2])
    assert "mv.total" in m.graph.nodes
    assert len(m.graph.nodes) == 3  # orders.amt, mv.total, report.sum
    ids = {(e.source, e.target) for e in m.graph.edges}
    assert ("orders.amt", "mv.total") in ids and ("mv.total", "report.sum") in ids


def test_merge_sticky_materialized_and_relation():
    g1 = _g([_n("mv.total")], [])
    g2 = _g([_n("mv.total", materialized=True)], [])
    m = merge_graphs([g1, g2])
    assert m.graph.nodes["mv.total"].materialized is True  # sticky across statements


def test_cycle_without_boundary_is_error():
    g = _g([_n("x.c"), _n("y.c")], [("x.c", "y.c"), ("y.c", "x.c")])
    m = merge_graphs([g])
    assert len(m.cycles) == 1
    assert m.cycles[0].classification == "error"
    assert set(m.cycles[0].nodes) == {"x.c", "y.c"}


def test_cycle_with_materialized_boundary_is_feedback():
    g = _g([_n("x.c", materialized=True), _n("y.c")], [("x.c", "y.c"), ("y.c", "x.c")])
    m = merge_graphs([g])
    assert m.cycles[0].classification == "feedback"
    assert m.cycles[0].has_materialization_boundary is True


def test_no_cycle_in_a_dag():
    g = _g([_n("a"), _n("b"), _n("c")], [("a", "b"), ("b", "c")])
    assert merge_graphs([g]).cycles == []


def test_mark_materialized_by_relation():
    g = _g([_n("mv.total"), _n("orders.amt", "source")], [("orders.amt", "mv.total")])
    mark_materialized(g, {"mv"})
    assert g.nodes["mv.total"].materialized is True
    assert g.nodes["orders.amt"].materialized is False


def test_slice_upstream_downstream_ego():
    g = _g(
        [_n("src.a", "source"), _n("mid.b"), _n("out.c")],
        [("src.a", "mid.b"), ("mid.b", "out.c")],
    )
    up = slice_graph(g, "mid.b", direction="upstream")
    assert set(up.nodes) == {"mid.b", "src.a"}
    down = slice_graph(g, "mid.b", direction="downstream")
    assert set(down.nodes) == {"mid.b", "out.c"}
    ego = slice_graph(g, "mid.b", direction="both")
    assert set(ego.nodes) == {"src.a", "mid.b", "out.c"}


def test_slice_depth_bounds_hops():
    g = _g(
        [_n("a"), _n("b"), _n("c"), _n("d")],
        [("a", "b"), ("b", "c"), ("c", "d")],
    )
    one = slice_graph(g, "a", direction="downstream", depth=1)
    assert set(one.nodes) == {"a", "b"}


def test_slice_unknown_node_fails_loud():
    g = _g([_n("a")], [])
    with pytest.raises(ValueError, match="not in graph"):
        slice_graph(g, "missing")


def test_merged_to_dict_includes_cycles():
    g = _g([_n("x.c"), _n("y.c")], [("x.c", "y.c"), ("y.c", "x.c")])
    d = merge_graphs([g]).to_dict()
    assert "cycles" in d and d["cycles"][0]["classification"] == "error"
    assert {"nodes", "edges", "outputs", "cycles"} <= set(d)
