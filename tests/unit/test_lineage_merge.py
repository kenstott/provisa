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
from provisa.lineage.merge import (
    build_federation_graph,
    mark_materialized,
    merge_graphs,
    slice_graph,
)


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


# ---- REQ-1161: federation build (stitch views by real relation identity) ----


def test_federation_stitches_views_end_to_end():
    views = [
        ("mv_daily", "SELECT o.amount AS total FROM orders o"),
        ("report", "SELECT d.total * 2 AS grand FROM mv_daily d"),
    ]
    m = build_federation_graph(views, materialized_relations={"mv_daily"})
    edges = {(e.source, e.target) for e in m.graph.edges}
    # base source → materialized view output → downstream view output, one continuous chain
    assert ("orders.amount", "mv_daily.total") in edges
    assert ("mv_daily.total", "report.grand") in edges
    assert m.graph.nodes["mv_daily.total"].materialized is True


def test_federation_stitches_schema_qualified_view_reference():
    # REQ-1161: a view that reads another view by its schema-qualified name (pet_store.test) — sqlglot
    # drops the schema to a bare 'test', which must requalify to the full relation so the two stitch
    # instead of leaving a duplicate, disconnected 'test' relation.
    views = [
        ("pet_store.test", "SELECT u.name AS name FROM users u"),
        ("pet_store.fun", 'SELECT substring(t.name, 2) AS first_two FROM "pet_store"."test" t'),
    ]
    m = build_federation_graph(views)
    relations = {n.relation for n in m.graph.nodes.values() if n.relation}
    assert "test" not in relations  # no bare-name duplicate
    edges = {(e.source, e.target) for e in m.graph.edges}
    assert ("pet_store.test.name", "pet_store.fun.first_two") in edges


def test_federation_cycle_across_views_is_characterized():
    # a → b (materialized) → a: legal feedback because the loop crosses a materialized boundary
    views = [
        ("mv_b", "SELECT a.x AS x FROM mv_a a"),
        ("mv_a", "SELECT b.x AS x FROM mv_b b"),
    ]
    m = build_federation_graph(views, materialized_relations={"mv_a", "mv_b"})
    assert len(m.cycles) == 1
    assert m.cycles[0].classification == "feedback"


def test_federation_skips_unparseable_view():
    views = [("good", "SELECT o.a AS a FROM orders o"), ("bad", "NOT ((( valid")]
    m = build_federation_graph(views)
    assert "good.a" in m.graph.nodes  # the good view still contributes despite the bad one
