# Copyright (c) 2026 Kenneth Stott
# Canary: 5f3c8a2b-1d74-4e96-b0c5-a8e27d4f6913
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1640, REQ-2126: a data product's lineage and its nearest physical inputs.

The product's lineage is its member tables, their full upstream ancestry and one hop downstream,
every member column an output; a checker table is no part of it. Its inputs are the first physical
datasets upstream of each member: a source table or a materialized view, through plain views, and
for a member read straight from a source, that source.
"""

from __future__ import annotations

from provisa.lineage.graph import Edge, LineageGraph, Node
from provisa.lineage.product import nearest_inputs, product_graph


def _graph() -> LineageGraph:
    # raw.orders → stg.v_orders (plain view) → sales.mart (member, a view) → sales.report
    # mv.daily (materialized) → sales.mart;  native → raw.orders;  sales.mart → sales.mart_dq (checker)
    g = LineageGraph()
    for nid, rel, kind, mat in [
        ("native:raw.orders.AMT", "native:raw.orders", "native", False),
        ("raw.orders.amount", "raw.orders", "source", False),
        ("stg.v_orders.amount", "stg.v_orders", "derived", False),
        ("mv.daily.total", "mv.daily", "derived", True),
        ("sales.mart.amount", "sales.mart", "derived", False),
        ("sales.mart.total", "sales.mart", "derived", False),
        ("sales.report.amount", "sales.report", "derived", False),
        ("sales.report2.amount", "sales.report2", "derived", False),
        ("sales.mart_dq.ok", "sales.mart_dq", "derived", False),
    ]:
        g.add_node(
            Node(id=nid, column=nid.rsplit(".", 1)[1], relation=rel, kind=kind, materialized=mat)
        )
    for s, t in [
        ("native:raw.orders.AMT", "raw.orders.amount"),
        ("raw.orders.amount", "stg.v_orders.amount"),
        ("stg.v_orders.amount", "sales.mart.amount"),
        ("mv.daily.total", "sales.mart.total"),
        ("sales.mart.amount", "sales.report.amount"),
        ("sales.report.amount", "sales.report2.amount"),
        ("sales.mart.amount", "sales.mart_dq.ok"),
    ]:
        g.add_edge(Edge(s, t, "", ()))
    return g


MEMBERS = {"sales.mart.amount", "sales.mart.total", "sales.mart.unused"}


def test_the_product_graph_is_its_ancestry_and_one_hop_downstream() -> None:
    out = product_graph(_graph(), MEMBERS, checker_relations={"sales.mart_dq"})
    assert set(out.nodes) == {
        "native:raw.orders.AMT",
        "raw.orders.amount",
        "stg.v_orders.amount",
        "mv.daily.total",
        "sales.mart.amount",
        "sales.mart.total",
        "sales.mart.unused",
        "sales.report.amount",
    }
    # Every member column is an output, the one nothing derives from included.
    assert set(out.outputs) == MEMBERS
    assert out.nodes["sales.mart.unused"].kind == "source"


def test_the_input_graph_is_left_as_it_was() -> None:
    g = _graph()
    product_graph(g, MEMBERS, checker_relations=set())
    assert "sales.mart.unused" not in g.nodes


def test_a_views_inputs_stop_at_the_first_physical_dataset() -> None:
    g = product_graph(_graph(), MEMBERS, checker_relations={"sales.mart_dq"})
    inputs = nearest_inputs(g, {"sales.mart"}, base={"raw.orders"})
    assert inputs == ["mv.daily", "raw.orders"]


def test_a_member_read_from_a_source_has_that_source_as_its_input() -> None:
    g = product_graph(_graph(), {"raw.orders.amount"}, checker_relations=set())
    assert nearest_inputs(g, {"raw.orders"}, base={"raw.orders"}) == ["native:raw.orders"]


def test_a_member_upstream_of_another_is_no_input() -> None:
    g = product_graph(_graph(), MEMBERS | {"raw.orders.amount"}, checker_relations=set())
    inputs = nearest_inputs(g, {"sales.mart", "raw.orders"}, base={"raw.orders"})
    assert inputs == ["mv.daily", "native:raw.orders"]
