# Copyright (c) 2026 Kenneth Stott
# Canary: 3f16a4b8-ca4b-49e8-a780-48cd96b8e8ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1484: what a column rename or removal would break."""

from __future__ import annotations

from provisa.api.admin.column_dependents import _mentions
from provisa.lineage.dependents import Dependent, graph_dependents, relation_candidates
from provisa.lineage.graph import Edge, LineageGraph, Node


def _graph() -> LineageGraph:
    """sales.orders.total -> sales.big_orders.total -> sales.daily_totals.total (the last an MV)."""
    graph = LineageGraph()
    for node_id, column, relation, kind, materialized in [
        ("sales.orders.total", "total", "sales.orders", "source", False),
        ("sales.orders.customer_id", "customer_id", "sales.orders", "source", False),
        ("sales.big_orders.total", "total", "sales.big_orders", "derived", False),
        ("sales.daily_totals.total", "total", "sales.daily_totals", "derived", True),
    ]:
        graph.add_node(Node(node_id, column, relation, kind, materialized))
    graph.edges.append(Edge("sales.orders.total", "sales.big_orders.total", "total", ()))
    graph.edges.append(Edge("sales.big_orders.total", "sales.daily_totals.total", "SUM(total)", ()))
    return graph


def test_graph_dependents_reports_downstream_view_and_mv() -> None:
    found = graph_dependents(_graph(), relation_candidates("sales", "orders"), "total")
    assert found == [
        Dependent(kind="view", name="sales.big_orders", detail="selects total", breaks_on="rename"),
        Dependent(kind="mv", name="sales.daily_totals", detail="selects total", breaks_on="rename"),
    ]


def test_graph_dependents_excludes_the_edited_table_itself() -> None:
    found = graph_dependents(_graph(), relation_candidates("sales", "orders"), "total")
    assert all(d.name != "sales.orders" for d in found)


def test_graph_dependents_empty_for_a_column_nothing_selects() -> None:
    assert graph_dependents(_graph(), relation_candidates("sales", "orders"), "customer_id") == []


def test_graph_dependents_empty_for_an_unknown_column() -> None:
    assert graph_dependents(_graph(), relation_candidates("sales", "orders"), "nope") == []


def test_relation_candidates_matches_bare_and_qualified_relations() -> None:
    graph = LineageGraph()
    graph.add_node(Node("orders.total", "total", "orders", "source", False))
    graph.add_node(Node("sales.big_orders.total", "total", "sales.big_orders", "derived", False))
    graph.edges.append(Edge("orders.total", "sales.big_orders.total", "total", ()))
    found = graph_dependents(graph, relation_candidates("sales", "orders"), "total")
    assert [d.name for d in found] == ["sales.big_orders"]


def test_mentions_matches_whole_identifiers_only() -> None:
    assert _mentions("SUM(total)", "total")
    assert _mentions("total + 1", "total")
    assert not _mentions("SUM(total_amount)", "total")
    assert not _mentions("SUM(subtotal)", "total")
    assert not _mentions(None, "total")
    assert not _mentions("", "total")
