# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2b9e41-0c7a-4f35-a8e6-91f3c5b7d204
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1161, REQ-2116: lineage shows only the fields its caller can read.

A field the caller is not served leaves the graph with nothing in its place: its neighbours join
directly (A→B→C reads A→C), and no node, edge, transform or count names it.
"""

from __future__ import annotations

import json

from provisa.lineage.graph import Edge, LineageGraph, Node, TransformOp
from provisa.lineage.read_rights import readable_graph

REGISTERED = {"sales.orders", "sales.v_orders", "sales.mart"}


def _graph(nodes: list[tuple[str, str, str | None, str]], edges: list[tuple[str, str, str]]):
    graph = LineageGraph()
    for nid, column, relation, kind in nodes:
        graph.add_node(Node(id=nid, column=column, relation=relation, kind=kind))
    for source, target, transform in edges:
        graph.add_edge(Edge(source, target, transform, (TransformOp("upper", "sql_function"),)))
    return graph


def _chain() -> LineageGraph:
    # A (orders.amount) → B (v_orders.amount) → C (mart.amount)
    return _graph(
        [
            ("sales.orders.amount", "amount", "sales.orders", "source"),
            ("sales.v_orders.amount", "amount", "sales.v_orders", "derived"),
            ("sales.mart.amount", "amount", "sales.mart", "derived"),
        ],
        [
            ("sales.orders.amount", "sales.v_orders.amount", "UPPER(amount)"),
            ("sales.v_orders.amount", "sales.mart.amount", "UPPER(amount)"),
        ],
    )


def test_a_hidden_intermediate_collapses_to_a_direct_edge() -> None:
    out = readable_graph(
        _chain(), registered=REGISTERED, readable={"sales.orders.amount", "sales.mart.amount"}
    )
    assert set(out.nodes) == {"sales.orders.amount", "sales.mart.amount"}
    assert [(e.source, e.target, e.transform, e.ops) for e in out.edges] == [
        ("sales.orders.amount", "sales.mart.amount", "", ())
    ]
    assert "v_orders" not in json.dumps(out.to_dict())


def test_an_unreadable_member_column_is_absent_with_its_edges() -> None:
    out = readable_graph(_chain(), registered=REGISTERED, readable={"sales.orders.amount"})
    assert set(out.nodes) == {"sales.orders.amount"}
    assert out.edges == []


def test_a_readable_chain_keeps_its_transforms() -> None:
    out = readable_graph(
        _chain(),
        registered=REGISTERED,
        readable={"sales.orders.amount", "sales.v_orders.amount", "sales.mart.amount"},
    )
    assert [e.transform for e in out.edges] == ["UPPER(amount)", "UPPER(amount)"]


def test_an_edge_beside_a_hidden_parent_carries_no_transform() -> None:
    # total = amount + discount; discount is hidden, and the expression names it.
    graph = _graph(
        [
            ("sales.orders.amount", "amount", "sales.orders", "source"),
            ("sales.orders.discount", "discount", "sales.orders", "source"),
            ("sales.mart.total", "total", "sales.mart", "derived"),
        ],
        [
            ("sales.orders.amount", "sales.mart.total", "amount + discount"),
            ("sales.orders.discount", "sales.mart.total", "amount + discount"),
        ],
    )
    out = readable_graph(
        graph, registered=REGISTERED, readable={"sales.orders.amount", "sales.mart.total"}
    )
    assert [(e.source, e.transform, e.ops) for e in out.edges] == [("sales.orders.amount", "", ())]
    assert "discount" not in json.dumps(out.to_dict())


def test_a_native_field_shows_only_behind_a_readable_column() -> None:
    graph = _graph(
        [
            ("native:sales.orders.AMT", "AMT", "native:sales.orders", "native"),
            ("sales.orders.amount", "amount", "sales.orders", "source"),
            ("native:sales.orders.SECRET", "SECRET", "native:sales.orders", "native"),
            ("sales.orders.secret", "secret", "sales.orders", "source"),
        ],
        [
            ("native:sales.orders.AMT", "sales.orders.amount", "AMT"),
            ("native:sales.orders.SECRET", "sales.orders.secret", "SECRET"),
        ],
    )
    out = readable_graph(graph, registered=REGISTERED, readable={"sales.orders.amount"})
    assert set(out.nodes) == {"native:sales.orders.AMT", "sales.orders.amount"}
    assert "SECRET" not in json.dumps(out.to_dict())


def test_an_unregistered_physical_column_is_never_shown() -> None:
    graph = _graph(
        [
            ("staging.raw.amount", "amount", "staging.raw", "source"),
            ("sales.mart.amount", "amount", "sales.mart", "derived"),
        ],
        [("staging.raw.amount", "sales.mart.amount", "amount")],
    )
    out = readable_graph(graph, registered=REGISTERED, readable={"sales.mart.amount"})
    assert set(out.nodes) == {"sales.mart.amount"}
    assert "staging" not in json.dumps(out.to_dict())


def test_a_hidden_views_internal_step_goes_with_it() -> None:
    # orders.amount → cte.x (a step inside v_orders) → v_orders.amount → mart.amount
    graph = _graph(
        [
            ("sales.orders.amount", "amount", "sales.orders", "source"),
            ("cte.x", "x", "cte", "derived"),
            ("sales.v_orders.amount", "amount", "sales.v_orders", "derived"),
            ("sales.mart.amount", "amount", "sales.mart", "derived"),
        ],
        [
            ("sales.orders.amount", "cte.x", "amount * 2"),
            ("cte.x", "sales.v_orders.amount", "x"),
            ("sales.v_orders.amount", "sales.mart.amount", "amount"),
        ],
    )
    hidden_view = readable_graph(
        graph, registered=REGISTERED, readable={"sales.orders.amount", "sales.mart.amount"}
    )
    assert "cte.x" not in hidden_view.nodes
    assert [(e.source, e.target) for e in hidden_view.edges] == [
        ("sales.orders.amount", "sales.mart.amount")
    ]
    readable_view = readable_graph(
        graph,
        registered=REGISTERED,
        readable={"sales.orders.amount", "sales.v_orders.amount", "sales.mart.amount"},
    )
    assert "cte.x" in readable_view.nodes


def test_a_statements_own_outputs_are_shown() -> None:
    graph = _graph(
        [
            ("sales.orders.amount", "amount", "sales.orders", "source"),
            ("total", "total", None, "derived"),
        ],
        [("sales.orders.amount", "total", "SUM(amount)")],
    )
    graph.outputs = ["total"]
    out = readable_graph(graph, registered=REGISTERED, readable={"sales.orders.amount"})
    assert set(out.nodes) == {"sales.orders.amount", "total"}
    assert out.outputs == ["total"]


def test_a_statement_output_over_only_hidden_fields_keeps_no_edge_to_them() -> None:
    graph = _graph(
        [
            ("sales.orders.amount", "amount", "sales.orders", "source"),
            ("total", "total", None, "derived"),
        ],
        [("sales.orders.amount", "total", "SUM(amount)")],
    )
    out = readable_graph(graph, registered=REGISTERED, readable=set())
    assert set(out.nodes) == {"total"}
    assert out.edges == []


def test_the_input_graph_is_left_as_it_was() -> None:
    graph = _chain()
    readable_graph(graph, registered=REGISTERED, readable=set())
    assert len(graph.nodes) == 3 and len(graph.edges) == 2
