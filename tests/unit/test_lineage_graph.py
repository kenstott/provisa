# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2c9a41-8d63-4b07-9f52-1a6c0d3e5b82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1160: single-statement column-level lineage graph — intermediates, named transforms, splice."""

from __future__ import annotations

from provisa.lineage.graph import build_column_graph

_ENRICH = {
    "enrich": {
        "name": "enrich",
        "arguments": [
            {
                "name": "input",
                "arg_kind": "result_set",
                "columns": [{"name": "id", "type": "integer"}, {"name": "region", "type": "text"}],
            }
        ],
        "output_columns": [
            {"name": "id", "type": "integer"},
            {"name": "embedding", "type": "text"},
            {"name": "geo", "type": "text"},
        ],
    }
}


def _edges(g):
    return {(e.source, e.target) for e in g.edges}


def test_preserves_intermediate_cte_nodes():
    sql = """
    WITH t AS (SELECT o.id AS id, o.amount * 1.1 AS gross FROM orders o)
    SELECT id, gross FROM t
    """
    g = build_column_graph(sql)
    # intermediate CTE columns exist as their own nodes (not collapsed to leaves)
    assert "t.gross" in g.nodes and "t.id" in g.nodes
    assert g.nodes["o.amount"].kind == "source"
    assert ("o.amount", "t.gross") in _edges(g)
    assert ("t.gross", "gross") in _edges(g)


def test_names_operator_and_function_transforms():
    sql = "SELECT o.amount * 1.1 AS gross, upper(o.region) AS r FROM orders o"
    g = build_column_graph(sql)
    by_target = {e.target: e for e in g.edges}
    gross_ops = {(o.name, o.kind) for o in by_target["gross"].ops}
    r_ops = {(o.name, o.kind) for o in by_target["r"].ops}
    assert ("*", "operator") in gross_ops
    assert ("UPPER", "sql_function") in r_ops


def test_identity_and_constant_transforms():
    sql = "SELECT o.id AS id, 42 AS const FROM orders o"
    g = build_column_graph(sql)
    by_target = {e.target: e for e in g.edges}
    assert any(o.kind == "identity" for o in by_target["id"].ops)
    # a pure literal has no upstream column edge; it is recorded as an output node
    assert "const" in g.outputs


def test_command_splice_taint_closure():
    sql = (
        "SELECT o.n AS name, e.embedding, upper(e.geo) AS geo_u "
        "FROM orders o JOIN enrich('main.public.orders') e ON o.id = e.id"
    )
    g = build_column_graph(sql, commands=_ENRICH)
    edges = _edges(g)
    # command nodes exist
    assert g.nodes["e.embedding"].kind == "command"
    # taint closure: every declared output derives from ALL declared input columns
    for out in ("e.embedding", "e.geo"):
        assert ("main.public.orders.id", out) in edges
        assert ("main.public.orders.region", out) in edges
    # the real source columns are leaves
    assert g.nodes["main.public.orders.region"].kind == "source"
    # command edge is named as a command op
    cmd_edges = [e for e in g.edges if e.target == "e.embedding"]
    assert all(any(o.kind == "command" and o.name == "enrich" for o in e.ops) for e in cmd_edges)
    # end-to-end continuity: geo_u ultimately reaches the source relation through the command
    assert ("e.geo", "geo_u") in edges and ("main.public.orders.region", "e.geo") in edges


def test_command_without_contract_is_opaque_leaf():
    # No commands passed → the call is just an unknown relation leaf; graph still builds.
    sql = "SELECT e.embedding FROM enrich('x') e"
    g = build_column_graph(sql)
    assert "embedding" in g.outputs
    # no splice without the contract; e.embedding is a leaf (kind source/derived, no command edges)
    assert not any(o.kind == "command" for e in g.edges for o in e.ops)


def test_shared_intermediate_is_one_node():
    # two outputs derive from the same CTE column → single shared node, not duplicated
    sql = """
    WITH t AS (SELECT o.amount AS amt FROM orders o)
    SELECT amt AS a, amt * 2 AS b FROM t
    """
    g = build_column_graph(sql)
    assert "t.amt" in g.nodes
    assert ("t.amt", "a") in _edges(g) and ("t.amt", "b") in _edges(g)


def test_to_dict_is_render_ready():
    g = build_column_graph("SELECT o.amount * 2 AS x FROM orders o")
    d = g.to_dict()
    assert set(d) == {"nodes", "edges", "outputs"}
    assert {"id", "column", "relation", "kind"} <= set(d["nodes"][0])
    edge = next(e for e in d["edges"] if e["target"] == "x")
    assert {"source", "target", "transform", "ops"} <= set(edge)
    assert any(o["kind"] == "operator" for o in edge["ops"])


def test_router_core_bad_sql_raises_valueerror():
    from provisa.api.admin.lineage_router import lineage_graph_for

    import pytest

    with pytest.raises(ValueError, match="could not parse"):
        lineage_graph_for("NOT ((( valid", {})


def test_router_core_splices_commands():
    from provisa.api.admin.lineage_router import lineage_graph_for

    g = lineage_graph_for(
        "SELECT e.embedding FROM orders o JOIN enrich('main.public.orders') e ON o.id = e.id",
        _ENRICH,
    )
    kinds = {n["id"]: n["kind"] for n in g["nodes"]}
    assert kinds.get("e.embedding") == "command"
    assert kinds.get("main.public.orders.region") == "source"
