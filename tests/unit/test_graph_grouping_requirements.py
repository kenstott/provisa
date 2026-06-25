# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for graph grouping requirements: REQ-644, REQ-645, REQ-646, REQ-647, REQ-648, REQ-649"""

from __future__ import annotations

import math
from typing import Any


# ---------------------------------------------------------------------------
# Pure-logic helpers that mirror the graph grouping contract.
# These are backend implementations of the algorithms described in the
# requirements. The UI layer (React/Cytoscape) is frontend-only, but the
# underlying algorithms are testable as pure functions.
# ---------------------------------------------------------------------------


def _apply_group_key(nodes: list[dict], attribute: str) -> list[dict]:
    """Derive groupKey for each node without modifying the original."""
    result = []
    for node in nodes:
        copy = dict(node)
        copy["groupKey"] = node.get("properties", {}).get(attribute)
        result.append(copy)
    return result


def _discover_groupable_attributes(nodes: list[dict]) -> list[str]:
    """Scan node properties to build a list of groupable attributes.

    Attributes with more than one distinct value across nodes are included.
    'domain' is always the first entry.
    """
    from collections import defaultdict

    value_sets: dict[str, set] = defaultdict(set)
    for node in nodes:
        label = node.get("label", "")
        domain = label.split("_")[0] if "_" in label else label
        value_sets["domain"].add(domain)
        for key, val in node.get("properties", {}).items():
            if isinstance(val, (str, int, float, bool)) and val is not None:
                value_sets[key].add(val)

    # schema cluster virtuals
    for scl_key, prop in [("schema_L1", "scl1"), ("schema_L2", "scl2"), ("schema_L3", "scl3")]:
        scl_values = {
            n.get("properties", {}).get(prop)
            for n in nodes
            if n.get("properties", {}).get(prop) is not None
        }
        if len(scl_values) > 1:
            value_sets[scl_key] = scl_values

    attrs = []
    # domain is always first
    if "domain" in value_sets:
        attrs.append("domain")

    for key, values in value_sets.items():
        if key == "domain":
            continue
        if len(values) > 1:
            attrs.append(key)

    return attrs


def _cluster_color(group_key: str) -> str:
    """Derive a stable color from a group key string (hex color)."""
    h = 0
    for ch in group_key:
        h = (h * 31 + ord(ch)) & 0xFFFFFF
    return f"#{h:06x}"


def _compute_hull(positions: list[tuple[float, float]]) -> dict[str, float]:
    """Fit a bounding-box centroid and radii from node positions."""
    if not positions:
        return {"cx": 0.0, "cy": 0.0, "rx": 0.0, "ry": 0.0}
    xs = [p[0] for p in positions]
    ys = [p[1] for p in positions]
    cx = (min(xs) + max(xs)) / 2
    cy = (min(ys) + max(ys)) / 2
    rx = (max(xs) - min(xs)) / 2 + 20
    ry = (max(ys) - min(ys)) / 2 + 20
    return {"cx": cx, "cy": cy, "rx": rx, "ry": ry}


def _to_screen_coords(
    graph_point: tuple[float, float], zoom: float, pan: tuple[float, float]
) -> tuple[float, float]:
    """Convert graph coordinates to screen coordinates using zoom and pan."""
    sx = graph_point[0] * zoom + pan[0]
    sy = graph_point[1] * zoom + pan[1]
    return sx, sy


def _collapse_to_supernode(
    nodes: list[dict],
    edges: list[dict],
    group_attr: str,
    collapsed_group_value: Any,
) -> tuple[list[dict], list[dict]]:
    """Collapse all nodes with the given group value into a supernode."""
    member_ids = {n["id"] for n in nodes if n.get(group_attr) == collapsed_group_value}
    supernode_id = f"__collapsed_group_{collapsed_group_value}"
    remaining_nodes = [n for n in nodes if n["id"] not in member_ids]
    supernode = {
        "id": supernode_id,
        "label": str(collapsed_group_value),
        "member_count": len(member_ids),
        "collapsed": True,
    }
    remaining_nodes.append(supernode)

    rewritten_edges = []
    for e in edges:
        src = e["source"] if e["source"] not in member_ids else supernode_id
        tgt = e["target"] if e["target"] not in member_ids else supernode_id
        if src == tgt:
            continue  # intra-group edge, drop
        rewritten_edges.append({**e, "source": src, "target": tgt})

    return remaining_nodes, rewritten_edges


def _scale_node_size(
    value: float, min_val: float, max_val: float, base: float, multiplier: float
) -> float:
    """Linearly scale value between min and max to produce node diameter."""
    if max_val == min_val:
        return base
    t = (value - min_val) / (max_val - min_val)
    return base + t * base * (multiplier - 1)


def _make_collapse_fixture() -> tuple[list[dict], list[dict]]:
    nodes = [
        {"id": "n1", "label": "Sales", "groupKey": "sales"},
        {"id": "n2", "label": "Sales", "groupKey": "sales"},
        {"id": "n3", "label": "Finance", "groupKey": "finance"},
    ]
    edges = [
        {"id": "e1", "source": "n1", "target": "n3"},
        {"id": "e2", "source": "n2", "target": "n3"},
        {"id": "e3", "source": "n1", "target": "n2"},  # intra-group
    ]
    return nodes, edges


# ---------------------------------------------------------------------------
# REQ-644: Node grouping is a view transform — underlying data never modified.
# ---------------------------------------------------------------------------


def test_group_key_applied_without_modifying_original():
    # REQ-644: Original node objects must not be mutated by groupKey derivation.
    nodes = [
        {"id": "n1", "label": "Sales_Customer", "properties": {"status": "active"}},
        {"id": "n2", "label": "Sales_Order", "properties": {"status": "pending"}},
    ]
    originals = [dict(n) for n in nodes]
    _apply_group_key(nodes, "status")
    for orig, node in zip(originals, nodes):
        assert "groupKey" not in node
        assert node == orig


def test_group_key_result_contains_group_key_field():
    # REQ-644: The derived list carries groupKey on each node.
    nodes = [{"id": "n1", "label": "A", "properties": {"region": "west"}}]
    result = _apply_group_key(nodes, "region")
    assert "groupKey" in result[0]
    assert result[0]["groupKey"] == "west"


def test_switching_group_attribute_does_not_change_underlying_data():
    # REQ-644: Changing the grouping attribute re-derives groupKey without altering nodes.
    nodes = [{"id": "n1", "label": "T", "properties": {"region": "east", "tier": "gold"}}]
    by_region = _apply_group_key(nodes, "region")
    by_tier = _apply_group_key(nodes, "tier")
    assert by_region[0]["groupKey"] == "east"
    assert by_tier[0]["groupKey"] == "gold"
    assert "groupKey" not in nodes[0]


def test_nodes_without_attribute_get_none_group_key():
    # REQ-644: Nodes missing the grouping attribute get groupKey=None (not an error).
    nodes = [{"id": "n1", "label": "T", "properties": {}}]
    result = _apply_group_key(nodes, "missing_attr")
    assert result[0]["groupKey"] is None


# ---------------------------------------------------------------------------
# REQ-645: Attribute discovery — attributes with > 1 distinct value included;
# domain is always first; schema_L1/L2/L3 from scl1/scl2/scl3.
# ---------------------------------------------------------------------------


def test_domain_is_always_first_entry():
    # REQ-645: domain (derived from node label prefix) is always first.
    nodes = [
        {"id": "n1", "label": "Sales_Customer", "properties": {"region": "west"}},
        {"id": "n2", "label": "Finance_Invoice", "properties": {"region": "east"}},
    ]
    attrs = _discover_groupable_attributes(nodes)
    assert attrs[0] == "domain"


def test_single_distinct_value_attribute_excluded():
    # REQ-645: Attributes with only one distinct value are not groupable.
    nodes = [
        {"id": "n1", "label": "A_X", "properties": {"constant": "same"}},
        {"id": "n2", "label": "B_Y", "properties": {"constant": "same"}},
    ]
    attrs = _discover_groupable_attributes(nodes)
    assert "constant" not in attrs


def test_multi_distinct_value_attribute_included():
    # REQ-645: Attributes with > 1 distinct value are included in the dropdown.
    nodes = [
        {"id": "n1", "label": "A_X", "properties": {"status": "active"}},
        {"id": "n2", "label": "A_Y", "properties": {"status": "inactive"}},
    ]
    attrs = _discover_groupable_attributes(nodes)
    assert "status" in attrs


def test_schema_cluster_virtuals_included_when_multiple_values():
    # REQ-645: schema_L1/L2/L3 included when scl1/scl2/scl3 have > 1 value.
    nodes = [
        {"id": "n1", "label": "A", "properties": {"scl1": "tier1"}},
        {"id": "n2", "label": "B", "properties": {"scl1": "tier2"}},
    ]
    attrs = _discover_groupable_attributes(nodes)
    assert "schema_L1" in attrs


def test_schema_cluster_virtual_excluded_when_single_value():
    # REQ-645: schema_L1/L2/L3 excluded when scl1/scl2/scl3 have only 1 value.
    nodes = [
        {"id": "n1", "label": "A", "properties": {"scl1": "tier1"}},
        {"id": "n2", "label": "B", "properties": {"scl1": "tier1"}},
    ]
    attrs = _discover_groupable_attributes(nodes)
    assert "schema_L1" not in attrs


# ---------------------------------------------------------------------------
# REQ-646: Color encoding — stable color per group key string.
# ---------------------------------------------------------------------------


def test_same_group_key_produces_same_color():
    # REQ-646: Same group key always produces the same color (stable encoding).
    assert _cluster_color("sales") == _cluster_color("sales")


def test_different_group_keys_produce_different_colors():
    # REQ-646: Different group keys should produce different colors.
    assert _cluster_color("sales") != _cluster_color("finance")


def test_color_is_valid_hex():
    # REQ-646: Color output is a valid CSS hex color string.
    color = _cluster_color("analytics")
    assert color.startswith("#")
    assert len(color) == 7
    int(color[1:], 16)  # must be valid hex


def test_empty_string_key_produces_valid_color():
    # REQ-646: Edge case: empty group key produces a valid color.
    color = _cluster_color("")
    assert color.startswith("#")
    assert len(color) == 7


# ---------------------------------------------------------------------------
# REQ-647: Hull SVG overlay — ellipses fitted from bounding-box centroids;
# re-renders on viewport events for correct pan/zoom tracking.
# ---------------------------------------------------------------------------


def test_hull_centroid_is_midpoint_of_bounding_box():
    # REQ-647: Centroid is the midpoint of all node positions.
    positions = [(0.0, 0.0), (4.0, 0.0), (0.0, 6.0), (4.0, 6.0)]
    hull = _compute_hull(positions)
    assert hull["cx"] == 2.0
    assert hull["cy"] == 3.0


def test_hull_radii_include_padding():
    # REQ-647: Radii include 20px padding beyond the bounding box half-extents.
    positions = [(0.0, 0.0), (10.0, 6.0)]
    hull = _compute_hull(positions)
    assert hull["rx"] >= 5.0 + 20
    assert hull["ry"] >= 3.0 + 20


def test_hull_single_node_has_padding_only():
    # REQ-647: A single-node group has radii equal to just the padding.
    positions = [(5.0, 5.0)]
    hull = _compute_hull(positions)
    assert hull["rx"] == 20.0
    assert hull["ry"] == 20.0


def test_screen_coords_apply_zoom_and_pan():
    # REQ-647: Screen coordinate conversion multiplies by zoom and adds pan.
    sx, sy = _to_screen_coords((10.0, 20.0), zoom=2.0, pan=(5.0, 5.0))
    assert sx == 25.0
    assert sy == 45.0


def test_screen_coords_identity_at_zoom_1_pan_0():
    # REQ-647: At zoom=1 and pan=(0,0), graph and screen coordinates match.
    sx, sy = _to_screen_coords((7.0, 3.0), zoom=1.0, pan=(0.0, 0.0))
    assert sx == 7.0
    assert sy == 3.0


# ---------------------------------------------------------------------------
# REQ-648: Collapse to supernode — synthetic node id format, edge rewriting.
# ---------------------------------------------------------------------------


def test_collapsed_nodes_removed_from_graph():
    # REQ-648: Member nodes are removed from the Cytoscape graph when collapsed.
    nodes, edges = _make_collapse_fixture()
    result_nodes, _ = _collapse_to_supernode(
        nodes, edges, group_attr="groupKey", collapsed_group_value="sales"
    )
    ids = {n["id"] for n in result_nodes}
    assert "n1" not in ids
    assert "n2" not in ids


def test_supernode_added_with_member_count():
    # REQ-648: A synthetic supernode is added showing the group label and member count.
    nodes, edges = _make_collapse_fixture()
    result_nodes, _ = _collapse_to_supernode(
        nodes, edges, group_attr="groupKey", collapsed_group_value="sales"
    )
    supernodes = [n for n in result_nodes if n.get("collapsed")]
    assert len(supernodes) == 1
    assert supernodes[0]["member_count"] == 2


def test_supernode_id_has_expected_prefix():
    # REQ-648: Supernode id contains the group identifier.
    nodes, edges = _make_collapse_fixture()
    result_nodes, _ = _collapse_to_supernode(
        nodes, edges, group_attr="groupKey", collapsed_group_value="sales"
    )
    supernode_ids = [n["id"] for n in result_nodes if n.get("collapsed")]
    assert all("sales" in sid for sid in supernode_ids)


def test_cross_group_edges_rewritten_to_supernode():
    # REQ-648: Edges crossing the collapsed boundary connect to/from the supernode.
    nodes, edges = _make_collapse_fixture()
    _, result_edges = _collapse_to_supernode(
        nodes, edges, group_attr="groupKey", collapsed_group_value="sales"
    )
    sources = {e["source"] for e in result_edges}
    targets = {e["target"] for e in result_edges}
    assert any("__collapsed" in s for s in sources)
    assert "n3" in targets


def test_intra_group_edges_dropped_on_collapse():
    # REQ-648: Intra-group edges (both endpoints collapsed) are dropped.
    nodes, edges = _make_collapse_fixture()
    _, result_edges = _collapse_to_supernode(
        nodes, edges, group_attr="groupKey", collapsed_group_value="sales"
    )
    # e3 (n1→n2, both in sales group) should be dropped
    assert len(result_edges) == 2  # only e1 and e2 cross boundary


def test_non_member_nodes_retained():
    # REQ-648: Non-member nodes (outside the collapsed group) remain in the graph.
    nodes, edges = _make_collapse_fixture()
    result_nodes, _ = _collapse_to_supernode(
        nodes, edges, group_attr="groupKey", collapsed_group_value="sales"
    )
    ids = {n["id"] for n in result_nodes}
    assert "n3" in ids


# ---------------------------------------------------------------------------
# REQ-649: Node size encoding — linear scaling between observed min/max.
# ---------------------------------------------------------------------------


def test_min_value_produces_base_size():
    # REQ-649: The node with the minimum value gets the base diameter.
    size = _scale_node_size(0.0, min_val=0.0, max_val=10.0, base=20.0, multiplier=3.0)
    assert size == 20.0


def test_max_value_produces_base_times_multiplier():
    # REQ-649: The node with the maximum value gets base * multiplier diameter.
    size = _scale_node_size(10.0, min_val=0.0, max_val=10.0, base=20.0, multiplier=3.0)
    assert math.isclose(size, 60.0)


def test_midpoint_value_produces_midpoint_size():
    # REQ-649: A value at the midpoint gets the midpoint size.
    size = _scale_node_size(5.0, min_val=0.0, max_val=10.0, base=20.0, multiplier=3.0)
    assert math.isclose(size, 40.0)


def test_uniform_values_produce_base_size():
    # REQ-649: When all values are equal (min==max), every node gets base size.
    size = _scale_node_size(5.0, min_val=5.0, max_val=5.0, base=20.0, multiplier=3.0)
    assert size == 20.0


def test_size_increases_monotonically_with_value():
    # REQ-649: Size encoding must be monotonically increasing with the property value.
    sizes = [
        _scale_node_size(v, min_val=0.0, max_val=100.0, base=10.0, multiplier=5.0)
        for v in (0, 25, 50, 75, 100)
    ]
    for i in range(len(sizes) - 1):
        assert sizes[i] <= sizes[i + 1]


def test_size_bounded_between_base_and_base_times_multiplier():
    # REQ-649: All node sizes must stay within [base, base*multiplier].
    base, multiplier = 15.0, 4.0
    for v in range(0, 101, 10):
        size = _scale_node_size(float(v), 0.0, 100.0, base, multiplier)
        assert base <= size <= base * multiplier
