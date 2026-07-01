# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for REQ-806: rel_ids composite_id formatting and pure id-substitution logic."""

from __future__ import annotations


from provisa.cypher.assembler import _apply_rel_id_map, _walk_for_edges


class TestCompositeIdFormat:
    """Verify edge composite_id string format: 'Type:startPk-endPk'."""

    def test_edge_collected_with_correct_composite_id(self):
        edge = {
            "identity": "OWNS:pk1-pk2",
            "type": "OWNS",
            "startNode": {"id": "pk1"},
            "endNode": {"id": "pk2"},
            "properties": {},
        }
        out: dict = {}
        _walk_for_edges(edge, out)
        assert "OWNS:pk1-pk2" in out

    def test_composite_id_rel_type_extracted(self):
        edge = {
            "identity": "SUBMITTED_BY:1-1",
            "type": "SUBMITTED_BY",
            "startNode": {"id": "1"},
            "endNode": {"id": "1"},
            "properties": {"since": 2024},
        }
        out: dict = {}
        _walk_for_edges(edge, out)
        rel_type, _ = out["SUBMITTED_BY:1-1"]
        assert rel_type == "SUBMITTED_BY"

    def test_composite_id_properties_extracted(self):
        edge = {
            "identity": "KNOWS:a-b",
            "type": "KNOWS",
            "startNode": {"id": "a"},
            "endNode": {"id": "b"},
            "properties": {"weight": 0.9},
        }
        out: dict = {}
        _walk_for_edges(edge, out)
        _, props = out["KNOWS:a-b"]
        assert props == {"weight": 0.9}

    def test_non_edge_dict_not_collected(self):
        node = {"id": "Label|123", "label": "Person", "properties": {}}
        out: dict = {}
        _walk_for_edges(node, out)
        assert out == {}

    def test_nested_edge_collected(self):
        row = {
            "r": {
                "identity": "FRIEND_OF:7-9",
                "type": "FRIEND_OF",
                "startNode": {"id": "7"},
                "endNode": {"id": "9"},
                "properties": {},
            }
        }
        out: dict = {}
        _walk_for_edges(row, out)
        assert "FRIEND_OF:7-9" in out

    def test_edge_list_collected(self):
        edges = [
            {
                "identity": "A:1-2",
                "type": "A",
                "startNode": {},
                "endNode": {},
                "properties": {},
            },
            {
                "identity": "B:3-4",
                "type": "B",
                "startNode": {},
                "endNode": {},
                "properties": {},
            },
        ]
        out: dict = {}
        _walk_for_edges(edges, out)
        assert "A:1-2" in out
        assert "B:3-4" in out

    def test_missing_startnode_not_collected(self):
        """A dict with identity+type but no startNode is not an edge."""
        not_edge = {
            "identity": "OWNS:pk1-pk2",
            "type": "OWNS",
            "properties": {},
        }
        out: dict = {}
        _walk_for_edges(not_edge, out)
        assert out == {}

    def test_integer_identity_not_collected(self):
        """Already-converted integer identity must not be re-collected."""
        edge = {
            "identity": 42,
            "type": "OWNS",
            "startNode": {},
            "properties": {},
        }
        out: dict = {}
        _walk_for_edges(edge, out)
        assert out == {}


class TestApplyRelIdMap:
    """Verify _apply_rel_id_map replaces composite string identities with integers."""

    def test_identity_replaced_in_edge(self):
        edge = {
            "identity": "OWNS:pk1-pk2",
            "type": "OWNS",
            "startNode": {"id": 1},
            "endNode": {"id": 2},
            "properties": {},
        }
        rel_map = {"OWNS:pk1-pk2": 99}
        result = _apply_rel_id_map(edge, rel_map)
        assert result["identity"] == 99

    def test_unmapped_identity_unchanged(self):
        edge = {
            "identity": "KNOWS:x-y",
            "type": "KNOWS",
            "startNode": {},
            "endNode": {},
            "properties": {},
        }
        rel_map = {}
        result = _apply_rel_id_map(edge, rel_map)
        assert result["identity"] == "KNOWS:x-y"

    def test_scalar_unchanged(self):
        assert _apply_rel_id_map(42, {"OWNS:1-2": 5}) == 42
        assert _apply_rel_id_map("hello", {}) == "hello"

    def test_nested_edge_in_list(self):
        rows = [
            {
                "identity": "LIKES:3-4",
                "type": "LIKES",
                "startNode": {},
                "endNode": {},
                "properties": {},
            }
        ]
        rel_map = {"LIKES:3-4": 77}
        result = _apply_rel_id_map(rows, rel_map)
        assert result[0]["identity"] == 77

    def test_empty_map_is_noop(self):
        edge = {
            "identity": "A:1-2",
            "type": "A",
            "startNode": {},
            "endNode": {},
            "properties": {},
        }
        result = _apply_rel_id_map(edge, {})
        assert result["identity"] == "A:1-2"

    def test_dict_without_type_key_identity_not_replaced(self):
        """_apply_rel_id_map requires both 'identity' and 'type' to replace."""
        d = {"identity": "OWNS:1-2", "other": "value"}
        rel_map = {"OWNS:1-2": 5}
        result = _apply_rel_id_map(d, rel_map)
        assert result["identity"] == "OWNS:1-2"
