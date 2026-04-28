# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-4567-89abcdef0123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Neo4j response normalizers."""

import pytest

from provisa.api_source.normalizers import (
    get_normalizer,
    neo4j_graph_nodes,
    neo4j_graph_rels,
    neo4j_legacy_cypher,
    neo4j_query_v2,
    neo4j_tabular,
)


class TestNeo4jLegacyCypher:
    """Legacy /db/data/cypher endpoint: {"columns": [...], "data": [[v1, v2], ...]}"""

    def test_single_row(self):
        response = {"columns": ["name", "age"], "data": [["Alice", 30]]}
        assert neo4j_legacy_cypher(response) == [{"name": "Alice", "age": 30}]

    def test_multi_row(self):
        response = {
            "columns": ["id", "amount"],
            "data": [[1, 99.5], [2, 42.0]],
        }
        assert neo4j_legacy_cypher(response) == [
            {"id": 1, "amount": 99.5},
            {"id": 2, "amount": 42.0},
        ]

    def test_empty_data(self):
        assert neo4j_legacy_cypher({"columns": ["name"], "data": []}) == []

    def test_missing_keys(self):
        assert neo4j_legacy_cypher({}) == []

    def test_null_value(self):
        response = {"columns": ["name", "email"], "data": [["Bob", None]]}
        assert neo4j_legacy_cypher(response) == [{"name": "Bob", "email": None}]

    def test_skips_non_list_rows(self):
        # Malformed entries are skipped gracefully
        response = {"columns": ["x"], "data": [["ok"], "bad"]}
        rows = neo4j_legacy_cypher(response)
        assert rows == [{"x": "ok"}]


class TestNeo4jGraphNodes:
    """Transaction API graph format — nodes only."""

    def test_single_node(self):
        response = {
            "results": [
                {
                    "columns": ["n"],
                    "data": [
                        {
                            "graph": {
                                "nodes": [
                                    {
                                        "id": "1",
                                        "labels": ["Person"],
                                        "properties": {"name": "Alice", "age": 30},
                                    }
                                ],
                                "relationships": [],
                            }
                        }
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_graph_nodes(response)
        assert rows == [{"_id": "1", "_labels": ["Person"], "name": "Alice", "age": 30}]

    def test_multiple_nodes(self):
        response = {
            "results": [
                {
                    "columns": ["n"],
                    "data": [
                        {
                            "graph": {
                                "nodes": [
                                    {"id": "1", "labels": ["Person"], "properties": {"name": "Alice"}},
                                    {"id": "2", "labels": ["Person"], "properties": {"name": "Bob"}},
                                ],
                                "relationships": [],
                            }
                        }
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_graph_nodes(response)
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

    def test_deduplicates_nodes_across_results(self):
        node = {"id": "1", "labels": ["X"], "properties": {"v": 1}}
        response = {
            "results": [
                {"columns": ["n"], "data": [{"graph": {"nodes": [node], "relationships": []}}]},
                {"columns": ["n"], "data": [{"graph": {"nodes": [node], "relationships": []}}]},
            ],
            "errors": [],
        }
        rows = neo4j_graph_nodes(response)
        assert len(rows) == 1

    def test_empty_response(self):
        assert neo4j_graph_nodes({}) == []

    def test_no_nodes_in_graph(self):
        response = {
            "results": [{"columns": ["n"], "data": [{"graph": {"nodes": [], "relationships": []}}]}],
            "errors": [],
        }
        assert neo4j_graph_nodes(response) == []

    def test_uses_element_id_fallback(self):
        response = {
            "results": [
                {
                    "columns": ["n"],
                    "data": [
                        {
                            "graph": {
                                "nodes": [
                                    {
                                        "elementId": "4:uuid:99",
                                        "labels": ["Thing"],
                                        "properties": {"x": 1},
                                    }
                                ],
                                "relationships": [],
                            }
                        }
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_graph_nodes(response)
        assert rows[0]["_id"] == "4:uuid:99"


class TestNeo4jGraphRels:
    """Transaction API graph format — relationships only."""

    def test_single_relationship(self):
        response = {
            "results": [
                {
                    "columns": ["r"],
                    "data": [
                        {
                            "graph": {
                                "nodes": [],
                                "relationships": [
                                    {
                                        "id": "9",
                                        "type": "KNOWS",
                                        "startNode": "1",
                                        "endNode": "2",
                                        "properties": {"since": 2020},
                                    }
                                ],
                            }
                        }
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_graph_rels(response)
        assert rows == [
            {"_id": "9", "_type": "KNOWS", "_start": "1", "_end": "2", "since": 2020}
        ]

    def test_no_properties(self):
        response = {
            "results": [
                {
                    "columns": ["r"],
                    "data": [
                        {
                            "graph": {
                                "nodes": [],
                                "relationships": [
                                    {"id": "5", "type": "HAS", "startNode": "1", "endNode": "3", "properties": {}},
                                ],
                            }
                        }
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_graph_rels(response)
        assert rows == [{"_id": "5", "_type": "HAS", "_start": "1", "_end": "3"}]

    def test_deduplicates_relationships(self):
        rel = {"id": "9", "type": "KNOWS", "startNode": "1", "endNode": "2", "properties": {}}
        response = {
            "results": [
                {"columns": ["r"], "data": [{"graph": {"nodes": [], "relationships": [rel]}}]},
                {"columns": ["r"], "data": [{"graph": {"nodes": [], "relationships": [rel]}}]},
            ],
            "errors": [],
        }
        assert len(neo4j_graph_rels(response)) == 1

    def test_empty_response(self):
        assert neo4j_graph_rels({}) == []


class TestNeo4jQueryV2:
    """Query API v2: {"data": {"fields": [...], "values": [[...], ...]}}"""

    def test_primitive_values(self):
        response = {
            "data": {
                "fields": ["name", "age"],
                "values": [["Alice", 30], ["Bob", 25]],
            },
            "bookmarks": [],
        }
        rows = neo4j_query_v2(response)
        assert rows == [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

    def test_node_object_flattened_to_properties(self):
        response = {
            "data": {
                "fields": ["person"],
                "values": [
                    [{"labels": ["Person"], "properties": {"name": "Phil", "age": 40}}]
                ],
            }
        }
        rows = neo4j_query_v2(response)
        assert rows == [{"person": {"name": "Phil", "age": 40}}]

    def test_mixed_primitive_and_node(self):
        response = {
            "data": {
                "fields": ["person", "name"],
                "values": [
                    [
                        {"labels": ["Person"], "properties": {"name": "Phil"}},
                        "Phil",
                    ]
                ],
            }
        }
        rows = neo4j_query_v2(response)
        assert rows[0]["name"] == "Phil"
        assert rows[0]["person"] == {"name": "Phil"}

    def test_empty_values(self):
        response = {"data": {"fields": ["x"], "values": []}}
        assert neo4j_query_v2(response) == []

    def test_missing_data_key(self):
        assert neo4j_query_v2({}) == []

    def test_null_value(self):
        response = {"data": {"fields": ["a", "b"], "values": [[None, 1]]}}
        assert neo4j_query_v2(response) == [{"a": None, "b": 1}]


class TestGetNormalizerRegistry:
    def test_all_neo4j_normalizers_registered(self):
        for name in ("neo4j_tabular", "neo4j_legacy_cypher", "neo4j_graph_nodes", "neo4j_graph_rels", "neo4j_query_v2"):
            fn = get_normalizer(name)
            assert callable(fn)

    def test_neo4j_legacy_cypher_is_correct_fn(self):
        assert get_normalizer("neo4j_legacy_cypher") is neo4j_legacy_cypher

    def test_neo4j_graph_nodes_is_correct_fn(self):
        assert get_normalizer("neo4j_graph_nodes") is neo4j_graph_nodes

    def test_neo4j_graph_rels_is_correct_fn(self):
        assert get_normalizer("neo4j_graph_rels") is neo4j_graph_rels

    def test_neo4j_query_v2_is_correct_fn(self):
        assert get_normalizer("neo4j_query_v2") is neo4j_query_v2

    def test_unknown_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown response_normalizer"):
            get_normalizer("does_not_exist")

    def test_error_lists_available_normalizers(self):
        with pytest.raises(ValueError, match="neo4j_tabular"):
            get_normalizer("bogus")
