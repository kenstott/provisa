# Copyright (c) 2026 Kenneth Stott
# Canary: c9d0e1f2-a3b4-5678-2345-789012345678
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for API source response normalizers (Phase AO)."""

import pytest

from provisa.api_source.normalizers import get_normalizer, neo4j_tabular, sparql_bindings


class TestNeo4jTabular:
    """Tests use the Neo4j legacy HTTP transaction API format:
    {"results": [{"columns": [...], "data": [{"row": [...], "meta": [...]}]}], "errors": []}
    """

    def test_single_row(self):
        response = {
            "results": [
                {
                    "columns": ["name", "age"],
                    "data": [{"row": ["Alice", 30], "meta": []}],
                }
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows == [{"name": "Alice", "age": 30}]

    def test_multi_row(self):
        response = {
            "results": [
                {
                    "columns": ["id", "amount"],
                    "data": [
                        {"row": [1, 99.5], "meta": []},
                        {"row": [2, 42.0], "meta": []},
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows == [{"id": 1, "amount": 99.5}, {"id": 2, "amount": 42.0}]

    def test_empty_data(self):
        response = {"results": [{"columns": ["name"], "data": []}], "errors": []}
        rows = neo4j_tabular(response)
        assert rows == []

    def test_missing_results_key(self):
        rows = neo4j_tabular({})
        assert rows == []

    def test_null_value_in_row(self):
        response = {
            "results": [
                {
                    "columns": ["name", "email"],
                    "data": [{"row": ["Bob", None], "meta": []}],
                }
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows == [{"name": "Bob", "email": None}]

    def test_multiple_result_sets_merged(self):
        response = {
            "results": [
                {"columns": ["name"], "data": [{"row": ["Alice"], "meta": []}]},
                {"columns": ["name"], "data": [{"row": ["Bob"], "meta": []}]},
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows == [{"name": "Alice"}, {"name": "Bob"}]


class TestSparqlBindings:
    def test_literal_bindings(self):
        response = {
            "results": {
                "bindings": [
                    {"city": {"type": "literal", "value": "London"}},
                    {"city": {"type": "literal", "value": "Paris"}},
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows == [{"city": "London"}, {"city": "Paris"}]

    def test_uri_type_becomes_string(self):
        response = {
            "results": {
                "bindings": [
                    {
                        "subject": {
                            "type": "uri",
                            "value": "http://example.org/resource/1",
                        }
                    }
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows == [{"subject": "http://example.org/resource/1"}]

    def test_bnode_type(self):
        response = {
            "results": {
                "bindings": [{"x": {"type": "bnode", "value": "_:b0"}}]
            }
        }
        rows = sparql_bindings(response)
        assert rows == [{"x": "_:b0"}]

    def test_empty_bindings(self):
        response = {"results": {"bindings": []}}
        rows = sparql_bindings(response)
        assert rows == []

    def test_missing_results(self):
        rows = sparql_bindings({})
        assert rows == []

    def test_mixed_variables(self):
        response = {
            "results": {
                "bindings": [
                    {
                        "name": {"type": "literal", "value": "Alice"},
                        "age": {"type": "literal", "value": "30", "datatype": "xsd:integer"},
                        "homepage": {"type": "uri", "value": "http://alice.example.org"},
                    }
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows == [
            {"name": "Alice", "age": "30", "homepage": "http://alice.example.org"}
        ]


class TestGetNormalizer:
    def test_known_normalizer_neo4j(self):
        fn = get_normalizer("neo4j_tabular")
        assert fn is neo4j_tabular

    def test_known_normalizer_sparql(self):
        fn = get_normalizer("sparql_bindings")
        assert fn is sparql_bindings

    def test_unknown_normalizer_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown response_normalizer"):
            get_normalizer("does_not_exist")

    def test_unknown_normalizer_lists_available(self):
        with pytest.raises(ValueError, match="neo4j_tabular"):
            get_normalizer("bogus")
