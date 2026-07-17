# Copyright (c) 2026 Kenneth Stott
# Canary: d236426b-e212-4d03-a283-8814f71bf568
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-252 — NoSQL schema discovery: ES live mapping + explicit-column precedence."""

from unittest.mock import MagicMock, patch

import pytest

from provisa.discovery.column_inference import merge_discovered_columns
from provisa.elasticsearch.source import (
    discover_schema,
    extract_mapping_properties,
    fetch_index_mapping,
)


class TestMergePrecedence:
    def test_discovered_fill_when_no_explicit(self):
        discovered = [{"name": "a"}, {"name": "b"}]
        merged = merge_discovered_columns([], discovered)
        assert [c["name"] for c in merged] == ["a", "b"]

    def test_explicit_takes_precedence(self):
        explicit = [{"name": "a", "type": "integer"}]
        discovered = [{"name": "a", "type": "varchar"}, {"name": "b"}]
        merged = merge_discovered_columns(explicit, discovered)
        # explicit "a" kept (integer), discovered "a" dropped, "b" appended
        assert merged[0] == {"name": "a", "type": "integer"}
        assert {c["name"] for c in merged} == {"a", "b"}
        assert len(merged) == 2

    def test_precedence_is_case_insensitive(self):
        explicit = [{"name": "Id"}]
        discovered = [{"name": "id"}, {"name": "name"}]
        merged = merge_discovered_columns(explicit, discovered)
        assert {c["name"] for c in merged} == {"Id", "name"}

    def test_supports_model_like_objects(self):
        col = MagicMock()
        col.name = "x"
        merged = merge_discovered_columns([col], [{"name": "x"}, {"name": "y"}])
        assert len(merged) == 2  # discovered "x" dropped


class TestESMappingBridge:
    def test_extract_properties_by_index(self):
        resp = {"orders": {"mappings": {"properties": {"id": {"type": "long"}}}}}
        props = extract_mapping_properties(resp, "orders")
        assert "id" in props

    def test_extract_properties_single_key_fallback(self):
        # request used an alias; response keyed by the concrete index
        resp = {"orders-000001": {"mappings": {"properties": {"id": {"type": "long"}}}}}
        props = extract_mapping_properties(resp, "orders")
        assert "id" in props

    def test_extract_properties_missing_raises(self):
        with pytest.raises(ValueError):
            extract_mapping_properties({}, "orders")

    def test_extract_properties_no_mappings_raises(self):
        with pytest.raises(ValueError):
            extract_mapping_properties({"orders": {}}, "orders")

    def test_fetch_index_mapping_calls_es_and_flattens(self):
        resp = MagicMock()
        resp.json.return_value = {
            "orders": {
                "mappings": {
                    "properties": {
                        "id": {"type": "long"},
                        "customer": {"properties": {"name": {"type": "text"}}},
                    }
                }
            }
        }
        resp.raise_for_status = MagicMock()
        with patch("httpx.get", return_value=resp) as httpx_get:
            props = fetch_index_mapping("localhost", 9200, "orders")
        httpx_get.assert_called_once()
        assert httpx_get.call_args.args[0] == "http://localhost:9200/orders/_mapping"
        cols = {c["name"]: c["type"] for c in discover_schema(props)}
        assert cols["id"] == "BIGINT"
        assert "customer_name" in cols

    def test_fetch_index_mapping_ssl_scheme(self):
        resp = MagicMock()
        resp.json.return_value = {"i": {"mappings": {"properties": {"x": {"type": "keyword"}}}}}
        resp.raise_for_status = MagicMock()
        with patch("httpx.get", return_value=resp) as httpx_get:
            fetch_index_mapping("h", 9243, "i", use_ssl=True)
        assert httpx_get.call_args.args[0] == "https://h:9243/i/_mapping"
