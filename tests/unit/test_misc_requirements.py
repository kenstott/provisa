# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for misc requirements: REQ-299, REQ-536, REQ-597"""

from __future__ import annotations

import time

import pytest


# ── REQ-299: Query-API Sources — response_normalizer field and named normalizers ──


class TestREQ299ResponseNormalizer:
    """REQ-299: ApiEndpoint has response_normalizer field; neo4j_tabular and
    sparql_bindings normalizers unwrap API envelopes; unknown names rejected."""

    def test_api_endpoint_has_response_normalizer_field(self):
        # REQ-299
        from provisa.api_source.models import ApiEndpoint

        ep = ApiEndpoint(
            source_id="src1",
            path="/db/neo4j/tx/commit",
            table_name="nodes",
            columns=[],
            response_normalizer="neo4j_tabular",
        )
        assert ep.response_normalizer == "neo4j_tabular"

    def test_api_endpoint_response_normalizer_defaults_to_none(self):
        # REQ-299
        from provisa.api_source.models import ApiEndpoint

        ep = ApiEndpoint(source_id="src1", path="/items", table_name="items", columns=[])
        assert ep.response_normalizer is None

    def test_neo4j_tabular_zips_fields_with_values(self):
        # REQ-299
        from provisa.api_source.normalizers import neo4j_tabular

        response = {
            "results": [
                {
                    "columns": ["name", "age"],
                    "data": [
                        {"row": ["Alice", 30], "meta": []},
                        {"row": ["Bob", 25], "meta": []},
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows == [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

    def test_neo4j_tabular_returns_list_of_dicts(self):
        # REQ-299
        from provisa.api_source.normalizers import neo4j_tabular

        response = {
            "results": [{"columns": ["id"], "data": [{"row": [1], "meta": []}]}],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert isinstance(rows, list)
        assert all(isinstance(r, dict) for r in rows)

    def test_sparql_bindings_extracts_bindings_and_maps_to_values(self):
        # REQ-299
        from provisa.api_source.normalizers import sparql_bindings

        response = {
            "results": {
                "bindings": [
                    {
                        "name": {"type": "literal", "value": "Alice"},
                        "age": {"type": "literal", "value": "30"},
                    },
                    {
                        "name": {"type": "literal", "value": "Bob"},
                        "age": {"type": "literal", "value": "25"},
                    },
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows == [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]

    def test_sparql_bindings_maps_uri_type_to_value(self):
        # REQ-299
        from provisa.api_source.normalizers import sparql_bindings

        response = {
            "results": {
                "bindings": [{"subject": {"type": "uri", "value": "http://example.org/Alice"}}]
            }
        }
        rows = sparql_bindings(response)
        assert rows == [{"subject": "http://example.org/Alice"}]

    def test_get_normalizer_returns_callable_for_neo4j_tabular(self):
        # REQ-299
        from provisa.api_source.normalizers import get_normalizer

        fn = get_normalizer("neo4j_tabular")
        assert callable(fn)

    def test_get_normalizer_returns_callable_for_sparql_bindings(self):
        # REQ-299
        from provisa.api_source.normalizers import get_normalizer

        fn = get_normalizer("sparql_bindings")
        assert callable(fn)

    def test_get_normalizer_raises_for_unknown_name(self):
        # REQ-299
        from provisa.api_source.normalizers import get_normalizer

        with pytest.raises(ValueError, match="Unknown response_normalizer"):
            get_normalizer("not_a_real_normalizer")

    def test_normalizer_registry_contains_neo4j_tabular_and_sparql_bindings(self):
        # REQ-299
        from provisa.api_source.normalizers import NORMALIZERS

        assert "neo4j_tabular" in NORMALIZERS
        assert "sparql_bindings" in NORMALIZERS

    def test_neo4j_tabular_applied_before_response_root_navigation(self):
        # REQ-299: normalizer produces flat dicts that subsequent flattening can use
        from provisa.api_source.normalizers import neo4j_tabular

        response = {
            "results": [
                {
                    "columns": ["city", "pop"],
                    "data": [{"row": ["NYC", 8_000_000], "meta": []}],
                }
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        # Each row must be a flat dict keyed by column name
        assert rows[0]["city"] == "NYC"
        assert rows[0]["pop"] == 8_000_000


# ── REQ-536: Cache status headers on every data response ───────────────────────


class TestREQ536CacheHeaders:
    """REQ-536: X-Provisa-Cache: HIT|MISS on every response; X-Provisa-Cache-Age
    present on HITs only, indicating seconds since cached."""

    def test_cache_miss_returns_miss_header(self):
        # REQ-536
        from provisa.cache.middleware import build_cache_headers

        headers = build_cache_headers(None)
        assert headers["X-Provisa-Cache"] == "MISS"

    def test_cache_miss_has_no_age_header(self):
        # REQ-536
        from provisa.cache.middleware import build_cache_headers

        headers = build_cache_headers(None)
        assert "X-Provisa-Cache-Age" not in headers

    def test_cache_hit_returns_hit_header(self):
        # REQ-536
        from provisa.cache.middleware import build_cache_headers
        from provisa.cache.store import CachedResult

        cached = CachedResult(data=b"[]", cached_at=time.time() - 10, ttl=300)
        headers = build_cache_headers(cached)
        assert headers["X-Provisa-Cache"] == "HIT"

    def test_cache_hit_includes_age_header(self):
        # REQ-536
        from provisa.cache.middleware import build_cache_headers
        from provisa.cache.store import CachedResult

        cached = CachedResult(data=b"[]", cached_at=time.time() - 42, ttl=300)
        headers = build_cache_headers(cached)
        assert "X-Provisa-Cache-Age" in headers

    def test_cache_hit_age_header_is_seconds_since_cached(self):
        # REQ-536
        from provisa.cache.middleware import build_cache_headers
        from provisa.cache.store import CachedResult

        age_offset = 30
        cached = CachedResult(data=b"[]", cached_at=time.time() - age_offset, ttl=300)
        headers = build_cache_headers(cached)
        age = int(headers["X-Provisa-Cache-Age"])
        # Allow ±2s clock tolerance
        assert abs(age - age_offset) <= 2

    def test_cache_hit_age_header_is_string(self):
        # REQ-536
        from provisa.cache.middleware import build_cache_headers
        from provisa.cache.store import CachedResult

        cached = CachedResult(data=b"[]", cached_at=time.time() - 5, ttl=300)
        headers = build_cache_headers(cached)
        assert isinstance(headers["X-Provisa-Cache-Age"], str)

    def test_x_provisa_cache_header_name_is_exact(self):
        # REQ-536: header name must be exactly X-Provisa-Cache
        from provisa.cache.middleware import build_cache_headers

        headers = build_cache_headers(None)
        assert "X-Provisa-Cache" in headers

    def test_cached_result_age_seconds_property(self):
        # REQ-536: CachedResult.age_seconds must report elapsed time
        from provisa.cache.store import CachedResult

        cached = CachedResult(data=b"x", cached_at=time.time() - 60, ttl=300)
        assert abs(cached.age_seconds - 60) <= 2


# ── REQ-597: GraphQL remote field_overrides reclassify query fields ────────────


class TestREQ597FieldOverrides:
    """REQ-597: field_overrides map reclassifies query fields as mutations at
    registration time; takes priority over structural classification; mutation
    fields have no override path."""

    def _minimal_schema(
        self, query_fields: list[dict], mutation_fields: list[dict] | None = None
    ) -> dict:
        types = [
            {
                "name": "Query",
                "kind": "OBJECT",
                "fields": query_fields,
            },
            {
                "name": "Payload",
                "kind": "OBJECT",
                "fields": [
                    {
                        "name": "id",
                        "type": {"kind": "SCALAR", "name": "ID", "ofType": None},
                        "description": None,
                        "args": [],
                    }
                ],
                "description": None,
            },
        ]
        schema: dict = {
            "queryType": {"name": "Query"},
            "types": types,
        }
        if mutation_fields is not None:
            types.append({"name": "Mutation", "kind": "OBJECT", "fields": mutation_fields})
            schema["mutationType"] = {"name": "Mutation"}
        return schema

    def _object_field(self, name: str) -> dict:
        return {
            "name": name,
            "type": {"kind": "OBJECT", "name": "Payload", "ofType": None},
            "description": None,
            "args": [],
        }

    def test_query_object_field_without_override_maps_to_table(self):
        # REQ-597
        from provisa.graphql_remote.mapper import map_schema

        schema = self._minimal_schema([self._object_field("users")])
        tables, functions, _ = map_schema(schema, namespace="", source_id="src1")
        table_names = [t["name"] for t in tables]
        assert "users" in table_names

    def test_field_override_mutation_reclassifies_query_field_as_function(self):
        # REQ-597: override takes priority — query OBJECT field becomes function
        from provisa.graphql_remote.mapper import map_schema

        schema = self._minimal_schema([self._object_field("createUser")])
        tables, functions, _ = map_schema(
            schema,
            namespace="",
            source_id="src1",
            field_overrides={"createUser": "mutation"},
        )
        table_names = [t["name"] for t in tables]
        function_names = [f["name"] for f in functions]
        assert "createUser" not in table_names
        assert "createUser" in function_names

    def test_field_override_takes_priority_over_structural_classification(self):
        # REQ-597: structural classification would make this a table; override wins
        from provisa.graphql_remote.mapper import map_schema

        schema = self._minimal_schema([self._object_field("lookupRecord")])
        # Without override → table
        tables_no_override, funcs_no_override, _ = map_schema(
            schema, namespace="", source_id="src1"
        )
        assert any(t["name"] == "lookupRecord" for t in tables_no_override)
        assert not any(f["name"] == "lookupRecord" for f in funcs_no_override)

        # With override → function
        tables_with_override, funcs_with_override, _ = map_schema(
            schema,
            namespace="",
            source_id="src1",
            field_overrides={"lookupRecord": "mutation"},
        )
        assert not any(t["name"] == "lookupRecord" for t in tables_with_override)
        assert any(f["name"] == "lookupRecord" for f in funcs_with_override)

    def test_mutation_fields_are_always_functions_regardless_of_overrides(self):
        # REQ-597: mutation fields have no override path — always functions
        from provisa.graphql_remote.mapper import map_schema

        mut_field = self._object_field("deleteUser")
        schema = self._minimal_schema([], mutation_fields=[mut_field])
        tables, functions, _ = map_schema(
            schema,
            namespace="",
            source_id="src1",
            field_overrides={"deleteUser": "query"},  # override has no effect on mutations
        )
        function_names = [f["name"] for f in functions]
        table_names = [t["name"] for t in tables]
        assert "deleteUser" in function_names
        assert "deleteUser" not in table_names

    def test_field_overrides_none_defaults_to_empty_map(self):
        # REQ-597: field_overrides=None should behave identically to {}
        from provisa.graphql_remote.mapper import map_schema

        schema = self._minimal_schema([self._object_field("items")])
        tables_none, _, _ = map_schema(schema, namespace="", source_id="src1", field_overrides=None)
        tables_empty, _, _ = map_schema(schema, namespace="", source_id="src1", field_overrides={})
        assert [t["name"] for t in tables_none] == [t["name"] for t in tables_empty]

    def test_only_query_fields_can_be_reclassified_as_mutations(self):
        # REQ-597: only query-type fields can be reclassified; this verifies
        # that applying a mutation override on a query OBJECT field moves it
        # into functions — confirming the one-way override path.
        from provisa.graphql_remote.mapper import map_schema

        schema = self._minimal_schema([self._object_field("profile")])
        _, funcs_overridden, _ = map_schema(
            schema,
            namespace="",
            source_id="src1",
            field_overrides={"profile": "mutation"},
        )
        assert any(f["name"] == "profile" for f in funcs_overridden)
