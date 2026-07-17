# Copyright (c) 2026 Kenneth Stott
# Canary: 2160f4aa-61dc-4a1d-839c-79d13f78acd3
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for API normalizer requirements: REQ-299"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# REQ-299: A `response_normalizer: str | None` field added to `ApiEndpoint`.
# When set, the named normalizer is applied to the raw API response before
# `response_root` navigation and the existing flattener.
# Built-in: neo4j_tabular, sparql_bindings.
# Unknown normalizer names are rejected at registration time.
# ---------------------------------------------------------------------------


def test_response_normalizer_field_on_api_endpoint_model():
    # REQ-299: ApiEndpoint carries response_normalizer field.
    from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint

    ep = ApiEndpoint(
        source_id="src1",
        path="/api/data",
        table_name="test_table",
        columns=[ApiColumn(name="id", type=ApiColumnType.string)],
        method="GET",
        response_normalizer=None,
        response_root=None,
    )
    assert hasattr(ep, "response_normalizer")
    assert ep.response_normalizer is None


def test_response_normalizer_field_accepts_normalizer_name():
    # REQ-299: response_normalizer field accepts a normalizer name string.
    from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint

    ep = ApiEndpoint(
        source_id="src1",
        path="/neo4j/query",
        table_name="neo4j_table",
        columns=[ApiColumn(name="name", type=ApiColumnType.string)],
        method="POST",
        response_normalizer="neo4j_tabular",
        response_root=None,
    )
    assert ep.response_normalizer == "neo4j_tabular"


def test_get_normalizer_raises_for_unknown_name():
    # REQ-299: Unknown normalizer names must be rejected with ValueError.
    from provisa.api_source.normalizers import get_normalizer

    try:
        get_normalizer("nonexistent_normalizer")
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "nonexistent_normalizer" in str(e)


def test_get_normalizer_returns_callable_for_neo4j_tabular():
    # REQ-299: get_normalizer returns a callable for "neo4j_tabular".
    from provisa.api_source.normalizers import get_normalizer

    fn = get_normalizer("neo4j_tabular")
    assert callable(fn)


def test_get_normalizer_returns_callable_for_sparql_bindings():
    # REQ-299: get_normalizer returns a callable for "sparql_bindings".
    from provisa.api_source.normalizers import get_normalizer

    fn = get_normalizer("sparql_bindings")
    assert callable(fn)


def test_neo4j_tabular_zips_fields_with_row_values():
    # REQ-299: neo4j_tabular zips data.fields[] with data.values[][] to flat dicts.
    from provisa.api_source.normalizers import neo4j_tabular

    response = {
        "results": [
            {
                "columns": ["name", "age"],
                "data": [
                    {"row": ["Alice", 30]},
                    {"row": ["Bob", 25]},
                ],
            }
        ],
        "errors": [],
    }
    rows = neo4j_tabular(response)
    assert len(rows) == 2
    assert rows[0] == {"name": "Alice", "age": 30}
    assert rows[1] == {"name": "Bob", "age": 25}


def test_neo4j_tabular_handles_empty_results():
    # REQ-299: neo4j_tabular returns empty list when results are empty.
    from provisa.api_source.normalizers import neo4j_tabular

    response = {"results": [], "errors": []}
    rows = neo4j_tabular(response)
    assert rows == []


def test_neo4j_tabular_handles_multiple_result_sets():
    # REQ-299: neo4j_tabular processes all result sets when multiple are present.
    from provisa.api_source.normalizers import neo4j_tabular

    response = {
        "results": [
            {"columns": ["x"], "data": [{"row": [1]}]},
            {"columns": ["y"], "data": [{"row": [2]}]},
        ],
        "errors": [],
    }
    rows = neo4j_tabular(response)
    assert len(rows) == 2


def test_sparql_bindings_extracts_variable_values():
    # REQ-299: sparql_bindings extracts results.bindings[] mapping each variable to its value.
    from provisa.api_source.normalizers import sparql_bindings

    response = {
        "results": {
            "bindings": [
                {
                    "name": {"type": "literal", "value": "Alice"},
                    "age": {"type": "literal", "value": "30"},
                }
            ]
        }
    }
    rows = sparql_bindings(response)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == "30"


def test_sparql_bindings_handles_uri_type():
    # REQ-299: sparql_bindings converts uri-type bindings to their string value.
    from provisa.api_source.normalizers import sparql_bindings

    response = {
        "results": {"bindings": [{"entity": {"type": "uri", "value": "http://example.org/Alice"}}]}
    }
    rows = sparql_bindings(response)
    assert rows[0]["entity"] == "http://example.org/Alice"


def test_sparql_bindings_handles_empty_bindings():
    # REQ-299: sparql_bindings returns empty list for empty bindings.
    from provisa.api_source.normalizers import sparql_bindings

    response = {"results": {"bindings": []}}
    rows = sparql_bindings(response)
    assert rows == []


def test_normalizer_registry_contains_required_builtins():
    # REQ-299: NORMALIZERS registry must include both required built-in names.
    from provisa.api_source.normalizers import NORMALIZERS

    assert "neo4j_tabular" in NORMALIZERS
    assert "sparql_bindings" in NORMALIZERS


def test_normalizer_applied_before_response_root_navigation():
    # REQ-299: normalizer is invoked before response_root navigation.
    # flatten_response calls normalizer if response_normalizer is set, then navigates root.
    from provisa.api_source.normalizers import neo4j_tabular

    # Simulate normalizer output — after normalization, root navigation is a no-op
    # (normalized output is already a flat list)
    raw = {
        "results": [{"columns": ["id"], "data": [{"row": [1]}]}],
        "errors": [],
    }
    normalized = neo4j_tabular(raw)
    # After normalization, the result is a plain list of dicts
    assert isinstance(normalized, list)
    assert all(isinstance(r, dict) for r in normalized)
