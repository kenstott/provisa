# Copyright (c) 2025 Kenneth Stott
# Canary: c353b07a-cadf-4ff0-b08f-b54a6ea93423
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for API response flattener (Phase U)."""

import json

import pytest

from provisa.api_source.flattener import flatten_response, _navigate_path
from provisa.api_source.models import ApiColumn, ApiColumnType


def _col(name: str, col_type: ApiColumnType) -> ApiColumn:
    return ApiColumn(name=name, type=col_type)


# --- Root path navigation ---

def test_navigate_path_simple():
    data = {"data": {"users": [{"id": 1}]}}
    result = _navigate_path(data, "data.users")
    assert result == [{"id": 1}]


def test_navigate_path_none():
    data = [{"id": 1}]
    result = _navigate_path(data, None)
    assert result == [{"id": 1}]


def test_navigate_path_empty():
    data = {"items": []}
    result = _navigate_path(data, "")
    assert result == {"items": []}


def test_navigate_path_missing_key():
    data = {"data": {}}
    with pytest.raises(KeyError):
        _navigate_path(data, "data.users")


# --- Primitive flattening ---

def test_flatten_primitives():
    """Primitive types become native Python values."""
    data = [
        {"name": "Alice", "age": 30, "score": 9.5, "active": True},
    ]
    columns = [
        _col("name", ApiColumnType.string),
        _col("age", ApiColumnType.integer),
        _col("score", ApiColumnType.number),
        _col("active", ApiColumnType.boolean),
    ]
    rows = flatten_response(data, None, columns)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == 30
    assert rows[0]["score"] == 9.5
    assert rows[0]["active"] is True


def test_flatten_objects_to_jsonb():
    """Objects become JSON strings for JSONB storage."""
    data = [
        {"id": 1, "address": {"street": "123 Main", "city": "NY"}},
    ]
    columns = [
        _col("id", ApiColumnType.integer),
        _col("address", ApiColumnType.jsonb),
    ]
    rows = flatten_response(data, None, columns)
    assert rows[0]["id"] == 1
    parsed = json.loads(rows[0]["address"])
    assert parsed == {"street": "123 Main", "city": "NY"}


def test_flatten_arrays_to_jsonb():
    """Arrays become JSON strings for JSONB storage."""
    data = [{"tags": ["a", "b", "c"]}]
    columns = [_col("tags", ApiColumnType.jsonb)]
    rows = flatten_response(data, None, columns)
    assert json.loads(rows[0]["tags"]) == ["a", "b", "c"]


def test_flatten_null_values():
    """Missing/null values become None."""
    data = [{"name": None, "age": None}]
    columns = [
        _col("name", ApiColumnType.string),
        _col("age", ApiColumnType.integer),
    ]
    rows = flatten_response(data, None, columns)
    assert rows[0]["name"] is None
    assert rows[0]["age"] is None


# --- Root path + flattening ---

def test_flatten_with_root_path():
    """Root path navigates to nested data before flattening."""
    data = {"data": {"users": [{"id": 1, "name": "Alice"}]}}
    columns = [
        _col("id", ApiColumnType.integer),
        _col("name", ApiColumnType.string),
    ]
    rows = flatten_response(data, "data.users", columns)
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"


def test_flatten_single_object():
    """Single object at root is wrapped in a list."""
    data = {"id": 1, "name": "Alice"}
    columns = [
        _col("id", ApiColumnType.integer),
        _col("name", ApiColumnType.string),
    ]
    rows = flatten_response(data, None, columns)
    assert len(rows) == 1
    assert rows[0]["id"] == 1
