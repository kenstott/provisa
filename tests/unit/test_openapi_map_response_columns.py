# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2b8e7a-4c1d-4a9b-9e3f-8d2c5a7b1e40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A JSON Schema response with only additionalProperties (a key->scalar map, e.g.
Petstore's /store/inventory returning {"available": 3, "sold": 12}) has no fixed
property names. pg_cache._normalize_rows already flattens such a response into
{"status": k, "count": v} rows; the column-inference helpers that determine what
columns the cache table is created with must agree, or the table is silently never
created (regression: "no such table" on every surface querying it)."""

from provisa.openapi.pg_cache import _schema_to_pg_cols
from provisa.openapi.register import _schema_to_columns

MAP_SCHEMA = {
    "type": "object",
    "additionalProperties": {"type": "integer", "format": "int32"},
}


def test_pg_cache_infers_status_count_columns_for_map_schema():
    assert _schema_to_pg_cols(MAP_SCHEMA) == [("status", "TEXT"), ("count", "BIGINT")]


def test_pg_cache_returns_columns_for_object_schema_unchanged():
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    assert _schema_to_pg_cols(schema) == [("id", "BIGINT")]


def test_register_infers_status_count_columns_for_map_schema():
    cols = _schema_to_columns(MAP_SCHEMA)
    assert cols == [
        {"name": "status", "type": "string"},
        {"name": "count", "type": "integer"},
    ]


def test_register_returns_columns_for_object_schema_unchanged():
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    assert _schema_to_columns(schema) == [{"name": "id", "type": "integer"}]
