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
property names. The registered columns are {status, count}, and the flattener turns such
an answer into {"status": k, "count": v} rows under exactly those columns; the two must
agree, or every row of the table is empty (regression: "no such table" on every surface
querying it, when the cache table was created from different columns)."""

from provisa.api_source.flattener import flatten_response
from provisa.api_source.models import ApiColumn, ApiColumnType
from provisa.openapi.register import _schema_to_columns

MAP_SCHEMA = {
    "type": "object",
    "additionalProperties": {"type": "integer", "format": "int32"},
}


def test_a_map_answer_flattens_to_status_count_rows_under_the_registered_columns():
    columns = [
        ApiColumn(name=c["name"], type=ApiColumnType(c["type"]))
        for c in _schema_to_columns(MAP_SCHEMA)
    ]
    assert flatten_response({"available": 3, "sold": 12}, None, columns) == [
        {"status": "available", "count": 3},
        {"status": "sold", "count": 12},
    ]


def test_an_object_answer_flattens_to_one_row_of_its_properties():
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    columns = [
        ApiColumn(name=c["name"], type=ApiColumnType(c["type"])) for c in _schema_to_columns(schema)
    ]
    assert flatten_response({"id": 7}, None, columns) == [{"id": 7}]


def test_register_infers_status_count_columns_for_map_schema():
    cols = _schema_to_columns(MAP_SCHEMA)
    assert cols == [
        {"name": "status", "type": "string"},
        {"name": "count", "type": "integer"},
    ]


def test_register_returns_columns_for_object_schema_unchanged():
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    assert _schema_to_columns(schema) == [{"name": "id", "type": "integer"}]
