# Copyright (c) 2026 Kenneth Stott
# Canary: 46b045ea-d2ea-4d89-bdd0-f2283c1aa6ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/executor/serialize.py.

Pure functions — construct real ColumnRef/CompiledQuery/row inputs and
assert on the serialized output shape.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from provisa.compiler.sql_gen import ColumnRef, CompiledQuery
from provisa.executor.serialize import (
    _convert_value,
    json_value,
    serialize_connection,
)
from tests.unit.json_encoder.rows import serialize_aggregate, serialize_rows


def _cr(
    field_name, column=None, nested_in=None, cardinality=None, is_agg=False, alias=None, json=None
):
    # A nodes / relationship column (is_agg) is one the statement builds as JSON: the compiler
    # marks it json (REQ-1867).
    return ColumnRef(
        alias=alias,
        column=column or field_name,
        field_name=field_name,
        nested_in=nested_in,
        cardinality=cardinality,
        is_agg=is_agg,
        json=is_agg if json is None else json,
    )


# ---------------------------------------------------------------------------
# _convert_value / json_value / the encoder's row identity
# ---------------------------------------------------------------------------


class TestConvertValue:
    # REQ-1867 (amended 2026-11-02): a DECIMAL is the JSON string of its exact digits. Before, it
    # was a float, or an int made from one.
    def test_decimal_integral(self):
        assert _convert_value(Decimal("5")) == "5"

    def test_decimal_fractional(self):
        assert _convert_value(Decimal("5.50")) == "5.50"

    def test_date_isoformat(self):
        d = date(2024, 1, 15)
        assert _convert_value(d) == "2024-01-15"

    def test_datetime_isoformat(self):
        dt = datetime(2024, 1, 15, 10, 30)
        assert _convert_value(dt) == dt.isoformat()

    # REQ-1867: text is the string it is, even when it reads as JSON. Before, it was parsed.
    def test_json_looking_text_stays_a_string(self):
        assert _convert_value('{"a": 1}') == '{"a": 1}'

    def test_json_looking_array_text_stays_a_string(self):
        assert _convert_value("[1, 2, 3]") == "[1, 2, 3]"

    def test_invalid_json_string_passthrough(self):
        assert _convert_value("{not valid json") == "{not valid json"

    def test_plain_string_passthrough(self):
        assert _convert_value("hello") == "hello"

    def test_none_passthrough(self):
        assert _convert_value(None) is None

    def test_native_dict_recursed(self):
        assert _convert_value({"x": Decimal("2")}) == {"x": "2"}

    def test_native_list_recursed(self):
        assert _convert_value([Decimal("2"), Decimal("3.5")]) == ["2", "3.5"]

    def test_int_passthrough(self):
        assert _convert_value(42) == 42

    def test_native_string_inside_a_dict_is_not_parsed(self):
        assert _convert_value({"a": "[1]"}) == {"a": "[1]"}


class TestJsonValue:
    """A JSON-typed column's value (REQ-1867): its text parsed, its strings left as text."""

    def test_json_text_is_parsed(self):
        result = json_value('{"items": [1, 2], "count": 2}', "f")
        assert result == {"items": [1, 2], "count": 2}

    def test_a_driver_parsed_value_is_kept(self):
        assert json_value({"a": Decimal("1.5")}, "f") == {"a": "1.5"}

    def test_a_string_inside_is_not_parsed_again(self):
        assert json_value('{"a": "[1]"}', "f") == {"a": "[1]"}

    def test_a_json_text_path_is_parsed(self):
        # Trino's json_format makes a nested relationship's list text inside its parent.
        got = json_value('[{"id": 1, "items": "[{\\"n\\": 2}]"}]', "f", frozenset({"items"}))
        assert got == [{"id": 1, "items": [{"n": 2}]}]


class TestRowIdentity:
    """Rows are one when they are the same typed value (REQ-2194, REQ-1867): the Rust encoder's
    dedup key, which replaced ``_to_hashable``'s ``json.dumps(default=str)`` text."""

    def test_equal_objects_are_one_row(self):
        cols = [_cr("doc", json=True)]
        rows = [('{"b": 1, "a": 2}',), ('{"b":1,"a":2}',)]
        assert serialize_rows(rows, cols, "t")["data"]["t"] == [{"doc": {"b": 1, "a": 2}}]

    def test_an_object_and_its_text_are_two_rows(self):
        cols = [_cr("doc", json=True)]
        rows = [('{"a": 1}',), ('"{\\"a\\": 1}"',)]
        assert serialize_rows(rows, cols, "t")["data"]["t"] == [
            {"doc": {"a": 1}},
            {"doc": '{"a": 1}'},
        ]

    def test_a_number_and_its_digits_are_two_rows(self):
        cols = [_cr("v", json=True)]
        rows = [("5",), ('"5"',), ("5.0",), ("5",)]
        assert serialize_rows(rows, cols, "t")["data"]["t"] == [{"v": 5}, {"v": "5"}, {"v": 5.0}]


# ---------------------------------------------------------------------------
# serialize_rows — flat (no nesting)
# ---------------------------------------------------------------------------


class TestSerializeRowsFlat:
    def test_simple_flat_rows(self):
        columns = [_cr("id"), _cr("name")]
        rows = [(1, "Rex"), (2, "Fido")]
        result = serialize_rows(rows, columns, "pets")
        assert result == {"data": {"pets": [{"id": 1, "name": "Rex"}, {"id": 2, "name": "Fido"}]}}

    def test_result_limit_applied(self):
        columns = [_cr("id")]
        rows = [(1,), (2,), (3,)]
        result = serialize_rows(rows, columns, "pets", result_limit=2)
        assert len(result["data"]["pets"]) == 2

    def test_many_to_one_nesting(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="owner", cardinality="many-to-one"),
        ]
        rows = [(1, "Alice")]
        result = serialize_rows(rows, columns, "pets")
        assert result == {"data": {"pets": [{"id": 1, "owner": {"name": "Alice"}}]}}

    def test_many_to_one_all_none_is_null(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="owner", cardinality="many-to-one"),
        ]
        rows = [(1, None)]
        result = serialize_rows(rows, columns, "pets")
        assert result["data"]["pets"][0]["owner"] is None

    def test_group_by_no_root_cols(self):
        # all columns nested (group_by / aggregate case with no root_cols)
        columns = [_cr("region", nested_in="groupKey"), _cr("count", nested_in="aggregate")]
        rows = [("east", 5), ("west", 3)]
        result = serialize_rows(rows, columns, "sales")
        pets = result["data"]["sales"]
        assert len(pets) == 2
        assert pets[0] == {"groupKey": {"region": "east"}, "aggregate": {"count": 5}}

    def test_truncated_many_to_one_warns(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="owner", cardinality="many-to-one"),
        ]
        rows = [(1, "Alice"), (1, "Bob")]
        result = serialize_rows(rows, columns, "pets")
        assert len(result["data"]["pets"]) == 1
        assert "extensions" in result
        assert result["extensions"]["warnings"][0]["path"] == "owner"

    def test_agg_cols_excluded_from_root_key(self):
        columns = [_cr("id"), _cr("cnt", is_agg=True)]
        # is_agg root col excluded from grouping key, but since it's a root col
        # (nested_in=None) it still appears in output.
        rows = [(1, 5), (1, 5)]
        result = serialize_rows(rows, columns, "pets")
        assert len(result["data"]["pets"]) == 1


# ---------------------------------------------------------------------------
# serialize_rows — one-to-many
# ---------------------------------------------------------------------------


class TestSerializeRowsOneToMany:
    def test_one_to_many_grouping(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="toys", cardinality="one-to-many"),
        ]
        rows = [(1, "Ball"), (1, "Bone"), (2, "Mouse")]
        result = serialize_rows(rows, columns, "pets")
        pets = result["data"]["pets"]
        assert pets[0] == {"id": 1, "toys": [{"name": "Ball"}, {"name": "Bone"}]}
        assert pets[1] == {"id": 2, "toys": [{"name": "Mouse"}]}

    def test_one_to_many_all_none_row_skipped(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="toys", cardinality="one-to-many"),
        ]
        rows = [(1, None)]
        result = serialize_rows(rows, columns, "pets")
        assert result["data"]["pets"][0]["toys"] == []

    def test_one_to_many_result_limit(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="toys", cardinality="one-to-many"),
        ]
        rows = [(1, "Ball"), (2, "Mouse"), (3, "Yarn")]
        result = serialize_rows(rows, columns, "pets", result_limit=1)
        assert len(result["data"]["pets"]) == 1

    def test_one_to_many_no_dupes(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="toys", cardinality="one-to-many"),
        ]
        rows = [(1, "Ball"), (1, "Ball")]
        result = serialize_rows(rows, columns, "pets")
        # duplicate child items should not be appended twice
        assert result["data"]["pets"][0]["toys"] == [{"name": "Ball"}]

    def test_nested_one_to_many_two_levels(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="toys", cardinality="one-to-many"),
            _cr("color", nested_in="toys.tags", cardinality="one-to-many"),
        ]
        rows = [(1, "Ball", "red"), (1, "Ball", "blue")]
        result = serialize_rows(rows, columns, "pets")
        toys = result["data"]["pets"][0]["toys"]
        assert len(toys) >= 1


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# serialize_aggregate
# ---------------------------------------------------------------------------


class TestSerializeAggregate:
    def test_basic_aggregate_no_nodes(self):
        agg_columns = [_cr("count")]
        agg_rows = [(5,)]
        result = serialize_aggregate(agg_rows, agg_columns, "orders")
        assert result == {"data": {"orders": {"aggregate": {"count": 5}}}}

    def test_aggregate_with_sub_path(self):
        agg_columns = [_cr("amount", nested_in="aggregate.sum")]
        agg_rows = [(Decimal("100.00"),)]
        result = serialize_aggregate(agg_rows, agg_columns, "orders")
        assert result["data"]["orders"]["aggregate"]["sum"]["amount"] == "100.00"

    def test_aggregate_empty_rows(self):
        result = serialize_aggregate([], [_cr("count")], "orders")
        assert result == {"data": {"orders": {"aggregate": {}}}}

    def test_aggregate_with_nodes(self):
        # Its nodes are a JSON list column of its one row (aggregates._nodes_column).
        agg_columns = [
            _cr("count"),
            _cr("nodes", cardinality="one-to-many", is_agg=True),
        ]
        agg_rows = [(2, '[{"id": 1, "name": "Rex"}, {"id": 2, "name": "Fido"}]')]
        result = serialize_aggregate(agg_rows, agg_columns, "orders")
        assert result["data"]["orders"]["aggregate"] == {"count": 2}
        assert result["data"]["orders"]["nodes"] == [
            {"id": 1, "name": "Rex"},
            {"id": 2, "name": "Fido"},
        ]

    def test_custom_agg_alias(self):
        agg_columns = [_cr("count", nested_in="derived")]
        agg_rows = [(3,)]
        result = serialize_aggregate(agg_rows, agg_columns, "orders", agg_alias="derived")
        assert result == {"data": {"orders": {"derived": {"count": 3}}}}


# ---------------------------------------------------------------------------
# group-by rows: a group's nodes are a column of its own row
# ---------------------------------------------------------------------------


class TestSerializeGroupBy:
    def test_group_by_without_nodes(self):
        columns = [_cr("region", nested_in="groupKey")]
        rows = [("east",)]
        result = serialize_rows(rows, columns, "sales")
        assert result == {"data": {"sales": [{"groupKey": {"region": "east"}}]}}

    def test_group_by_with_nodes(self):
        columns = [
            _cr("region", nested_in="groupKey"),
            _cr("nodes", cardinality="one-to-many", is_agg=True),
        ]
        rows = [("east", '[{"id": 1}, {"id": 2}]'), ("west", '[{"id": 3}]')]
        groups = serialize_rows(rows, columns, "sales")["data"]["sales"]
        assert groups == [
            {"groupKey": {"region": "east"}, "nodes": [{"id": 1}, {"id": 2}]},
            {"groupKey": {"region": "west"}, "nodes": [{"id": 3}]},
        ]

    def test_groups_with_the_same_nodes_stay_two_groups(self):
        columns = [
            _cr("count", nested_in="aggregate"),
            _cr("nodes", cardinality="one-to-many", is_agg=True),
        ]
        rows = [(1, '[{"id": 1}]'), (1, '[{"id": 2}]')]
        assert len(serialize_rows(rows, columns, "sales")["data"]["sales"]) == 2


# ---------------------------------------------------------------------------
# serialize_connection
# ---------------------------------------------------------------------------


def _compiled_connection(**overrides):
    defaults = dict(
        sql="SELECT id FROM pets",
        params=[],
        root_field="pets",
        columns=[_cr("id"), _cr("name")],
        sources={"pg"},
        sort_columns=["id"],
        page_size=2,
        is_backward=False,
        has_cursor=False,
    )
    defaults.update(overrides)
    return CompiledQuery(**defaults)


class TestSerializeConnection:
    def test_basic_forward_connection(self):
        compiled = _compiled_connection()
        rows = [(1, "Rex"), (2, "Fido")]
        result = serialize_connection(rows, compiled)
        edges = result["data"]["pets"]["edges"]
        assert len(edges) == 2
        assert edges[0]["node"] == {"id": 1, "name": "Rex"}
        page_info = result["data"]["pets"]["pageInfo"]
        assert page_info["hasNextPage"] is False
        assert page_info["hasPreviousPage"] is False
        assert page_info["startCursor"] == edges[0]["cursor"]
        assert page_info["endCursor"] == edges[-1]["cursor"]

    def test_has_more_forward_truncates_and_sets_next(self):
        compiled = _compiled_connection(page_size=2)
        rows = [(1, "Rex"), (2, "Fido"), (3, "Mouse")]
        result = serialize_connection(rows, compiled)
        edges = result["data"]["pets"]["edges"]
        assert len(edges) == 2
        assert result["data"]["pets"]["pageInfo"]["hasNextPage"] is True

    def test_has_cursor_forward_sets_has_previous(self):
        compiled = _compiled_connection(has_cursor=True)
        rows = [(1, "Rex")]
        result = serialize_connection(rows, compiled)
        assert result["data"]["pets"]["pageInfo"]["hasPreviousPage"] is True

    def test_backward_pagination_reverses_rows(self):
        compiled = _compiled_connection(is_backward=True, has_cursor=True, page_size=None)
        rows = [(2, "Fido"), (1, "Rex")]
        result = serialize_connection(rows, compiled)
        edges = result["data"]["pets"]["edges"]
        assert edges[0]["node"]["id"] == 1
        assert edges[1]["node"]["id"] == 2
        page_info = result["data"]["pets"]["pageInfo"]
        assert page_info["hasNextPage"] is True  # has_cursor
        assert page_info["hasPreviousPage"] is False  # has_more is False (page_size None)

    def test_empty_rows_gives_null_cursors(self):
        compiled = _compiled_connection()
        result = serialize_connection([], compiled)
        page_info = result["data"]["pets"]["pageInfo"]
        assert page_info["startCursor"] is None
        assert page_info["endCursor"] is None
        assert result["data"]["pets"]["edges"] == []

    def test_page_size_none_no_truncation(self):
        compiled = _compiled_connection(page_size=None)
        rows = [(1, "Rex"), (2, "Fido"), (3, "Mouse")]
        result = serialize_connection(rows, compiled)
        assert len(result["data"]["pets"]["edges"]) == 3

    def test_sort_column_not_in_columns_skipped(self):
        compiled = _compiled_connection(sort_columns=["missing_col"])
        rows = [(1, "Rex")]
        result = serialize_connection(rows, compiled)
        # cursor built from empty cursor_vals list
        edges = result["data"]["pets"]["edges"]
        assert len(edges) == 1
