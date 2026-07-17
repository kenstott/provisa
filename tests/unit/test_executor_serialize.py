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
    _recursive_json_convert,
    _to_hashable,
    serialize_aggregate,
    serialize_connection,
    serialize_group_by,
    serialize_rows,
    shape_transform,
)


def _cr(field_name, column=None, nested_in=None, cardinality=None, is_agg=False, alias=None):
    return ColumnRef(
        alias=alias,
        column=column or field_name,
        field_name=field_name,
        nested_in=nested_in,
        cardinality=cardinality,
        is_agg=is_agg,
    )


# ---------------------------------------------------------------------------
# _convert_value / _recursive_json_convert / _to_hashable
# ---------------------------------------------------------------------------


class TestConvertValue:
    def test_decimal_integral(self):
        assert _convert_value(Decimal("5")) == 5
        assert isinstance(_convert_value(Decimal("5")), int)

    def test_decimal_fractional(self):
        assert _convert_value(Decimal("5.50")) == 5.5

    def test_date_isoformat(self):
        d = date(2024, 1, 15)
        assert _convert_value(d) == "2024-01-15"

    def test_datetime_isoformat(self):
        dt = datetime(2024, 1, 15, 10, 30)
        assert _convert_value(dt) == dt.isoformat()

    def test_json_string_object_parsed(self):
        assert _convert_value('{"a": 1}') == {"a": 1}

    def test_json_string_array_parsed(self):
        assert _convert_value("[1, 2, 3]") == [1, 2, 3]

    def test_invalid_json_string_passthrough(self):
        assert _convert_value("{not valid json") == "{not valid json"

    def test_plain_string_passthrough(self):
        assert _convert_value("hello") == "hello"

    def test_none_passthrough(self):
        assert _convert_value(None) is None

    def test_native_dict_recursed(self):
        assert _convert_value({"x": Decimal("2")}) == {"x": 2}

    def test_native_list_recursed(self):
        assert _convert_value([Decimal("2"), Decimal("3.5")]) == [2, 3.5]

    def test_int_passthrough(self):
        assert _convert_value(42) == 42

    def test_nested_json_string_with_decimal_like_values(self):
        # Nested list-in-dict: outer dict, inner list of scalars
        result = _convert_value('{"items": [1, 2], "count": 2}')
        assert result == {"items": [1, 2], "count": 2}


class TestRecursiveJsonConvert:
    def test_dict_input(self):
        result = _recursive_json_convert({"a": Decimal("1.5")})
        assert result == {"a": 1.5}

    def test_list_input(self):
        result = _recursive_json_convert([Decimal("1"), "x"])
        assert result == [1, "x"]

    def test_scalar_passthrough(self):
        assert _recursive_json_convert("plain") == "plain"


class TestToHashable:
    def test_dict_becomes_json_string(self):
        result = _to_hashable({"b": 1, "a": 2})
        assert result == '{"a": 2, "b": 1}'

    def test_list_becomes_json_string(self):
        result = _to_hashable([1, 2])
        assert result == "[1, 2]"

    def test_scalar_passthrough(self):
        assert _to_hashable(5) == 5
        assert _to_hashable("x") == "x"
        assert _to_hashable(None) is None


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

    def test_one_to_many_agg_column(self):
        columns = [
            _cr("id"),
            _cr("count", nested_in="stats", is_agg=True),
        ]
        rows = [(1, 5)]
        result = serialize_rows(rows, columns, "pets")
        assert result["data"]["pets"][0]["stats"] == [{"count": 5}]

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
# shape_transform
# ---------------------------------------------------------------------------


class TestShapeTransform:
    def test_no_m2o_paths_returns_unchanged(self):
        result = {"data": {"pets": [{"id": 1}]}}
        columns = [_cr("id")]
        assert shape_transform(result, columns) is result

    def test_collapses_array_agg_m2o(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="owner", cardinality="many-to-one", is_agg=True),
        ]
        result = {"data": {"pets": [{"id": 1, "owner": [{"name": "Alice"}]}]}}
        transformed = shape_transform(result, columns)
        assert transformed["data"]["pets"][0]["owner"] == {"name": "Alice"}

    def test_collapses_empty_array_to_none(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="owner", cardinality="many-to-one", is_agg=True),
        ]
        result = {"data": {"pets": [{"id": 1, "owner": []}]}}
        transformed = shape_transform(result, columns)
        assert transformed["data"]["pets"][0]["owner"] is None

    def test_recurses_into_lists_of_dicts(self):
        columns = [
            _cr("id"),
            _cr("name", nested_in="toys.owner", cardinality="many-to-one", is_agg=True),
        ]
        result = {
            "data": {
                "pets": [
                    {"id": 1, "toys": [{"owner": [{"name": "Alice"}]}]},
                ]
            }
        }
        transformed = shape_transform(result, columns)
        assert transformed["data"]["pets"][0]["toys"][0]["owner"] == {"name": "Alice"}


# ---------------------------------------------------------------------------
# serialize_aggregate
# ---------------------------------------------------------------------------


class TestSerializeAggregate:
    def test_basic_aggregate_no_nodes(self):
        agg_columns = [_cr("count")]
        agg_rows = [(5,)]
        result = serialize_aggregate(agg_rows, agg_columns, None, None, "orders")
        assert result == {"data": {"orders": {"aggregate": {"count": 5}}}}

    def test_aggregate_with_sub_path(self):
        agg_columns = [_cr("amount", nested_in="aggregate.sum")]
        agg_rows = [(Decimal("100.00"),)]
        result = serialize_aggregate(agg_rows, agg_columns, None, None, "orders")
        assert result["data"]["orders"]["aggregate"]["sum"]["amount"] == 100

    def test_aggregate_empty_rows(self):
        result = serialize_aggregate([], [_cr("count")], None, None, "orders")
        assert result == {"data": {"orders": {"aggregate": {}}}}

    def test_aggregate_with_nodes(self):
        agg_columns = [_cr("count")]
        agg_rows = [(2,)]
        nodes_columns = [_cr("id"), _cr("name")]
        nodes_rows = [(1, "Rex"), (2, "Fido")]
        result = serialize_aggregate(agg_rows, agg_columns, nodes_rows, nodes_columns, "orders")
        assert result["data"]["orders"]["nodes"] == [
            {"id": 1, "name": "Rex"},
            {"id": 2, "name": "Fido"},
        ]

    def test_custom_agg_alias(self):
        agg_columns = [_cr("count", nested_in="derived")]
        agg_rows = [(3,)]
        result = serialize_aggregate(
            agg_rows, agg_columns, None, None, "orders", agg_alias="derived"
        )
        assert result == {"data": {"orders": {"derived": {"count": 3}}}}


# ---------------------------------------------------------------------------
# serialize_group_by
# ---------------------------------------------------------------------------


class TestSerializeGroupBy:
    def test_group_by_without_nodes(self):
        columns = [_cr("region", nested_in="groupKey")]
        rows = [("east",)]
        result = serialize_group_by(rows, columns, None, None, "sales")
        assert result == {"data": {"sales": [{"groupKey": {"region": "east"}}]}}

    def test_group_by_with_nodes(self):
        columns = [_cr("region", nested_in="groupKey")]
        rows = [("east",), ("west",)]
        nodes_columns = [
            _cr("region", nested_in="__join_key__"),
            _cr("id"),
        ]
        nodes_rows = [("east", 1), ("east", 2), ("west", 3)]
        result = serialize_group_by(rows, columns, nodes_rows, nodes_columns, "sales")
        groups = result["data"]["sales"]
        east = next(g for g in groups if g["groupKey"]["region"] == "east")
        west = next(g for g in groups if g["groupKey"]["region"] == "west")
        assert east["nodes"] == [{"id": 1}, {"id": 2}]
        assert west["nodes"] == [{"id": 3}]

    def test_group_by_with_nodes_no_match(self):
        columns = [_cr("region", nested_in="groupKey")]
        rows = [("north",)]
        nodes_columns = [_cr("region", nested_in="__join_key__"), _cr("id")]
        nodes_rows = []
        result = serialize_group_by(rows, columns, nodes_rows, nodes_columns, "sales")
        assert result["data"]["sales"][0]["nodes"] == []


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
