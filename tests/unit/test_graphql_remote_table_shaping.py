# Copyright (c) 2026 Kenneth Stott
# Canary: c1a1e8de-8f7f-4e39-8f0f-0c6f5f8a2b31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the graphql_remote registration shaping (REQ-1685).

Source coverage:
  - provisa/api/app_loaders.py — _graphql_remote_field_name, _build_graphql_remote_table

A `graphql_remote` source landed after boot must be queryable on its first query: the reload
builds each landed table's registration entry — its field name, its columns split from its
required call arguments, and its optional object-field shape — from exactly the rows the DB
loader reads. These two functions are that shaping, pulled out of the DB-coupled reload loop so
they are exercised directly rather than only through the live-endpoint integration test.
"""

import json

from provisa.api.app_loaders import _build_graphql_remote_table, _graphql_remote_field_name


class TestFieldName:
    def test_strips_the_domain_prefix_and_camel_cases_the_rest(self) -> None:
        assert _graphql_remote_field_name("sales__order_line_items") == "orderLineItems"

    def test_single_word_is_unchanged(self) -> None:
        assert _graphql_remote_field_name("sales__orders") == "orders"

    def test_no_domain_prefix_still_camel_cases(self) -> None:
        assert _graphql_remote_field_name("order_line_items") == "orderLineItems"


def _col(**overrides: object) -> dict:
    base = {
        "column_name": "id",
        "data_type": "text",
        "object_fields": None,
        "native_filter_type": None,
        "gql_selection": None,
    }
    base.update(overrides)
    return base


class TestBuildGraphqlRemoteTable:
    def test_shapes_name_and_source(self) -> None:
        tr = {"table_name": "sales__orders", "domain_id": "sales", "description": "Orders"}
        table = _build_graphql_remote_table(tr, [], "src-1")
        assert table["name"] == "sales__orders"
        assert table["sql_name"] == "sales__orders"
        assert table["field_name"] == "orders"
        assert table["source_id"] == "src-1"
        assert table["domain_id"] == "sales"
        assert table["description"] == "Orders"

    def test_missing_domain_id_defaults_to_empty_string(self) -> None:
        tr = {"table_name": "orders", "domain_id": None, "description": None}
        assert _build_graphql_remote_table(tr, [], "src-1")["domain_id"] == ""

    def test_query_param_columns_become_required_args_not_columns(self) -> None:
        tr = {"table_name": "orders", "domain_id": "sales", "description": None}
        col_rows = [_col(column_name="region", native_filter_type="query_param")]
        table = _build_graphql_remote_table(tr, col_rows, "src-1")
        assert table["columns"] == []
        assert table["required_args"] == [
            {"name": "region", "gql_type": "String", "provisa_type": "text"}
        ]

    def test_ordinary_column_carries_type_and_defaults_to_text(self) -> None:
        tr = {"table_name": "orders", "domain_id": "sales", "description": None}
        col_rows = [_col(column_name="amount", data_type=None)]
        table = _build_graphql_remote_table(tr, col_rows, "src-1")
        assert table["columns"] == [{"name": "amount", "type": "text"}]

    def test_gql_selection_is_carried_when_present(self) -> None:
        tr = {"table_name": "orders", "domain_id": "sales", "description": None}
        col_rows = [_col(column_name="customer", gql_selection="{ id name }")]
        table = _build_graphql_remote_table(tr, col_rows, "src-1")
        assert table["columns"][0]["gql_selection"] == "{ id name }"

    def test_object_fields_json_string_is_parsed(self) -> None:
        tr = {"table_name": "orders", "domain_id": "sales", "description": None}
        col_rows = [_col(column_name="meta", object_fields=json.dumps({"a": 1}))]
        table = _build_graphql_remote_table(tr, col_rows, "src-1")
        assert table["columns"][0]["gql_object_fields"] == {"a": 1}

    def test_object_fields_already_a_dict_passes_through(self) -> None:
        tr = {"table_name": "orders", "domain_id": "sales", "description": None}
        col_rows = [_col(column_name="meta", object_fields={"a": 1})]
        table = _build_graphql_remote_table(tr, col_rows, "src-1")
        assert table["columns"][0]["gql_object_fields"] == {"a": 1}

    def test_malformed_object_fields_json_is_dropped_not_raised(self) -> None:
        """A bad object_fields row must not lose the whole table — REQ-1685."""
        tr = {"table_name": "orders", "domain_id": "sales", "description": None}
        col_rows = [_col(column_name="meta", object_fields="{not json")]
        table = _build_graphql_remote_table(tr, col_rows, "src-1")
        assert "gql_object_fields" not in table["columns"][0]

    def test_multiple_columns_preserve_order_and_split_correctly(self) -> None:
        tr = {"table_name": "orders", "domain_id": "sales", "description": None}
        col_rows = [
            _col(column_name="id"),
            _col(column_name="region", native_filter_type="query_param"),
            _col(column_name="amount", data_type="numeric"),
        ]
        table = _build_graphql_remote_table(tr, col_rows, "src-1")
        assert [c["name"] for c in table["columns"]] == ["id", "amount"]
        assert [a["name"] for a in table["required_args"]] == ["region"]
