# Copyright (c) 2025 Kenneth Stott
# Canary: a7b3c1d2-4e5f-6789-abcd-ef0123456789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for PostgreSQL enum auto-detection (Phase AB4, REQ-221)."""

from unittest.mock import AsyncMock

import pytest
from graphql import GraphQLEnumType, GraphQLInputObjectType

from provisa.compiler.enum_detect import (
    build_enum_filter_types,
    build_enum_types,
    fetch_enum_registry,
    resolve_column_type,
    _sanitize_enum_name,
    _sanitize_enum_value,
)


# --- Sanitization helpers ---


class TestSanitizeEnumName:
    def test_simple_name(self):
        assert _sanitize_enum_name("order_status") == "order_status_Enum"

    def test_hyphenated_name(self):
        assert _sanitize_enum_name("order-status") == "order_status_Enum"

    def test_leading_digit(self):
        assert _sanitize_enum_name("3d_type") == "e_3d_type_Enum"

    def test_special_chars(self):
        assert _sanitize_enum_name("my.enum!type") == "my_enum_type_Enum"


class TestSanitizeEnumValue:
    def test_simple_value(self):
        assert _sanitize_enum_value("pending") == "PENDING"

    def test_hyphenated_value(self):
        assert _sanitize_enum_value("in-progress") == "IN_PROGRESS"

    def test_leading_digit(self):
        assert _sanitize_enum_value("3star") == "V_3STAR"

    def test_empty_string(self):
        assert _sanitize_enum_value("") == "V_"


# --- fetch_enum_registry ---


class TestFetchEnumRegistry:
    @pytest.mark.asyncio
    async def test_basic_fetch(self):
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"enum_name": "order_status", "enum_value": "pending"},
            {"enum_name": "order_status", "enum_value": "shipped"},
            {"enum_name": "order_status", "enum_value": "delivered"},
            {"enum_name": "color", "enum_value": "red"},
            {"enum_name": "color", "enum_value": "green"},
        ]

        result = await fetch_enum_registry(mock_conn)

        assert result == {
            "order_status": ["pending", "shipped", "delivered"],
            "color": ["red", "green"],
        }
        mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_result(self):
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []

        result = await fetch_enum_registry(mock_conn)

        assert result == {}

    @pytest.mark.asyncio
    async def test_query_uses_pg_enum_join(self):
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []

        await fetch_enum_registry(mock_conn)

        query = mock_conn.fetch.call_args[0][0]
        assert "pg_enum" in query
        assert "pg_type" in query
        assert "enumsortorder" in query


# --- build_enum_types ---


class TestBuildEnumTypes:
    def test_single_enum(self):
        registry = {"order_status": ["pending", "shipped", "delivered"]}

        result = build_enum_types(registry)

        assert "order_status" in result
        gql_type = result["order_status"]
        assert isinstance(gql_type, GraphQLEnumType)
        assert gql_type.name == "order_status_Enum"
        values = gql_type.values
        assert "PENDING" in values
        assert "SHIPPED" in values
        assert "DELIVERED" in values

    def test_multiple_enums(self):
        registry = {
            "status": ["active", "inactive"],
            "color": ["red", "blue"],
        }

        result = build_enum_types(registry)

        assert len(result) == 2
        assert "status" in result
        assert "color" in result

    def test_enum_values_preserve_original(self):
        registry = {"mood": ["happy"]}

        result = build_enum_types(registry)

        gql_type = result["mood"]
        # The GraphQL value should map back to the original PG label
        assert gql_type.values["HAPPY"].value == "happy"

    def test_empty_registry(self):
        assert build_enum_types({}) == {}

    def test_description_includes_pg_name(self):
        registry = {"priority": ["low", "high"]}

        result = build_enum_types(registry)

        assert "priority" in result["priority"].description


# --- build_enum_filter_types ---


class TestBuildEnumFilterTypes:
    def test_filter_type_created(self):
        registry = {"status": ["active", "inactive"]}
        enum_types = build_enum_types(registry)

        filters = build_enum_filter_types(enum_types)

        assert "status" in filters
        f = filters["status"]
        assert isinstance(f, GraphQLInputObjectType)
        assert f.name == "status_EnumFilter"
        # Trigger the thunk to get fields
        field_names = set(f.fields.keys())
        assert field_names == {"eq", "neq", "in", "is_null"}

    def test_empty_input(self):
        assert build_enum_filter_types({}) == {}


# --- resolve_column_type ---


class TestResolveColumnType:
    def setup_method(self):
        self.registry = {"order_status": ["pending", "shipped"]}
        self.enum_types = build_enum_types(self.registry)

    def test_direct_match(self):
        result = resolve_column_type("order_status", self.enum_types)
        assert result is not None
        assert isinstance(result, GraphQLEnumType)
        assert result.name == "order_status_Enum"

    def test_case_insensitive(self):
        result = resolve_column_type("ORDER_STATUS", self.enum_types)
        assert result is not None

    def test_schema_qualified(self):
        result = resolve_column_type("public.order_status", self.enum_types)
        assert result is not None
        assert result.name == "order_status_Enum"

    def test_no_match(self):
        result = resolve_column_type("varchar", self.enum_types)
        assert result is None

    def test_whitespace_stripped(self):
        result = resolve_column_type("  order_status  ", self.enum_types)
        assert result is not None

    def test_empty_enum_types(self):
        result = resolve_column_type("order_status", {})
        assert result is None
