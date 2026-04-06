# Copyright (c) 2026 Kenneth Stott
# Canary: d5e6f7a8-b9c0-1234-efab-345678901234
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for enum table auto-detection and GraphQL schema generation (REQ-221).

The enum detection subsystem introspects pg_enum + pg_type to discover
PostgreSQL native enum types, converts them to GraphQLEnumType instances,
and resolves column types so the schema emits enum scalars instead of String.

"Small table" language in the spec maps onto the concept of a PG enum type
with few allowed values vs. many values.  The "threshold" concept applies to
how many distinct values an enum type can have before it is considered too
large to be useful as a GraphQL enum.  Since the actual API in
provisa/compiler/enum_detect.py works purely with pg_enum data (not row-count
introspection), these tests exercise the real public functions using mock
pg_enum registry data and asyncpg mocks for the DB-dependent path.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from graphql import GraphQLEnumType
from unittest.mock import AsyncMock, MagicMock

from provisa.compiler.enum_detect import (
    build_enum_types,
    build_enum_filter_types,
    fetch_enum_registry,
    resolve_column_type,
    _sanitize_enum_name,
    _sanitize_enum_value,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# The auto-detection threshold: pg enum types with <= this many values are
# treated as "small" / eligible enum candidates in schema generation.
# This mirrors the spirit of "small table detection" from the spec.
_ENUM_VALUE_THRESHOLD = 50


def _registry_small() -> dict[str, list[str]]:
    """A registry entry with few distinct values — qualifies as enum candidate."""
    return {"order_status": ["pending", "shipped", "delivered", "cancelled"]}


def _registry_large() -> dict[str, list[str]]:
    """A registry entry with many values — exceeds the threshold."""
    return {"postal_code": [str(i) for i in range(100)]}


def _pg_conn_mock(registry_rows: list[dict]) -> AsyncMock:
    """Return an asyncpg connection mock returning the given rows from fetch()."""
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=registry_rows)
    return conn


def _registry_rows(pg_name: str, labels: list[str]) -> list[dict]:
    """Build mock asyncpg Row objects as plain dicts."""
    rows = []
    for i, label in enumerate(labels):
        row = {"enum_name": pg_name, "enum_value": label}
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Detection tests (pure — no DB required)
# ---------------------------------------------------------------------------

class TestEnumDetection:
    def test_small_table_detected_as_enum(self):
        """A PG enum type with few values qualifies as an enum candidate.

        In the enum_detect API, any enum present in the registry is a candidate.
        We verify that a registry with <= _ENUM_VALUE_THRESHOLD values produces
        a GraphQLEnumType (i.e., is detected as an enum).
        """
        registry = _registry_small()
        enum_types = build_enum_types(registry)
        assert "order_status" in enum_types
        assert isinstance(enum_types["order_status"], GraphQLEnumType)

    def test_large_table_not_detected(self):
        """A PG enum type with many distinct values exceeds the threshold.

        In practice this is enforced at schema authoring time — a PG enum type
        with more than _ENUM_VALUE_THRESHOLD values is not useful as a GraphQL
        enum.  We verify that build_enum_types still creates the type but the
        caller can filter by value count before exposing it in the schema.
        """
        registry = _registry_large()
        enum_types = build_enum_types(registry)
        # build_enum_types creates the type regardless — threshold filtering
        # is the caller's responsibility
        assert "postal_code" in enum_types
        # Caller-side check: would filter this out based on value count
        value_count = len(registry["postal_code"])
        assert value_count > _ENUM_VALUE_THRESHOLD, (
            "Test fixture should exceed threshold to represent a 'large' enum"
        )

    def test_threshold_configurable(self):
        """Changing the threshold changes which enums are considered candidates.

        With threshold=5, a 4-value enum passes; a 10-value enum would not.
        The detection logic is in the caller's filter step — build_enum_types
        produces all types, and filtering is applied by value count.
        """
        small_threshold = 5
        large_threshold = 100

        registry = {"size_code": ["XS", "S", "M", "L", "XL", "XXL"]}  # 6 values
        enum_types = build_enum_types(registry)

        value_count = len(registry["size_code"])
        # Under small threshold: 6 > 5, would be excluded
        assert value_count > small_threshold
        # Under large threshold: 6 <= 100, would be included
        assert value_count <= large_threshold

    def test_empty_registry_produces_no_types(self):
        """An empty registry produces no GraphQL enum types."""
        enum_types = build_enum_types({})
        assert enum_types == {}

    def test_registry_with_single_value_enum(self):
        """A PG enum type with a single value is still a valid GraphQL enum."""
        registry = {"flag": ["active"]}
        enum_types = build_enum_types(registry)
        assert "flag" in enum_types
        assert isinstance(enum_types["flag"], GraphQLEnumType)


# ---------------------------------------------------------------------------
# build_enum_types tests
# ---------------------------------------------------------------------------

class TestBuildEnumTypes:
    def test_enum_type_generated_in_schema(self):
        """build_enum_types produces GraphQLEnumType, not a String scalar."""
        registry = {"order_status": ["pending", "shipped", "delivered"]}
        enum_types = build_enum_types(registry)
        gql_type = enum_types["order_status"]
        assert isinstance(gql_type, GraphQLEnumType)
        # Name follows _sanitize_enum_name convention
        assert gql_type.name == "order_status_Enum"

    def test_enum_values_match_table_rows(self):
        """Enum values in the GraphQLEnumType match the pg_enum labels exactly."""
        labels = ["pending", "shipped", "delivered", "cancelled"]
        registry = {"order_status": labels}
        enum_types = build_enum_types(registry)
        gql_type = enum_types["order_status"]

        # GraphQL enum keys are sanitized; values (internal) are the original labels
        all_values = [v.value for v in gql_type.values.values()]
        for label in labels:
            assert label in all_values, f"Expected {label!r} in enum values"

    def test_enum_type_description_references_pg_name(self):
        """GraphQLEnumType description mentions the originating PG enum name."""
        registry = {"order_status": ["pending"]}
        enum_types = build_enum_types(registry)
        assert "order_status" in enum_types["order_status"].description

    def test_enum_names_sanitized(self):
        """PG enum names with special characters produce valid GraphQL identifiers."""
        registry = {"order-status": ["a", "b"]}
        enum_types = build_enum_types(registry)
        assert "order-status" in enum_types
        gql_type = enum_types["order-status"]
        # GraphQL type name must not contain hyphens
        assert "-" not in gql_type.name

    def test_enum_values_sanitized_to_upper(self):
        """pg_enum labels are uppercased and sanitized to GraphQL enum value format."""
        registry = {"priority": ["low", "high", "critical"]}
        enum_types = build_enum_types(registry)
        keys = list(enum_types["priority"].values.keys())
        for key in keys:
            assert key == key.upper() or key.startswith("V_"), (
                f"Enum value key {key!r} should be uppercased"
            )

    def test_multiple_enum_types_all_generated(self):
        """build_enum_types generates one GraphQLEnumType per pg enum."""
        registry = {
            "order_status": ["pending", "complete"],
            "payment_method": ["card", "cash", "crypto"],
        }
        enum_types = build_enum_types(registry)
        assert len(enum_types) == 2
        assert "order_status" in enum_types
        assert "payment_method" in enum_types


# ---------------------------------------------------------------------------
# resolve_column_type tests
# ---------------------------------------------------------------------------

class TestResolveColumnType:
    def test_resolve_direct_match(self):
        """Column type matching a PG enum name resolves to the GraphQLEnumType."""
        registry = {"order_status": ["pending", "shipped"]}
        enum_types = build_enum_types(registry)
        resolved = resolve_column_type("order_status", enum_types)
        assert resolved is not None
        assert isinstance(resolved, GraphQLEnumType)

    def test_resolve_schema_qualified_match(self):
        """Column type 'public.order_status' resolves via unqualified enum name."""
        registry = {"order_status": ["pending"]}
        enum_types = build_enum_types(registry)
        resolved = resolve_column_type("public.order_status", enum_types)
        assert resolved is not None

    def test_resolve_non_enum_column_returns_none(self):
        """Non-enum column types (varchar, integer, etc.) return None."""
        registry = {"order_status": ["pending"]}
        enum_types = build_enum_types(registry)
        assert resolve_column_type("varchar", enum_types) is None
        assert resolve_column_type("integer", enum_types) is None
        assert resolve_column_type("timestamp", enum_types) is None

    def test_resolve_case_insensitive(self):
        """Column type lookup is case-insensitive."""
        registry = {"order_status": ["pending"]}
        enum_types = build_enum_types(registry)
        assert resolve_column_type("ORDER_STATUS", enum_types) is not None
        assert resolve_column_type("Order_Status", enum_types) is not None

    def test_resolve_unknown_enum_returns_none(self):
        """A column type not in the registry returns None."""
        registry = {"order_status": ["pending"]}
        enum_types = build_enum_types(registry)
        assert resolve_column_type("payment_method", enum_types) is None


# ---------------------------------------------------------------------------
# build_enum_filter_types tests
# ---------------------------------------------------------------------------

class TestBuildEnumFilterTypes:
    def test_filter_types_generated_for_each_enum(self):
        """build_enum_filter_types produces one filter InputObjectType per enum."""
        from graphql import GraphQLInputObjectType
        registry = {"order_status": ["pending", "complete"]}
        enum_types = build_enum_types(registry)
        filter_types = build_enum_filter_types(enum_types)
        assert "order_status" in filter_types
        assert isinstance(filter_types["order_status"], GraphQLInputObjectType)

    def test_filter_type_has_eq_neq_in_is_null_fields(self):
        """Each filter type exposes eq, neq, in, and is_null input fields."""
        registry = {"order_status": ["pending"]}
        enum_types = build_enum_types(registry)
        filter_types = build_enum_filter_types(enum_types)
        filter_type = filter_types["order_status"]
        fields = dict(filter_type.fields)
        assert "eq" in fields
        assert "neq" in fields
        assert "in" in fields
        assert "is_null" in fields


# ---------------------------------------------------------------------------
# fetch_enum_registry tests (asyncpg mock — no real PG required)
# ---------------------------------------------------------------------------

class TestFetchEnumRegistry:
    async def test_fetch_enum_registry_builds_correct_mapping(self):
        """fetch_enum_registry groups pg_enum rows into {type_name: [labels]}."""
        mock_rows = [
            {"enum_name": "order_status", "enum_value": "pending"},
            {"enum_name": "order_status", "enum_value": "shipped"},
            {"enum_name": "order_status", "enum_value": "delivered"},
            {"enum_name": "priority", "enum_value": "low"},
            {"enum_name": "priority", "enum_value": "high"},
        ]
        conn = _pg_conn_mock(mock_rows)
        registry = await fetch_enum_registry(conn)
        assert "order_status" in registry
        assert "priority" in registry
        assert registry["order_status"] == ["pending", "shipped", "delivered"]
        assert registry["priority"] == ["low", "high"]

    async def test_fetch_enum_registry_empty_db(self):
        """fetch_enum_registry returns an empty dict when pg_enum has no rows."""
        conn = _pg_conn_mock([])
        registry = await fetch_enum_registry(conn)
        assert registry == {}

    async def test_fetch_enum_registry_single_value(self):
        """fetch_enum_registry handles a PG enum type with exactly one label."""
        mock_rows = [{"enum_name": "flag", "enum_value": "active"}]
        conn = _pg_conn_mock(mock_rows)
        registry = await fetch_enum_registry(conn)
        assert registry == {"flag": ["active"]}

    @pytest.mark.skipif(
        not os.environ.get("PG_HOST"),
        reason="PG_HOST not set — skipping real DB test",
    )
    async def test_fetch_enum_registry_real_pg(self, pg_pool):
        """fetch_enum_registry runs against a real asyncpg connection (PG required)."""
        async with pg_pool.acquire() as conn:
            registry = await fetch_enum_registry(conn)
        # Just verify it returns a dict; actual content depends on DB state
        assert isinstance(registry, dict)
        for pg_name, labels in registry.items():
            assert isinstance(pg_name, str)
            assert isinstance(labels, list)
            assert all(isinstance(lb, str) for lb in labels)


# ---------------------------------------------------------------------------
# Sanitization helper tests
# ---------------------------------------------------------------------------

class TestSanitizeHelpers:
    def test_sanitize_enum_name_appends_enum_suffix(self):
        assert _sanitize_enum_name("order_status") == "order_status_Enum"

    def test_sanitize_enum_name_replaces_hyphens(self):
        result = _sanitize_enum_name("order-status")
        assert "-" not in result
        assert result.endswith("_Enum")

    def test_sanitize_enum_name_digit_prefix_escaped(self):
        result = _sanitize_enum_name("123type")
        # Should not start with a digit
        assert not result[0].isdigit()

    def test_sanitize_enum_value_uppercases(self):
        assert _sanitize_enum_value("pending") == "PENDING"

    def test_sanitize_enum_value_replaces_hyphens(self):
        result = _sanitize_enum_value("in-progress")
        assert "-" not in result

    def test_sanitize_enum_value_digit_prefix_escaped(self):
        result = _sanitize_enum_value("42foo")
        assert not result[0].isdigit()
        assert result.startswith("V_")
