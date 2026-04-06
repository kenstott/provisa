# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3a2f1e-8b4d-4e9a-b6c5-3d0f7e2a1b8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the Actions feature — tracked DB functions and webhooks
exposed as GraphQL mutations (REQ-205 through REQ-211).

Covers:
- Function SQL generation and execution against a real DB function
- Webhook HTTP dispatch and response mapping
- GraphQL mutation field generation (function_gen.py)
- Role visibility gating on functions and webhooks
- Inline and table-backed return types
- Webhook timeout and error handling
"""

from __future__ import annotations

import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from graphql import GraphQLObjectType, GraphQLString, GraphQLField, GraphQLNonNull, GraphQLList

from provisa.compiler.function_gen import (
    build_function_mutations,
    build_function_sql,
)
from provisa.core.models import Function, FunctionArgument, InlineType, Webhook
from provisa.executor.pool import SourcePool
from provisa.webhooks.executor import execute_webhook, map_response_to_return_type, WebhookResult

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="module")
async def source_pool():
    sp = SourcePool()
    try:
        await sp.add(
            "test-pg",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
        )
        yield sp
    except Exception:
        pytest.skip("PostgreSQL not available")
        yield None
    finally:
        if sp:
            await sp.close_all()


@pytest_asyncio.fixture(scope="module")
async def pg_func(source_pool):
    """Create a real PG function for testing, drop it after the module."""
    if source_pool is None:
        pytest.skip("PostgreSQL not available")
    create_sql = """
        CREATE OR REPLACE FUNCTION public.provisa_test_action(p_region TEXT, p_limit INT)
        RETURNS TABLE(id INT, region TEXT, amount NUMERIC)
        LANGUAGE SQL STABLE AS $$
            SELECT id, region, amount
            FROM orders
            WHERE region = p_region
            LIMIT p_limit;
        $$;
    """
    await source_pool.execute("test-pg", create_sql)
    yield
    await source_pool.execute(
        "test-pg", "DROP FUNCTION IF EXISTS public.provisa_test_action(TEXT, INT)"
    )


# ---------------------------------------------------------------------------
# DB Function: SQL generation
# ---------------------------------------------------------------------------

class TestFunctionSQLGeneration:
    def test_build_function_sql_no_args(self):
        """Function with no args produces SELECT * FROM schema.func()."""
        func = Function(
            name="get_summary",
            source_id="pg",
            schema="public",
            function_name="get_summary",
            returns="pg.public.orders",
        )
        sql, params = build_function_sql(func, [])
        assert 'SELECT * FROM "public"."get_summary"()' == sql
        assert params == []

    def test_build_function_sql_with_args(self):
        """Function with args produces correct positional placeholders."""
        func = Function(
            name="action",
            source_id="pg",
            schema="public",
            function_name="provisa_test_action",
            returns="pg.public.orders",
            arguments=[
                FunctionArgument(name="p_region", type="String"),
                FunctionArgument(name="p_limit", type="Int"),
            ],
        )
        sql, params = build_function_sql(func, ["us-east", 5])
        assert '"public"."provisa_test_action"($1, $2)' in sql
        assert params == ["us-east", 5]

    def test_build_function_sql_special_schema(self):
        """Non-public schema is correctly quoted."""
        func = Function(
            name="action",
            source_id="pg",
            schema="reporting",
            function_name="monthly_totals",
            returns="pg.reporting.report",
        )
        sql, _ = build_function_sql(func, [])
        assert '"reporting"."monthly_totals"' in sql


# ---------------------------------------------------------------------------
# DB Function: real execution
# ---------------------------------------------------------------------------

class TestFunctionExecution:
    async def test_function_executes_and_returns_rows(self, source_pool, pg_func):
        """Calling a tracked DB function returns rows from the function result set."""
        from provisa.executor.direct import execute_direct

        func = Function(
            name="provisa_test_action",
            source_id="test-pg",
            schema="public",
            function_name="provisa_test_action",
            returns="test-pg.public.orders",
            arguments=[
                FunctionArgument(name="p_region", type="String"),
                FunctionArgument(name="p_limit", type="Int"),
            ],
        )
        sql, params = build_function_sql(func, ["us-east", 3])
        result = await execute_direct(source_pool, "test-pg", sql, params)

        assert result.column_names == ["id", "region", "amount"]
        assert len(result.rows) <= 3
        for row in result.rows:
            assert row[1] == "us-east"

    async def test_function_empty_result_on_no_match(self, source_pool, pg_func):
        """Function with no matching rows returns empty result."""
        from provisa.executor.direct import execute_direct

        func = Function(
            name="provisa_test_action",
            source_id="test-pg",
            schema="public",
            function_name="provisa_test_action",
            returns="test-pg.public.orders",
            arguments=[
                FunctionArgument(name="p_region", type="String"),
                FunctionArgument(name="p_limit", type="Int"),
            ],
        )
        sql, params = build_function_sql(func, ["__nonexistent_region__", 10])
        result = await execute_direct(source_pool, "test-pg", sql, params)
        assert result.rows == []

    async def test_function_limit_respected(self, source_pool, pg_func):
        """Function LIMIT arg caps the returned row count."""
        from provisa.executor.direct import execute_direct

        func = Function(
            name="provisa_test_action",
            source_id="test-pg",
            schema="public",
            function_name="provisa_test_action",
            returns="test-pg.public.orders",
            arguments=[
                FunctionArgument(name="p_region", type="String"),
                FunctionArgument(name="p_limit", type="Int"),
            ],
        )
        sql, params = build_function_sql(func, ["us-east", 1])
        result = await execute_direct(source_pool, "test-pg", sql, params)
        assert len(result.rows) <= 1


# ---------------------------------------------------------------------------
# GraphQL mutation field generation
# ---------------------------------------------------------------------------

class TestFunctionMutationGeneration:
    def _make_return_type(self) -> GraphQLObjectType:
        return GraphQLObjectType(
            "Order",
            lambda: {"id": GraphQLField(GraphQLString), "region": GraphQLField(GraphQLString)},
        )

    def test_function_produces_mutation_field(self):
        """build_function_mutations includes a field for each tracked function."""
        func = Function(
            name="doAction",
            source_id="pg",
            schema="public",
            function_name="do_action",
            returns="pg.public.orders",
            arguments=[FunctionArgument(name="p_id", type="Int")],
        )
        table_types = {"pg.public.orders": self._make_return_type()}
        fields = build_function_mutations([func], [], table_types)
        assert "doAction" in fields
        assert fields["doAction"].description == "Call DB function do_action"

    def test_function_return_type_is_list(self):
        """Function mutation return type is a list of the table type."""
        func = Function(
            name="doAction",
            source_id="pg",
            schema="public",
            function_name="do_action",
            returns="pg.public.orders",
        )
        table_types = {"pg.public.orders": self._make_return_type()}
        fields = build_function_mutations([func], [], table_types)
        # Should be GraphQLList(GraphQLNonNull(...))
        assert hasattr(fields["doAction"].type, "of_type"), "Should be a list type"

    def test_function_args_included_in_field(self):
        """Function arguments appear in the generated mutation field args."""
        func = Function(
            name="doAction",
            source_id="pg",
            schema="public",
            function_name="do_action",
            returns="pg.public.orders",
            arguments=[
                FunctionArgument(name="p_region", type="String"),
                FunctionArgument(name="p_limit", type="Int"),
            ],
        )
        table_types = {"pg.public.orders": self._make_return_type()}
        fields = build_function_mutations([func], [], table_types)
        args = fields["doAction"].args
        assert "p_region" in args
        assert "p_limit" in args

    def test_function_hidden_from_non_visible_role(self):
        """Function with visible_to set is excluded for roles not in the list."""
        func = Function(
            name="adminAction",
            source_id="pg",
            schema="public",
            function_name="admin_action",
            returns="pg.public.orders",
            visible_to=["admin"],
        )
        table_types = {"pg.public.orders": self._make_return_type()}
        fields = build_function_mutations([func], [], table_types, role_id="analyst")
        assert "adminAction" not in fields

    def test_function_visible_to_correct_role(self):
        """Function with visible_to is included for matching role."""
        func = Function(
            name="adminAction",
            source_id="pg",
            schema="public",
            function_name="admin_action",
            returns="pg.public.orders",
            visible_to=["admin"],
        )
        table_types = {"pg.public.orders": self._make_return_type()}
        fields = build_function_mutations([func], [], table_types, role_id="admin")
        assert "adminAction" in fields

    def test_function_skipped_when_return_type_missing(self):
        """Function is skipped if its return table type is not registered."""
        func = Function(
            name="orphanAction",
            source_id="pg",
            schema="public",
            function_name="orphan",
            returns="pg.public.nonexistent",
        )
        fields = build_function_mutations([func], [], {})
        assert "orphanAction" not in fields

    def test_unknown_arg_type_raises(self):
        """FunctionArgument with unknown type raises ValueError."""
        func = Function(
            name="badAction",
            source_id="pg",
            schema="public",
            function_name="bad",
            returns="pg.public.orders",
            arguments=[FunctionArgument(name="x", type="UUID")],
        )
        table_types = {"pg.public.orders": self._make_return_type()}
        with pytest.raises(ValueError, match="Unknown argument type"):
            build_function_mutations([func], [], table_types)


# ---------------------------------------------------------------------------
# Webhooks: mutation field generation
# ---------------------------------------------------------------------------

class TestWebhookMutationGeneration:
    def _make_return_type(self) -> GraphQLObjectType:
        return GraphQLObjectType(
            "Order",
            lambda: {"id": GraphQLField(GraphQLString)},
        )

    def test_webhook_produces_mutation_field(self):
        """build_function_mutations includes a field for each webhook."""
        wh = Webhook(
            name="notifyShipping",
            url="https://example.com/notify",
            method="POST",
            inline_return_type=[InlineType(name="status", type="String")],
        )
        fields = build_function_mutations([], [wh], {})
        assert "notifyShipping" in fields
        assert "notify" in fields["notifyShipping"].description.lower()

    def test_webhook_inline_return_type(self):
        """Webhook with inline_return_type uses an auto-generated object type."""
        wh = Webhook(
            name="syncAction",
            url="https://example.com/sync",
            inline_return_type=[
                InlineType(name="ok", type="Boolean"),
                InlineType(name="message", type="String"),
            ],
        )
        fields = build_function_mutations([], [wh], {})
        assert "syncAction" in fields
        # Return type should be an object type (not a scalar)
        rt = fields["syncAction"].type
        assert isinstance(rt, GraphQLObjectType) or hasattr(rt, "of_type")

    def test_webhook_table_backed_return_type(self):
        """Webhook with returns= referencing a registered table uses that type."""
        wh = Webhook(
            name="refreshCache",
            url="https://example.com/cache",
            returns="pg.public.orders",
        )
        table_types = {"pg.public.orders": self._make_return_type()}
        fields = build_function_mutations([], [wh], table_types)
        assert "refreshCache" in fields

    def test_webhook_hidden_from_non_visible_role(self):
        """Webhook with visible_to excludes non-matching roles."""
        wh = Webhook(
            name="secretHook",
            url="https://example.com/secret",
            visible_to=["admin"],
        )
        fields = build_function_mutations([], [wh], {}, role_id="analyst")
        assert "secretHook" not in fields

    def test_webhook_falls_back_to_json_scalar(self):
        """Webhook with no return type or inline_return_type uses JSON scalar."""
        from provisa.compiler.type_map import JSONScalar
        wh = Webhook(name="fireAndForget", url="https://example.com/fire")
        fields = build_function_mutations([], [wh], {})
        assert "fireAndForget" in fields
        assert fields["fireAndForget"].type is JSONScalar


# ---------------------------------------------------------------------------
# Webhooks: HTTP execution
# ---------------------------------------------------------------------------

class TestWebhookExecution:
    async def test_webhook_executes_post_request(self):
        """execute_webhook sends an HTTP POST with arguments as JSON body."""
        wh = Webhook(
            name="testHook",
            url="https://example.com/hook",
            method="POST",
            timeout_ms=5000,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True}
        mock_response.headers = {"content-type": "application/json"}
        mock_response.raise_for_status = MagicMock()

        with patch("provisa.webhooks.executor.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.request = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            result = await execute_webhook(wh, {"order_id": 42})

        assert result.status_code == 200
        assert result.data == {"ok": True}
        call_kwargs = mock_client.request.call_args
        assert call_kwargs.kwargs["json"] == {"order_id": 42}
        assert call_kwargs.kwargs["method"] == "POST"

    async def test_webhook_timeout_configured(self):
        """execute_webhook applies timeout from webhook config."""
        wh = Webhook(
            name="slowHook",
            url="https://example.com/slow",
            timeout_ms=2000,
        )
        captured_timeout = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.headers = {}
        mock_response.raise_for_status = MagicMock()

        with patch("provisa.webhooks.executor.httpx.AsyncClient") as mock_client_cls:
            def capture_timeout(timeout=None):
                captured_timeout["value"] = timeout
                mock_client = AsyncMock()
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_client.request = AsyncMock(return_value=mock_response)
                return mock_client
            mock_client_cls.side_effect = capture_timeout

            await execute_webhook(wh, {})

        import httpx as _httpx
        assert isinstance(captured_timeout.get("value"), _httpx.Timeout)

    async def test_webhook_http_error_propagates(self):
        """execute_webhook raises httpx.HTTPStatusError on 4xx/5xx."""
        import httpx as _httpx

        wh = Webhook(name="errHook", url="https://example.com/err")

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = _httpx.HTTPStatusError(
            "500", request=MagicMock(), response=mock_response
        )

        with patch("provisa.webhooks.executor.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.request = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            with pytest.raises(_httpx.HTTPStatusError):
                await execute_webhook(wh, {})

    async def test_webhook_get_method(self):
        """execute_webhook supports GET method (passes arguments as json body)."""
        wh = Webhook(name="getHook", url="https://example.com/get", method="GET")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.headers = {}
        mock_response.raise_for_status = MagicMock()

        with patch("provisa.webhooks.executor.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.request = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            result = await execute_webhook(wh, {"q": "test"})

        call_kwargs = mock_client.request.call_args
        assert call_kwargs.kwargs["method"] == "GET"
        assert result.status_code == 200


# ---------------------------------------------------------------------------
# Webhook response mapping
# ---------------------------------------------------------------------------

class TestWebhookResponseMapping:
    def test_map_dict_filters_to_inline_fields(self):
        """Dict response is filtered to only include declared inline fields."""
        data = {"id": 1, "region": "us", "internal": "secret"}
        inline = [{"name": "id", "type": "Int"}, {"name": "region", "type": "String"}]
        result = map_response_to_return_type(data, inline)
        assert result == {"id": 1, "region": "us"}
        assert "internal" not in result

    def test_map_list_filters_each_element(self):
        """List response maps each dict element through the inline field filter."""
        data = [
            {"id": 1, "region": "us", "extra": "x"},
            {"id": 2, "region": "eu", "extra": "y"},
        ]
        inline = [{"name": "id", "type": "Int"}, {"name": "region", "type": "String"}]
        result = map_response_to_return_type(data, inline)
        assert len(result) == 2
        assert all("extra" not in item for item in result)
        assert result[0] == {"id": 1, "region": "us"}

    def test_map_no_inline_fields_returns_raw(self):
        """Without inline_fields, raw response is returned unchanged."""
        data = {"any": "thing"}
        result = map_response_to_return_type(data, None)
        assert result == {"any": "thing"}

    def test_map_scalar_response_returned_as_is(self):
        """Non-dict, non-list response is returned unchanged."""
        result = map_response_to_return_type("ok", [{"name": "status", "type": "String"}])
        assert result == "ok"

    def test_map_non_dict_list_items_skipped(self):
        """List items that are not dicts are skipped in mapping."""
        data = [{"id": 1}, "not-a-dict", 42]
        inline = [{"name": "id", "type": "Int"}]
        result = map_response_to_return_type(data, inline)
        assert result == [{"id": 1}]
