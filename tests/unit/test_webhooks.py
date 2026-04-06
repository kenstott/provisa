# Copyright (c) 2026 Kenneth Stott
# Canary: 55be58cb-bdf3-4c5f-94b9-471f41d4b6ed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for webhooks module (executor.py)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from provisa.core.models import FunctionArgument, InlineType, Webhook
from provisa.webhooks.executor import (
    WebhookResult,
    execute_webhook,
    map_response_to_return_type,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_webhook(
    *,
    name: str = "createOrder",
    url: str = "https://api.example.com/orders",
    method: str = "POST",
    timeout_ms: int = 5000,
    arguments: list[FunctionArgument] | None = None,
    visible_to: list[str] | None = None,
) -> Webhook:
    return Webhook(
        name=name,
        url=url,
        method=method,
        timeout_ms=timeout_ms,
        arguments=arguments or [],
        visible_to=visible_to or [],
    )


def _mock_httpx_response(
    status_code: int = 200,
    json_data: Any = None,
    headers: dict[str, str] | None = None,
) -> MagicMock:
    """Build a mock httpx.Response."""
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data if json_data is not None else {}
    response.headers = httpx.Headers(headers or {"content-type": "application/json"})
    return response


# ---------------------------------------------------------------------------
# TestWebhookModel
# ---------------------------------------------------------------------------


class TestWebhookModel:
    def test_defaults(self):
        wh = Webhook(name="test", url="https://example.com/hook")
        assert wh.method == "POST"
        assert wh.timeout_ms == 5000
        assert wh.returns is None
        assert wh.arguments == []
        assert wh.visible_to == []
        assert wh.inline_return_type == []

    def test_custom_method(self):
        wh = _make_webhook(method="PUT")
        assert wh.method == "PUT"

    def test_timeout_ms(self):
        wh = _make_webhook(timeout_ms=10_000)
        assert wh.timeout_ms == 10_000

    def test_arguments_attached(self):
        wh = _make_webhook(
            arguments=[
                FunctionArgument(name="order_id", type="Int"),
                FunctionArgument(name="note", type="String"),
            ],
        )
        assert len(wh.arguments) == 2
        assert wh.arguments[0].name == "order_id"
        assert wh.arguments[1].type == "String"

    def test_visible_to(self):
        wh = _make_webhook(visible_to=["admin", "manager"])
        assert "admin" in wh.visible_to

    def test_inline_return_type(self):
        wh = Webhook(
            name="test",
            url="https://example.com/hook",
            inline_return_type=[
                InlineType(name="orderId", type="String"),
                InlineType(name="status", type="String"),
            ],
        )
        assert len(wh.inline_return_type) == 2
        assert wh.inline_return_type[0].name == "orderId"


# ---------------------------------------------------------------------------
# TestWebhookResult
# ---------------------------------------------------------------------------


class TestWebhookResult:
    def test_fields(self):
        result = WebhookResult(
            status_code=200,
            data={"id": 1},
            headers={"content-type": "application/json"},
        )
        assert result.status_code == 200
        assert result.data == {"id": 1}
        assert result.headers["content-type"] == "application/json"

    def test_data_can_be_list(self):
        result = WebhookResult(
            status_code=200,
            data=[{"id": 1}, {"id": 2}],
            headers={},
        )
        assert isinstance(result.data, list)
        assert len(result.data) == 2

    def test_data_can_be_none(self):
        result = WebhookResult(status_code=204, data=None, headers={})
        assert result.data is None


# ---------------------------------------------------------------------------
# TestExecuteWebhook — mock the HTTP client
# ---------------------------------------------------------------------------


class TestExecuteWebhook:
    @pytest.mark.asyncio
    async def test_successful_post(self):
        webhook = _make_webhook(url="https://api.example.com/orders")
        arguments = {"customer_id": 42, "amount": 99.99}

        mock_response = _mock_httpx_response(
            status_code=200,
            json_data={"order_id": "ord-001", "status": "created"},
        )

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            result = await execute_webhook(webhook, arguments)

        assert result.status_code == 200
        assert result.data["order_id"] == "ord-001"

    @pytest.mark.asyncio
    async def test_request_uses_correct_url(self):
        webhook = _make_webhook(url="https://hooks.example.com/notify")
        arguments = {"event": "signup"}

        mock_response = _mock_httpx_response(json_data={"ok": True})

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            await execute_webhook(webhook, arguments)

        call_kwargs = mock_client.request.call_args
        assert call_kwargs.kwargs["url"] == "https://hooks.example.com/notify"

    @pytest.mark.asyncio
    async def test_request_uses_correct_method(self):
        webhook = _make_webhook(method="PUT")
        arguments = {"id": 1}

        mock_response = _mock_httpx_response(json_data={})

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            await execute_webhook(webhook, arguments)

        call_kwargs = mock_client.request.call_args
        assert call_kwargs.kwargs["method"] == "PUT"

    @pytest.mark.asyncio
    async def test_arguments_sent_as_json_body(self):
        webhook = _make_webhook()
        arguments = {"name": "Alice", "age": 30}

        mock_response = _mock_httpx_response(json_data={"ok": True})

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            await execute_webhook(webhook, arguments)

        call_kwargs = mock_client.request.call_args
        assert call_kwargs.kwargs["json"] == arguments

    @pytest.mark.asyncio
    async def test_content_type_header_set(self):
        webhook = _make_webhook()

        mock_response = _mock_httpx_response(json_data={})

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            await execute_webhook(webhook, {})

        call_kwargs = mock_client.request.call_args
        assert call_kwargs.kwargs["headers"]["Content-Type"] == "application/json"

    @pytest.mark.asyncio
    async def test_timeout_set_from_timeout_ms(self):
        webhook = _make_webhook(timeout_ms=3000)

        mock_response = _mock_httpx_response(json_data={})

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        captured_timeout: list[httpx.Timeout] = []

        def _capture_client(timeout=None, **_kwargs):
            if timeout is not None:
                captured_timeout.append(timeout)
            return mock_client

        with patch("provisa.webhooks.executor.httpx.AsyncClient", side_effect=_capture_client):
            await execute_webhook(webhook, {})

        assert len(captured_timeout) == 1
        assert captured_timeout[0].read == 3.0

    @pytest.mark.asyncio
    async def test_response_headers_returned(self):
        webhook = _make_webhook()

        mock_response = _mock_httpx_response(
            json_data={"id": 1},
            headers={"x-request-id": "abc123", "content-type": "application/json"},
        )

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            result = await execute_webhook(webhook, {})

        assert "x-request-id" in result.headers

    @pytest.mark.asyncio
    async def test_raises_on_4xx_status(self):
        webhook = _make_webhook()

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=mock_response,
        )

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(httpx.HTTPStatusError):
                await execute_webhook(webhook, {})

    @pytest.mark.asyncio
    async def test_raises_on_5xx_status(self):
        webhook = _make_webhook()

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Internal Server Error",
            request=MagicMock(),
            response=mock_response,
        )

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(httpx.HTTPStatusError):
                await execute_webhook(webhook, {})

    @pytest.mark.asyncio
    async def test_raises_on_timeout(self):
        webhook = _make_webhook(timeout_ms=100)

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(
            side_effect=httpx.TimeoutException("timed out")
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(httpx.TimeoutException):
                await execute_webhook(webhook, {"data": "x"})

    @pytest.mark.asyncio
    async def test_empty_arguments(self):
        webhook = _make_webhook()

        mock_response = _mock_httpx_response(json_data={"ok": True})

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            result = await execute_webhook(webhook, {})

        assert result.status_code == 200

    @pytest.mark.asyncio
    async def test_list_response_data(self):
        webhook = _make_webhook()
        mock_response = _mock_httpx_response(
            json_data=[{"id": 1}, {"id": 2}, {"id": 3}],
        )

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            result = await execute_webhook(webhook, {})

        assert isinstance(result.data, list)
        assert len(result.data) == 3


# ---------------------------------------------------------------------------
# TestMapResponseToReturnType
# ---------------------------------------------------------------------------


class TestMapResponseToReturnType:
    def test_none_inline_fields_passthrough(self):
        data = {"id": 1, "name": "Alice", "secret": "hidden"}
        result = map_response_to_return_type(data, inline_fields=None)
        assert result == data

    def test_dict_filtered_to_inline_fields(self):
        data = {"id": 1, "name": "Alice", "secret": "hidden"}
        inline = [{"name": "id"}, {"name": "name"}]
        result = map_response_to_return_type(data, inline_fields=inline)
        assert result == {"id": 1, "name": "Alice"}
        assert "secret" not in result

    def test_list_of_dicts_filtered(self):
        data = [
            {"id": 1, "name": "Alice", "internal": True},
            {"id": 2, "name": "Bob", "internal": False},
        ]
        inline = [{"name": "id"}, {"name": "name"}]
        result = map_response_to_return_type(data, inline_fields=inline)
        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "Alice"}
        assert result[1] == {"id": 2, "name": "Bob"}

    def test_list_non_dict_items_skipped(self):
        data = [{"id": 1, "name": "Alice"}, "not_a_dict", 42]
        inline = [{"name": "id"}, {"name": "name"}]
        result = map_response_to_return_type(data, inline_fields=inline)
        # Non-dict items are excluded
        assert len(result) == 1
        assert result[0]["id"] == 1

    def test_empty_inline_fields_removes_all(self):
        data = {"id": 1, "name": "Alice"}
        result = map_response_to_return_type(data, inline_fields=[])
        assert result == {}

    def test_scalar_passthrough_with_inline_fields(self):
        """Non-dict, non-list scalar data is returned as-is."""
        result = map_response_to_return_type(42, inline_fields=[{"name": "value"}])
        assert result == 42

    def test_string_passthrough_with_inline_fields(self):
        result = map_response_to_return_type("ok", inline_fields=[{"name": "msg"}])
        assert result == "ok"

    def test_inline_fields_missing_from_response(self):
        """Fields listed in inline_fields but absent from response are omitted."""
        data = {"id": 1}
        inline = [{"name": "id"}, {"name": "nonexistent"}]
        result = map_response_to_return_type(data, inline_fields=inline)
        assert result == {"id": 1}
        assert "nonexistent" not in result

    def test_none_data_passthrough(self):
        result = map_response_to_return_type(None, inline_fields=None)
        assert result is None

    def test_empty_list_input(self):
        result = map_response_to_return_type([], inline_fields=[{"name": "id"}])
        assert result == []

    def test_empty_dict_input(self):
        result = map_response_to_return_type({}, inline_fields=[{"name": "id"}])
        assert result == {}


# ---------------------------------------------------------------------------
# TestWebhooksPublicInterface
# ---------------------------------------------------------------------------


class TestWebhooksPublicInterface:
    def test_execute_webhook_importable_from_package(self):
        from provisa.webhooks import execute_webhook as _ew
        assert callable(_ew)

    def test_execute_webhook_is_same_as_executor_function(self):
        from provisa.webhooks import execute_webhook as pkg_fn
        from provisa.webhooks.executor import execute_webhook as mod_fn
        assert pkg_fn is mod_fn


# ---------------------------------------------------------------------------
# TestTrackedFunctionSchema (webhook config as tracked function schema)
# ---------------------------------------------------------------------------


class TestTrackedFunctionSchema:
    """Verify that Webhook model can represent schema for tracked webhook mutations."""

    def test_webhook_with_returns_table_id(self):
        wh = Webhook(
            name="placeOrder",
            url="https://api.example.com/place-order",
            returns="sales.public.orders",
        )
        assert wh.returns == "sales.public.orders"

    def test_webhook_argument_types(self):
        wh = _make_webhook(
            arguments=[
                FunctionArgument(name="product_id", type="Int"),
                FunctionArgument(name="quantity", type="Int"),
                FunctionArgument(name="note", type="String"),
                FunctionArgument(name="price", type="Float"),
                FunctionArgument(name="rush", type="Boolean"),
            ],
        )
        type_map = {arg.name: arg.type for arg in wh.arguments}
        assert type_map["product_id"] == "Int"
        assert type_map["quantity"] == "Int"
        assert type_map["note"] == "String"
        assert type_map["price"] == "Float"
        assert type_map["rush"] == "Boolean"

    def test_webhook_inline_return_type_fields(self):
        wh = Webhook(
            name="summarize",
            url="https://api.example.com/summarize",
            inline_return_type=[
                InlineType(name="total", type="Float"),
                InlineType(name="count", type="Int"),
                InlineType(name="label", type="String"),
            ],
        )
        field_names = {f.name for f in wh.inline_return_type}
        assert field_names == {"total", "count", "label"}

    def test_webhook_method_variants(self):
        for method in ("GET", "POST", "PUT", "PATCH", "DELETE"):
            wh = _make_webhook(method=method)
            assert wh.method == method

    def test_webhook_without_inline_or_returns_is_valid(self):
        """A webhook with no return type config is valid (returns opaque JSON)."""
        wh = Webhook(name="fire_and_forget", url="https://hook.example.com/evt")
        assert wh.returns is None
        assert wh.inline_return_type == []
