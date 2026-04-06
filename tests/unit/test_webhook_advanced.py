# Copyright (c) 2026 Kenneth Stott
# Canary: f1e2d3c4-b5a6-9780-edcb-a09876543210
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Advanced unit tests for provisa/webhooks/executor.py — behaviors NOT
already covered in tests/unit/test_actions.py.

test_actions.py already covers:
- execute_webhook sends POST with JSON body (200 success)
- execute_webhook applies httpx.Timeout (type check only)
- execute_webhook raises HTTPStatusError on 500
- execute_webhook supports GET method
- map_response_to_return_type filters dict to inline fields
- map_response_to_return_type maps list of dicts through filter
- map_response_to_return_type returns raw data when inline_fields is None
- map_response_to_return_type returns scalar unchanged
- map_response_to_return_type skips non-dict list items

This file adds:
- HTTP 4xx (400, 403, 404) raises HTTPStatusError — distinct from the 500 case
- Timeout value is correctly computed as timeout_ms / 1000.0 (numeric accuracy)
- TimeoutException propagation from httpx
- WebhookResult headers are populated from response headers
- WebhookResult status_code matches the response status_code on success
- Empty inline_fields list (not None) returns empty dict / empty list elements
- map_response_to_return_type with extra fields in response → only declared kept
- map_response_to_return_type with None data returns None
- map_response_to_return_type with empty list returns empty list
- map_response_to_return_type with empty dict and inline_fields returns empty dict
- Content-Type header is set to application/json in the outgoing request
- execute_webhook returns WebhookResult dataclass (not a plain dict)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from provisa.core.models import Webhook
from provisa.webhooks.executor import (
    WebhookResult,
    execute_webhook,
    map_response_to_return_type,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_client(status_code: int, json_data, headers: dict | None = None):
    """Return (mock_client_cls, mock_response) with the given status and data."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.json.return_value = json_data
    mock_response.headers = headers or {"content-type": "application/json"}
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.request = AsyncMock(return_value=mock_response)

    return mock_client, mock_response


# ---------------------------------------------------------------------------
# HTTP 4xx responses
# ---------------------------------------------------------------------------

class TestWebhookHTTP4xxResponses:
    pytestmark = pytest.mark.asyncio(loop_scope="session")

    async def test_http_400_raises_status_error(self):
        """execute_webhook raises HTTPStatusError when response is 400."""
        wh = Webhook(name="badReq", url="https://example.com/bad")
        mock_client, mock_response = _make_mock_client(400, None)
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "400 Bad Request", request=MagicMock(), response=mock_response
        )

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(httpx.HTTPStatusError):
                await execute_webhook(wh, {"bad": "data"})

    async def test_http_403_raises_status_error(self):
        """execute_webhook raises HTTPStatusError when response is 403."""
        wh = Webhook(name="forbidden", url="https://example.com/forbidden")
        mock_client, mock_response = _make_mock_client(403, None)
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "403 Forbidden", request=MagicMock(), response=mock_response
        )

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(httpx.HTTPStatusError):
                await execute_webhook(wh, {})

    async def test_http_404_raises_status_error(self):
        """execute_webhook raises HTTPStatusError when response is 404."""
        wh = Webhook(name="notFound", url="https://example.com/missing")
        mock_client, mock_response = _make_mock_client(404, None)
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=MagicMock(), response=mock_response
        )

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(httpx.HTTPStatusError):
                await execute_webhook(wh, {})


# ---------------------------------------------------------------------------
# Timeout accuracy
# ---------------------------------------------------------------------------

class TestWebhookTimeoutAccuracy:
    pytestmark = pytest.mark.asyncio(loop_scope="session")

    async def test_timeout_value_is_timeout_ms_divided_by_1000(self):
        """httpx.Timeout receives the exact seconds value (timeout_ms / 1000.0)."""
        wh = Webhook(name="preciseHook", url="https://example.com/t", timeout_ms=3500)
        captured: dict = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.headers = {}
        mock_response.raise_for_status = MagicMock()

        def capture_init(timeout=None):
            captured["seconds"] = timeout.connect if isinstance(timeout, httpx.Timeout) else None
            client = AsyncMock()
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            client.request = AsyncMock(return_value=mock_response)
            return client

        with patch("provisa.webhooks.executor.httpx.AsyncClient", side_effect=capture_init):
            await execute_webhook(wh, {})

        # httpx.Timeout(3.5) stores 3.5 as its connect/read/write/pool values
        assert captured.get("seconds") == pytest.approx(3.5, abs=1e-9)

    async def test_default_timeout_ms_is_5000(self):
        """Default Webhook timeout_ms is 5000, translating to 5.0 seconds."""
        wh = Webhook(name="defaultTimeout", url="https://example.com/dt")
        assert wh.timeout_ms == 5000

        captured: dict = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.headers = {}
        mock_response.raise_for_status = MagicMock()

        def capture_init(timeout=None):
            captured["timeout"] = timeout
            client = AsyncMock()
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            client.request = AsyncMock(return_value=mock_response)
            return client

        with patch("provisa.webhooks.executor.httpx.AsyncClient", side_effect=capture_init):
            await execute_webhook(wh, {})

        assert isinstance(captured.get("timeout"), httpx.Timeout)
        assert captured["timeout"].connect == pytest.approx(5.0, abs=1e-9)


# ---------------------------------------------------------------------------
# Timeout exception propagation
# ---------------------------------------------------------------------------

class TestWebhookTimeoutException:
    pytestmark = pytest.mark.asyncio(loop_scope="session")

    async def test_timeout_exception_propagates(self):
        """httpx.TimeoutException raised by the request bubbles out unchanged."""
        wh = Webhook(name="slowHook", url="https://example.com/slow", timeout_ms=100)

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(
            side_effect=httpx.TimeoutException("Request timed out")
        )

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(httpx.TimeoutException):
                await execute_webhook(wh, {"q": "slow"})


# ---------------------------------------------------------------------------
# WebhookResult shape
# ---------------------------------------------------------------------------

class TestWebhookResultShape:
    pytestmark = pytest.mark.asyncio(loop_scope="session")

    async def test_result_is_webhook_result_dataclass(self):
        """execute_webhook returns a WebhookResult, not a plain dict."""
        wh = Webhook(name="shapeHook", url="https://example.com/shape")
        mock_client, _ = _make_mock_client(200, {"status": "ok"})

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            result = await execute_webhook(wh, {})

        assert isinstance(result, WebhookResult)

    async def test_result_status_code_matches_response(self):
        """WebhookResult.status_code mirrors the HTTP response status."""
        wh = Webhook(name="statusHook", url="https://example.com/s")
        mock_client, _ = _make_mock_client(201, {"created": True})

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            result = await execute_webhook(wh, {})

        assert result.status_code == 201

    async def test_result_headers_populated_from_response(self):
        """WebhookResult.headers contains the response headers as a dict."""
        wh = Webhook(name="headerHook", url="https://example.com/h")
        response_headers = {
            "content-type": "application/json",
            "x-request-id": "abc-123",
        }
        mock_client, _ = _make_mock_client(200, {}, headers=response_headers)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            result = await execute_webhook(wh, {})

        assert isinstance(result.headers, dict)
        assert result.headers.get("content-type") == "application/json"
        assert result.headers.get("x-request-id") == "abc-123"

    async def test_result_data_matches_json_response(self):
        """WebhookResult.data is the parsed JSON body."""
        wh = Webhook(name="dataHook", url="https://example.com/d")
        payload = {"order_id": 42, "confirmed": True}
        mock_client, _ = _make_mock_client(200, payload)

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            result = await execute_webhook(wh, {})

        assert result.data == payload

    async def test_content_type_header_sent_as_application_json(self):
        """Outgoing request always includes Content-Type: application/json."""
        wh = Webhook(name="ctHook", url="https://example.com/ct")
        mock_client, _ = _make_mock_client(200, {})

        with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
            await execute_webhook(wh, {"x": 1})

        call_kwargs = mock_client.request.call_args.kwargs
        headers_sent = call_kwargs.get("headers", {})
        assert headers_sent.get("Content-Type") == "application/json"


# ---------------------------------------------------------------------------
# map_response_to_return_type — edge cases not in test_actions.py
# ---------------------------------------------------------------------------

class TestMapResponseToReturnTypeAdvanced:
    def test_empty_inline_fields_list_returns_empty_dict(self):
        """Empty inline_fields list (not None) filters dict to nothing."""
        data = {"a": 1, "b": 2}
        result = map_response_to_return_type(data, [])
        assert result == {}

    def test_empty_inline_fields_list_on_list_returns_empty_dicts(self):
        """Empty inline_fields list applied to a list produces empty dicts."""
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        result = map_response_to_return_type(data, [])
        assert result == [{}, {}]

    def test_extra_response_fields_filtered_out(self):
        """Fields in the response that are not in inline_fields are excluded."""
        data = {"id": 1, "name": "Alice", "ssn": "000-00-0000", "salary": 99000}
        inline = [{"name": "id", "type": "Int"}, {"name": "name", "type": "String"}]
        result = map_response_to_return_type(data, inline)
        assert "ssn" not in result
        assert "salary" not in result
        assert result == {"id": 1, "name": "Alice"}

    def test_none_data_with_inline_fields_returns_none(self):
        """None data is returned as-is even when inline_fields are provided."""
        result = map_response_to_return_type(None, [{"name": "id", "type": "Int"}])
        assert result is None

    def test_empty_list_with_inline_fields_returns_empty_list(self):
        """An empty list input produces an empty list output."""
        result = map_response_to_return_type([], [{"name": "id", "type": "Int"}])
        assert result == []

    def test_empty_dict_with_inline_fields_returns_empty_dict(self):
        """An empty dict produces an empty dict output."""
        result = map_response_to_return_type({}, [{"name": "id", "type": "Int"}])
        assert result == {}

    def test_inline_fields_with_single_field_extracts_correctly(self):
        """Single inline field extracts exactly that one key."""
        data = {"alpha": 10, "beta": 20, "gamma": 30}
        result = map_response_to_return_type(data, [{"name": "beta", "type": "Int"}])
        assert result == {"beta": 20}

    def test_missing_inline_field_in_response_not_included(self):
        """If an inline field is declared but absent from the response, it's not added."""
        data = {"id": 5}
        inline = [{"name": "id", "type": "Int"}, {"name": "missing_field", "type": "String"}]
        result = map_response_to_return_type(data, inline)
        # The intersection with data keys means 'missing_field' won't appear
        assert "missing_field" not in result
        assert result == {"id": 5}

    def test_boolean_and_numeric_values_preserved(self):
        """Values of varied types (bool, int, float) are preserved correctly."""
        data = {"active": True, "score": 98, "ratio": 0.75, "label": "ok"}
        inline = [
            {"name": "active", "type": "Boolean"},
            {"name": "score", "type": "Int"},
            {"name": "ratio", "type": "Float"},
        ]
        result = map_response_to_return_type(data, inline)
        assert result == {"active": True, "score": 98, "ratio": 0.75}
        assert "label" not in result

    def test_list_with_varying_keys_per_element(self):
        """List items with varying key sets are individually filtered."""
        data = [
            {"id": 1, "name": "Alice", "secret": "x"},
            {"id": 2, "name": "Bob"},          # no 'secret' key
            {"id": 3, "secret": "z"},           # no 'name' key
        ]
        inline = [{"name": "id", "type": "Int"}, {"name": "name", "type": "String"}]
        result = map_response_to_return_type(data, inline)
        assert result[0] == {"id": 1, "name": "Alice"}
        assert result[1] == {"id": 2, "name": "Bob"}
        assert result[2] == {"id": 3}           # 'name' absent from source

    def test_integer_response_with_inline_fields_returned_as_is(self):
        """A bare integer (non-dict, non-list) is returned unchanged even with inline_fields."""
        result = map_response_to_return_type(42, [{"name": "count", "type": "Int"}])
        assert result == 42

    def test_none_inline_fields_returns_list_unchanged(self):
        """None inline_fields returns list data without any filtering."""
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        result = map_response_to_return_type(data, None)
        assert result == data
