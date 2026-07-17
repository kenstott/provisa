# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/api_source/caller.py.

httpx.AsyncClient is patched at its import site (provisa.api_source.caller.httpx)
since caller.py does `import httpx` at module scope.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.api_source import caller
from provisa.api_source.caller import (
    ApiCallError,
    ApiNotFoundError,
    _apply_auth,
    _build_request_parts,
    _fetch_oauth2_token,
    _paginate,
    call_api,
)
from provisa.api_source.models import (
    ApiColumn,
    ApiColumnType,
    ApiEndpoint,
    PaginationConfig,
    PaginationType,
    ParamType,
)
from provisa.core.auth_models import (
    ApiAuthApiKey,
    ApiAuthBasic,
    ApiAuthBearer,
    ApiAuthCustomHeaders,
    ApiAuthOAuth2ClientCredentials,
    ApiKeyLocation,
)


def _col(
    name: str,
    param_type: ParamType | None = None,
    param_name: str | None = None,
    col_type: ApiColumnType = ApiColumnType.string,
) -> ApiColumn:
    return ApiColumn(
        name=name,
        type=col_type,
        filterable=True,
        param_type=param_type,
        param_name=param_name,
    )


def _endpoint(**kwargs) -> ApiEndpoint:
    defaults: dict = dict(
        source_id="src1",
        path="/pets",
        method="GET",
        table_name="pets",
        columns=[],
    )
    defaults.update(kwargs)
    return ApiEndpoint(**defaults)


def _resp(status_code=200, json_data=None, headers=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    resp.headers = headers or {}
    resp.text = text
    resp.url = "http://example.com/pets"
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# _build_request_parts
# ---------------------------------------------------------------------------


class TestBuildRequestParts:
    def test_query_param(self):
        endpoint = _endpoint(
            columns=[_col("status", ParamType.query, "status")],
        )
        url, params, headers, body = _build_request_parts(endpoint, {"status": "open"})
        assert url == "/pets"
        assert params == {"status": "open"}
        assert headers == {}
        assert body is None

    def test_path_param_substitution(self):
        endpoint = _endpoint(
            path="/pets/{petId}",
            columns=[_col("petId", ParamType.path, "petId")],
        )
        url, params, headers, body = _build_request_parts(endpoint, {"petId": "42"})
        assert url == "/pets/42"
        assert params == {}

    def test_body_param(self):
        endpoint = _endpoint(columns=[_col("name", ParamType.body, "name")])
        url, params, headers, body = _build_request_parts(endpoint, {"name": "Rex"})
        assert body == {"name": "Rex"}

    def test_header_param(self):
        endpoint = _endpoint(columns=[_col("x", ParamType.header, "X-Custom")])
        url, params, headers, body = _build_request_parts(endpoint, {"x": 5})
        assert headers == {"X-Custom": "5"}

    def test_variable_param_goes_to_body(self):
        endpoint = _endpoint(columns=[_col("var1", ParamType.variable, "var1")])
        url, params, headers, body = _build_request_parts(endpoint, {"var1": "abc"})
        assert body == {"var1": "abc"}

    def test_no_param_type_skipped(self):
        endpoint = _endpoint(columns=[_col("plain")])
        url, params, headers, body = _build_request_parts(endpoint, {"plain": "x"})
        assert params == {}
        assert body is None

    def test_missing_value_skipped(self):
        endpoint = _endpoint(columns=[_col("status", ParamType.query, "status")])
        url, params, headers, body = _build_request_parts(endpoint, {})
        assert params == {}

    def test_falls_back_to_column_name_key(self):
        endpoint = _endpoint(columns=[_col("status", ParamType.query, "status_param")])
        url, params, headers, body = _build_request_parts(endpoint, {"status": "open"})
        assert params == {"status_param": "open"}

    def test_no_body_parts_is_none(self):
        endpoint = _endpoint(columns=[_col("status", ParamType.query, "status")])
        _, _, _, body = _build_request_parts(endpoint, {"status": "open"})
        assert body is None


# ---------------------------------------------------------------------------
# _request_with_retry (via _paginate for simplicity, and directly)
# ---------------------------------------------------------------------------


class TestRequestWithRetry:
    @pytest.mark.asyncio
    async def test_success_first_try(self):
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(200, {"ok": True}))
        resp = await caller._request_with_retry(client, "GET", "/pets")
        assert resp.status_code == 200
        client.request.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_404_raises_not_found(self):
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(404))
        with pytest.raises(ApiNotFoundError):
            await caller._request_with_retry(client, "GET", "/pets")

    @pytest.mark.asyncio
    async def test_500_retries_then_raises(self):
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(500, text="boom"))
        with patch("provisa.api_source.caller.asyncio.sleep", new=AsyncMock()):
            with pytest.raises(ApiCallError, match="failed after 3 retries"):
                await caller._request_with_retry(client, "GET", "/pets")
        assert client.request.await_count == 3

    @pytest.mark.asyncio
    async def test_429_retries_then_succeeds(self):
        client = MagicMock()
        client.request = AsyncMock(
            side_effect=[_resp(429, text="slow down"), _resp(200, {"ok": True})]
        )
        with patch("provisa.api_source.caller.asyncio.sleep", new=AsyncMock()) as sleep_mock:
            resp = await caller._request_with_retry(client, "GET", "/pets")
        assert resp.status_code == 200
        sleep_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_other_error_status_raises_for_status(self):
        resp_obj = _resp(400, text="bad request")
        resp_obj.raise_for_status.side_effect = RuntimeError("400 error")
        client = MagicMock()
        client.request = AsyncMock(return_value=resp_obj)
        with pytest.raises(RuntimeError):
            await caller._request_with_retry(client, "GET", "/pets")


# ---------------------------------------------------------------------------
# _paginate
# ---------------------------------------------------------------------------


class TestPaginate:
    @pytest.mark.asyncio
    async def test_no_pagination_single_page(self):
        endpoint = _endpoint(pagination=None)
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(200, {"items": [1, 2]}))
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert pages == [{"items": [1, 2]}]

    @pytest.mark.asyncio
    async def test_link_header_pagination(self):
        pagination = PaginationConfig(type=PaginationType.link_header, max_pages=5)
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(
            side_effect=[
                _resp(200, {"p": 1}, headers={"link": '<http://x/pets?page=2>; rel="next"'}),
                _resp(200, {"p": 2}, headers={}),
            ]
        )
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert pages == [{"p": 1}, {"p": 2}]
        assert client.request.await_count == 2

    @pytest.mark.asyncio
    async def test_link_header_pagination_no_next_stops(self):
        pagination = PaginationConfig(type=PaginationType.link_header, max_pages=5)
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(200, {"p": 1}, headers={}))
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert pages == [{"p": 1}]
        assert client.request.await_count == 1

    @pytest.mark.asyncio
    async def test_cursor_pagination(self):
        pagination = PaginationConfig(
            type=PaginationType.cursor,
            cursor_param="cursor",
            cursor_field="next_cursor",
            max_pages=5,
        )
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(
            side_effect=[
                _resp(200, {"items": [1], "next_cursor": "abc"}),
                _resp(200, {"items": [2]}),
            ]
        )
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert len(pages) == 2
        assert client.request.await_count == 2
        # second call should have cursor param injected
        second_call_kwargs = client.request.await_args_list[1].kwargs
        assert second_call_kwargs["params"] == {"cursor": "abc"}

    @pytest.mark.asyncio
    async def test_cursor_pagination_default_names(self):
        pagination = PaginationConfig(type=PaginationType.cursor, max_pages=3)
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(200, {"items": []}))
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert pages == [{"items": []}]

    @pytest.mark.asyncio
    async def test_cursor_pagination_non_dict_response_stops(self):
        pagination = PaginationConfig(type=PaginationType.cursor, max_pages=5)
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(200, [1, 2, 3]))
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert pages == [[1, 2, 3]]
        assert client.request.await_count == 1

    @pytest.mark.asyncio
    async def test_offset_pagination_short_page_stops(self):
        pagination = PaginationConfig(
            type=PaginationType.offset,
            page_size=2,
            page_size_param="limit",
            page_param="offset",
            max_pages=10,
        )
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(
            side_effect=[
                _resp(200, [1, 2]),
                _resp(200, [3]),
            ]
        )
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert pages == [[1, 2], [3]]
        assert client.request.await_count == 2
        second_call_kwargs = client.request.await_args_list[1].kwargs
        assert second_call_kwargs["params"] == {"limit": 2, "offset": 2}

    @pytest.mark.asyncio
    async def test_offset_pagination_max_pages_stops(self):
        pagination = PaginationConfig(
            type=PaginationType.offset,
            page_size=2,
            max_pages=2,
        )
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(200, [1, 2]))
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert len(pages) == 2

    @pytest.mark.asyncio
    async def test_page_number_pagination_short_page_stops(self):
        pagination = PaginationConfig(
            type=PaginationType.page_number,
            page_size=2,
            page_param="page",
            page_size_param="per_page",
            max_pages=10,
        )
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(
            side_effect=[
                _resp(200, [1, 2]),
                _resp(200, [3]),
            ]
        )
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert pages == [[1, 2], [3]]
        first_call_kwargs = client.request.await_args_list[0].kwargs
        assert first_call_kwargs["params"] == {"page": 1, "per_page": 2}

    @pytest.mark.asyncio
    async def test_page_number_pagination_max_pages_stops(self):
        pagination = PaginationConfig(
            type=PaginationType.page_number,
            page_size=2,
            max_pages=2,
        )
        endpoint = _endpoint(pagination=pagination)
        client = MagicMock()
        client.request = AsyncMock(return_value=_resp(200, [1, 2]))
        pages = await _paginate(client, endpoint, "/pets", {}, {}, None, 30.0)
        assert len(pages) == 2


# ---------------------------------------------------------------------------
# _apply_auth
# ---------------------------------------------------------------------------


class TestApplyAuth:
    def test_none_auth_noop(self):
        headers: dict = {}
        params: dict = {}
        _apply_auth(None, headers, params)
        assert headers == {}
        assert params == {}

    def test_legacy_dict_bearer(self):
        headers: dict = {}
        _apply_auth({"bearer": "tok123"}, headers, {})
        assert headers["Authorization"] == "Bearer tok123"

    def test_legacy_dict_api_key_header(self):
        headers: dict = {}
        _apply_auth({"api_key_header": "X-Api-Key", "api_key": "secret"}, headers, {})
        assert headers["X-Api-Key"] == "secret"

    def test_legacy_dict_headers(self):
        headers: dict = {}
        _apply_auth({"headers": {"X-A": "1", "X-B": "2"}}, headers, {})
        assert headers == {"X-A": "1", "X-B": "2"}

    def test_typed_bearer(self):
        headers: dict = {}
        _apply_auth(ApiAuthBearer(token="tok"), headers, {})
        assert headers["Authorization"] == "Bearer tok"

    def test_typed_basic(self):
        headers: dict = {}
        _apply_auth(ApiAuthBasic(username="u", password="p"), headers, {})
        assert headers["Authorization"].startswith("Basic ")

    def test_typed_api_key_header_location(self):
        headers: dict = {}
        params: dict = {}
        _apply_auth(
            ApiAuthApiKey(key="k", name="X-Key", location=ApiKeyLocation.header),
            headers,
            params,
        )
        assert headers["X-Key"] == "k"
        assert params == {}

    def test_typed_api_key_query_location(self):
        headers: dict = {}
        params: dict = {}
        _apply_auth(
            ApiAuthApiKey(key="k", name="api_key", location=ApiKeyLocation.query),
            headers,
            params,
        )
        assert params["api_key"] == "k"
        assert headers == {}

    def test_typed_custom_headers(self):
        headers: dict = {}
        _apply_auth(ApiAuthCustomHeaders(headers={"X-A": "1"}), headers, {})
        assert headers["X-A"] == "1"

    def test_typed_oauth2(self):
        headers: dict = {}
        with patch(
            "provisa.api_source.caller._fetch_oauth2_token", return_value="oauth-tok"
        ) as fetch_mock:
            _apply_auth(
                ApiAuthOAuth2ClientCredentials(
                    client_id="cid", client_secret="secret", token_url="http://tok"
                ),
                headers,
                {},
            )
        fetch_mock.assert_called_once()
        assert headers["Authorization"] == "Bearer oauth-tok"


# ---------------------------------------------------------------------------
# _fetch_oauth2_token
# ---------------------------------------------------------------------------


class TestFetchOauth2Token:
    def setup_method(self):
        caller._oauth2_cache.clear()

    def teardown_method(self):
        caller._oauth2_cache.clear()

    def test_fetches_and_caches(self):
        oauth = MagicMock(
            client_id="cid1",
            client_secret="secret1",
            token_url="http://token",
            scope=None,
        )
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"access_token": "tok-abc", "expires_in": 3600}
        with patch("httpx.post", return_value=resp) as post_mock:
            token = _fetch_oauth2_token(oauth)
        assert token == "tok-abc"
        post_mock.assert_called_once()
        sent_data = post_mock.call_args.kwargs["data"]
        assert sent_data["grant_type"] == "client_credentials"
        assert "scope" not in sent_data

    def test_uses_cache_on_second_call(self):
        oauth = MagicMock(
            client_id="cid2",
            client_secret="secret2",
            token_url="http://token2",
            scope="read",
        )
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"access_token": "tok-xyz", "expires_in": 3600}
        with patch("httpx.post", return_value=resp) as post_mock:
            token1 = _fetch_oauth2_token(oauth)
            token2 = _fetch_oauth2_token(oauth)
        assert token1 == token2 == "tok-xyz"
        post_mock.assert_called_once()

    def test_expired_cache_refetches(self):
        oauth = MagicMock(
            client_id="cid3",
            client_secret="secret3",
            token_url="http://token3",
            scope=None,
        )
        caller._oauth2_cache["cid3:http://token3"] = ("stale-tok", 0.0)
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"access_token": "fresh-tok", "expires_in": 3600}
        with patch("httpx.post", return_value=resp) as post_mock:
            token = _fetch_oauth2_token(oauth)
        assert token == "fresh-tok"
        post_mock.assert_called_once()


# ---------------------------------------------------------------------------
# call_api
# ---------------------------------------------------------------------------


class TestCallApi:
    @pytest.mark.asyncio
    async def test_basic_get_json_body(self):
        endpoint = _endpoint(columns=[_col("status", ParamType.query, "status")])
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(200, {"items": [1]}))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            pages = await call_api(endpoint, {"status": "open"}, base_url="http://api.test")
        assert pages == [{"items": [1]}]
        call_kwargs = mock_client.request.await_args.kwargs
        assert call_kwargs["params"] == {"status": "open"}

    @pytest.mark.asyncio
    async def test_prepends_base_url_for_relative_path(self):
        endpoint = _endpoint(path="pets")
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(200, {}))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            await call_api(endpoint, {}, base_url="http://api.test/")
        call_args = mock_client.request.await_args
        assert call_args.args[1] == "http://api.test/pets"

    @pytest.mark.asyncio
    async def test_absolute_path_not_prefixed(self):
        endpoint = _endpoint(path="http://other.test/pets")
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(200, {}))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            await call_api(endpoint, {}, base_url="http://api.test")
        call_args = mock_client.request.await_args
        assert call_args.args[1] == "http://other.test/pets"

    @pytest.mark.asyncio
    async def test_graphql_query_method_wraps_body(self):
        endpoint = _endpoint(
            path="query { pets { id } }",
            method="QUERY",
            columns=[_col("var1", ParamType.variable, "var1")],
        )
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(200, {"data": {}}))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            await call_api(endpoint, {"var1": "x"}, base_url="http://api.test")
        call_kwargs = mock_client.request.await_args.kwargs
        assert call_kwargs["json"] == {
            "query": "query { pets { id } }",
            "variables": {"var1": "x"},
        }

    @pytest.mark.asyncio
    async def test_rpc_method_delegates_to_grpc(self):
        endpoint = _endpoint(method="RPC", path="/svc/Method")
        with patch(
            "provisa.api_source.caller._call_grpc", new=AsyncMock(return_value=[{"ok": True}])
        ) as grpc_mock:
            pages = await call_api(endpoint, {}, base_url="host:1234")
        grpc_mock.assert_awaited_once()
        assert pages == [{"ok": True}]

    @pytest.mark.asyncio
    async def test_json_body_encoding_neo4j(self):
        endpoint = _endpoint(
            body_encoding="json",
            query_template="MATCH (n) RETURN n",
        )
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(200, {}))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            await call_api(endpoint, {}, base_url="http://api.test")
        call_kwargs = mock_client.request.await_args.kwargs
        assert call_kwargs["json"] == {"statement": "MATCH (n) RETURN n"}

    @pytest.mark.asyncio
    async def test_form_body_encoding_sparql(self):
        endpoint = _endpoint(
            body_encoding="form",
            query_template="SELECT * WHERE { ?s ?p ?o }",
            columns=[_col("limit", ParamType.body, "limit")],
        )
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(200, {}))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            await call_api(endpoint, {"limit": 5}, base_url="http://api.test")
        call_kwargs = mock_client.request.await_args.kwargs
        assert call_kwargs["data"] == {"query": "SELECT * WHERE { ?s ?p ?o }", "limit": 5}

    @pytest.mark.asyncio
    async def test_default_body_encoding_uses_json_body(self):
        endpoint = _endpoint(
            method="POST",
            columns=[_col("name", ParamType.body, "name")],
        )
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(200, {}))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            await call_api(endpoint, {"name": "Rex"}, base_url="http://api.test")
        call_kwargs = mock_client.request.await_args.kwargs
        assert call_kwargs["json"] == {"name": "Rex"}

    @pytest.mark.asyncio
    async def test_applies_auth_bearer(self):
        endpoint = _endpoint()
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(200, {}))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            await call_api(
                endpoint, {}, base_url="http://api.test", auth=ApiAuthBearer(token="tok")
            )
        call_kwargs = mock_client.request.await_args.kwargs
        assert call_kwargs["headers"]["Authorization"] == "Bearer tok"

    @pytest.mark.asyncio
    async def test_propagates_not_found(self):
        endpoint = _endpoint()
        mock_client = MagicMock()
        mock_client.request = AsyncMock(return_value=_resp(404))
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_ctx):
            with pytest.raises(ApiNotFoundError):
                await call_api(endpoint, {}, base_url="http://api.test")


# ---------------------------------------------------------------------------
# _call_grpc
# ---------------------------------------------------------------------------


class TestCallGrpc:
    @pytest.mark.asyncio
    async def test_invalid_path_raises(self):
        endpoint = _endpoint(method="RPC", path="/onlyoneseg")
        with pytest.raises(ApiCallError, match="Invalid gRPC path"):
            await caller._call_grpc(endpoint, {}, "host:1234")

    @pytest.mark.asyncio
    async def test_service_not_resolved_raises(self):
        endpoint = _endpoint(method="RPC", path="/svc.Service/Method")

        mock_channel = MagicMock()
        mock_stub = MagicMock()
        mock_stub.ServerReflectionInfo.return_value = iter([])

        with patch("grpc.insecure_channel", return_value=mock_channel):
            with patch(
                "grpc_reflection.v1alpha.reflection_pb2_grpc.ServerReflectionStub",
                return_value=mock_stub,
            ):
                with pytest.raises(ApiCallError, match="Could not resolve gRPC service"):
                    await caller._call_grpc(endpoint, {}, "host:1234")
        mock_channel.close.assert_called_once()
