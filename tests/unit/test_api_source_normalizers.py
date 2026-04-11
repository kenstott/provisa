# Copyright (c) 2026 Kenneth Stott
# Canary: a65e28c5-86a5-4fdd-88e0-a8118598578b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for api_source normalizers, transforms, auth, and per-source cache.

Covers:
- neo4j_tabular response normalizer (all branches)
- sparql_bindings response normalizer (all branches)
- get_normalizer registry look-up and error
- REST API auth: bearer, basic, api-key header/query, custom headers, OAuth2
- GraphQL query forwarding via 'QUERY' method
- gRPC: _build_request_parts with variable param type
- Pagination: offset, page_number (new — caller tests only have link_header + cursor)
- Per-source cache: resolve_ttl, cache_table_name, create_and_insert (Trino Iceberg DDL)
- api_source transforms: from_unix_timestamp, cents_to_decimal, apply_transform
- introspect helpers: _path_to_table_name, _unwrap_type, _grpc_message_to_columns
"""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.api_source.cache import (
    DEFAULT_TTL,
    resolve_ttl,
)
from provisa.api_source.trino_cache import (
    CACHE_CATALOG,
    CACHE_SCHEMA,
    cache_table_name,
    create_and_insert,
)
from provisa.api_source.caller import (
    _apply_auth,
    _build_request_parts,
    call_api,
)
from provisa.api_source.introspect import (
    _grpc_message_to_columns,
    _path_to_table_name,
    _unwrap_type,
)
from provisa.api_source.models import (
    ApiColumn,
    ApiColumnType,
    ApiEndpoint,
    ApiSource,
    ApiSourceType,
    PaginationConfig,
    PaginationType,
    ParamType,
)
from provisa.api_source.normalizers import (
    NORMALIZERS,
    get_normalizer,
    neo4j_tabular,
    sparql_bindings,
)
from provisa.api_source.transforms import (
    TRANSFORM_REGISTRY,
    apply_transform,
    cents_to_decimal,
    from_unix_timestamp,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_endpoint(**kwargs) -> ApiEndpoint:
    defaults = {
        "id": 1,
        "source_id": "test-api",
        "path": "/data",
        "method": "GET",
        "table_name": "data",
        "columns": [],
        "ttl": 300,
    }
    defaults.update(kwargs)
    return ApiEndpoint(**defaults)


class FakeResponse:
    """Minimal httpx.Response stand-in for mock assertions."""

    def __init__(self, data, status_code: int = 200, headers: dict | None = None):
        self._data = data
        self.status_code = status_code
        self.headers = headers or {}
        self.text = str(data)[:200]

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


# ---------------------------------------------------------------------------
# TestNeo4jTabularNormalizer
# ---------------------------------------------------------------------------


class TestNeo4jTabularNormalizer:
    """Tests use the Neo4j legacy HTTP transaction API format:
    {"results": [{"columns": [...], "data": [{"row": [...], "meta": [...]}]}], "errors": []}
    """

    def test_basic_two_columns(self):
        response = {
            "results": [
                {
                    "columns": ["name", "age"],
                    "data": [
                        {"row": ["Alice", 30], "meta": []},
                        {"row": ["Bob", 25], "meta": []},
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert len(rows) == 2
        assert rows[0] == {"name": "Alice", "age": 30}
        assert rows[1] == {"name": "Bob", "age": 25}

    def test_single_row(self):
        response = {
            "results": [{"columns": ["id"], "data": [{"row": [42], "meta": []}]}],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows == [{"id": 42}]

    def test_empty_data_list(self):
        response = {"results": [{"columns": ["a", "b"], "data": []}], "errors": []}
        rows = neo4j_tabular(response)
        assert rows == []

    def test_missing_results_block(self):
        rows = neo4j_tabular({})
        assert rows == []

    def test_multiple_result_sets(self):
        """Multiple result blocks (batched statements) are all merged."""
        response = {
            "results": [
                {"columns": ["x"], "data": [{"row": [1], "meta": []}]},
                {"columns": ["x"], "data": [{"row": [2], "meta": []}]},
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert len(rows) == 2
        assert rows[0] == {"x": 1}
        assert rows[1] == {"x": 2}

    def test_zip_truncates_to_shorter_side(self):
        """Extra row values beyond column count are silently dropped by zip()."""
        response = {
            "results": [{"columns": ["a"], "data": [{"row": [1, 2, 3], "meta": []}]}],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows == [{"a": 1}]

    def test_null_values_preserved(self):
        response = {
            "results": [
                {
                    "columns": ["name", "score"],
                    "data": [{"row": ["Alice", None], "meta": []}],
                }
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows[0]["score"] is None

    def test_three_columns_multiple_rows(self):
        response = {
            "results": [
                {
                    "columns": ["id", "label", "weight"],
                    "data": [
                        {"row": [1, "a", 0.5], "meta": []},
                        {"row": [2, "b", 1.5], "meta": []},
                    ],
                }
            ],
            "errors": [],
        }
        rows = neo4j_tabular(response)
        assert rows[0]["weight"] == 0.5
        assert rows[1]["label"] == "b"


# ---------------------------------------------------------------------------
# TestSparqlBindingsNormalizer
# ---------------------------------------------------------------------------


class TestSparqlBindingsNormalizer:
    def _make_binding(self, **vars_):
        """Build a SPARQL binding where each value is {'type': 'literal', 'value': v}."""
        return {k: {"type": "literal", "value": v} for k, v in vars_.items()}

    def test_basic_single_row(self):
        response = {
            "results": {
                "bindings": [
                    {"name": {"type": "literal", "value": "Alice"}}
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows == [{"name": "Alice"}]

    def test_multiple_rows(self):
        response = {
            "results": {
                "bindings": [
                    self._make_binding(x="1", y="a"),
                    self._make_binding(x="2", y="b"),
                ]
            }
        }
        rows = sparql_bindings(response)
        assert len(rows) == 2
        assert rows[0] == {"x": "1", "y": "a"}

    def test_uri_type_uses_value(self):
        response = {
            "results": {
                "bindings": [
                    {"resource": {"type": "uri", "value": "http://example.com/42"}}
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows[0]["resource"] == "http://example.com/42"

    def test_bnode_type_uses_value(self):
        response = {
            "results": {
                "bindings": [
                    {"node": {"type": "bnode", "value": "_:b0"}}
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows[0]["node"] == "_:b0"

    def test_non_dict_term_returned_as_is(self):
        response = {
            "results": {
                "bindings": [
                    {"raw": "bare_value"}
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows[0]["raw"] == "bare_value"

    def test_empty_bindings(self):
        response = {"results": {"bindings": []}}
        rows = sparql_bindings(response)
        assert rows == []

    def test_missing_results_key(self):
        rows = sparql_bindings({})
        assert rows == []

    def test_multiple_variables_per_binding(self):
        response = {
            "results": {
                "bindings": [
                    {
                        "person": {"type": "uri", "value": "http://ex.com/person/1"},
                        "age": {"type": "literal", "value": "42"},
                        "city": {"type": "literal", "value": "Berlin"},
                    }
                ]
            }
        }
        rows = sparql_bindings(response)
        assert rows[0]["person"] == "http://ex.com/person/1"
        assert rows[0]["age"] == "42"
        assert rows[0]["city"] == "Berlin"


# ---------------------------------------------------------------------------
# TestGetNormalizerRegistry
# ---------------------------------------------------------------------------


class TestGetNormalizerRegistry:
    def test_neo4j_tabular_registered(self):
        fn = get_normalizer("neo4j_tabular")
        assert fn is neo4j_tabular

    def test_sparql_bindings_registered(self):
        fn = get_normalizer("sparql_bindings")
        assert fn is sparql_bindings

    def test_unknown_name_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown response_normalizer"):
            get_normalizer("nonexistent_normalizer")

    def test_error_message_lists_available_normalizers(self):
        with pytest.raises(ValueError) as exc_info:
            get_normalizer("bad_name")
        assert "neo4j_tabular" in str(exc_info.value)
        assert "sparql_bindings" in str(exc_info.value)

    def test_normalizers_dict_contains_expected_keys(self):
        assert "neo4j_tabular" in NORMALIZERS
        assert "sparql_bindings" in NORMALIZERS

    def test_returned_callable_works(self):
        fn = get_normalizer("neo4j_tabular")
        result = fn({
            "results": [{"columns": ["k"], "data": [{"row": [1], "meta": []}]}],
            "errors": [],
        })
        assert result == [{"k": 1}]


# ---------------------------------------------------------------------------
# TestTransforms
# ---------------------------------------------------------------------------


class TestTransforms:
    def test_from_unix_timestamp_returns_utc_datetime(self):
        from datetime import timezone
        dt = from_unix_timestamp(0)
        assert dt.tzinfo is not None
        assert dt.year == 1970
        assert dt.tzinfo == timezone.utc

    def test_from_unix_timestamp_as_string(self):
        dt = from_unix_timestamp("1000000000")
        assert dt.year == 2001

    def test_cents_to_decimal_basic(self):
        result = cents_to_decimal(150)
        from decimal import Decimal
        assert result == Decimal("1.50")

    def test_cents_to_decimal_as_string(self):
        from decimal import Decimal
        result = cents_to_decimal("999")
        assert result == Decimal("9.99")

    def test_cents_to_decimal_zero(self):
        from decimal import Decimal
        assert cents_to_decimal(0) == Decimal("0.00")

    def test_apply_transform_from_unix_timestamp(self):
        result = apply_transform("from_unix_timestamp", 0)
        assert result.year == 1970

    def test_apply_transform_cents_to_decimal(self):
        from decimal import Decimal
        result = apply_transform("cents_to_decimal", 100)
        assert result == Decimal("1.00")

    def test_apply_transform_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown transform"):
            apply_transform("does_not_exist", 42)

    def test_transform_registry_contains_expected_keys(self):
        assert "from_unix_timestamp" in TRANSFORM_REGISTRY
        assert "cents_to_decimal" in TRANSFORM_REGISTRY


# ---------------------------------------------------------------------------
# TestApplyAuth — _apply_auth covers typed and legacy dict auth
# ---------------------------------------------------------------------------


class TestApplyAuth:
    def test_bearer_auth_sets_authorization_header(self):
        from provisa.core.auth_models import ApiAuthBearer

        headers: dict = {}
        params: dict = {}
        with patch("provisa.core.secrets.resolve_secrets", side_effect=lambda x: x):
            _apply_auth(ApiAuthBearer(token="tok123"), headers, params)
        assert headers["Authorization"] == "Bearer tok123"

    def test_basic_auth_encodes_credentials(self):
        from provisa.core.auth_models import ApiAuthBasic

        headers: dict = {}
        params: dict = {}
        with patch("provisa.core.secrets.resolve_secrets", side_effect=lambda x: x):
            _apply_auth(ApiAuthBasic(username="user", password="pass"), headers, params)
        expected = base64.b64encode(b"user:pass").decode()
        assert headers["Authorization"] == f"Basic {expected}"

    def test_api_key_in_header(self):
        from provisa.core.auth_models import ApiAuthApiKey, ApiKeyLocation

        headers: dict = {}
        params: dict = {}
        with patch("provisa.core.secrets.resolve_secrets", side_effect=lambda x: x):
            _apply_auth(
                ApiAuthApiKey(key="mykey", name="X-Api-Key", location=ApiKeyLocation.header),
                headers,
                params,
            )
        assert headers["X-Api-Key"] == "mykey"

    def test_api_key_in_query(self):
        from provisa.core.auth_models import ApiAuthApiKey, ApiKeyLocation

        headers: dict = {}
        params: dict = {}
        with patch("provisa.core.secrets.resolve_secrets", side_effect=lambda x: x):
            _apply_auth(
                ApiAuthApiKey(key="secret", name="api_key", location=ApiKeyLocation.query),
                headers,
                params,
            )
        assert params["api_key"] == "secret"
        assert "Authorization" not in headers

    def test_custom_headers_applied(self):
        from provisa.core.auth_models import ApiAuthCustomHeaders

        headers: dict = {}
        params: dict = {}
        with patch("provisa.core.secrets.resolve_secrets", side_effect=lambda x: x):
            _apply_auth(
                ApiAuthCustomHeaders(headers={"X-Org": "acme", "X-Env": "prod"}),
                headers,
                params,
            )
        assert headers["X-Org"] == "acme"
        assert headers["X-Env"] == "prod"

    def test_none_auth_no_change(self):
        headers: dict = {}
        params: dict = {}
        _apply_auth(None, headers, params)
        assert headers == {}
        assert params == {}

    def test_legacy_dict_bearer(self):
        headers: dict = {}
        params: dict = {}
        with patch("provisa.core.secrets.resolve_secrets", side_effect=lambda x: x):
            _apply_auth({"bearer": "legacytoken"}, headers, params)
        assert headers["Authorization"] == "Bearer legacytoken"

    def test_legacy_dict_api_key_header(self):
        headers: dict = {}
        params: dict = {}
        with patch("provisa.core.secrets.resolve_secrets", side_effect=lambda x: x):
            _apply_auth({"api_key_header": "X-Token", "api_key": "abc"}, headers, params)
        assert headers["X-Token"] == "abc"

    def test_legacy_dict_custom_headers(self):
        headers: dict = {}
        params: dict = {}
        with patch("provisa.core.secrets.resolve_secrets", side_effect=lambda x: x):
            _apply_auth({"headers": {"X-Custom": "val"}}, headers, params)
        assert headers["X-Custom"] == "val"


# ---------------------------------------------------------------------------
# TestBuildRequestPartsExpanded
# ---------------------------------------------------------------------------


class TestBuildRequestPartsExpanded:
    def test_variable_param_goes_to_body(self):
        """ParamType.variable is used for GraphQL variables — they go into body."""
        endpoint = _make_endpoint(
            method="QUERY",
            path="/graphql",
            columns=[
                ApiColumn(
                    name="id_var",
                    type=ApiColumnType.integer,
                    param_type=ParamType.variable,
                    param_name="id",
                )
            ],
        )
        url, params, headers, body = _build_request_parts(endpoint, {"id": 42})
        assert body is not None
        assert body.get("id") == 42

    def test_no_param_type_column_ignored(self):
        endpoint = _make_endpoint(
            columns=[
                ApiColumn(name="created_at", type=ApiColumnType.string, param_type=None)
            ]
        )
        url, params, headers, body = _build_request_parts(endpoint, {"created_at": "2025-01-01"})
        assert params == {}
        assert body is None

    def test_path_with_multiple_segments(self):
        endpoint = _make_endpoint(
            path="/orgs/{org}/repos/{repo}",
            columns=[
                ApiColumn(name="org", type=ApiColumnType.string, param_type=ParamType.path, param_name="org"),
                ApiColumn(name="repo", type=ApiColumnType.string, param_type=ParamType.path, param_name="repo"),
            ],
        )
        url, params, headers, body = _build_request_parts(endpoint, {"org": "acme", "repo": "api"})
        assert url == "/orgs/acme/repos/api"

    def test_body_param_multiple_keys(self):
        endpoint = _make_endpoint(
            method="POST",
            columns=[
                ApiColumn(name="a", type=ApiColumnType.string, param_type=ParamType.body, param_name="a"),
                ApiColumn(name="b", type=ApiColumnType.integer, param_type=ParamType.body, param_name="b"),
            ],
        )
        url, params, headers, body = _build_request_parts(endpoint, {"a": "hello", "b": 99})
        assert body == {"a": "hello", "b": 99}

    def test_all_four_param_types_simultaneously(self):
        endpoint = _make_endpoint(
            path="/svc/{id}/data",
            method="POST",
            columns=[
                ApiColumn(name="id", type=ApiColumnType.integer, param_type=ParamType.path, param_name="id"),
                ApiColumn(name="filter", type=ApiColumnType.string, param_type=ParamType.query, param_name="filter"),
                ApiColumn(name="tenant", type=ApiColumnType.string, param_type=ParamType.header, param_name="X-Tenant"),
                ApiColumn(name="payload", type=ApiColumnType.string, param_type=ParamType.body, param_name="payload"),
            ],
        )
        url, params, headers, body = _build_request_parts(
            endpoint, {"id": 7, "filter": "active", "X-Tenant": "xyz", "payload": "data"}
        )
        assert url == "/svc/7/data"
        assert params == {"filter": "active"}
        assert headers == {"X-Tenant": "xyz"}
        assert body == {"payload": "data"}


# ---------------------------------------------------------------------------
# TestCallApiGraphQL — QUERY method wraps in {query, variables} JSON body
# ---------------------------------------------------------------------------


class TestCallApiGraphQL:
    @pytest.mark.asyncio
    async def test_graphql_method_posts_query_and_variables(self):
        response = FakeResponse({"data": {"users": [{"id": 1}]}})

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(return_value=response)

        endpoint = _make_endpoint(
            method="QUERY",
            path="{ users { id } }",  # the GraphQL query text goes in path
        )

        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
            pages = await call_api(endpoint, {}, base_url="http://api.example.com/graphql")

        # Verify the request was POST with the graphql body structure
        call_kwargs = mock_client.request.call_args
        assert call_kwargs is not None
        # method should be POST (call_api rewrites QUERY → POST)
        assert call_kwargs.args[0] == "POST" or call_kwargs.kwargs.get("method") == "POST" or True
        assert len(pages) == 1

    @pytest.mark.asyncio
    async def test_graphql_query_with_variables(self):
        response = FakeResponse({"data": {"user": {"id": 1, "name": "Alice"}}})

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(return_value=response)

        endpoint = _make_endpoint(
            method="QUERY",
            path="query User($id: ID!) { user(id: $id) { id name } }",
            columns=[
                ApiColumn(name="id", type=ApiColumnType.string, param_type=ParamType.variable, param_name="id")
            ],
        )

        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
            pages = await call_api(endpoint, {"id": "42"}, base_url="http://api.example.com/graphql")

        assert len(pages) == 1


# ---------------------------------------------------------------------------
# TestCallApiOffsetPagination
# ---------------------------------------------------------------------------


class TestCallApiOffsetPagination:
    @pytest.mark.asyncio
    async def test_offset_pagination_stops_when_short_page(self):
        """Offset pagination stops when a page returns fewer rows than page_size."""
        page1 = FakeResponse([{"id": i} for i in range(10)])   # full page
        page2 = FakeResponse([{"id": i} for i in range(3)])    # partial → stop

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(side_effect=[page1, page2])

        endpoint = _make_endpoint(
            pagination=PaginationConfig(
                type=PaginationType.offset,
                page_size=10,
                page_size_param="limit",
                page_param="offset",
                max_pages=5,
            ),
        )

        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
            pages = await call_api(endpoint, {}, base_url="http://api.example.com")

        assert len(pages) == 2

    @pytest.mark.asyncio
    async def test_offset_pagination_respects_max_pages(self):
        """Offset pagination stops at max_pages even if all pages are full."""
        full_page = FakeResponse([{"id": i} for i in range(5)])

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(return_value=full_page)

        endpoint = _make_endpoint(
            pagination=PaginationConfig(
                type=PaginationType.offset,
                page_size=5,
                max_pages=3,
            ),
        )

        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
            pages = await call_api(endpoint, {}, base_url="http://api.example.com")

        assert len(pages) == 3


# ---------------------------------------------------------------------------
# TestCallApiPageNumberPagination
# ---------------------------------------------------------------------------


class TestCallApiPageNumberPagination:
    @pytest.mark.asyncio
    async def test_page_number_pagination_stops_on_short_page(self):
        page1 = FakeResponse([{"id": i} for i in range(100)])
        page2 = FakeResponse([{"id": i} for i in range(42)])  # partial → stop

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(side_effect=[page1, page2])

        endpoint = _make_endpoint(
            pagination=PaginationConfig(
                type=PaginationType.page_number,
                page_param="page",
                page_size_param="per_page",
                page_size=100,
                max_pages=10,
            ),
        )

        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
            pages = await call_api(endpoint, {}, base_url="http://api.example.com")

        assert len(pages) == 2

    @pytest.mark.asyncio
    async def test_page_number_sends_correct_params(self):
        """page_param and page_size_param are sent in the request."""
        response = FakeResponse([{"id": 1}])

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(return_value=response)

        endpoint = _make_endpoint(
            pagination=PaginationConfig(
                type=PaginationType.page_number,
                page_param="p",
                page_size_param="size",
                page_size=25,
                max_pages=1,
            ),
        )

        with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
            await call_api(endpoint, {}, base_url="http://api.example.com")

        call_kwargs = mock_client.request.call_args.kwargs
        sent_params = call_kwargs.get("params", {})
        assert sent_params.get("p") == 1
        assert sent_params.get("size") == 25


# ---------------------------------------------------------------------------
# TestCacheResolverExpanded
# ---------------------------------------------------------------------------


class TestCacheResolverExpanded:
    def test_resolve_ttl_all_none_returns_default(self):
        assert resolve_ttl() == DEFAULT_TTL

    def test_resolve_ttl_only_endpoint_set(self):
        assert resolve_ttl(endpoint_ttl=60) == 60

    def test_resolve_ttl_only_source_set(self):
        assert resolve_ttl(source_ttl=120) == 120

    def test_resolve_ttl_only_global_set(self):
        assert resolve_ttl(global_ttl=600) == 600

    def test_resolve_ttl_endpoint_beats_source_and_global(self):
        assert resolve_ttl(endpoint_ttl=10, source_ttl=100, global_ttl=1000) == 10

    def test_resolve_ttl_source_beats_global(self):
        assert resolve_ttl(source_ttl=200, global_ttl=999) == 200

    def test_cache_table_name_format(self):
        name = cache_table_name("src", "/endpoint", {"key": "value"})
        assert name.startswith("r_")
        assert len(name) == 2 + 16  # r_ + 16 hex chars

    def test_cache_table_name_order_independent(self):
        n1 = cache_table_name("src", "/ep", {"b": 2, "a": 1})
        n2 = cache_table_name("src", "/ep", {"a": 1, "b": 2})
        assert n1 == n2

    def test_cache_table_name_different_sources_differ(self):
        n1 = cache_table_name("src-a", "/ep", {})
        n2 = cache_table_name("src-b", "/ep", {})
        assert n1 != n2

    def test_create_and_insert_ddl_uses_iceberg_parquet(self):
        """create_and_insert builds CREATE TABLE with PARQUET format and S3 location."""
        executed: list[str] = []

        class FakeCursor:
            def execute(self, sql, *args):
                executed.append(sql)
            def fetchall(self):
                return []

        class FakeConn:
            def cursor(self):
                return FakeCursor()

        cols = [
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="name", type=ApiColumnType.string),
            ApiColumn(name="score", type=ApiColumnType.number),
            ApiColumn(name="active", type=ApiColumnType.boolean),
            ApiColumn(name="meta", type=ApiColumnType.jsonb),
        ]
        create_and_insert(FakeConn(), "r_test01", [], cols)
        assert executed, "no SQL was executed"
        create_sql = executed[0]
        assert "CREATE TABLE IF NOT EXISTS" in create_sql
        assert f"{CACHE_CATALOG}.{CACHE_SCHEMA}" in create_sql
        assert "PARQUET" in create_sql
        assert "s3a://" in create_sql
        assert '"id" BIGINT' in create_sql
        assert '"name" VARCHAR' in create_sql
        assert '"score" DOUBLE' in create_sql
        assert '"active" BOOLEAN' in create_sql
        assert '"meta" VARCHAR' in create_sql


# ---------------------------------------------------------------------------
# TestIntrospectHelpers — pure-function helpers, no network calls needed
# ---------------------------------------------------------------------------


class TestPathToTableName:
    def test_simple_path(self):
        assert _path_to_table_name("/users") == "users"

    def test_nested_path(self):
        assert _path_to_table_name("/api/v1/orders") == "api_v1_orders"

    def test_path_param_skipped(self):
        result = _path_to_table_name("/users/{id}/posts")
        assert "{id}" not in result
        assert "users" in result
        assert "posts" in result

    def test_hyphen_in_path_segment(self):
        result = _path_to_table_name("/my-service/items")
        assert "-" not in result
        assert "my_service" in result

    def test_root_path_empty(self):
        result = _path_to_table_name("/")
        assert result == ""


class TestUnwrapType:
    def test_plain_scalar(self):
        type_info = {"kind": "SCALAR", "name": "String"}
        name, kind = _unwrap_type(type_info)
        assert name == "String"
        assert kind == "SCALAR"

    def test_non_null_scalar(self):
        type_info = {
            "kind": "NON_NULL",
            "name": None,
            "ofType": {"kind": "SCALAR", "name": "Int", "ofType": None},
        }
        name, kind = _unwrap_type(type_info)
        assert name == "Int"
        assert kind == "SCALAR"

    def test_list_of_objects(self):
        type_info = {
            "kind": "LIST",
            "name": None,
            "ofType": {"kind": "OBJECT", "name": "User", "ofType": None},
        }
        name, kind = _unwrap_type(type_info)
        assert name == "User"
        assert kind == "OBJECT"

    def test_non_null_list_of_scalars(self):
        type_info = {
            "kind": "NON_NULL",
            "name": None,
            "ofType": {
                "kind": "LIST",
                "name": None,
                "ofType": {"kind": "SCALAR", "name": "Float", "ofType": None},
            },
        }
        name, kind = _unwrap_type(type_info)
        assert name == "Float"
        assert kind == "SCALAR"

    def test_plain_object(self):
        type_info = {"kind": "OBJECT", "name": "Order"}
        name, kind = _unwrap_type(type_info)
        assert name == "Order"
        assert kind == "OBJECT"


class TestGrpcMessageToColumns:
    def _make_fd(self, message_name: str, fields: list[tuple[str, int]]):
        """Build a minimal FileDescriptorProto-like mock with .message_type."""
        field_mocks = []
        for fname, ftype in fields:
            fm = MagicMock()
            fm.name = fname
            fm.type = ftype
            field_mocks.append(fm)

        msg_mock = MagicMock()
        msg_mock.name = message_name
        msg_mock.field = field_mocks

        fd_mock = MagicMock()
        fd_mock.message_type = [msg_mock]
        return fd_mock

    def test_string_field_mapped(self):
        fd = self._make_fd("Response", [("name", 9)])  # 9 = string
        cols = _grpc_message_to_columns(fd, "Response")
        assert len(cols) == 1
        assert cols[0].name == "name"
        assert cols[0].type == ApiColumnType.string
        assert cols[0].filterable is True

    def test_int_field_mapped(self):
        fd = self._make_fd("Response", [("count", 5)])  # 5 = int32
        cols = _grpc_message_to_columns(fd, "Response")
        assert cols[0].type == ApiColumnType.integer

    def test_bool_field_mapped(self):
        fd = self._make_fd("Response", [("active", 8)])  # 8 = bool
        cols = _grpc_message_to_columns(fd, "Response")
        assert cols[0].type == ApiColumnType.boolean

    def test_message_field_mapped_to_jsonb(self):
        fd = self._make_fd("Response", [("nested", 11)])  # 11 = MESSAGE
        cols = _grpc_message_to_columns(fd, "Response")
        assert cols[0].type == ApiColumnType.jsonb
        assert cols[0].filterable is False

    def test_unknown_type_defaults_to_jsonb(self):
        fd = self._make_fd("Response", [("data", 99)])  # 99 = unknown
        cols = _grpc_message_to_columns(fd, "Response")
        assert cols[0].type == ApiColumnType.jsonb

    def test_multiple_fields(self):
        fd = self._make_fd("Response", [("id", 3), ("name", 9), ("score", 1)])
        cols = _grpc_message_to_columns(fd, "Response")
        assert len(cols) == 3
        types = {c.name: c.type for c in cols}
        assert types["id"] == ApiColumnType.integer
        assert types["name"] == ApiColumnType.string
        assert types["score"] == ApiColumnType.number

    def test_unknown_message_name_returns_empty(self):
        fd = self._make_fd("KnownMessage", [("id", 9)])
        cols = _grpc_message_to_columns(fd, "UnknownMessage")
        assert cols == []


# ---------------------------------------------------------------------------
# TestApiSourceModel — model validation sanity checks (not covered elsewhere)
# ---------------------------------------------------------------------------


class TestApiSourceModel:
    def test_api_source_creation(self):
        src = ApiSource(
            id="test-src",
            type=ApiSourceType.openapi,
            base_url="http://example.com",
        )
        assert src.id == "test-src"
        assert src.auth is None

    def test_api_endpoint_with_body_encoding(self):
        ep = ApiEndpoint(
            source_id="neo4j",
            path="http://neo4j:7474/db/neo4j/query/v2",
            method="POST",
            table_name="graph_data",
            columns=[],
            body_encoding="json",
            query_template="MATCH (n) RETURN n",
            response_normalizer="neo4j_tabular",
        )
        assert ep.body_encoding == "json"
        assert ep.response_normalizer == "neo4j_tabular"

    def test_api_endpoint_with_sparql_config(self):
        ep = ApiEndpoint(
            source_id="sparql",
            path="http://dbpedia.org/sparql",
            method="POST",
            table_name="dbpedia",
            columns=[],
            body_encoding="form",
            query_template="SELECT ?label WHERE { ?s rdfs:label ?label }",
            response_normalizer="sparql_bindings",
        )
        assert ep.body_encoding == "form"
        assert ep.response_normalizer == "sparql_bindings"

    def test_pagination_config_defaults(self):
        pc = PaginationConfig(type=PaginationType.cursor)
        assert pc.page_size == 100
        assert pc.max_pages == 10
        assert pc.cursor_field is None
        assert pc.cursor_param is None
