# Copyright (c) 2026 Kenneth Stott
# Canary: 21a30b6e-ae29-4c43-85db-800e6c38f2f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-318: a live read of an API table calls the remote with the endpoint's default parameters
(the values that make its whole collection, taken from the spec at registration) under whatever
the statement binds. Every other whole-collection read (replica build, source loader) already
does; the query path sent none, and a remote that requires the parameter answered 400."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx

from provisa.api_source.engine_cache import CacheLocation
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint, ApiSource

BASE = "http://pets.test/api/v3"


class _Engine:
    @contextmanager
    def isolated_sync(self):
        yield MagicMock()


def _endpoint() -> ApiEndpoint:
    return ApiEndpoint(
        source_id="petstore",
        table_name="find_pets_by_status",
        path="/pet/findByStatus",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="status", type=ApiColumnType.string, param_type="query"),
        ],
        default_params={"status": ["available", "pending", "sold"]},
    )


async def _read(params: dict) -> httpx.Request:
    from provisa.api_source.router_integration import handle_api_query

    route = respx.get(f"{BASE}/pet/findByStatus").mock(
        return_value=httpx.Response(200, json=[{"id": 1, "status": "sold"}])
    )
    with (
        patch("provisa.api_source.router_integration.table_exists", return_value=False),
    ):
        result = await handle_api_query(
            _endpoint(),
            params,
            _Engine(),
            source=ApiSource(id="petstore", type="openapi", base_url=BASE),
            loc=CacheLocation("mat_store", "org_default_api_cache", "relational"),
        )
    assert result.rows == [{"id": 1, "status": "sold"}]
    return route.calls.last.request


@respx.mock
@pytest.mark.asyncio
async def test_a_read_that_binds_nothing_sends_the_endpoints_default_parameters():
    request = await _read({})
    assert request.url.params.get_list("status") == ["available", "pending", "sold"]


@respx.mock
@pytest.mark.asyncio
async def test_a_parameter_the_statement_binds_replaces_its_default():
    request = await _read({"status": "sold"})
    assert request.url.params.get_list("status") == ["sold"]
