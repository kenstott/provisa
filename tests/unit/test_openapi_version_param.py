# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A brand's version header read from the spec's own header parameter (REQ-1874).

Notion declares a ``Notion-Version`` header parameter with its own default; Cal.com declares
``cal-api-version`` per operation, with different values on different operations. Such a brand
sends, with each operation's calls, the value the spec declares for that operation, never the
spec's info.version.
"""

import httpx
import pytest
import respx

from provisa.api.admin import openapi_router
from provisa.api.admin.openapi_router import OpenAPIRegisterRequest
from provisa.api.errors import ApiError
from provisa.api_source.caller import prepare_call
from provisa.api_source.models import ApiEndpoint, ApiSource, ApiSourceType
from provisa.openapi import brands
from provisa.openapi.brands import BRANDS, Brand, VersionUnset, declared_versions

API = "https://api.vendor.test"


def _get(*params: dict, operation_id: str | None = None, servers: list | None = None) -> dict:
    op: dict = {"parameters": list(params), "responses": {"200": {"description": "ok"}}}
    if operation_id:
        op["operationId"] = operation_id
    if servers:
        op["servers"] = servers
    return op


def _spec(paths: dict, **components) -> dict:
    return {
        "openapi": "3.0.0",
        "info": {"title": "Vendor", "version": "1.0.0"},
        "servers": [{"url": API}],
        "paths": paths,
        "components": components,
    }


# Notion's shape: one header parameter in components, referenced by every operation.
NOTION_LIKE = _spec(
    {
        "/v1/users/me": {"get": _get({"$ref": "#/components/parameters/notionVersion"})},
        "/v1/users": {"get": _get({"$ref": "#/components/parameters/notionVersion"})},
    },
    parameters={
        "notionVersion": {
            "name": "Notion-Version",
            "in": "header",
            "required": True,
            "schema": {"type": "string", "default": "2022-06-28"},
        }
    },
)

# Cal.com's shape: the header declared on each operation, with different values.
CALCOM_LIKE = _spec(
    {
        "/v2/bookings": {
            "get": _get(
                {
                    "name": "cal-api-version",
                    "in": "header",
                    "required": True,
                    "schema": {"type": "string", "default": "2024-08-13"},
                }
            ),
            "post": _get(
                {
                    "name": "cal-api-version",
                    "in": "header",
                    "required": True,
                    "schema": {"type": "string", "enum": ["2024-08-13"]},
                }
            ),
        },
        "/v2/schedules": {
            "get": _get(
                {
                    "name": "cal-api-version",
                    "in": "header",
                    "required": True,
                    "schema": {"type": "string", "enum": ["2024-06-11"]},
                }
            )
        },
        "/v2/me": {"get": _get()},  # declares no version
    }
)


def _brand(spec: dict, monkeypatch, **over) -> Brand:
    monkeypatch.setattr(brands, "brand_spec", lambda _id: spec)
    return Brand(
        **{
            "id": "vendor",
            "label": "Vendor",
            "spec_url": "",
            "verify_path": "/v1/users/me",
            "version_header": "Notion-Version",
            "version_from": "parameter",
            **over,
        }
    )


# --- the value each operation declares ---


def test_a_header_parameter_shared_from_components_sends_its_default(monkeypatch):
    brand = _brand(NOTION_LIKE, monkeypatch)
    assert brand.headers() == {}  # nothing on every call: never info.version
    assert brand.call_headers("GET", "/v1/users/me") == {"Notion-Version": "2022-06-28"}
    assert brand.call_headers("GET", "/v1/users") == {"Notion-Version": "2022-06-28"}


def test_a_header_parameter_on_the_path_is_its_operations(monkeypatch):
    spec = _spec(
        {
            "/v1/pages": {
                "parameters": [
                    {"name": "Notion-Version", "in": "header", "schema": {"default": "2025-09-03"}}
                ],
                "get": _get(),
            }
        }
    )
    assert declared_versions(spec, "Notion-Version") == {
        "GET /v1/pages": {"Notion-Version": "2025-09-03"}
    }


def test_operations_that_declare_different_values_each_send_their_own(monkeypatch):
    brand = _brand(CALCOM_LIKE, monkeypatch, version_header="cal-api-version")
    assert brand.call_headers("GET", "/v2/bookings") == {"cal-api-version": "2024-08-13"}
    assert brand.call_headers("GET", "/v2/schedules") == {"cal-api-version": "2024-06-11"}
    assert brand.call_headers("post", "/v2/bookings") == {"cal-api-version": "2024-08-13"}
    # An operation that declares no version is sent none, not info.version.
    assert brand.call_headers("GET", "/v2/me") == {}


def test_an_enum_of_one_value_is_that_value():
    versions = declared_versions(CALCOM_LIKE, "cal-api-version")
    assert versions["GET /v2/schedules"] == {"cal-api-version": "2024-06-11"}
    assert versions["POST /v2/bookings"] == {"cal-api-version": "2024-08-13"}


def test_the_header_name_matches_whatever_its_case():
    versions = declared_versions(NOTION_LIKE, "notion-version")
    assert versions["GET /v1/users"] == {"notion-version": "2022-06-28"}


def test_an_operation_with_its_own_server_is_named_by_its_absolute_path_too():
    spec = _spec(
        {
            "/v1/files": {
                "get": _get(
                    {"name": "X-V", "in": "header", "schema": {"default": "7"}},
                    servers=[{"url": "https://files.vendor.test/"}],
                )
            }
        }
    )
    versions = declared_versions(spec, "X-V")
    assert versions["GET /v1/files"] == versions["GET https://files.vendor.test/v1/files"]


# --- the refusal ---

UNSET = _spec(
    {
        "/v2/bookings": {
            "get": _get(
                {
                    "name": "cal-api-version",
                    "in": "header",
                    "schema": {"type": "string", "enum": ["2024-08-13", "2024-04-15"]},
                },
                operation_id="BookingsController_getBookings",
            )
        },
        "/v2/slots": {"get": _get({"name": "cal-api-version", "in": "header", "schema": {}})},
    }
)


def test_a_declared_header_with_no_value_to_send_is_refused_naming_the_operation():
    with pytest.raises(VersionUnset) as unset:
        declared_versions(UNSET, "cal-api-version")
    assert unset.value.operation == "BookingsController_getBookings"
    assert "BookingsController_getBookings" in str(unset.value)
    without_id = _spec({"/v2/slots": UNSET["paths"]["/v2/slots"]})
    with pytest.raises(VersionUnset, match="GET /v2/slots"):
        declared_versions(without_id, "cal-api-version")


async def test_registration_refuses_such_a_brand_by_name_before_any_call(monkeypatch):
    brand = _brand(UNSET, monkeypatch, version_header="cal-api-version")
    monkeypatch.setitem(BRANDS, "vendor", brand)
    body = OpenAPIRegisterRequest(source_id="v", brand="vendor", token="k")
    with respx.mock:  # any call would fail as unmocked
        with pytest.raises(ApiError) as refused:
            await openapi_router._branded(body)
    assert refused.value.code == "openapi.version_unset"
    assert refused.value.params["operation"] == "BookingsController_getBookings"
    assert "BookingsController_getBookings" in str(refused.value)


@pytest.fixture
def _one_organisation(monkeypatch):
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


@respx.mock
@pytest.mark.usefixtures("_one_organisation")
async def test_registration_checks_the_key_with_its_operations_version(monkeypatch):
    brand = _brand(NOTION_LIKE, monkeypatch)
    monkeypatch.setitem(BRANDS, "vendor", brand)
    check = respx.get(f"{API}/v1/users/me").mock(return_value=httpx.Response(200, json={}))
    await openapi_router._branded(OpenAPIRegisterRequest(source_id="v", brand="vendor", token="k"))
    assert check.calls.last.request.headers["notion-version"] == "2022-06-28"


# --- a brand without a parameter version is unchanged ---


def test_a_brand_without_a_version_header_sends_none(monkeypatch):
    brand = _brand(NOTION_LIKE, monkeypatch, version_header=None, version_from="info")
    assert brand.headers() == {}
    assert brand.operation_headers() == {}
    assert brand.call_headers("GET", "/v1/users") == {}


def test_a_brand_versioned_by_info_sends_info_version_with_every_call(monkeypatch):
    brand = _brand(NOTION_LIKE, monkeypatch, version_header="X-Version", version_from="info")
    assert brand.operation_headers() == {}
    assert brand.call_headers("GET", "/v1/users") == {"X-Version": "1.0.0"}
    assert BRANDS["stripe"].operation_headers() == {}


def test_a_parameter_version_needs_its_header():
    with pytest.raises(ValueError, match="needs its header"):
        Brand(
            id="x",
            label="X",
            spec_url="",
            verify_path="/",
            version_from="parameter",
        )


# --- the call ---


def test_each_call_carries_its_operations_version(monkeypatch):
    brand = _brand(CALCOM_LIKE, monkeypatch, version_header="cal-api-version")
    source = ApiSource(
        id="cal",
        type=ApiSourceType.openapi,
        base_url=API,
        headers={"X-Every": "1"},
        operation_headers=brand.operation_headers(),
    )

    def sent(path: str) -> dict:
        endpoint = ApiEndpoint(source_id="cal", path=path, table_name="t", columns=[])
        call = prepare_call(
            endpoint, {}, API, None, source.headers_for(endpoint.method, endpoint.path)
        )
        return call.headers

    assert sent("/v2/bookings") == {"X-Every": "1", "cal-api-version": "2024-08-13"}
    assert sent("/v2/schedules") == {"X-Every": "1", "cal-api-version": "2024-06-11"}
    assert sent("/v2/me") == {"X-Every": "1"}


async def test_a_loaded_branded_source_carries_each_operations_version(monkeypatch):
    from provisa.api_source.loader import load_api_sources

    brand = _brand(CALCOM_LIKE, monkeypatch, id="vendor", version_header="cal-api-version")
    monkeypatch.setitem(BRANDS, "vendor", brand)

    class _LoadConn:
        async def fetch(self, sql: str, *args) -> list[dict]:
            if "FROM api_sources" in sql:
                return [
                    {
                        "id": "cal",
                        "type": "openapi",
                        "base_url": API,
                        "spec_url": None,
                        "auth": None,
                        "spec_path": "brand:vendor",
                        "source_mapping": None,
                        "source_username": None,
                        "source_password": None,
                    }
                ]
            return []

    _endpoints, sources = await load_api_sources(_LoadConn(), {})  # type: ignore[arg-type]
    assert sources["cal"].headers == {}
    assert sources["cal"].headers_for("GET", "/v2/schedules") == {"cal-api-version": "2024-06-11"}
