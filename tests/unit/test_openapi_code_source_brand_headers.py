# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2e8a14-7b3f-4d96-a0e1-9f4b6d2c8e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every brand header reaches the built-in OpenAPI code source's calls (REQ-2267, REQ-1874).

An OpenAPI source is served as a built-in code source, so the headers a brand adds to its calls
-- Stripe's ``Stripe-Version`` from the shipped spec's info.version on every call, and a
``version_from="parameter"`` brand's version as each operation declares it -- are written with
the code its save writes, and sent with that code's reads and commands.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from provisa.codesource.builtin import _openapi_code
from provisa.openapi import brands
from provisa.openapi import code_source as oc
from provisa.openapi.brands import BRANDS, Brand, brand_spec

pytestmark = pytest.mark.unit

VENDOR = "https://api.vendor.test"


def _header_param(**schema) -> dict:
    return {
        "name": "cal-api-version",
        "in": "header",
        "required": True,
        "schema": {"type": "string", **schema},
    }


def _rows(**props) -> dict:
    return {
        "200": {
            "description": "ok",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {"type": "object", "properties": props},
                    }
                }
            },
        }
    }


# Cal.com's shape: the version header declared per operation, with different values.
VENDOR_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Vendor", "version": "9.9.9"},
    "servers": [{"url": VENDOR}],
    "paths": {
        "/v2/bookings": {
            "get": {
                "operationId": "listBookings",
                "parameters": [_header_param(default="2024-08-13")],
                "responses": _rows(id={"type": "integer"}, title={"type": "string"}),
            },
            "post": {
                "operationId": "createBooking",
                "parameters": [_header_param(enum=["2024-08-13"])],
                "requestBody": {
                    "required": True,
                    "content": {"application/json": {"schema": {"type": "object"}}},
                },
                "responses": {"201": {"description": "made"}},
            },
        },
        "/v2/schedules": {
            "get": {
                "operationId": "listSchedules",
                "parameters": [_header_param(enum=["2024-06-11"])],
                "responses": _rows(id={"type": "integer"}, name={"type": "string"}),
            }
        },
        "/v2/teams": {
            "get": {
                "operationId": "listTeams",  # declares no version
                "responses": _rows(id={"type": "integer"}),
            }
        },
    },
}


@pytest.fixture(autouse=True)
def ps(monkeypatch):
    """Provisa's services module, on the path as the host puts it (provisa/python/host.py)."""
    import importlib
    import sys

    from provisa.python.source import HOST

    monkeypatch.syspath_prepend(str(HOST.parent / "services"))
    yield importlib.import_module("provisa_services")
    sys.modules.pop("provisa_services", None)


@pytest.fixture
def vendor(monkeypatch):
    """A brand whose version is read from the spec's own header parameter, shipping VENDOR_SPEC."""
    real = brand_spec

    def spec(brand_id: str) -> dict:
        return VENDOR_SPEC if brand_id == "vendor" else real(brand_id)

    monkeypatch.setattr(brands, "brand_spec", spec)
    monkeypatch.setitem(
        BRANDS,
        "vendor",
        Brand(
            id="vendor",
            label="Vendor",
            spec_url="",
            verify_path="/v2/teams",
            version_header="cal-api-version",
            version_from="parameter",
        ),
    )


def _saved(tmp_path, source_id: str, path: str, mapping: dict | None = None) -> oc.OpenAPICode:
    """The source's code as its save writes it (provisa/codesource/builtin.py), loaded as the
    code's entry module loads it."""
    directory = tmp_path / "code"
    _openapi_code(directory, source_id, path, mapping or {})
    return oc.OpenAPICode.load(directory)


def _sent(route: respx.Route, header: str) -> list[str | None]:
    return [call.request.headers.get(header) for call in route.calls]


# -- a version_from="parameter" brand: each operation's own version ---------------------------------


@respx.mock
async def test_a_parameter_brands_reads_carry_the_version_each_operation_declares(tmp_path, vendor):
    code = _saved(tmp_path, "vendor-src", "brand:vendor")
    bookings = respx.get(f"{VENDOR}/v2/bookings").mock(
        return_value=httpx.Response(200, json=[{"id": 1, "title": "a"}])
    )
    schedules = respx.get(f"{VENDOR}/v2/schedules").mock(
        return_value=httpx.Response(200, json=[{"id": 2, "name": "b"}])
    )
    teams = respx.get(f"{VENDOR}/v2/teams").mock(return_value=httpx.Response(200, json=[]))

    assert await code.serve_table("listBookings", {}, _Context()) == [{"id": 1, "title": "a"}]
    assert await code.serve_table("listSchedules", {}, _Context()) == [{"id": 2, "name": "b"}]
    assert await code.serve_table("listTeams", {}, _Context()) == []

    assert _sent(bookings, "cal-api-version") == ["2024-08-13"]
    assert _sent(schedules, "cal-api-version") == ["2024-06-11"]
    assert _sent(teams, "cal-api-version") == [None]  # an operation that declares none


@respx.mock
async def test_a_parameter_brands_command_carries_the_version_its_operation_declares(
    tmp_path, vendor
):
    code = _saved(tmp_path, "vendor-src", "brand:vendor")
    made = respx.post(f"{VENDOR}/v2/bookings").mock(
        return_value=httpx.Response(201, json={"id": 7})
    )

    await code.serve_command("createBooking", {"body": {"title": "c"}})

    assert _sent(made, "cal-api-version") == ["2024-08-13"]
    assert json.loads(made.calls[0].request.content) == {"title": "c"}


def test_a_parameter_brand_never_sends_the_specs_info_version_with_every_call(tmp_path, vendor):
    code = _saved(tmp_path, "vendor-src", "brand:vendor")
    assert "cal-api-version" not in code.headers
    assert code.source().headers_for("GET", "/v2/teams") == {}


# -- Stripe: the shipped spec's version with every call -----------------------------------------------

STRIPE = "https://api.stripe.com"


@pytest.fixture(scope="module")
def stripe_version() -> str:
    return brand_spec("stripe")["info"]["version"]


@respx.mock
async def test_stripes_reads_and_commands_carry_its_shipped_specs_version(tmp_path, stripe_version):
    code = _saved(tmp_path, "stripe-src", "brand:stripe")
    listed = respx.get(f"{STRIPE}/v1/customers").mock(
        return_value=httpx.Response(
            200, json={"object": "list", "data": [{"id": "cus_1"}], "has_more": False}
        )
    )
    made = respx.post(f"{STRIPE}/v1/customers").mock(
        return_value=httpx.Response(200, json={"id": "cus_2"})
    )

    rows = await code.serve_table("GetCustomers", {}, _Context())
    await code.serve_command("PostCustomers", {"body": {"email": "a@b.test"}})

    assert [r["id"] for r in rows] == ["cus_1"]
    assert _sent(listed, "Stripe-Version") == [stripe_version]
    assert _sent(made, "Stripe-Version") == [stripe_version]


def test_the_mappings_own_headers_are_kept_beside_the_brands(tmp_path, stripe_version):
    code = _saved(tmp_path, "stripe-src", "brand:stripe", {"headers": {"X-Trace": "on"}})
    assert code.headers == {"X-Trace": "on", "Stripe-Version": stripe_version}


def test_a_spec_of_the_stewards_own_carries_no_brand_header(tmp_path):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps(VENDOR_SPEC))
    code = _saved(tmp_path, "own-src", str(spec))
    assert code.headers == {}
    assert code.operation_headers == {}


class _Context:
    """A read's context as the host gives it to a table handler: not a loader call."""

    keys = None
