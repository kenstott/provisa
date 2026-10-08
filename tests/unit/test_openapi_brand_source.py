# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1f8e36-7b24-4a90-9c63-0e2a4f7b1d85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Stripe as a branded source carried by the OpenAPI source (REQ-1923).

These run against the spec that ships with Provisa. The remote is mocked: what Stripe itself
answers is checked by reading its tables with a sandbox key, which no unit test can do.
"""

from collections import Counter

import httpx
import pytest
import respx

from provisa.api.admin import openapi_router
from provisa.api.admin.openapi_router import OpenAPIRegisterRequest
from provisa.api.errors import ApiError
from provisa.core.auth_models import ApiAuthBearer
from provisa.api_source.caller import iter_api_pages
from provisa.api_source.models import ApiEndpoint
from provisa.api_source.openapi_endpoint import api_auth, endpoint_columns, openapi_operation
from provisa.openapi.brands import BRANDS, brand_spec, spec_headers
from provisa.openapi.loader import load_spec
from provisa.openapi.mapper import parse_spec

STRIPE = BRANDS["stripe"]
API = "https://api.stripe.com"


@pytest.fixture(scope="module")
def offered():
    return parse_spec(brand_spec("stripe"))


# --- the brand ---

VERSION = brand_spec("stripe")["info"]["version"]


def test_every_call_names_the_version_of_the_shipped_spec():
    assert STRIPE.headers() == {"Stripe-Version": VERSION}
    assert spec_headers("brand:stripe") == {"Stripe-Version": VERSION}
    assert spec_headers("https://pets.test/openapi.json") == {}  # a spec of the steward's own


def test_a_branded_sources_row_names_the_shipped_spec():
    assert STRIPE.spec_path == "brand:stripe"
    assert load_spec("brand:stripe") is brand_spec("stripe")
    assert brand_spec("stripe")["servers"][0]["url"].rstrip("/") == API


def test_a_row_naming_a_brand_this_build_does_not_carry_is_an_error():
    with pytest.raises(FileNotFoundError, match="stripe-of-the-future"):
        load_spec("brand:stripe-of-the-future")


def test_the_shipped_spec_offers_stripes_tables_and_commands(offered):
    tables, commands = offered
    assert {"GetCustomers", "GetInvoices", "GetCharges", "GetBalance"} <= {
        t.operation_id for t in tables
    }
    assert {"PostCustomers", "DeleteCustomersCustomer"} <= {c.operation_id for c in commands}
    assert all((t.response_schema or {}).get("properties") for t in tables if t.is_list)


def test_every_paged_list_is_offered_with_how_it_pages(offered):
    tables, _ = offered
    paged = [t for t in tables if t.is_list and any(p["name"] == "limit" for p in t.query_params)]
    kinds = Counter(t.pagination.type.value for t in paged)
    assert set(kinds) == {"last_row", "cursor"} and kinds["last_row"] > 100
    assert all(t.pagination.rows_field == "data" for t in paged)
    assert all(t.pagination.page_size_param == "limit" for t in paged)  # a search's cursor too


def test_lists_of_several_kinds_of_object_are_tables_and_the_file_upload_is_a_command(offered):
    tables, commands = offered
    by_id = {t.operation_id: t for t in tables}
    for mixed in ("GetAccountsAccountExternalAccounts", "GetCustomersCustomerSources"):
        table = by_id[mixed]
        assert (table.is_list, table.rows_field) == (True, "data")
        assert table.pagination.type.value == "last_row"
        assert {"id", "object", "last4"} <= set(table.response_schema["properties"])
    upload = next(c for c in commands if c.operation_id == "PostFiles")
    assert (upload.multipart, upload.files) == (True, frozenset({"file"}))
    assert upload.server == "https://files.stripe.com/"  # its own address, not the API's


# --- adding the source ---


def _body(**over) -> OpenAPIRegisterRequest:
    return OpenAPIRegisterRequest(
        **{"source_id": "pay", "brand": "stripe", "token": "sk_test_1", **over}
    )


@respx.mock
async def test_adding_it_checks_the_key_and_takes_the_brands_spec_and_address():
    check = respx.get(f"{API}/v1/balance").mock(return_value=httpx.Response(200, json={}))
    body = await openapi_router._branded(_body())
    assert check.calls.last.request.headers["authorization"] == "Bearer sk_test_1"
    assert check.calls.last.request.headers["stripe-version"] == VERSION
    assert (body.spec_path, body.base_url.rstrip("/")) == ("brand:stripe", API)
    assert body.auth_config == {"type": "bearer", "token": "sk_test_1"}


@respx.mock
async def test_a_key_stripe_does_not_accept_is_refused_by_name():
    respx.get(f"{API}/v1/balance").mock(return_value=httpx.Response(401, json={}))
    with pytest.raises(ApiError) as refused:
        await openapi_router._branded(_body())
    assert refused.value.code == "openapi.credential_rejected"


async def test_no_key_and_an_unknown_brand_are_refused_before_any_call():
    with pytest.raises(ApiError) as no_key:
        await openapi_router._branded(_body(token=""))
    with pytest.raises(ApiError) as unknown:
        await openapi_router._branded(_body(brand="stripe-of-the-future"))
    assert (no_key.value.code, unknown.value.code) == (
        "openapi.credential_required",
        "openapi.unknown_brand",
    )


# --- reading a table ---


@respx.mock
async def test_a_stripe_list_is_read_to_its_end_each_page_after_the_last_rows_id(offered):
    proposed = next(t.pagination for t in offered[0] if t.operation_id == "GetCustomers")
    paging = proposed.model_copy(update={"page_size": 2})
    match = openapi_operation(brand_spec("stripe"), "pay", "GetCustomers", paging)
    endpoint = ApiEndpoint(
        source_id="pay",
        path=match.path,
        table_name="GetCustomers",
        columns=endpoint_columns(match),
        pagination=paging,
        response_root=match.rows_field,
    )
    route = respx.get(f"{API}/v1/customers").mock(
        side_effect=[
            httpx.Response(
                200, json={"data": [{"id": "cus_1"}, {"id": "cus_2"}], "has_more": True}
            ),
            httpx.Response(200, json={"data": [{"id": "cus_3"}], "has_more": False}),
        ]
    )
    auth = ApiAuthBearer(**api_auth(STRIPE.auth("sk")))  # as the stored auth is read back
    pages = [
        p
        async for p in iter_api_pages(
            endpoint, {}, base_url=API, auth=auth, source_headers=STRIPE.headers()
        )
    ]
    assert [r["id"] for p in pages for r in p["data"]] == ["cus_1", "cus_2", "cus_3"]
    assert [dict(c.request.url.params) for c in route.calls] == [
        {"limit": "2"},
        {"limit": "2", "starting_after": "cus_2"},
    ]
    assert route.calls.last.request.headers["authorization"] == "Bearer sk"
    assert [c.request.headers["stripe-version"] for c in route.calls] == [VERSION, VERSION]


# --- fields Stripe answers that its published spec does not declare ------------------------------


def test_fields_stripe_answers_beyond_its_published_spec_are_columns(offered):
    from provisa.api_source.openapi_endpoint import endpoint_columns

    tables = {t.operation_id: t for t in offered[0]}
    columns = {
        op: {c["name"]: c["type"] for c in endpoint_columns(tables[op])}
        for op in ("GetCharges", "GetPaymentIntents", "GetProducts", "GetSubscriptions")
    }
    assert {"destination", "dispute", "order", "source"} <= set(columns["GetCharges"])
    assert {"shared_payment_granted_token", "source"} <= set(columns["GetPaymentIntents"])
    assert {"attributes", "type"} <= set(columns["GetProducts"])
    assert (columns["GetSubscriptions"]["quantity"], "plan" in columns["GetSubscriptions"]) == (
        "integer",
        True,
    )


def test_an_addition_the_published_spec_declares_or_has_no_schema_for_fails_the_load():
    from provisa.openapi.brands import _add_fields

    spec = {"components": {"schemas": {"charge": {"properties": {"id": {"type": "string"}}}}}}
    _add_fields("stripe", spec, {"_about": "a note", "charge": {"order": {"type": "string"}}})
    assert set(spec["components"]["schemas"]["charge"]["properties"]) == {"id", "order"}
    with pytest.raises(ValueError, match="now declares charge.order"):
        _add_fields("stripe", spec, {"charge": {"order": {"type": "string"}}})
    with pytest.raises(ValueError, match="does not have: refund"):
        _add_fields("stripe", spec, {"refund": {"x": {"type": "string"}}})
