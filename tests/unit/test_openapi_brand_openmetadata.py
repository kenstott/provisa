# Copyright (c) 2026 Kenneth Stott
# Canary: 6e3a9c71-2d48-4f05-b917-c4a0e8d25f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""OpenMetadata as a branded source carried by the OpenAPI source (REQ-2265).

A governance catalog read live: its base URL comes from setup, its spec from the running
instance, its credential is a bot's JWT token. The spec is a recording of an instance's
/swagger.json (OpenMetadata 2.0.9), trimmed to the operations read here
(tests/fixtures/catalog_brands/trim_openmetadata_spec.py). The remote is mocked; its answers are
written in the shape that spec declares.
"""

# Requirements: REQ-2265, REQ-1923

import gzip
import json
from pathlib import Path

import httpx
import pytest
import respx

from provisa.api.admin import openapi_router
from provisa.api.admin.openapi_router import OpenAPIRegisterRequest
from provisa.api.errors import ApiError
from provisa.api_source.caller import iter_api_pages
from provisa.api_source.models import ApiEndpoint
from provisa.api_source.openapi_endpoint import api_auth, endpoint_columns, openapi_operation
from provisa.core.auth_models import ApiAuthBearer
from provisa.openapi.brands import BRANDS
from provisa.openapi.loader import load_spec
from provisa.openapi.mapper import parse_spec

pytestmark = pytest.mark.unit

OM = BRANDS["openmetadata"]
HOST = "https://openmetadata.acme.test"
API = f"{HOST}/api"
SPEC = json.loads(
    gzip.open(
        Path(__file__).parents[1] / "fixtures/catalog_brands/openmetadata_swagger.json.gz"
    ).read()
)

# The catalog's tables a steward registers for the drift checks (docs/sources/catalog-brands.md).
CATALOG_TABLES = {
    "listTables": "/v1/tables",
    "listGlossaryTerm": "/v1/glossaryTerms",
    "listDomains": "/v1/domains",
    "listDataProducts": "/v1/dataProducts",
    "listClassifications": "/v1/classifications",
    "listTags": "/v1/tags",
    "listUsers": "/v1/users",
    "listTeams": "/v1/teams",
    "listTestCases": "/v1/dataQuality/testCases",
    "listTestCaseResultsFromSearchService": "/v1/dataQuality/testCases/testCaseResults/search/list",
}


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A call resolves its secret references, which asks whether the deployment holds many
    organisations; a deployment of one, as provisa.api.app declares it for a single-tenant
    state."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


@pytest.fixture(scope="module")
def offered():
    return {t.operation_id: t for t in parse_spec(SPEC)[0]}


# --- the brand ---


def test_the_api_is_under_api_and_the_spec_at_the_hosts_root():
    setup = OM.setup({"base_url": f" {HOST} "})
    assert (OM.base_url(setup), OM.source_spec_path(setup)) == (API, f"{HOST}/swagger.json")
    assert (OM.ships_spec, OM.headers(), OM.auth_type) == (False, {}, "bearer")


@pytest.mark.parametrize("given", ["openmetadata.acme.test", f"{HOST}/", f"{HOST}/api/", ""])
def test_a_base_url_that_is_not_one_http_address_without_a_trailing_slash_is_refused(given):
    from provisa.graphql_remote.brands import SetupInvalid

    with pytest.raises(SetupInvalid) as refused:
        OM.setup({"base_url": given})
    assert refused.value.field == "base_url"


# --- adding the source ---


def _body(**over) -> OpenAPIRegisterRequest:
    return OpenAPIRegisterRequest(
        **{
            "source_id": "om",
            "brand": "openmetadata",
            "setup": {"base_url": HOST},
            "token": "eyJbot",
            **over,
        }
    )


@respx.mock
async def test_adding_it_checks_the_bot_token_and_takes_the_instance_spec_and_address():
    check = respx.get(f"{API}/v1/users/loggedInUser").mock(
        return_value=httpx.Response(200, json={"name": "ingestion-bot", "isBot": True})
    )
    body = await openapi_router._branded(_body())
    assert check.calls.last.request.headers["authorization"] == "Bearer eyJbot"
    assert (body.spec_path, body.base_url, body.spec_content) == (f"{HOST}/swagger.json", API, "")
    assert (body.setup, body.auth_config) == (
        {"base_url": HOST},
        {"type": "bearer", "token": "eyJbot"},
    )


@respx.mock
async def test_a_token_openmetadata_rejects_is_refused_by_name():
    respx.get(f"{API}/v1/users/loggedInUser").mock(return_value=httpx.Response(401, json={}))
    with pytest.raises(ApiError) as refused:
        await openapi_router._branded(_body())
    assert refused.value.code == "openapi.credential_rejected"


async def test_no_token_or_no_base_url_is_refused_by_field_before_any_call():
    with pytest.raises(ApiError) as no_token:
        await openapi_router._branded(_body(token=""))
    with pytest.raises(ApiError) as no_host:
        await openapi_router._branded(_body(setup={}))
    assert [(e.value.code, e.value.params["field"]) for e in (no_token, no_host)] == [
        ("openapi.credential_required", "token"),
        ("openapi.setup_invalid", "base_url"),
    ]


@respx.mock
def test_the_spec_is_read_from_the_running_instance():
    respx.get(f"{HOST}/swagger.json").mock(return_value=httpx.Response(200, json=SPEC))
    assert load_spec(OM.source_spec_path({"base_url": HOST}))["info"]["title"] == (
        "OpenMetadata APIs"
    )


# --- what it offers ---


def test_the_catalog_tables_are_offered_as_paged_lists_of_rows(offered):
    for operation_id, path in CATALOG_TABLES.items():
        table = offered[operation_id]
        assert (table.path, table.is_list, table.rows_field) == (path, True, "data"), operation_id
        assert table.pagination is not None, operation_id


def test_a_catalog_table_carries_its_columns_tags_owners_domains_and_certification(offered):
    columns = {c["name"]: c["type"] for c in endpoint_columns(offered["listTables"])}
    for name in ("fullyQualifiedName", "columns", "tags", "owners", "domains", "certification"):
        assert name in columns, name
    # what a read asks OpenMetadata to include is a parameter of the table (REQ-2061)
    assert "fields" in columns


# --- reading a table ---

PAGE_1 = {
    "data": [
        {
            "id": "5f0c3a52-0d5e-4f53-9a43-1c2b0e6d8a11",
            "name": "customers",
            "fullyQualifiedName": "warehouse.shop.public.customers",
            "columns": [
                {
                    "name": "email",
                    "dataType": "VARCHAR",
                    "fullyQualifiedName": "warehouse.shop.public.customers.email",
                    "tags": [
                        {
                            "tagFQN": "PII.Sensitive",
                            "source": "Classification",
                            "labelType": "Manual",
                            "state": "Confirmed",
                        }
                    ],
                },
                {"name": "id", "dataType": "BIGINT", "tags": []},
            ],
            "owners": [{"id": "a1", "type": "user", "name": "ana", "displayName": "Ana Ruiz"}],
            "tags": [{"tagFQN": "Tier.Tier1", "source": "Classification"}],
            "domains": [{"id": "d1", "type": "domain", "name": "Sales"}],
            "certification": {"tagLabel": {"tagFQN": "Certification.Gold"}},
        }
    ],
    "paging": {"after": "Y3Vyc29yLTE=", "total": 2},
}
PAGE_2 = {
    "data": [
        {
            "id": "7a1d9e04-3b2c-4d8f-8e6a-2f4b1c0d9e22",
            "name": "orders",
            "fullyQualifiedName": "warehouse.shop.public.orders",
            "columns": [{"name": "total", "dataType": "DECIMAL", "tags": []}],
            "owners": [],
            "tags": [],
            "domains": [],
        }
    ],
    "paging": {"total": 2},
}
FIELDS = "columns,tags,owners,domains,certification"


@respx.mock
async def test_the_catalogs_tables_are_read_to_the_end_page_after_page(offered):
    paging = offered["listTables"].pagination.model_copy(update={"page_size": 1})
    match = openapi_operation(SPEC, "om", "listTables", paging)
    endpoint = ApiEndpoint(
        source_id="om",
        path=match.path,
        table_name="listTables",
        columns=endpoint_columns(match),
        pagination=paging,
        response_root=match.rows_field,
        default_params={"fields": FIELDS},  # the table's fixed argument (REQ-2061)
    )
    route = respx.get(f"{API}/v1/tables").mock(
        side_effect=[httpx.Response(200, json=PAGE_1), httpx.Response(200, json=PAGE_2)]
    )
    auth = ApiAuthBearer(**api_auth(OM.auth("eyJbot")))  # as the stored auth is read back
    pages = [
        p
        async for p in iter_api_pages(
            endpoint, dict(endpoint.default_params), base_url=API, auth=auth, source_headers={}
        )
    ]
    rows = [r for p in pages for r in p["data"]]
    assert [r["fullyQualifiedName"] for r in rows] == [
        "warehouse.shop.public.customers",
        "warehouse.shop.public.orders",
    ]
    assert [c.request.url.params.get("after") for c in route.calls] == [None, "Y3Vyc29yLTE="]
    assert all(c.request.url.params["fields"] == FIELDS for c in route.calls)
    assert route.calls.last.request.headers["authorization"] == "Bearer eyJbot"
