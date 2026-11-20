# Copyright (c) 2026 Kenneth Stott
# Canary: b333e7fb-cbdd-49ff-b4c7-3c8b1cacb7ed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DataHub as a branded source carried by the OpenAPI source (REQ-2265).

A governance catalog read live through its OpenAPI v3 spec: its base URL (the GMS) comes from
setup, its spec from the running GMS, its credential is a personal access token. The spec is a
recording of the test stack's GMS (docker-compose.test.yml, datahub-gms), trimmed to the entity
lists read here (tests/fixtures/catalog_brands/trim_datahub_spec.py); the integration test
tests/integration/test_catalog_brands_live.py checks the recording against that GMS. The remote
is mocked; its answers are written in the shape that spec declares.
"""

# Requirements: REQ-2265, REQ-1923

import gzip
import json

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
from tests.fixtures.catalog_brands.trim_datahub_spec import OUT, entity_paths

pytestmark = pytest.mark.unit

DH = BRANDS["datahub"]
GMS = "https://datahub.acme.test"
SPEC = json.loads(gzip.open(OUT).read())
PATHS = entity_paths(SPEC)
DATASETS = PATHS["dataset"]
# where the recorded spec says its calls go: its server, read against where it was read from
API = DH.base_url(DH.setup({"base_url": GMS}), SPEC)
ASPECTS = ["schemaMetadata", "globalTags", "ownership", "glossaryTerms", "domains"]


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A call resolves its secret references, which asks whether the deployment holds many
    organisations; a deployment of one, as provisa.api.app declares it for a single-tenant
    state."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


@pytest.fixture(scope="module")
def offered():
    return {t.path: t for t in parse_spec(SPEC)[0]}


# --- the brand ---


def test_its_spec_is_the_gms_v3_group_and_its_calls_go_where_that_spec_says():
    setup = DH.setup({"base_url": f" {GMS} "})
    assert DH.source_spec_path(setup) == f"{GMS}/openapi/v3/api-docs/10-openapi-v3"
    assert (DH.ships_spec, DH.address_from_spec, DH.headers(), DH.auth_type) == (
        False,
        True,
        {},
        "bearer",
    )
    for entity, path in PATHS.items():
        assert API + path == f"{GMS}/openapi/v3/entity/{entity}"
    assert DH.verify_url(setup, API) == f"{GMS}/openapi/v3/entity/corpuser?count=1"


@pytest.mark.parametrize(
    ("server", "path"),
    [("/openapi/", "/v3/entity/dataset"), ("/", "/openapi/v3/entity/dataset")],
)
def test_either_releases_layout_is_called_at_the_same_address(server, path):
    spec = {"servers": [{"url": server}], "paths": {}}
    assert DH.base_url({"base_url": GMS}, spec) + path == f"{GMS}/openapi/v3/entity/dataset"


@pytest.mark.parametrize("given", ["datahub.acme.test", f"{GMS}/", ""])
def test_a_base_url_that_is_not_one_http_address_without_a_trailing_slash_is_refused(given):
    from provisa.graphql_remote.brands import SetupInvalid

    with pytest.raises(SetupInvalid) as refused:
        DH.setup({"base_url": given})
    assert refused.value.field == "base_url"


# --- adding the source ---


def _body(**over) -> OpenAPIRegisterRequest:
    return OpenAPIRegisterRequest(
        **{
            "source_id": "dh",
            "brand": "datahub",
            "setup": {"base_url": GMS},
            "token": "eyJpat",
            **over,
        }
    )


@respx.mock
async def test_adding_it_checks_the_token_and_takes_the_instance_spec_and_address():
    respx.get(f"{GMS}/openapi/v3/api-docs/10-openapi-v3").mock(
        return_value=httpx.Response(200, json=SPEC)
    )
    check = respx.get(f"{GMS}/openapi/v3/entity/corpuser", params={"count": "1"}).mock(
        return_value=httpx.Response(200, json={"entities": []})
    )
    body = await openapi_router._branded(_body())
    assert check.calls.last.request.headers["authorization"] == "Bearer eyJpat"
    assert (body.spec_path, body.base_url, body.spec_content) == (
        f"{GMS}/openapi/v3/api-docs/10-openapi-v3",
        API,
        "",
    )
    assert (body.setup, body.auth_config) == (
        {"base_url": GMS},
        {"type": "bearer", "token": "eyJpat"},
    )


@respx.mock
async def test_a_token_datahub_rejects_is_refused_by_name():
    respx.get(f"{GMS}/openapi/v3/api-docs/10-openapi-v3").mock(
        return_value=httpx.Response(200, json=SPEC)
    )
    respx.get(f"{GMS}/openapi/v3/entity/corpuser").mock(return_value=httpx.Response(401))
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
def test_the_spec_is_read_from_the_running_gms():
    respx.get(f"{GMS}/openapi/v3/api-docs/10-openapi-v3").mock(
        return_value=httpx.Response(200, json=SPEC)
    )
    assert load_spec(DH.source_spec_path({"base_url": GMS}))["paths"].keys() >= set(PATHS.values())


# --- what it offers ---


def test_each_entity_is_offered_as_a_scroll_paged_list_of_rows(offered):
    for path in PATHS.values():
        table = offered[path]
        assert (table.is_list, table.rows_field) == (True, "entities"), path
        assert table.pagination is not None, path
        assert table.pagination.model_dump(mode="json", exclude_unset=True) == {
            "rows_field": "entities",
            "type": "cursor",
            "cursor_param": "scrollId",
            "cursor_field": "scrollId",
            "page_size_param": "count",
        }, path


def test_a_dataset_carries_the_aspects_the_drift_checks_read(offered):
    columns = {c["name"] for c in endpoint_columns(offered[DATASETS])}
    assert {"urn", *ASPECTS} <= columns
    # which aspects a read asks DataHub for is a parameter of the table (REQ-2061)
    assert "aspects" in columns


# --- reading a table ---


def _dataset(name: str, pii_field: str | None) -> dict:
    fields: list[dict] = [{"fieldPath": "id", "nativeDataType": "BIGINT"}]
    if pii_field is not None:
        fields.append(
            {
                "fieldPath": pii_field,
                "nativeDataType": "VARCHAR",
                "globalTags": {"tags": [{"tag": "urn:li:tag:PII"}]},
            }
        )
    return {
        "urn": f"urn:li:dataset:(urn:li:dataPlatform:postgres,shop.public.{name},PROD)",
        "schemaMetadata": {"value": {"schemaName": name, "fields": fields}},
        "ownership": {"value": {"owners": [{"owner": "urn:li:corpuser:ana", "type": "DATAOWNER"}]}},
        "domains": {"value": {"domains": ["urn:li:domain:sales"]}},
    }


PAGE_1 = {"scrollId": "c2Nyb2xsLTE=", "entities": [_dataset("customers", "email")]}
PAGE_2 = {"entities": [_dataset("orders", None)]}


@respx.mock
async def test_the_catalogs_datasets_are_read_to_the_end_scroll_after_scroll(offered):
    paging = offered[DATASETS].pagination.model_copy(update={"page_size": 1})
    match = openapi_operation(SPEC, "dh", offered[DATASETS].operation_id, paging)
    endpoint = ApiEndpoint(
        source_id="dh",
        path=match.path,
        table_name="datasets",
        columns=endpoint_columns(match),
        pagination=paging,
        response_root=match.rows_field,
        default_params={"aspects": ASPECTS},  # the table's fixed argument (REQ-2061)
    )
    route = respx.get(f"{GMS}/openapi/v3/entity/dataset").mock(
        side_effect=[httpx.Response(200, json=PAGE_1), httpx.Response(200, json=PAGE_2)]
    )
    auth = ApiAuthBearer(**api_auth(DH.auth("eyJpat")))  # as the stored auth is read back
    pages = [
        p
        async for p in iter_api_pages(
            endpoint, dict(endpoint.default_params), base_url=API, auth=auth, source_headers={}
        )
    ]
    rows = [r for p in pages for r in p["entities"]]
    assert [r["schemaMetadata"]["value"]["schemaName"] for r in rows] == ["customers", "orders"]
    assert [c.request.url.params.get("scrollId") for c in route.calls] == [None, "c2Nyb2xsLTE="]
    assert all(c.request.url.params["count"] == "1" for c in route.calls)
    assert all(c.request.url.params.get_list("aspects") == ASPECTS for c in route.calls)
    assert route.calls.last.request.headers["authorization"] == "Bearer eyJpat"
