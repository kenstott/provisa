# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2e4a17-5f39-4d0b-a6e1-3b7d9f02c5a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OpenAPI brand passes through the generic source's settings (REQ-2265): a base URL its
setup asks for, a spec read from the running instance, and OAuth client credentials auth.

The brand here is a test brand; the catalogs that use the mechanism are defined elsewhere. The
remote is mocked.
"""

import httpx
import pytest
import respx

from provisa.api.admin import openapi_router
from provisa.api.admin.openapi_router import OpenAPIRegisterRequest
from provisa.api.errors import ApiError
from provisa.api_source.openapi_endpoint import AUTH_SECRET, api_auth, auth_without_secret
from provisa.core.auth_models import ApiAuthOAuth2ClientCredentials
from provisa.graphql_remote.brands import SetupField
from provisa.openapi import brands
from provisa.openapi.brands import Brand, brand_spec
from provisa.openapi.loader import load_spec

HOST = "https://catalog.acme.test"
SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Acme Catalog", "version": "1.4.2"},
    "paths": {"/api/v1/tables": {"get": {"operationId": "listTables", "responses": {}}}},
}

CATALOG = Brand(
    id="acme_catalog",
    label="Acme Catalog",
    spec_url="",
    verify_path="/api/v1/users/me",
    url="{base_url}",
    setup_fields=(SetupField("base_url", r"https?://[^\s{}]+"),),
    instance_spec_url="{base_url}/swagger.json",
    auth_type="oauth2_client_credentials",
    token_url="{base_url}/oauth/token",
    scope="read",
)


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A call resolves its secret references, which asks whether the deployment holds many
    organisations; a deployment of one, as provisa.api.app declares it for a single-tenant
    state."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


@pytest.fixture(autouse=True)
def _catalog_brand(monkeypatch):
    monkeypatch.setitem(brands.BRANDS, CATALOG.id, CATALOG)


def _body(**over) -> OpenAPIRegisterRequest:
    return OpenAPIRegisterRequest(
        **{
            "source_id": "cat",
            "brand": CATALOG.id,
            "setup": {"base_url": HOST},
            "client_id": "provisa-app",
            "client_secret": "s3cret",
            **over,
        }
    )


# --- the brand definition ---


def test_the_base_url_comes_from_setup_and_the_spec_from_the_instance():
    setup = CATALOG.setup({"base_url": f" {HOST} "})
    assert setup == {"base_url": HOST}
    assert CATALOG.base_url(setup) == HOST
    assert CATALOG.source_spec_path(setup) == f"{HOST}/swagger.json"
    assert CATALOG.headers() == {}


def test_a_brand_reading_its_spec_from_the_instance_ships_none():
    with pytest.raises(ValueError, match="ships none"):
        _ = CATALOG.spec_path
    with pytest.raises(FileNotFoundError, match="acme_catalog"):
        brand_spec(CATALOG.id)


def test_client_credentials_are_the_generic_sources_auth_as_entered():
    setup = CATALOG.setup({"base_url": HOST})
    auth = CATALOG.credential_auth(setup, client_id="app", client_secret="${secret:CAT}")
    assert auth == {
        "type": "oauth2_client_credentials",
        "client_id": "app",
        "client_secret": "${secret:CAT}",  # as entered, resolved at each call
        "token_url": f"{HOST}/oauth/token",
        "scope": "read",
    }
    stated = api_auth(auth)
    assert stated is not None
    typed = ApiAuthOAuth2ClientCredentials(**stated)
    assert (typed.token_url, typed.scope) == (f"{HOST}/oauth/token", "read")
    assert auth_without_secret(typed) == {
        "type": "oauth2_client_credentials",
        "client_id": "app",
        "token_url": f"{HOST}/oauth/token",
        "scope": "read",
    }
    assert AUTH_SECRET["oauth2_client_credentials"] == "client_secret"


def test_a_definition_that_cannot_hold_is_refused():
    with pytest.raises(ValueError, match="version header"):
        Brand(
            id="x",
            label="X",
            spec_url="",
            verify_path="/",
            instance_spec_url="{base_url}/openapi.json",
            version_header="X-Version",
        )
    with pytest.raises(ValueError, match="token_url"):
        Brand(id="x", label="X", spec_url="", verify_path="/", token_url="https://t/token")
    with pytest.raises(ValueError, match="token_url"):
        Brand(
            id="x", label="X", spec_url="", verify_path="/", auth_type="oauth2_client_credentials"
        )


# --- adding the source ---


@respx.mock
async def test_adding_it_takes_the_setup_address_the_instance_spec_and_client_credentials():
    token = respx.post(f"{HOST}/oauth/token").mock(
        return_value=httpx.Response(200, json={"access_token": "tok-1", "expires_in": 60})
    )
    check = respx.get(f"{HOST}/api/v1/users/me").mock(return_value=httpx.Response(200, json={}))
    body = await openapi_router._branded(_body())
    assert token.calls.last.request.content.decode().split("&") == [
        "grant_type=client_credentials",
        "client_id=provisa-app",
        "client_secret=s3cret",
        "scope=read",
    ]
    assert check.calls.last.request.headers["authorization"] == "Bearer tok-1"
    assert (body.spec_path, body.base_url, body.spec_content) == (
        f"{HOST}/swagger.json",
        HOST,
        "",
    )
    assert body.setup == {"base_url": HOST}
    assert body.auth_config == {
        "type": "oauth2_client_credentials",
        "client_id": "provisa-app",
        "client_secret": "s3cret",
        "token_url": f"{HOST}/oauth/token",
        "scope": "read",
    }


@respx.mock
def test_the_instance_spec_is_read_from_the_running_instance():
    respx.get(f"{HOST}/swagger.json").mock(return_value=httpx.Response(200, json=SPEC))
    assert load_spec(CATALOG.source_spec_path({"base_url": HOST}))["info"]["version"] == "1.4.2"


@respx.mock
async def test_credentials_the_issuer_rejects_are_refused_by_name():
    respx.post(f"{HOST}/oauth/token").mock(return_value=httpx.Response(401, json={}))
    with pytest.raises(ApiError) as refused:
        await openapi_router._branded(_body(client_id="rejected-app"))
    assert refused.value.code == "openapi.credential_rejected"


async def test_a_missing_base_url_or_secret_is_refused_by_field_before_any_call():
    with pytest.raises(ApiError) as no_host:
        await openapi_router._branded(_body(setup={}))
    with pytest.raises(ApiError) as bad_host:
        await openapi_router._branded(_body(setup={"base_url": "catalog.acme.test"}))
    with pytest.raises(ApiError) as no_secret:
        await openapi_router._branded(_body(client_secret=""))
    with pytest.raises(ApiError) as extra:
        await openapi_router._branded(_body(setup={"base_url": HOST, "store": "x"}))
    assert [
        (e.value.code, e.value.params["field"]) for e in (no_host, bad_host, no_secret, extra)
    ] == [
        ("openapi.setup_invalid", "base_url"),
        ("openapi.setup_invalid", "base_url"),
        ("openapi.credential_required", "client_secret"),
        ("openapi.setup_invalid", "store"),
    ]


async def test_stripe_takes_no_setup():
    with pytest.raises(ApiError) as refused:
        await openapi_router._branded(
            OpenAPIRegisterRequest(
                source_id="pay", brand="stripe", token="sk", setup={"base_url": HOST}
            )
        )
    assert (refused.value.code, refused.value.params["field"]) == (
        "openapi.setup_invalid",
        "base_url",
    )


# --- parameters sent with the client credentials grant (REQ-1874, X-AUDIENCE) ---

TENANT = "acme.auth0.test"

AUDIENCED = Brand(
    id="acme_tenant",
    label="Acme Tenant",
    spec_url="",
    verify_path="/api/v2/stats/active-users",
    url="https://{tenant_domain}",
    setup_fields=(SetupField("tenant_domain", r"[A-Za-z0-9.-]+"),),
    instance_spec_url="https://{tenant_domain}/swagger.json",
    auth_type="oauth2_client_credentials",
    token_url="https://{tenant_domain}/oauth/token",
    token_params={"audience": "https://{tenant_domain}/api/v2/"},
)


@pytest.fixture
def _audienced_brand(monkeypatch):
    monkeypatch.setitem(brands.BRANDS, AUDIENCED.id, AUDIENCED)


@respx.mock
@pytest.mark.usefixtures("_audienced_brand")
async def test_the_token_request_carries_the_rendered_token_params():
    token = respx.post(f"https://{TENANT}/oauth/token").mock(
        return_value=httpx.Response(200, json={"access_token": "tok-a", "expires_in": 60})
    )
    respx.get(f"https://{TENANT}/swagger.json").mock(return_value=httpx.Response(200, json=SPEC))
    check = respx.get(f"https://{TENANT}/api/v2/stats/active-users").mock(
        return_value=httpx.Response(200, json=0)
    )
    body = await openapi_router._branded(_body(brand=AUDIENCED.id, setup={"tenant_domain": TENANT}))
    assert token.calls.last.request.content.decode().split("&") == [
        "grant_type=client_credentials",
        "client_id=provisa-app",
        "client_secret=s3cret",
        "audience=https%3A%2F%2Facme.auth0.test%2Fapi%2Fv2%2F",
    ]
    assert check.calls.last.request.headers["authorization"] == "Bearer tok-a"
    assert body.auth_config is not None
    assert body.auth_config["token_params"] == {"audience": f"https://{TENANT}/api/v2/"}


def test_token_params_travel_with_the_stored_auth():
    auth = AUDIENCED.credential_auth(
        {"tenant_domain": TENANT}, client_id="app", client_secret="${secret:T}"
    )
    stated = api_auth(auth)
    assert stated is not None
    typed = ApiAuthOAuth2ClientCredentials(**stated)
    assert typed.token_params == {"audience": f"https://{TENANT}/api/v2/"}
    assert auth_without_secret(typed) == {
        "type": "oauth2_client_credentials",
        "client_id": "app",
        "token_url": f"https://{TENANT}/oauth/token",
        "token_params": {"audience": f"https://{TENANT}/api/v2/"},
    }


async def test_a_token_param_naming_a_setup_field_the_source_lacks_is_refused_by_name(
    monkeypatch,
):
    lacking = Brand(
        id="acme_lacking",
        label="Acme Lacking",
        spec_url="",
        verify_path="/",
        url="https://{tenant_domain}",
        setup_fields=(SetupField("tenant_domain", r"[A-Za-z0-9.-]+"),),
        instance_spec_url="https://{tenant_domain}/swagger.json",
        auth_type="oauth2_client_credentials",
        token_url="https://{tenant_domain}/oauth/token",
        token_params={"audience": "https://{api_host}/api/v2/"},
    )
    monkeypatch.setitem(brands.BRANDS, lacking.id, lacking)
    with pytest.raises(ApiError) as refused:
        await openapi_router._branded(_body(brand=lacking.id, setup={"tenant_domain": TENANT}))
    assert (refused.value.code, refused.value.params["field"]) == (
        "openapi.setup_invalid",
        "api_host",
    )
    assert "api_host" in str(refused.value)


def test_token_params_that_cannot_hold_are_refused():
    with pytest.raises(ValueError, match="token_params"):
        Brand(
            id="x",
            label="X",
            spec_url="",
            verify_path="/",
            token_params={"audience": "https://a/"},
        )
    with pytest.raises(ValueError, match="grant_type"):
        Brand(
            id="x",
            label="X",
            spec_url="",
            verify_path="/",
            auth_type="oauth2_client_credentials",
            token_url="https://t/token",
            token_params={"grant_type": "password"},
        )
