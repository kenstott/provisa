# Copyright (c) 2026 Kenneth Stott
# Canary: 424a42c2-14bd-4441-a66d-4f794fbb6b54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.openapi.loader."""

import json
import pathlib
import pytest
import httpx
import respx

from provisa.openapi.loader import load_spec

SAMPLE_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Test API", "version": "1.0.0"},
    "paths": {},
}

SAMPLE_YAML = 'openapi: "3.0.0"\ninfo:\n  title: Test API\n  version: "1.0.0"\npaths: {}\n'
SAMPLE_JSON = json.dumps(SAMPLE_SPEC)


def test_load_local_yaml(tmp_path: pathlib.Path):
    f = tmp_path / "spec.yaml"
    f.write_text(SAMPLE_YAML)
    result = load_spec(str(f))
    assert result["openapi"] == "3.0.0"
    assert result["info"]["title"] == "Test API"


def test_load_local_json(tmp_path: pathlib.Path):
    f = tmp_path / "spec.json"
    f.write_text(SAMPLE_JSON)
    result = load_spec(str(f))
    assert result["openapi"] == "3.0.0"


@respx.mock
def test_load_remote_yaml():
    url = "https://example.com/api/spec.yaml"
    respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE_YAML))
    result = load_spec(url)
    assert result["openapi"] == "3.0.0"


@respx.mock
def test_load_remote_json():
    url = "https://example.com/api/spec.json"
    respx.get(url).mock(return_value=httpx.Response(200, text=SAMPLE_JSON))
    result = load_spec(url)
    assert result["openapi"] == "3.0.0"


def test_missing_local_path_raises():
    with pytest.raises(FileNotFoundError):
        load_spec("/nonexistent/path/to/spec.yaml")


@respx.mock
def test_http_404_raises():
    url = "https://example.com/api/missing.json"
    respx.get(url).mock(return_value=httpx.Response(404))
    with pytest.raises(httpx.HTTPStatusError):
        load_spec(url)


# --- where a spec's calls go (REQ-2265) ---


@pytest.mark.parametrize(
    ("server", "read_from", "called_at"),
    [
        (
            "https://api.example.com/v1/",
            "https://docs.example.com/spec.json",
            "https://api.example.com/v1",
        ),
        (
            "/openapi/",
            "http://gms:8080/openapi/v3/api-docs/10-openapi-v3",
            "http://gms:8080/openapi",
        ),
        ("/", "https://dh.example.com/openapi/v3/api-docs/10-openapi-v3", "https://dh.example.com"),
        ("v2", "https://h.example.com/specs/api.json", "https://h.example.com/specs/v2"),
        ("/api", "/srv/specs/api.json", "/api"),
    ],
)
def test_a_relative_server_is_read_against_where_the_spec_was_read_from(
    server, read_from, called_at
):
    from provisa.openapi.loader import spec_server_url

    assert spec_server_url({"servers": [{"url": server}]}, read_from) == called_at


def test_a_spec_naming_no_server_names_no_address():
    from provisa.openapi.loader import spec_server_url

    assert spec_server_url({}, "https://h.example.com/spec.json") == ""


# --- a spec behind auth is read with the source's credential (REQ-2265) ---

SPEC_URL = "https://catalog.example.com/openapi.json"
TOKEN_URL = "https://login.example.com/oauth2/token"


@pytest.fixture
def _one_organisation(monkeypatch):
    """Secret references resolve as for a deployment of one organisation."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


def _data_call_parts(auth) -> tuple[dict, dict]:
    """The headers and query parameters the source's data calls carry for ``auth``."""
    from provisa.api_source.caller import prepare_call
    from provisa.api_source.models import ApiEndpoint
    from pydantic import TypeAdapter

    from provisa.core.auth_models import ApiAuth, ApiAuthNone

    typed = TypeAdapter(ApiAuth).validate_python(auth)
    assert not isinstance(typed, ApiAuthNone)
    call = prepare_call(
        ApiEndpoint(source_id="s", path="/things", method="GET", table_name="things", columns=[]),
        {},
        "https://catalog.example.com",
        typed,
    )
    return call.headers, call.params


@pytest.mark.parametrize(
    "auth_config",
    [
        {"type": "bearer", "token": "s3cret-bearer"},
        {"type": "basic", "username": "steward", "password": "s3cret-pass"},
        {"type": "api_key", "header_name": "X-Catalog-Key", "api_key": "s3cret-key"},
    ],
    ids=["bearer", "basic", "api_key"],
)
@respx.mock
def test_a_spec_behind_auth_is_read_with_the_credential_the_data_calls_carry(
    auth_config, _one_organisation
):
    from provisa.api_source.openapi_endpoint import api_auth

    auth = api_auth(auth_config)
    headers, _ = _data_call_parts(auth)
    name, value = next((k, v) for k, v in headers.items() if "s3cret" in v or k == "Authorization")
    route = respx.get(SPEC_URL, headers={name: value}).mock(
        return_value=httpx.Response(200, json=SAMPLE_SPEC)
    )
    respx.get(SPEC_URL).mock(return_value=httpx.Response(401))
    assert load_spec(SPEC_URL, auth) == SAMPLE_SPEC
    assert route.called


@respx.mock
def test_an_api_key_in_the_query_is_sent_in_the_query(_one_organisation):
    auth = {"type": "api_key", "key": "s3cret-q", "name": "api_key", "location": "query"}
    _, params = _data_call_parts(auth)
    assert params == {"api_key": "s3cret-q"}
    route = respx.get(SPEC_URL, params=params).mock(
        return_value=httpx.Response(200, json=SAMPLE_SPEC)
    )
    assert load_spec(SPEC_URL, auth) == SAMPLE_SPEC
    assert route.called


@respx.mock
def test_a_client_credentials_spec_is_read_with_the_token_it_is_granted(_one_organisation):
    # a client secret no other test uses, so no token held from another test answers
    respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "granted-tok", "expires_in": 60})
    )
    route = respx.get(SPEC_URL, headers={"Authorization": "Bearer granted-tok"}).mock(
        return_value=httpx.Response(200, json=SAMPLE_SPEC)
    )
    auth = {
        "type": "oauth2_client_credentials",
        "client_id": "cid",
        "client_secret": "s3cret-client-spec-fetch",
        "token_url": TOKEN_URL,
    }
    assert load_spec(SPEC_URL, auth) == SAMPLE_SPEC
    assert route.called


@respx.mock
def test_a_credential_reference_is_resolved_for_the_spec_request(monkeypatch, _one_organisation):
    monkeypatch.setenv("CATALOG_TOKEN", "from-env")
    route = respx.get(SPEC_URL, headers={"Authorization": "Bearer from-env"}).mock(
        return_value=httpx.Response(200, json=SAMPLE_SPEC)
    )
    assert load_spec(SPEC_URL, {"type": "bearer", "token": "${env:CATALOG_TOKEN}"}) == SAMPLE_SPEC
    assert route.called


@respx.mock
def test_a_public_spec_is_read_without_a_credential():
    route = respx.get(SPEC_URL).mock(return_value=httpx.Response(200, json=SAMPLE_SPEC))
    assert load_spec(SPEC_URL, None) == SAMPLE_SPEC
    assert "authorization" not in route.calls.last.request.headers


def test_a_spec_file_loads_whatever_the_source_authenticates_with(tmp_path: pathlib.Path):
    f = tmp_path / "spec.json"
    f.write_text(SAMPLE_JSON)
    assert load_spec(str(f), {"type": "bearer", "token": "unused"}) == SAMPLE_SPEC


@respx.mock
def test_the_credential_is_in_no_error_or_log(caplog, _one_organisation):
    import logging

    caplog.set_level(logging.DEBUG)
    respx.get(SPEC_URL).mock(return_value=httpx.Response(403))
    query = {"type": "api_key", "key": "s3cret-q", "name": "api_key", "location": "query"}
    header = {"type": "bearer", "token": "s3cret-bearer"}
    for auth, secret in ((query, "s3cret-q"), (header, "s3cret-bearer")):
        with pytest.raises(httpx.HTTPStatusError) as refused:
            load_spec(SPEC_URL, auth)
        assert "403" in str(refused.value)
        assert secret not in str(refused.value)
        assert refused.value.__cause__ is None and refused.value.__context__ is None
        assert secret not in repr(refused.value.request.url)
    assert "s3cret-bearer" not in caplog.text
