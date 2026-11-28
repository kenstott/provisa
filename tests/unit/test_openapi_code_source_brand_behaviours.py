# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e8d17-3f6a-4c92-9e41-a27c8d03f6b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The brand behaviours of the in-process OpenAPI source, served through the built-in OpenAPI code
source (REQ-2267: one serving path per type; REQ-1874: brands ride it).

- a client credentials grant carries its brand's ``token_params`` (Auth0's ``audience``);
- a read follows a next-page cursor the answer holds deeper than one level or in camelCase;
- a GET whose row schema has no properties is a generated command that reads, never a table.

Each source's code is written as its save writes it (provisa/codesource/builtin.py) and loaded
as the code's entry module loads it; the remote is mocked at the HTTP layer."""

# Requirements: REQ-2267, REQ-1874, REQ-1924, REQ-318
from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest
import respx

from provisa.codesource.builtin import _openapi_code
from provisa.openapi import code_source as oc

pytestmark = pytest.mark.unit

API = "https://api.vendor.test"
TOKEN_URL = "https://vendor.test/oauth/token"


@pytest.fixture(autouse=True)
def ps(monkeypatch):
    """Provisa's services module, on the path as the host puts it (provisa/python/host.py), its
    secrets service answering the source's declared credential."""
    import importlib
    import sys

    from provisa.python.source import HOST

    monkeypatch.syspath_prepend(str(HOST.parent / "services"))
    module = importlib.import_module("provisa_services")
    secrets = {"credential": "s3cret"}
    monkeypatch.setattr(module.secrets, "get", staticmethod(lambda name: secrets[name]))
    yield module
    sys.modules.pop("provisa_services", None)


@pytest.fixture(autouse=True)
def _no_held_tokens(monkeypatch):
    """Each test asks for its own token: none held from another test."""
    from provisa.api_source import oauth_grants

    monkeypatch.setattr(oauth_grants, "held_token", lambda _key: None)


class _Context:
    """A read's context as the host gives it to a table handler: not a loader call."""

    keys = None


def _obj(**properties) -> dict:
    return {"type": "object", "properties": properties}


def _list_spec(answer: dict, params: dict[str, str]) -> dict:
    return {
        "openapi": "3.0.0",
        "info": {"title": "Vendor", "version": "1"},
        "servers": [{"url": API}],
        "paths": {
            "/things": {
                "get": {
                    "operationId": "listThings",
                    "parameters": [
                        {"name": n, "in": "query", "schema": {"type": t}} for n, t in params.items()
                    ],
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {"application/json": {"schema": answer}},
                        }
                    },
                }
            }
        },
    }


_ROW = _obj(id={"type": "string"}, name={"type": "string"})


def _paged(**extra) -> dict:
    answer = _obj(data={"type": "array", "items": _ROW}, **extra)
    answer["required"] = ["data"]
    return answer


def _saved(tmp_path, spec: dict, mapping: dict | None = None) -> oc.OpenAPICode:
    """The source's code as its save writes it from ``spec`` (an inline file path), loaded as its
    entry module loads it."""
    import json

    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec))
    directory = tmp_path / "code"
    _openapi_code(directory, "vendor-src", str(path), mapping or {})
    return oc.OpenAPICode.load(directory)


# -- token_params -----------------------------------------------------------------------------------

_TYPED_SPEC = _list_spec({"type": "array", "items": _ROW}, {})


@respx.mock
async def test_a_client_credentials_read_sends_the_brands_token_params(tmp_path):
    code = _saved(
        tmp_path,
        _TYPED_SPEC,
        {
            "auth": {
                "type": "oauth2_client_credentials",
                "client_id": "provisa-app",
                "client_secret": "${secret:VENDOR_SECRET}",
                "token_url": TOKEN_URL,
                "token_params": {"audience": "https://vendor.test/api/v2/"},
            }
        },
    )
    token = respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "tok-a", "expires_in": 60})
    )
    things = respx.get(f"{API}/things").mock(
        return_value=httpx.Response(200, json=[{"id": "1", "name": "a"}])
    )

    assert await code.serve_table("listThings", {}, _Context()) == [{"id": "1", "name": "a"}]

    assert parse_qs(token.calls.last.request.content.decode()) == {
        "grant_type": ["client_credentials"],
        "client_id": ["provisa-app"],
        "client_secret": ["s3cret"],
        "audience": ["https://vendor.test/api/v2/"],
    }
    assert things.calls.last.request.headers["authorization"] == "Bearer tok-a"


def test_the_code_directory_keeps_the_token_params_and_never_the_secret(tmp_path):
    code = _saved(
        tmp_path,
        _TYPED_SPEC,
        {
            "auth": {
                "type": "oauth2_client_credentials",
                "client_id": "provisa-app",
                "client_secret": "${secret:VENDOR_SECRET}",
                "token_url": TOKEN_URL,
                "token_params": {"audience": "https://vendor.test/api/v2/"},
            }
        },
    )
    assert code.auth is not None
    assert code.auth["token_params"] == {"audience": "https://vendor.test/api/v2/"}
    assert code.auth["client_secret"] == {"$secret": "credential"}


# -- nested cursors ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("brand", "answer", "params", "cursor_param", "pages"),
    [
        (
            "hubspot",
            _paged(paging=_obj(next=_obj(after={"type": "string"}))),
            {"after": "string", "limit": "integer"},
            "after",
            [
                {"data": [{"id": "1", "name": "a"}], "paging": {"next": {"after": "c2"}}},
                {"data": [{"id": "2", "name": "b"}]},
            ],
        ),
        (
            "calcom",
            _paged(pagination=_obj(nextCursor={"type": "string"})),
            {"cursor": "string", "limit": "integer"},
            "cursor",
            [
                {"data": [{"id": "1", "name": "a"}], "pagination": {"nextCursor": "c2"}},
                {"data": [{"id": "2", "name": "b"}], "pagination": {"nextCursor": None}},
            ],
        ),
        (
            "slack",
            _paged(response_metadata=_obj(next_cursor={"type": "string"})),
            {"cursor": "string", "limit": "integer"},
            "cursor",
            [
                {"data": [{"id": "1", "name": "a"}], "response_metadata": {"next_cursor": "c2"}},
                {"data": [{"id": "2", "name": "b"}], "response_metadata": {"next_cursor": ""}},
            ],
        ),
    ],
)
@respx.mock
async def test_a_read_follows_a_nested_or_camel_case_cursor_to_the_last_page(
    tmp_path, brand, answer, params, cursor_param, pages
):
    code = _saved(tmp_path, _list_spec(answer, params))
    route = respx.get(f"{API}/things").mock(
        side_effect=[httpx.Response(200, json=page) for page in pages]
    )

    rows = await code.serve_table("listThings", {}, _Context())

    # Each row also carries the read's parameters as columns (endpoint_columns), unbound here.
    assert [{k: r[k] for k in ("id", "name")} for r in rows] == [
        {"id": "1", "name": "a"},
        {"id": "2", "name": "b"},
    ], brand
    sent = [call.request.url.params.get(cursor_param) for call in route.calls]
    assert sent == [None, "c2"], brand


# -- propertyless GETs ------------------------------------------------------------------------------

_PROPERTYLESS_SPEC = {
    "openapi": "3.0.1",
    "info": {"title": "Shelter", "version": "1"},
    "servers": [{"url": API}],
    "paths": {
        "/dogs": {
            "get": {
                "operationId": "listDogs",
                "responses": {
                    "200": {
                        "description": "the dogs",
                        "content": {
                            "application/json": {
                                "schema": {"type": "array", "items": {"type": "object"}}
                            }
                        },
                    }
                },
            }
        }
    },
}


def test_a_propertyless_get_is_generated_as_a_command_and_no_table(tmp_path, ps):
    code = _saved(tmp_path, _PROPERTYLESS_SPEC)
    discovered = code.discover(None, None, object())
    assert [t.name for t in discovered.tables] == []
    assert [c.name for c in discovered.commands] == ["listDogs"]


@respx.mock
async def test_a_propertyless_gets_command_reads_its_json(tmp_path):
    code = _saved(tmp_path, _PROPERTYLESS_SPEC)
    dogs = respx.get(f"{API}/dogs").mock(
        return_value=httpx.Response(200, json=[{"name": "rex", "age": 3}])
    )

    assert await code.serve_command("listDogs", {}) == [{"name": "rex", "age": 3}]
    assert dogs.calls.last.request.method == "GET"
