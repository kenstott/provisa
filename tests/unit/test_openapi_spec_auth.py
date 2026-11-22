# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0f6c1e-2265-4b7a-9c55-5a0d2265e0a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every read of an OpenAPI source's spec carries the source's credential (REQ-2265).

The spec loader itself is tested in test_openapi_loader.py; here each place that reads a spec
hands it the credential the source's data calls carry: the registration's own, the stored one
on a refresh or a reload, the brand's at a branded registration, and the form's at a preview.
"""

# Requirements: REQ-2265

import json
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
import respx

from provisa.api.admin import openapi_router
from provisa.api.admin.openapi_router import OpenAPIPreviewRequest, OpenAPIRegisterRequest
from provisa.api_source import openapi_endpoint

pytestmark = pytest.mark.unit

SPEC = {"openapi": "3.0.0", "info": {"title": "Catalog", "version": "1"}, "paths": {}}
BEARER = {"type": "bearer", "token": "s3cret-bearer"}


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


class _Read(Exception):
    """Raised by the stand-in loader once it has seen what the spec is read with."""


def _seen_loader(monkeypatch) -> list[tuple[str, object]]:
    from provisa.openapi import loader

    seen: list[tuple[str, object]] = []

    def load_spec(spec_path, auth=None):
        seen.append((spec_path, auth))
        raise _Read

    monkeypatch.setattr(loader, "load_spec", load_spec)
    return seen


class _Conn:
    """A connection whose one ``api_sources.auth`` row is ``stored`` (as store_openapi_auth
    writes it: encrypted)."""

    def __init__(self, stored: dict | None, present: bool = True) -> None:
        from provisa.encryption import encryption_service

        self._row = (
            None
            if not present
            else SimpleNamespace(
                auth=None
                if stored is None
                else encryption_service().encrypt(json.dumps(stored).encode())
            )
        )
        self.asked: list[object] = []

    async def execute_core(self, stmt):
        self.asked.append(stmt)
        return SimpleNamespace(fetchone=lambda: self._row)


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool.conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


# --- what is stored ---


@pytest.mark.parametrize(
    ("stored", "present", "read"),
    [(BEARER, True, BEARER), (None, True, None), (None, False, None)],
    ids=["stored", "none-stored", "no-row"],
)
async def test_the_stored_auth_is_read_back_as_written(stored, present, read):
    conn = cast(Any, _Conn(stored, present))
    assert await openapi_endpoint.stored_openapi_auth(conn, "cat") == read


# --- registration and refresh ---


async def test_a_registration_reads_the_spec_with_the_credential_it_supplies(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(state, "model_db", _Pool(_Conn(None)), raising=False)
    seen = _seen_loader(monkeypatch)
    with pytest.raises(_Read):
        await openapi_router._load_and_register(
            "cat", "https://cat.example.com/openapi.json", "", BEARER, 300, store_auth=True
        )
    assert seen == [("https://cat.example.com/openapi.json", BEARER)]


async def test_a_refresh_reads_the_spec_with_the_stored_credential(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(state, "model_db", _Pool(_Conn(BEARER)), raising=False)
    seen = _seen_loader(monkeypatch)
    with pytest.raises(_Read):
        await openapi_router._load_and_register(
            "cat", "https://cat.example.com/openapi.json", "", None, 300
        )
    assert seen == [("https://cat.example.com/openapi.json", BEARER)]


async def test_a_reload_reads_the_spec_with_the_stored_credential(monkeypatch):
    from provisa.api.admin import schema_helpers
    from provisa.openapi import exclusions

    source_row = SimpleNamespace(
        _mapping={
            "type": "openapi",
            "path": "https://cat.example.com/openapi.json",
            "mapping": {},
            "brand_setup": {},
        }
    )
    stored = _Conn(BEARER)
    stored_row = stored._row

    class _Lookup(_Conn):
        """The source row first, then its stored auth."""

        async def execute_core(self, stmt):
            self.asked.append(stmt)
            row = source_row if len(self.asked) == 1 else stored_row
            return SimpleNamespace(fetchone=lambda: row)

    lookup = _Lookup(None)

    async def _pool():
        return _Pool(lookup)

    monkeypatch.setattr(schema_helpers, "_get_pool", _pool)
    from provisa.api.app import state

    monkeypatch.setattr(state, "openapi_specs", {}, raising=False)
    seen: list[object] = []

    def load_source_spec(spec_path, mapping, *, source_id, auth):
        assert (spec_path, mapping, source_id) == (
            "https://cat.example.com/openapi.json",
            {},
            "cat",
        )
        seen.append(auth)
        return SPEC

    monkeypatch.setattr(exclusions, "load_source_spec", load_source_spec)
    assert await schema_helpers._ensure_openapi_spec("cat") is True
    assert seen == [BEARER]


# --- a branded registration ---


@respx.mock
async def test_a_branded_instance_spec_behind_auth_is_read_with_the_brands_credential():
    from provisa.openapi.brands import BRANDS

    dh = BRANDS["datahub"]
    gms = "https://datahub.acme.test"
    spec_url = dh.source_spec_path(dh.setup({"base_url": gms}))
    respx.get(spec_url, headers={"Authorization": "Bearer eyJpat"}).mock(
        return_value=httpx.Response(200, json={**SPEC, "servers": [{"url": "/openapi/"}]})
    )
    respx.get(spec_url).mock(return_value=httpx.Response(401))
    respx.get(url__startswith=f"{gms}/openapi/v3/entity/").mock(
        return_value=httpx.Response(200, json={"entities": []})
    )
    body = await openapi_router._branded(
        OpenAPIRegisterRequest(
            source_id="dh", brand="datahub", setup={"base_url": gms}, token="eyJpat"
        )
    )
    assert body.base_url == f"{gms}/openapi"


# --- a preview ---


@respx.mock
async def test_a_preview_reads_a_spec_behind_auth_with_the_forms_credential(monkeypatch):
    monkeypatch.setattr(openapi_router, "require_capability_request", lambda *a: None)
    url = "https://cat.example.com/openapi.json"
    respx.get(url, headers={"Authorization": "Bearer s3cret-bearer"}).mock(
        return_value=httpx.Response(200, json=SPEC)
    )
    respx.get(url).mock(return_value=httpx.Response(401))
    answer = await openapi_router.preview_openapi_spec(
        cast(Any, SimpleNamespace()), OpenAPIPreviewRequest(spec_path=url, auth_config=BEARER)
    )
    assert answer["spec_description"] == "Catalog"


@respx.mock
async def test_a_refused_preview_does_not_repeat_the_credential(monkeypatch):
    from provisa.api.errors import ApiError

    monkeypatch.setattr(openapi_router, "require_capability_request", lambda *a: None)
    url = "https://cat.example.com/openapi.json"
    respx.get(url).mock(return_value=httpx.Response(403))
    with pytest.raises(ApiError) as refused:
        await openapi_router.preview_openapi_spec(
            cast(Any, SimpleNamespace()),
            OpenAPIPreviewRequest(
                spec_path=url,
                auth_config={"type": "api_key", "header_name": "X-Key", "api_key": "s3cret-key"},
            ),
        )
    assert "403" in str(refused.value)
    assert "s3cret-key" not in str(refused.value) + json.dumps(refused.value.params)
