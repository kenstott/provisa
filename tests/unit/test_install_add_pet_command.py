# Copyright (c) 2026 Kenneth Stott
# Canary: 2ccf0b6e-3a31-4e84-93f6-c8e72bf8d441
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The standard demo's add_pet command (config/provisa-install.yaml) is the petstore API's addPet
operation, called as it is (REQ-1924), not host code (REQ-2134); the demo's petstore mock answers
it with the pet it added."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx
from starlette.testclient import TestClient

from provisa.api_source.models import ApiSource
from provisa.core.config_loader import parse_config
from provisa.executor import source_operation as ops

_REPO = Path(__file__).resolve().parents[2]
BASE = "http://petstore-mock:8080/api/v3"


def _mock_app():
    spec = importlib.util.spec_from_file_location(
        "petstore_mock_server", _REPO / "demo" / "petstore_server" / "server.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.app


@pytest.fixture
def state(monkeypatch) -> SimpleNamespace:
    st = SimpleNamespace(
        multitenancy=False,
        source_types={"petstore-api": "openapi"},
        openapi_specs={
            "petstore-api": {
                "spec": json.loads((_REPO / "demo/petstore_server/openapi.json").read_text()),
                "base_url": BASE,
            }
        },
        api_sources={"petstore-api": ApiSource(id="petstore-api", type="openapi", base_url=BASE)},
    )
    monkeypatch.setattr("provisa.api.app.state", st)
    return st


def test_the_install_configs_add_pet_is_the_petstore_apis_add_pet_operation():
    [add_pet] = [
        f
        for f in parse_config(_REPO / "config/provisa-install.yaml").functions
        if f.name == "add_pet"
    ]
    assert (add_pet.impl_kind, add_pet.source_id, add_pet.function_name) == (
        "source_operation",
        "petstore-api",
        "addPet",
    )
    assert add_pet.product_id == "pet_health"
    assert [a.name for a in add_pet.arguments] == ["body"]


def test_the_install_config_holds_no_host_code():
    functions = parse_config(_REPO / "config/provisa-install.yaml").functions
    assert not [f.name for f in functions if f.impl_kind in {"python", "script"}]


@pytest.mark.asyncio
async def test_the_petstore_api_offers_add_pet_with_its_body(state):
    operation = await ops.offered_operation(state, "petstore-api", "addPet")
    assert operation.arguments == ("body",)
    assert not operation.reads


@pytest.mark.asyncio
@respx.mock
async def test_the_petstore_mock_answers_add_pet_with_the_pet_it_added(state):
    mock = TestClient(_mock_app())

    def answer(request: httpx.Request) -> httpx.Response:
        got = mock.request(
            request.method,
            request.url.path,
            content=request.content,
            headers={"content-type": request.headers.get("content-type", "application/json")},
        )
        return httpx.Response(got.status_code, content=got.content, headers=dict(got.headers))

    respx.post(f"{BASE}/pet").mock(side_effect=answer)
    body = {"name": "Biscuit", "photoUrls": [], "status": "available"}
    rows = await ops.call_operation(state, "petstore-api", "addPet", {"body": body})
    [pet] = rows
    assert pet["name"] == "Biscuit" and pet["status"] == "available"
    assert isinstance(pet["id"], int)
