# Copyright (c) 2026 Kenneth Stott
# Canary: 650803bf-8042-4108-811f-cf3ec297d9e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-831: demo datasources run as host ASGI apps (no Docker).

Drives the two demo ASGI apps in-process (no subprocess, no Docker) to prove
they serve the same OpenAPI spec / GraphQL schema Provisa introspects, so the
demo behaves identically whether the mock runs in Docker or on the host.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import httpx
import pytest

_REPO = Path(__file__).resolve().parents[2]


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, _REPO / rel)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Register before exec so dataclass/strawberry field resolution can look the
    # module up by __module__ (Strawberry types are dataclasses).
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def petstore_app():
    return _load("demo_petstore_server", "demo/petstore_server/server.py").app


@pytest.fixture(scope="module")
def graphql_app():
    return _load("demo_graphql_server", "demo/graphql_server/server.py").app


async def test_petstore_serves_openapi_spec(petstore_app):
    transport = httpx.ASGITransport(app=petstore_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://demo") as c:
        resp = await c.get("/api/v3/openapi.json")
    assert resp.status_code == 200
    spec = resp.json()
    # Spec is rewritten to /api/v3 so Provisa introspects the host server.
    assert spec["servers"][0]["url"] == "/api/v3"


async def test_petstore_serves_seed_data(petstore_app):
    transport = httpx.ASGITransport(app=petstore_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://demo") as c:
        resp = await c.get("/api/v3/pet/findByStatus", params={"status": "available"})
    assert resp.status_code == 200
    pets = resp.json()
    assert isinstance(pets, list) and pets, "seed pets must be served"


async def test_graphql_demo_answers_query(graphql_app):
    transport = httpx.ASGITransport(app=graphql_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://demo") as c:
        resp = await c.post("/", json={"query": "{ animalBreeds { name } }"})
    assert resp.status_code == 200
    body = resp.json()
    assert "errors" not in body, body
    assert body["data"]["animalBreeds"], "seed animal breeds must be served"
