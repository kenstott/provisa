# Copyright (c) 2026 Kenneth Stott
# Canary: d8705780-f0bf-4f46-a710-f0de023fda11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-257: the JSON:API surface's content negotiation, query parameter families, lookup of a
missing resource and JSON:API object, as JSON:API 1.1 states them (the JSON:API conformance
cases, REQ-2167).

- Content-Type: the JSON:API media type with a parameter other than ext or profile, or naming an
  extension (Provisa serves none), is 415.
- Accept: when every JSON:API media type instance is modified by a parameter other than ext or
  profile, or names an extension, 406; one plain instance is served.
- A query parameter whose family is all lowercase a-z and which the read does not process: 400.
- A lookup whose id is not a value of the key column's type names no resource: 404.
- A table read states ``jsonapi.version`` 1.1.
"""

# Requirements: REQ-257, REQ-2167

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from provisa.api.jsonapi.negotiation import (
    accept_refusal,
    content_type_refusal,
    unknown_reserved_parameter,
)
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from tests.role_state import with_surfaces

MEDIA = "application/vnd.api+json"
_ROLE = {"X-Provisa-Role": "org_admin"}
_COLLECTION = "/data/jsonapi/shelter/shelters"


# --- the rules -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "accept",
    [
        f"{MEDIA}; charset=utf-8",
        f'{MEDIA}; ext="https://example.invalid/ext"',
        f"{MEDIA}; charset=utf-8, {MEDIA}; q=0.5",
        f"{MEDIA}; charset=utf-8, */*",
        "application/json",
    ],
)
def test_an_accept_the_server_cannot_serve_is_refused(accept):
    assert accept_refusal(accept) is not None


@pytest.mark.parametrize(
    "accept",
    [
        None,
        "",
        MEDIA,
        f"{MEDIA}; charset=utf-8, {MEDIA}",
        f'{MEDIA}; profile="https://example.invalid/p"',
        "*/*",
        "application/*",
        "text/html, */*",
    ],
)
def test_an_accept_naming_a_plain_instance_or_any_type_is_served(accept):
    assert accept_refusal(accept) is None


@pytest.mark.parametrize(
    "content_type",
    [f"{MEDIA}; charset=utf-8", f'{MEDIA}; ext="https://example.invalid/ext"'],
)
def test_a_content_type_with_another_parameter_or_an_extension_is_refused(content_type):
    assert content_type_refusal(content_type) is not None


@pytest.mark.parametrize(
    "content_type",
    [None, "", MEDIA, f'{MEDIA}; profile="https://example.invalid/p"', "application/json"],
)
def test_a_plain_or_profiled_content_type_is_served(content_type):
    assert content_type_refusal(content_type) is None


def test_an_unknown_all_lowercase_parameter_is_named_and_the_known_ones_are_not():
    known = ["sort", "include", "fields[x]", "page[size]", "filter[a][gt]", "role", "params"]
    assert unknown_reserved_parameter([*known, "aggregate", "groupBy", "includeNodes"]) is None
    assert unknown_reserved_parameter(["sort", "unknownparam"]) == "unknownparam"
    assert unknown_reserved_parameter(["bogus[x]"]) == "bogus[x]"
    # an implementation-specific name (a character outside a-z) is not the specification's
    assert unknown_reserved_parameter(["x-custom", "camelCase", "a1"]) is None


# --- the router ----------------------------------------------------------------------------------


def _schema_input() -> SchemaInput:
    """shelters: a key column id (integer) and a name."""
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "shelter-sqlite",
                "domain_id": "shelter",
                "schema_name": "main",
                "table_name": "shelters",
                "columns": [
                    {"column_name": c, "visible_to": ["org_admin"], "is_primary_key": c == "id"}
                    for c in ("id", "shelter_name")
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="shelter_name", data_type="varchar", is_nullable=True),
            ]
        },
        naming_rules=[],
        role={"id": "org_admin", "capabilities": ["full_results"], "domain_access": ["*"]},
        domains=[{"id": "shelter", "description": "Shelter"}],
    )


@pytest.fixture()
def client(monkeypatch):
    """The JSON:API router over a real schema and context; the engine answers two shelters.

    # unit: mock-justified — the engine is the external boundary; the router's negotiation,
    # parameter checks, lookup and the Rust encoder, which are under test, run unmodified.
    """
    import pyarrow as pa
    from fastapi import FastAPI

    from provisa.api.jsonapi.generator import create_jsonapi_router
    from provisa.auth.middleware import AuthMiddleware
    from provisa.executor import json_encoders

    si = _schema_input()
    schema = generate_schema(si)
    ctx = build_context(si)
    state = MagicMock()
    state.schemas = {"org_admin": schema}
    state.contexts = {"org_admin": ctx}
    state.rls_contexts = {"org_admin": RLSContext.empty()}
    state.roles = {"org_admin": si.role}
    state.table_path_maps = {
        "org_admin": {
            gql: {
                "domain_id": "shelter",
                "table_name": meta.table_name,
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
            for gql, meta in ctx.tables.items()
        }
    }
    with_surfaces(state)
    state.masking_rules = {}
    state.source_types = {"shelter-sqlite": "sqlite"}
    state.source_dialects = {"shelter-sqlite": "sqlite"}

    # keyed by the name with case and underscores folded: the field is spelled by the schema's
    # naming convention (shelter_name or shelterName)
    rows = [{"id": 1, "sheltername": "North"}, {"id": 2, "sheltername": "South"}]
    real = json_encoders.jsonapi

    def encode(batches, columns, *args, **kwargs):
        def value(row: dict, field: str) -> object:
            return row[field.replace("_", "").lower()]

        batch = pa.record_batch(
            {
                f"c{i}": pa.array([value(r, c.field_name) for r in rows])
                for i, c in enumerate(columns)
            }
        )
        return real([batch], columns, *args, **kwargs)

    async def _fake_govern(
        sql, role_id, exec_params=None, state=None, deliver=None, buffered=False, **_
    ):
        return SimpleNamespace()

    async def _fake_execute(plan, state):
        result = MagicMock()
        result.redirect = None
        result.cache_entry = None
        return result

    monkeypatch.setattr(json_encoders, "jsonapi", encode)
    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(create_jsonapi_router(state))
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def _get(client, path: str, headers: dict, params: dict | None = None) -> httpx.Response:
    async with client as c:
        return await c.get(path, params=params, headers={**_ROLE, **headers})


@pytest.mark.anyio
@pytest.mark.parametrize(
    "accept", [f"{MEDIA}; charset=utf-8", f'{MEDIA}; ext="https://example.invalid/ext"']
)
async def test_an_accept_of_only_modified_instances_is_406(client, accept):
    r = await _get(client, _COLLECTION, {"accept": accept})
    assert r.status_code == 406, r.text
    assert r.headers["content-type"].startswith(MEDIA)
    assert r.json()["errors"][0]["status"] == "406"


@pytest.mark.anyio
async def test_a_content_type_with_another_parameter_is_415(client):
    r = await _get(
        client, _COLLECTION, {"accept": MEDIA, "content-type": f"{MEDIA}; charset=utf-8"}
    )
    assert r.status_code == 415, r.text
    assert r.json()["errors"][0]["status"] == "415"


@pytest.mark.anyio
async def test_an_unknown_all_lowercase_parameter_is_400(client):
    r = await _get(client, _COLLECTION, {"accept": MEDIA}, {"unknownparam": "1"})
    assert r.status_code == 400, r.text
    assert r.json()["errors"][0]["source"] == {"parameter": "unknownparam"}


@pytest.mark.anyio
async def test_a_plain_read_is_served_and_states_jsonapi_version_1_1(client):
    r = await _get(client, _COLLECTION, {"accept": MEDIA}, {"role": "org_admin"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["jsonapi"] == {"version": "1.1"}
    assert [(d["type"], d["id"]) for d in body["data"]] == [("shelters", "1"), ("shelters", "2")]


@pytest.mark.anyio
async def test_a_lookup_id_that_is_not_a_key_value_is_404(client):
    r = await _get(client, f"{_COLLECTION}/no-such-resource-id-0", {"accept": MEDIA})
    assert r.status_code == 404, r.text
    assert r.json()["errors"][0]["status"] == "404"
