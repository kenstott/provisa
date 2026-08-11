# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9c47ab-6d05-4e31-8a72-c1b3e05d9f48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1417: JSON:API names columns and relationships physically — params and emitted keys alike.

The schema here is built under the apollo (camelCase) convention, so ``user_id`` is exposed as
``userId`` and the two namings are distinguishable. Every assertion below is that the *physical*
spelling is the one the surface accepts and the one it emits, and that the exposed spelling is
rejected rather than quietly ignored.

``?include=``'s dotted form is covered in tests/unit/test_nl_jsonapi_include_roundtrip.py.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema

_HEADERS = {"accept": "application/vnd.api+json", "X-Provisa-Role": "org_admin"}

# One inquiries row joined to its pet — the shape serialize_rows produces, keyed as the *schema*
# exposes it, which is exactly what the handler has to translate back.
_GQL_ROW = {
    "id": 1,
    "userId": 7,
    "petId": 2,
    "pet": {"id": 2, "name": "Rex", "breedName": "Poodle"},
}


def _col(name: str, data_type: str = "varchar(100)") -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)


def _schema_input() -> SchemaInput:
    """inquiries -> pets, modeled on config/provisa-install.yaml's pet-store domain."""
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pet-store-sqlite",
                "domain_id": "pet-store",
                "schema_name": "main",
                "table_name": "inquiries",
                "enable_aggregates": True,
                "enable_group_by": True,
                "columns": [
                    {"column_name": "id", "visible_to": []},
                    {"column_name": "user_id", "visible_to": []},
                    {"column_name": "pet_id", "visible_to": []},
                ],
            },
            {
                "id": 2,
                "source_id": "pet-store-sqlite",
                "domain_id": "pet-store",
                "schema_name": "main",
                "table_name": "pets",
                "enable_aggregates": True,
                "enable_group_by": True,
                "columns": [
                    {"column_name": "id", "visible_to": []},
                    {"column_name": "name", "visible_to": []},
                    {"column_name": "breed_name", "visible_to": []},
                ],
            },
        ],
        relationships=[
            {
                "id": "inquiries-to-pets",
                "alias": "HAS_PETS",
                "cardinality": "many-to-one",
                "source_table_id": 1,
                "source_column": "pet_id",
                "target_table_id": 2,
                "target_column": "id",
            }
        ],
        column_types={
            1: [_col("id", "integer"), _col("user_id", "integer"), _col("pet_id", "integer")],
            2: [_col("id", "integer"), _col("name"), _col("breed_name")],
        },
        naming_rules=[],
        role={"id": "org_admin", "capabilities": ["full_results"], "domain_access": ["*"]},
        domains=[{"id": "pet-store", "description": "Pet store"}],
    )


@pytest.fixture()
def client(monkeypatch):
    """The JSON:API router over a real apollo-convention schema and context.

    # unit: mock-justified — the engine and the row serializer are the external boundaries; the
    # generator's own naming translation, which is what is under test, runs unmodified.
    """
    from fastapi import FastAPI

    from provisa.api.jsonapi.generator import create_jsonapi_router
    from provisa.auth.middleware import AuthMiddleware

    _naming.configure(gql="apollo_graphql", sql="snake")
    si = _schema_input()
    schema = generate_schema(si)
    ctx = build_context(si)
    gql_table = next(
        f for f, meta in ctx.tables.items() if meta.table_name == "inquiries"
    )

    state = MagicMock()
    state.schemas = {"org_admin": schema}
    state.contexts = {"org_admin": ctx}
    state.rls_contexts = {"org_admin": RLSContext.empty()}
    state.roles = {"org_admin": si.role}
    state.table_path_maps = {
        "org_admin": {gql_table: {"domain_id": "pet-store", "table_name": "inquiries"}}
    }
    state.masking_rules = {}
    state.source_types = {"pet-store-sqlite": "sqlite"}
    state.source_dialects = {"pet-store-sqlite": "sqlite"}

    async def _fake_govern(sql, role_id, exec_params=None, state=None, deliver=None, buffered=False):
        return SimpleNamespace(_is_count="COUNT(*)" in sql)

    async def _fake_execute(plan, state):
        result = MagicMock()
        result.redirect = None
        result.rows = [[1]] if plan._is_count else [[]]
        return result

    def _fake_serialize(rows, columns, table):
        # The engine result is stubbed, so the serialized shape is supplied directly — keyed as
        # the GraphQL schema exposes it, which is what the handler must translate back.
        return {"data": {table: [dict(_GQL_ROW)]}}

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)
    monkeypatch.setattr("provisa.executor.serialize.serialize_rows", _fake_serialize)

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(create_jsonapi_router(state))
    yield httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    _naming.configure()


def test_the_schema_renames_the_multi_word_column():
    """The premise: under apollo, ``user_id`` is exposed as ``userId``. Without this the rest of
    the file would pass on a schema that never renamed anything."""
    _naming.configure(gql="apollo_graphql", sql="snake")
    try:
        ctx = build_context(_schema_input())
        assert ctx.exposed_to_physical[(1, "userId")] == "user_id"
    finally:
        _naming.configure()


@pytest.mark.anyio
async def test_sparse_fieldset_takes_the_physical_column(client):
    async with client as c:
        r = await c.get(
            "/data/jsonapi/pet-store/inquiries",
            params={"fields[inquiries]": "user_id"},
            headers=_HEADERS,
        )
    assert r.status_code == 200, r.text


@pytest.mark.anyio
async def test_sparse_fieldset_rejects_the_exposed_column(client):
    async with client as c:
        r = await c.get(
            "/data/jsonapi/pet-store/inquiries",
            params={"fields[inquiries]": "userId"},
            headers=_HEADERS,
        )
    assert r.status_code == 400
    err = r.json()["errors"][0]
    assert err["detail"] == "Unknown field 'userId'"
    assert err["source"]["parameter"] == "fields[inquiries]"


@pytest.mark.anyio
async def test_filter_takes_the_physical_column(client):
    async with client as c:
        r = await c.get(
            "/data/jsonapi/pet-store/inquiries",
            params={"filter[user_id]": "7"},
            headers=_HEADERS,
        )
    assert r.status_code == 200, r.text


@pytest.mark.anyio
async def test_filter_rejects_the_exposed_column(client):
    """A filter naming a column the surface does not expose is a 400 — never a dropped predicate,
    which would silently widen the result set."""
    async with client as c:
        r = await c.get(
            "/data/jsonapi/pet-store/inquiries",
            params={"filter[userId]": "7"},
            headers=_HEADERS,
        )
    assert r.status_code == 400
    assert "userId" in r.json()["errors"][0]["detail"]


@pytest.mark.anyio
async def test_sort_takes_the_physical_column(client):
    async with client as c:
        r = await c.get(
            "/data/jsonapi/pet-store/inquiries",
            params={"sort": "-user_id"},
            headers=_HEADERS,
        )
    assert r.status_code == 200, r.text


@pytest.mark.anyio
async def test_sort_rejects_the_exposed_column(client):
    async with client as c:
        r = await c.get(
            "/data/jsonapi/pet-store/inquiries",
            params={"sort": "-userId"},
            headers=_HEADERS,
        )
    assert r.status_code == 400
    assert "userId" in r.json()["errors"][0]["detail"]


@pytest.mark.anyio
async def test_attributes_come_back_physically_named(client):
    async with client as c:
        r = await c.get("/data/jsonapi/pet-store/inquiries", headers=_HEADERS)
    assert r.status_code == 200, r.text
    attrs = r.json()["data"][0]["attributes"]
    assert "user_id" in attrs
    assert "userId" not in attrs


@pytest.mark.anyio
async def test_included_resources_come_back_physically_named(client):
    """The sideloaded table's columns go through the *related* table's map, not the base table's."""
    async with client as c:
        r = await c.get(
            "/data/jsonapi/pet-store/inquiries",
            params={"include": "pet"},
            headers=_HEADERS,
        )
    assert r.status_code == 200, r.text
    body = r.json()
    included = body["included"][0]["attributes"]
    assert included["breed_name"] == "Poodle"
    assert "breedName" not in included
    # the relationship linkage resolves off the physically named FK column
    assert body["data"][0]["relationships"]["pet"]["data"]["id"] == "2"


@pytest.mark.anyio
async def test_include_rejects_the_exposed_relationship_name(client):
    async with client as c:
        r = await c.get(
            "/data/jsonapi/pet-store/inquiries",
            params={"include": "petDetails"},
            headers=_HEADERS,
        )
    assert r.status_code == 400
    err = r.json()["errors"][0]
    assert err["detail"] == "Unknown relationship 'petDetails'"
    assert err["source"]["parameter"] == "include"
