# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0c7a2b-91d4-4f3e-8a66-2b7d9c41e053
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1929: the shelter demo's animal_breeds -> pets include answers, never a 500.

The JSON:API Explorer's request ``GET /data/jsonapi/shelter/animal_breeds?include=pets`` with
``page[size]=25`` and ``page[total]=true`` answered 500 ``TypeError``. The tables and
relationships are the demo's (config/provisa.yaml): animal_breeds (a GraphQL source, no primary
key) is pointed at by pets.breed_name (BREED_INFO), so its id is its name; PETS_OF_BREED reaches
pets one-to-many, and pets declares no primary key, so each pet is ``<breed name>:<n>``.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from tests.role_state import with_surfaces

_EXPLORER_ROLES = (
    "analyst,data_product_owner,data_steward,developer,dq_reader,dq_steward,modeler,org_admin,"
    "org_operator,pet_analyst,sandbox,security_admin,source_admin,viewer,platform_admin"
)

_BREED_COLUMNS = ["name", "species", "care_level", "avg_lifespan_years"]
_PET_COLUMNS = ["id", "name", "species", "breed_name", "price", "available"]


def _col(name: str) -> ColumnMetadata:
    integer = name in ("id", "avg_lifespan_years")
    return ColumnMetadata(
        column_name=name, data_type="integer" if integer else "varchar", is_nullable=True
    )


def _table(table_id: int, source_id: str, schema: str, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": source_id,
        "domain_id": "shelter",
        "schema_name": schema,
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": ["pet_analyst"]} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _schema_input() -> SchemaInput:
    """The demo's pets and animal_breeds with BREED_INFO and PETS_OF_BREED, domain-prefixed."""
    return SchemaInput(
        tables=[
            _table(1, "pet-store-pg", "pet_store", "pets", _PET_COLUMNS),
            _table(2, "graphql-demo", "graphql", "animal_breeds", _BREED_COLUMNS),
        ],
        relationships=[
            {
                "id": "pets-to-shelter-breed",
                "alias": "BREED_INFO",
                "cardinality": "many-to-one",
                "source_table_id": 1,
                "source_column": "breed_name",
                "target_table_id": 2,
                "target_column": "name",
            },
            {
                "id": "shelter-breed-to-pets",
                "alias": "PETS_OF_BREED",
                "cardinality": "one-to-many",
                "source_table_id": 2,
                "source_column": "name",
                "target_table_id": 1,
                "target_column": "breed_name",
            },
        ],
        column_types={1: [_col(c) for c in _PET_COLUMNS], 2: [_col(c) for c in _BREED_COLUMNS]},
        naming_rules=[],
        domain_prefix=True,
        role={"id": "pet_analyst", "capabilities": ["full_results"], "domain_access": ["*"]},
        domains=[{"id": "shelter", "description": "Shelter"}],
    )


def _pet(pet_id: int, name: str, breed: str) -> dict:
    return {
        "id": pet_id,
        "name": name,
        "species": "dog",
        "breed_name": breed,
        "price": "100",
        "available": "true",
    }


_BREEDS = [
    {
        "name": "Beagle",
        "species": "dog",
        "care_level": "low",
        "avg_lifespan_years": 13,
        "pets": [_pet(1, "Rex", "Beagle"), _pet(2, "Max", "Beagle")],
    },
    {
        "name": "Poodle",
        "species": "dog",
        "care_level": "high",
        "avg_lifespan_years": 15,
        "pets": [_pet(3, "Fifi", "Poodle")],
    },
]


def _value(row: dict, column, root: dict[str, str], child: dict[str, str]) -> object:
    """The engine's answer for ``column`` of a physically keyed ``row``: a column is the value at
    its physical name; the pets relationship, built as one JSON value, is that list's JSON text
    keyed as the schema exposes each pet column."""
    if column.nested_in is not None:
        raise AssertionError(f"unexpected nested column {column}")
    if column.json:
        return json.dumps([{child[k]: v for k, v in pet.items()} for pet in row["pets"]])
    return row[root[column.field_name]]


def _batches(columns, rows: list[dict], root: dict[str, str], child: dict[str, str]) -> list:
    import pyarrow as pa

    return [
        pa.record_batch(
            {
                f"c{i}": pa.array([_value(r, c, root, child) for r in rows])
                for i, c in enumerate(columns)
            }
        )
    ]


@pytest.fixture()
def client(monkeypatch):
    """The JSON:API router over the demo's schema and context, the Rust encoder real.

    # unit: mock-justified — the engine is the external boundary; the router's id rule, include
    # plan, compiler and the Rust encoder, which are under test, run unmodified.
    """
    from fastapi import FastAPI

    from provisa.api.jsonapi.generator import _get_scalar_fields, create_jsonapi_router
    from provisa.api.jsonapi.naming import scalar_name_maps
    from provisa.auth.middleware import AuthMiddleware
    from provisa.executor import json_encoders

    _naming.configure()
    si = _schema_input()
    schema = generate_schema(si)
    ctx = build_context(si)

    real = json_encoders.jsonapi
    root, _ = scalar_name_maps(ctx, 2, _get_scalar_fields(schema, "s__animalBreeds"))
    _, child = scalar_name_maps(ctx, 1, _get_scalar_fields(schema, "s__pets"))

    def encode(_engine_batches, columns, *args, **kwargs):
        return real(_batches(columns, _BREEDS, root, child), columns, *args, **kwargs)

    monkeypatch.setattr(json_encoders, "jsonapi", encode)

    state = MagicMock()
    state.schemas = {"pet_analyst": schema}
    state.contexts = {"pet_analyst": ctx}
    state.rls_contexts = {"pet_analyst": RLSContext.empty()}
    state.roles = {"pet_analyst": si.role}
    state.table_path_maps = {
        "pet_analyst": {
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
    state.source_types = {"pet-store-pg": "postgresql", "graphql-demo": "graphql_remote"}
    state.source_dialects = {"pet-store-pg": "postgres", "graphql-demo": "postgres"}

    async def _fake_govern(
        sql,
        role_id,
        exec_params=None,
        state=None,
        deliver=None,
        buffered=False,
        *,
        cache_hint,
        serve_cached,
        sdl_joins,
    ):
        return SimpleNamespace(_is_count="COUNT(*)" in sql)

    async def _fake_execute(plan, state):
        result = MagicMock()
        result.redirect = None
        result.cache_entry = None
        result.rows = [[len(_BREEDS)]] if plan._is_count else [[]]
        return result

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(create_jsonapi_router(state))
    yield httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    _naming.configure()


async def _breeds_with_pets(client: httpx.AsyncClient, params: dict[str, str]) -> dict:
    async with client as c:
        r = await c.get(
            "/data/jsonapi/shelter/animal_breeds",
            params={"page[size]": "25", "include": "pets", **params},
            headers={"accept": "application/vnd.api+json", "X-Provisa-Role": "pet_analyst"},
        )
    assert r.status_code == 200, r.text
    return r.json()


def _assert_breeds_include_their_pets(body: dict) -> None:
    assert [d["id"] for d in body["data"]] == ["Beagle", "Poodle"]
    ids = ["Beagle:0", "Beagle:1", "Poodle:0"]
    assert [(i["type"], i["id"]) for i in body["included"]] == [("pets", i) for i in ids]
    assert [i["attributes"]["name"] for i in body["included"]] == ["Rex", "Max", "Fifi"]
    linkage = [[x["id"] for x in d["relationships"]["pets"]["data"]] for d in body["data"]]
    assert linkage == [["Beagle:0", "Beagle:1"], ["Poodle:0"]]


@pytest.mark.anyio
@pytest.mark.parametrize("total", [True, False], ids=["total", "no-total"])
@pytest.mark.parametrize("roles", [_EXPLORER_ROLES, "pet_analyst"], ids=["many-roles", "one-role"])
async def test_breeds_include_their_pets(client, total, roles):
    """The Explorer's exact request, with and without page[total], under one role and many."""
    params = {"role": roles}
    if total:
        params["page[total]"] = "true"
    body = await _breeds_with_pets(client, params)
    _assert_breeds_include_their_pets(body)
    if total:
        assert body["meta"]["total"] == 2
