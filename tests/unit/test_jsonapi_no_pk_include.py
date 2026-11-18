# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f0e2a-93c4-4d57-b8a1-2f7e5c9d0a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1929 (amended 2026-11-18): a JSON:API row read needs no primary key.

No read or include is refused because a table declares no primary key. An include follows the
approved relationship, at any depth (``a.b``), and an included resource's id is the value of the
column that relationship points at. A root resource's id is its primary key where declared;
otherwise the column an approved relationship points at; otherwise its position in the response.
``fields[<type>]`` narrows an included type's attributes.

The schema is built under the apollo convention, so every multi-word name has a GraphQL spelling
distinct from its physical one, and none of the three tables declares a primary key.
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

_HEADERS = {"accept": "application/vnd.api+json", "X-Provisa-Role": "org_admin"}

_DISTRICT = {"districtCode": "D1", "districtName": "Downtown"}
_NEIGHBORHOOD = {"neighborhoodName": "Mission", "districtCode": "D1", "population": 60000}


def _permits(neighborhood: dict) -> list[dict]:
    """Two building_permits rows, both in ``neighborhood``, keyed as the schema exposes them."""
    return [
        {
            "permitNumber": number,
            "neighborhood": "Mission",
            "valuation": valuation,
            "permitNeighborhoodDimension": neighborhood,
        }
        for number, valuation in (("P-1", 100), ("P-2", 200))
    ]


def _col(name: str, data_type: str = "varchar(100)") -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=True)


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "shelter-sqlite",
        "domain_id": "shelter",
        "schema_name": "main",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": ["org_admin"]} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _schema_input() -> SchemaInput:
    """building_permits -> neighborhoods (permit_neighborhood_dimension) -> districts; no table
    declares a primary key."""
    return SchemaInput(
        tables=[
            _table(1, "building_permits", ["permit_number", "neighborhood", "valuation"]),
            _table(2, "neighborhoods", ["neighborhood_name", "district_code", "population"]),
            _table(3, "districts", ["district_code", "district_name"]),
        ],
        relationships=[
            {
                "id": "permit-neighborhood",
                "cardinality": "many-to-one",
                "source_table_id": 1,
                "source_column": "neighborhood",
                "target_table_id": 2,
                "target_column": "neighborhood_name",
                "graphql_alias": "permitNeighborhoodDimension",
            },
            {
                "id": "neighborhood-district",
                "cardinality": "many-to-one",
                "source_table_id": 2,
                "source_column": "district_code",
                "target_table_id": 3,
                "target_column": "district_code",
            },
        ],
        column_types={
            1: [_col("permit_number"), _col("neighborhood"), _col("valuation", "integer")],
            2: [_col("neighborhood_name"), _col("district_code"), _col("population", "integer")],
            3: [_col("district_code"), _col("district_name")],
        },
        naming_rules=[],
        role={"id": "org_admin", "capabilities": ["full_results"], "domain_access": ["*"]},
        domains=[{"id": "shelter", "description": "Shelter"}],
    )


def _value(row: dict, column) -> object:
    """The value the engine answers for ``column`` of ``row``: a relationship the statement
    builds as one JSON value is that object's JSON text; a column is the value at its field."""
    holder = row if column.nested_in is None else (row.get(column.nested_in) or {})
    v = holder.get(column.field_name)
    return json.dumps(v) if column.json and v is not None and not isinstance(v, str) else v


def _batches(columns, rows: list[dict]) -> list:
    import pyarrow as pa

    return [
        pa.record_batch(
            {f"c{i}": pa.array([_value(r, c) for r in rows]) for i, c in enumerate(columns)}
        )
    ]


@pytest.fixture()
def answer(monkeypatch):
    """Set the rows the engine answers each table read with (the Rust encoder is real)."""
    from provisa.executor import json_encoders

    real = json_encoders.jsonapi
    rows: dict[str, list[dict]] = {"rows": _permits(_NEIGHBORHOOD)}

    def encode(batches, columns, *args, **kwargs):
        return real(_batches(columns, rows["rows"]), columns, *args, **kwargs)

    monkeypatch.setattr(json_encoders, "jsonapi", encode)
    return rows


@pytest.fixture()
def client(monkeypatch, answer):
    """The JSON:API router over a real apollo-convention schema and context.

    # unit: mock-justified — the engine is the external boundary; the router's id rule, include
    # plan and the Rust encoder, which are under test, run unmodified.
    """
    from fastapi import FastAPI

    from provisa.api.jsonapi.generator import create_jsonapi_router
    from provisa.auth.middleware import AuthMiddleware

    _naming.configure(gql="apollo_graphql", sql="snake")
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
            if gql in ("buildingPermits", "neighborhoods")
        }
    }
    with_surfaces(state)
    state.masking_rules = {}
    state.source_types = {"shelter-sqlite": "sqlite"}
    state.source_dialects = {"shelter-sqlite": "sqlite"}

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
        result.rows = [[len(answer["rows"])]] if plan._is_count else [[]]
        return result

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(create_jsonapi_router(state))
    yield httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    _naming.configure()


async def _get(client, path: str, params: dict) -> httpx.Response:
    async with client as c:
        return await c.get(path, params=params, headers=_HEADERS)


@pytest.mark.anyio
async def test_the_screenshot_request_returns_data_with_included_neighborhoods(client):
    """The maintainer's request: a table with no primary key, included by an approved
    relationship, with a counted total. No refusal; the neighborhood is included once, its id the
    column the relationship points at, and each permit links to it."""
    r = await _get(
        client,
        "/data/jsonapi/shelter/building_permits",
        {
            "role": "org_admin",
            "page[size]": "25",
            "page[total]": "true",
            "include": "permit_neighborhood_dimension",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # no relationship points at building_permits: each id is its position in the response
    assert [d["id"] for d in body["data"]] == ["0", "1"]
    assert body["data"][0]["attributes"]["permit_number"] == "P-1"
    assert [(i["type"], i["id"]) for i in body["included"]] == [
        ("permit_neighborhood_dimension", "Mission")
    ]
    assert body["included"][0]["attributes"] == {
        "neighborhood_name": "Mission",
        "district_code": "D1",
        "population": 60000,
    }
    linkage = {"data": {"type": "permit_neighborhood_dimension", "id": "Mission"}}
    assert [d["relationships"]["permit_neighborhood_dimension"] for d in body["data"]] == [
        linkage,
        linkage,
    ]
    assert body["meta"]["total"] == 2


@pytest.mark.anyio
async def test_a_root_pointed_at_by_a_relationship_is_identified_by_that_column(client, answer):
    """neighborhoods has no primary key, but permit_neighborhood_dimension points at its
    neighborhood_name: that is its resources' id."""
    answer["rows"] = [dict(_NEIGHBORHOOD)]
    r = await _get(client, "/data/jsonapi/shelter/neighborhoods", {})
    assert r.status_code == 200, r.text
    assert r.json()["data"][0]["id"] == "Mission"


@pytest.mark.anyio
async def test_fields_of_an_included_type_narrow_its_attributes(client):
    r = await _get(
        client,
        "/data/jsonapi/shelter/building_permits",
        {
            "include": "permit_neighborhood_dimension",
            "fields[permit_neighborhood_dimension]": "population",
        },
    )
    assert r.status_code == 200, r.text
    included = r.json()["included"][0]
    assert included["id"] == "Mission"
    assert included["attributes"] == {"population": 60000}


@pytest.mark.anyio
async def test_fields_of_an_included_type_rejects_an_unknown_column(client):
    r = await _get(
        client,
        "/data/jsonapi/shelter/building_permits",
        {
            "include": "permit_neighborhood_dimension",
            "fields[permit_neighborhood_dimension]": "districtCode",
        },
    )
    assert r.status_code == 400
    err = r.json()["errors"][0]
    assert err["detail"] == "Unknown field 'districtCode'"
    assert err["source"]["parameter"] == "fields[permit_neighborhood_dimension]"


@pytest.mark.anyio
async def test_a_two_hop_dotted_include_includes_every_resource_along_it(client, answer):
    """``a.b`` includes the a resources and the b resources reached from them; an a resource
    links to its b, and both are physically named and narrowed by their own fields[]."""
    answer["rows"] = _permits({**_NEIGHBORHOOD, "district": _DISTRICT})
    r = await _get(
        client,
        "/data/jsonapi/shelter/building_permits",
        {
            "include": "permit_neighborhood_dimension.district",
            "fields[district]": "district_name",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert [(i["type"], i["id"]) for i in body["included"]] == [
        ("permit_neighborhood_dimension", "Mission"),
        ("district", "D1"),
    ]
    neighborhood, district = body["included"]
    assert "district" not in neighborhood["attributes"]
    assert neighborhood["relationships"]["district"] == {"data": {"type": "district", "id": "D1"}}
    assert district["attributes"] == {"district_name": "Downtown"}
    assert body["data"][0]["relationships"]["permit_neighborhood_dimension"] == {
        "data": {"type": "permit_neighborhood_dimension", "id": "Mission"}
    }


@pytest.mark.anyio
async def test_a_dotted_include_naming_no_relationship_is_refused_by_name(client):
    r = await _get(
        client,
        "/data/jsonapi/shelter/building_permits",
        {"include": "permit_neighborhood_dimension.nope"},
    )
    assert r.status_code == 400
    err = r.json()["errors"][0]
    assert err["detail"] == "Unknown relationship 'nope'"
    assert err["source"]["parameter"] == "include"
