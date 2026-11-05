# Copyright (c) 2026 Kenneth Stott
# Canary: 79e5517c-7a07-4675-b2c9-f09ac5d6d7ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REST and JSON:API read a parameterized table (REQ-1986, #242).

A table's parameters are given as ``?params=`` — a JSON object by parameter name — and sent with
the request's filter as GraphQL variables typed by the field's arguments, so no value is written
into the synthesized text. The real routers, schema, compiler and plan store run; only the
pipeline's two entry points are replaced by a recorder of the statements they are handed.
"""

# Requirements: REQ-1986, REQ-222, REQ-256

from __future__ import annotations

from provisa.executor.result import QueryResult

import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Request

from provisa.api.jsonapi import generator as jsonapi_generator
from provisa.api.rest import generator as rest_generator
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.pgwire import governed_plan
from tests.role_state import RoleState

pytestmark = [pytest.mark.anyio]

_ROLE = "admin"
_SURFACES = {
    "rest": ("/data/rest/sales", {"X-Provisa-Role": _ROLE}),
    "jsonapi": (
        "/data/jsonapi/sales",
        {"accept": "application/vnd.api+json", "X-Provisa-Role": _ROLE},
    ),
}


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _param(name: str, kind: str, *, required: bool) -> dict:
    return {
        "column_name": f"_nf_{name}",
        "visible_to": [],
        "native_filter_type": kind,
        "native_filter_required": required,
    }


def _table(table_id: int, name: str, columns: list[dict]) -> dict:
    return {
        "id": table_id,
        "source_id": "orders-api",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": name,
        "columns": columns,
        "enable_aggregates": True,
        "enable_group_by": True,
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _schema():
    data = [{"column_name": c, "visible_to": [_ROLE]} for c in ("id", "customer", "total")]
    tables = [
        # A table whose source requires region and takes an optional status.
        _table(
            1,
            "orders",
            [
                *data,
                _param("region", "query_param", required=True),
                _param("status", "query_param", required=False),
                # The same name as a data column: its argument is _customer (#247).
                _param("customer", "query_param", required=False),
            ],
        ),
        # A query table: its query's argument is a parameter.
        _table(2, "big_orders", [*data, _param("min", "query_arg", required=True)]),
    ]
    types = [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="customer", data_type="varchar", is_nullable=True),
        ColumnMetadata(column_name="total", data_type="double", is_nullable=True),
    ]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types={
            1: [
                *types,
                ColumnMetadata(column_name="_nf_region", data_type="varchar", is_nullable=True),
                ColumnMetadata(column_name="_nf_status", data_type="varchar", is_nullable=True),
                ColumnMetadata(column_name="_nf_customer", data_type="varchar", is_nullable=True),
            ],
            2: [
                *types,
                ColumnMetadata(column_name="_nf_min", data_type="double", is_nullable=True),
            ],
        },
        naming_rules=[],
        role={"id": _ROLE, "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si), build_context(si)


class _Pipeline:
    """Records every statement the surface hands the pipeline and answers it with no rows."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, list]] = []

    async def govern(self, sql, role_id, *, exec_params=None, state=None, **_kwargs):
        self.statements.append((sql, list(exec_params or [])))
        return SimpleNamespace(sql=sql, params=list(exec_params or []))

    async def execute(self, plan, state):
        if "COUNT(*)" in plan.sql and "GROUP BY" not in plan.sql:
            return QueryResult(rows=[(0,)], column_names=["total"])
        return QueryResult(rows=[], column_names=[])


@pytest.fixture
def surface(monkeypatch):
    schema, ctx = _schema()
    path = {"domain_id": "sales", "write_ops": [], "write_returns_rows": False}
    state = RoleState(
        schemas={_ROLE: schema},
        contexts={_ROLE: ctx},
        rls_contexts={_ROLE: RLSContext.empty()},
        roles={_ROLE: {"id": _ROLE, "capabilities": ["query_development"], "domain_access": ["*"]}},
        masking_rules={},
        tables=[],
        schema_boot_id="boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        approval_hook=None,
        kafka_table_configs={},
        source_types={"orders-api": "openapi"},
        table_path_maps={
            _ROLE: {
                "orders": {**path, "table_name": "orders", "write_refused_forms": []},
                "bigOrders": {**path, "table_name": "big_orders", "write_refused_forms": []},
            }
        },
    )
    pipeline = _Pipeline()
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", pipeline.govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", pipeline.execute)

    app = FastAPI()

    @app.middleware("http")
    async def _role(request: Request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.role = request.headers.get("x-provisa-role")
        return await call_next(request)

    app.include_router(rest_generator.create_rest_router(state))
    app.include_router(jsonapi_generator.create_jsonapi_router(state))
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    return SimpleNamespace(pipeline=pipeline, client=client)


async def _get(surface, kind: str, table: str, query: dict[str, str]) -> httpx.Response:
    base, headers = _SURFACES[kind]
    return await surface.client.get(f"{base}/{table}", params=query, headers=headers)


def _filter(kind: str, column: str, value: str) -> dict[str, str]:
    if kind == "rest":
        return {"filter": json.dumps([{"field": column, "comparator": "eq", "value": value}])}
    return {f"filter[{column}]": value}


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_required_parameter_is_read_from_params(surface, kind):
    resp = await _get(surface, kind, "orders", {"params": json.dumps({"region": "EU"})})
    assert resp.status_code == 200, resp.text
    (sql, _params) = surface.pipeline.statements[0]
    assert "\"_nf_region\" = 'EU'" in sql


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_read_without_a_required_parameter_is_refused_naming_it(surface, kind):
    resp = await _get(surface, kind, "orders", {})
    assert resp.status_code == 400
    assert "region" in resp.text
    assert not surface.pipeline.statements


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_an_optional_parameter_is_sent_when_given_and_left_out_when_not(surface, kind):
    given = {"params": json.dumps({"region": "EU", "status": "open"})}
    assert (await _get(surface, kind, "orders", given)).status_code == 200
    left_out = {"params": json.dumps({"region": "US"})}
    assert (await _get(surface, kind, "orders", left_out)).status_code == 200
    (with_status, _), (without_status, _) = surface.pipeline.statements
    assert "\"_nf_status\" = 'open'" in with_status
    assert "_nf_status" not in without_status and "\"_nf_region\" = 'US'" in without_status


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_parameter_is_named_as_the_table_names_it_whatever_its_argument(surface, kind):
    """The parameter customer shares its name with a column, so GraphQL offers it as _customer;
    a caller still names it customer, and its value reaches _nf_customer."""
    query = {"params": json.dumps({"region": "EU", "customer": "acme"})}
    resp = await _get(surface, kind, "orders", query)
    assert resp.status_code == 200, resp.text
    (sql, _params) = surface.pipeline.statements[0]
    assert "\"_nf_customer\" = 'acme'" in sql


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_query_tables_argument_is_a_parameter_of_its_own_type(surface, kind):
    resp = await _get(surface, kind, "big_orders", {"params": json.dumps({"min": 100})})
    assert resp.status_code == 200, resp.text
    (sql, _params) = surface.pipeline.statements[0]
    assert '"_nf_min" = 100' in sql


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_parameter_value_its_type_refuses_is_refused(surface, kind):
    resp = await _get(surface, kind, "big_orders", {"params": json.dumps({"min": "lots"})})
    assert resp.status_code == 400
    assert "min" in resp.text
    assert not surface.pipeline.statements


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_parameter_the_table_does_not_take_is_refused_naming_it(surface, kind):
    query = {"params": json.dumps({"region": "EU", "colour": "red"})}
    resp = await _get(surface, kind, "orders", query)
    assert resp.status_code == 400
    assert "colour" in resp.text
    assert not surface.pipeline.statements


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_filter_value_containing_a_quote_is_bound_as_given(surface, kind):
    query = {"params": json.dumps({"region": "EU"}), **_filter(kind, "customer", 'O"Brien')}
    resp = await _get(surface, kind, "orders", query)
    assert resp.status_code == 200, resp.text
    (sql, params) = surface.pipeline.statements[0]
    assert 'O"Brien' in params
    assert "Brien" not in sql


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_numeric_filter_value_reads_as_its_column_type(surface, kind):
    query = {"params": json.dumps({"region": "EU"}), **_filter(kind, "total", "12.5")}
    resp = await _get(surface, kind, "orders", query)
    assert resp.status_code == 200, resp.text
    assert surface.pipeline.statements[0][1][0] == 12.5


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_an_aggregate_of_a_parameterized_table_reads_its_parameters(surface, kind):
    """REQ-1991: the aggregate field takes the table's parameters, as its base field does."""
    query = {"aggregate": "count", "params": json.dumps({"region": "EU"})}
    resp = await _get(surface, kind, "orders", query)
    assert resp.status_code == 200, resp.text
    (sql, _params) = surface.pipeline.statements[0]
    assert "COUNT(*)" in sql and "\"_nf_region\" = 'EU'" in sql


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_a_group_by_of_a_parameterized_table_reads_its_parameters(surface, kind):
    query = {"aggregate": "count", "groupBy": "customer", "params": json.dumps({"region": "EU"})}
    resp = await _get(surface, kind, "orders", query)
    assert resp.status_code == 200, resp.text
    (sql, _params) = surface.pipeline.statements[0]
    assert "GROUP BY" in sql and "\"_nf_region\" = 'EU'" in sql


@pytest.mark.parametrize("kind", list(_SURFACES))
async def test_params_that_are_not_a_json_object_are_refused(surface, kind):
    resp = await _get(surface, kind, "orders", {"params": "region=EU"})
    assert resp.status_code == 400
    assert "params" in resp.text


def test_the_rest_spec_offers_a_tables_parameters_as_params():
    from provisa.api.rest.openapi_spec import generate_rest_openapi_spec

    from tests.role_state import RoleState

    schema, ctx = _schema()
    state = RoleState(
        schemas={_ROLE: schema},
        table_path_maps={_ROLE: {"orders": {"domain_id": "sales", "table_name": "orders"}}},
    )
    spec = generate_rest_openapi_spec(state, _ROLE)
    (operation,) = [op["get"] for path, op in spec["paths"].items() if path.endswith("/orders")]
    (params,) = [p for p in operation["parameters"] if p["name"] == "params"]
    assert params["required"] is True
    body = params["content"]["application/json"]["schema"]
    assert body["required"] == ["region"]
    assert set(body["properties"]) == {"region", "status", "customer"}
