# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3b9a51-2c84-4f6d-9a10-5b8e2d4c6f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REST reads a row's relationships nested, as the GraphQL it is derived from does (REQ-1929,
amended 2026-11-16).

``?fields=`` takes dot-paths through relationships (``customer.name``), the form ``?includeNodes=``
already takes. The read becomes the GraphQL that nests the relationship, so it compiles to the same
semantic SQL as that GraphQL and returns the related row nested: a many-to-one as an object, a
one-to-many as a list. A path the role cannot see is refused, naming the path.
"""

# Requirements: REQ-1929, REQ-222

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request

from provisa.api.rest import generator as rest_generator
from provisa.api.rest.generator import rest_read_graphql
from provisa.api.rest.openapi_spec import generate_rest_openapi_spec
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.executor.result import QueryResult
from provisa.pgwire import governed_plan
from tests.role_state import RoleState

pytestmark = [pytest.mark.anyio]

_ROLE = "admin"


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _col(name: str, data_type: str) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=False)


def _table(table_id: int, name: str, columns: dict[str, list[str]]) -> dict:
    return {
        "id": table_id,
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": roles} for c, roles in columns.items()],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _schema():
    """orders -> customer (many-to-one) and customers -> orders (one-to-many); a customer's
    ``secret`` is visible to another role only."""
    tables = [
        _table(
            1,
            "orders",
            {"id": [_ROLE], "customer_id": [_ROLE], "amount": [_ROLE], "region": [_ROLE]},
        ),
        _table(2, "customers", {"id": [_ROLE], "name": [_ROLE], "secret": ["auditor"]}),
    ]
    relationships = [
        {
            "id": "ord-cust",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
        {
            "id": "cust-ord",
            "source_table_id": 2,
            "target_table_id": 1,
            "source_column": "id",
            "target_column": "customer_id",
            "cardinality": "one-to-many",
            "alias": "orders",
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
        ],
        2: [_col("id", "integer"), _col("name", "varchar(100)"), _col("secret", "varchar(100)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": _ROLE, "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    return generate_schema(si), build_context(si)


_PATH_MAP = {
    "orders": {"domain_id": "sales", "table_name": "orders"},
    "customers": {"domain_id": "sales", "table_name": "customers"},
}


def _surface():
    schema, ctx = _schema()
    return SimpleNamespace(schema=schema, context=ctx, table_path_map=_PATH_MAP)


def _read(surface, table: str, fields: str):
    return rest_read_graphql(
        surface,
        "sales",
        table,
        {"fields": fields},
        limit=None,
        offset=None,
        fields=fields,
        as_of=None,
        as_recorded=None,
    )


def _sql(surface, gql: str, variables: dict | None = None) -> list[str]:
    doc = parse_query(surface.schema, gql, variables or {}, ctx=surface.context)
    return [c.sql for c in compile_query(doc, surface.context, variables or {})]


# -- the GraphQL a dot-path read becomes -----------------------------------------------------------


def test_a_many_to_one_path_compiles_as_the_graphql_that_nests_it():
    surface = _surface()
    read = _read(surface, "orders", "id,customer.name")
    assert _sql(surface, read.gql_query, read.variables.values) == _sql(
        surface, "{ orders { id customer { name } } }"
    )


def test_a_one_to_many_path_compiles_as_the_graphql_that_nests_it():
    surface = _surface()
    read = _read(surface, "customers", "name,orders.amount,orders.region")
    assert _sql(surface, read.gql_query, read.variables.values) == _sql(
        surface, "{ customers { name orders { amount region } } }"
    )


def test_a_path_back_through_two_relationships_nests_twice():
    surface = _surface()
    read = _read(surface, "customers", "id,orders.customer.name")
    assert _sql(surface, read.gql_query, read.variables.values) == _sql(
        surface, "{ customers { id orders { customer { name } } } }"
    )


def test_a_relationship_named_alone_reads_its_fields():
    surface = _surface()
    read = _read(surface, "orders", "id,customer")
    assert _sql(surface, read.gql_query, read.variables.values) == _sql(
        surface, "{ orders { id customer { id name } } }"
    )


def test_a_physical_field_name_reads_as_its_graphql_field():
    surface = _surface()
    read = _read(surface, "orders", "id,customer_id")
    assert _sql(surface, read.gql_query, read.variables.values) == _sql(
        surface, "{ orders { id customerId } }"
    )


# -- refused by name -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("table", "fields", "path"),
    [
        ("orders", "id,customer.secret", "customer.secret"),
        ("orders", "id,buyer.name", "buyer.name"),
        ("orders", "id,amount.value", "amount.value"),
        ("orders", "id,nope", "nope"),
    ],
)
def test_a_path_the_role_cannot_see_is_refused_naming_it(table, fields, path):
    with pytest.raises(HTTPException) as exc:
        _read(_surface(), table, fields)
    assert exc.value.status_code == 400
    assert repr(path) in str(exc.value.detail)


# -- the response: the related row nested ----------------------------------------------------------


class _Pipeline:
    """Answers the one statement a read sends with ``rows``, recording it."""

    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows
        self.sql: list[str] = []

    async def govern(self, sql, role_id, *, exec_params=None, state=None, **_kwargs):
        self.sql.append(sql)
        return SimpleNamespace(sql=sql, params=list(exec_params or []))

    async def execute(self, plan, state):
        width = len(self.rows[0])
        return QueryResult(rows=self.rows, column_names=[f"c{i}" for i in range(width)])


def _client(monkeypatch, rows: list[tuple]):
    schema, ctx = _schema()
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
        source_types={"sales-pg": "postgresql"},
        table_path_maps={_ROLE: _PATH_MAP},
    )
    pipeline = _Pipeline(rows)
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", pipeline.govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", pipeline.execute)
    app = FastAPI()

    @app.middleware("http")
    async def _role(request: Request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.role = _ROLE
        return await call_next(request)

    app.include_router(rest_generator.create_rest_router(state))
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    return client, pipeline


async def test_a_many_to_one_is_nested_as_an_object(monkeypatch):
    client, pipeline = _client(monkeypatch, [(1, json.dumps({"name": "Ann"}))])
    resp = await client.get("/data/rest/sales/orders", params={"fields": "id,customer.name"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"] == [{"id": 1, "customer": {"name": "Ann"}}]
    assert pipeline.sql == _sql(_surface(), "{ orders { id customer { name } } }")


async def test_a_one_to_many_is_nested_as_a_list(monkeypatch):
    client, _ = _client(monkeypatch, [("Ann", json.dumps([{"region": "EU"}, {"region": "US"}]))])
    resp = await client.get("/data/rest/sales/customers", params={"fields": "name,orders.region"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"] == [{"name": "Ann", "orders": [{"region": "EU"}, {"region": "US"}]}]


async def test_a_hidden_path_is_answered_400_naming_it(monkeypatch):
    client, pipeline = _client(monkeypatch, [(1,)])
    resp = await client.get("/data/rest/sales/orders", params={"fields": "id,customer.secret"})
    assert resp.status_code == 400
    assert "'customer.secret'" in resp.text
    assert pipeline.sql == []


# -- the OpenAPI document --------------------------------------------------------------------------


def _fields_param(spec: dict, path: str) -> dict:
    params = spec["paths"][path]["get"]["parameters"]
    return next(p for p in params if p["name"] == "fields")


def test_the_openapi_fields_parameter_takes_dot_paths_through_relationships():
    schema, ctx = _schema()
    state = RoleState(
        schemas={_ROLE: schema}, contexts={_ROLE: ctx}, table_path_maps={_ROLE: _PATH_MAP}
    )
    spec = generate_rest_openapi_spec(state, _ROLE)
    param = _fields_param(spec, "/sales/orders")
    items = param["schema"]["items"]
    # A field of the row, or a dot-path through one of its relationships.
    assert {"$ref": "#/components/schemas/OrdersField"} in items["anyOf"]
    path_form = next(s for s in items["anyOf"] if "pattern" in s)
    assert path_form["type"] == "string"
    assert "customer.id" in path_form["examples"]
    assert "relationship" in param["description"]
    row = spec["components"]["schemas"]["Orders"]["properties"]
    assert row["customer"]["type"] == "object"
    customers = spec["components"]["schemas"]["Customers"]["properties"]
    assert customers["orders"]["type"] == "array"
