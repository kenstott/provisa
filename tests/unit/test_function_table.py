# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7b9e41-8c3a-4f15-b6d0-91e5a7c3f248
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A parameterized table is an attribute of its function (REQ-1986).

A source operation that reads rows and requires a parameter is a function. Its table attribute
is on at registration unless an operation of the source without a required parameter returns the
same type; on, the function returns the table derived from it; off, the plain list's rows.
Turning it off is refused while anything refers to the derived table, naming it. Two plain lists
of one type both stay tables.
"""

# Requirements: REQ-1986

from __future__ import annotations

import types
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.errors import ApiError
from provisa.core.database import Database
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    rls_rules,
    roles,
    sources,
    tracked_functions,
)
from tests.unit.gate_identity import serve_every_column


def _rows(ref: str) -> dict:
    return {
        "200": {
            "description": "ok",
            "content": {
                "application/json": {
                    "schema": {"type": "array", "items": {"$ref": f"#/components/schemas/{ref}"}}
                }
            },
        }
    }


def _one(ref: str) -> dict:
    return {
        "200": {
            "description": "ok",
            "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{ref}"}}},
        }
    }


def _by(name: str, kind: str = "integer") -> list[dict]:
    return [{"name": name, "in": "path", "required": True, "schema": {"type": kind}}]


_OBJECT = {"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}}
# Pets: two plain lists (by status, by tags) and a lookup by id, all of Pet. Orders: a lookup
# by id only. Owners: a lookup whose schema is written in place, naming no type.
_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Store", "version": "1.0.0"},
    "components": {"schemas": {"Pet": _OBJECT, "Order": _OBJECT}},
    "paths": {
        "/pets/findByStatus": {
            "get": {
                "operationId": "findPetsByStatus",
                "parameters": [{"name": "status", "in": "query", "schema": {"type": "string"}}],
                "responses": _rows("Pet"),
            }
        },
        "/pets/findByTags": {
            "get": {
                "operationId": "findPetsByTags",
                "parameters": [{"name": "tags", "in": "query", "schema": {"type": "string"}}],
                "responses": _rows("Pet"),
            }
        },
        "/pets/{petId}": {
            "get": {
                "operationId": "getPetById",
                "parameters": _by("petId"),
                "responses": _one("Pet"),
            }
        },
        "/orders/{orderId}": {
            "get": {
                "operationId": "getOrderById",
                "parameters": _by("orderId"),
                "responses": _one("Order"),
            }
        },
        "/owners/{ownerId}": {
            "get": {
                "operationId": "getOwner",
                "parameters": _by("ownerId", "string"),
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {"application/json": {"schema": _OBJECT}},
                    }
                },
            }
        },
    },
}


class _State:
    def __init__(self, model_db=None) -> None:
        self.source_types = {"api": "openapi"}
        self.openapi_specs = {"api": {"spec": dict(_SPEC)}}
        self.tables: list[dict] = []
        self.model_db = model_db


async def _operation(state, name: str):
    from provisa.executor.source_operation import offered_operation

    return await offered_operation(state, "api", name)


# --- the default at registration -----------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation, exposed",
    [
        ("getPetById", False),  # findPetsByStatus and findPetsByTags return Pet
        ("getOrderById", True),  # no plain list returns Order
        ("getOwner", True),  # its type is not named, so nothing returns the same type
    ],
)
async def test_registration_sets_the_table_attribute_by_whether_a_plain_list_returns_its_type(
    operation, exposed
):
    from provisa.api.admin._function_table import default_exposed

    state = _State()
    op = await _operation(state, operation)
    assert await default_exposed(state, "api", op.returns_type) is exposed


@pytest.mark.asyncio
async def test_two_plain_lists_of_one_type_both_stay_tables():
    from provisa.executor.source_operation import offered_operations, plain_lists

    state = _State()
    lists = await plain_lists(state, "api")
    assert [(p.table, p.returns_type) for p in lists] == [
        ("findPetsByStatus", "Pet"),
        ("findPetsByTags", "Pet"),
    ]
    functions = {op.name for op in await offered_operations(state, "api") or [] if op.reads}
    assert "findPetsByStatus" not in functions and "findPetsByTags" not in functions
    assert {"getPetById", "getOrderById", "getOwner"} <= functions


# --- a control plane ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="api", type="openapi"))
        await conn.execute_core(insert(domains).values(id="pets"))
        await conn.execute_core(
            insert(roles).values(id="reader", capabilities=[], domain_access=["pets"])
        )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    return db


async def _table(db: Database, schema: str, name: str) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="api", domain_id="pets", schema_name=schema, table_name=name
            )
        )
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def _function(db: Database, name: str, returns: str, exposed: bool) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(tracked_functions).values(
                name=name,
                source_id="api",
                schema_name="openapi",
                function_name=name,
                returns=returns,
                arguments=[],
                visible_to=[],
                domain_id="pets",
                kind="query",
                impl_kind="source_operation",
                exposed_as_table=exposed,
            )
        )


def _request() -> Any:
    """An admin request as the auth middleware leaves it."""
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=None, role="admin"))


def _body(name: str, operation: str, exposed: bool | None = None):
    from provisa.api.admin.actions_router import FunctionInput

    return FunctionInput(
        name=name, sourceId="api", functionName=operation, domainId="pets", exposedAsTable=exposed
    )


@pytest.fixture
def state(monkeypatch, plane):
    state = _State(plane)
    monkeypatch.setattr(appmod, "state", state, raising=False)
    monkeypatch.setattr(
        "provisa.api.admin.schema_helpers._ensure_openapi_spec", AsyncMock(return_value=None)
    )
    return state


async def _settle(body, stored=None):
    from provisa.api.admin.actions_router import _as_source_operation, _settle_table

    operation = await _as_source_operation(body)
    return await _settle_table(_request(), body, operation, stored)


# --- registering ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_registered_off_it_returns_the_plain_lists_rows(state, plane):
    await _table(plane, "openapi", "findPetsByStatus")
    body = _body("get_pet", "getPetById")
    assert await _settle(body) is None
    assert body.exposedAsTable is False
    assert body.returns == "openapi.findPetsByStatus"


@pytest.mark.asyncio
async def test_registered_off_with_no_plain_list_registered_is_refused_naming_them(state):
    with pytest.raises(ApiError) as refused:
        await _settle(_body("get_pet", "getPetById"))
    assert refused.value.code == "functions.plain_list_unregistered"
    assert refused.value.params["lists"] == "findPetsByStatus, findPetsByTags"


@pytest.mark.asyncio
async def test_registered_on_it_returns_the_table_derived_from_it(state, monkeypatch):
    derive = AsyncMock(return_value="openapi.getOrderById")
    monkeypatch.setattr("provisa.api.admin._function_table.derive_table", derive)
    body = _body("get_order", "getOrderById")
    await _settle(body)
    assert body.exposedAsTable is True
    assert body.returns == "openapi.getOrderById"
    assert derive.await_args is not None
    assert derive.await_args.args[3].table == "getOrderById"


@pytest.mark.asyncio
async def test_deriving_the_table_takes_the_one_registered_for_the_operation(state, plane):
    from provisa.api.admin._function_table import derive_table

    await _table(plane, "default", "get_order_by_id")
    op = await _operation(state, "getOrderById")
    assert await derive_table(_request(), "api", "pets", op) == "default.get_order_by_id"


@pytest.mark.asyncio
async def test_deriving_the_table_registers_it_as_register_table_proposes_it(state, monkeypatch):
    """None registered: registerTable registers the operation's table from the columns and the
    parameters the source offers for it."""
    from provisa.api.admin import schema_mutation
    from provisa.api.admin._function_table import derive_table
    from provisa.api.admin.types import AvailableColumnType, MutationResult

    columns = [
        AvailableColumnType(name="id", data_type="integer", comment=None),
        AvailableColumnType(
            name="_nf_orderId",
            data_type="integer",
            comment=None,
            native_filter_type="path_param",
            native_filter_required=True,
        ),
    ]
    monkeypatch.setattr(
        "provisa.api.admin.schema_query.resolve_available_columns_metadata",
        AsyncMock(return_value=columns),
    )
    state.roles = {"reader": {}}
    registered = AsyncMock(return_value=MutationResult(success=True, message="registered"))
    monkeypatch.setattr(schema_mutation.Mutation, "register_table", registered)
    op = await _operation(state, "getOrderById")
    assert await derive_table(_request(), "api", "pets", op) == "openapi.getOrderById"
    assert registered.await_args is not None
    table = registered.await_args.args[-1]
    assert (table.source_id, table.schema_name, table.table_name) == (
        "api",
        "openapi",
        "getOrderById",
    )
    parameter = next(c for c in table.columns if c.name == "_nf_orderId")
    assert (parameter.native_filter_type, parameter.native_filter_required) == ("path_param", True)


# --- turning it off ------------------------------------------------------------------------------


async def _exposed_pet(plane) -> tuple[int, dict]:
    """getPetById registered with its table on: its derived table, and the function's row."""
    await _table(plane, "openapi", "findPetsByStatus")
    derived = await _table(plane, "openapi", "getPetById")
    await _function(plane, "get_pet", "openapi.getPetById", True)
    async with plane.acquire() as conn:
        from provisa.core.repositories import function as function_repo

        stored = await function_repo.get_function(conn, "get_pet")
    assert stored is not None
    return derived, stored


@pytest.mark.asyncio
async def test_turning_it_off_returns_the_plain_list_and_names_the_table_to_remove(state, plane):
    derived, stored = await _exposed_pet(plane)
    body = _body("get_pet", "getPetById", exposed=False)
    assert await _settle(body, stored) == derived
    assert body.returns == "openapi.findPetsByStatus"


@pytest.mark.asyncio
async def test_turning_it_off_is_refused_while_a_relationship_refers_to_the_table(state, plane):
    derived, stored = await _exposed_pet(plane)
    plain = await _table(plane, "openapi", "findPetsByTags")
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(relationships).values(
                id="pets-to-pet",
                source_table_id=plain,
                target_table_id=derived,
                source_column="id",
                target_column="_nf_petId",
                cardinality="many-to-one",
            )
        )
    with pytest.raises(ApiError) as refused:
        await _settle(_body("get_pet", "getPetById", exposed=False), stored)
    assert refused.value.code == "functions.table_has_dependents"
    assert [(d["kind"], d["id"]) for d in refused.value.params["dependents"]] == [
        ("relationship", "pets-to-pet")
    ]
    assert "relationship" in refused.value.detail


@pytest.mark.asyncio
async def test_turning_it_off_is_refused_while_a_row_rule_is_on_the_table(state, plane):
    derived, stored = await _exposed_pet(plane)
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(rls_rules).values(table_id=derived, role_id="reader", filter_expr=b"id > 0")
        )
    with pytest.raises(ApiError) as refused:
        await _settle(_body("get_pet", "getPetById", exposed=False), stored)
    assert [d["kind"] for d in refused.value.params["dependents"]] == ["row_filter"]


@pytest.mark.asyncio
async def test_left_out_the_attribute_keeps_its_stored_value(state, plane, monkeypatch):
    derived, stored = await _exposed_pet(plane)
    body = _body("get_pet", "getPetById")
    assert await _settle(body, stored) is None
    assert body.exposedAsTable is True
    assert body.returns == "openapi.getPetById"


# --- the table side ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_derived_table_is_not_deleted_on_its_own(state, plane):
    from provisa.api.admin._function_table import derived_delete_refusal

    await _exposed_pet(plane)
    async with plane.acquire() as conn:
        said = await derived_delete_refusal(
            conn, {"schema_name": "openapi", "table_name": "getPetById"}
        )
        plain = await derived_delete_refusal(
            conn, {"schema_name": "openapi", "table_name": "findPetsByStatus"}
        )
    assert said is not None and "get_pet" in said
    assert plain is None


@pytest.mark.asyncio
async def test_a_derived_tables_columns_are_not_edited_on_the_table(state, plane):
    from provisa.api.admin._function_table import derived_edit_refusal
    from provisa.core.models import Column, Table

    await _exposed_pet(plane)
    model = Table(
        source_id="api",
        domain_id="pets",
        schema_name="openapi",
        table_name="getPetById",
        columns=[Column(name="nickname", data_type="varchar", visible_to=[])],
    )
    async with plane.acquire() as conn:
        said = await derived_edit_refusal(conn, model)
    assert said is not None and "get_pet" in said
