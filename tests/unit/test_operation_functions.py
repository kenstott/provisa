# Copyright (c) 2026 Kenneth Stott
# Canary: 8b1d54e7-2c93-4a6f-9e08-d47a3c5b1f26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source operation that requires a parameter is a function (REQ-1986).

Called in FROM with its parameters as arguments, it is the same relation as its parameterized
table with those parameters assigned by equality: the statement the one pipeline governs, binds
and routes is the table's, so both forms ask the source the same call under the same rules.
"""

# Requirements: REQ-1986

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import sqlglot

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.operation_function import (
    OperationFunctionRefused,
    expand,
    is_operation_function,
)
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta
from provisa.core.parameter_binding import ParameterByEqualityOnly
from provisa.core.parameters import parameter_facts
from provisa.federation.replica_address import ReplicaRoutes
from tests.role_state import RoleState

_PARAMETERS = [
    {
        "column_name": "_nf_petId",
        "data_type": "integer",
        "visible_to": ["analyst"],
        "native_filter_type": "path_param",
        "native_filter_required": True,
    },
    {
        "column_name": "_nf_lang",
        "data_type": "varchar",
        "visible_to": ["analyst"],
        "native_filter_type": "query_param",
        "native_filter_required": False,
    },
]

_TABLES = [
    {
        "id": 1,
        "source_id": "api",
        "schema_name": "public",
        "table_name": "pet_by_id",
        "domain_id": "pets",
        "columns": [
            {"column_name": "id", "data_type": "integer", "visible_to": ["analyst"]},
            {"column_name": "name", "data_type": "varchar", "visible_to": ["analyst"]},
            *_PARAMETERS,
        ],
    }
]

_GET_PET = {
    "id": 7,
    "name": "get_pet",
    "source_id": "api",
    "schema_name": "openapi",
    "function_name": "getPetById",
    "returns": "public.pet_by_id",
    "impl_kind": "source_operation",
    "kind": "query",
    "domain_id": "pets",
    "visible_to": ["analyst"],
    "arguments": [
        {"name": "petId", "type": "integer"},
        {"name": "lang", "type": "varchar", "default": None},
    ],
}


def _state(functions: dict | None = None) -> RoleState:
    ctx = CompilationContext()
    ctx.tables = {
        "pet_by_id": TableMeta(
            table_id=1,
            field_name="pet_by_id",
            type_name="PetById",
            source_id="api",
            catalog_name="api",
            schema_name="public",
            table_name="pet_by_id",
            domain_id="pets",
        )
    }
    ctx.native_filter_columns = {1: {"_nf_petId": "integer", "_nf_lang": "varchar"}}
    ctx.parameters = {1: tuple(parameter_facts(_PARAMETERS))}
    role = {
        "id": "analyst",
        "capabilities": ["ignore_relationships", "command_execution"],
        "domain_access": ["*"],
    }
    return RoleState(
        model_stamp=1,
        admin_db=None,
        contexts={"analyst": ctx, "visitor": ctx},
        rls_contexts={},
        roles={
            "analyst": role,
            "visitor": {**role, "id": "visitor", "capabilities": ["ignore_relationships"]},
        },
        masking_rules={},
        tables=_TABLES,
        relationships=[],
        source_types={"api": "openapi"},
        source_catalogs={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        metrics={},
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
        tracked_functions={"get_pet": _GET_PET} if functions is None else functions,
    )


def _expanded(sql: str, role: str = "analyst", state: RoleState | None = None) -> str:
    tree = sqlglot.parse_one(sql, read="postgres")
    expand(tree, sql, role, state or _state())
    return tree.sql(dialect="postgres")


# --- what is a function of a source operation ---------------------------------------------------


def test_a_reading_source_operation_that_returns_a_table_is_an_operation_function():
    assert is_operation_function(_GET_PET)
    # A write operation, and a read that answers no rows a table holds (REQ-1924), are not.
    assert not is_operation_function({**_GET_PET, "kind": "mutation"})
    assert not is_operation_function({**_GET_PET, "returns": ""})
    assert not is_operation_function({**_GET_PET, "impl_kind": "grpc"})


# --- the function form is the table with its parameters assigned --------------------------------


def test_a_call_is_its_table_with_each_argument_assigned_by_equality():
    assert _expanded("SELECT id FROM get_pet(5)") == (
        'SELECT id FROM (SELECT * FROM "pets"."pet_by_id" AS _op WHERE "_op"."_nf_petId" = 5)'
        ' AS "get_pet"'
    )


def test_arguments_are_given_by_position_or_by_name():
    by_position = _expanded("SELECT g.id FROM get_pet(5, 'en') AS g")
    by_name = _expanded("SELECT g.id FROM get_pet(lang => 'en', petId => 5) AS g")
    assert '"_op"."_nf_petId" = 5' in by_position and '"_op"."_nf_lang" = \'en\'' in by_position
    assert '"_op"."_nf_petId" = 5' in by_name and '"_op"."_nf_lang" = \'en\'' in by_name
    assert by_position.endswith("AS g") and by_name.endswith("AS g")


def test_an_argument_given_null_or_left_out_is_not_given():
    assert "_nf_lang" not in _expanded("SELECT id FROM get_pet(5, NULL)")
    assert "_nf_lang" not in _expanded("SELECT id FROM get_pet(5)")
    # A required one left out is the table read without it, refused as the table form is.
    assert "WHERE" not in _expanded("SELECT id FROM get_pet()")


def test_an_argument_that_is_another_tables_column_makes_the_call_lateral():
    got = _expanded("SELECT g.name FROM pets.pet_list AS l, get_pet(l.id) AS g")
    assert (
        'LATERAL (SELECT * FROM "pets"."pet_by_id" AS _op WHERE "_op"."_nf_petId" = l.id) AS g'
        in got
    )


def test_a_bound_value_is_an_argument():
    assert '"_op"."_nf_petId" = $1' in _expanded("SELECT id FROM get_pet($1)")


@pytest.mark.parametrize(
    "call, said",
    [
        ("get_pet(5, 'en', 3)", "takes 2 argument"),
        ("get_pet(colour => 'red')", "'colour' is not one it declares"),
        ("get_pet(5, petId => 6)", "'petId' is given twice"),
    ],
)
def test_arguments_the_function_does_not_declare_are_refused_by_name(call, said):
    with pytest.raises(OperationFunctionRefused, match=said):
        _expanded(f"SELECT id FROM {call}")


def test_a_function_the_role_may_not_call_is_left_as_a_name_never_registered():
    tree = sqlglot.parse_one("SELECT id FROM get_pet(5)", read="postgres")
    assert expand(tree, "SELECT id FROM get_pet(5)", "visitor", _state()) is False
    assert "pet_by_id" not in tree.sql(dialect="postgres")


def test_a_function_whose_table_the_role_does_not_read_is_refused():
    state = _state()
    state.contexts["analyst"].tables = {}
    with pytest.raises(OperationFunctionRefused, match="does not read"):
        _expanded("SELECT id FROM get_pet(5)", state=state)


# --- through the one pipeline: both forms are bound and routed the same -------------------------


class _Captured(Exception):
    """What the optimization stage was handed: the test ends there."""


async def _routed(monkeypatch, sql: str, role: str = "analyst", params=None) -> dict:
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    state = _state()
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    captured: dict = {}

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        captured.update(exec_sql=exec_sql, nf_args=kwargs.get("nf_args"), reads=kwargs["read_args"])
        raise _Captured

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        with pytest.raises(_Captured):
            await _pipeline._govern_and_route(sql, role, params=params)
    return captured


_FORMS = [
    ("SELECT id, name FROM get_pet(5)", 'SELECT id, name FROM pet_by_id WHERE "_nf_petId" = 5'),
    (
        "SELECT g.name FROM get_pet(5, 'en') AS g",
        'SELECT g.name FROM pet_by_id AS g WHERE g."_nf_petId" = 5 AND g."_nf_lang" = \'en\'',
    ),
    ("SELECT id FROM get_pet()", "SELECT id FROM pet_by_id"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("function_form, table_form", _FORMS)
async def test_the_function_form_asks_the_source_what_the_table_form_asks(
    monkeypatch, function_form, table_form
):
    as_function = await _routed(monkeypatch, function_form)
    as_table = await _routed(monkeypatch, table_form)
    assert as_function["nf_args"] == as_table["nf_args"]
    assert as_function["reads"] == as_table["reads"] == {}
    assert "_nf_" not in as_function["exec_sql"]


@pytest.mark.asyncio
async def test_a_bound_argument_is_the_call_s_value(monkeypatch):
    got = await _routed(monkeypatch, "SELECT id FROM get_pet($1)", params=[9])
    assert got["nf_args"] == {"petId": 9}


@pytest.mark.asyncio
async def test_an_argument_that_assigns_nothing_is_the_table_forms_refusal(monkeypatch):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _state(), raising=False)
    with pytest.raises(ParameterByEqualityOnly, match="_nf_petId"):
        await _pipeline._govern_and_route("SELECT id FROM get_pet(2 + 3)", "analyst")
    with pytest.raises(ParameterByEqualityOnly, match="_nf_petId"):
        await _pipeline._govern_and_route(
            'SELECT id FROM pet_by_id WHERE "_nf_petId" = 2 + 3', "analyst"
        )


@pytest.mark.asyncio
async def test_a_role_that_may_not_call_the_function_reads_no_such_relation(monkeypatch):
    import provisa.api.app as app_mod
    from provisa.compiler.sql_validator import GovernanceRefused
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _state(), raising=False)
    with pytest.raises(GovernanceRefused, match="V006"):
        await _pipeline._govern_and_route("SELECT id FROM get_pet(5)", "visitor")
