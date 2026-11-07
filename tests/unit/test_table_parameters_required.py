# Copyright (c) 2026 Kenneth Stott
# Canary: 9679e2a8-8615-419c-afe3-d1bb431732cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read of a table that leaves a required parameter without a value is refused before anything
is sent to its source, naming the table and the parameter (REQ-1986). One rule for every source
kind: OpenAPI, remote GraphQL, remote gRPC. A parameter is required when the source requires it
or when a steward made it required."""

# Requirements: REQ-1986

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.data.materialization import _materialize_api_to_engine_cache
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint, ParamType

TABLE = 7


def _param(name: str, kind: str, by_source: bool, by_steward: bool = False) -> dict:
    return {
        "column_name": f"_nf_{name}",
        "native_filter_type": kind,
        "native_filter_required": by_source,
        "required_by_steward": by_steward,
    }


def _table(name: str, *params: dict) -> dict:
    return {
        "id": TABLE,
        "source_id": "src",
        "table_name": name,
        "columns": [{"column_name": "id", "native_filter_type": None}, *params],
    }


def _openapi_state(*params: dict) -> SimpleNamespace:
    endpoint = ApiEndpoint(
        source_id="src",
        path="/orders",
        method="GET",
        table_name="orders",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(
                name="_nf_region",
                type=ApiColumnType.string,
                param_type=ParamType.query,
                param_name="region",
                param_only=True,
            ),
        ],
        ttl=60,
    )
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={("src", "orders"): endpoint},
        graphql_remote_sources={},
        tables=[_table("orders", *params)],
    )


def _graphql_state(*params: dict) -> SimpleNamespace:
    reg = {
        "source_id": "src",
        "url": "https://example.test/graphql",
        "tables": [
            {
                "sql_name": "pets",
                "name": "pets",
                "field_name": "pets",
                "required_args": [],
                "optional_args": [{"name": "status", "gql_type": "String", "provisa_type": "text"}],
                "columns": [{"name": "id", "type": "integer"}],
            }
        ],
    }
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={"src": reg},
        tables=[_table("pets", *params)],
    )


def _grpc_state(*params: dict) -> SimpleNamespace:
    query = SimpleNamespace(service="Accounts", method="List")
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        grpc_remote_sources={"src": {"namespace": "", "queries": [query]}},
        tables=[_table("Accounts__List", *params)],
    )


async def _read(state, sql: str, nf_args: dict | None = None):
    return await _materialize_api_to_engine_cache(sql, state, nf_args=nf_args, table_ids=[TABLE])


_STEPS = {
    "openapi": "provisa.api.data.materialization._mat_api_ep_table",
    "graphql": "provisa.api.data.materialization._mat_gql_remote_table",
    "grpc": "provisa.api.data.materialization._mat_grpc_remote_table",
}


@pytest.mark.parametrize(
    ("kind", "state", "table", "parameter"),
    [
        ("openapi", _openapi_state(_param("region", "query_param", True)), "orders", "region"),
        ("grpc", _grpc_state(_param("account", "grpc_input", True)), "Accounts__List", "account"),
    ],
)
async def test_a_read_without_a_parameter_the_source_requires_is_refused_before_any_call(
    kind, state, table, parameter
):
    step = AsyncMock()
    with patch(_STEPS[kind], new=step), pytest.raises(ValueError) as refused:
        await _read(state, f"SELECT * FROM {table}")
    assert f"Table {table!r} requires the parameter(s) [{parameter!r}]" in str(refused.value)
    step.assert_not_awaited()


@pytest.mark.parametrize(
    ("kind", "state", "table", "parameter"),
    [
        (
            "openapi",
            _openapi_state(_param("region", "query_param", False, by_steward=True)),
            "orders",
            "region",
        ),
        (
            "graphql",
            _graphql_state(_param("status", "graphql_arg", False, by_steward=True)),
            "pets",
            "status",
        ),
        (
            "grpc",
            _grpc_state(_param("account", "grpc_input", False, by_steward=True)),
            "Accounts__List",
            "account",
        ),
    ],
)
async def test_a_read_without_a_parameter_a_steward_made_required_is_refused_before_any_call(
    kind, state, table, parameter
):
    step = AsyncMock()
    with patch(_STEPS[kind], new=step), pytest.raises(ValueError) as refused:
        await _read(state, f"SELECT * FROM {table}")
    assert f"Table {table!r} requires the parameter(s) [{parameter!r}]" in str(refused.value)
    step.assert_not_awaited()


@pytest.mark.parametrize(
    ("kind", "state", "table", "given"),
    [
        (
            "openapi",
            _openapi_state(_param("region", "query_param", False, by_steward=True)),
            "orders",
            {"region": "eu"},
        ),
        (
            "graphql",
            _graphql_state(_param("status", "graphql_arg", False, by_steward=True)),
            "pets",
            {"_status": "sold"},  # GraphQL prefixes an argument colliding with a field
        ),
        (
            "grpc",
            _grpc_state(_param("account", "grpc_input", True)),
            "Accounts__List",
            {"account": "a1"},
        ),
    ],
)
async def test_a_read_giving_every_required_parameter_is_sent(kind, state, table, given):
    step = AsyncMock()
    with patch(_STEPS[kind], new=step):
        _, _, dropped = await _read(state, f"SELECT * FROM {table}", given)
    step.assert_awaited_once()
    # The stand-in fetch lands nothing, so an OpenAPI read is left unmaterialized -- never refused
    # for a parameter.
    assert not any("parameter" in reason for reason in dropped.values())


async def test_an_optional_parameter_may_be_left_out():
    step = AsyncMock()
    with patch(_STEPS["openapi"], new=step):
        await _read(_openapi_state(_param("region", "query_param", False)), "SELECT * FROM orders")
    step.assert_awaited_once()


async def test_an_optional_graphql_argument_given_is_sent_under_its_remote_name():
    step = AsyncMock()
    state = _graphql_state(_param("status", "graphql_arg", False))
    with patch(_STEPS["graphql"], new=step):
        await _read(state, "SELECT * FROM pets", {"status": "sold"})
    assert step.await_args is not None
    assert step.await_args.kwargs["variables"] == {"status": "sold"}


async def test_a_table_only_in_a_union_branch_has_the_branch_dropped_instead():
    state = _openapi_state(_param("region", "query_param", True))
    state.tables.append({"id": 8, "source_id": "src", "table_name": "customers", "columns": []})
    sql = "SELECT id FROM orders UNION ALL SELECT id FROM customers"
    with patch(_STEPS["openapi"], new=AsyncMock()) as step:
        _, _, dropped = await _materialize_api_to_engine_cache(
            sql, state, nf_args={}, table_ids=[TABLE, 8]
        )
    assert dropped == {
        "orders": "requires the parameter(s) ['region']: give each a value (a WHERE condition "
        "on its column in SQL, an argument in GraphQL)"
    }
    step.assert_not_awaited()


async def test_a_parameter_a_model_join_feeds_is_given_by_the_join():
    state = _openapi_state(_param("region", "query_param", True))
    joined = SimpleNamespace(type_name="Customer")
    ctx = SimpleNamespace(
        joins={
            ("Customer", "orders"): SimpleNamespace(
                target=SimpleNamespace(table_name="orders"),
                target_column="_nf_region",
                source_column="region",
            )
        },
        tables={"customers": joined},
    )
    step = AsyncMock()
    with patch(_STEPS["openapi"], new=step):
        await _materialize_api_to_engine_cache(
            "SELECT * FROM orders", state, nf_args={}, table_ids=[TABLE], ctx=ctx
        )
    step.assert_awaited_once()


def test_graphql_marks_a_required_parameters_argument_non_null():
    from graphql import GraphQLNonNull

    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import _build_native_filter_args

    info = SimpleNamespace(
        visible_columns=[{"column_name": "id"}],
        column_metadata={
            name: ColumnMetadata(column_name=name, data_type="varchar", is_nullable=True)
            for name in ("id", "_nf_region", "_nf_status", "_nf_tag")
        },
        native_filter_columns=[
            _param("region", "query_param", True),
            _param("status", "graphql_arg", False, by_steward=True),
            _param("tag", "query_param", False),
        ],
    )
    args: dict = {}
    _build_native_filter_args(info, args)
    assert isinstance(args["region"].type, GraphQLNonNull)
    assert isinstance(args["status"].type, GraphQLNonNull)
    assert not isinstance(args["tag"].type, GraphQLNonNull)


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _Conn:
    """The stored table (id 3) and its one parameter, as the guard reads them."""

    def __init__(self, required):
        self._answers = [_Rows([(3, "sales")]), _Rows([("_nf_region", "query_param", required)])]

    async def execute_core(self, _stmt):
        return self._answers.pop(0)


def _save(*columns):
    from provisa.core.models import Column, Table

    return Table(
        source_id="src",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        columns=[Column(name="id", visible_to=[]), *columns],
    )


def _region(**kw):
    from provisa.core.models import Column

    return Column(name="_nf_region", visible_to=[], native_filter_type="query_param", **kw)


async def test_a_steward_switches_a_parameter_required_and_back():
    from provisa.api.admin._parameter_guard import source_parameter_refusal

    on = _save(_region(native_filter_required=False, required_by_steward=True))
    assert await source_parameter_refusal(_Conn(False), on) is None
    off = _save(_region(native_filter_required=False, required_by_steward=False))
    assert await source_parameter_refusal(_Conn(False), off) is None


async def test_a_save_cannot_make_a_parameter_the_source_requires_optional():
    from provisa.api.admin._parameter_guard import source_parameter_refusal

    save = _save(_region(native_filter_required=False))
    refusal = await source_parameter_refusal(_Conn(True), save)
    assert refusal is not None and "'_nf_region' of 'orders'" in refusal


async def test_a_save_cannot_change_how_a_parameter_reaches_the_source():
    from provisa.api.admin._parameter_guard import source_parameter_refusal
    from provisa.core.models import Column

    moved = Column(
        name="_nf_region",
        visible_to=[],
        native_filter_type="path_param",
        native_filter_required=True,
    )
    refusal = await source_parameter_refusal(_Conn(True), _save(moved))
    assert refusal is not None and "reaches the source as query_param" in refusal
