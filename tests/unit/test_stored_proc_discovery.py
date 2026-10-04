# Copyright (c) 2026 Kenneth Stott
# Canary: 3b5d7f91-2c4e-4a6b-8d0f-1e3a5c7b9d11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-887: stored-procedure / routine auto-discovery + classification + registration."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.api.admin.introspect import (
    DiscoveredRoutine,
    RoutineArg,
    _pg_type_to_gql,
    classify_routine,
    native_routines,
)
from provisa.executor.result import QueryResult


def _pool(rows):
    pool = MagicMock()
    pool.has.return_value = True
    pool.execute = AsyncMock(return_value=QueryResult(rows=rows, column_names=[]))
    return pool


# ── classification ───────────────────────────────────────────────────────────


def test_classify_procedure_is_mutation():
    # prokind 'p' = procedure → always side-effecting regardless of volatility.
    assert classify_routine("p", "v") == "mutation"
    assert classify_routine("p", "s") == "mutation"


def test_classify_immutable_and_stable_function_is_query():
    assert classify_routine("f", "i") == "query"
    assert classify_routine("f", "s") == "query"


def test_classify_volatile_function_is_mutation():
    assert classify_routine("f", "v") == "mutation"


# ── type mapping ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "pg_type,expected",
    [
        ("integer", "Int"),
        ("bigint", "Int"),
        ("numeric(10,2)", "Float"),
        ("double precision", "Float"),
        ("boolean", "Boolean"),
        ("timestamp without time zone", "DateTime"),
        ("date", "DateTime"),
        ("text", "String"),
        ("uuid", "String"),
    ],
)
def test_pg_type_to_gql(pg_type, expected):
    assert _pg_type_to_gql(pg_type) == expected


# ── native_routines (PG catalog rows → discovered+classified) ────────────────


@pytest.mark.asyncio
async def test_native_routines_set_returning_function():
    # Stable set-returning function → read-returning query with typed args.
    rows = [
        (
            "public",
            "list_orders",
            "f",  # prokind: function
            "s",  # provolatile: stable
            True,  # proretset: set-returning
            ["customer_id", "since"],
            ["integer", "date"],
            "Orders for a customer",
        )
    ]
    result = await native_routines("src1", "postgresql", "public", _pool(rows))
    assert len(result) == 1
    r = result[0]
    assert r.routine_name == "list_orders"
    assert r.kind == "query"
    assert r.returns_setof is True
    assert [(a.name, a.type) for a in r.arguments] == [
        ("customer_id", "Int"),
        ("since", "DateTime"),
    ]
    assert r.description == "Orders for a customer"


@pytest.mark.asyncio
async def test_native_routines_void_procedure():
    # A procedure with no args → side-effecting mutation, no arguments.
    rows = [("public", "purge_stale", "p", "v", False, [], [], None)]
    result = await native_routines("src1", "postgresql", "public", _pool(rows))
    assert len(result) == 1
    r = result[0]
    assert r.kind == "mutation"
    assert r.arguments == []
    assert r.returns_setof is False


@pytest.mark.asyncio
async def test_native_routines_unnamed_args_get_positional_names():
    rows = [("public", "add", "f", "i", False, [], ["integer", "integer"], None)]
    result = await native_routines("src1", "postgresql", "public", _pool(rows))
    assert [a.name for a in result[0].arguments] == ["arg1", "arg2"]


@pytest.mark.asyncio
async def test_native_routines_unsupported_vendor_returns_none():
    assert await native_routines("src1", "mysql", "db", _pool([])) is None


@pytest.mark.asyncio
async def test_native_routines_no_driver_returns_none():
    pool = MagicMock()
    pool.has.return_value = False
    assert await native_routines("src1", "postgresql", "public", pool) is None


@pytest.mark.asyncio
async def test_native_routines_error_propagates():
    pool = MagicMock()
    pool.has.return_value = True
    pool.execute = AsyncMock(side_effect=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        await native_routines("src1", "postgresql", "public", pool)


# ── discovery offers routines; a steward registers them ──────────────────────


def _offer_state(monkeypatch, source_type: str = "postgresql"):
    """App state for the availableFunctions resolver: one database source, and a command store
    that records any registration (discovery must make none)."""
    from types import SimpleNamespace

    from provisa.core.repositories import function as function_repo

    upsert = AsyncMock()
    monkeypatch.setattr(function_repo, "upsert_function", upsert)
    state = SimpleNamespace(source_types={"src1": source_type}, source_pools=MagicMock())
    monkeypatch.setattr("provisa.api.app.state", state)
    return state, upsert


@pytest.mark.asyncio
async def test_a_database_source_offers_its_schemas_routines_and_registers_none(monkeypatch):
    from provisa.api.admin.schema_query import Query
    from tests.unit.gate_identity import grant

    state, upsert = _offer_state(monkeypatch)
    routines = [
        DiscoveredRoutine(
            schema_name="public",
            routine_name="list_orders",
            kind="query",
            returns_setof=True,
            arguments=[RoutineArg(name="customer_id", type="Int")],
            description="Orders",
        ),
        DiscoveredRoutine(
            schema_name="public", routine_name="close_order", kind="mutation", returns_setof=False
        ),
    ]
    info, _ = grant(monkeypatch, "table_registration", state=state)
    with patch(
        "provisa.api.admin.introspect.native_routines", new=AsyncMock(return_value=routines)
    ) as discover:
        offered = await Query().available_functions(info, source_id="src1", schema_name="public")

    assert discover.call_args.args[:3] == ("src1", "postgresql", "public")
    assert [(o.name, o.comment) for o in offered] == [
        ("list_orders", "query — Orders"),
        ("close_order", "mutation"),
    ]
    upsert.assert_not_called()


@pytest.mark.asyncio
async def test_a_source_with_no_routine_catalog_offers_no_routines(monkeypatch):
    from provisa.api.admin.schema_query import Query
    from tests.unit.gate_identity import grant

    state, _ = _offer_state(monkeypatch, source_type="mysql")
    info, _ = grant(monkeypatch, "table_registration", state=state)
    with patch("provisa.api.admin.introspect.native_routines", new=AsyncMock(return_value=None)):
        assert await Query().available_functions(info, source_id="src1", schema_name="x") == []
