# Copyright (c) 2026 Kenneth Stott
# Canary: 5c7d2e90-3a18-4f6b-9e41-b0a6d8c3f715
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A gRPC source's tables are registered one at a time (REQ-322, amended 2026-10-02).

Adding or refreshing the source writes only the tables already registered, each with the
columns it was registered with and in its own domain. A query method that is not registered is
on offer; the steward registers it through the mutation every source uses."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.api.admin import _table_ops
from provisa.api.admin import grpc_remote_router as router
from provisa.core.models import Column, Table


def _field(name: str, type_: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, type=type_, object_fields=[])


def _query(method: str) -> SimpleNamespace:
    return SimpleNamespace(
        service="OrderService",
        method=method,
        full_method_path=f"/orders.OrderService/{method}",
        columns=[_field("id", "integer"), _field("total", "double"), _field("items", "jsonb")],
        input_fields=[_field("customer_id", "bigint")],
    )


class _Conn:
    def __init__(self, rows=()):
        self.execute = AsyncMock()
        self._rows = list(rows)

    @asynccontextmanager
    async def transaction(self):
        yield

    async def execute_core(self, *_a, **_k):
        result = MagicMock()
        result.fetchall.return_value = self._rows
        result.fetchone.return_value = None
        return result


def _row(table: str, domain: str, column: str) -> SimpleNamespace:
    return SimpleNamespace(table_name=table, domain_id=domain, column_name=column)


@pytest.fixture
def written(monkeypatch) -> list[Table]:
    tables: list[Table] = []

    async def _capture(_conn, tbl):
        tables.append(tbl)
        return 1

    monkeypatch.setattr("provisa.core.repositories.table.upsert", _capture)
    return tables


# --- adding or refreshing the source ---


async def test_a_source_with_nothing_registered_writes_no_table(written):
    conn = _Conn()
    n_tables = await router._register_schema(
        "g", [_query("ListOrders"), _query("GetOrder")], conn, "ns", "d", registered={}
    )
    assert n_tables == 0 and written == []
    conn.execute.assert_not_awaited()  # no registration record, and no command recorded


async def test_only_registered_tables_are_brought_up_to_date(written):
    registered = {"ns__OrderService__ListOrders": ("sales", {"id", "_nf_customer_id"})}
    n_tables = await router._register_schema(
        "g", [_query("ListOrders"), _query("GetOrder")], _Conn(), "ns", "d", registered
    )
    assert n_tables == 1
    (table,) = written
    assert table.table_name == "ns__OrderService__ListOrders"
    assert table.schema_name == "grpc_remote"
    assert table.domain_id == "sales"  # the domain it was registered into, not the source's
    # The columns it was registered with: total and items were not chosen and are not added.
    assert {c.name: c.data_type for c in table.columns} == {
        "id": "integer",
        "_nf_customer_id": "bigint",
    }


async def test_the_registered_set_is_read_from_the_registry():
    conn = _Conn(
        [
            _row("ns__OrderService__ListOrders", "sales", "id"),
            _row("ns__OrderService__ListOrders", "sales", "total"),
            _row("ns__OrderService__GetOrder", "", "id"),
        ]
    )
    assert await router.registered_query_tables(conn, "g") == {
        "ns__OrderService__ListOrders": ("sales", {"id", "total"}),
        "ns__OrderService__GetOrder": ("", {"id"}),
    }


# --- registering one ---


def _state(conn: _Conn, monkeypatch) -> None:
    db = MagicMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=ctx)
    state = SimpleNamespace(
        model_db=db,
        tenant_db=db,
        grpc_remote_sources={
            "g": {"namespace": "ns", "queries": [_query("ListOrders"), _query("GetOrder")]}
        },
        graphql_remote_sources={},
    )
    monkeypatch.setattr("provisa.api.app.state", state)


def _input(table: str, source: str = "g") -> SimpleNamespace:
    return SimpleNamespace(
        source_id=source, table_name=table, schema_name="grpc_remote", domain_id="sales"
    )


async def test_registering_a_table_takes_the_columns_chosen_and_every_filter(monkeypatch):
    conn = _Conn()
    _state(conn, monkeypatch)
    chosen = [Column(name="id", visible_to=["analyst"]), Column(name="total", visible_to=[])]
    columns, error = await _table_ops._grpc_columns_for_input(
        _input("ns__OrderService__ListOrders"), chosen
    )
    assert error is None
    by_name = {c.name: c for c in columns}
    assert set(by_name) == {"id", "total", "_nf_customer_id"}
    assert by_name["id"].visible_to == ["analyst"] and by_name["id"].data_type == "integer"
    assert by_name["_nf_customer_id"].native_filter_type == "grpc_input"
    # proto3 has no required fields, so no input is required by what the proto states.
    assert by_name["_nf_customer_id"].native_filter_required is False
    # The registration is recorded for the table, in the domain it is registered into.
    args = conn.execute.await_args.args
    assert args[1:3] == ("g", "ns__OrderService__ListOrders") and args[5] == "sales"


async def test_registering_with_no_columns_named_takes_them_all(monkeypatch):
    _state(_Conn(), monkeypatch)
    columns, error = await _table_ops._grpc_columns_for_input(
        _input("ns__OrderService__GetOrder"), []
    )
    assert error is None
    assert {c.name for c in columns} == {"id", "total", "items", "_nf_customer_id"}


async def test_a_table_the_proto_does_not_offer_is_refused(monkeypatch):
    _state(_Conn(), monkeypatch)
    columns, error = await _table_ops._grpc_columns_for_input(_input("ns__OrderService__Nope"), [])
    assert columns == [] and error is not None and not error.success
    assert "offers no table 'ns__OrderService__Nope'" in error.message


async def test_a_change_to_who_may_see_registered_columns_does_not_reread_the_proto(monkeypatch):
    conn = _Conn([_row("ns__OrderService__ListOrders", "sales", "id")])
    _state(conn, monkeypatch)
    result = await _table_ops._grpc_columns_for_input(
        _input("ns__OrderService__ListOrders"), [Column(name="id", visible_to=["*"])]
    )
    assert result is None  # answered from what is stored, by the ordinary path
    conn.execute.assert_not_awaited()


async def test_a_source_that_is_not_grpc_is_not_this_paths_to_answer(monkeypatch):
    _state(_Conn(), monkeypatch)
    assert await _table_ops._grpc_columns_for_input(_input("t", source="pg"), []) is None


# --- reading a registered table ---


async def test_a_registered_table_is_read_by_calling_its_method(monkeypatch):
    """A grpc_remote table has no engine table to scan: its rows come from calling the query
    method it is registered from, on the source's channel, with the columns it was registered
    with (REQ-325, REQ-941)."""
    from provisa.events.source_loader import make_grpc_remote_loader

    seen = {}

    async def _execute(channel, path, pb2, request, reply, args, server_streaming=False):
        seen.update(path=path, args=args, streaming=server_streaming)
        return [{"id": 1, "total": 9.5, "items": "[]"}, {"id": 2, "total": 3.0, "items": "[]"}]

    monkeypatch.setattr("provisa.grpc_remote.executor.execute_query", _execute)
    monkeypatch.setattr("provisa.grpc_remote.executor.channel_for", lambda reg: "channel")
    query = SimpleNamespace(
        **vars(_query("ListOrders")),
        input_message="Req",
        output_message="Res",
        server_streaming=True,
    )
    load = make_grpc_remote_loader({"g": {"namespace": "ns", "pb2": object(), "queries": [query]}})
    table = SimpleNamespace(
        table_name="ns__OrderService__ListOrders",
        columns=[
            SimpleNamespace(name="id"),
            SimpleNamespace(name="total"),
            SimpleNamespace(name="_nf_customer_id"),
        ],
    )
    rows = await load(SimpleNamespace(id="g"), table)
    assert rows == [{"id": 1, "total": 9.5}, {"id": 2, "total": 3.0}]
    assert seen == {"path": "/orders.OrderService/ListOrders", "args": {}, "streaming": True}


async def test_a_table_no_method_registers_as_is_not_read():
    from provisa.events.source_loader import UnsupportedSourceFetch, make_grpc_remote_loader

    load = make_grpc_remote_loader({"g": {"namespace": "ns", "queries": []}})
    with pytest.raises(UnsupportedSourceFetch):
        await load(SimpleNamespace(id="g"), SimpleNamespace(table_name="nope", columns=[]))
