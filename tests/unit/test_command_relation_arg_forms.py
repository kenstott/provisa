# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e7a21-3b58-4d06-8f1a-2e6d0b9c5a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1159 (amended 2026-11-27): a relation argument is given as a table, a query or a few rows,
on every surface as the one value the argument takes. A table is named by its logical, served name
and read through the one governed pipeline as the caller's role, its declared columns only; a
physical source.schema.table is refused by name (amended 2026-11-28). A query runs through the one governed pipeline as the caller's role; rows (a JSON array of
objects) are the relation themselves. A table_ref, which names a table its service reads, refuses
a query or rows by name. Each is validated against the argument's declared columns."""

# Requirements: REQ-1159

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import provisa.executor.function_dispatch as fd
from provisa.executor.function_dispatch import dispatch_function
from provisa.executor.result import QueryResult
from provisa.otel_compat import MemoryUdfTraceSink
from tests.served_names import served_names

pytestmark = pytest.mark.unit


class _Pools:
    def __init__(self):
        self.calls: list = []

    def has(self, _src):
        return True

    async def execute(self, src, sql, params):
        self.calls.append((src, sql, params))
        return QueryResult(rows=[(1, "ada")], column_names=["id", "name"], column_types=[])


def _state():
    return SimpleNamespace(
        roles={"ops": {"id": "ops", "capabilities": []}},
        source_pools=_Pools(),
        udf_trace_sink=MemoryUdfTraceSink(),
        minted_sessions=[],
        udf_egress_allowlist=["svc"],
        names_table=served_names("sales.orders", "shop.items"),
    )


def _fn(arg_kind: str = "result_set", columns: list[dict] | None = None) -> dict:
    arg: dict = {"name": "input", "type": "String", "arg_kind": arg_kind}
    if columns is not None:
        arg["columns"] = columns
    return {
        "name": "enrich",
        "kind": "query",
        "returns": "",
        "impl_kind": "http",
        "binding": {"url": "https://svc/enrich"},
        "materialize": False,
        "arguments": [arg],
    }


@pytest.fixture
def sent(monkeypatch):
    seen: dict = {}

    async def _http(method, url, payload, timeout):
        seen["args"] = payload["args"]
        return [{"ok": 1}]

    monkeypatch.setattr(fd, "_http_call", _http)
    return seen


@pytest.fixture
def governed(monkeypatch):
    """The one pipeline, recorded: the statement and the role it was governed as."""
    import provisa.pgwire._pipeline as pipeline

    ran: list = []

    async def _govern(sql, role_id, **_kw):
        ran.append((sql, role_id))
        return "plan"

    async def _execute(plan, _state):
        assert plan == "plan"
        return QueryResult(rows=[(7, "x")], column_names=["id", "name"], column_types=[])

    monkeypatch.setattr(pipeline, "_govern_and_route", _govern)
    monkeypatch.setattr(pipeline, "_execute_plan", _execute)
    return ran


@pytest.mark.asyncio
async def test_rows_given_as_json_text_are_the_relation(sent):
    st = _state()
    rows = [{"id": 1, "name": "a"}, {"id": 2, "extra": True}]
    await dispatch_function(_fn(), {"input": json.dumps(rows)}, st, "ops")
    assert sent["args"]["input"] == {
        "kind": "result_set",
        "columns": ["id", "name", "extra"],
        "rows": rows,
    }
    assert st.source_pools.calls == []  # nothing read from a source


@pytest.mark.asyncio
async def test_rows_given_as_a_list_are_the_relation(sent):
    rows = [{"id": 1}]
    await dispatch_function(_fn(), {"input": rows}, _state(), "ops")
    assert sent["args"]["input"]["rows"] == rows


@pytest.mark.asyncio
async def test_rows_that_are_not_objects_are_refused(sent):
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(_fn(), {"input": "[1, 2]"}, _state(), "ops")
    assert ei.value.status_code == 400
    assert "JSON array of objects" in ei.value.detail


@pytest.mark.asyncio
async def test_rows_that_are_not_json_are_refused(sent):
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(_fn(), {"input": "[{oops"}, _state(), "ops")
    assert ei.value.status_code == 400
    assert "not JSON" in ei.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query",
    [
        'SELECT "id", "name" FROM "sales"."orders" LIMIT 100',
        "  with t as (select 1 as id) select * from t",
        "(select 1)",
        "VALUES (1)",
    ],
)
async def test_a_query_runs_governed_as_the_callers_role(sent, governed, query):
    st = _state()
    await dispatch_function(_fn(), {"input": query}, st, "ops")
    assert governed == [(query, "ops")]
    assert sent["args"]["input"] == {
        "kind": "result_set",
        "columns": ["id", "name"],
        "rows": [{"id": 7, "name": "x"}],
    }
    assert st.source_pools.calls == []  # not read from the source past governance


@pytest.mark.asyncio
async def test_a_query_with_no_role_bound_is_refused(sent, governed):
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(_fn(), {"input": "select 1"}, _state(), None)
    assert ei.value.status_code == 400
    assert governed == []


@pytest.mark.asyncio
@pytest.mark.parametrize("ref", ["shop.items", '"shop"."items"', "SHOP.Items"])
async def test_a_table_is_read_governed_by_its_served_name(sent, governed, ref):
    st = _state()
    await dispatch_function(_fn(), {"input": ref}, st, "ops")
    assert len(governed) == 1 and governed[0][1] == "ops"
    assert governed[0][0].replace('"', "").lower() == "select * from shop.items"
    assert sent["args"]["input"]["rows"] == [{"id": 7, "name": "x"}]
    assert st.source_pools.calls == []  # never read from the source past governance


@pytest.mark.asyncio
async def test_a_table_is_read_for_its_declared_columns_only(sent, governed):
    fn = _fn(columns=[{"name": "id", "type": "integer"}, {"name": "name", "type": "text"}])
    await dispatch_function(fn, {"input": '"shop"."items"'}, _state(), "ops")
    assert governed == [('SELECT "items"."id", "items"."name" FROM "shop"."items"', "ops")]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["result_set", "table_ref"])
@pytest.mark.parametrize(
    "ref",
    [
        "s3.public.items",  # a physical source.schema.table
        "public.items",  # a physical schema.table SQL serves no table by
        "items",  # unqualified
        "shop.missing",  # a name SQL serves nothing by
        "shop.items x",  # not a table name alone
    ],
)
async def test_a_table_not_named_by_its_served_name_is_refused(sent, governed, kind, ref):
    st = _state()
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(_fn(kind), {"input": ref}, st, "ops")
    assert ei.value.status_code == 400
    assert "logical name" in ei.value.detail and "schema.table" in ei.value.detail
    assert "source.schema.table" in ei.value.detail and repr(ref) in ei.value.detail
    assert governed == [] and st.source_pools.calls == [] and "args" not in sent


@pytest.mark.asyncio
async def test_a_table_with_no_role_bound_is_refused(sent, governed):
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(_fn(), {"input": "shop.items"}, _state(), None)
    assert ei.value.status_code == 400
    assert governed == []


@pytest.mark.asyncio
async def test_given_rows_are_held_to_the_declared_columns(sent):
    fn = _fn(columns=[{"name": "id", "type": "text"}])
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(fn, {"input": [{"id": 1}]}, _state(), "ops")
    assert ei.value.status_code == 422
    assert "input 'input'" in ei.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["select 1", '[{"id": 1}]'])
async def test_a_table_ref_refuses_a_query_or_rows(sent, value):
    with pytest.raises(HTTPException) as ei:
        await dispatch_function(_fn("table_ref"), {"input": value}, _state(), "ops")
    assert ei.value.status_code == 400
    assert "table_ref" in ei.value.detail and "'input'" in ei.value.detail


@pytest.mark.asyncio
async def test_a_table_ref_takes_a_table_name(sent):
    await dispatch_function(_fn("table_ref"), {"input": '"sales"."orders"'}, _state(), "ops")
    assert sent["args"]["input"] == {"kind": "table_ref", "ref": '"sales"."orders"'}
