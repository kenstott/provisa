# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2a9c41-6e85-4b0f-a3d1-9f4c2e8b6a17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL root field is read through the one compiled pipeline and only shaped here.

``endpoint._execute_one_field`` hands the field's compiled statement to
``_govern_and_route_compiled`` with what the request carried (its delivery, cache opt-in, as-of,
route hint and session properties) and executes the plan with ``_execute_plan``. Governance, the
response cache, routing, the API stage and delivery are the pipeline's (tested with it); these
tests pin what the endpoint asks for and how it shapes what comes back."""

# Requirements: REQ-027, REQ-028, REQ-544, REQ-1194, REQ-1897

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.compiler.directives import CacheHint
from provisa.compiler.sql_types import ColumnRef, CompiledQuery
from provisa.executor.result import QueryResult
from provisa.transpiler.router import Route

_HINT = CacheHint(opt_in=True, ttl=30, debug_trace=False)


def _compiled(root_field: str = "orders", **over) -> CompiledQuery:
    kw: dict[str, Any] = dict(
        sql='SELECT "id" FROM "sales"."orders"',
        params=[5],
        root_field=root_field,
        columns=[ColumnRef(None, "id", "id", None)],
        sources={"sales-pg"},
        canonical_field="orders",
    )
    kw.update(over)
    return CompiledQuery(**kw)


def _plan(route=Route.DIRECT, cache_hit=None, materialize=None):
    return SimpleNamespace(
        route=route,
        cache_hit=cache_hit,
        materialize=materialize,
        sources=frozenset({"sales-pg"}),
        source_id="sales-pg",
        dialect="postgres",
        route_reason="t",
        sql="SELECT id FROM orders",
    )


@pytest.fixture
def pipeline(monkeypatch):
    """The pipeline stood in: every plan request is recorded; each plan answers ``results``."""
    from provisa.api.data import endpoint
    from provisa.pgwire import _pipeline

    asked: list[dict] = []
    plans: list = []
    results: list = []

    async def _govern(sql, role_id, **kwargs):
        asked.append({"sql": sql, "role_id": role_id, **kwargs})
        return plans.pop(0) if plans else _plan()

    async def _execute(plan, state):
        return results.pop(0)

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    monkeypatch.setattr(endpoint, "note_request_route", lambda route: None)
    monkeypatch.setattr(endpoint, "_record_per_source_stats", lambda *a, **k: None)
    return SimpleNamespace(asked=asked, plans=plans, results=results)


async def _field(compiled, *, delivery=None, output_format="json", **over):
    from provisa.api.data import endpoint

    args = dict(
        delivery=delivery,
        as_of="TIMESTAMP '2026-01-01 00:00:00'",
        steward_hint="engine",
        query_session_props={"query_max_run_time": "1m"},
        cache_hint=_HINT,
    )
    args.update(over)
    return await endpoint._execute_one_field(
        compiled, SimpleNamespace(tables={}), SimpleNamespace(), "analyst", output_format, **args
    )


async def test_the_field_asks_the_pipeline_with_what_the_request_carried(pipeline):
    pipeline.results.append(QueryResult(rows=[(1,)], column_names=["id"]))
    compiled = _compiled()
    await _field(compiled)
    (call,) = pipeline.asked
    assert call["sql"] == compiled.sql and call["role_id"] == "analyst"
    assert call["exec_params"] == [5]
    assert call["compiled"] is compiled
    assert call["cache_hint"] is _HINT
    assert call["serve_cached"] is True  # an inline read may be answered from the cache
    assert call["buffered"] is True  # REQ-1224: inlined under the threshold, landed over it
    assert call["deliver"] is None
    assert call["as_of"] == "TIMESTAMP '2026-01-01 00:00:00'"
    assert call["steward_hint"] == "engine"
    assert call["session_props"] == {"query_max_run_time": "1m"}


async def test_rows_are_shaped_under_the_fields_own_alias(pipeline):
    """REQ-544: what the cache holds is the statement's rows, alias-free; two reads of one field
    under different aliases are each answered under their own."""
    rows = [(1,), (2,)]
    for alias in ("a", "b"):
        pipeline.results.append(QueryResult(rows=rows, column_names=["id"]))
        root, field_rows, redirect, hit = await _field(_compiled(alias))
        assert (root, field_rows, redirect, hit) == (alias, [{"id": 1}, {"id": 2}], None, None)


async def test_a_cache_hit_reports_its_record(pipeline):
    record = SimpleNamespace(age=3)
    pipeline.plans.append(_plan(route=Route.CACHE, cache_hit=([], record)))
    pipeline.results.append(QueryResult(rows=[(7,)], column_names=["id"]))
    root, field_rows, redirect, hit = await _field(_compiled())
    assert field_rows == [{"id": 7}] and redirect is None and hit is record


async def test_a_forced_delivery_answers_with_its_handle(pipeline):
    delivery = SimpleNamespace(output_format="parquet")
    handle = {"sink": "object-store", "redirect_url": "https://x/r", "row_count": 9}
    pipeline.plans.append(_plan(route=Route.ENGINE, materialize=delivery))
    pipeline.results.append(QueryResult(rows=[], column_names=[], redirect=handle))
    root, field_rows, redirect, hit = await _field(_compiled(), delivery=delivery)
    assert (root, field_rows, redirect, hit) == ("orders", None, handle, None)
    (call,) = pipeline.asked
    assert call["deliver"] is delivery
    assert call["serve_cached"] is False  # a delivery is never answered from the cache


async def test_an_aggregate_reads_its_nodes_through_a_second_plan(pipeline):
    compiled = _compiled(
        sql='SELECT COUNT(*) AS "count" FROM "sales"."orders"',
        columns=[ColumnRef(None, "count", "count", None)],
        nodes_sql='SELECT "id" FROM "sales"."orders"',
        nodes_params=[3],
        nodes_columns=[ColumnRef(None, "id", "id", None)],
        api_args={"region": "EU"},
        gql_remote_extra_selections={"orders": {"x": "y"}},
        agg_alias="aggregate",
    )
    pipeline.results.append(QueryResult(rows=[(2,)], column_names=["count"]))
    pipeline.results.append(QueryResult(rows=[(1,), (2,)], column_names=["id"]))
    root, field_rows, redirect, hit = await _field(compiled)
    first, nodes = pipeline.asked
    assert first["buffered"] is False  # an aggregate's answer is always small
    assert nodes["sql"] == compiled.nodes_sql and nodes["exec_params"] == [3]
    assert nodes["api_args"] == {"region": "EU"}
    assert nodes["extra_selections"] == {"orders": {"x": "y"}}
    assert field_rows == {"aggregate": {"count": 2}, "nodes": [{"id": 1}, {"id": 2}]}


async def test_a_refused_field_reaches_the_caller_as_403():
    from fastapi import HTTPException

    from provisa.api.data.endpoint import _forbidden
    from provisa.api.errors import ApiError
    from provisa.pgwire._pipeline import ApprovalDenied

    denied = _forbidden(ApprovalDenied("outside business hours"))
    assert isinstance(denied, ApiError)
    assert (denied.status_code, denied.code) == (403, "data.approval_denied")
    assert denied.params == {"reason": "outside business hours"}
    refused = _forbidden(PermissionError("[V003] column not visible"))
    assert isinstance(refused, HTTPException) and refused.status_code == 403
    assert refused.detail == "[V003] column not visible"


async def test_the_endpoint_holds_no_cache_of_its_own():
    """REQ-1897: one response cache. GraphQL's opt-in rides the pipeline's; the endpoint neither
    reads nor writes a store."""
    import inspect

    from provisa.api.data import endpoint

    source = inspect.getsource(endpoint)
    for name in ("check_cache", "store_result", "cache_key(", "response_cache_store"):
        assert name not in source, name


def _refused_by(monkeypatch, exc):
    """The endpoint over a field whose pipeline refuses with ``exc``."""
    from provisa.api.data import endpoint
    from tests.unit.test_graphql_plan_cache import _endpoint_harness

    h = _endpoint_harness(monkeypatch)

    async def _refuse(*args, **kwargs):
        raise exc

    monkeypatch.setattr(endpoint, "_execute_one_field", _refuse)
    return h


def test_a_pipeline_refusal_of_a_read_is_a_403(monkeypatch):
    from fastapi import HTTPException

    h = _refused_by(monkeypatch, PermissionError("[V003] column not visible"))
    with pytest.raises(HTTPException) as refused:
        h.call()
    assert refused.value.status_code == 403
    assert refused.value.detail == "[V003] column not visible"


def test_the_complexity_refusal_keeps_the_apps_413(monkeypatch):
    """ComplexityLimitExceeded is a PermissionError the app answers itself (413, its params)."""
    from provisa.compiler.complexity import ComplexityLimitExceeded

    too_complex = ComplexityLimitExceeded(SimpleNamespace(score=7, describe=lambda: "x"), 4, "role")
    h = _refused_by(monkeypatch, too_complex)
    with pytest.raises(ComplexityLimitExceeded):
        h.call()


def test_every_graphql_answer_is_encoded_by_orjson(monkeypatch):
    """REQ-1867: /data/graphql returns its body as an orjson response, never a dict for FastAPI's
    jsonable_encoder pass: a query, a redirect handle and a mutation's body alike."""
    import decimal

    from provisa.api.data import endpoint
    from provisa.api.json_response import OrjsonResponse
    from tests.unit.test_graphql_plan_cache import _endpoint_harness

    h = _endpoint_harness(monkeypatch)

    async def _rows(compiled, *args, **kwargs):
        return compiled.root_field, [{"amount": decimal.Decimal("2.50")}], None, None

    monkeypatch.setattr(endpoint, "_execute_one_field", _rows)
    answered = h.call()
    assert isinstance(answered, OrjsonResponse)
    assert (
        bytes(answered.body) == b'{"data":{"orders":[{"amount":2.5}]}}'
    )  # as jsonable_encoder gives it

    async def _redirected(compiled, *args, **kwargs):
        return compiled.root_field, None, {"redirect_url": "https://x/r", "row_count": 3}, None

    monkeypatch.setattr(endpoint, "_execute_one_field", _redirected)
    assert isinstance(h.call(), OrjsonResponse)

    async def _mutation_body(*args, **kwargs):
        return {"data": {"insert_orders": {"affected_rows": 1}}}

    from graphql import parse

    monkeypatch.setattr(
        endpoint, "parse_query", lambda schema, query, variables=None, *, ctx: parse(query)
    )
    monkeypatch.setattr(endpoint, "_handle_mutation", _mutation_body)
    mutated = h.call('mutation { insert_orders(objects: [{region: "eu"}]) { affected_rows } }')
    assert isinstance(mutated, OrjsonResponse)
    assert bytes(mutated.body) == b'{"data":{"insert_orders":{"affected_rows":1}}}'
