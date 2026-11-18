# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2253: natural-language query works only within the domains selected in the domain filter.

Two domains: ``sales`` (orders -> customers) and ``hr`` (employees), with customers related to
employees across them. With ``sales`` selected, nothing of ``hr`` is sent to the model -- not in
the table list table selection reads, not in the schema block SQL generation reads, not in the
GraphQL SDL the strict chain and the GraphQL and Cypher branches read -- and no hr table is chosen
or read. No selection keeps the role's whole surface. Each entry point (``/query/nl``,
``/data/nl-to-sql`` and the MCP ``generate_explore_queries`` tool) takes the selection and
intersects it with the role's authority.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from graphql import print_schema

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.nl.job import InMemoryJobStore, NlJob, new_job_id
from provisa.nl.scope import schema_within, selected_scope
from tests.role_state import RoleState

_ROLE = {
    "id": "analyst",
    "capabilities": ["query_development"],
    "domain_access": ["*", "meta", "ontology", "ops"],
    # REQ-603: may bypass the relationship guard, so non-strict runs as asked.
    "relationship_guard": False,
}


def _col(name: str, data_type: str = "integer") -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=False)


def _table(tid: int, domain: str, name: str, cols: list[str]) -> dict:
    return {
        "id": tid,
        "source_id": "pg",
        "domain_id": domain,
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": ["analyst"]} for c in cols],
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }


def _schema_and_ctx():
    tables = [
        _table(1, "sales", "orders", ["id", "customer_id", "amount"]),
        _table(2, "sales", "customers", ["id", "name", "manager_id"]),
        _table(3, "hr", "employees", ["id", "salary"]),
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
            "id": "cust-emp",
            "source_table_id": 2,
            "target_table_id": 3,
            "source_column": "manager_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]
    column_types = {
        1: [_col("id"), _col("customer_id"), _col("amount", "decimal(10,2)")],
        2: [_col("id"), _col("name", "varchar(100)"), _col("manager_id")],
        3: [_col("id"), _col("salary", "decimal(10,2)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "sales", "description": "Sales"}, {"id": "hr", "description": "HR"}],
    )
    return generate_schema(si), build_context(si)


def _state():
    schema, ctx = _schema_and_ctx()
    state = RoleState(
        schemas={"analyst": schema},
        contexts={"analyst": ctx},
        roles={"analyst": _ROLE},
        tables=[],
        metrics={},
        config=SimpleNamespace(vector_models=[], nl=SimpleNamespace(rate_limit=None)),
        rate_limiter=None,
        security_high=False,
        engine_conn=None,
        schema_build_cache={},
    )
    return state, schema, ctx


class _Recorder:
    """ProvisaLLMClient's stand-in: every prompt and system text sent to the model, recorded."""

    sent: list[str] = []

    def __init__(self, *_a, **_k) -> None:
        pass

    async def complete(self, prompt: str, system: str | None = None, max_tokens: int = 0) -> str:
        _Recorder.sent.append(f"{system or ''}\n{prompt}")
        return "orders, customers, employees"


@pytest.fixture(autouse=True)
def _recorder():
    _Recorder.sent = []
    with (
        patch("provisa.llm.client.ProvisaLLMClient", _Recorder),
        patch("provisa.nl.runner._resolve_llm_org_ctx", AsyncMock(return_value=(None, {}))),
    ):
        yield


# --- the selection within the role's authority --------------------------------------------------


def test_selection_is_intersected_with_authority():
    assert selected_scope(_ROLE, None) is None
    assert selected_scope(_ROLE, ["sales"]) == frozenset({"sales"})
    narrow = {"id": "r", "domain_access": ["sales"]}
    assert selected_scope(narrow, ["sales", "hr"]) == frozenset({"sales"})
    # The no-domain domain's id is the empty string, and is a selection of its own.
    assert selected_scope(_ROLE, [""]) == frozenset({""})


def test_schema_within_drops_every_type_of_an_unselected_domain():
    schema, ctx = _schema_and_ctx()
    whole = print_schema(schema)
    hr_type = next(tm.type_name for tm in ctx.tables.values() if tm.domain_id == "hr")
    assert hr_type in whole

    narrowed = print_schema(schema_within(schema, ctx, frozenset({"sales"})))

    assert hr_type not in narrowed
    assert "salary" not in narrowed
    for tm in ctx.tables.values():
        if tm.domain_id == "sales":
            assert tm.type_name in narrowed
    assert schema_within(schema, ctx, None) is schema


# --- /data/nl-to-sql ----------------------------------------------------------------------------


async def _nl_to_sql(state, domains, strict: bool):
    from provisa.api.data import endpoint_dev

    captured: dict = {}

    async def _loop(question, schema_block, *a, **k):
        captured["schema_block"] = schema_block
        captured["scoped"] = k["scoped"]
        return "SELECT 1", 1, ""

    raw: Any = SimpleNamespace(state=SimpleNamespace(role="analyst"))
    body = endpoint_dev.NLToSQLRequest(question="total amount by manager", strict=strict)
    body.domains = domains
    with (
        patch("provisa.api.app.state", state, create=True),
        patch.object(state, "model_db", object()),
        patch("provisa.core.org_settings.resolve_org_config", AsyncMock(return_value=None)),
        patch("provisa.core.org_secrets.read_org_api_keys", AsyncMock(return_value={})),
        patch("provisa.pgwire.governed_plan.role_governance", return_value=None),
        patch.object(endpoint_dev, "_run_sql_generation_loop", _loop),
        patch("provisa.nl.runner.best_effort_cypher_for_sql", return_value=None),
    ):
        await endpoint_dev.nl_to_sql_endpoint(raw, body, x_provisa_role=None)
    return captured


async def test_nl_to_sql_sends_no_table_of_an_unselected_domain():
    state, _schema, _ctx = _state()
    captured = await _nl_to_sql(state, ["sales"], strict=False)

    table_selection = "\n".join(_Recorder.sent)
    assert "orders" in table_selection and "customers" in table_selection
    assert "employees" not in table_selection
    assert "employees" not in captured["schema_block"]
    assert "orders" in captured["schema_block"]
    assert captured["scoped"] is True


async def test_nl_to_sql_without_a_selection_keeps_the_role_s_surface():
    state, _schema, _ctx = _state()
    captured = await _nl_to_sql(state, None, strict=False)

    assert "employees" in "\n".join(_Recorder.sent)
    assert "employees" in captured["schema_block"]
    assert captured["scoped"] is False


async def test_nl_to_sql_strict_sends_no_type_of_an_unselected_domain():
    from provisa.api.data import endpoint_dev

    state, _schema, ctx = _state()
    hr_type = next(tm.type_name for tm in ctx.tables.values() if tm.domain_id == "hr")
    seen: dict = {}

    async def _gen(nl_query, target, schema_sdl, validator, llm, **_k):
        seen["sdl"] = schema_sdl
        return None, "stopped"

    raw: Any = SimpleNamespace(state=SimpleNamespace(role="analyst"))
    body = endpoint_dev.NLToSQLRequest(question="total amount", strict=True, domains=["sales"])
    with (
        patch("provisa.api.app.state", state, create=True),
        patch("provisa.nl.runner.generation_loop", _gen),
        pytest.raises(Exception, match="strict"),
    ):
        await endpoint_dev.nl_to_sql_endpoint(raw, body, x_provisa_role=None)
    assert hr_type not in seen["sdl"] and "salary" not in seen["sdl"]
    assert "Orders" in seen["sdl"] or "orders" in seen["sdl"]


def test_a_statement_reading_an_unlisted_table_is_refined_against():
    import sqlglot

    from provisa.api.data.endpoint_dev import _check_tables_within

    listed = {"orders": {"id"}, "customers": {"id"}}
    inside = sqlglot.parse_one("SELECT orders.id FROM sales.orders", read="postgres")
    outside = sqlglot.parse_one("SELECT employees.salary FROM hr.employees", read="postgres")
    assert _check_tables_within(inside, listed) is None
    assert "employees" in (_check_tables_within(outside, listed) or "")


# --- /query/nl ----------------------------------------------------------------------------------


async def _post_nl(monkeypatch, state, payload: dict):
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    import provisa.api.app as app_mod
    from provisa.api.rest import nl_router

    scopes: list = []

    async def _run_job(job_id, nl_query, role, app_state, llm, strict, delivery, scope):
        scopes.append(scope)

    async def _llm(_state):
        return object()

    monkeypatch.setattr(nl_router, "_run_job", _run_job)
    monkeypatch.setattr(nl_router, "_get_llm", _llm)
    monkeypatch.setattr(nl_router, "_job_store", InMemoryJobStore())
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    app = FastAPI()

    @app.middleware("http")
    async def _as_analyst(request, call_next):
        request.state.role = "analyst"
        return await call_next(request)

    app.include_router(nl_router.router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        resp = await client.post("/query/nl", json=payload)
    assert resp.status_code == 202, resp.text
    import asyncio

    for _ in range(20):
        if scopes:
            break
        await asyncio.sleep(0.01)
    return scopes


async def test_query_nl_runs_the_job_within_the_selection(monkeypatch):
    state, _schema, _ctx = _state()
    state.roles = {"analyst": {**_ROLE, "domain_access": ["sales", "hr"]}}
    scopes = await _post_nl(monkeypatch, state, {"q": "count", "domains": ["sales", "finance"]})
    assert scopes == [frozenset({"sales"})]


async def test_query_nl_without_a_selection_runs_unnarrowed(monkeypatch):
    state, _schema, _ctx = _state()
    assert await _post_nl(monkeypatch, state, {"q": "count"}) == [None]


async def _run_job(state, scope, strict: bool = False):
    from provisa.nl.loop import LLMClient
    from provisa.nl.runner import run_nl_job

    sdls: list[str] = []

    class _LLM(LLMClient):
        async def complete(self, prompt: str) -> str:
            return "NOT_APPLICABLE"

    async def _gen(nl_query, target, schema_sdl, validator, llm, **_k):
        sdls.append(schema_sdl)
        return None, "stopped"

    async def _sql(*_a, **_k):
        return None, "stopped"

    store = InMemoryJobStore()
    job_id = new_job_id()
    await store.put(NlJob(job_id=job_id, nl_query="total amount", role="analyst"))
    with (
        patch("provisa.nl.runner.generation_loop", _gen),
        patch("provisa.nl.runner._generate_sql_from_nl", _sql),
    ):
        await run_nl_job(
            job_id,
            "total amount",
            "analyst",
            state,
            store,
            _LLM(),
            strict,
            deliver=None,
            scope=scope,
        )
    return sdls


async def test_run_nl_job_sends_nothing_of_an_unselected_domain():
    state, _schema, ctx = _state()
    hr_type = next(tm.type_name for tm in ctx.tables.values() if tm.domain_id == "hr")

    sdls = await _run_job(state, frozenset({"sales"}))

    assert sdls, "no branch reached the model"
    for sdl in sdls:
        assert hr_type not in sdl and "salary" not in sdl
    table_selection = "\n".join(_Recorder.sent)
    assert "orders" in table_selection and "employees" not in table_selection


async def test_run_nl_job_strict_sends_nothing_of_an_unselected_domain():
    state, _schema, ctx = _state()
    hr_type = next(tm.type_name for tm in ctx.tables.values() if tm.domain_id == "hr")

    sdls = await _run_job(state, frozenset({"sales"}), strict=True)

    assert sdls, "the strict chain did not reach the model"
    for sdl in sdls:
        assert hr_type not in sdl and "salary" not in sdl


async def test_run_nl_job_without_a_selection_sends_the_role_s_surface():
    state, _schema, ctx = _state()
    hr_type = next(tm.type_name for tm in ctx.tables.values() if tm.domain_id == "hr")

    sdls = await _run_job(state, None)

    assert any(hr_type in sdl for sdl in sdls)
    assert "employees" in "\n".join(_Recorder.sent)


# --- MCP generate_explore_queries ---------------------------------------------------------------


async def _mcp(state, domains):
    from provisa.api.mcp import tools

    seen: list = []

    class _Store:
        job = None

        async def put(self, job):
            _Store.job = job

        async def get(self, _job_id):
            return _Store.job

    async def fake_run_nl_job(
        job_id, question, role, app_state, job_store, llm, strict=False, *, deliver, scope
    ):
        seen.append(scope)

    with (
        patch("provisa.nl.job.make_job_store", return_value=_Store()),
        patch.object(state, "model_db", object()),
        patch("provisa.core.org_settings.resolve_org_config", AsyncMock(return_value={})),
        patch("provisa.core.org_secrets.read_org_api_keys", AsyncMock(return_value={})),
        patch("provisa.nl.runner.run_nl_job", fake_run_nl_job),
    ):
        await tools.generate_explore_queries(state, "analyst", "total amount", domains)
    return seen


async def test_mcp_nl_tool_runs_within_the_given_domains():
    state, _schema, _ctx = _state()
    state.roles = {"analyst": {**_ROLE, "domain_access": ["sales", "hr"]}}
    assert await _mcp(state, ["sales", "finance"]) == [frozenset({"sales"})]


async def test_mcp_nl_tool_without_domains_runs_unnarrowed():
    state, _schema, _ctx = _state()
    assert await _mcp(state, None) == [None]
