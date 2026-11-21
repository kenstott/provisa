# Copyright (c) 2026 Kenneth Stott
# Canary: 98d2c814-d64b-46b4-ab85-e1f84a7357a9
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MCP chat works only within the domains selected in the domain filter (REQ-2253).

A chat request carries the selection; every tool call of the chat runs within it, intersected with
the role's domain access. Listing, describing, searching and querying a table outside it is not
possible: the catalog tools do not show it and the pipeline refuses a statement reading it. With
no selection the chat sees the role's whole surface, as before.
"""

# Requirements: REQ-2253

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.api.flight.catalog import CatalogColumn, CatalogTable
from provisa.api.mcp import chat as chat_mod
from provisa.api.mcp import tools
from provisa.compiler.sql_types import JoinMeta, TableMeta
from provisa.security import selected_domains
from tests.role_state import RoleState
from tests.unit.mcp_described import serve_whole

pytestmark = pytest.mark.asyncio

_CATALOG = [
    CatalogTable(
        domain_id="sales",
        table_name="orders",
        description="Order records",
        columns=[
            CatalogColumn("id", "integer", False, ""),
            CatalogColumn("customer_id", "integer", True, ""),
        ],
    ),
    CatalogTable(
        domain_id="crm",
        table_name="customers",
        description="Customer master",
        columns=[CatalogColumn("id", "integer", False, "")],
    ),
]


def _state(domain_access: list[str]) -> RoleState:
    orders = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
    )
    customers = TableMeta(
        table_id=2,
        field_name="customers",
        type_name="Customers",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="customers",
        domain_id="crm",
    )
    ctx = SimpleNamespace(
        tables={"orders": orders, "customers": customers},
        joins={
            ("Orders", "customer"): JoinMeta(
                source_column="customer_id",
                target_column="id",
                source_column_type="integer",
                target_column_type="integer",
                target=customers,
                cardinality="many-to-one",
            )
        },
    )
    return RoleState(
        contexts={"analyst": ctx},
        roles={"analyst": {"id": "analyst", "domain_access": domain_access}},
        config=SimpleNamespace(
            domains=[
                SimpleNamespace(id="sales", description="Sales"),
                SimpleNamespace(id="crm", description="CRM"),
            ],
            metrics=[],
        ),
        model_db=object(),
        tables=[
            {"id": 1, "source_id": "pg", "table_name": "orders", "domain_id": "sales"},
            {"id": 2, "source_id": "pg", "table_name": "customers", "domain_id": "crm"},
        ],
    )


@pytest.fixture(autouse=True)
def _catalog(monkeypatch):
    async def _fake(_state, _role_id=None):
        return list(_CATALOG)

    monkeypatch.setattr(tools, "_build_catalog_tables_async", _fake)
    serve_whole(monkeypatch)


async def _tool(state, name: str, args: dict, domains: list[str] | None):
    """One chat tool call, within the scope a chat carrying ``domains`` computes."""
    from provisa.nl.scope import selected_scope

    scope = None if domains is None else selected_scope(state.roles["analyst"], domains)
    return await chat_mod._execute_tool(state, "analyst", name, args, scope=scope)


# -- the catalog tools --------------------------------------------------------------------------


async def test_a_selection_lists_only_its_schemas():
    state = _state(["*"])
    listed = await _tool(state, "list_schemas", {}, ["sales"])
    assert [s["schema"] for s in listed] == ["sales"]


async def test_no_selection_lists_every_schema_the_role_reaches():
    state = _state(["*"])
    listed = await _tool(state, "list_schemas", {}, None)
    assert [s["schema"] for s in listed] == ["crm", "sales"]


async def test_a_selection_never_lists_tables_outside_it():
    state = _state(["*"])
    with pytest.raises(ValueError, match="Unknown schema"):
        await _tool(state, "list_tables", {"schema": "crm"}, ["sales"])


async def test_a_selection_never_describes_a_table_outside_it():
    state = _state(["*"])
    with pytest.raises(ValueError, match="Table not found"):
        await _tool(state, "describe_table", {"schema": "crm", "table": "customers"}, ["sales"])


async def test_a_described_table_names_no_key_into_a_domain_outside_the_selection():
    state = _state(["*"])
    described = await _tool(
        state, "describe_table", {"schema": "sales", "table": "orders"}, ["sales"]
    )
    assert described["foreign_keys"] == []
    unscoped = await _tool(state, "describe_table", {"schema": "sales", "table": "orders"}, None)
    assert [fk["references_table"] for fk in unscoped["foreign_keys"]] == ["customers"]


async def test_a_selection_never_names_fields_of_a_table_outside_it():
    state = _state(["*"])
    for name in ("graphql_field_names", "cypher_field_names"):
        with pytest.raises(ValueError, match="Table not found"):
            await _tool(state, name, {"schema": "crm", "table": "customers"}, ["sales"])


async def test_a_selection_never_finds_a_table_outside_it(monkeypatch):
    state = _state(["*"])
    hits = [
        SimpleNamespace(schema="crm", table="customers", column=None, level="table", distance=0.1),
        SimpleNamespace(schema="sales", table="orders", column=None, level="table", distance=0.2),
    ]
    index = SimpleNamespace(search=AsyncMock(return_value=hits))
    monkeypatch.setattr(tools, "_get_index", AsyncMock(return_value=index))
    found = await _tool(state, "search_catalog", {"query": "customers"}, ["sales"])
    assert [(r["schema"], r["table"]) for r in found] == [("sales", "orders")]
    found_all = await _tool(state, "search_catalog", {"query": "customers"}, None)
    assert [(r["schema"], r["table"]) for r in found_all] == [
        ("crm", "customers"),
        ("sales", "orders"),
    ]


async def test_a_selection_never_widens_the_roles_access():
    state = _state(["sales"])
    listed = await _tool(state, "list_schemas", {}, ["sales", "crm"])
    assert [s["schema"] for s in listed] == ["sales"]


async def test_generate_explore_queries_is_handed_the_selection(monkeypatch):
    state = _state(["*"])
    seen = {}

    async def _fake(state, role, question, domains=None):
        seen["domains"] = domains
        return {}

    monkeypatch.setattr(tools, "generate_explore_queries", _fake)
    await _tool(state, "generate_explore_queries", {"question": "orders?"}, ["sales"])
    assert seen["domains"] == ["sales"]
    await _tool(state, "generate_explore_queries", {"question": "orders?"}, None)
    assert seen["domains"] is None


# -- the query tools: the pipeline refuses a statement reading outside the selection ------------


async def test_the_raw_sql_pipeline_refuses_a_table_outside_the_selection(monkeypatch):
    from tests.unit.test_governed_sql_engine_internals import _govern, _Routed

    with selected_domains.within(frozenset({"crm"})):
        with pytest.raises(PermissionError, match="outside the domains selected"):
            await _govern(monkeypatch, "SELECT o.id FROM sales.orders o", ["*"])
    with selected_domains.within(frozenset({"sales"})):
        with pytest.raises(_Routed):
            await _govern(monkeypatch, "SELECT o.id FROM sales.orders o", ["*"])
    with pytest.raises(_Routed):  # no selection: unchanged
        await _govern(monkeypatch, "SELECT o.id FROM sales.orders o", ["*"])


async def test_the_compiled_pipeline_refuses_a_table_outside_the_selection():
    """Cypher, GraphQL, gRPC and REST reach the compiled stage, whose governed half is kept and
    reused: the selection is asked per call, before anything is routed."""
    from provisa.pgwire import _pipeline

    state = _state(["*"])
    with selected_domains.within(frozenset({"sales"})):
        with pytest.raises(PermissionError, match="'customers' is in domain 'crm'"):
            await _pipeline._route_compiled(
                "SELECT 1",
                "analyst",
                state,
                ctx=None,
                gov_ctx=None,
                governed_sql="SELECT 1",
                compiled_tree=None,
                table_ids=(2,),
                audit=None,
                cache_hint=None,  # type: ignore[arg-type]
                exec_params=None,
                api_args=None,
                deliver=None,
                buffered=False,
                memo={},
                cache_params=[],
                cache_missed=(),
                row_level=None,  # type: ignore[arg-type]
                travel=None,
            )


async def test_run_sql_in_a_chat_runs_within_the_selection(monkeypatch):
    """The scope is bound for the tool call the pipeline runs inside."""
    state = _state(["*"])
    seen = {}

    async def _fake_run_sql(state, role, sql, args):
        seen["scope"] = selected_domains.current()
        return {"columns": [], "rows": []}

    monkeypatch.setattr(chat_mod, "_run_sql", _fake_run_sql)
    await _tool(state, "run_sql", {"sql": "select 1"}, ["sales"])
    assert seen["scope"] == frozenset({"sales"})
    await _tool(state, "run_sql", {"sql": "select 1"}, None)
    assert seen["scope"] is None
    assert selected_domains.current() is None  # unbound once the call returns


async def test_a_selection_lists_only_metrics_within_it(monkeypatch):
    state = _state(["*"])
    metric = SimpleNamespace(
        name="revenue",
        description="",
        ai_context="",
        datatype="decimal",
        from_fact="orders",
        expression="SUM(customers.id)",
        visible_to=["analyst"],
    )
    state.config.metrics = [metric]
    monkeypatch.setattr(
        "provisa.compiler.metric_expand.metric_reference_tables", lambda _n, _e: ["customers"]
    )
    with selected_domains.within(frozenset({"sales"})):
        assert tools.list_metrics(state, "analyst") == []
    assert [m["name"] for m in tools.list_metrics(state, "analyst")] == ["revenue"]


# -- the chat request ---------------------------------------------------------------------------


async def test_run_chat_runs_every_tool_within_the_requests_selection(monkeypatch):
    state = _state(["*"])
    monkeypatch.setattr(chat_mod, "_llm_configured", AsyncMock(return_value=(True, "")))
    monkeypatch.setattr(chat_mod, "_resolve_vendor", AsyncMock(return_value="anthropic"))
    monkeypatch.setattr(
        "provisa.security.llm_budget.guard", lambda *_a, **_k: SimpleNamespace(debit=AsyncMock())
    )
    seen = {}

    async def _impl(state, role, convo, **kwargs):
        seen["scope"] = kwargs["scope"]
        yield {"type": "done"}

    monkeypatch.setattr(chat_mod, "_run_chat_anthropic", _impl)
    [e async for e in chat_mod.run_chat(state, "analyst", [], domains=["sales", "ghost"])]
    assert seen["scope"] == frozenset({"sales", "ghost"})
    state.roles["analyst"]["domain_access"] = ["sales"]
    [e async for e in chat_mod.run_chat(state, "analyst", [], domains=["sales", "crm"])]
    assert seen["scope"] == frozenset({"sales"})
    [e async for e in chat_mod.run_chat(state, "analyst", [])]
    assert seen["scope"] is None


async def test_the_chat_endpoint_hands_the_selection_to_the_chat(monkeypatch):
    from starlette.requests import Request

    from provisa.api.mcp import status

    monkeypatch.setattr(status, "require_capability_request", lambda *_a: None)
    seen = {}

    async def _fake_run_chat(state, role, messages, **kwargs):
        seen["domains"] = kwargs["domains"]
        yield {"type": "done"}

    monkeypatch.setattr(chat_mod, "run_chat", _fake_run_chat)

    async def _post(body: bytes):
        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        req = Request(
            {"type": "http", "method": "POST", "path": "/", "headers": [], "state": {}},
            receive,
        )
        req.state.role = "analyst"
        resp = await status.mcp_chat(req)
        async for _ in resp.body_iterator:
            pass

    await _post(b'{"messages": [], "domains": ["sales", ""]}')
    assert seen["domains"] == ["sales", ""]
    await _post(b'{"messages": []}')
    assert seen["domains"] is None
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        await _post(b'{"messages": [], "domains": "sales"}')
