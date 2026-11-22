# Copyright (c) 2026 Kenneth Stott
# Canary: 30766bf6-21a6-4d05-9b64-3e58214a7c0a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``POST /data/explain``: each language stops where it hands SQL to the one pipeline, and the
answer is what that pipeline planned (REQ-2222, REQ-1519)."""

# Requirements: REQ-1519, REQ-2222

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from provisa.compiler.sql_validator import GovernanceRefused, ValidationViolation
from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline
from provisa.pgwire._pipeline import ExplainStages
from provisa.transpiler.router import Route

_PG_PLAN = json.dumps(
    [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "orders", "Total Cost": 10.0}}]
)
_STAGES = ExplainStages(
    semantic="SELECT id FROM sales.orders",
    governed="SELECT id FROM sales.orders WHERE region = 'EU'",
    optimized="SELECT id FROM sales.orders WHERE region = 'EU' LIMIT 10",
    native='SELECT "id" FROM "public"."orders" WHERE "region" = \'EU\' LIMIT 10',
)
_COMPILED = SimpleNamespace(sql="SELECT id FROM sales.orders", params=[7])
_ORDERS_ROW = {
    "id": 11,
    "source_id": "pg",
    "domain_id": "sales",
    "schema_name": "public",
    "table_name": "orders",
}
_RETURNS_ROW = {**_ORDERS_ROW, "id": 12, "table_name": "returns"}
_CLASS_ROW = {
    "id": -5,
    "source_id": "__derived__",
    "domain_id": "ontology",
    "schema_name": "ontology",
    "table_name": "Sale",
    "class_bindings": [{"table_id": 12}, {"table_id": 11}],
}
_TABLES = (_ORDERS_ROW, _RETURNS_ROW, _CLASS_ROW)
_ORDERS_READ = {"address": "pg/public/orders", "domain": "sales", "table": "orders"}
_RETURNS_READ = {"address": "pg/public/returns", "domain": "sales", "table": "returns"}


def _plan() -> SimpleNamespace:
    return SimpleNamespace(
        route=Route.DIRECT,
        route_reason="one source",
        dialect="postgres",
        sql=f"EXPLAIN (FORMAT JSON) {_STAGES.native}",
        physical_sql=None,
        sources={"pg"},
        optimizations=["limit_pushdown"],
        semantic_sql=None,
        stages=_STAGES,
        table_ids=(11,),
    )


@pytest.fixture
async def door(monkeypatch):
    """The served app with the pipeline's two entrances recorded and its terminal stood in for."""
    import provisa.api.app as app_mod
    from provisa.api.app import create_app
    from provisa.core.config_loader import parse_config_dict

    monkeypatch.setattr(app_mod.state, "auth_config", None)
    monkeypatch.setattr(
        app_mod.state,
        "config",
        parse_config_dict({"sources": [], "domains": [], "tables": [], "roles": []}),
    )
    monkeypatch.setattr(
        app_mod.state,
        "roles",
        {"org_admin": {"id": "org_admin", "capabilities": ["query_development"]}},
    )
    monkeypatch.setattr(app_mod.state, "tables", list(_TABLES), raising=False)
    surface = SimpleNamespace(schema=MagicMock(), context=MagicMock())
    monkeypatch.setattr(
        "provisa.api.data.endpoint_explain.role_surface",
        lambda _state, role: surface if role == "org_admin" else None,
    )
    raw = AsyncMock(return_value=_plan())
    compiled = AsyncMock(return_value=_plan())
    terminal = AsyncMock(return_value=QueryResult(rows=[(_PG_PLAN,)], column_names=["QUERY PLAN"]))
    monkeypatch.setattr(_pipeline, "_govern_and_route", raw)
    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", compiled)
    monkeypatch.setattr(_pipeline, "_execute_plan", terminal)
    transport = ASGITransport(app=create_app(), raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield SimpleNamespace(
            client=client, raw=raw, compiled=compiled, terminal=terminal, surface=surface
        )


async def _explain(door, language: str, statement: str, **extra):
    body = {"language": language, "statement": statement, "role": "org_admin", **extra}
    return await door.client.post("/data/explain", json=body)


def _assert_explained(response, language: str) -> None:
    """Every stage REQ-2222 names, under one shape whatever the language."""
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["language"] == language
    assert len(body["statements"]) == 1
    statement = body["statements"][0]
    assert statement["stages"] == {
        "semantic": _STAGES.semantic,
        "governed": _STAGES.governed,
        "optimized": _STAGES.optimized,
        "native": _STAGES.native,
    }
    assert statement["optimizations"] == ["limit_pushdown"]
    assert statement["route"] == "DIRECT"
    assert statement["route_reason"] == "one source"
    assert statement["dialect"] == "postgres"
    assert statement["analyzed"] is False
    assert statement["plan"][0]["op"] == "Seq Scan orders"
    assert statement["tables"] == [_ORDERS_READ]


def _compiled_handoff(door, sdl_joins: bool) -> tuple:
    door.raw.assert_not_awaited()
    door.compiled.assert_awaited_once()
    kwargs = door.compiled.await_args.kwargs
    assert kwargs["explain"] is False
    assert kwargs["sdl_joins"] is sdl_joins
    assert kwargs.get("deliver") is None
    assert not kwargs.get("buffered")
    assert not kwargs.get("serve_cached")
    return door.compiled.await_args.args


def _patch_compile(monkeypatch) -> MagicMock:
    compile_mock = MagicMock(return_value=[_COMPILED])
    monkeypatch.setattr("provisa.api.generated_plan.compile_generated_graphql", compile_mock)
    return compile_mock


class TestTablesRead:
    async def test_each_table_once_in_address_order(self, door):
        door.raw.return_value = SimpleNamespace(**{**vars(_plan()), "table_ids": (12, 11)})
        response = await _explain(door, "sql", "SELECT 1")
        assert response.json()["statements"][0]["tables"] == [_ORDERS_READ, _RETURNS_READ]

    async def test_an_ontology_class_reports_the_tables_it_is_bound_to(self, door):
        door.raw.return_value = SimpleNamespace(**{**vars(_plan()), "table_ids": (-5, 11)})
        response = await _explain(door, "sql", "SELECT 1")
        assert response.json()["statements"][0]["tables"] == [_ORDERS_READ, _RETURNS_READ]

    async def test_a_statement_reading_no_table_reports_none(self, door):
        door.raw.return_value = SimpleNamespace(**{**vars(_plan()), "table_ids": ()})
        response = await _explain(door, "sql", "SELECT 1")
        assert response.json()["statements"][0]["tables"] == []


class TestEachLanguage:
    async def test_sql(self, door):
        response = await _explain(door, "sql", "SELECT id FROM orders")
        _assert_explained(response, "sql")
        door.raw.assert_awaited_once_with("SELECT id FROM orders", "org_admin", explain=False)
        door.compiled.assert_not_awaited()

    async def test_graphql(self, door, monkeypatch):
        compile_mock = _patch_compile(monkeypatch)
        response = await _explain(door, "graphql", "{ orders { id } }", arguments={"a": 1})
        _assert_explained(response, "graphql")
        assert compile_mock.call_args.args[4:] == ("{ orders { id } }", {"a": 1})
        assert _compiled_handoff(door, sdl_joins=True) == (_COMPILED.sql, "org_admin")
        assert door.compiled.await_args.kwargs["exec_params"] == [7]

    async def test_cypher(self, door, monkeypatch):
        from provisa.api.rest.cypher_router import PlannedRead

        planned_read = AsyncMock(return_value=PlannedRead(_plan(), {}, [], False, None, None))
        monkeypatch.setattr("provisa.api.rest.cypher_router.planned_read", planned_read)
        response = await _explain(door, "cypher", "MATCH (o) RETURN o", arguments={"a": 1})
        _assert_explained(response, "cypher")
        body = planned_read.await_args.args[0]
        assert (body.query, body.params) == ("MATCH (o) RETURN o", {"a": 1})
        assert planned_read.await_args.kwargs == {"explain": False}

    async def test_sparql(self, door, monkeypatch):
        compiled = SimpleNamespace(empty=False)
        monkeypatch.setattr(
            "provisa.api.rest.sparql_router.compiled_query",
            AsyncMock(return_value=(compiled, MagicMock(), MagicMock())),
        )
        monkeypatch.setattr(
            "provisa.api.rest.sparql_router.semantic_statement",
            MagicMock(return_value=("SELECT id FROM sales.orders", {})),
        )
        response = await _explain(door, "sparql", "SELECT ?s WHERE { ?s ?p ?o }")
        _assert_explained(response, "sparql")
        assert _compiled_handoff(door, sdl_joins=False) == (
            "SELECT id FROM sales.orders",
            "org_admin",
        )

    async def test_a_sparql_answered_without_a_statement_describes_none(self, door, monkeypatch):
        monkeypatch.setattr(
            "provisa.api.rest.sparql_router.compiled_query",
            AsyncMock(return_value=(SimpleNamespace(empty=True), MagicMock(), MagicMock())),
        )
        response = await _explain(door, "sparql", "ASK {}")
        assert response.json() == {"language": "sparql", "statements": []}
        door.compiled.assert_not_awaited()

    async def test_grpc_row_read(self, door, monkeypatch):
        compile_mock = _patch_compile(monkeypatch)
        lowered = MagicMock(return_value=("query { orders { id } }", {"v": 1}, None))
        monkeypatch.setattr("provisa.api.data.endpoint_grpc_proxy.row_read_graphql", lowered)
        response = await _explain(door, "grpc", "Sales_Orders", arguments={"limit": 5})
        _assert_explained(response, "grpc")
        assert lowered.call_args.args[2:4] == ("Sales_Orders", 5)
        assert compile_mock.call_args.args[4:] == ("query { orders { id } }", {"v": 1})
        _compiled_handoff(door, sdl_joins=True)

    @pytest.mark.parametrize("name", ["Sales_OrdersAggregate", "Sales_OrdersGroupBy"])
    async def test_grpc_aggregate_read(self, door, monkeypatch, name):
        _patch_compile(monkeypatch)
        lowered = MagicMock(return_value="query { a }")
        monkeypatch.setattr("provisa.api.data.endpoint_grpc_proxy.aggregate_graphql_text", lowered)
        response = await _explain(door, "grpc", name)
        _assert_explained(response, "grpc")
        assert lowered.call_args.args[1] == name

    @pytest.mark.parametrize(
        "statement",
        [
            "/data/rest/sales/orders?limit=2&offset=0&fields=id",
            "sales/orders?limit=2&offset=0&fields=id",
        ],
    )
    async def test_rest(self, door, monkeypatch, statement):
        from provisa.api.rest.generator import RestRead

        compile_mock = _patch_compile(monkeypatch)
        lowered = MagicMock(
            return_value=RestRead(
                "orders", "query { orders { id } }", SimpleNamespace(values={"x": 1}), False, False
            )
        )
        monkeypatch.setattr("provisa.api.rest.generator.rest_read_graphql", lowered)
        response = await _explain(door, "rest", statement)
        _assert_explained(response, "rest")
        assert lowered.call_args.args[1:3] == ("sales", "orders")
        kwargs = lowered.call_args.kwargs
        assert (kwargs["limit"], kwargs["offset"], kwargs["fields"]) == (2, 0, "id")
        assert compile_mock.call_args.args[4:] == ("query { orders { id } }", {"x": 1})
        _compiled_handoff(door, sdl_joins=True)

    async def test_mcp_run_sql(self, door):
        response = await _explain(door, "mcp", "run_sql", arguments={"sql": "SELECT 1"})
        _assert_explained(response, "mcp")
        door.raw.assert_awaited_once_with("SELECT 1", "org_admin", explain=False)

    async def test_mcp_run_cypher(self, door, monkeypatch):
        from provisa.api.rest.cypher_router import PlannedRead

        planned_read = AsyncMock(return_value=PlannedRead(_plan(), {}, [], False, None, None))
        monkeypatch.setattr("provisa.api.rest.cypher_router.planned_read", planned_read)
        arguments = {"cypher": "MATCH (o) RETURN o", "params": {"a": 1}}
        response = await _explain(door, "mcp", "run_cypher", arguments=arguments)
        _assert_explained(response, "mcp")
        assert planned_read.await_args.args[0].params == {"a": 1}

    async def test_mcp_query_metric(self, door, monkeypatch):
        metric_sql = MagicMock(return_value="SELECT total FROM metric")
        monkeypatch.setattr("provisa.api.mcp.tools._metric_sql", metric_sql)
        arguments = {"metric": "revenue", "dimensions": ["region"], "filters": "x > 1"}
        response = await _explain(door, "mcp", "query_metric", arguments=arguments)
        _assert_explained(response, "mcp")
        metric_sql.assert_called_once_with("revenue", ["region"], "x > 1")
        door.raw.assert_awaited_once_with("SELECT total FROM metric", "org_admin", explain=False)

    async def test_every_root_field_is_a_statement(self, door, monkeypatch):
        compile_mock = _patch_compile(monkeypatch)
        compile_mock.return_value = [_COMPILED, _COMPILED]
        response = await _explain(door, "graphql", "{ a { id } b { id } }")
        assert len(response.json()["statements"]) == 2


class TestAnalyze:
    async def test_analyze_reaches_the_pipeline(self, door):
        response = await _explain(door, "sql", "SELECT 1", analyze=True)
        assert response.json()["statements"][0]["analyzed"] is True
        door.raw.assert_awaited_once_with("SELECT 1", "org_admin", explain=True)


class TestRefusals:
    async def test_a_refused_role_gets_the_code(self, door):
        violation = ValidationViolation(code="V001", message="column not visible")
        door.raw.side_effect = GovernanceRefused([violation])
        response = await _explain(door, "sql", "SELECT secret FROM t")
        assert response.status_code == 403
        assert response.json()["code"] == "data.governance_refused"
        assert response.json()["params"]["violations"] == [
            {"code": "V001", "message": "column not visible"}
        ]
        door.terminal.assert_not_awaited()

    async def test_a_refused_compiled_statement_gets_the_code(self, door, monkeypatch):
        _patch_compile(monkeypatch)
        door.compiled.side_effect = GovernanceRefused(
            [ValidationViolation(code="V001", message="no")]
        )
        response = await _explain(door, "graphql", "{ orders { id } }")
        assert response.status_code == 403
        assert response.json()["code"] == "data.governance_refused"

    async def test_a_door_refusal_is_answered_as_that_door_answers(self, door, monkeypatch):
        from provisa.api.rest.cypher_router import cypher_error

        refusal = cypher_error(403, "cypher.permission_denied", "no")
        monkeypatch.setattr(
            "provisa.api.rest.cypher_router.planned_read", AsyncMock(return_value=refusal)
        )
        response = await _explain(door, "cypher", "MATCH (o) RETURN o")
        assert response.status_code == 403
        assert response.json()["code"] == "cypher.permission_denied"

    async def test_a_role_without_query_development_is_refused(self, door, monkeypatch):
        import provisa.api.app as app_mod

        monkeypatch.setattr(
            app_mod.state, "roles", {"org_admin": {"id": "org_admin", "capabilities": []}}
        )
        response = await _explain(door, "sql", "SELECT 1")
        assert response.status_code == 403
        door.raw.assert_not_awaited()

    async def test_a_cypher_write_is_not_explained(self, door):
        response = await _explain(door, "cypher", "CREATE (n:Orders {id: 1})")
        assert response.status_code == 400
        assert response.json()["code"] == "explain.invalid_statement"

    async def test_several_sql_statements_are_refused(self, door):
        response = await _explain(door, "sql", "SELECT 1; SELECT 2")
        assert response.status_code == 400
        assert response.json()["code"] == "explain.invalid_statement"

    async def test_a_tool_that_runs_no_statement_is_named(self, door):
        response = await _explain(door, "mcp", "list_native_tables")
        assert response.status_code == 400
        assert response.json()["code"] == "explain.tool_runs_no_statement"

    async def test_a_missing_tool_argument_is_named(self, door):
        response = await _explain(door, "mcp", "run_sql")
        assert response.status_code == 400
        assert "sql" in response.json()["detail"]

    @pytest.mark.parametrize("statement", ["sales", "sales/orders/extra", "/data/rest/sales"])
    async def test_a_rest_path_that_is_no_table_read_is_refused(self, door, statement):
        response = await _explain(door, "rest", statement)
        assert response.status_code == 400
        assert response.json()["code"] == "explain.invalid_statement"

    @pytest.mark.parametrize("query", ["limit=0", "limit=x", "offset=-1"])
    async def test_rest_paging_is_checked_as_the_route_checks_it(self, door, query):
        response = await _explain(door, "rest", f"sales/orders?{query}")
        assert response.status_code == 400

    async def test_an_unknown_language_is_refused(self, door):
        response = await _explain(door, "prolog", "x")
        assert response.status_code == 422

    async def test_an_engine_failure_is_named(self, door):
        door.terminal.side_effect = RuntimeError("engine down")
        response = await _explain(door, "sql", "SELECT 1")
        assert response.status_code == 400
        assert response.json()["code"] == "explain.failed"


class TestCypherStopsAtTheHandoff:
    """The real Cypher lowering, stopped where it hands SQL to the pipeline."""

    @staticmethod
    def _setup(monkeypatch):
        from tests.unit.test_cypher_kept_plan import _Pipeline, _state

        pipe = _Pipeline(monkeypatch, _state())
        monkeypatch.setattr("provisa.pgwire.governed_plan.role_governance", MagicMock())
        monkeypatch.setattr(
            "provisa.compiler.sql_validator.validate_sql", MagicMock(return_value=[])
        )
        request = MagicMock()
        request.state.role = "analyst"
        request.headers = {}
        return pipe, request

    async def test_the_plan_is_built_and_nothing_runs(self, monkeypatch):
        from provisa.api.rest.cypher_router import CypherRequest, PlannedRead, planned_read
        from tests.unit.test_cypher_kept_plan import _CYPHER

        pipe, request = self._setup(monkeypatch)
        body = CypherRequest(query=_CYPHER, params={"min": 1})
        planned = await planned_read(body, request, explain=False)
        assert isinstance(planned, PlannedRead)
        assert "orders" in pipe.governed[0][0].lower()
        assert pipe.governed[0][1] == [1]
        sent = pipe.governed_kw[0]
        assert sent["explain"] is False
        assert sent["deliver"] is None
        assert sent["buffered"] is False
        assert sent["serve_cached"] is False
        pipe.terminal.assert_not_awaited()

    async def test_a_registered_call_is_not_performed(self, monkeypatch):
        from provisa.api.rest.cypher_router import CypherRequest, planned_read

        _pipe, request = self._setup(monkeypatch)
        intercept = AsyncMock(return_value=None)
        monkeypatch.setattr("provisa.api.rest.cypher_router.intercept_precompile", intercept)
        from tests.unit.test_cypher_kept_plan import _CYPHER

        await planned_read(CypherRequest(query=_CYPHER, params={"min": 1}), request, explain=False)
        intercept.assert_not_awaited()

    async def test_a_composed_command_is_refused_not_run(self, monkeypatch):
        from provisa.api.rest.cypher_router import CypherRequest, planned_read
        from tests.unit.test_cypher_kept_plan import _CYPHER

        pipe, request = self._setup(monkeypatch)
        monkeypatch.setattr(
            "provisa.api.data.cypher_commands.calls_commands", MagicMock(return_value=True)
        )
        put_rows = AsyncMock()
        monkeypatch.setattr("provisa.api.data.cypher_commands.put_command_rows", put_rows)
        answer = await planned_read(
            CypherRequest(query=_CYPHER, params={"min": 1}), request, explain=False
        )
        assert answer.status_code == 400
        assert json.loads(bytes(answer.body))["code"] == "cypher.not_explainable"
        put_rows.assert_not_awaited()
        assert pipe.governed == []
