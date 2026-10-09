# Copyright (c) 2026 Kenneth Stott
# Canary: fb0a7b1c-c1e4-46de-aa09-68b183b9e374
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit/integration tests for provisa/api/data/endpoint_dev.py — /data/sql,
/data/proto/{role_id}, and their private helpers.

HTTP-level tests drive the FastAPI app with minimal AppState injection (the
canonical pattern from tests/unit/test_sql_endpoint_governance.py). Pure
helpers are exercised directly to reach branches HTTP cannot cheaply reach.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlglot
from httpx import ASGITransport, AsyncClient

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.executor.result import QueryResult

# asyncio_mode = "auto" (pyproject.toml) picks up async defs automatically.


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _table_meta(
    table_id: int = 1,
    table_name: str = "orders",
    schema_name: str = "public",
    source_id: str = "pg",
    domain_id: str = "",
) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name=schema_name,
        table_name=table_name,
        domain_id=domain_id,
    )


def _make_ctx(table_name: str = "orders", table_id: int = 1) -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {table_name: _table_meta(table_id, table_name)}
    return ctx


def _make_query_result(**kwargs) -> QueryResult:
    return QueryResult(
        rows=kwargs.get("rows", [(1, "test")]),
        column_names=kwargs.get("column_names", ["id", "name"]),
        column_types=kwargs.get("column_types"),
    )


@pytest.fixture
async def sql_client(monkeypatch):
    """ASGI test client with minimal state injected for /data/sql + /data/proto tests."""
    import provisa.api.app as app_mod
    from provisa.api.app import create_app

    _prev_auth_config = getattr(app_mod.state, "auth_config", None)
    app_mod.state.auth_config = None

    # REQ-074/REQ-1386: every governed statement appends a query_audit_log row to the org's tenant
    # database. These tests stub the executor, not the audit write, so stand in for the tenant
    # database and capture the append instead of reaching a real pool.
    # (The append is the audit writer's batch insert, on its own thread — provisa.audit.writer.)
    async def _log_queries(pool, rows):
        pass

    monkeypatch.setattr("provisa.audit.query_log.log_queries", _log_queries)
    _prev_tenant_db = app_mod.state.tenant_db
    _prev_model_db = app_mod.state.model_db
    _prev_record_db = app_mod.state.record_db
    app_mod.state.tenant_db = MagicMock()
    app_mod.state.record_db = app_mod.state.tenant_db
    app_mod.state.model_db = app_mod.state.tenant_db

    # The stand-in tenant database holds no source registry; routing reads the operator floor from
    # it (REQ-030), and none of these sources carries one.
    async def _no_registered_sources(_state, conn=None):
        return []

    monkeypatch.setattr(
        "provisa.federation.registry_view.registered_sources", _no_registered_sources
    )

    the_app = create_app()

    ctx = _make_ctx("orders", table_id=1)
    rls = RLSContext.empty()

    app_mod.state.schemas = {"org_admin": MagicMock()}
    app_mod.state.contexts = {"org_admin": ctx}
    app_mod.state.rls_contexts = {"org_admin": rls}
    app_mod.state.roles = {
        "org_admin": {
            "id": "org_admin",
            "capabilities": ["query_development", "full_results", "usage", "write"],
            "domain_access": ["*"],  # the seeded org_admin: every domain, said explicitly
        }
    }
    app_mod.state.masking_rules = {}
    app_mod.state.source_types = {"pg": "postgresql"}
    app_mod.state.source_dialects = {"pg": "postgres"}
    app_mod.state.tables = [
        {
            "id": 1,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "data_type": "integer"},
                {"column_name": "status", "data_type": "varchar"},
            ],
        }
    ]
    app_mod.state.source_pools = MagicMock()
    app_mod.state.view_sql_map = {}
    app_mod.state.proto_files = {}
    app_mod.state.schema_build_cache = {}

    transport = ASGITransport(app=the_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

    from provisa.executor.pool import SourcePool

    app_mod.state.auth_config = _prev_auth_config
    app_mod.state.tenant_db = _prev_tenant_db
    app_mod.state.model_db = _prev_model_db
    app_mod.state.record_db = _prev_record_db
    app_mod.state.schemas = {}
    app_mod.state.contexts = {}
    app_mod.state.rls_contexts = {}
    app_mod.state.roles = {}
    app_mod.state.masking_rules = {}
    app_mod.state.source_types = {}
    app_mod.state.source_dialects = {}
    app_mod.state.source_pools = SourcePool()


# ---------------------------------------------------------------------------
# /data/sql — capability / discovery / stats / accept-header branches
# ---------------------------------------------------------------------------


class TestSqlEndpointCapability:
    async def test_no_query_development_capability_403(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.roles["org_admin"] = {"id": "org_admin", "capabilities": []}
        resp = await sql_client.post(
            "/data/sql", json={"sql": "SELECT id FROM orders", "role": "org_admin"}
        )
        assert resp.status_code == 403

    async def test_no_request_field_can_waive_the_capability_check(self, sql_client):
        """A request body cannot grant itself rights the role does not hold.

        ``discovery_mode`` used to be a body flag that skipped this capability check, the domain
        check and the relationship guard at once — a break-out chosen by the caller rather than by
        a grant. It is gone; the field is now ignored and the gate holds.
        """
        import provisa.api.app as app_mod

        app_mod.state.roles["org_admin"] = {"id": "org_admin", "capabilities": []}
        resp = await sql_client.post(
            "/data/sql",
            json={
                "sql": "SELECT id FROM orders",
                "role": "org_admin",
                "discovery_mode": True,
            },
        )
        assert resp.status_code == 403


class TestSqlEndpointNoSchema:
    async def test_unknown_role_returns_400(self, sql_client):
        # A role unknown to state.roles is rejected 403 by rate_limit_middleware
        # before the endpoint runs, so to reach _compile_govern_execute's own
        # "no schema for role" 400 check we need a role registered in
        # state.roles (passes the middleware) but absent from state.schemas.
        import provisa.api.app as app_mod

        app_mod.state.roles["schemaless"] = {
            "id": "schemaless",
            "capabilities": ["query_development", "full_results", "usage", "write"],
        }
        try:
            resp = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT 1", "role": "schemaless"},
                headers={"x-provisa-role": "schemaless"},
            )
        finally:
            del app_mod.state.roles["schemaless"]
        assert resp.status_code == 400
        assert "schemaless" in resp.json()["detail"]


class TestSqlEndpointStatsAndFormat:
    async def test_stats_header_json_format_includes_provisa_stats(self, sql_client):
        fallback_result = _make_query_result(rows=[(1,)], column_names=["id"])
        with (
            patch(
                "provisa.executor.direct.execute_direct",
                new=AsyncMock(return_value=fallback_result),
            ),
            patch(
                "provisa.executor.trino.execute_trino", new=AsyncMock(return_value=fallback_result)
            ),
        ):
            resp = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT id FROM orders", "role": "org_admin"},
                headers={"accept": "application/json", "x-provisa-stats": "true"},
            )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert "provisa_stats" in body

    async def test_stats_report_the_governed_plan_not_a_placeholder(self, sql_client):
        # REQ-1517: the entry and the DAG come from the plan the pipeline terminal executed, so
        # the source column names the route that ran and the diagram is present for the SQL
        # surface the same way it is for GraphQL.
        fallback_result = _make_query_result(rows=[(1,)], column_names=["id"])
        with (
            patch(
                "provisa.executor.direct.execute_direct",
                new=AsyncMock(return_value=fallback_result),
            ),
            patch(
                "provisa.executor.trino.execute_trino", new=AsyncMock(return_value=fallback_result)
            ),
        ):
            resp = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT id FROM orders", "role": "org_admin"},
                headers={"accept": "application/json", "x-provisa-stats": "true"},
            )
        assert resp.status_code == 200
        stats = resp.json()["provisa_stats"]
        assert stats["mermaid"].startswith("flowchart LR")
        (entry,) = stats["sources"]
        assert entry["field"] == "sql"
        # "batch" was the placeholder strategy the endpoint used to invent for every statement.
        assert entry["strategy"] != "batch"
        assert entry["strategy"].startswith(("direct:", "federated:"))
        assert entry["rows"] == 1
        assert entry["physical_sql"]

    async def test_default_json_response_shape(self, sql_client):
        fallback_result = _make_query_result(rows=[(1,)], column_names=["id"])
        with (
            patch(
                "provisa.executor.direct.execute_direct",
                new=AsyncMock(return_value=fallback_result),
            ),
            patch(
                "provisa.executor.trino.execute_trino", new=AsyncMock(return_value=fallback_result)
            ),
        ):
            resp = await sql_client.post(
                "/data/sql", json={"sql": "SELECT id FROM orders", "role": "org_admin"}
            )
        assert resp.status_code == 200
        # REQ-1436: the projection rides alongside the rows, so a grid can draw its header
        # (and the filter inputs in it) even when the result set is empty.
        # REQ-1937: a result that reports no types says so, and the grid types its filters from
        # the values.
        assert resp.json() == {
            "data": {"sql": [{"id": 1}]},
            "columns": ["id"],
            "column_types": None,
        }

    async def test_column_types_ride_alongside_the_columns(self, sql_client):
        """REQ-1937: the grid types each column's filter from the result's column types."""
        typed = _make_query_result(
            rows=[(1, "a")], column_names=["id", "name"], column_types=["bigint", "varchar"]
        )
        with (
            patch("provisa.executor.direct.execute_direct", new=AsyncMock(return_value=typed)),
            patch("provisa.executor.trino.execute_trino", new=AsyncMock(return_value=typed)),
        ):
            resp = await sql_client.post(
                "/data/sql", json={"sql": "SELECT id, name FROM orders", "role": "org_admin"}
            )
        assert resp.status_code == 200
        assert resp.json()["column_types"] == ["bigint", "varchar"]

    async def test_parameter_values_are_bound_never_written_into_the_statement(self, sql_client):
        """REQ-1937: the values of $1…$n travel beside the statement to whatever runs it."""
        result = _make_query_result(rows=[(7,)], column_names=["id"])
        direct = AsyncMock(return_value=result)
        engine = AsyncMock(return_value=result)
        with (
            patch("provisa.executor.direct.execute_direct", new=direct),
            patch("provisa.executor.trino.execute_trino", new=engine),
        ):
            resp = await sql_client.post(
                "/data/sql",
                json={
                    "sql": "SELECT id FROM orders WHERE id = $1 AND name = $2",
                    "role": "org_admin",
                    "params": [7, "o'brien; DROP TABLE orders"],
                },
            )
        assert resp.status_code == 200, resp.text
        (call,) = [*direct.call_args_list, *engine.call_args_list]
        sent = [*call.args, *call.kwargs.values()]
        statement = next(a for a in sent if isinstance(a, str) and "orders" in a.lower())
        assert [7, "o'brien; DROP TABLE orders"] in sent, "the values were not handed on as values"
        assert "o'brien" not in statement and "DROP TABLE" not in statement

    @pytest.mark.parametrize(
        "sql, params, reason",
        [
            (
                "SELECT id FROM orders WHERE id = $1 AND name = $2",
                [7],
                "placeholder $2 has no value",
            ),
            ("SELECT id FROM orders WHERE id = $1", [7, 8], "no placeholder takes value $2"),
            ("SELECT id FROM orders", [7], "no placeholder takes value $1"),
            ("SELECT id FROM orders WHERE id = $1; SELECT 1", [7], "one statement"),
        ],
    )
    async def test_values_that_do_not_fit_the_statement_are_refused_before_it_runs(
        self, sql_client, sql, params, reason
    ):
        direct, engine = AsyncMock(), AsyncMock()
        with (
            patch("provisa.executor.direct.execute_direct", new=direct),
            patch("provisa.executor.trino.execute_trino", new=engine),
        ):
            resp = await sql_client.post(
                "/data/sql", json={"sql": sql, "role": "org_admin", "params": params}
            )
        assert resp.status_code == 400, resp.text
        body = resp.json()
        assert body["code"] == "data.sql_parameters_do_not_fit" and reason in body["detail"]
        assert not direct.called and not engine.called

    def test_the_field_is_described_in_the_openapi_document(self):
        """The endpoint's request model is what its OpenAPI operation is generated from."""
        from provisa.api.app import create_app

        spec = create_app().openapi()
        operation = spec["paths"]["/data/sql"]["post"]
        ref = operation["requestBody"]["content"]["application/json"]["schema"]["$ref"]
        params = spec["components"]["schemas"][ref.rsplit("/", 1)[1]]["properties"]["params"]
        assert "never written into its text" in params["description"]

    async def test_csv_accept_format_uses_format_response(self, sql_client):
        fallback_result = _make_query_result(rows=[(1, "test")], column_names=["id", "name"])
        with (
            patch(
                "provisa.executor.direct.execute_direct",
                new=AsyncMock(return_value=fallback_result),
            ),
            patch(
                "provisa.executor.trino.execute_trino", new=AsyncMock(return_value=fallback_result)
            ),
        ):
            resp = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT id FROM orders", "role": "org_admin"},
                headers={"accept": "text/csv"},
            )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")


class TestSqlExplainEndpoint:
    """REQ-1519: /data/sql/explain describes the plan the ONE pipeline would have executed."""

    PG_PLAN = json.dumps(
        [
            {
                "Plan": {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Total Cost": 10.0,
                    "Plan Rows": 42,
                }
            }
        ]
    )

    def _explain_result(self):
        return QueryResult(rows=[(self.PG_PLAN,)], column_names=["QUERY PLAN"])

    async def _post(self, client, body):
        direct = AsyncMock(return_value=self._explain_result())
        with (
            patch("provisa.executor.direct.execute_direct", new=direct),
            patch(
                "provisa.executor.trino.execute_trino",
                new=AsyncMock(return_value=self._explain_result()),
            ),
        ):
            resp = await client.post("/data/sql/explain", json=body)
        return resp, direct

    async def test_the_statement_the_source_receives_is_wrapped_in_explain(self, sql_client):
        resp, direct = await self._post(
            sql_client, {"sql": "SELECT id FROM orders", "role": "org_admin"}
        )
        assert resp.status_code == 200, resp.text
        executed = " ".join(str(a) for a in direct.await_args.args)
        assert "EXPLAIN (FORMAT JSON)" in executed
        assert "ANALYZE" not in executed

    async def test_the_plan_tree_and_the_route_come_back_normalized(self, sql_client):
        resp, _ = await self._post(
            sql_client, {"sql": "SELECT id FROM orders", "role": "org_admin"}
        )
        body = resp.json()
        assert body["route"] == "DIRECT"
        assert body["route_reason"]
        assert body["dialect"] == "postgres"
        assert body["analyzed"] is False
        (node,) = body["plan"]
        assert node["op"] == "Seq Scan orders"
        assert node["rows"] == 42
        assert body["mermaid"].startswith("flowchart TD")
        assert "Seq Scan orders" in body["mermaid"]

    async def test_analyze_true_asks_the_source_to_run_the_statement(self, sql_client):
        resp, direct = await self._post(
            sql_client, {"sql": "SELECT id FROM orders", "role": "org_admin", "analyze": True}
        )
        assert resp.status_code == 200, resp.text
        executed = " ".join(str(a) for a in direct.await_args.args)
        assert "EXPLAIN (ANALYZE, FORMAT JSON)" in executed
        assert resp.json()["analyzed"] is True

    async def test_a_write_is_refused_because_analyze_would_perform_it(self, sql_client):
        resp, _ = await self._post(sql_client, {"sql": "DELETE FROM orders", "role": "org_admin"})
        assert resp.status_code == 400
        assert "read statements" in resp.text

    async def test_a_batch_is_refused_rather_than_silently_explaining_one_statement(
        self, sql_client
    ):
        resp, _ = await self._post(
            sql_client,
            {"sql": "SELECT id FROM orders; SELECT id FROM orders", "role": "org_admin"},
        )
        assert resp.status_code == 400
        assert "single statement" in resp.text


class TestSqlEndpointRoleResolution:
    async def test_x_provisa_role_header_overrides_body_role(self, sql_client):
        # REQ-273 (amended 2026-10-01): the header carries the role. A body role that differs from
        # it is refused, naming both — the request neither runs as the body's role nor silently as
        # the header's. A body role equal to the header's is accepted. Execution (the engine
        # terminal) is mocked — the point is role RESOLUTION, not the engine result.
        result = _make_query_result(rows=[(1,)], column_names=["id"])
        with (
            patch("provisa.executor.direct.execute_direct", new=AsyncMock(return_value=result)),
            patch("provisa.executor.trino.execute_trino", new=AsyncMock(return_value=result)),
        ):
            resp = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT id FROM orders", "role": "ghost"},
                headers={"x-provisa-role": "org_admin"},
            )
            agreed = await sql_client.post(
                "/data/sql",
                json={"sql": "SELECT id FROM orders", "role": "org_admin"},
                headers={"x-provisa-role": "org_admin"},
            )
        assert resp.status_code == 400
        assert "'ghost'" in resp.json()["detail"] and "'org_admin'" in resp.json()["detail"]
        assert "No schema for role" not in resp.text  # refused for the mismatch, not run as ghost
        assert agreed.status_code != 400, agreed.text


# ---------------------------------------------------------------------------
# /data/proto/{role_id}
# ---------------------------------------------------------------------------


class TestProtoEndpoint:
    async def test_no_proto_file_404(self, sql_client, monkeypatch):
        """The model is built and the role holds no proto: 404, not the 503 an unbuilt model
        answers. The built model is arranged here; the test read it from whatever an earlier
        test in the process had left on the shared state, and answered 503 run on its own."""
        import provisa.api.app as app_mod

        monkeypatch.setattr(app_mod.state, "role_build_inputs", {"tables": []}, raising=False)
        monkeypatch.setattr(app_mod.state, "proto_files", {}, raising=False)
        resp = await sql_client.get("/data/proto/org_admin")
        assert resp.status_code == 404

    async def test_proto_before_the_model_is_built_503(self, sql_client, monkeypatch):
        import provisa.api.app as app_mod

        monkeypatch.setattr(app_mod.state, "role_build_inputs", {})
        resp = await sql_client.get("/data/proto/org_admin")
        assert resp.status_code == 503

    async def test_static_proto_file_returned(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.proto_files = {"org_admin": 'syntax = "proto3";'}
        resp = await sql_client.get("/data/proto/org_admin")
        assert resp.status_code == 200
        assert "proto3" in resp.text
        app_mod.state.proto_files = {}

    async def test_unknown_role_with_domains_404(self, sql_client):
        resp = await sql_client.get("/data/proto/ghost_role?domains=pet_store")
        assert resp.status_code == 404

    async def test_domains_without_schema_build_cache_503(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.schema_build_cache = {}
        resp = await sql_client.get("/data/proto/org_admin?domains=pet_store")
        assert resp.status_code == 503

    async def test_domains_generates_filtered_proto(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.roles["org_admin"] = {
            "id": "org_admin",
            "capabilities": ["query_development", "full_results", "usage", "write"],
            "domain_access": ["*"],
        }
        app_mod.state.schema_build_cache = {
            "tables": [
                {"id": 1, "domain_id": "pet_store", "name": "pets"},
            ],
            "relationships": [],
            "column_types": {},
            "naming_rules": {},
            "domains": {"pet_store": {}},
            "domain_prefix": {},
            "physical_table_map": {},
            "functions": [],
            "webhooks": [],
            "enum_types": {},
        }
        with (
            patch("provisa.api.data.sdl._reachable_table_ids", return_value=set()),
            patch(
                "provisa.grpc.proto_gen.generate_proto", return_value='syntax = "proto3";'
            ) as mock_gen,
        ):
            resp = await sql_client.get("/data/proto/org_admin?domains=pet_store")
        assert resp.status_code == 200
        mock_gen.assert_called_once()
        app_mod.state.schema_build_cache = {}

    async def test_domains_generate_proto_value_error_404(self, sql_client):
        import provisa.api.app as app_mod

        app_mod.state.roles["org_admin"] = {
            "id": "org_admin",
            "capabilities": ["query_development", "full_results", "usage", "write"],
            "domain_access": ["*"],
        }
        app_mod.state.schema_build_cache = {
            "tables": [{"id": 1, "domain_id": "pet_store", "name": "pets"}],
            "relationships": [],
            "column_types": {},
            "naming_rules": {},
            "domains": {"pet_store": {}},
            "domain_prefix": {},
            "physical_table_map": {},
            "functions": [],
            "webhooks": [],
            "enum_types": {},
        }
        with (
            patch("provisa.api.data.sdl._reachable_table_ids", return_value=set()),
            patch(
                "provisa.grpc.proto_gen.generate_proto",
                side_effect=ValueError("bad schema"),
            ),
        ):
            resp = await sql_client.get("/data/proto/org_admin?domains=pet_store")
        assert resp.status_code == 404
        app_mod.state.schema_build_cache = {}

    # --- a requested domain narrows a role's proto, never widens it ---------------------------

    _CACHE = {
        "tables": [
            {"id": 1, "domain_id": "sales", "name": "orders"},
            {"id": 2, "domain_id": "finance", "name": "ledger"},
        ],
        "relationships": [],
        "column_types": {},
        "naming_rules": {},
        "domains": {"sales": {}, "finance": {}},
        "domain_prefix": {},
        "physical_table_map": {},
        "functions": [],
        "webhooks": [],
        "enum_types": {},
    }

    def _role(self, app_mod, domain_access):
        app_mod.state.roles["scoped"] = {
            "id": "scoped",
            "capabilities": ["query_development", "usage"],
            "domain_access": domain_access,
        }
        app_mod.state.schema_build_cache = dict(self._CACHE)

    @pytest.mark.parametrize("held", [["sales"], []])
    async def test_a_domain_the_role_does_not_reach_is_refused_by_name(self, sql_client, held):
        import provisa.api.app as app_mod

        self._role(app_mod, held)
        with patch("provisa.grpc.proto_gen.generate_proto") as mock_gen:
            resp = await sql_client.get("/data/proto/scoped?domains=finance")
        assert resp.status_code == 403, resp.text
        body = resp.json()
        assert body["code"] == "data.domain_not_accessible"
        assert body["params"] == {"role_id": "scoped", "domain": "finance"}
        mock_gen.assert_not_called()
        app_mod.state.schema_build_cache = {}

    async def test_one_unreached_domain_refuses_the_whole_request(self, sql_client):
        import provisa.api.app as app_mod

        self._role(app_mod, ["sales"])
        with patch("provisa.grpc.proto_gen.generate_proto") as mock_gen:
            resp = await sql_client.get("/data/proto/scoped?domains=sales,finance")
        assert resp.status_code == 403, resp.text
        assert resp.json()["params"]["domain"] == "finance"
        mock_gen.assert_not_called()
        app_mod.state.schema_build_cache = {}

    @pytest.mark.parametrize(
        "held,asked,root_ids",
        [(["sales"], "sales", {1}), (["*"], "finance", {2}), (["*"], "sales,finance", {1, 2})],
    )
    async def test_a_reached_domain_is_served_under_the_roles_own_scope(
        self, sql_client, held, asked, root_ids
    ):
        import provisa.api.app as app_mod

        self._role(app_mod, held)
        with patch(
            "provisa.grpc.proto_gen.generate_proto", return_value='syntax = "proto3";'
        ) as mock_gen:
            resp = await sql_client.get(f"/data/proto/scoped?domains={asked}")
        assert resp.status_code == 200, resp.text
        si = mock_gen.call_args.args[0]
        # The role goes to the generator exactly as it is held: the request chose the roots and
        # added nothing to the role's scope.
        assert si.role["domain_access"] == held
        assert si.root_table_ids == root_ids
        app_mod.state.schema_build_cache = {}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestResolveRoleId:
    """REQ-273 (amended 2026-10-01): the acting role is the auth layer's, then the header's; the
    body's role is used only when nothing established one, and is refused when it differs."""

    @staticmethod
    def _body(role=None):
        from provisa.api.data.endpoint_dev import SQLRequest

        return SQLRequest(sql="SELECT 1") if role is None else SQLRequest(sql="SELECT 1", role=role)

    def test_auth_role_takes_precedence(self):
        from provisa.api.data.endpoint_dev import _resolve_role_id

        raw_request = SimpleNamespace(state=SimpleNamespace(role="auth_role"))
        assert _resolve_role_id(raw_request, "header_role", self._body()) == "auth_role"
        assert _resolve_role_id(raw_request, "header_role", self._body("auth_role")) == "auth_role"

    def test_header_role_used_when_no_auth_role(self):
        from provisa.api.data.endpoint_dev import _resolve_role_id

        raw_request = SimpleNamespace(state=SimpleNamespace())
        assert _resolve_role_id(raw_request, "header_role", self._body()) == "header_role"

    def test_body_role_fallback(self):
        from provisa.api.data.endpoint_dev import _resolve_role_id

        raw_request = SimpleNamespace(state=SimpleNamespace())
        assert _resolve_role_id(raw_request, None, self._body("body_role")) == "body_role"
        assert _resolve_role_id(raw_request, None, self._body()) == "org_admin"

    def test_a_body_role_that_differs_from_the_acting_role_is_refused(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_dev import _resolve_role_id

        for raw_request, header in (
            (SimpleNamespace(state=SimpleNamespace(role="auth_role")), None),
            (SimpleNamespace(state=SimpleNamespace()), "header_role"),
        ):
            with pytest.raises(HTTPException) as refused:
                _resolve_role_id(raw_request, header, self._body("body_role"))
            assert refused.value.status_code == 400
            assert "'body_role'" in refused.value.detail


class TestCheckSqlCapabilities:
    def test_no_role_noop(self):
        from provisa.api.data.endpoint_dev import _check_sql_capabilities

        _check_sql_capabilities(None)  # must not raise

    def test_missing_capability_raises_403(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint_dev import _check_sql_capabilities

        with pytest.raises(HTTPException) as exc_info:
            _check_sql_capabilities({"id": "admin", "capabilities": []})
        assert exc_info.value.status_code == 403

    def test_admin_capability_passes(self):
        from provisa.api.data.endpoint_dev import _check_sql_capabilities

        _check_sql_capabilities(
            {"capabilities": ["query_development", "full_results", "usage", "write"]}
        )  # no raise


class TestCheckQualifierBinding:
    def test_valid_binding_returns_none(self):
        from provisa.api.data.endpoint_dev import _check_qualifier_binding

        tree = sqlglot.parse_one("SELECT orders.id FROM orders", read="postgres")
        assert _check_qualifier_binding(tree) is None

    def test_unresolved_qualifier_flagged(self):
        from provisa.api.data.endpoint_dev import _check_qualifier_binding

        tree = sqlglot.parse_one("SELECT u.name FROM orders", read="postgres")
        error = _check_qualifier_binding(tree)
        assert error is not None
        assert "u" in error

    def test_schema_qualified_column_flagged(self):
        from provisa.api.data.endpoint_dev import _check_qualifier_binding

        tree = sqlglot.parse_one(
            "SELECT pet_store.orders.id FROM pet_store.orders", read="postgres"
        )
        error = _check_qualifier_binding(tree)
        assert error is not None
        assert "Schema-qualified" in error


# ---------------------------------------------------------------------------
# _dispatch_sql_execution / _execute_engine_route / _execute_direct_route
# ---------------------------------------------------------------------------


class _FakeAcquireCtx:
    """Async context manager standing in for an asyncpg-style pool.acquire()."""

    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False
