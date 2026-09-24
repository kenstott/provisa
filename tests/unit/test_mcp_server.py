# Copyright (c) 2026 Kenneth Stott
# Canary: 3fe86373-c89c-48e7-8759-1280e7074782
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the MCP protocol adapter (REQ-1008, phase 1).

Covers deterministic drill-down (list_schemas / list_tables / describe_table),
governed run_sql / explain_sql routing through _govern_and_route, the row cap,
and the fail-loud role rules (no role / unknown role / PermissionError never
becomes a silent empty result or an admin default).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.api.flight.catalog import CatalogColumn, CatalogTable
from provisa.api.mcp import tools
from provisa.compiler.sql_types import JoinMeta, TableMeta
from provisa.executor.result import QueryResult
from provisa.transpiler.router import Route

pytestmark = pytest.mark.asyncio


# --- fixtures ---------------------------------------------------------------

_CATALOG = [
    CatalogTable(
        domain_id="sales",
        table_name="orders",
        description="Order records",
        columns=[
            CatalogColumn("id", "integer", False, "Primary key"),
            CatalogColumn("customer_id", "integer", True, "FK to customers"),
        ],
    ),
    CatalogTable(
        domain_id="sales",
        table_name="line_items",
        description="Order lines",
        columns=[CatalogColumn("id", "integer", False, "")],
    ),
    CatalogTable(
        domain_id="crm",
        table_name="customers",
        description="Customer master",
        columns=[CatalogColumn("id", "integer", False, "")],
    ),
]


def _make_state():
    """Fake AppState: a role context with one FK join + config domains."""
    orders_meta = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
    )
    customers_meta = TableMeta(
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
        tables={"orders": orders_meta, "customers": customers_meta},
        joins={
            ("Orders", "customer"): JoinMeta(
                source_column="customer_id",
                target_column="id",
                source_column_type="integer",
                target_column_type="integer",
                target=customers_meta,
                cardinality="many-to-one",
            ),
        },
    )
    config = SimpleNamespace(
        domains=[
            SimpleNamespace(id="sales", description="Sales domain"),
            SimpleNamespace(id="crm", description="CRM domain"),
        ]
    )
    return SimpleNamespace(
        contexts={"analyst": ctx},
        roles={"analyst": {"id": "analyst"}},
        config=config,
        tenant_db=object(),  # _catalog() only checks truthiness; _build_catalog_tables_async is mocked
    )


@pytest.fixture
def state():
    return _make_state()


@pytest.fixture(autouse=True)
def _patch_catalog(monkeypatch):
    """_build_catalog_tables_async normally hits tenant_db; feed the fixed catalog."""

    async def _fake(_state):
        return list(_CATALOG)

    monkeypatch.setattr(tools, "_build_catalog_tables_async", _fake)


# --- drill-down -------------------------------------------------------------


async def test_list_schemas(state):
    result = await tools.list_schemas(state, "analyst")
    assert result == [
        {"schema": "crm", "description": "CRM domain", "table_count": 1},
        {"schema": "sales", "description": "Sales domain", "table_count": 2},
    ]


async def test_list_tables(state):
    result = await tools.list_tables(state, "analyst", "sales")
    assert result == [
        {"table": "line_items", "description": "Order lines", "column_count": 1},
        {"table": "orders", "description": "Order records", "column_count": 2},
    ]


async def test_list_tables_unknown_schema(state):
    with pytest.raises(ValueError):
        await tools.list_tables(state, "analyst", "nope")


async def test_describe_table(state):
    result = await tools.describe_table(state, "analyst", "sales", "orders")
    assert result["schema"] == "sales"
    assert result["table"] == "orders"
    assert result["columns"] == [
        {"name": "id", "type": "integer", "description": "Primary key"},
        {"name": "customer_id", "type": "integer", "description": "FK to customers"},
    ]
    assert result["foreign_keys"] == [
        {
            "column": "customer_id",
            "references_schema": "crm",
            "references_table": "customers",
            "references_column": "id",
        }
    ]


async def test_describe_table_not_found(state):
    with pytest.raises(ValueError):
        await tools.describe_table(state, "analyst", "sales", "ghost")


# --- semantic name normalization -------------------------------------------
# Raw domain ids (kebab) and domain-prefixed field names must NOT leak: an agent
# must be handed the exact schema.table the SQL engine accepts, or its plan fails
# to execute ("schema doesn't exist"). See provisa.compiler.sql_rewrite naming.


def _make_prefixed_state():
    """A role context whose domain is kebab-cased ('pet-store') and whose field names
    carry the domain acronym prefix ('ps__users'), mirroring the real leak."""
    users_meta = TableMeta(
        table_id=10,
        field_name="ps__users",
        type_name="PS__Users",
        source_id="ps",
        catalog_name="ps",
        schema_name="public",
        table_name="users",
        domain_id="pet-store",
    )
    pets_meta = TableMeta(
        table_id=11,
        field_name="ps__pets",
        type_name="PS__Pets",
        source_id="ps",
        catalog_name="ps",
        schema_name="public",
        table_name="pets",
        domain_id="pet-store",
    )
    ctx = SimpleNamespace(
        tables={"ps__users": users_meta, "ps__pets": pets_meta},
        joins={
            ("PS__Pets", "user"): JoinMeta(
                source_column="owner_id",
                target_column="id",
                source_column_type="integer",
                target_column_type="integer",
                target=users_meta,
                cardinality="many-to-one",
            ),
        },
        unique_constraints={},
    )
    config = SimpleNamespace(domains=[SimpleNamespace(id="pet-store", description="Pet store")])
    return SimpleNamespace(
        contexts={"analyst": ctx},
        roles={"analyst": {"id": "analyst"}},
        config=config,
        tenant_db=object(),
    )


_PREFIXED_CATALOG = [
    CatalogTable("pet-store", "users", "Users", [CatalogColumn("id", "integer", False, "")]),
    CatalogTable(
        "pet-store",
        "pets",
        "Pets",
        [
            CatalogColumn("id", "integer", False, ""),
            CatalogColumn("owner_id", "integer", True, "FK to users"),
        ],
    ),
]


async def test_names_are_semantic_not_raw(monkeypatch):
    """schema 'pet-store' → 'pet_store'; FK target field 'ps__users' → 'users'."""

    async def _fake(_state):
        return list(_PREFIXED_CATALOG)

    monkeypatch.setattr(tools, "_build_catalog_tables_async", _fake)
    state = _make_prefixed_state()

    schemas = await tools.list_schemas(state, "analyst")
    assert schemas == [{"schema": "pet_store", "description": "Pet store", "table_count": 2}]

    tables = await tools.list_tables(state, "analyst", "pet_store")
    assert {t["table"] for t in tables} == {"users", "pets"}

    described = await tools.describe_table(state, "analyst", "pet_store", "pets")
    assert described["schema"] == "pet_store"
    assert described["table"] == "pets"
    assert described["foreign_keys"] == [
        {
            "column": "owner_id",
            "references_schema": "pet_store",
            "references_table": "users",
            "references_column": "id",
        }
    ]


# --- role rules -------------------------------------------------------------


async def test_missing_role_raises(state):
    with pytest.raises(ValueError):
        await tools.list_schemas(state, "")


async def test_unknown_role_raises(state):
    with pytest.raises(PermissionError):
        await tools.list_schemas(state, "intruder")


# --- run_sql ----------------------------------------------------------------


async def test_run_sql_routes_through_govern_and_route(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    plan = pipeline._Plan(
        route=Route.ENGINE,
        sql="SELECT 1",
        source_id="pg",
        dialect="trino",
        physical_sql="SELECT 1",
    )
    govern = AsyncMock(return_value=plan)
    execute = AsyncMock(
        return_value=QueryResult(rows=[(1, "a"), (2, "b")], column_names=["id", "name"])
    )
    monkeypatch.setattr(pipeline, "_govern_and_route", govern)
    monkeypatch.setattr(pipeline, "_execute_plan", execute)

    result = await tools.run_sql(state, "analyst", "SELECT * FROM sales.orders")

    govern.assert_awaited_once_with(
        "SELECT * FROM sales.orders",
        "analyst",
        session_vars=None,
        as_of=None,
        deliver=None,
        buffered=False,
    )
    assert result["columns"] == ["id", "name"]
    assert result["rows"] == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert result["total_rows"] == 2
    assert result["truncated"] is False


async def test_run_sql_invokes_registered_command(state, monkeypatch):
    # REQ-1156: a SELECT that names a registered command runs through invoke_tracked_function
    # (the shared function hook), not the table compiler.
    import provisa.pgwire._pipeline as pipeline
    import provisa.pgwire.function_call as function_call

    govern = AsyncMock()
    monkeypatch.setattr(pipeline, "_govern_and_route", govern)
    monkeypatch.setattr(
        function_call,
        "maybe_invoke_registered_function",
        AsyncMock(return_value=QueryResult(rows=[(1, "east")], column_names=["id", "region"])),
    )

    result = await tools.run_sql(state, "analyst", "SELECT * FROM random_python_set(3)")

    govern.assert_not_awaited()  # the command hook short-circuits the table pipeline
    assert result["columns"] == ["id", "region"]
    assert result["rows"] == [{"id": 1, "region": "east"}]


async def test_run_sql_composed_command_routes_to_pipeline(state, monkeypatch):
    # REQ-1159: a command composed INLINE (joined) — not standalone — must NOT be
    # short-circuited by the standalone hook; it routes through _govern_and_route, where
    # the shared inline-localization pass (_localize_inline_commands) runs. The real hook
    # returns None for a joined statement, so govern is awaited with the composed SQL.
    import provisa.pgwire._pipeline as pipeline

    plan = pipeline._Plan(
        route=Route.ENGINE,
        sql="SELECT 1",
        source_id="pg",
        dialect="trino",
        physical_sql="SELECT 1",
    )
    govern = AsyncMock(return_value=plan)
    monkeypatch.setattr(pipeline, "_govern_and_route", govern)
    monkeypatch.setattr(
        pipeline,
        "_execute_plan",
        AsyncMock(return_value=QueryResult(rows=[(1,)], column_names=["id"])),
    )

    composed = (
        "SELECT o.id, e.score FROM orders o JOIN enrich_orders('sales.orders') e ON o.id = e.id"
    )
    await tools.run_sql(state, "analyst", composed)

    govern.assert_awaited_once_with(
        composed,
        "analyst",
        session_vars=None,
        as_of=None,
        deliver=None,
        buffered=False,
    )


async def test_run_sql_applies_row_cap(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    monkeypatch.setenv("PROVISA_MCP_MAX_ROWS", "2")
    plan = pipeline._Plan(route=Route.DIRECT, sql="SELECT 1", source_id="pg", dialect="postgres")
    rows = [(i,) for i in range(10)]
    monkeypatch.setattr(pipeline, "_govern_and_route", AsyncMock(return_value=plan))
    monkeypatch.setattr(
        pipeline,
        "_execute_plan",
        AsyncMock(return_value=QueryResult(rows=rows, column_names=["n"])),
    )

    result = await tools.run_sql(state, "analyst", "SELECT n FROM t")
    assert result["row_count"] == 2
    assert result["total_rows"] == 10
    assert result["truncated"] is True


async def test_run_sql_permission_error_propagates(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    monkeypatch.setattr(
        pipeline,
        "_govern_and_route",
        AsyncMock(side_effect=PermissionError("denied for role analyst")),
    )
    exec_mock = AsyncMock()
    monkeypatch.setattr(pipeline, "_execute_plan", exec_mock)

    with pytest.raises(PermissionError):
        await tools.run_sql(state, "analyst", "SELECT * FROM secret")
    exec_mock.assert_not_awaited()  # never a silent empty result


async def test_run_sql_missing_role(state):
    with pytest.raises(ValueError):
        await tools.run_sql(state, "", "SELECT 1")


# --- explain_sql ------------------------------------------------------------


async def test_explain_sql_returns_plan_without_executing(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    plan = pipeline._Plan(
        route=Route.ENGINE,
        sql="SELECT 1",
        source_id="pg",
        dialect="trino",
        physical_sql="SELECT 1 /*physical*/",
    )
    govern = AsyncMock(return_value=plan)
    execute = AsyncMock()
    monkeypatch.setattr(pipeline, "_govern_and_route", govern)
    monkeypatch.setattr(pipeline, "_execute_plan", execute)

    result = await tools.explain_sql(state, "analyst", "SELECT * FROM sales.orders")

    govern.assert_awaited_once()
    execute.assert_not_awaited()  # explain is plan-only
    # Physical is internal: no physical_sql, no raw source_id, no dialect ever leaks.
    assert result == {"ok": True}
    assert "physical_sql" not in result
    assert "source_id" not in result


async def test_explain_sql_permission_error_propagates(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    monkeypatch.setattr(
        pipeline,
        "_govern_and_route",
        AsyncMock(side_effect=PermissionError("nope")),
    )
    with pytest.raises(PermissionError):
        await tools.explain_sql(state, "analyst", "SELECT 1")


# --- server wiring ----------------------------------------------------------


async def test_build_mcp_server_registers_tools():
    from provisa.api.mcp.server import build_mcp_server

    mcp = build_mcp_server(_make_state())
    names = {t.name for t in await mcp.list_tools()}
    assert {
        "list_schemas",
        "list_tables",
        "describe_table",
        "run_sql",
        "explain_sql",
        "search_catalog",
    } <= names


async def test_jev_tool_absent_without_api_key(monkeypatch):
    from provisa.api.mcp.server import build_mcp_server

    monkeypatch.delenv("TYPESAFEAI_API_KEY", raising=False)
    mcp = build_mcp_server(_make_state())
    names = {t.name for t in await mcp.list_tools()}
    assert "jev_evaluate" not in names


async def test_jev_tool_present_with_api_key(monkeypatch):
    from provisa.api.mcp.server import build_mcp_server

    monkeypatch.setenv("TYPESAFEAI_API_KEY", "ts_test_key")
    mcp = build_mcp_server(_make_state())
    names = {t.name for t in await mcp.list_tools()}
    assert "jev_evaluate" in names


async def test_jev_evaluate_calls_client_with_configured_key(state, monkeypatch):
    monkeypatch.setenv("TYPESAFEAI_API_KEY", "ts_test_key")
    # resolve_jev_api_key treats tenant_db=None as "no org override, env var only" — the sentinel
    # `object()` this fixture otherwise carries (for _catalog()'s truthiness check) isn't a real
    # Database and would blow up read_org_secret's `.acquire()` call.
    state.tenant_db = None
    mock = AsyncMock(return_value={"answers": {"q1": {"noul": 0.7}}})
    monkeypatch.setattr("provisa.jev.client.evaluate", mock)
    result = await tools.jev_evaluate(
        state, "analyst", {"text": "hi"}, [{"id": "q1", "type": "noul"}]
    )
    assert result == {"answers": {"q1": {"noul": 0.7}}}
    mock.assert_awaited_once_with("ts_test_key", {"text": "hi"}, [{"id": "q1", "type": "noul"}])


async def test_jev_evaluate_requires_role(state, monkeypatch):
    monkeypatch.setenv("TYPESAFEAI_API_KEY", "ts_test_key")
    with pytest.raises(ValueError):
        await tools.jev_evaluate(state, "", None, [{"id": "q1", "type": "noul"}])


async def test_jev_evaluate_fails_loud_without_api_key(state, monkeypatch):
    monkeypatch.delenv("TYPESAFEAI_API_KEY", raising=False)
    state.tenant_db = None  # see note in test_jev_evaluate_calls_client_with_configured_key
    with pytest.raises(ValueError):
        await tools.jev_evaluate(state, "analyst", None, [{"id": "q1", "type": "noul"}])


# --- REQ-1847: GraphQL field-name authority --------------------------------


async def test_graphql_field_names_returns_the_compiled_schemas_real_names(state, monkeypatch):
    from unittest.mock import patch

    with patch(
        "provisa.api.admin._graphql_field_name.resolve_graphql_field_name",
        return_value="s__orders",
    ) as resolver:
        result = await tools.graphql_field_names(state, "analyst", "sales", "orders")

    resolver.assert_called_once_with(domain_id="sales", schema_name="public", table_name="orders")
    assert result["table_field"] == "s__orders"
    assert result["grpc_query_method"] == "QuerySOrders"
    assert result["grpc_aggregate_method"] == "QuerySOrdersAggregate"
    assert result["jsonapi_path"] == "/data/jsonapi/sales/orders?page[size]=20"
    assert result["openapi_path"] == "GET /data/rest/sales/orders?limit=20"
    assert result["columns"] == [
        {"name": "id", "graphql_name": "id"},
        {"name": "customer_id", "graphql_name": "customerId"},
    ]


async def test_graphql_field_names_jsonapi_openapi_paths_use_the_raw_domain_id(state):  # REQ-1851
    # sql-safe schema name ("pet_store") differs from the raw domain id ("pet-store") the
    # JSON:API/OpenAPI routers actually key on — reproduced live: reassembling the path from the
    # sql-safe schema 404s. jsonapi_path/openapi_path must use the raw meta.domain_id instead.
    from unittest.mock import patch

    hyphen_meta = TableMeta(
        table_id=3,
        field_name="pets",
        type_name="Pets",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="pets",
        domain_id="pet-store",
    )
    state.contexts["analyst"].tables["pets"] = hyphen_meta

    async def _fake_describe(_state, _role, _schema, _table):
        return {"columns": []}

    with (
        patch(
            "provisa.api.admin._graphql_field_name.resolve_graphql_field_name",
            return_value="ps__pets",
        ),
        patch.object(tools, "describe_table", _fake_describe),
    ):
        result = await tools.graphql_field_names(state, "analyst", "pet_store", "pets")

    assert result["jsonapi_path"] == "/data/jsonapi/pet-store/pets?page[size]=20"
    assert result["openapi_path"] == "GET /data/rest/pet-store/pets?limit=20"


async def test_graphql_field_names_unknown_table_raises(state):
    with pytest.raises(ValueError):
        await tools.graphql_field_names(state, "analyst", "sales", "ghost")


async def test_graphql_field_names_not_in_any_compiled_schema_raises(state):
    from unittest.mock import patch

    with patch(
        "provisa.api.admin._graphql_field_name.resolve_graphql_field_name",
        return_value=None,
    ):
        with pytest.raises(ValueError):
            await tools.graphql_field_names(state, "analyst", "sales", "orders")


# --- REQ-1848: Cypher field-name authority -----------------------------------


async def test_cypher_field_names_returns_the_compiled_label_maps_real_names(state):
    from unittest.mock import patch

    fake_node = SimpleNamespace(
        label="Sales:Orders",
        id_column="id",
        physical_properties={"id": "id", "customerId": "customer_id"},
    )
    fake_label_map = SimpleNamespace(nodes={"Orders": fake_node})
    with patch(
        "provisa.api.rest.cypher_exec._build_label_map", return_value=fake_label_map
    ) as builder:
        result = await tools.cypher_field_names(state, "analyst", "sales", "orders")

    builder.assert_called_once()
    assert result["label"] == "Sales:Orders"
    assert result["id_property"] == "id"
    assert result["columns"] == [
        {"name": "id", "cypher_property": "id"},
        {"name": "customer_id", "cypher_property": "customerId"},
    ]


async def test_cypher_field_names_unknown_table_raises(state):
    with pytest.raises(ValueError):
        await tools.cypher_field_names(state, "analyst", "sales", "ghost")


async def test_cypher_field_names_not_in_compiled_graph_schema_raises(state):
    from unittest.mock import patch

    fake_label_map = SimpleNamespace(nodes={})
    with patch("provisa.api.rest.cypher_exec._build_label_map", return_value=fake_label_map):
        with pytest.raises(ValueError):
            await tools.cypher_field_names(state, "analyst", "sales", "orders")


# --- REQ-1852: full NL-pipeline generation for all Explore surfaces --------


class _FakeJobStore:
    """Minimal in-process stand-in for InMemoryJobStore/RedisJobStore — just enough for
    generate_explore_queries' put/get round trip."""

    def __init__(self):
        self.job = None

    async def put(self, job):
        self.job = job

    async def get(self, job_id):
        return self.job


async def test_generate_explore_queries_returns_per_target_queries_and_errors(state):
    from unittest.mock import patch

    from provisa.nl.job import BranchResult

    state.config.nl = SimpleNamespace(rate_limit=None)  # no rate_limiter configured -> skipped
    fake_store = _FakeJobStore()

    async def fake_run_nl_job(job_id, question, role, app_state, job_store, llm, strict=False):
        job = await job_store.get(job_id)
        job.branches["sql"] = BranchResult(query="SELECT 1", error=None)
        job.branches["graphql"] = BranchResult(query=None, error="NOT_APPLICABLE")

    with (
        patch("provisa.nl.job.make_job_store", return_value=fake_store),
        patch("provisa.core.org_settings.resolve_org_config", AsyncMock(return_value={})),
        patch("provisa.core.org_secrets.read_org_api_keys", AsyncMock(return_value={})),
        patch("provisa.llm.client.ProvisaLLMClient"),
        patch("provisa.nl.runner.run_nl_job", fake_run_nl_job),
    ):
        result = await tools.generate_explore_queries(state, "analyst", "top orders by revenue")

    assert result["sql"] == {"query": "SELECT 1", "error": None}
    assert result["graphql"] == {"query": None, "error": "NOT_APPLICABLE"}


async def test_generate_explore_queries_enforces_the_same_nl_rate_limit(state):
    state.config.nl = SimpleNamespace(rate_limit=5)
    state.rate_limiter = SimpleNamespace(allow=AsyncMock(return_value=(False, 12.0)))

    with pytest.raises(ValueError, match="rate limit"):
        await tools.generate_explore_queries(state, "analyst", "top orders by revenue")

    state.rate_limiter.allow.assert_called_once_with("rl:nl:analyst", 5, 60.0)


async def test_pinned_stdio_role_requires_env(monkeypatch):
    from provisa.api.mcp import server

    monkeypatch.delenv("PROVISA_MCP_ROLE", raising=False)
    with pytest.raises(ValueError):
        server._pinned_stdio_role()


async def test_mcp_server_not_started_in_high_security_mode(monkeypatch):
    """REQ-693: a tool call hands rows to a model as text, so the port never opens."""
    from provisa.api.mcp import server

    monkeypatch.setenv("PROVISA_MCP_PORT", "8100")
    assert server.start_mcp_server(SimpleNamespace(security_high=True)) is None
