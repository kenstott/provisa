# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7f5c19-4b8e-4a1d-9c3f-7e2d6b1a9f04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for REQ-1359's NL-layer aggregation routing.

The NL Explore UI's gRPC/JSON:API/OpenAPI panels used to always emit a plain
"fetch N rows" query against the alphabetically-first table from the shared
LLM table-selection set, ignoring both the correct table and any GROUP BY/aggregation
intent the SQL branch's own LLM call already resolved. These tests cover:

  * runner.py's _resolve_aggregation_plan — parses the SQL branch's compiled semantic SQL
    to derive (table, group_by_columns, is_aggregate_only, funcs) — a 4-tuple (REQ-1361:
    funcs is the AGG_FUNCS-ordered subset actually called in the SQL, so protocol branches
    restrict their synthesized requests to the same functions the SQL branch used).
  * runner.py's _generate_grpc/jsonapi/openapi_query — build aggregate/group-by query text
    from a resolved plan (including funcs suffix when non-empty, REQ-1361), or fall back to
    the old plain-fetch text when there's no plan.
  * executor.py's _execute_grpc/_execute_jsonapi/_execute_openapi — route aggregate/group-by
    query text through the same compile_query pipeline the graphql branch uses.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from provisa.compiler.sql_types import ColumnRef, CompiledQuery, TableMeta
from provisa.nl.runner import (
    _generate_grpc_query,
    _generate_jsonapi_query,
    _generate_openapi_query,
    _resolve_aggregation_plan,
)


def _agg_only_compiled(root_field: str = "pets_aggregate") -> CompiledQuery:
    columns = [ColumnRef(alias=None, column="id", field_name="count", nested_in="aggregate")]
    return CompiledQuery(sql="", params=[], root_field=root_field, columns=columns, sources=set())


def _group_by_compiled(root_field: str = "pets_group_by") -> CompiledQuery:
    columns = [
        ColumnRef(alias=None, column="species", field_name="species", nested_in="groupKey"),
        ColumnRef(alias=None, column="id", field_name="count", nested_in="aggregate"),
    ]
    return CompiledQuery(sql="", params=[], root_field=root_field, columns=columns, sources=set())


class _FakeResult:
    def __init__(self, rows):
        self.rows = rows


def _pets_meta() -> TableMeta:
    return TableMeta(
        table_id=1,
        field_name="pets",
        type_name="Pets",
        source_id="pg1",
        catalog_name="pg1",
        schema_name="public",
        table_name="pets",
        domain_id="pet_store",
    )


def _ctx():
    return SimpleNamespace(tables={"pets": _pets_meta()})


def _app_state(enable_aggregates=True, enable_group_by=True):
    return SimpleNamespace(
        tables=[
            {
                "table_name": "pets",
                "domain_id": "pet_store",
                "enable_aggregates": enable_aggregates,
                "enable_group_by": enable_group_by,
            }
        ]
    )


class TestResolveAggregationPlan:
    def test_group_by_query_resolves_plan(self):
        ctx = _ctx()
        app_state = _app_state()
        sql = "SELECT species, COUNT(*) FROM pet_store.pets GROUP BY species"
        plan = _resolve_aggregation_plan(ctx, app_state, sql)
        assert plan is not None
        meta, group_cols, is_aggregate_only, funcs = plan
        assert meta.table_name == "pets"
        assert group_cols == ["species"]
        assert is_aggregate_only is False
        assert funcs == ["count"]

    def test_aggregate_only_query_resolves_plan(self):
        ctx = _ctx()
        app_state = _app_state()
        sql = "SELECT COUNT(*) FROM pet_store.pets"
        plan = _resolve_aggregation_plan(ctx, app_state, sql)
        assert plan is not None
        meta, group_cols, is_aggregate_only, funcs = plan
        assert meta.table_name == "pets"
        assert group_cols == []
        assert is_aggregate_only is True
        assert funcs == ["count"]

    def test_plain_select_returns_none(self):
        ctx = _ctx()
        app_state = _app_state()
        sql = "SELECT id, name FROM pet_store.pets LIMIT 20"
        assert _resolve_aggregation_plan(ctx, app_state, sql) is None

    def test_unknown_table_returns_none(self):
        ctx = _ctx()
        app_state = _app_state()
        sql = "SELECT COUNT(*) FROM pet_store.dim_pet"
        assert _resolve_aggregation_plan(ctx, app_state, sql) is None

    def test_group_by_disabled_returns_none(self):
        ctx = _ctx()
        app_state = _app_state(enable_group_by=False)
        sql = "SELECT species, COUNT(*) FROM pet_store.pets GROUP BY species"
        assert _resolve_aggregation_plan(ctx, app_state, sql) is None

    def test_aggregates_disabled_returns_none(self):
        ctx = _ctx()
        app_state = _app_state(enable_aggregates=False)
        sql = "SELECT COUNT(*) FROM pet_store.pets"
        assert _resolve_aggregation_plan(ctx, app_state, sql) is None

    def test_unparseable_sql_returns_none(self):
        ctx = _ctx()
        app_state = _app_state()
        assert _resolve_aggregation_plan(ctx, app_state, "NOT VALID SQL @@@") is None


class TestGenerateGrpcQuery:
    def test_aggregate_plan_no_funcs_builds_aggregate_query_text(self):
        meta = _pets_meta()
        plan = (meta, [], True, [])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == "QueryPetsAggregate"

    def test_aggregate_plan_with_funcs_appends_funcs_suffix(self):
        # REQ-1361: when the SQL branch called only COUNT, restrict the synthesized request.
        meta = _pets_meta()
        plan = (meta, [], True, ["count"])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == "QueryPetsAggregate(funcs=[count])"

    def test_group_by_plan_no_funcs_builds_group_by_query_text(self):
        meta = _pets_meta()
        plan = (meta, ["species"], False, [])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == "QueryPetsGroupBy(by=[species])"

    def test_group_by_plan_with_funcs_appends_funcs_suffix(self):
        # REQ-1361: by and funcs both present.
        meta = _pets_meta()
        plan = (meta, ["species"], False, ["count"])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == "QueryPetsGroupBy(by=[species], funcs=[count])"

    def test_no_plan_falls_back_to_old_behavior(self):
        nm = SimpleNamespace(type_name="DimPet")
        q, e = _generate_grpc_query(None, {"DimPet"}, {"DimPet": nm})
        assert e is None
        assert q == "QueryDimPet"

    def test_no_plan_no_selection_is_not_applicable(self):
        q, e = _generate_grpc_query(None, set(), {})
        assert q is None
        assert e == "NOT_APPLICABLE"


class TestGenerateJsonapiQuery:
    def test_aggregate_plan_no_funcs_builds_aggregate_query_text(self):
        meta = _pets_meta()
        plan = (meta, [], True, [])
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == "/data/jsonapi/pet_store/pets?aggregate=true"

    def test_aggregate_plan_with_funcs_restricts_aggregate_param(self):
        # REQ-1361: aggregate=count,sum instead of aggregate=true.
        meta = _pets_meta()
        plan = (meta, [], True, ["count"])
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == "/data/jsonapi/pet_store/pets?aggregate=count"

    def test_group_by_plan_no_funcs_builds_group_by_query_text(self):
        meta = _pets_meta()
        plan = (meta, ["species"], False, [])
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == "/data/jsonapi/pet_store/pets?groupBy=species&aggregate=true"

    def test_group_by_plan_with_funcs_restricts_aggregate_param(self):
        # REQ-1361: aggregate=count in group-by query when SQL only called COUNT.
        meta = _pets_meta()
        plan = (meta, ["species"], False, ["count"])
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == "/data/jsonapi/pet_store/pets?groupBy=species&aggregate=count"

    def test_no_plan_falls_back_to_old_behavior(self):
        nm = SimpleNamespace(type_name="DimPet", domain_id="pet_store", table_name="dim_pet")
        q, e = _generate_jsonapi_query(None, {"DimPet"}, {"DimPet": nm})
        assert e is None
        assert q == "/data/jsonapi/pet_store/dim_pet?page[size]=20"


class TestGenerateOpenapiQuery:
    def test_aggregate_plan_no_funcs_builds_aggregate_query_text(self):
        meta = _pets_meta()
        plan = (meta, [], True, [])
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == "GET /data/rest/pet_store/pets?aggregate=true"

    def test_aggregate_plan_with_funcs_restricts_aggregate_param(self):
        # REQ-1361: aggregate=count instead of aggregate=true.
        meta = _pets_meta()
        plan = (meta, [], True, ["count"])
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == "GET /data/rest/pet_store/pets?aggregate=count"

    def test_group_by_plan_no_funcs_builds_group_by_query_text(self):
        meta = _pets_meta()
        plan = (meta, ["species"], False, [])
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == "GET /data/rest/pet_store/pets?groupBy=species&aggregate=true"

    def test_group_by_plan_with_funcs_restricts_aggregate_param(self):
        # REQ-1361: aggregate=count in group-by query when SQL only called COUNT.
        meta = _pets_meta()
        plan = (meta, ["species"], False, ["count"])
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == "GET /data/rest/pet_store/pets?groupBy=species&aggregate=count"

    def test_no_plan_falls_back_to_old_behavior(self):
        nm = SimpleNamespace(type_name="DimPet", domain_id="pet_store", table_name="dim_pet")
        q, e = _generate_openapi_query(None, {"DimPet"}, {"DimPet": nm})
        assert e is None
        assert q == "GET /data/rest/pet_store/dim_pet"


class TestExecutorRouting:
    """executor.py's _execute_grpc/_execute_jsonapi/_execute_openapi must route
    aggregate/group-by query text through _compile_and_execute_graphql (the shared
    compile_query pipeline), not the raw SELECT ... LIMIT 20 path, and must shape the
    result into each protocol's real response envelope rather than GraphQL's raw body."""

    async def test_execute_grpc_aggregate_routes_through_graphql(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        cq = _agg_only_compiled()
        with (
            patch.object(
                executor, "_grpc_aggregate_graphql_text", return_value="{ pets_aggregate { count } }"
            ) as fake_text,
            patch.object(
                executor,
                "_compile_and_execute_graphql",
                new=AsyncMock(return_value=[(cq, _FakeResult([(5,)]), None)]),
            ) as fake_exec,
        ):
            result = await executor._execute_grpc("QueryPetsAggregate", "admin", app_state)
        # _execute_grpc calls _grpc_aggregate_graphql_text(ctx, "Pets", None) — funcs is None
        # when the query text carries no funcs= suffix (aggregate=true case).
        fake_text.assert_called_once_with(app_state.contexts["admin"], "Pets", None)
        fake_exec.assert_awaited_once_with("{ pets_aggregate { count } }", "admin", app_state)
        # split_agg_columns: nested_in="aggregate" → len==1 → top-level scalar
        assert result == {"count": 5}

    async def test_execute_grpc_group_by_routes_through_graphql(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        cq = _group_by_compiled()
        with (
            patch.object(
                executor,
                "_grpc_group_by_graphql_text",
                return_value="{ pets_group_by(by: [species]) { groupKey } }",
            ) as fake_text,
            patch.object(
                executor,
                "_compile_and_execute_graphql",
                new=AsyncMock(return_value=[(cq, _FakeResult([("dog", 3)]), None)]),
            ) as fake_exec,
        ):
            result = await executor._execute_grpc("QueryPetsGroupBy(by=[species])", "admin", app_state)
        # _execute_grpc calls _grpc_group_by_graphql_text(ctx, "Pets", ["species"], None) —
        # funcs is None when the query text carries no funcs= suffix.
        fake_text.assert_called_once_with(app_state.contexts["admin"], "Pets", ["species"], None)
        fake_exec.assert_awaited_once()
        # split_group_by_columns: species→groupKey idx, count→agg idx.
        # split_agg_columns on agg cols: nested_in="aggregate" → len==1 → top scalar.
        assert result == {"group_by": [{"group_key": '{"species": "dog"}', "aggregate": {"count": 3}}]}

    async def test_execute_grpc_plain_falls_back_to_sql(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        with (
            patch(
                "provisa.grpc.query_ir.grpc_table_to_semantic_sql",
                return_value="SELECT * FROM pet_store.pets LIMIT 20",
            ),
            patch.object(executor, "_execute_sql", new=AsyncMock(return_value={"columns": [], "rows": []})) as fake_exec,
        ):
            await executor._execute_grpc("QueryPets", "admin", app_state)
        fake_exec.assert_awaited_once()

    async def test_execute_jsonapi_aggregate_routes_through_graphql(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        cq = _agg_only_compiled()
        with (
            patch.object(
                executor, "_grpc_aggregate_graphql_text", return_value="{ pets_aggregate { count } }"
            ) as fake_text,
            patch.object(
                executor,
                "_compile_and_execute_graphql",
                new=AsyncMock(return_value=[(cq, _FakeResult([(5,)]), None)]),
            ) as fake_exec,
        ):
            result = await executor._execute_jsonapi(
                "/data/jsonapi/pet_store/pets?aggregate=true", "admin", app_state
            )
        # aggregate=true → _parse_aggregate_funcs("true") = None → called with (ctx, "Pets", None)
        fake_text.assert_called_once_with(app_state.contexts["admin"], "Pets", None)
        fake_exec.assert_awaited_once_with("{ pets_aggregate { count } }", "admin", app_state)
        # serialize_aggregate: nested_in="aggregate" → agg_alias path → {"count": 5}
        # _execute_domain_table_aggregate wraps as {"data": None, "meta": {"aggregate": ...}}
        assert result == {"data": None, "meta": {"aggregate": {"count": 5}}}

    async def test_execute_jsonapi_group_by_routes_through_graphql(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        cq = _group_by_compiled()
        with (
            patch.object(
                executor,
                "_grpc_group_by_graphql_text",
                return_value="{ pets_group_by(by: [species]) { groupKey } }",
            ) as fake_text,
            patch.object(
                executor,
                "_compile_and_execute_graphql",
                new=AsyncMock(return_value=[(cq, _FakeResult([("dog", 3)]), None)]),
            ) as fake_exec,
        ):
            result = await executor._execute_jsonapi(
                "/data/jsonapi/pet_store/pets?groupBy=species&aggregate=true", "admin", app_state
            )
        # aggregate=true → _parse_aggregate_funcs("true") = None → called with (ctx, "Pets", ["species"], None)
        fake_text.assert_called_once_with(app_state.contexts["admin"], "Pets", ["species"], None)
        fake_exec.assert_awaited_once()
        # serialize_group_by → [{"groupKey": {"species": "dog"}, "aggregate": {"count": 3}}]
        # jsonapi wraps each row as typed-resourceless JSON:API object.
        assert result == {
            "data": [
                {
                    "type": "petsGroupBy",
                    "id": "0",
                    "attributes": {"groupKey": {"species": "dog"}, "aggregate": {"count": 3}},
                }
            ]
        }

    async def test_execute_jsonapi_plain_falls_back_to_domain_table(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        with patch.object(
            executor, "_execute_domain_table", new=AsyncMock(return_value={"columns": [], "rows": []})
        ) as fake_exec:
            await executor._execute_jsonapi(
                "/data/jsonapi/pet_store/pets?page[size]=20", "admin", app_state
            )
        fake_exec.assert_awaited_once_with("pet_store", "pets", "admin", app_state)

    async def test_execute_openapi_aggregate_routes_through_graphql(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        cq = _agg_only_compiled()
        with (
            patch.object(
                executor, "_grpc_aggregate_graphql_text", return_value="{ pets_aggregate { count } }"
            ) as fake_text,
            patch.object(
                executor,
                "_compile_and_execute_graphql",
                new=AsyncMock(return_value=[(cq, _FakeResult([(5,)]), None)]),
            ) as fake_exec,
        ):
            result = await executor._execute_openapi(
                "GET /data/rest/pet_store/pets?aggregate=true", "admin", app_state
            )
        # aggregate=true → _parse_aggregate_funcs("true") = None → called with (ctx, "Pets", None)
        fake_text.assert_called_once_with(app_state.contexts["admin"], "Pets", None)
        fake_exec.assert_awaited_once_with("{ pets_aggregate { count } }", "admin", app_state)
        # _execute_domain_table_aggregate for openapi: {"data": agg_payload}
        assert result == {"data": {"count": 5}}

    async def test_execute_openapi_group_by_routes_through_graphql(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        cq = _group_by_compiled()
        with (
            patch.object(
                executor,
                "_grpc_group_by_graphql_text",
                return_value="{ pets_group_by(by: [species]) { groupKey } }",
            ) as fake_text,
            patch.object(
                executor,
                "_compile_and_execute_graphql",
                new=AsyncMock(return_value=[(cq, _FakeResult([("dog", 3)]), None)]),
            ) as fake_exec,
        ):
            result = await executor._execute_openapi(
                "GET /data/rest/pet_store/pets?groupBy=species&aggregate=true", "admin", app_state
            )
        # aggregate=true → _parse_aggregate_funcs("true") = None → called with (ctx, "Pets", ["species"], None)
        fake_text.assert_called_once_with(app_state.contexts["admin"], "Pets", ["species"], None)
        fake_exec.assert_awaited_once()
        # _execute_domain_table_aggregate for openapi: {"data": group_rows}
        assert result == {"data": [{"groupKey": {"species": "dog"}, "aggregate": {"count": 3}}]}

    async def test_execute_openapi_plain_falls_back_to_domain_table(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        with patch.object(
            executor, "_execute_domain_table", new=AsyncMock(return_value={"columns": [], "rows": []})
        ) as fake_exec:
            await executor._execute_openapi("GET /data/rest/pet_store/pets", "admin", app_state)
        fake_exec.assert_awaited_once_with("pet_store", "pets", "admin", app_state)
