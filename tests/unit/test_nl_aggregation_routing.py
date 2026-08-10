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

from provisa.compiler.sql_types import ColumnRef, CompiledQuery, JoinMeta, TableMeta
from provisa.nl.runner import (
    AggregationPlan,
    _aggregation_plan_from_graphql,
    _generate_grpc_query,
    _generate_jsonapi_query,
    _generate_openapi_query,
    _resolve_aggregation_plan,
)


def _agg_only_compiled(root_field: str = "pets_aggregate") -> CompiledQuery:
    # aggregates.py's count ColumnRef always hardcodes column="count" (it isn't tied to any
    # physical column) — matches field_name so the (physical-name) output key stays "count".
    columns = [ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate")]
    return CompiledQuery(sql="", params=[], root_field=root_field, columns=columns, sources=set())


def _group_by_compiled(root_field: str = "pets_group_by") -> CompiledQuery:
    columns = [
        ColumnRef(alias=None, column="species", field_name="species", nested_in="groupKey"),
        ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate"),
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


def _inquiries_meta() -> TableMeta:
    return TableMeta(
        table_id=2,
        field_name="inquiries",
        type_name="Inquiries",
        source_id="pg1",
        catalog_name="pg1",
        schema_name="public",
        table_name="inquiries",
        domain_id="pet_store",
    )


def _users_meta() -> TableMeta:
    return TableMeta(
        table_id=3,
        field_name="users",
        type_name="Users",
        source_id="pg1",
        catalog_name="pg1",
        schema_name="public",
        table_name="users",
        domain_id="pet_store",
    )


def _ctx(exposed_to_physical=None):
    # SchemaContext always carries exposed_to_physical (sql_types.py::SchemaContext); the stub
    # names it too so the plan can render JSON:API's include paths with the schema's field names.
    return SimpleNamespace(
        tables={"pets": _pets_meta(), "inquiries": _inquiries_meta(), "users": _users_meta()},
        exposed_to_physical=exposed_to_physical or {},
    )


def _app_state(enable_aggregates=True, enable_group_by=True):
    return SimpleNamespace(
        tables=[
            {
                "table_name": "pets",
                "domain_id": "pet_store",
                "enable_aggregates": enable_aggregates,
                "enable_group_by": enable_group_by,
            },
            {
                "table_name": "inquiries",
                "domain_id": "pet_store",
                "enable_aggregates": enable_aggregates,
                "enable_group_by": enable_group_by,
            },
        ]
    )


class TestResolveAggregationPlan:
    def test_group_by_query_resolves_plan(self):
        ctx = _ctx()
        app_state = _app_state()
        sql = "SELECT species, COUNT(*) FROM pet_store.pets GROUP BY species"
        plan = _resolve_aggregation_plan(ctx, app_state, sql)
        assert plan is not None
        meta, group_cols, is_aggregate_only, funcs, dim_paths = (
            plan.meta,
            plan.group_cols,
            plan.is_aggregate_only,
            plan.funcs,
            plan.dim_paths,
        )
        assert meta.table_name == "pets"
        assert group_cols == ["species"]
        assert is_aggregate_only is False
        assert funcs == ["count"]
        assert dim_paths == []

    def test_aggregate_only_query_resolves_plan(self):
        ctx = _ctx()
        app_state = _app_state()
        sql = "SELECT COUNT(*) FROM pet_store.pets"
        plan = _resolve_aggregation_plan(ctx, app_state, sql)
        assert plan is not None
        meta, group_cols, is_aggregate_only, funcs, dim_paths = (
            plan.meta,
            plan.group_cols,
            plan.is_aggregate_only,
            plan.funcs,
            plan.dim_paths,
        )
        assert meta.table_name == "pets"
        assert group_cols == []
        assert is_aggregate_only is True
        assert funcs == ["count"]
        assert dim_paths == []

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

    def test_group_by_on_joined_dimension_resolves_to_aggregate_table_fk(self):
        # "count of inquiries by user": the SQL branch joins users in for a readable name while
        # aggregating inquiries — the aggregate table is inquiries, and GROUP BY users.name must
        # resolve back to inquiries' own FK column (user_id), not bail to a plain fetch.
        ctx = _ctx()
        app_state = _app_state()
        sql = (
            "SELECT users.name, COUNT(inquiries.id) AS inquiry_count "
            "FROM pet_store.users JOIN pet_store.inquiries ON users.id = inquiries.user_id "
            "GROUP BY users.name"
        )
        plan = _resolve_aggregation_plan(ctx, app_state, sql)
        assert plan is not None
        meta, group_cols, is_aggregate_only, funcs, dim_paths = (
            plan.meta,
            plan.group_cols,
            plan.is_aggregate_only,
            plan.funcs,
            plan.dim_paths,
        )
        assert meta.table_name == "inquiries"
        assert group_cols == ["user_id"]
        assert is_aggregate_only is False
        assert funcs == ["count"]
        assert dim_paths == ["user.name"]

    def test_group_by_with_json_agg_detail_excludes_detail_table_from_group_cols(self):
        # "count of inquiries by user with pet details": pets is detail-only, not a grouping
        # dimension, so prompt.py's SQL instructions have the model wrap it in json_agg rather
        # than SELECT it raw (which would force it into GROUP BY too). dim_paths extraction must
        # walk the whole json_build_object(...) subtree (find_all, not find) so both pets columns
        # surface, not just the first.
        ctx = _ctx()
        app_state = _app_state()
        sql = (
            "SELECT users.name, COUNT(inquiries.id) AS inquiry_count, "
            "json_agg(json_build_object('id', pets.id, 'name', pets.name)) AS pet_details "
            "FROM pet_store.users "
            "JOIN pet_store.inquiries ON users.id = inquiries.user_id "
            "JOIN pet_store.pets ON pets.id = inquiries.pet_id "
            "GROUP BY users.name"
        )
        plan = _resolve_aggregation_plan(ctx, app_state, sql)
        assert plan is not None
        meta, group_cols, is_aggregate_only, funcs, dim_paths = (
            plan.meta,
            plan.group_cols,
            plan.is_aggregate_only,
            plan.funcs,
            plan.dim_paths,
        )
        assert meta.table_name == "inquiries"
        assert group_cols == ["user_id"]
        assert is_aggregate_only is False
        assert funcs == ["count"]
        assert sorted(dim_paths) == ["pet.id", "pet.name", "user.name"]

    def test_dim_paths_carry_both_namings_for_renamed_columns(self):
        # gRPC and REST take include paths in the API's own physical naming, JSON:API's ?include=
        # validates against the relationship's GraphQL fields — so a renamed column has to reach
        # the surfaces under two names, or JSON:API answers "Unknown field 'breed_name'".
        ctx = _ctx({(1, "breedName"): "breed_name"})
        app_state = _app_state()
        sql = (
            "SELECT pets.breed_name, COUNT(inquiries.id) "
            "FROM pet_store.inquiries "
            "JOIN pet_store.pets ON pets.id = inquiries.pet_id "
            "GROUP BY pets.breed_name"
        )
        plan = _resolve_aggregation_plan(ctx, app_state, sql)
        assert plan is not None
        assert plan.dim_paths == ["pet.breed_name"]
        assert plan.dim_paths_gql == ["pet.breedName"]
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q is not None and q.endswith("&includeNodes=true&include=pet.breedName")

    def test_group_by_on_unjoined_table_returns_none(self):
        # dimension column belongs to a table with no JOIN condition linking it to the
        # aggregate table — nothing to resolve, must still bail to a plain fetch.
        ctx = _ctx()
        app_state = _app_state()
        sql = "SELECT users.name, COUNT(inquiries.id) FROM pet_store.inquiries, pet_store.users GROUP BY users.name"
        assert _resolve_aggregation_plan(ctx, app_state, sql) is None


class TestGenerateGrpcQuery:
    def test_aggregate_plan_no_funcs_builds_aggregate_query_text(self):
        meta = _pets_meta()
        plan = AggregationPlan(meta, [], True, [], [])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == "QueryPetsAggregate"

    def test_aggregate_plan_with_funcs_includes_funcs_suffix(self):
        # REQ-1359/REQ-1361: {Type}AggregateRequest.funcs restricts to a caller-chosen subset
        # of aggregate functions, mirroring jsonapi/openapi's aggregate=... param.
        meta = _pets_meta()
        plan = AggregationPlan(meta, [], True, ["count"], [])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == "QueryPetsAggregate(funcs=[count])"

    def test_group_by_plan_no_funcs_builds_group_by_query_text(self):
        meta = _pets_meta()
        plan = AggregationPlan(meta, ["species"], False, [], [])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == "QueryPetsGroupBy(by=[species])"

    def test_group_by_plan_with_funcs_includes_funcs_suffix(self):
        # REQ-1359/REQ-1361: {Type}GroupByRequest.funcs restricts to a caller-chosen subset
        # of aggregate functions, same as the aggregate-only case.
        meta = _pets_meta()
        plan = AggregationPlan(meta, ["species"], False, ["count"], [])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == "QueryPetsGroupBy(by=[species], funcs=[count])"

    def test_group_by_plan_with_dim_paths_builds_include_nodes_call(self):
        # REQ-1408: GroupByRequest.include carries the nodes projection as dot-paths, so the
        # requested relation columns survive instead of collapsing to the relation name.
        meta = _pets_meta()
        plan = AggregationPlan(meta, ["user_id"], False, ["count"], ["user.name", "user.email"])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == (
            "QueryPetsGroupBy(by=[user_id], funcs=[count], include_nodes=true, "
            "include=[user.name, user.email])"
        )

    def test_group_by_plan_with_multiple_dim_relations_keeps_each_path(self):
        meta = _pets_meta()
        plan = AggregationPlan(meta, ["user_id", "vet_id"], False, [], ["user.name", "vet.name"])
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == (
            "QueryPetsGroupBy(by=[user_id, vet_id], include_nodes=true, "
            "include=[user.name, vet.name])"
        )

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
        plan = AggregationPlan(meta, [], True, [], [])
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == "/data/jsonapi/pet_store/pets?aggregate=true"

    def test_aggregate_plan_with_funcs_restricts_aggregate_param(self):
        # REQ-1361: aggregate=count,sum instead of aggregate=true.
        meta = _pets_meta()
        plan = AggregationPlan(meta, [], True, ["count"], [])
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == "/data/jsonapi/pet_store/pets?aggregate=count"

    def test_group_by_plan_no_funcs_builds_group_by_query_text(self):
        meta = _pets_meta()
        plan = AggregationPlan(meta, ["species"], False, [], [])
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == "/data/jsonapi/pet_store/pets?groupBy=species&aggregate=true&includeNodes=true"

    def test_group_by_plan_with_funcs_restricts_aggregate_param(self):
        # REQ-1361: aggregate=count in group-by query when SQL only called COUNT.
        meta = _pets_meta()
        plan = AggregationPlan(meta, ["species"], False, ["count"], [])
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == "/data/jsonapi/pet_store/pets?groupBy=species&aggregate=count&includeNodes=true"

    def test_no_plan_falls_back_to_old_behavior(self):
        nm = SimpleNamespace(type_name="DimPet", domain_id="pet_store", table_name="dim_pet")
        q, e = _generate_jsonapi_query(None, {"DimPet"}, {"DimPet": nm})
        assert e is None
        assert q == "/data/jsonapi/pet_store/dim_pet?page[size]=20"


class TestGenerateOpenapiQuery:
    def test_aggregate_plan_no_funcs_builds_aggregate_query_text(self):
        meta = _pets_meta()
        plan = AggregationPlan(meta, [], True, [], [])
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == "GET /data/rest/pet_store/pets?aggregate=true"

    def test_aggregate_plan_with_funcs_restricts_aggregate_param(self):
        # REQ-1361: aggregate=count instead of aggregate=true.
        meta = _pets_meta()
        plan = AggregationPlan(meta, [], True, ["count"], [])
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == "GET /data/rest/pet_store/pets?aggregate=count"

    def test_group_by_plan_no_funcs_builds_group_by_query_text(self):
        meta = _pets_meta()
        plan = AggregationPlan(meta, ["species"], False, [], [])
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == "GET /data/rest/pet_store/pets?groupBy=species&aggregate=true&includeNodes=true"

    def test_group_by_plan_with_funcs_restricts_aggregate_param(self):
        # REQ-1361: aggregate=count in group-by query when SQL only called COUNT.
        meta = _pets_meta()
        plan = AggregationPlan(meta, ["species"], False, ["count"], [])
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == "GET /data/rest/pet_store/pets?groupBy=species&aggregate=count&includeNodes=true"

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
        # _execute_grpc calls _grpc_group_by_graphql_text(ctx, "Pets", ["species"], None, False, None) —
        # funcs is None and include_nodes/include are absent when the query text carries no
        # funcs=/include_nodes= suffix.
        fake_text.assert_called_once_with(
            app_state.contexts["admin"], "Pets", ["species"], None, False, None
        )
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
        # aggregate=true, no includeNodes param → funcs=None, include_nodes=False
        fake_text.assert_called_once_with(
            app_state.contexts["admin"], "Pets", ["species"], None, False, []
        )
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

    async def test_execute_jsonapi_group_by_with_include_nodes_matches_trailing_param(self):
        # Regression for the live bug (screenshots, 2026-08-08): runner.py always appends
        # "&includeNodes=true" after "&aggregate=..." for group-by NL queries. The regex used
        # to be anchored with $ right after aggregate=([^&]+), so it never matched a query
        # string with a trailing &includeNodes=true and silently fell through to a plain
        # unaggregated SELECT * LIMIT 20. Assert the RESPONSE SHAPE is grouped/aggregated and
        # carries populated "nodes" — not just that includeNodes=true appears in the request URL.
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        cq = _group_by_compiled()
        nodes_columns = [
            ColumnRef(alias=None, column="species", field_name="species", nested_in="__join_key__"),
            ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
        ]
        cq.nodes_columns = nodes_columns
        with (
            patch.object(
                executor,
                "_grpc_group_by_graphql_text",
                return_value="{ pets_group_by(by: [species]) { groupKey nodes { name } } }",
            ) as fake_text,
            patch.object(
                executor,
                "_compile_and_execute_graphql",
                new=AsyncMock(
                    return_value=[(cq, _FakeResult([("dog", 3)]), _FakeResult([("dog", "Rex")]))]
                ),
            ),
        ):
            result = await executor._execute_jsonapi(
                "/data/jsonapi/pet_store/pets?groupBy=species&aggregate=true&includeNodes=true",
                "admin",
                app_state,
            )
        fake_text.assert_called_once_with(
            app_state.contexts["admin"], "Pets", ["species"], None, True, []
        )
        assert result == {
            "data": [
                {
                    "type": "petsGroupBy",
                    "id": "0",
                    "attributes": {
                        "groupKey": {"species": "dog"},
                        "aggregate": {"count": 3},
                        "nodes": [{"name": "Rex"}],
                    },
                }
            ]
        }

    async def test_execute_openapi_group_by_with_include_nodes_matches_trailing_param(self):
        from provisa.nl import executor

        app_state = SimpleNamespace(contexts={"admin": _ctx()})
        cq = _group_by_compiled()
        nodes_columns = [
            ColumnRef(alias=None, column="species", field_name="species", nested_in="__join_key__"),
            ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
        ]
        cq.nodes_columns = nodes_columns
        with (
            patch.object(
                executor,
                "_grpc_group_by_graphql_text",
                return_value="{ pets_group_by(by: [species]) { groupKey nodes { name } } }",
            ) as fake_text,
            patch.object(
                executor,
                "_compile_and_execute_graphql",
                new=AsyncMock(
                    return_value=[(cq, _FakeResult([("dog", 3)]), _FakeResult([("dog", "Rex")]))]
                ),
            ),
        ):
            result = await executor._execute_openapi(
                "GET /data/rest/pet_store/pets?groupBy=species&aggregate=true&includeNodes=true",
                "admin",
                app_state,
            )
        fake_text.assert_called_once_with(
            app_state.contexts["admin"], "Pets", ["species"], None, True, []
        )
        assert result == {
            "data": [
                {"groupKey": {"species": "dog"}, "aggregate": {"count": 3}, "nodes": [{"name": "Rex"}]}
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
        # aggregate=true, no includeNodes param → funcs=None, include_nodes=False
        fake_text.assert_called_once_with(
            app_state.contexts["admin"], "Pets", ["species"], None, False, []
        )
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


class TestAggregationPlanFromGraphql:
    """REQ-1408: in strict mode the protocol branches rewrite the schema-validated GraphQL
    query directly. Parsing the compiled SQL instead cannot see the base table through the
    merged group-by + nodes subquery, so every surface degraded to a plain row fetch."""

    def _strict_ctx(self):
        inquiries = TableMeta(
            table_id=2,
            field_name="ps__inquiries",
            type_name="PS__Inquiries",
            source_id="pg1",
            catalog_name="pg1",
            schema_name="public",
            table_name="inquiries",
            domain_id="pet-store",
        )
        users = TableMeta(
            table_id=3,
            field_name="ps__users",
            type_name="PS__Users",
            source_id="pg1",
            catalog_name="pg1",
            schema_name="public",
            table_name="users",
            domain_id="pet-store",
        )
        pets = TableMeta(
            table_id=1,
            field_name="ps__pets",
            type_name="PS__Pets",
            source_id="pg1",
            catalog_name="pg1",
            schema_name="public",
            table_name="pets",
            domain_id="pet-store",
        )
        join = lambda target: JoinMeta(  # noqa: E731
            source_column="x",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=target,
            cardinality="many-to-one",
        )
        return SimpleNamespace(
            tables={"ps__inquiriesGroupBy": inquiries},
            joins={
                ("PS__Inquiries", "user"): join(users),
                ("PS__Inquiries", "pet"): join(pets),
            },
            exposed_to_physical={
                (2, "userId"): "user_id",
                (2, "inquiryType"): "inquiry_type",
                (2, "submittedAt"): "submitted_at",
                (1, "breedName"): "breed_name",
            },
        )

    GQL = """
    query InquiriesCountByUser {
      ps__inquiriesGroupBy(by: [userId]) {
        groupKey
        aggregate { count }
        nodes {
          id
          inquiryType
          status
          submittedAt
          message
          user { id name email phone }
          pet { id name species breedName price available }
        }
      }
    }
    """

    def test_plan_mirrors_graphql_shape(self):
        plan = _aggregation_plan_from_graphql(self._strict_ctx(), self.GQL)
        assert plan is not None
        assert plan.meta.table_name == "inquiries"
        assert plan.group_cols == ["user_id"]
        assert plan.is_aggregate_only is False
        assert plan.funcs == ["count"]
        assert plan.node_scalars == [
            "id",
            "inquiry_type",
            "status",
            "submitted_at",
            "message",
        ]
        assert plan.dim_paths == [
            "user.id",
            "user.name",
            "user.email",
            "user.phone",
            "pet.id",
            "pet.name",
            "pet.species",
            "pet.breed_name",
            "pet.price",
            "pet.available",
        ]

    def test_grpc_query_carries_group_by_and_relations(self):
        plan = _aggregation_plan_from_graphql(self._strict_ctx(), self.GQL)
        q, e = _generate_grpc_query(plan, set(), {})
        assert e is None
        assert q == (
            "QueryPsInquiriesGroupBy(by=[user_id], funcs=[count], include_nodes=true, "
            "include=[id, inquiry_type, status, submitted_at, message, "
            "user.id, user.name, user.email, user.phone, "
            "pet.id, pet.name, pet.species, pet.breed_name, pet.price, pet.available])"
        )

    def test_jsonapi_query_splits_include_nodes_flag_from_include_list(self):
        # JSON:API honours includeNodes only as true/1 and takes the projection via ?include=,
        # whose entries are dot-paths — naming the relationship alone would widen the nodes
        # selection to every column of the related table. Those dot-paths carry the schema's
        # field names (pet.breedName), not the physical ones gRPC and REST use: ?include= is
        # validated against the relationship's GraphQL fields, so breed_name is rejected.
        plan = _aggregation_plan_from_graphql(self._strict_ctx(), self.GQL)
        q, e = _generate_jsonapi_query(plan, set(), {})
        assert e is None
        assert q == (
            "/data/jsonapi/pet-store/inquiries?groupBy=user_id&aggregate=count"
            "&includeNodes=true&include="
            "user.id,user.name,user.email,user.phone,"
            "pet.id,pet.name,pet.species,pet.breedName,pet.price,pet.available"
        )

    def test_openapi_query_uses_dot_paths(self):
        # REST has no ?include= — the whole projection rides on ?includeNodes= as dot-paths.
        plan = _aggregation_plan_from_graphql(self._strict_ctx(), self.GQL)
        q, e = _generate_openapi_query(plan, set(), {})
        assert e is None
        assert q == (
            "GET /data/rest/pet-store/inquiries?groupBy=user_id&aggregate=count&includeNodes="
            "id,inquiry_type,status,submitted_at,message,"
            "user.id,user.name,user.email,user.phone,"
            "pet.id,pet.name,pet.species,pet.breed_name,pet.price,pet.available"
        )

    def test_unknown_root_field_returns_none(self):
        assert _aggregation_plan_from_graphql(self._strict_ctx(), "{ ps__orders { id } }") is None


class TestIncludeRelations:
    """REQ-1408: both surfaces' URLs reduce to the relation list the group-by GraphQL text
    expands into ``nodes { rel { ... } }``, despite naming it differently."""

    def test_jsonapi_include_param(self):
        from provisa.nl.executor import _include_relations

        q = (
            "/data/jsonapi/pet-store/inquiries?groupBy=user_id&aggregate=count"
            "&includeNodes=true&include=user.name,user.email,pet.name"
        )
        assert _include_relations(q) == ["user.name", "user.email", "pet.name"]

    def test_rest_dot_paths(self):
        from provisa.nl.executor import _include_relations

        q = (
            "GET /data/rest/pet-store/inquiries?groupBy=user_id&aggregate=count"
            "&includeNodes=id,status,user.name,user.email,pet.name"
        )
        assert _include_relations(q) == ["id", "status", "user.name", "user.email", "pet.name"]

    def test_flag_only_has_no_relations(self):
        from provisa.nl.executor import _include_relations

        assert _include_relations("/data/jsonapi/d/t?groupBy=a&aggregate=true&includeNodes=true") == []
