# Copyright (c) 2026 Kenneth Stott
# Canary: 1502dfb2-2fea-4581-af9d-68de77a734d5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's Preview and Profile are read as their caller, in the table's own domain — and a
withdrawn table can be previewed by the people who decide to put it back (REQ-1921, REQ-452).

A withdrawn table is out of production: in no role's schema, refused to every statement that
names it. The table detail's Preview and Profile read it all the same, for a caller who holds the
right putting it back needs, through one entry point that writes the statement itself and governs
it against the caller's role with that one table added. These tests run that on the real role build and the
real governance stage, with routing stood in for: what reaches routing is what would be read.
"""

# Requirements: REQ-1921, REQ-452

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.admin import table_read
from provisa.api.errors import ApiError
from provisa.api.role_surface import role_surface
from tests.unit.test_empty_domain_access_reaches_nothing import SALES, _built_roles, _tables

_REFUNDS = 9
_ROLE = {"id": "r", "capabilities": ["usage"], "domain_access": ["sales"]}


def _refunds(**over) -> dict:
    """A registered table of the sales domain that is withdrawn."""
    return {
        "id": _REFUNDS,
        "domain_id": "sales",
        "source_id": "pg",
        "schema_name": "public",
        "table_name": "refunds",
        "alias": None,
        "withdrawn": True,
        "source_withdrawn": False,
        "region": None,
        "columns": [
            {
                "column_name": "id",
                "data_type": "integer",
                "visible_to": ["r"],
                "native_filter_type": None,
                "subtype": None,
            },
            {
                "column_name": "note",
                "data_type": "varchar",
                "visible_to": ["r"],
                "native_filter_type": None,
                "subtype": None,
            },
        ],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
        **over,
    }


class _Routed(Exception):
    """The statement cleared governance and reached routing; ``sql`` is what it would read."""

    def __init__(self, sql: str) -> None:
        super().__init__(sql)
        self.sql = sql


@pytest.fixture
def multi_domain(monkeypatch):
    monkeypatch.setenv("PROVISA_SINGLE_DOMAIN", "false")


@pytest.fixture
def state(multi_domain, monkeypatch):
    """The real state after the real role build, with ``refunds`` registered and withdrawn."""
    import provisa.api.app as appmod

    for name in ("meta_roles", "meta_resolved_at", "masking_rules"):
        monkeypatch.setattr(appmod.state._active_runtime(), name, {}, raising=False)
    built = _built_roles(monkeypatch, [dict(_ROLE)])
    # As a rebuild publishes them beside the role build: the tables in service.
    monkeypatch.setattr(built, "tables", _tables(), raising=False)
    built.role_build_inputs["withdrawn_tables"] = [_refunds()]
    return built


@pytest.fixture
def routing():
    """Routing stood in for: raises ``_Routed`` with the governed statement it was handed."""
    from provisa.pgwire import _pipeline

    async def _route(*args, **kwargs):
        raise _Routed(str(kwargs.get("governed_sql") or (args[0] if args else "")))

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)) as mock:
        yield mock


async def _ordinary(sql: str) -> None:
    from provisa.pgwire import _pipeline

    await _pipeline._govern_and_route(sql, "r")


_STATEMENT = 'SELECT * FROM "sales"."refunds" LIMIT 5'


async def test_an_ordinary_statement_naming_the_withdrawn_table_is_refused(state, routing):
    with pytest.raises(PermissionError, match="not a registered table"):
        await _ordinary(_STATEMENT)
    routing.assert_not_awaited()


async def test_the_steward_read_of_a_withdrawn_table_reaches_routing_governed(state, routing):
    with pytest.raises(_Routed):
        await table_read.read_as_caller(state, _REFUNDS, "r", limit=5)
    routing.assert_awaited_once()


async def test_the_same_words_are_still_refused_after_a_steward_read(state, routing):
    """Nothing of the steward's read is kept where an ordinary statement would find it: not a
    governed plan, not a validation, not the table among the role's tables."""
    tables_before, surface_before = list(state.tables), state.surfaces["r"]
    kept_before = (
        len(state.compiled_query_cache._store)
        if hasattr(state.compiled_query_cache, "_store")
        else None
    )
    with pytest.raises(_Routed):
        await table_read.read_as_caller(state, _REFUNDS, "r", limit=5)
    routing.reset_mock()

    with pytest.raises(PermissionError, match="not a registered table"):
        await _ordinary(_STATEMENT)
    routing.assert_not_awaited()
    assert state.tables == tables_before and _REFUNDS not in [t["id"] for t in state.tables]
    assert state.surfaces["r"] is surface_before
    assert "refunds" not in role_surface(state, "r").context.tables
    if kept_before is not None:
        assert len(state.compiled_query_cache._store) == kept_before


async def test_a_withdrawn_table_is_read_under_the_callers_own_masks(state, routing):
    from provisa.security.masking import MaskingRule, MaskType

    state.masking_rules = {
        (_REFUNDS, "r"): {
            "note": (MaskingRule(mask_type=MaskType.constant, value="MASKED"), "varchar")
        }
    }
    with pytest.raises(_Routed):
        await table_read.read_as_caller(state, _REFUNDS, "r", limit=5)
    governed = " ".join(str(a) for a in routing.await_args.args) + str(routing.await_args.kwargs)
    assert "MASKED" in governed


async def test_a_withdrawn_table_kept_in_another_region_is_not_read_here(
    state, routing, monkeypatch
):
    from provisa.core import process_region
    from provisa.core.read_refusal import ReadRefused

    monkeypatch.setattr(process_region, "_region", "us")
    state.role_build_inputs["withdrawn_tables"] = [_refunds(region="eu")]
    with pytest.raises(ReadRefused) as refused:
        await table_read.read_as_caller(state, _REFUNDS, "r", limit=5)
    assert refused.value.code == "query.home_region_unavailable"
    assert refused.value.params == {"table": "refunds", "region": "eu"}
    routing.assert_not_awaited()

    state.role_build_inputs["withdrawn_tables"] = [_refunds(region="us")]
    with pytest.raises(_Routed):
        await table_read.read_as_caller(state, _REFUNDS, "r", limit=5)


async def test_a_table_in_service_is_read_as_any_statement_reads_it(state, monkeypatch):
    from provisa.pgwire import _pipeline

    seen: list[tuple[str, str]] = []

    async def _govern(sql, role_id):
        seen.append((sql, role_id))
        return "the plan"

    async def _execute(plan, _state):
        return SimpleNamespace(column_names=["id"], rows=[(1,)], plan=plan)

    monkeypatch.setattr(_pipeline, "_govern_and_route", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    result = await table_read.read_as_caller(state, SALES, "r", limit=2000)
    assert seen == [('SELECT * FROM "sales"."orders" LIMIT 2000', "r")]
    assert result.plan == "the plan"


async def test_a_table_that_is_not_registered_is_not_found(state):
    with pytest.raises(ApiError) as err:
        await table_read.read_as_caller(state, 404404, "r", limit=5)
    assert (err.value.status_code, err.value.code) == (404, "schema.table_not_found")


# --- the right: registering tables, in the table's own domain ---------------------------------------


def test_the_caller_needs_the_right_putting_back_needs_in_the_tables_domain(state, monkeypatch):
    from provisa.api.admin import capabilities

    asked: list[tuple[str, str]] = []
    monkeypatch.setattr(
        capabilities,
        "require_capability_request",
        lambda request, cap: asked.append(("right", cap)),
    )
    monkeypatch.setattr(
        capabilities,
        "require_domain_request",
        lambda request, domain: asked.append(("domain", domain)),
    )
    request = object()
    assert table_read.table_for_steward(request, state, _REFUNDS)["table_name"] == "refunds"
    assert table_read.table_for_steward(request, state, SALES)["table_name"] == "orders"
    assert asked == [
        ("right", "table_registration"),
        ("domain", "sales"),
        ("right", "table_registration"),
        ("domain", "sales"),
    ]
    with pytest.raises(ApiError) as err:
        table_read.table_for_steward(request, state, 404404)
    assert err.value.code == "schema.table_not_found"


def test_a_caller_without_the_right_or_outside_the_domain_is_refused(state, monkeypatch):
    from provisa.api.admin import capabilities

    def _no_right(request, cap):
        raise ApiError(403, "auth.missing_capability", f"Missing capability: {cap!r}")

    monkeypatch.setattr(capabilities, "require_capability_request", _no_right)
    with pytest.raises(ApiError) as err:
        table_read.table_for_steward(object(), state, _REFUNDS)
    assert err.value.code == "auth.missing_capability"

    monkeypatch.setattr(capabilities, "require_capability_request", lambda request, cap: None)

    def _other_domain(request, domain):
        raise ApiError(403, "auth.domain_denied", f"No access to domain {domain!r}")

    monkeypatch.setattr(capabilities, "require_domain_request", _other_domain)
    with pytest.raises(ApiError) as err:
        table_read.table_for_steward(object(), state, SALES)
    assert err.value.code == "auth.domain_denied"


def test_withdrawing_and_the_read_ask_for_the_same_right():
    """REQ-1921: withdrawing a table or putting it back requires table_registration in its
    domain; so does the steward's read of it."""
    import inspect

    from provisa.api.admin import schema_mutation

    release = inspect.getsource(schema_mutation._set_table_withdrawn)
    assert 'require_capability(info, "table_registration", domain_id=row.domain_id)' in release
    read = inspect.getsource(table_read.table_for_steward)
    assert 'require_capability_request(request, "table_registration")' in read
    assert 'require_domain_request(request, table["domain_id"])' in read


def test_both_routes_read_through_the_one_function_and_nothing_else():
    import inspect

    from provisa.api.admin import table_profile_router as routes

    source = inspect.getsource(routes)
    assert source.count("await read_as_caller(") == 1
    assert "run_admin_catalog_sql" not in source and "TABLESAMPLE" not in source
    for route in (routes.profile_table, routes.preview_table):
        assert "await _rows(request, table_id, x_provisa_role," in inspect.getsource(route)


# --- the viewer of a withdrawn table: filters, sorts and pages, each a statement the SERVER writes ------------


def _page(**choices):
    from provisa.api.admin.viewer_statement import ViewerRequest

    return ViewerRequest.model_validate(choices)


def _governed(routing) -> str:
    """The statement routing was handed to run: governed, and addressed at what it reads. Only
    the statement -- never the role's context, which names every rule whether applied or not."""
    return str(routing.await_args.args[0])


@pytest.fixture
def masked_and_filtered(state):
    """The caller's role sees ``note`` masked and only the rows its row rule admits."""
    from provisa.compiler.rls import RLSContext
    from provisa.security.masking import MaskingRule, MaskType

    state.masking_rules = {
        (_REFUNDS, "r"): {
            "note": (MaskingRule(mask_type=MaskType.constant, value="MASKED"), "varchar")
        }
    }
    state.rls_contexts["r"] = RLSContext(
        rules={_REFUNDS: "id > 100"}, domain_rules={}, action_rules={}
    )
    return state


@pytest.mark.parametrize(
    ("choices", "written"),
    [
        (
            {"filters": [{"col": "id", "kind": "number", "op": "ge", "a": "7"}]},
            '"id" >= $1',
        ),
        ({"sorts": [{"col": "id", "dir": "desc"}]}, 'ORDER BY "id" DESC'),
        ({"page": 1, "pageSize": 50}, "LIMIT 51 OFFSET 50"),
    ],
    ids=["a filter", "a sort", "a second page"],
)
async def test_each_choice_of_the_viewer_reaches_the_governed_stage_under_the_callers_rules(
    masked_and_filtered, routing, choices, written
):
    from provisa.api.admin.viewer_statement import viewer_statement

    sql, _params = viewer_statement(_refunds(), '"sales"."refunds"', _page(**choices))
    assert written in sql  # what the server wrote from the choice
    with pytest.raises(_Routed):
        await table_read.read_as_caller(
            masked_and_filtered, _REFUNDS, "r", request=_page(**choices)
        )
    governed = _governed(routing)
    assert "MASKED" in governed, "the caller's mask is in the statement"
    assert "id > 100" in governed.replace('"', ""), "and so is its row rule"


async def test_a_bound_value_travels_beside_the_statement_never_inside_it(state, routing):
    choices = {"filters": [{"col": "note", "kind": "text", "op": "contains", "a": "x'; DROP"}]}
    with pytest.raises(_Routed):
        await table_read.read_as_caller(state, _REFUNDS, "r", request=_page(**choices))
    from provisa.api.admin.viewer_statement import viewer_statement

    sql, params = viewer_statement(_refunds(), '"sales"."refunds"', _page(**choices))
    assert "DROP" not in sql and params == ["x'; drop"]


@pytest.mark.parametrize(
    ("choices", "code", "names"),
    [
        (
            {"filters": [{"col": "secret", "kind": "text", "op": "contains", "a": "x"}]},
            "data.viewer_column_unknown",
            {"column": "secret", "table": "refunds"},
        ),
        (
            {"sorts": [{"col": "id; DROP TABLE x", "dir": "asc"}]},
            "data.viewer_column_unknown",
            {"column": "id; DROP TABLE x", "table": "refunds"},
        ),
        (
            {"groupBy": ["nope"]},
            "data.viewer_column_unknown",
            {"column": "nope", "table": "refunds"},
        ),
        (
            {"filters": [{"col": "id", "kind": "number", "op": "regexp", "a": "1"}]},
            "data.viewer_filter_unknown",
            {"operator": "regexp", "column": "id"},
        ),
        (
            {"filters": [{"col": "id", "kind": "number", "op": "eq", "a": "1 OR 1=1"}]},
            "data.viewer_value_invalid",
            {"column": "id", "value": "1 OR 1=1"},
        ),
        (
            {"filters": [{"col": "id", "kind": "date", "op": "today", "from": "now()"}]},
            "data.viewer_value_invalid",
            {"column": "id", "value": "now() .. None"},
        ),
    ],
    ids=["column", "sort column", "group column", "operator", "number", "date bound"],
)
async def test_what_the_table_is_not_registered_with_is_refused_before_any_statement(
    state, routing, choices, code, names
):
    with pytest.raises(ApiError) as err:
        await table_read.read_as_caller(state, _REFUNDS, "r", request=_page(**choices))
    assert (err.value.status_code, err.value.code) == (400, code)
    assert err.value.params == names
    routing.assert_not_awaited()


def test_a_sort_direction_and_a_page_are_what_they_are_or_nothing():
    from pydantic import ValidationError

    for bad in (
        {"sorts": [{"col": "id", "dir": "asc; DROP"}]},
        {"page": -1},
        {"pageSize": 0},
        {"pageSize": 1_000_000},
        {"filters": [{"col": "id", "kind": "blob", "op": "eq"}]},
    ):
        with pytest.raises(ValidationError):
            _page(**bad)


async def test_after_the_viewer_has_filtered_sorted_and_paged_the_table_is_as_refused_as_before(
    masked_and_filtered, routing
):
    state = masked_and_filtered
    tables_before, surface_before = list(state.tables), state.surfaces["r"]
    for choices in (
        {"filters": [{"col": "id", "kind": "number", "op": "ge", "a": "7"}]},
        {"sorts": [{"col": "id", "dir": "desc"}], "page": 3, "pageSize": 25},
    ):
        with pytest.raises(_Routed):
            await table_read.read_as_caller(state, _REFUNDS, "r", request=_page(**choices))
    routing.reset_mock()

    for words in (_STATEMENT, 'SELECT * FROM "sales"."refunds" WHERE "id" >= 7 LIMIT 51 OFFSET 0'):
        with pytest.raises(PermissionError, match="not a registered table"):
            await _ordinary(words)
    routing.assert_not_awaited()
    assert state.tables == tables_before and state.surfaces["r"] is surface_before


async def test_the_viewer_request_of_a_table_in_service_is_an_ordinary_bound_statement(
    state, monkeypatch
):
    from provisa.pgwire import _pipeline

    seen: list = []

    async def _govern(sql, role_id, **kw):
        seen.append((sql, role_id, kw))
        return "the plan"

    async def _execute(plan, _state):
        return SimpleNamespace(column_names=["id"], rows=[(1,)])

    monkeypatch.setattr(_pipeline, "_govern_and_route", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    await table_read.read_as_caller(
        state,
        SALES,
        "r",
        request=_page(filters=[{"col": "id", "kind": "number", "op": "eq", "a": "3"}]),
    )
    assert seen == [
        ('SELECT * FROM "sales"."orders" WHERE "id" = $1 LIMIT 51 OFFSET 0', "r", {"params": [3]})
    ]


# --- a withdrawn table keeps no replica: read live from its source, or refused ---------------------


class _Face:
    """The engine face as the state binds it: an engine, and the state it reads routes from."""

    def __init__(self, engine, bound_to) -> None:
        self.engine, self._state = engine, bound_to


def _engine_reading(state, monkeypatch, *, through_replica: bool):
    """The bound engine reads the table's source in place, or only through a replica."""
    from provisa.federation import registry_view, replica_routing
    from provisa.federation.replica_address import ReplicaRoutes

    source = SimpleNamespace(id="pg", type=SimpleNamespace(value="postgresql"))
    engine = SimpleNamespace(name="e", backend=SimpleNamespace())

    async def _sources(_state):
        return [source]

    monkeypatch.setattr(registry_view, "registered_sources", _sources)
    monkeypatch.setattr(replica_routing, "reads_replica", lambda *a, **kw: through_replica)
    runtime = state._active_runtime()
    monkeypatch.setattr(runtime, "federation_engine", _Face(engine, state), raising=False)
    monkeypatch.setattr(runtime, "replica_routes", ReplicaRoutes(engine_name="e"), raising=False)
    monkeypatch.setattr(runtime, "source_catalogs", {"pg": "pg"}, raising=False)
    return state


async def test_a_withdrawn_table_is_previewed_live_from_its_source_by_this_read_alone(
    state, monkeypatch
):
    from provisa.pgwire import _pipeline

    in_place = _engine_reading(state, monkeypatch, through_replica=False)
    seen: list = []

    async def _govern(sql, role_id, **kw):
        seen.append(kw["state"])
        return "the plan"

    async def _execute(plan, state):
        seen.append(state)
        return SimpleNamespace(column_names=["id"], rows=[])

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    published = in_place.replica_routes
    await table_read.read_as_caller(in_place, _REFUNDS, "r", limit=5)

    governed_in, executed_in = seen
    assert governed_in is executed_in
    # Read live: no route to a replica, the table read from its source in place.
    assert governed_in.replica_routes.routes == {}
    assert governed_in.replica_routes.unfloored == {_REFUNDS: "pg"}
    assert governed_in.federation_engine._state is governed_in
    assert governed_in.previewed_withdrawn_ids == {_REFUNDS}
    # The published routes -- every other statement's -- never name it, before or after.
    assert in_place.replica_routes is published and _REFUNDS not in published.unfloored
    assert in_place.federation_engine._state is in_place
    assert not hasattr(in_place, "previewed_withdrawn_ids")


async def test_a_withdrawn_table_the_engine_reads_only_through_a_replica_is_refused(
    state, monkeypatch, routing
):
    replica_only = _engine_reading(state, monkeypatch, through_replica=True)
    with pytest.raises(ApiError) as refused:
        await table_read.read_as_caller(replica_only, _REFUNDS, "r", limit=5)
    assert (refused.value.status_code, refused.value.code) == (
        409,
        "schema.withdrawn_table_not_previewable",
    )
    routing.assert_not_awaited()


async def test_no_withdrawn_table_is_judged_for_a_replica(monkeypatch):
    from provisa.federation import registry_view

    in_service = [SimpleNamespace(id=1), SimpleNamespace(id=2)]

    async def _served(_state, conn=None):
        return in_service

    monkeypatch.setattr(registry_view, "registered_tables", _served)
    ordinary = await registry_view.tables_a_read_judges(SimpleNamespace())
    assert [t.id for t in ordinary] == [1, 2]
    previewing = await registry_view.tables_a_read_judges(
        SimpleNamespace(previewed_withdrawn_ids={9})
    )
    assert [t.id for t in previewing] == [1, 2]  # a withdrawn table keeps no replica


# --- a withdrawn VIEW: expanded for this read as a view in service is, and for no other ---------------

_REFUNDS_VIEW = 10


@pytest.fixture
def withdrawn_view(state):
    """``big_orders``: a view over the table in service, registered and withdrawn."""
    view = _refunds(
        id=_REFUNDS_VIEW,
        source_id="__derived__",
        schema_name="sales",
        table_name="big_orders",
        view_sql='SELECT id FROM "sales"."orders" WHERE id > 5;',
        columns=[_refunds()["columns"][0]],
    )
    state.role_build_inputs["withdrawn_tables"] = [_refunds(), view]
    return state


async def test_a_withdrawn_view_is_expanded_for_its_stewards_read_alone(
    withdrawn_view, monkeypatch
):
    from provisa.pgwire import _pipeline

    seen: list = []

    async def _govern(sql, role_id, **kw):
        seen.append((sql, kw["state"]))
        return "the plan"

    async def _execute(plan, state):
        return SimpleNamespace(column_names=["id"], rows=[])

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    published = dict(withdrawn_view.view_sql_map)
    await table_read.read_as_caller(withdrawn_view, _REFUNDS_VIEW, "r", limit=5)

    sql, view = seen[0]
    assert sql == 'SELECT * FROM "sales"."big_orders" LIMIT 5'
    # Physical, as a published view's entry is: the engine knows the table by its address.
    assert view.view_sql_map["big_orders"] == (
        'SELECT id FROM "pg"."public"."orders" AS "orders" WHERE id > 5'
    )
    # The published map -- what every other statement is expanded from -- never held it.
    assert "big_orders" not in withdrawn_view.view_sql_map
    assert dict(withdrawn_view.view_sql_map) == published


async def test_a_withdrawn_table_adds_nothing_to_the_view_map(state, monkeypatch):
    from provisa.pgwire import _pipeline

    seen: list = []

    async def _govern(sql, role_id, **kw):
        seen.append(kw["state"])
        return "the plan"

    async def _execute(plan, _state):
        return SimpleNamespace(column_names=["id"], rows=[])

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    await table_read.read_as_caller(state, _REFUNDS, "r", limit=5)
    assert seen[0].view_sql_map is state.view_sql_map


async def test_a_withdrawn_views_body_reaches_routing_under_the_callers_rules(
    withdrawn_view, routing
):
    from provisa.compiler.rls import RLSContext

    withdrawn_view.rls_contexts["r"] = RLSContext(
        rules={SALES: "id > 100"}, domain_rules={}, action_rules={}
    )
    with pytest.raises(_Routed):
        await table_read.read_as_caller(withdrawn_view, _REFUNDS_VIEW, "r", limit=5)
    governed = _governed(routing).replace('"', "")
    assert "id > 5" in governed, "the view's own body was expanded for this read"
    assert "id > 100" in governed, "under the caller's row rule on the table it reads"
