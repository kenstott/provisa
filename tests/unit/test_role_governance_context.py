# Copyright (c) 2026 Kenneth Stott
# Canary: PENDING
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's governance context is built once per role and model version (REQ-1911).

The live proof that a change is seen on the next request, over Cypher and SPARQL on HTTP, is
tests/integration/test_graph_surfaces_see_rights_change.py. This holds the rules: what moves the
context, and that one request's governance never reaches another's. A SQL catalog's views stay out
of the landing paths' table list, and in the governance context.
"""

# Requirements: REQ-1911, REQ-1132

from __future__ import annotations

import copy
from collections import defaultdict
from types import SimpleNamespace

import pytest

from provisa.api.admin import db_queries
from provisa.api.admin.db_queries import fetch_tables
from provisa.api.rest.cypher_router import access_violations
from provisa.compiler import stage2
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.rls import RLSContext
from provisa.compiler.stage2 import apply_governance
from provisa.fakes.measurement import MEASURING
from provisa.federation import registry_view
from provisa.pgwire import governed_plan
from provisa.pgwire.governed_plan import role_governance
from provisa.security.rights import SQL_CATALOG_TABLES
from tests.role_state import RoleState

_TABLES = [
    {
        "id": 1,
        "source_id": "pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "columns": [
            {"column_name": "id", "data_type": "integer", "visible_to": ["analyst", "viewer"]},
            {"column_name": "region", "data_type": "varchar", "visible_to": ["analyst", "viewer"]},
        ],
    }
]


def _meta() -> SimpleNamespace:
    return SimpleNamespace(
        table_id=1,
        domain_id="sales",
        field_name="sales__orders",
        type_name="Sales_Orders",
        source_id="pg",
        catalog_name="postgresql",
        schema_name="public",
        table_name="orders",
        alias=None,
        original_table_name=None,
        display_name="",
    )


def _ctx() -> stage2.CompilationContext:
    ctx = stage2.CompilationContext()
    ctx.tables = {"sales__orders": _meta()}  # type: ignore[assignment]
    return ctx


def _role(role_id: str) -> dict:
    return {"id": role_id, "domain_access": ["*"], "capabilities": []}


def _state() -> RoleState:
    return RoleState(
        contexts={"analyst": _ctx(), "viewer": _ctx()},
        rls_contexts={"analyst": RLSContext(rules={}), "viewer": RLSContext(rules={})},
        roles={"analyst": _role("analyst"), "viewer": _role("viewer")},
        masking_rules={},
        tables=copy.deepcopy(_TABLES),
        relationships=[],
        source_types={"pg": "postgresql"},
        schema_boot_id="boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(expires=False),
        security_high=False,
    )


def _governance(state: RoleState, role: str = "analyst") -> stage2.GovernanceContext:
    return role_governance(state, state.surfaces[role], role)


class _Builds:
    def __init__(self) -> None:
        self.count = 0
        self._real = stage2.build_governance_context

    def __call__(self, *args, **kwargs):
        self.count += 1
        return self._real(*args, **kwargs)


@pytest.fixture
def builds(monkeypatch) -> _Builds:
    counter = _Builds()
    monkeypatch.setattr(stage2, "build_governance_context", counter)
    return counter


@pytest.fixture(autouse=True)
def _no_rebuild(monkeypatch):
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)


def _bump(state: RoleState) -> None:
    state.schema_version += 1


def _swap_tables(state: RoleState) -> None:
    state.tables = copy.deepcopy(_TABLES)


def _swap_masks(state: RoleState) -> None:
    state.masking_rules = {}


def _swap_role(state: RoleState) -> None:
    state.roles = {**state.roles, "analyst": _role("analyst")}


def _swap_relationships(state: RoleState) -> None:
    state.relationships = []


def _swap_context(state: RoleState) -> None:
    state.contexts["analyst"] = _ctx()


def _swap_row_filters(state: RoleState) -> None:
    state.rls_contexts["analyst"] = RLSContext(rules={})


def test_many_requests_build_the_context_once(builds):
    state = _state()
    for _ in range(5):
        _governance(state)
    assert builds.count == 1


def test_each_role_has_its_own_context(builds):
    state = _state()
    assert _governance(state, "analyst").role_id == "analyst"
    assert _governance(state, "viewer").role_id == "viewer"
    assert _governance(state, "analyst").role_id == "analyst"
    assert builds.count == 2


@pytest.mark.parametrize(
    "change",
    [
        _bump,
        _swap_tables,
        _swap_masks,
        _swap_role,
        _swap_relationships,
        _swap_context,
        _swap_row_filters,
    ],
)
def test_any_change_of_what_it_is_built_from_is_built_again_on_the_next_request(builds, change):
    state = _state()
    _governance(state)
    change(state)
    _governance(state)
    assert builds.count == 2


def test_the_new_rights_are_what_the_next_request_reads():
    state = _state()
    assert _governance(state).visible_columns[1] is None
    tables = copy.deepcopy(_TABLES)
    tables[0]["columns"][1]["visible_to"] = ["viewer"]
    state.tables = tables
    assert _governance(state).visible_columns[1] == frozenset({"id"})


def test_a_row_limit_setting_moved_without_a_generation_is_read(builds, monkeypatch):
    import provisa.core.limits as limits

    state = _state()
    monkeypatch.setattr(limits, "default_row_limit", lambda: 10)
    _governance(state)
    monkeypatch.setattr(limits, "default_row_limit", lambda: 20)
    _governance(state)
    assert builds.count == 2


def test_a_context_built_during_a_rebuild_is_not_kept(builds, monkeypatch):
    state = _state()
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: True)
    _governance(state)
    _governance(state)
    assert builds.count == 2


def test_a_request_holding_a_replaced_surface_leaves_nothing_for_the_next(builds):
    state = _state()
    held = state.surfaces["analyst"]
    state.contexts["analyst"] = _ctx()
    role_governance(state, held, "analyst")
    _governance(state)
    assert builds.count == 2


def test_the_measurement_of_fakes_has_its_own_context(builds):
    state = _state()
    _governance(state)
    token = MEASURING.set(True)
    try:
        _governance(state)
    finally:
        MEASURING.reset(token)
    assert builds.count == 2


def test_a_refused_build_is_not_kept(builds):
    state = _state()
    state.contexts["analyst"].refusal = "held"
    for _ in range(2):
        with pytest.raises(PermissionError):
            _governance(state)
    assert builds.count == 2


def test_a_statements_own_limits_stay_on_its_own_copy():
    state = _state()
    mine = _governance(state)
    mine.row_limit = "mine"
    mine.list_limit = "mine"
    mine.embedding_dims = {"m": 3}
    theirs = _governance(state)
    assert (theirs.row_limit, theirs.list_limit, theirs.embedding_dims) == (None, None, {})


def test_governing_a_statement_leaves_the_shared_context_as_it_was():
    state = _state()
    state.rls_contexts["analyst"] = RLSContext(rules={1: "region = 'east'"})
    shared = _governance(state)
    before = copy.deepcopy(vars(shared))
    apply_governance("SELECT region FROM public.orders", _governance(state))
    after = vars(_governance(state))
    assert {k: after[k] for k in before} == before


def test_the_graph_surfaces_check_and_the_pipeline_read_one_context(builds, monkeypatch):
    state = _state()
    surface = state.surfaces["analyst"]
    monkeypatch.setattr("provisa.compiler.sql_validator.validate_sql", lambda *a, **k: [])
    for _ in range(3):
        assert access_violations("SELECT 1", surface, "analyst", state) is None
    _governance(state)
    assert builds.count == 1


def test_a_refusal_is_still_reached_with_the_kept_context(monkeypatch):
    state = _state()
    surface = state.surfaces["analyst"]
    seen = []

    def validate(sql, ctx, governance, role, tables, **_):
        seen.append(governance.visible_columns[1])
        return [SimpleNamespace(code="V", message="no")] if len(seen) > 1 else []

    monkeypatch.setattr("provisa.compiler.sql_validator.validate_sql", validate)
    assert access_violations("SELECT 1", surface, "analyst", state) is None
    refused = access_violations("SELECT 1", surface, "analyst", state)
    assert refused is not None and refused.status_code == 403
    assert seen == [None, None]


# -- a SQL catalog's views ------------------------------------------------------------------------


class _Conn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def fetch(self, sql: str, *args):
        self.statements.append(sql)
        return []


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def test_the_catalog_list_names_every_view_of_a_sql_catalog():
    clause = db_queries._outside_sql_catalog()
    assert len(SQL_CATALOG_TABLES) > 200
    assert all(f"'{name}'" in clause for _, name in SQL_CATALOG_TABLES)


@pytest.mark.asyncio
async def test_a_reader_of_the_data_tables_reads_no_catalog_view_or_its_columns():
    conn = _Conn()
    await fetch_tables(conn, catalog=False)  # type: ignore[arg-type]
    tables, columns = conn.statements[0], conn.statements[-1]
    assert "FROM table_columns" in columns
    assert db_queries._outside_sql_catalog() in tables
    assert db_queries._outside_sql_catalog() in columns


@pytest.mark.asyncio
async def test_a_schema_build_still_reads_every_catalog_view():
    conn = _Conn()
    await fetch_tables(conn)  # type: ignore[arg-type]
    assert not any(db_queries._outside_sql_catalog() in s for s in conn.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("catalog", [False, True])
async def test_the_landing_list_leaves_them_out_unless_asked(monkeypatch, catalog):
    asked = defaultdict(int)

    async def fetch(conn, *, catalog=True):
        asked[catalog] += 1
        return []

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", fetch)
    conn = object()
    state = SimpleNamespace(
        model_db=SimpleNamespace(acquire=lambda: _Acquire(conn)),
        schema_boot_id="boot",
        schema_version=1,
    )
    await registry_view.registered_tables(state, catalog=catalog)
    assert asked == {catalog: 1}


def test_the_catalog_views_stay_in_the_governance_context():
    """REQ-1132 and the pgwire catalog read them through it: visibility and table map, all of it."""
    state = _state()
    name = sorted(n for _, n in SQL_CATALOG_TABLES)[0]
    state.tables = [
        *state.tables,
        {
            "id": 9,
            "source_id": "provisa-admin",
            "domain_id": "meta",
            "schema_name": "meta",
            "table_name": name,
            "columns": [{"column_name": "c", "data_type": "text", "visible_to": []}],
        },
    ]
    governance = _governance(state)
    assert 9 in governance.visible_columns
    assert governance.table_map[f"meta.{name}"] == 9
