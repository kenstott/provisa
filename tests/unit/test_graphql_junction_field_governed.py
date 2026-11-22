# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9a7c51-0b42-4d86-9f17-a6c2e85d1b04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL serves a junction-backed relationship as a field (REQ-1586): a pet's inquirers are the
users its inquiries name, reached through the inquiries junction in two hops. The field takes
where, order_by and limit as any to-many field does, a where can state it as a condition, and it
is governed as any join is: the row rules and masks of the source, the junction and the target
all hold.

The pipeline is the real one (``_govern_and_route_compiled``: validation and governance); only its
router and its terminal are stand-ins, the terminal running the governed statement on PostgreSQL
(tests/governed_pg.py).
"""

# Requirements: REQ-1586, REQ-040, REQ-041

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from graphql import parse

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.directives import NO_CACHE_HINT
from provisa.compiler.rls import RLSContext, as_rule
from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta, compile_query
from provisa.compiler.sql_types import JunctionMeta
from provisa.executor.result import QueryResult
from provisa.federation.replica_address import ReplicaRoutes
from provisa.security.masking import MaskingRule, MaskType
from tests.governed_pg import governed_database
from tests.role_state import RoleState
from tests.unit.test_buffered_auto_threshold import _decision

pytestmark = pytest.mark.asyncio

PETS, USERS, INQUIRIES = 1, 2, 3


def _table(table_id: int, name: str, type_name: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=type_name,
        source_id="pg",
        catalog_name="pg",
        schema_name="shop",
        table_name=name,
        domain_id="shop",
    )


_PETS = _table(PETS, "pets", "Pets")
_USERS = _table(USERS, "users", "Users")
_INQUIRIES = _table(INQUIRIES, "inquiries", "Inquiries")


def _through_inquiries(type_value: str | None) -> JoinMeta:
    return JoinMeta(
        source_column="id",
        target_column="id",
        source_column_type="integer",
        target_column_type="integer",
        target=_USERS,
        cardinality="one-to-many",
        via=JunctionMeta(
            table=_INQUIRIES,
            source_columns=("pet_id",),
            target_columns=("user_id",),
            type_column="status" if type_value else None,
            type_value=type_value,
            attributes=(("message", "message"),),
            # The registry holds every junction with its edge's label source (REQ-1586).
            label_source="column" if type_value else "table",
        ),
        registered=True,
    )


#: The rows the source holds. Rex was asked about by Ann (open) and Bob (closed), Tom by Ann
#: (open), Kit by nobody.
_ROWS = {
    "shop.pets": [
        {"id": 1, "name": "Rex"},
        {"id": 2, "name": "Tom"},
        {"id": 3, "name": "Kit"},
    ],
    "shop.users": [
        {"id": 100, "name": "Ann", "email": "ann@example.com", "region": "EU"},
        {"id": 101, "name": "Bob", "email": "bob@example.com", "region": "US"},
    ],
    "shop.inquiries": [
        {"id": 7, "pet_id": 1, "user_id": 100, "message": "hi", "status": "open"},
        {"id": 8, "pet_id": 1, "user_id": 101, "message": "hey", "status": "closed"},
        {"id": 9, "pet_id": 2, "user_id": 100, "message": "yo", "status": "open"},
    ],
}

#: full reads everything; regional reads only EU users; open_only only open inquiries; masked
#: reads users' email masked.
_ROLES = ["full", "regional", "open_only", "masked"]
_COLUMNS = {
    PETS: {"id": "integer", "name": "varchar"},
    USERS: {"id": "integer", "name": "varchar", "email": "varchar", "region": "varchar"},
    INQUIRIES: {
        "id": "integer",
        "pet_id": "integer",
        "user_id": "integer",
        "message": "varchar",
        "status": "varchar",
    },
}
_RULES = {
    "regional": {USERS: as_rule("region = 'EU'")},
    "open_only": {INQUIRIES: as_rule("status = 'open'")},
}


def _context(role: str) -> CompilationContext:
    ctx = CompilationContext(role_id=role)
    ctx.tables = {"pets": _PETS, "users": _USERS, "inquiries": _INQUIRIES}
    ctx.joins[("Pets", "inquirers")] = _through_inquiries(None)
    ctx.joins[("Pets", "openInquirers")] = _through_inquiries("open")
    return ctx


def _state(pg) -> SimpleNamespace:
    async def _source_read(_source_id, sql, params):
        done = pg.written(sql, _ROWS, params=list(params))
        return QueryResult(rows=done.returned, column_names=list(pg.names))

    return RoleState(
        admin_db=None,
        contexts={role: _context(role) for role in _ROLES},
        rls_contexts={
            role: RLSContext(rules=_RULES[role]) if role in _RULES else RLSContext.empty()
            for role in _ROLES
        },
        roles={
            role: {"id": role, "capabilities": ["full_results"], "domain_access": ["*"]}
            for role in _ROLES
        },
        masking_rules={
            (USERS, "masked"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
            }
        },
        tables=[
            {
                "id": meta.table_id,
                "source_id": "pg",
                "schema_name": "shop",
                "table_name": meta.table_name,
                "domain_id": "shop",
                "columns": [
                    {"column_name": c, "data_type": t, "visible_to": list(_ROLES)}
                    for c, t in _COLUMNS[meta.table_id].items()
                ],
            }
            for meta in (_PETS, _USERS, _INQUIRIES)
        ],
        source_types={"pg": "postgresql"},
        source_dialects={"pg": "postgres"},
        source_catalogs={"pg": "pg"},
        source_pools=SimpleNamespace(
            source_ids=["pg"], has=lambda sid: sid == "pg", execute=_source_read
        ),
        relationships=[],
        metrics={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
        role_build_inputs={"withdrawn_tables": []},
        model_stamp=1,
        tracked_functions={},
        tracked_webhooks={},
    )


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    return governed_database(embedded_postgres, "test_graphql_junction_field_governed")


@pytest.fixture(autouse=True)
def _org(bind_org):
    bind_org("org-a")


@pytest.fixture
def served(monkeypatch, pg):
    """The state, served by the one pipeline: its router answers DIRECT and its terminal runs the
    governed statement the plan holds on PostgreSQL."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline, governed_plan

    state = _state(pg)
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr(app_mod, "state", state, raising=False)

    async def _terminal(plan, _state=None):
        done = pg.written(plan.exec_sql or plan.sql, _ROWS, params=plan.exec_params)
        return QueryResult(rows=done.returned, column_names=list(pg.names))

    with (
        patch.object(
            _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_decision("DIRECT"))
        ),
        patch.object(_pipeline, "_execute_plan", new=AsyncMock(side_effect=_terminal)),
    ):
        yield state


async def _query(state, role: str, query: str) -> list[dict]:
    """``query``'s one root field read as ``role``: compiled, then the governed pipeline."""
    from provisa.executor.json_encoders import graphql_field, result_batches
    from provisa.pgwire import _pipeline

    (compiled,) = compile_query(parse(query), state.contexts[role])
    plan = await _pipeline._govern_and_route_compiled(
        compiled.sql,
        role,
        exec_params=compiled.params or None,
        state=state,
        cache_hint=NO_CACHE_HINT,
        compiled=compiled,
        sdl_joins=True,
    )
    result = await _pipeline._execute_plan(plan, state)
    return json.loads(graphql_field(result_batches(result), compiled.columns).body)


def _reached(rows: list[dict], field: str, key: str = "name") -> dict[str, list]:
    """Each pet's reached users, by pet name, sorted (a list field's order is unstated)."""
    return {r["name"]: sorted(u[key] for u in r[field]) for r in rows}


_INQUIRERS = "{ pets(order_by: [{id: asc}]) { name inquirers { name email } } }"


async def test_the_field_reaches_the_users_the_junction_names(served):
    rows = await _query(served, "full", _INQUIRERS)
    assert _reached(rows, "inquirers") == {"Rex": ["Ann", "Bob"], "Tom": ["Ann"], "Kit": []}


async def test_a_typed_junction_reaches_only_its_type(served):
    rows = await _query(
        served, "full", "{ pets(order_by: [{id: asc}]) { name openInquirers { name } } }"
    )
    assert _reached(rows, "openInquirers") == {"Rex": ["Ann"], "Tom": ["Ann"], "Kit": []}


async def test_the_field_takes_where_order_by_and_limit(served):
    rows = await _query(
        served,
        "full",
        "{ pets(order_by: [{id: asc}]) { name"
        ' inquirers(where: {region: {in: ["EU", "US"]}}, order_by: [{name: desc}], limit: 1)'
        " { name } } }",
    )
    assert _reached(rows, "inquirers") == {"Rex": ["Bob"], "Tom": ["Ann"], "Kit": []}


async def test_a_where_states_the_field_as_a_condition(served):
    rows = await _query(
        served, "full", '{ pets(where: {inquirers: {name: {eq: "Bob"}}}) { name } }'
    )
    assert rows == [{"name": "Rex"}]


async def test_a_row_rule_on_the_target_holds(served):
    rows = await _query(served, "regional", _INQUIRERS)
    assert _reached(rows, "inquirers") == {"Rex": ["Ann"], "Tom": ["Ann"], "Kit": []}


async def test_a_row_rule_on_the_junction_holds(served):
    rows = await _query(served, "open_only", _INQUIRERS)
    assert _reached(rows, "inquirers") == {"Rex": ["Ann"], "Tom": ["Ann"], "Kit": []}
    # The condition reads the junction as governed too: Bob's inquiry is closed.
    rows = await _query(
        served, "open_only", '{ pets(where: {inquirers: {name: {eq: "Bob"}}}) { name } }'
    )
    assert rows == []


async def test_a_mask_on_the_target_holds(served):
    rows = await _query(served, "masked", _INQUIRERS)
    assert _reached(rows, "inquirers", "email") == {
        "Rex": ["***", "***"],
        "Tom": ["***"],
        "Kit": [],
    }
