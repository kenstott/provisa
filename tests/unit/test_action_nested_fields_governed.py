# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e7c1a-3f4d-4a86-9e21-c7d18a2f6b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The nested relationship fields of an action's result are read as the caller's role, through the
one governed pipeline (REQ-361, REQ-1176): the role's row rule, its masks and its column
visibility hold on the related rows as on any query that reads the relationship, and the source
is never read directly.

The pipeline is the real one (``_govern_and_route_compiled``: validation and governance); only its
router and its terminal are stand-ins, the terminal running the governed statement on PostgreSQL
(tests/governed_pg.py). The source's own pool answers the same tables ungoverned, as a source does.
"""

# Requirements: REQ-001, REQ-361, REQ-362, REQ-1176

from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from graphql import parse

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.directives import NO_CACHE_HINT
from provisa.compiler.rls import RLSContext, as_rule
from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta, compile_query
from provisa.executor.result import QueryResult
from provisa.federation.replica_address import ReplicaRoutes
from provisa.security.masking import MaskingRule, MaskType
from tests.governed_pg import governed_database
from tests.role_state import RoleState
from tests.unit.test_buffered_auto_threshold import _decision

pytestmark = pytest.mark.asyncio

ORDERS, CUSTOMERS = 1, 2

_ORDERS = TableMeta(
    table_id=ORDERS,
    field_name="orders",
    type_name="Orders",
    source_id="pg",
    catalog_name="pg",
    schema_name="sales",
    table_name="orders",
    domain_id="sales",
)
_CUSTOMERS = TableMeta(
    table_id=CUSTOMERS,
    field_name="customers",
    type_name="Customers",
    source_id="pg",
    catalog_name="pg",
    schema_name="sales",
    table_name="customers",
    domain_id="sales",
)
_CUSTOMER_OF_ORDER = JoinMeta(
    source_column="customer_id",
    target_column="id",
    source_column_type="integer",
    target_column_type="integer",
    target=_CUSTOMERS,
    cardinality="many-to-one",
)
_ORDERS_OF_CUSTOMER = JoinMeta(
    source_column="id",
    target_column="customer_id",
    source_column_type="integer",
    target_column_type="integer",
    target=_ORDERS,
    cardinality="one-to-many",
)

#: The rows the source holds.
_ROWS = {
    "sales.orders": [
        {"id": 10, "customer_id": 1, "amount": 100},
        {"id": 11, "customer_id": 2, "amount": 200},
        {"id": 12, "customer_id": 1, "amount": 300},
    ],
    "sales.customers": [
        {"id": 1, "name": "Ann", "email": "ann@example.com", "region": "EU"},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "region": "US"},
    ],
}

#: Every role, and what it reads of customers: full reads all of it; regional only the EU rows;
#: masked reads email masked; no_email is not shown email; no_customers cannot read the table.
_ROLES = ["full", "regional", "masked", "no_email", "no_customers"]
_CUSTOMER_COLUMNS = {
    "id": ["full", "regional", "masked", "no_email"],
    "name": ["full", "regional", "masked", "no_email"],
    "email": ["full", "regional", "masked"],
    "region": ["full", "regional", "masked", "no_email"],
}


def _context(role: str) -> CompilationContext:
    ctx = CompilationContext(role_id=role)
    ctx.tables = {"orders": _ORDERS}
    if role != "no_customers":
        ctx.tables["customers"] = _CUSTOMERS
        ctx.joins[("Customers", "orders")] = _ORDERS_OF_CUSTOMER
    # The relationship, held for every role so the read of its table is what refuses a role that
    # may not read it.
    ctx.joins[("Orders", "customer")] = _CUSTOMER_OF_ORDER
    return ctx


def _state(pg) -> SimpleNamespace:
    async def _source_read(_source_id, sql, params):
        """The source's own pool: the statement as given, over every row the source holds."""
        done = pg.written(sql, _ROWS, params=list(params))
        return QueryResult(rows=done.returned, column_names=list(pg.names))

    return RoleState(
        admin_db=None,
        contexts={role: _context(role) for role in _ROLES},
        rls_contexts={
            role: RLSContext(rules={CUSTOMERS: as_rule("region = 'EU'")})
            if role == "regional"
            else RLSContext.empty()
            for role in _ROLES
        },
        roles={
            role: {"id": role, "capabilities": ["full_results"], "domain_access": ["*"]}
            for role in _ROLES
        },
        masking_rules={
            (CUSTOMERS, "masked"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
            }
        },
        tables=[
            {
                "id": ORDERS,
                "source_id": "pg",
                "schema_name": "sales",
                "table_name": "orders",
                "domain_id": "sales",
                "columns": [
                    {"column_name": c, "data_type": "integer", "visible_to": list(_ROLES)}
                    for c in ("id", "customer_id", "amount")
                ],
            },
            {
                "id": CUSTOMERS,
                "source_id": "pg",
                "schema_name": "sales",
                "table_name": "customers",
                "domain_id": "sales",
                "columns": [
                    {
                        "column_name": c,
                        "data_type": "integer" if c == "id" else "varchar",
                        "visible_to": list(roles),
                    }
                    for c, roles in _CUSTOMER_COLUMNS.items()
                ],
            },
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
        role_build_inputs={"draft_tables": [], "withdrawn_tables": []},
        model_stamp=1,
        tracked_functions={
            "recent_orders": {"name": "recent_orders", "returns": "sales.orders"},
            "top_customers": {"name": "top_customers", "returns": "sales.customers"},
        },
        tracked_webhooks={},
    )


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    return governed_database(embedded_postgres, "test_action_nested_fields_governed")


@pytest.fixture(autouse=True)
def _org(bind_org):
    bind_org("org-a")


@pytest.fixture
def served(monkeypatch, pg):
    """The state, served by the one pipeline: its router answers DIRECT and its terminal runs the
    governed statement the plan holds on PostgreSQL."""
    import provisa.api.app as app_mod
    import provisa.api.data.mutations as mutations
    from provisa.pgwire import _pipeline, governed_plan

    state = _state(pg)
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr(app_mod, "state", state, raising=False)

    async def _terminal(plan, _state=None):
        done = pg.written(plan.exec_sql or plan.sql, _ROWS, params=plan.exec_params)
        return QueryResult(rows=done.returned, column_names=list(pg.names))

    # The action itself: each command answers rows of the table it returns, as its code gave them.
    answers = {
        "recent_orders": [{"id": 10, "customer_id": 1}, {"id": 11, "customer_id": 2}],
        "top_customers": [{"id": 1, "name": "Ann"}, {"id": 2, "name": "Bob"}],
    }

    async def _invoke(name, _args, _state, _role_id):
        return copy.deepcopy(answers[name])

    monkeypatch.setattr(mutations, "invoke_tracked_function", _invoke)
    monkeypatch.setattr(mutations, "bind_named_args", lambda _name, args, _state, _role: args)
    with (
        patch.object(
            _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_decision("DIRECT"))
        ),
        patch.object(_pipeline, "_execute_plan", new=AsyncMock(side_effect=_terminal)),
    ):
        yield state


async def _call(state, role: str, query: str) -> list[dict]:
    """The action field of ``query``, called as ``role``."""
    from provisa.api.data.mutations import _execute_action_field

    (field,) = parse(query).definitions[0].selection_set.selections  # type: ignore[union-attr]
    return await _execute_action_field(
        field.name.value, field, state, None, ctx=state.contexts[role], role_id=role
    )


async def _query(state, role: str, query: str) -> list[dict]:
    """``query``'s one root field read as ``role`` reads any query: compiled, then the pipeline."""
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


_ORDERS_WITH_CUSTOMER = "{ recent_orders { id customer_id customer { name email } } }"


async def test_a_row_rule_on_the_related_table_filters_the_nested_field(served):
    rows = await _call(served, "regional", _ORDERS_WITH_CUSTOMER)
    # Bob is a US customer: the regional role reads no such row, here as on any query.
    assert rows == [
        {"id": 10, "customer_id": 1, "customer": {"name": "Ann", "email": "ann@example.com"}},
        {"id": 11, "customer_id": 2, "customer": None},
    ]


async def test_a_masked_column_of_the_related_table_reads_masked(served):
    rows = await _call(served, "masked", _ORDERS_WITH_CUSTOMER)
    assert [r["customer"] for r in rows] == [
        {"name": "Ann", "email": "***"},
        {"name": "Bob", "email": "***"},
    ]


async def test_a_column_the_role_is_not_shown_is_refused(served):
    # As the same column is refused on a query that reads the relationship.
    with pytest.raises(PermissionError) as on_query:
        await _query(served, "no_email", "{ orders { id customer { name email } } }")
    with pytest.raises(PermissionError) as on_action:
        await _call(served, "no_email", _ORDERS_WITH_CUSTOMER)
    assert "email" in str(on_action.value)
    assert type(on_action.value) is type(on_query.value)


async def test_a_table_the_role_cannot_read_is_refused(served):
    from provisa.compiler.sql_gen import UnknownRootField

    # The product's answer to a read of a table the role is not served.
    with pytest.raises(UnknownRootField) as on_query:
        await _query(served, "no_customers", "{ customers { name } }")
    with pytest.raises(UnknownRootField) as on_action:
        await _call(served, "no_customers", _ORDERS_WITH_CUSTOMER)
    assert on_action.value.code == on_query.value.code
    assert on_action.value.params == on_query.value.params


async def test_a_role_with_full_read_gets_what_a_query_of_the_relationship_gets(served):
    rows = await _call(served, "full", _ORDERS_WITH_CUSTOMER)
    assert rows == [
        {"id": 10, "customer_id": 1, "customer": {"name": "Ann", "email": "ann@example.com"}},
        {"id": 11, "customer_id": 2, "customer": {"name": "Bob", "email": "bob@example.com"}},
    ]
    queried = await _query(
        served,
        "full",
        "{ orders(where: {id: {in: [10, 11]}}, order_by: [{id: asc}])"
        " { id customer_id customer { name email } } }",
    )
    assert rows == queried


async def test_a_to_many_relationship_is_read_governed_too(served):
    rows = await _call(served, "full", "{ top_customers { id name orders { id amount } } }")
    assert [(r["name"], sorted(o["id"] for o in r["orders"])) for r in rows] == [
        ("Ann", [10, 12]),
        ("Bob", [11]),
    ]
    assert rows[1]["orders"] == [{"id": 11, "amount": 200}]


async def test_the_source_is_never_read_directly(served):
    read = AsyncMock(side_effect=AssertionError("the related rows were read from the source pool"))
    served.source_pools.execute = read
    await _call(served, "full", _ORDERS_WITH_CUSTOMER)
    read.assert_not_called()
