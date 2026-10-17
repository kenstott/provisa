# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each org's materialized views are its own (REQ-1266, REQ-849).

Two orgs may each have a view of the same name. A read in one org is answered only from a fresh
view of that org: the optimize stage (pgwire/_pipeline._view_answering) asks the bound org's
registry for its fresh views, and a view another org registered is not among them."""

# Requirements: REQ-1266, REQ-849, REQ-543

from __future__ import annotations

import time

import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus, TableIdentity

pytestmark = pytest.mark.unbound

_ORDERS = '"pg"."public"."orders"'
_CUSTOMERS = '"pg"."public"."customers"'
_SQL = (
    f'SELECT "o"."id", "c"."name" FROM {_ORDERS} AS "o" '
    f'LEFT JOIN {_CUSTOMERS} AS "c" ON "o"."customer_id" = "c"."id"'
)


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    for i, name in enumerate(("orders", "customers")):
        ctx.tables[name] = TableMeta(
            table_id=i,
            field_name=name,
            type_name=name.title(),
            source_id="sales-pg",
            catalog_name="pg",
            schema_name="public",
            table_name=name,
            domain_id="sales",
        )
    return ctx


def _view(schema: str) -> MVDefinition:
    """The same view, by name, as an org would declare it: its copy in that org's store."""
    return MVDefinition(
        id="view-orders_customers",
        source_tables=["orders", "customers"],
        inputs=[
            TableIdentity("sales-pg", "public", "orders"),
            TableIdentity("sales-pg", "public", "customers"),
        ],
        target_catalog="mat",
        target_schema=schema,
        target_table="orders_customers",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        status=MVStatus.FRESH,
        last_refresh_at=time.time(),
    )


@pytest.fixture
def state(monkeypatch):
    from provisa.api.app import AppState

    state = AppState()
    state.org_id = "root"
    for org in ("acme", "globex"):
        state.org_registry.set(org, OrgRuntime(org_id=org))
    monkeypatch.setattr("provisa.api.app.state", state)
    return state


def _in(org: str, fn):
    token = set_current_org(org)
    try:
        return fn()
    finally:
        reset_current_org(token)


def _answering(state):
    from provisa.pgwire._pipeline import _view_answering

    return _view_answering(_SQL, _ctx(), state, is_mutation=False)


def test_a_read_is_not_answered_from_another_orgs_view(state):
    _in("globex", lambda: state.mv_registry.register(_view("org_globex_mv_cache")))
    assert _in("acme", lambda: _answering(state)) is None
    hit = _in("globex", lambda: _answering(state))
    assert hit is not None and '"org_globex_mv_cache"' in hit.sql


def test_two_orgs_views_of_one_name_are_each_answered_from_its_own(state):
    _in("acme", lambda: state.mv_registry.register(_view("org_acme_mv_cache")))
    _in("globex", lambda: state.mv_registry.register(_view("org_globex_mv_cache")))
    acme = _in("acme", lambda: _answering(state))
    globex = _in("globex", lambda: _answering(state))
    assert acme is not None and '"org_acme_mv_cache"' in acme.sql
    assert globex is not None and '"org_globex_mv_cache"' in globex.sql
