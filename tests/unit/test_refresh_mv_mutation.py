# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2c4a90-1d53-4b68-8f37-a6c9e0b2d415
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``refreshMv`` admin mutation answers for the refresh it ran.

``refresh_mv`` records a failed refresh on the view and returns — it also runs on the scheduler,
where there is nobody to raise to. The mutation used to answer "refreshed" whatever happened.
It now reports a failed refresh as one, with the view's own error (the text the view list shows
as its last error), and success only when the view was built."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import provisa.api.app as app_mod
from provisa.api.admin.schema_mutation import Mutation
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry


@pytest.fixture
def view(monkeypatch):
    mv = MVDefinition(
        id="view-sales",
        source_tables=[],
        target_catalog="store",
        target_schema="org_acme_mv_cache",
        sql="SELECT region, count(*) AS n FROM sales.orders GROUP BY region",
    )
    registry = MVRegistry()
    registry.register(mv)
    state = SimpleNamespace(
        mv_registry=registry,
        federation_engine=object(),
        model_db=None,
        tenant_db=None,
        # The model the view's input resolves against (provisa/mv/view_inputs.py).
        tables=[{"id": 1, "source_id": "pg", "schema_name": "sales", "table_name": "orders"}],
        source_catalogs={},
        contexts={},
    )
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    return mv, registry


def _info(monkeypatch):
    from tests.unit.gate_identity import grant

    return grant(monkeypatch, "table_registration", state=app_mod.state)[0]


async def test_a_failed_refresh_is_reported_with_the_views_error(view, monkeypatch):
    mv, registry = view

    async def _fails(_engine, target, reg, store=None, ledger=None):
        reg.mark_refresh_failed(target.id, "relation sales.orders does not exist")

    monkeypatch.setattr("provisa.mv.refresh.refresh_mv", _fails)
    result = await Mutation().refresh_mv(_info(monkeypatch), mv_id="view-sales")

    assert result.success is False
    assert result.message == "relation sales.orders does not exist"
    assert result.message == mv.last_error  # what the view list shows
    assert mv.status == MVStatus.STALE


async def test_a_refresh_that_built_the_view_is_reported_as_refreshed(view, monkeypatch):
    mv, registry = view
    mv.last_error = "an earlier failure"

    async def _builds(_engine, target, reg, store=None, ledger=None):
        reg.mark_refreshed(target.id, row_count=4)

    monkeypatch.setattr("provisa.mv.refresh.refresh_mv", _builds)
    result = await Mutation().refresh_mv(_info(monkeypatch), mv_id="view-sales")

    assert result.success is True
    assert result.code == "schema.mv_refreshed"
    assert mv.last_error is None and mv.status == MVStatus.FRESH


async def test_a_view_refused_for_an_unreadable_input_is_reported_with_that_reason(
    view, monkeypatch
):
    """The real refresh, over an input that has become row-level: the mutation answers with the
    refusal, not "refreshed"."""
    mv, registry = view

    async def _row_level(_state):
        return {"orders": object()}

    async def _sources(_state):
        return []

    monkeypatch.setattr(
        "provisa.federation.query_residency.row_materialized_tables_by_name", _row_level
    )
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    app_mod.state.api_endpoints = {}
    app_mod.state.graphql_remote_sources = {}

    result = await Mutation().refresh_mv(_info(monkeypatch), mv_id="view-sales")

    assert result.success is False
    assert "'sales.orders' is a row-level replicated table" in result.message
    assert result.message == mv.last_error
