# Copyright (c) 2026 Kenneth Stott
# Canary: 4c1e8a72-9b3d-4f06-a2e5-7d8b0c6f1e39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Authoring a metric asks create_view, on the server as in the UI (REQ-1327, amended 2026-11-14,
item 2 of THE GATES FOR VIEWS, METRICS AND COMMANDS).

Every path that writes a metric -- the GraphQL create, update, delete and bulk delete, and the MCP
upsert and delete tools -- accepts a holder of create_view and refuses a role holding only
table_registration, or neither.
"""

# Requirements: REQ-1327

from __future__ import annotations

import types

import pytest

from provisa.api import app as appmod
from provisa.api.admin import schema_helpers, schema_mutation
from provisa.api.admin.types import MetricInput
from provisa.api.errors import ApiError
from provisa.api.mcp import tool_groups, tools
from provisa.core.schema_org import metrics
from tests.unit.test_admin_deletes_need_the_domain import (  # noqa: F401
    _info,
    _names,
    _request,
    plane,
)

ROLES = {
    "builder": {"id": "builder", "capabilities": ["create_view"], "domain_access": ["*"]},
    "registrar": {
        "id": "registrar",
        "capabilities": ["table_registration"],
        "domain_access": ["*"],
    },
    "reader": {"id": "reader", "capabilities": ["query_development"], "domain_access": ["*"]},
}
REFUSED = ["registrar", "reader"]


@pytest.fixture
def gated(monkeypatch, plane):  # noqa: F811
    monkeypatch.setattr(appmod.state, "roles", ROLES, raising=False)

    async def _rebuild() -> None:
        return None

    monkeypatch.setattr(schema_helpers, "_rebuild_schemas", _rebuild)
    return plane


def _mcp_state(db) -> types.SimpleNamespace:
    return types.SimpleNamespace(contexts={r: object() for r in ROLES}, model_db=db)


# --- GraphQL ------------------------------------------------------------------------------------


async def test_a_create_view_holder_creates_updates_and_deletes_a_metric(gated):
    m = schema_mutation.Mutation()
    created = await m.create_metric(
        _info("builder"), MetricInput(name="orders_total", expression="SUM(orders.amount)")
    )
    assert created.success is True, created.message
    updated = await m.update_metric(
        _info("builder"), MetricInput(name="orders_total", expression="MAX(orders.amount)")
    )
    assert updated.success is True, updated.message
    deleted = await m.delete_metric(_info("builder"), "orders_total")
    assert deleted.success is True, deleted.message
    bulk = await m.delete_metrics(_info("builder"), ["revenue"])
    assert "revenue" not in await _names(gated, metrics)
    assert bulk is not None


@pytest.mark.parametrize("role", REFUSED)
async def test_a_role_without_create_view_is_refused_every_metric_write(gated, role):
    m = schema_mutation.Mutation()
    with pytest.raises(PermissionError, match="create_view"):
        await m.create_metric(
            _info(role), MetricInput(name="nope", expression="SUM(orders.amount)")
        )
    with pytest.raises(PermissionError, match="create_view"):
        await m.update_metric(
            _info(role), MetricInput(name="revenue", expression="MAX(orders.amount)")
        )
    with pytest.raises(PermissionError, match="create_view"):
        await m.delete_metric(_info(role), "revenue")
    with pytest.raises(PermissionError, match="create_view"):
        await m.delete_metrics(_info(role), ["revenue"])
    names = await _names(gated, metrics)
    assert "nope" not in names
    assert "revenue" in names


# --- MCP ----------------------------------------------------------------------------------------


async def test_a_create_view_holder_upserts_and_deletes_a_metric_over_mcp(gated):
    state = _mcp_state(gated)
    saved = await tools.upsert_metric(
        state, "builder", _request("builder"), "orders_total", "SUM(orders.amount)"
    )
    assert saved["name"] == "orders_total"
    assert "orders_total" in await _names(gated, metrics)
    await tools.delete_metric(state, "builder", _request("builder"), "orders_total")
    assert "orders_total" not in await _names(gated, metrics)


@pytest.mark.parametrize("role", REFUSED)
async def test_a_role_without_create_view_is_refused_over_mcp(gated, role):
    state = _mcp_state(gated)
    with pytest.raises(ApiError, match="create_view"):
        await tools.upsert_metric(state, role, _request(role), "nope", "SUM(orders.amount)")
    with pytest.raises(ApiError, match="create_view"):
        await tools.delete_metric(state, role, _request(role), "revenue")
    names = await _names(gated, metrics)
    assert "nope" not in names
    assert "revenue" in names


def test_the_metric_tools_are_offered_on_create_view():
    for name in ("upsert_metric", "delete_metric"):
        assert tool_groups.RIGHTS_OF[name] == frozenset({"create_view"}), name
