# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8f1d27-6e49-4a0c-9d52-7c1e4f6a8b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The operator's debug-trace admin API (REQ-1910).

Start, list and stop debug-trace windows and set the per-role hint permission, against a real
control-plane store. What is pinned: the write and the read are both gated on the deployment-wide
right, a window reports the time it has left, and a bad window is a 400 naming what is wrong.
"""

# Requirements: REQ-1910

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import debug_trace_router as dr
from provisa.api.errors import ApiError
from provisa.core import trace_scope as ts
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import debug_trace_hint_roles, debug_trace_windows, metadata


@pytest.fixture
def db(tmp_path, monkeypatch) -> Database:
    database = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"), name="debug-trace-api"
    )
    with database.engine.begin() as conn:
        metadata.create_all(conn, tables=[debug_trace_windows, debug_trace_hint_roles])
    monkeypatch.setattr(dr, "_admin_pool", lambda: database)
    monkeypatch.setattr(dr, "require_platform_settings", lambda _r: None)
    ts.invalidate()
    return database


def _request(user_id: str = "ops@example.com"):
    return SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id=user_id)))


@pytest.mark.asyncio
async def test_a_deployment_with_no_windows_reads_as_none_open(db):
    result = await dr.read_debug_trace(_request())
    assert result["windows"] == []
    assert result["hint_roles"] == []
    assert result["max_minutes"] == ts.MAX_WINDOW_MINUTES
    assert result["hint_setting"] == ts.HINT_SETTING


@pytest.mark.asyncio
async def test_starting_a_window_lists_it_with_its_time_remaining_and_who_opened_it(db):
    result = await dr.start_debug_window(
        _request(), dr.WindowBody(scope="org", org_id="acme", minutes=15)
    )
    (window,) = result["windows"]
    assert (window["scope"], window["org_id"], window["target"]) == ("org", "acme", None)
    assert 15 * 60 - 5 <= window["remaining_seconds"] <= 15 * 60
    assert window["created_by"] == "ops@example.com"


@pytest.mark.asyncio
async def test_a_role_window_and_a_source_window_carry_their_target(db):
    await dr.start_debug_window(
        _request(), dr.WindowBody(scope="role", org_id="acme", target=" analyst ", minutes=5)
    )
    result = await dr.start_debug_window(
        _request(), dr.WindowBody(scope="source", org_id="acme", target="sales_pg", minutes=10)
    )
    assert [(w["scope"], w["target"]) for w in result["windows"]] == [
        ("role", "analyst"),
        ("source", "sales_pg"),
    ]


@pytest.mark.asyncio
async def test_stopping_a_window_removes_it_and_an_unknown_one_is_404(db):
    started = await dr.start_debug_window(
        _request(), dr.WindowBody(scope="org", org_id="acme", minutes=15)
    )
    window_id = started["windows"][0]["id"]
    assert (await dr.stop_debug_window(_request(), window_id))["windows"] == []
    with pytest.raises(ApiError) as exc:
        await dr.stop_debug_window(_request(), window_id)
    assert exc.value.status_code == 404
    assert exc.value.code == "debug_trace.window_not_found"


@pytest.mark.parametrize(
    "body",
    [
        dr.WindowBody(scope="org", org_id="acme", minutes=0),
        dr.WindowBody(scope="org", org_id="acme", minutes=ts.MAX_WINDOW_MINUTES + 1),
        dr.WindowBody(scope="role", org_id="acme", minutes=5),
        dr.WindowBody(scope="source", org_id="acme", target="  ", minutes=5),
        dr.WindowBody(scope="table", org_id="acme", target="t", minutes=5),
    ],
)
@pytest.mark.asyncio
async def test_a_window_without_a_bounded_duration_or_its_target_is_refused(db, body):
    with pytest.raises(ApiError) as exc:
        await dr.start_debug_window(_request(), body)
    assert exc.value.status_code == 400
    assert exc.value.code == "debug_trace.invalid_window"
    assert (await dr.read_debug_trace(_request()))["windows"] == []


@pytest.mark.asyncio
async def test_hint_permission_is_set_and_cleared_per_role(db):
    result = await dr.set_hint_role(
        _request(), dr.HintRoleBody(org_id="acme", role_id="analyst", permitted=True)
    )
    assert result["hint_roles"] == [{"org_id": "acme", "role_id": "analyst"}]
    result = await dr.set_hint_role(
        _request(), dr.HintRoleBody(org_id="acme", role_id="analyst", permitted=False)
    )
    assert result["hint_roles"] == []


@pytest.mark.parametrize("call", ["read", "start", "stop", "hint"])
@pytest.mark.asyncio
async def test_every_endpoint_is_gated_on_the_deployment_wide_right(db, monkeypatch, call):
    def _deny(_request):
        raise ApiError(403, "platform.settings_capability_required", "denied")

    monkeypatch.setattr(dr, "require_platform_settings", _deny)
    calls = {
        "read": lambda: dr.read_debug_trace(_request()),
        "start": lambda: dr.start_debug_window(
            _request(), dr.WindowBody(scope="org", org_id="acme", minutes=5)
        ),
        "stop": lambda: dr.stop_debug_window(_request(), "any"),
        "hint": lambda: dr.set_hint_role(
            _request(), dr.HintRoleBody(org_id="acme", role_id="analyst", permitted=True)
        ),
    }
    with pytest.raises(ApiError) as exc:
        await calls[call]()
    assert exc.value.status_code == 403
    assert await ts.list_windows(db) == []
    assert await ts.list_hint_roles(db) == []
