# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7f9c21-6b48-4e0d-9c15-2f8e6a1d47b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-964 (Phase 0): MV registration rejects non-deterministic SQL.

_sync_view_mv is the single register/update hook for a materialized user view.
It must raise before registering an MV whose SQL is non-deterministic, so
recompute-to-current and replay stay sound.
"""

from __future__ import annotations

import types

import pytest

from provisa.api.admin import schema_common


@pytest.fixture
def fake_state(monkeypatch):
    """Minimal app-state stand-in with a spy MV registry and no engine dialect."""
    registered: list = []
    registry = types.SimpleNamespace(
        get=lambda _: None,
        register=lambda mv: registered.append(mv),
        unregister=lambda _: None,
    )
    state = types.SimpleNamespace(mv_registry=registry, org_id="test", engine=None)
    fake_app = types.ModuleType("provisa.api.app")
    fake_app.state = state  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "provisa.api.app", fake_app)
    state.registered = registered
    return state


def test_deterministic_mv_registers(fake_state):
    schema_common._sync_view_mv("sales", "SELECT region, sum(amt) AS t FROM o GROUP BY region", 300)
    assert len(fake_state.registered) == 1
    assert fake_state.registered[0].id == "view-sales"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT now() AS ts, region FROM o",
        "SELECT random() AS r, region FROM o",
        "SELECT a FROM o LIMIT 10",  # unordered LIMIT — arbitrary rows
    ],
)
def test_non_deterministic_mv_rejected(fake_state, sql):
    with pytest.raises(ValueError, match="non-deterministic MV"):
        schema_common._sync_view_mv("bad", sql, 300)
    assert fake_state.registered == []
