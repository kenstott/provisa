# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7f9b04-6a13-4d58-8e92-b40d6c1f7a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow Flight answers a ``healthcheck`` action with the report ``GET /health`` gives."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pyarrow.flight as flight

from provisa.api import health_report as health_report_mod
from provisa.api.flight.server import ProvisaFlightServer


def _server(state) -> ProvisaFlightServer:
    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    server._state = state
    return server


def test_the_report_of_a_worker_with_no_control_plane_has_the_health_shape(monkeypatch):
    monkeypatch.delenv("PROVISA_LAUNCH_ID", raising=False)
    monkeypatch.delenv("PROVISA_WORKERS", raising=False)

    async def _no_nodes(_db):
        return []

    monkeypatch.setattr("provisa.core.platform_state.nodes.live", _no_nodes)
    report = asyncio.run(
        health_report_mod.health_report(
            SimpleNamespace(model_db=None, tenant_db=None, admin_db=object())
        )
    )
    assert report == {
        "status": "ok",
        "dependencies": {"postgres": "unavailable"},
        "workers": {"ready": 1, "expected": 1},
        "config": None,
        "nodes": [],  # REQ-1916: the cluster's node list
    }


def test_the_healthcheck_action_answers_that_report_as_one_json_result(monkeypatch):
    seen = {}

    async def _report(state):
        seen["state"] = state
        return {
            "status": "ok",
            "dependencies": {"postgres": "ok"},
            "workers": {"ready": 4, "expected": 4},
            "config": {"model": {"loaded": 7, "stored": 7}},
        }

    monkeypatch.setattr(health_report_mod, "health_report", _report)
    state = SimpleNamespace()
    results = _server(state).do_action(None, flight.Action("healthcheck", b""))
    assert len(results) == 1
    assert json.loads(results[0].body.to_pybytes()) == {
        "status": "ok",
        "dependencies": {"postgres": "ok"},
        "workers": {"ready": 4, "expected": 4},
        "config": {"model": {"loaded": 7, "stored": 7}},
    }
    assert seen["state"] is state


def test_the_action_is_listed_by_name():
    names = [name for name, _description in _server(SimpleNamespace()).list_actions(None)]
    assert names == ["healthcheck"]


def test_another_action_is_answered_as_before():
    body = json.dumps({"query": "SELECT 1"}).encode()
    assert _server(SimpleNamespace()).do_action(None, flight.Action("trace", body)) == []
