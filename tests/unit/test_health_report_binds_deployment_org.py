# Copyright (c) 2026 Kenneth Stott
# Canary: a323f145-683c-4a94-809d-7ed730906e9d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``GET /health`` asks for no org, so every per-org read it makes -- the state store probe and the
config stamps -- is bound to the deployment's own org by name (REQ-1266)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.api import health_report as health_report_mod
from provisa.core.request_context import current_org


class _Conn:
    async def fetchval(self, _sql):
        return 1


class _Acquire:
    async def __aenter__(self):
        return _Conn()

    async def __aexit__(self, *_exc):
        return False


class _Db:
    def acquire(self):
        return _Acquire()


@pytest.mark.unbound
def test_a_healthy_report_reads_config_stamps_bound_to_the_deployment_org(monkeypatch):
    monkeypatch.delenv("PROVISA_LAUNCH_ID", raising=False)
    monkeypatch.delenv("PROVISA_WORKERS", raising=False)
    seen: list[str | None] = []

    async def _config_health():
        seen.append(current_org.get(None))
        return {}

    async def _no_nodes(_db):
        return []

    monkeypatch.setattr("provisa.api.model_reload.health", _config_health)
    monkeypatch.setattr("provisa.core.platform_state.nodes.live", _no_nodes)
    state = SimpleNamespace(org_id="deploy", tenant_db=_Db(), platform_state_db=object())

    report = asyncio.run(health_report_mod.health_report(state))

    assert report["dependencies"]["postgres"] == "ok"
    assert seen == ["deploy"]
    assert current_org.get(None) is None


class _BusyDb:
    """A control plane whose every connection is checked out: the checkout gives up as busy."""

    def acquire(self):
        from provisa.core.database import PoolBusy

        class _Refused:
            async def __aenter__(self):
                raise PoolBusy("no control-plane connection freed within 1.0s")

            async def __aexit__(self, *_exc):
                return False

        return _Refused()


@pytest.mark.unbound
def test_a_busy_control_plane_is_reported_busy_at_once_never_healthy(monkeypatch):
    """REQ-1900: the probe answers fast and says why. A pool with no free connection is the
    dependency's state, ``busy``; the report is ``degraded``, and nothing that needs a connection
    is reported as read."""
    from provisa.core.database import PoolBusy

    monkeypatch.delenv("PROVISA_LAUNCH_ID", raising=False)
    monkeypatch.delenv("PROVISA_WORKERS", raising=False)

    async def _busy(*_a):
        raise PoolBusy("no control-plane connection freed within 1.0s")

    monkeypatch.setattr("provisa.api.model_reload.health", _busy)
    monkeypatch.setattr("provisa.core.platform_state.nodes.live", _busy)
    state = SimpleNamespace(org_id="deploy", tenant_db=_BusyDb(), platform_state_db=object())

    report = asyncio.run(health_report_mod.health_report(state))

    assert report["status"] == "degraded"
    assert report["dependencies"]["postgres"] == "busy"
    assert report["config"] is None and report["nodes"] is None


def test_the_probe_bounds_every_checkout_it_makes():
    """Each checkout the report makes waits for a free connection only as long as the probe
    allows (database.checkout_wait), not the 30 s an ordinary request may wait."""
    from provisa.core import database

    with database.checkout_wait(0.2):
        assert database._checkout_wait() == 0.2
    assert database._checkout_wait() is None
