# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e91a4-5b2d-4f08-9e6a-d18f0b47c2e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org reaped while the config watcher holds it is not read again (REQ-1914, REQ-2006).

The watcher lists its targets at the start of a check and reads each plane in turn. A demo
sandbox reaped during that check has its runtime evicted (``destroy_org``) before its schemas are
dropped: from that moment the watcher no longer watches it, and the rest of the check does not
read the org's planes."""

# Requirements: REQ-1914, REQ-2006

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api import model_reload
from provisa.api.org_runtime import OrgRegistry, OrgRuntime
from provisa.core import config_stamp, config_watch, deployment_settings

pytestmark = pytest.mark.unit

SANDBOX = "sbxf1abe297857b"
STAMPS = {config_stamp.MODEL: 1, config_stamp.SETTINGS: 1, config_stamp.REPLICA: 1}


async def test_an_org_reaped_while_the_watcher_holds_it_is_not_read_again(monkeypatch):
    admin_db = SimpleNamespace(name="admin plane")
    rt = OrgRuntime(org_id=SANDBOX)
    rt.model_db = SimpleNamespace(name="sandbox model plane")
    rt.tenant_db = SimpleNamespace(name="sandbox tenant plane")
    rt.model_stamp = rt.settings_stamp = rt.replica_stamp = 1
    registry = OrgRegistry()
    registry.set(SANDBOX, rt)
    monkeypatch.setattr(
        "provisa.api.app.state",
        SimpleNamespace(admin_db=admin_db, org_registry=registry),
        raising=False,
    )
    monkeypatch.setattr(deployment_settings, "loaded_stamp", lambda: 1)
    read: list[str] = []

    async def _read(db):
        read.append(db.name)
        if db is admin_db:
            # The reaper, while this check reads the operator settings: the runtime is evicted,
            # then the org's schemas are dropped (destroy_org).
            registry.invalidate_org(SANDBOX)
        return STAMPS

    monkeypatch.setattr(config_stamp, "read", _read)

    watched = model_reload.targets()  # listed before the reap
    assert {t.name for t in watched} >= {f"org {SANDBOX}: model", f"org {SANDBOX}: replicas"}

    assert await config_watch.check(watched) == []
    assert read == ["admin plane"], "the watcher read a reaped org's planes"
    assert all(t.db is admin_db for t in model_reload.targets())
