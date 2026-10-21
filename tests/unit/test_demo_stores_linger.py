# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2049: a demo's stores linger sandbox.demo_stores_linger seconds after its last sandbox
closes. The tier marks the time in <name>/requested when none is left open, and marks it
requested again while one is; the node takes the stores down once the time has passed
(scripts/demo_stores_sync.sh, tested in test_demo_bundle_setup.py)."""

# Requirements: REQ-2049

from __future__ import annotations

import time
from contextlib import asynccontextmanager

import pytest

from provisa.core import demo_bundles


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Pool:
    def __init__(self, open_orgs):
        self._open = open_orgs

    @asynccontextmanager
    async def acquire(self):
        pool = self

        class _Conn:
            async def execute_core(self, _stmt):
                return _Rows([(org,) for org in pool._open])

        yield _Conn()


@pytest.fixture
def bundles(tmp_path, monkeypatch):
    from provisa.api.admin import orgs_router
    from provisa.core import settings_registry

    root = tmp_path / "bundles"
    (root / "weblogs").mkdir(parents=True)
    (root / "weblogs" / "requested").write_text("requested\n")
    monkeypatch.setattr(demo_bundles, "bundles_root", lambda: root)
    monkeypatch.setenv("PROVISA_DEMO_BUNDLES", "weblogs")
    monkeypatch.setattr(demo_bundles, "own_demo_directory", lambda _org: None)
    real = settings_registry.value
    monkeypatch.setattr(
        settings_registry,
        "value",
        lambda key: 1800 if key == "sandbox.demo_stores_linger" else real(key),
    )

    def open_now(orgs):
        monkeypatch.setattr(orgs_router, "_admin_pool", lambda: _Pool(orgs))

    return root / "weblogs", open_now


@pytest.mark.asyncio
async def test_the_last_sandbox_closing_marks_when_its_stores_are_taken_down(bundles):
    from provisa.api.demo_sandbox import note_in_use

    demo, open_now = bundles
    open_now([])
    before = time.time()
    await note_in_use("weblogs")
    marked = (demo / "requested").read_text().strip()
    assert marked.startswith("until ")
    assert before + 1800 - 1 <= int(marked.removeprefix("until ")) <= time.time() + 1800


@pytest.mark.asyncio
async def test_a_sandbox_open_again_keeps_its_stores_up(bundles):
    from provisa.api.demo_sandbox import note_in_use

    demo, open_now = bundles
    open_now([])
    await note_in_use("weblogs")
    open_now(["sbx0123456789ab"])
    await note_in_use("weblogs")
    assert (demo / "requested").read_text().strip() == "requested"


def test_the_linger_is_an_operator_setting_of_half_an_hour():
    from provisa.core.settings_catalog_sandbox import DECLARED

    (setting,) = [s for s in DECLARED if s.key == "sandbox.demo_stores_linger"]
    assert setting.default == 1800 and setting.unit == "seconds"
