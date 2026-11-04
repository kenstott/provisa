# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9e4b71-6f08-4d3a-9e15-b7a0d4f2c863
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo template loads its demo's scans for terms, carried in its bundle (REQ-2201), when it is
prepared and at every rebuild of its model, whose glossary terms the drafts name. Its sandboxes
are cloned with them (env_copy.copy_term_scans). A template without them would give every sandbox
an empty review queue, and the demo's tour opens the scan done ahead."""

# Requirements: REQ-2201

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import yaml

from provisa.api import demo_templates
from provisa.term_finding import carry

_RULE = (
    "https://www.ecfr.gov/api/versioner/v1/full/2026-10-15/title-31.xml?part=1010&section=1010.311"
)


def _demo(tmp_path, steps):
    (tmp_path / "setup.yaml").write_text(yaml.safe_dump({"steps": steps}))
    return SimpleNamespace(name="bsa", directory=tmp_path)


def _carried(tmp_path):
    carry.scans_path(tmp_path).parent.mkdir()
    carry.scans_path(tmp_path).write_text('{"documents": [], "scans": []}')


def test_the_scanned_documents_are_the_setup_s_scan_steps(tmp_path):
    demo = _demo(tmp_path, [{"command": "x"}, {"scan": _RULE}, {"refresh": "v"}])
    assert demo_templates.demo_scans(demo.directory) == [_RULE]
    assert demo_templates.demo_scans(tmp_path / "none") == []


class _Db:
    """The model database: its one connection, scoped to the org current when it is acquired."""

    def __init__(self, seen: list):
        self.seen = seen

    def acquire(self):
        from provisa.core.request_context import require_current_org

        @asynccontextmanager
        async def transaction():
            yield

        @asynccontextmanager
        async def conn():
            self.seen.append(require_current_org())
            yield SimpleNamespace(transaction=transaction)

        return conn()


def test_the_template_loads_the_scans_its_bundle_carries_as_its_own_org(tmp_path, monkeypatch):
    from provisa.api import app

    seen: list = []
    loaded: list = []

    async def load_scans(_conn, path):
        loaded.append(path)
        return [_RULE]

    monkeypatch.setattr(type(app.state), "model_db", property(lambda _s: _Db(seen)))
    monkeypatch.setattr(carry, "load_scans", load_scans)
    demo = _demo(tmp_path, [{"scan": _RULE}])
    _carried(tmp_path)
    assert asyncio.run(demo_templates.scan_template(demo, "sbxtplbsa")) == []
    assert seen == ["sbxtplbsa"] and loaded == [carry.scans_path(tmp_path)]


def test_a_bundle_that_carries_no_scans_leaves_the_template_not_ready_by_name(tmp_path):
    demo = _demo(tmp_path, [{"scan": _RULE}])
    failed = asyncio.run(demo_templates.scan_template(demo, "sbxtplbsa"))
    assert failed == ["the bundle carries no scans (term_scans.json): rebuild it"]


def test_a_demo_that_scans_nothing_loads_nothing(tmp_path):
    demo = _demo(tmp_path, [{"command": "x"}])
    assert asyncio.run(demo_templates.scan_template(demo, "sbxtplbsa")) == []
    assert asyncio.run(demo_templates.unheld_term_scans(demo)) == []


def test_a_term_the_org_does_not_hold_is_refused_by_name(tmp_path, monkeypatch):
    from provisa.api import app

    async def load_scans(_conn, _path):
        raise ValueError("the bundle's scan of ... names the glossary term 'x', ...")

    monkeypatch.setattr(type(app.state), "model_db", property(lambda _s: _Db([])))
    monkeypatch.setattr(carry, "load_scans", load_scans)
    demo = _demo(tmp_path, [{"scan": _RULE}])
    _carried(tmp_path)
    failed = asyncio.run(demo_templates.scan_template(demo, "sbxtplbsa"))
    assert failed == ["the bundle's scan of ... names the glossary term 'x', ..."]


def test_a_rebuild_loads_the_carried_scans_after_its_apply(tmp_path, monkeypatch):
    from provisa.core import config_loader

    asked: list = []

    async def rebuild_from_config(seed, conn, engine, **carried):
        asked.append(carried)
        return []

    monkeypatch.setattr(config_loader, "rebuild_from_config", rebuild_from_config)
    _demo(tmp_path, [{"scan": _RULE}])
    _carried(tmp_path)
    asyncio.run(demo_templates.rebuild_template(tmp_path, None, None, None))
    assert asked == [{"carried_runs": None, "carried_scans": carry.scans_path(tmp_path)}]


def test_a_rebuild_of_a_bundle_carrying_no_scans_is_refused_by_name(tmp_path):
    _demo(tmp_path, [{"scan": _RULE}])
    with pytest.raises(ValueError, match=r"carries no scans \(term_scans\.json\)"):
        asyncio.run(demo_templates.rebuild_template(tmp_path, None, None, None))
