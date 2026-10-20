# Copyright (c) 2026 Kenneth Stott
# Canary: e5b002a1-0d45-412a-afa8-3105785082ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo template runs its demo's profilers (REQ-2052). Its sandboxes are cloned with its
profile runs (env_copy.copy_profile_results); the bundle build's runs are on its throwaway
control plane, so a template that profiled nothing gave every sandbox of the profiling demo an
empty run history, and its tour's runs step waited out its time on cloud-dev."""

# Requirements: REQ-2052, REQ-2006

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import yaml

from provisa.api import demo_templates


def _demo(tmp_path, steps):
    (tmp_path / "setup.yaml").write_text(yaml.safe_dump({"steps": steps}))
    return SimpleNamespace(name="profiling", directory=tmp_path)


def test_the_profilers_are_the_setup_s_profile_steps(tmp_path):
    demo = _demo(tmp_path, [{"command": "x"}, {"profile": "harbor-profiler"}, {"refresh": "v"}])
    assert demo_templates.demo_profilers(demo.directory) == ["harbor-profiler"]
    assert demo_templates.demo_profilers(tmp_path / "none") == []


def test_the_template_runs_each_profiler_and_names_a_failed_run(tmp_path, monkeypatch):
    from provisa.core.request_context import require_current_org
    from provisa.profiler import source

    ran: list[tuple[str, str | None]] = []

    async def run_source(_state, profiler):
        ran.append((profiler, require_current_org()))
        return [
            {"table": "customers", "run_id": "r1", "error": None},
            {"table": "orders", "run_id": None, "error": "no rows"},
        ]

    monkeypatch.setattr(source, "run_source", run_source)
    demo = _demo(tmp_path, [{"profile": "harbor-profiler"}])
    failed = asyncio.run(demo_templates.profile_template(demo, "sbxtplprofiling"))
    assert ran == [("harbor-profiler", "sbxtplprofiling")]
    assert failed == ["harbor-profiler/orders: no rows"]


def test_a_template_still_being_prepared_is_not_ready(monkeypatch):
    """Its preparation -- its profile runs among it -- is what a sandbox is cloned from. A demo
    with no replica read as ready from its provisioning alone, so the profiling demo's first
    sandbox was cloned before its template had profiled anything. One whose preparation failed is
    refused too: testdata's sandbox was opened from a template whose dev was never generated, and
    the visitor was answered 500 by the copy."""
    import pytest

    from provisa.api import demo_sandbox
    from provisa.api.admin import orgs_router
    from provisa.api.errors import ApiError

    class _Conn:
        async def execute_core(self, _stmt):
            return SimpleNamespace(fetchone=lambda: ("ready",))  # provisioned

    class _Pool:
        def acquire(self):
            from contextlib import asynccontextmanager

            @asynccontextmanager
            async def conn():
                yield _Conn()

            return conn()

    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: _Pool())
    demo = SimpleNamespace(name="profiling", template_org="sbxtplprofiling", title="Profiling")
    for state in ("queued", "building", "failed"):
        monkeypatch.setattr(demo_templates.template_builds, "state_of", lambda _n, s=state: s)
        with pytest.raises(ApiError) as refused:
            asyncio.run(demo_sandbox._template_ready(demo))
        assert refused.value.code == "sandbox.demo_not_ready"
    monkeypatch.setattr(demo_templates.template_builds, "state_of", lambda _n: "ready")
    asyncio.run(demo_sandbox._template_ready(demo))
