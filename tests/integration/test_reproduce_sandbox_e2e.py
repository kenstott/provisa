# Copyright (c) 2026 Kenneth Stott
# Canary: 4f1c2b7e-9a3d-4e8b-b6f2-1d7c5a9e3b40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the named demo ``reproduce`` works in a sandbox as the sandbox tier deploys it.

Its bundle is built as the Demo bundles workflow builds it (scripts/demo_bundle.py build: the
setup files the report, corrects a rate and builds it again), published to a registry of this
test's own, and synced as the node syncs it. A sandbox tier serving it (PROVISA_DEMO_BUNDLES) opens
a visitor's sandbox, and the tour's statements are run as its visitor, in order, under the time
travel the steps before them set. The records of the two refreshes the build ran travel with the
history they built (REQ-862), so the refreshes step answers them and the as-recorded time the
tour takes from it reads the report as it was filed."""

# Requirements: REQ-862, REQ-1162, REQ-2049, REQ-2083

from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from tests.integration.test_demo_bundle_build_e2e import build
from tests.integration.test_demo_bundle_sandbox_e2e import _with_a_source_of_its_own
from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET, _deployment_config

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_REPO = Path(__file__).resolve().parents[2]
_NAME = "reproduce"
_ORG = "reproducetiere2e"
# The rates desk: 1,000,000 USD at 1.00 and 500,000 EUR at 1.08 as filed, at 1.10 once corrected.
_FILED, _CORRECTED = 1_540_000, 1_550_000
_STEPS = yaml.safe_load((_REPO / "demo" / "named" / _NAME / "tour.yaml").read_text())["steps"]


@pytest.fixture(scope="module")
def registry():
    from tests.integration.demo_registry import DemoRegistry

    reg = DemoRegistry()
    reg.start()
    yield reg
    reg.stop()


def _published(named: Path) -> None:
    """The demo as checked in, offered as a sandbox: its card is not this test's concern."""
    demo = named / _NAME
    shutil.copytree(_REPO / "demo" / "named" / _NAME, demo)
    spec = yaml.safe_load((demo / "demo.yaml").read_text())
    spec.update(published=True, requires_provisa="0.1.0")
    (demo / "demo.yaml").write_text(yaml.safe_dump(spec))


def _tier(work: Path, registry):
    from tests.integration.isolated_server import IsolatedServer

    config = work / "config.yaml"
    _deployment_config(config)
    _with_a_source_of_its_own(config, work)
    return IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_DEMO_BUNDLES": _NAME,
            "PROVISA_DEMO_REGISTRY": registry.url,
            "PROVISA_DATA_DIR": str(work / "data"),
            # As packaging/linux/first-launch.sh's provisa-sandbox: the SDK on, its trace records
            # on the data volume.
            "OTEL_SDK_DISABLED": "false",
            "PROVISA_TELEMETRY_DIR": str(work / "data" / "telemetry"),
        },
    )


def _open(tier) -> dict:
    deadline = time.monotonic() + 300
    while True:
        opened = httpx.post(f"{tier.base_url}/public/demos/{_NAME}", timeout=300)
        if opened.status_code == 200:
            return opened.json()
        assert opened.json().get("code") == "sandbox.demo_not_ready", (
            opened.text,
            tier.dump_stderr_debug()[-6000:],
        )
        assert time.monotonic() < deadline, tier.dump_stderr_debug()[-6000:]
        time.sleep(2)


def _close(org_id: str) -> None:
    """The session's control plane is shared by every module's tier: a sandbox left open here
    counts against the next tier's sandbox.max_concurrent."""
    import os

    from sqlalchemy import create_engine, delete

    from provisa.core.schema_admin import demo_sandboxes

    engine = create_engine(os.environ["PLATFORM_DATABASE_URL"])
    try:
        with engine.begin() as conn:
            conn.execute(delete(demo_sandboxes).where(demo_sandboxes.c.org_id == org_id))
    finally:
        engine.dispose()


def _sql(tier, sandbox: dict, sql: str, params: list[str]) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {sandbox['token']}", "x-org-provisa": sandbox["org_id"]}
    body = {"sql": sql, **({"params": params} if params else {})}
    resp = httpx.post(f"{tier.base_url}/data/sql", json=body, headers=headers, timeout=300)
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def _rates(rows: list[dict[str, Any]]) -> float:
    [rates] = [r for r in rows if r["desk"] == "rates"]
    return float(rates["exposure"])


@pytest.fixture(scope="module")
def answers(tmp_path_factory, registry) -> dict[str, list[dict[str, Any]]]:
    """The tour's statements, run in a visitor's sandbox as the tour runs them."""
    work = tmp_path_factory.mktemp("reproduce_tier")
    named = work / "named"
    _published(named)
    registry.push(build(named, _NAME, work / "out", "d" * 40))
    done = registry.sync(_NAME, work / "data" / "demo-bundles")
    assert done.returncode == 0, (done.stdout, done.stderr)
    tier = _tier(work, registry)
    tier.start(timeout=400)
    try:
        sandbox = _open(tier)
        try:
            # A time an as_recorded_from step takes is bound to the next statement's $1.
            bound: list[str] = []
            out: dict[str, list[dict[str, Any]]] = {}
            for step in _STEPS:
                if "as_recorded_from" in step:
                    taken = step["as_recorded_from"]
                    bound = [str(out[taken["step"]][taken["row"]][taken["column"]])]
                if "sql" in step:
                    out[step["key"]] = _sql(tier, sandbox, step["sql"], bound)
                    bound = []
            return out
        finally:
            _close(sandbox["org_id"])
    finally:
        tier.stop_process()


def test_the_sandbox_holds_the_two_refreshes_the_build_ran(answers):
    # The build's two -- as filed, then after the correction -- carried with the history they
    # built; the template's and the sandbox's builds over unchanged sources record none of their own.
    assert len(answers["reproduceRefreshes"]) == 2, answers["reproduceRefreshes"]
    first, second = answers["reproduceRefreshes"]
    assert first["definition_version"] == second["definition_version"]
    assert first["refreshed_at"] < second["refreshed_at"]


def test_the_report_now_carries_the_correction(answers):
    assert _rates(answers["reproduceNow"]) == _CORRECTED


def test_as_recorded_when_filed_is_the_figure_the_regulator_received(answers):
    assert _rates(answers["reproduceFiled"]) == _FILED
