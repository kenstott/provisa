# Copyright (c) 2026 Kenneth Stott
# Canary: 514fce9b-0ca4-4c78-8909-5f32bdb323b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the named demo ``profiling`` shows its profile runs in a sandbox as the tier deploys it.

Its bundle is built as the Demo bundles workflow builds it, published to a registry of this test's
own and synced as the node syncs it. A sandbox tier serving it opens a visitor's sandbox, and the
tour's runs step -- the profile runs of the customers table -- is run as its visitor. A profile
run's results are tables of the org's own schema, carried into each sandbox from its template
(env_copy.copy_profile_results); the bundle build's runs, made on its throwaway control plane,
travel in the bundle and the template loads them (REQ-2052). A template with no runs left every
sandbox with none, and the step waited out its time on cloud-dev; one that profiled its sources
itself measured the data as setup left it, and flagged no drift. Each statement runs in the
environment the tour's switcher last picked: synthetic's rows are generated from the profile."""

# Requirements: REQ-2052, REQ-2049, REQ-2006

from __future__ import annotations

import json
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
_NAME = "profiling"
_ORG = "profilingtiere2e"
_STEPS = yaml.safe_load((_REPO / "demo" / "named" / _NAME / "tour.yaml").read_text())["steps"]
# Every measure the latest run flags drifting, with the columns it describes: the tour's step shows
# the first rows only.
_ALL_DRIFTING = "allDrifting"
_ALL_DRIFTING_SQL = (
    'SELECT d.scope, d.column_name, d.subject, d.measure, d.involved_columns FROM "harbor"."customer_drift" '
    "AS d WHERE d.drifting AND d.run_time = "
    '(SELECT MAX(p.run_time) FROM "harbor"."customer_drift" AS p)'
)


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


def _sql(tier, sandbox: dict, sql: str, travel: dict[str, str]) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {sandbox['token']}", "x-org-provisa": sandbox["org_id"]}
    if travel.get("as_of"):
        headers["X-Provisa-As-Of"] = travel["as_of"]
    if travel.get("as_recorded"):
        headers["X-Provisa-As-Recorded"] = travel["as_recorded"]
    # The environment the tour's switcher last picked; prod is a request naming none.
    if travel.get("env", "prod") != "prod":
        headers["x-provisa-env"] = travel["env"]
    resp = httpx.post(f"{tier.base_url}/data/sql", json={"sql": sql}, headers=headers, timeout=300)
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


@pytest.fixture(scope="module")
def answers(tmp_path_factory, registry) -> dict[str, list[dict[str, Any]]]:
    """The tour's statements, run in a visitor's sandbox as the tour runs them."""
    work = tmp_path_factory.mktemp("profiling_tier")
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
            travel: dict[str, str] = {}
            out: dict[str, list[dict[str, Any]]] = {}
            for step in _STEPS:
                if "as_of" in step:
                    travel["as_of"] = step["as_of"]
                if "env" in step:
                    travel["env"] = step["env"]
                if "as_recorded_from" in step:
                    taken = step["as_recorded_from"]
                    travel["as_recorded"] = str(out[taken["step"]][taken["row"]][taken["column"]])
                if "sql" in step:
                    out[step["key"]] = _sql(tier, sandbox, step["sql"], dict(travel))
                if step["key"] == "profilingDrift":
                    out[_ALL_DRIFTING] = _sql(tier, sandbox, _ALL_DRIFTING_SQL, dict(travel))
            return out
        finally:
            _close(sandbox["org_id"])
    finally:
        tier.stop_process()


def test_a_new_sandbox_shows_the_templates_profile_runs(answers):
    runs = answers["profilingRuns"]
    assert runs, "the sandbox holds no profile run"
    assert all(r["status"] for r in runs), runs


def test_the_second_run_finds_the_column_setup_changed_drifting(answers):
    """REQ-2052: the build's two runs, carried in the bundle: between them its setup wrote every US
    postal code as ZIP+4. A template that profiled its sources itself compared two runs of the same
    data and flagged nothing."""
    assert answers["profilingDrift"], "the latest run flags nothing drifting"
    drifting = answers[_ALL_DRIFTING]
    # A measure relating postal_code to another column (a correlation, a dependency) moves with it;
    # one describing columns without postal_code is of data the sync left alone. A table's measures
    # (its duplicate rows) describe no column.
    described = [json.loads(r["involved_columns"]) for r in drifting]
    assert all("postal_code" in cols for cols in described if cols), drifting
    columns = {r["column_name"] for r in drifting if r["scope"] in ("column", "category")}
    assert columns == {"postal_code"}, drifting


def test_the_synthetic_environment_answers_with_generated_customers_only(answers):
    real, generated = answers["profilingOriginal"], answers["profilingSynthetic"]
    assert real and generated
    assert set(generated[0]) == set(real[0])
    assert not {tuple(r.values()) for r in generated} & {tuple(r.values()) for r in real}
