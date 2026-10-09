# Copyright (c) 2026 Kenneth Stott
# Canary: 9b0d692c-8ff1-4603-8843-c5ee5fe0780d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The core UI lane runs the specs that start and stop containers alone, one worker, last.

A compose stack brought up or down is a Docker network made or removed on the host, and Chromium
aborts every request in flight when the host's interfaces change (net::ERR_NETWORK_CHANGED) --
in whichever worker's page is loading. On v0.1.0-alpha.477 seventeen core-lane tests failed on it
(run 37874182519). playwright.config.ts names those specs (PROVISIONING_SPECS) and gives them a
project of their own; the workflow and the npm script run it after "core" with one worker."""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
UI = REPO / "provisa-ui"
CONFIG = (UI / "playwright.config.ts").read_text()
RUNNER = "playwright " + "test"

# What marks a spec as changing the host's Docker networks while it runs.
_STARTS_CONTAINERS = re.compile(
    r"\"sources\",\s*\"provision\.py\"|spawnSync\(\s*\"docker\"|execFileSync\(\s*\"docker\"|"
    r"startDemoSources\(|removeDemoSources\("
)


def _list(name: str) -> set[str]:
    body = re.search(rf"const {name} = \[(.*?)\];", CONFIG, re.S)
    assert body, name
    return {m.rsplit("/", 1)[-1] for m in re.findall(r'"([^"]+)"', body.group(1))}


def test_every_core_spec_that_starts_containers_is_in_the_provisioning_project():
    elsewhere = _list("TRINO_SPECS") | _list("SWAP_SPECS") | _list("REGIONS_SPECS")
    starts = {
        spec.name
        for spec in (UI / "e2e").glob("*.spec.ts")
        if _STARTS_CONTAINERS.search(spec.read_text()) and spec.name not in elsewhere
    }
    assert starts, "the marker no longer matches any spec"
    assert starts <= _list("PROVISIONING_SPECS"), sorted(starts - _list("PROVISIONING_SPECS"))


def test_the_core_project_leaves_those_specs_to_the_provisioning_project():
    core = re.search(r'name: "core",\s*testIgnore: \[(.*?)\]', CONFIG, re.S)
    assert core and "...PROVISIONING_SPECS" in core.group(1)
    assert re.search(r'name: "core-provisioning",\s*testMatch: PROVISIONING_SPECS', CONFIG)


def test_the_lane_runs_the_provisioning_project_alone_with_one_worker():
    """Two jobs of the core UI workflow, each a call of the lane workflow and so each on a runner
    of its own: "core" with the lane's workers, and "core-provisioning" with one. No other
    worker's page is on the host whose networks the provisioning specs change."""
    workflow = yaml.safe_load((REPO / ".github/workflows/ui-e2e-core.yml").read_text())
    jobs = workflow["jobs"]
    assert {job: body["uses"] for job, body in jobs.items()} == {
        "playwright": "./.github/workflows/lane.yml",
        "provisioning": "./.github/workflows/lane.yml",
    }
    core, provisioning = jobs["playwright"]["with"]["run"], jobs["provisioning"]["with"]["run"]
    assert RUNNER in core and "--project=core " in core and "--workers" not in core
    assert RUNNER in provisioning and "--project=core-provisioning --workers=1" in provisioning
    assert "needs" not in jobs["provisioning"]  # side by side, not after
    # Each keeps a report of its own.
    names = {body["with"]["artifact-name"] for body in jobs.values()}
    assert names == {"playwright-report-core", "playwright-report-core-provisioning"}
    # The provisioning job is the one that starts SQL Server's primer.
    assert jobs["provisioning"]["with"]["odbc"] is True

    script = json.loads((UI / "package.json").read_text())["scripts"]["test:e2e:core"]
    first = re.search(r"--project=core(?!-)", script)
    second = script.index("--project=core-provisioning")
    assert first and first.start() < second and "--workers=1" in script[second:]
