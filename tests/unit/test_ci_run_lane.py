# Copyright (c) 2026 Kenneth Stott
# Canary: 6eb2c96e-6ab7-4a06-acda-1c5dc33497c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The CI lane runner: its lanes are scripts/test-all's, and its shards partition a lane."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _runner():
    spec = importlib.util.spec_from_file_location(
        "run_lane", REPO / "scripts" / "ci" / "run_lane.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["run_lane"] = module  # dataclasses resolve their module through sys.modules
    spec.loader.exec_module(module)
    return module


def test_the_shards_partition_the_lane():
    run_lane = _runner()
    files = run_lane.test_files(("tests/integration", "tests/steps"))
    assert len(files) > 100
    shards = [run_lane.shard(files, k, 6) for k in range(1, 7)]
    flat = [f for s in shards for f in s]
    assert sorted(flat) == files  # every file once
    assert max(map(len, shards)) - min(map(len, shards)) <= 1


def test_an_out_of_range_shard_is_refused():
    with pytest.raises(ValueError, match="out of range"):
        _runner().shard(["a"], 7, 6)


@pytest.mark.parametrize("lane", ["core", "app", "hive_s3", "kafka", "neo4j", "datastores"])
def test_the_lane_selectors_are_test_alls(lane):
    """scripts/test-all is the local definition of these lanes; CI must select the same tests."""
    test_all = (REPO / "scripts" / "test-all").read_text()
    run_lane = _runner()
    expanded = run_lane.LANES[lane].marker
    selectors = [
        m.replace("$_BASE", run_lane._BASE)
        .replace("$_KAFKA", run_lane._KAFKA)
        .replace("$_DATASTORE", run_lane._DATASTORE)
        .replace("$_HIVE_S3", run_lane._HIVE_S3)
        for m in re.findall(r'-m "([^"]+)"', test_all)
    ]
    assert expanded in selectors


def test_the_base_exclusions_are_test_alls():
    test_all = (REPO / "scripts" / "test-all").read_text()
    run_lane = _runner()
    assert f'_BASE="{run_lane._BASE}"' in test_all
    assert f'_KAFKA="{run_lane._KAFKA}"' in test_all
    assert f'_DATASTORE="{run_lane._DATASTORE}"' in test_all


def test_a_shard_runs_only_its_files():
    run_lane = _runner()
    cmd = run_lane.command("core", "2/6", [])
    files = run_lane.shard(run_lane.test_files(("tests/integration", "tests/steps")), 2, 6)
    assert cmd[3 : 3 + len(files)] == files
    assert "--durations=50" in cmd


def test_all_is_every_suite_lane_with_its_shards():
    include = _runner().matrix("all")["include"]
    assert [m["shard"] for m in include if m["lane"] == "core"] == [f"{k}/6" for k in range(1, 7)]
    assert [m["shard"] for m in include if m["lane"] == "app"] == ["1/3", "2/3", "3/3"]
    assert {"lane": "kafka", "shard": "", "timeout": 90} in include
    assert not [m for m in include if m["lane"] in ("cluster", "warehouse")]


def test_a_selection_runs_only_those_lanes():
    include = _runner().matrix("kafka, app")["include"]
    assert sorted({m["lane"] for m in include}) == ["app", "kafka"]


def test_an_unknown_lane_is_refused():
    with pytest.raises(ValueError, match="no such lane: kafak"):
        _runner().matrix("kafak")


def test_the_workflow_takes_its_matrix_from_the_runner():
    workflow = (REPO / ".github" / "workflows" / "integration-suite-lanes.yml").read_text()
    assert "run_lane.py --matrix" in workflow
    assert "fromJSON(needs.plan.outputs.matrix)" in workflow


def _job_steps(job: str) -> list[dict]:
    import yaml

    workflow = yaml.safe_load(
        (REPO / ".github" / "workflows" / "integration-suite-lanes.yml").read_text()
    )
    return workflow["jobs"][job]["steps"]


@pytest.mark.parametrize("job", ["suite", "cluster", "warehouse"])
def test_every_collecting_job_caches_the_pinned_trino_plugins_around_its_lane(job):
    """tests/conftest.py fetches the pinned Trino plugin jars from Maven Central at collection, and
    a refused fetch ended the lane before any test ran (run 37573213103: neo4j 403, kafka 404).
    Each job that collects tests restores them by pin before its lane and saves them after."""
    steps = _job_steps(job)
    names = [step.get("name") for step in steps]
    lane = names.index("Run lane")
    assert names.index("Trino plugin pin") < names.index("Restore Trino plugins") < lane
    assert lane < names.index("Trino plugins fetched") < names.index("Cache Trino plugins")
    by_name = {step.get("name"): step for step in steps}
    restored = by_name["Restore Trino plugins"]["with"]
    assert by_name["Cache Trino plugins"]["with"] == restored
    assert restored["key"] == "trino-plugins-${{ steps.trino-pin.outputs.version }}"
    assert "always()" in by_name["Cache Trino plugins"]["if"]


def test_the_trino_plugin_pin_step_names_the_harness_pin():
    import subprocess

    pin = next(s for s in _job_steps("suite") if s.get("name") == "Trino plugin pin")
    script = pin["run"].replace('>> "$GITHUB_OUTPUT" ', "")
    out = subprocess.run(  # noqa: S603 — the workflow's own step, run at the repo root
        ["bash", "-c", script], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout
    conftest = (REPO / "tests" / "conftest.py").read_text()
    version = re.search(r'^_TRINO_PLUGIN_VERSION = "([^"]+)"$', conftest, re.M)
    assert version is not None
    lines = out.splitlines()
    assert lines[0] == f"version={version.group(1)}"
    jars = lines[1].removeprefix("jars=").split()
    assert len(jars) == 3
    assert all(j.endswith(f"-{version.group(1)}.jar") for j in jars)
