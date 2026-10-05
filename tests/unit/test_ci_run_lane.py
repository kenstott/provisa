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
    workflow = (REPO / ".github" / "workflows" / "integration-suite.yml").read_text()
    assert "run_lane.py --matrix" in workflow
    assert "fromJSON(needs.plan.outputs.matrix)" in workflow


@pytest.mark.parametrize("lane", ["core", "app", "e2e", "warehouse"])
def test_every_lane_bounds_each_test(lane):
    """A hung test failed nothing: core 6/6 held its runner until the job was cancelled, and a
    cancelled job's log is gone. Every lane command now bounds each test (pytest-timeout), so a hang
    fails by name with every thread's stack."""
    run_lane = _runner()
    cmd = run_lane.command(lane, None, [])
    assert cmd[cmd.index("--timeout") + 1] == str(run_lane.TEST_TIMEOUT_S)
    assert cmd[cmd.index("--timeout-method") + 1] == "signal"


def test_files_lists_exactly_the_modules_the_shard_runs(capsys):
    """The workflow's Splunk CIM step reads --files to decide whether this shard boots splunk; the
    list must be the shard's own files, or a shard that runs a splunk test would boot it empty."""
    run_lane = _runner()
    assert run_lane.main(["app", "--shard", "3/3", "--files"]) == 0
    listed = capsys.readouterr().out.split()
    assert listed == run_lane.shard(run_lane.test_files(("tests/integration", "tests/steps")), 3, 3)


def test_files_expands_an_unsharded_lane_to_its_modules(capsys):
    run_lane = _runner()
    assert run_lane.main(["e2e", "--files"]) == 0
    listed = capsys.readouterr().out.split()
    assert listed == run_lane.test_files(("tests/e2e",))
    assert listed and all(f.endswith(".py") for f in listed)
