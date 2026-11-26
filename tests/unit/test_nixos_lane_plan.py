# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The NixOS lanes: test_demo_open_steady_e2e.py keeps its 20 s bound and runs alone in a job of its
own, its core shards deselecting it (maintainer 2026-11-26); a dispatch can name tests and pytest
arguments, so one test can be run alone with -vv in the guest."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import yaml

_ROOT = Path(__file__).parents[2]
_PLANNER = _ROOT / "scripts" / "ci" / "nixos_lane_plan.py"
_spec = importlib.util.spec_from_file_location("nixos_lane_plan", _PLANNER)
assert _spec is not None and _spec.loader is not None
nixos_lane_plan = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nixos_lane_plan)
ALONE, GUEST_MINUTES, plan = (
    nixos_lane_plan.ALONE,
    nixos_lane_plan.GUEST_MINUTES,
    nixos_lane_plan.plan,
)
_STEADY = "tests/integration/test_demo_open_steady_e2e.py"
_CORE = {
    "include": [
        {"lane": "core", "shard": "1/2", "timeout": 60},
        {"lane": "core", "shard": "2/2", "timeout": 60},
        {"lane": "core_long", "shard": "", "timeout": 80},
    ]
}


def _lane_job() -> dict:
    return yaml.safe_load((_ROOT / ".github" / "workflows" / "nixos.yml").read_text())["jobs"]


def test_the_steady_open_bound_stays_20_seconds_and_its_file_runs_alone():
    source = (_ROOT / _STEADY).read_text()
    assert "_STEADY_BOUND_S = 20.0" in source
    assert _STEADY in ALONE


def test_each_core_shard_deselects_the_alone_file_and_it_gets_a_job_of_its_own():
    include = plan(_CORE, targeted=False)["include"]
    shards = [e for e in include if e["lane"] == "core" and "alone" not in e]
    assert [e["shard"] for e in shards] == ["1/2", "2/2"]
    assert all(e["deselect"] == f"--deselect {_STEADY}" for e in shards)
    (alone,) = [e for e in include if e.get("alone")]
    assert alone == {"lane": "core", "shard": "", "alone": _STEADY, "timeout": 60 + GUEST_MINUTES}
    (long,) = [e for e in include if e["lane"] == "core_long"]
    assert "deselect" not in long and long["timeout"] == 80 + GUEST_MINUTES


def test_a_targeted_run_or_a_run_without_core_is_only_given_the_guest_s_minutes():
    assert plan(_CORE, targeted=True)["include"] == [
        {**e, "timeout": e["timeout"] + GUEST_MINUTES} for e in _CORE["include"]
    ]
    kafka = {"include": [{"lane": "kafka", "shard": "", "timeout": 90}]}
    assert plan(kafka, targeted=False) == {
        "include": [{"lane": "kafka", "shard": "", "timeout": 90 + GUEST_MINUTES}]
    }


def test_the_planner_reads_a_targeted_run_from_the_dispatch_s_named_tests():
    for named, alone_jobs in (("", 1), (_STEADY, 0)):
        done = subprocess.run(
            [sys.executable, str(_PLANNER), json.dumps(_CORE)],
            capture_output=True,
            text=True,
            check=True,
            env={"PROVISA_LANE_TESTS": named},
        )
        include = json.loads(done.stdout)["include"]
        assert sum(1 for e in include if e.get("alone")) == alone_jobs


def test_the_workflow_plans_with_the_nixos_planner_and_runs_what_it_planned():
    jobs = _lane_job()
    step = next(s for s in jobs["plan"]["steps"] if s.get("id") == "plan")
    assert step["env"]["PROVISA_LANE_TESTS"] == "${{ inputs.tests }}"
    assert 'scripts/ci/nixos_lane_plan.py "$(python3 scripts/ci/run_lane.py --matrix' in step["run"]
    lane = jobs["lane"]
    assert lane["name"].endswith("${{ matrix.alone && ' alone' || '' }}")
    steps = {s.get("name"): s.get("run") or "" for s in lane["steps"]}
    assert "${{ matrix.alone && '-alone' || '' }}" in steps["Lane name"]
    run = steps["Run lane"]
    assert 'export PROVISA_LANE_TESTS="${{ matrix.alone || inputs.tests }}"' in run
    assert "${{ matrix.deselect }} ${{ inputs.pytest_args }} --junitxml" in run


def test_a_dispatch_names_its_tests_and_pytest_arguments():
    on = yaml.safe_load((_ROOT / ".github" / "workflows" / "nixos.yml").read_text())
    inputs = on[True]["workflow_dispatch"]["inputs"]
    assert inputs["tests"]["default"] == "" and inputs["pytest_args"]["default"] == ""
