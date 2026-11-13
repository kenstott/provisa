# Copyright (c) 2026 Kenneth Stott
# Canary: d1f97a91-2b56-49d4-a24a-df31230fdf26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The core lane's shards, planned from measured file times (REQ-1989).

Core 3/6 of release 509 never finished in five full runs: every sixth file of the lane gave it
the three engine replays, about 147 minutes of tests in a 150-minute job. The shards are now
planned from scripts/ci/lane_file_seconds.json, so that none holds more than its target, and a file
no core shard can hold runs in core_long."""

from __future__ import annotations

import importlib.util
import json
import random
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
_CI = REPO / "scripts" / "ci"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, _CI / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolve their module through sys.modules
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def run_lane():
    return _load("run_lane")


def _shards(run_lane, lane: str) -> list[list[str]]:
    total = run_lane.SUITE[lane][0]
    return [run_lane.targets(lane, f"{k}/{total}") for k in range(1, total + 1)]


def test_the_packing_is_deterministic(run_lane):
    files = run_lane.lane_files("core")
    seconds = run_lane.recorded_seconds("core")
    total = run_lane.SUITE["core"][0]
    planned = run_lane.pack(files, seconds, total)
    shuffled = list(files)
    random.Random(509).shuffle(shuffled)
    assert run_lane.pack(shuffled, seconds, total) == planned
    assert run_lane.pack(files, seconds, total) == planned


def test_largest_first_into_the_least_loaded_shard(run_lane):
    seconds = {"a": 50.0, "b": 40.0, "c": 30.0, "d": 20.0, "e": 10.0}
    assert run_lane.pack(list(seconds), seconds, 2) == [["a", "d", "e"], ["b", "c"]]


def test_every_core_test_file_is_in_exactly_one_shard(run_lane):
    """Of core or core_long: together they run every file under the core lane's paths once."""
    everything = run_lane.test_files(run_lane.LANES["core"].paths)
    assert len(everything) > 100
    ran = [f for lane in ("core", "core_long") for s in _shards(run_lane, lane) for f in s]
    assert sorted(ran) == everything


def test_no_shard_holds_more_than_its_target_on_the_measured_times(run_lane):
    seconds = run_lane.recorded_seconds("core")
    for lane in ("core", "core_long"):
        target = run_lane.SHARD_TARGET_S[lane]
        for files in _shards(run_lane, lane):
            load = run_lane.shard_load(files, seconds)
            assert load + run_lane.SESSION_START_S <= target, (lane, load / 60, files)


def test_the_core_shard_target_is_thirty_minutes_and_its_budget_fails_a_shard_that_outgrows_it(
    run_lane,
):
    assert run_lane.SHARD_TARGET_S["core"] == 30 * 60
    for lane in ("core", "core_long"):
        target = run_lane.SHARD_TARGET_S[lane]
        assert run_lane.budget_s(lane) == target + run_lane.HEADROOM_S
        assert run_lane.SUITE[lane][1] == run_lane.planned_timeout(lane)


def test_the_shard_count_is_the_fewest_that_fit(run_lane):
    total = run_lane.planned_shards("core")
    assert run_lane.SUITE["core"][0] == total
    seconds = run_lane.recorded_seconds("core")
    fewer = run_lane.pack(run_lane.lane_files("core"), seconds, total - 1)
    assert max(run_lane.shard_load(s, seconds) for s in fewer) > run_lane.capacity_s("core")


def test_a_file_no_core_shard_can_hold_runs_in_core_long(run_lane):
    seconds = run_lane.recorded_seconds("core")
    capacity = run_lane.capacity_s("core")
    long = [f for f in run_lane.test_files(run_lane.LANES["core"].paths) if seconds[f] > capacity]
    assert tuple(long) == run_lane.CORE_LONG
    assert "tests/integration/test_text_search_on_engines.py" in long  # job R509-T
    assert run_lane.LANES["core_long"].paths == run_lane.CORE_LONG
    assert run_lane.LANES["core_long"].marker == run_lane.LANES["core"].marker
    assert not set(run_lane.lane_files("core")) & set(long)


def test_an_unmeasured_file_costs_the_default(run_lane, monkeypatch):
    seconds = {"tests/a.py": 600.0}
    assert run_lane.shard_load(["tests/a.py", "tests/new.py"], seconds) == 600.0 + 120
    assert run_lane.UNMEASURED_S == 120
    # Six unmeasured files of a 4-minute capacity: two to a shard, three shards.
    monkeypatch.setitem(run_lane.SHARD_TARGET_S, "core", run_lane.SESSION_START_S + 240)
    monkeypatch.setattr(run_lane, "lane_files", lambda lane: [f"tests/n{i}.py" for i in range(6)])
    monkeypatch.setattr(run_lane, "recorded_seconds", lambda lane: {})
    assert run_lane.planned_shards("core") == 3


def test_a_file_over_a_planned_lanes_capacity_is_refused_by_name(run_lane, monkeypatch):
    monkeypatch.setattr(run_lane, "lane_files", lambda lane: ["tests/huge.py", "tests/small.py"])
    monkeypatch.setattr(
        run_lane, "recorded_seconds", lambda lane: {"tests/huge.py": 6000.0, "tests/small.py": 1.0}
    )
    with pytest.raises(ValueError, match="tests/huge.py alone hold"):
        run_lane.planned_shards("core_long")


def test_every_measured_file_exists(run_lane):
    recorded = json.loads(run_lane.DURATIONS.read_text())
    assert set(recorded) == {"core"}
    missing = [f for f in recorded["core"] if not (REPO / f).is_file()]
    assert not missing


def test_the_workflow_matrix_is_the_planners(run_lane):
    include = run_lane.matrix("all")["include"]
    for lane in ("core", "core_long"):
        total = run_lane.planned_shards(lane)
        assert [m["shard"] for m in include if m["lane"] == lane] == [
            f"{k}/{total}" for k in range(1, total + 1)
        ]
        assert {m["timeout"] for m in include if m["lane"] == lane} == {
            run_lane.planned_timeout(lane)
        }
    lanes = (REPO / ".github" / "workflows" / "integration-suite-lanes.yml").read_text()
    suite = yaml.safe_load(lanes)["jobs"]["suite"]
    assert suite["strategy"]["matrix"] == "${{ fromJSON(needs.plan.outputs.matrix) }}"
    assert "--shard {0}" in suite["with"]["run"] and "matrix.shard" in suite["with"]["run"]
    assert "run_lane.py --matrix" in lanes


def test_naming_core_runs_core_long_too(run_lane):
    lanes = {m["lane"] for m in run_lane.matrix("core")["include"]}
    assert lanes == {"core", "core_long"}


def test_a_full_run_fits_the_accounts_concurrent_runners(run_lane):
    """Every suite lane's jobs, the own jobs and every live lane with the warehouse UI job at once:
    the account runs 40 jobs at a time."""
    jobs = len(run_lane.matrix("all")["include"]) + len(run_lane.OWN_JOBS)
    jobs += len(run_lane.LIVE) + 1  # the warehouse lane's UI job
    assert jobs <= run_lane.RUNNERS == 40


def _shard_dir(tmp_path: Path, name: str, cases: list[tuple[str, str, float]], record: dict):
    out = tmp_path / f"lane-{name}" / name
    out.mkdir(parents=True)
    body = "".join(f'<testcase classname="{c}" name="{n}" time="{t}" />' for c, n, t in cases)
    (out / "junit.xml").write_text(
        f'<testsuites><testsuite name="pytest">{body}</testsuite></testsuites>'
    )
    (out / "shard_files.json").write_text(json.dumps(record))


_GEO = "tests/integration/test_geometric_on_engines.py"
_HASH = "tests/integration/test_hashes_on_engines.py"
_TEXT = "tests/integration/test_text_search_on_engines.py"
_STEPS = "tests/steps/steps_aggregates.py"


def test_a_folded_run_replaces_what_it_measured(tmp_path):
    durations = _load("lane_durations")
    files = [_GEO, _HASH, _STEPS]
    _shard_dir(
        tmp_path,
        "core-1of2",
        [
            ("tests.integration.test_geometric_on_engines", "test_a", 100.0),
            ("tests.integration.test_geometric_on_engines", "test_b", 20.5),
            ("tests.steps.steps_aggregates", "test_scenario", 3.0),
            ("", "tests.integration.test_hashes_on_engines", 2.0),  # a collection error
        ],
        {"lane": "core", "files": files, "complete": True},
    )
    recorded = {"core": {_GEO: 999.0, _HASH: 999.0, _STEPS: 999.0, "tests/gone.py": 5.0}}
    folded = durations.fold(recorded, [tmp_path])
    assert folded == {"core": {_GEO: 120.5, _HASH: 2.0, _STEPS: 3.0}}


def test_a_complete_shard_records_a_file_with_no_core_test_as_zero_and_a_stopped_one_keeps_it(
    tmp_path,
):
    durations = _load("lane_durations")
    case = [("tests.integration.test_geometric_on_engines", "test_a", 10.0)]
    _shard_dir(
        tmp_path / "a",
        "core-1of2",
        case,
        {"lane": "core", "files": [_GEO, _HASH], "complete": True},
    )
    _shard_dir(
        tmp_path / "b",
        "core_long-1of1",
        [],
        {"lane": "core_long", "files": [_TEXT], "complete": False},
    )
    folded = durations.fold({"core": {_HASH: 50.0, _TEXT: 3000.0}}, [tmp_path])
    assert folded["core"] == {_GEO: 10.0, _HASH: 0.0, _TEXT: 3000.0}


def test_a_fold_with_no_shard_records_is_refused(tmp_path):
    with pytest.raises(ValueError, match="no shard_files.json"):
        _load("lane_durations").fold({"core": {}}, [tmp_path])


def test_a_planned_shard_writes_what_it_ran_beside_its_junit(run_lane, monkeypatch, tmp_path):
    monkeypatch.delenv("PROVISA_LANE_TESTS", raising=False)
    monkeypatch.setattr(run_lane.subprocess, "call", lambda cmd, **k: 0)
    out = tmp_path / "core-3"
    total = run_lane.SUITE["core"][0]
    assert (
        run_lane.main(["core", "--shard", f"3/{total}", "--junitxml", str(out / "junit.xml")]) == 0
    )
    record = json.loads((out / run_lane.SHARD_FILES).read_text())
    assert record == {
        "lane": "core",
        "files": run_lane.targets("core", f"3/{total}"),
        "complete": True,
    }
