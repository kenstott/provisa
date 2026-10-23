# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The unit and vitest suites run in shards, and every test in exactly one of them
(scripts/ci/unit_shard.py, unit-tests.yml, ui-unit-tests.yml)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"


def _shard():
    spec = importlib.util.spec_from_file_location(
        "unit_shard", REPO / "scripts" / "ci" / "unit_shard.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_shards_partition_the_unit_test_files():
    shard = _shard()
    every = shard.test_files()
    assert "tests/unit/test_ci_unit_shard.py" in every
    parts = [shard.files(f"{i}/4") for i in range(1, 5)]
    assert sorted(f for part in parts for f in part) == every
    assert all(parts)


def test_a_file_keeps_its_shard_whatever_else_is_added():
    shard = _shard()
    assert shard.shard_of("tests/unit/test_x.py", 4) == shard.shard_of("tests/unit/test_x.py", 4)
    assert {shard.shard_of(f"tests/unit/test_{i}.py", 4) for i in range(40)} == {1, 2, 3, 4}


def test_the_check_names_a_test_missing_run_twice_or_unknown(tmp_path):
    shard = _shard()
    full = tmp_path / "full.txt"
    full.write_text("a::t1\na::t2\nb::t3\n")
    one, two = tmp_path / "1.txt", tmp_path / "2.txt"
    one.write_text("a::t1\na::t2\n")
    two.write_text("b::t3\n")
    assert shard.check(full, [one, two]) == []
    two.write_text("a::t2\nc::t4\n")
    assert shard.check(full, [one, two]) == [
        "missing from every shard: b::t3",
        "run by 2 shards: a::t2",
        "not in the full collection: c::t4",
    ]
    assert shard.main(["check", str(full), str(one), str(two)]) == 1


def test_the_vitest_check_names_a_file_in_no_shard_or_two(tmp_path):
    shard = _shard()
    full = tmp_path / "files.json"
    full.write_text(json.dumps([{"file": f"/w/provisa/provisa-ui/src/{n}.test.ts"} for n in "abc"]))

    def results(path: Path, *names: str) -> Path:
        reports = [{"name": f"/r/provisa/provisa-ui/src/{n}.test.ts"} for n in names]
        path.write_text(json.dumps({"testResults": reports}))
        return path

    one, two = results(tmp_path / "1.json", "a", "b"), results(tmp_path / "2.json", "c")
    assert shard.check_vitest(full, [one, two]) == (3, [])
    two = results(tmp_path / "2.json", "b")
    assert shard.check_vitest(full, [one, two]) == (
        3,
        ["in no shard: src/c.test.ts", "in 2 shards: src/b.test.ts"],
    )


def _jobs(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())["jobs"]


def test_the_unit_workflow_shards_checks_and_keeps_its_verdicts_name():
    jobs = _jobs("unit-tests.yml")
    assert jobs["shard"]["strategy"]["matrix"]["shard"] == [1, 2, 3, 4]
    run = next(s for s in jobs["shard"]["steps"] if s.get("name") == "Run unit tests")
    assert "unit_shard.py files" in run["run"] and "-n auto" in run["run"]
    assert run["env"]["SHARD"] == "${{ matrix.shard }}/4"
    assert jobs["check"]["needs"] == "shard"
    assert jobs["unit"]["name"] == "pytest tests/unit"
    assert jobs["unit"]["needs"] == ["shard", "check"] and jobs["unit"]["if"] == "always()"


def test_the_ui_workflow_shards_vitest_beside_tsc_and_lint_and_checks():
    jobs = _jobs("ui-unit-tests.yml")
    assert jobs["vitest"]["strategy"]["matrix"]["shard"] == [1, 2, 3]
    assert "--shard=" in str(jobs["vitest"]["steps"])
    static = [s.get("run", "") for s in jobs["static"]["steps"]]
    assert "npx tsc -b --noEmit" in static and "npx eslint ." in static
    assert "vitest" not in jobs["static"].get("needs", "")
    assert jobs["ui"]["name"] == "tsc + vitest"
    assert jobs["ui"]["needs"] == ["static", "vitest", "check"]
