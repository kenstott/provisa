# Copyright (c) 2026 Kenneth Stott
# Canary: 8737c784-0290-4e92-bab2-ae6a10cd8966
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The CI run's per-lane table: counts per lane, the first line of each problem, skips not green."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _report():
    spec = importlib.util.spec_from_file_location(
        "lane_report", REPO / "scripts" / "ci" / "lane_report.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["lane_report"] = module
    spec.loader.exec_module(module)
    return module


def _junit(directory: Path, lane: str, cases: str) -> None:
    (directory / lane).mkdir(parents=True)
    (directory / lane / "junit.xml").write_text(
        f'<testsuites><testsuite name="pytest">{cases}</testsuite></testsuites>'
    )


def test_each_lane_is_a_row_and_each_problem_is_named(tmp_path):
    _junit(
        tmp_path,
        "core-1",
        '<testcase classname="tests.a" name="test_ok"/>'
        '<testcase classname="tests.a" name="test_bad"><failure message="assert 1 == 2"/></testcase>',
    )
    _junit(tmp_path, "kafka", '<testcase classname="tests.k" name="test_ok"/>')
    text, green = _report().report(tmp_path, "https://example/run/1")
    assert "| core-1 | 1 | 1 | 0 | 0 | **not green** |" in text
    assert "| kafka | 1 | 0 | 0 | 0 | ok |" in text
    assert "FAILURE tests.a::test_bad: assert 1 == 2" in text
    assert "Run: https://example/run/1" in text
    assert green is False


def test_a_skip_is_not_green(tmp_path):
    _junit(
        tmp_path,
        "warehouse",
        '<testcase classname="tests.w" name="test_w"><skipped message="SNOWFLAKE_USER not set"/></testcase>',
    )
    text, green = _report().report(tmp_path)
    assert "| warehouse | 0 | 0 | 0 | 1 | **not green** |" in text
    assert "SKIPPED tests.w::test_w: SNOWFLAKE_USER not set" in text
    assert green is False


def test_all_lanes_passing_is_green(tmp_path):
    _junit(tmp_path, "core-1", '<testcase classname="tests.a" name="test_ok"/>')
    assert _report().report(tmp_path)[1] is True


def test_no_lane_results_is_not_green(tmp_path):
    assert _report().report(tmp_path)[1] is False
