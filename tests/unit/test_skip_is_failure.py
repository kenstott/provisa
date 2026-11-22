# Copyright (c) 2026 Kenneth Stott
# Canary: 220a70b7-fb50-467e-9bc1-e8f2e153c262
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A warehouse test that skips is reported as a failure naming what was missing, unless it is
skipped for a named calcite issue: then it is a known calcite skip, never a pass."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

_CASES = """
import os
import pytest

pytestmark = []


@pytest.mark.requires_warehouse
@pytest.mark.skipif(not os.environ.get("EXAMPLE_WAREHOUSE_KEY"), reason="EXAMPLE_WAREHOUSE_KEY not set")
def test_marked_skipif():
    pass


@pytest.mark.requires_warehouse
def test_marked_runtime_skip():
    pytest.skip("EXAMPLE_BUCKET not set")


@pytest.mark.skipif(True, reason="an ordinary skip")
def test_unmarked_skip():
    pass


@pytest.mark.requires_warehouse
def test_marked_pass():
    pass


@pytest.mark.requires_warehouse
@pytest.mark.calcite_issue("govdata-ops#1416")
@pytest.mark.skip(reason="the served catalog gives Trino no column types")
def test_calcite_marked_skip():
    pass


@pytest.mark.requires_warehouse
@pytest.mark.calcite_issue("govdata-ops#1417")
def test_calcite_marked_runtime_skip():
    pytest.skip("DESCRIBE answers no columns")


@pytest.mark.requires_warehouse
@pytest.mark.calcite_issue("govdata-ops#1418")
def test_calcite_marked_pass():
    pass
"""


def _run(tmp_path: Path) -> str:
    (tmp_path / "test_cases.py").write_text(_CASES)
    (tmp_path / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    requires_warehouse: needs a live warehouse\n"
        "    calcite_issue: skipped for a calcite bug\n"
    )
    env = {k: v for k, v in os.environ.items() if k != "EXAMPLE_WAREHOUSE_KEY"}
    env["PYTHONPATH"] = str(REPO)
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "tests.skip_is_failure",
            "-rA",
            "-q",
            "--color=no",
            "-p",
            "no:cacheprovider",
            "--junitxml",
            str(tmp_path / "junit.xml"),
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
    )
    return done.stdout


def test_a_warehouse_skip_fails_naming_the_variable(tmp_path):
    out = _run(tmp_path)
    # A skipif mark skips during setup, so pytest reports its failure as an error of the test.
    assert "ERROR test_cases.py::test_marked_skipif" in out
    assert "EXAMPLE_WAREHOUSE_KEY not set" in out
    assert "FAILED test_cases.py::test_marked_runtime_skip" in out
    assert "EXAMPLE_BUCKET not set" in out


def test_other_skips_and_passes_are_unchanged(tmp_path):
    out = _run(tmp_path)
    assert "SKIPPED" in out and "an ordinary skip" in out
    assert "PASSED test_cases.py::test_marked_pass" in out
    assert "1 failed, 2 passed, 3 skipped, 1 error" in out


def test_a_calcite_marked_skip_is_a_known_calcite_skip_naming_the_issue(tmp_path):
    out = _run(tmp_path)
    for name, issue, reason in (
        (
            "test_calcite_marked_skip",
            "govdata-ops#1416",
            "the served catalog gives Trino no column types",
        ),
        ("test_calcite_marked_runtime_skip", "govdata-ops#1417", "DESCRIBE answers no columns"),
    ):
        # Still a skip, never a pass, and never the warehouse failure.
        assert f"PASSED test_cases.py::{name}" not in out
        assert f"FAILED test_cases.py::{name}" not in out
        assert f"ERROR test_cases.py::{name}" not in out
        assert f"known calcite skip ({issue}): {reason}" in out
    assert "warehouse test did not run: the served catalog" not in out


def test_a_calcite_marked_test_that_runs_is_a_plain_pass(tmp_path):
    out = _run(tmp_path)
    assert "PASSED test_cases.py::test_calcite_marked_pass" in out
    assert "govdata-ops#1418" not in out


def _lane_report():
    spec = importlib.util.spec_from_file_location(
        "lane_report", REPO / "scripts" / "ci" / "lane_report.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["lane_report"] = module
    spec.loader.exec_module(module)
    return module


def test_the_lane_report_lists_a_calcite_skip_and_fails_every_other_skip(tmp_path):
    _run(tmp_path)
    lanes = tmp_path / "artifacts"
    (lanes / "warehouse").mkdir(parents=True)
    (tmp_path / "junit.xml").rename(lanes / "warehouse" / "junit.xml")
    text, green = _lane_report().report(lanes)
    # The unmarked skip still fails the lane; the two calcite skips are listed on their own.
    assert "| warehouse | 2 | 1 | 1 | 1 | **not green**, 2 known calcite skip(s) |" in text
    assert (
        "KNOWN CALCITE SKIP test_cases::test_calcite_marked_skip: known calcite skip "
        "(govdata-ops#1416): the served catalog gives Trino no column types"
    ) in text
    assert "SKIPPED test_cases::test_unmarked_skip: an ordinary skip" in text
    assert green is False


def test_a_lane_whose_only_skips_are_calcite_skips_is_green(tmp_path):
    lanes = tmp_path / "artifacts"
    (lanes / "warehouse").mkdir(parents=True)
    (lanes / "warehouse" / "junit.xml").write_text(
        '<testsuites><testsuite name="pytest">'
        '<testcase classname="tests.w" name="test_ok"/>'
        '<testcase classname="tests.w" name="test_c"><skipped type="pytest.skip" '
        'message="known calcite skip (govdata-ops#1416): no column types"/></testcase>'
        "</testsuite></testsuites>"
    )
    text, green = _lane_report().report(lanes)
    assert "| warehouse | 1 | 0 | 0 | 0 | ok, 1 known calcite skip(s) |" in text
    assert green is True


def test_the_marker_takes_exactly_the_issue(tmp_path):
    (tmp_path / "test_bad.py").write_text(
        "import pytest\n\n\n@pytest.mark.calcite_issue()\ndef test_x():\n    pass\n"
    )
    (tmp_path / "pytest.ini").write_text("[pytest]\nmarkers =\n    calcite_issue: x\n")
    env = dict(os.environ, PYTHONPATH=str(REPO))
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "tests.skip_is_failure",
            "-q",
            "--color=no",
            "-p",
            "no:cacheprovider",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
    )
    assert done.returncode != 0
    assert "takes one argument, the calcite issue" in done.stdout + done.stderr


def test_the_report_and_the_plugin_agree_on_the_calcite_prefix():
    from tests.skip_is_failure import KNOWN_CALCITE_SKIP

    assert _lane_report().KNOWN_CALCITE_SKIP == KNOWN_CALCITE_SKIP
