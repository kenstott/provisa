# Copyright (c) 2026 Kenneth Stott
# Canary: 220a70b7-fb50-467e-9bc1-e8f2e153c262
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A warehouse test that skips is reported as a failure naming what was missing."""

from __future__ import annotations

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
"""


def _run(tmp_path: Path) -> str:
    (tmp_path / "test_cases.py").write_text(_CASES)
    (tmp_path / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    requires_warehouse: needs a live warehouse\n"
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
    assert "1 failed, 1 passed, 1 skipped, 1 error" in out
