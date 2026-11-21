# Copyright (c) 2026 Kenneth Stott
# Canary: 2b55bdf3-1d61-4504-9210-8b2a5b730c25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A type checker holds each decorated function to its tag's rules: pyright passes every allowed
form (typing_cases/positive.py) and fails each mistake (typing_cases/negative.py) on its marked
line, with the SDK and its provisa_services stubs as an author installs them."""

# Requirements: REQ-1984
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SDK = Path(__file__).resolve().parents[1]
CASES = Path(__file__).parent / "typing_cases"
MARK = "# error:"


@pytest.fixture(scope="module")
def errors(tmp_path_factory) -> dict[str, set[int]]:
    """The lines pyright reports an error on, by case file."""
    config = tmp_path_factory.mktemp("pyright") / "pyrightconfig.json"
    config.write_text(json.dumps({"extraPaths": [str(SDK)], "pythonVersion": "3.12"}))
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "pyright",
            "-p",
            str(config),
            "--outputjson",
            str(CASES / "positive.py"),
            str(CASES / "negative.py"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    report = json.loads(done.stdout)
    assert report["summary"]["filesAnalyzed"] == 2, done.stdout
    found: dict[str, set[int]] = {"positive.py": set(), "negative.py": set()}
    for diagnostic in report["generalDiagnostics"]:
        if diagnostic["severity"] == "error":
            found[Path(diagnostic["file"]).name].add(diagnostic["range"]["start"]["line"] + 1)
    return found


def test_every_allowed_form_type_checks(errors):
    assert errors["positive.py"] == set()


def test_each_mistake_is_a_type_error_on_its_marked_line(errors):
    lines = (CASES / "negative.py").read_text().splitlines()
    marked = {n for n, line in enumerate(lines, 1) if MARK in line}
    assert len(marked) == 17
    assert errors["negative.py"] == marked
