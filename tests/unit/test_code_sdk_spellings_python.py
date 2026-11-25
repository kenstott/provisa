# Copyright (c) 2026 Kenneth Stott
# Canary: 5076160b-6a8a-4b5a-976e-08b8e3cfa89b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The binding corpus in Python (REQ-1984): each case's docstring spelling and SDK spelling,
read by Provisa's own reader and manifest, give the same manifest, or the refusal the case states;
and pyright passes each SDK spelling or fails it, as the case states."""

# Requirements: REQ-1984
from __future__ import annotations

import functools
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from provisa.codesource.manifest import (
    ManifestRefused,
    PYTHON_RULES,
    manifest_json,
    read_manifest,
)
from provisa.codesource.native_connector import (
    _project as spark_project,  # pyright: ignore[reportPrivateUsage]
)
from provisa.python.source import READER
from tests.fixtures.code_sdk_spellings.collector import Case, cases

pytestmark = pytest.mark.unit

SDK = Path(__file__).resolve().parents[2] / "provisa-sdk" / "python"
#: REQ-2225: a Spark script runs on the Spark connector's host, whose code is the connector's
#: project, so ``provisa_spark`` is importable beside it -- the native host's own path.
SPARK = spark_project()
CASES = {c.name: c for c in cases() if c.language == "python"}
SPELLED = [(c.name, s) for c in CASES.values() for s in c.spellings]
TWINNED = [c.name for c in CASES.values() if len(c.spellings) == 2 and c.accepted]


@pytest.fixture(scope="module")
def read(tmp_path_factory):
    """The manifest JSON of one spelling, or its refusal's problems, read once."""
    root = tmp_path_factory.mktemp("spellings")
    return functools.cache(lambda name, spelling: _read(root, name, spelling))


def _read(root: Path, name: str, spelling: str) -> dict | tuple[str, ...]:
    """The SDK is on the code's path, as an author's installed dependency is, and provisa_spark
    as the Spark connector's host has it; the reader runs as Provisa runs it:
    ``python reader.py <code> <entry>``."""
    code = CASES[name].write(root, spelling)
    done = subprocess.run(
        [sys.executable, "-s", str(READER), str(code), "source"],
        capture_output=True,
        text=True,
        env={"PATH": os.environ["PATH"], "PYTHONPATH": os.pathsep.join((str(SDK), str(SPARK)))},
        check=False,
    )
    assert done.returncode == 0, done.stderr
    try:
        return manifest_json(read_manifest(json.loads(done.stdout), PYTHON_RULES))
    except ManifestRefused as refused:
        return tuple(refused.problems)


def test_the_corpus_has_python_cases():
    assert len(TWINNED) >= 15
    assert any(name.startswith("docs-") for name in TWINNED)


@pytest.mark.parametrize(("name", "spelling"), SPELLED)
def test_each_spelling_gives_what_its_case_states(read, name, spelling):
    expected = CASES[name].spellings[spelling].problems
    got = read(name, spelling)
    if expected:
        assert got == expected
    else:
        assert isinstance(got, dict), got
        assert got["exports"]


@pytest.mark.parametrize("name", TWINNED)
def test_the_sdk_spelling_gives_its_twins_manifest(read, name):
    assert read(name, "sdk") == read(name, "doc")


@pytest.fixture(scope="module")
def pyright_errors(tmp_path_factory) -> dict[str, int]:
    """The number of errors pyright reports on each case's SDK spelling, with the SDK and its
    provisa_services stubs as an author installs them."""
    root = tmp_path_factory.mktemp("pyright")
    files = {
        str((case.write(root, "sdk") / "source.py").resolve()): case.name
        for case in CASES.values()
        if "sdk" in case.spellings
    }
    config = root / "pyrightconfig.json"
    # A docs example calls the reader's own client by name (``api``, ``MODEL``), left undefined.
    settings = {
        "extraPaths": [str(SDK), str(SPARK)],
        "pythonVersion": "3.12",
        "reportUndefinedVariable": "none",
    }
    config.write_text(json.dumps(settings))
    done = subprocess.run(
        [sys.executable, "-m", "pyright", "-p", str(config), "--outputjson", *files],
        capture_output=True,
        text=True,
        check=False,
    )
    report = json.loads(done.stdout)
    assert report["summary"]["filesAnalyzed"] == len(files), done.stdout
    errors = dict.fromkeys(files.values(), 0)
    for diagnostic in report["generalDiagnostics"]:
        if diagnostic["severity"] == "error":
            errors[files[str(Path(diagnostic["file"]).resolve())]] += 1
    return errors


@pytest.mark.parametrize("name", [c.name for c in CASES.values() if "sdk" in c.spellings])
def test_pyright_passes_or_fails_the_sdk_spelling_as_its_case_states(pyright_errors, name):
    case: Case = CASES[name]
    accepts = case.spellings["sdk"].checker_accepts
    assert (pyright_errors[name] == 0) is accepts, pyright_errors[name]
