# Copyright (c) 2026 Kenneth Stott
# Canary: 9ea3d905-273b-48cb-abea-bacc31dcd8fa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each SDK decorator reads, through Provisa's own reader and manifest, exactly as its docstring
twin does (REQ-1984: a decorator and the docstring tag produce the same manifest)."""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from provisa.codesource.manifest import PYTHON_RULES, manifest_json, read_manifest
from provisa.python.source import READER

pytestmark = pytest.mark.unit

SDK = Path(__file__).resolve().parents[3] / "provisa-sdk" / "python"
SPELLINGS = Path(__file__).parent / "spellings"
#: Every function of the twin modules: one per decorator form.
NAMES = [
    "plain",
    "called",
    "with_context",
    "context_only",
    "loaded",
    "loaded_n",
    "ticks",
    "async_ticks",
    "cancel",
    "add_order",
    "edit_order",
    "drop_order",
    "lookup",
    "scores",
    "refresh",
    "start",
    "stop",
    "status",
    "secured",
    "twice",
    "undocumented",
    "paragraphs",
    "aliased",
]


def _read(spelling: str) -> dict:
    """The reader's raw document of ``spellings/<spelling>/twins.py``, with the SDK installed as
    the code's dependency (on its path), as Provisa runs it: ``python reader.py <code> <entry>``."""
    env = {"PATH": os.environ["PATH"], "PYTHONPATH": str(SDK)}
    done = subprocess.run(
        [sys.executable, "-s", str(READER), str(SPELLINGS / spelling), "twins"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


@pytest.fixture(scope="module")
def raw() -> dict[str, dict]:
    return {spelling: _read(spelling) for spelling in ("doc", "sdk")}


def _export(document: dict, name: str) -> dict:
    (found,) = [e for e in document["exports"] if e["name"] == name]
    return found


def test_both_spellings_read_every_twin_and_nothing_else(raw):
    for document in raw.values():
        assert document["diagnostics"] == []
        assert [e["name"] for e in document["exports"]] == NAMES


@pytest.mark.parametrize("name", NAMES)
def test_the_decorator_reads_as_its_docstring_twin(raw, name):
    assert _export(raw["sdk"], name) == _export(raw["doc"], name)


def test_the_column_type_aliases_read_as_the_hints_they_alias(raw):
    assert raw["sdk"]["types"] == raw["doc"]["types"]


def test_both_spellings_give_the_same_manifest(raw):
    doc = manifest_json(read_manifest(raw["doc"], PYTHON_RULES))
    sdk = manifest_json(read_manifest(raw["sdk"], PYTHON_RULES))
    assert sdk == doc
    assert {e["name"] for e in doc["exports"]} == set(NAMES)
