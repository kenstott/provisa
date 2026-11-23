# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Python SDK's own tests run with only pytest, pyright and the SDK installed, as the release
job builds them: no file under provisa-sdk/python/tests imports the Provisa server (provisa) or the
server's test tree (tests). A test that needs the server lives in tests/unit/sdk."""

# Requirements: REQ-1984
from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SDK_TESTS = Path(__file__).resolve().parents[3] / "provisa-sdk" / "python" / "tests"
#: Top-level packages that exist only with the server installed or checked out.
SERVER = {"provisa", "tests"}


def _imported(tree: ast.Module) -> set[str]:
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.add(node.module.split(".")[0])
    return found


def test_the_sdk_tests_exist():
    assert sorted(p.name for p in SDK_TESTS.glob("test_*.py"))


def test_no_sdk_test_imports_the_server():
    offenders = {
        str(path.relative_to(SDK_TESTS)): sorted(_imported(ast.parse(path.read_text())) & SERVER)
        for path in sorted(SDK_TESTS.rglob("*.py"))
    }
    assert {name: mods for name, mods in offenders.items() if mods} == {}


def test_the_check_sees_a_server_import():
    tree = ast.parse("from provisa.python.source import READER\nimport tests.conftest\n")
    assert _imported(tree) & SERVER == {"provisa", "tests"}
