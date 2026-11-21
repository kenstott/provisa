# Copyright (c) 2026 Kenneth Stott
# Canary: 9d1e4f52-7b0a-4c3e-a6d8-3f5b2c8e1a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An engine replay boots its heavy engine once (REQ-2171, tests/integration/every_case.py).

A heavy engine's marker starts the engine for each test and removes it after
(tests/conftest.py _heavy_db_service). test_text_search_on_clickhouse, one test to a function, booted
ClickHouse 48 times and held the core 3/6 shard of release 509 past its job limit."""

# Requirements: REQ-2171

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

REPLAYS = (
    "tests/integration/test_text_search_on_engines.py",
    "tests/integration/test_everyday_functions_on_engines.py",
    "tests/integration/test_geometric_on_engines.py",
)


def _heavy_markers() -> frozenset[str]:
    conftest = ast.parse((REPO / "tests" / "conftest.py").read_text())
    for node in conftest.body:
        target = node.target if isinstance(node, ast.AnnAssign) else None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        if isinstance(target, ast.Name) and target.id == "_HEAVY_MARKERS":
            assert isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None
            call = node.value
            assert isinstance(call, ast.Call)
            return frozenset(ast.literal_eval(call.args[0]))
    raise AssertionError("tests/conftest.py has no _HEAVY_MARKERS")


@pytest.mark.parametrize("path", REPLAYS)
def test_no_replay_boots_a_heavy_engine_once_per_case(path):
    heavy = _heavy_markers()
    module = ast.parse((REPO / path).read_text())
    per_case = []
    for node in module.body:
        if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test"):
            continue
        decorators = [ast.unparse(d) for d in node.decorator_list]
        parametrized = any("parametrize" in d for d in decorators)
        marked = {m for m in heavy for d in decorators if d.endswith(f"mark.{m}")}
        if parametrized and marked:
            per_case.append(f"{node.name} ({', '.join(sorted(marked))})")
    assert per_case == []
