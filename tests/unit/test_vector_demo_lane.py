# Copyright (c) 2026 Kenneth Stott
# Canary: e3e25a65-9618-4242-b820-95aca3b68c4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo that embeds with a local sentence-transformers model is tested in the vector lane.

The model needs the vector extra (sentence-transformers, CPU-only torch), which only the vector
lane installs (scripts/ci/run_lane.py, REQ-2016). A demo test without the requires_vector marker
runs in a core lane instead, where its setup's first embedding fails with EmbeddingError: the
cloudpolicy demo's embed_clauses did.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]


def _local_model_demos() -> list[str]:
    demos = []
    for config in sorted((_ROOT / "demo" / "named").glob("*/config.yaml")):
        models = (yaml.safe_load(config.read_text()) or {}).get("vector_models") or []
        if any(m.get("provider") == "huggingface" for m in models):
            demos.append(config.parent.name)
    return demos


def _markers(test_file: Path) -> set[str]:
    for node in ast.parse(test_file.read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "pytestmark" for t in node.targets
        ):
            return {n.attr for n in ast.walk(node.value) if isinstance(n, ast.Attribute)}
    return set()


_TESTED = [
    d for d in _local_model_demos() if (_ROOT / f"tests/integration/test_{d}_demo.py").exists()
]


def test_the_local_model_demos_include_cloudpolicy() -> None:
    assert "cloudpolicy" in _TESTED


@pytest.mark.parametrize("demo", _TESTED)
def test_a_local_model_demos_test_runs_in_the_vector_lane(demo: str) -> None:
    assert "requires_vector" in _markers(_ROOT / f"tests/integration/test_{demo}_demo.py")
