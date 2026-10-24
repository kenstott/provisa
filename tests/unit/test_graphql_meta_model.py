# Copyright (c) 2026 Kenneth Stott
# Canary: 55dc8dc8-9278-4ad4-94ae-a2b8213bada1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The meta model the GraphQL corpus reads the Ontology View's queries against (REQ-2135) is the
one the server registers: its committed form (tests/fixtures/graphql_golden/meta_model.yaml) is
what the meta seed registers on a fresh control plane, read back as a model config, so it cannot
drift from the server's.

``python scripts/graphql_golden.py capture-meta`` writes it (this test, with the capture switch)."""

# Requirements: REQ-2135

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]


def _golden():
    spec = importlib.util.spec_from_file_location(
        "graphql_golden", _REPO / "scripts" / "graphql_golden.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["graphql_golden"] = module  # its dataclass reads its own module
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_the_meta_model_is_the_one_the_server_registers(control_plane):
    golden = _golden()
    config = await golden.meta_model_config(control_plane.org())
    if os.environ.get("PROVISA_GRAPHQL_GOLDEN_CAPTURE"):
        golden.META_MODEL.parent.mkdir(parents=True, exist_ok=True)
        golden.META_MODEL.write_text(yaml.safe_dump(config, sort_keys=False))
    assert yaml.safe_load(golden.META_MODEL.read_text()) == config


def test_the_meta_schema_a_client_reads_is_the_meta_models():
    golden = _golden()
    doc = golden.meta_schema_doc()
    if os.environ.get("PROVISA_GRAPHQL_GOLDEN_CAPTURE"):
        golden.META_SCHEMA.write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n")
    held, now = json.loads(golden.META_SCHEMA.read_text()), json.loads(json.dumps(doc))
    assert held == now, _first_difference(held, now)


def _first_difference(a, b, path="") -> str:
    """Where two JSON values first differ, for the failure's message."""
    if type(a) is not type(b):
        return f"{path}: {a!r} != {b!r}"
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if a.get(k) != b.get(k):
                return _first_difference(a.get(k), b.get(k), f"{path}.{k}")
    if isinstance(a, list):
        if len(a) != len(b):
            return f"{path}: {len(a)} items != {len(b)}"
        for i, (x, y) in enumerate(zip(a, b, strict=True)):
            if x != y:
                return _first_difference(x, y, f"{path}[{i}]")
    return f"{path}: {str(a)[:300]} != {str(b)[:300]}"
