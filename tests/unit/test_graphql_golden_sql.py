# Copyright (c) 2026 Kenneth Stott
# Canary: ce60e94a-377b-4ac9-b957-5b754900c39c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every GraphQL read of the corpus compiles to the semantic SQL it did when the baseline was
captured (REQ-2135, REQ-863): the demos' queries, the perf bench's and the UI e2e specs' data
reads, each against its own model (scripts/graphql_golden.py). GraphQL's move into the graph IR is
held to it: a difference is a regression unless it is approved read by read."""

# Requirements: REQ-2135, REQ-863

import functools
import importlib.util
import json
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_ENTRIES = [
    json.loads(line)
    for line in (_REPO / "tests" / "fixtures" / "graphql_golden" / "entries.jsonl")
    .read_text()
    .splitlines()
]


@functools.cache
def _golden():
    spec = importlib.util.spec_from_file_location(
        "graphql_golden", _REPO / "scripts" / "graphql_golden.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["graphql_golden"] = module  # its dataclass reads its own module
    spec.loader.exec_module(module)
    return module


@functools.cache
def _model(config: str):
    return _golden().model(_REPO / config)


def test_the_corpus_is_the_one_captured():
    held = [(e["origin"], e["graphql"]) for e in _ENTRIES]
    assert [(r.origin, r.text) for r in _golden().corpus()] == held


@pytest.mark.parametrize("entry", _ENTRIES, ids=[e["origin"] for e in _ENTRIES])
def test_the_same_semantic_sql_as_the_baseline(entry):
    schema, ctx = _model(entry["model"])
    got = _golden().compile_read(schema, ctx, entry["graphql"], entry["variables"])
    if got != entry["expected"]:
        shown = _golden().compile_read(schema, ctx, entry["graphql"], entry["variables"], sql=True)
        pytest.fail(f"{entry['origin']} compiles otherwise now:\n{json.dumps(shown, indent=1)}")
