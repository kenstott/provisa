# Copyright (c) 2026 Kenneth Stott
# Canary: 67678a63-ec4b-4559-80c9-f04216832629
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What every GraphQL read of the corpus answers (REQ-2135, REQ-863): the baseline the graph IR's
SQL is held to. Each named demo's PostgreSQL sources are seeded on the session's embedded
PostgreSQL from the demo's own seed SQL; a read of them is answered on DuckDB, the sources
attached, and a read of one source on PostgreSQL directly too. Each demo's answers are committed
under tests/fixtures/graphql_golden/answers/ and this test holds them to what the reads answer
now; ``python scripts/graphql_golden.py capture-answers`` writes them (this test, with the capture
switch)."""

# Requirements: REQ-2135, REQ-863

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

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


_DEMOS = _golden().answered_demos()


@pytest.mark.parametrize("demo", _DEMOS, ids=[d.name for d in _DEMOS])
def test_each_demo_read_answers_what_it_did(embedded_postgres, demo):
    golden = _golden()
    answers = golden.demo_answers(embedded_postgres, demo, f"gqa{os.getpid()}_{demo.name}")
    path = golden.GOLDEN / "answers" / f"{demo.name}.json"
    if os.environ.get("PROVISA_GRAPHQL_GOLDEN_CAPTURE"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(answers, indent=1, sort_keys=True) + "\n")
    assert json.loads(path.read_text()) == json.loads(json.dumps(answers))
