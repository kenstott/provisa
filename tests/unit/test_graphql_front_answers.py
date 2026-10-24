# Copyright (c) 2026 Kenneth Stott
# Canary: 35b40b74-1d78-46b0-a185-e733bfd83ed9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The gate GraphQL's move into the graph IR is held to (REQ-2135, REQ-863, step 2): every corpus
read the GraphQL front end lowers, printed as Cypher and read back by the Cypher front end, answers
on DuckDB what the read's own SQL answers there (tests/fixtures/graphql_golden/answers/), as the
GraphQL endpoint serializes it. A read the front end does not lower yet says why; the share it
lowers is held in tests/fixtures/graphql_golden/front_coverage/, so a change in it is seen."""

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
def test_each_lowered_read_answers_as_its_sql_does(embedded_postgres, demo):
    golden = _golden()
    outcome = golden.front_answers(embedded_postgres, demo, f"gqf{os.getpid()}_{demo.name}")
    for entry in outcome:
        if "rows" not in entry:
            continue
        key = (entry["origin"], entry["root_field"])
        assert entry["rows"] == entry["expected"], f"{key}\n{entry['cypher']}"
    path = golden.GOLDEN / "front_coverage" / f"{demo.name}.json"
    coverage = [{k: e[k] for k in ("origin", "root_field", "lowered")} for e in outcome]
    if os.environ.get("PROVISA_GRAPHQL_GOLDEN_CAPTURE"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(coverage, indent=1, sort_keys=True) + "\n")
    assert json.loads(path.read_text()) == coverage
