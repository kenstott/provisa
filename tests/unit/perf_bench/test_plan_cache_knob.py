# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2092 knob: plan cache hit vs miss. "hit" repeats one statement; "miss" names the table
(SQL) or the root field (GraphQL) anew on every request, so the server plans each one afresh."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import local_pass  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_mix  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402
from governance_variants import VARIANTS  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)


def _raw() -> dict[str, Any]:
    return json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))


def _load(raw: dict[str, Any], tmp_path: Path) -> contract_model.Setup:
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


def _renders(setup: contract_model.Setup, transport: str, n: int) -> list[Any]:
    gen = request_mix.RequestGenerator(setup, transport, f"{transport}/0/0", cacheable=False)
    renderer = request_render.Renderer(setup)
    return [renderer.render(gen.next()) for _ in range(n)]


def test_absent_is_hit(tmp_path: Path) -> None:
    assert _load(_raw(), tmp_path).knobs.plan_cache == "hit"


def test_unknown_value_refused(tmp_path: Path) -> None:
    raw = _raw()
    raw["knobs"]["plan_cache"] = "cold"
    with pytest.raises(contract_model.SetupError, match="plan_cache"):
        _load(raw, tmp_path)


def test_hit_repeats_one_statement(tmp_path: Path) -> None:
    setup = _load(_raw(), tmp_path)
    queries = _renders(setup, "pgwire", 5)
    assert len({q.sql for q in queries}) == 1
    assert len({q.graphql for q in queries}) == 1


def test_miss_sends_a_new_statement_each_time_with_the_same_columns(tmp_path: Path) -> None:
    raw = _raw()
    raw["knobs"]["plan_cache"] = "miss"
    setup = _load(raw, tmp_path)
    queries = _renders(setup, "pgwire", 5)
    (tmp_path / "hit").mkdir()
    hit = _renders(_load(_raw(), tmp_path / "hit"), "pgwire", 1)[0]
    assert len({q.sql for q in queries}) == 5
    assert len({q.graphql for q in queries}) == 5
    for q in queries:
        # the table takes a name of its own; what is selected and how many rows are not changed
        assert q.sql.split(" FROM ")[0] == hit.sql.split(" FROM ")[0]
        assert q.sql.endswith(hit.sql.rsplit(" LIMIT ", 1)[1])
        assert q.graphql.split("{ ", 1)[1].split(": ", 1)[1] == hit.graphql.split("{ ", 1)[1]


def test_two_renderers_never_share_a_name(tmp_path: Path) -> None:
    raw = _raw()
    raw["knobs"]["plan_cache"] = "miss"
    setup = _load(raw, tmp_path)
    a = {q.sql for q in _renders(setup, "pgwire", 20)}
    b = {q.sql for q in _renders(setup, "pgwire", 20)}
    assert not a & b


def test_rows_and_masks_is_row_rules_and_masks_only() -> None:
    assert VARIANTS["rows_and_masks"] == {
        "row_rules_per_table": VARIANTS["row_rules_2"]["row_rules_per_table"],
        "masked_column_share": VARIANTS["masked_quarter"]["masked_column_share"],
    }


def test_local_pass_miss_sends_only_text_transports() -> None:
    base = {
        "sources": {"bench-postgresql": {"tables": [{"table": "orders", "columns": [{}, {}]}]}},
        "cross_source_joins": [],
        "knobs": {"route": {"bench-postgresql": "direct"}},
        "load": {},
    }
    raw = local_pass.contract(base, "zero", 0.0, [1], 1.0, "miss")
    assert raw["knobs"]["plan_cache"] == "miss"
    assert tuple(raw["transports"]) == local_pass.PLAN_MISS_TRANSPORTS
    assert tuple(local_pass.contract(base, "zero", 0.0, [1], 1.0)["transports"]) == (
        local_pass.TRANSPORTS
    )
