# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The timing sanity report and the refusal to compare two VMs (REQ-1911)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import bench_sanity as bs  # noqa: E402


def _cell(query: str, cache_pass: str, p50: float, transport: str = "sql") -> dict[str, Any]:
    return {
        "query_id": query,
        "transport": transport,
        "dialect": "sql",
        "cache_pass": cache_pass,
        "latency_ms_p50": p50,
    }


HOST = {"instance_id": "1", "cpu_platform": "AMD Turin"}


def _report(*cells: dict[str, Any], host: dict[str, str] = HOST) -> dict[str, Any]:
    return {"engine": "pg", "host": host, "results": list(cells)}


def _kinds(out: dict[str, Any]) -> list[str]:
    return [f["kind"] for f in out["flags"]]


def test_a_clean_run_raises_nothing() -> None:
    report = _report(
        _cell("point_lookup", "nocache", 5),
        _cell("point_lookup", "cache", 1),
        _cell("large_scan", "nocache", 500),
        _cell("large_scan", "cache", 1.5),
        _cell("neo4j_materialize_cold", "nocache", 50),
        _cell("neo4j_materialize_warm", "nocache", 5),
    )
    assert bs.sanity(report, None) == {
        "engine": "pg",
        "cpu_platform": "AMD Turin",
        "instance_id": "1",
        "comparison": "no baseline",
        "flags": [],
    }


def test_a_cached_cell_slower_than_uncached_is_flagged() -> None:
    report = _report(_cell("q", "nocache", 100), _cell("q", "cache", 113))
    out = bs.sanity(report, None)
    assert out["flags"] == [
        {"kind": "cached_slower", "cell": "q sql(sql)", "cached_ms": 113, "uncached_ms": 100}
    ]


def test_a_cached_cell_within_the_noise_is_not_flagged() -> None:
    report = _report(_cell("q", "nocache", 100), _cell("q", "cache", 109))
    assert bs.sanity(report, None)["flags"] == []


def test_a_large_query_faster_than_its_small_counterpart_is_flagged() -> None:
    report = _report(
        _cell("federated_join", "nocache", 100), _cell("large_federated_join", "nocache", 90)
    )
    assert _kinds(bs.sanity(report, None)) == ["large_faster"]


def test_a_cold_query_faster_than_warm_is_flagged() -> None:
    report = _report(
        _cell("neo4j_materialize_cold", "nocache", 4), _cell("neo4j_materialize_warm", "nocache", 5)
    )
    assert _kinds(bs.sanity(report, None)) == ["cold_faster"]


def test_a_cell_that_moved_2x_either_way_is_flagged_on_one_vm() -> None:
    then = _report(_cell("a", "cache", 10), _cell("b", "cache", 10), _cell("c", "cache", 10))
    now = _report(_cell("a", "cache", 20), _cell("b", "cache", 5), _cell("c", "cache", 19))
    out = bs.sanity(now, then)
    assert out["comparison"] == "same VM"
    assert [(f["cell"], f["ratio"]) for f in out["flags"]] == [
        ("a sql(sql) cache", 2.0),
        ("b sql(sql) cache", 0.5),
    ]


def test_a_baseline_from_another_vm_is_refused_and_the_run_checks_still_stand() -> None:
    then = _report(_cell("q", "cache", 10), host={"instance_id": "2", "cpu_platform": "AMD Turin"})
    now = _report(_cell("q", "nocache", 10), _cell("q", "cache", 30))
    out = bs.sanity(now, then)
    assert out["comparison"].startswith("refused: the baseline ran on 2 (AMD Turin), this run on 1")
    assert _kinds(out) == ["cached_slower"]


def test_a_baseline_that_records_no_vm_is_refused() -> None:
    out = bs.sanity(_report(_cell("q", "cache", 1)), {"results": []})
    assert out["comparison"] == "refused: the baseline does not record its VM and CPU platform"


def test_one_vm_on_another_platform_is_refused() -> None:
    with pytest.raises(bs.CrossVmComparison, match="timings from two VMs are not compared"):
        bs.refuse_cross_vm(HOST, {**HOST, "cpu_platform": "Intel Broadwell"})


def test_the_cli_writes_the_report(tmp_path: Path) -> None:
    current = tmp_path / "now.json"
    current.write_text(json.dumps(_report(_cell("q", "nocache", 1), _cell("q", "cache", 2))))
    out = bs.report_files(current, None)
    assert _kinds(out) == ["cached_slower"]
