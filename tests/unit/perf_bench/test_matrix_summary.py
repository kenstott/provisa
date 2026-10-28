# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The query matrix's per-cell summary and printed table: each cell names its transport and the
dialect it carried, percentiles are nearest-rank, and a failure's count and text reach the report."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import run_benchmark as rb  # noqa: E402
from queries import QUERIES  # noqa: E402


class _Fake(rb.Transport):
    def __init__(self, name: str, fail: bool = False) -> None:
        self.name = name
        self._fail = fail

    def available(self) -> bool:
        return True

    def _answer(self, *_args, **_kwargs) -> tuple[int, int, float]:
        if self._fail:
            raise RuntimeError("Unknown label(s): ['Orders']")
        return 1, 10, 0.0

    run_sql = run_cypher = run_graphql = run_grpc = _answer


def _results(fail: bool = False) -> list[rb.QueryResult]:
    transports = {n: _Fake(n, fail) for n in rb.TRANSPORTS}
    return rb.run_all(transports, query_ids={"point_lookup"}, no_cache=True)


def test_each_cell_names_the_dialect_its_transport_carried() -> None:
    labels = {rb.cell_label(r.summary()) for r in _results()}
    assert labels == {
        "sql(sql)",
        "flight(sql)",
        "cypher(cypher)",
        "http(cypher)",
        "graphql(graphql)",
        "grpc(grpc)",
    }


def test_the_printed_table_shows_dialect_and_pass() -> None:
    table = rb.format_table([r.summary() for r in _results()])
    assert "flight(sql)" in table
    assert "flight(cypher)" not in table
    assert "nocache" in table
    assert "p95ms" in table


@pytest.mark.parametrize(
    ("n", "p95_index"),
    [(1, 0), (2, 1), (3, 2), (10, 9), (20, 18), (200, 189)],
)
def test_p95_is_nearest_rank_and_never_below_the_median(n: int, p95_index: int) -> None:
    elapsed = [(i + 1) / 1000 for i in range(n)]
    r = rb.QueryResult("q", "c", "sql", [rb.Sample(e, 1, 1) for e in elapsed], cache_pass="nocache")
    s = r.summary()
    assert s["latency_ms_p95"] == pytest.approx((p95_index + 1))
    assert s["latency_ms_p95"] >= s["latency_ms_p50"]
    assert s["latency_ms_p99"] >= s["latency_ms_p95"]


def test_a_failed_cell_counts_its_errors_and_keeps_the_first_error_text() -> None:
    summaries = [r.summary() for r in _results(fail=True)]
    (by_id,) = {s["query_id"] for s in summaries}
    iterations = next(q.iterations for q in QUERIES if q.id == by_id)
    for s in summaries:
        assert s["status"] == "all_failed"
        assert s["errors"] == iterations
        assert s["first_error"] == "Unknown label(s): ['Orders']"
    json.loads(json.dumps(summaries))  # the report's JSON round-trips
    assert "ALL FAILED: Unknown label(s)" in rb.format_table(summaries)


def test_the_row_materialized_neo4j_reads_name_a_key() -> None:
    for q in QUERIES:
        if q.category == "materialize_cost":
            assert q.sql is not None and "WHERE order_id = :order_id" in q.sql
            assert "order_id" in q.params
