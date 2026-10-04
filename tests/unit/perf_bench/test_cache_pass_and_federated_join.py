# Copyright (c) 2026 Kenneth Stott
# Canary: 92b2f245-9421-487f-afa7-82dd163e0db3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1894 / REQ-1889: the benchmark's two-pass cache reporting and its federated_join query text."""

from __future__ import annotations

import sys
from pathlib import Path

import graphql
import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import run_benchmark as rb  # noqa: E402
from queries import QUERIES  # noqa: E402

from provisa.compiler.naming import rel_field_name  # noqa: E402


def _result(cache_pass: str, elapsed: list[float]) -> rb.QueryResult:
    return rb.QueryResult(
        query_id="q",
        category="c",
        transport="graphql",
        samples=[rb.Sample(e, 1, 10) for e in elapsed],
        cache_pass=cache_pass,
    )


# One slow first iteration, nine fast ones: the shape a response cache produces.
_ELAPSED = [30.0] + [0.03] * 9


def test_the_nocache_pass_measures_every_iteration() -> None:
    s = _result("nocache", _ELAPSED).summary()
    assert s["n"] == 10
    assert s["cache_pass"] == "nocache"
    assert s["latency_ms_max"] == pytest.approx(30000.0)


def test_the_cache_pass_drops_iteration_one_before_percentiles() -> None:
    s = _result("cache", _ELAPSED).summary()
    assert s["n"] == 9
    assert s["cache_pass"] == "cache"
    assert s["latency_ms_max"] == pytest.approx(30.0)


def test_summary_reports_ops_per_hour_alongside_qps() -> None:
    s = _result("nocache", [0.5, 0.5]).summary()
    assert s["ops_per_hour"] == pytest.approx(s["qps"] * 3600)


class _Transport(rb.Transport):
    name = "graphql"

    def __init__(self) -> None:
        self.no_cache_flags: list[bool] = []

    def run_graphql(self, text, params, no_cache=False):
        self.no_cache_flags.append(no_cache)
        return 1, 10, 0.0


def _query() -> rb.Query:
    return rb.Query(id="q", category="c", description="d", graphql="query { x }", iterations=3)


@pytest.mark.parametrize(("no_cache", "pass_name"), [(True, "nocache"), (False, "cache")])
def test_each_pass_tags_its_result_and_forwards_the_cache_flag(no_cache, pass_name) -> None:
    t = _Transport()
    result = rb._run_sequential(t, _query(), "graphql", "query { x }", no_cache=no_cache)
    assert result.cache_pass == pass_name
    assert t.no_cache_flags == [no_cache] * 3


def test_the_two_passes_of_one_query_are_separate_rows_not_a_blend() -> None:
    t = _Transport()
    nocache = rb._run_sequential(t, _query(), "graphql", "query { x }", no_cache=True)
    cache = rb._run_sequential(t, _query(), "graphql", "query { x }", no_cache=False)
    assert {r.summary()["cache_pass"] for r in (nocache, cache)} == {"nocache", "cache"}


def test_the_nested_relationship_fields_are_named_from_target_and_cardinality() -> None:  # REQ-1889
    assert rel_field_name("pb__orderEvents", "one-to-many") == "orderEvents"
    assert rel_field_name("pb__orderDocs", "one-to-one") == "orderDoc"


def test_federated_join_is_one_nested_selection_under_the_orders_root_field() -> None:  # REQ-1889
    q = next(q for q in QUERIES if q.id == "federated_join")
    doc = graphql.parse(q.graphql)
    root_fields = doc.definitions[0].selection_set.selections
    assert [f.name.value for f in root_fields] == ["pb__orders"]
    nested = {f.name.value for f in root_fields[0].selection_set.selections}
    assert {"orderEvents", "orderDoc"} <= nested
