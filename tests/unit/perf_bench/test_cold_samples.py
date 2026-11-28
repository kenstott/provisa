# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7d1c52-8f0e-4a6b-9d21-6e4c5a9f0b17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""neo4j_materialize_cold samples many times, each on a key no earlier request used."""

from __future__ import annotations

import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import optimistic_load  # noqa: E402
import run_benchmark as rb  # noqa: E402
from queries import QUERIES  # noqa: E402

COLD = next(q for q in QUERIES if q.id == "neo4j_materialize_cold")


class _Recorder(rb.Transport):
    def __init__(self, name: str) -> None:
        self.name = name
        self.keys: list[int] = []
        self.texts: list[str] = []

    def available(self) -> bool:
        return True

    def run_sql(self, sql: str, params: dict, category: str = "") -> tuple[int, int, float]:
        self.keys.append(params["order_id"])
        text = sql
        for k, v in params.items():
            text = text.replace(f":{k}", str(v))
        self.texts.append(text)
        return 1, 10, 0.0


def _run(no_cache: bool, transports: dict[str, rb.Transport], **kw) -> list[rb.QueryResult]:
    return rb.run_all(transports, query_ids={COLD.id}, no_cache=no_cache, **kw)


def test_the_cold_cell_samples_ten_times() -> None:
    assert COLD.iterations == 10
    assert COLD.cold_key is not None


def test_no_key_or_text_repeats_across_transports_and_passes(monkeypatch) -> None:
    monkeypatch.setattr(rb, "_COLD_KEYS", {})
    sql, flight = _Recorder("sql"), _Recorder("flight")
    transports = {"sql": sql, "flight": flight}
    results = _run(True, transports) + _run(False, transports)
    keys = sql.keys + flight.keys
    assert len(keys) == 4 * COLD.iterations
    assert len(set(keys)) == len(keys)
    assert len(set(sql.texts + flight.texts)) == len(keys)
    assert 12345 not in keys
    assert all(COLD.cold_key.first <= k < 33_936 for k in keys)
    for r in results:
        s = r.summary()
        assert s["errors"] == 0
        assert "stages" not in s


def test_other_cells_keep_one_key(monkeypatch) -> None:
    monkeypatch.setattr(rb, "_COLD_KEYS", {})
    t = _Recorder("sql")
    rb.run_all({"sql": t}, query_ids={"neo4j_materialize_warm"}, no_cache=True)
    assert set(t.keys) == {12345}


def test_stage_spans_of_the_cold_samples_are_reported(monkeypatch) -> None:
    monkeypatch.setattr(rb, "_COLD_KEYS", {})
    t = _Recorder("sql")
    totals = [
        {"requests": {}, "stages": {}},
        {
            "requests": {"pgwire": COLD.iterations},
            "stages": {
                "pgwire": {
                    "compile": {"ms": 20.0, "cpu_ms": 18.0, "count": 10.0},
                    "ensure_rows_resident": {"ms": 500.0, "cpu_ms": 50.0, "count": 10.0},
                }
            },
        },
    ]
    monkeypatch.setattr(optimistic_load, "_stage_totals", lambda url: totals.pop(0))
    (result,) = rb.run_all(
        {"sql": t}, query_ids={COLD.id}, no_cache=True, stage_tally_url="http://x"
    )
    stages = result.summary()["stages"]["pgwire"]
    assert stages["request_spans"] == COLD.iterations
    assert stages["stages"]["compile"]["ms_per_request"] == 2.0
    assert stages["stages"]["ensure_rows_resident"]["ms_per_request"] == 50.0
    assert "error" not in result.summary()["stages"]


def test_missing_spans_are_reported_not_hidden(monkeypatch) -> None:
    monkeypatch.setattr(rb, "_COLD_KEYS", {})
    monkeypatch.setattr(rb, "_SPAN_WAIT_S", 0.0)
    t = _Recorder("sql")
    monkeypatch.setattr(
        optimistic_load, "_stage_totals", lambda url: {"requests": {}, "stages": {}}
    )
    (result,) = rb.run_all(
        {"sql": t}, query_ids={COLD.id}, no_cache=True, stage_tally_url="http://x"
    )
    assert "0 of 10 request spans" in result.summary()["stages"]["error"]
