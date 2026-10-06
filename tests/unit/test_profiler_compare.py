# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7e1a95-2d38-4f60-b9a4-e5f2c8d1b037
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each run compared with the one before (REQ-1934): the comparison read off two runs' rows."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from provisa.profiler import compare
from provisa.profiler.statement import QUANTILE_POINTS


def _sketch(lo: float, hi: float) -> list[float]:
    return [lo + (hi - lo) * q for q in QUANTILE_POINTS]


def _run(
    *,
    rows: int = 100,
    dup_share: float = 0.0,
    amount: tuple[float, float] = (0.0, 100.0),
    regions: dict[str | None, int] | None = None,
    columns: tuple[str, ...] = ("amount", "region"),
    amount_type: str = "integer",
) -> dict[str, list[dict]]:
    regions = regions if regions is not None else {"east": 50, "west": 50}
    cols = []
    for name in columns:
        cols.append(
            {
                "column_name": name,
                "data_type": amount_type if name == "amount" else "varchar",
                "row_count": rows,
                "null_count": 0,
                "null_share": 0.0,
                "distinct_ratio": 0.5,
                "mean": sum(amount) / 2 if name == "amount" else None,
                "stddev": 10.0 if name == "amount" else None,
            }
        )
    out: dict[str, list[dict]] = {
        "runs": [{"row_count": rows, "duplicate_share": dup_share, "freshness_seconds": None}],
        "columns": cols,
        "quantiles": [],
        "top_values": [],
        "fanout": [],
        "fanout_runs": [],
        "duplicates": [
            {
                "subject": "key",
                "key_name": "primary key",
                "involved_columns": json.dumps(["amount"]),
                "repeated_values": 0,
            }
        ],
    }
    if "amount" in columns:
        out["quantiles"] = [
            {"column_name": "amount", "measure": "value", "q": q, "value": v}
            for q, v in zip(QUANTILE_POINTS, _sketch(*amount))
        ]
    if "region" in columns:
        out["top_values"] = [
            {"column_name": "region", "kind": "frequency", "value": v, "row_count": n}
            for v, n in regions.items()
        ]
    return out


def _by(rows: list[dict]) -> dict[tuple, dict]:
    return {(r["scope"], r["column_name"], r["subject"], r["measure"]): r for r in rows}


def test_a_first_run_records_that_it_has_no_previous_run():
    rows = compare.compare(compare.measures_of(_run()), None)
    assert (rows[0]["scope"], rows[0]["measure"], rows[0]["detail"]) == (
        "run",
        "previous_run",
        "no previous run",
    )
    # Still one row per measure of the run, with nothing to set it beside.
    count = _by(rows)[("table", None, None, "row_count")]
    assert (count["current"], count["previous"], count["change"]) == (100, None, None)


def test_an_unchanged_run_shows_no_change():
    rows = _by(compare.compare(compare.measures_of(_run()), compare.measures_of(_run())))
    assert rows[("table", None, None, "row_count")]["change"] == 0
    dist = rows[("column", "amount", None, "distribution")]
    assert dist["ks_previous"] == pytest.approx(0) and dist["psi_previous"] == pytest.approx(0)
    assert rows[("column", "region", None, "category_shares")]["psi_previous"] == pytest.approx(0)


def test_changes_in_counts_moments_and_duplicates_are_recorded():
    prev = compare.measures_of(_run())
    cur = compare.measures_of(_run(rows=150, dup_share=0.1, amount=(50.0, 150.0)))
    rows = _by(compare.compare(cur, prev))
    assert rows[("table", None, None, "row_count")]["change"] == 50
    assert rows[("table", None, None, "duplicate_share")]["current"] == 0.1
    mean = rows[("column", "amount", None, "mean")]
    assert (mean["previous"], mean["current"], mean["value_bearing"]) == (50.0, 100.0, True)
    assert rows[("column", "amount", None, "min")]["change"] == 50.0
    assert rows[("column", "amount", None, "null_share")]["value_bearing"] is False
    assert json.loads(mean["involved_columns"]) == ["amount"]
    # Half of the new range lies above the old maximum.
    assert rows[("column", "amount", None, "distribution")]["ks_previous"] == pytest.approx(0.5)
    assert rows[("column", "amount", None, "distribution")]["psi_previous"] > 1


def test_categories_that_appear_or_vanish_and_the_largest_share_changes():
    prev = compare.measures_of(_run(regions={"east": 50, "west": 50}))
    cur = compare.measures_of(_run(regions={"east": 80, "north": 20}))
    rows = _by(compare.compare(cur, prev))
    assert rows[("category", "region", "north", "appeared")]["current"] == 0.2
    assert rows[("category", "region", "west", "vanished")]["previous"] == 0.5
    share = rows[("category", "region", "east", "share")]
    assert share["change"] == pytest.approx(0.3) and share["value_bearing"] is True
    assert rows[("column", "region", None, "category_shares")]["psi_previous"] > 0.25


def test_columns_added_removed_and_retyped():
    prev = compare.measures_of(_run(columns=("amount", "region")))
    cur = compare.measures_of(_run(columns=("amount",), amount_type="bigint"))
    rows = _by(compare.compare(cur, prev))
    assert rows[("column", "region", None, "removed")]["detail"] == "varchar"
    assert rows[("column", "amount", None, "type_changed")]["detail"] == "integer -> bigint"
    cur2 = compare.measures_of(_run(columns=("amount", "region", "code")))
    assert ("column", "code", None, "added") in _by(compare.compare(cur2, prev))


def test_a_pooled_distribution_weighs_each_run_by_its_rows():
    pooled = compare.mixture_cdf([(_sketch(0, 10), 100), (_sketch(10, 20), 300)])
    assert pooled(10) == pytest.approx(0.25)
    with pytest.raises(ValueError, match="at least one row"):
        compare.mixture_cdf([(_sketch(0, 10), 0)])


# -- drift across runs (REQ-1934 DRIFT ACROSS RUNS) ------------------------------------------------

_T0 = datetime(2026, 9, 1, 3, 0, tzinfo=UTC)
_SETTINGS = SimpleNamespace(
    drift_window=4,
    drift_season="none",
    drift_distance=3.0,
    drift_slope=3.0,
    drift_ks=0.2,
    drift_psi=0.25,
)


def _window(counts: list[int], **kw) -> list[tuple[datetime, compare.RunMeasures]]:
    """Runs a day apart, oldest first, with the given row counts."""
    return [
        (_T0 + timedelta(days=i), compare.measures_of(_run(rows=n, **kw)))
        for i, n in enumerate(counts)
    ]


def _drift(current: dict, window, settings=_SETTINGS) -> dict[tuple, dict]:
    cur = compare.measures_of(current)
    rows = compare.compare(cur, window[-1][1])
    run_time = window[-1][0] + timedelta(days=1)
    compare.window_drift(rows, cur, run_time, list(reversed(window)), settings)
    return _by(rows)


def test_no_drift_is_measured_before_a_full_window():
    """REQ-1934 NO DRIFT BEFORE A FULL WINDOW: every window measure is NULL, the prior-run count is
    recorded, and the comparison with the previous run still is."""
    rows = _drift(_run(rows=500), _window([100, 100, 100]))
    count = rows[("table", None, None, "row_count")]
    assert count["window_runs"] == 3 and count["change"] == 400
    window_fields = ("baseline", "spread", "distance", "slope", "ks", "psi", "drifting")
    assert all(r[f] is None for r in rows.values() for f in window_fields)


def test_a_run_far_from_its_windows_baseline_is_drifting_in_mad_units():
    rows = _drift(_run(rows=130), _window([100, 102, 98, 101]))
    count = rows[("table", None, None, "row_count")]
    # median 100.5; absolute deviations 0.5, 1.5, 2.5, 0.5 -> MAD 1.0
    assert (count["baseline"], count["spread"], count["window_runs"]) == (100.5, 1.0, 4)
    assert count["distance"] == pytest.approx(29.5)
    assert count["drifting"] is True and "distance" in count["drift_reason"]
    steady = _drift(_run(rows=101), _window([100, 102, 98, 101]))
    assert steady[("table", None, None, "row_count")]["drifting"] is False


def test_a_gradual_drift_no_single_step_reveals_is_caught_by_its_slope():
    rows = _drift(_run(rows=108), _window([100, 102, 104, 106]))
    count = rows[("table", None, None, "row_count")]
    # Each step is 2 rows; the trend across the window is far past the slope threshold in MADs.
    assert count["slope"] == pytest.approx(2.0)
    assert count["drifting"] is True and "slope" in count["drift_reason"]


def test_a_measure_constant_across_its_window_that_changes_is_drifting():
    rows = _drift(_run(rows=101), _window([100, 100, 100, 100]))
    count = rows[("table", None, None, "row_count")]
    assert count["spread"] == 0 and count["distance"] is None
    assert count["drifting"] is True


def test_a_distribution_shifted_from_the_pooled_window_is_drifting():
    window = _window([100, 100, 100, 100])
    rows = _drift(_run(amount=(50.0, 150.0), regions={"east": 90, "west": 10}), window)
    dist = rows[("column", "amount", None, "distribution")]
    assert dist["ks"] == pytest.approx(0.5) and dist["drifting"] is True
    assert set(dist["drift_reason"].split(", ")) == {"ks", "psi"}
    shares = rows[("column", "region", None, "category_shares")]
    assert shares["psi"] > 0.25 and shares["drifting"] is True
    calm = _drift(_run(), window)
    assert calm[("column", "amount", None, "distribution")]["drifting"] is False


def test_a_seasonal_window_holds_the_runs_at_the_same_point_in_the_season():
    """REQ-1934 SEASONAL BASELINES: weekly -- the last N runs on the same weekday."""
    monday = datetime(2026, 10, 5, 3, 0, tzinfo=UTC)
    previous = [(f"r{i}", monday - timedelta(days=i)) for i in range(1, 30)]
    window = compare.window_of("weekly", 3, monday, previous)
    assert [rid for rid, _ in window] == ["r7", "r14", "r21"]
    assert len(compare.window_of("none", 3, monday, previous)) == 3
    assert [rid for rid, _ in compare.window_of("daily", 2, monday, previous)] == ["r1", "r2"]
    assert compare.in_season("monthly", monday, monday - timedelta(days=30)) is True
    with pytest.raises(ValueError, match="unknown drift season"):
        compare.in_season("yearly", monday, monday)
