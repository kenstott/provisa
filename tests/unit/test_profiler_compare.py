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
        "runs": [{"row_count": rows, "duplicate_share": dup_share}],
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
    assert [(r["scope"], r["measure"], r["detail"]) for r in rows] == [
        ("run", "previous_run", "no previous run")
    ]


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
