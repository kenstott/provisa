# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6b3d92-7a48-4c05-9e21-c8d4a0b7e563
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each run compared with the table's previous successful run (REQ-1934, EACH RUN COMPARED WITH
THE ONE BEFORE).

A run's measures are read off its result rows (:func:`measures_of`) -- the rows this run is about
to write, or a stored run's rows read back from its result relations -- so the comparison is a
measure of the current run computed from what both runs recorded, never a second read of the
table. :func:`compare` turns two runs' measures into the run's ``drift`` rows.
"""

# Requirements: REQ-1934

from __future__ import annotations

import json
import statistics
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from provisa.profiler.measures import sketch_cdf
from provisa.profiler.statement import QUANTILE_POINTS

# REQ-1934: how many of a categorical column's largest share changes the comparison records.
TOP_SHARE_CHANGES = 5
# REQ-1934: the population stability index takes the log of each bin's share ratio; a bin empty in
# one run would make it infinite, so every share is floored at this, the usual convention.
PSI_FLOOR = 1e-4

# The column measures that hold values of the column (governed as values, REQ-1934).
_VALUE_MEASURES = frozenset({"mean", "stddev", "min", "max"})


@dataclass(frozen=True)
class Scalar:
    value: float
    value_bearing: bool
    involved: tuple[str, ...]


@dataclass(frozen=True)
class Categories:
    """A categorical column's shares: ``full`` when every value is listed (its whole frequency
    table), else its most frequent values only."""

    shares: dict[str | None, float]
    full: bool


@dataclass
class RunMeasures:
    """What a run recorded, keyed by ``(scope, column_name, subject, measure)``."""

    scalars: dict[tuple[str, str | None, str | None, str], Scalar] = field(default_factory=dict)
    # column -> (101-point value sketch, non-null rows)
    sketches: dict[str, tuple[list[float], int]] = field(default_factory=dict)
    # relationship -> (101-point children-per-parent sketch, parents)
    fanouts: dict[str, tuple[list[float], int]] = field(default_factory=dict)
    categories: dict[str, Categories] = field(default_factory=dict)
    # column -> data type, for columns added, removed or changed in type
    types: dict[str, str | None] = field(default_factory=dict)


def _num(value: object) -> float | None:
    return None if value is None else float(value)  # type: ignore[arg-type]


def measures_of(results: dict[str, list[dict]]) -> RunMeasures:
    """One run's measures from its rows, by result kind (``runs`` holding the run's one row)."""
    out = RunMeasures()
    put = out.scalars

    def scalar(key: tuple, value: object, value_bearing: bool, involved: tuple[str, ...]) -> None:
        v = _num(value)
        if v is not None:
            put[key] = Scalar(v, value_bearing, involved)

    (run,) = results["runs"]
    scalar(("table", None, None, "row_count"), run["row_count"], False, ())
    scalar(("table", None, None, "duplicate_share"), run["duplicate_share"], False, ())
    scalar(("table", None, None, "freshness_seconds"), run["freshness_seconds"], False, ())
    for d in results.get("duplicates", []):
        if d["subject"] == "key":
            cols = tuple(json.loads(d["involved_columns"]))
            scalar(
                ("key", None, d["key_name"], "repeated_values"), d["repeated_values"], False, cols
            )
    sketch_points: dict[str, dict[float, float]] = {}
    for q in results.get("quantiles", []):
        if q["measure"] == "value" and q["value"] is not None:
            sketch_points.setdefault(q["column_name"], {})[round(float(q["q"]), 2)] = float(
                q["value"]
            )
    rows_by_column: dict[str, int] = {}
    for c in results.get("columns", []):
        name = c["column_name"]
        out.types[name] = c["data_type"]
        rows_by_column[name] = int(c["row_count"])
        one = (name,)
        scalar(("column", name, None, "null_share"), c["null_share"], False, one)
        scalar(("column", name, None, "distinct_ratio"), c["distinct_ratio"], False, one)
        scalar(("column", name, None, "mean"), c["mean"], True, one)
        scalar(("column", name, None, "stddev"), c["stddev"], True, one)
        points = sketch_points.get(name)
        if points is not None and len(points) == len(QUANTILE_POINTS):
            sketch = [points[q] for q in QUANTILE_POINTS]
            out.sketches[name] = (sketch, int(c["row_count"]) - int(c["null_count"]))
            scalar(("column", name, None, "min"), sketch[0], True, one)
            scalar(("column", name, None, "max"), sketch[-1], True, one)
    tops: dict[str, dict[str | None, int]] = {}
    freqs: dict[str, dict[str | None, int]] = {}
    for t in results.get("top_values", []):
        (freqs if t["kind"] == "frequency" else tops).setdefault(t["column_name"], {})[
            t["value"]
        ] = int(t["row_count"])
    for name in set(tops) | set(freqs):
        rows = rows_by_column.get(name)
        if not rows:
            continue
        full = name in freqs
        counts = freqs[name] if full else tops[name]
        out.categories[name] = Categories({v: n / rows for v, n in counts.items()}, full)
    for c in results.get("correlations", []):
        cols = tuple(json.loads(c["involved_columns"]))
        scalar(
            ("correlation", c["column_name"], c["other_column"], c["measure"]),
            c["value"],
            False,
            cols,
        )
    for c in results.get("constraint_checks", []):
        cols = tuple(json.loads(c["involved_columns"]))
        what = (
            c["constraint"]
            if c["other_column"] is None
            else f"{c['constraint']} {c['other_column']}"
        )
        scalar(("constraint", c["column_name"], what, "pass_share"), c["pass_share"], False, cols)
    for d in results.get("dependencies", []):
        cols = tuple(json.loads(d["involved_columns"]))
        parents = ", ".join(p for p in (d["parent_1"], d["parent_2"]) if p is not None)
        key = ("dependency", d["column_name"], parents, "mutual_information")
        scalar(key, d["mutual_information"], False, cols)
    fan_points: dict[str, dict[float, float]] = {}
    for f in results.get("fanout", []):
        fan_points.setdefault(f["relationship"], {})[round(float(f["q"]), 2)] = float(f["value"])
    for f in results.get("fanout_runs", []):
        rel = f["relationship"]
        scalar(("relationship", None, rel, "mean"), f["mean"], False, ())
        scalar(("relationship", None, rel, "max"), f["max"], False, ())
        scalar(("relationship", None, rel, "childless_share"), f["childless_share"], False, ())
        points = fan_points.get(rel)
        if points is not None and len(points) == len(QUANTILE_POINTS):
            out.fanouts[rel] = ([points[q] for q in QUANTILE_POINTS], int(f["parents"]))
    return out


# -- distribution shift --------------------------------------------------------------------------


def _cdf(sketch: list[float]) -> Callable[[float], float]:
    return lambda x: sketch_cdf(sketch, x)


def mixture_cdf(parts: list[tuple[list[float], int]]) -> Callable[[float], float]:
    """The CDF of several runs' rows pooled: each sketch's CDF weighted by its rows."""
    total = sum(n for _, n in parts)
    if total <= 0:
        raise ValueError("a pooled distribution needs at least one row")
    return lambda x: sum(n * sketch_cdf(s, x) for s, n in parts) / total


def ks_stat(cdf_a: Callable[[float], float], cdf_b: Callable[[float], float], points) -> float:
    """The largest gap between two CDFs over ``points`` (every sketch point of both): both are
    piecewise linear between their points, so the gap is largest at one of them."""
    return max(abs(cdf_a(x) - cdf_b(x)) for x in points)


def _quantile_of(cdf: Callable[[float], float], p: float, lo: float, hi: float) -> float:
    for _ in range(60):
        mid = (lo + hi) / 2
        if cdf(mid) < p:
            lo = mid
        else:
            hi = mid
    return hi


def psi(
    expected: Callable[[float], float], actual: Callable[[float], float], lo: float, hi: float
) -> float:
    """Population stability index of ``actual`` against ``expected``, over the ten bins of
    ``expected``'s deciles (bins tied by repeated values merged)."""
    edges = sorted({_quantile_of(expected, d / 10, lo, hi) for d in range(1, 10)})
    bounds = [-math.inf, *edges, math.inf]

    def share(cdf: Callable[[float], float], a: float, b: float) -> float:
        upper = 1.0 if b == math.inf else cdf(b)
        lower = 0.0 if a == -math.inf else cdf(a)
        return max(upper - lower, PSI_FLOOR)

    total = 0.0
    for a, b in zip(bounds, bounds[1:]):
        e, s = share(expected, a, b), share(actual, a, b)
        total += (s - e) * math.log(s / e)
    return total


def sketch_shift(
    expected: list[tuple[list[float], int]], actual: list[float]
) -> tuple[float, float]:
    """``(ks, psi)`` of the ``actual`` sketch against the pooled ``expected`` sketches."""
    pooled = mixture_cdf(expected)
    points = sorted({x for s, _ in expected for x in s} | set(actual))
    lo, hi = points[0], points[-1]
    cur = _cdf(actual)
    return ks_stat(pooled, cur, points), psi(pooled, cur, lo, hi)


def category_shift(
    expected: dict[str | None, float], actual: dict[str | None, float]
) -> tuple[float, float]:
    """``(ks, psi)`` of two categorical share tables. The values not listed in a table are its
    remainder, one bin. KS runs over the cumulative shares in value order (null first)."""
    values = sorted(set(expected) | set(actual), key=lambda v: (v is not None, v or ""))
    rest_e = max(0.0, 1.0 - sum(expected.values()))
    rest_a = max(0.0, 1.0 - sum(actual.values()))
    cum_e = cum_a = 0.0
    ks = abs(rest_e - rest_a)
    total = 0.0
    for v in values:
        e, a = expected.get(v, 0.0), actual.get(v, 0.0)
        cum_e, cum_a = cum_e + e, cum_a + a
        ks = max(ks, abs(cum_e - cum_a))
        ef, af = max(e, PSI_FLOOR), max(a, PSI_FLOOR)
        total += (af - ef) * math.log(af / ef)
    if rest_e > 0 or rest_a > 0:
        ef, af = max(rest_e, PSI_FLOOR), max(rest_a, PSI_FLOOR)
        total += (af - ef) * math.log(af / ef)
    return ks, total


def pooled_categories(parts: list[tuple[Categories, int]]) -> dict[str | None, float]:
    """Several runs' shares pooled, each weighted by its rows."""
    total = sum(n for _, n in parts)
    pooled: dict[str | None, float] = {}
    for cats, n in parts:
        for v, s in cats.shares.items():
            pooled[v] = pooled.get(v, 0.0) + s * n / total
    return pooled


# -- the comparison ------------------------------------------------------------------------------

DRIFT_SCOPES = (
    "run",
    "table",
    "key",
    "column",
    "category",
    "relationship",
    "correlation",
    "dependency",
    "constraint",
)


def _row(
    scope: str,
    measure: str,
    *,
    column: str | None = None,
    subject: str | None = None,
    involved: tuple[str, ...] = (),
    value_bearing: bool = False,
    current: float | None = None,
    previous: float | None = None,
    ks: float | None = None,
    psi_value: float | None = None,
    detail: str | None = None,
) -> dict:
    return {
        "scope": scope,
        "column_name": column,
        "involved_columns": json.dumps(list(involved)),
        "value_bearing": value_bearing,
        "measure": measure,
        "subject": subject,
        "current": current,
        "previous": previous,
        "change": None if current is None or previous is None else current - previous,
        "ks_previous": ks,
        "psi_previous": psi_value,
        "detail": detail,
        # Set by window_drift, once the table has a full window of previous runs.
        "window_runs": None,
        "baseline": None,
        "spread": None,
        "distance": None,
        "slope": None,
        "slope_spread": None,
        "ks": None,
        "psi": None,
        "drifting": None,
        "drift_reason": None,
    }


def compare(current: RunMeasures, previous: RunMeasures | None) -> list[dict]:
    """The current run's comparison with ``previous`` (None for a table's first run), as drift
    rows without their run key: one per measure of the run (REQ-1934, the drift rows shaped for a
    checker to scan), its previous value beside it where the previous run recorded it."""
    rows: list[dict] = []
    if previous is None:
        rows.append(_row("run", "previous_run", detail="no previous run"))
    for key in sorted(current.scalars, key=str):
        scope, column, subject, measure = key
        cur = current.scalars[key]
        prev = None if previous is None else previous.scalars.get(key)
        rows.append(
            _row(
                scope,
                measure,
                column=column,
                subject=subject,
                involved=cur.involved,
                value_bearing=cur.value_bearing,
                current=cur.value,
                previous=None if prev is None else prev.value,
            )
        )
    if previous is None:
        return rows
    for name in sorted(set(current.types) - set(previous.types)):
        rows.append(
            _row("column", "added", column=name, involved=(name,), detail=current.types[name])
        )
    for name in sorted(set(previous.types) - set(current.types)):
        rows.append(
            _row("column", "removed", column=name, involved=(name,), detail=previous.types[name])
        )
    for name in sorted(set(current.types) & set(previous.types)):
        if current.types[name] != previous.types[name]:
            rows.append(
                _row(
                    "column",
                    "type_changed",
                    column=name,
                    involved=(name,),
                    detail=f"{previous.types[name]} -> {current.types[name]}",
                )
            )
    for name in sorted(set(current.sketches) & set(previous.sketches)):
        ks, p = sketch_shift([previous.sketches[name]], current.sketches[name][0])
        rows.append(
            _row("column", "distribution", column=name, involved=(name,), ks=ks, psi_value=p)
        )
    for rel in sorted(set(current.fanouts) & set(previous.fanouts)):
        ks, p = sketch_shift([previous.fanouts[rel]], current.fanouts[rel][0])
        rows.append(_row("relationship", "distribution", subject=rel, ks=ks, psi_value=p))
    for name in sorted(set(current.categories) & set(previous.categories)):
        rows += _category_rows(name, current.categories[name], previous.categories[name])
    return rows


def _category_rows(name: str, cur: Categories, prev: Categories) -> list[dict]:
    one = (name,)
    ks, p = category_shift(prev.shares, cur.shares)
    rows = [
        _row(
            "column",
            "category_shares",
            column=name,
            involved=one,
            value_bearing=True,
            ks=ks,
            psi_value=p,
        )
    ]
    if cur.full and prev.full:
        # Only a column's whole frequency table can say a value appeared or vanished: a value
        # leaving a most-frequent list may still be held.
        for v in sorted(set(cur.shares) - set(prev.shares), key=lambda x: (x is not None, x or "")):
            rows.append(
                _row(
                    "category",
                    "appeared",
                    column=name,
                    subject=v,
                    involved=one,
                    value_bearing=True,
                    current=cur.shares[v],
                    previous=0.0,
                )
            )
        for v in sorted(set(prev.shares) - set(cur.shares), key=lambda x: (x is not None, x or "")):
            rows.append(
                _row(
                    "category",
                    "vanished",
                    column=name,
                    subject=v,
                    involved=one,
                    value_bearing=True,
                    current=0.0,
                    previous=prev.shares[v],
                )
            )
    both = set(cur.shares) & set(prev.shares)
    largest = sorted(
        both, key=lambda v: (-abs(cur.shares[v] - prev.shares[v]), v is not None, v or "")
    )
    for v in largest[:TOP_SHARE_CHANGES]:
        rows.append(
            _row(
                "category",
                "share",
                column=name,
                subject=v,
                involved=one,
                value_bearing=True,
                current=cur.shares[v],
                previous=prev.shares[v],
            )
        )
    return rows


# -- drift across runs ---------------------------------------------------------------------------


def in_season(season: str, run_time: datetime, other: datetime) -> bool:
    """Whether ``other`` is at the same point in ``season`` as ``run_time`` (UTC): the same hour of
    the day (daily), weekday (weekly) or day of the month (monthly); every run for ``none``."""
    a, b = run_time.astimezone(UTC), other.astimezone(UTC)
    if season == "none":
        return True
    if season == "daily":
        return a.hour == b.hour
    if season == "weekly":
        return a.weekday() == b.weekday()
    if season == "monthly":
        return a.day == b.day
    raise ValueError(f"unknown drift season {season!r}")


def window_of(
    season: str, size: int, run_time: datetime, previous: list[tuple[str, datetime]]
) -> list[tuple[str, datetime]]:
    """The run's window: the latest ``size`` of ``previous`` (latest first) at its point in the
    season. Fewer when the table has not run that often at that point yet."""
    return [p for p in previous if in_season(season, run_time, p[1])][:size]


def _slope(points: list[tuple[float, float]]) -> float | None:
    """Least-squares slope of ``(x, y)`` points; None when every x is the same."""
    n = len(points)
    mx = sum(x for x, _ in points) / n
    my = sum(y for _, y in points) / n
    sxx = sum((x - mx) ** 2 for x, _ in points)
    if sxx == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in points) / sxx


def window_drift(
    rows: list[dict],
    current: RunMeasures,
    run_time: datetime,
    window: list[tuple[datetime, RunMeasures]],
    settings: Any,
) -> None:
    """Set each drift row's window measures in place (REQ-1934 DRIFT ACROSS RUNS; NO DRIFT BEFORE A
    FULL WINDOW; SEASONAL AND ROBUST BASELINES): against the ``window`` (its runs' times and
    measures) when it is full, every window field NULL otherwise. ``settings`` is the profiler's
    ProfilerSettings."""
    for row in rows:
        row["window_runs"] = len(window)
    if len(window) < settings.drift_window:
        return
    t0 = min(t for t, _ in window)
    for row in rows:
        key = (row["scope"], row["column_name"], row["subject"], row["measure"])
        if key in current.scalars:
            _scalar_drift(row, key, current, run_time, t0, window, settings)
        elif row["measure"] == "distribution":
            if row["scope"] == "column":
                cur_sketch, past = current.sketches, [m.sketches for _, m in window]
                name = row["column_name"]
            else:
                cur_sketch, past = current.fanouts, [m.fanouts for _, m in window]
                name = row["subject"]
            if all(name in p for p in past):
                parts = [p[name] for p in past]
                if sum(n for _, n in parts) > 0:
                    row["ks"], row["psi"] = sketch_shift(parts, cur_sketch[name][0])
                    _flag(row, settings)
        elif row["measure"] == "category_shares":
            name = row["column_name"]
            past = [m.categories.get(name) for _, m in window]
            rows_of = [m.scalars.get(("table", None, None, "row_count")) for _, m in window]
            if all(p is not None for p in past) and all(r is not None for r in rows_of):
                parts = [(p, int(r.value)) for p, r in zip(past, rows_of) if p and r]
                if parts and sum(n for _, n in parts) > 0:
                    pooled = pooled_categories(parts)
                    row["ks"], row["psi"] = category_shift(pooled, current.categories[name].shares)
                    _flag(row, settings)


def _scalar_drift(
    row: dict,
    key: tuple,
    current: RunMeasures,
    run_time: datetime,
    t0: datetime,
    window: list[tuple[datetime, RunMeasures]],
    settings: Any,
) -> None:
    past = [(t, m.scalars[key].value) for t, m in window if key in m.scalars]
    if len(past) < settings.drift_window:
        return  # the measure is younger than the window: no drift before a full window
    value = current.scalars[key].value
    values = [v for _, v in past]
    baseline = statistics.median(values)
    mad = statistics.median(abs(v - baseline) for v in values)
    day = 86400.0
    points = [((t - t0).total_seconds() / day, v) for t, v in past]
    points.append(((run_time - t0).total_seconds() / day, value))
    slope = _slope(points)
    span = max(x for x, _ in points) - min(x for x, _ in points)
    row.update(baseline=baseline, spread=mad, slope=slope)
    if mad > 0:
        row["distance"] = (value - baseline) / mad
        if slope is not None:
            row["slope_spread"] = slope * span / mad
    reasons = []
    if row["distance"] is not None and abs(row["distance"]) > settings.drift_distance:
        reasons.append("distance")
    # A measure constant across its window has no spread to measure a distance in: any change
    # from that constant is drifting (REQ-1934 SEASONAL AND ROBUST BASELINES, MAD spread).
    if mad == 0 and value != baseline:
        reasons.append("distance")
    if row["slope_spread"] is not None and abs(row["slope_spread"]) > settings.drift_slope:
        reasons.append("slope")
    row["drifting"] = bool(reasons)
    row["drift_reason"] = ", ".join(reasons) or None


def _flag(row: dict, settings: Any) -> None:
    reasons = []
    if row["ks"] > settings.drift_ks:
        reasons.append("ks")
    if row["psi"] > settings.drift_psi:
        reasons.append("psi")
    row["drifting"] = bool(reasons)
    row["drift_reason"] = ", ".join(reasons) or None
