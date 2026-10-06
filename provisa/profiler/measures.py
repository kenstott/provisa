# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e8c37-94d2-4f6a-8c0e-2d7a6f1b9e48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Measures derived from the profile statement's aggregates, with no second read (REQ-1934).

Everything here is closed-form arithmetic over what the one statement returned: central moments from
the raw moments, a histogram read off the 101-point quantile sketch, method-of-moments fits of the
named families, and each fit's Kolmogorov-Smirnov distance against the sketch.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from typing import Any

from scipy import stats

from provisa.profiler.statement import QUANTILE_POINTS, ColumnAggregates

HISTOGRAM_BUCKETS = 20


@dataclass(frozen=True)
class Moments:
    mean: float | None
    stddev: float | None
    variance: float | None
    skewness: float | None
    kurtosis: float | None
    log_mean: float | None
    log_variance: float | None
    integer_only: bool | None
    zero_share: float | None


def moments(agg: ColumnAggregates) -> Moments:
    """Central moments of a numeric/temporal column; all None for any other family."""
    if agg.spec.family not in ("numeric", "temporal") or agg.non_null == 0:
        return Moments(None, None, None, None, None, None, None, None, None)
    mean, sd = agg.m1, agg.stddev
    assert mean is not None and sd is not None and agg.m2 is not None
    assert agg.m3 is not None and agg.m4 is not None
    variance = sd * sd
    skewness = kurtosis = None
    if sd > 0:
        third = agg.m3 - 3 * mean * agg.m2 + 2 * mean**3
        fourth = agg.m4 - 4 * mean * agg.m3 + 6 * mean**2 * agg.m2 - 3 * mean**4
        skewness = third / sd**3
        kurtosis = fourth / variance**2 - 3
    log_mean = log_variance = None
    if agg.positive == agg.non_null:
        assert agg.log_m1 is not None and agg.log_m2 is not None
        log_mean = agg.log_m1
        log_variance = max(agg.log_m2 - agg.log_m1**2, 0.0)
    return Moments(
        mean=mean,
        stddev=sd,
        variance=variance,
        skewness=skewness,
        kurtosis=kurtosis,
        log_mean=log_mean,
        log_variance=log_variance,
        integer_only=agg.integers == agg.non_null,
        zero_share=agg.zeros / agg.non_null,
    )


def sketch_cdf(sketch: list[float], x: float) -> float:
    """The empirical CDF the 101-point sketch describes, linear between its points."""
    if x < sketch[0]:
        return 0.0
    if x >= sketch[-1]:
        return 1.0
    hi = bisect.bisect_right(sketch, x)
    lo = hi - 1
    p_lo, p_hi = QUANTILE_POINTS[lo], QUANTILE_POINTS[hi]
    span = sketch[hi] - sketch[lo]
    return p_lo + (p_hi - p_lo) * (x - sketch[lo]) / span


def histogram(sketch: list[float], rows: int) -> list[tuple[int, float, float, float]]:
    """``(bucket, lo, hi, rows)`` of equal-width buckets over the sketch's range."""
    lo, hi = sketch[0], sketch[-1]
    if hi == lo:
        return [(1, lo, hi, float(rows))]
    width = (hi - lo) / HISTOGRAM_BUCKETS
    out = []
    for b in range(HISTOGRAM_BUCKETS):
        b_lo = lo + b * width
        b_hi = hi if b == HISTOGRAM_BUCKETS - 1 else lo + (b + 1) * width
        start = 0.0 if b == 0 else sketch_cdf(sketch, b_lo)
        share = sketch_cdf(sketch, b_hi) - start
        out.append((b + 1, b_lo, b_hi, share * rows))
    return out


@dataclass(frozen=True)
class Fit:
    family: str
    params: dict[str, float]
    ks_stat: float


def _ks(sketch: list[float], cdf) -> float:
    return max(abs(float(cdf(v)) - p) for v, p in zip(sketch, QUANTILE_POINTS))


def fits(agg: ColumnAggregates, m: Moments) -> list[Fit]:
    """Method-of-moments fits of every family that applies, best KS first."""
    sketch = agg.quantiles
    if sketch is None or m.mean is None or not m.variance or agg.vmin is None:
        return []
    assert agg.vmax is not None and m.stddev is not None
    mean, var, sd = m.mean, m.variance, m.stddev
    candidates: list[tuple[str, dict[str, float], Any]] = [
        ("normal", {"mu": mean, "sigma": sd}, stats.norm(loc=mean, scale=sd)),
        (
            "uniform",
            {"min": agg.vmin, "max": agg.vmax},
            stats.uniform(loc=agg.vmin, scale=agg.vmax - agg.vmin),
        ),
    ]
    if m.log_mean is not None and m.log_variance:
        sigma = math.sqrt(m.log_variance)
        candidates.append(
            (
                "log_normal",
                {"mu": m.log_mean, "sigma": sigma},
                stats.lognorm(s=sigma, scale=math.exp(m.log_mean)),
            )
        )
    if agg.vmin >= 0 and mean > 0:
        candidates.append(("exponential", {"rate": 1 / mean}, stats.expon(scale=mean)))
        shape, scale = mean * mean / var, var / mean
        candidates.append(
            ("gamma", {"shape": shape, "scale": scale}, stats.gamma(a=shape, scale=scale))
        )
        if m.integer_only:
            candidates.append(("poisson", {"lambda": mean}, stats.poisson(mu=mean)))
            if var > mean:
                r, p = mean * mean / (var - mean), mean / var
                candidates.append(("negative_binomial", {"r": r, "p": p}, stats.nbinom(n=r, p=p)))
    out = [Fit(family, params, _ks(sketch, dist.cdf)) for family, params, dist in candidates]
    return sorted(out, key=lambda f: f.ks_stat)
