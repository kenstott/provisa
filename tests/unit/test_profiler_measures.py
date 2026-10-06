# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6b8d24-c03a-4e97-8b5d-7a2e9c4f0d31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Measures derived from the profile statement's aggregates (REQ-1934): moments, the histogram read
off the sketch, and the closed-form fits ranked by their KS distance against the sketch."""

from __future__ import annotations

import math

import numpy as np
import pytest

from provisa.profiler import measures
from provisa.profiler.statement import QUANTILE_POINTS, ColumnAggregates, ColumnSpec


def _aggregates(values: np.ndarray, data_type: str = "double") -> ColumnAggregates:
    """What the profile statement returns for ``values``."""
    x = values.astype(float)
    positive = x[x > 0]
    return ColumnAggregates(
        spec=ColumnSpec("v", data_type, "numeric", "v"),
        non_null=len(x),
        distinct=len(np.unique(x)),
        vmin=float(x.min()),
        vmax=float(x.max()),
        m1=float(x.mean()),
        m2=float((x**2).mean()),
        m3=float((x**3).mean()),
        m4=float((x**4).mean()),
        stddev=float(x.std()),
        log_m1=float(np.log(positive).mean()) if len(positive) else None,
        log_m2=float((np.log(positive) ** 2).mean()) if len(positive) else None,
        positive=len(positive),
        integers=int((x == np.floor(x)).sum()),
        zeros=int((x == 0).sum()),
        quantiles=[float(q) for q in np.quantile(x, QUANTILE_POINTS)],
    )


def test_moments_of_a_normal_sample():
    agg = _aggregates(np.random.default_rng(1).normal(50, 5, 20000))
    m = measures.moments(agg)
    assert m.mean == pytest.approx(50, abs=0.2)
    assert m.stddev == pytest.approx(5, abs=0.1)
    assert m.skewness == pytest.approx(0, abs=0.1)
    assert m.kurtosis == pytest.approx(0, abs=0.2)
    assert m.integer_only is False
    assert m.log_mean == pytest.approx(math.log(50), abs=0.01)


def test_log_moments_need_every_value_positive():
    m = measures.moments(_aggregates(np.array([-1.0, 2.0, 3.0])))
    assert m.log_mean is None and m.log_variance is None
    assert m.zero_share == 0


def test_the_histogram_covers_every_row_and_the_whole_range():
    agg = _aggregates(np.random.default_rng(2).uniform(0, 100, 10000))
    buckets = measures.histogram(agg.quantiles or [], agg.non_null)
    assert len(buckets) == measures.HISTOGRAM_BUCKETS
    assert sum(b[3] for b in buckets) == pytest.approx(agg.non_null)
    assert buckets[0][1] == agg.vmin and buckets[-1][2] == agg.vmax
    assert all(b[3] == pytest.approx(500, rel=0.1) for b in buckets)


def test_a_constant_column_has_one_bucket():
    assert measures.histogram([7.0] * 101, 40) == [(1, 7.0, 7.0, 40.0)]


@pytest.mark.parametrize(
    "sample,family,param,expected",
    [
        (np.random.default_rng(3).normal(10, 2, 20000), "normal", "mu", 10),
        (np.random.default_rng(4).lognormal(1, 0.5, 20000), "log_normal", "mu", 1),
        (np.random.default_rng(6).uniform(5, 9, 20000), "uniform", "max", 9),
    ],
)
def test_the_best_fit_is_the_generating_family(sample, family, param, expected):
    agg = _aggregates(sample)
    fits = measures.fits(agg, measures.moments(agg))
    assert fits[0].family == family
    assert fits[0].params[param] == pytest.approx(expected, rel=0.05)
    assert fits == sorted(fits, key=lambda f: f.ks_stat)


def test_an_exponential_sample_fits_the_exponential_and_the_gamma_it_nests():
    agg = _aggregates(np.random.default_rng(5).exponential(4, 20000))
    fits = measures.fits(agg, measures.moments(agg))
    # The gamma family contains the exponential (shape 1), so the two lead, near-equal.
    assert {f.family for f in fits[:2]} == {"exponential", "gamma"}
    by = {f.family: f for f in fits}
    assert by["exponential"].params["rate"] == pytest.approx(0.25, rel=0.05)
    assert by["gamma"].params["shape"] == pytest.approx(1, rel=0.05)


def test_integer_counts_are_fitted_with_the_discrete_families():
    agg = _aggregates(np.random.default_rng(7).poisson(6, 20000))
    fits = {f.family: f for f in measures.fits(agg, measures.moments(agg))}
    assert "poisson" in fits
    assert fits["poisson"].params["lambda"] == pytest.approx(6, rel=0.05)


def test_overdispersed_counts_get_a_negative_binomial():
    agg = _aggregates(np.random.default_rng(8).negative_binomial(3, 0.3, 20000))
    fits = {f.family: f for f in measures.fits(agg, measures.moments(agg))}
    assert fits["negative_binomial"].params["r"] == pytest.approx(3, rel=0.1)


def test_a_constant_column_is_not_fitted():
    agg = _aggregates(np.full(50, 3.0))
    assert measures.fits(agg, measures.moments(agg)) == []
