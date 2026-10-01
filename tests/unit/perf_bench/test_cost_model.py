# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 derived results: the cost model (base cost plus a cost per field, filter, row and join)
and the break-even between live and replica reads."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import cost_model as cm  # noqa: E402

TRUE = {
    "base": 0.20,
    "fields": 0.010,
    "filters": 0.030,
    "rows": 0.0010,
    "joins_same": 0.50,
    "joins_cross": 1.20,
}


def _obs(**knobs: float) -> cm.Observation:
    means = {
        "fields": 1.0,
        "filters": 0.0,
        "rows": 1.0,
        "joins_same": 0.0,
        "joins_cross": 0.0,
    } | knobs
    cost = TRUE["base"] + sum(TRUE[k] * v for k, v in means.items())
    return cm.Observation(knob_means=means, cpu_ms=cost)


def _sweep() -> list[cm.Observation]:
    out = [_obs()]
    for field in (3.0, 5.0):
        out.append(_obs(fields=field))
    for n in (1.0, 2.0):
        out.append(_obs(filters=n))
    for rows in (10.0, 100.0):
        out.append(_obs(rows=rows))
    for n in (1.0, 2.0):
        out.append(_obs(joins_same=n))
    for n in (1.0, 2.0):
        out.append(_obs(joins_cross=n))
    return out


def test_fit_recovers_the_base_and_each_knobs_cost() -> None:
    model = cm.fit(_sweep())
    assert model.base == pytest.approx(TRUE["base"], abs=1e-9)
    for knob in cm.KNOBS:
        assert model.per_unit[knob] == pytest.approx(TRUE[knob], abs=1e-9), knob
    assert model.r2 == pytest.approx(1.0) and model.n == len(_sweep())


def test_predict_a_mix_the_sweeps_never_ran() -> None:
    model = cm.fit(_sweep())
    mix = {"fields": 2.5, "filters": 0.7, "rows": 30.0, "joins_same": 0.3, "joins_cross": 0.6}
    expected = TRUE["base"] + sum(TRUE[k] * v for k, v in mix.items())
    assert model.predict(mix) == pytest.approx(expected)


def test_a_knob_that_never_varies_cannot_be_costed() -> None:
    obs = [o for o in _sweep() if o.knob_means["filters"] == 0.0]
    with pytest.raises(ValueError, match="'filters' does not vary across the observations"):
        cm.fit(obs)


def test_too_few_observations() -> None:
    with pytest.raises(ValueError, match="needs at least 6 observations, got 3"):
        cm.fit(_sweep()[:3])


def test_a_collinear_pair_is_named() -> None:
    obs = []
    for n in (0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0):
        means = {
            "fields": 1 + n,
            "filters": n,
            "rows": 1.0 + n * 2,
            "joins_same": n,
            "joins_cross": n * 0.5,
        }
        obs.append(cm.Observation(means, 1.0 + n))
    with pytest.raises(ValueError, match="knobs move together"):
        cm.fit(obs)


def test_check_reports_how_far_off_the_prediction_is() -> None:
    model = cm.fit(_sweep())
    mixed = cm.Observation(
        {"fields": 2.0, "filters": 1.0, "rows": 20.0, "joins_same": 0.5, "joins_cross": 0.5},
        cpu_ms=0.0,
    )
    predicted = model.predict(mixed.knob_means)
    mixed = cm.Observation(mixed.knob_means, cpu_ms=predicted * 1.1)
    check = model.check(mixed)
    assert check.predicted_ms == pytest.approx(predicted)
    assert check.measured_ms == pytest.approx(predicted * 1.1)
    assert check.relative_error == pytest.approx(0.1 / 1.1)


def test_marginal_costs_are_the_per_unit_costs() -> None:
    model = cm.fit(_sweep())
    assert model.marginal() == {k: pytest.approx(TRUE[k]) for k in cm.KNOBS}


# ------------------------------------------------------------------ break-even


def test_break_even_rate() -> None:
    # live 2.0 ms of CPU per request, replica 0.5; a refresh costs 3 CPU-s every 60 s
    be = cm.break_even(live_ms=2.0, replica_ms=0.5, refresh_cpu_s=3.0, ttl_s=60.0)
    # a replica pays 50 ms of CPU/s for refreshing; each request saves 1.5 ms -> 33.3 req/s
    assert be.requests_per_s == pytest.approx(50.0 / 1.5)
    assert be.reason is None


def test_break_even_below_it_live_wins_above_it_a_replica_wins() -> None:
    be = cm.break_even(live_ms=2.0, replica_ms=0.5, refresh_cpu_s=3.0, ttl_s=60.0)
    assert be.requests_per_s is not None
    for rate, winner in ((be.requests_per_s / 2, "live"), (be.requests_per_s * 2, "replica")):
        direct = rate * 2.0
        materialized = rate * 0.5 + 3.0 / 60.0 * 1000
        assert (direct < materialized) == (winner == "live")


def test_break_even_never_when_a_replica_is_not_cheaper_per_request() -> None:
    be = cm.break_even(live_ms=0.5, replica_ms=0.6, refresh_cpu_s=1.0, ttl_s=30.0)
    assert be.requests_per_s is None
    assert be.reason == "a replica request costs no less than a live one"


def test_break_even_at_any_rate_when_the_refresh_is_free() -> None:
    be = cm.break_even(live_ms=2.0, replica_ms=0.5, refresh_cpu_s=0.0, ttl_s=30.0)
    assert be.requests_per_s == 0.0


@pytest.mark.parametrize("ttl", [0.0, -1.0])
def test_break_even_needs_a_ttl(ttl: float) -> None:
    with pytest.raises(ValueError, match="ttl_s must be > 0"):
        cm.break_even(live_ms=2.0, replica_ms=0.5, refresh_cpu_s=1.0, ttl_s=ttl)
