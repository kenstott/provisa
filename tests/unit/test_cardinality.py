# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2d9a71-4b08-4e75-8f12-3c7a0d4f9c08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-673 cardinality estimate + REQ-875 cheap-count route guards."""

from __future__ import annotations

from provisa.federation.cardinality import (
    CardinalityMethod,
    Estimate,
    can_route_cheap_count,
    resolve_cardinality,
)

_EXACT = Estimate(value=100, exact=True, method=CardinalityMethod.NATIVE_COUNT)
_APPROX = Estimate(value=95, exact=False, method=CardinalityMethod.NATIVE_STAT)


# ---- REQ-673 resolution order -----------------------------------------------


def test_native_stat_wins():
    assert resolve_cardinality(native_stat=_APPROX) is _APPROX


def test_exact_count_when_cheap():
    est = resolve_cardinality(exact_count_cheap=True, exact_count=42)
    assert est.value == 42 and est.exact is True
    assert est.method is CardinalityMethod.COUNT_STAR


def test_unknown_when_expensive_and_no_native():
    est = resolve_cardinality(exact_count_cheap=False)
    assert est.value is None and est.exact is False
    assert est.method is CardinalityMethod.UNKNOWN


def test_native_stat_preferred_over_cheap_count():
    est = resolve_cardinality(native_stat=_APPROX, exact_count_cheap=True, exact_count=42)
    assert est is _APPROX  # order (1) native stat wins over (2) count


# ---- REQ-875 cheap-count route (three guards, fail-closed) -------------------


def test_route_fires_for_exact_bare_count_without_rls():
    assert (
        can_route_cheap_count(is_bare_count_star=True, estimate=_EXACT, rls_applies=False) is True
    )


def test_route_disabled_for_approximate_estimate():
    # EXACTNESS guard: an approximate statistic is never served as the count(*) answer.
    assert (
        can_route_cheap_count(is_bare_count_star=True, estimate=_APPROX, rls_applies=False) is False
    )


def test_route_disabled_when_rls_applies():
    # GOVERNANCE guard: a native total would over-count the visible subset → fall back.
    assert (
        can_route_cheap_count(is_bare_count_star=True, estimate=_EXACT, rls_applies=True) is False
    )


def test_route_disabled_for_non_bare_count_shape():
    # SHAPE guard: a filtered/projected count the source can't honor → materialize.
    assert (
        can_route_cheap_count(is_bare_count_star=False, estimate=_EXACT, rls_applies=False) is False
    )


def test_route_disabled_when_estimate_has_no_value():
    unknown = Estimate(value=None, exact=True, method=CardinalityMethod.NATIVE_COUNT)
    assert (
        can_route_cheap_count(is_bare_count_star=True, estimate=unknown, rls_applies=False) is False
    )
