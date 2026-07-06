# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e9d34-2a7c-4f80-9e63-8c0a1f4d7b25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-905: cost-based VIRTUAL/SCAN -> MATERIALIZED promotion gate.

Pure logic — no I/O. Exercises the two-condition gate (unmet reducing pushdown +
known-large estimate) and the fail-open UNKNOWN rule.
"""

from __future__ import annotations

import pytest

from provisa.federation.cardinality import UNKNOWN_ESTIMATE, CardinalityMethod, Estimate
from provisa.federation.connector import Capability
from provisa.federation.promote import (
    DEFAULT_PROMOTE_ROW_THRESHOLD,
    PushdownDemand,
    should_promote,
    unmet_reducing_pushdown,
)


def _est(value: int | None) -> Estimate:
    if value is None:
        return UNKNOWN_ESTIMATE
    return Estimate(value=value, exact=False, method=CardinalityMethod.NATIVE_STAT)


# ---- unmet_reducing_pushdown -------------------------------------------------


def test_predicate_gap_is_unmet_when_connector_cannot_push_predicate():
    cap = Capability(predicate_pushdown=False)
    assert unmet_reducing_pushdown(cap, PushdownDemand(predicate=True)) is True


def test_aggregate_gap_is_unmet_when_connector_cannot_push_aggregate():
    cap = Capability(predicate_pushdown=True, aggregate_pushdown=False)
    assert unmet_reducing_pushdown(cap, PushdownDemand(aggregate=True)) is True


def test_no_gap_when_connector_pushes_the_demanded_reducers():
    cap = Capability(predicate_pushdown=True, aggregate_pushdown=True)
    demand = PushdownDemand(predicate=True, aggregate=True)
    assert unmet_reducing_pushdown(cap, demand) is False


def test_join_only_gap_is_not_reducing():
    # join may reduce or explode — it never on its own signals a wasteful scan.
    cap = Capability(predicate_pushdown=True, aggregate_pushdown=True, join_pushdown=False)
    assert unmet_reducing_pushdown(cap, PushdownDemand(join=True)) is False


# ---- should_promote: both conditions required --------------------------------


def test_promotes_when_gap_and_estimate_at_threshold():
    cap = Capability(predicate_pushdown=False)
    demand = PushdownDemand(predicate=True)
    assert should_promote(cap, demand, _est(DEFAULT_PROMOTE_ROW_THRESHOLD)) is True


def test_promotes_when_gap_and_estimate_above_threshold():
    cap = Capability(predicate_pushdown=False)
    demand = PushdownDemand(predicate=True)
    assert should_promote(cap, demand, _est(DEFAULT_PROMOTE_ROW_THRESHOLD + 1)) is True


def test_no_promote_below_threshold():
    cap = Capability(predicate_pushdown=False)
    demand = PushdownDemand(predicate=True)
    assert should_promote(cap, demand, _est(DEFAULT_PROMOTE_ROW_THRESHOLD - 1)) is False


def test_no_promote_without_capability_gap_even_when_huge():
    # Full pushdown -> live read is efficient regardless of size.
    cap = Capability(predicate_pushdown=True, aggregate_pushdown=True)
    demand = PushdownDemand(predicate=True, aggregate=True)
    assert should_promote(cap, demand, _est(10 * DEFAULT_PROMOTE_ROW_THRESHOLD)) is False


def test_unknown_cardinality_never_promotes_fail_open():
    cap = Capability(predicate_pushdown=False)
    demand = PushdownDemand(predicate=True)
    assert should_promote(cap, demand, UNKNOWN_ESTIMATE) is False


def test_threshold_is_one_million_by_default():
    assert DEFAULT_PROMOTE_ROW_THRESHOLD == 1_000_000


def test_custom_threshold_override():
    cap = Capability(predicate_pushdown=False)
    demand = PushdownDemand(predicate=True)
    assert should_promote(cap, demand, _est(500), threshold_rows=100) is True
    assert should_promote(cap, demand, _est(50), threshold_rows=100) is False


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
