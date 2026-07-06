# Copyright (c) 2026 Kenneth Stott
# Canary: 70eefddf-af6f-4256-9bb4-1fd528773ee2
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-905: cost-based VIRTUAL/SCAN -> MATERIALIZED promotion."""

from __future__ import annotations

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.federation.cardinality import UNKNOWN_ESTIMATE, CardinalityMethod, Estimate
from provisa.federation.connector import Capability
from provisa.federation.promote import (
    DEFAULT_PROMOTE_ROW_THRESHOLD,
    PushdownDemand,
    should_promote,
    unmet_reducing_pushdown,
)

# ---------------------------------------------------------------------------
# Scenario registration
# ---------------------------------------------------------------------------

@scenario(
    "../features/REQ-905.feature",
    "REQ-905 default behaviour",
)
def test_req_905_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_estimate(value: int | None) -> Estimate:
    if value is None:
        return UNKNOWN_ESTIMATE
    return Estimate(value=value, exact=False, method=CardinalityMethod.NATIVE_STAT)


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------

@given(
    "a federated query with row-reducing operators (filter/aggregate) over a VIRTUAL/SCAN source",
    target_fixture="shared_data",
)
def given_federated_query_with_row_reducing_operators():
    """Set up the query context: row-reducing demand (predicate + aggregate) over a VIRTUAL source."""
    data: dict = {}
    # The query applies both a predicate filter and an aggregate to the source rows.
    data["demand"] = PushdownDemand(predicate=True, aggregate=True)
    # The source is reachable (VIRTUAL/SCAN), so a connector exists.
    # By default, the connector cannot push down either reducing operator.
    data["cap"] = Capability(
        predicate_pushdown=False,
        aggregate_pushdown=False,
        join_pushdown=True,
    )
    return data


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------

@when("the source connector cannot push down these operators", target_fixture="shared_data")
def when_connector_cannot_push_down(shared_data):
    """Assert that there is an unmet reducing pushdown gap and record the result."""
    cap: Capability = shared_data["cap"]
    demand: PushdownDemand = shared_data["demand"]

    gap = unmet_reducing_pushdown(cap, demand)
    assert gap is True, (
        f"Expected an unmet reducing pushdown gap but got gap={gap}. "
        f"cap={cap}, demand={demand}"
    )
    shared_data["has_pushdown_gap"] = gap
    return shared_data


@when(
    parsers.parse("the estimated row count >= {threshold:d}"),
    target_fixture="shared_data",
)
def when_estimated_row_count_at_or_above_threshold(shared_data, threshold):
    """Set the cardinality estimate at the specified threshold."""
    shared_data["estimate"] = _make_estimate(threshold)
    return shared_data


# pytest-bdd matches the literal string from the feature; provide a fallback for the
# default 1,000,000 threshold phrasing as written in the feature file.
@when("the estimated row count >= 1,000,000", target_fixture="shared_data")
def when_estimated_row_count_default_threshold(shared_data):
    """Set the cardinality estimate at the default 1,000,000-row threshold."""
    shared_data["estimate"] = _make_estimate(DEFAULT_PROMOTE_ROW_THRESHOLD)
    return shared_data


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------

@then("the source is promoted to MATERIALIZED")
def then_source_is_promoted_to_materialized(shared_data):
    """should_promote must return True given the gap and known-large estimate."""
    cap: Capability = shared_data["cap"]
    demand: PushdownDemand = shared_data["demand"]
    estimate: Estimate = shared_data["estimate"]

    result = should_promote(cap, demand, estimate)
    assert result is True, (
        f"Expected should_promote=True but got {result}. "
        f"cap={cap}, demand={demand}, estimate={estimate}"
    )
    shared_data["promoted"] = result


@then("the engine store handles the reduction, amortized across the TTL")
def then_engine_store_handles_reduction_amortized(shared_data):
    """When promoted, reduction is delegated to the engine store.

    Verify that requires_residency(MATERIALIZED) is True, confirming the plan stage
    will emit a residency prep step (load/refresh), which is the mechanism by which
    amortization across the TTL is realised.
    """
    from provisa.federation.strategy import Strategy, requires_residency

    assert shared_data.get("promoted") is True, (
        "Source must have been promoted before checking engine-store handling."
    )
    # MATERIALIZED strategy must demand a residency prep step - that is the amortization.
    assert requires_residency(Strategy.MATERIALIZED) is True, (
        "MATERIALIZED strategy must require a residency prep (load/refresh) step."
    )


@then("UNKNOWN cardinality does not trigger promotion.")
def then_unknown_cardinality_does_not_promote(shared_data):
    """Fail-open rule: an UNKNOWN estimate must never result in promotion."""
    cap: Capability = shared_data["cap"]
    demand: PushdownDemand = shared_data["demand"]

    # The capability gap exists, but the estimate is UNKNOWN.
    result_unknown = should_promote(cap, demand, UNKNOWN_ESTIMATE)
    assert result_unknown is False, (
        f"UNKNOWN cardinality must never trigger promotion (fail-open), but got {result_unknown}."
    )

    # Also verify that a join-only gap (non row-reducing) does not promote, even with a
    # known-large estimate, as stated in the requirement.
    join_only_cap = Capability(predicate_pushdown=True, aggregate_pushdown=True, join_pushdown=False)
    join_only_demand = PushdownDemand(join=True)
    large_estimate = _make_estimate(DEFAULT_PROMOTE_ROW_THRESHOLD * 10)
    result_join_only = should_promote(join_only_cap, join_only_demand, large_estimate)
    assert result_join_only is False, (
        f"Join-only gap must not trigger promotion, but got {result_join_only}."
    )
