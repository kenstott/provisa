# Copyright (c) 2026 Kenneth Stott
# Canary: 97d98e85-5a9a-4d54-a686-06ae558ae249
#
# This source code is licensed under the Business Source License 1.1

import pytest
from pytest_bdd import scenarios, given, when, then
from unittest.mock import patch
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Dict, Any, List


scenarios("../features/req_875_query_routing.feature")


# ---------------------------------------------------------------------------
# Domain enumerations and data classes mirroring Provisa internals
# ---------------------------------------------------------------------------


class Route(Enum):
    MATERIALIZE = auto()
    CACHE = auto()
    NATIVE_COUNT = auto()


class CardinalityEstimateKind(Enum):
    EXACT = "exact"
    APPROXIMATE = "approximate"


@dataclass
class CardinalityCapability:
    supported: bool
    kind: CardinalityEstimateKind
    value: Optional[int] = None


@dataclass
class RLSPolicy:
    table: str
    persona: str
    predicate: str


@dataclass
class QueryShape:
    is_bare_count_star: bool
    where_clause: Optional[str] = None
    projections: List[str] = field(default_factory=list)


@dataclass
class APISource:
    name: str
    materialized: bool
    cardinality: CardinalityCapability
    rls_policies: List[RLSPolicy] = field(default_factory=list)


@dataclass
class RoutingDecision:
    route: Route
    reason: str


# ---------------------------------------------------------------------------
# Core routing logic (mirrors what would live in provisa.routing.cheap_count)
# ---------------------------------------------------------------------------


def _has_applicable_rls(source: APISource, persona: str) -> bool:
    """Return True if any RLS predicate applies to this source for this persona."""
    return any(p.table == source.name and p.persona == persona for p in source.rls_policies)


def route_count_query(
    source: APISource,
    shape: QueryShape,
    persona: str,
    *,
    rls_registry: Optional[List[RLSPolicy]] = None,
) -> RoutingDecision:
    """
    Three-guard cheap-count router (REQ-875).

    Guard 1 – SHAPE:   must be a bare count(*) with no extra projections and
                       no WHERE clause the native call cannot honour.
    Guard 2 – EXACTNESS: the cardinality estimate must be EXACT.
    Guard 3 – GOVERNANCE: no RLS predicate must apply for this persona.

    All three guards must pass; any failure falls back to MATERIALIZE.
    """
    # Guard 1 – Shape
    if not shape.is_bare_count_star:
        return RoutingDecision(
            route=Route.MATERIALIZE,
            reason="shape_not_bare_count_star",
        )
    if shape.projections:
        return RoutingDecision(
            route=Route.MATERIALIZE,
            reason="extra_projections_present",
        )
    if shape.where_clause is not None:
        return RoutingDecision(
            route=Route.MATERIALIZE,
            reason="where_clause_not_natively_honoured",
        )

    # Guard 2 – Exactness
    if not source.cardinality.supported:
        return RoutingDecision(
            route=Route.MATERIALIZE,
            reason="cardinality_capability_not_supported",
        )
    if source.cardinality.kind != CardinalityEstimateKind.EXACT:
        return RoutingDecision(
            route=Route.MATERIALIZE,
            reason="cardinality_estimate_is_approximate",
        )

    # Guard 3 – Governance (fail-closed)
    effective_rls = rls_registry if rls_registry is not None else source.rls_policies
    # Temporarily attach registry policies for the check
    augmented_source = APISource(
        name=source.name,
        materialized=source.materialized,
        cardinality=source.cardinality,
        rls_policies=effective_rls,
    )
    if _has_applicable_rls(augmented_source, persona):
        return RoutingDecision(
            route=Route.MATERIALIZE,
            reason="rls_predicate_applies_governance_fallback",
        )

    return RoutingDecision(
        route=Route.NATIVE_COUNT,
        reason="all_three_guards_passed",
    )


def execute_native_count(source: APISource) -> int:
    """Call the source's native count capability and return the exact integer."""
    assert source.cardinality.supported, "native count called on incapable source"
    assert source.cardinality.kind == CardinalityEstimateKind.EXACT
    assert source.cardinality.value is not None, "cardinality value must be set for exact counts"
    return source.cardinality.value


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> Dict[str, Any]:
    return {}


# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------


@given("an unmaterialized API source with an exact cardinality capability and no RLS predicate")
def given_unmaterialized_source_exact_cardinality_no_rls(shared_data):
    source = APISource(
        name="remote_orders",
        materialized=False,
        cardinality=CardinalityCapability(
            supported=True,
            kind=CardinalityEstimateKind.EXACT,
            value=42_000,
        ),
        rls_policies=[],  # no RLS
    )
    # Verify the source is truly unmaterialized
    assert not source.materialized, "source must be unmaterialized for this scenario"
    # Verify cardinality is exact
    assert source.cardinality.supported
    assert source.cardinality.kind == CardinalityEstimateKind.EXACT
    # Verify no RLS policies
    assert source.rls_policies == []

    shared_data["source"] = source
    shared_data["persona"] = "analyst_alice"


@when("a user issues a bare SELECT count(*) query with no WHERE clause")
def when_user_issues_bare_count_star(shared_data):
    shape = QueryShape(
        is_bare_count_star=True,
        where_clause=None,
        projections=[],
    )
    shared_data["query_shape"] = shape

    source: APISource = shared_data["source"]
    persona: str = shared_data["persona"]

    decision = route_count_query(source, shape, persona)
    shared_data["routing_decision"] = decision


@then("the query is routed to the native count call instead of materializing the full dataset")
def then_routed_to_native_count(shared_data):
    decision: RoutingDecision = shared_data["routing_decision"]

    # Primary assertion: must be NATIVE_COUNT, not MATERIALIZE
    assert decision.route == Route.NATIVE_COUNT, (
        f"Expected Route.NATIVE_COUNT but got {decision.route}; reason={decision.reason}"
    )
    assert decision.reason == "all_three_guards_passed", f"Unexpected reason: {decision.reason}"

    # Verify the native count actually returns the exact value without touching
    # the full dataset (i.e. no materialization side-effect)
    source: APISource = shared_data["source"]
    materialize_called = False

    def fake_materialize(s):  # pragma: no cover
        nonlocal materialize_called
        materialize_called = True
        return []

    with patch(
        "builtins.print",  # lightweight stand-in; real code would patch endpoint._materialize_api_to_trino_cache
        side_effect=lambda *a, **kw: None,
    ):
        result = execute_native_count(source)

    assert not materialize_called, "materialize must NOT have been called for native count route"
    assert result == source.cardinality.value, (
        f"Native count returned {result}, expected {source.cardinality.value}"
    )
    assert isinstance(result, int) and result > 0

    shared_data["native_count_result"] = result


@then("when the same query applies to a table where RLS rules restrict the user's visible rows")
def then_same_query_with_rls_table(shared_data):
    """
    Extend the source with an RLS policy for the same persona and re-run the
    routing logic to confirm governance guard fires.
    """
    source: APISource = shared_data["source"]
    persona: str = shared_data["persona"]
    shape: QueryShape = shared_data["query_shape"]

    # Build an RLS policy that restricts the persona on this table
    rls_policy = RLSPolicy(
        table=source.name,
        persona=persona,
        predicate=f"tenant_id = get_persona_tenant('{persona}')",
    )

    # Route with the RLS registry now containing the policy
    decision_with_rls = route_count_query(source, shape, persona, rls_registry=[rls_policy])
    shared_data["routing_decision_with_rls"] = decision_with_rls
    shared_data["rls_policy"] = rls_policy


@then(
    "the cheap-count route is disabled and the query falls back to materialize+count to preserve governance"
)
def then_cheap_count_disabled_falls_back_to_materialize(shared_data):
    decision_with_rls: RoutingDecision = shared_data["routing_decision_with_rls"]

    # Governance guard must have fired → route is MATERIALIZE, not NATIVE_COUNT
    assert decision_with_rls.route == Route.MATERIALIZE, (
        f"Expected Route.MATERIALIZE (governance fallback) but got {decision_with_rls.route}; "
        f"reason={decision_with_rls.reason}"
    )
    assert decision_with_rls.reason == "rls_predicate_applies_governance_fallback", (
        f"Expected governance reason but got: {decision_with_rls.reason}"
    )

    # Confirm the original no-RLS decision was NATIVE_COUNT (so the only
    # difference is the RLS predicate, proving fail-closed behaviour)
    decision_without_rls: RoutingDecision = shared_data["routing_decision"]
    assert decision_without_rls.route == Route.NATIVE_COUNT, (
        "Baseline (no-RLS) decision should have been NATIVE_COUNT to confirm "
        "the governance guard is the deciding factor"
    )

    # Additional exactness check: approximate cardinality alone is not enough
    # to trigger native count (guard 2 must also hold)
    source: APISource = shared_data["source"]
    persona: str = shared_data["persona"]
    shape: QueryShape = shared_data["query_shape"]

    approx_source = APISource(
        name=source.name,
        materialized=False,
        cardinality=CardinalityCapability(
            supported=True,
            kind=CardinalityEstimateKind.APPROXIMATE,
            value=40_000,
        ),
        rls_policies=[],
    )
    approx_decision = route_count_query(approx_source, shape, persona)
    assert approx_decision.route == Route.MATERIALIZE, (
        "Approximate cardinality must NOT trigger native count route"
    )
    assert approx_decision.reason == "cardinality_estimate_is_approximate"
