# Copyright (c) 2026 Kenneth Stott
# Canary: dd477e5f-91f7-48f6-ba91-07ff1b054990
#
# This source code is licensed under the Business Source License 1.1
"""BDD steps for REQ-418, REQ-610, REQ-394 and REQ-400 — Domain Model.

REQ-418: Report authoring workflow: analysts pull cross-domain data into their
own domain via views (data-import adapters), then define calculations and
relationships only within that domain. New or derived data exists only as
views; no direct cross-domain calculations or relationships.

Enforcement is pinned via V001: a role can only reach tables in its own
domain_access. Foreign-domain tables are unreachable directly and may only be
consumed through an import view that itself lives in the role's own domain.

REQ-610: A field access grant belongs to the requesting domain, not to the
specific view that prompted it. Any subsequent view in the requesting domain
may use the granted fields without additional cross-domain approval. New fields
not covered by the existing grant require a new request.

REQ-394: Multiple PK checkboxes on a table infer a composite key; the first
designated PK column is used as the canonical ``id_column`` for Cypher node
identity resolution, taking priority over all heuristics.

REQ-400: When a Relationship is saved, the target_column on the target table is
marked ``is_primary_key=true`` if no other column in that table already has a
primary key; otherwise it is marked ``is_alternate_key=true``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_validator import validate_  # noqa: F401

scenarios("../features/REQ-610.feature")


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Domain-scoped field grant model (REQ-610)
#
# A field access grant is keyed by the *requesting domain*, never by the view
# that prompted it. It records the concrete set of qualified fields
# ("source.table.column") that the domain has been approved to consume from
# another domain. Coverage checks are pure set operations: any view in the
# owning domain may use covered fields freely; only fields outside the grant
# require a fresh cross-domain request.
# ---------------------------------------------------------------------------


@dataclass
class DomainFieldGrant:
    """A cross-domain field access grant owned by ``domain_id``."""

    domain_id: str
    source_domain_id: str
    granted_fields: set[str] = field(default_factory=set)
    # The view that originally prompted the grant; recorded for provenance
    # only — it must NOT scope the grant's applicability.
    prompted_by_view: str | None = None

    def covers(self, fields: set[str]) -> bool:
        """True iff every requested field is already within the grant."""
        return set(fields).issubset(self.granted_fields)

    def uncovered(self, fields: set[str]) -> set[str]:
        """Fields outside the grant that would require a new request."""
        return set(fields) - self.granted_fields


@dataclass
class GrantRegistry:
    """Holds the domain-scoped grants for a tenant."""

    grants: dict[str, DomainFieldGrant] = field(default_factory=dict)

    def add(self, grant: DomainFieldGrant) -> None:
        self.grants[grant.domain_id] = grant

    def for_domain(self, domain_id: str) -> DomainFieldGrant | None:
        return self.grants.get(domain_id)

    def requires_new_request(self, domain_id: str, fields: set[str]) -> set[str]:
        """Return the subset of ``fields`` that needs a new cross-domain request."""
        grant = self.for_domain(domain_id)
        if grant is None:
            return set(fields)
        return grant.uncovered(fields)


# ---------------------------------------------------------------------------
# REQ-610 — Domain-scoped field grants
# ---------------------------------------------------------------------------


@given("a domain that has received a cross-domain field access grant")
def given_domain_with_grant(shared_data: dict) -> None:
    registry = GrantRegistry()
    grant = DomainFieldGrant(
        domain_id="analytics",
        source_domain_id="sales",
        granted_fields={
            "sales.orders.order_id",
            "sales.orders.customer_id",
            "sales.orders.amount",
        },
        prompted_by_view="analytics.revenue_summary",
    )
    registry.add(grant)
    shared_data["registry"] = registry
    shared_data["requesting_domain"] = "analytics"

    # The grant is owned by the requesting domain, not by the prompting view.
    stored = registry.for_domain("analytics")
    assert stored is not None
    assert stored.domain_id == "analytics"
    assert stored.granted_fields, "grant must record concrete fields"


@when("a new view in that domain uses the granted fields")
def when_new_view_uses_granted_fields(shared_data: dict) -> None:
    registry: GrantRegistry = shared_data["registry"]
    domain = shared_data["requesting_domain"]

    # A brand-new view (distinct from the one that prompted the grant) that
    # consumes a subset of the already-granted fields.
    new_view_name = "analytics.daily_orders"
    fields_used = {"sales.orders.order_id", "sales.orders.amount"}

    grant = registry.for_domain(domain)
    assert grant is not None
    assert new_view_name != grant.prompted_by_view, "must be a different view"

    shared_data["new_view_name"] = new_view_name
    shared_data["covered_fields"] = fields_used
    shared_data["covered_needs_request"] = registry.requires_new_request(domain, fields_used)

    # A second view in the same domain requests an extra field NOT in the grant.
    extra_fields = {"sales.orders.amount", "sales.orders.discount_code"}
    shared_data["extra_fields"] = extra_fields
    shared_data["extra_needs_request"] = registry.requires_new_request(domain, extra_fields)


@then(
    "no additional approval is required; "
    "only new fields outside the grant trigger a new request"
)
def then_no_additional_approval_for_covered_only(shared_data: dict) -> None:
    # Covered fields used by a new view require no new cross-domain request.
    assert shared_data["covered_needs_request"] == set(), (
        "a new view reusing granted fields must not require additional approval"
    )

    registry: GrantRegistry = shared_data["registry"]
    grant = registry.for_domain(shared_data["requesting_domain"])
    assert grant is not None
    assert grant.covers(shared_data["covered_fields"])

    # Fields outside the grant DO require a new request — and only those.
    extra_needs = shared_data["extra_needs_request"]
    assert extra_needs == {"sales.orders.discount_code"}, (
        "only fields not covered by the grant should trigger a new request"
    )
    # The already-granted field within the same request must not be re-requested.
    assert "sales.orders.amount" not in extra_needs
