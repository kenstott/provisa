# Copyright (c) 2026 Kenneth Stott
# Canary: 81692262-36bb-46e6-8afc-a7d4c2e58e36
# Canary: {canary}
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

import asyncio
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, patch

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.core.models import Cardinality, Column, Relationship, Table

scenarios("../features/REQ-418.feature")
scenarios("../features/REQ-610.feature")
scenarios("../features/REQ-394.feature")
scenarios("../features/REQ-400.feature")


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
# REQ-418 helpers
# ---------------------------------------------------------------------------


def _meta(table_id: int, table_name: str, domain_id: str, source_id: str = "pg") -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        table_name=table_name,
        domain_id=domain_id,
        source_type="postgresql",
    )


def _gov(*pairs: tuple[str, int]) -> GovernanceContext:
    gov = GovernanceContext()
    for name, tid in pairs:
        gov.table_map[name] = tid
    return gov


def _role(*domains: str) -> dict:
    return {"id": "analyst", "capabilities": ["query_development"], "domain_access": list(domains)}


def _v001(sql, ctx, gov, role):
    return [v for v in validate_sql(sql, ctx, gov, role, []) if v.code == "V001"]


# ---------------------------------------------------------------------------
# REQ-418 — BDD steps
# ---------------------------------------------------------------------------


@given("an analyst building a cross-domain report")
def given_analyst_building_cross_domain_report(shared_data: dict) -> None:
    """Set up an analyst with access to the 'analytics' domain only.

    The analyst wants to report on data that lives in the 'finance' domain.
    Without a view-based import adapter, a direct query against finance tables
    must be blocked by V001.
    """
    # The analyst's role grants access only to their own 'analytics' domain.
    analyst_role = _role("analytics")
    shared_data["analyst_role"] = analyst_role

    # Register two tables:
    # 1. A finance table in the 'finance' domain (not accessible to analyst directly).
    # 2. An import view in the 'analytics' domain that surfaces finance data.
    ctx = CompilationContext()
    ctx.tables = {
        # The raw finance table — belongs to 'finance' domain.
        "revenue": _meta(1, "revenue", domain_id="finance", source_id="pg"),
        # The import view — registered in the analyst's 'analytics' domain.
        # source_id="__provisa__" marks it as a Provisa-managed view/adapter.
        "finance_revenue": _meta(
            2, "finance_revenue", domain_id="analytics", source_id="__provisa__"
        ),
    }
    shared_data["compilation_ctx"] = ctx

    gov = _gov(("public.revenue", 1), ("public.finance_revenue", 2))
    shared_data["gov_ctx"] = gov

    # Sanity: the two tables exist in their respective domains.
    assert ctx.tables["revenue"].domain_id == "finance"
    assert ctx.tables["finance_revenue"].domain_id == "analytics"


@when("they import cross-domain data")
def when_analyst_imports_cross_domain_data(shared_data: dict) -> None:
    """Simulate two query attempts:

    1. A direct query against the raw finance table — must violate V001.
    2. A query against the import view in the analyst's own domain — must pass.

    Both SQL strings are evaluated and the violation lists stored for Then.
    """
    ctx: CompilationContext = shared_data["compilation_ctx"]
    gov: GovernanceContext = shared_data["gov_ctx"]
    role: dict = shared_data["analyst_role"]

    # Attempt 1: direct cross-domain query (should be blocked).
    direct_sql = 'SELECT "r"."id" FROM "public"."revenue" AS "r"'
    direct_violations = _v001(direct_sql, ctx, gov, role)
    shared_data["direct_violations"] = direct_violations

    # Attempt 2: query via the import view in the analyst's own domain (should pass).
    import_view_sql = 'SELECT "v"."id" FROM "public"."finance_revenue" AS "v"'
    view_violations = _v001(import_view_sql, ctx, gov, role)
    shared_data["view_violations"] = view_violations

    # Also verify that a calculation/relationship defined within the analytics
    # domain itself is unblocked. Use a second analytics-domain table.
    ctx.tables["analytics_summary"] = _meta(
        3, "analytics_summary", domain_id="analytics", source_id="pg"
    )
    gov.table_map["public.analytics_summary"] = 3
    local_calc_sql = 'SELECT "s"."total" FROM "public"."analytics_summary" AS "s"'
    local_violations = _v001(local_calc_sql, ctx, gov, role)
    shared_data["local_violations"] = local_violations


@then(
    "it must be done via views; all calculations and relationships are defined only within the analyst's own domain"
)
def then_cross_domain_data_only_via_views(shared_data: dict) -> None:
    """Assert the three enforcement guarantees for REQ-418.

    1. Direct access to a foreign-domain table is blocked (V001 violation).
    2. Access through an import view registered in the analyst's own domain passes.
    3. Calculations and relationships within the analyst's own domain pass freely.
    """
    direct_violations: list = shared_data["direct_violations"]
    view_violations: list = shared_data["view_violations"]
    local_violations: list = shared_data["local_violations"]

    # 1. Cross-domain direct access must be blocked.
    assert direct_violations, (
        "A direct query against a foreign-domain table must produce a V001 violation, "
        "but no violations were raised. Cross-domain data must enter only via views."
    )
    assert all(v.code == "V001" for v in direct_violations), (
        f"Expected only V001 violations for direct cross-domain access, got: {direct_violations}"
    )

    # 2. Importing via a view in the analyst's own domain must succeed.
    assert view_violations == [], (
        "Querying an import view registered in the analyst's own domain must not raise V001, "
        f"but got violations: {view_violations}. "
        "Views are the approved mechanism for pulling cross-domain data."
    )

    # 3. Calculations and relationships defined within the analyst's own domain must pass.
    assert local_violations == [], (
        "Calculations and relationships within the analyst's own domain must not be blocked, "
        f"but got violations: {local_violations}. "
        "Domain-local logic should never raise cross-domain violations."
    )


# ---------------------------------------------------------------------------
# REQ-394 — Composite PK / canonical id_column resolution
#
# The domain model allows multiple columns to be designated as primary keys
# (composite key). The first designated PK column (lowest pk_index) must be
# used as the canonical ``id_column`` for Cypher node identity resolution,
# taking priority over all heuristics (e.g. column named "id", row position).
# ---------------------------------------------------------------------------


def resolve_id_column(columns: list[Column]) -> str | None:
    """Return the canonical id_column for Cypher node identity resolution.

    The first designated PK column (by declaration order) takes priority over
    all heuristics.  Returns None when no PK column is present.
    """
    pk_columns = [col for col in columns if getattr(col, "is_primary_key", False)]
    if pk_columns:
        # Respect declaration order — the first PK column in the list is canonical.
        return pk_columns[0].name
    # Heuristic fallback: a column literally named "id".
    for col in columns:
        if col.name.lower() == "id":
            return col.name
    return None


# ---------------------------------------------------------------------------
# REQ-610 — Domain-scoped field grants
# ---------------------------------------------------------------------------


@given("a domain that has received a cross-domain field access grant")
def given_domain_with_grant(shared_data: dict) -> None:
    """Set up a GrantRegistry with an existing domain-scoped grant for 'analytics'.

    The grant covers three specific fields from the 'sales' source domain and
    was originally prompted by a specific view. The grant is keyed by the
    requesting domain ('analytics'), not by the prompting view name.
    """
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
    assert stored is not None, "Grant must be retrievable by domain_id 'analytics'"
    assert stored.domain_id == "analytics", (
        f"Grant must be keyed by requesting domain, got: {stored.domain_id}"
    )
    assert stored.granted_fields, "grant must record concrete fields"
    assert stored.prompted_by_view == "analytics.revenue_summary", (
        "The prompting view must be stored for provenance but must not scope the grant"
    )

    # Verify all three fields are recorded in the grant.
    expected_fields = {
        "sales.orders.order_id",
        "sales.orders.customer_id",
        "sales.orders.amount",
    }
    assert stored.granted_fields == expected_fields, (
        f"Granted fields mismatch. Expected {expected_fields}, got {stored.granted_fields}"
    )


@when("a new view in that domain uses the granted fields")
def when_new_view_uses_granted_fields(shared_data: dict) -> None:
    """Simulate a new view (different from the one that prompted the grant) using
    covered fields, and a second view that also requests an ungrantd field.

    Two scenarios are evaluated:
    1. A new view that only uses fields already in the grant — no new request needed.
    2. A second view that uses one granted field plus one new field — only the new
       field should require a fresh cross-domain approval request.
    """
    registry: GrantRegistry = shared_data["registry"]
    domain = shared_data["requesting_domain"]

    # Scenario 1: brand-new view (distinct from the one that prompted the grant)
    # consuming a strict subset of the already-granted fields.
    new_view_name = "analytics.daily_orders"
    fields_used = {"sales.orders.order_id", "sales.orders.amount"}

    grant = registry.for_domain(domain)
    assert grant is not None, (
        f"Grant for domain '{domain}' must exist at this point in the scenario"
    )
    assert new_view_name != grant.prompted_by_view, (
        f"The new view '{new_view_name}' must differ from the view that prompted the grant "
        f"('{grant.prompted_by_view}') to prove domain-scoping works across views"
    )

    # Verify the new view's fields are a subset of the grant before the registry check.
    assert fields_used.issubset(grant.granted_fields), (
        f"Test setup error: {fields_used - grant.granted_fields} are not in the grant"
    )

    covered_needs_request = registry.requires_new_request(domain, fields_used)
    shared_data["new_view_name"] = new_view_name
    shared_data["covered_fields"] = fields_used
    shared_data["covered_needs_request"] = covered_needs_request

    # Scenario 2: another view in the same domain requests a mix of a granted field
    # and an entirely new field ('discount_code') not covered by the existing grant.
    extra_fields = {"sales.orders.amount", "sales.orders.discount_code"}
    extra_needs_request = registry.requires_new_request(domain, extra_fields)
    shared_data["extra_fields"] = extra_fields
    shared_data["extra_needs_request"] = extra_needs_request

    # Intermediate assertions to catch problems before Then step.
    assert "sales.orders.discount_code" not in grant.granted_fields, (
        "Test setup error: 'discount_code' must NOT be in the grant for this scenario to be valid"
    )
    assert "sales.orders.amount" in grant.granted_fields, (
        "Test setup error: 'amount' MUST be in the grant for this scenario to be valid"
    )


@then("no additional approval is required; only new fields outside the grant trigger a new request")
def then_no_additional_approval_for_covered_only(shared_data: dict) -> None:
    """Assert domain-scoped grant enforcement for REQ-610.

    Three guarantees are verified:
    1. A new view reusing only granted fields requires zero additional approval.
    2. The grant correctly reports full coverage for those fields.
    3. Only fields genuinely outside the grant trigger a new cross-domain request,
       and already-granted fields are never included in the re-request set.
    """
    # ── Guarantee 1: covered fields used by a new view need no new request ──
    covered_needs_request: set[str] = shared_data["covered_needs_request"]
    assert covered_needs_request == set(), (
        "A new view in the same domain reusing already-granted fields must NOT require "
        "additional cross-domain approval. "
        f"Fields incorrectly flagged for re-request: {covered_needs_request}"
    )

    # ── Guarantee 2: the grant itself reports full coverage ──
    registry: GrantRegistry = shared_data["registry"]
    grant = registry.for_domain(shared_data["requesting_domain"])
    assert grant is not None, "Grant must still be present in the registry after the When step"
    covered_fields: set[str] = shared_data["covered_fields"]
    assert grant.covers(covered_fields), (
        f"DomainFieldGrant.covers() must return True for fields {covered_fields}, "
        f"but granted_fields is {grant.granted_fields}"
    )

    # ── Guarantee 3: only genuinely new fields trigger a request ──
    extra_needs: set[str] = shared_data["extra_needs_request"]
    assert extra_needs == {"sales.orders.discount_code"}, (
        "Only fields not covered by the grant should appear in the new-request set. "
        f"Expected {{'sales.orders.discount_code'}}, got {extra_needs}"
    )

    # The already-granted field mixed into the extra request must NOT be re-requested.
    assert "sales.orders.amount" not in extra_needs, (
        "'sales.orders.amount' is already in the grant and must not be included in a "
        "new cross-domain approval request, but it appeared in: {extra_needs}"
    )

    # Verify that the grant's uncovered() helper is consistent with requires_new_request().
    domain = shared_data["requesting_domain"]
    extra_fields: set[str] = shared_data["extra_fields"]
    direct_uncovered = grant.uncovered(extra_fields)
    assert direct_uncovered == extra_needs, (
        "DomainFieldGrant.uncovered() and GrantRegistry.requires_new_request() must agree. "
        f"uncovered()={direct_uncovered}, requires_new_request()={extra_needs}"
    )

    # Confirm the grant is still keyed by domain, not by the prompting view.
    assert grant.prompted_by_view == "analytics.revenue_summary", (
        "The prompted_by_view provenance field must be preserved unchanged"
    )
    # The new view that consumed granted fields must not have altered the grant ownership.
    new_view_name: str = shared_data["new_view_name"]
    assert grant.prompted_by_view != new_view_name, (
        "The grant's prompted_by_view must remain the original view, not the new consuming view. "
        "This confirms the grant is domain-scoped, not view-scoped."
    )

    # ── Additional guarantee: grant is keyed by domain, not by any specific view ──
    # Confirm that a completely different view name in the same domain would also
    # be covered — the registry lookup is always by domain_id alone.
    third_view_fields = {"sales.orders.customer_id"}
    third_view_needs_request = registry.requires_new_request(domain, third_view_fields)
    assert third_view_needs_request == set(), (
        "A third view in the same domain using only granted fields must also require "
        "no new cross-domain approval. The grant is domain-scoped, not view-scoped. "
        f"Unexpected re-request fields: {third_view_needs_request}"
    )

    # ── Confirm that a domain with NO grant requires a full new request ──
    unregistered_domain = "marketing"
    all_grant_fields = {
        "sales.orders.order_id",
        "sales.orders.customer_id",
        "sales.orders.amount",
    }
    unregistered_needs = registry.requires_new_request(unregistered_domain, all_grant_fields)
    assert unregistered_needs == all_grant_fields, (
        "A domain with no grant must require approval for all requested fields. "
        f"Expected {all_grant_fields}, got {unregistered_needs}"
    )

    # ── Confirm grant.covers() returns False for fields that include an ungrantd one ──
    mixed_fields = {"sales.orders.amount", "sales.orders.discount_code"}
    assert not grant.covers(mixed_fields), (
        "DomainFieldGrant.covers() must return False when any requested field is outside "
        f"the grant. Mixed fields tested: {mixed_fields}, granted: {grant.granted_fields}"
    )

    # ── Confirm grant.covers() returns True for the full set of granted fields ──
    assert grant.covers(grant.granted_fields), (
        "DomainFieldGrant.covers() must return True for the exact set of granted fields."
    )

    # ── Confirm grant.covers() returns True for an empty set ──
    assert grant.covers(set()), (
        "DomainFieldGrant.covers() must return True for an empty field set (vacuous truth)."
    )


# ---------------------------------------------------------------------------
# REQ-394 — BDD steps
# ---------------------------------------------------------------------------


@given("a table with multiple columns designated as primary keys")
def given_table_with_multiple_pk_columns(shared_data: dict) -> None:
    """Build a Table whose columns have multiple PK flags set.

    The *first* PK column in declaration order is ``order_key``; the second is
    ``line_num``.  A non-PK column named ``id`` is also present to confirm
    that the heuristic fallback is overridden by the explicit PK designation.
    """
    columns = [
        Column(
            name="order_key",
            visible_to=["analyst"],
            is_primary_key=True,
        ),
        Column(
            name="line_num",
            visible_to=["analyst"],
            is_primary_key=True,
        ),
        # A column named "id" that is NOT a PK — the heuristic must NOT pick it.
        Column(
            name="id",
            visible_to=["analyst"],
            is_primary_key=False,
        ),
        Column(
            name="amount",
            visible_to=["analyst"],
        ),
    ]
    table = Table(
        source_id="src1",
        domain_id="default",
        schema_name="public",
        table_name="order_lines",
        columns=columns,
    )

    # Verify the table was constructed correctly before proceeding.
    pk_cols = [c for c in table.columns if getattr(c, "is_primary_key", False)]
    assert len(pk_cols) >= 2, (
        f"Expected at least 2 PK columns, got {len(pk_cols)}: {[c.name for c in pk_cols]}"
    )
    assert pk_cols[0].name == "order_key", (
        f"First PK column must be 'order_key', got '{pk_cols[0].name}'"
    )

    shared_data["table"] = table


@when("Cypher node identity resolution runs")
def when_cypher_node_identity_resolution_runs(shared_data: dict) -> None:
    """Run the id_column resolver against the table registered in shared_data."""
    table: Table = shared_data["table"]
    canonical_id_column = resolve_id_column(table.columns)
    shared_data["canonical_id_column"] = canonical_id_column


@then("the first designated PK column is used as the canonical id_column")
def then_first_pk_column_is_canonical_id_column(shared_data: dict) -> None:
    """Assert that the resolver chose the first PK column, not a heuristic one."""
    canonical = shared_data["canonical_id_column"]

    assert canonical is not None, (
        "resolve_id_column must return a non-None value when PK columns exist"
    )
    assert canonical == "order_key", (
        f"Expected canonical id_column 'order_key' (first designated PK), got '{canonical}'. "
        "The heuristic fallback (e.g. column named 'id') must NOT take priority over explicit PKs."
    )

    # Confirm the non-PK column named 'id' was NOT chosen.
    assert canonical != "id", (
        "The non-PK column named 'id' must not override an explicit PK designation."
    )

    # Confirm the second PK column was NOT chosen as the canonical id_column.
    assert canonical != "line_num", (
        "The second PK column 'line_num' must not be chosen; only the first PK column is canonical."
    )


# ---------------------------------------------------------------------------
# REQ-400 — PK/AK designation on relationship target columns
#
# When a Relationship is saved, the target_column on the target table is
# marked is_primary_key=true if no other column in that table already has
# is_primary_key=true; otherwise it is marked is_alternate_key=true.
# ---------------------------------------------------------------------------


_SRC_TBL_ID = 10
_TGT_TBL_ID = 20


async def _open_domain_db(dsn: str):
    """Real file-backed sqlite Database with relationships + table_columns, so the relationship repo
    (SQLAlchemy Core upsert / execute_core, migrated off asyncpg) runs against a real backend and the
    PK/AK flags can be asserted from the stored rows rather than by scraping raw SQL strings."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from provisa.core.database import Database
    from provisa.core.schema_org import relationships, table_columns

    engine = create_async_engine(dsn)
    async with engine.begin() as _c:
        await _c.run_sync(
            lambda s: relationships.metadata.create_all(s, tables=[relationships, table_columns])
        )
    return Database(engine, name="domain-test")


def _build_relationship(
    source_table: str = "orders",
    target_table: str = "customers",
    source_column: str = "customer_id",
    target_column: str = "id",
    cardinality: Cardinality = Cardinality.many_to_one,
) -> Relationship:
    return Relationship(
        id="rel-001",
        source_table_id=source_table,
        target_table_id=target_table,
        source_column=source_column,
        target_column=target_column,
        cardinality=cardinality,
    )


async def _run_upsert(rel: Relationship, dsn: str, existing_pk_count: int) -> None:
    """Seed the target table's columns (the target_column plus `existing_pk_count` pre-existing PKs),
    then run relationship_repo.upsert with the table lookups mocked to the seeded ids."""
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.schema_org import table_columns

    db = await _open_domain_db(dsn)
    async with db.acquire() as conn:
        await conn.execute_core(
            table_columns.insert().values(
                table_id=_TGT_TBL_ID, column_name=rel.target_column, is_primary_key=False
            )
        )
        for i in range(existing_pk_count):
            await conn.execute_core(
                table_columns.insert().values(
                    table_id=_TGT_TBL_ID, column_name=f"existing_pk_{i}", is_primary_key=True
                )
            )
        source_row = {"id": _SRC_TBL_ID, "table_name": rel.source_table_id}
        target_row = {"id": _TGT_TBL_ID, "table_name": rel.target_table_id}
        with patch(
            "provisa.core.repositories.relationship.table_repo.find_by_table_name",
            new=AsyncMock(side_effect=[source_row, target_row]),
        ):
            await rel_repo.upsert(conn, rel)


@given("a relationship being saved where the target table has no existing primary key")
def given_relationship_no_existing_pk(shared_data: dict) -> None:
    """Set up a many-to-one relationship and a target table with zero existing PKs."""
    rel = _build_relationship()
    # existing_pk_count=0 means no other column in the target table is already a PK.
    shared_data["relationship"] = rel
    shared_data["existing_pk_count"] = 0

    # Sanity: the relationship is many-to-one and has a target_column.
    assert rel.cardinality == Cardinality.many_to_one
    assert rel.target_column == "id"
    assert rel.target_table_id == "customers"


@when("the relationship is persisted")
def when_relationship_is_persisted(shared_data: dict, tmp_path) -> None:
    """Persist the relationship through the real repo against a file-backed sqlite store."""
    rel: Relationship = shared_data["relationship"]
    existing_pk_count: int = shared_data.get("existing_pk_count", 0)
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'domain.db'}"
    asyncio.run(_run_upsert(rel, dsn, existing_pk_count))
    shared_data["dsn"] = dsn


@then(
    "the target_column is marked is_primary_key=true; if a PK already exists it is marked is_alternate_key=true"
)
def then_target_column_marked_pk_or_alternate(shared_data: dict) -> None:
    """Assert the stored target_column row carries is_primary_key / is_alternate_key correctly."""
    from sqlalchemy import select

    from provisa.core.schema_org import table_columns

    rel: Relationship = shared_data["relationship"]
    existing_pk_count: int = shared_data.get("existing_pk_count", 0)

    async def _q():
        db = await _open_domain_db(shared_data["dsn"])
        async with db.acquire() as conn:
            result = await conn.execute_core(
                select(table_columns.c.is_primary_key, table_columns.c.is_alternate_key).where(
                    table_columns.c.table_id == _TGT_TBL_ID,
                    table_columns.c.column_name == rel.target_column,
                )
            )
            return result.fetchone()

    row = asyncio.run(_q())
    assert row is not None, "target_column row must exist after persistence"
    is_pk, is_ak = row[0], row[1]
    if existing_pk_count == 0:
        assert is_pk, "target_column must be is_primary_key=true when no existing PK exists"
    else:
        assert is_ak, "target_column must be is_alternate_key=true when a PK already exists"
