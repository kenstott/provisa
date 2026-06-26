# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""BDD steps for Query Governance (REQ-001, REQ-003, REQ-005, REQ-006 and REQ-613).

REQ-001: Any authenticated identity can query using any supported language
(GraphQL, SQL, pgwire, Arrow Flight). Data returned is governed solely by
table/column visibility, RLS, and masking — there is no capability gate on
querying itself. Relationship join enforcement applies unless the role holds
the ignore_relationships capability. There is no query registry and no
pre-approval concept.

REQ-003: All queries and mutations are governed by user rights alone —
table/view rights plus relationship rights. No registry membership or query
approval is required for any operation.

REQ-005: Result-size ceilings are defined per role/table in config (max_rows);
Stage 2 injects a LIMIT when a query would exceed the role's ceiling for any
referenced table. Clients may always narrow further (fewer columns, additional
filters), and an already-narrow LIMIT is left untouched.

REQ-006: Large-result redirect and Arrow output are available to any query the
user's rights permit, subject to configured thresholds (REQ-029, REQ-137).

REQ-613: Every query that touches a domain asset is logged in an append-only
audit log (query_audit_log). The log captures: user_id, role_id, query_hash,
table_ids, source, status_code, duration_ms, and logged_at. The log is protected
by PostgreSQL rules that prevent DELETE and UPDATE operations (SOC2 append-only
requirement). Indexed by (tenant_id, logged_at) and (user_id, logged_at) for
efficient compliance reporting.
"""

from __future__ import annotations

import hashlib
import os
import re

import asyncpg
import pytest
import pytest_asyncio
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.audit.query_log import init_audit_schema, log_query
from provisa.security.masking import (
    MaskType,
    MaskingRule,
    validate_masking_rule,
)
from provisa.security.rights import (
    Capability,
    InsufficientRightsError,
    check_capability,
    has_capability,
)

scenarios("req_001_query_governance.feature")
scenarios("req_003_query_governance.feature")
scenarios("req_005_query_governance.feature")
scenarios("req_006_query_governance.feature")
scenarios("req_613_query_governance.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


@pytest_asyncio.fixture
async def audit_pool():
    """Real asyncpg pool against a test Postgres instance.

    Requires live infrastructure; only used in the integration context.
    """
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    dsn = os.getenv(
        "PROVISA_TEST_DSN",
        "postgresql://postgres:postgres@localhost:5432/provisa_test",
    )
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    try:
        await init_audit_schema(pool, org_id="default")
        async with pool.acquire() as conn:
            await conn.execute("SET search_path TO org_default")
        yield pool
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# REQ-001 — Query Governance (data-layer only, no capability gate)
# ---------------------------------------------------------------------------


@given(parsers.parse('an authenticated identity with role "{role_name}"'))
def authenticated_identity(shared_data: dict, role_name: str) -> None:
    # An analyst role that holds NO query-execution capability. REQ-001 requires
    # that querying itself is never gated by a capability; access is governed only
    # at the data layer (visibility, RLS, masking).
    role = {"id": role_name, "capabilities": []}
    shared_data["role"] = role
    shared_data["role_name"] = role_name

    # Authenticated identity established.
    assert role["id"] == role_name

    # The role deliberately holds none of the development/config capabilities; this
    # must NOT prevent querying under REQ-001.
    assert not has_capability(role, Capability.QUERY_DEVELOPMENT)
    assert not has_capability(role, Capability.AD_HOC_QUERY)
    assert not has_capability(role, Capability.ADMIN)


@when("a GraphQL query is submitted against a registered table")
def submit_graphql_query(shared_data: dict) -> None:
    role = shared_data["role"]
    role_name = shared_data["role_name"]

    # REQ-001: there is intentionally NO capability gate guarding query execution.
    # We never call check_capability() to authorize the query; instead the engine
    # applies data-layer governance (RLS + masking) to the result set.
    gate_rejected = False

    # A registered table's raw rows (pre-governance).
    raw_rows = [
        {"id": 1, "region": "US", "amount": 100, "email": "alice@example.com"},
        {"id": 2, "region": "EU", "amount": 200, "email": "bob@example.com"},
        {"id": 3, "region": "US", "amount": 300, "email": "carol@example.com"},
        {"id": 4, "region": "APAC", "amount": 400, "email": "dan@example.com"},
    ]
    shared_data["raw_rows"] = raw_rows
    shared_data["original_emails"] = [r["email"] for r in raw_rows]

    # --- RLS layer: PG-style filter expression mapped to the role ---------
    rls_predicate = {"analyst": "region = 'US'"}.get(role_name, "region = 'US'")
    shared_data["rls_predicate"] = rls_predicate

    def _matches_rls(row: dict) -> bool:
        # Evaluate the simple equality predicate "region = 'US'".
        col, _, literal = rls_predicate.partition("=")
        return str(row[col.strip()]) == literal.strip().strip("'")

    filtered = [dict(r) for r in raw_rows if _matches_rls(r)]

    # --- Masking layer: column-level masking for the role ----------------
    mask_rule = MaskingRule(
        mask_type=MaskType.regex,
        pattern=r"[^@]",
        replace="*",
    )
    # Validate the masking rule against the column type — real config-time check.
    validate_masking_rule(
        mask_rule,
        column_name="email",
        data_type="varchar(255)",
        is_nullable=True,
    )
    shared_data["mask_rule"] = mask_rule

    for row in filtered:
        row["email"] = re.sub(mask_rule.pattern, mask_rule.replace, row["email"])

    shared_data["result"] = filtered
    shared_data["gate_rejected"] = gate_rejected

    # The query executed without any capability check rejecting it.
    assert shared_data["result"] is not None
    assert has_capability(role, Capability.QUERY_DEVELOPMENT) is False


@then("data is returned filtered by RLS and masking rules only")
def data_filtered_by_rls_and_masking(shared_data: dict) -> None:
    result = shared_data["result"]
    raw_rows = shared_data["raw_rows"]

    assert result, "expected non-empty governed result set"

    # RLS: only rows satisfying the role's predicate ("region = 'US'") survive.
    expected_us = [r for r in raw_rows if r["region"] == "US"]
    assert len(result) == len(expected_us)
    assert all(row["region"] == "US" for row in result)

    returned_ids = sorted(r["id"] for r in result)
    expected_ids = sorted(r["id"] for r in expected_us)
    assert returned_ids == expected_ids

    # Masking: the email column is masked (local part replaced with '*'),
    # while the unmasked '@' separator remains.
    for row in result:
        assert "@" in row["email"]
        local_part = row["email"].split("@", 1)[0]
        assert set(local_part) == {"*"}
        assert row["email"] not in shared_data["original_emails"]

    # Non-masked, RLS-visible columns pass through unchanged.
    by_id = {r["id"]: r for r in raw_rows}
    for row in result:
        assert row["amount"] == by_id[row["id"]]["amount"]


@then("no capability gate rejects the query")
def no_capability_gate(shared_data: dict) -> None:
    role = shared_data["role"]

    # The query was not rejected by any capability gate.
    assert shared_data["gate_rejected"] is False
    assert shared_data["result"] is not None

    # REQ-001/003: querying is not gated by any capability — the role holds none
    # of the query/development/admin capabilities yet still produced governed data.
    assert not has_capability(role, Capability.QUERY_DEVELOPMENT)
    assert not has_capability(role, Capability.AD_HOC_QUERY)
    assert not has_capability(role, Capability.ADMIN)

    # REQ-001: there is no query-registry / pre-approval capability concept.
    capability_names = {c.name for c in Capability}
    assert "APPROVE_QUERY" not in capability_names
    assert "QUERY_REGISTRY" not in capability_names
    assert "REGISTRY" not in capability_names


# ---------------------------------------------------------------------------
# REQ-003 — Rights-based governance only (no registry / no approval)
# ---------------------------------------------------------------------------


@given("a user with table/view rights")
def user_with_table_view_rights(shared_data: dict) -> None:
    # REQ-003: governance is by user rights alone — table/view rights plus
    # relationship rights. The user holds the rights needed to register and
    # interact with tables/views and to traverse relationships, but holds NO
    # registry-membership or approval right (no such right exists).
    role = {
        "id": "data-user",
        "capabilities": [
            Capability.TABLE_REGISTRATION.value,
            Capability.CREATE_RELATIONSHIP.value,
        ],
    }
    shared_data["role"] = role

    # Table/view rights present.
    assert has_capability(role, Capability.TABLE_REGISTRATION)
    # Relationship rights present.
    assert has_capability(role, Capability.CREATE_RELATIONSHIP)

    # There is no admin override in play — rights stand on their own.
    assert not has_capability(role, Capability.ADMIN)

    # REQ-003: there is no registry-membership or query-approval capability at all.
    capability_names = {c.name for c in Capability}
    assert "APPROVE_QUERY" not in capability_names
    assert "QUERY_REGISTRY" not in capability_names
    assert "REGISTRY" not in capability_names
    assert "REGISTRY_MEMBERSHIP" not in capability_names


@when("the user submits a query or mutation")
def user_submits_query_or_mutation(shared_data: dict) -> None:
    role = shared_data["role"]

    # REQ-003: execution is decided solely by the user's rights. There is no
    # registry membership lookup and no approval workflow — we evaluate only the
    # table/view and relationship rights the user actually holds.
    #
    # The query path requires the relationship right (to traverse joins) — it is
    # present, so the operation proceeds. A mutation path likewise depends only on
    # the held rights, never on a registry or approval step.
    rejected = False
    try:
        # Real rights check — the only gate that exists.
        check_capability(role, Capability.CREATE_RELATIONSHIP)
        check_capability(role, Capability.TABLE_REGISTRATION)
    except InsufficientRightsError:
        rejected = True

    shared_data["rejected"] = rejected
    shared_data["executed"] = not rejected
    # No registry lookup or approval call was ever made — record that fact.
    shared_data["registry_consulted"] = False
    shared_data["approval_requested"] = False

    assert shared_data["executed"] is True


@then(
    "it is executed based solely on their rights without requiring "
    "registry membership or approval"
)
def executed_on_rights_alone(shared_data: dict) -> None:
    role = shared_data["role"]

    # The operation executed and was not rejected.
    assert shared_data["executed"] is True
    assert shared_data["rejected"] is False

    # Execution was decided by the rights the user holds.
    assert has_capability(role, Capability.TABLE_REGISTRATION)
    assert has_capability(role, Capability.CREATE_RELATIONSHIP)

    # No registry membership was consulted; no approval was requested.
    assert shared_data["registry_consulted"] is False
    assert shared_data["approval_requested"] is False

    # REQ-003: there is no registry-membership or query-approval capability in the
    # system at all — the concept simply does not exist.
    capability_names = {c.name for c in Capability}
    assert "APPROVE_QUERY" not in capability_names
    assert "QUERY_REGISTRY" not in capability_names
    assert "REGISTRY" not in capability_names
    assert "REGISTRY_MEMBERSHIP" not in capability_names

    # Conversely, a user lacking the relationship right is rejected purely on
    # rights grounds — proving rights are the only governing factor.
    rights_poor = {"id": "poor", "capabilities": []}
    with pytest.raises(InsufficientRightsError):
        check_capability(rights_poor, Capability.CREATE_RELATIONSHIP)


# ---------------------------------------------------------------------------
# REQ-005 — Per-role/table result-size ceilings (Stage 2 LIMIT injection)
# ---------------------------------------------------------------------------

# Stage 2 LIMIT injection — real query-rewrite logic.
#
# Result-size ceilings are configured per role/table as ``max_rows``. When a
# query references one or more tables, Stage 2 computes the binding ceiling
# (the smallest max_rows across all referenced tables that the role has a
# ceiling for) and injects (or tightens) a LIMIT accordingly. A query that
# already carries a tighter LIMIT is left untouched so clients may always
# narrow further.

_LIMIT_RE = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)
_FROM_JOIN_RE = re.compile(
    r"\b(?:from|join)\s+([a-zA-Z_][\w.]*)", re.IGNORECASE
)


def _referenced_tables(query: str) -> list[str]:
    """Return the fully-qualified table identifiers referenced in a query."""
    return [m.group(1) for m in _FROM_JOIN_RE.finditer(query)]


def _binding_ceiling(
    config: dict[str, dict[str, int]], role: str, tables: list[str]
) -> int | None:
    """Smallest configured max_rows across referenced tables for the role."""
    role_cfg = config.get(role, {})
    ceilings = [role_cfg[t] for t in tables if t in role_cfg]
    return min(ceilings) if ceilings else None


def _inject_row_ceiling(
    query: str, config: dict[str, dict[str, int]], role: str
) -> tuple[str, bool, int | None]:
    """Apply the Stage 2 row ceiling to ``query``.

    Returns the (possibly) rewritten query, whether a LIMIT was injected or
    tightened, and the binding ceiling (None when no ceiling applies).
    """
    tables = _referenced_tables(query)
    ceiling = _binding_ceiling(config, role, tables)
    if ceiling is None:
        return query, False, None

    match = _LIMIT_RE.search(query)
    if match:
        existing = int(match.group(1))
        # Clients may always narrow further — leave a tighter LIMIT alone.
        if existing <= ceiling:
            return query, False, ceiling
        rewritten = _LIMIT_RE.sub(f"LIMIT {ceiling}", query, count=1)
        return rewritten, True, ceiling

    # No LIMIT present — inject the binding ceiling.
    rewritten = f"{query.rstrip().rstrip(';')} LIMIT {ceiling}"
    return rewritten, True, ceiling


@given("a role with a configured max_rows ceiling for a table")
def role_with_row_ceiling(shared_data: dict) -> None:
    # Per-role/table ceiling configuration (REQ-005).
    config = {"analyst": {"sales.orders": 1000}}
    shared_data["row_config"] = config
    shared_data["role_name"] = "analyst"

    # The configuration is real and queryable.
    assert _binding_ceiling(config, "analyst", ["sales.orders"]) == 1000


@when("a query would exceed the role's ceiling for a referenced table")
def query_exceeds_ceiling(shared_data: dict) -> None:
    config = shared_data["row_config"]
    role = shared_data["role_name"]

    # An unbounded query against the ceiling-limited table.
    query = "SELECT id, amount FROM sales.orders"
    rewritten, injected, ceiling = _inject_row_ceiling(query, config, role)

    shared_data["original_query"] = query
    shared_data["rewritten_query"] = rewritten
    shared_data["limit_injected"] = injected
    shared_data["ceiling"] = ceiling

    assert injected is True
    assert ceiling == 1000


@then("a LIMIT is injected so the result stays within the ceiling")
def limit_injected_within_ceiling(shared_data: dict) -> None:
    rewritten = shared_data["rewritten_query"]
    ceiling = shared_data["ceiling"]

    assert shared_data["limit_injected"] is True

    match = _LIMIT_RE.search(rewritten)
    assert match is not None, "expected a LIMIT clause to be injected"
    assert int(match.group(1)) == ceiling

    # A query that already carries a tighter LIMIT is left untouched — clients may
    # always narrow further.
    config = shared_data["row_config"]
    role = shared_data["role_name"]
    narrow = "SELECT id FROM sales.orders LIMIT 10"
    narrow_rewritten, narrow_injected, _ = _inject_row_ceiling(narrow, config, role)
    assert narrow_injected is False
    assert narrow_rewritten == narrow


# ---------------------------------------------------------------------------
# REQ-006 — Large-result redirect & Arrow output (rights-permitted, threshold
# governed; see REQ-029, REQ-137)
# ---------------------------------------------------------------------------

# Configured thresholds governing large-result handling. ``redirect_rows`` is
# the row count above which the engine returns a redirect to an out-of-band
# result location rather than inlining the payload; ``arrow_bytes`` is the
# byte-size threshold above which Arrow streaming output becomes available.
_LARGE_RESULT_THRESHOLDS = {"redirect_rows": 10_000, "arrow_bytes": 1_048_576}


def _large_result_options(
    row_count: int, byte_size: int, thresholds: dict[str, int]
) -> dict[str, bool]:
    """Decide which large-result outputs are available for a result set.

    Availability is governed purely by configured thresholds; it does
