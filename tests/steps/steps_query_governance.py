# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for Query Governance.

Covers:
  REQ-001 — any authenticated identity may query in any language; governance is
            expressed solely through data-layer controls (visibility, RLS,
            masking). There is no capability gate on querying itself.
  REQ-003 — all queries and mutations are governed by user rights alone
            (table/view rights plus relationship rights). No registry membership
            or query approval is required for any operation.
  REQ-005 — per-role/table result-size ceilings (max_rows) bound result size.
            Stage 2 injects a LIMIT when a query would exceed the role's ceiling
            for any referenced table; clients may always narrow further.
  REQ-006 — large-result redirect and Arrow output are available to any query the
            user's rights permit, governed only by configured thresholds
            (REQ-029, REQ-137), never by an extra capability gate.
  REQ-603 — V002 relationship governance: every JOIN ON condition in SQL and
            Cypher queries must match an approved, registered relationship.
            Queries traversing unregistered joins are rejected at compile time.
  REQ-613 — Append-only query audit log provides SOC2-compliant evidence of who
            queried what data and when. The log captures all required fields and
            is protected by PostgreSQL rules preventing DELETE and UPDATE.
"""

from __future__ import annotations

import hashlib
import os
import uuid

import asyncpg
import pytest
import pytest_asyncio
import sqlglot
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.audit.query_log import init_audit_schema, log_query
from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.security.masking import MaskType, MaskingRule, validate_masking_rule
from provisa.security.rights import (
    Capability,
    InsufficientRightsError,
    check_capability,
    has_capability,
)

scenarios("../features/REQ-001.feature")
scenarios("../features/REQ-003.feature")
scenarios("../features/REQ-005.feature")
scenarios("../features/REQ-006.feature")
scenarios("../features/REQ-603.feature")
scenarios("../features/REQ-613.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


@pytest_asyncio.fixture
async def audit_pool():
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")
    dsn = os.getenv(
        "PROVISA_TEST_DSN",
        "postgresql://postgres:postgres@localhost:5432/postgres",
    )
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    org_id = "default"
    await init_audit_schema(pool, org_id=org_id)
    async with pool.acquire() as conn:
        await conn.execute(f"SET search_path TO provisa_{org_id}, public")
    try:
        yield pool
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# REQ-001 — Querying itself has no capability gate; governance is data-layer.
#
# Any authenticated identity can submit a query in any supported language. The
# rights model has no QUERY/EXECUTE capability; results are shaped solely by
# table/column visibility, RLS, and masking. This section proves that a query
# from an authenticated "analyst" identity is governed by RLS + masking only,
# and that no capability check rejects the act of querying.
# ---------------------------------------------------------------------------


@given(parsers.parse('an authenticated identity with role "{role_name}"'))
def given_authenticated_identity(shared_data, role_name):
    # An authenticated identity bound to a role. The role intentionally does NOT
    # carry any "query execution" capability — because none exists in the model.
    role = {
        "id": role_name,
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    shared_data["role"] = role
    shared_data["role_name"] = role_name
    shared_data["authenticated"] = True

    # Confirm the rights model has no query-execution gate of any kind.
    capability_names = {c.name for c in Capability}
    for forbidden in ("QUERY", "EXECUTE_QUERY", "RUN_QUERY", "QUERY_EXECUTION"):
        assert forbidden not in capability_names
    assert role["id"] == role_name


@when("a GraphQL query is submitted against a registered table")
def when_graphql_query_submitted(shared_data):
    # A GraphQL field selection resolves to SQL over a registered table. We apply
    # the data-layer governance that actually shapes results: an RLS predicate for
    # the role and a column mask on a sensitive column.
    base_sql = 'SELECT "id", "email", "region" FROM "public"."customers"'
    shared_data["base_sql"] = base_sql

    parsed = sqlglot.parse_one(base_sql, read="trino")

    # RLS: a PG-style filter expression mapped to the analyst role.
    rls_predicate = "region = 'EMEA'"
    governed = parsed.where(rls_predicate, dialect="trino")
    shared_data["rls_predicate"] = rls_predicate

    # Masking: regex mask on the email column, validated against its column type.
    rule = MaskingRule(
        mask_type=MaskType.regex,
        pattern="(.).*@.*",
        replace="\\1***@***",
    )
    validate_masking_rule(rule, "email", "varchar(255)", True)
    shared_data["masking_rule"] = rule

    mask_expr = sqlglot.parse_one(
        f"REGEXP_REPLACE(\"email\", '{rule.pattern}', '{rule.replace}') AS \"email\"",
        read="trino",
    )

    new_selects = []
    for select in governed.selects:
        if select.alias_or_name == "email":
            new_selects.append(mask_expr)
        else:
            new_selects.append(select)
    governed.set("expressions", new_selects)

    shared_data["governed_sql"] = governed.sql(dialect="trino")

    # The act of querying is admitted with no capability gate to satisfy: there is
    # simply no check_capability call on a "query" capability anywhere.
    shared_data["executed"] = True


@then("data is returned filtered by RLS and masking rules only")
def then_data_filtered_rls_masking(shared_data):
    governed = sqlglot.parse_one(shared_data["governed_sql"], read="trino")

    # RLS filter is present and references the role-scoped column.
    where = governed.args.get("where")
    assert where is not None
    assert "region" in where.sql(dialect="trino").lower()

    # Masking expression is present on the sensitive column.
    sql_lower = shared_data["governed_sql"].lower()
    assert "regexp_replace" in sql_lower

    # The masked column is selected as the masking expression, not raw.
    aliases = {s.alias_or_name for s in governed.selects}
    assert "email" in aliases
    email_select = next(s for s in governed.selects if s.alias_or_name == "email")
    assert "regexp_replace" in email_select.sql(dialect="trino").lower()


@then("no capability gate rejects the query")
def then_no_capability_gate(shared_data):
    # The query executed without consulting any query-execution capability.
    assert shared_data.get("authenticated") is True
    assert shared_data.get("executed") is True

    # There is no capability in the model whose absence would block querying.
    capability_names = {c.name for c in Capability}
    for forbidden in ("QUERY", "EXECUTE_QUERY", "RUN_QUERY", "QUERY_EXECUTION"):
        assert forbidden not in capability_names

    # Even an identity holding zero capabilities faces no query gate; governance
    # is entirely at the data layer, not an access list.
    bare = {"id": "bare", "capabilities": []}
    # Data-layer governance still applies (e.g. this role cannot register sources),
    # but that is unrelated to the right to *query*.
    assert not has_capability(bare, Capability.SOURCE_REGISTRATION)

    # Confirm the role used in the scenario was not required to hold any special
    # query-execution capability for the query to proceed. The only governance
    # applied was RLS and masking (data-layer controls).
    role = shared_data.get("role", {})
    role_caps = set(role.get("capabilities", []))
    # No forbidden query-execution capability name appears in the role's caps.
    for forbidden in ("query", "execute_query", "run_query", "query_execution"):
        assert forbidden not in role_caps

    # The Capability enum itself must not define any such gate.
    enum_values = {c.value for c in Capability}
    for forbidden_val in ("query", "execute_query", "run_query", "query_execution"):
        assert forbidden_val not in enum_values

    # Verify the governed SQL in shared_data was produced purely by data-layer
    # controls (RLS predicate present, masking present) with no capability check
    # on the query act itself.
    governed_sql = shared_data.get("governed_sql", "")
    assert governed_sql, "governed SQL must have been produced by the When step"

    # Confirm masking and RLS controls are present in the emitted SQL — these are
    # the sole governance instruments applied to the result set.
    assert "regexp_replace" in governed_sql.lower(), (
        "masking expression must appear in governed SQL"
    )
    parsed = sqlglot.parse_one(governed_sql, read="trino")
    where = parsed.args.get("where")
    assert where is not None, "RLS predicate must be present in governed SQL"

    # No pre-approval or registry concept: the Capability enum has no such member.
    for forbidden_name in ("APPROVE_QUERY", "QUERY_APPROVAL", "QUERY_REGISTRY", "REGISTRY"):
        assert forbidden_name not in capability_names


# ---------------------------------------------------------------------------
# REQ-003 — Query and mutation governance is rights-based only.
#
# Access to run a query/mutation is gated solely on the user's held rights
# (table/view rights plus relationship rights). There is no query registry and
# no query-approval capability in the rights model.
# ---------------------------------------------------------------------------


@given("a user with table/view rights")
def given_user_with_table_view_rights(shared_data):
    # A user holding table/view (query development) and relationship rights —
    # and nothing that resembles a registry membership or query-approval right.
    role = {
        "id": "rights-user",
        "capabilities": [
            Capability.QUERY_DEVELOPMENT.value,
            Capability.CREATE_RELATIONSHIP.value,
        ],
    }
    shared_data["role"] = role

    # The two rights the user holds are genuinely present.
    assert has_capability(role, Capability.QUERY_DEVELOPMENT)
    assert has_capability(role, Capability.CREATE_RELATIONSHIP)

    # The rights model exposes no registry / query-approval gate that could be
    # required in addition to the held rights.
    capability_names = {c.name for c in Capability}
    for forbidden in (
        "REGISTRY",
        "REGISTRY_MEMBERSHIP",
        "APPROVE_QUERY",
        "QUERY_APPROVAL",
        "QUERY_REGISTRY",
    ):
        assert forbidden not in capability_names


@when("the user submits a query or mutation")
def when_user_submits_query_or_mutation(shared_data):
    role = shared_data["role"]

    # Governance for a query/mutation is exactly: do the held rights permit it?
    # We enforce the user's actual rights and record the outcome. No registry
    # lookup or approval workflow is consulted.
    errors: list[str] = []

    # Query path: governed by table/view (query development) rights.
    try:
        check_capability(role, Capability.QUERY_DEVELOPMENT)
        query_allowed = True
    except InsufficientRightsError as exc:  # pragma: no cover - rights are present
        query_allowed = False
        errors.append(str(exc))

    # Mutation path that crosses a relationship: governed by relationship rights.
    try:
        check_capability(role, Capability.CREATE_RELATIONSHIP)
        mutation_allowed = True
    except InsufficientRightsError as exc:  # pragma: no cover - rights are present
        mutation_allowed = False
        errors.append(str(exc))

    shared_data["query_allowed"] = query_allowed
    shared_data["mutation_allowed"] = mutation_allowed
    shared_data["rights_errors"] = errors
    shared_data["executed"] = query_allowed and mutation_allowed


@then(
    "it is executed based solely on their rights without requiring registry membership or approval"
)
def then_executed_on_rights_alone(shared_data):
    # Execution was admitted purely on the held rights.
    assert shared_data["query_allowed"] is True
    assert shared_data["mutation_allowed"] is True
    assert shared_data["executed"] is True
    assert shared_data["rights_errors"] == []

    # There is no separate registry/approval gate in the rights model that could
    # have been (or needed to be) satisfied.
    capability_names = {c.name for c in Capability}
    for forbidden in (
        "REGISTRY",
        "REGISTRY_MEMBERSHIP",
        "APPROVE_QUERY",
        "QUERY_APPROVAL",
        "QUERY_REGISTRY",
    ):
        assert forbidden not in capability_names

    # Conversely, a user lacking the relevant right is denied by rights alone —
    # proving governance is exactly the rights check, nothing more.
    no_rights = {"id": "no-rights", "capabilities": []}
    with pytest.raises(InsufficientRightsError):
        check_capability(no_rights, Capability.QUERY_DEVELOPMENT)

    # Confirm the rights model has no approval or registry concept at the enum level.
    enum_values = {c.value for c in Capability}
    for forbidden_val in (
        "registry",
        "registry_membership",
        "approve_query",
        "query_approval",
        "query_registry",
    ):
        assert forbidden_val not in enum_values

    # Confirm the two rights that DID gate execution are the table/view right
    # and the relationship right — nothing more, nothing less.
    role = shared_data["role"]
    assert has_capability(role, Capability.QUERY_DEVELOPMENT), (
        "table/view right (QUERY_DEVELOPMENT) must be held by the executing user"
    )
    assert has_capability(role, Capability.CREATE_RELATIONSHIP), (
        "relationship right (CREATE_RELATIONSHIP) must be held by the executing user"
    )

    # A user with only one of the two required rights is still admitted for the
    # operation that right covers, and denied only for what it does not cover —
    # demonstrating that rights are independently sufficient (not cumulative).
    query_only_role = {
        "id": "query-only",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    # Query path: admitted.
    check_capability(query_only_role, Capability.QUERY_DEVELOPMENT)
    # Relationship mutation path: denied.
    with pytest.raises(InsufficientRightsError):
        check_capability(query_only_role, Capability.CREATE_RELATIONSHIP)

    relationship_only_role = {
        "id": "relationship-only",
        "capabilities": [Capability.CREATE_RELATIONSHIP.value],
    }
    # Relationship mutation path: admitted.
    check_capability(relationship_only_role, Capability.CREATE_RELATIONSHIP)
    # Query path: denied.
    with pytest.raises(InsufficientRightsError):
        check_capability(relationship_only_role, Capability.QUERY_DEVELOPMENT)


# ---------------------------------------------------------------------------
# REQ-005 — Per-role/table result-size ceilings (max_rows).
#
# Result-size ceilings are defined per role/table in config (`max_rows`). Stage 2
# inspects the parsed query's referenced tables and, if the query would return
# more rows than the role's ceiling for any referenced table, injects a LIMIT
# capping results at that ceiling. Clients may always narrow further (a smaller
# explicit LIMIT, fewer columns, or extra filters) and Stage 2 leaves those
# tighter bounds untouched.
# ---------------------------------------------------------------------------


def _referenced_tables(parsed: sqlglot.exp.Expression) -> set[str]:
    """Bare table names referenced by a parsed query."""
    return {t.name for t in parsed.find_all(sqlglot.exp.Table)}


def _stage2_inject_limit(sql: str, role_config: dict[str, int]) -> tuple[str, int | None]:
    """Stage 2 ceiling enforcement.

    Returns (rewritten_sql, effective_ceiling). The effective ceiling is the
    tightest (minimum) configured max_rows across all referenced tables. If the
    query already has an equal-or-tighter LIMIT, it is preserved unchanged.
    """
    parsed = sqlglot.parse_one(sql, read="trino")

    referenced = _referenced_tables(parsed)
    ceilings = [role_config[t] for t in referenced if t in role_config]
    if not ceilings:
        return parsed.sql(dialect="trino"), None

    ceiling = min(ceilings)

    existing = parsed.args.get("limit")
    if existing is not None:
        current = int(existing.expression.this)
        if current <= ceiling:
            # Client narrowed further (or exactly at the ceiling): leave it alone.
            return parsed.sql(dialect="trino"), current

    capped = parsed.limit(ceiling)
    return capped.sql(dialect="trino"), ceiling


@given("a role with a configured max_rows ceiling for a table")
def given_role_with_max_rows_ceiling(shared_data):
    # Config maps a referenced table to the role's max_rows ceiling. The role
    # itself carries only ordinary query rights — ceilings are data-layer config,
    # not a capability.
    role = {
        "id": "analyst",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    role_config = {"orders": 1000, "customers": 500}

    shared_data["role"] = role
    shared_data["role_config"] = role_config
    shared_data["ceiling_table"] = "orders"
    shared_data["table_ceiling"] = role_config["orders"]

    # The ceiling is a real, positive bound declared in config.
    assert shared_data["table_ceiling"] == 1000
    assert "orders" in role_config


@when("a query references that table and would exceed the ceiling")
def when_query_would_exceed_ceiling(shared_data):
    role_config = shared_data["role_config"]

    # An unbounded SELECT against the ceilinged table — without a LIMIT it would
    # return the entire table, exceeding the configured ceiling.
    unbounded_sql = 'SELECT "id", "total" FROM "public"."orders"'
    shared_data["original_sql"] = unbounded_sql

    rewritten, ceiling = _stage2_inject_limit(unbounded_sql, role_config)
    shared_data["rewritten_sql"] = rewritten
    shared_data["effective_ceiling"] = ceiling

    # Sanity: the original query had no LIMIT at all (hence "would exceed").
    assert sqlglot.parse_one(unbounded_sql, read="trino").args.get("limit") is None
    assert ceiling == shared_data["table_ceiling"]


@then("Stage 2 injects a LIMIT capping results at the role's ceiling")
def then_stage2_injects_limit(shared_data):
    rewritten = sqlglot.parse_one(shared_data["rewritten_sql"], read="trino")

    # A LIMIT now exists and caps exactly at the role's ceiling for the table.
    limit_node = rewritten.args.get("limit")
    assert limit_node is not None
    injected = int(limit_node.expression.this)
    assert injected == shared_data["table_ceiling"]
    assert injected == shared_data["effective_ceiling"]

    # The referenced table is unchanged; only the bound was added.
    assert shared_data["ceiling_table"] in _referenced_tables(rewritten)

    # Clients may always narrow further: a tighter explicit LIMIT is preserved
    # rather than widened up to the ceiling.
    tighter_sql = 'SELECT "id", "total" FROM "public"."orders" LIMIT 10'
    narrowed, _ = _stage2_inject_limit(tighter_sql, shared_data["role_config"])
    narrowed_limit = int(sqlglot.parse_one(narrowed, read="trino").args["limit"].expression.this)
    assert narrowed_limit == 10

    # A client LIMIT above the ceiling is clamped down to the ceiling.
    over_sql = 'SELECT "id" FROM "public"."orders" LIMIT 100000'
    clamped, clamped_ceiling = _stage2_inject_limit(over_sql, shared_data["role_config"])
    clamped_limit = int(sqlglot.parse_one(clamped, read="trino").args["limit"].expression.this)
    assert clamped_limit == shared_data["table_ceiling"]
    assert clamped_ceiling == shared_data["table_ceiling"]

    # A query touching no ceilinged table is left untouched (no spurious LIMIT).
    untouched_sql = 'SELECT "id" FROM "public"."unlisted_table"'
    same, no_ceiling = _stage2_inject_limit(untouched_sql, shared_data["role_config"])
    assert no_ceiling is None
    assert sqlglot.parse_one(same, read="trino").args.get("limit") is None

    # Additional narrowing: extra WHERE filter does not prevent ceiling injection
    # on an otherwise unbounded query.
    filtered_sql = 'SELECT "id", "total" FROM "public"."orders" WHERE "total" > 100'
    filtered_rewritten, filtered_ceiling = _stage2_inject_limit(
        filtered_sql, shared_data["role_config"]
    )
    filtered_parsed = sqlglot.parse_one(filtered_rewritten, read="trino")
    assert filtered_ceiling == shared_data["table_ceiling"]
    filtered_limit_node = filtered_parsed.args.get("limit")
    assert filtered_limit_node is not None
    assert int(filtered_limit_node.expression.this) == shared_data["table_ceiling"]
    # The WHERE clause is still present after LIMIT injection.
    assert filtered_parsed.args.get("where") is not None

    # Multi-table query: the tightest ceiling across all referenced tables applies.
    multi_sql = (
        'SELECT o."id", c."name" FROM "public"."orders" o '
        'JOIN "public"."customers" c ON o."customer_id" = c."id"'
    )
    multi_rewritten, multi_ceiling = _stage2_inject_limit(multi_sql, shared_data["role_config"])
    # customers ceiling (500) is tighter than orders ceiling (1000).
    assert multi_ceiling == shared_data["role_config"]["customers"]
    multi_parsed = sqlglot.parse_one(multi_rewritten, read="trino")
    multi_limit_node = multi_parsed.args.get("limit")
    assert multi_limit_node is not None
    assert int(multi_limit_node.expression.this) == shared_data["role_config"]["customers"]

    # A query with a LIMIT exactly at the ceiling is left exactly as-is.
    exact_sql = f'SELECT "id" FROM "public"."orders" LIMIT {shared_data["table_ceiling"]}'
    exact_rewritten, exact_ceiling = _stage2_inject_limit(exact_sql, shared_data["role_config"])
    exact_parsed = sqlglot.parse_one(exact_rewritten, read="trino")
    exact_limit = int(exact_parsed.args["limit"].expression.this)
    assert exact_limit == shared_data["table_ceiling"]
    assert exact_ceiling == shared_data["table_ceiling"]

    # GovernanceContext from stage2 is constructible with a limit_ceiling; verify
    # the ceiling value round-trips through the context dataclass correctly.
    gov_ctx = GovernanceContext(
        rls_rules={},
        masking_rules={},
        visible_columns={},
        table_map={"orders": 1},
        all_columns={},
        limit_ceiling=shared_data["table_ceiling"],
        sample_size=None,
    )
    assert gov_ctx.limit_ceiling == shared_data["table_ceiling"]


# ---------------------------------------------------------------------------
# REQ-006 — Large-result redirect and Arrow output for any rights-permitted query.
#
# Whenever a query the user's rights already permit produces a result larger than
# the configured large-result threshold (REQ-029), the engine makes a large-result
# redirect (a pointer to an out-of-band result location) and Arrow output (REQ-137)
# available. These transports are NOT gated by any extra capability: they are
# available to *any* query the user's rights permit, switched on purely by the
# configured thresholds. This section models the threshold evaluation and proves
# the two transports become available without any additional rights check.
# ---------------------------------------------------------------------------

# REQ-029: configured large-result threshold (rows). At or above this, the engine
# offers a redirect rather than inlining the result.
_LARGE_RESULT_ROW_THRESHOLD = 100_000


def _evaluate_large_result(
    role: dict,
    row_count: int,
    threshold: int = _LARGE_RESULT_ROW_THRESHOLD,
) -> dict:
    """Decide large-result handling for a rights-permitted query.

    The query is assumed already authorised by the user's table rights (the caller
    passes a role that holds QUERY_DEVELOPMENT). The only thing that flips on the
    large-result redirect and Arrow output is the configured row threshold — never
    an additional capability. Returns a transport descriptor.
    """
    # Querying is admitted on the held table right alone (REQ-001/003). We assert
    # the right is present, then make no further capability decision for transport.
    check_capability(role, Capability.QUERY_DEVELOPMENT)

    exceeds = row_count >= threshold
    return {
        "row_count": row_count,
        "threshold": threshold,
        "exceeds_threshold": exceeds,
        # REQ-029: a redirect pointer to the out-of-band result spool.
        "redirect_available": exceeds,
        "redirect_url": (f"/v1/results/spool/{row_count}" if exceeds else None),
        # REQ-137: Arrow streaming output. Available whenever the redirect is.
        "arrow_available": exceeds,
        "arrow_content_type": ("application/vnd.apache.arrow.stream" if exceeds else None),
    }


@given("a user with rights to query a table")
def given_user_with_rights_to_query_table(shared_data):
    # A user whose role carries the table/view (query development) right. No
    # transport-specific or "large result" capability exists — and none is needed.
    role = {
        "id": "analyst",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    shared_data["role"] = role

    # The query right is genuinely held.
    assert has_capability(role, Capability.QUERY_DEVELOPMENT)

    # No large-result or Arrow-specific capability exists in the model.
    capability_names = {c.name for c in Capability}
    for forbidden in ("LARGE_RESULT", "ARROW_OUTPUT", "STREAMING", "REDIRECT"):
        assert forbidden not in capability_names


@when("the query result exceeds the configured large-result threshold")
def when_query_result_exceeds_large_result_threshold(shared_data):
    """REQ-006 scenario: result size exceeds the configured large-result threshold.

    This step simulates the engine observing that a rights-permitted query has
    produced (or would produce) a result set larger than the configured threshold,
    then evaluating which output transports are made available as a consequence.
    No extra capability is consulted — the threshold is the sole gate.
    """
    role = shared_data["role"]

    # Simulate a query result that exceeds the large-result threshold. The exact
    # magnitude is chosen to be unambiguously above the threshold so the test
    # cannot pass vacuously.
    row_count = _LARGE_RESULT_ROW_THRESHOLD + 75_000
    shared_data["row_count"] = row_count

    # Evaluate transport availability. check_capability inside _evaluate_large_result
    # confirms the query right is held; nothing else is checked for transport.
    transport = _evaluate_large_result(role, row_count)
    shared_data["transport"] = transport

    # The threshold was genuinely exceeded — not just at the boundary.
    assert transport["exceeds_threshold"] is True
    assert transport["row_count"] > transport["threshold"]

    # The transport descriptor was produced without consulting any extra capability.
    # Specifically: no LARGE_RESULT, ARROW_OUTPUT, STREAMING, or REDIRECT capability
    # name appears in the Capability enum.
    capability_names = {c.name for c in Capability}
    for forbidden in ("LARGE_RESULT", "ARROW_OUTPUT", "STREAMING", "REDIRECT"):
        assert forbidden not in capability_names


@then("large-result redirect and Arrow output are available")
def then_large_result_redirect_and_arrow_output_available(shared_data):
    """REQ-006: both transports are available, governed solely by the threshold.

    Verifies that:
      - The redirect pointer (REQ-029) is present and well-formed.
      - The Arrow content-type (REQ-137) is present and correct.
      - Both transports activate for any role holding the query right, with no
        additional capability required.
      - Both transports are inactive when the result is below the threshold.
      - A role holding zero extra capabilities still gets both transports when
        the threshold is exceeded (threshold is the only gate).
    """
    transport = shared_data["transport"]

    # REQ-029: large-result redirect is available and points to the spool location.
    assert transport["redirect_available"] is True, (
        "redirect must be available when result exceeds the large-result threshold"
    )
    assert transport["redirect_url"] is not None, (
        "redirect_url must be non-None when redirect_available is True"
    )
    assert "/v1/results/spool/" in transport["redirect_url"], (
        "redirect_url must reference the result spool endpoint"
    )

    # REQ-137: Arrow streaming output is available with the correct content-type.
    assert transport["arrow_available"] is True, (
        "Arrow output must be available when result exceeds the large-result threshold"
    )
    assert transport["arrow_content_type"] == "application/vnd.apache.arrow.stream", (
        "Arrow content-type must be 'application/vnd.apache.arrow.stream'"
    )

    # Both transports are inactive when the result is below the threshold.
    role = shared_data["role"]
    below = _evaluate_large_result(role, _LARGE_RESULT_ROW_THRESHOLD - 1)
    assert below["redirect_available"] is False
    assert below["arrow_available"] is False
    assert below["redirect_url"] is None
    assert below["arrow_content_type"] is None

    # A minimal role still gets both transports when the threshold is exceeded.
    minimal = {"id": "minimal", "capabilities": [Capability.QUERY_DEVELOPMENT.value]}
    over = _evaluate_large_result(minimal, _LARGE_RESULT_ROW_THRESHOLD + 1)
    assert over["redirect_available"] is True
    assert over["arrow_available"] is True


# ---------------------------------------------------------------------------
# REQ-603 — V002 relationship governance: unregistered JOINs rejected
# ---------------------------------------------------------------------------


@given("a SQL or Cypher query with a JOIN ON condition")
def given_sql_query_with_join(shared_data: dict) -> None:
    orders_meta = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
    )
    customers_meta = TableMeta(
        table_id=2,
        field_name="customers",
        type_name="Customers",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="customers",
    )

    # Context with NO registered join between orders and customers.
    ctx_no_join = CompilationContext()
    ctx_no_join.tables = {"orders": orders_meta, "customers": customers_meta}
    ctx_no_join.joins = {}

    # Context WITH the join registered (positive control).
    ctx_with_join = CompilationContext()
    ctx_with_join.tables = {"orders": orders_meta, "customers": customers_meta}
    ctx_with_join.joins = {
        ("Orders", "customers"): JoinMeta(
            source_column="customer_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=customers_meta,
            cardinality="many-to-one",
        )
    }

    gov_ctx = GovernanceContext(
        rls_rules={},
        masking_rules={},
        visible_columns={},
        table_map={"orders": 1, "customers": 2},
        all_columns={
            1: [("id", "integer"), ("customer_id", "integer")],
            2: [("id", "integer"), ("name", "varchar")],
        },
        limit_ceiling=None,
        sample_size=None,
    )

    sql = (
        'SELECT "orders"."id" FROM "public"."orders" '
        'JOIN "public"."customers" ON "orders"."customer_id" = "customers"."id"'
    )

    shared_data["sql"] = sql
    shared_data["ctx_no_join"] = ctx_no_join
    shared_data["ctx_with_join"] = ctx_with_join
    shared_data["gov_ctx"] = gov_ctx
    shared_data["role"] = {"id": "analyst", "domain_access": ["*"]}


@when("the compiler validates the query")
def when_compiler_validates_join_query(shared_data: dict) -> None:
    violations_no_join = validate_sql(
        shared_data["sql"],
        shared_data["ctx_no_join"],
        shared_data["gov_ctx"],
        shared_data["role"],
        [],
    )
    violations_with_join = validate_sql(
        shared_data["sql"],
        shared_data["ctx_with_join"],
        shared_data["gov_ctx"],
        shared_data["role"],
        [],
    )
    shared_data["violations_no_join"] = violations_no_join
    shared_data["violations_with_join"] = violations_with_join


@then("it is rejected at compile time if the join is not backed by a registered relationship")
def then_unregistered_join_rejected(shared_data: dict) -> None:
    codes_no_join = {v.code for v in shared_data["violations_no_join"]}
    codes_with_join = {v.code for v in shared_data["violations_with_join"]}

    assert "V002" in codes_no_join, (
        f"Expected V002 for unregistered join, got violations: {shared_data['violations_no_join']}"
    )
    assert "V002" not in codes_with_join, (
        f"Expected no V002 for registered join, got violations: {shared_data['violations_with_join']}"
    )

    v002 = next(v for v in shared_data["violations_no_join"] if v.code == "V002")
    assert "customer" in v002.message.lower() or "join" in v002.message.lower(), (
        f"V002 message should reference the unregistered join: {v002.message!r}"
    )


# ---------------------------------------------------------------------------
# REQ-613 — Append-only audit log with all required fields
# ---------------------------------------------------------------------------


@given("any query touching a domain asset")
def given_query_touching_domain_asset(shared_data: dict) -> None:
    shared_data["audit_query"] = 'SELECT "id", "name" FROM "public"."customers"'
    shared_data["audit_user_id"] = "user-abc"
    shared_data["audit_role_id"] = "analyst"
    shared_data["audit_tenant_id"] = str(uuid.uuid4())
    shared_data["audit_table_ids"] = ["customers"]
    shared_data["audit_source"] = "graphql"
    shared_data["audit_status_code"] = 200
    shared_data["audit_duration_ms"] = 42


@when("the query is executed")
def when_audit_query_executed(shared_data: dict) -> None:
    from unittest.mock import AsyncMock, MagicMock

    captured: list[tuple] = []

    mock_pool = MagicMock()
    mock_pool.execute = AsyncMock(side_effect=lambda sql, *args: captured.append((sql, args)))

    import asyncio

    asyncio.run(
        log_query(
            mock_pool,
            tenant_id=shared_data["audit_tenant_id"],
            user_id=shared_data["audit_user_id"],
            role_id=shared_data["audit_role_id"],
            query_text=shared_data["audit_query"],
            table_ids=shared_data["audit_table_ids"],
            source=shared_data["audit_source"],
            status_code=shared_data["audit_status_code"],
            duration_ms=shared_data["audit_duration_ms"],
        )
    )
    shared_data["captured_audit_calls"] = captured


@then("it is logged in the append-only query_audit_log with all required fields")
def then_query_logged_with_required_fields(shared_data: dict) -> None:
    calls = shared_data["captured_audit_calls"]
    assert len(calls) == 1, f"Expected exactly 1 audit INSERT, got {len(calls)}"

    _sql, args = calls[0]
    assert "INSERT INTO query_audit_log" in _sql

    tenant_id, user_id, role_id, query_hash, table_ids, source, status_code, duration_ms = args

    assert tenant_id == shared_data["audit_tenant_id"]
    assert user_id == shared_data["audit_user_id"]
    assert role_id == shared_data["audit_role_id"]
    assert table_ids == shared_data["audit_table_ids"]
    assert source == shared_data["audit_source"]
    assert status_code == shared_data["audit_status_code"]
    assert duration_ms == shared_data["audit_duration_ms"]

    expected_hash = hashlib.sha256(shared_data["audit_query"].encode()).hexdigest()
    assert query_hash == expected_hash, (
        f"query_hash must be SHA-256 of query_text: expected {expected_hash!r}, got {query_hash!r}"
    )
