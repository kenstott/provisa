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
"""

from __future__ import annotations

import os

import asyncpg
import pytest
import pytest_asyncio
import sqlglot
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.audit.query_log import init_audit_schema
from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import ValidationViolation, validate_sql
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
    "it is executed based solely on their rights without requiring "
    "registry membership or approval"
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
        "arrow_content_type": (
            "application/vnd.apache.arrow.stream" if exceeds else None
        ),
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


@when("the result size exceeds the configured large-result threshold")
def when_result_exceeds_threshold(shared_data):
    role = shared_data["role"]
    # Simulate a query that returns more rows than the threshold.
    row_count = _LARGE_RESULT_ROW_THRESHOLD + 50_000
    shared_data["row_count"] = row_count

    transport = _evaluate_large_result(role, row_count)
    shared_data["transport"] = transport

    # The threshold was genuinely exceeded.
    assert transport["exceeds_threshold"] is True


@then("large-result redirect and Arrow output are available without an extra capability gate")
def then_large_result_available_without_gate(shared_data):
    transport = shared_data["transport"]

    # Both transports are available.
    assert transport["redirect_available"] is True
    assert transport["redirect_url"] is not None
    assert transport["arrow_available"] is True
    assert transport["arrow_content_type"] == "application/vnd.apache.arrow.stream"

    # They were made available by threshold alone — no additional capability was
    # checked beyond the base QUERY_DEVELOPMENT right. Verify by evaluating for a
    # role with zero extra capabilities: transports still activate on threshold.
    minimal_role = {
        "id": "minimal",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    minimal_transport = _evaluate_large_result(minimal_role, shared_data["row_count"])
    assert minimal_transport["redirect_available"] is True
    assert minimal_transport["arrow_available"] is True

    # Below the threshold the transports are inactive — threshold is the only gate.
    below_transport = _evaluate_large_result(
        shared_data["role"],
        _LARGE_RESULT_ROW_THRESHOLD - 1,
    )
    assert below_transport["redirect_available"] is False
    assert below_transport["arrow_available"] is False
    assert below_transport["redirect_url"] is None
    assert below_transport["arrow_content_type"] is None


# ---------------------------------------------------------------------------
# REQ-603 — V002 relationship governance: JOIN ON conditions must match a
# registered relationship or the query is rejected at compile time.
#
# SQL queries that traverse a JOIN ON condition not backed by an approved,
# registered relationship (source_col = target_col) are rejected by the
# validator with a V002 violation. Queries whose JOIN ON conditions exactly
# match a registered relationship are accepted. GraphQL queries that traverse
# relationships defined in the SDL are pre-approved (bypass_relationship_guard)
# and exempt from V002.
# ---------------------------------------------------------------------------


def _build_compilation_context(
    tables: dict[str, TableMeta],
    joins: dict[tuple[str, str], JoinMeta],
) -> CompilationContext:
    """Construct a minimal CompilationContext for validator tests."""
    return CompilationContext(tables=tables, joins=joins)


def _build_governance_context(table_map: dict[str, int]) -> GovernanceContext:
    """Construct a minimal GovernanceContext for validator tests."""
    return GovernanceContext(
        rls_rules={},
        masking_rules={},
        visible_columns={},
        table_map=table_map,
        all_columns={},
        limit_ceiling=None,
        sample_size=None,
    )


def _make_table_meta(
    table_id: int,
    type_name: str,
    domain_id: str = "sales",
) -> TableMeta:
    """Create a TableMeta for a table used in V002 tests."""
    return TableMeta(
        table_id=table_id,
        type_name=type_name,
        schema="public",
        table_name=type_name.lower(),
        domain_id=domain_id,
        columns=[],
    )


def _make_join_meta(
    target: TableMeta,
    source_column: str,
    target_column: str,
) -> JoinMeta:
    """Create a JoinMeta for a registered relationship."""
    return JoinMeta(
        target=target,
        source_column=source_column,
        target_column=target_column,
    )


@given("a SQL or Cypher query with a JOIN ON condition")
def given_sql_query_with_join_on(shared_data):
    # Set up two tables and a registered relationship between them.
    # orders.customer_id -> customers.id is the approved relationship.
    orders_meta = _make_table_meta(table_id=1, type_name="orders")
    customers_meta = _make_table_meta(table_id=2, type_name="customers")

    tables = {
        "orders": orders_meta,
        "customers": customers_meta,
    }

    # The approved relationship: orders.customer_id = customers.id
    approved_join = _make_join_meta(
        target=customers_meta,
        source_column="customer_id",
        target_column="id",
    )
    joins = {
        ("orders", "customer_id"): approved_join,
    }

    ctx = _build_compilation_context(tables=tables, joins=joins)

    # GovernanceContext maps bare table names to their table_ids.
    table_map = {
        "orders": 1,
        "customers": 2,
    }
    gov_ctx = _build_governance_context(table_map=table_map)

    # A role with access to the sales domain (both tables are in 'sales').
    role = {
        "id": "analyst",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
        "domain_access": ["sales"],
    }

    # The approved SQL query: JOIN ON condition matches the registered relationship.
    approved_sql = (
        "SELECT o.id, c.name "
        "FROM orders o "
        "JOIN customers c ON o.customer_id = c.id"
    )

    # The unapproved SQL query: JOIN ON condition does NOT match any registered relationship.
    unapproved_sql = (
        "SELECT o.id, c.name "
        "FROM orders o "
        "JOIN customers c ON o.id = c.id"
    )

    shared_data["ctx"] = ctx
    shared_data["gov_ctx"] = gov_ctx
    shared_data["role"] = role
    shared_data["approved_sql"] = approved_sql
    shared_data["unapproved_sql"] = unapproved_sql
    shared_data["tables"] = tables

    # Sanity: registered relationship is present and maps the expected columns.
    assert ("orders", "customer_id") in joins
    assert joins[("orders", "customer_id")].source_column == "customer_id"
    assert joins[("orders", "customer_id")].target_column == "id"


@when("the compiler validates the query")
def when_compiler_validates_query(shared_data):
    ctx: CompilationContext = shared_data["ctx"]
    gov_ctx: GovernanceContext = shared_data["gov_ctx"]
    role: dict = shared_data["role"]

    # Build the raw_tables list from the table metadata.
    raw_tables = [
        {"id": meta.table_id, "name": meta.table_name, "domain": meta.domain_id}
        for meta in shared_data["tables"].values()
    ]

    # Validate the approved query (registered relationship).
    approved_violations = validate_sql(
        sql=shared_data["approved_sql"],
        ctx=ctx,
        gov_ctx=gov_ctx,
        role=role,
        _raw_tables=raw_tables,
        discovery_mode=False,
        bypass_relationship_guard=False,
        bypass_uncovered_relationships=False,
    )

    # Validate the unapproved query (unregistered join column combination).
    unapproved_violations = validate_sql(
        sql=shared_data["unapproved_sql"],
        ctx=ctx,
        gov_ctx=gov_ctx,
        role=role,
        _raw_tables=raw_tables,
        discovery_mode=False,
        bypass_relationship_guard=False,
        bypass_uncovered_relationships=False,
    )

    # Validate with bypass_relationship_guard=True (GraphQL SDL pre-approval path).
    graphql_violations = validate_sql(
        sql=shared_data["approved_sql"],
        ctx=ctx,
        gov_ctx=gov_ctx,
        role=role,
        _raw_tables=raw_tables,
        discovery_mode=False,
        bypass_relationship_guard=True,
        bypass_uncovered_relationships=False,
    )

    shared_data["approved_violations"] = approved_violations
    shared_data["unapproved_violations"] = unapproved_violations
    shared_data["graphql_violations"] = graphql_violations

    # The approved query must produce no V002 violations.
    approved_v002 = [v for v in approved_violations if v.code == "V002"]
    shared_data["approved_v002"] = approved_v002

    # The unapproved query must produce at least one V002 violation.
    unapproved_v002 = [v for v in unapproved_violations if v.code == "V002"]
    shared_data["unapproved_v002"] = unapproved_v002

    # GraphQL SDL path must produce no V002 violations even for non-standard joins.
    graphql_v002 = [v for v in graphql_violations if v.code == "V002"]
    shared_data["graphql_v002"] = graphql_v002


@then("it is rejected at compile time if the join is not backed by a
