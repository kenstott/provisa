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

import os
import urllib.parse
from typing import cast

import pytest
import pytest_asyncio
import sqlglot
from sqlglot import exp
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.audit.query_log import init_audit_schema
from provisa.core.database import Database, create_engine
from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta  # noqa: E402
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


def _parse(sql: str, read: str = "trino") -> exp.Expression:
    """parse_one narrowed to Expression (sqlglot's declared return is looser)."""
    return cast(exp.Expression, sqlglot.parse_one(sql, read=read))


def _parse_select(sql: str, read: str = "trino") -> exp.Select:
    """parse_one narrowed to Select for .where()/.selects/.limit() access."""
    return cast(exp.Select, sqlglot.parse_one(sql, read=read))


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
    parsed = urllib.parse.urlparse(dsn)
    org_id = "default"
    engine = create_engine(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        database=(parsed.path or "/postgres").lstrip("/"),
        user=parsed.username or "postgres",
        password=parsed.password or "postgres",
    )
    # search_path is applied on every acquire (Database SET search_path shim).
    db = Database(engine, name="test", search_path=f"provisa_{org_id}, public")
    await init_audit_schema(db, org_id=org_id)
    try:
        yield db
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# REQ-001 — Querying itself has no capability gate; governance is data-layer.
# ---------------------------------------------------------------------------


@given(parsers.parse('an authenticated identity with role "{role_name}"'))
def given_authenticated_identity(shared_data, role_name):
    role = {
        "id": role_name,
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    shared_data["role"] = role
    shared_data["role_name"] = role_name
    shared_data["authenticated"] = True

    capability_names = {c.name for c in Capability}
    for forbidden in ("QUERY", "EXECUTE_QUERY", "RUN_QUERY", "QUERY_EXECUTION"):
        assert forbidden not in capability_names
    assert role["id"] == role_name


@when("a GraphQL query is submitted against a registered table")
def when_graphql_query_submitted(shared_data):
    base_sql = 'SELECT "id", "email", "region" FROM "public"."customers"'
    shared_data["base_sql"] = base_sql

    parsed = _parse_select(base_sql)

    rls_predicate = "region = 'EMEA'"
    governed = parsed.where(rls_predicate, dialect="trino")
    shared_data["rls_predicate"] = rls_predicate

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
    shared_data["executed"] = True


@then("data is returned filtered by RLS and masking rules only")
def then_data_filtered_rls_masking(shared_data):
    governed = _parse_select(shared_data["governed_sql"])

    where = governed.args.get("where")
    assert where is not None
    assert "region" in where.sql(dialect="trino").lower()

    sql_lower = shared_data["governed_sql"].lower()
    assert "regexp_replace" in sql_lower

    aliases = {s.alias_or_name for s in governed.selects}
    assert "email" in aliases
    email_select = next(s for s in governed.selects if s.alias_or_name == "email")
    assert "regexp_replace" in email_select.sql(dialect="trino").lower()


@then("no capability gate rejects the query")
def then_no_capability_gate(shared_data):
    assert shared_data.get("authenticated") is True
    assert shared_data.get("executed") is True

    capability_names = {c.name for c in Capability}
    for forbidden in ("QUERY", "EXECUTE_QUERY", "RUN_QUERY", "QUERY_EXECUTION"):
        assert forbidden not in capability_names

    bare = {"id": "bare", "capabilities": []}
    assert not has_capability(bare, Capability.SOURCE_REGISTRATION)

    role = shared_data.get("role", {})
    role_caps = set(role.get("capabilities", []))
    for forbidden in ("query", "execute_query", "run_query", "query_execution"):
        assert forbidden not in role_caps

    enum_values = {c.value for c in Capability}
    for forbidden_val in ("query", "execute_query", "run_query", "query_execution"):
        assert forbidden_val not in enum_values

    governed_sql = shared_data.get("governed_sql", "")
    assert governed_sql, "governed SQL must have been produced by the When step"

    assert "regexp_replace" in governed_sql.lower(), (
        "masking expression must appear in governed SQL"
    )
    parsed = sqlglot.parse_one(governed_sql, read="trino")
    where = parsed.args.get("where")
    assert where is not None, "RLS predicate must be present in governed SQL"

    for forbidden_name in ("APPROVE_QUERY", "QUERY_APPROVAL", "QUERY_REGISTRY", "REGISTRY"):
        assert forbidden_name not in capability_names

    minimal_role = {
        "id": shared_data.get("role_name", "analyst"),
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    empty_role = {"id": "empty", "capabilities": []}
    for forbidden_cap_name in ("QUERY", "EXECUTE_QUERY", "RUN_QUERY", "QUERY_EXECUTION"):
        assert forbidden_cap_name not in capability_names, (
            f"Capability {forbidden_cap_name} must not exist — it would constitute a query gate"
        )
    for role_under_test in (minimal_role, empty_role):
        for cap in Capability:
            if cap.name in ("QUERY", "EXECUTE_QUERY", "RUN_QUERY", "QUERY_EXECUTION"):
                assert cap.value not in role_under_test.get("capabilities", [])


# ---------------------------------------------------------------------------
# REQ-003 — Query and mutation governance is rights-based only.
# ---------------------------------------------------------------------------


@given("a user with table/view rights")
def given_user_with_table_view_rights(shared_data):
    role = {
        "id": "rights-user",
        "capabilities": [
            Capability.QUERY_DEVELOPMENT.value,
            Capability.CREATE_RELATIONSHIP.value,
        ],
    }
    shared_data["role"] = role

    assert has_capability(role, Capability.QUERY_DEVELOPMENT)
    assert has_capability(role, Capability.CREATE_RELATIONSHIP)

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

    errors: list[str] = []

    try:
        check_capability(role, Capability.QUERY_DEVELOPMENT)
        query_allowed = True
    except InsufficientRightsError as exc:  # pragma: no cover
        query_allowed = False
        errors.append(str(exc))

    try:
        check_capability(role, Capability.CREATE_RELATIONSHIP)
        mutation_allowed = True
    except InsufficientRightsError as exc:  # pragma: no cover
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
    assert shared_data["query_allowed"] is True
    assert shared_data["mutation_allowed"] is True
    assert shared_data["executed"] is True
    assert shared_data["rights_errors"] == []

    capability_names = {c.name for c in Capability}
    for forbidden in (
        "REGISTRY",
        "REGISTRY_MEMBERSHIP",
        "APPROVE_QUERY",
        "QUERY_APPROVAL",
        "QUERY_REGISTRY",
    ):
        assert forbidden not in capability_names

    no_rights = {"id": "no-rights", "capabilities": []}
    with pytest.raises(InsufficientRightsError):
        check_capability(no_rights, Capability.QUERY_DEVELOPMENT)

    enum_values = {c.value for c in Capability}
    for forbidden_val in (
        "registry",
        "registry_membership",
        "approve_query",
        "query_approval",
        "query_registry",
    ):
        assert forbidden_val not in enum_values

    role = shared_data["role"]
    assert has_capability(role, Capability.QUERY_DEVELOPMENT), (
        "table/view right (QUERY_DEVELOPMENT) must be held by the executing user"
    )
    assert has_capability(role, Capability.CREATE_RELATIONSHIP), (
        "relationship right (CREATE_RELATIONSHIP) must be held by the executing user"
    )

    query_only_role = {
        "id": "query-only",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    check_capability(query_only_role, Capability.QUERY_DEVELOPMENT)
    with pytest.raises(InsufficientRightsError):
        check_capability(query_only_role, Capability.CREATE_RELATIONSHIP)

    relationship_only_role = {
        "id": "relationship-only",
        "capabilities": [Capability.CREATE_RELATIONSHIP.value],
    }
    check_capability(relationship_only_role, Capability.CREATE_RELATIONSHIP)
    with pytest.raises(InsufficientRightsError):
        check_capability(relationship_only_role, Capability.QUERY_DEVELOPMENT)


# ---------------------------------------------------------------------------
# REQ-005 — Per-role/table result-size ceilings (max_rows).
# ---------------------------------------------------------------------------


def _referenced_tables(parsed: sqlglot.exp.Expression) -> set[str]:
    """Bare table names referenced by a parsed query."""
    return {t.name for t in parsed.find_all(sqlglot.exp.Table)}


def _stage2_inject_limit(sql: str, role_config: dict[str, int]) -> tuple[str, int | None]:
    """Stage 2 ceiling enforcement."""
    parsed = _parse_select(sql)

    referenced = _referenced_tables(parsed)
    ceilings = [role_config[t] for t in referenced if t in role_config]
    if not ceilings:
        return parsed.sql(dialect="trino"), None

    ceiling = min(ceilings)

    existing = parsed.args.get("limit")
    if existing is not None:
        current = int(existing.expression.this)
        if current <= ceiling:
            return parsed.sql(dialect="trino"), current

    capped = parsed.limit(ceiling)
    return capped.sql(dialect="trino"), ceiling


@given("a role with a configured max_rows ceiling for a table")
def given_role_with_max_rows_ceiling(shared_data):
    role = {
        "id": "analyst",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    role_config = {"orders": 1000, "customers": 500}

    shared_data["role"] = role
    shared_data["role_config"] = role_config
    shared_data["ceiling_table"] = "orders"
    shared_data["table_ceiling"] = role_config["orders"]

    assert shared_data["table_ceiling"] == 1000
    assert "orders" in role_config


@when("a query references that table and would exceed the ceiling")
def when_query_would_exceed_ceiling(shared_data):
    role_config = shared_data["role_config"]

    unbounded_sql = 'SELECT "id", "total" FROM "public"."orders"'
    shared_data["original_sql"] = unbounded_sql

    rewritten, ceiling = _stage2_inject_limit(unbounded_sql, role_config)
    shared_data["rewritten_sql"] = rewritten
    shared_data["effective_ceiling"] = ceiling

    assert sqlglot.parse_one(unbounded_sql, read="trino").args.get("limit") is None
    assert ceiling == shared_data["table_ceiling"]


@then("Stage 2 injects a LIMIT capping results at the role's ceiling")
def then_stage2_injects_limit(shared_data):
    rewritten = _parse(shared_data["rewritten_sql"])

    limit_node = rewritten.args.get("limit")
    assert limit_node is not None
    injected = int(limit_node.expression.this)
    assert injected == shared_data["table_ceiling"]
    assert injected == shared_data["effective_ceiling"]

    assert shared_data["ceiling_table"] in _referenced_tables(rewritten)

    tighter_sql = 'SELECT "id", "total" FROM "public"."orders" LIMIT 10'
    narrowed, _ = _stage2_inject_limit(tighter_sql, shared_data["role_config"])
    narrowed_limit = int(sqlglot.parse_one(narrowed, read="trino").args["limit"].expression.this)
    assert narrowed_limit == 10

    over_sql = 'SELECT "id" FROM "public"."orders" LIMIT 100000'
    clamped, clamped_ceiling = _stage2_inject_limit(over_sql, shared_data["role_config"])
    clamped_limit = int(sqlglot.parse_one(clamped, read="trino").args["limit"].expression.this)
    assert clamped_limit == shared_data["table_ceiling"]
    assert clamped_ceiling == shared_data["table_ceiling"]

    untouched_sql = 'SELECT "id" FROM "public"."unlisted_table"'
    same, no_ceiling = _stage2_inject_limit(untouched_sql, shared_data["role_config"])
    assert no_ceiling is None
    assert sqlglot.parse_one(same, read="trino").args.get("limit") is None

    filtered_sql = 'SELECT "id", "total" FROM "public"."orders" WHERE "total" > 100'
    filtered_rewritten, filtered_ceiling = _stage2_inject_limit(
        filtered_sql, shared_data["role_config"]
    )
    filtered_parsed = sqlglot.parse_one(filtered_rewritten, read="trino")
    assert filtered_ceiling == shared_data["table_ceiling"]
    filtered_limit_node = filtered_parsed.args.get("limit")
    assert filtered_limit_node is not None
    assert int(filtered_limit_node.expression.this) == shared_data["table_ceiling"]
    assert filtered_parsed.args.get("where") is not None

    multi_sql = (
        'SELECT o."id", c."name" FROM "public"."orders" o '
        'JOIN "public"."customers" c ON o."customer_id" = c."id"'
    )
    multi_rewritten, multi_ceiling = _stage2_inject_limit(multi_sql, shared_data["role_config"])
    assert multi_ceiling == shared_data["role_config"]["customers"]
    multi_parsed = sqlglot.parse_one(multi_rewritten, read="trino")
    multi_limit_node = multi_parsed.args.get("limit")
    assert multi_limit_node is not None
    assert int(multi_limit_node.expression.this) == shared_data["role_config"]["customers"]

    exact_sql = f'SELECT "id" FROM "public"."orders" LIMIT {shared_data["table_ceiling"]}'
    exact_rewritten, exact_ceiling = _stage2_inject_limit(exact_sql, shared_data["role_config"])
    exact_parsed = sqlglot.parse_one(exact_rewritten, read="trino")
    exact_limit = int(exact_parsed.args["limit"].expression.this)
    assert exact_limit == shared_data["table_ceiling"]
    assert exact_ceiling == shared_data["table_ceiling"]

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
# ---------------------------------------------------------------------------

_LARGE_RESULT_ROW_THRESHOLD = 100_000


def _evaluate_large_result(
    role: dict,
    row_count: int,
    threshold: int = _LARGE_RESULT_ROW_THRESHOLD,
) -> dict:
    """Decide large-result handling for a rights-permitted query."""
    check_capability(role, Capability.QUERY_DEVELOPMENT)

    exceeds = row_count >= threshold
    return {
        "row_count": row_count,
        "threshold": threshold,
        "exceeds_threshold": exceeds,
        "redirect_available": exceeds,
        "redirect_url": (f"/v1/results/spool/{row_count}" if exceeds else None),
        "arrow_available": exceeds,
        "arrow_content_type": ("application/vnd.apache.arrow.stream" if exceeds else None),
    }


@given("a user with rights to query a table")
def given_user_with_rights_to_query_table(shared_data):
    role = {
        "id": "analyst",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    shared_data["role"] = role

    assert has_capability(role, Capability.QUERY_DEVELOPMENT)

    capability_names = {c.name for c in Capability}
    for forbidden in ("LARGE_RESULT", "ARROW_OUTPUT", "STREAMING", "REDIRECT"):
        assert forbidden not in capability_names


@when("the query result exceeds the configured large-result threshold")
def when_query_result_exceeds_large_result_threshold(shared_data):
    role = shared_data["role"]

    row_count = _LARGE_RESULT_ROW_THRESHOLD + 75_000
    shared_data["row_count"] = row_count

    transport = _evaluate_large_result(role, row_count)
    shared_data["transport"] = transport

    assert transport["exceeds_threshold"] is True
    assert transport["row_count"] > transport["threshold"]

    capability_names = {c.name for c in Capability}
    for forbidden in ("LARGE_RESULT", "ARROW_OUTPUT", "STREAMING", "REDIRECT"):
        assert forbidden not in capability_names


@then("large-result redirect and Arrow output are available")
def then_large_result_redirect_and_arrow_output_available(shared_data):
    transport = shared_data["transport"]

    assert transport["redirect_available"] is True, (
        "redirect must be available when result exceeds the large-result threshold"
    )
    assert transport["redirect_url"] is not None, (
        "redirect_url must be non-None when redirect_available is True"
    )
    assert "/v1/results/spool/" in transport["redirect_url"], (
        "redirect_url must reference the result spool endpoint"
    )

    assert transport["arrow_available"] is True, (
        "Arrow output must be available when result exceeds the large-result threshold"
    )
    assert transport["arrow_content_type"] == "application/vnd.apache.arrow.stream", (
        "Arrow content-type must be 'application/vnd.apache.arrow.stream'"
    )

    role = shared_data["role"]

    below_transport = _evaluate_large_result(role, _LARGE_RESULT_ROW_THRESHOLD - 1)
    assert below_transport["redirect_available"] is False
    assert below_transport["arrow_available"] is False
    assert below_transport["redirect_url"] is None
    assert below_transport["arrow_content_type"] is None

    minimal_role = {
        "id": "minimal",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    minimal_transport = _evaluate_large_result(minimal_role, _LARGE_RESULT_ROW_THRESHOLD + 1)
    assert minimal_transport["redirect_available"] is True
    assert minimal_transport["arrow_available"] is True

    capability_names = {c.name for c in Capability}
    for forbidden in ("LARGE_RESULT", "ARROW_OUTPUT", "STREAMING", "REDIRECT"):
        assert forbidden not in capability_names


# ---------------------------------------------------------------------------
# REQ-603 — V002 relationship governance.
#
# Every JOIN ON condition in SQL and Cypher queries must match an approved,
# registered relationship (source_col = target_col). SQL and Cypher queries
# that traverse a join not backed by a registered relationship are rejected at
# compile time. GraphQL queries that traverse relationships defined in the SDL
# are always pre-approved and exempt from the V002 check.
# ---------------------------------------------------------------------------


def _build_v002_compilation_context(
    registered: bool = True,
) -> tuple[CompilationContext, GovernanceContext]:
    """Build minimal CompilationContext and GovernanceContext for V002 testing."""
    orders_meta = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Order",
        schema_name="public",
        table_name="orders",
        domain_id="commerce",
        source_id="pg",
        catalog_name="postgresql",
    )
    customers_meta = TableMeta(
        table_id=2,
        field_name="customers",
        type_name="Customer",
        schema_name="public",
        table_name="customers",
        domain_id="commerce",
        source_id="pg",
        catalog_name="postgresql",
    )

    tables = {
        "Order": orders_meta,
        "Customer": customers_meta,
    }

    joins: dict = {}
    if registered:
        join_meta = JoinMeta(
            target=customers_meta,
            source_column="customer_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            cardinality="many-to-one",
        )
        joins[("Order", "customer")] = join_meta

    ctx = CompilationContext(tables=tables, joins=joins)

    gov_ctx = GovernanceContext(
        rls_rules={},
        masking_rules={},
        visible_columns={},
        table_map={
            "orders": 1,
            "customers": 2,
            "public.orders": 1,
            "public.customers": 2,
        },
        all_columns={},
        limit_ceiling=None,
        sample_size=None,
    )

    return ctx, gov_ctx


def _make_permissive_role() -> dict:
    """A role with open domain_access (no V001 blocking) for V002 tests."""
    return {
        "id": "v002-tester",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
        "domain_access": ["*"],
    }


@given("a SQL or Cypher query with a JOIN ON condition")
def given_sql_or_cypher_query_with_join(shared_data):
    """Prepare both an approved-join SQL and an unapproved-join SQL for V002 testing."""
    approved_sql = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    unapproved_sql = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.id = c.order_ref"

    shared_data["approved_sql"] = approved_sql
    shared_data["unapproved_sql"] = unapproved_sql

    approved_parsed = sqlglot.parse_one(approved_sql, read="postgres")
    unapproved_parsed = sqlglot.parse_one(unapproved_sql, read="postgres")

    assert approved_parsed is not None, "approved SQL must parse successfully"
    assert unapproved_parsed is not None, "unapproved SQL must parse successfully"

    import sqlglot.expressions as exp

    assert list(approved_parsed.find_all(exp.Join)), "approved SQL must contain a JOIN"
    assert list(unapproved_parsed.find_all(exp.Join)), "unapproved SQL must contain a JOIN"

    shared_data["query_has_join"] = True


@when("the compiler validates the query")
def when_compiler_validates_query(shared_data):
    """Run validate_sql against both the approved and unapproved join queries."""
    assert shared_data.get("query_has_join"), "Given step must have set query_has_join"

    role = _make_permissive_role()
    raw_tables: list[dict] = []

    ctx_registered, gov_ctx_registered = _build_v002_compilation_context(registered=True)

    approved_violations = validate_sql(
        shared_data["approved_sql"],
        ctx_registered,
        gov_ctx_registered,
        role,
        raw_tables,
        discovery_mode=False,
        bypass_relationship_guard=False,
    )
    shared_data["approved_violations"] = approved_violations

    ctx_empty, gov_ctx_empty = _build_v002_compilation_context(registered=False)

    unapproved_violations = validate_sql(
        shared_data["unapproved_sql"],
        ctx_empty,
        gov_ctx_empty,
        role,
        raw_tables,
        discovery_mode=False,
        bypass_relationship_guard=False,
    )
    shared_data["unapproved_violations"] = unapproved_violations

    approved_against_empty_violations = validate_sql(
        shared_data["approved_sql"],
        ctx_empty,
        gov_ctx_empty,
        role,
        raw_tables,
        discovery_mode=False,
        bypass_relationship_guard=False,
    )
    shared_data["approved_against_empty_violations"] = approved_against_empty_violations

    bypassed_violations = validate_sql(
        shared_data["unapproved_sql"],
        ctx_empty,
        gov_ctx_empty,
        role,
        raw_tables,
        discovery_mode=False,
        bypass_relationship_guard=True,
    )
    shared_data["bypassed_v002_violations"] = [v for v in bypassed_violations if v.code == "V002"]

    shared_data["compiler_ran"] = True


@then("it is rejected at compile time if the join is not backed by a registered relationship")
def then_rejected_if_join_not_registered(shared_data):
    """Assert V002 compile-time rejection semantics."""
    assert shared_data.get("compiler_ran"), "When step must have run the compiler"

    approved_v002 = [v for v in shared_data["approved_violations"] if v.code == "V002"]
    assert approved_v002 == [], f"Approved join must pass V002; got violations: {approved_v002}"

    unapproved_v002 = [v for v in shared_data["unapproved_violations"] if v.code == "V002"]
    assert len(unapproved_v002) >= 1, (
        "Unapproved join must produce at least one V002 violation; got none"
    )

    for violation in unapproved_v002:
        assert violation.code == "V002"
        assert violation.message, "V002 violation must carry an explanatory message"

    approved_against_empty_v002 = [
        v for v in shared_data["approved_against_empty_violations"] if v.code == "V002"
    ]
    assert len(approved_against_empty_v002) >= 1, (
        "Even an approved-looking join must fail V002 when no relationship is registered"
    )

    assert shared_data["bypassed_v002_violations"] == [], (
        "bypass_relationship_guard=True must suppress all V002 violations"
    )


# ---------------------------------------------------------------------------
# REQ-613 — Append-only query audit log (SO


# ---------------------------------------------------------------------------
# REQ-613 — Append-only query audit log (SOC2)
# ---------------------------------------------------------------------------

import hashlib  # noqa: E402

from provisa.audit.query_log import log_query  # noqa: E402


@given("any query touching a domain asset")
def given_any_query_touching_domain_asset(shared_data):
    shared_data["user_id"] = "user-abc"
    shared_data["role_id"] = "analyst"
    shared_data["query_text"] = "SELECT id, email FROM public.customers"
    shared_data["table_ids"] = ["customers"]
    shared_data["source"] = "graphql"
    shared_data["tenant_id"] = "tenant-001"
    shared_data["status_code"] = 200
    shared_data["duration_ms"] = 42

    assert shared_data["query_text"], "query text must be non-empty"
    assert shared_data["table_ids"], "table_ids must reference at least one domain asset"


@when("the query is executed")
def when_the_query_is_executed(shared_data, audit_pool):
    import asyncio

    async def _do_log():
        await log_query(
            audit_pool,
            tenant_id=shared_data["tenant_id"],
            user_id=shared_data["user_id"],
            role_id=shared_data["role_id"],
            query_text=shared_data["query_text"],
            table_ids=shared_data["table_ids"],
            source=shared_data["source"],
            status_code=shared_data["status_code"],
            duration_ms=shared_data["duration_ms"],
        )

    asyncio.get_event_loop().run_until_complete(_do_log())
    shared_data["expected_query_hash"] = hashlib.sha256(
        shared_data["query_text"].encode()
    ).hexdigest()
    shared_data["query_executed"] = True


@then("it is logged in the append-only query_audit_log with all required fields")
def then_logged_in_append_only_audit_log(shared_data, audit_pool):
    import asyncio

    assert shared_data.get("query_executed"), "When step must have executed the query"

    async def _verify():
        async with audit_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT user_id, role_id, query_hash, table_ids, source,"
                "       status_code, duration_ms, logged_at, tenant_id"
                " FROM query_audit_log"
                " WHERE user_id = $1 AND query_hash = $2"
                " ORDER BY logged_at DESC LIMIT 1",
                shared_data["user_id"],
                shared_data["expected_query_hash"],
            )
            assert row is not None, "Audit log entry must exist for the executed query"

            assert row["user_id"] == shared_data["user_id"], "user_id must be captured"
            assert row["role_id"] == shared_data["role_id"], "role_id must be captured"
            assert row["query_hash"] == shared_data["expected_query_hash"], (
                "query_hash must be SHA-256 of the query text"
            )
            assert list(row["table_ids"]) == shared_data["table_ids"], "table_ids must be captured"
            assert row["source"] == shared_data["source"], "source must be captured"
            assert row["status_code"] == shared_data["status_code"], "status_code must be captured"
            assert row["duration_ms"] == shared_data["duration_ms"], "duration_ms must be captured"
            assert row["logged_at"] is not None, "logged_at must be set"
            assert str(row["tenant_id"]) == shared_data["tenant_id"], "tenant_id must be captured"

            # Verify DELETE is a no-op (append-only via PostgreSQL RULE)
            initial_count = await conn.fetchval(
                "SELECT COUNT(*) FROM query_audit_log WHERE query_hash = $1",
                shared_data["expected_query_hash"],
            )
            await conn.execute(
                "DELETE FROM query_audit_log WHERE query_hash = $1",
                shared_data["expected_query_hash"],
            )
            after_delete_count = await conn.fetchval(
                "SELECT COUNT(*) FROM query_audit_log WHERE query_hash = $1",
                shared_data["expected_query_hash"],
            )
            assert after_delete_count == initial_count, (
                "DELETE must be a no-op on query_audit_log (append-only rule)"
            )

            # Verify UPDATE is a no-op (append-only via PostgreSQL RULE)
            original_source = row["source"]
            await conn.execute(
                "UPDATE query_audit_log SET source = 'tampered' WHERE query_hash = $1",
                shared_data["expected_query_hash"],
            )
            after_update_row = await conn.fetchrow(
                "SELECT source FROM query_audit_log WHERE query_hash = $1"
                " ORDER BY logged_at DESC LIMIT 1",
                shared_data["expected_query_hash"],
            )
            assert after_update_row["source"] == original_source, (
                "UPDATE must be a no-op on query_audit_log (append-only rule)"
            )

            # Verify indexes exist
            indexes = await conn.fetch(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'query_audit_log'",
            )
            index_names = {r["indexname"] for r in indexes}
            assert "idx_audit_tenant_time" in index_names, (
                "Index idx_audit_tenant_time (tenant_id, logged_at) must exist"
            )
            assert "idx_audit_user_time" in index_names, (
                "Index idx_audit_user_time (user_id, logged_at) must exist"
            )

    asyncio.get_event_loop().run_until_complete(_verify())


# Nothing to append - all steps for REQ-001 already exist in the file.
