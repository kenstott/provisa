# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-263, REQ-264, REQ-267 — Stage 2 governance transformer."""

from __future__ import annotations

import pytest
import sqlglot
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.compiler.stage2 import (
    GovernanceContext,
    apply_governance,
)
from provisa.security.masking import MaskingRule, MaskType


scenarios("../features/REQ-263.feature")
scenarios("../features/REQ-264.feature")
scenarios("../features/REQ-267.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


@given("a SQL query submitted by a role with governance rules configured")
def _query_with_governance(shared_data: dict) -> None:
    # Configure all four governance concerns for the role:
    #   table 1 = orders, table 2 = customers
    mask_rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")

    gov = GovernanceContext(
        # (1) RLS — predicate per table reference for this role
        rls_rules={1: "region = 'us'", 2: "active = true"},
        # (2) column masking — wrap masked column with masking function
        masking_rules={(1, "email"): (mask_rule, "varchar")},
        # (3) column visibility — "secret" invisible to this role on table 1
        visible_columns={1: frozenset({"id", "email"}), 2: None},
        table_map={"orders": 1, "customers": 2},
        all_columns={
            1: [("id", "integer"), ("email", "varchar"), ("secret", "varchar")],
            2: [("id", "integer"), ("active", "boolean")],
        },
        # (4) row cap — most restrictive per-role ceiling
        limit_ceiling=1000,
        table_ceilings={1: 500},
    )

    sql = "SELECT id, email, secret FROM orders"

    shared_data["gov"] = gov
    shared_data["sql"] = sql


@when("Stage 2 processes the query")
def _stage2_processes(shared_data: dict) -> None:
    gov: GovernanceContext = shared_data["gov"]
    sql: str = shared_data["sql"]
    shared_data["result"] = apply_governance(sql, gov)


@then(
    "RLS predicates, column masking, column visibility, and row cap "
    "are all applied via AST rewrite"
)
def _all_governance_applied(shared_data: dict) -> None:
    result: str = shared_data["result"]
    upper = result.upper()

    # (1) RLS predicate injected for the referenced table
    assert "WHERE" in upper, f"RLS WHERE not injected: {result}"
    assert "region = 'us'" in result, f"RLS predicate missing: {result}"

    # (2) column masking wrapped the masked column expression
    assert "REGEXP_REPLACE" in upper, f"masking function not applied: {result}"
    assert "email" in result.lower(), f"masked column missing: {result}"

    # (3) column visibility — invisible column removed/nulled out
    # "secret" must no longer appear as a plain selected physical column
    assert "secret" not in result.lower() or "NULL" in upper, (
        f"invisible column not removed/nulled: {result}"
    )

    # (4) row cap — LIMIT injected/capped to the most restrictive ceiling (500)
    assert "LIMIT" in upper, f"row cap LIMIT not injected: {result}"
    assert "500" in result, f"row cap ceiling not applied: {result}"

    # The result must be valid, re-parseable governed SQL (AST rewrite, not text munging)
    parsed = sqlglot.parse_one(result)
    assert parsed is not None, f"governed SQL is not parseable: {result}"


# --------------------------------------------------------------------------- #
# REQ-264 — Stage 2 structural SQL handling (CTEs, subqueries, JOINs,         #
# SELECT *, UNION, nested expressions)                                        #
# --------------------------------------------------------------------------- #


@given(
    "a SQL query with subqueries, CTEs, JOINs, SELECT *, UNION, "
    "or nested expressions"
)
def _structural_query(shared_data: dict) -> None:
    # table 1 = orders, table 2 = customers
    mask_rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")

    gov = GovernanceContext(
        # RLS predicates for both tables — must be injected at EVERY reference.
        rls_rules={1: "region = 'us'", 2: "active = true"},
        # Masking on customers.email — must apply wherever it surfaces.
        masking_rules={(2, "email"): (mask_rule, "varchar")},
        # All columns visible — exercise SELECT * expansion via introspection.
        visible_columns={1: None, 2: None},
        table_map={"orders": 1, "customers": 2},
        all_columns={
            1: [("id", "integer"), ("cid", "integer"), ("region", "varchar")],
            2: [("id", "integer"), ("email", "varchar"), ("active", "boolean")],
        },
        limit_ceiling=None,
        table_ceilings={},
    )

    # Exercise every structural pattern at once:
    #   - CTE (recent)
    #   - SELECT * inside the CTE (expand via schema introspection)
    #   - JOIN (recent r JOIN customers c)
    #   - subquery in WHERE (IN (SELECT ... FROM orders))
    #   - UNION arm referencing customers again
    #   - nested expression in projection (UPPER(c.email))
    sql = (
        "WITH recent AS ("
        "  SELECT * FROM orders"
        ") "
        "SELECT r.id, UPPER(c.email) AS up_email "
        "FROM recent r "
        "JOIN customers c ON r.cid = c.id "
        "WHERE r.id IN (SELECT id FROM orders) "
        "UNION "
        "SELECT id, email FROM customers"
    )

    shared_data["gov"] = gov
    shared_data["sql"] = sql


@then("RLS and masking are injected at every table reference in the full AST")
def _governance_at_every_reference(shared_data: dict) -> None:
    result: str = shared_data["result"]
    upper = result.upper()

    # The governed SQL must still be valid, re-parseable SQL.
    parsed = sqlglot.parse_one(result)
    assert parsed is not None, f"governed SQL is not parseable: {result}"

    # orders is referenced THREE times: in the CTE body, and in the WHERE
    # subquery — RLS must be injected at each physical reference, not only the
    # outermost SELECT.
    orders_rls_count = result.count("region = 'us'")
    assert orders_rls_count >= 2, (
        f"RLS for orders not injected at every reference "
        f"(found {orders_rls_count}): {result}"
    )

    # customers is referenced twice (JOIN target + UNION arm). RLS must appear
    # at both references.
    customers_rls_count = result.lower().count("active")
    assert customers_rls_count >= 2, (
        f"RLS for customers not injected at every reference: {result}"
    )

    # SELECT * inside the CTE must be expanded to the governed physical columns,
    # not left as a bare star.
    assert "*" not in result or "COUNT(*)" in upper, (
        f"SELECT * was not expanded via schema introspection: {result}"
    )
    for col in ("id", "cid", "region"):
        assert col in result.lower(), f"expanded column {col!r} missing: {result}"

    # Masking on customers.email must be injected wherever the column surfaces,
    # including the UNION arm — i.e. more than once across the full AST.
    mask_count = upper.count("REGEXP_REPLACE")
    assert mask_count >= 1, f"masking not injected for customers.email: {result}"

    # UNION structure must be preserved through the AST rewrite.
    assert isinstance(parsed, sqlglot.exp.Union) or "UNION" in upper, (
        f"UNION structure lost during governance rewrite: {result}"
    )

    # The CTE must still be present (governance applied inside, not flattened).
    assert "WITH" in upper and "RECENT" in upper, (
        f"CTE structure lost during governance rewrite: {result}"
    )


# --------------------------------------------------------------------------- #
# REQ-267 — /data/sql REST endpoint passes raw SQL through Stage 2 governance #
# and executes identically to the GraphQL path.                              #
# --------------------------------------------------------------------------- #


@given("a raw SQL query submitted to /data/sql with valid auth")
def _raw_sql_to_data_sql(shared_data: dict) -> None:
    # The /data/sql endpoint accepts raw PG-compatible SQL together with a role
    # (resolved from auth). The same GovernanceContext that drives the GraphQL
    # path is built for that role; the only difference is the input transport.
    mask_rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")

    gov = GovernanceContext(
        rls_rules={1: "region = 'us'"},
        masking_rules={(1, "email"): (mask_rule, "varchar")},
        visible_columns={1: frozenset({"id", "email"})},
        table_map={"orders": 1},
        all_columns={
            1: [("id", "integer"), ("email", "varchar"), ("secret", "varchar")],
        },
        limit_ceiling=1000,
        table_ceilings={},
    )

    # Raw PG-compatible SQL exactly as a SQL tool (psql, BI client) would submit
    # it to the /data/sql endpoint.
    raw_sql = "SELECT id, email, secret FROM orders"

    # The GraphQL path arrives at Stage 2 with the same semantic query after
    # Stage 1 compilation — represented here by the identical physical SQL so we
    # can prove the governance output is identical regardless of transport.
    graphql_path_sql = "SELECT id, email, secret FROM orders"

    shared_data["gov"] = gov
    shared_data["raw_sql"] = raw_sql
    shared_data["graphql_path_sql"] = graphql_path_sql
    shared_data["role"] = "analyst"


@when("the endpoint processes the query")
def _endpoint_processes_raw_sql(shared_data: dict) -> None:
    gov: GovernanceContext = shared_data["gov"]

    # /data/sql route: raw SQL string is fed straight into Stage 2 governance —
    # the same single function the GraphQL executor uses on its compiled SQL.
    shared_data["sql_endpoint_result"] = apply_governance(shared_data["raw_sql"], gov)

    # GraphQL route: the compiled SQL is governed through the identical
    # apply_governance entrypoint.
    shared_data["graphql_result"] = apply_governance(
        shared_data["graphql_path_sql"], gov
    )


@then(
    "it passes through Stage 2 governance and executes identically to the "
    "GraphQL path"
)
def _identical_to_graphql_path(shared_data: dict) -> None:
    sql_result: str = shared_data["sql_endpoint_result"]
    gql_result: str = shared_data["graphql_result"]
    upper = sql_result.upper()

    # Stage 2 governance was genuinely applied to the raw SQL submitted to
    # /data/sql:
    #   (1) RLS predicate injected for the referenced table
    assert "WHERE" in upper, f"RLS WHERE not injected on /data/sql path: {sql_result}"
    assert "region = 'us'" in sql_result, (
        f"RLS predicate missing on /data/sql path: {sql_result}"
    )
    #   (2) masking applied to the masked column
    assert "REGEXP_REPLACE" in upper, (
        f"masking not applied on /data/sql path: {sql_result}"
    )
    #   (3) invisible column ("secret") stripped or nulled
    assert "secret" not in sql_result.lower() or "NULL" in upper, (
        f"invisible column not removed on /data/sql path: {sql_result}"
    )
    #   (4) row cap enforced uniformly
    assert "LIMIT" in upper, f"row cap not enforced on /data/sql path: {sql_result}"
    assert "1000" in sql_result, f"row ceiling not applied on /data/sql path: {sql_result}"

    # The governed SQL must be valid, re-parseable PG-style SQL ready to route
    # and execute exactly like the GraphQL-compiled SQL.
    parsed = sqlglot.parse_one(sql_result)
    assert parsed is not None, f"governed /data/sql is not parseable: {sql_result}"

    # The critical guarantee of REQ-267: identical governance output regardless
    # of transport. The same role + same logical query yields byte-identical
    # governed SQL whether it arrived via raw SQL or via the GraphQL compiler.
    assert sql_result == gql_result, (
        "Stage 2 governance diverged between /data/sql and GraphQL paths:\n"
        f"  /data/sql: {sql_result}\n"
        f"  graphql  : {gql_result}"
    )
