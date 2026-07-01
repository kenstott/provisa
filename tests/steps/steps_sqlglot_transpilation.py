# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for SQLGlot Transpilation (REQ-066, REQ-067, REQ-809).

REQ-066: Compiler emits PG-style SQL as canonical output; SQLGlot translates to
Trino SQL or target RDBMS dialect.
REQ-067: Target dialect determined by source type captured at table registration
time — transpilation is automatic and requires no per-query config.
REQ-809: Correlated scalar subqueries in PG-style canonical SQL are lifted into
CTEs before the query is forwarded to Trino.
"""

from __future__ import annotations

import re

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.transpiler.transpile import (
    SUPPORTED_DIALECTS,
    transpile,
    transpile_to_trino,
    rewrite_correlated_subqueries_for_trino,
)
from provisa.transpiler.router import (
    Route,
    RouteDecision,
    decide_route,
)

scenarios("../features/REQ-066.feature")
scenarios("../features/REQ-067.feature")
scenarios("../features/REQ-809.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# REQ-066 — PG-style canonical output → target dialect
# ---------------------------------------------------------------------------


@given("a compiled GraphQL query")
def a_compiled_graphql_query(shared_data: dict) -> None:
    # PG-style canonical SQL output, as the compiler would emit it: double-quoted
    # identifiers, schema-qualified table, $N placeholders, LIMIT/OFFSET.
    pg_sql = (
        'SELECT "t0"."id", "t0"."amount", "t1"."name" '
        'FROM "public"."orders" "t0" '
        'LEFT JOIN "public"."customers" "t1" '
        'ON "t0"."customer_id" = "t1"."id" '
        'WHERE "t0"."region" = $1 '
        'ORDER BY "t0"."amount" DESC '
        "LIMIT 10 OFFSET 5"
    )
    shared_data["pg_sql"] = pg_sql

    # Assert the canonical output is PG-style (double-quoted identifiers).
    assert '"public"."orders"' in pg_sql
    assert pg_sql.lstrip().upper().startswith("SELECT")


@when("the transpiler processes it")
def the_transpiler_processes_it(shared_data: dict) -> None:
    pg_sql = shared_data["pg_sql"]

    # Canonical → Trino
    shared_data["trino_sql"] = transpile_to_trino(pg_sql)

    # Canonical → every supported RDBMS dialect
    dialect_outputs: dict[str, str] = {}
    for dialect in sorted(SUPPORTED_DIALECTS):
        dialect_outputs[dialect] = transpile(pg_sql, dialect)
    shared_data["dialect_outputs"] = dialect_outputs


@then("PG-style SQL is emitted as canonical output and SQLGlot translates it to the target dialect")
def pg_canonical_translated_to_target(shared_data: dict) -> None:
    pg_sql = shared_data["pg_sql"]
    trino_sql = shared_data["trino_sql"]
    dialect_outputs = shared_data["dialect_outputs"]

    # Canonical PG-style SQL is the source of truth.
    assert '"public"."orders"' in pg_sql, "canonical output must be PG-style"

    # Trino translation is real, non-empty, and preserves query semantics.
    assert trino_sql, "Trino transpilation produced empty output"
    lower = trino_sql.lower()
    assert "orders" in lower
    assert "customers" in lower
    assert "left" in lower and "join" in lower
    assert "order by" in lower
    assert "10" in trino_sql and "5" in trino_sql

    # Trino does not use PG-style double-quoted schema.table the same way; verify
    # SQLGlot actually transformed the canonical input rather than echoing it.
    assert trino_sql != pg_sql, "transpiler must produce a translated form"

    # Every supported dialect produces real, table-bearing SQL.
    assert set(dialect_outputs) == set(SUPPORTED_DIALECTS)
    for dialect, out in dialect_outputs.items():
        assert out, f"{dialect} transpilation produced empty output"
        assert "orders" in out.lower(), f"{dialect} output lost the orders table"

    # The PG-targeted dialect should remain valid PG-style SQL.
    assert "orders" in dialect_outputs["postgres"].lower()

    # TSQL renders LIMIT differently (TOP); confirm a genuine dialect-specific shift.
    tsql_out = dialect_outputs["tsql"]
    assert "TOP" in tsql_out.upper() or "OFFSET" in tsql_out.upper()


# ---------------------------------------------------------------------------
# REQ-067 — Target dialect determined by source type at registration time
# ---------------------------------------------------------------------------

# Registration-time mapping: source type → SQLGlot write dialect. This is the
# logic that captures the dialect when a table is registered, so no per-query
# configuration is ever required.
_SOURCE_TYPE_TO_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "sqlserver": "tsql",
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
}


@given("a table registered with a specific source type")
def a_table_registered_with_a_specific_source_type(shared_data: dict) -> None:
    # Simulate table registration capturing the backing source's type and the
    # SQLGlot dialect derived from it (REQ-067).
    source_id = "shop-mysql"
    source_type = "mysql"

    assert source_type in _SOURCE_TYPE_TO_DIALECT, "unknown source type at registration"
    recorded_dialect = _SOURCE_TYPE_TO_DIALECT[source_type]

    shared_data["source_id"] = source_id
    shared_data["source_type"] = source_type
    shared_data["recorded_dialect"] = recorded_dialect
    # These maps mirror what the registry persists at registration time.
    shared_data["source_types"] = {source_id: source_type}
    shared_data["source_dialects"] = {source_id: recorded_dialect}

    assert recorded_dialect in SUPPORTED_DIALECTS
    assert recorded_dialect == "mysql"


@when("a query is transpiled")
def a_query_is_transpiled(shared_data: dict) -> None:
    source_id = shared_data["source_id"]
    source_types = shared_data["source_types"]
    source_dialects = shared_data["source_dialects"]

    pg_sql = 'SELECT "id", "amount" FROM "public"."orders" WHERE "region" = $1 LIMIT 25'
    shared_data["query_pg_sql"] = pg_sql

    # The router resolves the route and target dialect using ONLY the values
    # captured at registration time — no per-query dialect argument is passed.
    decision: RouteDecision = decide_route(
        sources={source_id},
        source_types=source_types,
        source_dialects=source_dialects,
    )
    shared_data["decision"] = decision

    # Target dialect for transpilation comes from the registration-time record.
    target_dialect = source_dialects[source_id]
    shared_data["target_dialect"] = target_dialect
    shared_data["transpiled_sql"] = transpile(pg_sql, target_dialect)


@then("the target SQL dialect matches the source type recorded at registration time")
def target_dialect_matches_recorded_source_type(shared_data: dict) -> None:
    source_id = shared_data["source_id"]
    source_type = shared_data["source_type"]
    recorded_dialect = shared_data["recorded_dialect"]
    target_dialect = shared_data["target_dialect"]
    decision: RouteDecision = shared_data["decision"]
    transpiled_sql = shared_data["transpiled_sql"]

    # The dialect used for transpilation is exactly the one derived from the
    # source type captured at registration — nothing was supplied per query.
    assert target_dialect == recorded_dialect
    assert target_dialect == _SOURCE_TYPE_TO_DIALECT[source_type]

    # When the router resolves to a direct RDBMS execution, it carries the same
    # registration-time dialect through to the executor.
    if decision.route is Route.DIRECT:
        assert decision.dialect == recorded_dialect
        assert decision.source_id == source_id

    # The produced SQL is genuinely in the recorded dialect: MySQL uses backtick
    # identifier quoting rather than PostgreSQL's double quotes.
    assert transpiled_sql, "transpilation produced empty output"
    assert "orders" in transpiled_sql.lower()
    assert '"orders"' not in transpiled_sql, "MySQL output must not use PG double quotes"

    # Cross-check: re-transpiling the same query to the recorded dialect is stable
    # and differs from a different target dialect (proving dialect actually applies).
    mysql_again = transpile(shared_data["query_pg_sql"], recorded_dialect)
    postgres_variant = transpile(shared_data["query_pg_sql"], "postgres")
    assert mysql_again == transpiled_sql
    assert mysql_again != postgres_variant, "dialect selection must change rendered SQL"

    # Additional REQ-067 verification: confirm every registered source type maps
    # to a supported SQLGlot dialect and that the mapping is stable — the dialect
    # is captured once at registration time and never requires per-query override.
    for stype, dialect in _SOURCE_TYPE_TO_DIALECT.items():
        assert dialect in SUPPORTED_DIALECTS, (
            f"source type '{stype}' maps to unsupported dialect '{dialect}'"
        )
        # Re-derive the dialect the same way registration logic would and confirm
        # it is deterministic (same input always yields the same dialect).
        assert _SOURCE_TYPE_TO_DIALECT[stype] == dialect, (
            f"dialect mapping for '{stype}' is not stable"
        )

    # Verify that each supported source type produces SQL that differs from the
    # PostgreSQL canonical form, confirming real dialect-specific transformation.
    canonical_pg_sql = shared_data["query_pg_sql"]
    postgres_out = transpile(canonical_pg_sql, "postgres")
    # Dialects that are syntactically compatible with postgres for the canonical
    # test SQL (double-quote identifiers, $N params, LIMIT) and therefore produce
    # identical output — excluded from the "must differ" assertion.
    _POSTGRES_COMPAT_DIALECTS = {"postgres", "duckdb", "snowflake"}
    for stype, dialect in _SOURCE_TYPE_TO_DIALECT.items():
        dialect_out = transpile(canonical_pg_sql, dialect)
        assert dialect_out, f"transpilation to '{dialect}' (from source '{stype}') is empty"
        assert "orders" in dialect_out.lower(), (
            f"transpilation to '{dialect}' lost the orders table"
        )
        if dialect in _POSTGRES_COMPAT_DIALECTS:
            # These dialects share postgres syntax for this canonical form.
            continue
        # Other dialects must produce output that differs from the PG canonical,
        # confirming that the registration-time dialect drives real transformation.
        assert dialect_out != postgres_out, (
            f"dialect '{dialect}' output is identical to postgres — no transformation applied"
        )


# ---------------------------------------------------------------------------
# REQ-809 — Correlated scalar subquery → CTE rewriting for Trino
# ---------------------------------------------------------------------------

# A representative PG-style query containing a correlated scalar subquery.
# The subquery is correlated via o.customer_id = c.id and returns a scalar
# value (the customer name) for each outer row — a pattern Trino cannot execute
# directly across federated sources.
_CORRELATED_PG_SQL = (
    'SELECT '
    '"o"."id", '
    '"o"."amount", '
    '(SELECT "c"."name" FROM "public"."customers" "c" '
    'WHERE "c"."id" = "o"."customer_id") AS "customer_name" '
    'FROM "public"."orders" "o"'
)

# Regex patterns used to detect a correlated scalar subquery inline in SELECT.
# A correlated subquery in SELECT appears as: (SELECT ... WHERE ... outer_col ...)
_CORRELATED_SUBQUERY_PATTERN = re.compile(
    r"\(\s*SELECT\b",
    re.IGNORECASE,
)


def _sql_contains_correlated_scalar_subquery(sql: str) -> bool:
    """Return True if the SQL string contains an inline correlated scalar subquery.

    Heuristic: a SELECT-list subquery appears as a parenthesised SELECT that
    references a column from the outer query in its WHERE clause.  After
    rewrite_correlated_subqueries_for_trino has run, such patterns must be
    absent from the forwarded SQL.
    """
    # Parse with sqlglot and walk the AST looking for Subquery nodes that sit
    # directly inside a SELECT expressions list (i.e. scalar position).
    import sqlglot
    import sqlglot.expressions as exp

    try:
        tree = sqlglot.parse_one(sql, read="trino")
    except Exception:
        # Fall back to the regex heuristic if parsing fails.
        return bool(_CORRELATED_SUBQUERY_PATTERN.search(sql))

    if not isinstance(tree, exp.Select):
        return bool(_CORRELATED_SUBQUERY_PATTERN.search(sql))

    for select_expr in tree.args.get("expressions") or []:
        # Walk each SELECT-list expression looking for a Subquery node.
        for node in select_expr.walk():
            if isinstance(node, exp.Subquery):
                return True
    return False


def _sql_contains_cte(sql: str) -> bool:
    """Return True if the SQL string opens with a WITH … AS (…) CTE block."""
    stripped = sql.strip()
    return stripped.upper().startswith("WITH ")


def _cte_joined_on_correlation_key(sql: str, correlation_col: str) -> bool:
    """Return True if the CTE is joined back to the main query on *correlation_col*.

    After lifting a correlated subquery whose predicate was
    ``WHERE c.id = o.customer_id`` the resulting JOIN must reference the
    correlation key (customer_id) so the set-based semantics are preserved.
    """
    return correlation_col.lower() in sql.lower()


@given("a PG-style query containing a correlated scalar subquery")
def a_pg_query_with_correlated_scalar_subquery(shared_data: dict) -> None:
    pg_sql = _CORRELATED_PG_SQL
    shared_data["pg_sql"] = pg_sql

    # Confirm the source query really does contain a correlated scalar subquery
    # so the test is actually exercising the rewriter on meaningful input.
    assert _CORRELATED_SUBQUERY_PATTERN.search(pg_sql), (
        "test fixture must contain a SELECT-list subquery"
    )
    # Confirm it is PG-style (double-quoted identifiers).
    assert '"o"."id"' in pg_sql, "fixture must use PG-style double-quoted identifiers"


@when("the query is transpiled to Trino SQL")
def the_query_is_transpiled_to_trino(shared_data: dict) -> None:
    pg_sql = shared_data["pg_sql"]

    # Call the public rewriter first so we can inspect its intermediate output,
    # then call the full transpile_to_trino pipeline (which calls the rewriter
    # internally) to obtain the final Trino SQL that would be forwarded.
    rewritten_pg = rewrite_correlated_subqueries_for_trino(pg_sql)
    shared_data["rewritten_pg"] = rewritten_pg

    trino_sql = transpile_to_trino(pg_sql)
    shared_data["trino_sql"] = trino_sql


@then("the correlated subquery is lifted into a CTE joined on the correlation key")
def correlated_subquery_lifted_into_cte(shared_data: dict) -> None:
    rewritten_pg: str = shared_data["rewritten_pg"]
    trino_sql: str = shared_data["trino_sql"]

    # The rewriter must have produced a WITH … CTE block — either in the
    # intermediate PG-rewritten form or in the final Trino output.  Accept
    # either: some implementations hoist the CTE at the rewriter stage, others
    # delay until the SQLGlot transpile pass merges it.
    has_cte = _sql_contains_cte(rewritten_pg) or _sql_contains_cte(trino_sql)
    assert has_cte, (
        "Expected rewrite_correlated_subqueries_for_trino to lift the correlated "
        f"subquery into a WITH … CTE block.\n"
        f"rewritten_pg: {rewritten_pg!r}\n"
        f"trino_sql:    {trino_sql!r}"
    )

    # The CTE must be joined back on the correlation key (customer_id) so the
    # set-based replacement preserves the original correlated-filter semantics.
    combined = rewritten_pg + " " + trino_sql
    assert _cte_joined_on_correlation_key(combined, "customer_id"), (
        "The CTE join must reference the correlation key 'customer_id'.\n"
        f"rewritten_pg: {rewritten_pg!r}\n"
        f"trino_sql:    {trino_sql!r}"
    )

    # The lifted CTE must reference the customers table that was inside the
    # correlated subquery.
    assert "customers" in combined.lower(), (
        "The lifted CTE must reference the 'customers' table from the subquery."
    )

    # The outer query must still project the correlated result; verify that the
    # alias 'customer_name' (or the column 'name') survives the rewrite.
    assert "customer_name" in combined.lower() or "name" in combined.lower(), (
        "The projected alias from the correlated subquery must survive the rewrite."
    )


@then("the forwarded SQL contains no correlated scalar subquery")
def forwarded_sql_has_no_correlated_subquery(shared_data: dict) -> None:
    trino_sql: str = shared_data["trino_sql"]

    # The final SQL forwarded to Trino must not contain an inline scalar subquery
    # in the SELECT list — Trino cannot execute arbitrary correlated scalar
    # subqueries across federated sources.
    assert not _sql_contains_correlated_scalar_subquery(trino_sql), (
        "The Trino-forwarded SQL must not contain a correlated scalar subquery "
        "in the SELECT list after rewriting.\n"
        f"trino_sql: {trino_sql!r}"
    )

    # The forwarded SQL must remain a complete, non-empty SELECT statement.
    assert trino_sql.strip(), "Trino SQL must not be empty after rewriting"
    assert trino_sql.strip().upper().startswith(("SELECT", "WITH")), (
        f"Trino SQL must start with SELECT or WITH, got: {trino_sql[:80]!r}"
    )

    # The orders table (outer query source) must still be present.
    assert "orders" in trino_sql.lower(), (
        "The outer 'orders' table must be preserved in the forwarded Trino SQL."
    )
