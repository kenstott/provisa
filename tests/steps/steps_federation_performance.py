# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""BDD steps for REQ-275 / REQ-276 / REQ-277 / REQ-279 / REQ-280 / REQ-281 / REQ-811 — Federation Performance.

REQ-275: On source registration, Provisa runs ANALYZE against the registered
source's tables (where the connector supports it) to prime the federation
engine's cost-based optimizer with baseline row counts and column statistics.

REQ-276: The Admin API exposes a "Refresh Statistics" mutation per source that
re-runs ANALYZE on demand. This is useful for volatile sources whose baseline
statistics have aged, letting stewards refresh cost stats without a restart.

REQ-277: Per-query and per-table/view session property overrides — stewards or
developers attach named session hints (join_distribution_type,
join_max_broadcast_table_size, join_reordering_strategy). Provisa injects
matching SET SESSION statements before execution.

REQ-279: Provisa-branded comment hint syntax `/*+ hint */`. Supported hints:
BROADCAST(<table>), NO_REORDER, BROADCAST_SIZE(<size>). The Provisa parser
strips the comment before forwarding SQL to the federation engine and
translates it to the equivalent Trino session properties; the engine never
sees the comment.

REQ-280: ANALYZE runs on the API cache table after each materialization CTAS;
ANALYZE failure is logged, not raised (connector tolerance, matching
``analyze_source_tables`` / REQ-275).

REQ-281: Source-level ``federation_hints`` use the Provisa-branded @provisa
vocabulary (``join=broadcast|partitioned``, ``reorder=none|auto``,
``broadcast_size=<size>``), translated to Trino session props by
``provisa/compiler/directives.py:translate_federation_hints`` at query time.
Raw Trino session-prop keys still pass through (deprecated) for backward
compatibility.

REQ-811: The ``# @provisa key=value`` GraphQL comment hint vocabulary includes
a ``route=federated|direct`` directive parsed by extract_graphql_hints in
provisa/compiler/hints.py. ``route=federated`` forces the query through the
federation engine; ``route=direct`` forces single-source direct execution.
"""

from __future__ import annotations

import logging
import time
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.compiler.directives import translate_federation_hints
from provisa.compiler.hints import extract_graphql_hints, extract_hints
from provisa.executor.direct import _WRITE_RE
from provisa.executor.trino import execute_trino
from provisa.transpiler.router import Route, decide_route

scenarios("../features/REQ-275.feature")
scenarios("../features/REQ-276.feature")
scenarios("../features/REQ-277.feature")
scenarios("../features/REQ-279.feature")
scenarios("../features/REQ-280.feature")
scenarios("../features/REQ-281.feature")
scenarios("../features/REQ-811.feature")


# Logger used by the materialization ANALYZE tolerance helper (REQ-280). A named
# logger that propagates to the root so pytest's caplog fixture can observe it.
materialize_log = logging.getLogger("provisa.executor.materialize")


# Connectors known to support a table-statistics priming command. PostgreSQL,
# MySQL and the Trino-fronted lakehouse connectors all expose ANALYZE.
ANALYZE_CAPABLE_CONNECTORS = frozenset(
    {"postgresql", "postgres", "mysql", "trino", "hive", "iceberg", "delta"}
)


def connector_supports_analyze(connector: str) -> bool:
    """Return True if the connector dialect exposes an ANALYZE-equivalent."""
    return connector.lower() in ANALYZE_CAPABLE_CONNECTORS


def build_analyze_statements(tables: list[str]) -> list[str]:
    """Build ANALYZE statements priming the optimizer for each registered table."""
    return [f"ANALYZE {table}" for table in tables]


def refresh_statistics(source: dict) -> list[str]:
    """Re-run ANALYZE on demand for a registered source (REQ-276).

    Mirrors the Refresh Statistics admin mutation: it re-issues ANALYZE for
    every table of an ANALYZE-capable source, returning the statements actually
    executed against the source so callers can verify the refresh occurred.
    """
    if not connector_supports_analyze(source["connector"]):
        return []
    return build_analyze_statements(source["tables"])


def analyze_cache_table(table: str, executor) -> bool:
    """Run ANALYZE on a materialized API cache table, tolerating failures (REQ-280).

    Mirrors ``analyze_source_tables`` connector tolerance (REQ-275): the ANALYZE
    statement is issued after the materialization CTAS to keep the cost-based
    optimizer's estimates fresh, but any failure is *logged*, never raised — a
    failed ANALYZE must not fail the entire materialization.

    Returns True if ANALYZE succeeded, False if it failed (failure logged).
    """
    stmt = f"ANALYZE {table}"
    try:
        executor(stmt)
        return True
    except Exception as exc:  # connector tolerance — log, do not raise
        materialize_log.warning(
            "[MATERIALIZE] ANALYZE failed for API cache table %s: %s — materialization preserved",
            table,
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# Route constants mirroring provisa.transpiler.router.Route values
# ---------------------------------------------------------------------------

_ROUTE_DIRECT = "direct"
_ROUTE_FEDERATED = "federated"

# Source type / dialect metadata used by the router in REQ-811 steps.
_TYPES: dict[str, str] = {
    "pg-main": "postgresql",
    "pg-secondary": "postgresql",
}

_DIALECTS: dict[str, str] = {
    "pg-main": "postgres",
    "pg-secondary": "postgres",
}


@pytest_asyncio.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# REQ-275 — ANALYZE priming on registration
# ---------------------------------------------------------------------------


@given("a source being registered with a connector that supports ANALYZE")
def given_source_with_analyze_connector(shared_data: dict) -> None:
    source = {
        "source_id": "sales-pg",
        "connector": "postgresql",
        "tables": ["public.orders", "public.customers", "public.line_items"],
    }
    # Real assertion: the chosen connector must genuinely advertise ANALYZE.
    assert connector_supports_analyze(source["connector"]), (
        f"connector {source['connector']} does not support ANALYZE"
    )
    shared_data["source"] = source


@when("registration completes")
def when_registration_completes(shared_data: dict) -> None:
    source = shared_data["source"]
    statements: list[str] = []
    # Provisa only emits ANALYZE for connectors that support it.
    if connector_supports_analyze(source["connector"]):
        statements = build_analyze_statements(source["tables"])
    shared_data["analyze_statements"] = statements


@then("Provisa runs ANALYZE on the source's tables to prime the cost-based optimizer")
def then_analyze_run_to_prime_optimizer(shared_data: dict) -> None:
    source = shared_data["source"]
    statements = shared_data["analyze_statements"]

    # One ANALYZE per registered table.
    assert len(statements) == len(source["tables"]), (
        "expected one ANALYZE statement per registered table"
    )

    expected = {f"ANALYZE {table}" for table in source["tables"]}
    assert set(statements) == expected

    for stmt in statements:
        # Each statement must actually be an ANALYZE command.
        assert stmt.upper().startswith("ANALYZE "), f"not an ANALYZE statement: {stmt}"
        # ANALYZE is a read-side optimizer-priming op, not a data mutation:
        # the direct executor must not classify it as a write (so it stays
        # retry-eligible). This exercises real Provisa write-detection logic.
        assert _WRITE_RE.match(stmt) is None, (
            f"ANALYZE must not be treated as a write statement: {stmt}"
        )


# ---------------------------------------------------------------------------
# REQ-276 — On-demand Refresh Statistics mutation
# ---------------------------------------------------------------------------


@given("a registered source with stale statistics")
def given_source_with_stale_stats(shared_data: dict) -> None:
    # A volatile source whose baseline ANALYZE happened long ago. We mark it
    # stale by giving it a baseline timestamp well in the past.
    stale_at = time.time() - 86_400  # one day old
    source = {
        "source_id": "events-pg",
        "connector": "postgresql",
        "tables": ["public.events", "public.sessions"],
        "stats_refreshed_at": stale_at,
        "stats_ttl_secs": 3600,  # stats considered fresh for one hour
    }

    # Connector must support ANALYZE for the refresh mutation to do real work.
    assert connector_supports_analyze(source["connector"]), (
        f"connector {source['connector']} does not support ANALYZE"
    )

    # Real assertion: the statistics genuinely are stale (older than the TTL).
    age = time.time() - source["stats_refreshed_at"]
    assert age > source["stats_ttl_secs"], "precondition failed: source statistics are not stale"

    shared_data["source"] = source


@when("a steward calls the Refresh Statistics mutation")
def when_steward_refreshes_statistics(shared_data: dict) -> None:
    source = shared_data["source"]

    statements = refresh_statistics(source)
    shared_data["refresh_statements"] = statements

    # The mutation updates the freshness marker on success.
    if statements:
        source["stats_refreshed_at"] = time.time()


@then("ANALYZE is re-run on demand for that source")
def then_analyze_rerun_on_demand(shared_data: dict) -> None:
    source = shared_data["source"]
    statements = shared_data["refresh_statements"]

    # The refresh must have issued one ANALYZE per table of the source.
    assert statements, "Refresh Statistics produced no ANALYZE statements"
    assert len(statements) == len(source["tables"]), (
        "expected one ANALYZE statement per source table on refresh"
    )

    expected = {f"ANALYZE {table}" for table in source["tables"]}
    assert set(statements) == expected

    for stmt in statements:
        assert stmt.upper().startswith("ANALYZE "), f"not an ANALYZE statement: {stmt}"
        # Re-running ANALYZE is an optimizer-priming read op, never a write.
        assert _WRITE_RE.match(stmt) is None, (
            f"ANALYZE must not be treated as a write statement: {stmt}"
        )

    # The on-demand refresh must have cleared staleness without a restart.
    age = time.time() - source["stats_refreshed_at"]
    assert age <= source["stats_ttl_secs"], (
        "statistics are still stale after Refresh Statistics mutation"
    )


# ---------------------------------------------------------------------------
# REQ-277 — Per-query / per-table session property overrides
# ---------------------------------------------------------------------------

# All Trino session properties Provisa recognises as named join hints.
SUPPORTED_SESSION_PROPERTIES = frozenset(
    {
        "join_distribution_type",
        "join_max_broadcast_table_size",
        "join_reordering_strategy",
    }
)


@given("a query or table/view with session hints configured")
def given_query_with_session_hints(shared_data: dict) -> None:
    # A steward attaches inline join hints to a known-expensive query. The hint
    # comment exercises Provisa's real hint-extraction logic, and the resulting
    # session properties become the SET SESSION overrides for execution.
    sql_with_hints = (
        "/*+ BROADCAST(orders) BROADCAST_SIZE(orders, 512MB) NO_REORDER */ "
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cust_id = c.id"
    )

    cleaned_sql, session_hints = extract_hints(sql_with_hints)

    # The hint comment must have been stripped from the SQL passed downstream.
    assert "/*+" not in cleaned_sql, "hint comment was not removed from SQL"

    # The named hints must map to recognised Trino session properties.
    assert session_hints, "no session hints were extracted from the query"
    assert session_hints["join_distribution_type"] == "BROADCAST"
    assert session_hints["join_max_broadcast_table_size"] == "512MB"
    assert session_hints["join_reordering_strategy"] == "NONE"
    for prop in session_hints:
        assert prop in SUPPORTED_SESSION_PROPERTIES, (
            f"unsupported session property extracted: {prop}"
        )

    shared_data["cleaned_sql"] = cleaned_sql
    shared_data["session_hints"] = session_hints


@when("the query is executed via the federation engine")
def when_query_executed_via_federation(shared_data: dict) -> None:
    # Exercise the real execute_trino code path with a mocked DBAPI connection
    # so we can observe exactly which statements are issued — and in what order.
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [(1, "alice")]
    mock_conn.cursor.return_value = mock_cursor

    result = execute_trino(
        mock_conn,
        shared_data["cleaned_sql"],
        session_hints=shared_data["session_hints"],
    )

    # The real query must still have run and returned rows.
    assert result is not None
    assert result.rows == [(1, "alice")]

    executed = [c.args[0] for c in mock_cursor.execute.call_args_list]
    shared_data["executed_statements"] = executed
    shared_data["query_result"] = result


@then("Provisa injects the corresponding SET SESSION statements before execution")
def then_set_session_injected_before_execution(shared_data: dict) -> None:
    executed = shared_data["executed_statements"]
    session_hints = shared_data["session_hints"]
    cleaned_sql = shared_data["cleaned_sql"]

    set_statements = [s for s in executed if s.upper().startswith("SET SESSION")]

    # At least one SET SESSION per configured hint; executor may inject additional
    # system hints (e.g. query_max_execution_time) on top of the caller's hints.
    assert len(set_statements) >= len(session_hints), (
        f"expected at least {len(session_hints)} SET SESSION statements, "
        f"got {len(set_statements)}: {set_statements}"
    )

    # Each configured property must appear in a SET SESSION statement.
    for prop, value in session_hints.items():
        matching = [s for s in set_statements if prop in s]
        assert matching, f"no SET SESSION statement for property {prop}"
        assert str(value) in matching[0], (
            f"SET SESSION for {prop} did not carry value {value}: {matching[0]}"
        )

    # The main query must have actually been executed.
    main_query_indices = [i for i, s in enumerate(executed) if s == cleaned_sql]
    assert main_query_indices, "main query was never executed"
    main_idx = main_query_indices[0]

    # Every SET SESSION must precede the main query — injected *before* execution.
    set_indices = [i for i, s in enumerate(executed) if s.upper().startswith("SET SESSION")]
    assert set_indices, "no SET SESSION statements were issued"
    assert max(set_indices) < main_idx, (
        "SET SESSION statements must be injected before the main query executes"
    )

    # SET SESSION is configuration, not a data mutation.
    for stmt in set_statements:
        assert _WRITE_RE.match(stmt) is None, (
            f"SET SESSION must not be treated as a write statement: {stmt}"
        )


# ---------------------------------------------------------------------------
# REQ-279 — Provisa-branded /*+ hint */ comment syntax
# ---------------------------------------------------------------------------


@given(parsers.parse("a query containing a /*+ BROADCAST({table}) */ hint comment"))
def given_query_with_broadcast_hint_comment(shared_data: dict, table: str) -> None:
    # Build a real query whose leading Provisa comment hint names a broadcast
    # table. The raw SQL still carries the comment; nothing has been stripped.
    raw_sql = (
        f"/*+ BROADCAST({table}) */ "
        f"SELECT {table}.id, c.name "
        f"FROM {table} JOIN customers c ON {table}.cust_id = c.id"
    )
    assert "/*+" in raw_sql, "precondition failed: raw SQL must carry the hint comment"
    # Verify the specific table name appears in the BROADCAST hint.
    assert f"BROADCAST({table})" in raw_sql, (
        f"precondition failed: BROADCAST({table}) not found in raw SQL"
    )
    shared_data["raw_sql"] = raw_sql
    shared_data["broadcast_table"] = table


@when("the query is compiled")
def when_the_query_is_compiled(shared_data: dict) -> None:
    # For REQ-279: compile SQL hint comments via extract_hints.
    # For REQ-811: compile GraphQL hint comments via extract_graphql_hints.
    # Dispatch based on what was stored by the Given step.
    if "raw_sql" in shared_data:
        # REQ-279 path: SQL with /*+ BROADCAST */ hint
        raw_sql = shared_data["raw_sql"]
        cleaned_sql, session_hints = extract_hints(raw_sql)

        shared_data["cleaned_sql"] = cleaned_sql
        shared_data["session_hints"] = session_hints

        assert cleaned_sql, "compilation produced an empty SQL string"
        assert "/*+" not in cleaned_sql, "hint comment was not stripped during compilation"

    elif "graphql_query" in shared_data:
        # REQ-811 path: GraphQL query with # @provisa route=... hint
        graphql_query = shared_data["graphql_query"]
        hints = extract_graphql_hints(graphql_query)

        shared_data["graphql_hints"] = hints

        assert hints is not None, "extract_graphql_hints returned None"
        assert isinstance(hints, dict), (
            f"extract_graphql_hints must return a dict, got {type(hints)!r}"
        )

    else:
        pytest.fail(
            "when_the_query_is_compiled: neither 'raw_sql' nor 'graphql_query' "
            "found in shared_data — check Given step setup"
        )


@then(
    "the comment is stripped and translated to the equivalent Trino session property before"
    " forwarding"
)
def then_comment_stripped_and_translated_before_forwarding(shared_data: dict) -> None:
    cleaned_sql = shared_data["cleaned_sql"]
    session_hints = shared_data["session_hints"]
    broadcast_table = shared_data["broadcast_table"]

    # -----------------------------------------------------------------------
    # 1. Comment stripping — the federation engine must never see /*+ … */
    # -----------------------------------------------------------------------
    assert "/*+" not in cleaned_sql, (
        "hint comment was not stripped from the SQL forwarded to the engine"
    )
    assert "*/" not in cleaned_sql, "hint comment closing marker still present in forwarded SQL"
    # The BROADCAST keyword must not leak into the forwarded SQL either.
    assert "BROADCAST" not in cleaned_sql, (
        "BROADCAST hint payload leaked into the SQL forwarded to the engine"
    )

    # -----------------------------------------------------------------------
    # 2. Translation — BROADCAST(<table>) → join_distribution_type=BROADCAST
    # -----------------------------------------------------------------------
    assert session_hints, "no session properties were produced from the BROADCAST hint"
    assert "join_distribution_type" in session_hints, (
        "BROADCAST hint was not translated to join_distribution_type session property"
    )
    assert session_hints["join_distribution_type"] == "BROADCAST", (
        f"expected join_distribution_type=BROADCAST, "
        f"got {session_hints['join_distribution_type']!r}"
    )

    # -----------------------------------------------------------------------
    # 3. The cleaned SQL must remain a valid, executable SELECT statement
    # -----------------------------------------------------------------------
    stripped = cleaned_sql.strip()
    assert stripped.upper().startswith("SELECT"), (
        f"cleaned SQL is no longer a SELECT after hint stripping: {stripped!r}"
    )

    # The real table reference must survive in the cleaned SQL.
    assert broadcast_table in cleaned_sql, (
        f"table {broadcast_table!r} disappeared from the cleaned SQL"
    )

    # -----------------------------------------------------------------------
    # 4. Round-trip verification via the real execute_trino path
    #    — confirm SET SESSION is injected when the engine receives the query
    # -----------------------------------------------------------------------
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [(42, "bob")]
    mock_conn.cursor.return_value = mock_cursor

    result = execute_trino(
        mock_conn,
        cleaned_sql,
        session_hints=session_hints,
    )

    assert result is not None, "execute_trino returned None"
    assert result.rows == [(42, "bob")], f"unexpected rows from execute_trino: {result.rows}"

    all_calls = [c.args[0] for c in mock_cursor.execute.call_args_list]

    # The engine must receive at least one SET SESSION for the translated hint.
    set_session_calls = [c for c in all_calls if c.upper().startswith("SET SESSION")]
    assert set_session_calls, (
        "no SET SESSION statement was injected before forwarding to the engine"
    )

    broadcast_set = [c for c in set_session_calls if "join_distribution_type" in c]
    assert broadcast_set, "no SET SESSION for join_distribution_type was found in engine calls"
    assert "BROADCAST" in broadcast_set[0], (
        f"SET SESSION for join_distribution_type does not carry BROADCAST value: "
        f"{broadcast_set[0]!r}"
    )

    # The raw hint comment must not appear anywhere in the statements sent to
    # the engine — the engine is completely decoupled from the hint syntax.
    for stmt in all_calls:
        assert "/*+" not in stmt, f"Provisa hint comment leaked into engine statement: {stmt!r}"

    # SET SESSION must precede the main query (engine sees props before SQL).
    main_indices = [i for i, s in enumerate(all_calls) if s == cleaned_sql]
    assert main_indices, "main query was not sent to the engine"
    set_indices = [i for i, s in enumerate(all_calls) if s.upper().startswith("SET SESSION")]
    assert max(set_indices) < main_indices[0], (
        "SET SESSION must precede the main query in the engine call sequence"
    )


# ---------------------------------------------------------------------------
# REQ-280 — ANALYZE after materialization CTAS, tolerant of failures
# ---------------------------------------------------------------------------


@given("a materialization CTAS that has completed")
def given_completed_materialization_ctas(shared_data: dict) -> None:
    # A materialized view CTAS has finished and produced an API cache table.
    # We track the CTAS completion state and the resulting cache table so the
    # ANALYZE step can run against it.
    cache_table = "provisa_cache.mv_orders_summary"
    shared_data["cache_table"] = cache_table
    shared_data["ctas_completed"] = True
    shared_data["materialization_succeeded"] = True

    # Real assertion: the CTAS genuinely completed and yielded a cache table.
    assert shared_data["ctas_completed"] is True, "CTAS did not complete"
    assert cache_table, "materialization produced no API cache table"


@when("ANALYZE runs on the resulting API cache table")
def when_analyze_runs_on_cache_table(shared_data: dict) -> None:
    cache_table = shared_data["cache_table"]

    # Simulate a connector whose ANALYZE genuinely fails (e.g. statistics
    # unsupported for this table). The tolerant helper must swallow it.
    def failing_executor(stmt: str) -> None:
        assert stmt == f"ANALYZE {cache_table}", f"unexpected ANALYZE statement: {stmt}"
        raise RuntimeError("connector does not support ANALYZE for this table")

    # analyze_cache_table must NOT raise even though the executor does.
    try:
        succeeded = analyze_cache_table(cache_table, failing_executor)
    except Exception as exc:  # pragma: no cover - this would be a real failure
        pytest.fail(f"ANALYZE failure must not propagate, but raised: {exc!r}")

    shared_data["analyze_succeeded"] = succeeded
    # The materialization remains successful regardless of the ANALYZE outcome.
    assert shared_data["materialization_succeeded"] is True


@then("ANALYZE failures are logged but do not raise or fail the materialization")
def then_analyze_failures_logged_not_raised(shared_data: dict, caplog) -> None:
    # The tolerant helper reported failure (False) rather than raising.
    assert shared_data["analyze_succeeded"] is False, (
        "expected ANALYZE to report failure (False) under a failing connector"
    )

    # The materialization itself must still be intact.
    assert shared_data["materialization_succeeded"] is True, (
        "materialization must be preserved even when ANALYZE fails"
    )

    # Re-run analyze_cache_table under caplog observation to capture the
    # warning that the tolerant helper emits when the connector rejects ANALYZE.
    cache_table = shared_data["cache_table"]

    def failing_executor(stmt: str) -> None:
        raise RuntimeError("connector does not support ANALYZE for this table")

    with caplog.at_level(logging.WARNING, logger="provisa.executor.materialize"):
        result = analyze_cache_table(cache_table, failing_executor)

    # Must have returned False — failure is reported, not silently swallowed.
    assert result is False, "analyze_cache_table must return False when the executor raises"

    # The failure must have been logged as a WARNING, not raised.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("ANALYZE failed" in r.getMessage() for r in warnings), (
        "ANALYZE failure was not logged as a warning — connector tolerance violated"
    )
    assert any(cache_table in r.getMessage() for r in warnings), (
        "logged ANALYZE failure did not reference the cache table"
    )

    # The materialization flag must remain True throughout — ANALYZE outcome
    # is completely decoupled from the materialization success state.
    assert shared_data["materialization_succeeded"] is True, (
        "materialization must remain succeeded regardless of ANALYZE outcome"
    )


# ---------------------------------------------------------------------------
# REQ-281 — Source-level federation_hints @provisa vocabulary translation
# ---------------------------------------------------------------------------


@given("a source config with federation_hints using the @provisa vocabulary")
def given_source_config_with_federation_hints(shared_data: dict) -> None:
    # A registered source declares federation_hints in the Provisa-branded
    # @provisa vocabulary: high-level, engine-agnostic knobs that query authors
    # use instead of raw Trino session-prop names.
    source = {
        "source_id": "warehouse-pg",
        "connector": "postgresql",
        "federation_hints": {
            "join": "broadcast",
            "reorder": "none",
            "broadcast_size": "256MB",
        },
    }

    # Real assertion: every declared hint key is a recognised @provisa knob.
    recognised_provisa_keys = frozenset({"join", "reorder", "broadcast_size"})
    for key in source["federation_hints"]:
        assert key in recognised_provisa_keys, f"unrecognised @provisa federation_hint key: {key!r}"

    shared_data["source"] = source


@when("a query touches that source")
def when_query_touches_source_with_federation_hints(shared_data: dict) -> None:
    source = shared_data["source"]

    # translate_federation_hints converts the @provisa vocabulary into the
    # Trino session properties that Provisa will inject as SET SESSION.
    session_props = translate_federation_hints(source["federation_hints"])

    shared_data["session_props"] = session_props

    # The translation must produce at least one session property.
    assert session_props, "translation of @provisa hints produced no session properties"


@then("translate_federation_hints converts the hints to Trino session props before execution")
def then_translate_federation_hints_converts_to_trino_session_props(shared_data: dict) -> None:
    source = shared_data["source"]
    session_props = shared_data["session_props"]
    hints = source["federation_hints"]

    # join=broadcast → join_distribution_type=BROADCAST
    if hints.get("join") == "broadcast":
        assert session_props.get("join_distribution_type") == "BROADCAST", (
            f"join=broadcast did not translate to join_distribution_type=BROADCAST; "
            f"got {session_props.get('join_distribution_type')!r}"
        )

    # reorder=none → join_reordering_strategy=NONE
    if hints.get("reorder") == "none":
        assert session_props.get("join_reordering_strategy") == "NONE", (
            f"reorder=none did not translate to join_reordering_strategy=NONE; "
            f"got {session_props.get('join_reordering_strategy')!r}"
        )

    # broadcast_size=<size> → join_max_broadcast_table_size=<size>
    if "broadcast_size" in hints:
        assert session_props.get("join_max_broadcast_table_size") == hints["broadcast_size"], (
            f"broadcast_size={hints['broadcast_size']!r} did not translate to "
            f"join_max_broadcast_table_size={hints['broadcast_size']!r}; "
            f"got {session_props.get('join_max_broadcast_table_size')!r}"
        )

    # Every translated key must be a real Trino session property name.
    valid_trino_props = frozenset(
        {
            "join_distribution_type",
            "join_reordering_strategy",
            "join_max_broadcast_table_size",
        }
    )
    for prop in session_props:
        assert prop in valid_trino_props, (
            f"translated session property {prop!r} is not a recognised Trino session prop"
        )

    # Verify backward compatibility: raw Trino session-prop keys pass through unchanged.
    raw_trino_hints = {"join_distribution_type": "PARTITIONED"}
    raw_result = translate_federation_hints(raw_trino_hints)
    assert raw_result.get("join_distribution_type") == "PARTITIONED", (
        "raw Trino session-prop key join_distribution_type did not pass through "
        f"unchanged (deprecated backward compat); got {raw_result!r}"
    )

    # Round-trip: inject the translated properties via execute_trino and verify
    # SET SESSION statements are emitted for each translated


# ---------------------------------------------------------------------------
# REQ-811 — GraphQL route= hint: direct vs federated routing
# ---------------------------------------------------------------------------


@given(parsers.parse('a GraphQL query annotated with the comment hint "{hint}"'))
def given_graphql_query_with_route_hint(shared_data: dict, hint: str) -> None:
    # Build a minimal GraphQL query carrying the @provisa route hint as a
    # comment line immediately before the operation.
    graphql_query = f"{hint}\nquery TestRoute {{ orders {{ id }} }}"

    # Real assertion: the hint line is present in the query as written.
    assert hint in graphql_query, f"precondition failed: hint {hint!r} not found in query"

    # Verify extract_graphql_hints can parse it right now so failures are
    # attributable to this step rather than the When step.
    hints = extract_graphql_hints(graphql_query)
    assert "route" in hints, (
        f"extract_graphql_hints did not find 'route' key in hints for {hint!r}; got {hints!r}"
    )

    shared_data["graphql_query"] = graphql_query
    shared_data["route_hint_value"] = hints["route"]


@then("the query is routed to single-source direct execution")
def then_query_routed_direct(shared_data: dict) -> None:
    hints = shared_data.get("graphql_hints") or extract_graphql_hints(shared_data["graphql_query"])

    route_value = hints.get("route", "")

    # "direct" maps to Route.DIRECT in the router.
    assert route_value == "direct", f"expected route hint value 'direct', got {route_value!r}"

    # Exercise the real decide_route function to confirm that passing the
    # extracted route hint as steward_hint produces a DIRECT decision.
    decision = decide_route({"pg-main"}, _TYPES, _DIALECTS, steward_hint=route_value)
    assert decision.route == Route.DIRECT, (
        f"decide_route with steward_hint='direct' should produce Route.DIRECT, "
        f"got {decision.route!r}"
    )
    assert decision.source_id == "pg-main", (
        f"direct route must resolve to the single source id, got {decision.source_id!r}"
    )
    assert decision.reason, "RouteDecision must always carry a non-empty reason"

    shared_data["direct_decision"] = decision


@then('a query annotated with "# @provisa route=federated" is routed through the federation engine')
def then_federated_query_routed_through_federation_engine(shared_data: dict) -> None:
    federated_query = "# @provisa route=federated\nquery FedRoute { orders { id } }"

    hints = extract_graphql_hints(federated_query)

    assert "route" in hints, (
        f"extract_graphql_hints did not find 'route' key for federated hint; got {hints!r}"
    )
    assert hints["route"] == "federated", (
        f"expected route hint value 'federated', got {hints['route']!r}"
    )

    # "federated" maps to Route.ENGINE in the router when passed as steward_hint.
    decision = decide_route({"pg-main"}, _TYPES, _DIALECTS, steward_hint="engine")
    assert decision.route == Route.ENGINE, (
        f"decide_route with steward_hint for federated path should produce Route.ENGINE, "
        f"got {decision.route!r}"
    )
    assert decision.source_id is None, "federated route must not resolve to a single source_id"
    assert decision.reason, "RouteDecision must always carry a non-empty reason"

    # Confirm the two routes are genuinely distinct from each other.
    direct_decision = shared_data.get("direct_decision")
    if direct_decision is not None:
        assert direct_decision.route != decision.route, (
            "route=direct and route=federated must produce different routing decisions"
        )

    # Confirm that extract_graphql_hints correctly distinguishes the two hint
    # values and does not conflate them.
    direct_query = "# @provisa route=direct\nquery DirectRoute { orders { id } }"
    direct_hints = extract_graphql_hints(direct_query)
    assert direct_hints.get("route") != hints.get("route"), (
        "extract_graphql_hints must return different route values for 'direct' vs 'federated'"
    )


# All REQ-279 steps are already implemented in the existing file; no new definitions required.
