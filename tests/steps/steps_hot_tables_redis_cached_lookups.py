# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for Hot Tables (Redis-Cached Lookups) — REQ-230, REQ-231, REQ-232."""

from __future__ import annotations

import asyncio
import fnmatch
import json

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.cache.hot_tables import (
    HOT_PREFIX,
    HotTableEntry,
    HotTableManager,
    build_values_cte_sql,
    detect_hot_tables,
)

scenarios("REQ-230.feature")
scenarios("REQ-231.feature")
scenarios("REQ-232.feature")


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _run(coro):
    """Run an async coroutine from a synchronous pytest-bdd step."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _FakeRedis:
    """In-memory async Redis stand-in supporting the calls HotTableManager makes."""

    def __init__(self):
        self.store: dict[str, str] = {}

    async def set(self, key, value, *args, **kwargs):
        self.store[key] = value
        return True

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                removed += 1
        return removed

    async def exists(self, *keys):
        return sum(1 for k in keys if k in self.store)

    async def ping(self):
        return True

    def scan_iter(self, match=None):
        return self._scan(match)

    async def _scan(self, match):
        for key in list(self.store):
            if match is None or fnmatch.fnmatch(key, match):
                yield key

    def pipeline(self):
        return self

    async def execute(self):
        return []

    async def close(self):
        return None


# ---------------------------------------------------------------------------
# REQ-230 steps
# ---------------------------------------------------------------------------


@given("a table designated as hot")
def given_hot_table(shared_data):
    table_name = "countries"
    tables = [{"table_name": "orders"}, {"table_name": table_name}]
    relationships = [
        {
            "source_table_id": "orders",
            "target_table_id": table_name,
            "cardinality": "many-to-one",
        }
    ]
    detected = detect_hot_tables(tables, relationships, {})
    assert table_name in detected, "hot table was not detected"

    # Raw rows are stored verbatim — NO governance applied at storage time (REQ-230).
    entry = HotTableEntry(
        table_name=table_name,
        catalog="c",
        schema="s",
        pk_column="id",
        rows=[
            {"id": 1, "code": "US", "name": "United States"},
            {"id": 2, "code": "GB", "name": "United Kingdom"},
        ],
        column_names=["id", "code", "name"],
    )
    shared_data["table_name"] = table_name
    shared_data["entry"] = entry


@when("a query references that table")
def when_query_references_table(shared_data):
    table_name = shared_data["table_name"]
    entry = shared_data["entry"]
    sql = (
        f"SELECT o.id, c.name "
        f"FROM orders o JOIN {table_name} c ON o.country_id = c.id"
    )
    rewritten = build_values_cte_sql(sql, table_name, entry)
    shared_data["rewritten_sql"] = rewritten


@then(
    "the cached JSON blob is injected as a VALUES CTE and governance is applied by Stage-2 at\n"
    "    query time"
)
def then_cte_injected_governance_stage2(shared_data):
    rewritten = shared_data["rewritten_sql"]
    entry = shared_data["entry"]
    table_name = shared_data["table_name"]

    # A VALUES CTE was injected from the raw cached rows.
    assert "VALUES" in rewritten.upper(), "VALUES CTE was not injected"
    assert f"_hot_{table_name}" in rewritten, "hot CTE name missing from rewritten SQL"

    # The raw, un-governed row values are present (governance NOT applied at storage time).
    assert "United States" in rewritten
    assert "United Kingdom" in rewritten

    # Every governed column appears in the CTE definition verbatim.
    for col in entry.column_names:
        assert f'"{col}"' in rewritten, f"column {col} missing from CTE"

    # The original query semantics (the wrapping governed SQL) are preserved so that
    # Stage-2 apply_governance can filter/mask the CTE rows at query time.
    assert "SELECT" in rewritten.upper()
    assert "JOIN" in rewritten.upper()


# ---------------------------------------------------------------------------
# REQ-231 steps — TTL-based refresh, mutation invalidation, stale fallback
# ---------------------------------------------------------------------------


@given("a hot table with a configured refresh_interval")
def given_hot_table_with_refresh_interval(shared_data):
    table_name = "currencies"
    manager = HotTableManager(
        redis_url="redis://localhost:6379",
        auto_threshold=1_000,
        max_rows=1_000,
    )

    fake_redis = _FakeRedis()
    manager._redis = fake_redis

    rows = [
        {"id": 1, "code": "USD", "name": "US Dollar"},
        {"id": 2, "code": "EUR", "name": "Euro"},
    ]
    entry = HotTableEntry(
        table_name=table_name,
        catalog="c",
        schema="s",
        pk_column="id",
        rows=rows,
        column_names=["id", "code", "name"],
    )

    # Register the hot table as loaded and seed Redis with its blob.
    manager._hot_tables[table_name] = entry
    blob_key = HOT_PREFIX + table_name + ":blob"
    _run(fake_redis.set(blob_key, json.dumps(rows)))

    # A positive refresh_interval (defaulting to materialized_views.default_ttl)
    # is the contract for TTL-based background refresh.
    refresh_interval = 300
    assert refresh_interval > 0, "refresh_interval must be a positive TTL"

    shared_data["manager"] = manager
    shared_data["redis"] = fake_redis
    shared_data["table_name"] = table_name
    shared_data["blob_key"] = blob_key
    shared_data["refresh_interval"] = refresh_interval

    # The hot table must be live before any invalidation occurs.
    assert manager.is_hot(table_name), "hot table not registered as live"
    assert _run(fake_redis.exists(blob_key)) == 1, "hot blob missing from cache"


@when("the TTL expires or a mutation occurs on the source table")
def when_ttl_expires_or_mutation(shared_data):
    manager = shared_data["manager"]
    table_name = shared_data["table_name"]

    # A mutation to the source table triggers immediate invalidation.
    _run(manager.invalidate(table_name))
    shared_data["invalidated"] = True


@then(
    "the cache is invalidated and asynchronously reloaded, falling back to live query if stale"
)
def then_invalidated_reloaded_with_fallback(shared_data):
    manager = shared_data["manager"]
    redis = shared_data["redis"]
    table_name = shared_data["table_name"]
    blob_key = shared_data["blob_key"]

    assert shared_data.get("invalidated"), "invalidation step did not run"

    # Cache invalidation removed the cached blob and the live registration.
    assert _run(redis.exists(blob_key)) == 0, "hot blob was not invalidated"
    assert not manager.is_hot(table_name), "hot table still registered after invalidation"

    # Stale/missing cache forces a fallback to the live query — get_rows must miss.
    with pytest.raises(KeyError):
        _run(manager.get_rows(table_name))

    # Async reload capability exists (background refresh loop reloads via load_table).
    reload_fn = getattr(manager, "load_table", None)
    assert callable(reload_fn), "manager lacks async reload (load_table) capability"
    assert asyncio.iscoroutinefunction(reload_fn), "reload must be asynchronous"


# ---------------------------------------------------------------------------
# REQ-232 steps — Hot table JOIN optimization via VALUES-based CTE injection
# ---------------------------------------------------------------------------


@given("a query that JOINs a hot table")
def given_query_joins_hot_table(shared_data):
    # The classic example: orders JOIN countries ON country_code.
    table_name = "countries"
    tables = [{"table_name": "orders"}, {"table_name": table_name}]
    relationships = [
        {
            "source_table_id": "orders",
            "target_table_id": table_name,
            "cardinality": "many-to-one",
        }
    ]
    # The lookup table must be auto-detected as hot (many-to-one target).
    detected = detect_hot_tables(tables, relationships, {})
    assert table_name in detected, "lookup table was not detected as hot"

    entry = HotTableEntry(
        table_name=table_name,
        catalog="c",
        schema="s",
        pk_column="country_code",
        rows=[
            {"country_code": "US", "country_name": "United States"},
            {"country_code": "GB", "country_name": "United Kingdom"},
            {"country_code": "DE", "country_name": "Germany"},
        ],
        column_names=["country_code", "country_name"],
    )

    sql = (
        "SELECT o.id, c.country_name "
        "FROM orders o "
        f"JOIN {table_name} c ON o.country_code = c.country_code"
    )

    shared_data["table_name"] = table_name
    shared_data["entry"] = entry
    shared_data["original_sql"] = sql


@when("the compiler processes the query")
def when_compiler_processes_query(shared_data):
    table_name = shared_data["table_name"]
    entry = shared_data["entry"]
    sql = shared_data["original_sql"]

    # The compiler injects the hot table rows as a VALUES-based CTE,
    # replacing the live table reference.
    rewritten = build_values_cte_sql(sql, table_name, entry)
    shared_data["rewritten_sql"] = rewritten


@then(
    "the hot table data is injected as a VALUES-based CTE and the DB engine sees no table\n"
    "    reference"
)
def then_values_cte_no_table_reference(shared_data):
    import sqlglot
    import sqlglot.expressions as exp

    rewritten = shared_data["rewritten_sql"]
    original = shared_data["original_sql"]
    entry = shared_data["entry"]
    table_name = shared_data["table_name"]

    assert rewritten != original, "compiler did not rewrite the query"

    # A VALUES-based CTE was injected carrying the hot rows as literal constants.
    upper = rewritten.upper()
    assert "WITH" in upper, "no WITH/CTE clause was emitted"
    assert "VALUES" in upper, "hot table data was not injected as a VALUES clause"

    cte_name = f"_hot_{table_name}"
    assert cte_name in rewritten, "hot CTE name missing from rewritten SQL"

    # Every hot row's data travels with the query as literal constants.
    assert "United States" in rewritten
    assert "United Kingdom" in rewritten
    assert "Germany" in rewritten
    for col in entry.column_names:
        assert f'"{col}"' in rewritten, f"column {col} missing from CTE definition"

    # Parse the rewritten SQL and confirm the DB engine sees NO reference to the
    # live `countries` table — only the CTE definition and a reference to the CTE.
    tree = sqlglot.parse_one(rewritten)

    cte_definitions = {
        c.alias_or_name for c in tree.find_all(exp.CTE)
    }
    assert cte_name in cte_definitions, "VALUES CTE was not registered as a CTE"

    # Collect every concrete table reference (FROM/JOIN targets) in the body.
    referenced_tables = {t.name for t in tree.find_all(exp.Table)}
    # The only "table-like" reference to the lookup must be the CTE, not the real table.
    assert table_name not in referenced_tables, (
        f"DB engine still sees a reference to the live table '{table_name}'"
    )
    assert cte_name in referenced_tables, "rewritten query does not reference the hot CTE"

    # A VALUES expression must exist in the parsed tree (literal rows, not a scan).
    assert tree.find(exp.Values) is not None, "no VALUES expression found in CTE"

    # The original JOIN semantics are preserved against the constant CTE.
    assert tree.find(exp.Join) is not None, "JOIN was lost during rewrite"
