# Copyright (c) 2026 Kenneth Stott
# Canary: 708b620c-fca0-422b-8485-5940b3a4756f
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1730: ensure_cache_schema never trusts an engine connection to WRITE the store.

Trino's postgresql connector refuses CREATE SCHEMA outright (NOT_SUPPORTED) for a catalog whose
JDBC URL pins a currentSchema (provisa_admin) — on EVERY call, regardless of whether the schema
already exists (something else, e.g. a direct Postgres connection, may have created it moments
earlier). The fallback must verify existence via a READ instead of assuming the CREATE's failure
means the schema is missing.
"""

from __future__ import annotations

import pytest

from provisa.api_source.engine_cache import CacheLocation, _SCHEMA_EXISTS_CACHE, ensure_cache_schema


class _FakeConn:
    """Records executed SQL; scripted per call via a list of (matches, outcome) rules."""

    def __init__(self, responses: list[tuple[str, object]]):
        self._responses = responses
        self.executed: list[str] = []
        self._last_result: object = None

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        for substr, outcome in self._responses:
            if substr in sql:
                if isinstance(outcome, Exception):
                    raise outcome
                self._last_result = outcome
                return
        raise AssertionError(f"unscripted SQL: {sql}")

    def fetchall(self):
        return self._last_result


@pytest.fixture(autouse=True)
def _clear_schema_cache():
    _SCHEMA_EXISTS_CACHE.clear()
    yield
    _SCHEMA_EXISTS_CACHE.clear()


def test_create_succeeds_normally():
    conn = _FakeConn([("CREATE SCHEMA", [])])
    loc = CacheLocation(catalog="mongodb_src", schema="api_cache", backend="relational")
    ensure_cache_schema(conn, loc)
    assert ("mongodb_src", "api_cache") in _SCHEMA_EXISTS_CACHE


def test_not_supported_falls_back_to_read_and_succeeds_when_schema_exists():
    """The Trino case: CREATE always refuses, but the schema already exists (created directly
    against the store by something else) — this must succeed, not raise."""
    conn = _FakeConn(
        [
            (
                "CREATE SCHEMA",
                RuntimeError(
                    'TrinoUserError: NOT_SUPPORTED: "This connector does not support creating schemas"'
                ),
            ),
            ("information_schema.schemata", [("provisa_admin", "org_e2e_api_cache")]),
        ]
    )
    loc = CacheLocation(catalog="provisa_admin", schema="org_e2e_api_cache", backend="relational")
    ensure_cache_schema(conn, loc)  # must not raise
    assert ("provisa_admin", "org_e2e_api_cache") in _SCHEMA_EXISTS_CACHE


def test_not_supported_and_schema_missing_creates_directly_against_store(monkeypatch):
    """The Trino case with NO one having pre-created the schema yet (a dynamic UI registration on
    a running instance, before the next boot/reload runs refresh_landed_views for this org): must
    self-heal by creating the schema directly against the materialize store, not raise."""
    import provisa.api_source.engine_cache as engine_cache_mod

    created: list[CacheLocation] = []
    monkeypatch.setattr(engine_cache_mod, "_create_schema_directly_against_store", created.append)
    conn = _FakeConn(
        [
            (
                "CREATE SCHEMA",
                RuntimeError("NOT_SUPPORTED: This connector does not support creating schemas"),
            ),
            ("information_schema.schemata", []),
        ]
    )
    loc = CacheLocation(catalog="provisa_admin", schema="org_e2e_api_cache", backend="relational")
    ensure_cache_schema(conn, loc)  # must not raise
    assert created == [loc]
    assert ("provisa_admin", "org_e2e_api_cache") in _SCHEMA_EXISTS_CACHE


def test_not_supported_schema_missing_and_direct_create_fails_raises_clear_error():
    conn = _FakeConn(
        [
            (
                "CREATE SCHEMA",
                RuntimeError("NOT_SUPPORTED: This connector does not support creating schemas"),
            ),
            ("information_schema.schemata", []),
        ]
    )
    loc = CacheLocation(catalog="provisa_admin", schema="org_e2e_api_cache", backend="relational")
    with pytest.raises(RuntimeError, match="cannot create schemas"):
        ensure_cache_schema(conn, loc)
    assert ("provisa_admin", "org_e2e_api_cache") not in _SCHEMA_EXISTS_CACHE


def test_a_different_create_failure_still_raises_immediately():
    """A non-NOT_SUPPORTED failure (a real connection error, a permission error) must not be
    masked by the read-fallback — it is not the "Trino refuses writes" case at all."""
    conn = _FakeConn([("CREATE SCHEMA", RuntimeError("connection refused"))])
    loc = CacheLocation(catalog="mongodb_src", schema="api_cache", backend="relational")
    with pytest.raises(RuntimeError, match="ensure_cache_schema failed"):
        ensure_cache_schema(conn, loc)


def test_already_cached_skips_both_create_and_read():
    _SCHEMA_EXISTS_CACHE.add(("provisa_admin", "org_e2e_api_cache"))
    conn = _FakeConn([])  # any execute() call is unscripted and would raise
    loc = CacheLocation(catalog="provisa_admin", schema="org_e2e_api_cache", backend="relational")
    ensure_cache_schema(conn, loc)
    assert conn.executed == []
