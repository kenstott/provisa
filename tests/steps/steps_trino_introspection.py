# Copyright (c) 2026 Kenneth Stott
# Canary: af5d4f25-0426-4de1-835a-2292275606a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pytest-bdd step implementations for REQ-923 - Trino Introspection retry behaviour."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import trino.exceptions
from pytest_bdd import given, scenario, then, when

from provisa.compiler import introspect as _introspect
from provisa.compiler.introspect import introspect_column_types

# ---------------------------------------------------------------------------
# Scenario registration
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-923.feature",
    "REQ-923 default behaviour",
)
def test_req_923_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Helper: build a mock Trino connection that always raises *exc*
# ---------------------------------------------------------------------------


def _conn_raising(exc: Exception) -> MagicMock:
    cur = MagicMock()
    cur.execute.side_effect = exc
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@given("Trino introspection is invoked during coordinator startup")
def trino_introspection_invoked_during_startup(shared_data, monkeypatch):
    """
    Set up the shared state that will be used by both When/Then pairs.

    We patch backoff to 0 so tests run instantly, and record the monkeypatch
    handle for subsequent steps that may need to further adjust timeouts.
    """
    monkeypatch.setattr(_introspect, "_STARTUP_BACKOFF_SECS", 0.0)
    shared_data["monkeypatch"] = monkeypatch
    # Build the startup error we will reuse across steps
    shared_data["startup_exc"] = trino.exceptions.TrinoQueryError(
        {
            "errorName": "SERVER_STARTING_UP",
            "errorType": "INTERNAL_ERROR",
            "message": "Trino server is still initializing",
        },
        query_id="q-bdd-923",
    )


@when("the coordinator reports SERVER_STARTING_UP")
def coordinator_reports_starting_up(shared_data):
    """
    Simulate one transient SERVER_STARTING_UP followed by a successful response.

    The first cursor call raises the startup error; the second returns a real row.
    We call introspect_column_types and store the result for the Then step.
    """
    startup_exc = shared_data["startup_exc"]

    starting_cur = MagicMock()
    starting_cur.execute.side_effect = startup_exc

    ready_cur = MagicMock()
    ready_cur.fetchall.return_value = [("id", "integer"), ("name", "varchar")]

    conn = MagicMock()
    conn.cursor.side_effect = [starting_cur, ready_cur]

    # Store connection and result for the Then step
    shared_data["retry_conn"] = conn
    shared_data["retry_result"] = introspect_column_types(conn, "mycat", "myschema", "mytable")


@then("the introspection retries with backoff up to the ready timeout")
def introspection_retries_with_backoff(shared_data):
    """
    Assert that:
    1. introspect_column_types eventually succeeded (returned a non-empty dict).
    2. The connection's cursor() was called more than once, proving a retry occurred.
    """
    result = shared_data["retry_result"]
    conn = shared_data["retry_conn"]

    assert isinstance(result, dict), "Expected a dict from introspect_column_types"
    assert result == {"id": "integer", "name": "varchar"}, (
        f"Unexpected column map after retry: {result!r}"
    )
    # cursor() must have been called at least twice: once for the failing attempt,
    # once for the successful attempt.
    assert conn.cursor.call_count >= 2, (
        f"Expected at least 2 cursor() calls (retry), got {conn.cursor.call_count}"
    )


@when("any other Trino error occurs")
def any_other_trino_error_occurs(shared_data):
    """
    Simulate a non-startup Trino error (e.g. CATALOG_NOT_FOUND).

    We store the connection and the raised exception for the Then step to inspect.
    """
    genuine_exc = trino.exceptions.TrinoUserError(
        {
            "errorName": "CATALOG_NOT_FOUND",
            "errorType": "USER_ERROR",
            "message": "Catalog 'nocat' does not exist",
        },
        query_id="q-bdd-923-genuine",
    )
    conn = _conn_raising(genuine_exc)
    shared_data["genuine_conn"] = conn
    shared_data["genuine_exc_type"] = type(genuine_exc)

    raised = None
    try:
        introspect_column_types(conn, "nocat", "s", "t")
    except Exception as exc:  # noqa: BLE001
        raised = exc

    shared_data["genuine_raised"] = raised


@then("the error propagates without retry")
def error_propagates_without_retry(shared_data):
    """
    Assert that:
    1. The genuine error was re-raised (not swallowed).
    2. Only one cursor() call was made - no retry for genuine errors.
    """
    raised = shared_data["genuine_raised"]
    conn = shared_data["genuine_conn"]
    exc_type = shared_data["genuine_exc_type"]

    assert raised is not None, (
        "Expected the genuine Trino error to propagate, but no exception was raised"
    )
    assert isinstance(raised, exc_type), (
        f"Expected {exc_type.__name__}, got {type(raised).__name__}: {raised}"
    )
    # Only one cursor() call - the error must not have triggered any retry loop
    assert conn.cursor.call_count == 1, (
        f"Genuine error should not trigger retry; cursor() called {conn.cursor.call_count} times"
    )
