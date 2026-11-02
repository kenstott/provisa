# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4b17c6-52e8-4d31-b7f0-63c8a05e2914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1188: per-plan Trino session properties, and the ones that are never optional.

A session hint is a plan-level decision — this query needs a bigger memory ceiling, that one is
retryable — so it has to travel with the plan rather than be set globally and hoped for. Three
things then matter: the hints reach the session the statement runs in, the deployment-wide hints
are underneath the plan's rather than over them, and the runaway-query timeout is always applied
even when the plan asked for nothing.

REQ-2194: a read runs through the Zaychik proxy, where every statement is a Trino session of its
own, so the hints travel WITH the statement (the ``x-provisa-trino-context`` call header) rather
than as ``SET SESSION`` statements before it. Ordering still matters: the session must be on the
statement it is for, not the next one.
"""

# Requirements: REQ-1188

from __future__ import annotations

from tests.zaychik_double import ProxyConnection, backend_state, trino_backend


def _run(session_hints=None, engine_session_hints=None) -> ProxyConnection:
    proxy = ProxyConnection()
    state = backend_state(proxy, engine_session_hints=engine_session_hints or {})
    trino_backend().execute_sync(state, "SELECT id FROM orders", session_hints=session_hints).rows
    return proxy


def test_a_plans_hints_are_set_on_the_session_before_its_query():
    proxy = _run({"query_max_memory": "8GB"})

    (session,) = proxy.sessions
    assert session["properties"]["query_max_memory"] == "8GB"
    # The session is the one the query runs in: sent with it, not after it.
    assert [kind for kind, _ in proxy.log] == ["session", "statement"]
    assert proxy.statements == ["SELECT id FROM orders"]


def test_the_runaway_query_timeout_is_applied_even_with_no_plan_hints():
    """A query with no hints is the common case and the one that starves workers when it hangs."""
    (session,) = _run().sessions

    assert session["properties"]["query_max_execution_time"].endswith("s")


def test_a_plan_may_override_the_default_timeout():
    (session,) = _run({"query_max_execution_time": "30s"}).sessions

    assert session["properties"]["query_max_execution_time"] == "30s"


def test_the_plans_hints_win_over_the_deployments():
    """The deployment sets a floor; a plan that asked for something specific asked for a reason."""
    (session,) = _run(
        {"query_max_memory": "8GB"},
        engine_session_hints={"query_max_memory": "2GB", "retry_policy": "QUERY"},
    ).sessions

    assert session["properties"]["query_max_memory"] == "8GB"
    # A deployment hint the plan said nothing about still applies.
    assert session["properties"]["retry_policy"] == "QUERY"


def test_a_quote_in_a_hint_cannot_close_a_statement():
    """A hint never becomes SQL: it is a session property value, carried as data, so a quote in it
    cannot end a string and let whatever follows execute as its own statement."""
    proxy = _run({"query_max_memory": "8GB'; DROP TABLE orders; --"})

    assert proxy.sessions[0]["properties"]["query_max_memory"] == "8GB'; DROP TABLE orders; --"
    assert proxy.statements == ["SELECT id FROM orders"]
