# Copyright (c) 2026 Kenneth Stott
# Canary: 3842e5d3-8547-4919-89a1-a7cb1a802178
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the CRITICAL Arrow-Flight-over-Trino transport (REQ-045, REQ-144, REQ-271).

This guards the engine's Arrow terminal: Provisa streams governed query results out of Trino as
Arrow via the zaychik Flight SQL proxy (provisa/executor/trino_flight.py →
provisa/federation/backend.py::execute_arrow). Distinct from tests/integration/
test_arrow_flight_integration.py, which covers Provisa's OWN Flight server (api/flight/server.py);
this covers the Provisa→zaychik→Trino read path.

Auth model (why password is empty — see REQ-045): the Flight connection uses a single trusted
service identity (user="provisa", password=""). Per-user governance/RLS is applied in the SQL by
the compiler BEFORE the query reaches Trino; Trino is accessed as one identity over the isolated
network. Password authentication is intentionally NOT used here — Trino requires TLS for password
auth, and the proxy runs plaintext on the private network. (A Flight client that supplies a
non-empty password over this plaintext hop gets UNAUTHENTICATED from Trino — expected, not a bug;
that constraint, plus the airport extension's incompatible DO_ACTION protocol, is what issue #74
is actually about, not this transport.)
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]


def _zaychik_port() -> int:
    return int(os.environ["ZAYCHIK_PORT"])  # allocated by the isolated-stack harness (conftest)


def test_trino_flight_arrow_round_trip():
    from provisa.executor.trino_flight import create_flight_connection, execute_trino_flight_arrow

    conn = create_flight_connection(host="localhost", port=_zaychik_port(), user="provisa")
    try:
        table = execute_trino_flight_arrow(conn, "SELECT 1 AS x, 'hi' AS y", None)
        assert table.num_rows == 1
        assert set(table.column_names) == {"x", "y"}
        assert table.to_pydict() == {"x": [1], "y": ["hi"]}
    finally:
        conn.close()


def test_trino_flight_multi_row_arrow():
    from provisa.executor.trino_flight import create_flight_connection, execute_trino_flight_arrow

    conn = create_flight_connection(host="localhost", port=_zaychik_port(), user="provisa")
    try:
        table = execute_trino_flight_arrow(
            conn, "SELECT n FROM UNNEST(sequence(1, 5)) AS t(n) ORDER BY n", None
        )
        assert table.num_rows == 5
        assert table.column("n").to_pylist() == [1, 2, 3, 4, 5]
    finally:
        conn.close()
