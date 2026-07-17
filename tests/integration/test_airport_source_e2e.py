# Copyright (c) 2026 Kenneth Stott
# Canary: 8e1f6a2d-4c90-4b31-9a7e-5d0c2f8b1e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: Arrow Flight source read through DuckDB's `airport` COMMUNITY EXTENSION (REQ-899, REQ-1097).

Airport is neither a direct driver (provisa/executor/drivers/registry.py has no factory for it) nor
a Trino connector — the ONLY way Provisa reaches an Arrow Flight server is DuckDBAirportConnector
(provisa/federation/connector_duckdb.py:278), issuing ``ATTACH '<base_url>' AS "<id>" (TYPE AIRPORT)``.
This test drives that real seam against an in-process DuckDB connection, the same one
provisa.federation.duckdb_backend.DuckDBBackend wraps — same shape as test_firebird_source_e2e.py.

Why this can't reach a live table today, and why that's a documented gap, not a dodge
---------------------------------------------------------------------------------------
`zaychik` (tests/conftest.py `_CORE_SERVICES`) is the only Arrow Flight endpoint already running in
this harness's core stack. It is NOT reachable by the airport extension, for two independently
confirmed reasons:

1. Protocol mismatch. zaychik (docker-compose.core.yml `zaychik` service, built from
   Raiffeisen-DGTL/zaychik-trino-proxy) implements Arrow's standard *Flight SQL* protocol
   (`ru.raiffeisen.trino.arrow.flight.sql.server.FlightSQLTrinoProxy`, extending Arrow Java's
   `FlightSqlProducer`) — the same protocol `provisa/executor/trino_flight.py` speaks via
   `adbc_driver_flightsql`. DuckDB's `airport` extension does NOT speak Flight SQL: it issues a
   `DO_ACTION` RPC carrying its own proprietary action name (`airport-action-name: list_schemas`,
   captured live from a real ATTACH attempt against zaychik below) — an action zaychik's
   `FlightSqlProducer`-based `doAction` dispatch has no handler for.

2. Independently, and more fundamentally: authentication to zaychik is broken for ANY Arrow Flight
   client (Flight SQL included) under the current core config — reproduced live with the SAME
   `adbc_driver_flightsql` driver `trino_flight.py` uses in production:
   ``UNAUTHENTICATED: [FlightSQL]  (Unauthenticated; Prepare)``. zaychik's own logs show why:
   ``TrinoCredentialsValidator ... "TLS/SSL is required for authentication with username and
   password"`` — `TF_FLIGHT_AUTH_TYPE=trino` requires forwarding user/password to Trino, but
   `TF_TRINO_SSL=false` makes Trino's own JDBC driver refuse to carry them. Filed as
   https://github.com/kenstott/provisa/issues/74 (self-contradicting config, not an airport-specific
   issue — no client can authenticate to zaychik as currently configured).

No airport-protocol-compatible Arrow Flight server exists as installable test infra: Query-farm ships
only an abstract framework (`query-farm-flight-server`, pip) with ~10 required abstract methods and a
bespoke wire contract (MessagePack parameters, hashed catalog/schema serialization, DuckDB-expression
deserialization) — no ready reference backend. The extension's own CI tests against a private hosted
server (`grpc+tls://airport-ci.query.farm`), not self-hostable from public artifacts. Standing one up
from scratch is out of proportion to a test-infra addition.

What this test DOES verify live (no mocking)
----------------------------------------------
* The `airport` community extension genuinely installs + loads in this environment and registers
  `airport_take_flight` — the same load-only check `_DuckDBExtensionConnector.probe()` (REQ-904)
  performs, run for real here rather than against a fake `fetch`.
* `DuckDBAirportConnector.details()` builds the exact ATTACH DDL shape the runtime issues.
* The real ATTACH DDL is issued, in-process, against zaychik (the CORE service) — the one live Flight
  endpoint this harness provisions. ``ATTACH ... (TYPE AIRPORT)`` itself is lazy (confirmed live: it
  returns in <5ms and never contacts the server), so the connection is only actually exercised — and
  the auth failure surfaces — on the first schema-resolving query (``SHOW ALL TABLES``, which triggers
  the `list_schemas` DO_ACTION RPC). That query is asserted to fail with the exact captured,
  root-caused error above (not a bare/broad catch). Any other failure means the environment changed
  and this test should be revisited, not silently pass.
"""

from __future__ import annotations

import os

import duckdb
import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector_duckdb import DuckDBAirportConnector

pytestmark = [pytest.mark.integration]

_ZAYCHIK_HOST = os.environ.get("ZAYCHIK_HOST", "localhost")
_ZAYCHIK_PORT = int(os.environ.get("ZAYCHIK_PORT", "8480"))


def test_airport_extension_loads_and_registers_take_flight():
    """REQ-899/904: the real (non-mocked) load-only probe path — extension installs, loads, and
    airport_take_flight is registered. Mirrors _DuckDBExtensionConnector.probe() but against a live
    DuckDB connection instead of a fake fetch (see test_duckdb_community_connectors.py for the unit
    version)."""
    connector = DuckDBAirportConnector()
    conn = duckdb.connect()
    try:
        conn.execute(connector._install_sql())  # "INSTALL airport FROM community"
        conn.execute(f"LOAD {connector.extension}")

        rows = conn.execute(
            "SELECT count(*) FROM duckdb_functions() "
            f"WHERE function_name = '{connector.probe_symbol}'"
        ).fetchall()
        assert rows[0][0] >= 1  # airport_take_flight registered — extension genuinely loaded
    finally:
        conn.close()


def test_airport_attach_ddl_shape_against_zaychik():
    """DuckDBAirportConnector.details() builds the real ATTACH DDL; issuing it, unmodified, against
    zaychik (the one live Arrow Flight endpoint in the core stack) succeeds (ATTACH is lazy — confirmed
    live, it never contacts the server). The first schema-resolving query against that catalog (SHOW ALL
    TABLES, which triggers the `list_schemas` DO_ACTION RPC) fails with the exact protocol/auth error
    root-caused in the module docstring and filed as https://github.com/kenstott/provisa/issues/74 — not
    a generic catch-all."""
    src = Source(
        id="airport_itest",
        type=SourceType.airport,
        base_url=f"grpc://{_ZAYCHIK_HOST}:{_ZAYCHIK_PORT}",
    )
    connector = DuckDBAirportConnector()
    details = connector.details(src)
    assert details["attach"] == (
        f"ATTACH 'grpc://{_ZAYCHIK_HOST}:{_ZAYCHIK_PORT}' AS \"airport_itest\" (TYPE AIRPORT)"
    )

    conn = duckdb.connect()
    try:
        conn.execute(connector._install_sql())
        conn.execute(f"LOAD {connector.extension}")
        conn.execute(details["attach"])  # the real ATTACH DDL, unmodified — lazy, does not touch zaychik

        with pytest.raises(duckdb.IOException, match="list_schemas.*unauthenticated"):
            conn.execute("SHOW ALL TABLES")  # first schema resolution — triggers list_schemas RPC
    finally:
        conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
