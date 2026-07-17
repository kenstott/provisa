# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2c9d41-6a5f-4b8e-9c3a-1f5d8b0e4a97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: an airport source read through DuckDB's `airport` COMMUNITY EXTENSION (REQ-899, REQ-1097).

airport is neither a direct driver (provisa/executor/drivers/registry.py has no factory for it)
nor a Trino connector — the ONLY way Provisa reaches it is DuckDBAirportConnector
(provisa/federation/connector_duckdb.py:278), one of the REQ-899 DuckDB community-extension
connectors wired into the DuckDB partial-federator engine. There is no coordinator process to talk
to: the engine IS an in-process DuckDB connection, and reaching an airport source means that
connection LOADs the `airport` DuckDB extension and issues
``ATTACH '<location>' AS "<id>" (TYPE AIRPORT)`` — exactly the DDL DuckDBAirportConnector.details()
builds. This test drives that real seam against an in-process DuckDB connection.

Why not zaychik (this repo's other Flight fixture)
----------------------------------------------------
zaychik implements Arrow **Flight SQL** (a standardized SQL-over-Flight application protocol).
DuckDB's `airport` extension makes DuckDB a client of a DIFFERENT, bespoke Flight application
protocol (custom DoAction/catalog-listing RPCs defined by the Query.Farm airport extension itself)
— not Flight SQL. The two are wire-incompatible: an airport ATTACH against zaychik cannot get past
the catalog-listing handshake. Reaching a REAL airport-protocol server therefore needs a REAL
airport-protocol server — this test builds and attaches to one: tests/fixtures/airport_shim, a
~30-line Go program (github.com/hugr-lab/airport-go) exposing a static catalog (schema "test",
table "widgets") over plaintext gRPC, no TLS, no auth. Verified live before writing this test:

    ATTACH 'grpc://localhost:<port>' AS "airport_itest" (TYPE AIRPORT)
    SELECT id, name FROM "airport_itest".test.widgets ORDER BY id
    -> [(1, 'Widget A'), (2, 'Widget B'), (3, 'Widget C')]

— i.e. DuckDBAirportConnector.details()'s DDL is correct as written: the ATTACH string itself is
the Flight location (no separate `location` option needed for a bare grpc:// endpoint).

Why the network call is bounded in a subprocess
------------------------------------------------
The airport ATTACH/query is a DuckDB C-extension network call to a server outside this process; a
misconfigured or hung Flight handshake could otherwise hang the whole suite. It is run in a
subprocess with a hard wall-clock timeout so a hang can never propagate.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

pytestmark = [pytest.mark.integration]

_AIRPORT_PORT = int(os.environ.get("AIRPORT_PORT", "50051"))
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]

# Runs in a fresh subprocess (bounded by subprocess.run(timeout=...) below) so a hung Flight
# handshake against a misconfigured server can never hang the test suite itself.
_DRIVER_SCRIPT = """
import json
import sys

import duckdb

from provisa.core.models import Source, SourceType
from provisa.federation.connector_duckdb import DuckDBAirportConnector

src = Source(id="airport_itest", type=SourceType.airport, base_url=sys.argv[1])
connector = DuckDBAirportConnector()
details = connector.details(src)
assert "ATTACH" in details["attach"] and '(TYPE AIRPORT)' in details["attach"], details

conn = duckdb.connect()
conn.execute(connector._install_sql())  # "INSTALL airport FROM community"
conn.execute(f"LOAD {connector.extension}")

probe = conn.execute(
    "SELECT count(*) FROM duckdb_functions() WHERE function_name = ?", [connector.probe_symbol]
).fetchall()
assert probe[0][0] >= 1, "airport_take_flight not registered — extension not actually loaded"

conn.execute(details["attach"])  # the real ATTACH DDL, unmodified

rows = conn.execute('SELECT id, name FROM "airport_itest".test.widgets ORDER BY id').fetchall()
print(json.dumps(rows))
"""


@pytest.mark.requires_airport
def test_airport_attached_and_queried_through_duckdb_engine():
    """Drive the REAL DuckDBAirportConnector.details() ATTACH DDL against an in-process DuckDB
    connection — the same seam provisa.federation.duckdb_backend.DuckDBBackend's persistent
    DuckDBFederationRuntime uses (REQ-899/1097) — against the airport_shim fixture server, asserting
    the 3 real rows come back through the connector."""
    location = f"grpc://localhost:{_AIRPORT_PORT}"

    result = subprocess.run(
        [sys.executable, "-c", _DRIVER_SCRIPT, location],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"airport driver subprocess failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    )

    rows = json.loads(result.stdout.strip().splitlines()[-1])
    assert [tuple(r) for r in rows] == _WIDGETS


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
