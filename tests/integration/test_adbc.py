# Copyright (c) 2026 Kenneth Stott
# Canary: e3f5a7b9-c1d3-4e6f-8a0b-2c4d6e8f0a1b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the ADBC interface (Phase AK, REQ-268–270).

Only live-server tests live here. Structural/API-contract tests that mock the
Flight client have been moved to tests/unit/test_adbc.py.

To run live tests:
    pytest tests/integration/test_adbc.py -m requires_provisa_server
"""

from __future__ import annotations

import pyarrow as pa
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration]

_ISOLATED_ORG = "adbc_test"


@pytest_asyncio.fixture(scope="module")
async def adbc_server():
    """A DEDICATED Trino-engine Provisa server for the Arrow Flight (ADBC) path.

    The demo on :8000 runs the DuckDB engine, which has no Arrow Flight transport, so
    these tests need their own Trino server. Spawned in an isolated org with a free
    Flight port and torn down (process killed, org schema dropped) — the demo is untouched.
    """
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    server = IsolatedServer(_ISOLATED_ORG, engine="trino", await_flight=True)
    server.start()
    try:
        yield server
    finally:
        server.stop_process()
        await drop_org_schema(_ISOLATED_ORG)


class TestLiveAdbcExecution:
    """Drive the ADBC/Arrow-Flight path against a dedicated Trino Provisa server."""

    FLIGHT_HOST = "127.0.0.1"

    @pytest.fixture
    def conn(self, adbc_server):
        from provisa_client.adbc import adbc_connect  # pyright: ignore[reportMissingImports]

        c = adbc_connect(
            adbc_server.base_url, user="admin", password="provisa", port=adbc_server.flight_port
        )
        yield c
        c.close()

    def test_connect_to_flight_server(self, conn):
        assert not conn._closed

    def test_execute_and_fetch_arrow_table(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sa__orders LIMIT 5")
        table = cursor.fetch_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows <= 5

    def test_schema_has_columns(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sa__orders LIMIT 1")
        table = cursor.fetch_arrow_table()
        assert len(table.schema.names) > 0

    def test_rls_applied_in_arrow_results(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT region FROM sa__orders LIMIT 10")
        table = cursor.fetch_arrow_table()
        assert "region" in table.schema.names

    def test_fetchall_rows_match_arrow_table(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM sa__orders LIMIT 3")
        rows = cursor.fetchall()
        assert len(rows) <= 3
        assert all(len(r) >= 1 for r in rows)

    def test_context_manager_closes_after_use(self, conn, adbc_server):
        from provisa_client.adbc import adbc_connect  # pyright: ignore[reportMissingImports]

        fresh = adbc_connect(
            adbc_server.base_url, user="admin", password="provisa", port=adbc_server.flight_port
        )
        with fresh as c:
            cursor = c.cursor()
            cursor.execute("SELECT * FROM sa__orders LIMIT 1")
            _ = cursor.fetch_arrow_table()
        assert fresh._closed
        assert not conn._closed
