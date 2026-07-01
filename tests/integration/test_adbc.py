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

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Live-server tests — require running Provisa Arrow Flight server (port 8815)
# ---------------------------------------------------------------------------


@pytest.mark.requires_provisa_server
class TestLiveAdbcExecution:
    """Require running Provisa + Arrow Flight service at localhost:8815.
    Docker Compose stack must include the backend with --port 8815 exposed.
    """

    PROVISA_URL = "http://localhost:8000"
    FLIGHT_HOST = "localhost"
    FLIGHT_PORT = 8815

    @pytest.fixture
    def conn(self):
        from provisa_client.adbc import adbc_connect  # pyright: ignore[reportMissingImports]

        c = adbc_connect(self.PROVISA_URL, user="admin", password="provisa")
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

    def test_context_manager_closes_after_use(self, conn):
        from provisa_client.adbc import adbc_connect  # pyright: ignore[reportMissingImports]

        fresh = adbc_connect(self.PROVISA_URL, user="admin", password="provisa")
        with fresh as c:
            cursor = c.cursor()
            cursor.execute("SELECT * FROM sa__orders LIMIT 1")
            _ = cursor.fetch_arrow_table()
        assert fresh._closed
        assert not conn._closed
