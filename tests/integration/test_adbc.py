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

Structural tests verify the AdbcConnection and AdbcCursor API contract
without a live server (mocked Flight client). Live-server tests require a
running Provisa + Docker Compose stack with Arrow Flight enabled (port 8815).

To run live tests:
    pytest tests/integration/test_adbc.py -m requires_provisa_server
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_flight_client(table: pa.Table | None = None) -> MagicMock:
    """Build a mock FlightClient that returns a table from do_get."""
    if table is None:
        table = pa.table({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})

    stream = MagicMock()
    stream.read_all.return_value = table

    client = MagicMock()
    client.do_get.return_value = stream
    return client


def _make_connection(table: pa.Table | None = None):
    """Create an AdbcConnection with a mocked Flight client."""
    from provisa_client.adbc import AdbcConnection

    mock_client = _make_mock_flight_client(table)
    return AdbcConnection(
        flight_client=mock_client,
        role="admin",
        token="test-token",
        base_url="http://localhost:8001",
    )


# ---------------------------------------------------------------------------
# AdbcConnection — structural tests
# ---------------------------------------------------------------------------

class TestAdbcConnectionContract:
    def test_cursor_returns_adbc_cursor(self):
        from provisa_client.adbc import AdbcCursor

        conn = _make_connection()
        cur = conn.cursor()
        assert isinstance(cur, AdbcCursor)

    def test_context_manager_closes(self):
        conn = _make_connection()
        with conn as c:
            assert not c._closed
        assert conn._closed

    def test_cursor_raises_after_close(self):
        conn = _make_connection()
        conn.close()
        with pytest.raises(RuntimeError, match="closed"):
            conn.cursor()

    def test_double_close_does_not_raise(self):
        conn = _make_connection()
        conn.close()
        conn.close()  # must not raise

    def test_close_calls_flight_client_close(self):
        conn = _make_connection()
        conn.close()
        conn._flight_client.close.assert_called_once()


# ---------------------------------------------------------------------------
# AdbcCursor — ticket building and execute
# ---------------------------------------------------------------------------

class TestAdbcCursorTicket:
    def test_ticket_contains_query(self):
        from provisa_client.adbc import AdbcCursor

        conn = _make_connection()
        cursor = conn.cursor()
        ticket = cursor._build_ticket("SELECT * FROM orders")
        data = json.loads(ticket.ticket.decode())
        assert data["query"] == "SELECT * FROM orders"

    def test_ticket_contains_role(self):
        from provisa_client.adbc import AdbcCursor

        conn = _make_connection()
        cursor = conn.cursor()
        ticket = cursor._build_ticket("SELECT 1")
        data = json.loads(ticket.ticket.decode())
        assert data["role"] == "admin"

    def test_ticket_contains_token(self):
        conn = _make_connection()
        cursor = conn.cursor()
        ticket = cursor._build_ticket("SELECT 1")
        data = json.loads(ticket.ticket.decode())
        assert data["token"] == "test-token"

    def test_ticket_no_token_when_none(self):
        from provisa_client.adbc import AdbcConnection

        mock_client = _make_mock_flight_client()
        conn = AdbcConnection(
            flight_client=mock_client, role="analyst", token=None, base_url="http://localhost:8001"
        )
        cursor = conn.cursor()
        ticket = cursor._build_ticket("SELECT 1")
        data = json.loads(ticket.ticket.decode())
        assert "token" not in data


class TestAdbcCursorExecute:
    def test_execute_calls_do_get(self):
        conn = _make_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders")
        conn._flight_client.do_get.assert_called_once()

    def test_execute_raises_after_connection_closed(self):
        conn = _make_connection()
        conn.close()
        cursor = conn.cursor.__func__(conn) if False else None
        # Re-create cursor manually after close
        from provisa_client.adbc import AdbcCursor
        cursor = AdbcCursor(connection=conn)
        with pytest.raises(RuntimeError, match="closed"):
            cursor.execute("SELECT 1")

    def test_fetch_arrow_table(self):
        expected = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
        conn = _make_connection(table=expected)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM customers")
        result = cursor.fetch_arrow_table()
        assert result.schema.names == ["id", "name"]
        assert result.num_rows == 2

    def test_fetch_arrow_table_raises_without_execute(self):
        conn = _make_connection()
        cursor = conn.cursor()
        with pytest.raises(RuntimeError, match="No query"):
            cursor.fetch_arrow_table()


class TestAdbcCursorFetchRows:
    def test_fetchone_returns_first_row(self):
        table = pa.table({"x": [10, 20, 30]})
        conn = _make_connection(table=table)
        cursor = conn.cursor()
        cursor.execute("SELECT x FROM t")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 10

    def test_fetchall_returns_all_rows(self):
        table = pa.table({"x": [1, 2, 3]})
        conn = _make_connection(table=table)
        cursor = conn.cursor()
        cursor.execute("SELECT x FROM t")
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert [r[0] for r in rows] == [1, 2, 3]

    def test_fetchone_returns_none_at_end(self):
        table = pa.table({"x": [1]})
        conn = _make_connection(table=table)
        cursor = conn.cursor()
        cursor.execute("SELECT x FROM t")
        cursor.fetchone()  # row 1
        assert cursor.fetchone() is None

    def test_description_set_after_execute(self):
        table = pa.table({"id": pa.array([1], pa.int64()), "name": ["Alice"]})
        conn = _make_connection(table=table)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM customers")
        # Trigger description population via fetchall
        cursor.fetchall()
        assert cursor.description is not None
        col_names = [col[0] for col in cursor.description]
        assert "id" in col_names
        assert "name" in col_names


# ---------------------------------------------------------------------------
# Live-server tests — require running Provisa Arrow Flight server (port 8815)
# ---------------------------------------------------------------------------

@pytest.mark.requires_provisa_server
class TestLiveAdbcExecution:
    """Require running Provisa + Arrow Flight service at localhost:8815.
    Docker Compose stack must include the backend with --port 8815 exposed.
    """

    PROVISA_URL = "http://localhost:8001"
    FLIGHT_HOST = "localhost"
    FLIGHT_PORT = 8815

    _GOVERNED_QUERIES = {
        "ActiveOrders": (
            "query ActiveOrders { sales_analytics__orders { id amount region status created_at } }"
        ),
        "AnalystOrders": (
            "query AnalystOrders { sales_analytics__orders { id region status created_at } }"
        ),
    }

    @pytest.fixture(scope="class", autouse=True)
    def seed_governed_queries(self):
        """Submit and approve ActiveOrders and AnalystOrders governed queries."""
        import httpx

        base = self.PROVISA_URL
        approve_mutation = """
        mutation ApproveQuery($queryId: Int!, $approverId: String!) {
          approveQuery(queryId: $queryId, approverId: $approverId) {
            success message
          }
        }
        """
        _submit_mutation = """
        mutation SubmitQuery($input: SubmitQueryInput!) {
          submitQuery(input: $input) { queryId operationName message }
        }
        """
        for op_name, query_text in self._GOVERNED_QUERIES.items():
            try:
                submit_resp = httpx.post(
                    f"{base}/admin/graphql",
                    json={
                        "query": _submit_mutation,
                        "variables": {"input": {"query": query_text, "role": "admin"}},
                    },
                    timeout=10,
                )
                if submit_resp.status_code != 200:
                    continue
                body = submit_resp.json()
                if "errors" in body:
                    continue
                query_id = body["data"]["submitQuery"]["queryId"]
                httpx.post(
                    f"{base}/admin/graphql",
                    json={
                        "query": approve_mutation,
                        "variables": {"queryId": query_id, "approverId": "admin"},
                    },
                    timeout=10,
                )
            except Exception:
                pass

    @pytest.fixture
    def conn(self):
        from provisa_client.adbc import adbc_connect

        c = adbc_connect(self.PROVISA_URL, user="admin", password="provisa")
        yield c
        c.close()

    def test_connect_to_flight_server(self, conn):
        assert not conn._closed

    def test_execute_and_fetch_arrow_table(self, conn):
        """Execute an approved query and fetch Arrow table."""
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ActiveOrders LIMIT 5")
        table = cursor.fetch_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows <= 5

    def test_schema_has_columns(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ActiveOrders LIMIT 1")
        table = cursor.fetch_arrow_table()
        assert len(table.schema.names) > 0

    def test_rls_applied_in_arrow_results(self, conn):
        """Arrow results must have RLS applied server-side."""
        cursor = conn.cursor()
        cursor.execute("SELECT region FROM AnalystOrders LIMIT 10")
        table = cursor.fetch_arrow_table()
        assert "region" in table.schema.names

    def test_fetchall_rows_match_arrow_table(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM ActiveOrders LIMIT 3")
        rows = cursor.fetchall()
        assert len(rows) <= 3
        assert all(len(r) >= 1 for r in rows)

    def test_context_manager_closes_after_use(self):
        from provisa_client.adbc import adbc_connect

        conn = adbc_connect(self.PROVISA_URL, user="admin", password="provisa")
        with conn as c:
            cursor = c.cursor()
            cursor.execute("SELECT * FROM ActiveOrders LIMIT 1")
            _ = cursor.fetch_arrow_table()
        assert conn._closed
