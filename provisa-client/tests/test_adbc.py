# Copyright (c) 2026 Kenneth Stott
# Canary: a29ccd69-9393-41c5-a404-b53491d7daa7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for ADBC interface (AK5)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
import respx
import httpx

from provisa_client.adbc import adbc_connect, AdbcConnection, AdbcCursor

BASE = "http://localhost:8001"


# ── adbc_connect() ────────────────────────────────────────────────────────────

@respx.mock
def test_adbc_connect_authenticates():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "flight-tok"})
    )
    mock_flight = MagicMock()
    with patch("pyarrow.flight.connect", return_value=mock_flight):
        conn = adbc_connect(BASE, user="alice", password="secret")
    assert conn._token == "flight-tok"
    assert conn._role == "admin"


@respx.mock
def test_adbc_connect_falls_back_to_user_as_role_when_auth_fails():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(401, json={"error": "bad creds"})
    )
    mock_flight = MagicMock()
    with patch("pyarrow.flight.connect", return_value=mock_flight):
        conn = adbc_connect(BASE, user="tester", password="wrong")
    assert conn._token is None
    assert conn._role == "tester"


@respx.mock
def test_adbc_connect_creates_flight_client():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    mock_flight = MagicMock()
    with patch("pyarrow.flight.connect", return_value=mock_flight) as mock_connect:
        conn = adbc_connect(BASE, user="u", password="p")
    mock_connect.assert_called_once_with("grpc://localhost:8815")


# ── AdbcConnection ────────────────────────────────────────────────────────────

def test_adbc_connection_cursor():
    mock_flight = MagicMock()
    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token="tok", base_url=BASE
    )
    cur = conn.cursor()
    assert isinstance(cur, AdbcCursor)


def test_adbc_connection_context_manager():
    mock_flight = MagicMock()
    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token="tok", base_url=BASE
    )
    with conn as c:
        assert not c._closed
    assert conn._closed


def test_adbc_connection_closed_raises_on_cursor():
    mock_flight = MagicMock()
    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token=None, base_url=BASE
    )
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        conn.cursor()


# ── AdbcCursor.execute() ──────────────────────────────────────────────────────

def test_cursor_execute_builds_correct_ticket():
    mock_flight = MagicMock()
    conn = AdbcConnection(
        flight_client=mock_flight, role="analyst", token="tok", base_url=BASE
    )
    mock_reader = MagicMock()
    mock_flight.do_get.return_value = mock_reader

    cur = conn.cursor()
    cur.execute("{ orders { id } }")

    call_args = mock_flight.do_get.call_args[0]
    ticket = call_args[0]
    data = json.loads(ticket.ticket.decode())
    assert data["query"] == "{ orders { id } }"
    assert data["role"] == "analyst"
    assert data["token"] == "tok"


def test_cursor_execute_ticket_no_token_when_none():
    mock_flight = MagicMock()
    conn = AdbcConnection(
        flight_client=mock_flight, role="guest", token=None, base_url=BASE
    )
    mock_reader = MagicMock()
    mock_flight.do_get.return_value = mock_reader

    cur = conn.cursor()
    cur.execute("{ x { y } }")

    call_args = mock_flight.do_get.call_args[0]
    ticket = call_args[0]
    data = json.loads(ticket.ticket.decode())
    assert "token" not in data


# ── fetch_arrow_table() ───────────────────────────────────────────────────────

def test_fetch_arrow_table_returns_table():
    mock_flight = MagicMock()
    expected_table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    mock_reader = MagicMock()
    mock_reader.read_all.return_value = expected_table
    mock_flight.do_get.return_value = mock_reader

    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token=None, base_url=BASE
    )
    cur = conn.cursor()
    cur.execute("{ items { id name } }")
    tbl = cur.fetch_arrow_table()
    assert tbl.num_rows == 3
    assert tbl.column_names == ["id", "name"]


# ── fetchone() / fetchall() ───────────────────────────────────────────────────

def test_fetchall_returns_tuples():
    mock_flight = MagicMock()
    expected_table = pa.table({"id": [10, 20], "val": ["x", "y"]})
    mock_reader = MagicMock()
    mock_reader.read_all.return_value = expected_table
    mock_flight.do_get.return_value = mock_reader

    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token=None, base_url=BASE
    )
    cur = conn.cursor()
    cur.execute("{ items { id val } }")
    rows = cur.fetchall()
    assert rows == [(10, "x"), (20, "y")]


def test_fetchone_iterates_rows():
    mock_flight = MagicMock()
    expected_table = pa.table({"id": [1, 2]})
    mock_reader = MagicMock()
    mock_reader.read_all.return_value = expected_table
    mock_flight.do_get.return_value = mock_reader

    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token=None, base_url=BASE
    )
    cur = conn.cursor()
    cur.execute("{ items { id } }")
    assert cur.fetchone() == (1,)
    assert cur.fetchone() == (2,)
    assert cur.fetchone() is None


# ── description ───────────────────────────────────────────────────────────────

def test_description_returns_column_tuples():
    mock_flight = MagicMock()
    expected_table = pa.table({"id": [1], "status": ["ok"]})
    mock_reader = MagicMock()
    mock_reader.read_all.return_value = expected_table
    mock_flight.do_get.return_value = mock_reader

    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token=None, base_url=BASE
    )
    cur = conn.cursor()
    cur.execute("{ items { id status } }")
    # Trigger read
    cur.fetch_arrow_table()
    desc = cur.description
    assert desc is not None
    assert [d[0] for d in desc] == ["id", "status"]


def test_description_none_before_execute():
    mock_flight = MagicMock()
    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token=None, base_url=BASE
    )
    cur = conn.cursor()
    assert cur.description is None


# ── context manager ───────────────────────────────────────────────────────────

def test_cursor_context_manager():
    mock_flight = MagicMock()
    conn = AdbcConnection(
        flight_client=mock_flight, role="admin", token=None, base_url=BASE
    )
    with conn.cursor() as cur:
        assert not cur._closed
    assert cur._closed
