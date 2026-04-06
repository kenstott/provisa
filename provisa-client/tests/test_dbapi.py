# Copyright (c) 2026 Kenneth Stott
# Canary: 66283ce0-df33-4d26-a3e3-38e874554bd2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for PEP 249 DB-API 2.0 interface (AK3)."""

from __future__ import annotations

import pytest
import respx
import httpx

from provisa_client import connect
from provisa_client.dbapi import (
    Connection,
    Cursor,
    OperationalError,
    ProgrammingError,
    _is_graphql,
)

BASE = "http://localhost:8001"


# ── _is_graphql() ─────────────────────────────────────────────────────────────

def test_is_graphql_curly_brace():
    assert _is_graphql("{ orders { id } }") is True


def test_is_graphql_query_keyword():
    assert _is_graphql("query GetOrders { orders { id } }") is True


def test_is_graphql_mutation_keyword():
    assert _is_graphql("mutation CreateOrder { createOrder { id } }") is True


def test_is_graphql_case_insensitive():
    assert _is_graphql("  Query GetOrders { orders { id } }") is True


def test_is_not_graphql_sql():
    assert _is_graphql("SELECT id FROM orders") is False


def test_is_not_graphql_select_lower():
    assert _is_graphql("select * from orders") is False


# ── connect() authentication ──────────────────────────────────────────────────

@respx.mock
def test_connect_authenticates_and_stores_token():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "my-token"})
    )
    conn = connect(BASE, username="alice", password="secret", role="analyst")
    assert conn._token == "my-token"
    assert conn._role == "analyst"


@respx.mock
def test_connect_falls_back_to_username_as_role_when_auth_fails():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(401, json={"error": "unauthorized"})
    )
    conn = connect(BASE, username="testuser", password="wrong")
    assert conn._token is None
    assert conn._role == "testuser"


@respx.mock
def test_connect_context_manager():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    with connect(BASE, username="u", password="p") as conn:
        assert not conn._closed
    assert conn._closed


# ── execute() GraphQL ─────────────────────────────────────────────────────────

@respx.mock
def test_execute_graphql_returns_rows():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"orders": [{"id": 1, "amount": 9.99}, {"id": 2, "amount": 4.50}]}},
        )
    )
    conn = connect(BASE, username="u", password="p", role="analyst")
    cur = conn.cursor()
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()
    assert rows == [(1, 9.99), (2, 4.50)]


@respx.mock
def test_execute_graphql_sets_description():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"orders": [{"id": 1, "name": "foo"}]}},
        )
    )
    conn = connect(BASE, username="u", password="p")
    cur = conn.cursor()
    cur.execute("{ orders { id name } }")
    assert cur.description is not None
    col_names = [d[0] for d in cur.description]
    assert col_names == ["id", "name"]


@respx.mock
def test_execute_graphql_raises_on_errors():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200,
            json={"errors": [{"message": "field not found"}]},
        )
    )
    conn = connect(BASE, username="u", password="p")
    cur = conn.cursor()
    with pytest.raises(ProgrammingError):
        cur.execute("{ badField { id } }")


# ── execute() SQL ─────────────────────────────────────────────────────────────

@respx.mock
def test_execute_sql_returns_rows():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/sql").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"_sql": [{"id": 10, "status": "active"}]}},
        )
    )
    conn = connect(BASE, username="u", password="p")
    cur = conn.cursor()
    cur.execute("SELECT id, status FROM orders")
    rows = cur.fetchall()
    assert rows == [(10, "active")]


@respx.mock
def test_execute_sql_list_response():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/sql").mock(
        return_value=httpx.Response(
            200,
            json=[{"id": 5, "val": "x"}],
        )
    )
    conn = connect(BASE, username="u", password="p")
    cur = conn.cursor()
    cur.execute("SELECT id, val FROM t")
    assert cur.fetchall() == [(5, "x")]


# ── fetchone / fetchmany / fetchall ───────────────────────────────────────────

@respx.mock
def test_fetchone_returns_single_row():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}},
        )
    )
    conn = connect(BASE, username="u", password="p")
    cur = conn.cursor()
    cur.execute("{ items { id } }")
    assert cur.fetchone() == (1,)
    assert cur.fetchone() == (2,)
    assert cur.fetchone() == (3,)
    assert cur.fetchone() is None


@respx.mock
def test_fetchmany_respects_size():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"items": [{"id": i} for i in range(5)]}},
        )
    )
    conn = connect(BASE, username="u", password="p")
    cur = conn.cursor()
    cur.execute("{ items { id } }")
    batch = cur.fetchmany(3)
    assert len(batch) == 3
    rest = cur.fetchall()
    assert len(rest) == 2


@respx.mock
def test_fetchmany_uses_arraysize_default():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"items": [{"id": 1}, {"id": 2}]}},
        )
    )
    conn = connect(BASE, username="u", password="p")
    cur = conn.cursor()
    cur.arraysize = 1
    cur.execute("{ items { id } }")
    assert cur.fetchmany() == [(1,)]


# ── rowcount ──────────────────────────────────────────────────────────────────

@respx.mock
def test_rowcount_set_after_execute():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"token": "tok"})
    )
    respx.post(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"items": [{"id": 1}, {"id": 2}]}},
        )
    )
    conn = connect(BASE, username="u", password="p")
    cur = conn.cursor()
    cur.execute("{ items { id } }")
    assert cur.rowcount == 2


# ── closed cursor / connection ────────────────────────────────────────────────

def test_closed_cursor_raises():
    conn = Connection(base_url=BASE, token=None, role="admin", mode="approved")
    cur = conn.cursor()
    cur.close()
    with pytest.raises(OperationalError):
        cur.execute("SELECT 1")


def test_closed_connection_raises_on_cursor():
    conn = Connection(base_url=BASE, token=None, role="admin", mode="approved")
    conn.close()
    with pytest.raises(OperationalError):
        conn.cursor()


# ── no-op commit / rollback ───────────────────────────────────────────────────

def test_commit_is_noop():
    conn = Connection(base_url=BASE, token=None, role="admin", mode="approved")
    conn.commit()  # must not raise


def test_rollback_is_noop():
    conn = Connection(base_url=BASE, token=None, role="admin", mode="approved")
    conn.rollback()  # must not raise
