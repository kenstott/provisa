# Copyright (c) 2026 Kenneth Stott
# Canary: 79c221c0-f265-473b-b4b5-f46aa2028c54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for SQLAlchemy dialect (AK4)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import respx
import httpx
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from provisa_client.sqlalchemy_dialect import ProvisaDialect

BASE = "http://localhost:8001"


# ── Dialect registration ──────────────────────────────────────────────────────

def test_dialect_name():
    assert ProvisaDialect.name == "provisa"


def test_dialect_driver():
    assert ProvisaDialect.driver == "provisa_client"


def test_dbapi_returns_dbapi_module():
    module = ProvisaDialect.dbapi()
    assert hasattr(module, "connect")
    assert hasattr(module, "apilevel")


# ── create_connect_args() ─────────────────────────────────────────────────────

def test_create_connect_args_http():
    dialect = ProvisaDialect()
    url = URL.create(
        "provisa+http",
        username="alice",
        password="secret",
        host="myserver",
        port=8001,
    )
    args, kwargs = dialect.create_connect_args(url)
    assert args == []
    assert kwargs["url"] == "http://myserver:8001"
    assert kwargs["username"] == "alice"
    assert kwargs["password"] == "secret"
    assert kwargs["role"] == "admin"
    assert kwargs["mode"] == "approved"


def test_create_connect_args_https():
    dialect = ProvisaDialect()
    url = URL.create(
        "provisa+https",
        username="bob",
        password="pw",
        host="secure.example.com",
        port=443,
    )
    args, kwargs = dialect.create_connect_args(url)
    assert kwargs["url"] == "https://secure.example.com:443"


def test_create_connect_args_defaults_port():
    dialect = ProvisaDialect()
    url = URL.create(
        "provisa+http",
        username="u",
        password="p",
        host="localhost",
    )
    _, kwargs = dialect.create_connect_args(url)
    assert ":8001" in kwargs["url"]


def test_create_connect_args_query_params():
    dialect = ProvisaDialect()
    url = URL.create(
        "provisa+http",
        username="u",
        password="p",
        host="localhost",
        port=8001,
        query={"role": "viewer", "mode": "catalog"},
    )
    _, kwargs = dialect.create_connect_args(url)
    assert kwargs["role"] == "viewer"
    assert kwargs["mode"] == "catalog"


# ── get_table_names() ─────────────────────────────────────────────────────────

@respx.mock
def test_get_table_names_returns_stable_ids():
    respx.get(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "persistedQueries": [
                        {"stableId": "get_orders", "status": "approved"},
                        {"stableId": "get_users", "status": "approved"},
                    ]
                }
            },
        )
    )
    dialect = ProvisaDialect()
    mock_conn = MagicMock()
    mock_conn.connection._base_url = BASE
    mock_conn.connection._role = "admin"

    with patch.object(dialect, "_get_base_url_and_role", return_value=(BASE, "admin")):
        names = dialect.get_table_names(mock_conn)

    assert "get_orders" in names
    assert "get_users" in names


@respx.mock
def test_get_table_names_returns_empty_on_error():
    respx.get(f"{BASE}/data/graphql").mock(
        return_value=httpx.Response(500)
    )
    dialect = ProvisaDialect()
    mock_conn = MagicMock()

    with patch.object(dialect, "_get_base_url_and_role", return_value=(BASE, "admin")):
        names = dialect.get_table_names(mock_conn)

    assert names == []


# ── has_table() ───────────────────────────────────────────────────────────────

def test_has_table_true():
    dialect = ProvisaDialect()
    mock_conn = MagicMock()
    with patch.object(dialect, "get_table_names", return_value=["get_orders", "get_users"]):
        assert dialect.has_table(mock_conn, "get_orders") is True


def test_has_table_false():
    dialect = ProvisaDialect()
    mock_conn = MagicMock()
    with patch.object(dialect, "get_table_names", return_value=["get_orders"]):
        assert dialect.has_table(mock_conn, "nonexistent") is False


# ── Engine construction ───────────────────────────────────────────────────────

def test_create_engine_does_not_raise():
    """Engine construction must succeed without connecting."""
    from sqlalchemy.dialects import registry as _registry
    _registry.register("provisa.http", "provisa_client.sqlalchemy_dialect", "ProvisaDialect")
    engine = create_engine("provisa+http://user:pass@localhost:8001")
    assert engine is not None
    assert engine.dialect.name == "provisa"
