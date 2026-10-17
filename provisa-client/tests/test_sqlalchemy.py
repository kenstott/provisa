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

import httpx
import pytest
import respx
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import NoSuchTableError

from provisa_client.sqlalchemy_dialect import ProvisaDialect
from .catalog_double import SHOP_AND_SUPPORT, sa_connection

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
    assert "role" not in kwargs


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
        query={"role": "viewer"},
    )
    _, kwargs = dialect.create_connect_args(url)
    assert kwargs["role"] == "viewer"


# ── Reflection from /data/catalog (the SQL catalog the role is served) ─────────


@respx.mock
def test_get_schema_names_are_the_catalogs_domains():
    respx.get(f"{BASE}/data/catalog").mock(return_value=httpx.Response(200, json=SHOP_AND_SUPPORT))
    assert ProvisaDialect().get_schema_names(sa_connection()) == ["shop", "support"]


@respx.mock
def test_get_table_names_are_the_sql_names_of_one_schema():
    respx.get(f"{BASE}/data/catalog").mock(return_value=httpx.Response(200, json=SHOP_AND_SUPPORT))
    dialect = ProvisaDialect()
    assert dialect.get_table_names(sa_connection(), schema="shop") == ["orders", "users"]
    assert dialect.get_table_names(sa_connection(), schema="support") == ["tickets"]


@respx.mock
def test_get_table_names_without_a_schema_is_none_every_table_lives_in_one():
    respx.get(f"{BASE}/data/catalog").mock(return_value=httpx.Response(200, json=SHOP_AND_SUPPORT))
    assert ProvisaDialect().get_table_names(sa_connection()) == []


@respx.mock
def test_a_catalog_the_server_refuses_raises_rather_than_listing_nothing():
    respx.get(f"{BASE}/data/catalog").mock(return_value=httpx.Response(500, text="boom"))
    with pytest.raises(httpx.HTTPStatusError):
        ProvisaDialect().get_table_names(sa_connection(), schema="shop")


@respx.mock
def test_get_columns_reads_the_tables_arrow_schema():
    respx.get(f"{BASE}/data/catalog").mock(return_value=httpx.Response(200, json=SHOP_AND_SUPPORT))
    cols = ProvisaDialect().get_columns(sa_connection(), "orders", schema="shop")
    assert [c["name"] for c in cols] == ["id", "amount", "region", "placed_at"]


@respx.mock
def test_get_columns_of_a_table_not_in_the_schema_raises():
    respx.get(f"{BASE}/data/catalog").mock(return_value=httpx.Response(200, json=SHOP_AND_SUPPORT))
    with pytest.raises(NoSuchTableError):
        ProvisaDialect().get_columns(sa_connection(), "orders", schema="support")


@respx.mock
def test_the_catalog_is_read_as_the_connections_user_role_and_org():
    """The catalog is the role's: it goes to /data/catalog with the connection's credential."""
    route = respx.get(f"{BASE}/data/catalog").mock(
        return_value=httpx.Response(200, json=SHOP_AND_SUPPORT)
    )
    ProvisaDialect().get_table_names(sa_connection(org="acme"), schema="shop")
    headers = route.calls[0].request.headers
    assert headers["x-provisa-role"] == "analyst"
    assert headers["authorization"] == "Bearer tok"
    assert headers["x-org-provisa"] == "acme"


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
