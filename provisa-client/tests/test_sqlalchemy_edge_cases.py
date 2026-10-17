# Copyright (c) 2026 Kenneth Stott
# Canary: 9a0b1c2d-3e4f-4a5b-6c7d-8e9f0a1b2c3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLAlchemy dialect edge-case tests not covered by test_sqlalchemy.py.

Already tested in test_sqlalchemy.py / test_dbapi.py (NOT duplicated here):
  - ProvisaDialect.name / driver / dbapi()
  - create_connect_args() for http / https / defaults / query params
  - get_schema_names() / get_table_names() / get_columns() from /data/catalog, and a refused
    catalog raising
  - has_table() true / false
  - create_engine() construction
  - cursor.fetchmany(size=N) with explicit size
  - cursor.fetchmany() using arraysize default
  - cursor.description after execute
  - cursor.rowcount set after execute
  - closed cursor / connection raises OperationalError
  - commit / rollback no-ops
  - _is_graphql detection

Gap areas covered here:
  1. get_columns() — each column's SQLAlchemy type, nullability and comment from its Arrow field.
  2. get_columns() — an unknown table raises NoSuchTableError.
  3. get_columns() — a refused catalog raises.
  4. get_columns() — an Arrow type with no SQLAlchemy counterpart is NullType, with a warning.
  5. get_foreign_keys() — always returns [].
  6. get_indexes() — always returns [].
  7. get_pk_constraint() — returns empty constraint dict.
  8. get_unique_constraints() — returns [].
  9. do_execute() — delegates to cursor.execute().
  10. Connection string parsing — provisa+http scheme, user/pass/host/port extracted.
  11. _apply_parameters() — :name placeholder substitution for string values.
  12. _apply_parameters() — :name substitution for numeric values.
  13. _apply_parameters() — no-op when parameters is None.
  14. cursor.rowcount is -1 before any execute() call (PEP 249).
  15. cursor.description is None before any execute() call (PEP 249).
  16. A SQLAlchemy connection whose DB-API connection is not Provisa's is refused.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pyarrow as pa
import pytest
import respx
import sqlalchemy.types as sqltypes
from sqlalchemy.engine import URL
from sqlalchemy.exc import NoSuchTableError, SAWarning

from provisa_client.dbapi import (
    Connection,
    _apply_parameters,
)
from provisa_client.sqlalchemy_dialect import ProvisaDialect

from .catalog_double import SHOP_AND_SUPPORT, catalog, sa_connection

BASE = "http://localhost:8001"


# ---------------------------------------------------------------------------
# get_columns()
# ---------------------------------------------------------------------------


class TestGetColumns:
    @respx.mock
    def test_each_columns_type_nullability_and_comment_come_from_its_arrow_field(self):
        respx.get(f"{BASE}/data/catalog").mock(
            return_value=httpx.Response(200, json=SHOP_AND_SUPPORT)
        )
        cols = ProvisaDialect().get_columns(sa_connection(), "orders", schema="shop")
        by_name = {c["name"]: c for c in cols}
        assert isinstance(by_name["id"]["type"], sqltypes.BigInteger)
        assert by_name["id"]["nullable"] is False
        amount = by_name["amount"]["type"]
        assert isinstance(amount, sqltypes.Numeric)
        assert (amount.precision, amount.scale) == (12, 2)
        assert by_name["amount"]["comment"] == "Order total"
        assert isinstance(by_name["region"]["type"], sqltypes.String)
        placed = by_name["placed_at"]["type"]
        assert isinstance(placed, sqltypes.DateTime) and placed.timezone is True
        users = ProvisaDialect().get_columns(sa_connection(), "users", schema="shop")
        assert [type(c["type"]) for c in users] == [sqltypes.Integer, sqltypes.Boolean]
        tickets = ProvisaDialect().get_columns(sa_connection(), "tickets", schema="support")
        assert isinstance(tickets[1]["type"], sqltypes.Date)

    @respx.mock
    def test_an_unknown_table_raises(self):
        respx.get(f"{BASE}/data/catalog").mock(
            return_value=httpx.Response(200, json=SHOP_AND_SUPPORT)
        )
        with pytest.raises(NoSuchTableError):
            ProvisaDialect().get_columns(sa_connection(), "nonexistent_table", schema="shop")

    @respx.mock
    def test_a_refused_catalog_raises(self):
        respx.get(f"{BASE}/data/catalog").mock(return_value=httpx.Response(403, text="no"))
        with pytest.raises(httpx.HTTPStatusError):
            ProvisaDialect().get_columns(sa_connection(), "orders", schema="shop")

    @respx.mock
    def test_an_arrow_type_with_no_sqlalchemy_counterpart_is_nulltype_with_a_warning(self):
        listed = pa.schema([pa.field("tags", pa.list_(pa.utf8()))])
        respx.get(f"{BASE}/data/catalog").mock(
            return_value=httpx.Response(200, json=catalog(("shop", "tagged", listed)))
        )
        with pytest.warns(SAWarning, match="tags"):
            [col] = ProvisaDialect().get_columns(sa_connection(), "tagged", schema="shop")
        assert isinstance(col["type"], sqltypes.NullType)


# ---------------------------------------------------------------------------
# Schema introspection stubs
# ---------------------------------------------------------------------------


class TestDialectSchemaStubs:
    """Verify the stub methods return the expected constant values."""

    @respx.mock
    def test_get_schema_names_are_the_domains(self):
        respx.get(f"{BASE}/data/catalog").mock(
            return_value=httpx.Response(200, json=SHOP_AND_SUPPORT)
        )
        assert ProvisaDialect().get_schema_names(sa_connection()) == ["shop", "support"]

    def test_get_foreign_keys_returns_empty_list(self):
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        result = dialect.get_foreign_keys(mock_conn, "orders")
        assert result == []

    def test_get_indexes_returns_empty_list(self):
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        result = dialect.get_indexes(mock_conn, "orders")
        assert result == []

    def test_get_pk_constraint_returns_empty_constraint(self):
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        result = dialect.get_pk_constraint(mock_conn, "orders")
        assert result == {"constrained_columns": [], "name": None}

    def test_get_unique_constraints_returns_empty_list(self):
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        result = dialect.get_unique_constraints(mock_conn, "orders")
        assert result == []


# ---------------------------------------------------------------------------
# do_execute()
# ---------------------------------------------------------------------------


class TestDoExecute:
    def test_do_execute_calls_cursor_execute(self):
        """do_execute() must delegate to cursor.execute(statement, parameters)."""
        dialect = ProvisaDialect()
        mock_cursor = MagicMock()
        mock_context = MagicMock()

        dialect.do_execute(mock_cursor, "{ orders { id } }", None, mock_context)

        mock_cursor.execute.assert_called_once_with("{ orders { id } }", None)

    def test_do_execute_passes_parameters(self):
        """do_execute() forwards a non-None parameters value."""
        dialect = ProvisaDialect()
        mock_cursor = MagicMock()
        params = {"id": 42}

        dialect.do_execute(mock_cursor, "SELECT 1", params, None)

        mock_cursor.execute.assert_called_once_with("SELECT 1", params)


# ---------------------------------------------------------------------------
# Connection string parsing
# ---------------------------------------------------------------------------


class TestConnectionStringParsing:
    """Test URL → connect-args conversion for different URL forms."""

    def test_http_scheme_extracted(self):
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="user", password="pass", host="myhost", port=8001)
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["url"].startswith("http://")

    def test_https_scheme_extracted(self):
        dialect = ProvisaDialect()
        url = URL.create(
            "provisa+https", username="user", password="pass", host="secure.host", port=443
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["url"].startswith("https://")

    def test_host_and_port_in_url(self):
        dialect = ProvisaDialect()
        url = URL.create(
            "provisa+http", username="u", password="p", host="db.example.com", port=9090
        )
        _, kwargs = dialect.create_connect_args(url)
        assert "db.example.com:9090" in kwargs["url"]

    def test_username_and_password_forwarded(self):
        dialect = ProvisaDialect()
        url = URL.create(
            "provisa+http", username="alice", password="s3cret", host="localhost", port=8001
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["username"] == "alice"
        assert kwargs["password"] == "s3cret"

    def test_no_default_role_when_not_in_query(self):
        """When no role query param is given, role is omitted from connect args."""
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="u", password="p", host="localhost", port=8001)
        _, kwargs = dialect.create_connect_args(url)
        assert "role" not in kwargs

    def test_role_from_query_string(self):
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

    def test_args_list_is_empty(self):
        """create_connect_args must return an empty positional args list."""
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="u", password="p", host="localhost", port=8001)
        args, _ = dialect.create_connect_args(url)
        assert args == []


# ---------------------------------------------------------------------------
# _apply_parameters() — parameter substitution
# ---------------------------------------------------------------------------


class TestApplyParameters:
    """Named-parameter substitution used by the DB-API cursor execute path."""

    def test_string_value_quoted(self):
        """String parameters must be wrapped in single quotes."""
        result = _apply_parameters("SELECT * FROM t WHERE region = :region", {"region": "us-east"})
        assert "region = 'us-east'" in result

    def test_numeric_value_unquoted(self):
        """Numeric parameters must be substituted without quotes."""
        result = _apply_parameters("SELECT * FROM t WHERE id = :id", {"id": 42})
        assert "id = 42" in result
        assert "'" not in result.split("id = ")[1].split()[0]

    def test_none_parameters_returns_query_unchanged(self):
        """When parameters is None, the query string is returned unmodified."""
        query = "{ orders { id } }"
        result = _apply_parameters(query, None)
        assert result == query

    def test_empty_parameters_returns_query_unchanged(self):
        """An empty parameters dict leaves the query unmodified."""
        query = "SELECT 1"
        result = _apply_parameters(query, {})
        assert result == query

    def test_multiple_parameters_all_substituted(self):
        """All :name placeholders are replaced when multiple params provided."""
        query = "SELECT * FROM t WHERE region = :region AND amount > :amount"
        result = _apply_parameters(query, {"region": "us-east", "amount": 100})
        assert "'us-east'" in result
        assert "100" in result
        assert ":region" not in result
        assert ":amount" not in result

    def test_float_parameter_substituted(self):
        """Float values are substituted as their string representation."""
        result = _apply_parameters("SELECT * FROM t WHERE price > :price", {"price": 9.99})
        assert "9.99" in result


# ---------------------------------------------------------------------------
# Cursor initial state (PEP 249 compliance)
# ---------------------------------------------------------------------------


class TestCursorInitialState:
    """Verify PEP 249 initial cursor state before any execute() call."""

    def test_rowcount_is_negative_one_before_execute(self):
        """PEP 249 §.rowcount: -1 when no operation has been performed."""
        conn = Connection(base_url=BASE, token=None, role="admin")
        cur = conn.cursor()
        assert cur.rowcount == -1

    def test_description_is_none_before_execute(self):
        """PEP 249 §.description: None when no operation has been performed."""
        conn = Connection(base_url=BASE, token=None, role="admin")
        cur = conn.cursor()
        assert cur.description is None

    def test_arraysize_default_is_one(self):
        """PEP 249 §.arraysize: default value is 1."""
        conn = Connection(base_url=BASE, token=None, role="admin")
        cur = conn.cursor()
        assert cur.arraysize == 1


# ---------------------------------------------------------------------------
# The DB-API connection reflection reads the catalog as
# ---------------------------------------------------------------------------


class TestDbapiConnection:
    def test_a_connection_that_is_not_provisas_is_refused(self):
        """Reflection reads the catalog as the Provisa connection's user; another driver's
        connection has none to read it as."""
        conn = MagicMock()
        conn.connection.dbapi_connection = object()
        with pytest.raises(TypeError, match="not a Provisa DB-API connection"):
            ProvisaDialect().get_schema_names(conn)
