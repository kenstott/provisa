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
  - get_table_names() success and HTTP error paths
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
  1. get_columns() — returns column list for matching table.
  2. get_columns() — returns empty list when table not found.
  3. get_columns() — returns empty list on HTTP error.
  4. get_schema_names() — always returns ["default"].
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
  16. _get_base_url_and_role() — falls back to defaults when attributes absent.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
import sqlalchemy.types as sqltypes
from sqlalchemy.engine import URL

from provisa_client.dbapi import (
    Connection,
    Cursor,
    _apply_parameters,
)
from provisa_client.sqlalchemy_dialect import ProvisaDialect

BASE = "http://localhost:8001"


# ---------------------------------------------------------------------------
# get_columns()
# ---------------------------------------------------------------------------


class TestGetColumns:
    @respx.mock
    def test_returns_columns_for_matching_table(self):
        """get_columns() returns a list of column dicts for the named table."""
        respx.post(f"{BASE}/admin/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "semanticModel": {
                            "tables": [
                                {
                                    "name": "orders",
                                    "columns": [
                                        {"name": "id", "dataType": "integer"},
                                        {"name": "amount", "dataType": "decimal"},
                                    ],
                                }
                            ]
                        }
                    }
                },
            )
        )
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        with patch.object(dialect, "_get_base_url_and_role", return_value=(BASE, "admin")):
            cols = dialect.get_columns(mock_conn, "orders")

        assert len(cols) == 2
        col_names = [c["name"] for c in cols]
        assert "id" in col_names
        assert "amount" in col_names

    @respx.mock
    def test_column_dicts_have_required_keys(self):
        """Each column dict returned by get_columns() must have name, type, nullable."""
        respx.post(f"{BASE}/admin/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "semanticModel": {
                            "tables": [
                                {
                                    "name": "orders",
                                    "columns": [
                                        {"name": "id", "dataType": "integer"},
                                    ],
                                }
                            ]
                        }
                    }
                },
            )
        )
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        with patch.object(dialect, "_get_base_url_and_role", return_value=(BASE, "admin")):
            cols = dialect.get_columns(mock_conn, "orders")

        assert len(cols) == 1
        col = cols[0]
        assert col["name"] == "id"
        assert isinstance(col["type"], sqltypes.String)
        assert col["nullable"] is True

    @respx.mock
    def test_returns_empty_list_when_table_not_found(self):
        """get_columns() returns [] when the table_name is not in the schema."""
        respx.post(f"{BASE}/admin/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "semanticModel": {
                            "tables": [
                                {"name": "orders", "columns": []}
                            ]
                        }
                    }
                },
            )
        )
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        with patch.object(dialect, "_get_base_url_and_role", return_value=(BASE, "admin")):
            cols = dialect.get_columns(mock_conn, "nonexistent_table")

        assert cols == []

    @respx.mock
    def test_returns_empty_list_on_http_error(self):
        """get_columns() returns [] gracefully when the server returns an error."""
        respx.post(f"{BASE}/admin/graphql").mock(
            return_value=httpx.Response(500)
        )
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        with patch.object(dialect, "_get_base_url_and_role", return_value=(BASE, "admin")):
            cols = dialect.get_columns(mock_conn, "orders")

        assert cols == []


# ---------------------------------------------------------------------------
# Schema introspection stubs
# ---------------------------------------------------------------------------


class TestDialectSchemaStubs:
    """Verify the stub methods return the expected constant values."""

    def test_get_schema_names_returns_default(self):
        dialect = ProvisaDialect()
        mock_conn = MagicMock()
        assert dialect.get_schema_names(mock_conn) == ["default"]

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
        url = URL.create("provisa+http", username="user", password="pass",
                         host="myhost", port=8001)
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["url"].startswith("http://")

    def test_https_scheme_extracted(self):
        dialect = ProvisaDialect()
        url = URL.create("provisa+https", username="user", password="pass",
                         host="secure.host", port=443)
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["url"].startswith("https://")

    def test_host_and_port_in_url(self):
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="u", password="p",
                         host="db.example.com", port=9090)
        _, kwargs = dialect.create_connect_args(url)
        assert "db.example.com:9090" in kwargs["url"]

    def test_username_and_password_forwarded(self):
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="alice", password="s3cret",
                         host="localhost", port=8001)
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["username"] == "alice"
        assert kwargs["password"] == "s3cret"

    def test_default_role_is_admin(self):
        """When no role query param is given, role defaults to 'admin'."""
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="u", password="p",
                         host="localhost", port=8001)
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["role"] == "admin"

    def test_default_mode_is_approved(self):
        """When no mode query param is given, mode defaults to 'approved'."""
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="u", password="p",
                         host="localhost", port=8001)
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["mode"] == "approved"

    def test_role_from_query_string(self):
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="u", password="p",
                         host="localhost", port=8001,
                         query={"role": "viewer"})
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["role"] == "viewer"

    def test_args_list_is_empty(self):
        """create_connect_args must return an empty positional args list."""
        dialect = ProvisaDialect()
        url = URL.create("provisa+http", username="u", password="p",
                         host="localhost", port=8001)
        args, _ = dialect.create_connect_args(url)
        assert args == []


# ---------------------------------------------------------------------------
# _apply_parameters() — parameter substitution
# ---------------------------------------------------------------------------


class TestApplyParameters:
    """Named-parameter substitution used by the DB-API cursor execute path."""

    def test_string_value_quoted(self):
        """String parameters must be wrapped in single quotes."""
        result = _apply_parameters("SELECT * FROM t WHERE region = :region",
                                   {"region": "us-east"})
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
        result = _apply_parameters("SELECT * FROM t WHERE price > :price",
                                   {"price": 9.99})
        assert "9.99" in result


# ---------------------------------------------------------------------------
# Cursor initial state (PEP 249 compliance)
# ---------------------------------------------------------------------------


class TestCursorInitialState:
    """Verify PEP 249 initial cursor state before any execute() call."""

    def test_rowcount_is_negative_one_before_execute(self):
        """PEP 249 §.rowcount: -1 when no operation has been performed."""
        conn = Connection(base_url=BASE, token=None, role="admin", mode="approved")
        cur = conn.cursor()
        assert cur.rowcount == -1

    def test_description_is_none_before_execute(self):
        """PEP 249 §.description: None when no operation has been performed."""
        conn = Connection(base_url=BASE, token=None, role="admin", mode="approved")
        cur = conn.cursor()
        assert cur.description is None

    def test_arraysize_default_is_one(self):
        """PEP 249 §.arraysize: default value is 1."""
        conn = Connection(base_url=BASE, token=None, role="admin", mode="approved")
        cur = conn.cursor()
        assert cur.arraysize == 1


# ---------------------------------------------------------------------------
# _get_base_url_and_role() fallback
# ---------------------------------------------------------------------------


class TestGetBaseUrlAndRole:
    def test_falls_back_to_defaults_when_no_attributes(self):
        """Returns ('http://localhost:8001', 'admin') when raw connection lacks attrs."""
        dialect = ProvisaDialect()
        mock_conn = MagicMock(spec=[])  # no attributes at all
        base_url, role = dialect._get_base_url_and_role(mock_conn)
        assert base_url == "http://localhost:8001"
        assert role == "admin"

    def test_reads_from_connection_direct_attributes(self):
        """Reads _base_url and _role from connection.connection directly."""
        dialect = ProvisaDialect()

        mock_raw = MagicMock()
        mock_raw._base_url = "http://myserver:9001"
        mock_raw._role = "analyst"

        mock_conn = MagicMock()
        mock_conn.connection = mock_raw

        base_url, role = dialect._get_base_url_and_role(mock_conn)
        assert base_url == "http://myserver:9001"
        assert role == "analyst"
