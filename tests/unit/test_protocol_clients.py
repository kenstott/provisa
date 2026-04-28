# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7f91b2-d4a8-4c1e-b83f-9c5d02e6a847
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Provisa protocol client libraries: DB-API, SQLAlchemy dialect, ADBC."""

from __future__ import annotations

import json
import sys
import types
from typing import Any
from unittest.mock import MagicMock, patch, call

import pyarrow as pa
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_httpx_response(
    status_code: int = 200,
    body: Any = None,
    raise_for_status_exc: Exception | None = None,
) -> MagicMock:
    """Build a mock httpx Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body if body is not None else {}
    if raise_for_status_exc is not None:
        resp.raise_for_status.side_effect = raise_for_status_exc
    else:
        resp.raise_for_status.return_value = None
    return resp


def _http_status_error() -> Exception:
    """Return a plausible httpx.HTTPStatusError."""
    import httpx
    request = httpx.Request("POST", "http://localhost:8001/data/graphql")
    response = httpx.Response(status_code=500, request=request)
    return httpx.HTTPStatusError("Server error", request=request, response=response)


# ===========================================================================
# DB-API 2.0  (dbapi.py)
# ===========================================================================

class TestDbApiModuleConstants:
    def test_apilevel(self):
        from provisa_client import dbapi
        assert dbapi.apilevel == "2.0"

    def test_threadsafety(self):
        from provisa_client import dbapi
        assert dbapi.threadsafety == 1

    def test_paramstyle(self):
        from provisa_client import dbapi
        assert dbapi.paramstyle == "named"


class TestDbApiExceptionHierarchy:
    def test_error_is_exception(self):
        from provisa_client.dbapi import Error
        assert issubclass(Error, Exception)

    def test_database_error_inherits_error(self):
        from provisa_client.dbapi import DatabaseError, Error
        assert issubclass(DatabaseError, Error)

    def test_operational_error_inherits_database_error(self):
        from provisa_client.dbapi import OperationalError, DatabaseError
        assert issubclass(OperationalError, DatabaseError)

    def test_programming_error_inherits_database_error(self):
        from provisa_client.dbapi import ProgrammingError, DatabaseError
        assert issubclass(ProgrammingError, DatabaseError)

    def test_operational_error_can_be_raised_and_caught_as_error(self):
        from provisa_client.dbapi import OperationalError, Error
        with pytest.raises(Error):
            raise OperationalError("test")

    def test_programming_error_can_be_raised_and_caught_as_error(self):
        from provisa_client.dbapi import ProgrammingError, Error
        with pytest.raises(Error):
            raise ProgrammingError("test")


class TestIsGraphql:
    def test_curly_brace_query(self):
        from provisa_client.dbapi import _is_graphql
        assert _is_graphql("{ orders { id } }")

    def test_whitespace_before_brace(self):
        from provisa_client.dbapi import _is_graphql
        assert _is_graphql("   { users { name } }")

    def test_query_keyword(self):
        from provisa_client.dbapi import _is_graphql
        assert _is_graphql("query GetUsers { users { id } }")

    def test_mutation_keyword(self):
        from provisa_client.dbapi import _is_graphql
        assert _is_graphql("mutation CreateUser { createUser(name: \"x\") { id } }")

    def test_query_keyword_case_insensitive(self):
        from provisa_client.dbapi import _is_graphql
        assert _is_graphql("QUERY GetAll { items { id } }")

    def test_plain_sql_select(self):
        from provisa_client.dbapi import _is_graphql
        assert not _is_graphql("SELECT id FROM orders")

    def test_plain_sql_with(self):
        from provisa_client.dbapi import _is_graphql
        assert not _is_graphql("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_empty_string(self):
        from provisa_client.dbapi import _is_graphql
        assert not _is_graphql("")


class TestApplyParameters:
    def test_no_parameters_returns_query_unchanged(self):
        from provisa_client.dbapi import _apply_parameters
        sql = "SELECT * FROM orders"
        assert _apply_parameters(sql, None) == sql

    def test_empty_parameters_returns_query_unchanged(self):
        from provisa_client.dbapi import _apply_parameters
        sql = "SELECT * FROM orders"
        assert _apply_parameters(sql, {}) == sql

    def test_string_value_is_quoted(self):
        from provisa_client.dbapi import _apply_parameters
        result = _apply_parameters("SELECT * FROM t WHERE name = :name", {"name": "Alice"})
        assert result == "SELECT * FROM t WHERE name = 'Alice'"

    def test_numeric_value_is_unquoted(self):
        from provisa_client.dbapi import _apply_parameters
        result = _apply_parameters("SELECT * FROM t WHERE id = :id", {"id": 42})
        assert result == "SELECT * FROM t WHERE id = 42"

    def test_float_value_is_unquoted(self):
        from provisa_client.dbapi import _apply_parameters
        result = _apply_parameters("SELECT * FROM t WHERE score > :score", {"score": 3.14})
        assert result == "SELECT * FROM t WHERE score > 3.14"

    def test_multiple_parameters(self):
        from provisa_client.dbapi import _apply_parameters
        result = _apply_parameters(
            "SELECT * FROM t WHERE name = :name AND age = :age",
            {"name": "Bob", "age": 30},
        )
        assert "'Bob'" in result
        assert "30" in result
        assert ":name" not in result
        assert ":age" not in result

    def test_boolean_value_is_unquoted(self):
        from provisa_client.dbapi import _apply_parameters
        result = _apply_parameters("SELECT * FROM t WHERE active = :flag", {"flag": True})
        assert "True" in result


class TestConnect:
    def test_successful_auth_returns_connection_with_token(self):
        from provisa_client import dbapi
        login_resp = _make_httpx_response(200, {"token": "tok-abc"})
        with patch("provisa_client.dbapi.httpx.post", return_value=login_resp):
            conn = dbapi.connect(
                "http://localhost:8001",
                username="alice",
                password="secret",
            )
        assert conn._token == "tok-abc"
        assert conn._role == "admin"

    def test_failed_auth_falls_back_to_username_as_role(self):
        from provisa_client import dbapi
        login_resp = _make_httpx_response(401, {})
        with patch("provisa_client.dbapi.httpx.post", return_value=login_resp):
            conn = dbapi.connect(
                "http://localhost:8001",
                username="analyst_user",
                password="wrong",
            )
        assert conn._token is None
        assert conn._role == "analyst_user"

    def test_http_error_during_auth_falls_back_gracefully(self):
        import httpx
        from provisa_client import dbapi
        with patch("provisa_client.dbapi.httpx.post", side_effect=httpx.HTTPError("network fail")):
            conn = dbapi.connect(
                "http://localhost:8001",
                username="fallback_user",
                password="pw",
            )
        assert conn._token is None
        assert conn._role == "fallback_user"

    def test_trailing_slash_stripped_from_base_url(self):
        from provisa_client import dbapi
        login_resp = _make_httpx_response(200, {"token": "t"})
        with patch("provisa_client.dbapi.httpx.post", return_value=login_resp):
            conn = dbapi.connect(
                "http://localhost:8001/",
                username="u",
                password="p",
            )
        assert conn._base_url == "http://localhost:8001"

    def test_mode_propagated(self):
        from provisa_client import dbapi
        login_resp = _make_httpx_response(200, {"token": "t"})
        with patch("provisa_client.dbapi.httpx.post", return_value=login_resp):
            conn = dbapi.connect(
                "http://localhost:8001",
                username="u",
                password="p",
                mode="approved",
            )
        assert conn._mode == "approved"


class TestConnection:
    def _make_conn(self, token: str | None = "tok-xyz", role: str = "admin") -> Any:
        from provisa_client.dbapi import Connection
        return Connection(
            base_url="http://localhost:8001",
            token=token,
            role=role,
            mode="approved",
        )

    def test_headers_include_content_type_and_role(self):
        conn = self._make_conn()
        h = conn._headers()
        assert h["Content-Type"] == "application/json"
        assert h["X-Role"] == "admin"

    def test_headers_include_bearer_token_when_present(self):
        conn = self._make_conn(token="tok-xyz")
        h = conn._headers()
        assert h["Authorization"] == "Bearer tok-xyz"

    def test_headers_omit_authorization_when_no_token(self):
        conn = self._make_conn(token=None)
        h = conn._headers()
        assert "Authorization" not in h

    def test_cursor_returns_cursor_instance(self):
        from provisa_client.dbapi import Cursor
        conn = self._make_conn()
        cur = conn.cursor()
        assert isinstance(cur, Cursor)

    def test_close_marks_connection_closed(self):
        conn = self._make_conn()
        conn.close()
        assert conn._closed is True

    def test_cursor_on_closed_connection_raises_operational_error(self):
        from provisa_client.dbapi import OperationalError
        conn = self._make_conn()
        conn.close()
        with pytest.raises(OperationalError, match="closed"):
            conn.cursor()

    def test_commit_is_noop(self):
        conn = self._make_conn()
        conn.commit()  # should not raise

    def test_rollback_is_noop(self):
        conn = self._make_conn()
        conn.rollback()  # should not raise

    def test_context_manager_returns_connection(self):
        conn = self._make_conn()
        with conn as c:
            assert c is conn

    def test_context_manager_closes_on_exit(self):
        conn = self._make_conn()
        with conn:
            pass
        assert conn._closed is True

    def test_context_manager_closes_on_exception(self):
        conn = self._make_conn()
        try:
            with conn:
                raise ValueError("oops")
        except ValueError:
            pass
        assert conn._closed is True


class TestCursorExecuteRouting:
    def _make_cursor(self):
        from provisa_client.dbapi import Connection, Cursor
        conn = Connection(
            base_url="http://localhost:8001",
            token="tok",
            role="admin",
            mode="approved",
        )
        return Cursor(connection=conn)

    def test_graphql_curly_brace_routes_to_graphql(self):
        cur = self._make_cursor()
        with patch.object(cur, "_execute_graphql") as mock_gql:
            cur.execute("{ users { id } }")
        mock_gql.assert_called_once()

    def test_graphql_query_keyword_routes_to_graphql(self):
        cur = self._make_cursor()
        with patch.object(cur, "_execute_graphql") as mock_gql:
            cur.execute("query GetAll { items { id } }")
        mock_gql.assert_called_once()

    def test_graphql_mutation_keyword_routes_to_graphql(self):
        cur = self._make_cursor()
        with patch.object(cur, "_execute_graphql") as mock_gql:
            cur.execute("mutation DoIt { createItem { id } }")
        mock_gql.assert_called_once()

    def test_sql_routes_to_sql(self):
        cur = self._make_cursor()
        with patch.object(cur, "_execute_sql") as mock_sql:
            cur.execute("SELECT id FROM orders")
        mock_sql.assert_called_once()

    def test_parameters_applied_before_routing(self):
        cur = self._make_cursor()
        with patch.object(cur, "_execute_sql") as mock_sql:
            cur.execute("SELECT * FROM t WHERE id = :id", {"id": 99})
        applied_query = mock_sql.call_args[0][0]
        assert "99" in applied_query
        assert ":id" not in applied_query

    def test_closed_cursor_raises_operational_error(self):
        from provisa_client.dbapi import OperationalError
        cur = self._make_cursor()
        cur.close()
        with pytest.raises(OperationalError, match="closed"):
            cur.execute("SELECT 1")


class TestCursorExecuteGraphql:
    def _make_cursor(self):
        from provisa_client.dbapi import Connection, Cursor
        conn = Connection(
            base_url="http://localhost:8001",
            token="tok",
            role="admin",
            mode="approved",
        )
        return Cursor(connection=conn)

    def test_successful_graphql_response_sets_rows(self):
        cur = self._make_cursor()
        body = {"data": {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}}
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            cur._execute_graphql("{ users { id name } }")
        assert cur.rowcount == 2
        assert cur.description is not None
        assert len(cur.description) == 2

    def test_graphql_error_in_body_raises_programming_error(self):
        from provisa_client.dbapi import ProgrammingError
        cur = self._make_cursor()
        body = {"errors": [{"message": "Unknown field"}]}
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            with pytest.raises(ProgrammingError):
                cur._execute_graphql("{ bad_field }")

    def test_http_error_raises_operational_error(self):
        from provisa_client.dbapi import OperationalError
        cur = self._make_cursor()
        resp = _make_httpx_response(500, raise_for_status_exc=_http_status_error())
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            with pytest.raises(OperationalError):
                cur._execute_graphql("{ users { id } }")

    def test_empty_data_object_yields_empty_rows(self):
        cur = self._make_cursor()
        body = {"data": {}}
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            cur._execute_graphql("{ emptyQuery }")
        assert cur._rows == []
        assert cur.rowcount == 0

    def test_single_object_root_field_wrapped_in_list(self):
        cur = self._make_cursor()
        body = {"data": {"user": {"id": 1, "name": "Alice"}}}
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            cur._execute_graphql("{ user { id name } }")
        assert cur.rowcount == 1


class TestCursorExecuteSql:
    def _make_cursor(self):
        from provisa_client.dbapi import Connection, Cursor
        conn = Connection(
            base_url="http://localhost:8001",
            token="tok",
            role="admin",
            mode="approved",
        )
        return Cursor(connection=conn)

    def test_list_response_sets_rows(self):
        cur = self._make_cursor()
        body = [{"id": 1}, {"id": 2}]
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            cur._execute_sql("SELECT id FROM orders")
        assert cur.rowcount == 2

    def test_dict_with_data_key_as_list(self):
        cur = self._make_cursor()
        body = {"data": [{"id": 10}, {"id": 20}]}
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            cur._execute_sql("SELECT id FROM orders")
        assert cur.rowcount == 2

    def test_nested_dict_data_key(self):
        cur = self._make_cursor()
        body = {"data": {"_sql": [{"id": 1}, {"id": 2}, {"id": 3}]}}
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            cur._execute_sql("SELECT id FROM orders")
        assert cur.rowcount == 3

    def test_http_error_raises_operational_error(self):
        from provisa_client.dbapi import OperationalError
        cur = self._make_cursor()
        resp = _make_httpx_response(500, raise_for_status_exc=_http_status_error())
        with patch("provisa_client.dbapi.httpx.post", return_value=resp):
            with pytest.raises(OperationalError):
                cur._execute_sql("SELECT 1")


class TestCursorSetRows:
    def _make_cursor(self):
        from provisa_client.dbapi import Connection, Cursor
        conn = Connection(
            base_url="http://localhost:8001",
            token=None,
            role="admin",
            mode="approved",
        )
        return Cursor(connection=conn)

    def test_dict_rows_sets_description_and_tuples(self):
        cur = self._make_cursor()
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        cur._set_rows(rows)
        assert cur.description is not None
        assert len(cur.description) == 2
        col_names = [d[0] for d in cur.description]
        assert "id" in col_names
        assert "name" in col_names
        assert cur._rows[0] == (1, "Alice")
        assert cur._rows[1] == (2, "Bob")

    def test_empty_rows_sets_empty_description(self):
        cur = self._make_cursor()
        cur._set_rows([])
        assert cur._rows == []
        assert cur.description == []
        assert cur.rowcount == 0

    def test_rowcount_matches_number_of_rows(self):
        cur = self._make_cursor()
        cur._set_rows([{"x": i} for i in range(5)])
        assert cur.rowcount == 5

    def test_position_reset_to_zero(self):
        cur = self._make_cursor()
        cur._pos = 99
        cur._set_rows([{"x": 1}])
        assert cur._pos == 0

    def test_description_tuple_has_seven_elements(self):
        cur = self._make_cursor()
        cur._set_rows([{"col": 1}])
        assert len(cur.description[0]) == 7


class TestCursorFetchMethods:
    def _make_cursor_with_rows(self, rows=None):
        from provisa_client.dbapi import Connection, Cursor
        conn = Connection(
            base_url="http://localhost:8001",
            token=None,
            role="admin",
            mode="approved",
        )
        cur = Cursor(connection=conn)
        if rows is not None:
            cur._set_rows(rows)
        return cur

    def test_fetchone_returns_rows_one_by_one(self):
        cur = self._make_cursor_with_rows([{"id": 1}, {"id": 2}, {"id": 3}])
        assert cur.fetchone() == (1,)
        assert cur.fetchone() == (2,)
        assert cur.fetchone() == (3,)

    def test_fetchone_returns_none_at_end(self):
        cur = self._make_cursor_with_rows([{"id": 1}])
        cur.fetchone()
        assert cur.fetchone() is None

    def test_fetchmany_returns_n_rows(self):
        cur = self._make_cursor_with_rows([{"id": i} for i in range(5)])
        batch = cur.fetchmany(3)
        assert len(batch) == 3
        assert batch[0] == (0,)

    def test_fetchmany_respects_arraysize_default(self):
        cur = self._make_cursor_with_rows([{"id": i} for i in range(5)])
        assert cur.arraysize == 1
        batch = cur.fetchmany()
        assert len(batch) == 1

    def test_fetchmany_advances_position(self):
        cur = self._make_cursor_with_rows([{"id": i} for i in range(4)])
        cur.fetchmany(2)
        remaining = cur.fetchall()
        assert len(remaining) == 2

    def test_fetchall_returns_all_remaining(self):
        cur = self._make_cursor_with_rows([{"id": i} for i in range(3)])
        all_rows = cur.fetchall()
        assert len(all_rows) == 3

    def test_fetchall_after_fetchone_returns_remaining(self):
        cur = self._make_cursor_with_rows([{"id": 1}, {"id": 2}, {"id": 3}])
        cur.fetchone()
        remaining = cur.fetchall()
        assert len(remaining) == 2
        assert remaining[0] == (2,)

    def test_fetchone_on_closed_cursor_raises(self):
        from provisa_client.dbapi import OperationalError
        cur = self._make_cursor_with_rows([{"id": 1}])
        cur.close()
        with pytest.raises(OperationalError):
            cur.fetchone()

    def test_fetchmany_on_closed_cursor_raises(self):
        from provisa_client.dbapi import OperationalError
        cur = self._make_cursor_with_rows([{"id": 1}])
        cur.close()
        with pytest.raises(OperationalError):
            cur.fetchmany()

    def test_fetchall_on_closed_cursor_raises(self):
        from provisa_client.dbapi import OperationalError
        cur = self._make_cursor_with_rows([{"id": 1}])
        cur.close()
        with pytest.raises(OperationalError):
            cur.fetchall()


class TestCursorExecuteMany:
    def _make_cursor(self):
        from provisa_client.dbapi import Connection, Cursor
        conn = Connection(
            base_url="http://localhost:8001",
            token=None,
            role="admin",
            mode="approved",
        )
        return Cursor(connection=conn)

    def test_executemany_calls_execute_for_each_param_set(self):
        cur = self._make_cursor()
        call_args = []

        def capture_execute(query, params=None):
            call_args.append((query, params))

        with patch.object(cur, "execute", side_effect=capture_execute):
            cur.executemany("SELECT :id", [{"id": 1}, {"id": 2}, {"id": 3}])

        assert len(call_args) == 3
        assert call_args[0] == ("SELECT :id", {"id": 1})
        assert call_args[2] == ("SELECT :id", {"id": 3})

    def test_executemany_on_closed_cursor_raises(self):
        from provisa_client.dbapi import OperationalError
        cur = self._make_cursor()
        cur.close()
        with pytest.raises(OperationalError):
            cur.executemany("SELECT 1", [{}])


class TestCursorContextManager:
    def test_cursor_context_manager_returns_self(self):
        from provisa_client.dbapi import Connection, Cursor
        conn = Connection(
            base_url="http://localhost:8001",
            token=None,
            role="admin",
            mode="approved",
        )
        cur = Cursor(connection=conn)
        with cur as c:
            assert c is cur

    def test_cursor_context_manager_closes_on_exit(self):
        from provisa_client.dbapi import Connection, Cursor
        conn = Connection(
            base_url="http://localhost:8001",
            token=None,
            role="admin",
            mode="approved",
        )
        cur = Cursor(connection=conn)
        with cur:
            pass
        assert cur._closed is True


# ===========================================================================
# SQLAlchemy Dialect (sqlalchemy_dialect.py)
# ===========================================================================

class TestProvisaDialectAttributes:
    def test_name_attribute(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        assert ProvisaDialect.name == "provisa"

    def test_driver_attribute(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        assert ProvisaDialect.driver == "provisa_client"

    def test_supports_alter_is_false(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        assert ProvisaDialect.supports_alter is False


class TestProvisaDialectCreateConnectArgs:
    def _make_url(
        self,
        drivername="provisa+http",
        host="myserver",
        port=8001,
        username="alice",
        password="secret",
        query=None,
    ):
        url = MagicMock()
        url.drivername = drivername
        url.host = host
        url.port = port
        url.username = username
        url.password = password
        url.query = query or {}
        return url

    def test_extracts_scheme_from_drivername(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(drivername="provisa+https", port=443)
        _, opts = dialect.create_connect_args(url)
        assert opts["url"].startswith("https://")

    def test_extracts_host(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(host="data.example.com")
        _, opts = dialect.create_connect_args(url)
        assert "data.example.com" in opts["url"]

    def test_extracts_port(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(port=9999)
        _, opts = dialect.create_connect_args(url)
        assert "9999" in opts["url"]

    def test_default_port_8001_when_not_specified(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(port=None)
        _, opts = dialect.create_connect_args(url)
        assert "8001" in opts["url"]

    def test_extracts_username(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(username="bob")
        _, opts = dialect.create_connect_args(url)
        assert opts["username"] == "bob"

    def test_extracts_password(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(password="hunter2")
        _, opts = dialect.create_connect_args(url)
        assert opts["password"] == "hunter2"

    def test_extracts_role_from_query(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(query={"role": "analyst"})
        _, opts = dialect.create_connect_args(url)
        assert opts["role"] == "analyst"

    def test_default_role_is_admin(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(query={})
        _, opts = dialect.create_connect_args(url)
        assert opts["role"] == "admin"

    def test_extracts_mode_from_query(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(query={"mode": "catalog"})
        _, opts = dialect.create_connect_args(url)
        assert opts["mode"] == "catalog"

    def test_default_mode_is_approved(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(query={})
        _, opts = dialect.create_connect_args(url)
        assert opts["mode"] == "approved"

    def test_returns_empty_positional_args(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url()
        positional, _ = dialect.create_connect_args(url)
        assert positional == []

    def test_drivername_without_plus_defaults_to_http(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        url = self._make_url(drivername="provisa")
        _, opts = dialect.create_connect_args(url)
        assert opts["url"].startswith("http://")


class TestProvisaDialectGetTableNames:
    def _make_connection(self, base_url="http://localhost:8001", role="admin"):
        mock_conn = MagicMock()
        inner = MagicMock()
        inner._base_url = base_url
        inner._role = role
        mock_conn.connection = inner
        return mock_conn

    def test_returns_field_names_from_introspection(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        body = {
            "data": {
                "__schema": {
                    "queryType": {
                        "fields": [
                            {"name": "orders"},
                            {"name": "users"},
                        ]
                    },
                    "types": [],
                }
            }
        }
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.sqlalchemy_dialect.httpx.post", return_value=resp):
            names = dialect.get_table_names(conn)
        assert names == ["orders", "users"]

    def test_http_error_returns_empty_list(self):
        import httpx
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        with patch(
            "provisa_client.sqlalchemy_dialect.httpx.post",
            side_effect=httpx.HTTPError("connection refused"),
        ):
            names = dialect.get_table_names(conn)
        assert names == []

    def test_filters_fields_without_name(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        body = {
            "data": {
                "__schema": {
                    "queryType": {
                        "fields": [
                            {"name": "orders"},
                            {},  # no name
                        ]
                    },
                    "types": [],
                }
            }
        }
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.sqlalchemy_dialect.httpx.post", return_value=resp):
            names = dialect.get_table_names(conn)
        assert names == ["orders"]


class TestProvisaDialectGetColumns:
    def _make_connection(self):
        mock_conn = MagicMock()
        inner = MagicMock()
        inner._base_url = "http://localhost:8001"
        inner._role = "admin"
        mock_conn.connection = inner
        return mock_conn

    def test_returns_columns_for_matching_table(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        body = {
            "data": {
                "__schema": {
                    "queryType": {
                        "fields": [
                            {"name": "orders", "type": {"name": None, "kind": "LIST", "ofType": {"name": "Orders", "kind": "OBJECT", "ofType": None}}},
                        ]
                    },
                    "types": [
                        {"name": "Orders", "kind": "OBJECT", "fields": [{"name": "id"}, {"name": "total"}]},
                    ],
                }
            }
        }
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.sqlalchemy_dialect.httpx.post", return_value=resp):
            cols = dialect.get_columns(conn, "orders")
        assert len(cols) == 2
        col_names = [c["name"] for c in cols]
        assert "id" in col_names
        assert "total" in col_names

    def test_returns_empty_list_when_table_not_found(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        body = {
            "data": {
                "__schema": {
                    "queryType": {
                        "fields": [
                            {"name": "orders", "type": {"name": None, "kind": "LIST", "ofType": {"name": "Orders", "kind": "OBJECT", "ofType": None}}},
                        ]
                    },
                    "types": [
                        {"name": "Orders", "kind": "OBJECT", "fields": [{"name": "id"}]},
                    ],
                }
            }
        }
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.sqlalchemy_dialect.httpx.post", return_value=resp):
            cols = dialect.get_columns(conn, "missing_table")
        assert cols == []

    def test_http_error_returns_empty_list(self):
        import httpx
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        with patch(
            "provisa_client.sqlalchemy_dialect.httpx.post",
            side_effect=httpx.HTTPError("timeout"),
        ):
            cols = dialect.get_columns(conn, "orders")
        assert cols == []

    def test_columns_are_nullable(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        body = {
            "data": {
                "__schema": {
                    "queryType": {
                        "fields": [
                            {"name": "t", "type": {"name": None, "kind": "LIST", "ofType": {"name": "T", "kind": "OBJECT", "ofType": None}}},
                        ]
                    },
                    "types": [
                        {"name": "T", "kind": "OBJECT", "fields": [{"name": "col"}]},
                    ],
                }
            }
        }
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.sqlalchemy_dialect.httpx.post", return_value=resp):
            cols = dialect.get_columns(conn, "t")
        assert cols[0]["nullable"] is True


class TestProvisaDialectHasTable:
    def _make_connection(self):
        mock_conn = MagicMock()
        inner = MagicMock()
        inner._base_url = "http://localhost:8001"
        inner._role = "admin"
        mock_conn.connection = inner
        return mock_conn

    def test_returns_true_when_table_present(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        body = {"data": {"__schema": {"queryType": {"fields": [{"name": "my-query"}]}}}}
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.sqlalchemy_dialect.httpx.post", return_value=resp):
            assert dialect.has_table(conn, "my-query") is True

    def test_returns_false_when_table_absent(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        conn = self._make_connection()
        body = {"data": {"__schema": {"queryType": {"fields": [{"name": "other-query"}]}}}}
        resp = _make_httpx_response(200, body)
        with patch("provisa_client.sqlalchemy_dialect.httpx.post", return_value=resp):
            assert dialect.has_table(conn, "missing") is False


class TestProvisaDialectMiscMethods:
    def _make_connection(self):
        return MagicMock()

    def test_get_schema_names_returns_default(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        assert dialect.get_schema_names(self._make_connection()) == ["default"]

    def test_get_foreign_keys_returns_empty(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        assert dialect.get_foreign_keys(self._make_connection(), "t") == []

    def test_get_indexes_returns_empty(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        assert dialect.get_indexes(self._make_connection(), "t") == []

    def test_get_pk_constraint_returns_empty_dict(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        result = dialect.get_pk_constraint(self._make_connection(), "t")
        assert result == {"constrained_columns": [], "name": None}

    def test_get_unique_constraints_returns_empty(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        assert dialect.get_unique_constraints(self._make_connection(), "t") == []

    def test_check_unicode_returns_true(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        assert dialect._check_unicode_returns(self._make_connection()) is True

    def test_check_unicode_description_returns_true(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        assert dialect._check_unicode_description(self._make_connection()) is True

    def test_do_execute_delegates_to_cursor(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        cursor = MagicMock()
        dialect.do_execute(cursor, "SELECT 1", None)
        cursor.execute.assert_called_once_with("SELECT 1", None)

    def test_do_execute_passes_parameters(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        cursor = MagicMock()
        dialect.do_execute(cursor, "SELECT :id", {"id": 1})
        cursor.execute.assert_called_once_with("SELECT :id", {"id": 1})

    def test_do_execute_converts_empty_dict_to_none(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        dialect = ProvisaDialect()
        cursor = MagicMock()
        # parameters={} is falsy, so or None makes it None
        dialect.do_execute(cursor, "SELECT 1", {})
        cursor.execute.assert_called_once_with("SELECT 1", None)


# ===========================================================================
# ADBC / Arrow Flight  (adbc.py)
# ===========================================================================

def _make_arrow_table(data: dict | None = None) -> pa.Table:
    if data is None:
        data = {"id": [1, 2], "name": ["Alice", "Bob"]}
    return pa.table(data)


def _make_mock_stream(table: pa.Table | None = None) -> MagicMock:
    if table is None:
        table = _make_arrow_table()
    stream = MagicMock()
    stream.read_all.return_value = table
    return stream


class TestAdbcConnect:
    def test_successful_auth_gives_role_admin(self):
        from provisa_client.adbc import adbc_connect
        login_resp = _make_httpx_response(200, {"token": "flight-tok"})
        mock_flight_client = MagicMock()
        with patch("provisa_client.adbc.httpx.post", return_value=login_resp), \
             patch("provisa_client.adbc.fl.connect", return_value=mock_flight_client):
            conn = adbc_connect("http://localhost:8001", user="alice", password="secret")
        assert conn._token == "flight-tok"
        assert conn._role == "admin"

    def test_failed_auth_gives_role_equal_to_username(self):
        from provisa_client.adbc import adbc_connect
        login_resp = _make_httpx_response(401, {})
        mock_flight_client = MagicMock()
        with patch("provisa_client.adbc.httpx.post", return_value=login_resp), \
             patch("provisa_client.adbc.fl.connect", return_value=mock_flight_client):
            conn = adbc_connect("http://localhost:8001", user="analyst_bob", password="bad")
        assert conn._token is None
        assert conn._role == "analyst_bob"

    def test_flight_client_connected_to_grpc_endpoint(self):
        from provisa_client.adbc import adbc_connect
        login_resp = _make_httpx_response(200, {"token": "t"})
        mock_flight_client = MagicMock()
        with patch("provisa_client.adbc.httpx.post", return_value=login_resp), \
             patch("provisa_client.adbc.fl.connect", return_value=mock_flight_client) as mock_connect:
            adbc_connect("http://myhost:8001", user="u", password="p")
        mock_connect.assert_called_once_with("grpc://myhost:8815")

    def test_http_error_during_auth_falls_back(self):
        import httpx
        from provisa_client.adbc import adbc_connect
        mock_flight_client = MagicMock()
        with patch("provisa_client.adbc.httpx.post", side_effect=httpx.HTTPError("fail")), \
             patch("provisa_client.adbc.fl.connect", return_value=mock_flight_client):
            conn = adbc_connect("http://localhost:8001", user="guest", password="pw")
        assert conn._token is None
        assert conn._role == "guest"


class TestAdbcConnection:
    def _make_conn(self, token: str | None = "tok") -> Any:
        from provisa_client.adbc import AdbcConnection
        mock_flight_client = MagicMock()
        return AdbcConnection(
            flight_client=mock_flight_client,
            role="admin",
            token=token,
            base_url="http://localhost:8001",
        )

    def test_cursor_returns_adbc_cursor(self):
        from provisa_client.adbc import AdbcCursor
        conn = self._make_conn()
        cur = conn.cursor()
        assert isinstance(cur, AdbcCursor)

    def test_close_marks_connection_closed(self):
        conn = self._make_conn()
        conn.close()
        assert conn._closed is True

    def test_close_calls_flight_client_close(self):
        conn = self._make_conn()
        conn.close()
        conn._flight_client.close.assert_called_once()

    def test_cursor_on_closed_connection_raises_runtime_error(self):
        conn = self._make_conn()
        conn.close()
        with pytest.raises(RuntimeError, match="closed"):
            conn.cursor()

    def test_context_manager_returns_connection(self):
        conn = self._make_conn()
        with conn as c:
            assert c is conn

    def test_context_manager_closes_on_exit(self):
        conn = self._make_conn()
        with conn:
            pass
        assert conn._closed is True

    def test_context_manager_closes_on_exception(self):
        conn = self._make_conn()
        try:
            with conn:
                raise ValueError("oops")
        except ValueError:
            pass
        assert conn._closed is True

    def test_close_ignores_flight_client_exception(self):
        conn = self._make_conn()
        conn._flight_client.close.side_effect = Exception("already closed")
        conn.close()  # should not raise
        assert conn._closed is True


class TestAdbcCursorBuildTicket:
    def _make_cursor(self, token: str | None = "tok-abc", role: str = "admin") -> Any:
        from provisa_client.adbc import AdbcConnection, AdbcCursor
        mock_fc = MagicMock()
        conn = AdbcConnection(
            flight_client=mock_fc,
            role=role,
            token=token,
            base_url="http://localhost:8001",
        )
        return AdbcCursor(connection=conn)

    def test_ticket_includes_query(self):
        cur = self._make_cursor()
        ticket = cur._build_ticket("{ orders { id } }")
        data = json.loads(ticket.ticket.decode())
        assert data["query"] == "{ orders { id } }"

    def test_ticket_includes_role(self):
        cur = self._make_cursor(role="analyst")
        ticket = cur._build_ticket("{ x }")
        data = json.loads(ticket.ticket.decode())
        assert data["role"] == "analyst"

    def test_ticket_includes_token_when_present(self):
        cur = self._make_cursor(token="tok-abc")
        ticket = cur._build_ticket("{ x }")
        data = json.loads(ticket.ticket.decode())
        assert data["token"] == "tok-abc"

    def test_ticket_omits_token_when_none(self):
        cur = self._make_cursor(token=None)
        ticket = cur._build_ticket("{ x }")
        data = json.loads(ticket.ticket.decode())
        assert "token" not in data


class TestAdbcCursorExecute:
    def _make_cursor(self, token: str | None = "tok") -> Any:
        from provisa_client.adbc import AdbcConnection, AdbcCursor
        mock_fc = MagicMock()
        conn = AdbcConnection(
            flight_client=mock_fc,
            role="admin",
            token=token,
            base_url="http://localhost:8001",
        )
        return AdbcCursor(connection=conn)

    def test_execute_calls_do_get_with_ticket(self):
        cur = self._make_cursor()
        mock_stream = _make_mock_stream()
        cur._conn._flight_client.do_get.return_value = mock_stream
        cur.execute("{ orders { id } }")
        cur._conn._flight_client.do_get.assert_called_once()
        ticket_arg = cur._conn._flight_client.do_get.call_args[0][0]
        data = json.loads(ticket_arg.ticket.decode())
        assert data["query"] == "{ orders { id } }"

    def test_execute_resets_table_and_rows(self):
        cur = self._make_cursor()
        mock_stream = _make_mock_stream()
        cur._conn._flight_client.do_get.return_value = mock_stream
        cur._table = object()
        cur._rows = [(1,)]
        cur._pos = 5
        cur.execute("{ x }")
        assert cur._table is None
        assert cur._rows is None
        assert cur._pos == 0

    def test_execute_on_closed_cursor_raises(self):
        cur = self._make_cursor()
        cur._closed = True
        with pytest.raises(RuntimeError, match="closed"):
            cur.execute("{ x }")

    def test_execute_on_closed_connection_raises(self):
        cur = self._make_cursor()
        cur._conn._closed = True
        with pytest.raises(RuntimeError, match="closed"):
            cur.execute("{ x }")


class TestAdbcCursorFetchArrowTable:
    def _make_cursor_with_stream(self, table: pa.Table | None = None) -> Any:
        from provisa_client.adbc import AdbcConnection, AdbcCursor
        mock_fc = MagicMock()
        conn = AdbcConnection(
            flight_client=mock_fc,
            role="admin",
            token="tok",
            base_url="http://localhost:8001",
        )
        cur = AdbcCursor(connection=conn)
        t = table if table is not None else _make_arrow_table()
        cur._stream = _make_mock_stream(t)
        return cur

    def test_fetch_arrow_table_reads_all_from_stream(self):
        expected = _make_arrow_table({"id": [1, 2, 3]})
        cur = self._make_cursor_with_stream(expected)
        result = cur.fetch_arrow_table()
        assert result.num_rows == 3

    def test_fetch_arrow_table_returns_pyarrow_table(self):
        cur = self._make_cursor_with_stream()
        result = cur.fetch_arrow_table()
        assert isinstance(result, pa.Table)

    def test_fetchone_before_execute_raises_runtime_error(self):
        from provisa_client.adbc import AdbcConnection, AdbcCursor
        mock_fc = MagicMock()
        conn = AdbcConnection(
            flight_client=mock_fc,
            role="admin",
            token=None,
            base_url="http://localhost:8001",
        )
        cur = AdbcCursor(connection=conn)
        with pytest.raises(RuntimeError, match="No query has been executed"):
            cur.fetchone()


class TestAdbcCursorFetchMethods:
    def _make_cursor_with_data(self, data: dict | None = None) -> Any:
        from provisa_client.adbc import AdbcConnection, AdbcCursor
        mock_fc = MagicMock()
        conn = AdbcConnection(
            flight_client=mock_fc,
            role="admin",
            token="tok",
            base_url="http://localhost:8001",
        )
        cur = AdbcCursor(connection=conn)
        tbl = _make_arrow_table(data)
        cur._stream = _make_mock_stream(tbl)
        return cur

    def test_fetchone_returns_tuples_one_by_one(self):
        cur = self._make_cursor_with_data({"id": [10, 20, 30]})
        assert cur.fetchone() == (10,)
        assert cur.fetchone() == (20,)
        assert cur.fetchone() == (30,)

    def test_fetchone_returns_none_at_end(self):
        cur = self._make_cursor_with_data({"id": [1]})
        cur.fetchone()
        assert cur.fetchone() is None

    def test_fetchall_returns_all_rows(self):
        cur = self._make_cursor_with_data({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "a")

    def test_fetchall_after_fetchone_returns_remaining(self):
        cur = self._make_cursor_with_data({"id": [1, 2, 3]})
        cur.fetchone()
        remaining = cur.fetchall()
        assert len(remaining) == 2
        assert remaining[0] == (2,)

    def test_fetchall_advances_position_to_end(self):
        cur = self._make_cursor_with_data({"id": [1, 2]})
        cur.fetchall()
        assert cur.fetchone() is None


class TestAdbcCursorDescription:
    def _make_cursor_with_stream(self) -> Any:
        from provisa_client.adbc import AdbcConnection, AdbcCursor
        mock_fc = MagicMock()
        conn = AdbcConnection(
            flight_client=mock_fc,
            role="admin",
            token="tok",
            base_url="http://localhost:8001",
        )
        cur = AdbcCursor(connection=conn)
        tbl = _make_arrow_table({"id": [1], "name": ["Alice"], "score": [9.5]})
        cur._stream = _make_mock_stream(tbl)
        return cur

    def test_description_returns_none_before_execute(self):
        from provisa_client.adbc import AdbcConnection, AdbcCursor
        mock_fc = MagicMock()
        conn = AdbcConnection(
            flight_client=mock_fc,
            role="admin",
            token=None,
            base_url="http://localhost:8001",
        )
        cur = AdbcCursor(connection=conn)
        assert cur.description is None

    def test_description_has_one_entry_per_column(self):
        cur = self._make_cursor_with_stream()
        desc = cur.description
        assert len(desc) == 3

    def test_description_first_element_is_column_name(self):
        cur = self._make_cursor_with_stream()
        desc = cur.description
        names = [d[0] for d in desc]
        assert names == ["id", "name", "score"]

    def test_description_tuple_has_seven_elements(self):
        cur = self._make_cursor_with_stream()
        for entry in cur.description:
            assert len(entry) == 7


class TestAdbcCursorClose:
    def _make_cursor(self) -> Any:
        from provisa_client.adbc import AdbcConnection, AdbcCursor
        mock_fc = MagicMock()
        conn = AdbcConnection(
            flight_client=mock_fc,
            role="admin",
            token="tok",
            base_url="http://localhost:8001",
        )
        cur = AdbcCursor(connection=conn)
        cur._stream = MagicMock()
        return cur

    def test_close_marks_cursor_closed(self):
        cur = self._make_cursor()
        cur.close()
        assert cur._closed is True

    def test_close_clears_stream(self):
        cur = self._make_cursor()
        cur.close()
        assert cur._stream is None

    def test_context_manager_returns_cursor(self):
        cur = self._make_cursor()
        with cur as c:
            assert c is cur

    def test_context_manager_closes_on_exit(self):
        cur = self._make_cursor()
        with cur:
            pass
        assert cur._closed is True

    def test_context_manager_closes_on_exception(self):
        cur = self._make_cursor()
        try:
            with cur:
                raise ValueError("test error")
        except ValueError:
            pass
        assert cur._closed is True
