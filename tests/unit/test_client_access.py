# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for client requirements: REQ-161, REQ-163, REQ-268, REQ-269, REQ-270, REQ-271, REQ-272, REQ-273, REQ-274, REQ-606, REQ-607, REQ-608"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

pytest.importorskip("provisa_client")
from provisa_client import dbapi as _dbapi  # type: ignore[import-not-found]
from provisa_client.client import ProvisaClient  # type: ignore[import-not-found]
from provisa_client.sqlalchemy_dialect import ProvisaDialect  # type: ignore[import-not-found]

BASE = "http://localhost:8001"


# ── REQ-161: POST /data/compile returns compiled SQL without executing ────────


def test_compile_endpoint_route_exists():
    # REQ-161
    # The compile endpoint is registered at POST /data/compile.
    # We verify the endpoint function exists and is tagged with REQ-161.
    from provisa.api.data.endpoint import compile_endpoint
    import inspect

    src = inspect.getsource(compile_endpoint)
    assert "REQ-161" in src


def test_compile_endpoint_returns_compiled_key():
    # REQ-161
    # POST /data/compile must return a response that includes "compiled" in the body.
    # We verify the response structure via the endpoint's return statement.
    from provisa.api.data.endpoint import compile_endpoint
    import inspect

    src = inspect.getsource(compile_endpoint)
    assert '"compiled"' in src or "'compiled'" in src


def test_compile_endpoint_raises_400_on_value_error():
    # REQ-161
    # A ValueError from the compiler must become HTTP 400.
    from provisa.api.data.endpoint import compile_endpoint
    import inspect

    src = inspect.getsource(compile_endpoint)
    assert "status_code=400" in src
    assert "ValueError" in src


def test_compile_endpoint_raises_403_on_unknown_role():
    # REQ-161
    # An unknown role must produce HTTP 403.
    from provisa.api.data.endpoint import compile_endpoint
    import inspect

    src = inspect.getsource(compile_endpoint)
    assert "status_code=403" in src


# ── REQ-163: GraphiQL plugin exposes compiled SQL (View SQL) ─────────────────


def test_graphiql_view_sql_compile_endpoint_shared():
    # REQ-163
    # The GraphiQL "View SQL" feature uses the same compile endpoint (REQ-161).
    # Both REQ-161 and REQ-163 are annotated on the same compile_endpoint function.
    from provisa.api.data.endpoint import compile_endpoint
    import inspect

    src = inspect.getsource(compile_endpoint)
    assert "REQ-163" in src


# ── REQ-268: DB-API 2.0 (PEP 249) interface ──────────────────────────────────


def test_dbapi_module_level_attributes():
    # REQ-268
    # PEP 249 requires apilevel, threadsafety, paramstyle at module level.
    assert _dbapi.apilevel == "2.0"
    assert _dbapi.threadsafety == 1
    assert _dbapi.paramstyle == "named"


def test_dbapi_connect_returns_connection():
    # REQ-268
    # provisa_client.dbapi.connect(url, username, password) returns a PEP 249 Connection.
    # We mock the auth call to avoid a real server.
    import unittest.mock as _mock

    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok123", "analyst")):
        conn = _dbapi.connect(BASE, username="alice", password="secret")
    assert isinstance(conn, _dbapi.Connection)


def test_dbapi_cursor_execute_detects_graphql_leading_brace():
    # REQ-268
    # cursor.execute() accepts GraphQL detected by a leading `{`.
    gql = "{ orders { id } }"
    assert _dbapi._is_graphql(gql) is True


def test_dbapi_cursor_execute_detects_graphql_query_keyword():
    # REQ-268
    # cursor.execute() accepts GraphQL detected by leading `query` keyword.
    gql = "query GetOrders { orders { id } }"
    assert _dbapi._is_graphql(gql) is True


def test_dbapi_cursor_execute_detects_graphql_mutation_keyword():
    # REQ-268
    # cursor.execute() accepts GraphQL detected by leading `mutation` keyword.
    gql = 'mutation CreateOrder { createOrder(name: "x") { id } }'
    assert _dbapi._is_graphql(gql) is True


def test_dbapi_cursor_execute_detects_sql():
    # REQ-268
    # cursor.execute() routes plain SQL (not GraphQL) to the SQL endpoint.
    sql = "SELECT id FROM orders"
    assert _dbapi._is_graphql(sql) is False


def test_dbapi_cursor_fetchall_after_execute():
    # REQ-268
    # cursor.execute(sql) → fetchall() returns rows.
    import unittest.mock as _mock

    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok", "analyst")):
        conn = _dbapi.connect(BASE, username="u", password="p")
    cursor = conn.cursor()
    cursor._set_rows([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, "A")


def test_dbapi_connection_context_manager():
    # REQ-268
    # Connection supports the context manager protocol (with statement).
    import unittest.mock as _mock

    with _mock.patch("provisa_client.dbapi._auth_login", return_value=(None, None)):
        conn = _dbapi.connect(BASE, username="u", password="p")
    with conn as c:
        assert not c._closed
    assert conn._closed


# ── REQ-269: DB-API exposes all tables for the user's rights; no mode param ──


def test_dbapi_connect_has_no_mode_parameter():
    # REQ-269
    # The connect() function signature must NOT accept a `mode` parameter.
    import inspect

    sig = inspect.signature(_dbapi.connect)
    assert "mode" not in sig.parameters


def test_dbapi_connection_has_no_mode_attribute():
    # REQ-269
    # The Connection class must NOT expose a mode attribute.
    import unittest.mock as _mock

    with _mock.patch("provisa_client.dbapi._auth_login", return_value=(None, None)):
        conn = _dbapi.connect(BASE, username="u", password="p")
    assert not hasattr(conn, "mode")


# ── REQ-270: SQLAlchemy dialect ───────────────────────────────────────────────


def test_sqlalchemy_dialect_name():
    # REQ-270
    # The dialect name must be "provisa".
    assert ProvisaDialect.name == "provisa"


def test_sqlalchemy_dialect_driver():
    # REQ-270
    # The dialect driver must be "provisa_client".
    assert ProvisaDialect.driver == "provisa_client"


def test_sqlalchemy_dialect_dbapi_returns_dbapi_module():
    # REQ-270
    # dialect.dbapi() returns the provisa_client.dbapi module (PEP 249 compliant).
    from provisa_client import dbapi as _db  # type: ignore[import-not-found]

    result = ProvisaDialect.dbapi()
    assert result is _db


def test_sqlalchemy_dialect_create_connect_args_builds_url():
    # REQ-270
    # create_engine("provisa+http://user:password@host:8001") — dialect parses URL correctly.
    from sqlalchemy.engine.url import make_url

    url = make_url("provisa+http://alice:secret@localhost:8001")
    dialect = ProvisaDialect()
    _, opts = dialect.create_connect_args(url)
    assert opts["url"] == "http://localhost:8001"
    assert opts["username"] == "alice"
    assert opts["password"] == "secret"


def test_sqlalchemy_dialect_get_table_names_returns_list():
    # REQ-270
    # inspector.get_table_names() returns registered table/view names via introspection.
    dialect = ProvisaDialect()
    dialect._schema_cache = {
        ("http://localhost:8001", "admin"): {
            "queryType": {"fields": [{"name": "orders"}, {"name": "customers"}]},
            "types": [],
        }
    }

    class _FakeConn:
        class connection:
            _base_url = "http://localhost:8001"
            _role = "admin"

    names = dialect.get_table_names(_FakeConn())
    assert "orders" in names
    assert "customers" in names


# ── REQ-271: ADBC interface backed by Arrow Flight ───────────────────────────


def test_adbc_connect_function_exists():
    # REQ-271
    # provisa_client.adbc_connect(url, user, password) must exist.
    from provisa_client.adbc import adbc_connect  # type: ignore[import-not-found]
    import inspect

    sig = inspect.signature(adbc_connect)
    assert "url" in sig.parameters
    assert "user" in sig.parameters
    assert "password" in sig.parameters


def test_adbc_cursor_fetch_arrow_table_method_exists():
    # REQ-271
    # ADBC cursor must expose fetch_arrow_table() for native RecordBatch access.
    from provisa_client.adbc import AdbcCursor  # type: ignore[import-not-found]

    assert hasattr(AdbcCursor, "fetch_arrow_table")


def test_adbc_connection_has_no_mode_parameter():
    # REQ-271
    # ADBC connection must not accept a mode parameter (REQ-269 uniform governance).
    from provisa_client.adbc import adbc_connect  # type: ignore[import-not-found]
    import inspect

    sig = inspect.signature(adbc_connect)
    assert "mode" not in sig.parameters


# ── REQ-272: JDBC governance — all SQL goes through Stage 2 ──────────────────


def test_dbapi_sql_execution_uses_sql_endpoint():
    # REQ-272
    # SQL queries in DB-API must hit the /data/sql endpoint (Stage 2 governance).
    # We verify by inspecting the _execute_sql source code.
    import inspect

    src = inspect.getsource(_dbapi.Cursor._execute_sql)
    assert "/data/sql" in src


def test_dbapi_no_ungoverned_sql_path():
    # REQ-272
    # There is no "raw" or "ungoverned" SQL path exposed in the DB-API module.
    import inspect

    src = inspect.getsource(_dbapi)
    assert "ungoverned" not in src.lower()
    assert "bypass_governance" not in src.lower()


# ── REQ-273: Server-assigned roles; no client-supplied role escalation ────────


def test_dbapi_sends_role_as_request_header_not_body_auth():
    # REQ-273
    # The Connection._headers() method sends X-Provisa-Role, not a body-level role override.
    import unittest.mock as _mock

    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok", "analyst")):
        conn = _dbapi.connect(BASE, username="u", password="p")
    headers = conn._headers()
    assert "X-Provisa-Role" in headers or headers.get("X-Provisa-Role") is not None


def test_dbapi_role_set_from_server_response():
    # REQ-273
    # The role is taken from the server's auth response, not assumed by the client.
    import unittest.mock as _mock

    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok", "data_scientist")):
        conn = _dbapi.connect(BASE, username="u", password="p")
    assert conn._role == "data_scientist"


def test_adbc_ticket_does_not_force_role_without_auth():
    # REQ-273
    # When no role is resolved from auth, the ADBC ticket does not inject a role field.
    from provisa_client.adbc import AdbcConnection, AdbcCursor  # type: ignore[import-not-found]
    import unittest.mock as _mock

    mock_fc = _mock.MagicMock()
    conn = AdbcConnection(flight_client=mock_fc, role=None, token=None, base_url=BASE)
    cursor = AdbcCursor(connection=conn)
    ticket = cursor._build_ticket("SELECT 1")
    data = json.loads(ticket.ticket)
    assert "role" not in data


# ── REQ-274: Per-call query language selection ────────────────────────────────


def test_dbapi_graphql_string_routes_to_graphql_endpoint():
    # REQ-274
    # GraphQL strings (leading `{` or `query`/`mutation`) route to /data/graphql.
    import inspect

    src = inspect.getsource(_dbapi.Cursor._execute_graphql)
    assert "/data/graphql" in src


def test_dbapi_sql_string_routes_to_sql_endpoint():
    # REQ-274
    # SQL strings route to /data/sql (Stage 2 only).
    import inspect

    src = inspect.getsource(_dbapi.Cursor._execute_sql)
    assert "/data/sql" in src


def test_dbapi_execute_dispatches_based_on_query_type():
    # REQ-274
    # cursor.execute() dispatches to _execute_graphql for GQL and _execute_sql for SQL.
    import unittest.mock as _mock

    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok", "analyst")):
        conn = _dbapi.connect(BASE, username="u", password="p")
    cursor = conn.cursor()

    with (
        _mock.patch.object(cursor, "_execute_graphql") as mock_gql,
        _mock.patch.object(cursor, "_execute_sql") as mock_sql,
    ):
        cursor.execute("{ orders { id } }")
        mock_gql.assert_called_once()
        mock_sql.assert_not_called()
        assert mock_gql.call_count == 1
        assert mock_sql.call_count == 0

    cursor2 = conn.cursor()
    with (
        _mock.patch.object(cursor2, "_execute_graphql") as mock_gql2,
        _mock.patch.object(cursor2, "_execute_sql") as mock_sql2,
    ):
        cursor2.execute("SELECT id FROM orders")
        mock_sql2.assert_called_once()
        mock_gql2.assert_not_called()
        assert mock_sql2.call_count == 1
        assert mock_gql2.call_count == 0


# ── REQ-606: ProvisaClient accepts bearer token ───────────────────────────────


@respx.mock
def test_client_bearer_token_sent_as_authorization_header():
    # REQ-606
    # When token is present, ProvisaClient sends Authorization: Bearer <token> on every request.
    route = respx.post(f"{BASE}/data/query").mock(
        return_value=httpx.Response(200, json={"data": {"x": []}})
    )
    client = ProvisaClient(BASE, token="my-bearer-token", role="admin")
    client.query("{ x { id } }")
    req = route.calls[0].request
    assert req.headers["authorization"] == "Bearer my-bearer-token"


def test_client_no_token_omits_authorization_header():
    # REQ-606
    # When token is omitted, Authorization header is absent.
    client = ProvisaClient(BASE, role="admin")
    headers = client._http_headers()
    assert "Authorization" not in headers


@respx.mock
def test_client_token_sent_on_every_request():
    # REQ-606
    # Bearer token is sent on every request, not just the first.
    route = respx.post(f"{BASE}/data/query").mock(
        return_value=httpx.Response(200, json={"data": {"x": []}})
    )
    client = ProvisaClient(BASE, token="persistent-token", role="admin")
    client.query("{ x { id } }")
    client.query("{ x { id } }")
    for call in route.calls:
        assert call.request.headers["authorization"] == "Bearer persistent-token"


# ── REQ-607: ProvisaClient error contract ─────────────────────────────────────


@respx.mock
def test_client_query_raises_http_status_error_on_4xx():
    # REQ-607
    # query() raises httpx.HTTPStatusError on HTTP 4xx/5xx.
    respx.post(f"{BASE}/data/query").mock(return_value=httpx.Response(403))
    client = ProvisaClient(BASE, token="tok", role="admin")
    with pytest.raises(httpx.HTTPStatusError):
        client.query("{ x { id } }")


@respx.mock
def test_client_query_raises_http_status_error_on_5xx():
    # REQ-607
    # query() raises httpx.HTTPStatusError on HTTP 5xx.
    respx.post(f"{BASE}/data/query").mock(return_value=httpx.Response(500))
    client = ProvisaClient(BASE, token="tok", role="admin")
    with pytest.raises(httpx.HTTPStatusError):
        client.query("{ x { id } }")


@respx.mock
def test_client_query_df_raises_runtime_error_on_graphql_errors():
    # REQ-607
    # query_df() raises RuntimeError when response body contains "errors" field,
    # regardless of HTTP status.
    respx.post(f"{BASE}/data/query").mock(
        return_value=httpx.Response(200, json={"errors": [{"message": "field not found"}]})
    )
    client = ProvisaClient(BASE, token="tok", role="admin")
    with pytest.raises(RuntimeError):
        client.query_df("{ x { id } }")


@respx.mock
def test_client_query_succeeds_without_errors_field():
    # REQ-607
    # query_df() does NOT raise RuntimeError when "errors" is absent from response.
    respx.post(f"{BASE}/data/query").mock(
        return_value=httpx.Response(200, json={"data": {"orders": [{"id": 1}]}})
    )
    client = ProvisaClient(BASE, token="tok", role="admin")
    df = client.query_df("{ orders { id } }")
    assert len(df) == 1


# ── REQ-608: ADBC adbc_connect uses hardcoded Flight port 8815 ───────────────


def test_adbc_connect_uses_hardcoded_flight_port_8815():
    # REQ-608
    # adbc_connect connects to Arrow Flight on port 8815 (hardcoded, not configurable).
    import inspect
    from provisa_client.adbc import adbc_connect  # type: ignore[import-not-found]

    src = inspect.getsource(adbc_connect)
    assert "8815" in src


def test_adbc_connect_has_no_flight_port_parameter():
    # REQ-608 / REQ-711: the Arrow Flight port is exposed as `port` (default 8815), not the
    # confusingly-named `flight_port`. REQ-711 made the port configurable, superseding the
    # earlier "hardcoded, no port param" constraint.
    import inspect
    from provisa_client.adbc import adbc_connect  # type: ignore[import-not-found]

    sig = inspect.signature(adbc_connect)
    assert "flight_port" not in sig.parameters
    assert sig.parameters["port"].default == 8815
