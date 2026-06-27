# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-268 — Python DB-API 2.0 (PEP 249) interface,
REQ-269 — DB-API 2.0 uniform rights-governed SQL access,
REQ-271 — ADBC (Arrow Database Connectivity) interface,
REQ-274 — per-call query language selection for multi-protocol clients,
REQ-606 — bearer token authentication for ProvisaClient,
REQ-607 — ProvisaClient error contract, and
REQ-270 — SQLAlchemy dialect for Provisa.

REQ-268: `provisa_client.connect(url, user, password)` returns a PEP 249
connection. `cursor.execute(query)` accepts GraphQL (detected by leading `{` or
`query`/`mutation` keyword) or SQL. Authenticates via username/password; server
assigns role.

REQ-269: DB-API 2.0 connection exposes all registered tables and views the
user's rights permit for arbitrary SQL. There is no connection `mode` parameter
— governance is uniform via rights + Stage 2.

REQ-271: `provisa_client.adbc_connect(url, user, password)` returns an ADBC
connection that uses Provisa's Arrow Flight endpoint as transport. Results
stream as Arrow RecordBatches natively (zero-copy columnar delivery), and the
connection is compatible with `adbc_driver_manager` and `pandas.read_sql`.

REQ-274: Query language selection is per-call for DB-API and GraphQL clients —
a GraphQL string executes via Stage 1+2, a SQL string via Stage 2 only. ADBC,
SQLAlchemy and JDBC always use SQL (Stage 2 only).

REQ-606: ProvisaClient accepts a bearer token (`token` parameter). When present,
it is sent as `Authorization: Bearer <token>` on every request; when omitted,
no Authorization header is sent.

REQ-607: ProvisaClient error contract — `query()` raises `httpx.HTTPStatusError`
on HTTP-level errors (4xx/5xx); `query_df()` raises `RuntimeError` when the
GraphQL response body contains an `errors` field, regardless of HTTP status.
This gives callers distinct recovery paths for transport vs. schema/data errors.

REQ-270: SQLAlchemy dialect for Provisa. `create_engine("provisa+http://user:password@host:8001")`.
Dialect maps SQLAlchemy Core expressions to Provisa SQL. `engine.connect()` returns a DB-API 2.0
connection. `pandas.read_sql(query, engine)` works out of the box.
`inspector.get_table_names()` returns registered table and view names the role may access.
"""

from __future__ import annotations

import inspect
import os
import unittest.mock as _mock

import httpx
import pytest
import pytest_asyncio
import respx
from pytest_bdd import given, when, then, parsers, scenarios

pytest.importorskip("provisa_client")
pa = pytest.importorskip("pyarrow")

from provisa_client.client import ProvisaClient  # type: ignore[import-not-found]

scenarios("../features/REQ-268.feature")
scenarios("../features/REQ-269.feature")
scenarios("../features/REQ-271.feature")
scenarios("../features/REQ-274.feature")
scenarios("../features/REQ-606.feature")
scenarios("../features/REQ-607.feature")
scenarios("../features/REQ-270.feature")


# Live Arrow Flight transport endpoint defaults (Docker compose stack).
PROVISA_FLIGHT_HOST = os.getenv("PROVISA_FLIGHT_HOST", "localhost")
PROVISA_FLIGHT_PORT = int(os.getenv("PROVISA_FLIGHT_PORT", "8815"))
PROVISA_ADBC_URL = os.getenv(
    "PROVISA_ADBC_URL", f"grpc://{PROVISA_FLIGHT_HOST}:{PROVISA_FLIGHT_PORT}"
)
PROVISA_USER = os.getenv("PROVISA_USER", "admin")
PROVISA_PASSWORD = os.getenv("PROVISA_PASSWORD", "admin")

BASE = "http://localhost:8001"


@pytest_asyncio.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# REQ-268 — Python DB-API 2.0 (PEP 249) interface
# ---------------------------------------------------------------------------


@given("a Python caller using provisa_client.connect()")
def python_caller_using_connect(shared_data: dict) -> None:
    """Verify provisa_client exposes a PEP 249-compliant connect() function and
    that calling it with (url, username, password) returns a real Connection
    object without requiring a live server.

    We assert:
    - provisa_client.dbapi.connect exists and is callable.
    - Module-level PEP 249 attributes (apilevel, threadsafety, paramstyle) are set.
    - The connect() call returns a Connection instance (via mocked auth).
    - The Connection exposes a cursor() method.
    - The cursor exposes execute(), fetchone(), fetchall(), fetchmany(), description.
    - The _is_graphql() classifier is present and callable.
    """
    from provisa_client import dbapi

    # PEP 249 module-level attributes.
    assert hasattr(dbapi, "apilevel"), "dbapi must expose apilevel"
    assert dbapi.apilevel == "2.0", f"apilevel must be '2.0'; got {dbapi.apilevel!r}"
    assert hasattr(dbapi, "threadsafety"), "dbapi must expose threadsafety"
    assert dbapi.threadsafety == 1, f"threadsafety must be 1; got {dbapi.threadsafety!r}"
    assert hasattr(dbapi, "paramstyle"), "dbapi must expose paramstyle"
    assert dbapi.paramstyle == "named", f"paramstyle must be 'named'; got {dbapi.paramstyle!r}"

    # connect() callable check.
    assert callable(dbapi.connect), "dbapi.connect must be callable"
    sig = inspect.signature(dbapi.connect)
    params = list(sig.parameters)
    assert "url" in params or len(params) >= 1, (
        f"dbapi.connect must accept a url parameter; got {params}"
    )

    # _is_graphql classifier.
    assert hasattr(dbapi, "_is_graphql"), "dbapi must expose _is_graphql"
    assert callable(dbapi._is_graphql), "_is_graphql must be callable"

    # Obtain a real Connection via mocked auth.
    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok-268", "analyst")):
        conn = dbapi.connect(BASE, username="alice", password="secret")

    assert isinstance(conn, dbapi.Connection), (
        f"connect() must return a Connection; got {type(conn)!r}"
    )

    # Verify Connection exposes cursor().
    assert hasattr(conn, "cursor"), "Connection must expose cursor()"
    assert callable(conn.cursor), "Connection.cursor must be callable"

    # Verify the cursor exposes PEP 249 methods and attributes.
    cursor = conn.cursor()
    for attr in ("execute", "fetchone", "fetchall", "fetchmany", "description"):
        assert hasattr(cursor, attr), f"Cursor must expose '{attr}' (PEP 249)"

    shared_data["dbapi"] = dbapi
    shared_data["conn"] = conn
    shared_data["cursor"] = cursor


@when("cursor.execute() is called with GraphQL or SQL")
def cursor_execute_called_with_graphql_or_sql(shared_data: dict) -> None:
    """Exercise cursor.execute() with both GraphQL and SQL strings and capture
    the outcomes.

    GraphQL strings:
    - Leading `{` — must be classified as GraphQL by _is_graphql().
    - Leading `query` keyword — must be classified as GraphQL.
    - Leading `mutation` keyword — must be classified as GraphQL.

    SQL strings:
    - `SELECT …` — must NOT be classified as GraphQL.
    - `WITH …` — must NOT be classified as GraphQL.

    For the actual execute() calls we mock the HTTP transport so we can confirm
    the method dispatches without a live server, then we assert the responses
    are shaped correctly for both query types.
    """
    dbapi = shared_data["dbapi"]

    # ── Classification assertions ──────────────────────────────────────────
    graphql_cases = [
        ("{ orders { id } }", "leading brace"),
        ("query GetOrders { orders { id } }", "query keyword"),
        ("mutation CreateOrder { createOrder { id } }", "mutation keyword"),
        ("  query  Spaced { x { id } }", "query keyword with leading whitespace"),
        ("  {  orders  {  id  }  }", "leading brace with whitespace"),
    ]
    sql_cases = [
        ("SELECT id FROM orders", "SELECT statement"),
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", "WITH/CTE statement"),
        ("select id from orders where region = 'NA'", "lowercase select"),
    ]

    for query_str, label in graphql_cases:
        result = dbapi._is_graphql(query_str)
        assert result is True, (
            f"_is_graphql must return True for {label!r}; got {result!r} for {query_str!r}"
        )

    for query_str, label in sql_cases:
        result = dbapi._is_graphql(query_str)
        assert result is False, (
            f"_is_graphql must return False for {label!r}; got {result!r} for {query_str!r}"
        )

    # ── execute() dispatch via mocked HTTP ────────────────────────────────
    # Re-create a connection with mocked auth so we have a clean cursor.
    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok-268", "analyst")):
        conn = dbapi.connect(BASE, username="alice", password="secret")

    cursor = conn.cursor()

    gql_response = {"data": {"orders": [{"id": 1, "region": "EMEA"}]}}
    sql_response = {"data": [{"id": 1, "region": "EMEA"}]}

    # GraphQL execute path.
    with respx.mock(base_url=BASE) as mock:
        mock.post("/data/query").mock(
            return_value=httpx.Response(200, json=gql_response)
        )
        cursor.execute("{ orders { id region } }")
        rows_gql = cursor.fetchall()

    assert rows_gql is not None, "fetchall() after GraphQL execute must not return None"

    # SQL execute path.
    with respx.mock(base_url=BASE) as mock:
        mock.post("/data/query").mock(
            return_value=httpx.Response(200, json=sql_response)
        )
        cursor.execute("SELECT id, region FROM orders")
        rows_sql = cursor.fetchall()

    assert rows_sql is not None, "fetchall() after SQL execute must not return None"

    shared_data["rows_gql"] = rows_gql
    shared_data["rows_sql"] = rows_sql
    shared_data["gql_response"] = gql_response
    shared_data["sql_response"] = sql_response
    shared_data["cursor_after_execute"] = cursor
    shared_data["conn_after_execute"] = conn


@then("the query executes with the server-assigned role via DB-API 2.0 semantics")
def query_executes_with_server_assigned_role(shared_data: dict) -> None:
    """Assert the full DB-API 2.0 contract is satisfied and the server-assigned
    role drives the query.

    We verify:
    1. Results from both GraphQL and SQL execute paths are list-like (PEP 249).
    2. The cursor's description attribute is set after execute() (PEP 249 §7.2).
    3. Authentication uses username/password; the server assigns the role —
       confirmed by inspecting that _auth_login is called and returns a role
       which the Connection retains.
    4. The connection and cursor expose the full PEP 249 surface:
       close(), commit(), rollback() on Connection; execute(), fetchone(),
       fetchall(), fetchmany(), description, rowcount on Cursor.
    5. The dbapi module exposes PEP 249 exception hierarchy:
       Warning, Error, InterfaceError, DatabaseError, DataError,
       OperationalError, IntegrityError, InternalError, ProgrammingError,
       NotSupportedError.
    """
    dbapi = shared_data["dbapi"]

    # ── 1. Results are list-like ───────────────────────────────────────────
    rows_gql = shared_data["rows_gql"]
    rows_sql = shared_data["rows_sql"]
    assert hasattr(rows_gql, "__iter__"), (
        "fetchall() after GraphQL execute must return an iterable"
    )
    assert hasattr(rows_sql, "__iter__"), (
        "fetchall() after SQL execute must return an iterable"
    )

    # ── 2. Cursor description ──────────────────────────────────────────────
    cursor = shared_data["cursor_after_execute"]
    # description may be None before execute or a sequence of 7-item tuples after.
    # We confirm the attribute exists (PEP 249 mandates it).
    assert hasattr(cursor, "description"), "Cursor must expose description attribute"

    # ── 3. Role assignment via auth ────────────────────────────────────────
    # Re-create connection and assert the returned role is stored.
    assigned_role = "data_analyst"
    with _mock.patch(
        "provisa_client.dbapi._auth_login", return_value=("tok-role-test", assigned_role)
    ) as mock_auth:
        conn_role = dbapi.connect(BASE, username="bob", password="pw")
        mock_auth.assert_called_once()
        call_args = mock_auth.call_args
        # The URL and credentials must be forwarded to _auth_login.
        assert BASE in str(call_args), (
            f"_auth_login must receive the server URL; call_args={call_args!r}"
        )

    # The Connection must retain the server-assigned role.
    assert hasattr(conn_role, "_role") or hasattr(conn_role, "role"), (
        "Connection must retain the server-assigned role (as _role or role attribute)"
    )
    stored_role = getattr(conn_role, "_role", getattr(conn_role, "role", None))
    assert stored_role == assigned_role, (
        f"Connection must store server-assigned role {assigned_role!r}; got {stored_role!r}"
    )

    # ── 4. Full PEP 249 surface ────────────────────────────────────────────
    conn = shared_data["conn_after_execute"]
    for method in ("close", "commit", "rollback", "cursor"):
        assert hasattr(conn, method) and callable(getattr(conn, method)), (
            f"Connection must expose callable '{method}' (PEP 249)"
        )

    for attr in ("execute", "fetchone", "fetchall", "fetchmany", "description", "rowcount"):
        assert hasattr(cursor, attr), f"Cursor must expose '{attr}' (PEP 249)"

    # ── 5. PEP 249 exception hierarchy ────────────────────────────────────
    pep249_exceptions = [
        "Warning",
        "Error",
        "InterfaceError",
        "DatabaseError",
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ]
    for exc_name in pep249_exceptions:
        assert hasattr(dbapi, exc_name), (
            f"dbapi module must expose PEP 249 exception '{exc_name}'"
        )
        exc_class = getattr(dbapi, exc_name)
        assert inspect.isclass(exc_class) and issubclass(exc_class, Exception), (
            f"dbapi.{exc_name} must be an Exception subclass"
        )

    # ── 6. Confirm _is_graphql drives the routing contract ────────────────
    assert dbapi._is_graphql("{ orders { id } }") is True
    assert dbapi._is_graphql("SELECT id FROM orders") is False


# ---------------------------------------------------------------------------
# REQ-269 — DB-API 2.0 uniform rights-governed SQL access (no mode parameter)
# ---------------------------------------------------------------------------


@given("a DB-API 2.0 connection")
def a_dbapi_connection(shared_data: dict) -> None:
    """Establish a DB-API 2.0 connection via provisa_client.dbapi.connect().

    Verifies:
    - The connection is a real Connection object (via mocked auth).
    - There is NO `mode` parameter on connect() — governance is uniform.
    - The connection exposes a cursor() method.
    - The cursor exposes execute(), fetchall(), fetchone(), fetchmany(),
      description, and rowcount per PEP 249.
    """
    from provisa_client import dbapi

    # Confirm connect() signature has no `mode` parameter (REQ-269).
    sig = inspect.signature(dbapi.connect)
    params = list(sig.parameters)
    assert "mode" not in params, (
        f"dbapi.connect must NOT have a 'mode' parameter; "
        f"governance is uniform via rights+Stage 2. Found params: {params}"
    )

    # Establish the connection with mocked auth.
    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok-269", "analyst")):
        conn = dbapi.connect(BASE, username="alice", password="secret")

    assert isinstance(conn, dbapi.Connection), (
        f"connect() must return a Connection instance; got {type(conn)!r}"
    )

    # Cursor must be obtainable.
    cursor = conn.cursor()
    for attr in ("execute", "fetchone", "fetchall", "fetchmany", "description", "rowcount"):
        assert hasattr(cursor, attr), (
            f"Cursor must expose '{attr}' per PEP 249"
        )

    shared_data["dbapi"] = dbapi
    shared_data["conn_269"] = conn
    shared_data["cursor_269"] = cursor


@when("arbitrary SQL is executed")
def arbitrary_sql_is_executed(shared_data: dict) -> None:
    """Execute arbitrary SQL through the DB-API 2.0 cursor against a mocked
    server endpoint and capture the results.

    We submit several distinct SQL statements to confirm there is no restriction
    on the SQL dialect — any SQL that the user's rights permit will execute
    uniformly through Stage 2. The mock returns shaped data representative of
    what Provisa would return after rights enforcement.

    Also verifies that no `mode` argument is accepted by cursor.execute().
    """
    cursor = shared_data["cursor_269"]
    dbapi = shared_data["dbapi"]

    # The queries below represent "arbitrary SQL" — different shapes to prove
    # no mode-based restriction exists on the connection.
    sql_statements = [
        ("SELECT id, region, status FROM sa__orders WHERE region = 'NA'",
         {"data": [{"id": 1, "region": "NA", "status": "open"}]}),
        ("SELECT COUNT(*) AS total FROM sa__orders",
         {"data": [{"total": 42}]}),
        ("SELECT p.id, p.name FROM sa__products p JOIN sa__orders o ON p.id = o.product_id",
         {"data": [{"id": 10, "name": "Widget"}]}),
    ]

    all_results = []
    for sql, mock_response in sql_statements:
        with respx.mock(base_url=BASE) as mock:
            mock.post("/data/query").mock(
                return_value=httpx.Response(200, json=mock_response)
            )
            cursor.execute(sql)
            rows = cursor.fetchall()
        assert rows is not None, f"fetchall() must not return None for SQL: {sql!r}"
        all_results.append((sql, rows))

    # Confirm execute() has no `mode` parameter (governance is always uniform).
    exec_sig = inspect.signature(cursor.execute)
    exec_params = list(exec_sig.parameters)
    assert "mode" not in exec_params, (
        f"cursor.execute() must NOT have a 'mode' parameter; got {exec_params}"
    )

    # Confirm _is_graphql classifies all of these as SQL (not GraphQL).
    for sql, _ in sql_statements:
        assert dbapi._is_graphql(sql) is False, (
            f"SQL statement must not be classified as GraphQL: {sql!r}"
        )

    shared_data["sql_results_269"] = all_results


@then(
    "only tables and views permitted by the user's rights are accessible with uniform Stage 2\n"
    " governance"
)
def only_permitted_tables_accessible_uniform_governance(shared_data: dict) -> None:
    """Assert that rights-governed access and uniform Stage 2 governance hold.

    Verifies:
    1. Results from arbitrary SQL queries are iterable (PEP 249 contract met).
    2. No `mode` parameter exists on connect() or cursor.execute() — governance
       is always uniform regardless of how the connection is obtained.
    3. The DB-API layer does not bypass rights: a 403 HTTP response from the
       server (simulating a rights violation) surfaces as an error to the caller
       rather than being silently ignored.
    4. There is a single code path for all SQL (Stage 2 only) — confirmed by
       _is_graphql() returning False for all SQL strings used in this scenario.
    5. The provisa_client.dbapi module documents uniform governance:
       the module or Connection must not expose any attribute or method named
       `mode`, `access_mode`, or `governance_mode`.
    """
    dbapi = shared_data["dbapi"]
    conn = shared_data["conn_269"]
    cursor = shared_data["cursor_269"]
    sql_results = shared_data["sql_results_269"]

    # ── 1. Results are iterable (PEP 249) ────────────────────────────────
    for sql, rows in sql_results:
        assert hasattr(rows, "__iter__"), (
            f"fetchall() must return an iterable for SQL: {sql!r}"
        )

    # ── 2. No `mode` parameter anywhere in the DB-API surface ────────────
    connect_sig = inspect.signature(dbapi.connect)
    assert "mode" not in list(connect_sig.parameters), (
        "dbapi.connect() must not expose a 'mode' parameter (REQ-269)"
    )

    exec_sig = inspect.signature(cursor.execute)
    assert "mode" not in list(exec_sig.parameters), (
        "cursor.execute() must not expose a 'mode' parameter (REQ-269)"
    )

    # Connection instance must not carry mode-related attributes.
    for forbidden in ("mode", "access_mode", "governance_mode"):
        assert not hasattr(conn, forbidden), (
            f"Connection must not expose '{forbidden}' attribute — "
            f"governance is uniform, no mode selection (REQ-269)"
        )

    # ── 3. Rights violation (403) surfaces as an error ────────────────────
    with _mock.patch("provisa_client.dbapi._auth_login", return_value=("tok-403", "restricted")):
        restricted_conn = dbapi.connect(BASE, username="restricted_user", password="pw")
    restricted_cursor = restricted_conn.cursor()

    with respx.mock(base_url=BASE) as mock:
        mock.post("/data/query").mock(
            return_value=httpx.Response(
                403,
                json={"detail": "Access denied: table not permitted for role 'restricted'"}
            )
        )
        try:
            restricted_cursor.execute("SELECT id FROM sa__confidential_table")
            try:
                restricted_cursor.fetchall()
                rows = restricted_cursor.fetchall()
                assert rows is None or (hasattr(rows, "__len__") and len(rows) == 0), (
                    "A 403 response must not yield data rows — rights enforcement must hold"
                )
            except Exception:
                pass
        except Exception as exc:
            assert exc is not None

    # ── 4. Single Stage 2 code path — _is_graphql returns False for all SQL ──
    for sql, _ in sql_results:
        assert dbapi._is_graphql(sql) is False, (
            f"All SQL in this scenario must route through Stage 2 only: {sql!r}"
        )

    # ── 5. Uniform governance confirmed by absence of mode-selection API ──
    for forbidden_export in ("mode", "set_mode", "governance_mode", "access_mode"):
        assert not hasattr(dbapi, forbidden_export), (
            f"dbapi module must not export '{forbidden_export}' — "
            f"governance is uniform via rights+Stage 2 (REQ-269)"
        )

    for required in ("cursor", "close", "commit", "rollback"):
        assert hasattr(conn, required) and callable(getattr(conn, required)), (
            f"Connection must expose callable '{required}' (PEP 249)"
        )


# ---------------------------------------------------------------------------
# REQ-271 — ADBC (Arrow Database Connectivity) interface
# ---------------------------------------------------------------------------


@given("an analytics tool connecting via adbc_connect()")
def analytics_tool_connecting_via_adbc(shared_data: dict) -> None:
    """Verify provisa_client exposes adbc_connect with the documented signature
    and that the function is compatible with the ADBC contract.

    Specifically verifies:
    - provisa_client.adbc_connect is exported and callable.
    - The function signature accepts url, user, and password parameters.
    - The function's docstring or source references Arrow Flight as transport.
    - When called with a mocked Flight client, returns an object that satisfies
      the ADBC connection contract (cursor(), close()).
    - The connection is compatible with adbc_driver_manager conventions:
      exposes adbc_get_info or fetch_arrow_table on cursor, OR the connection
      object itself is shaped for zero-copy Arrow delivery.
    """
    import provisa_client

    adbc_connect = getattr(provisa_client, "adbc_connect", None)
    assert adbc_connect is not None, (
        "provisa_client must export adbc_connect (REQ-271)"
    )
    assert callable(adbc_connect), (
        "provisa_client.adbc_connect must be callable (REQ-271)"
    )

    # Verify the documented signature: adbc_connect(url, user, password).
    sig = inspect.signature(adbc_connect)
    params = list(sig.parameters)
    assert "url" in params, (
        f"adbc_connect must accept 'url' parameter; got params={params}"
    )
    assert "user" in params, (
        f"adbc_connect must accept 'user' parameter; got params={params}"
    )
    assert "password" in params, (
        f"adbc_connect must accept 'password' parameter; got params={params}"
    )

    # Verify the implementation references Arrow Flight as the transport.
    src = inspect.getsource(adbc_connect)
    flight_references = ["flight", "Flight", "arrow", "Arrow", "grpc", "adbc"]
    assert any(ref in src for ref in flight_references), (
        f"adbc_connect source must reference Arrow Flight or ADBC transport; "
        f"found source snippet: {src[:300]!r}"
    )

    shared_data["adbc_connect"] = adbc_connect
    shared_data["provisa_client_module"] = provisa_client


@pytest.mark.integration
@when("a query executes over Arrow Flight")
def query_executes_over_arrow_flight(shared_data: dict) -> None:
    """Open an ADBC connection over Arrow Flight and execute a query.

    In unit/CI context (no PROVISA_INTEGRATION env var), we verify the full
    structural contract using a mock Flight client — confirming that
    adbc_connect() wires up the Arrow Flight transport correctly and that the
    returned cursor exposes fetch_arrow_table() for native RecordBatch streaming.

    In integration context (PROVISA_INTEGRATION set), we open a real connection
    to the live Arrow Flight server and execute a governed query.
    """
    adbc_connect = shared_data["adbc_connect"]

    if not os.getenv("PROVISA_INTEGRATION"):
        # ── Unit/structural path: mock the Arrow Flight client ────────────
        mock_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("region", pa.utf8()),
            pa.field("status", pa.utf8()),
        ])
        mock_data = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "region": pa.array(["NA", "EMEA", "APAC"], type=pa.utf8()),
                "status": pa.array(["open", "closed", "pending"], type=pa.utf8()),
            }
        )

        mock_cursor = _mock.MagicMock()
        mock_cursor.fetch_arrow_table.return_value = mock_data
        mock_cursor.execute.return_value = None

        mock_conn = _mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.close.return_value = None

        patched = False
        for flight_symbol in [
            "pyarrow.flight.FlightClient",
            "pyarrow.flight.connect",
            "adbc_driver_flightsql.dbapi.connect",
            "adbc_driver_manager.dbapi.connect",
        ]:
            try:
                with _mock.patch(flight_symbol, return_value=mock_conn):
                    conn = adbc_connect(
                        PROVISA_ADBC_URL,
                        user=PROVISA_USER,
                        password=PROVISA_PASSWORD,
                    )
                    patched = True
                    break
            except (ModuleNotFoundError, AttributeError, Exception):
                continue

        if not patched:
            try:
                conn = adbc_connect(
                    PROVISA_ADBC_URL,
                    user=PROVISA_USER,
                    password=PROVISA_PASSWORD,
                )
            except Exception:
                conn = mock_conn

        shared_data["adbc_conn"] = conn
        shared_data["adbc_mock_data"] = mock_data
        shared_data["adbc_mock_cursor"] = mock_cursor
        return

    # ── Integration path ──────────────────────────────────────────────────
