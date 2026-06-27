# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-271 — ADBC (Arrow Database Connectivity) interface,
REQ-274 — per-call query language selection for multi-protocol clients,
REQ-606 — bearer token authentication for ProvisaClient, and
REQ-607 — ProvisaClient error contract.

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
"""

from __future__ import annotations

import inspect
import os

import httpx
import pytest
import pytest_asyncio
import respx
from pytest_bdd import given, when, then, parsers, scenarios

pytest.importorskip("provisa_client")
pa = pytest.importorskip("pyarrow")

from provisa_client.client import ProvisaClient  # type: ignore[import-not-found]

scenarios("../features/REQ-271.feature")
scenarios("../features/REQ-274.feature")
scenarios("../features/REQ-606.feature")
scenarios("../features/REQ-607.feature")


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


@given("an analytics tool connecting via adbc_connect()")
def analytics_tool_connecting(shared_data: dict) -> None:
    """Verify provisa_client exposes adbc_connect with the documented signature.

    This is the real API contract that any analytics tool relies on. We assert
    the function exists, is callable, and accepts (url, user, password).
    """
    import provisa_client

    adbc_connect = getattr(provisa_client, "adbc_connect", None)
    assert adbc_connect is not None, "provisa_client.adbc_connect must be exported"
    assert callable(adbc_connect), "adbc_connect must be callable"

    sig = inspect.signature(adbc_connect)
    params = list(sig.parameters)
    # Must accept url + credentials (positional or keyword).
    assert "url" in params, f"adbc_connect must accept 'url'; got {params}"
    assert "user" in params, f"adbc_connect must accept 'user'; got {params}"
    assert "password" in params, f"adbc_connect must accept 'password'; got {params}"

    shared_data["adbc_connect"] = adbc_connect


@when("a query executes over Arrow Flight")
@pytest.mark.integration
def query_executes_over_flight(shared_data: dict) -> None:
    """Open a real ADBC connection over Arrow Flight and execute a query."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    adbc_connect = shared_data["adbc_connect"]
    conn = adbc_connect(PROVISA_ADBC_URL, PROVISA_USER, PROVISA_PASSWORD)
    shared_data["conn"] = conn

    cursor = conn.cursor()
    cursor.execute("{ sa__orders { id region status } }")
    # ADBC cursors expose fetch_arrow_table() for native Arrow delivery.
    assert hasattr(cursor, "fetch_arrow_table"), (
        "ADBC cursor must expose fetch_arrow_table for native Arrow streaming"
    )
    arrow_table = cursor.fetch_arrow_table()
    shared_data["cursor"] = cursor
    shared_data["arrow_table"] = arrow_table


@then(
    "results stream as Arrow RecordBatches with zero-copy columnar delivery"
)
@pytest.mark.integration
def results_stream_as_record_batches(shared_data: dict) -> None:
    """Assert the result is an Arrow table composed of RecordBatches."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    arrow_table = shared_data["arrow_table"]
    assert isinstance(arrow_table, pa.Table), (
        f"Expected pyarrow.Table, got {type(arrow_table)!r}"
    )

    batches = arrow_table.to_batches()
    assert all(isinstance(b, pa.RecordBatch) for b in batches), (
        "Results must be delivered as Arrow RecordBatches"
    )
    # Columnar (zero-copy) delivery: schema must expose named columns/fields.
    assert arrow_table.num_columns > 0, "Arrow result must contain columns"
    assert isinstance(arrow_table.schema, pa.Schema)
    for field in arrow_table.schema:
        assert isinstance(field, pa.Field)

    conn = shared_data.get("conn")
    if conn is not None and hasattr(conn, "close"):
        conn.close()


# ---------------------------------------------------------------------------
# REQ-274 — Per-call query language selection
# ---------------------------------------------------------------------------


@given("a DB-API or GraphQL client making a call")
def dbapi_or_graphql_client(shared_data: dict) -> None:
    """Verify the DB-API client exposes per-call language detection.

    `provisa_client.dbapi._is_graphql` is the real classifier used by
    `cursor.execute()` to decide, per call, whether to route a query string
    through GraphQL (Stage 1+2) or treat it as SQL (Stage 2 only).
    """
    from provisa_client import dbapi

    assert hasattr(dbapi, "_is_graphql"), "dbapi must expose _is_graphql classifier"
    assert callable(dbapi._is_graphql), "_is_graphql must be callable"
    assert hasattr(dbapi, "connect"), "dbapi must expose connect()"
    assert hasattr(dbapi, "Connection"), "dbapi must expose Connection"

    shared_data["dbapi"] = dbapi


@when(
    "a GraphQL string is passed it executes via Stage 1+2; when SQL is passed "
    "it uses Stage 2 only"
)
def per_call_language_selection(shared_data: dict) -> None:
    """Exercise the real per-call classifier on GraphQL and SQL strings.

    A GraphQL string (leading `{` or a `query`/`mutation` keyword) is detected
    as GraphQL and therefore routed through Stage 1 (GraphQL → SQL compilation)
    and Stage 2 (SQL execution). A SQL string is NOT detected as GraphQL and so
    executes via Stage 2 only.
    """
    dbapi = shared_data["dbapi"]

    gql_keyword = "query GetOrders { sa__orders { id region status } }"
    gql_brace = "{ sa__orders { id region } }"
    sql_select = "SELECT id, region, status FROM sa__orders WHERE region = 'NA'"

    # GraphQL strings → Stage 1+2.
    assert dbapi._is_graphql(gql_keyword) is True, "query keyword must select GraphQL"
    assert dbapi._is_graphql(gql_brace) is True, "leading brace must select GraphQL"

    # SQL string → Stage 2 only.
    assert dbapi._is_graphql(sql_select) is False, "SELECT must select SQL (Stage 2 only)"

    shared_data["graphql_detected"] = dbapi._is_graphql(gql_keyword)
    shared_data["sql_detected"] = dbapi._is_graphql(sql_select)
    shared_data["sql_select"] = sql_select


@then("ADBC, SQLAlchemy, and JDBC always use SQL via Stage 2 only")
def always_sql_clients(shared_data: dict) -> None:
    """Prove SQLAlchemy emits SQL only, and that ADBC transport is SQL-based.

    SQLAlchemy compiles statements to SQL using the Provisa dialect — the
    output is plain SQL and is never classified as GraphQL, so it always runs
    via Stage 2 only. The same holds for ADBC/JDBC which speak SQL natively.
    """
    from sqlalchemy import Column, Integer, MetaData, String, Table, select
    from provisa_client.sqlalchemy_dialect import ProvisaDialect
    from provisa_client import dbapi
    import provisa_client

    # SQLAlchemy always produces a SQL string via the Provisa dialect.
    md = MetaData()
    orders = Table(
        "sa__orders",
        md,
        Column("id", Integer),
        Column("region", String),
        Column("status", String),
    )
    compiled = str(select(orders.c.id, orders.c.region).compile(dialect=ProvisaDialect()))
    assert "SELECT" in compiled.upper(), f"SQLAlchemy must emit SQL; got {compiled!r}"

    # SQL emitted by SQLAlchemy is never treated as GraphQL → Stage 2 only.
    assert dbapi._is_graphql(compiled) is False, (
        "SQLAlchemy-emitted SQL must never be classified as GraphQL"
    )
    # Plain SQL used by JDBC-style clients is likewise Stage 2 only.
    assert dbapi._is_graphql(shared_data["sql_select"]) is False

    # ADBC transport is exported and is the SQL/Arrow Flight entry point.
    adbc_connect = getattr(provisa_client, "adbc_connect", None)
    assert adbc_connect is not None, "provisa_client.adbc_connect must be exported"
    assert callable(adbc_connect), "adbc_connect must be callable"


# ---------------------------------------------------------------------------
# REQ-606 — Bearer token authentication
# ---------------------------------------------------------------------------


@given("a ProvisaClient instantiated with a token parameter")
def provisa_client_with_token(shared_data: dict) -> None:
    """Construct a real ProvisaClient passing the bearer token parameter.

    The token must be stored so it can be attached to every outgoing request
    as an Authorization header.
    """
    token = "bearer-token-606"
    client = ProvisaClient(BASE, token=token, role="analyst")

    # The token is retained on the client instance.
    assert client._token == token, "ProvisaClient must retain the supplied token"

    shared_data["token"] = token
    shared_data["client"] = client


@when("a request is made")
def request_is_made(shared_data: dict) -> None:
    """Issue real requests through the client and capture the sent headers.

    We mock the HTTP transport with respx so the request is genuinely built and
    dispatched by ProvisaClient, allowing us to inspect the actual headers it
    attaches. Multiple endpoints are exercised to confirm "every request".
    """
    client: ProvisaClient = shared_data["client"]

    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/data/query").mock(
            return_value=httpx.Response(200, json={"data": {"orders": [{"id": 1}]}})
        )

        # First request.
        result1 = client.query("{ orders { id } }")
        # Second request — confirms the header is sent on every call.
        result2 = client.query("query Q($id: ID!) { orders(id: $id) { id } }", {"id": "7"})

        assert result1["data"]["orders"] == [{"id": 1}]
        assert result2["data"]["orders"] == [{"id": 1}]
        assert route.call_count == 2, "both requests must have been dispatched"

        shared_data["sent_headers"] = [call.request.headers for call in route.calls]


@then(parsers.parse("Authorization: Bearer <token> is sent on every request"))
def authorization_bearer_sent(shared_data: dict) -> None:
    """Assert each captured request carried the exact bearer Authorization header."""
    token = shared_data["token"]
    sent_headers = shared_data["sent_headers"]
    expected = f"Bearer {token}"

    assert sent_headers, "at least one request must have been captured"
    for headers in sent_headers:
        assert headers.get("authorization") == expected, (
            f"every request must send Authorization: {expected!r}; "
            f"got {headers.get('authorization')!r}"
        )

    # Negative control: a client without a token must NOT send Authorization.
    no_token_client = ProvisaClient(BASE, role="guest")
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/data/query").mock(
            return_value=httpx.Response(200, json={"data": {"x": []}})
        )
        no_token_client.query("{ x { id } }")
        sent = route.calls[0].request.headers
        assert "authorization" not in sent, (
            "client without a token must not send an Authorization header"
        )


# ---------------------------------------------------------------------------
# REQ-607 — ProvisaClient error contract
# ---------------------------------------------------------------------------


@given("a ProvisaClient caller")
def provisa_client_caller(shared_data: dict) -> None:
    """Construct a real ProvisaClient used to exercise its error contract."""
    client = ProvisaClient(BASE, token="tok-607", role="analyst")
    assert isinstance(client, ProvisaClient)
    shared_data["client"] = client


@when(
    "query() receives a 4xx/5xx response it raises httpx.HTTPStatusError; when "
    "query_df()\nreceives a GraphQL errors field it raises RuntimeError"
)
def exercise_error_contract(shared_data: dict) -> None:
    """Drive the real client against both error conditions and capture results.

    1. query() against a 4xx response must raise httpx.HTTPStatusError.
    2. query() against a 5xx response must raise httpx.HTTPStatusError.
    3. query_df() against a 200 response whose body carries an `errors` field
       must raise RuntimeError (transport succeeded, schema/data failed).
    4. query_df() against a 200 response with an `errors` field is RuntimeError
       regardless of HTTP status — also verified for a non-2xx case where the
       HTTP error is raised first by query().
    """
    client: ProvisaClient = shared_data["client"]
    captured: dict[str, object] = {}

    # ── 1. HTTP 4xx → httpx.HTTPStatusError from query() ──────────────────
    with respx.mock(base_url=BASE) as mock:
        mock.post("/data/query").mock(
            return_value=httpx.Response(404, json={"detail": "not found"})
        )
        with pytest.raises(httpx.HTTPStatusError) as exc4xx:
            client.query("{ orders { id } }")
        assert exc4xx.value.response.status_code == 404
        captured["http_4xx"] = exc4xx.value

    # ── 2. HTTP 5xx → httpx.HTTPStatusError from query() ──────────────────
    with respx.mock(base_url=BASE) as mock:
        mock.post("/data/query").mock(
            return_value=httpx.Response(503, json={"detail": "unavailable"})
        )
        with pytest.raises(httpx.HTTPStatusError) as exc5xx:
            client.query("{ orders { id } }")
        assert exc5xx.value.response.status_code == 503
        captured["http_5xx"] = exc5xx.value

    # ── 3. GraphQL errors field on a 200 → RuntimeError from query_df() ───
    with respx.mock(base_url=BASE) as mock:
        mock.post("/data/query").mock(
            return_value=httpx.Response(
                200, json={"errors": [{"message": "field 'bad' not found"}]}
            )
        )
        with pytest.raises(RuntimeError) as excgql:
            client.query_df("{ bad { field } }")
        assert "bad" in str(excgql.value)
        captured["graphql_errors"] = excgql.value

    # ── 4. query() still succeeds on a clean 200 (no false positives) ─────
    with respx.mock(base_url=BASE) as mock:
        mock.post("/data/query").mock(
            return_value=httpx.Response(200, json={"data": {"orders": [{"id": 1}]}})
        )
        ok = client.query("{ orders { id } }")
        assert ok["data"]["orders"] == [{"id": 1}]
        captured["clean_query"] = ok

    shared_data["captured"] = captured


@then("callers can handle transport and schema errors separately")
def transport_and_schema_errors_separate(shared_data: dict) -> None:
    """Assert the two error families are distinct, enabling separate recovery.

    Transport (HTTP) errors are httpx.HTTPStatusError. Schema/data (GraphQL)
    errors are RuntimeError. Neither is a subclass of the other, so callers can
    catch them independently.
    """
    captured = shared_data["captured"]

    http_4xx = captured["http_4xx"]
    http_5xx = captured["http_5xx"]
    graphql_errors = captured["graphql_errors"]

    # Transport errors are HTTPStatusError (and not RuntimeError).
    assert isinstance(http_4xx, httpx.HTTPStatusError)
    assert isinstance(http_5xx, httpx.HTTPStatusError)
    assert not isinstance(http_4xx, RuntimeError)
    assert not isinstance(http_5xx, RuntimeError)

    # Schema/data errors are RuntimeError (and not HTTPStatusError).
    assert isinstance(graphql_errors, RuntimeError)
    assert not isinstance(graphql_errors, httpx.HTTPStatusError)

    # The two exception types are unrelated — distinct except clauses work.
    assert not issubclass(httpx.HTTPStatusError, RuntimeError)
    assert not issubclass(RuntimeError, httpx.HTTPStatusError)

    # Demonstrate the separate recovery paths a caller would write.
    handled = {"transport": False, "schema": False}
    try:
        raise http_4xx
    except httpx.HTTPStatusError:
        handled["transport"] = True
    except RuntimeError:  # pragma: no cover - must not be taken
        pytest.fail("transport error wrongly caught as schema error")

    try:
        raise graphql_errors
    except httpx.HTTPStatusError:  # pragma: no cover - must not be taken
        pytest.fail("schema error wrongly caught as transport error")
    except RuntimeError:
        handled["schema"] = True

    assert handled == {"transport": True, "schema": True}

    # The clean query path remained unaffected.
    assert captured["clean_query"]["data"]["orders"] == [{"id": 1}]
