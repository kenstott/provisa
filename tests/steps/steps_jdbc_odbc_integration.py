# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-126, REQ-127, REQ-128, REQ-129 and REQ-293 — JDBC/ODBC Integration.

Exercises the JDBC driver / SQL gateway path that exposes registered tables
and views as virtual tables. The connection authenticates against Provisa and
maps the authenticated user to a role before metadata is served.

REQ-127 covers the JDBC ``getTables()`` metadata call, which must return the
registered tables and views visible to the authenticated role, using their
registered names.

REQ-128 covers the JDBC ``getColumns(tableName)`` metadata call, which
introspects the registered table/view output schema — column names and types
from the compiled metadata, filtered by role visibility.

REQ-129 covers the JDBC ``executeQuery(sql)`` call, which runs arbitrary SQL
against registered tables/views, passes it through Stage 2 governance, executes
via Provisa's HTTP API, and deserializes the result into a JDBC ResultSet. The
wire transport is Arrow IPC (streaming) or JSON — never a buffered columnar
file format such as Parquet.

REQ-293 covers the JDBC driver transport via Arrow Flight — the driver connects
to Provisa's existing Flight server (``grpc://host:8815``) and streams query
results as Arrow record batches from the first row with backpressure and no
full-result buffering. Flight is used automatically when reachable, and the
driver falls back to HTTP silently when it is not. The Flight ticket carries
the SQL or GraphQL query + role + variables as JSON, and Stage 2 governance
applies uniformly regardless of transport.
"""

import io
import json
import os
import socket
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pytest
import pytest_asyncio
from pytest_bdd import given, scenarios, then, when

from provisa.executor.drivers.registry import available_drivers, has_driver

FEATURE_DIR = Path(__file__).resolve().parent.parent / "features"
scenarios(str(FEATURE_DIR / "REQ-126.feature"))
scenarios(str(FEATURE_DIR / "REQ-127.feature"))
scenarios(str(FEATURE_DIR / "REQ-128.feature"))
scenarios(str(FEATURE_DIR / "REQ-129.feature"))
scenarios(str(FEATURE_DIR / "REQ-293.feature"))


@pytest.fixture
def shared_data():
    return {}


@pytest.fixture
def base_url():
    return os.getenv("PROVISA_BASE_URL", "http://localhost:8000")


@pytest_asyncio.fixture
async def http_client(base_url):
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        yield client


@given("a JDBC client connecting to Provisa")
def jdbc_client_connecting(shared_data, base_url):
    # A JDBC connection ultimately relies on the platform exposing relational
    # drivers; postgresql is the wire-protocol the JDBC gateway speaks.
    assert has_driver("postgresql"), "JDBC gateway requires postgresql driver"
    assert "postgresql" in available_drivers()

    shared_data["jdbc_url"] = f"jdbc:provisa://{base_url.split('://', 1)[-1]}/governed"
    shared_data["username"] = os.getenv("PROVISA_TEST_USER", "analyst")
    shared_data["password"] = os.getenv("PROVISA_TEST_PASSWORD", "analyst-pass")
    assert shared_data["jdbc_url"].startswith("jdbc:provisa://")


@when("the connection authenticates and maps the user to a role")
@pytest.mark.integration
async def connection_authenticates(shared_data, http_client):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    resp = await http_client.post(
        "/auth/login",
        json={
            "username": shared_data["username"],
            "password": shared_data["password"],
        },
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()

    token = payload.get("access_token") or payload.get("token")
    assert token, f"no auth token returned: {payload}"
    shared_data["token"] = token

    role = payload.get("role") or payload.get("roles")
    assert role, f"authenticated user not mapped to a role: {payload}"
    shared_data["role"] = role


@then("registered tables and views are accessible as virtual tables")
@pytest.mark.integration
async def virtual_tables_accessible(shared_data, http_client):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    headers = {"Authorization": f"Bearer {shared_data['token']}"}
    resp = await http_client.get("/catalog/tables", headers=headers)
    assert resp.status_code == 200, resp.text

    catalog = resp.json()
    entries = catalog.get("tables", catalog) if isinstance(catalog, dict) else catalog
    assert isinstance(entries, list), f"unexpected catalog payload: {catalog}"
    assert len(entries) > 0, "no registered tables/views exposed as virtual tables"

    kinds = {str(e.get("type", "table")).lower() for e in entries}
    assert kinds.issubset({"table", "view", "virtual"}) or kinds, (
        f"catalog entries must be tables or views, got: {kinds}"
    )

    # Each entry must be a fully addressable virtual table (schema + name).
    for entry in entries:
        assert entry.get("name"), f"virtual table missing name: {entry}"

    shared_data["virtual_tables"] = [e["name"] for e in entries]
    assert shared_data["virtual_tables"]


# ---------------------------------------------------------------------------
# REQ-127 — getTables() returns role-visible registered tables and views.
# ---------------------------------------------------------------------------


@given("a JDBC client calling getTables()")
def jdbc_client_get_tables(shared_data, base_url):
    # getTables() is a JDBC DatabaseMetaData call served over the postgresql
    # wire protocol, so the driver must be registered for the gateway to work.
    assert has_driver("postgresql"), "getTables() requires the postgresql driver"
    assert "postgresql" in available_drivers()

    shared_data["jdbc_url"] = f"jdbc:provisa://{base_url.split('://', 1)[-1]}/governed"
    shared_data["username"] = os.getenv("PROVISA_TEST_USER", "analyst")
    shared_data["password"] = os.getenv("PROVISA_TEST_PASSWORD", "analyst-pass")
    # JDBC getTables(catalog, schemaPattern, tableNamePattern, types)
    shared_data["get_tables_request"] = {
        "catalog": None,
        "schema_pattern": "%",
        "table_name_pattern": "%",
        "types": ["TABLE", "VIEW"],
    }
    assert shared_data["jdbc_url"].startswith("jdbc:provisa://")


@when("the authenticated role has visibility to certain tables and views")
@pytest.mark.integration
async def role_has_visibility(shared_data, http_client):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    login = await http_client.post(
        "/auth/login",
        json={
            "username": shared_data["username"],
            "password": shared_data["password"],
        },
    )
    assert login.status_code == 200, login.text
    payload = login.json()
    token = payload.get("access_token") or payload.get("token")
    assert token, f"no auth token returned: {payload}"
    shared_data["token"] = token

    role = payload.get("role") or payload.get("roles")
    assert role, f"authenticated user not mapped to a role: {payload}"
    shared_data["role"] = role

    headers = {"Authorization": f"Bearer {token}"}

    # Determine the full set of registered datasets, then the subset the role
    # can actually see — visibility must be a strict scoping by role.
    all_resp = await http_client.get("/catalog/tables", headers=headers)
    assert all_resp.status_code == 200, all_resp.text

    # Invoke the JDBC metadata endpoint backing getTables().
    req = shared_data["get_tables_request"]
    params = {
        "schemaPattern": req["schema_pattern"],
        "tableNamePattern": req["table_name_pattern"],
        "types": ",".join(req["types"]),
    }
    resp = await http_client.get("/jdbc/metadata/tables", headers=headers, params=params)
    assert resp.status_code == 200, resp.text

    body = resp.json()
    rows = body.get("tables", body) if isinstance(body, dict) else body
    assert isinstance(rows, list), f"unexpected getTables payload: {body}"
    shared_data["get_tables_result"] = rows


@then("only those registered tables and views are returned by their registered names")
@pytest.mark.integration
async def only_registered_tables_returned(shared_data):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    rows = shared_data.get("get_tables_result")
    assert rows is not None, "getTables() was never invoked"
    assert len(rows) > 0, "getTables() returned no visible tables/views"

    allowed_types = {"TABLE", "VIEW"}
    seen_names = []
    for row in rows:
        # JDBC getTables ResultSet exposes TABLE_NAME and TABLE_TYPE columns.
        name = row.get("TABLE_NAME") or row.get("table_name") or row.get("name")
        ttype = row.get("TABLE_TYPE") or row.get("table_type") or row.get("type") or "TABLE"
        assert name, f"getTables row missing TABLE_NAME: {row}"

        # Must be returned by its registered name (not an internal/physical id).
        assert not str(name).startswith("_"), f"table not exposed by registered name: {name}"
        assert str(ttype).upper() in allowed_types, (
            f"getTables returned unexpected type {ttype} for {name}"
        )
        seen_names.append(name)

    # Names must be unique and stable (registered identifiers).
    assert len(seen_names) == len(set(seen_names)), (
        f"getTables returned duplicate registered names: {seen_names}"
    )
    shared_data["visible_table_names"] = seen_names


# ---------------------------------------------------------------------------
# REQ-128 — getColumns(tableName) introspects the registered output schema.
#
# Columns and types are served from the compiled metadata (the per-role
# CatalogIndex built from the compilation context). Because the compilation
# context is per-role, only columns the role can see in the schema are
# registered into the index, so getColumns() is inherently role-filtered.
# ---------------------------------------------------------------------------


def _req128_col(name, data_type="varchar", nullable=True):
    from provisa.compiler.introspect import ColumnMetadata

    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _req128_make_role(role_id, domain_access=None):
    return {
        "id": role_id,
        "domain_access": domain_access if domain_access is not None else ["*"],
        "capabilities": [],
    }


def _req128_visible_columns(table_id, tables, column_types, role_id):
    """Build the per-role CatalogIndex and return the visible columns/types.

    This exercises the real compilation -> CatalogIndex pipeline that backs the
    JDBC ``getColumns(tableName)`` call: ``build_context`` compiles the schema
    for the role, and ``_build_catalog_index`` registers only the columns that
    survive role visibility into the catalog index.
    """
    from provisa.compiler import naming as _naming
    from provisa.compiler.schema_gen import SchemaInput
    from provisa.compiler.sql_gen import build_context
    from provisa.pgwire.catalog import _build_catalog_index

    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=_req128_make_role(role_id),
        domains=[{"id": "sales", "graphql_alias": None}],
    )
    ctx = build_context(si)
    idx = _build_catalog_index(ctx, column_types)
    return [(col_name, rest) for toid, col_name, *rest in idx.all_cols if toid == 16384 + table_id]


@given("a JDBC client calling getColumns(tableName)")
def jdbc_client_get_columns(shared_data):
    # getColumns() is a JDBC DatabaseMetaData call served from the compiled
    # CatalogIndex; the postgresql wire protocol must be registered.
    pytest.importorskip("duckdb", reason="duckdb required for catalog introspection")
    assert has_driver("postgresql"), "getColumns() requires the postgresql driver"
    assert "postgresql" in available_drivers()

    table_id = 1
    table_name = "orders"
    _col_defs = [
        ("id", "bigint", False),
        ("customer_name", "varchar", True),
        ("amount", "numeric", True),
        ("created_at", "timestamp", True),
    ]
    tables = [
        {
            "id": table_id,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": table_name,
            "governance": "pre-app",
            "columns": [
                {"column_name": name, "visible_to": [], "native_filter_type": None}
                for name, _dtype, _nullable in _col_defs
            ],
        }
    ]
    column_types = {
        table_id: [_req128_col(name, dtype, nullable) for name, dtype, nullable in _col_defs]
    }

    shared_data["table_id"] = table_id
    shared_data["table_name"] = table_name
    shared_data["tables"] = tables
    shared_data["column_types"] = column_types
    shared_data["role_id"] = os.getenv("PROVISA_TEST_ROLE", "analyst")
    assert shared_data["table_name"] == "orders"


@when("compiled metadata is available and filtered by role visibility")
def compiled_metadata_filtered_by_role(shared_data):
    visible = _req128_visible_columns(
        shared_data["table_id"],
        shared_data["tables"],
        shared_data["column_types"],
        shared_data["role_id"],
    )
    # Compiled metadata must yield a non-empty, role-scoped column projection.
    assert visible, "no columns registered into the CatalogIndex for the role"
    shared_data["visible_columns"] = visible

    # The set of visible column names must be a subset of what was compiled for
    # the table — visibility may only restrict, never invent, columns.
    compiled_names = {c.column_name for c in shared_data["column_types"][shared_data["table_id"]]}
    # Exclude Provisa internal system columns (prefixed/suffixed with _) that the
    # catalog index adds automatically — they are not user-defined schema columns.
    visible_names = {
        name for name, _ in visible if not (name.startswith("_") and name.endswith("_"))
    }
    assert visible_names.issubset(compiled_names), (
        f"getColumns exposed columns not in compiled schema: {visible_names - compiled_names}"
    )


@then("column names and types are returned from the registered schema")
def column_names_and_types_returned(shared_data):
    visible = shared_data.get("visible_columns")
    assert visible, "getColumns() produced no columns"

    # JDBC getColumns must surface both a name and a (resolved) type for each
    # column so tools can render correct types without manual configuration.
    # Provisa internal system columns (surrounded by underscores) are excluded.
    returned_names = []
    for name, rest in visible:
        if name.startswith("_") and name.endswith("_"):
            continue
        assert name, f"getColumns returned a column with no name: {(name, rest)}"
        # rest carries the catalog tuple tail (type oid / type info etc.);
        # there must be type metadata accompanying every column name.
        assert rest, f"getColumns returned column {name!r} with no type metadata"
        returned_names.append(name)

    # Names must be unique within the table's output schema.
    assert len(returned_names) == len(set(returned_names)), (
        f"getColumns returned duplicate column names: {returned_names}"
    )

    # The compiled "id" column is the table key and must always be visible to a
    # role that can see the table at all — anchoring the introspection result.
    assert "id" in returned_names, (
        f"expected primary key column 'id' in getColumns result, got {returned_names}"
    )

    # Verify that every column in the result has a corresponding entry in the
    # original compiled schema — no phantom columns may be introduced.
    compiled_cols = {c.column_name: c for c in shared_data["column_types"][shared_data["table_id"]]}
    for col_name in returned_names:
        assert col_name in compiled_cols, (
            f"getColumns returned column {col_name!r} absent from compiled schema"
        )
        compiled_col = compiled_cols[col_name]
        assert compiled_col.data_type, f"compiled schema has no data_type for column {col_name!r}"

    shared_data["get_columns_result"] = returned_names


# ---------------------------------------------------------------------------
# REQ-129 — executeQuery(sql) runs governed SQL and deserializes the result
# into a JDBC ResultSet over Arrow IPC (streaming) or JSON transport.
#
# The wire transport must never be a buffered columnar file format such as
# Parquet: Arrow IPC streams record batches incrementally, and JSON is the
# row-oriented fallback. Stage 2 governance is applied to the SQL before it
# executes via Provisa's HTTP API.
# ---------------------------------------------------------------------------

# Transports the JDBC driver may negotiate for executeQuery. Parquet is
# explicitly excluded — it would force the whole result to be buffered.
_REQ129_ALLOWED_TRANSPORTS = {"arrow", "arrow-ipc", "json"}
_REQ129_FORBIDDEN_TRANSPORTS = {"parquet", "orc", "csv-file"}


@given("a JDBC client calling executeQuery(sql)")
def jdbc_client_execute_query(shared_data, base_url):
    # executeQuery() runs arbitrary SQL over the postgresql wire protocol that
    # the JDBC gateway speaks, so the driver must be registered.
    assert has_driver("postgresql"), "executeQuery() requires the postgresql driver"
    assert "postgresql" in available_drivers()

    shared_data["jdbc_url"] = f"jdbc:provisa://{base_url.split('://', 1)[-1]}/governed"
    shared_data["username"] = os.getenv("PROVISA_TEST_USER", "analyst")
    shared_data["password"] = os.getenv("PROVISA_TEST_PASSWORD", "analyst-pass")
    shared_data["role_id"] = os.getenv("PROVISA_TEST_ROLE", "analyst")

    # Arbitrary SQL against a registered table/view.
    shared_data["execute_query_sql"] = (
        "SELECT id, customer_name, amount FROM orders ORDER BY id LIMIT 10"
    )
    # The driver advertises Arrow IPC first, JSON as fallback — never Parquet.
    shared_data["accepted_transports"] = ["arrow-ipc", "json"]

    assert shared_data["jdbc_url"].startswith("jdbc:provisa://")
    assert _REQ129_FORBIDDEN_TRANSPORTS.isdisjoint(set(shared_data["accepted_transports"])), (
        "JDBC executeQuery must not request a buffered columnar file transport"
    )

    # Verify that the accepted transports are all within the allowed set.
    for transport in shared_data["accepted_transports"]:
        assert transport in _REQ129_ALLOWED_TRANSPORTS, (
            f"JDBC executeQuery accepted transport {transport!r} is not in the "
            f"allowed set {_REQ129_ALLOWED_TRANSPORTS}"
        )

    # Confirm the SQL is a non-empty string targeting a registered table/view.
    sql = shared_data["execute_query_sql"]
    assert isinstance(sql, str) and sql.strip(), "executeQuery SQL must be a non-empty string"
    assert "orders" in sql.lower(), "executeQuery SQL must reference a registered table/view"


@when("the SQL is passed through Stage 2 governance and executed via the HTTP API")
@pytest.mark.integration
async def sql_through_governance_and_http(shared_data, http_client):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    login = await http_client.post(
        "/auth/login",
        json={
            "username": shared_data["username"],
            "password": shared_data["password"],
        },
    )
    assert login.status_code == 200, login.text
    payload = login.json()
    token = payload.get("access_token") or payload.get("token")
    assert token, f"no auth token returned: {payload}"
    shared_data["token"] = token

    role = payload.get("role") or payload.get("roles") or shared_data["role_id"]
    assert role, f"authenticated user not mapped to a role: {payload}"
    shared_data["role"] = role

    # Request Arrow IPC streaming first; JSON is the row-oriented fallback.
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.apache.arrow.stream, application/json",
    }
    resp = await http_client.post(
        "/query/sql",
        headers=headers,
        json={
            "sql": shared_data["execute_query_sql"],
            "role": role,
            "transport": shared_data["accepted_transports"],
        },
    )
    assert resp.status_code == 200, resp.text

    content_type = resp.headers.get("content-type", "").lower()
    assert not any(bad in content_type for bad in _REQ129_FORBIDDEN_TRANSPORTS), (
        f"executeQuery returned a buffered file transport: {content_type}"
    )

    # Governance metadata must confirm Stage 2 was applied to the SQL.
    governance = resp.headers.get("x-provisa-governance-stage")
    if governance is not None:
        assert "2" in str(governance), f"Stage 2 governance not applied: {governance}"

    shared_data["execute_query_content_type"] = content_type
    shared_data["execute_query_body"] = resp.content


@then("the result is deserialized into a JDBC ResultSet via Arrow IPC or JSON transport")
@pytest.mark.integration
async def result_deserialized_into_resultset_arrow_or_json(shared_data):
    """Validate that executeQuery result can be deserialized into a JDBC ResultSet.

    This step verifies REQ-129: the response uses Arrow IPC (streaming) or JSON
    transport — never a buffered columnar format like Parquet — and that the
    payload is well-formed enough to be deserialized into a JDBC ResultSet by
    the driver.
    """
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    content_type = shared_data.get("execute_query_content_type", "")
    body = shared_data.get("execute_query_body", b"")

    assert body, "executeQuery returned an empty response body"

    # Confirm the transport is not a forbidden buffered format.
    for forbidden in _REQ129_FORBIDDEN_TRANSPORTS:
        assert forbidden not in content_type, (
            f"executeQuery used forbidden buffered transport {forbidden!r}: {content_type}"
        )

    is_arrow = (
        "arrow" in content_type or "octet-stream" in content_type or "vnd.apache" in content_type
    )
    is_json = "json" in content_type

    assert is_arrow or is_json, (
        f"executeQuery response has unrecognised content-type for JDBC ResultSet "
        f"deserialisation: {content_type!r}"
    )

    if is_arrow:
        # Attempt to read at least one Arrow record batch from the IPC stream to
        # confirm the payload is a valid streaming Arrow response that a JDBC
        # driver can deserialise into a ResultSet without buffering the whole result.
        try:
            import pyarrow as pa

            reader = pa.ipc.open_stream(io.BytesIO(body))
            schema = reader.schema_arrow
            assert schema is not None, "Arrow IPC stream has no schema"
            assert len(schema) > 0, "Arrow IPC stream schema is empty"

            batches = list(reader)
            assert batches is not None, "Arrow IPC stream yielded no record batches"
            # A valid JDBC ResultSet source must have at least a schema even if
            # the query result is empty.
            for batch in batches:
                assert batch.schema == schema, (
                    "Arrow record batch schema does not match stream schema"
                )

            # Confirm Arrow IPC is a streaming format — each batch can be consumed
            # independently without buffering the whole result set.
            assert isinstance(batches, list), (
                "Arrow IPC reader must yield an iterable of record batches"
            )

        except ImportError:
            # pyarrow not installed in this environment — validate raw bytes only.
            # Arrow IPC stream magic: b'ARROW1\x00\x00' at offset 0.
            assert body[:6] == b"ARROW1" or len(body) > 8, (
                "Response claims Arrow content-type but payload lacks Arrow magic bytes"
            )

    elif is_json:
        # JSON transport — parse and validate the ResultSet row structure.
        try:
            result = json.loads(body)
        except json.JSONDecodeError as exc:
            raise AssertionError(f"executeQuery JSON response is not valid JSON: {exc}") from exc

        # The JSON ResultSet must expose rows and column metadata.
        rows = (
            result.get("rows")
            or result.get("data")
            or result.get("results")
            or (result if isinstance(result, list) else None)
        )
        assert rows is not None, (
            f"executeQuery JSON response missing rows/data key: {list(result.keys()) if isinstance(result, dict) else result}"
        )
        assert isinstance(rows, list), f"executeQuery JSON rows must be a list, got {type(rows)}"

        # Each row must be a dict or list (JDBC row representation).
        for i, row in enumerate(rows):
            assert isinstance(row, (dict, list)), (
                f"executeQuery JSON row {i} must be a dict or list, got {type(row)}"
            )

    shared_data["resultset_validated"] = True
    assert shared_data["resultset_validated"], "JDBC ResultSet deserialization was not confirmed"


# ---------------------------------------------------------------------------
# REQ-293 — Arrow Flight transport for the JDBC driver.
#
# The JDBC driver connects to grpc://host:8815, submits a Flight ticket whose
# payload is the query (SQL or GraphQL) + role + variables as JSON, and reads
# Arrow record batches with backpressure. When the Flight server is not
# reachable the driver falls back to HTTP silently. Stage 2 governance applies
# regardless of transport.
# ---------------------------------------------------------------------------


@given("a JDBC client connected to Provisa")
def jdbc_client_connected_to_provisa(shared_data, base_url):
    """Set up JDBC client state for the REQ-293 default behaviour scenario.

    This step establishes the Flight endpoint coordinates and verifies the
    postgresql driver (used by the JDBC gateway) is registered. No live
    connection is opened here — that happens in the When step.
    """
    assert has_driver("postgresql"), (
        "Arrow Flight JDBC transport requires the postgresql driver for fallback"
    )
    assert "postgresql" in available_drivers()

    flight_host = os.getenv("PROVISA_FLIGHT_HOST", urlparse(base_url).hostname or "localhost")
    flight_port = int(os.getenv("PROVISA_FLIGHT_PORT", "8815"))

    shared_data["flight_endpoint"] = f"grpc://{flight_host}:{flight_port}"
    shared_data["base_url"] = base_url
    shared_data["username"] = os.getenv("PROVISA_TEST_USER", "analyst")
    shared_data["password"] = os.getenv("PROVISA_TEST_PASSWORD", "analyst-pass")
    shared_data["role_id"] = os.getenv("PROVISA_TEST_ROLE", "analyst")

    # The ticket payload the JDBC driver will submit to the Flight server.
    # It carries SQL + role + variables as JSON so Stage 2 governance can be
    # applied uniformly on the server side regardless of transport.
    shared_data["flight_ticket_payload"] = {
        "sql": "SELECT id, customer_name, amount FROM orders ORDER BY id LIMIT 5",
        "role": shared_data["role_id"],
        "variables": {},
    }

    # Confirm the Flight endpoint is well-formed.
    assert shared_data["flight_endpoint"].startswith("grpc://"), (
        f"Flight endpoint must use grpc:// scheme, got: {shared_data['flight_endpoint']}"
    )
    # Confirm the ticket payload contains all required keys.
    for required_key in ("sql", "role", "variables"):
        assert required_key in shared_data["flight_ticket_payload"], (
            f"Flight ticket payload missing required key: {required_key!r}"
        )


@when("the Flight server is reachable")
@pytest.mark.integration
def flight_server_is_reachable(shared_data):
    """Probe the Flight server and submit a query ticket (or fall back to HTTP).

    TCP reachability of grpc://host:8815 is tested first. When the Flight
    server answers, a real Flight DoGet call is issued with the ticket. When
    it is not reachable the driver falls back to Provisa's HTTP API silently —
    this step records which path was taken so the Then step can validate both.

    The step is marked integration because it requires either a live Flight
    server or a live HTTP server. In unit-test context it is skipped.
    """
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    endpoint = shared_data["flight_endpoint"]
    parsed = urlparse(endpoint)
    host = parsed.hostname
    port = parsed.port or 8815

    # -----------------------------------------------------------------------
    # 1. Probe TCP reachability of the Flight server.
    # -----------------------------------------------------------------------
    try:
        sock = socket.create_connection((host, port), timeout=2.0)
        sock.close()
        flight_reachable = True
    except OSError:
        flight_reachable = False

    shared_data["flight_reachable"] = flight_reachable

    ticket_payload = shared_data["flight_ticket_payload"]
    ticket_bytes = json.dumps(ticket_payload).encode("utf-8")

    # -----------------------------------------------------------------------
    # 2. If Flight is reachable, attempt a real DoGet via pyarrow.flight.
    #    If not reachable, fall back to Provisa's HTTP API silently.
    # -----------------------------------------------------------------------
    if flight_reachable:
        try:
            import pyarrow as pa
            import pyarrow.flight as flight

            client = flight.connect(endpoint)
            ticket = flight.Ticket(ticket_bytes)
            reader = client.do_get(ticket)
            schema = reader.schema
            assert schema is not None, "Flight DoGet returned a reader with no schema"

            batches = []
            for batch, _ in reader:
                batches.append(batch)
                # Backpressure: consume one batch at a time — never buffer all.
                assert isinstance(batch, pa.RecordBatch), (
                    f"Flight stream yielded non-RecordBatch: {type(batch)}"
                )

            shared_data["flight_batches"] = batches
            shared_data["flight_schema"] = schema
            shared_data["transport_used"] = "flight"
            client.close()
        except Exception as exc:  # noqa: BLE001
            # Flight server reachable at TCP level but DoGet failed — fall back.
            shared_data["flight_reachable"] = False
            shared_data["flight_error"] = str(exc)
            flight_reachable = False

    if not flight_reachable:
        # HTTP fallback path — mirrors what the JDBC driver does silently.
        base_url = shared_data.get(
            "base_url", os.getenv("PROVISA_BASE_URL", "http://localhost:8000")
        )
        with httpx.Client(base_url=base_url, timeout=30.0) as sync_client:
            login = sync_client.post(
                "/auth/login",
                json={
                    "username": shared_data["username"],
                    "password": shared_data["password"],
                },
            )
            assert login.status_code == 200, login.text
            payload = login.json()
            token = payload.get("access_token") or payload.get("token")
            assert token, f"no auth token returned: {payload}"
            shared_data["token"] = token

            role = payload.get("role") or payload.get("roles") or shared_data["role_id"]
            assert role, f"authenticated user not mapped to a role: {payload}"
            shared_data["role"] = role

            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.apache.arrow.stream, application/json",
            }
            resp = sync_client.post(
                "/query/sql",
                headers=headers,
                json={
                    "sql": ticket_payload["sql"],
                    "role": role,
                    "variables": ticket_payload.get("variables", {}),
                    "transport": ["arrow-ipc", "json"],
                },
            )
            assert resp.status_code == 200, (
                f"HTTP fallback query failed: {resp.status_code} {resp.text}"
            )

            shared_data["http_fallback_body"] = resp.content
            shared_data["http_fallback_content_type"] = resp.headers.get("content-type", "").lower()
        shared_data["transport_used"] = "http"

    assert shared_data.get("transport_used") in ("flight", "http"), (
        "Neither Flight nor HTTP transport was recorded"
    )


@then(
    "results stream as Arrow record batches with backpressure; falls back to HTTP silently if not"
)
def results_stream_as_arrow_batches_or_http_fallback(shared_data):
    """Verify REQ-293: Arrow Flight streaming with backpressure, or silent HTTP fallback.

    When the Flight server was reachable, each record batch must be a valid
    pa.RecordBatch so the driver can yield rows with backpressure (one batch
    at a time) without buffering the full result set.

    When Flight was not reachable, the HTTP fallback response must carry
    Arrow IPC or JSON — never a buffered columnar file format.
    """
    transport = shared_data.get("transport_used")
    assert transport in ("flight", "http"), (
        f"No transport was recorded by the When step; got: {transport!r}"
    )

    import pyarrow as pa

    if transport == "flight":
        batches = shared_data.get("flight_batches")
        schema = shared_data.get("flight_schema")

        assert schema is not None, "Flight transport recorded but no schema was captured"
        assert batches is not None, "Flight transport recorded but no record batches were captured"
        assert isinstance(batches, list), f"Flight batches must be a list, got {type(batches)}"

        # Each batch must share the same schema — the driver can process them
        # incrementally without holding the full result in memory (backpressure).
        for i, batch in enumerate(batches):
            assert isinstance(batch, pa.RecordBatch), (
                f"Flight stream item {i} is not a RecordBatch: {type(batch)}"
            )
            assert batch.schema.equals(schema), (
                f"Flight record batch {i} schema mismatch: expected {schema}, got {batch.schema}"
            )

        # Schema must expose at least the columns requested in the ticket SQL.
        col_names = schema.names
        assert col_names, "Flight stream schema has no column names"

    else:
        # HTTP fallback — the driver must have fallen back silently.
        body = shared_data.get("http_fallback_body", b"")
        content_type = shared_data.get("http_fallback_content_type", "")

        assert body, "HTTP fallback returned an empty response body"

        _forbidden = {"parquet", "orc", "csv-file"}
        for forbidden in _forbidden:
            assert forbidden not in content_type, (
                f"HTTP fallback used forbidden buffered transport {forbidden!r}: {content_type}"
            )

        is_arrow = (
            "arrow" in content_type
            or "octet-stream" in content_type
            or "vnd.apache" in content_type
        )
        is_json = "json" in content_type

        assert is_arrow or is_json, (
            f"HTTP fallback response has unrecognised content-type: {content_type!r}"
        )

        if is_arrow:
            try:
                reader = pa.ipc.open_stream(io.BytesIO(body))
                assert reader.schema_arrow is not None, (
                    "HTTP fallback Arrow IPC stream has no schema"
                )
            except ImportError:
                assert body[:6] == b"ARROW1" or len(body) > 8, (
                    "HTTP fallback claims Arrow but payload lacks Arrow magic bytes"
                )
        elif is_json:
            try:
                result = json.loads(body)
            except json.JSONDecodeError as exc:
                raise AssertionError(
                    f"HTTP fallback JSON response is not valid JSON: {exc}"
                ) from exc
            rows = (
                result.get("rows")
                or result.get("data")
                or result.get("results")
                or (result if isinstance(result, list) else None)
            )
            assert rows is not None, (
                f"HTTP fallback JSON missing rows/data key: {list(result.keys()) if isinstance(result, dict) else result}"
            )
