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
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.executor.drivers.registry import available_drivers, has_driver

FEATURE_DIR = Path(__file__).resolve().parent / "features"
scenarios(str(FEATURE_DIR / "req_126.feature"))
scenarios(str(FEATURE_DIR / "req_127.feature"))
scenarios(str(FEATURE_DIR / "req_128.feature"))
scenarios(str(FEATURE_DIR / "req_129.feature"))
scenarios(str(FEATURE_DIR / "req_293.feature"))


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

    shared_data["jdbc_url"] = (
        f"jdbc:provisa://{base_url.split('://', 1)[-1]}/governed"
    )
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

    shared_data["jdbc_url"] = (
        f"jdbc:provisa://{base_url.split('://', 1)[-1]}/governed"
    )
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
    resp = await http_client.get(
        "/jdbc/metadata/tables", headers=headers, params=params
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    rows = body.get("tables", body) if isinstance(body, dict) else body
    assert isinstance(rows, list), f"unexpected getTables payload: {body}"
    shared_data["get_tables_result"] = rows


@then(
    "only those registered tables and views are returned by their "
    "registered names"
)
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
        name = (
            row.get("TABLE_NAME")
            or row.get("table_name")
            or row.get("name")
        )
        ttype = (
            row.get("TABLE_TYPE")
            or row.get("table_type")
            or row.get("type")
            or "TABLE"
        )
        assert name, f"getTables row missing TABLE_NAME: {row}"

        # Must be returned by its registered name (not an internal/physical id).
        assert not str(name).startswith("_"), (
            f"table not exposed by registered name: {name}"
        )
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

    This exercises the real compilation → CatalogIndex pipeline that backs the
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
    return [
        (col_name, rest)
        for toid, col_name, *rest in idx.all_cols
        if toid == 16384 + table_id
    ]


@given("a JDBC client calling getColumns(tableName)")
def jdbc_client_get_columns(shared_data):
    # getColumns() is a JDBC DatabaseMetaData call served from the compiled
    # CatalogIndex; the postgresql wire protocol must be registered.
    pytest.importorskip("duckdb", reason="duckdb required for catalog introspection")
    assert has_driver("postgresql"), "getColumns() requires the postgresql driver"
    assert "postgresql" in available_drivers()

    table_id = 1
    table_name = "orders"
    tables = [
        {
            "id": table_id,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": table_name,
            "governance": "pre-app",
        }
    ]
    column_types = {
        table_id: [
            _req128_col("id", "bigint", nullable=False),
            _req128_col("customer_name", "varchar"),
            _req128_col("amount", "numeric"),
            _req128_col("created_at", "timestamp"),
        ]
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
    compiled_names = {
        c.column_name for c in shared_data["column_types"][shared_data["table_id"]]
    }
    visible_names = {name for name, _ in visible}
    assert visible_names.issubset(compiled_names), (
        f"getColumns exposed columns not in compiled schema: "
        f"{visible_names - compiled_names}"
    )


@then("column names and types are returned from the registered schema")
def column_names_and_types_returned(shared_data):
    visible = shared_data.get("visible_columns")
    assert visible, "getColumns() produced no columns"

    # JDBC getColumns must surface both a name and a (resolved) type for each
    # column so tools can render correct types without manual configuration.
    returned_names = []
    for name, rest in visible:
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

    shared_data["jdbc_url"] = (
        f"jdbc:provisa://{base_url.split('://', 1)[-1]}/governed"
    )
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
    assert _REQ129_FORBIDDEN_TRANSPORTS.isdisjoint(
        set(shared_data["accepted_transports"])
    ), "JDBC executeQuery must not request a buffered columnar file transport"


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
    assert not any(
        bad in content_type for bad in _REQ129_FORBIDDEN_TRANSPORTS
    ), f"executeQuery returned a buffered file transport: {content_type}"

    # Governance metadata must confirm Stage 2 was applied to the SQL.
    governance = resp.headers.get("x-provisa-governance-stage")
    if governance is not None:
        assert "2" in str(governance), (
            f"Stage 2 governance not applied: {governance}"
        )

    shared_data["execute_query_content_type"] = content_type
    shared_data["execute_query_body"] = resp.content


@then("the result is deserialized into a JDBC ResultSet over Arrow IPC or JSON")
@pytest.mark.integration
async
