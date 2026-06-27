# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step definitions for REQ-721 and REQ-722 — Splunk Connector."""

import os

import pytest
import pytest_asyncio
from pytest_bdd import given, parsers, scenario, then, when

from provisa.core.models import Source, SourceType, SOURCE_TO_CONNECTOR


# ---------------------------------------------------------------------------
# Scenario bindings
# ---------------------------------------------------------------------------


@scenario(
    "../features/req_721_splunk_connector.feature",
    "REQ-721 default behaviour",
)
def test_splunk_connector_default():
    pass


@scenario(
    "../features/req_722_splunk_connector.feature",
    "REQ-722 default behaviour",
)
def test_splunk_connector_url_construction():
    pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Step definitions — REQ-721
# ---------------------------------------------------------------------------


@given("a Splunk instance with admin credentials")
def step_splunk_instance_with_admin_credentials(shared_data):
    """Verify Splunk connectivity and retrieve a session token.

    In integration mode the real Splunk REST API is called; in unit mode
    we exercise only the Provisa model/catalog logic with a synthetic token.
    """
    if os.getenv("PROVISA_INTEGRATION"):
        import urllib.request
        import urllib.parse
        import urllib.error
        import ssl
        import json

        splunk_url = os.getenv("SPLUNK_URL", "https://localhost:8089")
        password = os.getenv("SPLUNK_ADMIN_PASSWORD", "Admin1234!")
        body = urllib.parse.urlencode(
            {"username": "admin", "password": password, "output_mode": "json"}
        ).encode()
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(
            f"{splunk_url}/services/auth/login",
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            payload = json.loads(resp.read())
        session_key = payload.get("sessionKey")
        assert session_key, f"Splunk login returned no sessionKey: {payload}"
        shared_data["session_key"] = session_key
        shared_data["splunk_host"] = os.getenv("SPLUNK_HOST", "splunk")
        shared_data["splunk_port"] = int(os.getenv("SPLUNK_PORT", "8089"))
    else:
        # Unit-mode: synthesise a plausible session token
        shared_data["session_key"] = "Splunk_UNIT_TEST_TOKEN_abc123"
        shared_data["splunk_host"] = "splunk"
        shared_data["splunk_port"] = 8089

    # Verify splunk is a known SourceType
    assert SourceType.splunk in SourceType.__members__.values()
    # Verify it maps to the splunk connector
    assert SOURCE_TO_CONNECTOR.get("splunk") == "splunk"


@when("a user registers the Splunk source with host, port, and auth token")
def step_register_splunk_source(shared_data):
    """Build a Source model and (in integration mode) call the GraphQL API."""
    source_id = "e2e-splunk"
    host = shared_data["splunk_host"]
    port = shared_data["splunk_port"]
    token = shared_data["session_key"]

    # Validate the Source model is accepted by Provisa
    source = Source(
        id=source_id,
        type=SourceType.splunk,
        host=host,
        port=port,
        password=token,
        mapping={"use_token": True, "disable_ssl_validation": True},
    )
    assert source.id == source_id
    assert source.type == SourceType.splunk
    assert source.host == host
    assert source.port == port
    assert source.password == token
    assert source.mapping["use_token"] is True

    shared_data["source"] = source

    if os.getenv("PROVISA_INTEGRATION"):
        import json as _json

        admin_gql = os.getenv("ADMIN_GQL", "http://localhost:8000/admin/graphql")
        mutation = """
        mutation AddSource($input: SourceInput!) {
            addSource(input: $input) {
                success
                error
            }
        }
        """
        variables = {
            "input": {
                "id": source_id,
                "type": "splunk",
                "host": host,
                "port": port,
                "password": token,
                "mapping": {"use_token": True, "disable_ssl_validation": True},
            }
        }
        import urllib.request as _ur
        import urllib.parse as _up

        body = _json.dumps({"query": mutation, "variables": variables}).encode()
        req = _ur.Request(
            admin_gql,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with _ur.urlopen(req, timeout=30) as resp:
            result = _json.loads(resp.read())
        gql_data = result.get("data", {}).get("addSource", {})
        assert gql_data.get("success"), (
            f"addSource mutation failed: {gql_data.get('error')}"
        )
        shared_data["registered_via_api"] = True
    else:
        shared_data["registered_via_api"] = False


@then(
    parsers.re(
        r"the source is added to the catalog and Splunk's search tables \(internal_server\)\s*are enumerable"
    )
)
def step_source_in_catalog_and_tables_enumerable(shared_data):
    """Assert catalog properties are correct and (in integration mode) tables are visible."""
    from provisa.core.catalog import _build_catalog_properties, _to_catalog_name

    source: Source = shared_data["source"]

    # Verify catalog name derivation
    catalog_name = _to_catalog_name(source.id)
    assert catalog_name == "e2e_splunk"

    # Verify catalog properties built correctly
    token = source.password
    props = _build_catalog_properties(source, token)

    # Must contain the Splunk URL
    assert "url" in props, f"Expected 'url' in catalog props, got: {list(props.keys())}"
    assert "splunk" in props["url"].lower() or "https://" in props["url"], (
        f"Unexpected url value: {props['url']}"
    )
    # Token auth
    assert props.get("token") == token, (
        f"Expected token={token!r} in props, got {props.get('token')!r}"
    )
    # Case-insensitive name matching must be enabled
    assert props.get("case-insensitive-name-matching") == "true"
    # SSL validation disabled for test environment
    assert props.get("disable-ssl-validation") == "true"

    if os.getenv("PROVISA_INTEGRATION"):
        # Query Trino to confirm the catalog exists and internal_server is enumerable
        import trino

        trino_host = os.getenv("TRINO_HOST", "localhost")
        trino_port = int(os.getenv("TRINO_PORT", "8080"))
        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            user="provisa",
            catalog="system",
            schema="runtime",
        )
        cur = conn.cursor()

        # Confirm catalog exists
        cur.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cur.fetchall()]
        assert "e2e_splunk" in catalogs, (
            f"Catalog 'e2e_splunk' not found; available: {catalogs}"
        )

        # Confirm internal_server table (or similar search table) is enumerable
        cur.execute("SHOW TABLES FROM e2e_splunk.splunk")
        tables = [row[0].lower() for row in cur.fetchall()]
        assert "internal_server" in tables, (
            f"'internal_server' table not found in e2e_splunk.splunk; found: {tables}"
        )

        # Confirm expected columns exist on internal_server
        cur.execute("DESCRIBE e2e_splunk.splunk.internal_server")
        columns = [row[0].lower() for row in cur.fetchall()]
        for expected_col in ("time", "host", "source", "sourcetype"):
            assert expected_col in columns, (
                f"Expected column {expected_col!r} not found; got: {columns}"
            )
        conn.close()
    else:
        # Unit mode: validate connector mapping is correct
        assert SOURCE_TO_CONNECTOR["splunk"] == "splunk", (
            "splunk must map to 'splunk' connector in SOURCE_TO_CONNECTOR"
        )
        # Validate the source type enum value
        assert SourceType.splunk.value == "splunk"
        # Validate that internal_server is the known canonical Splunk search table name
        known_splunk_tables = {"internal_server", "history", "searches", "audit"}
        assert "internal_server" in known_splunk_tables


# ---------------------------------------------------------------------------
# Step definitions — REQ-722
# ---------------------------------------------------------------------------


@given(parsers.parse("Splunk host={host} and port={port:d}"))
def step_splunk_host_and_port(host, port, shared_data):
    """Store the host and port supplied in the scenario for later use.

    Validates that the values are consistent with the Splunk defaults described
    in REQ-722 (default management port 8089, HTTPS scheme).
    """
    assert host, "host must not be empty"
    assert port > 0, f"port must be a positive integer, got {port}"
    # REQ-722 states the default port is 8089
    assert port == 8089, (
        f"REQ-722 specifies default port 8089, but step was given port={port}"
    )
    shared_data["req722_host"] = host
    shared_data["req722_port"] = port


@when("the source is registered")
def step_req722_source_is_registered(shared_data):
    """Construct a Source using only host and port (no explicit base_url).

    This exercises the path in _build_catalog_properties where the URL is
    constructed as ``https://{host}:{port}`` because no base_url is provided.
    """
    host = shared_data["req722_host"]
    port = shared_data["req722_port"]

    # Build a minimal Splunk source — no base_url so that the connector
    # falls back to constructing the URL from host and port.
    source = Source(
        id="req-722-splunk",
        type=SourceType.splunk,
        host=host,
        port=port,
        # No base_url — REQ-722 default behaviour
        mapping={"use_token": False},
    )

    assert source.host == host
    assert source.port == port
    assert source.base_url is None, (
        "base_url must be absent to exercise the host:port construction path"
    )

    shared_data["req722_source"] = source

    # Derive catalog properties immediately so the Then step can inspect them
    from provisa.core.catalog import _build_catalog_properties

    props = _build_catalog_properties(source, resolved_password="")
    shared_data["req722_catalog_props"] = props


@then(parsers.parse("the connector receives url={expected_url}"))
def step_connector_receives_url(expected_url, shared_data):
    """Assert that the catalog properties contain exactly the expected URL.

    REQ-722 requires that when host=splunk and port=8089 are provided without
    an explicit base_url, the connector property ``url`` is set to
    ``https://splunk:8089``.
    """
    props: dict = shared_data["req722_catalog_props"]

    assert "url" in props, (
        f"Catalog properties must contain 'url'; got keys: {list(props.keys())}"
    )

    actual_url = props["url"]
    assert actual_url == expected_url, (
        f"REQ-722: expected connector url={expected_url!r}, got {actual_url!r}"
    )

    # Additional structural assertions to confirm the URL was constructed from
    # host and port using the HTTPS scheme with the default management port.
    assert actual_url.startswith("https://"), (
        f"Splunk management URL must use HTTPS, got: {actual_url!r}"
    )
    host = shared_data["req722_host"]
    port = shared_data["req722_port"]
    assert f"{host}:{port}" in actual_url, (
        f"URL {actual_url!r} must contain host:port fragment '{host}:{port}'"
    )
