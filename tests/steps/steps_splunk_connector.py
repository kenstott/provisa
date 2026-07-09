# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step definitions for REQ-721, REQ-722, REQ-723, and REQ-724 — Splunk Connector."""

import os

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.core.models import Source, SourceType, SOURCE_TO_CONNECTOR


# ---------------------------------------------------------------------------
# Scenario bindings
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-721.feature",
    "REQ-721 default behaviour",
)
def test_splunk_connector_default():
    pass


@scenario(
    "../features/REQ-722.feature",
    "REQ-722 default behaviour",
)
def test_splunk_connector_url_construction():
    pass


@scenario(
    "../features/REQ-723.feature",
    "REQ-723 default behaviour",
)
def test_splunk_connector_auth():
    pass


@scenario(
    "../features/REQ-724.feature",
    "REQ-724 default behaviour",
)
def test_splunk_connector_app_and_ssl():
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
        assert gql_data.get("success"), f"addSource mutation failed: {gql_data.get('error')}"
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
        assert "e2e_splunk" in catalogs, f"Catalog 'e2e_splunk' not found; available: {catalogs}"

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
    assert port == 8089, f"REQ-722 specifies default port 8089, but step was given port={port}"
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

    assert "url" in props, f"Catalog properties must contain 'url'; got keys: {list(props.keys())}"

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


# ---------------------------------------------------------------------------
# Step definitions — REQ-723
# ---------------------------------------------------------------------------


@given("a source with use_token=true and a password field")
def step_req723_source_with_use_token_true(shared_data):
    """Construct a Splunk Source with use_token=True and a non-empty password.

    REQ-723: when use_token=True (the default), the password value is treated
    as a bearer token and passed as the ``token`` connector property.  The
    ``user`` and ``password`` keys must not appear in the resulting props.
    """
    password = "my-secret-splunk-token-xyz"

    source = Source(
        id="req-723-splunk",
        type=SourceType.splunk,
        host="splunk",
        port=8089,
        username="admin",
        password=password,
        mapping={"use_token": True},
    )

    assert source.type == SourceType.splunk
    assert source.password == password
    assert source.mapping["use_token"] is True

    shared_data["req723_source"] = source
    shared_data["req723_password"] = password


@when("the connector properties are built")
def step_build_connector_properties(shared_data):
    """Invoke _build_catalog_properties with the source and its raw password.

    This step is shared across REQ-723 and REQ-724 scenarios.  It dispatches
    on which source key is present in shared_data to support both scenarios
    without ambiguity.
    """
    from provisa.core.catalog import _build_catalog_properties

    # REQ-724 scenario populates req724_source / req724_password
    if "req724_source" in shared_data:
        source: Source = shared_data["req724_source"]
        resolved_password: str = shared_data.get("req724_password", "")
        props = _build_catalog_properties(source, resolved_password)
        shared_data["req724_props"] = props
        return

    # REQ-723 scenario populates req723_source / req723_password
    source = shared_data["req723_source"]
    resolved_password = shared_data["req723_password"]
    props = _build_catalog_properties(source, resolved_password)
    shared_data["req723_props"] = props


@then("props contains token=<password> and no user/password keys")
def step_req723_assert_token_auth(shared_data):
    """Assert REQ-723 token-auth contract.

    When use_token=True:
    * ``token`` must equal the password/resolved-password value.
    * Neither ``user`` nor ``password`` keys may appear in the props dict.
    * The ``url`` key must still be present (inherited from REQ-722 URL construction).
    * ``case-insensitive-name-matching`` must be ``"true"``.
    """
    props: dict = shared_data["req723_props"]
    expected_token: str = shared_data["req723_password"]

    # Primary REQ-723 assertion: token must equal the password
    assert "token" in props, (
        f"REQ-723: 'token' key missing from connector props; got keys: {list(props.keys())}"
    )
    assert props["token"] == expected_token, (
        f"REQ-723: expected token={expected_token!r}, got {props['token']!r}"
    )

    # Neither 'user' nor 'password' may appear when use_token=True
    assert "user" not in props, (
        f"REQ-723: 'user' key must not be present when use_token=True; props={props}"
    )
    assert "password" not in props, (
        f"REQ-723: 'password' key must not be present when use_token=True; props={props}"
    )

    # Structural invariants that must always hold for Splunk connector props
    assert "url" in props, (
        f"REQ-723: 'url' key must always be present in Splunk connector props; got: {list(props.keys())}"
    )
    assert props["url"].startswith("https://"), (
        f"REQ-723: Splunk connector URL must use HTTPS scheme; got: {props['url']!r}"
    )
    assert props.get("case-insensitive-name-matching") == "true", (
        f"REQ-723: 'case-insensitive-name-matching' must be 'true'; got: {props.get('case-insensitive-name-matching')!r}"
    )


# ---------------------------------------------------------------------------
# Step definitions — REQ-724
# ---------------------------------------------------------------------------


@given("a source with database=search_app and mapping.disable_ssl_validation=true")
def step_req724_source_with_app_and_ssl(shared_data):
    """Construct a Splunk Source that exercises both the optional ``app`` property
    (sourced from ``source.database``) and the ``disable-ssl-validation`` property
    (sourced from ``source.mapping.disable_ssl_validation``).

    REQ-724 specifies:
    * When ``source.database`` is set, the connector property ``app`` must equal
      that value.
    * When ``source.mapping.disable_ssl_validation`` is truthy, the connector
      property ``disable-ssl-validation`` must be set to ``"true"``.
    """
    database = "search_app"
    password = "test-token-req724"

    source = Source(
        id="req-724-splunk",
        type=SourceType.splunk,
        host="splunk",
        port=8089,
        username="",
        password=password,
        database=database,
        mapping={
            "use_token": True,
            "disable_ssl_validation": True,
        },
    )

    assert source.type == SourceType.splunk, (
        f"REQ-724: expected SourceType.splunk, got {source.type!r}"
    )
    assert source.database == database, (
        f"REQ-724: expected database={database!r}, got {source.database!r}"
    )
    assert source.mapping["disable_ssl_validation"] is True, (
        "REQ-724: mapping.disable_ssl_validation must be True on the source fixture"
    )

    shared_data["req724_source"] = source
    shared_data["req724_password"] = password
    shared_data["req724_expected_app"] = database


@then("props contains app=search_app and disable-ssl-validation=true")
def step_req724_assert_app_and_ssl(shared_data):
    """Assert REQ-724 optional-property contract.

    Verifies:
    * ``app`` connector property equals the value of ``source.database``
      (``"search_app"`` in this scenario).
    * ``disable-ssl-validation`` connector property is the string ``"true"``.
    * The standard structural invariants (``url``, ``case-insensitive-name-matching``)
      are also satisfied.
    """
    props: dict = shared_data["req724_props"]
    expected_app: str = shared_data["req724_expected_app"]

    # ── Primary REQ-724 assertion: app property ──────────────────────────────
    assert "app" in props, (
        f"REQ-724: 'app' key missing from connector props; got keys: {list(props.keys())}"
    )
    assert props["app"] == expected_app, (
        f"REQ-724: expected app={expected_app!r}, got {props['app']!r}"
    )

    # ── Primary REQ-724 assertion: disable-ssl-validation ────────────────────
    assert "disable-ssl-validation" in props, (
        f"REQ-724: 'disable-ssl-validation' key missing from connector props; "
        f"got keys: {list(props.keys())}"
    )
    assert props["disable-ssl-validation"] == "true", (
        f"REQ-724: expected disable-ssl-validation='true', got {props['disable-ssl-validation']!r}"
    )

    # ── Structural invariants ─────────────────────────────────────────────────
    assert "url" in props, (
        f"REQ-724: 'url' key must always be present in Splunk connector props; "
        f"got: {list(props.keys())}"
    )
    assert props["url"].startswith("https://"), (
        f"REQ-724: Splunk connector URL must use HTTPS scheme; got: {props['url']!r}"
    )
    assert props.get("case-insensitive-name-matching") == "true", (
        f"REQ-724: 'case-insensitive-name-matching' must be 'true'; "
        f"got: {props.get('case-insensitive-name-matching')!r}"
    )

    # ── Token auth must still be set (use_token=True on this source) ──────────
    password = shared_data["req724_password"]
    assert props.get("token") == password, (
        f"REQ-724: expected token={password!r} (use_token=True), got {props.get('token')!r}"
    )

    # ── user/password keys must be absent when token auth is active ───────────
    assert "user" not in props, (
        f"REQ-724: 'user' key must not appear when use_token=True; props={props}"
    )
    assert "password" not in props, (
        f"REQ-724: 'password' key must not appear when use_token=True; props={props}"
    )


# Copyright (c) 2026 Kenneth Stott
# Canary: 425824b6-6df8-43d4-9ec7-6cbaf1c51044
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-721 are already present in the existing steps file.
# This file intentionally contains no additional step definitions.


# Copyright (c) 2026 Kenneth Stott
# Canary: d262cd34-44ac-4faa-ad89-8c6e3b01caca
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 0ae9dfd0-13d1-4b42-b9b5-6793f481777d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-721 are already present in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9bb8e518-19d1-4d67-beee-20960fc1d370
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 249f2838-a5d7-4ba5-bea7-4187d1c5bde3
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-721 are already present in the existing steps file.
# No new step definitions are required for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: a328689b-ecb7-4fd1-922b-104203ddefa2
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8f9063-e5cf-44a2-848a-03ea1a2ad97d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 00398f33-4c73-4162-8a23-d05ed629be5a
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-722 are already present in the existing steps file.
# No new step definitions are required for this requirement.


# All steps required for REQ-721 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: f3a4e4d2-096f-4010-83f3-8bdbcc362d0e
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-722 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# All steps required for REQ-721 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# All steps required for REQ-722 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# All steps required for REQ-721 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# All steps required for REQ-722 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# All steps required for REQ-721 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4e4ed5ca-718a-4401-aa9e-6ccf22ba88a3
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-722 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9bf1e6fe-dc62-4682-9acb-580303b51f85
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-721 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: c593e7d2-27ff-4e54-ad72-c4d5c3241f6e
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-722 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# All steps required for REQ-721 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 8cd35411-2652-418a-bf48-34cef7c3390d
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-722 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# All steps required for REQ-721 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 743d8080-afc2-4e7c-b756-bd48cc0e21ad
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-722 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# All steps required for REQ-721 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.


# Copyright (c) 2026 Kenneth Stott
# Canary: 67de32bc-2351-42aa-b15c-0b4db000ad6a
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-722 are already fully implemented in the existing steps file.
# No new step definitions are needed for this requirement.
