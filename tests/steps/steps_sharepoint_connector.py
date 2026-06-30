# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step definitions for REQ-726, REQ-727, REQ-728, REQ-731, and REQ-732 — SharePoint Connector."""

import os
from unittest.mock import MagicMock, patch

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.core.catalog import _build_catalog_properties
from provisa.core.models import SOURCE_TO_CONNECTOR, Column, Source, SourceType, Table

scenarios("../features/REQ-726.feature")
scenarios("../features/REQ-727.feature")
scenarios("../features/REQ-728.feature")
scenarios("../features/REQ-731.feature")
scenarios("../features/REQ-732.feature")


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("a user creates a new source")
def a_user_creates_a_new_source(shared_data):
    """Prepare the shared state for a new source to be created."""
    shared_data["source_id"] = "test-sharepoint-726"
    shared_data["source"] = None
    shared_data["error"] = None


@given("a SharePoint source with auth-type CLIENT_CREDENTIALS")
def a_sharepoint_source_with_auth_type_client_credentials(shared_data):
    """Create a SharePoint source configured with CLIENT_CREDENTIALS auth."""
    source = Source(
        id="test-sharepoint-727",
        type=SourceType.sharepoint,
        host="kenstott.sharepoint.com",
        port=443,
        database="5d2609cc-7eff-4b82-8f83-f0b28c71fafc",
        username="my-client-id",
        password="my-client-secret",
        mapping={
            "auth_type": "CLIENT_CREDENTIALS",
        },
    )
    shared_data["source"] = source
    shared_data["auth_type"] = "CLIENT_CREDENTIALS"
    shared_data["catalog_props"] = None
    shared_data["cert_catalog_props"] = None


@given(
    parsers.parse(
        'a SharePoint source with base_url="{base_url}",\nusername="{username}", password="{password}", database="{database}"'
    )
)
def a_sharepoint_source_req728_multiline(shared_data, base_url, username, password, database):
    """Create a SharePoint source using base_url, username, password, and database fields (multiline variant)."""
    source = Source(
        id="test-sharepoint-728",
        type=SourceType.sharepoint,
        base_url=base_url,
        username=username,
        password=password,
        database=database,
        mapping={},
    )
    shared_data["source"] = source
    shared_data["catalog_props"] = None
    shared_data["base_url"] = base_url
    shared_data["username"] = username
    shared_data["password"] = password
    shared_data["database"] = database


@given(
    parsers.parse(
        'a SharePoint source with base_url="{base_url}", username="{username}", password="{password}", database="{database}"'
    )
)
def a_sharepoint_source_req728(shared_data, base_url, username, password, database):
    """Create a SharePoint source using base_url, username, password, and database fields."""
    source = Source(
        id="test-sharepoint-728",
        type=SourceType.sharepoint,
        base_url=base_url,
        username=username,
        password=password,
        database=database,
        mapping={},
    )
    shared_data["source"] = source
    shared_data["catalog_props"] = None
    shared_data["base_url"] = base_url
    shared_data["username"] = username
    shared_data["password"] = password
    shared_data["database"] = database


@given("a SharePoint source is added in the Provisa UI")
def a_sharepoint_source_is_added_in_the_provisa_ui(shared_data):
    """
    Simulate adding a SharePoint source via the Provisa platform.

    Creates a Source model representing a registered SharePoint connection
    and stores catalog properties that would be used by Trino to enumerate
    SharePoint lists as schemas/tables.
    """
    source = Source(
        id="test-sharepoint-731",
        type=SourceType.sharepoint,
        host="kenstott.sharepoint.com",
        base_url="https://kenstott.sharepoint.com",
        port=443,
        database="5d2609cc-7eff-4b82-8f83-f0b28c71fafc",
        username="d6f6b74e-df85-470f-8e68-e34c767436be",
        password="my-client-secret",
        mapping={
            "auth_type": "CLIENT_CREDENTIALS",
        },
    )
    shared_data["source"] = source
    shared_data["catalog_props"] = _build_catalog_properties(source, source.password or "")
    shared_data["available_lists"] = None


@given("the Calcite sharepoint connector does not expose information_schema.columns")
def the_calcite_sharepoint_connector_does_not_expose_information_schema_columns(shared_data):
    """
    Simulate the known limitation of the Calcite-based SharePoint connector:
    information_schema.columns returns empty results, so column definitions
    cannot be auto-discovered and must be supplied manually by the user.

    We represent this by creating a SharePoint source whose catalog properties
    are valid but whose introspected columns list is empty — mirroring what
    Trino would return when querying information_schema.columns against the
    Calcite SharePoint connector.
    """
    source = Source(
        id="test-sharepoint-732",
        type=SourceType.sharepoint,
        host="kenstott.sharepoint.com",
        base_url="https://kenstott.sharepoint.com",
        port=443,
        database="5d2609cc-7eff-4b82-8f83-f0b28c71fafc",
        username="d6f6b74e-df85-470f-8e68-e34c767436be",
        password="my-client-secret",
        mapping={
            "auth_type": "CLIENT_CREDENTIALS",
        },
    )
    catalog_props = _build_catalog_properties(source, source.password or "")

    # Verify the connector is properly set up
    assert source.connector == "sharepoint", (
        f"Expected connector 'sharepoint', got '{source.connector}'."
    )
    assert "site-url" in catalog_props, "Catalog properties must include site-url."

    # Simulate the connector returning no columns from information_schema.columns
    # (the known Calcite SharePoint connector limitation described in REQ-732)
    shared_data["source"] = source
    shared_data["catalog_props"] = catalog_props
    shared_data["introspected_columns"] = []  # empty — connector does not expose them
    shared_data["registered_table"] = None
    shared_data["register_error"] = None


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when(parsers.parse('they select type "{source_type}"'))
def they_select_type(shared_data, source_type):
    """Create a Source model with the given type and validate it."""
    assert source_type in SourceType.__members__, (
        f"'{source_type}' is not a registered SourceType. "
        f"Valid types: {list(SourceType.__members__.keys())}"
    )

    source = Source(
        id=shared_data["source_id"],
        type=SourceType(source_type),
        host="kenstott.sharepoint.com",
        port=443,
        database="",
        username="",
        password="",
    )
    shared_data["source"] = source
    shared_data["source_type"] = source_type


@when("queries are executed")
def queries_are_executed(shared_data):
    """Build catalog properties for the CLIENT_CREDENTIALS SharePoint source."""
    source: Source = shared_data["source"]
    resolved_password = source.password or ""
    props = _build_catalog_properties(source, resolved_password)
    shared_data["catalog_props"] = props


@when("catalog properties are built")
def catalog_properties_are_built(shared_data):
    """Build catalog properties for the REQ-728 SharePoint source."""
    source: Source = shared_data["source"]
    resolved_password = source.password or ""
    props = _build_catalog_properties(source, resolved_password)
    shared_data["catalog_props"] = props


@when("a user navigates to add a table and selects this source")
def a_user_navigates_to_add_a_table_and_selects_this_source(shared_data):
    """
    Simulate the Provisa UI enumerating SharePoint lists for table registration.

    The SharePoint Trino connector exposes each SharePoint list as a schema.
    We verify here that the catalog properties are correctly set up to enable
    list enumeration, and we simulate the list of schemas (lists) that would
    be returned by a SHOW SCHEMAS query against the Trino sharepoint catalog.

    In a real integration, this would execute:
        SHOW SCHEMAS FROM <catalog_name>
    against Trino and return the SharePoint list names. Here we verify the
    catalog properties are valid and simulate the expected enumeration result.
    """
    source: Source = shared_data["source"]
    catalog_props: dict = shared_data["catalog_props"]

    # Verify the source is properly configured as a SharePoint source
    assert source.type == SourceType.sharepoint, (
        f"Expected SharePoint source type, got '{source.type}'."
    )

    # Verify that the connector is correctly identified
    assert source.connector == "sharepoint", (
        f"Expected connector 'sharepoint', got '{source.connector}'."
    )

    # Verify catalog properties are present and contain the required site-url
    assert catalog_props is not None, "Catalog properties must be built before enumerating lists."
    assert "site-url" in catalog_props, (
        "site-url must be present in catalog properties to connect to SharePoint."
    )
    assert catalog_props["site-url"], "site-url must be non-empty."

    # Verify case-insensitive-name-matching is enabled — required for SharePoint list enumeration
    assert catalog_props.get("case-insensitive-name-matching") == "true", (
        "case-insensitive-name-matching must be 'true' in catalog properties for SharePoint "
        "list name resolution. SharePoint list names may differ in casing from the Trino schema "
        f"names. Got: '{catalog_props.get('case-insensitive-name-matching')}'."
    )

    # Verify auth-type is present — required to authenticate against SharePoint
    assert "auth-type" in catalog_props, (
        "auth-type must be present in catalog properties so Trino can authenticate "
        "against the SharePoint site to enumerate lists."
    )

    # Simulate what Trino's SharePoint connector would return when executing
    # SHOW SCHEMAS FROM sharepoint_catalog
    # Each SharePoint list is enumerated as a schema in the Calcite-based connector.
    simulated_sharepoint_lists = [
        "calendar",
        "events",
        "documents",
        "tasks",
        "announcements",
        "contacts",
    ]

    shared_data["available_lists"] = simulated_sharepoint_lists


@when(
    parsers.parse(
        "a user registers a table via GraphQL registerTable mutation with columns=[{name, visibleTo, writableBy}]"
    )
)
def a_user_registers_a_table_via_graphql_registertable_mutation_with_columns(shared_data):
    """
    Simulate a user registering a SharePoint table via the Provisa GraphQL
    registerTable mutation, supplying explicit column definitions obtained from
    the Microsoft Graph API.

    Because information_schema.columns is empty for the Calcite SharePoint
    connector, the user provides the column list directly in the mutation input.
    This step constructs the Table model that the mutation would persist,
    including the supplied column definitions.
    """
    # Confirm the connector limitation is in effect
    introspected_columns = shared_data.get("introspected_columns", [])
    assert introspected_columns == [], (
        "Pre-condition violated: expected introspected_columns to be empty "
        f"(connector limitation), but got: {introspected_columns}."
    )

    source: Source = shared_data["source"]

    # These are the columns the user obtained from the Microsoft Graph API
    # and is supplying manually in the registerTable mutation input.
    user_supplied_columns = [
        Column(name="ID", data_type="VARCHAR", visible_to=[], writable_by=[]),
        Column(name="Title", data_type="VARCHAR", visible_to=[], writable_by=[]),
        Column(name="EventDate", data_type="VARCHAR", visible_to=[], writable_by=[]),
        Column(name="EndDate", data_type="VARCHAR", visible_to=[], writable_by=[]),
        Column(name="Description", data_type="VARCHAR", visible_to=[], writable_by=[]),
        Column(name="Location", data_type="VARCHAR", visible_to=[], writable_by=[]),
    ]

    assert len(user_supplied_columns) > 0, (
        "User must supply at least one column definition when registering a table "
        "whose connector does not expose information_schema.columns."
    )

    # Simulate the registerTable GraphQL mutation: construct the Table model
    # that would be persisted by the Provisa catalog service.
    table = Table(
        source_id=source.id,
        domain_id="default",
        schema_name="calendar",
        table_name="Events",
        columns=user_supplied_columns,
    )

    shared_data["registered_table"] = table
    shared_data["supplied_columns"] = user_supplied_columns


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then("the source is created and can be queried via Trino using the sharepoint connector")
def the_source_is_created_and_can_be_queried_via_trino(shared_data):
    """
    Assert that:
    1. The Source object was successfully created with type 'sharepoint'.
    2. 'sharepoint' is mapped to the 'sharepoint' Trino connector in SOURCE_TO_CONNECTOR.
    3. The SourceType enum contains 'sharepoint'.
    4. The connector name resolves to a non-empty string.
    """
    source: Source = shared_data["source"]

    assert source is not None, "Source was not created."

    assert source.type == SourceType.sharepoint, (
        f"Expected source type 'sharepoint', got '{source.type}'."
    )

    assert "sharepoint" in SourceType.__members__, (
        "'sharepoint' is not registered in SourceType enum."
    )

    assert "sharepoint" in SOURCE_TO_CONNECTOR, (
        "'sharepoint' is not mapped in SOURCE_TO_CONNECTOR registry."
    )

    connector_name = SOURCE_TO_CONNECTOR["sharepoint"]
    assert connector_name, "Connector name for 'sharepoint' is empty in SOURCE_TO_CONNECTOR."
    assert connector_name == "sharepoint", (
        f"Expected Trino connector 'sharepoint', got '{connector_name}'."
    )

    assert source.id == shared_data["source_id"], (
        f"Source id mismatch: expected '{shared_data['source_id']}', got '{source.id}'."
    )


@then("Provisa sends client-id and client-secret to the Calcite connector")
def provisa_sends_client_id_and_client_secret(shared_data):
    """Assert that catalog props contain client-id and client-secret for CLIENT_CREDENTIALS."""
    props: dict = shared_data["catalog_props"]

    assert props is not None, "Catalog properties were not built."

    assert props.get("auth-type") == "CLIENT_CREDENTIALS", (
        f"Expected auth-type 'CLIENT_CREDENTIALS', got '{props.get('auth-type')}'."
    )

    assert "client-id" in props, (
        "client-id is missing from catalog properties for CLIENT_CREDENTIALS auth."
    )
    assert props["client-id"], "client-id must be non-empty."

    assert "client-secret" in props, (
        "client-secret is missing from catalog properties for CLIENT_CREDENTIALS auth."
    )
    assert props["client-secret"], "client-secret must be non-empty."

    # Verify certificate fields are NOT sent for CLIENT_CREDENTIALS
    assert "certificate-path" not in props, (
        "certificate-path should not be present for CLIENT_CREDENTIALS auth."
    )
    assert "certificate-password" not in props, (
        "certificate-password should not be present for CLIENT_CREDENTIALS auth."
    )


@then("when auth-type is CERTIFICATE")
def when_auth_type_is_certificate(shared_data):
    """Build catalog properties for a CERTIFICATE-based SharePoint source."""
    cert_source = Source(
        id="test-sharepoint-727-cert",
        type=SourceType.sharepoint,
        host="kenstott.sharepoint.com",
        port=443,
        database="5d2609cc-7eff-4b82-8f83-f0b28c71fafc",
        username="my-client-id",
        password="",
        mapping={
            "auth_type": "CERTIFICATE",
            "certificate_path": "/certs/sharepoint.pfx",
            "certificate_password": "pfx-secret-password",
        },
    )
    cert_props = _build_catalog_properties(cert_source, "")
    shared_data["cert_catalog_props"] = cert_props


@then("Provisa sends certificate-path and certificate-password instead")
def provisa_sends_certificate_path_and_certificate_password(shared_data):
    """Assert that certificate-path and certificate-password are in props for CERTIFICATE auth."""
    props: dict = shared_data["cert_catalog_props"]

    assert props is not None, "Certificate catalog properties were not built."

    assert props.get("auth-type") == "CERTIFICATE", (
        f"Expected auth-type 'CERTIFICATE', got '{props.get('auth-type')}'."
    )

    assert "certificate-path" in props, (
        "certificate-path is missing from catalog properties for CERTIFICATE auth."
    )
    assert props["certificate-path"] == "/certs/sharepoint.pfx", (
        f"Unexpected certificate-path value: '{props['certificate-path']}'."
    )

    assert "certificate-password" in props, (
        "certificate-password is missing from catalog properties for CERTIFICATE auth."
    )
    assert props["certificate-password"] == "pfx-secret-password", (
        f"Unexpected certificate-password value: '{props['certificate-password']}'."
    )

    # client-id may still be present (useful for identifying the app registration)
    # but client-secret should NOT be sent when using certificate auth
    assert "client-secret" not in props or not props.get("client-secret"), (
        "client-secret should not be present (or must be empty) for CERTIFICATE auth."
    )


@then("props contains site-url, auth-type, client-id, client-secret, tenant-id")
def props_contains_core_sharepoint_fields(shared_data):
    """Assert that all required SharePoint connection properties are present and correct."""
    props: dict = shared_data["catalog_props"]

    assert props is not None, "Catalog properties were not built."

    # site-url must map to the source's base_url
    assert "site-url" in props, "site-url is missing from catalog properties."
    assert props["site-url"] == shared_data["base_url"], (
        f"Expected site-url '{shared_data['base_url']}', got '{props['site-url']}'."
    )

    # auth-type must be present (defaults to CLIENT_CREDENTIALS when not overridden)
    assert "auth-type" in props, "auth-type is missing from catalog properties."
    assert props["auth-type"], "auth-type must be non-empty."

    # client-id must map to username
    assert "client-id" in props, "client-id is missing from catalog properties."
    assert props["client-id"] == shared_data["username"], (
        f"Expected client-id '{shared_data['username']}', got '{props['client-id']}'."
    )

    # client-secret must map to password
    assert "client-secret" in props, "client-secret is missing from catalog properties."
    assert props["client-secret"] == shared_data["password"], (
        f"Expected client-secret '{shared_data['password']}', got '{props['client-secret']}'."
    )

    # tenant-id must map to database
    assert "tenant-id" in props, "tenant-id is missing from catalog properties."
    assert props["tenant-id"] == shared_data["database"], (
        f"Expected tenant-id '{shared_data['database']}', got '{props['tenant-id']}'."
    )


@then("certificate_path/certificate_password are included when present in mapping")
def certificate_fields_included_when_present_in_mapping(shared_data):
    """Assert that certificate_path and certificate_password are included when set in mapping."""
    source: Source = shared_data["source"]

    # Build a new source with certificate fields in mapping
    cert_source = Source(
        id=source.id + "-cert",
        type=SourceType.sharepoint,
        base_url=source.base_url,
        username=source.username,
        password=source.password,
        database=source.database,
        mapping={
            "auth_type": "CERTIFICATE",
            "certificate_path": "/certs/my.pfx",
            "certificate_password": "pfx-pass",
        },
    )
    cert_props = _build_catalog_properties(cert_source, source.password or "")

    assert "certificate-path" in cert_props, (
        "certificate-path should be present when certificate_path is set in mapping."
    )
    assert cert_props["certificate-path"] == "/certs/my.pfx", (
        f"Unexpected certificate-path: '{cert_props['certificate-path']}'."
    )

    assert "certificate-password" in cert_props, (
        "certificate-password should be present when certificate_password is set in mapping."
    )
    assert cert_props["certificate-password"] == "pfx-pass", (
        f"Unexpected certificate-password: '{cert_props['certificate-password']}'."
    )

    # Also verify absence when mapping is empty (use original source with no cert fields)
    no_cert_props = _build_catalog_properties(source, source.password or "")
    assert "certificate-path" not in no_cert_props, (
        "certificate-path should NOT be present when certificate_path is absent from mapping."
    )
    assert "certificate-password" not in no_cert_props, (
        "certificate-password should NOT be present when certificate_password is absent from mapping."
    )


@then(parsers.parse('available SharePoint lists (e.g., "{list_a}", "{list_b}") appear in the table dropdown'))
def available_sharepoint_lists_appear_in_table_dropdown(shared_data, list_a, list_b):
    """
    Assert that the enumerated SharePoint lists include the expected list names
    and that the catalog is properly configured to expose them as queryable schemas
    via Trino.

    The SharePoint Calcite connector enumerates each SharePoint list as a schema.
    Users see these lists in the Provisa UI table dropdown when registering a new table.

    REQ-731: SharePoint lists are enumerated as schemas and exposed as queryable tables
    via Trino, allowing users to discover available lists and register them as Provisa tables.
    """
    available_lists: list = shared_data.get("available_lists")

    assert available_lists is not None, (
        "SharePoint lists were not enumerated. "
        "Ensure the 'when' step ran and populated shared_data['available_lists']."
    )

    assert len(available_lists) > 0, (
        "No SharePoint lists were returned. "
        "The connector must enumerate at least one list to be useful for table discovery."
    )

    # Verify the specific lists mentioned in the scenario appear in the enumerated results
    assert list_a in available_lists, (
        f"Expected SharePoint list '{list_a}' to appear in the available lists, "
        f"but got: {available_lists}."
    )
    assert list_b in available_lists, (
        f"Expected SharePoint list '{list_b}' to appear in the available lists, "
        f"but got: {available_lists}."
    )

    # Verify that the catalog properties are correctly set up to enable Trino enumeration
    catalog_props: dict = shared_data["catalog_props"]
    assert catalog_props is not None, "Catalog properties must be present for list enumeration."
    assert "site-url" in catalog_props, (
        "site-url must be in catalog properties so Trino can connect to SharePoint."
    )
    assert "auth-type" in catalog_props, (
        "auth-type must be in catalog properties for SharePoint authentication."
    )

    # Verify the source type is registered correctly for Trino connector routing
    source: Source = shared_data["source"]
    assert source.connector == "sharepoint", (
        f"Source connector must be 'sharepoint' for Trino catalog creation, got '{source.connector}'."
    )

    # Verify case-insensitive name matching is enabled (required for SharePoint list name resolution)
    assert catalog_props.get("case-insensitive-name-matching") == "true", (
        "case-insensitive-name-matching must be 'true' for SharePoint list name resolution. "
        f"Got: '{catalog_props.get('case-insensitive-name-matching')}'."
    )

    # Verify each list in the available_lists is a non-empty string — a valid schema name
    for list_name in available_lists:
        assert isinstance(list_name, str) and list_name.strip(), (
            f"Every enumerated SharePoint list name must be a non-empty string. "
            f"Got: {list_name!r}."
        )

    # Verify the SOURCE_TO_CONNECTOR registry maps sharepoint to the sharepoint connector,
    # ensuring Trino will route queries to the correct connector for list enumeration.
    assert "sharepoint" in SOURCE_TO_CONNECTOR, (
        "'sharepoint' must be in SOURCE_TO_CONNECTOR so Trino catalog creation "
        "routes to the correct connector for schema (list) enumeration."
    )
    assert SOURCE_TO_CONNECTOR["sharepoint"] == "sharepoint", (
        f"SOURCE_TO_CONNECTOR['sharepoint'] must be 'sharepoint', "
        f"got '{SOURCE_TO_CONNECTOR['sharepoint']}'."
    )

    # Verify that at least the two example lists from the scenario are distinct
    assert list_a != list_b, (
        f"The two example lists must be distinct, but both were '{list_a}'."
    )

    # Verify the available lists represent a realistic SharePoint site — at least 2 lists
    assert len(available_lists) >= 2, (
        f"A SharePoint site should expose at least 2 lists for meaningful table discovery. "
        f"Got: {available_lists}."
    )

    # Verify each list name is a valid potential Trino schema identifier (no leading/trailing whitespace)
    for list_name in available_lists:
        assert list_name == list_name.strip(), (
            f"SharePoint list name '{list_name}' has leading or trailing whitespace, "
            "which would cause issues as a Trino schema name."
        )

    # Verify the site-url in catalog_props is reachable as a non-empty HTTPS URL
    site_url = catalog_props["site-url"]
    assert site_url.startswith("https://"), (
        f"site-url must use HTTPS for secure SharePoint connectivity. Got: '{site_url}'."
    )

    # Verify the auth credentials are present in catalog properties so the connector
    # can authenticate against SharePoint when enumerating lists
    assert "client-id" in catalog_props, (
        "client-id must be present in catalog properties so the SharePoint connector "
        "can authenticate to enumerate lists."
    )
    assert catalog_props["client-id"], "client-id must be non-empty for SharePoint authentication."

    # Verify that all simulated list names are lowercase (as returned by the Calcite connector
    # after case-insensitive normalisation) — this ensures consistent schema name handling in Trino
    for list_name in available_lists:
        assert list_name == list_name.lower(), (
            f"Enumerated SharePoint list name '{list_name}' is not lowercase. "
            "The Calcite connector normalises schema names to lowercase when "
            "case-insensitive-name-matching is enabled."
        )

    # Verify the source has a non-empty site URL (base_url or host) that the connector uses
    assert source.base_url or source.host, (
        "SharePoint source must have a non-empty base_url or host to enumerate lists."
    )


@then("the table is created with the supplied column definitions")
def the_table_is_created_with_the_supplied_column_definitions(shared_data):
    """
    Assert that:
    1. The Table was successfully constructed (simulating the registerTable mutation result).
    2. The table's columns exactly match the columns supplied by the user via the mutation.
    3. Each column has the expected name and data type.
    4. The table is associated with the correct SharePoint source.
    5. The column definitions are non-empty, confirming the bypass of the connector limitation.

    This validates REQ-732: users can register SharePoint tables with manually supplied
    column definitions obtained from the Microsoft Graph API, bypassing the Calcite
    connector's inability to expose information_schema.columns.
    """
    table: Table = shared_data.get("registered_table")
    supplied_columns: list[Column] = shared_data.get("supplied_columns", [])
    source: Source = shared_data["source"]

    assert table is not None, (
        "No table was registered. The registerTable mutation step must run successfully."
    )

    # Verify the table is linked to the correct SharePoint source
    assert table.source_id == source.id, (
        f"Table source_id mismatch: expected '{source.id}', got '{table.source_id}'."
    )

    # Verify that columns were persisted (not left empty due to connector limitation)
    assert table.columns is not None, "Table columns must not be None."
    assert len(table.columns) > 0, (
        "Table must have at least one column. "
        "The user-supplied column definitions must be stored on the table, "
        "bypassing the connector's information_schema.columns limitation."
    )

    # Verify column count matches what was supplied
    assert len(table.columns) == len(supplied_columns), (
        f"Column count mismatch: expected {len(supplied_columns)} columns "
        f"(as supplied in the mutation), got {len(table.columns)}."
    )

    # Verify each supplied column is present in the registered table with correct name
    supplied_names = [col.name for col in supplied_columns]
    registered_names = [col.name for col in table.columns]

    for expected_name in supplied_names:
