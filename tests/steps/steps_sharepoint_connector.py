# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step definitions for REQ-726, REQ-727, and REQ-728 — SharePoint Connector."""

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.core.catalog import _build_catalog_properties
from provisa.core.models import SOURCE_TO_CONNECTOR, Source, SourceType

scenarios("../features/REQ-726.feature")
scenarios("../features/REQ-727.feature")
scenarios("../features/REQ-728.feature")


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
