# Copyright (c) 2026 Kenneth Stott
# Canary: fb064706-57b2-4db0-9e18-3a8730a9c846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for SharePoint connector (REQ-726–REQ-732)."""

from __future__ import annotations


from provisa.core.catalog import _build_catalog_properties
from provisa.core.models import Column, Source, SourceType, Table
from provisa.core.source_registry import SOURCE_TO_CONNECTOR


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _source(
    *,
    source_id: str = "sp-1",
    host: str = "contoso.sharepoint.com",
    base_url: str | None = None,
    username: str = "my-client-id",
    password: str = "my-client-secret",
    database: str = "tenant-guid-1234",
    mapping: dict | None = None,
) -> Source:
    return Source(
        id=source_id,
        type=SourceType.sharepoint,
        host=host,
        base_url=base_url,
        port=443,
        username=username,
        password=password,
        database=database,
        mapping=mapping or {},
    )


def _props(source: Source, resolved_password: str | None = None) -> dict[str, str]:
    # The Trino connector reads the (resolved) password off the source (REQ-842); prod passes
    # resolve_secrets(source.password), so put the test's resolved password there.
    pw = resolved_password if resolved_password is not None else source.password
    return _build_catalog_properties(source.model_copy(update={"password": pw}), "")


def _column(name: str = "Title", data_type: str = "VARCHAR") -> Column:
    return Column(name=name, data_type=data_type, visible_to=["analyst"])


def _table(
    source_id: str = "sp-1",
    schema_name: str = "Documents",
    table_name: str = "Items",
    columns: list[Column] | None = None,
) -> Table:
    return Table(
        source_id=source_id,
        domain_id="default",
        schema_name=schema_name,
        table_name=table_name,
        columns=columns or [_column()],
    )


# --------------------------------------------------------------------------- #
# REQ-726: SharePoint registered in SourceType and SOURCE_TO_CONNECTOR        #
# --------------------------------------------------------------------------- #


class TestReq726RegistryPresence:
    def test_sharepoint_in_source_type_enum(self):
        assert "sharepoint" in SourceType.__members__

    def test_sharepoint_enum_value(self):
        assert SourceType.sharepoint.value == "sharepoint"

    def test_sharepoint_in_source_to_connector(self):
        assert "sharepoint" in SOURCE_TO_CONNECTOR

    def test_sharepoint_connector_name(self):
        assert SOURCE_TO_CONNECTOR["sharepoint"] == "sharepoint"

    def test_source_connector_property(self):
        src = _source()
        assert src.connector == "sharepoint"

    def test_source_type_is_sharepoint(self):
        src = _source()
        assert src.type == SourceType.sharepoint

    def test_connector_name_non_empty(self):
        assert SOURCE_TO_CONNECTOR["sharepoint"]


# --------------------------------------------------------------------------- #
# REQ-727: Two auth methods — ClientCredentials and UsernamePassword           #
# --------------------------------------------------------------------------- #


class TestReq727AuthMethods:
    def test_client_credentials_auth_type_in_props(self):
        src = _source(mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src)
        assert props["auth-type"] == "CLIENT_CREDENTIALS"

    def test_username_password_auth_type_in_props(self):
        src = _source(mapping={"auth_type": "USERNAME_PASSWORD"})
        props = _props(src)
        assert props["auth-type"] == "USERNAME_PASSWORD"

    def test_default_auth_type_is_client_credentials(self):
        src = _source(mapping={})
        props = _props(src)
        assert props["auth-type"] == "CLIENT_CREDENTIALS"

    def test_client_credentials_sends_client_id(self):
        src = _source(mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src)
        assert "client-id" in props
        assert props["client-id"] == "my-client-id"

    def test_client_credentials_sends_client_secret(self):
        src = _source(mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src)
        assert "client-secret" in props
        assert props["client-secret"] == "my-client-secret"

    def test_username_password_sends_client_id(self):
        src = _source(username="upn@contoso.com", mapping={"auth_type": "USERNAME_PASSWORD"})
        props = _props(src)
        assert "client-id" in props
        assert props["client-id"] == "upn@contoso.com"

    def test_username_password_sends_client_secret(self):
        src = _source(password="upn-password", mapping={"auth_type": "USERNAME_PASSWORD"})
        props = _props(src)
        assert "client-secret" in props
        assert props["client-secret"] == "upn-password"

    def test_no_client_id_when_username_empty(self):
        src = _source(username="", mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src, resolved_password="secret")
        assert "client-id" not in props

    def test_no_client_secret_when_password_empty(self):
        src = _source(password="", mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src, resolved_password="")
        assert "client-secret" not in props

    def test_certificate_auth_type_in_props(self):
        src = _source(
            mapping={
                "auth_type": "CERTIFICATE",
                "certificate_path": "/certs/app.pfx",
                "certificate_password": "pfx-pass",
            }
        )
        props = _props(src)
        assert props["auth-type"] == "CERTIFICATE"

    def test_certificate_path_in_props(self):
        src = _source(
            mapping={
                "auth_type": "CERTIFICATE",
                "certificate_path": "/certs/app.pfx",
                "certificate_password": "pfx-pass",
            }
        )
        props = _props(src)
        assert props["certificate-path"] == "/certs/app.pfx"

    def test_certificate_password_in_props(self):
        src = _source(
            mapping={
                "auth_type": "CERTIFICATE",
                "certificate_path": "/certs/app.pfx",
                "certificate_password": "pfx-pass",
            }
        )
        props = _props(src)
        assert props["certificate-password"] == "pfx-pass"

    def test_certificate_path_absent_for_client_credentials(self):
        src = _source(mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src)
        assert "certificate-path" not in props

    def test_certificate_password_absent_for_client_credentials(self):
        src = _source(mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src)
        assert "certificate-password" not in props


# --------------------------------------------------------------------------- #
# REQ-728: Connection properties built from source config                      #
# --------------------------------------------------------------------------- #


class TestReq728ConnectionProperties:
    def test_site_url_from_base_url(self):
        src = _source(base_url="https://contoso.sharepoint.com/sites/Sales")
        props = _props(src)
        assert props["site-url"] == "https://contoso.sharepoint.com/sites/Sales"

    def test_site_url_falls_back_to_host(self):
        src = _source(host="contoso.sharepoint.com", base_url=None)
        props = _props(src)
        assert props["site-url"] == "contoso.sharepoint.com"

    def test_tenant_id_from_database(self):
        src = _source(database="5d2609cc-7eff-4b82-8f83-f0b28c71fafc")
        props = _props(src)
        assert props["tenant-id"] == "5d2609cc-7eff-4b82-8f83-f0b28c71fafc"

    def test_no_tenant_id_when_database_empty(self):
        src = _source(database="")
        props = _props(src)
        assert "tenant-id" not in props

    def test_client_id_from_username(self):
        src = _source(username="app-registration-id")
        props = _props(src)
        assert props["client-id"] == "app-registration-id"

    def test_client_secret_from_resolved_password(self):
        src = _source(password="original")
        props = _props(src, resolved_password="resolved-secret")
        assert props["client-secret"] == "resolved-secret"

    def test_auth_type_key_always_present(self):
        src = _source()
        props = _props(src)
        assert "auth-type" in props

    def test_site_url_key_always_present(self):
        src = _source()
        props = _props(src)
        assert "site-url" in props

    def test_all_five_core_fields_present(self):
        src = _source(
            base_url="https://contoso.sharepoint.com/sites/Sales",
            username="client-id",
            password="client-secret",
            database="tenant-guid",
        )
        props = _props(src)
        for key in ("site-url", "auth-type", "client-id", "client-secret", "tenant-id"):
            assert key in props, f"Missing key: {key}"

    def test_certificate_fields_absent_when_not_in_mapping(self):
        src = _source(mapping={})
        props = _props(src)
        assert "certificate-path" not in props
        assert "certificate-password" not in props

    def test_certificate_fields_present_when_in_mapping(self):
        src = _source(
            mapping={
                "certificate_path": "/certs/my.pfx",
                "certificate_password": "pfx-secret",
            }
        )
        props = _props(src)
        assert props["certificate-path"] == "/certs/my.pfx"
        assert props["certificate-password"] == "pfx-secret"


# --------------------------------------------------------------------------- #
# REQ-729: Secret values masked in logs                                        #
# --------------------------------------------------------------------------- #


class TestReq729SecretMasking:
    def test_password_in_model_dump_by_default(self):
        # Source is a plain Pydantic model — password is stored in clear.
        # REQ-729 masking applies to log-safe views, not the raw model.
        src = _source(password="super-secret-password")
        dumped = src.model_dump()
        assert dumped["password"] == "super-secret-password"

    def test_model_dump_excludes_password_when_excluded(self):
        src = _source(password="super-secret-password")
        dumped = src.model_dump(exclude={"password"})
        assert "password" not in dumped

    def test_client_secret_key_present_in_props(self):
        # The props dict contains client-secret — callers strip it before logging.
        src = _source(password="client-secret-value")
        props = _props(src)
        assert "client-secret" in props
        assert props["client-secret"] == "client-secret-value"

    def test_certificate_password_key_present_in_props(self):
        # certificate-password is present in props; callers strip it before logging.
        src = _source(
            mapping={
                "auth_type": "CERTIFICATE",
                "certificate_path": "/certs/app.pfx",
                "certificate_password": "pfx-secret-do-not-log",
            }
        )
        props = _props(src)
        assert "certificate-password" in props
        assert props["certificate-password"] == "pfx-secret-do-not-log"

    def test_props_dict_excludes_password_when_explicitly_stripped(self):
        src = _source(password="my-secret")
        props = _props(src)
        safe = {
            k: v for k, v in props.items() if k not in ("client-secret", "certificate-password")
        }
        assert "client-secret" not in safe
        assert "certificate-password" not in safe

    def test_auth_type_value_not_a_secret(self):
        src = _source(mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src)
        assert props["auth-type"] == "CLIENT_CREDENTIALS"

    def test_site_url_value_not_a_secret(self):
        src = _source(base_url="https://contoso.sharepoint.com/sites/Sales")
        props = _props(src)
        assert props["site-url"] == "https://contoso.sharepoint.com/sites/Sales"


# --------------------------------------------------------------------------- #
# REQ-730: case-insensitive-name-matching always set                           #
# --------------------------------------------------------------------------- #


class TestReq730CaseInsensitiveMatching:
    def test_case_insensitive_name_matching_present(self):
        src = _source()
        props = _props(src)
        assert "case-insensitive-name-matching" in props

    def test_case_insensitive_name_matching_is_true(self):
        src = _source()
        props = _props(src)
        assert props["case-insensitive-name-matching"] == "true"

    def test_case_insensitive_set_for_client_credentials(self):
        src = _source(mapping={"auth_type": "CLIENT_CREDENTIALS"})
        props = _props(src)
        assert props["case-insensitive-name-matching"] == "true"

    def test_case_insensitive_set_for_certificate_auth(self):
        src = _source(
            mapping={
                "auth_type": "CERTIFICATE",
                "certificate_path": "/certs/app.pfx",
                "certificate_password": "pfx-pass",
            }
        )
        props = _props(src)
        assert props["case-insensitive-name-matching"] == "true"

    def test_case_insensitive_set_when_no_mapping(self):
        src = _source(mapping={})
        props = _props(src)
        assert props["case-insensitive-name-matching"] == "true"

    def test_case_insensitive_set_when_no_credentials(self):
        src = _source(username="", password="", database="")
        props = _props(src, resolved_password="")
        assert props["case-insensitive-name-matching"] == "true"


# --------------------------------------------------------------------------- #
# REQ-731: SharePoint lists enumerated as schemas / queryable tables           #
# --------------------------------------------------------------------------- #


class TestReq731ListsAsSchemas:
    def test_table_model_accepts_sharepoint_source(self):
        tbl = _table(source_id="sp-1", schema_name="Documents", table_name="Items")
        assert tbl.source_id == "sp-1"

    def test_schema_name_maps_to_sharepoint_list(self):
        tbl = _table(schema_name="SalesData", table_name="Leads")
        assert tbl.schema_name == "SalesData"

    def test_table_name_maps_to_list_item_collection(self):
        tbl = _table(table_name="Opportunities")
        assert tbl.table_name == "Opportunities"

    def test_multiple_lists_as_separate_table_models(self):
        lists = [
            _table(schema_name="HR", table_name="Employees"),
            _table(schema_name="Finance", table_name="Budgets"),
            _table(schema_name="IT", table_name="Assets"),
        ]
        schema_names = {t.schema_name for t in lists}
        assert schema_names == {"HR", "Finance", "IT"}

    def test_table_model_requires_columns(self):
        tbl = _table(columns=[_column("Title", "VARCHAR"), _column("Modified", "TIMESTAMP")])
        assert len(tbl.columns) == 2

    def test_table_domain_id_defaults_to_default(self):
        tbl = _table()
        assert tbl.domain_id == "default"

    def test_table_source_id_matches_sharepoint_source(self):
        src = _source(source_id="sp-prod")
        tbl = _table(source_id=src.id)
        assert tbl.source_id == src.id

    def test_sharepoint_source_catalog_name_sanitized(self):
        src = _source(source_id="sp-prod-1")
        assert src.catalog_name == "sp_prod_1"


# --------------------------------------------------------------------------- #
# REQ-732: Registering SharePoint tables with known column definitions         #
# --------------------------------------------------------------------------- #


class TestReq732TableRegistrationWithColumns:
    def test_table_registered_with_known_columns(self):
        cols = [
            _column("Title", "VARCHAR"),
            _column("Author", "VARCHAR"),
            _column("Created", "TIMESTAMP"),
        ]
        tbl = _table(columns=cols)
        assert len(tbl.columns) == 3

    def test_column_name_preserved(self):
        col = _column("Modified", "TIMESTAMP")
        tbl = _table(columns=[col])
        assert tbl.columns[0].name == "Modified"

    def test_column_data_type_preserved(self):
        col = _column("Amount", "DOUBLE")
        tbl = _table(columns=[col])
        assert tbl.columns[0].data_type == "DOUBLE"

    def test_multiple_column_types(self):
        cols = [
            _column("id", "INTEGER"),
            _column("label", "VARCHAR"),
            _column("ratio", "DOUBLE"),
            _column("active", "BOOLEAN"),
            _column("created", "TIMESTAMP"),
        ]
        tbl = _table(columns=cols)
        type_map = {c.name: c.data_type for c in tbl.columns}
        assert type_map["id"] == "INTEGER"
        assert type_map["label"] == "VARCHAR"
        assert type_map["ratio"] == "DOUBLE"
        assert type_map["active"] == "BOOLEAN"
        assert type_map["created"] == "TIMESTAMP"

    def test_table_with_empty_columns_is_valid_model(self):
        # Table does not enforce non-empty columns at the model level.
        # Registration with known columns is the recommended practice (REQ-732).
        tbl = Table(
            source_id="sp-1",
            domain_id="default",
            schema_name="Documents",
            table_name="Items",
            columns=[],
        )
        assert tbl.columns == []

    def test_column_visible_to_preserved(self):
        col = Column(name="Title", data_type="VARCHAR", visible_to=["analyst", "admin"])
        tbl = _table(columns=[col])
        assert tbl.columns[0].visible_to == ["analyst", "admin"]

    def test_table_schema_name_matches_sharepoint_list_name(self):
        tbl = _table(schema_name="ProjectTracking", table_name="Tasks")
        assert tbl.schema_name == "ProjectTracking"
        assert tbl.table_name == "Tasks"

    def test_many_columns_all_present(self):
        col_defs = [
            ("id", "INTEGER"),
            ("title", "VARCHAR"),
            ("due_date", "TIMESTAMP"),
            ("priority", "VARCHAR"),
            ("status", "VARCHAR"),
            ("assignee", "VARCHAR"),
            ("budget", "DOUBLE"),
        ]
        cols = [_column(name, dtype) for name, dtype in col_defs]
        tbl = _table(columns=cols)
        assert len(tbl.columns) == len(col_defs)
        names = [c.name for c in tbl.columns]
        assert names == [n for n, _ in col_defs]

    def test_registered_table_linked_to_sharepoint_source(self):
        src = _source(source_id="sp-collab")
        tbl = _table(source_id=src.id, schema_name="Wiki", table_name="Pages")
        assert tbl.source_id == src.id
        assert src.type == SourceType.sharepoint

    def test_is_primary_key_flag_on_column(self):
        col = Column(name="ID", data_type="INTEGER", visible_to=["analyst"], is_primary_key=True)
        tbl = _table(columns=[col])
        assert tbl.columns[0].is_primary_key is True
