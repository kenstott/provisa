# Copyright (c) 2026 Kenneth Stott
# Canary: fb064706-57b2-4db0-9e18-3a8730a9c846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Splunk connector catalog properties (REQ-721–REQ-725)."""

from __future__ import annotations


from provisa.core.catalog import _build_catalog_properties
from provisa.core.models import SOURCE_TO_CONNECTOR, Source, SourceType


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _source(
    *,
    host: str = "splunk",
    port: int = 8089,
    base_url: str | None = None,
    username: str = "",
    password: str = "",
    database: str = "",
    mapping: dict | None = None,
) -> Source:
    return Source(
        id="splunk-1",
        type=SourceType.splunk,
        host=host,
        port=port,
        base_url=base_url,
        username=username,
        password=password,
        database=database,
        mapping=mapping or {},
    )


def _props(source: Source, resolved_password: str = "") -> dict[str, str]:
    # The Trino connector reads the (resolved) password off the source (REQ-842); prod passes
    # resolve_secrets(source.password), so put the test's resolved password there.
    return _build_catalog_properties(source.model_copy(update={"password": resolved_password}), "")


# --------------------------------------------------------------------------- #
# TestSourceTypeRegistration (REQ-721)                                         #
# --------------------------------------------------------------------------- #


class TestSourceTypeRegistration:
    def test_splunk_in_source_type_enum(self):
        assert SourceType.splunk in SourceType.__members__.values()

    def test_splunk_value_is_splunk(self):
        assert SourceType.splunk.value == "splunk"

    def test_splunk_maps_to_splunk_connector(self):
        assert SOURCE_TO_CONNECTOR["splunk"] == "splunk"

    def test_source_with_type_splunk_is_valid(self):
        src = _source()
        assert src.type == SourceType.splunk

    def test_connector_property_returns_splunk(self):
        src = _source()
        assert src.connector == "splunk"


# --------------------------------------------------------------------------- #
# TestUrlConstruction (REQ-722)                                                #
# --------------------------------------------------------------------------- #


class TestUrlConstruction:
    def test_url_constructed_from_host_and_port(self):
        props = _props(_source(host="splunk", port=8089))
        assert props["url"] == "https://splunk:8089"

    def test_url_uses_https_scheme_by_default(self):
        props = _props(_source(host="myhost", port=9089))
        assert props["url"].startswith("https://")

    def test_url_includes_host(self):
        props = _props(_source(host="mysplunk.example.com", port=8089))
        assert "mysplunk.example.com" in props["url"]

    def test_url_includes_port(self):
        props = _props(_source(host="splunk", port=9999))
        assert "9999" in props["url"]

    def test_base_url_takes_precedence_over_host_port(self):
        props = _props(_source(host="ignored", port=0, base_url="https://custom.splunk.io:8089"))
        assert props["url"] == "https://custom.splunk.io:8089"

    def test_base_url_used_verbatim(self):
        props = _props(_source(base_url="https://splunk.corp.example.com"))
        assert props["url"] == "https://splunk.corp.example.com"

    def test_default_port_is_8089_when_port_zero(self):
        props = _props(_source(host="splunk", port=0))
        assert props["url"] == "https://splunk:8089"

    def test_url_key_always_present(self):
        props = _props(_source())
        assert "url" in props

    def test_case_insensitive_name_matching_always_set(self):
        props = _props(_source())
        assert props.get("case-insensitive-name-matching") == "true"


# --------------------------------------------------------------------------- #
# TestAuthentication (REQ-722, REQ-723)                                        #
# --------------------------------------------------------------------------- #


class TestAuthentication:
    def test_token_auth_sets_token_key(self):
        props = _props(_source(mapping={"use_token": True}), resolved_password="mytoken")
        assert props["token"] == "mytoken"

    def test_token_auth_is_default_when_use_token_absent(self):
        props = _props(_source(), resolved_password="defaulttoken")
        assert props["token"] == "defaulttoken"

    def test_token_auth_does_not_set_user(self):
        props = _props(
            _source(username="admin", mapping={"use_token": True}), resolved_password="tok"
        )
        assert "user" not in props

    def test_token_auth_does_not_set_password_key(self):
        props = _props(_source(mapping={"use_token": True}), resolved_password="tok")
        assert "password" not in props

    def test_use_token_false_sets_user_and_password(self):
        props = _props(
            _source(username="admin", mapping={"use_token": False}),
            resolved_password="secret",
        )
        assert props["user"] == "admin"
        assert props["password"] == "secret"

    def test_use_token_false_no_token_key(self):
        props = _props(
            _source(username="admin", mapping={"use_token": False}),
            resolved_password="secret",
        )
        assert "token" not in props

    def test_use_token_false_omits_user_when_username_empty(self):
        props = _props(
            _source(username="", mapping={"use_token": False}),
            resolved_password="secret",
        )
        assert "user" not in props

    def test_use_token_false_omits_password_when_resolved_password_empty(self):
        props = _props(
            _source(username="admin", mapping={"use_token": False}),
            resolved_password="",
        )
        assert "password" not in props

    def test_token_absent_when_resolved_password_empty_and_use_token_true(self):
        props = _props(_source(mapping={"use_token": True}), resolved_password="")
        assert "token" not in props


# --------------------------------------------------------------------------- #
# TestOptionalParameters (REQ-724)                                             #
# --------------------------------------------------------------------------- #


class TestOptionalParameters:
    def test_app_set_from_database_field(self):
        props = _props(_source(database="search"))
        assert props["app"] == "search"

    def test_app_absent_when_database_empty(self):
        props = _props(_source(database=""))
        assert "app" not in props

    def test_datamodel_filter_propagated(self):
        props = _props(_source(mapping={"datamodel_filter": "Authentication"}))
        assert props["datamodel-filter"] == "Authentication"

    def test_datamodel_filter_absent_when_not_set(self):
        props = _props(_source())
        assert "datamodel-filter" not in props

    def test_disable_ssl_validation_propagated(self):
        props = _props(_source(mapping={"disable_ssl_validation": True}))
        assert props.get("disable-ssl-validation") == "true"

    def test_disable_ssl_validation_absent_by_default(self):
        props = _props(_source())
        assert "disable-ssl-validation" not in props

    def test_app_and_datamodel_filter_coexist(self):
        props = _props(
            _source(database="myapp", mapping={"datamodel_filter": "Malware"}),
            resolved_password="tok",
        )
        assert props["app"] == "myapp"
        assert props["datamodel-filter"] == "Malware"


# --------------------------------------------------------------------------- #
# TestEndToEnd (REQ-725)                                                       #
# --------------------------------------------------------------------------- #


class TestEndToEnd:
    def test_full_token_auth_config_has_expected_keys(self):
        props = _props(
            _source(
                host="splunk.corp.example.com",
                port=8089,
                database="search",
                mapping={"use_token": True, "datamodel_filter": "Authentication"},
            ),
            resolved_password="Splunk_mytoken_abc123",
        )
        assert props["url"] == "https://splunk.corp.example.com:8089"
        assert props["token"] == "Splunk_mytoken_abc123"
        assert props["app"] == "search"
        assert props["datamodel-filter"] == "Authentication"
        assert props["case-insensitive-name-matching"] == "true"

    def test_full_user_password_config_has_expected_keys(self):
        props = _props(
            _source(
                host="splunk.corp.example.com",
                port=8089,
                username="admin",
                database="search",
                mapping={"use_token": False},
            ),
            resolved_password="changeme",
        )
        assert props["url"] == "https://splunk.corp.example.com:8089"
        assert props["user"] == "admin"
        assert props["password"] == "changeme"
        assert props["app"] == "search"
        assert "token" not in props

    def test_base_url_overrides_host_port_in_full_config(self):
        props = _props(
            _source(
                host="ignored",
                port=0,
                base_url="https://splunk.saas.io",
                mapping={"use_token": True},
            ),
            resolved_password="tok",
        )
        assert props["url"] == "https://splunk.saas.io"

    def test_catalog_name_derived_from_source_id(self):
        src = Source(
            id="splunk-prod",
            type=SourceType.splunk,
            host="splunk",
            port=8089,
        )
        assert src.catalog_name == "splunk_prod"

    def test_no_token_and_no_user_when_all_credentials_empty(self):
        props = _props(_source(username="", mapping={}), resolved_password="")
        assert "token" not in props
        assert "user" not in props
        assert "password" not in props
        assert "url" in props
