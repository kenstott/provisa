# Copyright (c) 2026 Kenneth Stott
# Canary: 4d699dd7-4f72-4168-b047-93f53508b42f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1265, REQ-919: the admin auth registry lists the LDAP provider and saves its block."""

from __future__ import annotations

import dataclasses

import pytest

from provisa.api.admin.auth_settings import (
    AUTH_PROVIDERS,
    apply_auth_settings,
    provider_config_view,
)
from provisa.api.errors import ApiError


def _fields(key: str) -> dict[str, dict]:
    (entry,) = [p for p in AUTH_PROVIDERS if p["key"] == key]
    return {f["config_key"]: f for f in entry["config_fields"]}


class TestLdapRegistryEntry:
    def test_ldap_is_listed_with_the_existing_providers(self):
        assert [p["key"] for p in AUTH_PROVIDERS] == [
            "none",
            "basic",
            "firebase",
            "keycloak",
            "oauth",
            "ldap",
            "saml",
            "simple",
        ]

    def test_the_fields_are_the_provider_settings_plus_the_session_secret(self):
        from provisa.auth.providers.ldap import LdapSettings

        declared = set(_fields("ldap"))
        assert declared == {f.name for f in dataclasses.fields(LdapSettings)} | {"jwt_secret"}

    def test_required_fields_are_the_settings_without_a_default(self):
        from provisa.auth.providers.ldap import LdapSettings

        required = {k for k, f in _fields("ldap").items() if f["required"]}
        assert required == {
            f.name for f in dataclasses.fields(LdapSettings) if f.default is dataclasses.MISSING
        }

    def test_the_bind_password_and_session_secret_are_secret(self):
        assert {k for k, f in _fields("ldap").items() if f.get("secret")} == {
            "bind_password",
            "jwt_secret",
        }

    def test_start_tls_is_a_boolean_field(self):
        assert _fields("ldap")["start_tls"]["type"] == "boolean"


class TestSamlRegistryEntry:
    def test_the_fields_are_the_provider_settings_plus_the_session_secret(self):
        from provisa.auth.providers.saml import SamlSettings

        assert set(_fields("saml")) == {f.name for f in dataclasses.fields(SamlSettings)} | {
            "jwt_secret"
        }

    def test_required_fields_are_the_settings_without_a_default_and_the_secret(self):
        from provisa.auth.providers.saml import SamlSettings

        required = {k for k, f in _fields("saml").items() if f["required"]}
        assert required == {
            f.name for f in dataclasses.fields(SamlSettings) if f.default is dataclasses.MISSING
        } | {"jwt_secret"}

    def test_only_the_session_secret_is_secret(self):
        assert {k for k, f in _fields("saml").items() if f.get("secret")} == {"jwt_secret"}


_LDAP = {
    "server_url": "ldap://directory.example:389",
    "bind_dn": "cn=svc,dc=example,dc=com",
    "bind_password": "${env:LDAP_BIND_PASSWORD}",
    "user_base_dn": "ou=people,dc=example,dc=com",
    "user_filter": "(uid={username})",
    "user_id_attribute": "uid",
}


class TestApplyAuthSettings:
    def test_only_declared_keys_are_kept_and_the_session_secret_is_top_level(self):
        auth = apply_auth_settings(
            {},
            {
                "provider": "ldap",
                "config": {**_LDAP, "start_tls": True, "jwt_secret": "s" * 48, "bogus": "x"},
            },
        )
        assert auth["provider"] == "ldap"
        assert auth["ldap"] == {**_LDAP, "start_tls": True}
        assert auth["jwt_secret"] == "s" * 48

    def test_a_boolean_field_refuses_a_string(self):
        with pytest.raises(ApiError) as refused:
            apply_auth_settings({}, {"provider": "ldap", "config": {**_LDAP, "start_tls": "true"}})
        assert refused.value.status_code == 400
        assert refused.value.code == "settings.auth_field_type"

    def test_an_unknown_provider_is_refused(self):
        with pytest.raises(ApiError) as refused:
            apply_auth_settings({}, {"provider": "no-such-provider"})
        assert refused.value.status_code == 400
        assert refused.value.code == "settings.unknown_auth_provider"

    def test_a_saved_block_survives_a_save_that_omits_a_secret(self):
        saved = apply_auth_settings({}, {"provider": "ldap", "config": dict(_LDAP)})
        again = apply_auth_settings(
            saved,
            {"provider": "ldap", "config": {"server_url": "ldaps://directory.example:636"}},
        )
        assert again["ldap"]["bind_password"] == _LDAP["bind_password"]
        assert again["ldap"]["server_url"] == "ldaps://directory.example:636"

    def test_common_role_settings_are_saved(self):
        auth = apply_auth_settings(
            {}, {"provider": "none", "common": {"default_role": "viewer", "bogus": 1}}
        )
        assert auth["default_role"] == "viewer"
        assert "bogus" not in auth


class TestProviderConfigView:
    def test_the_session_secret_is_shown_under_each_provider_that_declares_it(self):
        view = provider_config_view({"jwt_secret": "s" * 48, "ldap": dict(_LDAP)})
        assert view["ldap"]["jwt_secret"] == "s" * 48
        assert view["simple"]["jwt_secret"] == "s" * 48
        assert view["basic"]["jwt_secret"] == "s" * 48
        assert "jwt_secret" not in view["oauth"]


def test_registration_can_be_turned_off_from_the_admin():
    auth = apply_auth_settings({}, {"provider": "none", "common": {"allow_registration": False}})
    assert auth["allow_registration"] is False
