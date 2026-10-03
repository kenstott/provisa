# Copyright (c) 2026 Kenneth Stott
# Canary: d7543e50-9974-45d1-bf19-659a19f13827
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1265: the Helm chart configures the auth provider from values.yaml.

The operator picks the provider at install time (corporate OIDC, an LDAP directory, or the
break-glass account alone). The chart renders it into the API's provisa.yaml, takes every
credential from a Kubernetes Secret, and never mentions Firebase. Rendered with the real
``helm template``; the rendered ``auth`` block is then built into a provider by the product's
own factory, so a block the chart renders is a block the product accepts.

Placed with the unit tests because it needs no service: under tests/integration the session
would provision the whole container stack to run a template render.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[2] / "helm" / "provisa"
_KEY = "encryption.existingSecret=provisa-master-key"

_LDAP = [
    "auth.provider=ldap",
    "auth.sessionSecret.existingSecret=provisa-session",
    "auth.ldap.server=ldaps://directory.corp.example:636",
    "auth.ldap.bindDn=cn=provisa\\,ou=services\\,dc=corp\\,dc=example",
    "auth.ldap.bindPassword.existingSecret=provisa-ldap",
    "auth.ldap.baseDn=ou=people\\,dc=corp\\,dc=example",
    "auth.ldap.userFilter=(uid={username})",
    "auth.ldap.userIdAttribute=uid",
]
_OIDC = [
    "auth.provider=oidc",
    "auth.oidc.issuerUrl=https://login.corp.example/realms/main",
    "auth.oidc.clientId=provisa",
]
_SAML = [
    "auth.provider=saml",
    "auth.sessionSecret.existingSecret=provisa-session",
    "auth.saml.idpMetadataUrl=https://idp.corp.example/metadata",
    "auth.saml.publicUrl=https://provisa.corp.example",
]
_LOCAL = [
    "auth.provider=local",
    "auth.sessionSecret.existingSecret=provisa-session",
    "auth.breakGlass.username=platform-admin",
    "auth.breakGlass.existingSecret=provisa-break-glass",
]


def _render(*sets: str) -> subprocess.CompletedProcess:
    helm = shutil.which("helm")
    assert helm is not None, "the helm CLI is required to verify the chart"
    cmd = [helm, "template", "rel", str(CHART), "--set", _KEY]
    for s in sets:
        cmd += ["--set", s]
    return subprocess.run(cmd, capture_output=True, text=True)


def _documents(rendered: str) -> list[dict]:
    return [d for d in yaml.safe_load_all(rendered) if isinstance(d, dict)]


def _provisa_yaml(rendered: str) -> dict:
    config_map = next(
        d
        for d in _documents(rendered)
        if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "rel-config"
    )
    return yaml.safe_load(config_map["data"]["provisa.yaml"])


def _api_env(rendered: str) -> dict[str, dict]:
    deployment = next(
        d
        for d in _documents(rendered)
        if d["kind"] == "Deployment" and d["metadata"]["name"] == "rel-provisa"
    )
    containers = deployment["spec"]["template"]["spec"]["containers"]
    container = next(c for c in containers if c["name"] == "provisa")
    return {e["name"]: e for e in container["env"]}


def _ok(*sets: str) -> str:
    r = _render(*sets)
    assert r.returncode == 0, r.stderr
    return r.stdout


class TestTheProviderIsChosen:
    def test_the_chart_does_not_render_until_a_provider_is_chosen(self):
        r = _render()
        assert r.returncode != 0
        assert "auth.provider is not set" in r.stderr
        for choice in ("none", "oidc", "saml", "ldap", "local"):
            assert choice in r.stderr

    def test_none_chosen_explicitly_renders_no_auth_block(self):
        rendered = _ok("auth.provider=none")
        assert "auth" not in _provisa_yaml(rendered)
        assert not [name for name in _api_env(rendered) if name.startswith("PROVISA_AUTH_")]

    def test_an_unknown_provider_does_not_render(self):
        r = _render("auth.provider=kerberos")
        assert r.returncode != 0
        assert "auth.provider" in r.stderr
        for choice in ("none", "oidc", "saml", "ldap", "local"):
            assert choice in r.stderr


class TestOidc:
    def test_the_issuer_and_client_become_the_oidc_block(self):
        auth = _provisa_yaml(_ok(*_OIDC))["auth"]
        assert auth["provider"] == "oidc"
        assert auth["oidc"] == {
            "discovery_url": (
                "https://login.corp.example/realms/main/.well-known/openid-configuration"
            ),
            "client_id": "provisa",
        }

    def test_audience_and_role_claim_are_rendered_when_set(self):
        auth = _provisa_yaml(
            _ok(*_OIDC, "auth.oidc.audience=provisa-api", "auth.oidc.roleClaim=groups")
        )["auth"]
        assert auth["oidc"]["audience"] == "provisa-api"
        assert auth["oidc"]["role_claim"] == "groups"

    def test_the_product_builds_the_rendered_block(self):
        from provisa.auth.providers.oauth import OAuthProvider
        from provisa.auth.wiring import build_auth_provider

        provider = build_auth_provider(_provisa_yaml(_ok(*_OIDC))["auth"])
        assert isinstance(provider, OAuthProvider)

    def test_oidc_without_an_issuer_does_not_render(self):
        r = _render("auth.provider=oidc", "auth.oidc.clientId=provisa")
        assert r.returncode != 0
        assert "auth.oidc.issuerUrl" in r.stderr


class TestLdap:
    def test_the_directory_settings_become_the_ldap_block(self):
        auth = _provisa_yaml(_ok(*_LDAP))["auth"]
        assert auth["provider"] == "ldap"
        assert auth["ldap"] == {
            "server_url": "ldaps://directory.corp.example:636",
            "bind_dn": "cn=provisa,ou=services,dc=corp,dc=example",
            "bind_password": "${env:PROVISA_AUTH_LDAP_BIND_PASSWORD}",
            "user_base_dn": "ou=people,dc=corp,dc=example",
            "user_filter": "(uid={username})",
            "user_id_attribute": "uid",
            "start_tls": False,
        }
        assert auth["jwt_secret"] == "${env:PROVISA_AUTH_SESSION_SECRET}"

    def test_credentials_come_from_secrets_never_from_the_manifest(self):
        env = _api_env(_ok(*_LDAP))
        assert env["PROVISA_AUTH_LDAP_BIND_PASSWORD"]["valueFrom"]["secretKeyRef"] == {
            "name": "provisa-ldap",
            "key": "bind-password",
        }
        assert env["PROVISA_AUTH_SESSION_SECRET"]["valueFrom"]["secretKeyRef"] == {
            "name": "provisa-session",
            "key": "session-secret",
        }
        assert "value" not in env["PROVISA_AUTH_LDAP_BIND_PASSWORD"]
        assert "value" not in env["PROVISA_AUTH_SESSION_SECRET"]

    def test_group_settings_and_attributes_are_rendered_when_set(self):
        auth = _provisa_yaml(
            _ok(
                *_LDAP,
                "auth.ldap.groupBaseDn=ou=groups\\,dc=corp\\,dc=example",
                "auth.ldap.groupFilter=(member={user_dn})",
                "auth.ldap.groupNameAttribute=cn",
                "auth.ldap.emailAttribute=mail",
                "auth.ldap.displayNameAttribute=cn",
                "auth.ldap.startTls=true",
                "auth.ldap.caCertFile=/etc/provisa/ldap-ca.pem",
            )
        )["auth"]
        assert auth["ldap"]["group_base_dn"] == "ou=groups,dc=corp,dc=example"
        assert auth["ldap"]["group_filter"] == "(member={user_dn})"
        assert auth["ldap"]["group_name_attribute"] == "cn"
        assert auth["ldap"]["email_attribute"] == "mail"
        assert auth["ldap"]["display_name_attribute"] == "cn"
        assert auth["ldap"]["start_tls"] is True
        assert auth["ldap"]["ca_cert_file"] == "/etc/provisa/ldap-ca.pem"

    def test_the_product_builds_the_rendered_block(self, monkeypatch):
        from provisa.auth.providers.ldap import LdapAuthProvider
        from provisa.auth.wiring import build_auth_provider

        monkeypatch.setenv("PROVISA_AUTH_LDAP_BIND_PASSWORD", "from-the-secret")
        monkeypatch.setenv("PROVISA_AUTH_SESSION_SECRET", "s" * 48)
        provider = build_auth_provider(_provisa_yaml(_ok(*_LDAP))["auth"])
        assert isinstance(provider, LdapAuthProvider)

    @pytest.mark.parametrize(
        "missing",
        [
            "auth.ldap.server",
            "auth.ldap.bindDn",
            "auth.ldap.bindPassword.existingSecret",
            "auth.ldap.baseDn",
            "auth.ldap.userFilter",
            "auth.ldap.userIdAttribute",
            "auth.sessionSecret.existingSecret",
        ],
    )
    def test_ldap_without_a_required_value_does_not_render(self, missing):
        r = _render(*[s for s in _LDAP if not s.startswith(missing + "=")])
        assert r.returncode != 0
        assert missing in r.stderr


class TestLocalBreakGlass:
    def test_local_renders_the_break_glass_account_and_no_identity_provider(self):
        auth = _provisa_yaml(_ok(*_LOCAL))["auth"]
        assert auth["provider"] == "basic"
        assert auth["allow_registration"] is False
        assert auth["superuser"] == {
            "username": "platform-admin",
            "password": "${env:PROVISA_AUTH_BREAK_GLASS_PASSWORD}",
        }
        assert auth["jwt_secret"] == "${env:PROVISA_AUTH_SESSION_SECRET}"
        assert not ({"oidc", "ldap", "saml", "firebase"} & set(auth))

    def test_the_break_glass_password_comes_from_a_secret(self):
        env = _api_env(_ok(*_LOCAL))
        assert env["PROVISA_AUTH_BREAK_GLASS_PASSWORD"]["valueFrom"]["secretKeyRef"] == {
            "name": "provisa-break-glass",
            "key": "password",
        }

    def test_local_without_the_account_does_not_render(self):
        r = _render("auth.provider=local", "auth.sessionSecret.existingSecret=provisa-session")
        assert r.returncode != 0
        assert "auth.breakGlass" in r.stderr

    def test_break_glass_is_available_beside_an_identity_provider(self):
        auth = _provisa_yaml(
            _ok(
                *_OIDC,
                "auth.sessionSecret.existingSecret=provisa-session",
                "auth.breakGlass.username=platform-admin",
                "auth.breakGlass.existingSecret=provisa-break-glass",
            )
        )["auth"]
        assert auth["provider"] == "oidc"
        assert auth["superuser"]["username"] == "platform-admin"


class TestCommonSettings:
    def test_default_role_and_role_mapping_are_rendered(self, tmp_path):
        values = tmp_path / "values.yaml"
        values.write_text(
            yaml.safe_dump(
                {
                    "auth": {
                        "provider": "oidc",
                        "defaultRole": "viewer",
                        "oidc": {
                            "issuerUrl": "https://login.corp.example/realms/main",
                            "clientId": "provisa",
                        },
                        "roleMapping": [
                            {
                                "type": "contains",
                                "claim": "groups",
                                "value": "data-admins",
                                "role": "org_admin",
                            }
                        ],
                    }
                }
            )
        )
        helm = shutil.which("helm")
        assert helm is not None
        r = subprocess.run(
            [helm, "template", "rel", str(CHART), "--set", _KEY, "-f", str(values)],
            capture_output=True,
            text=True,
        )
        assert r.returncode == 0, r.stderr
        auth = _provisa_yaml(r.stdout)["auth"]
        assert auth["default_role"] == "viewer"
        assert auth["role_mapping"] == [
            {"type": "contains", "claim": "groups", "value": "data-admins", "role": "org_admin"}
        ]


class TestNoFirebase:
    @pytest.mark.parametrize("sets", [["auth.provider=none"], _OIDC, _LDAP, _LOCAL, _SAML])
    def test_nothing_rendered_mentions_firebase(self, sets):
        assert "firebase" not in _ok(*sets).lower()


class TestSaml:
    def test_the_identity_provider_and_public_address_become_the_saml_block(self):
        auth = _provisa_yaml(_ok(*_SAML))["auth"]
        assert auth["provider"] == "saml"
        assert auth["saml"] == {
            "idp_metadata_url": "https://idp.corp.example/metadata",
            "sp_entity_id": "https://provisa.corp.example/auth/saml/metadata",
            "acs_url": "https://provisa.corp.example/auth/saml/acs",
            "login_page_url": "https://provisa.corp.example/login",
        }
        assert auth["jwt_secret"] == "${env:PROVISA_AUTH_SESSION_SECRET}"

    def test_attributes_are_rendered_when_set(self):
        auth = _provisa_yaml(
            _ok(
                *_SAML,
                "auth.saml.userIdAttribute=uid",
                "auth.saml.emailAttribute=mail",
                "auth.saml.displayNameAttribute=cn",
                "auth.saml.groupsAttribute=groups",
            )
        )["auth"]
        assert auth["saml"]["user_id_attribute"] == "uid"
        assert auth["saml"]["email_attribute"] == "mail"
        assert auth["saml"]["display_name_attribute"] == "cn"
        assert auth["saml"]["groups_attribute"] == "groups"

    def test_the_product_builds_the_rendered_block(self, monkeypatch):
        from provisa.auth.providers.saml import SamlAuthProvider
        from provisa.auth.wiring import build_auth_provider

        monkeypatch.setenv("PROVISA_AUTH_SESSION_SECRET", "s" * 48)
        assert isinstance(build_auth_provider(_provisa_yaml(_ok(*_SAML))["auth"]), SamlAuthProvider)

    @pytest.mark.parametrize(
        "missing",
        ["auth.saml.idpMetadataUrl", "auth.saml.publicUrl", "auth.sessionSecret.existingSecret"],
    )
    def test_saml_without_a_required_value_does_not_render(self, missing):
        r = _render(*[s for s in _SAML if not s.startswith(missing + "=")])
        assert r.returncode != 0
        assert missing in r.stderr
