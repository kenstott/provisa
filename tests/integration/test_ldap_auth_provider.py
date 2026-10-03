# Copyright (c) 2026 Kenneth Stott
# Canary: fcf0fae2-10da-43b7-8daf-03edd6f926bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1265: the LDAP auth provider, against a real OpenLDAP directory.

The directory is the ``openldap`` service of the isolated test stack, loaded from
tests/fixtures/ldap/directory.ldif: alice (analysts), bob (analysts, stewards), carol (no
groups). Nothing here stands in for the directory.
"""

from __future__ import annotations

import base64
import os
import threading
import time

import httpx
import pytest
from fastapi import FastAPI

pytestmark = [pytest.mark.integration, pytest.mark.requires_ldap]

_BASE = "dc=provisa,dc=test"
_ADMIN_DN = f"cn=admin,{_BASE}"
_ADMIN_PASSWORD = "admin-password"
_JWT_SECRET = "ldap-session-signing-secret-of-48-bytes-or-more!"
_ALICE_DN = f"uid=alice,ou=people,{_BASE}"
_STEWARDS_DN = f"cn=stewards,ou=groups,{_BASE}"


def _server_url() -> str:
    return f"ldap://localhost:{os.environ['LDAP_PORT']}"


def _config(**overrides) -> dict:
    return {
        "server_url": _server_url(),
        "bind_dn": _ADMIN_DN,
        "bind_password": _ADMIN_PASSWORD,
        "user_base_dn": f"ou=people,{_BASE}",
        "user_filter": "(uid={username})",
        "user_id_attribute": "uid",
        "email_attribute": "mail",
        "display_name_attribute": "cn",
        "group_base_dn": f"ou=groups,{_BASE}",
        "group_filter": "(member={user_dn})",
        "group_name_attribute": "cn",
        **overrides,
    }


def _provider(session_secret: str | None = _JWT_SECRET, **overrides):
    from provisa.auth.providers.ldap import LdapAuthProvider, LdapSettings

    return LdapAuthProvider(LdapSettings(**_config(**overrides)), session_secret=session_secret)


def _basic(username: str, password: str) -> str:
    return base64.b64encode(f"{username}:{password}".encode()).decode()


class TestSignIn:
    async def test_a_user_signs_in_with_the_directory_password(self):
        identity = await _provider().authenticate("alice", "alice-password")

        assert identity.user_id == "alice"
        assert identity.email == "alice@provisa.test"
        assert identity.display_name == "Alice Analyst"
        assert identity.roles == ["analysts"]
        assert identity.raw_claims["groups"] == ["analysts"]
        assert identity.raw_claims["dn"] == _ALICE_DN

    async def test_every_group_of_the_user_is_a_role(self):
        identity = await _provider().authenticate("bob", "bob-password")
        assert identity.roles == ["analysts", "stewards"]

    async def test_a_user_in_no_group_has_no_roles(self):
        identity = await _provider().authenticate("carol", "carol-password")
        assert identity.roles == []

    async def test_without_group_settings_no_groups_are_read(self):
        provider = _provider(group_base_dn=None, group_filter=None, group_name_attribute=None)
        identity = await provider.authenticate("bob", "bob-password")
        assert identity.roles == []

    async def test_http_basic_presents_the_same_credential(self):
        identity = await _provider().validate_token(_basic("alice", "alice-password"))
        assert identity.user_id == "alice"


class TestRefusals:
    async def test_a_wrong_password_is_refused(self):
        with pytest.raises(ValueError, match="Invalid credentials"):
            await _provider().authenticate("alice", "not-her-password")

    async def test_an_unknown_user_is_refused(self):
        with pytest.raises(ValueError, match="Invalid credentials"):
            await _provider().authenticate("mallory", "anything")

    async def test_an_empty_password_is_refused(self):
        with pytest.raises(ValueError, match="Invalid credentials"):
            await _provider().authenticate("alice", "")

    async def test_another_users_password_is_refused(self):
        with pytest.raises(ValueError, match="Invalid credentials"):
            await _provider().authenticate("alice", "bob-password")

    @pytest.mark.parametrize("username", ["*", "ali*", "alice)(uid=*", "*)(objectClass=*"])
    async def test_filter_syntax_in_the_username_matches_no_one(self, username):
        with pytest.raises(ValueError, match="Invalid credentials"):
            await _provider().authenticate(username, "alice-password")


class TestOperatorFaults:
    async def test_a_service_account_that_cannot_bind_is_not_a_refused_login(self):
        provider = _provider(bind_password="not-the-admin-password")
        with pytest.raises(RuntimeError, match="could not bind"):
            await provider.authenticate("alice", "alice-password")

    async def test_a_filter_matching_several_users_is_not_a_refused_login(self):
        provider = _provider(user_filter="(|(uid={username})(objectClass=inetOrgPerson))")
        with pytest.raises(RuntimeError, match="exactly one"):
            await provider.authenticate("alice", "alice-password")

    async def test_an_unreachable_directory_is_not_a_refused_login(self):
        from tests.port_lease import lease_port

        provider = _provider(server_url=f"ldap://localhost:{lease_port()}")
        with pytest.raises(Exception) as raised:
            await provider.authenticate("alice", "alice-password")
        assert not isinstance(raised.value, ValueError)

    def test_group_settings_are_set_together(self):
        with pytest.raises(ValueError, match="together"):
            _provider(group_filter=None)


class TestWiring:
    def test_the_configured_provider_is_built_with_its_secret_resolved(self, monkeypatch):
        from provisa.auth.providers.ldap import LdapAuthProvider
        from provisa.auth.wiring import build_auth_provider

        monkeypatch.setenv("LDAP_TEST_BIND_PASSWORD", _ADMIN_PASSWORD)
        provider = build_auth_provider(
            {
                "provider": "ldap",
                "ldap": _config(bind_password="${env:LDAP_TEST_BIND_PASSWORD}"),
                "jwt_secret": _JWT_SECRET,
            }
        )

        assert isinstance(provider, LdapAuthProvider)
        assert set(provider.token_validators) == {"basic", "bearer"}

    async def test_the_built_provider_signs_a_user_in(self, monkeypatch):
        from provisa.auth.wiring import build_auth_provider

        monkeypatch.setenv("LDAP_TEST_BIND_PASSWORD", _ADMIN_PASSWORD)
        provider = build_auth_provider(
            {
                "provider": "ldap",
                "ldap": _config(bind_password="${env:LDAP_TEST_BIND_PASSWORD}"),
                "jwt_secret": _JWT_SECRET,
            }
        )
        identity = await provider.validate_token(_basic("bob", "bob-password"))
        assert identity.roles == ["analysts", "stewards"]


@pytest.fixture()
def login_server(monkeypatch):
    """A real uvicorn server serving /auth/login for a deployment configured with the LDAP
    provider, read from the app state the way a started deployment holds it."""
    import uvicorn

    from provisa.api.app import state
    from provisa.auth.login_router import router as login_router
    from tests.port_lease import lease_port

    monkeypatch.setattr(
        state, "auth_config", {"provider": "ldap", "ldap": _config(), "jwt_secret": _JWT_SECRET}
    )
    monkeypatch.setattr(state, "admin_db", None)
    app = FastAPI()
    app.include_router(login_router)
    port = lease_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and not server.started:
            time.sleep(0.1)
        if not server.started:
            raise RuntimeError(f"login server did not start on {port} within 120s")
        yield f"http://127.0.0.1:{port}", _provider()
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def _admin_connection():
    from ldap3 import Connection, Server

    conn = Connection(Server(_server_url()), user=_ADMIN_DN, password=_ADMIN_PASSWORD)
    assert conn.bind(), conn.result
    return conn


class TestBrowserSession:
    async def test_login_issues_a_token_the_provider_accepts(self, login_server):
        url, provider = login_server
        resp = httpx.post(
            f"{url}/auth/login", json={"username": "alice", "password": "alice-password"}
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["token_type"] == "bearer"

        identity = await provider.validate_bearer(body["access_token"])
        assert identity.user_id == "alice"
        assert identity.roles == ["analysts"]

    def test_a_wrong_password_gets_no_token(self, login_server):
        url, _provider_ = login_server
        resp = httpx.post(f"{url}/auth/login", json={"username": "alice", "password": "nope"})
        assert resp.status_code == 401
        assert "access_token" not in resp.json()

    async def test_a_group_change_takes_effect_on_a_live_session(self, login_server):
        from ldap3 import MODIFY_ADD, MODIFY_DELETE

        url, provider = login_server
        token = httpx.post(
            f"{url}/auth/login", json={"username": "alice", "password": "alice-password"}
        ).json()["access_token"]
        assert (await provider.validate_bearer(token)).roles == ["analysts"]

        admin = _admin_connection()
        try:
            assert admin.modify(_STEWARDS_DN, {"member": [(MODIFY_ADD, [_ALICE_DN])]})
            try:
                assert (await provider.validate_bearer(token)).roles == ["analysts", "stewards"]
            finally:
                assert admin.modify(_STEWARDS_DN, {"member": [(MODIFY_DELETE, [_ALICE_DN])]})
            assert (await provider.validate_bearer(token)).roles == ["analysts"]
        finally:
            admin.unbind()

    async def test_a_token_signed_with_another_key_is_refused(self, login_server):
        import jwt

        _url, provider = login_server
        forged = jwt.encode(
            {"sub": "alice", "username": "alice", "aud": "ldap", "exp": time.time() + 600},
            "some-other-signing-secret-of-48-bytes-or-more!!!",
            algorithm="HS256",
        )
        with pytest.raises(jwt.InvalidSignatureError):
            await provider.validate_bearer(forged)

    def test_no_signing_secret_means_no_session_rather_than_a_default_key(self):
        import asyncio

        from provisa.api.errors import ApiError

        provider = _provider(session_secret=None)
        with pytest.raises(ApiError) as refused:
            asyncio.run(provider.issue_session_token("alice", "alice-password"))
        assert refused.value.status_code == 503
