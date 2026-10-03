# Copyright (c) 2026 Kenneth Stott
# Canary: 9e59a6c6-476a-4da0-9422-bc7339129976
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1265: the SAML auth provider, signing in through a real identity provider.

The identity provider is the ``saml-idp`` service of the isolated test stack (SimpleSAMLphp,
users user1/password and user2/password). The test plays the browser: it follows the redirect
to the identity provider, submits the sign-in form there, and posts what comes back to the
service provider's ACS, exactly as the page's auto-submitting form would.
"""

from __future__ import annotations

import base64
import os
import threading
import time
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
import pytest
from fastapi import FastAPI
from lxml import html  # pyright: ignore[reportAttributeAccessIssue]

pytestmark = [pytest.mark.integration, pytest.mark.requires_saml_idp]

_JWT_SECRET = "saml-session-signing-secret-of-48-bytes-or-more!"
_TIMEOUT = 60


def _sp_base() -> str:
    return f"http://localhost:{os.environ['SAML_SP_PORT']}"


def _idp_base() -> str:
    return f"http://localhost:{os.environ['SAML_IDP_PORT']}"


@pytest.fixture(scope="module")
def service_provider():
    """The SAML routes on a real server, at the address the identity provider was given."""
    import uvicorn

    from provisa.auth.providers import saml as saml_mod

    from provisa.api.app import state

    settings = {
        "sp_entity_id": f"{_sp_base()}/auth/saml/metadata",
        "acs_url": f"{_sp_base()}/auth/saml/acs",
        "login_page_url": f"{_sp_base()}/login",
        "idp_metadata_url": f"{_idp_base()}/simplesaml/saml2/idp/metadata.php",
        "user_id_attribute": "uid",
        "email_attribute": "email",
        "groups_attribute": "eduPersonAffiliation",
    }
    patch = pytest.MonkeyPatch()
    patch.setattr(
        state,
        "auth_config",
        {"provider": "saml", "saml": settings, "jwt_secret": _JWT_SECRET},
    )
    patch.setattr(state, "auth_reconfig_generation", state.auth_reconfig_generation + 1)
    app = FastAPI()
    app.include_router(saml_mod.router)
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=int(os.environ["SAML_SP_PORT"]), log_level="warning"
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and not server.started:
            time.sleep(0.1)
        if not server.started:
            raise RuntimeError("the service-provider server did not start within 120s")
        yield saml_mod._require_provider()
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        patch.undo()


def _attributes_in(saml_response: str) -> dict[str, list[str]]:
    """The attributes the identity provider sent, by Name, for a failure message."""
    from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]

    root = etree.fromstring(base64.b64decode(saml_response))
    ns = {"saml": "urn:oasis:names:tc:SAML:2.0:assertion"}
    return {
        a.get("Name"): [v.text for v in a.findall("saml:AttributeValue", ns)]
        for a in root.iterfind(".//saml:Attribute", ns)
    }


def _form(page: httpx.Response, containing: str):
    """The form on ``page`` that has an input named ``containing``: (action URL, fields)."""
    document = html.fromstring(page.text)
    for form in document.forms:
        fields = dict(form.fields)
        if containing in fields:
            return urljoin(str(page.url), form.action or ""), fields
    raise AssertionError(
        f"no form with a {containing!r} input at {page.url} (HTTP {page.status_code}):\n"
        f"{page.text[:1500]}"
    )


def _sign_in_at_idp(username: str, password: str) -> httpx.Response:
    """Follow /auth/saml/login to the identity provider and submit its sign-in form."""
    with httpx.Client(follow_redirects=True, timeout=_TIMEOUT) as browser:
        login_page = browser.get(f"{_sp_base()}/auth/saml/login")
        assert urlparse(str(login_page.url)).port == int(os.environ["SAML_IDP_PORT"]), (
            f"the browser was not sent to the identity provider: {login_page.url}"
        )
        action, fields = _form(login_page, "password")
        return browser.post(action, data={**fields, "username": username, "password": password})


def _posted_back(username: str = "user1", password: str = "password") -> tuple[str, dict]:
    """The ACS URL and form fields the identity provider hands the browser after sign-in."""
    return _form(_sign_in_at_idp(username, password), "SAMLResponse")


def _post_to_acs(action: str, fields: dict) -> dict[str, list[str]]:
    """Post to the ACS; return the fragment of the sign-in page URL the browser is sent to."""
    resp = httpx.post(action, data=fields, follow_redirects=False, timeout=_TIMEOUT)
    assert resp.status_code == 303, resp.text
    location = urlparse(resp.headers["location"])
    assert f"{location.scheme}://{location.netloc}{location.path}" == f"{_sp_base()}/login"
    return parse_qs(location.fragment)


class TestSignIn:
    async def test_a_user_signs_in_through_the_identity_provider(self, service_provider):
        action, fields = _posted_back("user1", "password")
        assert action == f"{_sp_base()}/auth/saml/acs"

        attributes = _attributes_in(fields["SAMLResponse"])
        fragment = _post_to_acs(action, fields)
        assert "saml_error" not in fragment
        identity = await service_provider.validate_token(fragment["saml_token"][0])

        assert identity.user_id == "user1"
        assert identity.email == "user1@example.com", attributes
        assert identity.roles == ["group1"], attributes

    async def test_another_user_gets_their_own_identity(self, service_provider):
        fragment = _post_to_acs(*_posted_back("user2", "password"))
        identity = await service_provider.validate_token(fragment["saml_token"][0])
        assert identity.email == "user2@example.com"
        assert identity.roles == ["group2"]

    def test_a_wrong_password_never_reaches_the_service(self, service_provider):
        page = _sign_in_at_idp("user1", "not-the-password")
        assert "SAMLResponse" not in page.text


class TestRefusals:
    def test_the_same_response_is_refused_a_second_time(self, service_provider):
        action, fields = _posted_back()
        assert "saml_token" in _post_to_acs(action, fields)
        assert _post_to_acs(action, fields) == {"saml_error": ["rejected"]}

    def test_a_response_changed_in_transit_is_refused(self, service_provider):
        action, fields = _posted_back()
        document = base64.b64decode(fields["SAMLResponse"])
        assert b"user1@example.com" in document
        changed = document.replace(b"user1@example.com", b"admin@example.com")
        fields["SAMLResponse"] = base64.b64encode(changed).decode()
        assert _post_to_acs(action, fields) == {"saml_error": ["rejected"]}

    def test_a_response_without_its_relay_state_is_refused(self, service_provider):
        action, fields = _posted_back()
        fields["RelayState"] = ""
        assert _post_to_acs(action, fields) == {"saml_error": ["rejected"]}

    def test_a_response_with_another_sign_ins_relay_state_is_refused(self, service_provider):
        _action, other = _posted_back("user2", "password")
        action, fields = _posted_back("user1", "password")
        fields["RelayState"] = other["RelayState"]
        assert _post_to_acs(action, fields) == {"saml_error": ["rejected"]}


class TestMetadata:
    def test_the_service_describes_itself(self, service_provider):
        resp = httpx.get(f"{_sp_base()}/auth/saml/metadata", timeout=_TIMEOUT)
        assert resp.status_code == 200
        assert f'entityID="{_sp_base()}/auth/saml/metadata"' in resp.text
        assert f'Location="{_sp_base()}/auth/saml/acs"' in resp.text
