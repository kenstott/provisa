# Copyright (c) 2026 Kenneth Stott
# Canary: 25f3747b-e88c-49b5-9726-6342563e7a4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A Microsoft 365 source as it is set up and signed in: its settings, the organisation's
client it signs in with, the addresses that carry the directory, the token taken through the
store that keeps a replaced refresh token, and what the table picker lists for it."""

# Requirements: REQ-1923

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from provisa.core import mail_platforms, source_sign_in
from provisa.core.source_sign_in import SignInRefused
from provisa.microsoft365 import SOURCE_TYPE, loader, settings
from provisa.microsoft365.settings import InvalidMicrosoft365Source, parse

TENANT = "0a1b2c3d-1111-2222-3333-444455556666"
GOOD = {
    "accounts": ["megan@contoso.com"],
    "resources": ["mail"],
    "refresh_token": "${secret:source_m365_refresh_token}",
}


# -------------------------------------------------------------------------------- settings


def test_the_settings_a_source_keeps():
    read = parse(GOOD)
    assert read.account == "megan@contoso.com" and read.resources == ("mail",)
    assert read.refresh_token_name == "source_m365_refresh_token"
    assert read.scopes() == [
        "https://graph.microsoft.com/Mail.Read",
        "offline_access",
        "https://graph.microsoft.com/User.Read",
    ]
    assert settings.SECRET_KEYS == ("refresh_token",)


@pytest.mark.parametrize(
    ("change", "says"),
    [
        ({"accounts": []}, "one mailbox"),
        ({"accounts": ["a@b.co", "c@d.co"]}, "one mailbox"),
        ({"accounts": ["not an address"]}, "not a mailbox address"),
        ({"resources": []}, "what is read"),
        ({"resources": ["calendar"]}, "cannot read calendar"),
        ({"refresh_token": ""}, "not connected"),
        ({"refresh_token": "a-literal-token"}, "reference to the organisation's vault"),
        ({"client_secret": "x"}, "unknown setting"),
    ],
)
def test_settings_that_cannot_be_a_source_are_refused_by_name(change, says):
    with pytest.raises(InvalidMicrosoft365Source, match=says) as refused:
        parse({**GOOD, **change})
    assert "a-literal-token" not in str(refused.value)


def test_a_save_is_refused_in_the_setups_terms():
    from provisa.api.admin.schema_common import SOURCE_MAPPING_SECRET_KEYS, microsoft_365_refusal

    def source(mapping) -> SimpleNamespace:
        return SimpleNamespace(id="m365", type=SOURCE_TYPE, mapping_json=json.dumps(mapping))

    refused = microsoft_365_refusal(source({**GOOD, "refresh_token": ""}))
    assert refused is not None and refused.code == "schema.microsoft_365_invalid"
    assert "not connected" in refused.message and refused.params["source"] == "m365"
    assert microsoft_365_refusal(source(GOOD)) is None
    other = SimpleNamespace(id="pg", type="postgresql", mapping_json="{}")
    assert microsoft_365_refusal(other) is None
    assert SOURCE_MAPPING_SECRET_KEYS[SOURCE_TYPE] == settings.SECRET_KEYS


# --------------------------------------------------------------------------------- sign-in


def test_the_platform_and_the_sign_in_kind_carry_the_source_types_name():
    import provisa.microsoft365.sign_in  # noqa: F401  (registers the kind)

    assert mail_platforms.platform(SOURCE_TYPE).settings == ("tenant",)
    assert source_sign_in.KINDS[SOURCE_TYPE].id == SOURCE_TYPE == "microsoft_365"


@pytest.mark.parametrize("tenant", [TENANT, "contoso.onmicrosoft.com", "contoso.com"])
def test_the_addresses_carry_the_organisations_directory(tenant):
    from provisa.microsoft365.sign_in import MICROSOFT

    base = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0"
    assert MICROSOFT.authorization_endpoint({"tenant": tenant}) == f"{base}/authorize"
    assert MICROSOFT.token_endpoint({"tenant": tenant}) == f"{base}/token"


@pytest.mark.parametrize("tenant", ["", "common/../x", "a b", "evil.example/path?x=", "x", None])
def test_only_a_directory_id_or_domain_goes_into_an_address(tenant):
    from provisa.microsoft365.sign_in import MICROSOFT

    stated = {} if tenant is None else {"tenant": tenant}
    with pytest.raises(SignInRefused) as refused:
        MICROSOFT.token_endpoint(stated)
    assert refused.value.code == "source_sign_in.tenant_invalid"


def test_the_person_is_offered_the_account_the_source_names():
    from provisa.microsoft365.sign_in import MICROSOFT

    assert MICROSOFT.authorization_params("megan@contoso.com") == {
        "login_hint": "megan@contoso.com",
        "prompt": "select_account",
    }


# ------------------------------------------------------------------------- the connection


@pytest.fixture
def signed_in(monkeypatch):
    """What `connect` reaches: the organisation's client, the vault, the store, the exchange."""
    from provisa.api_source import oauth_grants, oauth_store
    from provisa.core import config_loader, request_context, secrets

    calls = SimpleNamespace(required=[], stored=[], exchanged=[])

    async def require(admin_db, org_id, platform_id):
        calls.required.append((org_id, platform_id))
        return mail_platforms.Configured(platform_id, "client-id", {"tenant": TENANT})

    async def stored_access_token(admin_db, platform_url, org_id, **kw):
        calls.stored.append({"platform_url": platform_url, "org_id": org_id, **kw})
        grant = await asyncio.to_thread(kw["exchange"], "rt-from-the-vault")
        return grant.access_token

    def exchange_refresh_token(auth):
        calls.exchanged.append(auth)
        return oauth_grants.RefreshGrant("access-1", 3600.0, "rt-replacement")

    monkeypatch.setattr(mail_platforms, "require", require)
    monkeypatch.setattr(oauth_store, "stored_access_token", stored_access_token)
    monkeypatch.setattr(oauth_grants, "exchange_refresh_token", exchange_refresh_token)
    monkeypatch.setattr(secrets, "resolve_secrets", lambda value: "the-client-secret")
    monkeypatch.setattr(request_context, "require_current_org", lambda: "acme")
    monkeypatch.setattr(
        config_loader,
        "load_control_plane",
        lambda path: SimpleNamespace(resolved_platform_url=lambda: "sqlite:///platform.db"),
    )
    return calls


def test_a_source_is_signed_in_with_the_organisations_client_through_the_store(signed_in):
    state = SimpleNamespace(admin_db=object())
    source = SimpleNamespace(id="m365", mapping=GOOD)

    async def run():
        account, token = await loader.make_connect(state)(source)
        return account, await asyncio.to_thread(token)  # Graph is read off the loop

    account, token = asyncio.run(run())
    assert (account, token) == ("megan@contoso.com", "access-1")
    assert signed_in.required == [("acme", SOURCE_TYPE)]
    (stored,) = signed_in.stored
    assert stored["org_id"] == "acme" and stored["source_id"] == "m365"
    assert stored["secret_name"] == "source_m365_refresh_token"
    assert stored["replaces"] is True  # Microsoft replaces a refresh token on every use
    assert stored["platform_url"] == "sqlite:///platform.db"
    (auth,) = signed_in.exchanged
    assert auth.refresh_token == "rt-from-the-vault"  # what the store read, not the mapping's
    assert auth.client_id == "client-id" and auth.client_secret == "the-client-secret"
    assert auth.token_url == f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token"
    assert auth.scope == (
        "https://graph.microsoft.com/Mail.Read offline_access https://graph.microsoft.com/User.Read"
    )


def test_an_organisation_with_no_client_is_refused_by_name(signed_in, monkeypatch):
    async def require(admin_db, org_id, platform_id):
        raise mail_platforms.MailPlatformRefused("not_configured", "no client entered")

    monkeypatch.setattr(mail_platforms, "require", require)
    connect = loader.make_connect(SimpleNamespace(admin_db=object()))
    with pytest.raises(mail_platforms.MailPlatformRefused):
        asyncio.run(connect(SimpleNamespace(id="m365", mapping=GOOD)))
    assert signed_in.stored == []


def test_a_source_that_is_not_connected_is_not_read(signed_in):
    connect = loader.make_connect(SimpleNamespace(admin_db=object()))
    with pytest.raises(InvalidMicrosoft365Source, match="not connected"):
        asyncio.run(connect(SimpleNamespace(id="m365", mapping={**GOOD, "refresh_token": ""})))
    assert signed_in.required == []


# ------------------------------------------------------------------------ the table picker


def test_the_table_picker_lists_one_schema_and_the_six_canonical_tables():
    from provisa.api.admin.introspect import native_schemas, native_tables
    from provisa.core import canonical_mail as cm

    async def run():
        schemas = await native_schemas("m365", SOURCE_TYPE, None, None)  # type: ignore[arg-type]
        tables = await native_tables("m365", SOURCE_TYPE, "default", None, None, None)  # type: ignore[arg-type]
        elsewhere = await native_tables("m365", SOURCE_TYPE, "other", None, None, None)  # type: ignore[arg-type]
        return schemas, tables, elsewhere

    schemas, tables, elsewhere = asyncio.run(run())
    assert schemas == ["default"]
    assert [t.name for t in tables] == list(loader.TABLES)
    assert [t.comment for t in tables] == [cm.TABLES[name].note for name in loader.TABLES]
    assert elsewhere == []


def test_the_loader_is_given_to_the_product():
    import inspect

    from provisa.events import app_wiring

    assert 'loaders["microsoft_365"] = make_microsoft365_loader(make_connect(state))' in (
        inspect.getsource(app_wiring)
    )
