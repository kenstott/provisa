# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The claim carrying the nested entitlement lists is named per identity provider, in that
provider's settings; with no name set, that provider sends no nested lists."""

# Requirements: REQ-2219

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin.auth_settings import (
    AUTH_PROVIDERS,
    apply_auth_settings,
    provider_config_view,
)
from provisa.auth.models import AuthIdentity
from provisa.auth.wiring import build_auth_provider
from provisa.entitlements import runtime
from provisa.entitlements.lists import (
    EntitlementClaimError,
    nested_claim_name,
    parse_idp_entitlements,
)

_NESTED = {"viewer": {"tables": ["finance.revenue"]}}
_FLAT = ["provisa:viewer:columns:sales.orders.id", "provisa:analyst:tables:"]
_FLAT_LISTS = {"viewer": {"columns": ("sales.orders.id",)}, "analyst": {"tables": ()}}


# -- reading the named claim ----------------------------------------------------------------------


def test_a_provider_with_a_claim_name_reads_nested_lists_from_that_claim():
    claims = {"acme_access": _NESTED}
    assert parse_idp_entitlements(claims, nested_claim="acme_access") == {
        "viewer": {"tables": ("finance.revenue",)}
    }


def test_a_provider_with_a_name_does_not_read_any_other_claim_as_nested():
    claims = {"provisa_entitlements": _NESTED, "other": _NESTED}
    assert parse_idp_entitlements(claims, nested_claim="acme_access") == {}


def test_a_provider_with_no_name_reads_no_nested_lists_there_is_no_default_name():
    claims = {"provisa_entitlements": _NESTED, "acme_access": _NESTED}
    assert parse_idp_entitlements(claims, nested_claim=None) == {}


def test_with_no_name_a_map_claim_is_not_read_at_all_so_it_cannot_refuse_the_sign_in():
    assert parse_idp_entitlements({"provisa_entitlements": "garbage"}, nested_claim=None) == {}


@pytest.mark.parametrize("name", ["acme_access", None])
def test_flat_entries_are_read_with_or_without_a_name(name):
    assert parse_idp_entitlements({"groups": _FLAT}, nested_claim=name) == _FLAT_LISTS


def test_flat_and_named_nested_forms_combine():
    claims = {
        "acme_access": {"viewer": {"tables": ["sales.*"]}},
        "groups": ["provisa:viewer:tables:finance.revenue"],
    }
    assert parse_idp_entitlements(claims, nested_claim="acme_access")["viewer"]["tables"] == (
        "sales.*",
        "finance.revenue",
    )


def test_a_malformed_named_claim_is_refused_naming_that_claim():
    with pytest.raises(EntitlementClaimError, match="acme_access"):
        parse_idp_entitlements({"acme_access": ["not", "a", "map"]}, nested_claim="acme_access")


# -- the setting ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "settings",
    [{}, {"entitlements_claim": None}, {"entitlements_claim": ""}, {"entitlements_claim": "  "}],
)
def test_an_unset_or_blank_setting_is_no_name(settings):
    assert nested_claim_name(settings) is None


def test_a_set_name_is_read_trimmed():
    assert nested_claim_name({"entitlements_claim": " acme_access "}) == "acme_access"


def test_a_setting_that_is_not_text_is_refused():
    with pytest.raises(ValueError, match="entitlements_claim"):
        nested_claim_name({"entitlements_claim": ["acme_access"]})


# -- the settings API -----------------------------------------------------------------------------

_CLAIM_PROVIDERS = {"firebase", "keycloak", "oauth"}


def _field(provider: str) -> dict | None:
    entry = next(p for p in AUTH_PROVIDERS if p["key"] == provider)
    return next(
        (f for f in entry["config_fields"] if f["config_key"] == "entitlements_claim"), None
    )


@pytest.mark.parametrize("provider", sorted(_CLAIM_PROVIDERS))
def test_a_provider_whose_token_carries_claims_offers_the_setting(provider):
    field = _field(provider)
    assert field is not None
    assert field["type"] == "string"
    assert field["required"] is False
    # A placeholder would read as a default name, and there is none.
    assert "placeholder" not in field


@pytest.mark.parametrize("provider", ["none", "basic", "ldap", "saml", "simple"])
def test_a_provider_that_carries_no_nested_claim_does_not_offer_it(provider):
    assert _field(provider) is None


def test_a_saved_name_is_kept_on_that_providers_own_settings_and_read_back():
    saved = apply_auth_settings(
        {},
        {
            "provider": "oauth",
            "config": {
                "discovery_url": "https://i/.well-known/openid-configuration",
                "client_id": "c",
                "entitlements_claim": "acme_access",
            },
        },
    )
    assert saved["oauth"]["entitlements_claim"] == "acme_access"
    assert provider_config_view(saved)["oauth"]["entitlements_claim"] == "acme_access"
    assert "entitlements_claim" not in provider_config_view(saved)["keycloak"]


# -- from the settings to the identity ------------------------------------------------------------


def _oauth_config(**settings) -> dict:
    return {
        "provider": "oauth",
        "oauth": {"discovery_url": "https://i/d", "client_id": "c", **settings},
    }


def _keycloak_config(**settings) -> dict:
    return {
        "provider": "keycloak",
        "keycloak": {"server_url": "https://k", "realm": "r", "client_id": "c", **settings},
    }


def _signed_in(monkeypatch, provider, module, decoded):
    class _Key:
        key = "k"

    class _Jwks:
        def get_signing_key_from_jwt(self, _token):
            return _Key()

    monkeypatch.setattr(provider, "_get_jwks_client", lambda: _Jwks())
    monkeypatch.setattr(module.jwt, "decode", lambda *a, **k: decoded)


async def _oauth_identity(monkeypatch, config, decoded) -> AuthIdentity:
    from provisa.auth.providers import oauth as module

    provider = build_auth_provider(config)
    _signed_in(monkeypatch, provider, module, decoded)
    monkeypatch.setattr(provider, "_discovery", lambda: {"issuer": "https://i"})
    return await provider.validate_token("t")


async def _keycloak_identity(monkeypatch, config, decoded) -> AuthIdentity:
    from provisa.auth.providers import keycloak as module

    provider = build_auth_provider(config)
    _signed_in(monkeypatch, provider, module, decoded)
    return await provider.validate_token("t")


@pytest.mark.parametrize(
    ("sign_in", "config"),
    [(_oauth_identity, _oauth_config), (_keycloak_identity, _keycloak_config)],
)
@pytest.mark.asyncio
async def test_the_request_reads_the_claim_its_provider_names(monkeypatch, sign_in, config):
    decoded = {"sub": "u", "acme_access": _NESTED, "groups": _FLAT}
    identity = await sign_in(monkeypatch, config(entitlements_claim="acme_access"), decoded)
    with runtime.entitlements_scope(identity):
        bound = runtime._current.get()
    assert bound is not None
    assert bound.idp == {
        "viewer": {"tables": ("finance.revenue",), "columns": ("sales.orders.id",)},
        "analyst": {"tables": ()},
    }


@pytest.mark.parametrize(
    ("sign_in", "config"),
    [(_oauth_identity, _oauth_config), (_keycloak_identity, _keycloak_config)],
)
@pytest.mark.asyncio
async def test_the_request_of_a_provider_with_no_name_reads_only_flat_entries(
    monkeypatch, sign_in, config
):
    decoded = {"sub": "u", "acme_access": _NESTED, "groups": _FLAT}
    identity = await sign_in(monkeypatch, config(), decoded)
    with runtime.entitlements_scope(identity):
        bound = runtime._current.get()
    assert bound is not None
    assert bound.idp == _FLAT_LISTS


@pytest.mark.asyncio
async def test_firebase_carries_the_name_its_settings_give(monkeypatch):
    from provisa.auth.providers import firebase as module
    from provisa.auth.providers.firebase import FirebaseAuthProvider

    class _Auth:
        def verify_id_token(self, _token):
            return {"uid": "u", "acme_access": _NESTED}

    monkeypatch.setattr(module, "firebase_auth", _Auth())
    provider = FirebaseAuthProvider.__new__(FirebaseAuthProvider)
    provider._entitlements_claim = "acme_access"
    identity = await provider.validate_token("t")
    assert identity.entitlements_claim == "acme_access"


def test_the_oidc_selector_reads_the_name_from_its_own_block():
    provider = build_auth_provider(
        {
            "provider": "oidc",
            "oidc": {
                "discovery_url": "https://i/d",
                "client_id": "c",
                "entitlements_claim": "acme_access",
            },
        }
    )
    assert provider._entitlements_claim == "acme_access"


def test_an_identity_no_provider_named_a_claim_for_reads_no_nested_lists():
    identity = SimpleNamespace(
        user_id="u", raw_claims={"provisa_entitlements": _NESTED}, entitlements_claim=None
    )
    with runtime.entitlements_scope(identity):
        bound = runtime._current.get()
    assert bound is not None
    assert bound.idp == {}
