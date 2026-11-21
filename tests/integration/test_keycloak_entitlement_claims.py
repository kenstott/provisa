# Copyright (c) 2026 Kenneth Stott
# Canary: 45d362e1-6fea-4db8-9df8-e1e65928eb60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, REQ-122: entitlement lists as a real Keycloak issues them, nested and flat.

The realm is the ``keycloak`` service of the isolated test stack, imported from
tests/fixtures/keycloak/provisa-test-realm.json: alice carries the nested claim (a user attribute
mapped as JSON), bob the flat form (groups named ``provisa:<role>:<kind>:<id>``), carol neither,
and dana both, on different role grants. Nothing here stands in for the identity provider.
"""

from __future__ import annotations

import os

import httpx
import pytest

from provisa.auth.providers.keycloak import KeycloakAuthProvider
from provisa.entitlements.lists import parse_idp_entitlements

pytestmark = [pytest.mark.integration, pytest.mark.requires_keycloak]

_REALM = "provisa-test"
_CLIENT = "provisa-test"
_NESTED_CLAIM = "provisa_entitlements"


def _server_url() -> str:
    return f"http://localhost:{os.environ['KEYCLOAK_PORT']}"


def _provider() -> KeycloakAuthProvider:
    return KeycloakAuthProvider(_server_url(), _REALM, _CLIENT, entitlements_claim=_NESTED_CLAIM)


async def _token(username: str) -> str:
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{_server_url()}/realms/{_REALM}/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": _CLIENT,
                "username": username,
                "password": f"{username}-password",
                "scope": "openid",
            },
        )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


async def _signed_in(username: str):
    return await _provider().validate_token(await _token(username))


def _lists(identity) -> dict:
    return parse_idp_entitlements(identity.raw_claims, identity.entitlements_claim)


async def test_the_nested_claim_is_read_per_role_and_kind():
    identity = await _signed_in("alice")
    assert "viewer" in identity.roles
    assert _lists(identity) == {
        "viewer": {"tables": ("finance.revenue",), "columns": ("finance.revenue.id",)}
    }


async def test_the_flat_form_is_read_from_the_groups_claim():
    identity = await _signed_in("bob")
    lists = _lists(identity)
    assert set(lists["viewer"]["tables"]) == {"finance.revenue", "sales.orders"}


async def test_a_flat_entry_with_no_identifier_is_an_explicit_empty_list():
    assert _lists(await _signed_in("bob"))["viewer"]["relationships"] == ()


async def test_a_plain_role_grant_carries_no_lists():
    identity = await _signed_in("carol")
    assert "viewer" in identity.roles
    assert _lists(identity) == {}


async def test_nested_and_flat_combine_across_role_grants():
    identity = await _signed_in("dana")
    assert {"viewer", "analyst"} <= set(identity.roles)
    assert _lists(identity) == {
        "analyst": {"tables": ("finance.*",)},
        "viewer": {"tables": ("sales.orders",)},
    }


async def test_a_provider_naming_no_claim_reads_only_the_flat_form():
    provider = KeycloakAuthProvider(_server_url(), _REALM, _CLIENT)
    identity = await provider.validate_token(await _token("dana"))
    assert _lists(identity) == {"viewer": {"tables": ("sales.orders",)}}
