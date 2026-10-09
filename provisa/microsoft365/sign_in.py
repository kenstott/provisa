# Copyright (c) 2026 Kenneth Stott
# Canary: 348e2fce-1e24-4e46-84c7-31bd6da1a703
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a person approves a Microsoft 365 source (REQ-1923): what is particular to Microsoft in
the sign-in every such source kind shares (``provisa.core.source_sign_in``). The addresses carry
the directory (tenant) the organisation's client is registered in."""

# Requirements: REQ-1923
from __future__ import annotations

from collections.abc import Mapping

import httpx

from provisa.core.source_sign_in import SignInKind, SignInRefused, register_kind
from provisa.microsoft365 import SOURCE_TYPE
from provisa.microsoft365.settings import (
    InvalidMicrosoft365Source,
    authorization_url,
    token_url,
)

PROFILE_URL = "https://graph.microsoft.com/v1.0/me?$select=mail,userPrincipalName"
_TIMEOUT = 10.0


def _endpoint(make) -> "object":
    def endpoint(settings: Mapping[str, str]) -> str:
        try:
            return make(settings.get("tenant"))
        except InvalidMicrosoft365Source as exc:
            raise SignInRefused("tenant_invalid", str(exc)) from None

    return endpoint


def _authorization_params(account: str) -> dict[str, str]:
    # login_hint: the account the source names is the one offered. select_account: a person
    # signed in to several accounts chooses, instead of the browser's current one being taken.
    return {"login_hint": account, "prompt": "select_account"}


async def _approved_account(access_token: str) -> str:
    """The mailbox the token reads, by Graph's own account of who signed in."""
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        answer = await client.get(PROFILE_URL, headers={"Authorization": f"Bearer {access_token}"})
    if answer.status_code != 200:
        raise SignInRefused(
            "microsoft_account_unread",
            f"Microsoft approved the sign-in and would not say whose mailbox it is "
            f"(HTTP {answer.status_code}).",
        )
    profile = answer.json()
    account = profile.get("mail") or profile.get("userPrincipalName")
    if not account:
        raise SignInRefused(
            "microsoft_account_unread",
            "Microsoft approved the sign-in for an account with no address.",
        )
    return account


MICROSOFT = SignInKind(
    id=SOURCE_TYPE,
    authorization_endpoint=_endpoint(authorization_url),  # type: ignore[arg-type]
    token_endpoint=_endpoint(token_url),  # type: ignore[arg-type]
    authorization_params=_authorization_params,
    approved_account=_approved_account,
)

register_kind(MICROSOFT)
