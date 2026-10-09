# Copyright (c) 2026 Kenneth Stott
# Canary: 7eb0f484-ba36-47c5-ac91-a510abbc5d21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a person approves a Google Workspace source (REQ-1923): what is particular to Google in
the sign-in every such source kind shares (``provisa.core.source_sign_in``)."""

# Requirements: REQ-1923
from __future__ import annotations

import httpx

from provisa.core.source_sign_in import SignInKind, SignInRefused, register_kind
from provisa.google_workspace import SOURCE_TYPE
from provisa.google_workspace.settings import TOKEN_URL

#: Google's authorization endpoint, as its OpenID configuration document publishes it.
AUTHORIZATION_URL = "https://accounts.google.com/o/oauth2/v2/auth"
PROFILE_URL = "https://gmail.googleapis.com/gmail/v1/users/me/profile"
_TIMEOUT = 10.0


def _authorization_params(account: str) -> dict[str, str]:
    # offline: a refresh token is issued. consent: it is issued again to a person who approved
    # this client before. login_hint: the account the source names is the one offered.
    return {"access_type": "offline", "prompt": "consent", "login_hint": account}


async def _approved_account(access_token: str) -> str:
    """The mailbox the token reads, by Gmail's own account of it (users.getProfile)."""
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        answer = await client.get(PROFILE_URL, headers={"Authorization": f"Bearer {access_token}"})
    if answer.status_code != 200:
        raise SignInRefused(
            "account_unread",
            f"Google approved the sign-in and would not say whose mailbox it is "
            f"(HTTP {answer.status_code}). Is the Gmail API enabled for the client's project?",
        )
    return answer.json()["emailAddress"]


GOOGLE = SignInKind(
    id=SOURCE_TYPE,
    authorization_endpoint=AUTHORIZATION_URL,
    token_endpoint=TOKEN_URL,
    authorization_params=_authorization_params,
    approved_account=_approved_account,
)

register_kind(GOOGLE)
