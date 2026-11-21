# Copyright (c) 2026 Kenneth Stott
# Canary: 92b7d0e4-5c18-4f3a-a6d9-e04c1b8f2735
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A QuickBooks source's settings (REQ-2197), as its row holds them.

As a Salesforce source's connected app does (REQ-1946), the Intuit app's client id and client
secret ride in the source's username and password; the company (its realm id) is its database.
Its mapping holds the rest: ``environment`` (``sandbox`` or ``production``), and
``refresh_token``, the approval entered on the form -- a ``${secret:…}`` reference or the
token itself. An entered refresh token is the first one the source is read with: it is kept in
the organisation's state store at the first read, and the replacements QuickBooks answers each
refresh with are kept there from then on (``quickbooks.token``).
"""

# Requirements: REQ-2197
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from provisa.core.auth_models import ApiAuthOAuth2RefreshToken

SANDBOX = "sandbox"
PRODUCTION = "production"
ENVIRONMENTS = (SANDBOX, PRODUCTION)

#: Where each environment's Accounting API answers.
API_HOSTS = {
    SANDBOX: "https://sandbox-quickbooks.api.intuit.com",
    PRODUCTION: "https://quickbooks.api.intuit.com",
}
#: Intuit's token endpoint, the same for both environments.
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
#: Where a person approves an Intuit app.
AUTHORIZATION_URL = "https://appcenter.intuit.com/connect/oauth2"
#: What a source asks to be granted: the Accounting API.
ACCOUNTING_SCOPE = "com.intuit.quickbooks.accounting"

# A realm id is the company's number; a ${…} reference to one is resolved when it is used.
_REALM = re.compile(r"\d{1,32}")


class InvalidQuickBooksSource(ValueError):
    """A QuickBooks source whose settings cannot be read with: names the field."""


@dataclass(frozen=True)
class QuickBooksSettings:
    source_id: str
    client_id: str
    client_secret: str
    realm_id: str
    environment: str
    #: The approval entered on the form, where one was: the first refresh token the source is
    #: read with. None where the owner signs in instead.
    refresh_token: str | None

    def __repr__(self) -> str:  # settings are never printed with their credentials
        return (
            f"QuickBooksSettings(source_id={self.source_id!r}, realm_id={self.realm_id!r}, "
            f"environment={self.environment!r})"
        )

    @property
    def base_url(self) -> str:
        """The company's address in the Accounting API, the base every query is sent under."""
        return f"{API_HOSTS[self.environment]}/v3/company/{self.realm_id}"

    def auth(self) -> ApiAuthOAuth2RefreshToken:
        """The source's credential as its API source row keeps it: the app, and the approval
        entered on the form (empty where the owner signs in instead)."""
        return ApiAuthOAuth2RefreshToken(
            client_id=self.client_id,
            client_secret=self.client_secret,
            refresh_token=self.refresh_token or "",
            token_url=TOKEN_URL,
            scope=ACCOUNTING_SCOPE,
        )


def parse(
    source_id: str,
    *,
    username: str | None,
    password: str | None,
    database: str | None,
    mapping: Mapping[str, Any] | None,
) -> QuickBooksSettings:
    """A source row's QuickBooks settings, each missing or malformed one refused by name."""
    who = f"quickbooks source {source_id!r}"
    held = dict(mapping or {})
    if not username:
        raise InvalidQuickBooksSource(f"{who}: requires the app's client id")
    if not password:
        raise InvalidQuickBooksSource(f"{who}: requires the app's client secret")
    realm = (database or "").strip()
    if not realm:
        raise InvalidQuickBooksSource(f"{who}: requires the company's realm id")
    if not (realm.startswith("${") or _REALM.fullmatch(realm)):
        raise InvalidQuickBooksSource(
            f"{who}: the realm id is the company's number (for example 9341453456789012), "
            f"got {realm!r}"
        )
    environment = str(held.get("environment") or SANDBOX).strip().lower()
    if environment not in ENVIRONMENTS:
        raise InvalidQuickBooksSource(
            f"{who}: environment is sandbox or production, got {environment!r}"
        )
    refresh_token = held.get("refresh_token")
    return QuickBooksSettings(
        source_id=source_id,
        client_id=username,
        client_secret=password,
        realm_id=realm,
        environment=environment,
        refresh_token=str(refresh_token) if refresh_token else None,
    )
