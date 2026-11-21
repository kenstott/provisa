# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7a92c1-58e4-4d0b-b6a3-e1c94d72f805
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a person approves a QuickBooks source (REQ-2197): what is particular to Intuit in the
sign-in every such source kind shares (``provisa.core.source_sign_in``).

The client is the source's own Intuit app (its client id and secret, as its settings hold them),
not an organisation-wide entry. The person chooses the company at Intuit, and Intuit names it in
the address it returns the browser to (``realmId``): the approval is kept for that company, and
the form sets it as the source's realm id. Intuit authenticates the app with HTTP Basic and its
answer does not say what it grants: it grants the Accounting API it was asked or refuses."""

# Requirements: REQ-2197
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from provisa.core.source_sign_in import SignInKind, register_kind
from provisa.quickbooks import SOURCE_TYPE
from provisa.quickbooks.settings import ACCOUNTING_SCOPE, AUTHORIZATION_URL, TOKEN_URL

#: The parameter of Intuit's return address that names the company approved.
REALM_PARAMETER = "realmId"


def _authorization_params(account: str) -> dict[str, str]:
    # Intuit takes no hint of the company: the person chooses it at Intuit.
    return {}


def _scopes(mapping: Mapping[str, Any]) -> list[str]:
    """A QuickBooks source reads the company's Accounting API, whatever its settings."""
    return [ACCOUNTING_SCOPE]


QUICKBOOKS = SignInKind(
    id=SOURCE_TYPE,
    authorization_endpoint=AUTHORIZATION_URL,
    token_endpoint=TOKEN_URL,
    authorization_params=_authorization_params,
    scopes=_scopes,
    returned_account=REALM_PARAMETER,
    client_of_source=True,
    basic_auth=True,
    states_scope=False,
)

register_kind(QUICKBOOKS)
