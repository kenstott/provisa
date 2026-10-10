# Copyright (c) 2026 Kenneth Stott
# Canary: 971214d3-ce4f-4738-a976-0c0edf2a7953
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the Sources form asks about a Microsoft 365 source of the organisation's mailboxes
(REQ-1923): how many mailboxes a choice names, before the source is saved.

One call to the directory with the organisation's own client. It is refused as a build of
such a source is: by name when the organisation has entered no client, when its administrator
has not allowed sources that read the organisation's mailboxes, when Microsoft refuses the
client, and when the directory cannot be read. No secret and no token is part of an answer.
"""

# Requirements: REQ-1923
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.api_source.oauth_grants import CredentialRefused
from provisa.core.mail_platforms import MailPlatformRefused
from provisa.microsoft365 import settings as m365
from provisa.microsoft365.directory import DirectoryRefused
from provisa.microsoft365.graph import GraphRefused, GraphThrottled
from provisa.microsoft365.loader import listed_mailboxes, organisation_token

router = APIRouter(prefix="/admin/microsoft-365", tags=["admin", "sources"])


class MailboxesBody(BaseModel):
    #: The source's choice, as its mapping states it: one of everyone, group, list.
    mailboxes: dict[str, Any]


def _admin_db() -> Any:
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


@router.post("/mailboxes/check")
async def check_mailboxes(request: Request, body: MailboxesBody) -> dict:
    """How many mailboxes the choice names in the organisation's directory now."""
    require_capability_request(request, "source_registration")
    from provisa.core.request_context import require_current_org
    from provisa.core.secrets_store import bound_to_request_org

    try:
        chosen = m365.parse({"resources": [m365.MAIL], "mailboxes": body.mailboxes}).mailboxes
    except m365.InvalidMicrosoft365Source as invalid:
        raise ApiError(
            400, "microsoft_365.mailboxes_invalid", str(invalid), None, error=str(invalid)
        ) from None
    try:
        # The client's secret is the organisation's: its vault is bound for this request.
        async with bound_to_request_org():
            token = await organisation_token(_admin_db(), require_current_org())
        found = await listed_mailboxes(token, chosen)
    except MailPlatformRefused as refused:
        raise ApiError(refused.status, refused.code, str(refused), None, **refused.params) from None
    except CredentialRefused as refused:
        raise ApiError(
            400, "microsoft_365.client_refused", str(refused), None, reason=refused.reason
        ) from None
    except GraphThrottled as throttled:
        raise ApiError(
            503,
            "microsoft_365.throttled",
            str(throttled),
            None,
            retry_after=throttled.retry_after,
        ) from None
    except GraphRefused as refused:
        raise ApiError(
            400,
            "microsoft_365.directory_refused",
            str(refused),
            None,
            status=refused.status,
            reason=refused.code,
        ) from None
    except DirectoryRefused as refused:
        raise ApiError(
            400,
            "microsoft_365.directory_refused",
            str(refused),
            None,
            status=400,
            reason=str(refused),
        ) from None
    return {"count": len(found)}
