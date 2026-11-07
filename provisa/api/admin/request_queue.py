# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7d9a14-5e3c-4f81-a6d0-9c4e8b17f2a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Raising a request: what it is for, who it is addressed to, and who is told (REQ-2215..2217).

Every request names the environment it is for (REQ-2217). It is addressed to the roles that hold
the right it names -- the roles whose members may decide it -- and those roles' members are
resolved when it is raised (REQ-2215) and kept on the request, which ops.requests publishes
(REQ-2216). A role none of whose members can be resolved is kept by name.

The org's delivery setting says whether Provisa itself tells the members: ``email`` (the default
REQ-2216 names), ``events`` (Provisa sends no notice; a workflow tool subscribes to ops.requests)
or ``both``.
"""

# Requirements: REQ-2215, REQ-2216, REQ-2217

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, cast

from provisa.core.role_members import (
    ProvisaAssignmentsResolver,
    RoleMemberResolver,
    RoleMembers,
    resolve_role_members,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

log = logging.getLogger(__name__)

#: The org setting's key in ``org_settings`` (provisa.core.org_settings.ORG_OVERRIDABLE_KEYS).
SETTINGS_KEY = "requests"

DeliveryMode = Literal["email", "events", "both"]
DELIVERY_MODES: tuple[DeliveryMode, ...] = ("email", "events", "both")
#: REQ-2216: "email (built in, the default)" -- the requirement's own default, not a fallback.
DEFAULT_DELIVERY: DeliveryMode = "email"


def delivery_of(settings: dict[str, Any]) -> DeliveryMode:
    """The delivery mode an org's resolved settings name; REQ-2216's default when it names none.
    A stored value outside :data:`DELIVERY_MODES` is refused by name."""
    block = settings.get(SETTINGS_KEY) or {}
    mode = block.get("delivery", DEFAULT_DELIVERY)
    if mode not in DELIVERY_MODES:
        raise ValueError(
            f"requests.delivery is {mode!r}; it must be one of {', '.join(DELIVERY_MODES)}"
        )
    return cast("DeliveryMode", mode)


def sends_email(mode: DeliveryMode) -> bool:
    return mode in ("email", "both")


def addressed_roles(capability: str, roles: dict[str, dict]) -> list[str]:
    """The roles a request naming ``capability`` is addressed to: every role that holds it.
    ``roles`` is the loaded registry (``state.roles``), whose capabilities are already flattened
    through inheritance (REQ-2155)."""
    return sorted(rid for rid, r in roles.items() if capability in (r.get("capabilities") or []))


async def resolve_addressees(
    resolver: RoleMemberResolver, org_id: str, role_ids: list[str]
) -> list[RoleMembers]:
    return [await resolve_role_members(resolver, org_id, rid) for rid in role_ids]


def members_of(resolved: list[RoleMembers]) -> list[dict]:
    """Every principal of the resolved roles once, with the roles they are reached through."""
    by_id: dict[str, dict] = {}
    for rm in resolved:
        for p in rm.principals:
            entry = by_id.setdefault(p.id, {**p.as_dict(), "roles": []})
            entry["roles"].append(rm.role_id)
    return [by_id[k] for k in sorted(by_id)]


def unresolved_of(resolved: list[RoleMembers]) -> list[str]:
    """The roles resolved that have no resolvable members, by name (REQ-2215)."""
    return [rm.role_id for rm in resolved if not rm.resolvable]


async def raise_request(  # REQ-2215, REQ-2216, REQ-2217
    conn: "Connection",
    request_type: str,
    capability: str,
    payload: dict,
    requested_by: str | None,
) -> int:
    """Persist a pending request for the environment this call is bound to, addressed to the roles
    holding ``capability`` with their members resolved, and tell those members by email when the
    org's delivery setting says Provisa sends notices. Returns the request id."""
    from provisa.api.app import state
    from provisa.core.repositories import creation_request as cr_repo
    from provisa.core.request_context import active_env, require_current_org

    assert state.model_db is not None, "the org's model store is bound with its runtime"
    org_id = require_current_org()
    environment = active_env()
    role_ids = addressed_roles(capability, state.roles)
    resolved = await resolve_addressees(
        ProvisaAssignmentsResolver(state.model_db), org_id, role_ids
    )
    members = members_of(resolved)
    unresolved = unresolved_of(resolved)
    rid = await cr_repo.create(
        conn,
        request_type,
        capability,
        payload,
        requested_by,
        environment=environment,
        addressed_roles=role_ids,
        addressed_members=members,
        unresolved_roles=unresolved,
    )
    for role_id in unresolved:
        log.warning(
            "request #%s in org %s is addressed to role %r, which has no resolvable members",
            rid,
            org_id,
            role_id,
        )
    await _notify(
        state.model_db,
        org_id,
        rid,
        request_type=request_type,
        environment=environment,
        requested_by=requested_by,
        role_ids=role_ids,
        members=members,
    )
    return rid


def notice_text(
    org_id: str,
    rid: int,
    *,
    request_type: str,
    environment: str,
    requested_by: str | None,
    role_ids: list[str],
) -> tuple[str, str]:
    """The subject and body of the email telling a member of a new request."""
    who = requested_by or "an anonymous user"
    roles = ", ".join(role_ids)
    return (
        f"Request #{rid} ({request_type}) in {org_id} awaits a decision",
        f"{who} raised request #{rid} ({request_type}) for the {environment} environment of the "
        f"organization {org_id}. It is addressed to the role(s) {roles}, which you hold. "
        "Open Requests in Provisa to approve or reject it.",
    )


async def _notify(
    model_db: "Database",
    org_id: str,
    rid: int,
    *,
    request_type: str,
    environment: str,
    requested_by: str | None,
    role_ids: list[str],
    members: list[dict],
) -> None:
    """Email each member with an address, unless the org's delivery is events only or the
    deployment has no mail provider (the request row is then the record, as for every notice)."""
    from provisa.api.app import state
    from provisa.core.org_settings import resolve_org_config

    mode = delivery_of(await resolve_org_config(model_db))
    if not sends_email(mode):
        return
    assert state.config is not None, "the deployment's configuration is loaded"
    if not state.config.mail.provider:
        return
    subject, body = notice_text(
        org_id,
        rid,
        request_type=request_type,
        environment=environment,
        requested_by=requested_by,
        role_ids=role_ids,
    )
    for member in members:
        to = member["email"]
        if to and member["id"] != requested_by:
            await _send(org_id, to, subject, body)


async def _send(org_id: str, to: str, subject: str, body: str) -> None:
    """One message through the org mail channel, recorded as every send is (REQ-1576). A failed
    send is logged and recorded, never raised: the request is already stored and listed."""
    from provisa.api.app import state
    from provisa.core.mail import MailMessage, email_sender
    from provisa.core.mail_stats import MailAttempt, record

    assert state.config is not None and state.admin_db is not None
    provider = state.config.mail.provider
    try:
        email_sender(state.config.mail).send(MailMessage(to=to, subject=subject, body=body))
    except Exception as exc:  # recorded and logged below; the next member's notice still goes
        log.error("request notice to %s for org %s not delivered: %s", to, org_id, exc)
        await record(
            state.admin_db,
            MailAttempt(
                provider=provider,
                kind="request",
                recipient=to,
                succeeded=False,
                org_id=org_id,
                error=str(exc),
            ),
        )
        return
    await record(
        state.admin_db,
        MailAttempt(provider=provider, kind="request", recipient=to, succeeded=True, org_id=org_id),
    )
