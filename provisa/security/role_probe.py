# Copyright (c) 2026 Kenneth Stott
# Canary: a7dd3a8b-470a-47c5-b2f3-6bf0a133ff3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Naming a role the caller does not hold is a high-severity security event (REQ-2076).

Every transport refuses it (security.meta_role.RoleNotHeld) and hands it here with what it knows
of the call (:class:`Probe`). What follows depends on the credential and the transport:

- a personal access token, on any transport: the token is suspended;
- a token an external identity provider signed, over MCP or a transport that is never a browser
  session (pgwire, Bolt, Flight, gRPC): that principal's token access to the org is suspended.
  On the HTTP API the browser presents the same bearer, and a tab still naming a role the user
  has since lost is routine, so there it is refused and recorded, not suspended;
- a signed-in session (or a password this deployment checks): refused and recorded;
- a local stdio MCP session: refused, and written to the operator's log.

A suspension lasts until an administrator lifts it; nothing lifts it on a timer. Each event is
written to the org's admin trail (``admin_audit_log``, action ``security.role_probe``) with the
ids of the principal's most recent audited statements -- their text stays in the query audit log,
read under its own right -- and the org's administrators are notified: by email where this
deployment sends mail, and by the operator's security webhook (``security.webhook_url``) where
one is set. What leaves the org names no statement and no held role.
"""

# Requirements: REQ-2076

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_log = logging.getLogger(__name__)

#: What every refused caller who may be probing is told: nothing about whether the role exists.
NOT_PERMITTED = "not permitted"

#: The providers whose bearer tokens an external identity provider signs. SAML and LDAP sign in
#: through this deployment, which issues its own session; ``basic`` and ``simple`` are its own.
IDP_SIGNED = frozenset({"firebase", "oauth", "keycloak"})

#: The transports that are never a browser session.
NOT_A_BROWSER = frozenset({"mcp", "pgwire", "bolt", "flight", "airport", "grpc"})

#: How many of the principal's most recent audited statements an event points at.
RECENT_STATEMENTS = 5

ACTION = "security.role_probe"
SEVERITY = "high"


@dataclass(frozen=True)
class Probe:
    """One call that named a role its caller does not hold."""

    org_id: str | None
    user_id: str | None
    credential: str | None  # "pat", "idp", or None (a session, a password, or stdio)
    credential_id: str | None  # a PAT's hash
    transport: str  # "http", "mcp", "pgwire", "bolt", "flight", "airport", "grpc", "stdio"
    endpoint: str  # the route, tool or command the call made
    requested: str  # the role named
    held: tuple[str, ...]  # the roles the caller holds
    client: str | None = None  # the peer address (and user agent, where the wire carries one)


def probe_of(
    identity: Any,
    refused: Any,
    *,
    org_id: str | None,
    transport: str,
    endpoint: str,
    client: str | None = None,
) -> Probe:
    """The event for ``refused`` (a security.meta_role.RoleNotHeld) raised for ``identity``."""
    return Probe(
        org_id=org_id or getattr(identity, "active_org_id", None),
        user_id=getattr(identity, "user_id", None),
        credential=getattr(identity, "credential", None),
        credential_id=getattr(identity, "credential_id", None),
        transport=transport,
        endpoint=endpoint,
        requested=refused.role,
        held=refused.held,
        client=client,
    )


async def suspended(identity: Any, org_id: str | None, transport: str) -> bool:
    """Whether ``identity`` is an IdP-signed principal whose token access to ``org_id`` is
    suspended, calling over ``transport`` (one that is never a browser session). A suspended PAT
    is refused where it is validated (auth.pat), on every transport."""
    if (
        getattr(identity, "credential", None) != "idp"
        or transport not in NOT_A_BROWSER
        or org_id is None
    ):
        return False
    from provisa.api.app import state
    from provisa.core.org_membership import token_access_suspended

    assert state.admin_db is not None  # the admin plane is always bound where a principal is
    return await token_access_suspended(state.admin_db, user_id=identity.user_id, org_id=org_id)


def suspension(probe: Probe) -> str | None:
    """What the event suspends: "token" (the PAT), "principal" (the IdP principal's token access
    to the org), or None."""
    if probe.transport == "stdio":
        return None
    if probe.credential == "pat" and probe.credential_id:
        return "token"
    if probe.credential == "idp" and probe.transport in NOT_A_BROWSER and probe.user_id:
        return "principal"
    return None


def refusal(probe: Probe, said: str) -> str:
    """What the caller is told: ``said`` (the transport's own refusal naming the role) to a
    signed-in session, which may simply have gone stale; :data:`NOT_PERMITTED` to everyone else."""
    if probe.credential is None and probe.transport == "http":
        return said
    return NOT_PERMITTED


async def record(probe: Probe) -> str | None:
    """Suspend what the event calls for, write it to the org's admin trail and notify its
    administrators. Returns what was suspended (:func:`suspension`). A local stdio session is
    written to the operator's log alone: it has no principal, org trail or credential."""
    at = datetime.now(timezone.utc)
    if probe.transport == "stdio":
        _log.critical(
            "security.role_probe severity=high transport=stdio tool=%s requested=%r pinned=%s at=%s",
            probe.endpoint,
            probe.requested,
            ",".join(probe.held),
            at.isoformat(),
        )
        return None
    from provisa.api.app import state

    assert state.admin_db is not None  # the admin plane is always bound where a principal is
    suspended = suspension(probe)
    reason = (
        f"named role {probe.requested!r}, not held, over {probe.transport} ({probe.endpoint}) "
        f"at {at.isoformat()}"
    )
    if suspended == "token":
        from provisa.auth.pat import PersonalAccessTokenStore

        assert probe.credential_id is not None
        await PersonalAccessTokenStore(state.admin_db).suspend(
            token_hash=probe.credential_id, reason=reason
        )
    elif suspended == "principal":
        from provisa.core.org_membership import suspend_token_access

        assert probe.user_id is not None and probe.org_id is not None
        await suspend_token_access(
            state.admin_db, user_id=probe.user_id, org_id=probe.org_id, reason=reason
        )
    _log.warning(
        "security.role_probe severity=high org=%s user=%s credential=%s transport=%s endpoint=%s "
        "requested=%r suspended=%s",
        probe.org_id,
        probe.user_id,
        probe.credential,
        probe.transport,
        probe.endpoint,
        probe.requested,
        suspended,
    )
    if probe.org_id is None:
        # No org was bound to the call (it named none, or was refused before one was): there is
        # no org trail to write and no administrator to tell; the operator's log above has it.
        return suspended
    from provisa.api.app import ensure_org_runtime
    from provisa.core.org_membership import record_admin_action

    runtime = await ensure_org_runtime(probe.org_id)
    detail = {
        "severity": SEVERITY,
        "principal": probe.user_id,
        "credential": probe.credential,
        "credential_id": probe.credential_id,
        "client": probe.client,
        "transport": probe.transport,
        "endpoint": probe.endpoint,
        "requested_role": probe.requested,
        "held_roles": list(probe.held),
        "at": at.isoformat(),
        "suspended": suspended,
        "recent_statement_ids": await _recent_statement_ids(runtime.record_db, probe.user_id),
    }
    assert runtime.model_db is not None
    await record_admin_action(
        runtime.model_db,
        action=ACTION,
        actor_id=probe.user_id or "unknown",
        subject_id=probe.user_id or "unknown",
        detail=detail,
    )
    await _notify(probe, at, suspended, runtime.model_db)
    return suspended


async def _recent_statement_ids(record_db: Any, user_id: str | None) -> list[int]:
    """The ids of the principal's most recent audited statements in the org, newest first."""
    if record_db is None or user_id is None:
        return []
    from sqlalchemy import select

    from provisa.core.schema_org import query_audit_log

    async with record_db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(query_audit_log.c.id)
                .where(query_audit_log.c.user_id == user_id)
                .order_by(query_audit_log.c.logged_at.desc())
                .limit(RECENT_STATEMENTS)
            )
        ).fetchall()
    return [int(r[0]) for r in rows]


def notice(probe: Probe, at: datetime, suspended: str | None, link: str | None) -> dict:
    """What leaves the org about the event: no statement and no held role."""
    return {
        "event": ACTION,
        "severity": SEVERITY,
        "org": probe.org_id,
        "principal": probe.user_id,
        "credential_id": probe.credential_id,
        "transport": probe.transport,
        "endpoint": probe.endpoint,
        "requested_role": probe.requested,
        "at": at.isoformat(),
        "suspended": suspended,
        "link": link,
    }


async def _notify(probe: Probe, at: datetime, suspended: str | None, model_db: Any) -> None:
    """Email the org's administrators (where this deployment sends mail) and post to the
    operator's security webhook (where one is set).

    A delivery failure is logged at ERROR with its cause and never raised: the refusal and the
    suspension are what protect the org, and they stand whether or not a notice was delivered.
    The admin trail already holds the event (REQ-2076)."""
    from provisa.api.app import state
    from provisa.core import settings_registry

    cfg = getattr(state, "config", None)
    mail = getattr(cfg, "mail", None)
    base = getattr(mail, "base_url", None) or None
    link = f"{base.rstrip('/')}/team?section=suspended" if base else None
    payload = notice(probe, at, suspended, link)
    url = settings_registry.value("security.webhook_url")
    if url:
        import httpx

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                (await client.post(url, json=payload)).raise_for_status()
        except httpx.HTTPError as exc:
            _log.error("security.role_probe: the security webhook was not delivered: %s", exc)
    if not getattr(cfg, "multitenancy", False):
        return  # REQ-1330: this deployment sends no mail
    from provisa.core.mail import MailMessage, MailNotConfiguredError, email_sender
    from provisa.core.org_membership import org_admin_user_ids

    recipients = await _admin_emails(await org_admin_user_ids(model_db))
    if not recipients:
        return
    body = "\n".join(f"{k}: {v}" for k, v in payload.items() if v is not None)
    try:
        sender = email_sender(mail)
        for to in recipients:
            # Blocks the call's own request thread, as an invitation's send does (REQ-1882).
            sender.send(
                MailMessage(
                    to=to,
                    subject=f"Security event in {probe.org_id}: a role was named that is not held",
                    body=body,
                )
            )
    except MailNotConfiguredError as exc:
        _log.error("security.role_probe: no mail to the org's administrators: %s", exc)
    except Exception as exc:  # noqa: BLE001  # allow-blind-except: a mail transport fails in its own way (SMTP, an HTTP API, an SDK); none may undo a refusal or a suspension already made, and each is logged by its cause
        _log.error("security.role_probe: mail to the org's administrators failed: %s", exc)


async def _admin_emails(user_ids: set[str]) -> list[str]:
    from sqlalchemy import select

    from provisa.api.app import state
    from provisa.core.schema_admin import user_profiles

    if not user_ids:
        return []
    assert state.admin_db is not None  # the admin plane is always bound where a principal is
    async with state.admin_db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(user_profiles.c.email).where(user_profiles.c.user_id.in_(sorted(user_ids)))
            )
        ).fetchall()
    return sorted({r[0] for r in rows if r[0]})
