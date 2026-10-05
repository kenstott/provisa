# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1f0c72-9d84-4e13-8b26-71c4a90ef5d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Org resolution for the non-HTTP protocol entrypoints (REQ-1266).

The HTTP path resolves ``active_org_id`` in ``AuthMiddleware`` and binds it in the
``_OrgRoutingMiddleware`` (REQ-1355: registered unconditionally in ``create_app`` — it spent a
release behind a ``state.multitenancy`` guard that is always False at factory time, so nothing
bound the ContextVar and every request fell through to the default org's runtime). The wire
protocols (pgwire/bolt/flight/gRPC) authenticate a
principal but have no middleware chain, so they resolve the org here — once, at session
establishment — using the SAME membership rule as the HTTP middleware:

  - single-org (multitenancy off): every session binds the deployment's one org (``state.org_id``),
    explicitly -- an unbound session is served no org's runtime.
  - multitenant: the authenticated ``user_id`` is looked up in ``user_org_memberships``.
    A platform admin (``admin``/``superadmin``) or a client-supplied org they belong to is
    honored; an org-scoped credential binds its own org; an org nobody named, or a non-member
    request, RAISES — no silent default and no selection by lone membership (REQ-1235: a wrong
    default here is a cross-tenant data escape).

The resolved org id is stored on the protocol's session object and bound around each query
via ``current_org`` (``set_current_org``/``reset_current_org``).
"""

from __future__ import annotations

from typing import Any

from provisa.core.org_membership import bindable_memberships


class OrgResolutionError(Exception):
    """Org could not be resolved for an authenticated principal — must fail the session."""


# The database name a pgwire client connects to when it names no org: the one the catalog shows.
DEFAULT_DATABASE = "provisa"


def org_named_by_host_or_database(
    host_org: str | None, database: str | None
) -> str | None:  # REQ-1235
    """The org a wire connection names by its TLS hostname or its database name.

    The database name names an org unless it is empty or the default ``provisa``. When both name
    one and they differ, the connection is refused, by name: neither is taken over the other.
    """
    db_org = database if database and database != DEFAULT_DATABASE else None
    if host_org is not None and db_org is not None and host_org != db_org:
        raise OrgResolutionError(
            f"the hostname names org {host_org!r} and the database name names org {db_org!r}"
        )
    return host_org if host_org is not None else db_org


async def resolve_session_org(
    state: Any,
    *,
    user_id: str | None,
    can_act_any_org: bool = False,  # REQ-1337: the cross_org RIGHT, never a role name
    requested_org: str | None = None,
    credential_org: str | None = None,
    named_by: str = "name the org in the request",
) -> str:
    """Resolve the org a protocol session binds.

    Single-org deployments bind the deployment's one org. Under multitenancy, returns the org id
    to bind; raises :class:`OrgResolutionError` when the principal is unresolvable to exactly one
    permitted org.

    REQ-1235: ``requested_org`` is what the client NAMED (SNI host, ticket or metadata org) and
    authorizes nothing. ``credential_org`` is the org a credential was issued for (a personal
    access token's); it is the only org that credential opens. A request naming another org is
    refused even when the owner belongs to it and even for a cross-org principal, and the owner
    must still belong to the credential's org.

    An org nobody named is refused. Belonging to exactly one org does not name it: the request
    says which org it is for, or the credential does. ``named_by`` is how a client of the
    calling surface names one; it goes into the refusal so the client is told what to send.
    """
    if not getattr(state, "multitenancy", False):
        return state.org_id

    member_org_ids: list[str] = []
    if user_id is not None and state.admin_db is not None:
        async with state.admin_db.acquire() as conn:
            result = await conn.execute_core(bindable_memberships(user_id))
            member_org_ids = [dict(r._mapping)["org_id"] for r in result.fetchall()]

    if credential_org is not None:
        if requested_org is not None and requested_org != credential_org:
            raise OrgResolutionError(
                f"credential is scoped to org {credential_org!r}, not {requested_org!r}"
            )
        if can_act_any_org or credential_org in member_org_ids:
            return credential_org
        raise OrgResolutionError(f"principal not a member of org {credential_org!r}")
    if requested_org is not None:
        if can_act_any_org or requested_org in member_org_ids:
            return requested_org
        raise OrgResolutionError(f"principal not a member of org {requested_org!r}")
    # REQ-1935: a cross_org principal naming no org is refused like anyone else -- there is no
    # implied org. It acts in any org it names (above).
    raise OrgResolutionError(
        "org selection required: authenticated principal belongs to "
        f"{len(member_org_ids)} orgs and none was requested. To name one, {named_by}, "
        "or present a personal access token issued for the org"
    )
