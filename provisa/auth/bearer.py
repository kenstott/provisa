# Copyright (c) 2026 Kenneth Stott
# Canary: 0d5a8c72-4f19-4b3e-a6c7-2e9b1f8d4a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bearer authentication for the transports that carry one credential and no user name
(REQ-1263, REQ-273): gRPC, Arrow Flight and the airport Flight service.

Each presents a bearer credential — a provider token or a personal access token — and, apart
from it, may REQUEST a role. The three questions are answered here once: does this deployment
authenticate callers, whose credential is this, and which role may that identity act as. A
transport supplies only where the credential and the request travel on its wire, and how a
refusal is said in its protocol.
"""

# Requirements: REQ-1263, REQ-273, REQ-1393, REQ-1620

from __future__ import annotations

import jwt

# What validating a bearer credential raises when the caller is not authenticated: a rejected
# credential (ValueError, or the JWT library's own error), or a provider that takes no bearer
# credential at all (PermissionError). A transport catches these and answers "unauthenticated"
# in its protocol with :func:`credential_refusal` as the reason — never a generic server error.
CREDENTIAL_ERRORS = (ValueError, jwt.PyJWTError, PermissionError)


def credential_refusal(exc: BaseException) -> str:
    """The reason a caller is told its credential did not authenticate it.

    Every rejected credential reads the same on the wire: a caller must not learn from the
    response whether the credential was unknown, expired or revoked. A provider that accepts no
    bearer credential is a fact about the deployment, not about the credential, and is said.
    """
    if isinstance(exc, PermissionError):
        return str(exc)
    return "credential rejected"


def auth_active(state, surface: str) -> bool:
    """Whether this deployment authenticates ``surface``'s callers.

    Fail-closed, as pgwire reads the same state: a live auth middleware with no resolved
    ``auth_config`` is a misconfiguration, and a secured server must never degrade to trust mode
    because its config went missing.
    """
    if getattr(state, "auth_config", None) is not None:
        return True
    if getattr(state, "auth_middleware_active", False):
        raise RuntimeError(f"{surface} auth_config not configured")
    return False


async def validate_bearer_credential(state, token: str, surface: str):
    """Validate a caller's bearer credential and return its identity.

    These transports carry exactly one credential presentation — a bearer token — so the bearer
    validator is selected by name rather than calling ``validate_token``, whose meaning differs
    per provider (under ``basic`` it expects base64 ``user:password``, and every bearer
    credential, personal access token included, would fail there). The platform pool is passed
    through so a PAT resolves here exactly as it does on every other surface.
    """
    from provisa.auth.models import validator_for_scheme
    from provisa.auth.throttle import throttled
    from provisa.auth.wiring import build_auth_provider

    provider = build_auth_provider(state.auth_config, admin_pool=getattr(state, "admin_db", None))
    validator = validator_for_scheme(provider, "bearer")
    if validator is None:
        raise PermissionError(
            f"auth provider {provider.provider_name!r} accepts no bearer credential, "
            f"so it cannot authenticate {surface}"
        )
    # REQ-1393: the transport names no principal, so the throttle keys on the credential itself —
    # replaying one rejected token is bounded, and a caller cannot lock out an account it does
    # not know.
    return await throttled(validator, token, principal=None)


def authorize_role(state, identity, requested: str | None, acts_as: str, may_name: list) -> str:
    """The role a call executes as — derived from the validated identity, never asserted.

    A call may REQUEST a role (one, or a comma-separated set of held roles acting as their
    meta-role), and it is honored only when ``may_name`` carries it; anything else is a privilege
    claim by the client and raises PermissionError. With no request it executes as ``acts_as``.
    Both are the identity's, as every transport reads them
    (provisa.auth.held_assignments.sign_in_roles).
    """
    from provisa.security.meta_role import resolve_requested_role

    if not requested:
        return acts_as
    from provisa.auth.derived_roles import principal_of

    # REQ-2057: a derived role the identity holds is named like any role.
    return resolve_requested_role(
        state, set(may_name), requested, lambda: principal_of(state, identity)[1]
    )


def authorize_role_or_probe(
    state,
    identity,
    requested: str | None,
    *,
    transport: str,
    endpoint: str,
    run,
    client: str | None = None,
) -> str:
    """:func:`authorize_role`, for a transport that is never a browser session (REQ-2076): an
    IdP-signed principal whose token access to the org is suspended is refused, and a role named
    that the identity does not hold is recorded as a security event (security.role_probe) and
    refused with nothing said about the role. ``run`` runs a coroutine on the transport's own loop
    and returns its result; the org is the one bound to the call."""
    from provisa.core.request_context import current_org
    from provisa.security import role_probe
    from provisa.security.meta_role import RoleNotHeld

    org_id = current_org.get(None) or getattr(identity, "active_org_id", None)
    if org_id is None and not getattr(state, "multitenancy", False):
        org_id = state.org_id
    if run(role_probe.suspended(identity, org_id, transport)):
        raise PermissionError(role_probe.NOT_PERMITTED)
    from provisa.auth.held_assignments import sign_in_roles

    acts_as, may_name = run(sign_in_roles(state, identity, org_id, naming=bool(requested)))
    try:
        return authorize_role(state, identity, requested, acts_as, may_name)
    except RoleNotHeld as refused:
        probe = role_probe.probe_of(
            identity, refused, org_id=org_id, transport=transport, endpoint=endpoint, client=client
        )
        run(role_probe.record(probe))
        raise PermissionError(role_probe.NOT_PERMITTED) from refused
