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


def authorize_role(state, identity, requested: str | None) -> str:
    """The role a call executes as — derived from the validated identity, never asserted.

    A call may REQUEST a role (one, or a comma-separated set of held roles acting as their
    meta-role), and it is honored only when the identity's own assignments carry it; anything
    else is a privilege claim by the client and raises PermissionError. With no request, the
    identity's claims map to a role through the same rules every other surface uses.
    """
    from provisa.auth.role_mapping import resolve_assignments, resolve_role
    from provisa.security.meta_role import resolve_requested_role

    auth_config = state.auth_config
    default_role = auth_config.get("default_role")
    if not default_role:
        # No admin default: an identity matching no mapping rule is refused, not escalated.
        raise PermissionError("identity matched no role and no default_role is configured")
    mapped = resolve_role(identity, auth_config.get("role_mapping", []), default_role)
    if not requested:
        return mapped
    permitted = {a.role_id for a in resolve_assignments(identity)} | {mapped}
    return resolve_requested_role(state, permitted, requested)
