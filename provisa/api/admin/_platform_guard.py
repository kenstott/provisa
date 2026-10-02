# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7c1b96-8a04-4e52-b1f7-2c9e6d40a5b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Authorization gates for the admin settings surfaces (REQ-1337, REQ-1349).

Three rights, three scopes: ``platform_settings`` is the deployment, ``org_settings`` is the org
being acted in, ``observability`` is read-only performance and health. Each gate names its right —
no gate here ever tests a role name.
"""

# Requirements: REQ-1297, REQ-1337, REQ-1349

from __future__ import annotations

from collections.abc import Iterable

from fastapi import Request

from provisa.api.errors import ApiError
from provisa.security.rights import (
    DEPLOYMENT_GRANTER,
    Capability,
    PlatformRoleGrantError,
    check_role_grant,
    platform_rights_in,
    unknown_capabilities,
)

_ANONYMOUS = "anonymous"


def require_platform_settings(request: Request) -> None:  # REQ-1337
    """Raise 403 unless the caller holds the ``platform_settings`` right.

    The check reads a RIGHT, never a role name: which roles carry the right is decided once, at
    seed time, by the deployment's tenancy mode (``apply_tenancy_role_grants``). In a multitenant
    deployment org_admin does not hold it, so an org administrator can neither read nor change the
    federation engine, cache storage, encryption provider, auth provider, the config file, or the
    query-engine lifecycle. platform_admin holds it in both modes.

    Dev mode (no auth configured — anonymous identity) is allowed, matching every other admin gate.
    """
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, state)
    if Capability.PLATFORM_SETTINGS.value in caps:
        return
    raise ApiError(
        403, "platform.settings_capability_required", "platform_settings capability required"
    )


def has_platform_settings(request: Request) -> bool:  # REQ-1349
    """Whether the caller holds ``platform_settings`` — the non-raising form of the gate above.

    For surfaces that serve BOTH scopes from one door: ``GET /admin/settings`` is read by ordinary
    pages (the domain filter, the tables and sources views), so it cannot carry a blanket gate.
    It omits the deployment-wide blocks for a caller without the right instead of refusing.
    """
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return True  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, state)
    return Capability.PLATFORM_SETTINGS.value in caps


def _require_right(request: Request, right: str) -> None:
    """Shared body of the org-scoped gates below: the named right, and nothing in its place."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, state)
    if right in caps:
        return
    raise ApiError(403, "platform.right_required", f"{right} capability required", right=right)


def has_right(request: Request, right: str) -> bool:  # REQ-1592
    """The non-raising twin of :func:`_require_right`, for surfaces that OMIT rather than refuse.

    The workbook report (REQ-1592) is one document covering several object types, each gated on its
    own right. Refusing the whole download because one sheet is out of reach would make the report
    useless to every role but the org administrator, so the sheet is left out instead and the
    leading Report sheet names what was omitted. Same two answers as the raising form: dev/no-auth
    holds everything, otherwise the named right decides.
    """
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return True  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, state)
    return right in caps


def require_org_settings(request: Request) -> None:  # REQ-1349
    """Raise 403 unless the caller holds ``org_settings``.

    Gates surfaces whose subject is the org bound to the request — its AI model / NL provider
    overrides, domains, scheduled tasks, creation requests. The org being acted in is decided by
    the request's ``active_org_id`` (the org-runtime router), so this gate only answers "may you
    change org settings at all"; which org it lands in is never this gate's choice.
    """
    _require_right(request, Capability.ORG_SETTINGS.value)


def require_observability(request: Request) -> None:  # REQ-1349
    """Raise 403 unless the caller holds ``observability`` (read-only performance and health)."""
    _require_right(request, Capability.OBSERVABILITY.value)


def is_anonymous(request: Request) -> bool:  # REQ-1913
    """Whether the caller is the anonymous identity of a deployment with no auth provider."""
    identity = getattr(request.state, "identity", None)
    return identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS


def granter_capabilities(request: Request) -> frozenset[str]:  # REQ-1337
    """The capability set a caller GRANTS WITH — what ``rights.check_role_grant`` compares a role's
    platform rights against. The anonymous identity of a deployment with no auth provider grants
    as the deployment itself, matching every other admin gate's dev-mode answer."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    if is_anonymous(request):
        return DEPLOYMENT_GRANTER  # dev mode — no auth configured
    return frozenset(_resolved_capabilities(request.state.identity, state))


def has_deployment_settings(request: Request) -> bool:  # REQ-1913
    """Whether the caller may read and edit deployment settings — the non-raising form of
    :func:`require_deployment_settings`, for ``GET /admin/settings``, which omits the
    deployment-wide blocks rather than refusing the request (ordinary pages read it)."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    if is_anonymous(request):
        return True  # dev mode — no auth configured
    caps = _resolved_capabilities(request.state.identity, state)
    return Capability.PLATFORM_SETTINGS.value in caps and Capability.CROSS_ORG.value in caps


def require_deployment_settings(request: Request) -> None:  # REQ-1913, REQ-1337
    """Raise 403 unless the caller is a platform administrator — in EVERY deployment.

    The operator settings catalog is the control plane's. ``platform_settings`` alone does not
    reach it: a single-tenant deployment grants that right to org_admin (``apply_tenancy_role_
    grants``), and the catalog is not the org administrator's there either. The gate still reads
    rights, never a role name: a control-plane role is the one holding ``cross_org`` (REQ-1337 —
    withdrawn from every role but platform_admin in both tenancy modes), so the catalog needs
    ``platform_settings`` AND ``cross_org``.

    Dev mode (no auth configured — anonymous identity) is allowed, matching every other admin
    gate; the guarded settings are refused for that caller where they are stored.
    """

    if has_deployment_settings(request):
        return
    raise ApiError(
        403,
        "platform.control_plane_role_required",
        "deployment settings are edited by a platform administrator",
    )


def require_role_grantable(  # REQ-1337
    request: Request, role_id: str, role_capabilities: Iterable[str] | None
) -> None:
    """Raise 403 unless the caller may confer (or remove) ``role_id`` — the HTTP form of
    ``rights.check_role_grant``, which is the one rule every grant path asks."""
    try:
        check_role_grant(role_id, role_capabilities, granter_capabilities(request))
    except PlatformRoleGrantError as exc:
        raise ApiError(
            403,
            "users.platform_role_requires_platform_admin",
            f"role {role_id!r} carries platform rights; only a holder of them may grant or "
            "remove it",
            role=role_id,
            rights=exc.missing,
        ) from exc


def role_definition_problem(  # REQ-042, REQ-1337
    request: Request, capabilities: Iterable[str] | None, inherited: Iterable[str] | None = None
) -> ApiError | None:
    """Why a role may not be DEFINED with ``capabilities``, or None when it may.

    Two refusals, asked of every surface that writes a role's definition:

    * a string naming no right — the vocabulary is closed (``rights.unknown_capabilities``);
    * a platform right, held directly or through ``inherited`` (the parent chain's capabilities),
      unless the caller is a platform administrator. Defining a role is ``user_management``'s act,
      and that right is over an org's people — it does not reach authority over the deployment.

    Returned rather than raised because the GraphQL surface answers with a result, not an error.
    """
    unknown = unknown_capabilities(capabilities)
    if unknown:
        return ApiError(
            422,
            "roles.unknown_capability",
            "Unknown capability: " + ", ".join(repr(c) for c in unknown),
            capabilities=unknown,
        )
    platform = sorted(platform_rights_in(capabilities) | platform_rights_in(inherited))
    if platform and not has_deployment_settings(request):
        return ApiError(
            403,
            "roles.platform_right_requires_platform_admin",
            "A role carrying " + ", ".join(platform) + " is defined by a platform administrator",
            rights=platform,
        )
    return None
