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

from fastapi import Request

from provisa.api.errors import ApiError
from provisa.security.rights import Capability, has_platform_bypass

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
    if has_platform_bypass(caps) or Capability.PLATFORM_SETTINGS.value in caps:
        return
    raise ApiError(
        403, "platform.settings_capability_required", "platform_settings capability required"
    )


def _require_right(request: Request, right: str) -> None:
    """Shared body of the org-scoped gates below: platform bypass, or the named right."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, state)
    if has_platform_bypass(caps) or right in caps:
        return
    raise ApiError(403, "platform.right_required", f"{right} capability required", right=right)


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
