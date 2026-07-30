# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7c1b96-8a04-4e52-b1f7-2c9e6d40a5b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Authorization gate for the deployment-wide settings surface (REQ-1337)."""

# Requirements: REQ-1297, REQ-1337

from __future__ import annotations

from fastapi import HTTPException, Request

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
    raise HTTPException(status_code=403, detail="platform_settings capability required")
