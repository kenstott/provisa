# Copyright (c) 2026 Kenneth Stott
# Canary: 41a59f65-4390-47dd-9eb7-01c008609b71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A local MCP server configured to act for a principal who is in the org (REQ-1105), for the
unit tests of local tool calls whose subject is not the principal itself. The org lookups are
answered here; tests/unit/test_mcp_principal.py runs them against a control plane."""

from __future__ import annotations

from typing import Any

from provisa.api.mcp import local_principal


def act_for(monkeypatch: Any, state: Any = None, principal: str = "alice") -> None:
    async def _member(_state: Any, who: str, _org_id: str) -> bool:
        return who == principal

    async def _supplied(_state: Any, _who: str) -> dict[str, str]:
        return {}

    monkeypatch.setenv("PROVISA_MCP_PRINCIPAL", principal)
    monkeypatch.setattr(local_principal, "_in_org", _member)
    monkeypatch.setattr(local_principal, "_provider_attributes", _supplied)
    if state is not None and not hasattr(state, "idp_entitlements"):
        state.idp_entitlements = {}  # as the real state: the mirror, empty until a sign-in


def bound_principal(monkeypatch: Any, principal: str = "alice", roles: tuple[str, ...] = ()) -> Any:
    """The principal a local call acts for, bound as the tool wrapper binds it for the call: for a
    test that calls the server's role resolution directly, under no tool wrapper."""
    from contextvars import ContextVar

    from provisa.api.mcp import server
    from provisa.auth.models import AuthIdentity

    identity = AuthIdentity(user_id=principal, email=None, display_name=None, roles=list(roles))
    bound: ContextVar[Any | None] = ContextVar("provisa_mcp_local_identity", default=identity)
    monkeypatch.setattr(server, "_local_identity", bound)
    return identity
