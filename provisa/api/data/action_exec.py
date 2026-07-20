# Copyright (c) 2026 Kenneth Stott
# Canary: 6b3d9c17-4a82-4e56-9f01-2c7a0d4f8b62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared, surface-agnostic executor for registered tracked functions (REQ-872, REQ-869).

Authorizes by contract (REQ-869) then hands off to the extensible-function dispatcher
(REQ-885), which routes on ``impl_kind`` and emits a non-bypassable invocation trace
(REQ-886). ``source_procedure`` is the original REQ-205–208 stored-procedure path.
"""

from __future__ import annotations

from fastapi import HTTPException

from provisa.executor.function_dispatch import dispatch_function
from provisa.security.mutation_authz import require_mutation_write


def list_visible_commands(state, role_id: str | None) -> list[dict]:
    """Every registered command visible to ``role_id``, as ordered metadata dicts (REQ-1156).

    The one discovery path every surface (MCP, Arrow Flight, gRPC, Cypher/Bolt) projects, so a
    command registered once is listable on all of them — not only invocable. ``visible_to``
    filtering matches the REST/OpenAPI surface exactly (openapi_spec.py): an empty ``visible_to``
    means visible to every role. Each entry carries the two orthogonal dimensions the req names:
    ``kind`` (query vs mutation) and ``set_returning`` (``return_schema`` or ``returns =
    "schema.table"`` -> table-valued). Aliased duplicates (the domain-prefixed keys added in
    app_loaders) collapse to one entry per command name.

    ``role_id`` None means the broadest, role-agnostic catalog view (every command), matching the
    Flight table catalog; a concrete role filters by ``visible_to`` (empty ``visible_to`` = every
    role), matching the REST/OpenAPI surface.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for fn in (getattr(state, "tracked_functions", {}) or {}).values():
        name = fn.get("name")
        if not name or name in seen:
            continue
        visible_to = fn.get("visible_to") or []
        if role_id is not None and visible_to and role_id not in visible_to:
            continue
        seen.add(name)
        out.append(
            {
                "name": name,
                "domain": fn.get("domain_id", "") or "",
                "kind": fn.get("kind", "mutation"),
                "set_returning": bool(fn.get("return_schema") or fn.get("returns")),
                "arguments": [
                    {"name": a.get("name"), "type": a.get("type", "String")}
                    for a in (fn.get("arguments") or [])
                    if a.get("name")
                ],
                "description": fn.get("description", "") or "",
            }
        )
    return sorted(out, key=lambda c: (c["domain"], c["name"]))


async def invoke_tracked_function(name: str, args: dict, state, role_id: str | None) -> list[dict]:
    """The one path every surface routes through to invoke a registered function.

    GraphQL today, plus pgwire / SQL / Cypher via REQ-872: enforces per-mutation
    ``writable_by`` (REQ-869) by contract, then dispatches by implementation kind
    (REQ-885) with a mandatory invocation trace (REQ-886). ``args`` is an ordered dict
    of positional argument values. Raises HTTPException for an unknown function, an
    unauthorized write, a missing binding, an unknown kind, or a disconnected source.
    """
    role = state.roles.get(role_id) if role_id is not None else None
    fn = state.tracked_functions.get(name)
    if not fn:
        raise HTTPException(status_code=400, detail=f"Unknown function: {name!r}")
    require_mutation_write(fn, role, name)
    return await dispatch_function(fn, args, state, role_id)
