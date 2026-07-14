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
