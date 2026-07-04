# Copyright (c) 2026 Kenneth Stott
# Canary: 6b3d9c17-4a82-4e56-9f01-2c7a0d4f8b62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared, surface-agnostic executor for registered tracked functions (REQ-872, REQ-869)."""

from __future__ import annotations

from fastapi import HTTPException

from provisa.security.mutation_authz import require_mutation_write


async def invoke_tracked_function(name: str, args: dict, state, role_id: str | None) -> list[dict]:
    """The one path every surface routes through to invoke a registered function.

    GraphQL today, plus pgwire / SQL / Cypher via REQ-872: enforces per-mutation
    ``writable_by`` (REQ-869) by contract, then runs ``SELECT * FROM "schema"."fn"(args)``
    through the function's source pool and returns serialized row dicts. ``args`` is an
    ordered dict of positional argument values. Raises HTTPException for an unknown
    function, an unauthorized write, or a disconnected source.
    """
    role = state.roles.get(role_id) if role_id is not None else None
    fn = state.tracked_functions.get(name)
    if not fn:
        raise HTTPException(status_code=400, detail=f"Unknown function: {name!r}")
    require_mutation_write(fn, role, name)
    src_id = fn["source_id"]
    if not state.source_pools.has(src_id):
        raise HTTPException(status_code=503, detail=f"Source '{src_id}' not connected")
    params = list(args.values())  # empty args → no placeholders → "()"
    placeholders = ", ".join(f"${i + 1}" for i in range(len(params)))
    sql = f'SELECT * FROM "{fn["schema_name"]}"."{fn["function_name"]}"({placeholders})'
    result = await state.source_pools.execute(src_id, sql, params)
    from provisa.executor.serialize import _convert_value

    cols = result.column_names
    return [{c: _convert_value(v) for c, v in zip(cols, r)} for r in result.rows]
