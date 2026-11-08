# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1c8e27-0d4b-4a96-b3e2-7a9d16c4e8f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An approve or decide call that carries an ``Idempotency-Key`` applies once (REQ-2216).

A workflow tool retries a decision it is unsure was delivered. The first call with a key is
carried out and its result kept under (the call's method and path, the caller, the key); a repeat
of the same key by the same caller on the same endpoint is answered with that result and changes
nothing. A call that failed kept nothing, so its retry is carried out afresh. A call with no key
is carried out as it always was.

Two calls with one key at once are serialized on an advisory lock, so the second waits for the
first and is answered with its result rather than applying again.
"""

# Requirements: REQ-2216

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select

from provisa.core.schema_org import idempotent_calls

if TYPE_CHECKING:
    from provisa.core.database import Database

HEADER = "Idempotency-Key"


def _lock_key(scope: str, actor: str, key: str) -> int:
    """A signed 64-bit advisory-lock key for one (scope, actor, key)."""
    digest = hashlib.sha256(f"idempotent\0{scope}\0{actor}\0{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _actor(request: Request) -> str:
    identity = getattr(request.state, "identity", None)
    user_id = getattr(identity, "user_id", None) if identity is not None else None
    # The no-auth dev principal is one caller, as every gate treats it.
    return user_id or "anonymous"


async def once(db: "Database", request: Request, run: Callable[[], Awaitable[Any]]) -> Any:
    """``run()``'s result, carried out at most once per ``Idempotency-Key`` (REQ-2216).

    ``db`` is the org store the call's own writes go to; the kept results live beside them."""
    key = request.headers.get(HEADER)
    if key is None:
        return await run()
    if not key.strip():
        from provisa.api.errors import ApiError

        raise ApiError(400, "idempotency.key_empty", f"The {HEADER} header is empty")
    scope = f"{request.method} {request.url.path}"
    actor = _actor(request)
    where = (
        idempotent_calls.c.scope == scope,
        idempotent_calls.c.actor == actor,
        idempotent_calls.c.call_key == key,
    )
    async with db.acquire() as conn:
        async with conn.advisory_lock(_lock_key(scope, actor, key)):
            kept = (
                await conn.execute_core(select(idempotent_calls.c.response).where(*where))
            ).scalar()
            if kept is not None:
                return json.loads(kept) if isinstance(kept, str) else kept
            result = jsonable_encoder(await run())
            await conn.insert_returning(
                idempotent_calls,
                {"scope": scope, "actor": actor, "call_key": key, "response": result},
                returning="call_key",
            )
    return result
