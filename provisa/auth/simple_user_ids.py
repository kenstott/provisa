# Copyright (c) 2026 Kenneth Stott
# Canary: 3f05888d-f58a-4510-a917-87c0f4a2479c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The stored id of each simple-provider user: a GUID assigned at first sign-in."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from provisa.core.schema_admin import simple_user_ids


class SimpleUserIds:
    """Reads, and on first sign-in assigns, a simple-provider user's id on the platform plane."""

    def __init__(self, admin_pool) -> None:
        self._pool = admin_pool

    async def id_for(self, username: str) -> str:
        found = await self._read(username)
        if found is not None:
            return found
        try:
            async with self._pool.acquire() as conn:
                await conn.upsert(
                    simple_user_ids,
                    {"username": username, "user_id": str(uuid.uuid4())},
                    index_elements=["username"],
                    update_columns=[],
                )
        except IntegrityError:
            # Another node assigned this user's id between the read and the insert; its row is
            # the id. Read it back below.
            pass
        assigned = await self._read(username)
        if assigned is None:
            raise RuntimeError(f"no id was stored for simple-provider user {username!r}")
        return assigned

    async def _read(self, username: str) -> str | None:
        async with self._pool.acquire() as conn:
            row = (
                await conn.execute_core(
                    select(simple_user_ids.c.user_id).where(simple_user_ids.c.username == username)
                )
            ).fetchone()
        return None if row is None else row[0]
