# Copyright (c) 2026 Kenneth Stott
# Canary: 34ff4129-75f2-4592-8658-20bc366e350e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The record of the store schemas sessions hold their temporary tables in, and the reaper of the
ones left behind (REQ-1926).

A session's temporary tables live in a store schema of its own, dropped when the session ends
(:func:`provisa.pgwire.temp_exec.end`). A session that ends without that -- its worker was
killed, the store was unreachable as it closed -- leaves the schema in the store. Each schema is
therefore recorded before its first table is written, in the STATE of its org environment
(``session_temp_schemas``, beside ``replica_state``), with the last time the session's worker said
the session is alive. Every worker's keeper renews that for its own sessions and drops the schemas
whose session has said nothing for longer than :data:`GONE_AFTER`.

It is state, not model: nothing declares it, it is written while sessions run, and losing it
loses only the reaper's knowledge of schemas to drop. This module is the one place it is read and
written.

Every worker runs a keeper: a session's liveness is known only to the worker that holds it. A
keeper reaps in every org environment whose runtime its process holds. Two keepers that find the
same abandoned schema both drop it; the second finds nothing there, which a drop of a schema
that is gone means.
"""

# Requirements: REQ-1926

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select, update

from provisa.compiler.temp_tables import TempSession
from provisa.core.request_context import (
    current_env,
    require_current_org,
    reset_current_env,
    reset_current_org,
    set_current_env,
    set_current_org,
)
from provisa.core.schema_org import session_temp_schemas as recorded

log = logging.getLogger(__name__)

BEAT_SECONDS = 30.0
GONE_AFTER = timedelta(minutes=5)

#: This worker's sessions that hold a stored schema: session id -> (session, org, env).
_live: dict[str, tuple[TempSession, str, str | None]] = {}


async def note_stored(state: Any, session: TempSession, schema: str) -> None:
    """Record ``schema`` BEFORE its first table is written, so a schema that exists is always
    one the reaper knows of. A kept session's schema is never recorded (REQ-2176)."""
    assert not session.kept, "a kept session's schema is its own sweep's, not a keeper's"
    async with state.tenant_db.acquire() as conn:
        await conn.upsert(
            recorded,
            {"store_schema": schema, "session_id": session.id, "beat_at": datetime.now(UTC)},
            index_elements=["store_schema"],
        )
    _live[session.id] = (session, require_current_org(), current_env.get())


async def note_ended(state: Any, session: TempSession, schema: str) -> None:
    """Forget ``schema`` once it is gone from the store."""
    async with state.tenant_db.acquire() as conn:
        await conn.execute_core(delete(recorded).where(recorded.c.store_schema == schema))
    _live.pop(session.id, None)


class _In:
    """Bind an org environment for a block, so ``state`` resolves to its runtime."""

    def __init__(self, org_id: str, env: str | None) -> None:
        self._org_id, self._env = org_id, env

    def __enter__(self) -> None:
        self._org_token = set_current_org(self._org_id)
        self._env_token = set_current_env(self._env)

    def __exit__(self, *_exc: object) -> None:
        reset_current_env(self._env_token)
        reset_current_org(self._org_token)


async def beat(state: Any, now: datetime) -> int:
    """Say this worker's sessions are alive. Returns how many sessions that was."""
    by_env: dict[tuple[str, str | None], list[str]] = {}
    for sid, (_session, org_id, env) in _live.items():
        by_env.setdefault((org_id, env), []).append(sid)
    for (org_id, env), held in by_env.items():
        with _In(org_id, env):
            async with state.tenant_db.acquire() as conn:
                await conn.execute_core(
                    update(recorded).where(recorded.c.session_id.in_(held)).values(beat_at=now)
                )
    return len(_live)


async def _reap_here(state: Any, now: datetime) -> list[str]:
    """Reap in the org environment that is bound."""
    from provisa.federation.store_scope import drop_temp_schema

    async with state.tenant_db.acquire() as conn:
        gone = (
            await conn.execute_core(select(recorded).where(recorded.c.beat_at < now - GONE_AFTER))
        ).fetchall()
    dropped: list[str] = []
    for row in gone:
        try:
            await drop_temp_schema(
                state.federation_engine.engine.materialize_store(), row.store_schema
            )
            async with state.tenant_db.acquire() as conn:
                await conn.execute_core(
                    delete(recorded).where(recorded.c.store_schema == row.store_schema)
                )
            dropped.append(row.store_schema)
        except Exception:  # noqa: BLE001 — one schema that cannot be dropped must not stop the sweep
            log.exception(
                "temporary tables of session %s (gone), schema %s, were not dropped; they are "
                "tried again on the next sweep",
                row.session_id,
                row.store_schema,
            )
    return dropped


async def reap(state: Any, now: datetime) -> list[str]:
    """Drop every recorded schema whose session has said nothing since ``now - GONE_AFTER``, in
    every org environment this process holds a runtime of. Returns the schemas dropped. A drop
    that fails is logged and tried again next time."""
    dropped: list[str] = []
    for runtime in state.org_registry.runtimes():
        with _In(runtime.org_id, runtime.env):
            dropped += await _reap_here(state, now)
    if dropped:
        log.warning(
            "dropped the temporary tables of %d session(s) that are gone: %s",
            len(dropped),
            ", ".join(dropped),
        )
    return dropped


async def keep(state: Any) -> None:
    """This worker's keeper: renew its sessions, drop what gone sessions left. Runs until the
    worker stops."""
    while True:
        try:
            now = datetime.now(UTC)
            await beat(state, now)
            await reap(state, now)
        except Exception:  # noqa: BLE001 — the keeper outlives a sweep that failed; it says so
            log.exception("temporary-table keeper sweep failed; it runs again")
        await asyncio.sleep(BEAT_SECONDS)
