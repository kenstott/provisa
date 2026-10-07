# Copyright (c) 2026 Kenneth Stott
# Canary: bbafe370-a478-4f86-bde5-c5cb5a1e3956
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's inherited source bindings (REQ-1491, REQ-1529, REQ-1538, REQ-1942).

A created environment's sources rows travel stripped of where they point (IDENTITY_ONLY). Each
row says how the environment reaches its source (its binding): through a connection of its OWN,
through its parent's by REFERENCE (INHERITED), or not at all (UNBOUND). An inherited source reads
the connection of the environment its parent chain reaches it through: the parent's own binding,
or, where the parent inherits it too, further up. Nothing is written into the environment, so it
discloses nothing, and rotating the parent's binding re-points it at its next build. A chain that
ends at an unbound row leaves the source unbound.
"""

# Requirements: REQ-1491, REQ-1529, REQ-1538, REQ-1942

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.env_classes import BINDING_COLUMN, BINDING_COLUMNS, INHERITED, OWN, UNBOUND
from provisa.core.environments import PROD, org_schema

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database


async def _parent(admin_db: "Database", org_id: str, env: str) -> str | None:
    from provisa.core.env_store import get_env

    row = await get_env(admin_db, org_id, env)
    if row is None:
        raise KeyError(f"organization {org_id!r} has no environment {env!r}")
    return row["parent"]


async def _rows_of(conn: "Connection", schema: str, ids: set[str]) -> dict[str, dict]:
    """The ``sources`` rows of ``ids`` in ``schema``'s environment."""
    from provisa.core.env_copy import _scoped
    from provisa.core.schema_org import sources

    table = _scoped(sources, schema)
    result = await conn.execute_core(select(table).where(table.c.id.in_(sorted(ids))))
    return {r._mapping["id"]: dict(r._mapping) for r in result.fetchall()}


async def inherited_sources(
    conn: "Connection", admin_db: "Database", org_id: str, env: str, rows: dict[str, dict]
) -> dict[str, dict[str, Any]]:
    """``rows`` -- ``env``'s ``sources`` rows by id -- with each INHERITED source given the
    connection (its binding columns) of the first environment up ``env``'s ``parent`` chain whose
    row binds it as its OWN. The row's own binding marker stays INHERITED: the connection is the
    ancestor's. A source the chain reaches unbound, or that no environment up it binds, is marked
    UNBOUND: it is reached through no connection. A chain that returns to an environment it passed
    is refused."""
    if env == PROD:
        return rows
    out = dict(rows)
    pending = {sid for sid, row in rows.items() if row[BINDING_COLUMN] == INHERITED}
    walked = [env]
    current = env
    while pending:
        parent = await _parent(admin_db, org_id, current)
        if parent is None:
            break
        if parent in walked:
            raise RuntimeError(
                f"organization {org_id!r}: environment {env!r}'s parent chain returns to "
                f"{parent!r} ({' -> '.join([*walked, parent])})"
            )
        walked.append(parent)
        found = await _rows_of(conn, org_schema(org_id, parent), pending)
        for sid in list(pending):
            row = found.get(sid)
            if row is None or row[BINDING_COLUMN] not in (OWN, INHERITED):
                # Unbound there, or not there: reached through no connection here either.
                out[sid] = {**rows[sid], BINDING_COLUMN: UNBOUND}
                pending.discard(sid)
            elif row[BINDING_COLUMN] == OWN:
                out[sid] = {**rows[sid], **{c: row[c] for c in BINDING_COLUMNS["sources"]}}
                pending.discard(sid)
        current = parent
    for sid in pending:  # the chain ended before any environment bound it
        out[sid] = {**rows[sid], BINDING_COLUMN: UNBOUND}
    return out
