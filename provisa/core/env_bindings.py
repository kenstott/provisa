# Copyright (c) 2026 Kenneth Stott
# Canary: bbafe370-a478-4f86-bde5-c5cb5a1e3956
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A branch's inherited source bindings (REQ-1491, REQ-1529, REQ-1538).

A created environment's sources rows travel stripped of where they point (IDENTITY_ONLY) and
land unbound. A BRANCH -- an environment created with its connections inherited, recorded as its
branched_from -- inherits its parent's bindings by REFERENCE: a source unbound in the branch
reads the connection of the nearest environment up its branched_from chain that bound it.
Nothing is written into the branch, so it discloses nothing, and rotating the parent's binding
re-points the branch at its next build. A base (no branched_from) inherits nothing: its
unbound sources stay unbound.
"""

# Requirements: REQ-1491, REQ-1529, REQ-1538

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.env_classes import BINDING_COLUMNS, BOUND_COLUMN
from provisa.core.environments import PROD, org_schema

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database


async def _parent(admin_db: "Database", org_id: str, env: str) -> str | None:
    from provisa.core.env_store import get_env

    row = await get_env(admin_db, org_id, env)
    if row is None:
        raise KeyError(f"organization {org_id!r} has no environment {env!r}")
    return row["branched_from"]


async def _bound_rows(conn: "Connection", schema: str, ids: set[str]) -> dict[str, dict]:
    """The rows of ids bound in schema's environment."""
    from provisa.core.env_copy import _scoped
    from provisa.core.schema_org import sources

    table = _scoped(sources, schema)
    result = await conn.execute_core(
        select(table).where(table.c.id.in_(sorted(ids)), table.c[BOUND_COLUMN].is_(True))
    )
    return {r._mapping["id"]: dict(r._mapping) for r in result.fetchall()}


async def inherited_sources(
    conn: "Connection", admin_db: "Database", org_id: str, env: str, rows: dict[str, dict]
) -> dict[str, dict[str, Any]]:
    """rows -- env's sources rows by id -- with each source unbound in env given the
    connection (its binding columns) of the nearest environment up env's branched_from
    chain that bound it. The row stays marked unbound: the binding is the ancestor's, not
    env's own. A source no environment of the chain bound is left as it is. A chain that
    returns to an environment it passed is refused."""
    if env == PROD:
        return rows
    out = dict(rows)
    unbound = {sid for sid, row in rows.items() if not row[BOUND_COLUMN]}
    walked = [env]
    current = env
    while unbound:
        parent = await _parent(admin_db, org_id, current)
        if parent is None:
            break
        if parent in walked:
            raise RuntimeError(
                f"organization {org_id!r}: environment {env!r}'s branched_from chain returns to "
                f"{parent!r} ({' -> '.join([*walked, parent])})"
            )
        walked.append(parent)
        for sid, bound in (await _bound_rows(conn, org_schema(org_id, parent), unbound)).items():
            out[sid] = {**rows[sid], **{c: bound[c] for c in BINDING_COLUMNS["sources"]}}
            unbound.discard(sid)
        current = parent
    return out
