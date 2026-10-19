# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Named bases (REQ-2094): context the data cannot show -- a regulation, a policy, a strategy --
kept as a short list a judgment cites by id. Declaring one changed, with a note, sets its
``changed_at``; every judgment citing it decided before then is dated by context at once."""

# Requirements: REQ-2094

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import select, update

from provisa.core.schema_org import judgment_bases as jb

_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")


class BasisRefused(ValueError):
    """A basis that cannot be stored, said by field."""


def _row(r: Any) -> dict[str, Any]:
    d = dict(r._mapping)
    for k in ("changed_at",):
        if d.get(k) is not None:
            d[k] = d[k].isoformat()
    return d


async def list_bases(conn: Any) -> list[dict[str, Any]]:
    rows = (await conn.execute_core(select(jb).order_by(jb.c.name))).fetchall()
    return [_row(r) for r in rows]


async def get_basis(conn: Any, basis_id: str) -> dict[str, Any] | None:
    row = (await conn.execute_core(select(jb).where(jb.c.id == basis_id))).fetchone()
    return None if row is None else _row(row)


async def put_basis(
    conn: Any, basis_id: str, *, name: str, description: str | None, owner: str | None
) -> dict[str, Any]:
    """Create or edit a basis. Editing its words is not a change of the basis itself: only
    :func:`declare_changed` dates the judgments citing it."""
    if not _ID.match(basis_id):
        raise BasisRefused(
            f"basis id {basis_id!r}: 1-64 lower-case letters, digits, '_', '.' or '-'"
        )
    if not name.strip():
        raise BasisRefused(f"basis {basis_id!r}: a name is required")
    clash = (
        await conn.execute_core(select(jb.c.id).where(jb.c.name == name, jb.c.id != basis_id))
    ).fetchone()
    if clash is not None:
        raise BasisRefused(f"basis name {name!r} is already basis {clash[0]!r}")
    from provisa.core import model_change

    model_change.name("put", "basis", basis_id)
    await conn.upsert(
        jb,
        {"id": basis_id, "name": name, "description": description, "owner": owner},
        index_elements=["id"],
        update_columns=["name", "description", "owner"],
    )
    held = await get_basis(conn, basis_id)
    assert held is not None  # just written
    return held


async def declare_changed(
    conn: Any, basis_id: str, *, note: str, by: str | None, at: Any
) -> dict[str, Any] | None:
    """Declare the basis changed as of ``at``, with ``note``. None when there is no such basis."""
    if not note.strip():
        raise BasisRefused(f"basis {basis_id!r}: say what changed")
    from provisa.core import model_change

    model_change.name("declare changed", "basis", basis_id)
    result = await conn.execute_core(
        update(jb).where(jb.c.id == basis_id).values(changed_at=at, changed_by=by, change_note=note)
    )
    if (result.rowcount or 0) == 0:
        return None
    return await get_basis(conn, basis_id)
