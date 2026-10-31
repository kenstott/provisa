# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The store's gate on a mask list (REQ-2059): a list holding an entry no reader can reach is not
saved. Reachability is read against the roles on the caller's own connection, never ``state.roles``:
a config load registers tables before the runtime that fills that map is built."""

# Requirements: REQ-2059

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.auth.derived_roles import SEPARATOR
from provisa.core.schema_org import roles
from provisa.security.inheritance import role_chains
from provisa.security.masking import MaskUnreachable, unreachable_refusal

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def _stored_chains(conn: "Connection") -> Callable[[str], list[str]]:
    """``chain_of`` as the org's store has it: a role and its ancestors. A derived role is no row
    of ``roles``: its value role ``t__v`` has the parent ``t`` (REQ-2057)."""
    rows = (await conn.execute_core(select(roles.c.id, roles.c.parents))).fetchall()
    chains = role_chains({"id": r[0], "parents": r[1]} for r in rows)

    def chain_of(role_id: str) -> list[str]:
        if role_id in chains:
            return chains[role_id]
        attribute, sep, _value = role_id.partition(SEPARATOR)
        return [role_id, attribute] if sep else [role_id]

    return chain_of


async def refuse_unreachable_masks(conn: "Connection", lists: Mapping[str, list[dict]]) -> None:
    """Raise :class:`MaskUnreachable` for the first of ``lists`` -- column name to its mask list --
    holding an entry no reader reaches."""
    if not any(lists.values()):
        return
    chain_of = await _stored_chains(conn)
    for column, masks in lists.items():
        refusal = unreachable_refusal(masks, chain_of)
        if refusal is not None:
            raise MaskUnreachable(refusal, column)
