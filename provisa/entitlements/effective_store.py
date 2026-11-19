# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b1d82-5c47-4e09-9a2d-8e1c7b4f6a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The stored effective rows: one per person, role and capability (REQ-2219).

Each row is what a capability comes to for a person through one role grant, after precedence and
within the ceiling: the mask's bits over the role's schema (a data-plane capability), the version
of the bit positions they are over (:func:`provisa.entitlements.mask.index_version`), and the
readable expansion, the identifiers it covers (:func:`provisa.entitlements.effective
.capability_views`). Requests are enforced from the in-memory mask; the row is its record.

Rows are recomputed when an input changes: a sign-in, a Provisa write to the person's lists or
role grants. :func:`write_person` appends a row only for a key whose bits, version or expansion
changed, and a ``removed`` row for a key the person no longer has (a role no longer held, a
capability the role dropped). Every person Provisa knows to hold a role gets rows
(:func:`write_every_holder`). Append-only as the source rows are: a row's valid-to is the next
row's valid-from for the same key. A row is never read against another model version
(:func:`current_rows`).
"""

# Requirements: REQ-2219

from __future__ import annotations

import zlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select, union

from provisa.core.schema_org import (
    effective_entitlements,
    entitlements,
    user_directory,
    user_idp_roles,
    user_role_assignments,
)
from provisa.entitlements import roles
from provisa.entitlements.effective import capability_views, model_index
from provisa.entitlements.index import DATA_PLANE_CAPABILITIES
from provisa.entitlements.lists import HeldEntitlements
from provisa.entitlements.mask import index_version
from provisa.entitlements.runtime import principal_mask, role_index

if TYPE_CHECKING:
    from provisa.core.database import Connection

_NOTHING_HELD = HeldEntitlements()

#: Serializes one person's writers: two workers serving the same sign-in would append the same
#: change twice. The high half of a 64-bit lock key ("EFF1"), the low half a digest of the person.
_EFFECTIVE_LOCK = 0x45464631


def _lock_key(principal: str) -> int:
    return (_EFFECTIVE_LOCK << 32) | zlib.crc32(principal.encode())


_Key = tuple[str, str]  # (role, capability)


@dataclass(frozen=True)
class EffectiveRow:
    """One capability through one role grant. ``bits`` is None for a capability with no mask;
    ``expansion`` None for an unscoped one, held wherever the role is."""

    bits: int | None
    model_version: str
    expansion: tuple[str, ...] | None


def _kind_bits(index: Any, capability: str) -> int:
    """The bits of ``index``'s objects that ``capability`` reaches when held."""
    kinds = DATA_PLANE_CAPABILITIES[capability][1]
    out = 0
    for obj in index.objects:
        if obj.kind in kinds:
            out |= 1 << obj.bit
    return out


def compute_rows(
    state: Any, principal: str, role_ids: Iterable[str], now: datetime
) -> dict[_Key, EffectiveRow]:
    """``principal``'s effective rows through each of ``role_ids`` at ``now``, by (role,
    capability). A data-plane capability's bits are the person's mask over the role's schema,
    restricted to the objects that capability reaches; every row's version is that of the role's
    bit positions, or, for a role with no schema, of the whole model's, which its lists are
    checked against."""
    held = state.held_entitlements.get(principal, _NOTHING_HELD)
    whole = model_index(state)
    if whole is None:
        raise RuntimeError("there is no model yet to compute effective entitlements against")
    out: dict[_Key, EffectiveRow] = {}
    for role_id in role_ids:
        index = role_index(state, role_id)
        mask = principal_mask(state, principal, role_id, held, now)
        version = index_version(whole if index is None else index)
        for view in capability_views(state, principal, [role_id], held, now):
            bits = None
            if mask is not None and view.capability in DATA_PLANE_CAPABILITIES:
                bits = mask.bits & _kind_bits(mask.index, view.capability)
            out[(role_id, view.capability)] = EffectiveRow(bits, version, view.identifiers)
    return out


def _as_row(row: Any) -> EffectiveRow:
    return EffectiveRow(
        None if row.bits is None else int(row.bits, 16),
        row.model_version,
        None if row.expansion is None else tuple(row.expansion),
    )


async def _latest(conn: "Connection", principal: str | None) -> dict[tuple[str, str, str], Any]:
    """Each key's last row, by (principal, role, capability), removed or not."""
    t = effective_entitlements
    query = select(t).order_by(t.c.valid_from, t.c.recorded_at, t.c.id)
    if principal is not None:
        query = query.where(t.c.principal == principal)
    rows = await conn.execute_core(query)
    return {(r.principal, r.role_id, r.capability): r for r in rows.fetchall()}


async def stored_rows(conn: "Connection", principal: str) -> dict[_Key, EffectiveRow]:
    """``principal``'s effective rows as stored now, whatever model version they are over."""
    return {
        (role_id, capability): _as_row(r)
        for (_p, role_id, capability), r in (await _latest(conn, principal)).items()
        if not r.removed
    }


def current_version(state: Any, role_id: str) -> str:
    """The version of ``role_id``'s bit positions in the model built now: what a stored row must
    be over to be read."""
    index = role_index(state, role_id)
    if index is None:
        whole = model_index(state)
        if whole is None:
            raise RuntimeError("there is no model yet to read effective entitlements against")
        index = whole
    return index_version(index)


async def current_rows(
    conn: "Connection", state: Any, principal: str | None = None
) -> dict[tuple[str, str, str], EffectiveRow]:
    """The stored effective rows over the model built now, by (principal, role, capability);
    ``principal`` reads one person's. A row over another model version is not returned: its bits
    are positions in an index this model does not have."""
    versions: dict[str, str] = {}
    out: dict[tuple[str, str, str], EffectiveRow] = {}
    for key, r in (await _latest(conn, principal)).items():
        if r.removed or r.role_id not in state.roles:
            continue
        if r.role_id not in versions:
            versions[r.role_id] = current_version(state, r.role_id)
        if r.model_version == versions[r.role_id]:
            out[key] = _as_row(r)
    return out


def _values(principal: str, key: _Key, row: EffectiveRow) -> dict[str, Any]:
    return {
        "principal": principal,
        "role_id": key[0],
        "capability": key[1],
        "bits": None if row.bits is None else format(row.bits, "x"),
        "model_version": row.model_version,
        "expansion": None if row.expansion is None else list(row.expansion),
    }


async def write_rows(
    conn: "Connection", principal: str, wanted: Mapping[_Key, EffectiveRow]
) -> int:
    """Make ``principal``'s stored rows equal ``wanted``: append a row per key that changed, and a
    ``removed`` row per stored key not wanted. Returns the rows appended; nothing changed appends
    nothing."""
    appended = 0
    async with conn.transaction():
        await conn.advisory_xact_lock(_lock_key(principal))
        stored = await stored_rows(conn, principal)
        for key, row in wanted.items():
            if stored.get(key) == row:
                continue
            await conn.execute_core(
                insert(effective_entitlements).values(**_values(principal, key, row))
            )
            appended += 1
        for key in stored.keys() - wanted.keys():
            gone = stored[key]
            await conn.execute_core(
                insert(effective_entitlements).values(**_values(principal, key, gone), removed=True)
            )
            appended += 1
    return appended


async def held_role_ids(conn: "Connection", state: Any, principal: str) -> list[str]:
    """The roles ``principal`` holds that the org defines, as the admin API lists them."""
    held = await roles.held_roles(
        conn,
        principal,
        state.idp_entitlements.get(principal, {}),
        # As the admin API's person_roles reads it: no auth section is the claims default.
        state.auth_config or {},
        state.roles,
    )
    return [r.role_id for r in held if r.role_id in state.roles]


async def write_person(
    conn: "Connection", state: Any, principal: str, now: datetime | None = None
) -> int:
    """Recompute ``principal``'s effective rows from what the runtime holds now and store what
    changed. Returns the rows appended. Called after an input changed: their sign-in, or a write
    to their lists or role grants, once the runtime holds it. With no model built yet there is
    no version for the bits to be over; the build that makes one recomputes every holder's rows
    (REQ-2219: recomputed on a model build)."""
    if state.names_table is None:
        return 0
    at = datetime.now(UTC) if now is None else now
    role_ids = await held_role_ids(conn, state, principal)
    return await write_rows(conn, principal, compute_rows(state, principal, role_ids, at))


async def known_principals(conn: "Connection") -> list[str]:
    """Every person Provisa knows of in the org: signed in, granted a role, sent role claims, or
    holding a list."""
    query = union(
        select(user_directory.c.user_id.label("principal")),
        select(user_role_assignments.c.user_id),
        select(user_idp_roles.c.user_id),
        select(entitlements.c.principal).where(entitlements.c.principal.is_not(None)),
    )
    rows = await conn.execute_core(query)
    return sorted(r[0] for r in rows.fetchall())


async def write_every_holder(conn: "Connection", state: Any, now: datetime | None = None) -> int:
    """:func:`write_person` for every person Provisa knows of: each who holds a role gets rows,
    and one who holds none has any left closed. Returns the rows appended."""
    if state.names_table is None:
        return 0
    at = datetime.now(UTC) if now is None else now
    appended = 0
    for principal in await known_principals(conn):
        appended += await write_person(conn, state, principal, at)
    return appended
