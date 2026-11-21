# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e2d17-6c3a-4f85-a0e1-7d2c8b5f3a96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Valid time on the entitlement store (REQ-2219).

Every row carries its valid-from and its recorded time. A stored row's valid-to is the next row's
valid-from for the same key, among the rows recorded as of the time asked: rows are append-only and
carry no valid-to column. A change appends a row that closes the one before it; a ``removed`` row
closes it with nothing held after. The store reads as of either timeline (:func:`as_of`), and what
is held now is each key's open period (:func:`current`).

An entitlement row's key is its source, person, role, kind and capability; an expiry rule's is its
person, role and rule key. Within a key, rows order by valid-from, then recorded time, then id: of
two rows valid from the same moment, the one recorded later is the one in force.
"""

# Requirements: REQ-2219

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import Table, select

from provisa.core.schema_org import entitlement_expiries, entitlements

if TYPE_CHECKING:
    from datetime import datetime

    from provisa.core.database import Connection


@dataclass(frozen=True)
class Period:
    """One stored row and its valid period: from its own valid-from to the next row's for the
    same key, as recorded; ``valid_to`` is None while no row follows it."""

    row: Any
    valid_from: "datetime"
    valid_to: "datetime | None"

    def holds_at(self, valid_at: "datetime") -> bool:
        """Whether this row is in force at ``valid_at``: a ``removed`` row holds nothing."""
        if self.row.removed or valid_at < self.valid_from:
            return False
        return self.valid_to is None or valid_at < self.valid_to


@dataclass(frozen=True)
class AsOf:
    """The store at one valid time, as recorded at one recorded time: the rows then in force."""

    entitlements: tuple[Period, ...]
    expiries: tuple[Period, ...]


def _entitlement_key(row: Any) -> Hashable:
    return (row.source, row.principal, row.role_id, row.kind, row.capability)


def _expiry_key(row: Any) -> Hashable:
    return (row.principal, row.role_id, row.rule_key)


def _periods(rows: Iterable[Any], key: Callable[[Any], Hashable]) -> list[Period]:
    """Each row with its period; ``rows`` come ordered by valid-from, recorded time and id."""
    by_key: dict[Hashable, list[Any]] = {}
    for row in rows:
        by_key.setdefault(key(row), []).append(row)
    out: list[Period] = []
    for chain in by_key.values():
        for row, following in zip(chain, [*chain[1:], None], strict=True):
            out.append(
                Period(row, row.valid_from, None if following is None else following.valid_from)
            )
    return out


async def _read(
    conn: "Connection",
    table: Table,
    key: Callable[[Any], Hashable],
    recorded_at: "datetime | None",
    **match: Any,
) -> list[Period]:
    query = select(table)
    for column, value in match.items():
        if value is not None:
            query = query.where(table.c[column] == value)
    if recorded_at is not None:
        query = query.where(table.c.recorded_at <= recorded_at)
    rows = await conn.execute_core(
        query.order_by(table.c.valid_from, table.c.recorded_at, table.c.id)
    )
    return _periods(rows.fetchall(), key)


async def entitlement_periods(
    conn: "Connection",
    *,
    recorded_at: "datetime | None" = None,
    source: str | None = None,
    principal: str | None = None,
    role_id: str | None = None,
    kind: str | None = None,
) -> list[Period]:
    """The entitlement rows with their periods, as recorded at ``recorded_at`` (None: everything
    recorded). The other arguments narrow the rows read; a key is never split by them."""
    return await _read(
        conn,
        entitlements,
        _entitlement_key,
        recorded_at,
        source=source,
        principal=principal,
        role_id=role_id,
        kind=kind,
    )


async def expiry_periods(
    conn: "Connection",
    *,
    recorded_at: "datetime | None" = None,
    principal: str | None = None,
    role_id: str | None = None,
) -> list[Period]:
    """The expiry rule rows with their periods, as recorded at ``recorded_at`` (None: everything
    recorded)."""
    return await _read(
        conn,
        entitlement_expiries,
        _expiry_key,
        recorded_at,
        principal=principal,
        role_id=role_id,
    )


def in_force(periods: Sequence[Period], valid_at: "datetime") -> tuple[Period, ...]:
    """The periods holding at ``valid_at``."""
    return tuple(p for p in periods if p.holds_at(valid_at))


def current(periods: Sequence[Period]) -> tuple[Period, ...]:
    """What is held now: each key's open period, unless its row ended the key."""
    return tuple(p for p in periods if p.valid_to is None and not p.row.removed)


async def as_of(conn: "Connection", valid_at: "datetime", recorded_at: "datetime") -> AsOf:
    """The entitlement rows and expiry rules in force at ``valid_at``, as recorded at
    ``recorded_at``."""
    return AsOf(
        in_force(await entitlement_periods(conn, recorded_at=recorded_at), valid_at),
        in_force(await expiry_periods(conn, recorded_at=recorded_at), valid_at),
    )
