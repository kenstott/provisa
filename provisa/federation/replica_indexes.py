# Copyright (c) 2026 Kenneth Stott
# Canary: a3d826b1-d605-4018-981e-ee0cb24da6a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The indexes a replica carries (REQ-2015).

A replica is indexed as its table is used: its declared primary key, enforced as a key, and the
columns its declared relationships join on -- the foreign key of each relationship whose source
is this table (``orders.customer_id`` for ``orders.customer_id -> customers.id``), the target
column of one whose target is this table when that column is not the key already, and the two
foreign keys of a junction table a relationship traverses.

The indexes are built after the replica lands, never in its landing: the replica is read as soon
as it is swapped in, and its indexes follow. What a landing asked for, what was built and what
failed is kept on the replica's record (``replica_state.index_state``), each by its name, for
the landing it belongs to. A failed index is logged, kept with its cause, and asked for again by
the next landing; it never fails the landing. A landing that replaces the table while its last
landing's indexes are still being built leaves those builds behind: they stop at the next index
and record nothing, and the new table gets its own.

Names are fixed by the replica and the columns (``index_name``), so a store holds at most one of
each and a rebuild finds the one it made.

A materialized view is indexed by this same rule, plan, record, lock and runner pass
(``provisa.mv.view_indexes``); its index state is on its build row and its key is
``replica_state.view_index_key``.
"""

# Requirements: REQ-2015

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from sqlalchemy import or_, select

from provisa.core.schema_org import registered_tables, relationships
from provisa.federation import replica_state

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)

ReplicaKey = tuple[str, str, str]

#: The code of an index that could not be built because the replica's rows repeat its key: a fact
#: of the source's data, not a failure a retry fixes.
DUPLICATE_KEY = "replica.index_duplicate_key"
#: The code of any other index that could not be built.
INDEX_FAILED = "replica.index_failed"


@dataclass(frozen=True)
class IndexSpec:
    """One index of a replica: its fixed name, its columns in order, and whether it is the
    replica's primary key."""

    name: str
    columns: tuple[str, ...]
    primary: bool = False

    def as_record(self) -> dict[str, Any]:
        return {"name": self.name, "columns": list(self.columns), "primary": self.primary}

    @classmethod
    def from_record(cls, rec: dict[str, Any]) -> IndexSpec:
        return cls(rec["name"], tuple(rec["columns"]), bool(rec["primary"]))


class DuplicateKey(Exception):
    """The replica's rows repeat the values of the key being built: the source holds duplicates
    of a column set the model declares unique."""

    def __init__(self, columns: Iterable[str], detail: str) -> None:
        self.columns = tuple(columns)
        super().__init__(
            f"the replica's rows repeat the key ({', '.join(self.columns)}), so it cannot be its "
            f"primary key: the source holds duplicates of a key the model declares ({detail})"
        )


class IndexBuildAbandoned(Exception):
    """The table the index was being built on was replaced by a later landing, which stopped the
    build (``replica_target._pg_swap``): the index belonged to the table that is gone."""


class Indexer(Protocol):
    """A store's way of building one replica's indexes, after it has landed."""

    async def build(self, spec: IndexSpec) -> None:
        """Build ``spec`` on the standing replica, unless it is there already. Raises
        :class:`DuplicateKey` when a key cannot be built over the rows, anything else on any
        other failure."""
        ...


def index_name(replica: str, columns: Iterable[str], *, primary: bool = False) -> str:
    """The fixed name of an index of the replica ``replica`` on ``columns`` (in order): short,
    so a long replica name cannot run it past the store's identifier limit."""
    digest = hashlib.sha256(f"{replica}.{','.join(columns)}".encode()).hexdigest()[:24]
    return ("pk__" if primary else "idx__") + digest


def plan(
    replica: str,
    columns: Iterable[str],
    pk_columns: Iterable[str],
    joins: Iterable[tuple[str, ...]],
) -> list[IndexSpec]:
    """The indexes of the replica ``replica`` with ``columns``: its key when it declares one, and
    one index per join column set in ``joins`` that is not the key itself. A join on a column the
    replica does not hold (a computed one) has nothing to index."""
    held = set(columns)
    pk = tuple(pk_columns)
    out: list[IndexSpec] = []
    if pk:
        out.append(IndexSpec(index_name(replica, pk, primary=True), pk, primary=True))
    seen = {pk}
    for join in joins:
        if join in seen or not join or not set(join) <= held:
            continue
        seen.add(join)
        out.append(IndexSpec(index_name(replica, join), join))
    return out


def _columns(listed: str | None) -> tuple[str, ...]:
    """A relationship's column list (``a`` or ``a,b`` for a composite key) as a tuple."""
    return tuple(c.strip() for c in (listed or "").split(",") if c.strip())


async def join_columns(
    conn: Connection, key: ReplicaKey, pk_columns: Iterable[str]
) -> list[tuple[str, ...]]:
    """The column sets the declared relationships join ``key``'s table on, from the model as the
    control plane holds it now: the source column of each relationship it is the source of, the
    target column of each it is the target of (unless that is its key), and both foreign keys of
    each relationship that traverses it as a junction."""
    ids = [
        r[0]
        for r in (
            await conn.execute_core(
                select(registered_tables.c.id).where(
                    registered_tables.c.source_id == key[0],
                    registered_tables.c.schema_name == key[1],
                    registered_tables.c.table_name == key[2],
                )
            )
        ).fetchall()
    ]
    return await joins_of(conn, ids, pk_columns)


async def joins_of(
    conn: Connection, ids: list[int], pk_columns: Iterable[str]
) -> list[tuple[str, ...]]:
    """The column sets the declared relationships join the table registered as ``ids`` on: a
    replica's (:func:`join_columns`) or a materialized view's."""
    if not ids:
        return []
    r = relationships.c
    rows = (
        await conn.execute_core(
            select(
                r.source_table_id,
                r.target_table_id,
                r.via_table_id,
                r.source_column,
                r.target_column,
                r.via_source_column,
                r.via_target_column,
            )
            .where(
                or_(
                    r.source_table_id.in_(ids),
                    r.target_table_id.in_(ids),
                    r.via_table_id.in_(ids),
                )
            )
            .order_by(r.id)
        )
    ).fetchall()
    pk = tuple(pk_columns)
    found: list[tuple[str, ...]] = []
    for row in rows:
        if row.via_table_id in ids:
            found += [_columns(row.via_source_column), _columns(row.via_target_column)]
        if row.via_table_id is not None:
            # A junction's ends join through it: their own key columns, not a foreign key here.
            continue
        if row.source_table_id in ids:
            found.append(_columns(row.source_column))
        if row.target_table_id in ids and _columns(row.target_column) != pk:
            found.append(_columns(row.target_column))
    return [cols for cols in found if cols]


record_plan = replica_state.record_index_plan
record_view_plan = replica_state.record_view_index_plan
due = replica_state.due_indexes
DueIndexes = replica_state.DueIndexes


async def build_due(
    db: Any,
    item: DueIndexes,
    indexer: Indexer,
) -> None:
    """Build ``item``'s indexes one at a time on the standing replica, recording each by name.
    Stops, recording nothing more, as soon as a later landing has replaced the table."""
    for spec in item.specs:
        async with db.acquire() as conn:
            if not await replica_state.index_state_current(conn, item.key, item.landed_at):
                log.info(
                    "replica of %s landed again; its earlier indexes are left to the new landing",
                    ".".join(item.key),
                )
                return
        try:
            await indexer.build(spec)
        except IndexBuildAbandoned:
            log.info(
                "replica of %s landed again while index %s was built; it is left to the new "
                "landing",
                ".".join(item.key),
                spec.name,
            )
            return
        except Exception as exc:  # allow-ble: an index that fails is recorded by name with its cause and logged; the landing it follows stands
            code = DUPLICATE_KEY if isinstance(exc, DuplicateKey) else INDEX_FAILED
            failure = {
                "error": str(exc) or type(exc).__name__,
                "code": code,
                "columns": list(spec.columns),
            }

            def _failed(state: dict, failure: dict = failure, name: str = spec.name) -> None:
                state["failed"][name] = failure

            async with db.acquire() as conn:
                kept = await replica_state.settle_index_state(
                    conn, item.key, item.landed_at, _failed
                )
            if kept:
                log.error(
                    "replica of %s: index %s on (%s) failed: %s",
                    ".".join(item.key),
                    spec.name,
                    ", ".join(spec.columns),
                    failure["error"],
                )
            continue

        def _built(state: dict, name: str = spec.name) -> None:
            if name not in state["built"]:
                state["built"].append(name)

        async with db.acquire() as conn:
            await replica_state.settle_index_state(conn, item.key, item.landed_at, _built)
