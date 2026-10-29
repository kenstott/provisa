# Copyright (c) 2026 Kenneth Stott
# Canary: 037ab5df-c00a-48cb-a1cb-8ea7d1582932
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a Cypher write did, counted as Neo4j counts it: the ``stats`` of a Bolt summary, and the
``counters`` of an HTTP Cypher answer.

A surface opens one collector per statement (:func:`counting`); each write that statement makes,
on the governed path or in the graph session, adds the rows it wrote (:func:`count`). The rows
written are counted, never the statement's shape: a MERGE that matched creates nothing, and a
SET of an unchanged value is a property set, as Neo4j counts it. A count the write cannot make
exactly -- a source that reports no row count, a SET to null of rows whose prior value is
unread -- makes the statement's counters unknown (:func:`unknown`): none are reported, and the
surface says why.
"""

# Requirements: REQ-670, REQ-2176

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, fields
from typing import Any, Awaitable, Callable

from provisa.core.statement_warnings import ServerWarning

#: Each counter, as the Bolt summary's ``stats`` names it.
STATS_KEYS = {
    "nodes_created": "nodes-created",
    "nodes_deleted": "nodes-deleted",
    "relationships_created": "relationships-created",
    "relationships_deleted": "relationships-deleted",
    "properties_set": "properties-set",
    "labels_added": "labels-added",
    "labels_removed": "labels-removed",
    "indexes_added": "indexes-added",
    "indexes_removed": "indexes-removed",
    "constraints_added": "constraints-added",
    "constraints_removed": "constraints-removed",
}


@dataclass
class Counters:
    """One statement's counters; ``unknown`` the reasons a count could not be made."""

    nodes_created: int = 0
    nodes_deleted: int = 0
    relationships_created: int = 0
    relationships_deleted: int = 0
    properties_set: int = 0
    labels_added: int = 0
    labels_removed: int = 0
    indexes_added: int = 0
    indexes_removed: int = 0
    constraints_added: int = 0
    constraints_removed: int = 0
    unknown: list[str] = field(default_factory=list)

    def stats(self) -> dict[str, Any]:
        """The Bolt summary's ``stats``: each counter that is not zero, and ``contains-updates``;
        empty when nothing changed. Asked only of known counters."""
        assert not self.unknown, "unknown counters are not reported"
        out: dict[str, Any] = {
            STATS_KEYS[f.name]: getattr(self, f.name)
            for f in fields(self)
            if f.name in STATS_KEYS and getattr(self, f.name)
        }
        if out:
            out["contains-updates"] = True
        return out


_open: ContextVar[Counters | None] = ContextVar("provisa_cypher_counters", default=None)


@contextmanager
def counting() -> Generator[Counters]:
    """Count the writes made inside the block. A block nested in an open one adds to the same
    counters: one statement, one count."""
    current = _open.get()
    if current is not None:
        yield current
        return
    found = Counters()
    token = _open.set(found)
    try:
        yield found
    finally:
        _open.reset(token)


@contextmanager
def counting_apart() -> Generator[Counters]:
    """Count the writes made inside the block on counters of their own, even inside an open
    collector: for a batch answered one result per statement."""
    found = Counters()
    token = _open.set(found)
    try:
        yield found
    finally:
        _open.reset(token)


def count(**by: int) -> None:
    """Add ``by`` (counter name -> rows) to the statement's counters. A write no surface counts
    (no collector open) is told to no one."""
    found = _open.get()
    if found is None:
        return
    for name, n in by.items():
        setattr(found, name, getattr(found, name) + n)


def unknown(reason: str) -> None:
    """The statement's counters cannot be made exactly: ``reason`` says why."""
    found = _open.get()
    if found is not None and reason not in found.unknown:
        found.unknown.append(reason)


def count_unknown_warning(statement: str, reason: str) -> ServerWarning:
    """What an answer says when one count could not be made: the browser's node/relationship
    totals, or a write's counters."""
    return ServerWarning(
        code="cypher.count_unknown",
        params={"statement": statement, "reason": reason},
        message=f"A count could not be made, so the total is unknown: {statement}: {reason}",
    )


#: The surface's own governed write of one SQL statement: its columns, rows and the source's
#: row count (None when the source reports none).
SqlWrite = Callable[[str, list[Any]], Awaitable[tuple[list[str], list[list[Any]], int | None]]]


def _returns_rows(sql: str) -> bool:
    import sqlglot  # noqa: PLC0415

    return sqlglot.parse_one(sql, read="postgres").args.get("returning") is not None


async def written_rows(
    write: SqlWrite, sql: str, bound: list[Any], **per_row: int
) -> tuple[list[str], list[list[Any]]]:
    """Run ``sql`` through ``write`` and count ``per_row`` once per row it wrote: the source's row
    count, or the rows its RETURNING gave. A statement neither counts is unknown."""
    columns, rows, rowcount = await write(sql, bound)
    if rowcount is not None:
        n: int | None = rowcount
    elif _returns_rows(sql):
        n = len(rows)
    else:
        n = None
    if n is None:
        unknown(f"the source reported no row count for {sql.split(' ', 1)[0]}")
    else:
        count(**{name: by * n for name, by in per_row.items()})
    return columns, rows


def carried_labels(label: str) -> int:
    """The labels a model row of ``label`` is read with: a ``Domain:Table`` label is two (as the
    Bolt surface packs it), any other one."""
    return 2 if ":" in label else 1


def set_values(values: list[Any], provided: dict[str, Any]) -> list[Any]:
    """Each value a write sets, a ``$name`` parameter's as the request supplies it."""
    from provisa.cypher.write_translator import CypherParam  # noqa: PLC0415

    return [provided[v.name] if isinstance(v, CypherParam) else v for v in values]
