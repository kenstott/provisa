# Copyright (c) 2026 Kenneth Stott
# Canary: 0014d1e6-01c7-429d-b705-c52ef939345a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's kept mutations (REQ-1942, Reversible mutation handling).

Under Reversible, a mutation never changes the data underneath: it is kept in the environment's
own change log, in its relational store (the environment's schema on the control plane), one log
per table written. A log row is a version of one row of the table, keyed by its primary key:
``upsert`` with the row's values, or ``delete``; ``__seq`` orders the versions. A read of the
table shows its rows with the log applied -- the latest version of each key, a deleted key left
out (:func:`overlay_sql`). Reset mutations drops every log, returning the environment to its
baseline: the parent's real rows, the generated rows, or a database of its own.

A log is runtime state of the environment that kept it: never copied, never registered, never
part of the model.
"""

# Requirements: REQ-1942

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sqlalchemy as sa

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: A change log's name in the environment's schema: the prefix, then the table's id.
PREFIX = "__changes__"
UPSERT = "upsert"
DELETE = "delete"
#: A TRUNCATE: one marker, no key -- every row read before it is deleted.
TRUNCATE = "truncate"
#: The log's own columns, beside the table's.
SEQ, OP, AT, BY = "__seq", "__op", "__at", "__by"


def log_name(table_id: int) -> str:
    return f"{PREFIX}{table_id}"


def log_table(schema: str, table_id: int, columns: list[tuple[str, str]]) -> sa.Table:
    """The change log of table ``table_id`` -- ``columns`` its (name, IR type) -- in ``schema``."""
    from provisa.core.ir_types import to_sqlalchemy

    return sa.Table(
        log_name(table_id),
        sa.MetaData(schema=schema),
        sa.Column(SEQ, sa.BigInteger, sa.Identity(), primary_key=True),
        sa.Column(OP, sa.Text, nullable=False),
        sa.Column(AT, sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column(BY, sa.Text),
        *(sa.Column(name, to_sqlalchemy(ir)) for name, ir in columns),
        sa.CheckConstraint(f"{OP} IN ('{UPSERT}', '{DELETE}', '{TRUNCATE}')"),
    )


async def keep(
    conn: "Connection",
    schema: str,
    table_id: int,
    columns: list[tuple[str, str]],
    op: str,
    rows: list[dict[str, Any]],
    by: str | None,
) -> int:
    """Keep ``rows`` -- each a version of one row, every column given for an upsert, the key for a
    delete -- in table ``table_id``'s log, creating it on the first; how many were kept."""
    if op not in (UPSERT, DELETE, TRUNCATE):
        raise ValueError(f"a kept mutation is {UPSERT!r}, {DELETE!r} or {TRUNCATE!r}, not {op!r}")
    if op == TRUNCATE and rows:
        raise ValueError("a kept TRUNCATE is one marker, with no rows")
    table = log_table(schema, table_id, columns)
    await conn.execute_core(sa.schema.CreateTable(table, if_not_exists=True))
    if op == TRUNCATE:
        await conn.execute_core(table.insert().values({OP: op, BY: by}))
        return 0
    if rows:
        await conn.execute_core(table.insert().values([{**row, OP: op, BY: by} for row in rows]))
    return len(rows)


async def logged(conn: "Connection", schema: str) -> frozenset[int]:
    """The tables of the environment whose schema is ``schema`` that have kept mutations."""
    rows = (
        await conn.execute_core(
            sa.text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :s AND table_name LIKE :p"
            ).bindparams(s=schema, p=PREFIX + "%")
        )
    ).fetchall()
    return frozenset(int(r[0][len(PREFIX) :]) for r in rows)


async def reset(conn: "Connection", schema: str) -> list[int]:
    """Drop every change log of the environment whose schema is ``schema``: its tables read their
    baseline again. The tables whose mutations were dropped."""
    dropped = sorted(await logged(conn, schema))
    for table_id in dropped:
        quoted = '"' + schema.replace('"', '""') + '"'
        await conn.execute_core(sa.text(f'DROP TABLE IF EXISTS {quoted}."{log_name(table_id)}"'))
    return dropped


def overlay_sql(
    base: str, log: str, key: list[str], columns: list[str], log_filter: str | None = None
) -> str:
    """``base`` -- a statement reading a table's rows -- with ``log`` -- the qualified address of
    its change log -- applied in ``__seq`` order: after a truncate marker, the base and every
    version before the latest marker count as deleted; otherwise a row whose key the log holds is
    left out, and the latest version of each key in the log is added unless it is a delete. ``key`` the primary key's columns,
    ``columns`` every column, in the order ``base`` gives them. ``log_filter``: the table's row
    filter over the log's versions (alias ``l``), which ``base`` has applied to its own rows; None
    for a table with none. In the governed dialect."""

    def q(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    cols = ", ".join(f"b.{q(c)}" for c in columns)
    latest_cols = ", ".join(f"l.{q(c)}" for c in columns)
    key_match = " AND ".join(f"k.{q(c)} = b.{q(c)}" for c in key)
    partition = ", ".join(q(c) for c in key)
    marked = f"{q(OP)} = '{TRUNCATE}'"
    # The latest truncate marker's position: versions at or before it went with the base.
    cut = f"COALESCE((SELECT MAX(t.{q(SEQ)}) FROM {log} AS t WHERE t.{marked}), -1)"
    return (
        f"SELECT {cols} FROM ({base}) AS b WHERE NOT EXISTS "
        f"(SELECT 1 FROM {log} AS t WHERE t.{marked}) AND NOT EXISTS "
        f"(SELECT 1 FROM {log} AS k WHERE k.{q(OP)} <> '{TRUNCATE}' AND {key_match}) "
        f"UNION ALL SELECT {latest_cols} FROM (SELECT *, ROW_NUMBER() OVER "
        f"(PARTITION BY {partition} ORDER BY {q(SEQ)} DESC) AS {q('__rank')} FROM {log} "
        f"WHERE {q(OP)} <> '{TRUNCATE}' AND {q(SEQ)} > {cut}) AS l "
        f"WHERE l.{q('__rank')} = 1 AND l.{q(OP)} = '{UPSERT}'"
        + (f" AND ({log_filter})" if log_filter is not None else "")
    )
