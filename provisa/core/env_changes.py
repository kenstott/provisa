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
out (:func:`overlay`). Reset mutations drops every log, returning the environment to its
baseline: the parent's real rows, the generated rows, or a database of its own.

A log is runtime state of the environment that kept it: never copied, never registered, never
part of the model.
"""

# Requirements: REQ-1942

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
import sqlglot.expressions as exp

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


def _q(name: str) -> exp.Identifier:
    return exp.to_identifier(name, quoted=True)


def _col(table: str, name: str) -> exp.Column:
    return exp.Column(this=_q(name), table=exp.to_identifier(table))


def _op_is(table: str | None, op: str, *, negated: bool = False) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    column = _col(table, OP) if table else exp.Column(this=_q(OP))
    kind = exp.NEQ if negated else exp.EQ
    return kind(this=column, expression=exp.Literal.string(op))


def overlay(
    base: exp.Query,
    log: tuple[str, ...],
    key: list[str],
    columns: list[str],
    log_filter: exp.Expr | None = None,  # pyright: ignore[reportPrivateImportUsage]
    carried: dict[str, exp.Expr] | None = None,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Union:  # REQ-1942, REQ-913
    """``base`` -- a statement reading a table's rows -- with ``log`` -- its change log's address,
    by its name parts -- applied in ``__seq`` order, built on the AST: after a truncate marker,
    the base and every version before the latest marker count as deleted; otherwise a row whose
    key the log holds is left out, and the latest version of each key in the log is added unless
    it is a delete. ``key`` the primary key's columns, ``columns`` every column, in the order
    ``base`` gives them. ``log_filter``: the table's row filter over the log's versions (alias
    ``l``), which ``base`` has applied to its own rows; None for a table with none. ``carried``:
    further outputs, by name, that ``base`` gives after ``columns``, each with the same expression
    over a log version (alias ``l``) -- a meta-role's member flags and real values, computed alike
    on the base's rows and the log's (REQ-1620)."""
    *qualifier, name = log
    db = qualifier[-1] if qualifier else None
    catalog = qualifier[-2] if len(qualifier) > 1 else None

    def log_as(alias: str | None) -> exp.Table:
        table = exp.table_(_q(name), db=db and _q(db), catalog=catalog and _q(catalog))
        if alias is not None:
            table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
        return table

    extra = carried or {}
    kept_rows = (
        exp.select(*[_col("b", c) for c in columns], *[_col("b", n) for n in extra])
        .from_(exp.Subquery(this=base.copy(), alias=exp.TableAlias(this=exp.to_identifier("b"))))
        .where(
            exp.Not(
                this=exp.Exists(
                    this=exp.select(exp.Literal.number(1))
                    .from_(log_as("t"))
                    .where(_op_is("t", TRUNCATE))
                )
            )
        )
        .where(
            exp.Not(
                this=exp.Exists(
                    this=exp.select(exp.Literal.number(1))
                    .from_(log_as("k"))
                    .where(
                        exp.and_(
                            _op_is("k", TRUNCATE, negated=True),
                            *(exp.EQ(this=_col("k", c), expression=_col("b", c)) for c in key),
                        )
                    )
                )
            )
        )
    )
    # The latest truncate marker's position: versions at or before it went with the base.
    cut = exp.func(
        "COALESCE",
        exp.Subquery(
            this=exp.select(exp.func("MAX", _col("t", SEQ)))
            .from_(log_as("t"))
            .where(_op_is("t", TRUNCATE))
        ),
        exp.Neg(this=exp.Literal.number(1)),
    )
    ranked = (
        exp.select(
            exp.Star(),
            exp.alias_(
                exp.Window(
                    this=exp.RowNumber(),
                    partition_by=[exp.Column(this=_q(c)) for c in key],
                    order=exp.Order(
                        expressions=[
                            exp.Ordered(this=exp.Column(this=_q(SEQ)), desc=True, nulls_first=True)
                        ]
                    ),
                ),
                _q("__rank"),
            ),
        )
        .from_(log_as(None))
        .where(
            exp.and_(
                _op_is(None, TRUNCATE, negated=True),
                exp.GT(this=exp.Column(this=_q(SEQ)), expression=cut),
            )
        )
    )
    latest = exp.select(
        *[_col("l", c) for c in columns],
        *[exp.alias_(e.copy(), _q(n)) for n, e in extra.items()],
    ).from_(exp.Subquery(this=ranked, alias=exp.TableAlias(this=exp.to_identifier("l"))))
    conditions = [
        exp.EQ(this=_col("l", "__rank"), expression=exp.Literal.number(1)),
        _op_is("l", UPSERT),
    ]
    if log_filter is not None:
        conditions.append(exp.Paren(this=log_filter.copy()))
    latest = latest.where(exp.and_(*conditions))
    return exp.union(kept_rows, latest, distinct=False)
