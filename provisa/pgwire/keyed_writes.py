# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e4f19-8c63-4a05-9d1e-6f3a0c8b5e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governed write led by WITH, in two steps, for a source that cannot run it as one (REQ-039).

Stage 2 gives an UPDATE or DELETE that reads beyond its target a leading key-set CTE over the
role's governed relation (``WITH k AS (SELECT <key>, <new values> FROM <governed relation> WHERE
<role's WHERE>) UPDATE t ... FROM k WHERE t.<key> = k.<key> AND <grant> AND <rules>``), and a write
whose RETURNING is read stands in a CTE (``WITH w AS (<write> RETURNING <served>) SELECT ... FROM
w``). Where the target's source refuses that form (``with_write``, executor/writable.py), the
same write runs in two steps:

1. the key read (codesource.mutations.key_select, the read a code table's write makes): the
   target's key and each row's new values, over the statement's own WITH, FROM and WHERE -- read
   as any read is;
2. a plain write by key: ``UPDATE t SET <column> = <value> WHERE <key> IN (<keys>) AND <grant>
   AND <rules>``, one a set of new values (a DELETE one a batch of keys), RETURNING the served,
   masked columns of the rows it wrote.

The two steps are not one transaction: a row can change between them. The second step carries
the grant's rows, the role's read row rule and its U or D rule in its WHERE, so it never writes a
row the role may not both read and write, whatever the first step read."""

# Requirements: REQ-039
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import sqlglot.expressions as exp

from provisa.codesource.mutations import BATCH, changes_of, key_select


@dataclass
class TwoStepWrite:
    """A WITH-led write split in two (:func:`split`)."""

    #: The write as it runs: no WITH. An UPDATE's SET values and its key match are given per batch
    #: (:func:`writes`) when it is keyed.
    write: exp.Expr
    #: The key read: the target's key, then the UPDATE's new values. None: the write needs none.
    read: exp.Select | None = None
    #: The target's key columns, each as the write names it (qualified to its alias).
    key: list[exp.Column] = field(default_factory=list)
    #: The columns an UPDATE sets, in order, as the key read gives their new values.
    columns: list[str] = field(default_factory=list)

    @property
    def returned(self) -> list[str]:
        """The names of the columns the write returns (its served, masked RETURNING)."""
        returning = self.write.args.get("returning")
        return [e.alias_or_name for e in returning.expressions] if returning is not None else []


def leads_with_a_write(tree: exp.Expr) -> bool:
    """Whether ``tree``, a governed write, is in the WITH-led form: a WITH on the write, or the
    write in a CTE."""
    from provisa.compiler.write_admission import the_write

    write = the_write(tree)  # pyright: ignore[reportArgumentType]  # sqlglot stub types Expr apart from Expression
    return write is not None and (write is not tree or write.args.get("with_") is not None)


def _conjuncts(where: exp.Where | None) -> list[exp.Expr]:
    if where is None:
        return []
    return (
        list(where.this.flatten(unnest=False)) if isinstance(where.this, exp.And) else [where.this]
    )


def _reads(node: exp.Expr, name: str) -> bool:
    return any(c.table == name for c in node.find_all(exp.Column)) or any(
        t.name == name and not t.db for t in node.find_all(exp.Table)
    )


def split(tree: exp.Expr) -> TwoStepWrite:
    """``tree`` -- a governed write in the WITH-led form (:func:`leads_with_a_write`) -- as its two
    steps. ``tree`` is not changed."""
    from provisa.compiler.stage2 import KEY_MARK
    from provisa.compiler.write_admission import WriteFormNotSupported, _target, the_write

    found = the_write(tree)  # pyright: ignore[reportArgumentType]  # sqlglot stub types Expr apart from Expression
    assert found is not None
    if found is tree:
        own = found.args.get("with_")
        ctes = [c.copy() for c in own.expressions] if own is not None else []
    else:
        cte = found.parent
        assert isinstance(cte, exp.CTE)
        with_ = cte.parent
        assert isinstance(with_, exp.With)
        before = list(with_.expressions)
        ctes = [c.copy() for c in before[: before.index(cte)]]
    write = found.copy()
    write.set("with_", None)
    keyed = f"{KEY_MARK}k"
    if isinstance(write, (exp.Update, exp.Delete)) and _reads(write, keyed):
        return _keyed(write, ctes, keyed)
    if not ctes:
        return TwoStepWrite(write=write)
    source = write.args.get("expression")
    if isinstance(write, exp.Insert) and isinstance(source, exp.Query):
        # An INSERT ... SELECT reading the statement's CTEs reads them in its own SELECT.
        own = source.args.get("with_")
        source.set("with_", exp.With(expressions=[*ctes, *(own.expressions if own else [])]))
        return TwoStepWrite(write=write)
    if any(_reads(write, c.alias) for c in ctes):
        # A MERGE whose source is a CTE of the statement has no form without a leading WITH.
        raise WriteFormNotSupported(_target(write)[0].name, "with_write")
    return TwoStepWrite(write=write)


def _keyed(write: exp.Expr, ctes: list[exp.CTE], keyed: str) -> TwoStepWrite:
    """A keyed UPDATE or DELETE: its key read, and the write by key left once the key-set CTE
    and the key match are taken out."""
    matches: list[exp.Column] = []
    kept: list[exp.Expr] = []
    for term in _conjuncts(write.args.get("where")):
        if (
            isinstance(term, exp.EQ)
            and isinstance(term.this, exp.Column)
            and isinstance(term.expression, exp.Column)
            and term.expression.table == keyed
        ):
            matches.append(term.this)
        elif _reads(term, keyed):
            raise ValueError(
                f"a keyed write's WHERE reads its key set outside the key match: {term}"
            )
        else:
            kept.append(term)
    assert matches, "a keyed write matches its key set by key"
    if isinstance(write, exp.Update):
        changed = {a.this.name for a in write.expressions if isinstance(a, exp.EQ)}
        moved = sorted(changed & {c.name for c in matches})
        if moved:
            # Written by key in batches, a row whose key one batch changes could be matched again
            # by the next: the key is never changed in two steps.
            raise PermissionError(
                f"[V006] {write.this.name!r} is written in two steps, by its key, so a write "
                f"that reads beyond it may not change its key ({', '.join(moved)})"
            )
    reader = write.copy()
    reader.set("with_", exp.With(expressions=ctes) if ctes else None)
    read, columns = key_select(reader, [c.name for c in matches])
    for arg in ("from_", "using", "joins"):
        write.set(arg, None)
    write.set("where", exp.Where(this=exp.and_(*kept)) if kept else None)
    return TwoStepWrite(write=write, read=read, key=matches, columns=columns)


def _parameter(values: list[Any], value: Any) -> exp.Parameter:
    values.append(value)
    return exp.Parameter(this=exp.Literal.number(len(values)))


def writes(two: TwoStepWrite, rows: list[tuple] | None) -> list[tuple[exp.Expr, list[Any]]]:
    """The second step's statements, each with the values its ``$n`` bind: the write as it is
    when it needs no key read, else one for each set of new values the key read's ``rows`` give
    (a DELETE: one), :data:`BATCH` keys a statement. A key read more than once is written once,
    with the first values read for it."""
    if two.read is None:
        return [(two.write.copy(), [])]
    assert rows is not None
    width = len(two.key)
    seen: set[tuple] = set()
    once: list[tuple] = []
    for row in rows:
        if tuple(row[:width]) not in seen:
            seen.add(tuple(row[:width]))
            once.append(row)
    groups: dict[tuple, list[tuple]] = (
        changes_of(once, width, two.columns)
        if isinstance(two.write, exp.Update)
        else ({(): once} if once else {})
    )
    statements: list[tuple[exp.Expr, list[Any]]] = []
    for changes, grouped in groups.items():
        for at in range(0, len(grouped), BATCH):
            values: list[Any] = []
            statement = two.write.copy()
            for assignment, (_column, value) in zip(statement.expressions, changes, strict=True):
                assignment.set("expression", _parameter(values, value))
            batch = grouped[at : at + BATCH]
            if width == 1:
                match: exp.Expr = exp.In(
                    this=two.key[0].copy(),
                    expressions=[_parameter(values, row[0]) for row in batch],
                )
            else:
                match = exp.or_(
                    *(
                        exp.and_(
                            *(
                                exp.EQ(this=column.copy(), expression=_parameter(values, v))
                                for column, v in zip(two.key, row[:width], strict=True)
                            )
                        )
                        for row in batch
                    )
                )
            where = statement.args.get("where")
            terms = [exp.paren(match), *([where.this] if where is not None else [])]
            statement.set("where", exp.Where(this=exp.and_(*terms)))
            statements.append((statement, values))
    return statements
