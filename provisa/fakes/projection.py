# Copyright (c) 2026 Kenneth Stott
# Canary: 767ebe1b-94d8-4546-b112-b2f039f68c4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The faked projection of a base relation (REQ-1494).

A table some of whose columns a role reads as fakes is read through one projection that defines
each faked column once, in dependency order -- a relative fake naming the faked form of the column
it follows -- so every use of the column in the statement (select list, filters, joins, grouping,
ordering, windows, subqueries) reads the same fake::

    (SELECT <each column, faked or real> FROM
      (... (SELECT t.*, <digest and uniform point of each faked column> FROM base AS t
            WHERE <the role's row filter, on the real values>) ...)) AS t

The innermost level reads the base relation, applies the row filter to the real values and computes
each faked column's digest and uniform point once; each level after it computes the fakes whose
references the levels below hold; the outer level gives every column its own name.
"""

# Requirements: REQ-1494

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.fakes.kinds import Ordered, Sql
from provisa.fakes.read_sql import (
    Column,
    column,
    definition_of,
    digest,
    expression,
    seed,
    uniform,
)
from provisa.fakes.sql_subset import read as read_sql

_DIGEST = "__digest__"
#: The name a level gives a faked column's digest (``DIGEST + <column>``), for :func:`layered`.
DIGEST = _DIGEST
_UNIFORM = "__u__"
_FAKE = "__fake__"


def references(col: Column) -> frozenset[str]:
    """The columns a fake reads by name (a relative fake's, an sql fake's)."""
    return _references(col)


def _references(col: Column) -> frozenset[str]:
    k = col.kind
    if isinstance(k, Ordered):
        return frozenset({k.column})
    if isinstance(k, Sql):
        return read_sql(k.expression, group=False).columns
    return frozenset()


def _depths(fakes: dict[str, Column]) -> dict[str, int]:
    """Each faked column's level: one more than the deepest faked column it reads. The model's
    checks refuse a cycle when the fakes are saved, so the walk ends."""
    depth: dict[str, int] = {}

    def of(name: str) -> int:
        if name not in depth:
            refs = [r for r in _references(fakes[name]) if r in fakes]
            depth[name] = 1 + max((of(r) for r in refs), default=0)
        return depth[name]

    for name in fakes:
        of(name)
    return depth


def _derived(query: exp.Query, alias: str) -> exp.Subquery:
    return exp.Subquery(
        this=query, alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True))
    )


def faked_projection(
    base: exp.Table,
    alias: str,
    columns: list[tuple[str, str, str]],
    fakes: dict[str, Column],
    fingerprint: str,
    row_filter: exp.Expr | None,  # pyright: ignore[reportPrivateImportUsage]
    carried: dict[str, exp.Expr] | None = None,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Select:  # REQ-1494, REQ-913
    """The projection, built on the AST. ``base`` is the base relation, ``columns`` every column
    as (name, data type, family), ``fakes`` the faked ones by name, and ``row_filter`` the role's
    filter qualified by ``alias``. ``carried``: further outputs (name -> expression over the real
    row, qualified by ``alias``), computed before any fake and given as they are -- a meta-role's
    member flags and the real values its members read (REQ-1620)."""

    def real(name: str) -> exp.Column:
        return exp.column(name, table=alias, quoted=True)

    inner: list[exp.Expr] = [real(name) for name, _, _ in columns]  # pyright: ignore[reportPrivateImportUsage]
    for name in fakes:
        inner.append(exp.alias_(digest(fingerprint, real(name)), _DIGEST + name, quoted=True))
    inner += [exp.alias_(e.copy(), name, quoted=True) for name, e in (carried or {}).items()]
    source = base.copy()
    source.set("alias", exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
    level = exp.select(*inner).from_(source)
    if row_filter is not None:
        level = level.where(row_filter.copy())
    return layered(level, alias, columns, fakes, keep=tuple(carried or ()))


def layered(
    level: exp.Query,
    alias: str,
    columns: list[tuple[str, str, str]],
    fakes: dict[str, Column],
    keep: tuple[str, ...] = (),
    uniforms: dict[str, exp.Expr] | None = None,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Select:  # REQ-1494, REQ-913
    """The fakes computed over ``level``, a statement giving every column of ``columns`` and each
    faked column's digest as ``__digest__<name>``: each column's uniform point, then the fakes in
    dependency order, then every column under its own name (faked or as ``level`` gives it). A
    faked read's level digests the real values (:func:`faked_projection`); synthetic generation's
    digests the dataset's seed and the row (REQ-1939). ``keep`` names further columns of ``level``
    carried through to the outer level as they are. ``uniforms`` gives a faked column's uniform
    point where something else decides it (synthetic generation's dependence, REQ-1939)."""
    families = {name: fam for name, _, fam in columns}
    # The uniform point reads the digest the level below computed, once per row.
    held = (
        [name for name, _, _ in columns]
        + [_DIGEST + n for n in fakes]
        + [k for k in keep if k not in {_DIGEST + n for n in fakes}]
    )
    # REQ-1494: each column's point is drawn from its digest mixed with its fake's definition.
    decided = uniforms or {}
    points = [
        exp.alias_(
            decided[n].copy()
            if n in decided
            else uniform(seed(column(_DIGEST + n), definition_of(c))),
            _UNIFORM + n,
            quoted=True,
        )
        for n, c in fakes.items()
    ]
    current = exp.select(*(column(h) for h in held), *points).from_(_derived(level, alias))
    held += [_UNIFORM + n for n in fakes]

    def faked(name: str) -> exp.Column:
        return column(_FAKE + name if name in fakes else name)

    depths = _depths(fakes)
    for depth in range(1, max(depths.values(), default=0) + 1):
        computed = []
        for name, col in fakes.items():
            if depths[name] != depth:
                continue
            value = expression(
                col,
                real=column(name),
                u=column(_UNIFORM + name),
                digest=column(_DIGEST + name),
                faked=faked,
                family_of=families.__getitem__,
            )
            computed.append(exp.alias_(value, _FAKE + name, quoted=True))
        current = exp.select(*(column(h) for h in held), *computed).from_(_derived(current, alias))
        held += [_FAKE + n for n, d in depths.items() if d == depth]
    outer: list[exp.Expr] = [  # pyright: ignore[reportPrivateImportUsage]
        exp.alias_(column(_FAKE + name), name, quoted=True) if name in fakes else column(name)
        for name, _, _ in columns
    ] + [column(k) for k in keep]
    return exp.select(*outer).from_(_derived(current, alias))
