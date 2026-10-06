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

from provisa.fakes.kinds import Ordered, Sql
from provisa.fakes.read_sql import (
    Column,
    _q,
    definition_of,
    digest_sql,
    expression,
    seed_sql,
    uniform_sql,
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


def faked_projection(
    base: str,
    alias: str,
    columns: list[tuple[str, str, str]],
    fakes: dict[str, Column],
    fingerprint: str,
    row_filter: str | None,
) -> str:
    """The projection, in the governed dialect. ``base`` is the base relation as written,
    ``columns`` every column as (name, data type, family), ``fakes`` the faked ones by name, and
    ``row_filter`` the role's filter qualified by ``alias``."""
    a = _q(alias)
    inner = [f"{a}.{_q(name)}" for name, _, _ in columns]
    for name in fakes:
        d = digest_sql(fingerprint, f"{a}.{_q(name)}")
        inner.append(f"{d} AS {_q(_DIGEST + name)}")
    where = f" WHERE {row_filter}" if row_filter else ""
    level = f"SELECT {', '.join(inner)} FROM {base} AS {a}{where}"
    return f"({layered(level, alias, columns, fakes)})"


def layered(
    level: str,
    alias: str,
    columns: list[tuple[str, str, str]],
    fakes: dict[str, Column],
    keep: tuple[str, ...] = (),
) -> str:
    """The fakes computed over ``level``, a statement giving every column of ``columns`` and each
    faked column's digest as ``__digest__<name>``: each column's uniform point, then the fakes in
    dependency order, then every column under its own name (faked or as ``level`` gives it). A
    faked read's level digests the real values (:func:`faked_projection`); synthetic generation's
    digests the dataset's seed and the row (REQ-1939). ``keep`` names further columns of ``level``
    carried through to the outer level as they are."""
    families = {name: fam for name, _, fam in columns}
    a = _q(alias)
    # The uniform point reads the digest the level below computed, once per row.
    held = (
        [_q(name) for name, _, _ in columns]
        + [_q(_DIGEST + n) for n in fakes]
        + [_q(k) for k in keep if k not in {_DIGEST + n for n in fakes}]
    )
    # REQ-1494: each column's point is drawn from its digest mixed with its fake's definition.
    uniforms = [
        f"{uniform_sql(seed_sql(_q(_DIGEST + n), definition_of(c)))} AS {_q(_UNIFORM + n)}"
        for n, c in fakes.items()
    ]
    level = f"SELECT {', '.join(held + uniforms)} FROM ({level}) AS {a}"
    held += [_q(_UNIFORM + n) for n in fakes]

    def faked(name: str) -> str:
        return _q(_FAKE + name) if name in fakes else _q(name)

    depths = _depths(fakes)
    for depth in range(1, max(depths.values(), default=0) + 1):
        computed = []
        for name, col in fakes.items():
            if depths[name] != depth:
                continue
            value = expression(
                col,
                real=_q(name),
                u=_q(_UNIFORM + name),
                digest=_q(_DIGEST + name),
                faked=faked,
                family_of=families.__getitem__,
            )
            computed.append(f"{value} AS {_q(_FAKE + name)}")
        level = f"SELECT {', '.join(held + computed)} FROM ({level}) AS {a}"
        held += [_q(_FAKE + n) for n, d in depths.items() if d == depth]
    outer = [
        f"{_q(_FAKE + name)} AS {_q(name)}" if name in fakes else _q(name) for name, _, _ in columns
    ] + [_q(k) for k in keep]
    return f"SELECT {', '.join(outer)} FROM ({level}) AS {a}"
