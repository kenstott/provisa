# Copyright (c) 2026 Kenneth Stott
# Canary: 4cd8ccaf-882e-4b23-9f93-adfc37b410f9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The checks a table's fakes pass when saved and when the model is loaded (REQ-1494).

Each column's fake must fit the column's type; a relative fake (after, before, greater_than,
less_than, sql) must name columns the table holds, of a type it can follow; a ``sql_group`` fake
reads children only through a relationship of which the table is the parent; the references of a
table's fakes must not form a cycle; a stable fake must be one the portable definition computes;
and two columns joined by a relationship, both faked, must declare the same fake and agree on
stable, so a join through them still matches. Every refusal names the column.
"""

# Requirements: REQ-1494

from __future__ import annotations

import datetime as _dt
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TypeVar

from provisa.fakes import sql_subset
from provisa.fakes.kinds import (
    kind_name,
    Bool,
    Bucket,
    Categories,
    Encrypt,
    FakeKind,
    FakeRefused,
    Hash,
    LogNormal,
    Method,
    Normal,
    Ordered,
    Pattern,
    Percentiles,
    Poisson,
    Prefix,
    Profile,
    Sequence,
    Sql,
    Triangular,
    Truncate,
    Uniform,
    parse,
)
from provisa.fakes.methods import STABLE_METHODS, check_method

_INTEGER = {"tinyint", "smallint", "integer", "int", "bigint", "int2", "int4", "int8", "long"}
_NUMERIC = {"real", "double", "double precision", "float", "float4", "float8", "decimal", "numeric"}
_TEXT = {"varchar", "char", "text", "string", "character varying", "character", "uuid"}
_DATE = {"date"}
_TIMESTAMP = {
    "timestamp",
    "timestamp with time zone",
    "timestamp without time zone",
    "timestamptz",
    "datetime",
}


def family(data_type: str) -> str:
    """The family of values a column of ``data_type`` holds: text, integer, numeric, boolean, date,
    timestamp, or other."""
    base = data_type.lower().split("(")[0].strip()
    if base in _INTEGER:
        return "integer"
    if base in _NUMERIC:
        return "numeric"
    if base in _TEXT:
        return "text"
    if base in ("boolean", "bool"):
        return "boolean"
    if base in _DATE:
        return "date"
    if base in _TIMESTAMP:
        return "timestamp"
    return "other"


_N = TypeVar("_N", str, tuple[str, str])

_NUMBERS = ("integer", "numeric")
_TEMPORAL = ("date", "timestamp")


@dataclass(frozen=True)
class DeclaredColumn:
    """A column as its table declares it: its type, its fake as written (None for none), whether
    that fake is stable, and its synthetic rule (None for none) -- laid over the fake, used by
    synthetic generation only (REQ-1494, REQ-1939)."""

    name: str
    data_type: str
    fake: str | None = None
    stable: bool = False
    rule: str | None = None


@dataclass(frozen=True)
class Checked:
    """A table's fakes and synthetic rules, checked, by column."""

    fakes: dict[str, FakeKind]
    rules: dict[str, FakeKind]

    def generated(self, name: str) -> FakeKind | None:
        """What generates the column: its synthetic rule, else its fake."""
        return self.rules.get(name, self.fakes.get(name))


@dataclass(frozen=True)
class Child:
    """A table joined to this one as its child by a relationship: its name and columns' types."""

    table: str
    columns: dict[str, str] = field(default_factory=dict)


def check_table(table: str, columns: list[DeclaredColumn], children: dict[str, Child]) -> Checked:
    """Each column's fake and synthetic rule, the table's checked together; ``children`` the
    tables joined to this one as children, by relationship name. Neither the fakes a read computes
    nor what generation computes -- each column's rule, else its fake -- may name one another in a
    cycle."""
    types = {c.name: c.data_type for c in columns}
    fakes: dict[str, FakeKind] = {}
    rules: dict[str, FakeKind] = {}
    fake_reads: dict[str, frozenset[str]] = {}
    rule_reads: dict[str, frozenset[str]] = {}
    for c in columns:
        if c.fake is None and c.stable:
            raise FakeRefused(f"{table}.{c.name} is declared stable but declares no fake")
        try:
            if c.fake is not None:
                kind = parse(c.fake)
                fake_reads[c.name] = _check_column(kind, c, types, children)
                if c.stable:
                    _check_stable(kind)
                fakes[c.name] = kind
            if c.rule is not None:
                try:
                    rule = parse(c.rule, rule=True)
                    rule_reads[c.name] = _check_column(rule, c, types, children)
                except FakeRefused as exc:
                    raise FakeRefused(f"its synthetic rule: {exc}") from exc
                rules[c.name] = rule
        except FakeRefused as exc:
            raise FakeRefused(f"{table}.{c.name}: {exc}") from exc
    stable = {c.name for c in columns if c.stable}
    for name in sorted(stable):
        for read in sorted(fake_reads.get(name, frozenset())):
            if read in fakes and read not in stable:
                raise FakeRefused(
                    f"{table}.{name}: a stable fake reads {read}, whose fake is not stable, so it "
                    f"would differ by engine; make {read}'s fake stable too"
                )
    generation = {**fake_reads, **rule_reads}
    for reads, what in ((fake_reads, "fakes"), (generation, "synthetic rules and fakes")):
        cycle = _cycle(reads)
        if cycle:
            raise FakeRefused(
                f"{table}: the {what} of {', '.join(cycle)} name one another in a cycle "
                f"({' -> '.join(cycle + [cycle[0]])})"
            )
    return Checked(fakes, rules)


def _check_column(
    kind: FakeKind, c: DeclaredColumn, types: dict[str, str], children: dict[str, Child]
) -> frozenset[str]:
    """Check one column's fake against its type and the table; return the columns it reads."""
    fam = family(c.data_type)
    if isinstance(kind, Categories):
        for v in kind.values or ():
            _value_fits(v, fam)
    elif isinstance(kind, Bool):
        _needs(fam, ("boolean",), "bool")
    elif isinstance(kind, (Percentiles, Normal, LogNormal, Uniform, Triangular)):
        # Points given as dates or times describe a date or time column; numbers a numeric one.
        _needs(fam, _TEMPORAL if kind.temporal else _NUMBERS, type(kind).__name__.lower())
    elif isinstance(kind, Poisson):
        _needs(fam, ("integer",), "poisson")
    elif isinstance(kind, Profile):
        _needs(fam, _NUMBERS + _TEMPORAL, "profile")
    elif isinstance(kind, Bucket):
        _needs(fam, _NUMBERS, "bucket")
    elif isinstance(kind, Truncate):
        _needs(fam, _TEMPORAL, "truncate")
        if fam == "date" and kind.unit in ("hour", "minute", "second"):
            raise FakeRefused(f"a date has no {kind.unit} to truncate to")
    elif isinstance(kind, Prefix):
        _needs(fam, ("text",), "prefix")
    elif isinstance(kind, Pattern):
        _needs(fam, ("text",), "pattern")
    elif isinstance(kind, (Hash, Encrypt)):
        _needs(fam, ("text", "integer"), type(kind).__name__.lower())
    elif isinstance(kind, Ordered):
        return _check_ordered(kind, fam, types)
    elif isinstance(kind, Sql):
        return _check_sql(kind, types, children)
    elif isinstance(kind, Sequence):
        for v in kind.states:
            _value_fits(v, fam)
        for col in (kind.entity, kind.order):
            if col not in types:
                raise FakeRefused(f"sequence() names {col!r}, which the table does not hold")
        return frozenset({kind.entity, kind.order})
    elif isinstance(kind, Method):
        check_method(kind, fam)
    return frozenset()


def _needs(fam: str, families: tuple[str, ...], kind: str) -> None:
    if fam not in families:
        raise FakeRefused(f"{kind}() fakes a {' or '.join(families)} column, not a {fam} one")


def _value_fits(value: str, fam: str) -> None:
    try:
        if fam == "integer":
            int(value)
        elif fam == "numeric":
            float(value)
        elif fam == "boolean":
            if value.lower() not in ("true", "false"):
                raise ValueError(value)
        elif fam == "date":
            _dt.date.fromisoformat(value)
        elif fam == "timestamp":
            _dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise FakeRefused(f"the value {value!r} is not a {fam}") from exc


def _check_ordered(kind: Ordered, fam: str, types: dict[str, str]) -> frozenset[str]:
    temporal = kind.kind in ("after", "before")
    families = _TEMPORAL if temporal else _NUMBERS
    _needs(fam, families, kind.kind)
    if kind.column not in types:
        raise FakeRefused(f"{kind.kind}() names {kind.column!r}, which the table does not hold")
    named = family(types[kind.column])
    if named not in families:
        raise FakeRefused(
            f"{kind.kind}() names {kind.column!r}, a {named} column, not a "
            f"{' or '.join(families)} one"
        )
    return frozenset({kind.column})


#: The name a ``sql_group`` expression gives the value its fake= parameter drew (REQ-1494, THE
#: FAKE PARAMETER OF SQL_GROUP AND SEQUENCE).
SELF = "self"


def _check_sql(kind: Sql, types: dict[str, str], children: dict[str, Child]) -> frozenset[str]:
    read = sql_subset.read(kind.expression, group=kind.group)
    columns = read.columns - {SELF} if kind.group else read.columns
    for col in sorted(columns):
        if col not in types:
            raise FakeRefused(f"{kind.expression!r} names {col!r}, which the table does not hold")
    for rel, cols in sorted(read.children.items()):
        child = children.get(rel)
        if child is None:
            raise FakeRefused(
                f"{kind.expression!r} reads {rel!r}, which is no relationship to a child of this "
                f"table"
            )
        missing = sorted(cols - set(child.columns))
        if missing:
            raise FakeRefused(
                f"{kind.expression!r} names {rel}.{missing[0]}, which {child.table} does not hold"
            )
    return columns


def own_reads(kind: FakeKind) -> frozenset[str]:
    """The columns of its own table a fake -- already checked -- reads."""
    if isinstance(kind, Ordered):
        return frozenset({kind.column})
    if isinstance(kind, Sql):
        cols = sql_subset.read(kind.expression, group=kind.group).columns
        return cols - {SELF} if kind.group else cols
    if isinstance(kind, Sequence):
        return frozenset({kind.entity, kind.order})
    return frozenset()


def model_reads(
    table: str, kind: FakeKind, children: dict[str, Child]
) -> frozenset[tuple[str, str]]:
    """The (table, column) pairs a fake -- already checked -- reads: its own table's columns, and
    the children's columns a ``sql_group`` fake aggregates."""
    out = {(table, col) for col in own_reads(kind)}
    if isinstance(kind, Sql) and kind.group:
        read = sql_subset.read(kind.expression, group=True)
        out |= {(children[rel].table, col) for rel, cols in read.children.items() for col in cols}
    return frozenset(out)


def model_cycle(reads: dict[tuple[str, str], frozenset[tuple[str, str]]]) -> list[tuple[str, str]]:
    """A cycle among the model's fakes, across tables, as the columns in it, or empty (REQ-1939,
    GENERATION IN PASSES)."""
    return _cycle(reads)


def _check_stable(kind: FakeKind) -> None:
    """A stable fake is the same on every engine and in every region (REQ-1494, A STABLE FAKE):
    the portable definition's methods, with no arguments it does not take, and the kinds that are
    pure functions of their declaration -- never one that reads what is measured where it is read."""
    if isinstance(kind, Method):
        if kind.name not in STABLE_METHODS:
            raise FakeRefused(
                f"{kind.name}() cannot be stable: a stable fake is the same on every engine, which "
                f"only these methods are -- {', '.join(sorted(STABLE_METHODS))}"
            )
        if kind.args:
            raise FakeRefused(
                f"a stable {kind.name}() takes no arguments: the portable definition computes it "
                f"one way on every engine"
            )
    if isinstance(kind, Profile) and kind.run is None:
        raise FakeRefused("a stable profile() fake pins the run it reads: name it as run=<id>")
    if isinstance(kind, Pattern):
        raise FakeRefused("pattern() cannot be stable: it fills the shapes the latest run records")
    measured = (
        (isinstance(kind, Categories) and kind.values is None)
        or (isinstance(kind, Bool) and kind.share is None)
        or (isinstance(kind, Ordered) and kind.distance is None)
    )
    if measured:
        raise FakeRefused(
            f"a stable {kind_name(kind)}() declares what it would otherwise measure where it is "
            f"read, so it is the same everywhere"
        )


def _cycle(reads: Mapping[_N, frozenset[_N]]) -> list[_N]:
    """A cycle among the columns' references, as the columns in it, or empty."""
    state: dict[_N, int] = {}
    path: list[_N] = []

    def visit(col: _N) -> list[_N]:
        state[col] = 1
        path.append(col)
        for nxt in sorted(reads.get(col, frozenset())):
            if state.get(nxt) == 1:
                return path[path.index(nxt) :]
            if nxt not in state:
                found = visit(nxt)
                if found:
                    return found
        state[col] = 2
        path.pop()
        return []

    for col in sorted(reads):
        if col not in state:
            found = visit(col)
            if found:
                return found
    return []


@dataclass(frozen=True)
class Join:
    """Two columns a relationship joins."""

    relationship: str
    one: tuple[str, str]  # (table, column)
    other: tuple[str, str]


def check_joins(
    fakes: dict[tuple[str, str], tuple[FakeKind, bool, int | None, str]], joins: list[Join]
) -> None:
    """Two columns joined by a relationship and both faked hold one canonical definition -- the
    same fake, stable or not, for a stable fake the same pinned portable definition version, and
    the same type as the definition names it (provisa.fakes.digest.canonical_type) -- so the join
    keeps matching (REQ-1494, A STABLE FAKE)."""
    for j in joins:
        one, other = fakes.get(j.one), fakes.get(j.other)
        if one is None or other is None or one == other:
            continue
        if one[0] != other[0]:
            how = ""
        elif one[1] != other[1]:
            how = " in stable"
        elif one[2] != other[2]:
            how = f" in the definition version they are pinned to ({one[2]} and {other[2]})"
        else:
            how = f" in type ({one[3]} and {other[3]})"
        a, b = ".".join(j.one), ".".join(j.other)
        raise FakeRefused(
            f"relationship {j.relationship!r} joins {a} to {b}, whose fakes differ{how}; give "
            f"them one fake, so the join still matches"
        )
