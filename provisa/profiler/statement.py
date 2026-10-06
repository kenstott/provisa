# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2f6a41-c39e-4b75-a0d8-17e5b9c3f6a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one profile statement (REQ-1934) and the reading of its result.

A run reads the profiled table ONCE. Every measure comes out of a single grouped SELECT over the
table's rows cross-joined to a small set of group selectors ``k``:

* ``k = 0`` groups every row together; the table-wide aggregates (counts, extremes, moments, the
  101-point quantile sketch, text lengths, children per parent) are computed only on that group,
  each aggregate's input being ``CASE WHEN k = 0 THEN … END`` so the other groups feed it nulls;
* ``k = i`` (one per profiled column) groups by the column's value, which yields its distinct count,
  its most frequent values and, for a low-cardinality column, its whole value-frequency table;
* ``k = n + j`` (one per text column) groups by the value's SHAPE — upper-case letters to ``A``,
  lower-case to ``a``, digits to ``9``, other characters kept;
* one ``k`` groups by the whole row (every profiled column), and one per declared primary or
  unique key by the key's columns, which yields the duplicate rows and the key values held by more
  than one row.

A window ranks each group within its ``k`` and the outer filter keeps the ``k = 0`` row and the
best-ranked value and shape groups, so the engine returns a bounded result whatever the table's
size. Children per parent come from each child table aggregated by its key and left-joined to the
profiled rows inside the same statement, so the profiled table is still scanned once.

The statement is written in the governed SQL dialect (postgres) against the names pgwire publishes
and goes through the one governed pipeline (``provisa.profiler.run``), which transpiles it for the
engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from provisa.compiler.sql_literals import sql_literal
from provisa.dq.catalog import NUMERIC_TYPES

# REQ-1934: the quantile sketch is 101 points, p0..p100.
QUANTILE_POINTS: tuple[float, ...] = tuple(round(i / 100, 2) for i in range(101))
# How many most-frequent values and shapes a run keeps per column.
TOP_N = 20

_NUMERIC = frozenset(NUMERIC_TYPES) | {"real", "decimal", "int", "float4", "float8", "int4", "int8"}
_TEMPORAL = frozenset({"date", "timestamp", "timestamptz"})
_TEXT = frozenset({"text", "varchar", "uuid", "char", "string"})
_BOOLEAN = frozenset({"boolean", "bool"})


def family_of(data_type: str | None) -> str:
    """numeric | temporal | text | boolean | other — by the column's registered IR type."""
    base = (data_type or "").lower().split("(")[0].strip()
    if base in _NUMERIC:
        return "numeric"
    if base in _TEMPORAL:
        return "temporal"
    if base in _TEXT:
        return "text"
    if base in _BOOLEAN:
        return "boolean"
    return "other"


@dataclass(frozen=True)
class ColumnSpec:
    name: str  # the column as pgwire publishes it to the org admin
    data_type: str | None
    family: str
    physical: str  # the column's registered name


@dataclass(frozen=True)
class FanoutSpec:
    relationship: str
    child_table: str  # domain.table as pgwire publishes it
    parent_key: str  # column of the profiled table
    child_key: str  # column of the child table


@dataclass(frozen=True)
class KeySpec:
    """A declared primary or unique key: its name and its columns as pgwire publishes them."""

    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class CheckSpec:
    """An accepted constraint as the profile statement checks it (REQ-1934 PROPOSED CONSTRAINTS):
    ``column`` (and ``other``, for an ordering) as published; ``values`` for a value set; ``low``
    and ``high`` for a range, in epoch seconds for a temporal column."""

    kind: str  # not_null | unique | value_set | range | ordering
    column: str
    other: str | None = None
    values: tuple[str, ...] = ()
    low: float | None = None
    high: float | None = None


@dataclass
class ColumnAggregates:
    spec: ColumnSpec
    non_null: int = 0
    distinct: int = 0
    min_text: str | None = None
    max_text: str | None = None
    vmin: float | None = None
    vmax: float | None = None
    m1: float | None = None
    m2: float | None = None
    m3: float | None = None
    m4: float | None = None
    stddev: float | None = None
    log_m1: float | None = None
    log_m2: float | None = None
    positive: int = 0
    integers: int = 0
    zeros: int = 0
    quantiles: list[float] | None = None
    length_min: int | None = None
    length_max: int | None = None
    length_quantiles: list[float] | None = None
    values: list[tuple[str | None, int]] = field(default_factory=list)
    shapes: list[tuple[str | None, int]] = field(default_factory=list)
    # Non-null values held by more than one row, and the rows beyond the first holding them.
    repeated_values: int = 0
    repeated_rows: int = 0


@dataclass
class FanoutAggregates:
    spec: FanoutSpec
    parents: int = 0
    mean: float | None = None
    max: int | None = None
    childless: int = 0
    quantiles: list[float] | None = None


@dataclass(frozen=True)
class DuplicateAggregates:
    """Rows, or key values, held more than once. ``key`` is None for whole rows (every profiled
    column); ``repeated``: the distinct tuples held by more than one row; ``extra``: the rows beyond
    the first holding each; ``top_counts``: the most repeated tuples' row counts, highest first."""

    key: KeySpec | None
    repeated: int
    extra: int
    top_counts: list[int]


@dataclass
class ProfileAggregates:
    profiled_rows: int
    columns: list[ColumnAggregates]
    fanouts: list[FanoutAggregates]
    rows: DuplicateAggregates
    keys: list[DuplicateAggregates]
    # Per accepted constraint checked, the rows breaking it (REQ-1934 PROPOSED CONSTRAINTS).
    violations: list[int] = field(default_factory=list)


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qualified(table: str) -> str:
    """``domain.table`` as a quoted two-part name."""
    schema, _, name = table.partition(".")
    if not name:
        raise ValueError(f"expected a domain.table name, got {table!r}")
    return f"{_ident(schema)}.{_ident(name)}"


def count_sql(table: str, watermark: str | None) -> str:
    """The table's row count, which sizes the sample, and -- where the table declares a temporal
    watermark column (``watermark``, as published) -- its latest watermark in epoch seconds, over
    the whole table, for the run's freshness (REQ-1934 FRESHNESS)."""
    latest = (
        f", MAX(EXTRACT(EPOCH FROM t.{_ident(watermark)})) AS watermark_max"
        if watermark is not None
        else ""
    )
    return f"SELECT COUNT(*) AS row_count{latest} FROM {qualified(table)} t"


# REQ-1934: how a run reads its table. ``whole`` reads every row (the table fits the cell budget);
# the others read a sample, chosen per table at run time (``provisa.profiler.sampling``).
SAMPLE_METHODS: tuple[str, ...] = ("whole", "block", "key_range", "random")

_INTEGER_TYPES = frozenset(
    {"smallint", "integer", "int", "bigint", "int2", "int4", "int8", "serial", "bigserial"}
)


def is_integer_key(spec: ColumnSpec) -> bool:
    """Whether ``spec`` can carry a key-range sample: an integer column, whose ranges can be sized
    in key units. A text or temporal key is left to the other methods."""
    return (spec.data_type or "").lower().split("(")[0].strip() in _INTEGER_TYPES


@dataclass(frozen=True)
class Sample:
    """What the profile statement reads. ``fraction``: the share of rows asked for (None for
    ``whole``); ``key`` and ``ranges``: the key column and its inclusive ranges, for ``key_range``."""

    method: str
    fraction: float | None = None
    key: str | None = None
    ranges: tuple[tuple[int, int], ...] = ()

    def __post_init__(self) -> None:
        if self.method not in SAMPLE_METHODS:
            raise ValueError(f"unknown sample method {self.method!r}; expected {SAMPLE_METHODS}")
        if (self.method == "whole") != (self.fraction is None):
            raise ValueError(f"sample method {self.method!r} with fraction {self.fraction!r}")
        if (self.method == "key_range") != bool(self.ranges) or (self.method == "key_range") != (
            self.key is not None
        ):
            raise ValueError("a key-range sample, and only one, names a key and its ranges")

    @property
    def percent(self) -> float:
        """The block sample's size as TABLESAMPLE takes it: a percentage, at most 100."""
        if self.fraction is None:
            raise ValueError("a whole-table read has no sample percentage")
        return min(100.0, self.fraction * 100.0)


def key_bounds_sql(table: str, key: str) -> str:
    """The one cheap statement a key-range sample is sized from: the key's extremes."""
    k = _ident(key)
    return f"SELECT MIN(t.{k}) AS lo, MAX(t.{k}) AS hi FROM {qualified(table)} t"


# Most ranges a key-range sample reads. Each is one index range scan at the source.
KEY_RANGES = 16


def key_ranges(
    lo: int, hi: int, fraction: float, rng: Any, count: int = KEY_RANGES
) -> tuple[tuple[int, int], ...]:
    """Up to ``count`` inclusive key ranges covering ``fraction`` of the key span ``lo..hi``, one
    at a random place in each of ``count`` equal strata of the span, so the sample is spread over
    the whole key space rather than bunched.

    Bias (REQ-1934): the realised share is the share of KEYS in the ranges, which equals the share
    of rows only where keys are dense. A key assigned in insertion order (a sequence, an identity)
    makes every range a contiguous window of insertion time: rows written together are sampled
    together, so the sample is clustered in time, and a key space with gaps (deleted rows, a
    sequence's cache jumps) makes the realised share differ from ``fraction``. The run records
    the realised share."""
    if hi < lo:
        raise ValueError(f"key bounds {lo}..{hi} are empty")
    span = hi - lo + 1
    width_total = max(1, round(span * fraction))
    n = max(1, min(count, width_total))
    width = max(1, width_total // n)
    stratum = span / n
    ranges = []
    for i in range(n):
        start = lo + int(i * stratum)
        end = lo + int((i + 1) * stratum) - 1
        room = max(0, end - start + 1 - width)
        a = start + rng.randint(0, room)
        ranges.append((a, min(end, a + width - 1)))
    return tuple(ranges)


def _quantile_array() -> str:
    return "ARRAY[" + ", ".join(f"{q:.2f}" for q in QUANTILE_POINTS) + "]"


def _numeric_expr(col: str, family: str) -> str:
    if family == "temporal":
        return f"EXTRACT(EPOCH FROM x.{col})"
    return f"CAST(x.{col} AS DOUBLE PRECISION)"


def _shape_expr(expr: str) -> str:
    inner = f"CAST({expr} AS TEXT)"
    for pattern, cls in (("[A-Z]", "A"), ("[a-z]", "a"), ("[0-9]", "9")):
        inner = f"REGEXP_REPLACE({inner}, '{pattern}', '{cls}', 'g')"
    return inner


def _scalar(expr: str) -> str:
    return f"CASE WHEN x.k = 0 THEN {expr} END"


@dataclass(frozen=True)
class _Group:
    k: int
    column: int  # index into the column list; the key's index for ``key``, -1 for ``row``
    what: str  # value | shape | row | key


def _groups(columns: list[ColumnSpec], keys: list[KeySpec]) -> list[_Group]:
    groups = [_Group(i + 1, i, "value") for i in range(len(columns))]
    text = [i for i, c in enumerate(columns) if c.family == "text"]
    groups += [_Group(len(columns) + 1 + j, i, "shape") for j, i in enumerate(text)]
    groups.append(_Group(len(groups) + 1, -1, "row"))
    base = len(groups) + 1
    groups += [_Group(base + j, j, "key") for j in range(len(keys))]
    return groups


def _identity_expr(refs: list[str], null_if_any_null: bool) -> str:
    """One text per tuple of ``refs``, equal exactly when the tuples are equal (REQ-1934 duplicate
    rows): each part is its length, a colon and its text, so no two different tuples concatenate to
    the same text; a null part is ``N``. With ``null_if_any_null`` a tuple with a null part is NULL
    -- a key is held only by rows whose key columns are all set, as SQL uniqueness counts."""
    parts = [
        f"CASE WHEN {r} IS NULL THEN 'N' ELSE "
        f"CAST(LENGTH(CAST({r} AS TEXT)) AS TEXT) || ':' || CAST({r} AS TEXT) END"
        for r in refs
    ]
    joined = " || '|' || ".join(parts)
    if not null_if_any_null:
        return joined
    any_null = " OR ".join(f"{r} IS NULL" for r in refs)
    return f"CASE WHEN {any_null} THEN NULL ELSE {joined} END"


def _base_sql(table: str, base_cols: list[str], sample: Sample) -> str:
    """The profiled rows: the whole table, or the sample ``sample`` names (REQ-1934)."""
    select = f"SELECT {', '.join(base_cols)} FROM {qualified(table)} t"
    if sample.method == "whole":
        return select
    if sample.method == "block":
        # Written in the governed dialect (postgres) and transpiled per engine/source; the run
        # refuses a statement whose transpiled form lost the clause (run._require_sample_clause).
        return f"{select} TABLESAMPLE SYSTEM ({sample.percent!r})"
    if sample.method == "key_range":
        # One single-range branch per range, joined by UNION ALL: measured, a lone BETWEEN on the
        # key reaches the source's index through every reach that claims key_range, where an OR
        # of ranges is not pushed by all of them (DuckDB's postgres scanner reads the whole table).
        k = _ident(sample.key or "")
        return " UNION ALL ".join(
            f"{select} WHERE t.{k} BETWEEN {a} AND {b}" for a, b in sample.ranges
        )
    # REQ-1934 (maintainer ruling): where the table's reach offers neither block sampling at the
    # source nor an indexed key, the row filter stays. It cuts the profile's work, not the read.
    return f"{select} WHERE RANDOM() < {sample.fraction!r}"


def profile_sql(
    table: str,
    columns: list[ColumnSpec],
    fanouts: list[FanoutSpec],
    sample: Sample,
    low_cardinality_max: int,
    keys: list[KeySpec],
    checks: list[CheckSpec],
) -> str:
    """The one statement profiling ``table``, reading what ``sample`` names;
    ``low_cardinality_max`` is the profiler's run default: a column with no more distinct values
    has every value returned, for its full value-frequency table. ``keys``: the table's declared
    primary and unique keys, whose values held by more than one row are counted. ``checks``: the
    table's accepted constraints, whose breaking rows are counted."""
    if not columns:
        raise ValueError(f"table {table!r} has no column the org admin can read to profile")
    groups = _groups(columns, keys)
    col_refs = [_ident(c.name) for c in columns]

    base_cols = [f"t.{ref} AS {_ident(f'c{i}')}" for i, ref in enumerate(col_refs)]
    base = _base_sql(table, base_cols, sample)
    joins = ""
    fan_cols: list[str] = []
    names = [c.name for c in columns]
    for r, fan in enumerate(fanouts):
        if fan.parent_key not in names:
            raise ValueError(
                f"relationship {fan.relationship!r}: parent key {fan.parent_key!r} is not a "
                f"column the org admin can read on {table!r}"
            )
        parent = _ident(f"c{names.index(fan.parent_key)}")
        child = (
            f"(SELECT {_ident(fan.child_key)} AS fk, COUNT(*) AS n "
            f"FROM {qualified(fan.child_table)} GROUP BY {_ident(fan.child_key)})"
        )
        joins += f" LEFT JOIN {child} f{r} ON f{r}.fk = b.{parent}"
        fan_cols.append(f"COALESCE(f{r}.n, 0) AS {_ident(f'n{r}')}")

    for key in keys:
        missing = [c for c in key.columns if c not in names]
        if missing:
            raise ValueError(
                f"key {key.name!r}: {missing} are not columns the org admin can read on {table!r}"
            )

    def _b(name: str) -> str:
        return f"b.{_ident(f'c{names.index(name)}')}"

    def _val(g: _Group) -> str:
        if g.what == "value":
            return f"CAST(b.{_ident(f'c{g.column}')} AS TEXT)"
        if g.what == "shape":
            return _shape_expr(f"b.{_ident(f'c{g.column}')}")
        if g.what == "row":
            return _identity_expr([_b(n) for n in names], null_if_any_null=False)
        return _identity_expr([_b(n) for n in keys[g.column].columns], null_if_any_null=True)

    val_cases = " ".join(f"WHEN {g.k} THEN {_val(g)}" for g in groups)
    selectors = ", ".join(f"({k})" for k in range(len(groups) + 1))
    expanded = (
        f"SELECT k.k AS k, CASE k.k {val_cases} END AS val, b.*"
        + ("".join(f", {fc}" for fc in fan_cols))
        + f" FROM ({base}) b{joins} CROSS JOIN (VALUES {selectors}) AS k(k)"
    )

    q = _quantile_array()
    aggs: list[str] = []
    for i, col in enumerate(columns):
        c = _ident(f"c{i}")
        aggs.append(f"COUNT({_scalar(f'x.{c}')}) AS a{i}_nn")
        if col.family in ("numeric", "temporal"):
            v = _numeric_expr(c, col.family)
            aggs += [
                f"MIN({_scalar(v)}) AS a{i}_min",
                f"MAX({_scalar(v)}) AS a{i}_max",
                f"AVG({_scalar(v)}) AS a{i}_m1",
                f"AVG({_scalar(f'{v} * {v}')}) AS a{i}_m2",
                f"AVG({_scalar(f'{v} * {v} * {v}')}) AS a{i}_m3",
                f"AVG({_scalar(f'{v} * {v} * {v} * {v}')}) AS a{i}_m4",
                f"STDDEV_POP({_scalar(v)}) AS a{i}_sd",
                f"AVG({_scalar(f'CASE WHEN {v} > 0 THEN LN({v}) END')}) AS a{i}_l1",
                f"AVG({_scalar(f'CASE WHEN {v} > 0 THEN LN({v}) * LN({v}) END')}) AS a{i}_l2",
                f"COUNT({_scalar(f'CASE WHEN {v} > 0 THEN 1 END')}) AS a{i}_pos",
                f"COUNT({_scalar(f'CASE WHEN {v} = FLOOR({v}) THEN 1 END')}) AS a{i}_int",
                f"COUNT({_scalar(f'CASE WHEN {v} = 0 THEN 1 END')}) AS a{i}_zero",
                f"PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {_scalar(v)}) AS a{i}_q",
            ]
            if col.family == "temporal":
                aggs += [
                    f"CAST(MIN({_scalar(f'x.{c}')}) AS TEXT) AS a{i}_mint",
                    f"CAST(MAX({_scalar(f'x.{c}')}) AS TEXT) AS a{i}_maxt",
                ]
        elif col.family == "text":
            length = f"LENGTH(CAST(x.{c} AS TEXT))"
            aggs += [
                f"MIN({_scalar(f'x.{c}')}) AS a{i}_mint",
                f"MAX({_scalar(f'x.{c}')}) AS a{i}_maxt",
                f"MIN({_scalar(length)}) AS a{i}_lmin",
                f"MAX({_scalar(length)}) AS a{i}_lmax",
                f"PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {_scalar(length)}) AS a{i}_lq",
            ]
    for r in range(len(fanouts)):
        n = f"x.{_ident(f'n{r}')}"
        aggs += [
            f"COUNT({_scalar(n)}) AS f{r}_parents",
            f"AVG({_scalar(f'CAST({n} AS DOUBLE PRECISION)')}) AS f{r}_mean",
            f"MAX({_scalar(n)}) AS f{r}_max",
            f"COUNT({_scalar(f'CASE WHEN {n} = 0 THEN 1 END')}) AS f{r}_zero",
            f"PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {_scalar(n)}) AS f{r}_q",
        ]

    for j, check in enumerate(checks):
        broken = _violation(check, columns)
        if broken is not None:
            aggs.append(f"COUNT({_scalar(f'CASE WHEN {broken} THEN 1 END')}) AS v{j}")

    keep = max(TOP_N, low_cardinality_max + 1)
    grouped = (
        "SELECT x.k AS k, x.val AS val, COUNT(*) AS cnt, "
        "ROW_NUMBER() OVER (PARTITION BY x.k ORDER BY COUNT(*) DESC, x.val) AS rn, "
        "COUNT(*) OVER (PARTITION BY x.k) AS ngroups, "
        # Per group selector: the non-null values held by more than one row, and the rows beyond
        # the first holding them (duplicate rows and keys, REQ-1934). The k = 0 group's val is
        # NULL, so both are 0 there.
        "SUM(CASE WHEN COUNT(*) > 1 AND x.val IS NOT NULL THEN 1 ELSE 0 END) "
        "OVER (PARTITION BY x.k) AS nrepeated, "
        "SUM(CASE WHEN COUNT(*) > 1 AND x.val IS NOT NULL THEN COUNT(*) - 1 ELSE 0 END) "
        "OVER (PARTITION BY x.k) AS nextra, "
        + ", ".join(aggs)
        + f" FROM ({expanded}) x GROUP BY x.k, x.val"
    )
    return f"SELECT * FROM ({grouped}) z WHERE z.k = 0 OR z.rn <= {keep}"


def _violation(check: CheckSpec, columns: list[ColumnSpec]) -> str | None:
    """The predicate a row breaking ``check`` meets, over the expanded rows ``x``; None for a
    uniqueness check, which the column's value group counts."""
    names = [c.name for c in columns]
    for name in (check.column, check.other):
        if name is not None and name not in names:
            raise ValueError(
                f"constraint {check.kind} on {check.column!r}: {name!r} is not a column the org "
                f"admin can read"
            )
    i = names.index(check.column)
    a = f"x.{_ident(f'c{i}')}"
    if check.kind == "not_null":
        return f"{a} IS NULL"
    if check.kind == "unique":
        return None
    if check.kind == "value_set":
        if not check.values:
            return f"{a} IS NOT NULL"
        listed = ", ".join(sql_literal(v, "postgres") for v in check.values)
        return f"{a} IS NOT NULL AND CAST({a} AS TEXT) NOT IN ({listed})"
    if check.kind == "range":
        if check.low is None or check.high is None:
            raise ValueError(f"range constraint on {check.column!r} has no bounds")
        v = _numeric_expr(_ident(f"c{i}"), columns[i].family)
        return f"({v} < {check.low!r} OR {v} > {check.high!r})"
    if check.kind == "ordering":
        assert check.other is not None, "an ordering names its other column"
        b = f"x.{_ident(f'c{names.index(check.other)}')}"
        return f"{a} > {b}"
    raise ValueError(f"unknown constraint kind {check.kind!r}")


def _f(value: Any) -> float | None:
    return None if value is None else float(value)


def _i(value: Any) -> int:
    if value is None:
        raise ValueError("profile statement returned no count where one is always produced")
    return int(value)


def _quantiles(value: Any) -> list[float] | None:
    if value is None:
        return None
    points = list(value)
    if len(points) != len(QUANTILE_POINTS):
        raise ValueError(
            f"quantile sketch has {len(points)} points, expected {len(QUANTILE_POINTS)}"
        )
    if any(p is None for p in points):
        return None
    return [float(p) for p in points]


def parse_profile_result(
    column_names: list[str],
    rows: list[tuple],
    columns: list[ColumnSpec],
    fanouts: list[FanoutSpec],
    keys: list[KeySpec],
    checks: list[CheckSpec],
) -> ProfileAggregates:
    """The statement's result as aggregates per column and per relationship."""
    records = [dict(zip(column_names, r)) for r in rows]
    if not records:
        # The statement groups by (k, val): over no input rows there is no group at all, so not
        # even the k = 0 row comes back. That is a read of zero rows -- an empty table, or a block
        # sample that drew no block -- and the run decides what it means (REQ-1934).
        return ProfileAggregates(
            profiled_rows=0,
            columns=[ColumnAggregates(spec=c) for c in columns],
            fanouts=[FanoutAggregates(spec=f) for f in fanouts],
            rows=DuplicateAggregates(key=None, repeated=0, extra=0, top_counts=[]),
            keys=[DuplicateAggregates(key=k, repeated=0, extra=0, top_counts=[]) for k in keys],
            # No row read breaks a constraint.
            violations=[0 for _ in checks],
        )
    scalar = [r for r in records if r["k"] == 0]
    if len(scalar) != 1:
        raise ValueError(f"profile statement returned {len(scalar)} table-wide rows, expected 1")
    s = scalar[0]
    groups = _groups(columns, keys)
    by_k: dict[int, list[dict]] = {}
    for r in records:
        if r["k"] != 0:
            by_k.setdefault(int(r["k"]), []).append(r)

    out: list[ColumnAggregates] = []
    for i, spec in enumerate(columns):
        agg = ColumnAggregates(spec=spec, non_null=_i(s[f"a{i}_nn"]))
        if spec.family in ("numeric", "temporal"):
            agg.vmin, agg.vmax = _f(s[f"a{i}_min"]), _f(s[f"a{i}_max"])
            agg.m1, agg.m2 = _f(s[f"a{i}_m1"]), _f(s[f"a{i}_m2"])
            agg.m3, agg.m4 = _f(s[f"a{i}_m3"]), _f(s[f"a{i}_m4"])
            agg.stddev = _f(s[f"a{i}_sd"])
            agg.log_m1, agg.log_m2 = _f(s[f"a{i}_l1"]), _f(s[f"a{i}_l2"])
            agg.positive, agg.integers = _i(s[f"a{i}_pos"]), _i(s[f"a{i}_int"])
            agg.zeros = _i(s[f"a{i}_zero"])
            agg.quantiles = _quantiles(s[f"a{i}_q"])
            if spec.family == "temporal":
                agg.min_text, agg.max_text = s[f"a{i}_mint"], s[f"a{i}_maxt"]
            else:
                agg.min_text = None if agg.vmin is None else repr(agg.vmin)
                agg.max_text = None if agg.vmax is None else repr(agg.vmax)
        elif spec.family == "text":
            agg.min_text, agg.max_text = s[f"a{i}_mint"], s[f"a{i}_maxt"]
            agg.length_min = None if s[f"a{i}_lmin"] is None else int(s[f"a{i}_lmin"])
            agg.length_max = None if s[f"a{i}_lmax"] is None else int(s[f"a{i}_lmax"])
            agg.length_quantiles = _quantiles(s[f"a{i}_lq"])
        out.append(agg)
    row_dups: DuplicateAggregates | None = None
    key_dups: list[DuplicateAggregates] = []
    for g in groups:
        ranked = sorted(by_k.get(g.k, []), key=lambda r: int(r["rn"]))
        pairs = [(r["val"], int(r["cnt"])) for r in ranked]
        # An empty group (no rows read) holds nothing more than once.
        repeated = int(ranked[0]["nrepeated"]) if ranked else 0
        extra = int(ranked[0]["nextra"]) if ranked else 0
        if g.what in ("row", "key"):
            dups = DuplicateAggregates(
                key=None if g.what == "row" else keys[g.column],
                repeated=repeated,
                extra=extra,
                top_counts=[n for v, n in pairs if v is not None and n > 1][:TOP_N],
            )
            if g.what == "row":
                row_dups = dups
            else:
                key_dups.append(dups)
            continue
        agg = out[g.column]
        if g.what == "value":
            agg.values = pairs
            group_count = int(ranked[0]["ngroups"]) if ranked else 0
            has_null = agg.non_null < _i(s["cnt"])
            agg.distinct = group_count - (1 if has_null else 0)
            agg.repeated_values, agg.repeated_rows = repeated, extra
        else:
            agg.shapes = [p for p in pairs if p[0] is not None]
    assert row_dups is not None, "the row group is always among the groups"

    fans: list[FanoutAggregates] = []
    for r, spec in enumerate(fanouts):
        fans.append(
            FanoutAggregates(
                spec=spec,
                parents=_i(s[f"f{r}_parents"]),
                mean=_f(s[f"f{r}_mean"]),
                max=None if s[f"f{r}_max"] is None else int(s[f"f{r}_max"]),
                childless=_i(s[f"f{r}_zero"]),
                quantiles=_quantiles(s[f"f{r}_q"]),
            )
        )
    names = [c.name for c in columns]
    violations = [
        out[names.index(c.column)].repeated_rows if c.kind == "unique" else _i(s[f"v{j}"])
        for j, c in enumerate(checks)
    ]
    return ProfileAggregates(
        profiled_rows=_i(s["cnt"]),
        columns=out,
        fanouts=fans,
        rows=row_dups,
        keys=key_dups,
        violations=violations,
    )
