# Copyright (c) 2026 Kenneth Stott
# Canary: b83e15d7-2c94-4e06-a7f1-58c0d39e6b24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's ranges and multiranges on every engine (REQ-2171, RANGES ARE EMULATED ON EVERY
ENGINE; REQ-2167).

PostgreSQL runs them natively. Everywhere else a range is its PostgreSQL text form, read here into
its parts as expressions of the target dialect: is it empty, each bound's value in the element
type (so ``[1,10)`` and ``[9,20)`` compare as numbers), whether it is unbounded and whether it is
inclusive. Every operator, function and sort then follows rangetypes.c over those parts, and any
range-valued result is written back in PostgreSQL's text form. The expressions are portable
(CASE, comparison, a cast, substring, concatenation): sqlglot writes them for the target, and
``_FORMS`` names what a dialect spells its own way. No dialect is refused a range.

A multirange's text is ``{[1,3),[5,7)}``. A predicate over one quantifies over its ranges with the
dialect's own list form (``any``). A multirange built from columns is a fold over its ranges with a
text as its state, in PostgreSQL's multirange form, written as SQL with the dialect's own sort and
reduce (``reduce``, ``sorted_ranges``): a dialect with neither refuses it by name. Values the
statement writes as literals are answered here, on every dialect, by provisa.core.pg_range."""

# Requirements: REQ-2171, REQ-2167

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace

import sqlglot.expressions as exp

from provisa.core import pg_range
from provisa.core.pg_error import MARK, PostgresError
from provisa.core.pg_range import MULTIRANGE_TYPES, RANGE_TYPES, Multirange, Range, RangeType
from provisa.transpiler.native_subtype import (
    COMPARISONS,
    SubtypeOperationRefused,
    marked,
)

_T = exp.DataType.Type
_CAST_RANGES = {
    _T.INT4RANGE: "int4range",
    _T.INT8RANGE: "int8range",
    _T.NUMRANGE: "numrange",
    _T.TSRANGE: "tsrange",
    _T.TSTZRANGE: "tstzrange",
    _T.DATERANGE: "daterange",
}
_CAST_MULTIRANGES = {
    _T.INT4MULTIRANGE: "int4multirange",
    _T.INT8MULTIRANGE: "int8multirange",
    _T.NUMMULTIRANGE: "nummultirange",
    _T.TSMULTIRANGE: "tsmultirange",
    _T.TSTZMULTIRANGE: "tstzmultirange",
    _T.DATEMULTIRANGE: "datemultirange",
}

#: A range's bound type on a dialect whose spelling of it sqlglot's own would change: a numeric
#: wide enough for the column, a timestamp to the microsecond. Written as given.
_BOUND_TYPES: dict[str, dict[str, str]] = {
    "trino": {"timestamp": "TIMESTAMP(6)", "timestamptz": "TIMESTAMP(6) WITH TIME ZONE"},
    "mysql": {"timestamp": "DATETIME(6)", "integer": "SIGNED", "bigint": "SIGNED"},
    "tsql": {"timestamp": "DATETIME2(6)", "timestamptz": "DATETIMEOFFSET(6)"},
    "snowflake": {"timestamp": "TIMESTAMP_NTZ(6)", "timestamptz": "TIMESTAMP_TZ(6)"},
    "bigquery": {"numeric": "BIGNUMERIC", "timestamp": "DATETIME", "timestamptz": "TIMESTAMP"},
    "clickhouse": {
        "numeric": "Decimal(38, 10)",
        "timestamp": "DateTime64(6)",
        "timestamptz": "DateTime64(6, 'UTC')",
        "date": "Date32",
    },
    "databricks": {"timestamp": "TIMESTAMP_NTZ", "timestamptz": "TIMESTAMP"},
    "spark": {"timestamp": "TIMESTAMP_NTZ", "timestamptz": "TIMESTAMP"},
}
_BOUND_DEFAULTS = {
    "integer": "INT",
    "bigint": "BIGINT",
    "numeric": "DECIMAL(38, 10)",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
    "date": "DATE",
}
#: How a dialect reads a zoned timestamp's text (its offset written +hh:mm) where a cast does not.
_INSTANT_FROM_TEXT = {
    "clickhouse": "parseDateTime64BestEffortOrNull({a}, 6, 'UTC')",
    "snowflake": "TO_TIMESTAMP_TZ({a})",
    "mysql": "CONVERT_TZ(LEFT({a}, CHAR_LENGTH({a}) - 6), RIGHT({a}, 6), '+00:00')",
}
#: A timestamp's text to six fractional digits, on a dialect where a cast to text is not
#: PostgreSQL's form. ``{a}`` is the timestamp; a zoned one is first made its UTC wall time.
_CLOCK_TEXT = {
    "trino": "CAST(CAST({a} AS TIMESTAMP(6)) AS VARCHAR)",
    "mysql": "DATE_FORMAT({a}, '%Y-%m-%d %H:%i:%s.%f')",
    "tsql": "CONVERT(VARCHAR(26), CAST({a} AS DATETIME2(6)), 121)",
    "snowflake": "TO_CHAR({a}, 'YYYY-MM-DD HH24:MI:SS.FF6')",
    "bigquery": "FORMAT_DATETIME('%F %H:%M:%E6S', {a})",
    "clickhouse": "toString(CAST({a} AS DateTime64(6)))",
    "databricks": "date_format({a}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
    "spark": "date_format({a}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
}
_UTC_WALL = {
    "trino": "CAST({a} AT TIME ZONE 'UTC' AS TIMESTAMP(6))",
    "tsql": "CAST(SWITCHOFFSET({a}, 0) AS DATETIME2(6))",
    "snowflake": "CONVERT_TIMEZONE('UTC', {a})",
    "bigquery": "DATETIME({a}, 'UTC')",
    "clickhouse": "toTimeZone({a}, 'UTC')",
    "mysql": "CONVERT_TZ({a}, @@session.time_zone, '+00:00')",
}
#: The dialects whose reading of an offset needs its minutes.
_OFFSET_WITH_MINUTES = frozenset({"trino", "athena", "tsql", "snowflake", "mysql"})
#: Those with a regular expression for it. A bound is read many times over in one statement, and
#: this names the bound once: Trino's compiler refused the statement that named it five times.
_OFFSET_MINUTES_ADDED = {
    "trino": "regexp_replace({a}, '([+-][0-9][0-9])$', '$1:00')",
    "athena": "regexp_replace({a}, '([+-][0-9][0-9])$', '$1:00')",
    "snowflake": "REGEXP_REPLACE({a}, '([+-][0-9][0-9])$', '\\\\1:00')",
}
#: The seconds from ``{b}`` to ``{a}``, two timestamps, as a double.
_SECONDS_BETWEEN = {
    "duckdb": "(EPOCH({a}) - EPOCH({b}))",
    "trino": "(to_unixtime({a}) - to_unixtime({b}))",
    "mysql": "(TIMESTAMPDIFF(MICROSECOND, {b}, {a}) / 1000000E0)",
    "tsql": "(CAST(DATEDIFF_BIG(MICROSECOND, {b}, {a}) AS FLOAT) / 1000000E0)",
    "snowflake": "(TIMESTAMPDIFF(MICROSECOND, {b}, {a}) / 1000000E0)",
    "bigquery": "(UNIX_MICROS(TIMESTAMP({a})) - UNIX_MICROS(TIMESTAMP({b}))) / 1000000E0",
    "clickhouse": "(dateDiff('microsecond', {b}, {a}) / 1000000E0)",
    "databricks": "(TIMESTAMPDIFF(MICROSECOND, {b}, {a}) / 1000000E0)",
    "spark": "(TIMESTAMPDIFF(MICROSECOND, {b}, {a}) / 1000000E0)",
}
#: The function a dialect raises an error with, in an expression of any type. A dialect absent
#: here fails the same way through a cast of the message to an integer (MySQL's never fails, so
#: an error PostgreSQL raises there is a listed difference).
_RAISE = {
    "duckdb": "error",
    "trino": "fail",
    "athena": "fail",
    "bigquery": "ERROR",
    "databricks": "raise_error",
    "spark": "raise_error",
}
_REGEX_LIST = {
    "duckdb": "regexp_extract_all",
    "trino": "regexp_extract_all",
    "athena": "regexp_extract_all",
    "bigquery": "REGEXP_EXTRACT_ALL",
    "clickhouse": "extractAll",
    "databricks": "regexp_extract_all",
    "spark": "regexp_extract_all",
    "snowflake": "REGEXP_SUBSTR_ALL",
}
#: A dialect's fold over a list, on the dialects that have one.
_REDUCE = {
    "duckdb": "list_reduce",
    "trino": "reduce",
    "athena": "reduce",
    "clickhouse": "arrayFold",
    "databricks": "aggregate",
    "spark": "aggregate",
    "snowflake": "REDUCE",
}
#: A dialect's row of sort keys, its sort of a list of rows, and how a row's field ``{n}`` of
#: ``{a}`` is read: the dialects whose sort compares rows. Snowflake's ARRAY_SORT does not.
_SORT = {
    "duckdb": ("row", "list_sort", "{a}[{n}]"),
    "trino": ("ROW", "array_sort", "{a}[{n}]"),
    "athena": ("ROW", "array_sort", "{a}[{n}]"),
    "clickhouse": ("tuple", "arraySort", "tupleElement({a}, {n})"),
    "databricks": ("struct", "array_sort", "{a}.col{n}"),
    "spark": ("struct", "array_sort", "{a}.col{n}"),
}
#: One range of a multirange's text.
_RANGE_PATTERN = r"[\[(][^\])]*[\])]"
#: The dialects with a boolean value in a select list.
_NO_BOOLEAN_VALUE = frozenset({"tsql", "oracle"})

_EMPTY_MULTIRANGE = "{}"
#: A multirange's text among several joined.
_MULTIRANGE_PATTERN = r"\{[^}]*\}"
#: A value of each element type, for a sort key whose row a dialect refuses to compare with a NULL
#: in it: the flag before it has already told an unbounded side from a bounded one.
_FILLER = {
    "integer": "0",
    "bigint": "0",
    "numeric": "0",
    "date": "1970-01-01",
    "timestamp": "1970-01-01 00:00:00",
    "timestamptz": "1970-01-01 00:00:00+00",
}


# --- expressions, folded where a part is known ----------------------------------------------------


def _null() -> exp.Expr:
    return exp.Null()


def _is_true(node: exp.Expr) -> bool:
    return isinstance(node, exp.Boolean) and bool(node.this)


def _is_false(node: exp.Expr) -> bool:
    return isinstance(node, exp.Boolean) and not node.this


def _bool(value: bool) -> exp.Expr:
    return exp.true() if value else exp.false()


def _and(*parts: exp.Expr) -> exp.Expr:
    kept = [p for p in parts if not _is_true(p)]
    if any(_is_false(p) for p in kept):
        return exp.false()
    if not kept:
        return exp.true()
    return kept[0] if len(kept) == 1 else exp.Paren(this=exp.and_(*[p.copy() for p in kept]))


def _or(*parts: exp.Expr) -> exp.Expr:
    kept = [p for p in parts if not _is_false(p)]
    if any(_is_true(p) for p in kept):
        return exp.true()
    if not kept:
        return exp.false()
    return kept[0] if len(kept) == 1 else exp.Paren(this=exp.or_(*[p.copy() for p in kept]))


def _not(part: exp.Expr) -> exp.Expr:
    if isinstance(part, exp.Boolean):
        return _bool(not part.this)
    if isinstance(part, exp.Not):
        return part.this.copy()
    return exp.Not(this=part.copy())


def _case(whens: Iterable[tuple[exp.Expr, exp.Expr]], default: exp.Expr | None) -> exp.Expr:
    """CASE with the conditions known at compile time gone. Never an empty CASE."""
    ifs: list[exp.If] = []
    for condition, value in whens:
        if _is_false(condition):
            continue
        if _is_true(condition):
            default = value
            break
        ifs.append(exp.If(this=condition.copy(), true=value.copy()))
    if not ifs:
        return default.copy() if default is not None else _null()
    return exp.Case(ifs=ifs, default=default.copy() if default is not None else None)


def _int(value: int) -> exp.Expr:
    return exp.Literal.number(value)


def _string(value: str) -> exp.Expr:
    return exp.Literal.string(value)


def _is_zero(node: exp.Expr) -> bool:
    return isinstance(node, exp.Literal) and not node.is_string and node.this == "0"


def _concat(*parts: exp.Expr) -> exp.Expr:
    joined: list[exp.Expr] = []
    for part in parts:
        if (
            joined
            and isinstance(part, exp.Literal)
            and isinstance(joined[-1], exp.Literal)
            and part.is_string
            and joined[-1].is_string
        ):
            joined[-1] = _string(joined[-1].this + part.this)
        else:
            joined.append(part.copy())
    out = joined[0]
    for part in joined[1:]:
        out = exp.DPipe(this=out, expression=part)
    return out


def _call(name: str, *args: exp.Expr) -> exp.Expr:
    return exp.Anonymous(this=name, expressions=[a.copy() for a in args])


def _text_cast(value: exp.Expr) -> exp.Expr:
    return exp.Cast(this=value.copy(), to=exp.DataType.build("TEXT"))


def _strpos(text: exp.Expr, what: str) -> exp.Expr:
    return exp.StrPosition(this=text.copy(), substr=_string(what))


def _length(text: exp.Expr) -> exp.Expr:
    return exp.Length(this=text.copy())


def _substring(text: exp.Expr, start: exp.Expr, length: exp.Expr) -> exp.Expr:
    return exp.Substring(this=text.copy(), start=start.copy(), length=length.copy())


def _replace(text: exp.Expr, what: str, by: str) -> exp.Expr:
    return _call("REPLACE", text, _string(what), _string(by))


def _plus(a: exp.Expr, b: exp.Expr) -> exp.Expr:
    return exp.Add(this=a.copy(), expression=b.copy())


def _minus(a: exp.Expr, b: exp.Expr) -> exp.Expr:
    right = exp.Paren(this=b.copy()) if isinstance(b, exp.Binary) else b.copy()
    return exp.Sub(this=a.copy(), expression=right)


def _eq(a: exp.Expr, b: exp.Expr) -> exp.Expr:
    return exp.EQ(this=a.copy(), expression=b.copy())


def _is_null(a: exp.Expr) -> exp.Expr:
    if isinstance(a, exp.Null):
        return exp.true()
    if isinstance(a, exp.Literal):
        return exp.false()
    return exp.Is(this=a.copy(), expression=exp.Null())


def _unwrapped(node: exp.Expr) -> exp.Expr:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _null_value(node: exp.Expr) -> bool:
    inner = _unwrapped(node)
    return isinstance(inner, exp.Null) or (isinstance(inner, exp.Cast) and _null_value(inner.this))


def _string_literal(node: exp.Expr) -> str | None:
    inner = _unwrapped(node)
    return inner.this if isinstance(inner, exp.Literal) and inner.is_string else None


# --- a range, as expressions of its parts ---------------------------------------------------------


@dataclass
class _Side:
    """One bound: unbounded, its value in the element type, inclusive, and its text."""

    inf: exp.Expr
    value: exp.Expr
    inc: exp.Expr
    text: exp.Expr

    def flipped(self) -> _Side:
        return _Side(self.inf, self.value, _not(self.inc), self.text)


@dataclass
class _R:
    """A range: ``null`` is whether the SQL value is NULL (and where a value that PostgreSQL would
    refuse raises, tested first), ``literal`` the value where it is known here."""

    rtype: RangeType
    null: exp.Expr
    empty: exp.Expr
    lower: _Side
    upper: _Side
    literal: Range | None = None


@dataclass
class _M:
    """A multirange: its text, canonical, and ``literal`` where its value is known here."""

    rtype: RangeType
    null: exp.Expr
    text: exp.Expr
    literal: Multirange | None = None


@dataclass
class _Nothing:
    """An operand with nothing but whether it is NULL: an element beside its range."""

    null: exp.Expr


_Value = _R | _M


def _choose(condition: exp.Expr, a: _Side, b: _Side) -> _Side:
    """``a`` where ``condition`` holds, else ``b``. A predicate is no value on every dialect, so
    an inclusivity is chosen with AND and OR."""
    if _is_true(condition):
        return a
    if _is_false(condition):
        return b
    otherwise = _not(condition)
    return _Side(
        _or(_and(condition, a.inf), _and(otherwise, b.inf)),
        _case([(condition, a.value)], b.value),
        _or(_and(condition, a.inc), _and(otherwise, b.inc)),
        _case([(condition, a.text)], b.text),
    )


def _bound_cmp(a: _Side, a_lower: bool, b: _Side, b_lower: bool) -> exp.Expr:
    """range_cmp_bounds, as -1, 0 or 1. An unbounded lower side comes before every value, an
    unbounded upper side after; at equal values a bound's inclusivity decides."""
    if a_lower == b_lower:
        first, second = (-1, 1) if a_lower else (1, -1)
        ties = [(_and(a.inc, _not(b.inc)), _int(first)), (_and(_not(a.inc), b.inc), _int(second))]
        tied = _int(0)
        same_side = _and(a.inf, b.inf)
    else:
        ties = [(_and(a.inc, b.inc), _int(0))]
        tied = _int(1 if a_lower else -1)
        same_side = exp.false()
    return _case(
        [
            (same_side, _int(0)),
            (a.inf, _int(-1 if a_lower else 1)),
            (b.inf, _int(1 if b_lower else -1)),
            (exp.LT(this=a.value.copy(), expression=b.value.copy()), _int(-1)),
            (exp.GT(this=a.value.copy(), expression=b.value.copy()), _int(1)),
            *ties,
        ],
        tied,
    )


def _ll(a: _R, b: _R) -> exp.Expr:
    return _bound_cmp(a.lower, True, b.lower, True)


def _uu(a: _R, b: _R) -> exp.Expr:
    return _bound_cmp(a.upper, False, b.upper, False)


def _lu(a: _R, b: _R) -> exp.Expr:
    return _bound_cmp(a.lower, True, b.upper, False)


def _lte0(a: exp.Expr) -> exp.Expr:
    return exp.LTE(this=a, expression=_int(0))


def _gte0(a: exp.Expr) -> exp.Expr:
    return exp.GTE(this=a, expression=_int(0))


def _lt0(a: exp.Expr) -> exp.Expr:
    return exp.LT(this=a, expression=_int(0))


def _gt0(a: exp.Expr) -> exp.Expr:
    return exp.GT(this=a, expression=_int(0))


def _both(a: _R, b: _R) -> exp.Expr:
    return _and(_not(a.empty), _not(b.empty))


def _overlaps(a: _R, b: _R) -> exp.Expr:
    return _and(_both(a, b), _lte0(_lu(a, b)), _lte0(_lu(b, a)))


def _contains(a: _R, b: _R) -> exp.Expr:
    return _or(b.empty, _and(_not(a.empty), _lte0(_ll(a, b)), _gte0(_uu(a, b))))


def _contains_element(a: _R, element: exp.Expr) -> exp.Expr:
    point = _Side(exp.false(), element, exp.true(), element)
    return _and(
        _not(a.empty),
        _lte0(_bound_cmp(a.lower, True, point, True)),
        _gte0(_bound_cmp(a.upper, False, point, False)),
    )


def _before(a: _R, b: _R) -> exp.Expr:
    return _and(_both(a, b), _gt0(_lu(b, a)))


def _overleft(a: _R, b: _R) -> exp.Expr:
    return _and(_both(a, b), _lte0(_uu(a, b)))


def _overright(a: _R, b: _R) -> exp.Expr:
    return _and(_both(a, b), _gte0(_ll(a, b)))


def _touch(upper: _Side, lower: _Side) -> exp.Expr:
    """An upper bound meeting a lower one: the same value, exactly one of them inclusive."""
    differ = _or(_and(upper.inc, _not(lower.inc)), _and(_not(upper.inc), lower.inc))
    return _and(_not(upper.inf), _not(lower.inf), _eq(upper.value, lower.value), differ)


def _adjacent(a: _R, b: _R) -> exp.Expr:
    return _and(_both(a, b), _or(_touch(a.upper, b.lower), _touch(b.upper, a.lower)))


def _sign(a: _R, b: _R) -> exp.Expr:
    """range_cmp, as -1, 0 or 1: the empty range first, then by lower bound, then upper."""
    lower = _ll(a, b)
    return _case(
        [
            (_and(a.empty, b.empty), _int(0)),
            (a.empty, _int(-1)),
            (b.empty, _int(1)),
            (_lt0(lower), _int(-1)),
            (_gt0(lower.copy()), _int(1)),
        ],
        _uu(a, b),
    )


class _Writer:
    """Everything that depends on the target dialect."""

    def __init__(self, dialect: str) -> None:
        self.dialect = dialect
        self.serial = 0

    def fresh(self, stem: str) -> str:
        """A name for a lambda's variable, unique in the statement: lambdas here nest."""
        self.serial += 1
        return f"_{stem}{self.serial}"

    # -- values -------------------------------------------------------------------------------

    def sql(self, node: exp.Expr) -> str:
        return node.sql(dialect=self.dialect, identify=self.dialect in ("snowflake", "oracle"))

    def raw(self, template: str, **args: exp.Expr) -> exp.Expr:
        """A dialect's own form, written as given, its arguments rendered for the dialect."""
        return exp.Var(this=template.format(**{k: self.sql(v) for k, v in args.items()}))

    def bound_type(self, element: str) -> exp.DataType:
        written = _BOUND_TYPES.get(self.dialect, {}).get(element)
        if written is not None:
            return exp.DataType(this=_T.USERDEFINED, kind=written)
        return exp.DataType.build(_BOUND_DEFAULTS[element])

    def typed(self, value: exp.Expr, element: str) -> exp.Expr:
        return exp.Cast(this=value.copy(), to=self.bound_type(element))

    def bound_from_text(self, text: exp.Expr, element: str) -> exp.Expr:
        if element != "timestamptz":
            return self.typed(text, element)
        template = _INSTANT_FROM_TEXT.get(self.dialect)
        if self.dialect not in _OFFSET_WITH_MINUTES:
            return self.typed(text, element) if template is None else self.raw(template, a=text)
        # PostgreSQL's +00 is the +00:00 these readers of an offset ask for.
        regex = _OFFSET_MINUTES_ADDED.get(self.dialect)
        if regex is not None:
            written = self.raw(regex, a=text)
        else:
            offset_only = exp.In(
                this=_substring(text, _minus(_length(text), _int(2)), _int(1)),
                expressions=[_string("+"), _string("-")],
            )
            written = _case([(offset_only, _concat(text, _string(":00")))], text)
        return self.typed(written, element) if template is None else self.raw(template, a=written)

    def element_value(self, node: exp.Expr, rtype: RangeType) -> exp.Expr:
        """An element beside a range: a literal is read as a bound's text is."""
        inner = _unwrapped(node)
        # A literal, or a literal cast to its type ('2020-01-10'::date): read as a bound's text is.
        written = _string_literal(inner.this if isinstance(inner, exp.Cast) else inner)
        if written is not None:
            return self._literal_bound(pg_range.read_element(rtype, written), rtype)
        return self.typed(node, rtype.element)

    def _literal_bound(self, value: object, rtype: RangeType) -> exp.Expr:  # object-ok: an element
        return self.bound_from_text(_string(pg_range.element_text(value)), rtype.element)

    def raised(self, sqlstate: str, message: str) -> exp.Expr:
        """An expression of integer type raising PostgreSQL's error ``message``."""
        text = _string(f"{MARK} {sqlstate}: {message}")
        if self.dialect == "clickhouse":
            return _call("throwIf", _int(1), text)
        name = _RAISE.get(self.dialect)
        if name is not None:
            return _call(name, text)
        return exp.Cast(this=text, to=exp.DataType.build("INT"))

    def refusal(self, condition: exp.Expr, sqlstate: str, message: str, null: exp.Expr) -> exp.Expr:
        """``null``, raising ``message`` where ``condition`` holds."""
        if _is_false(condition):
            return null
        return _eq(
            _case(
                [(condition, self.raised(sqlstate, message)), (null, _int(1))],
                _int(0),
            ),
            _int(1),
        )

    def clock_text(self, value: exp.Expr, zoned: bool) -> exp.Expr:
        """A timestamp in PostgreSQL's text form: the fraction's trailing zeros gone, a zoned one
        at UTC, which is the session's zone."""
        if self.dialect == "duckdb":
            return _text_cast(value)
        template = _CLOCK_TEXT.get(self.dialect)
        if template is None:
            return _text_cast(value)
        wall = value
        if zoned and self.dialect in _UTC_WALL:
            wall = self.raw(_UTC_WALL[self.dialect], a=value)
        fixed = self.raw(template, a=wall)
        fraction = _substring(fixed, _int(21), _int(6))
        # Trailing zeros go as trailing spaces do: every dialect trims those.
        kept = _replace(exp.Trim(this=_replace(fraction, "0", " "), position="TRAILING"), " ", "0")
        whole = _substring(fixed, _int(1), _int(19))
        written = _case(
            [(_eq(fraction, _string("000000")), whole)], _concat(whole, _string("."), kept)
        )
        return _concat(written, _string("+00")) if zoned else written

    def element_text(self, value: exp.Expr, rtype: RangeType) -> exp.Expr:
        if rtype.element in ("timestamp", "timestamptz"):
            return self.clock_text(value, rtype.element == "timestamptz")
        return _text_cast(value)

    def after(self, value: exp.Expr, steps: exp.Expr, rtype: RangeType) -> exp.Expr:
        """The discrete value ``steps`` after ``value``."""
        if rtype.element != "date":
            return _plus(value, steps) if not _is_zero(steps) else value
        added = exp.DateAdd(this=value.copy(), expression=steps.copy(), unit=exp.Var(this="DAY"))
        return self.typed(added, "date")

    def step(self, value: exp.Expr, rtype: RangeType) -> exp.Expr:
        return self.after(value, _int(1), rtype)

    # -- ranges -------------------------------------------------------------------------------

    def literal(self, value: Range) -> _R:
        def side(bound: object, inc: bool) -> _Side:  # object-ok: an element or an unbounded side
            if bound is None:
                return _Side(exp.true(), _null(), exp.false(), _string(""))
            return _Side(
                exp.false(),
                self._literal_bound(bound, value.rtype),
                _bool(inc),
                _string(pg_range.element_text(bound)),
            )

        return _R(
            value.rtype,
            exp.false(),
            _bool(value.empty),
            side(value.lower, value.lower_inc),
            side(value.upper, value.upper_inc),
            value,
        )

    def parsed(self, text: exp.Expr, rtype: RangeType) -> _R:
        """A range from its text, in PostgreSQL's form: ``empty``, or ``[lo,hi)`` with either
        bound absent. A discrete range written with another bound is made canonical as range_in
        makes it ('(1,5]' is '[2,6)'). Each part is one short expression: an operator names them
        many times over, so nothing here repeats a bound's reading. ``NULLIF`` is what keeps the
        word ``empty`` from a cast, as NULL passes through every function here."""
        held = exp.Nullif(this=text.copy(), expression=_string("empty"))
        comma = _strpos(held, ",")
        quoted = rtype.element in ("timestamp", "timestamptz")

        def part(start: exp.Expr, length: exp.Expr) -> exp.Expr:
            piece = _substring(held, start, length)
            return _replace(piece, '"', "") if quoted else piece

        lower_text = part(_int(2), _minus(comma, _int(2)))
        upper_text = part(_plus(comma, _int(1)), _minus(_minus(_length(held), comma), _int(1)))
        first = _substring(text, _int(1), _int(1))
        last = _substring(text, _length(text), _int(1))
        lower_inf = _eq(_substring(text, _int(2), _int(1)), _string(","))
        upper_inf = _eq(_substring(text, _minus(_length(text), _int(1)), _int(1)), _string(","))

        def bound(written: exp.Expr) -> exp.Expr:
            return self.bound_from_text(
                exp.Nullif(this=written, expression=_string("")), rtype.element
            )

        low, high = bound(lower_text), bound(upper_text)
        if rtype.discrete:
            low = self.after(low, _case([(_eq(first, _string("(")), _int(1))], _int(0)), rtype)
            high = self.after(high, _case([(_eq(last, _string("]")), _int(1))], _int(0)), rtype)
            lower_inc, upper_inc = _not(lower_inf), exp.false()
            lower_text, upper_text = _text_cast(low), _text_cast(high)
        else:
            lower_inc = _and(_eq(first, _string("[")), _not(lower_inf))
            upper_inc = _and(_eq(last, _string("]")), _not(upper_inf))
        return _R(
            rtype,
            _is_null(text),
            _eq(text, _string("empty")),
            _Side(lower_inf, low, lower_inc, lower_text),
            _Side(upper_inf, high, upper_inc, upper_text),
        )

    def null_range(self, rtype: RangeType) -> _R:
        side = _Side(exp.false(), _null(), exp.false(), _null())
        return _R(rtype, exp.true(), exp.false(), side, side)

    def text(self, value: _Value) -> exp.Expr:
        """A range or multirange in PostgreSQL's text form."""
        if isinstance(value, _M):
            if _is_true(value.null):
                return _text_cast(_null())
            return _case([(value.null, _null())], value.text)
        if value.literal is not None:
            return _string(pg_range.range_text(value.literal))
        if _is_true(value.null):
            return _text_cast(_null())
        quote = _string('"' if value.rtype.element in ("timestamp", "timestamptz") else "")

        def bound(side: _Side) -> exp.Expr:
            return _case([(side.inf, _string(""))], _concat(quote, side.text, quote))

        body = _concat(
            _case([(value.lower.inc, _string("["))], _string("(")),
            bound(value.lower),
            _string(","),
            bound(value.upper),
            _case([(value.upper.inc, _string("]"))], _string(")")),
        )
        return _case([(value.null, _null()), (value.empty, _string("empty"))], body)

    def order_keys(self, value: _R, filled: bool = False) -> list[exp.Expr]:
        """Sort keys in range_cmp's order: the empty range first, then the lower bound (an
        unbounded one first, an inclusive one before an exclusive), then the upper (an unbounded
        one last, an inclusive one after an exclusive). ``filled``: no NULL among them, for a row
        a dialect compares (Trino refuses a NULL there)."""

        def bound(side: _Side) -> exp.Expr:
            if not filled:
                return side.value.copy()
            filler = self.bound_from_text(
                _string(_FILLER[value.rtype.element]), value.rtype.element
            )
            return exp.Coalesce(this=side.value.copy(), expressions=[filler])

        def flag(condition: exp.Expr, when: int, otherwise: int) -> exp.Expr:
            return _case([(condition, _int(when))], _int(otherwise))

        keys = [
            _case([(value.null, _null()), (value.empty, _int(0))], _int(1)),
            flag(value.lower.inf, 0, 1),
            bound(value.lower),
            flag(value.lower.inc, 0, 1),
            flag(value.upper.inf, 1, 0),
            bound(value.upper),
            flag(value.upper.inc, 1, 0),
        ]
        # A constant says nothing, and a number in ORDER BY would name a column.
        return [key for key in keys if not isinstance(key, (exp.Literal, exp.Null))]

    def constructed(self, rtype: RangeType, args: list[exp.Expr]) -> _R:
        """int4range(lower, upper [, flags]): a NULL bound is unbounded, as the constructor has
        it; reversed bounds are refused, as range_serialize refuses them."""
        low_arg, high_arg = args[0], args[1]
        flags = args[2] if len(args) > 2 else _string("[)")  # noqa: PLR2004 -- the flags argument
        if _null_value(flags):
            raise PostgresError("22000", "range constructor flags argument must not be null")
        known = _string_literal(flags)
        if known is not None and known not in ("[)", "(]", "[]", "()"):
            raise PostgresError("42601", "invalid range bound flags")
        if known is not None and all(_constant(a) for a in (low_arg, high_arg)):
            return self.literal(
                pg_range.make_range(
                    rtype,
                    _constant_element(low_arg, rtype),
                    _constant_element(high_arg, rtype),
                    known[0] == "[",
                    known[1] == "]",
                )
            )
        if known is not None:
            lower_inc, upper_inc = _bool(known[0] == "["), _bool(known[1] == "]")
        else:
            lower_inc = _eq(_substring(flags, _int(1), _int(1)), _string("["))
            upper_inc = _eq(_substring(flags, _int(2), _int(1)), _string("]"))
        lower_inf, upper_inf = _is_null(low_arg), _is_null(high_arg)
        low, high = self.element_value(low_arg, rtype), self.element_value(high_arg, rtype)
        both_set = _and(_not(lower_inf), _not(upper_inf))
        reversed_bounds = _and(both_set, exp.GT(this=low.copy(), expression=high.copy()))
        lower_inc, upper_inc = _and(lower_inc, _not(lower_inf)), _and(upper_inc, _not(upper_inf))
        if rtype.discrete:
            low = _case([(lower_inc, low)], self.step(low, rtype))
            high = _case([(upper_inc, self.step(high, rtype))], high)
            lower_inc, upper_inc = _not(lower_inf), exp.false()
            holds_none = exp.GTE(this=low.copy(), expression=high.copy())
            written_low, written_high = _text_cast(low), _text_cast(high)
        else:
            holds_none = _or(
                exp.GT(this=low.copy(), expression=high.copy()),
                _and(_eq(low, high), _not(_and(lower_inc, upper_inc))),
            )
            written_low = _numeric_or_element_text(self, low_arg, low, rtype)
            written_high = _numeric_or_element_text(self, high_arg, high, rtype)
        return _R(
            rtype,
            self.refusal(
                reversed_bounds,
                "22000",
                "range lower bound must be less than or equal to range upper bound",
                exp.false(),
            ),
            _and(both_set, holds_none),
            _Side(lower_inf, low, lower_inc, written_low),
            _Side(upper_inf, high, upper_inc, written_high),
        )

    def union(self, a: _R, b: _R) -> _R:
        apart = _and(
            _not(_or(a.null, b.null)),
            _both(a, b),
            _not(_or(_overlaps(a, b), _adjacent(a, b))),
        )
        merged = self.merge(a, b)
        merged.null = self.refusal(
            apart, "22000", "result of range union would not be contiguous", merged.null
        )
        return merged

    def merge(self, a: _R, b: _R) -> _R:
        lower = _or(b.empty, _and(_not(a.empty), _lte0(_ll(a, b))))
        upper = _or(b.empty, _and(_not(a.empty), _gte0(_uu(a, b))))
        return _R(
            a.rtype,
            _or(a.null, b.null),
            _and(a.empty, b.empty),
            _choose(lower, a.lower, b.lower),
            _choose(upper, a.upper, b.upper),
        )

    def intersect(self, a: _R, b: _R) -> _R:
        return _R(
            a.rtype,
            _or(a.null, b.null),
            _not(_overlaps(a, b)),
            _choose(_gte0(_ll(a, b)), a.lower, b.lower),
            _choose(_lte0(_uu(a, b)), a.upper, b.upper),
        )

    def minus(self, a: _R, b: _R) -> _R:
        """range_minus_internal: b reaching a's lower bound leaves what follows b, b reaching its
        upper what precedes b, both nothing, and b inside a with a left on both sides is refused."""
        both = _both(a, b)
        l1l2, u1u2 = _ll(a, b), _uu(a, b)
        reaches_lower = _and(both, _gte0(l1l2), _lte0(_lu(a, b)))
        reaches_upper = _and(both, _lte0(u1u2), _lte0(_lu(b, a)))
        return _R(
            a.rtype,
            self.refusal(
                _and(both, _lt0(l1l2), _gt0(u1u2)),
                "22000",
                "result of range difference would not be contiguous",
                _or(a.null, b.null),
            ),
            _or(a.empty, _and(reaches_lower, reaches_upper)),
            _choose(reaches_lower, b.upper.flipped(), a.lower),
            _choose(reaches_upper, b.lower.flipped(), a.upper),
        )

    # -- multiranges --------------------------------------------------------------------------

    def literal_multirange(self, value: Multirange) -> _M:
        return _M(value.rtype, exp.false(), _string(pg_range.multirange_text(value)), value)

    def first(self, value: _M) -> _R:
        """A multirange's first range; the empty range for one with none."""
        if value.literal is not None:
            ranges = value.literal.ranges
            return self.literal(ranges[0] if ranges else pg_range.parse_range(value.rtype, "empty"))
        t = value.text
        closes = _strpos(_replace(t, "]", ")"), ")")
        range_text = _case(
            [(_eq(t, _string(_EMPTY_MULTIRANGE)), _string("empty"))],
            _substring(t, _int(2), _minus(closes, _int(1))),
        )
        out = self.parsed(range_text, value.rtype)
        out.null = _is_null(t)
        return out

    def last(self, value: _M) -> _R:
        """A multirange's last range; the empty range for one with none."""
        if value.literal is not None:
            ranges = value.literal.ranges
            return self.literal(
                ranges[-1] if ranges else pg_range.parse_range(value.rtype, "empty")
            )
        t = value.text
        reversed_text = exp.Anonymous(this="REVERSE", expressions=[_replace(t, "(", "[")])
        opens = _plus(_minus(_length(t), _strpos(reversed_text, "[")), _int(1))
        range_text = _case(
            [(_eq(t, _string(_EMPTY_MULTIRANGE)), _string("empty"))],
            _substring(t, opens, _minus(_length(t), opens)),
        )
        out = self.parsed(range_text, value.rtype)
        out.null = _is_null(t)
        return out

    def whole(self, value: _M) -> _R:
        if value.literal is not None:
            return self.literal(pg_range.whole(value.literal))
        first, last = self.first(value), self.last(value)
        return _R(value.rtype, value.null, first.empty, first.lower, last.upper)

    def is_none(self, value: _M) -> exp.Expr:
        if value.literal is not None:
            return _bool(not value.literal.ranges)
        return _eq(value.text, _string(_EMPTY_MULTIRANGE))

    def of_range(self, value: _R) -> _M:
        mr = MULTIRANGE_TYPES[value.rtype.multirange]
        if value.literal is not None:
            return self.literal_multirange(pg_range.make_multirange(mr, [value.literal]))
        body = _concat(_string("{"), self.text(replace(value, null=exp.false())), _string("}"))
        return _M(mr, value.null, _case([(value.empty, _string(_EMPTY_MULTIRANGE))], body))

    def joined(self, text: exp.Expr) -> exp.Expr:
        """An aggregate of a group's texts, joined by commas."""
        if self.dialect == "clickhouse":  # groupConcat is of later servers
            return _call("arrayStringConcat", _call("groupArray", text), _string(","))
        if self.dialect in ("trino", "athena"):  # its LISTAGG asks a WITHIN GROUP
            return _call("array_join", _call("array_agg", text), _string(","))
        return exp.GroupConcat(this=text.copy(), separator=_string(","))

    def _matches(self, text: exp.Expr, pattern: str) -> exp.Expr:
        name = _REGEX_LIST[self.dialect]
        if self.dialect == "clickhouse":  # its arrays are never Nullable
            text = _call("ifNull", text, _string(""))
        extra = [_int(0)] if self.dialect in ("databricks", "spark") else []
        return _call(name, text, _string(pattern), *extra)

    def _ranges_list(self, text: exp.Expr) -> exp.Expr:
        return self._matches(text, _RANGE_PATTERN)

    def _list_form(self, rtype: RangeType, operation: str) -> None:
        if self.dialect not in _REGEX_LIST:
            raise SubtypeOperationRefused(rtype.multirange, operation, self.dialect)

    def reduce(
        self,
        items: exp.Expr,
        start: exp.Expr,
        step: Callable[[exp.Expr, exp.Expr], exp.Expr],
        rtype: RangeType,
        operation: str,
    ) -> exp.Expr:
        """A fold of a list of texts, its state a text, with the dialect's own reduce."""
        if self.dialect not in _REDUCE:
            raise SubtypeOperationRefused(rtype.multirange, operation, self.dialect)
        state_name, item_name = self.fresh("s"), self.fresh("x")
        state, item = exp.column(state_name), exp.column(item_name)
        if self.dialect == "snowflake":  # a lambda's variables are VARIANT there
            state, item = _text_cast(state), _text_cast(item)
        body = step(state, item)
        fold = exp.Lambda(
            this=body,
            expressions=[exp.to_identifier(state_name), exp.to_identifier(item_name)],
        )
        name = _REDUCE[self.dialect]
        if self.dialect == "duckdb":  # its state is the list's own first item
            return _call(name, _call("list_prepend", start, items), fold)
        if self.dialect == "clickhouse":
            return _call(name, fold, items, _text_cast(start))
        if self.dialect in ("trino", "athena"):
            done = self.fresh("d")
            same = exp.Lambda(this=exp.column(done), expressions=[exp.to_identifier(done)])
            return _call(name, items, start, fold, same)
        return _call(name, items, start, fold)  # databricks, spark, snowflake

    def sorted_ranges(self, text: exp.Expr, rtype: RangeType, operation: str) -> exp.Expr:
        """The ranges in ``text`` in range_cmp's order: each a row of its sort keys and itself,
        the rows sorted, the text taken back. Empty ranges are not in it: the pattern reads only
        a bracketed range."""
        forms = _SORT.get(self.dialect)
        if forms is None:
            raise SubtypeOperationRefused(rtype.multirange, operation, self.dialect)
        row, sort, pick = forms
        name = self.fresh("r")
        var = exp.column(name)
        keys = [*self.order_keys(self.parsed(var, rtype), filled=True), var]
        keyed = exp.Lambda(this=_call(row, *keys), expressions=[exp.to_identifier(name)])
        back = self.fresh("k")
        taken = exp.Var(this=pick.format(a=back, n=len(keys)))
        unkeyed = exp.Lambda(this=taken, expressions=[exp.to_identifier(back)])
        return self._mapped(_call(sort, self._mapped(self._ranges_list(text), keyed)), unkeyed)

    def _mapped(self, items: exp.Expr, function: exp.Expr) -> exp.Expr:
        if self.dialect == "duckdb":
            return _call("list_transform", items, function)
        if self.dialect == "clickhouse":
            return _call("arrayMap", function, items)
        return _call("transform", items, function)

    def _bound_text(self, side: _Side, rtype: RangeType) -> exp.Expr:
        quote = _string('"' if rtype.element in ("timestamp", "timestamptz") else "")
        return _case([(side.inf, _string(""))], _concat(quote, side.text, quote))

    def _last_upper(self, state: exp.Expr, rtype: RangeType) -> tuple[_Side, exp.Expr]:
        """A multirange text's last upper bound, and the text up to it. A sort has put every
        later range's lower bound after this one's, so this bound is all a merge needs of it."""
        reverse = exp.Anonymous(this="REVERSE", expressions=[state.copy()])
        comma = _plus(_minus(_length(state), _strpos(reverse, ",")), _int(1))
        raw = _substring(
            state, _plus(comma, _int(1)), _minus(_length(state), _plus(comma, _int(2)))
        )
        written = raw
        if rtype.element in ("timestamp", "timestamptz"):
            written = _replace(raw, '"', "")
        side = _Side(
            _eq(_substring(state, _minus(_length(state), _int(2)), _int(1)), _string(",")),
            self.bound_from_text(exp.Nullif(this=written, expression=_string("")), rtype.element),
            _eq(_substring(state, _minus(_length(state), _int(1)), _int(1)), _string("]")),
            written,
        )
        return side, _substring(state, _int(1), comma)

    def _appended(self, state: exp.Expr, range_text: exp.Expr) -> exp.Expr:
        """A multirange text with a range after its last. The range is named once: it is the
        largest thing in a fold, and a compiler counts each naming."""
        opening = _case(
            [(_eq(state, _string(_EMPTY_MULTIRANGE)), _string("{"))],
            _concat(_substring(state, _int(1), _minus(_length(state), _int(1))), _string(",")),
        )
        return _concat(opening, range_text, _string("}"))

    def _lower_written(self, held: exp.Expr) -> exp.Expr:
        """A range's text up to its comma: its bracket and lower bound, as held."""
        return _substring(held, _int(1), _minus(_strpos(held, ","), _int(1)))

    def _upper_written(self, held: exp.Expr) -> exp.Expr:
        """A range's text after its comma: its upper bound and bracket, as held."""
        comma = _strpos(held, ",")
        return _substring(held, _plus(comma, _int(1)), _minus(_length(held), comma))

    def _absorbed(self, state: exp.Expr, item: exp.Expr, rtype: RangeType) -> exp.Expr:
        """multirange_canonicalize's step: the next range in order, merged into the last it
        overlaps or meets, else after it."""
        next_range = self.parsed(item, rtype)
        upper, head = self._last_upper(state, rtype)
        reached = _or(
            _lte0(_bound_cmp(next_range.lower, True, upper, False)),
            _touch(upper, next_range.lower),
        )
        kept = _choose(
            _gte0(_bound_cmp(upper, False, next_range.upper, False)), upper, next_range.upper
        )
        merged = _concat(
            head,
            self._bound_text(kept, rtype),
            _case([(kept.inc, _string("]"))], _string(")")),
            _string("}"),
        )
        written = self.text(replace(next_range, null=exp.false()))
        return _case(
            [
                (
                    _not(_eq(state, _string(_EMPTY_MULTIRANGE))),
                    _case([(reached, merged)], self._appended(state, written)),
                )
            ],
            _concat(_string("{"), written, _string("}")),
        )

    def canonical(self, text: exp.Expr, rtype: RangeType, operation: str) -> exp.Expr:
        """multirange_canonicalize of every range in ``text``, a joining of ranges and
        multiranges."""
        self._list_form(rtype, operation)
        return self.reduce(
            self.sorted_ranges(text, rtype, operation),
            _string(_EMPTY_MULTIRANGE),
            lambda state, item: self._absorbed(state, item, rtype),
            rtype,
            operation,
        )

    def multirange_intersect(
        self, a: exp.Expr, b: exp.Expr, rtype: RangeType, operation: str
    ) -> exp.Expr:
        """Each range of a with each of b. Both are in order and apart, so the pieces are too."""
        self._list_form(rtype, operation)

        def against(state: exp.Expr, x: exp.Expr) -> exp.Expr:
            first = self.parsed(x, rtype)

            def piece(held: exp.Expr, y: exp.Expr) -> exp.Expr:
                # Each side of the overlap is one range's, chosen once from the text held.
                second = self.parsed(y, rtype)
                lower = _case(
                    [(_gte0(_ll(first, second)), self._lower_written(x))],
                    self._lower_written(y),
                )
                upper = _case(
                    [(_lte0(_uu(first, second)), self._upper_written(x))],
                    self._upper_written(y),
                )
                written = _concat(lower, _string(","), upper)
                return _case([(_overlaps(first, second), self._appended(held, written))], held)

            return self.reduce(self._ranges_list(b), state, piece, rtype, operation)

        return self.reduce(
            self._ranges_list(a), _string(_EMPTY_MULTIRANGE), against, rtype, operation
        )

    def multirange_complement(self, text: exp.Expr, rtype: RangeType, operation: str) -> exp.Expr:
        """Everything a multirange does not hold: the whole line, cut at each range. The state's
        last range is open until the next cut."""
        self._list_form(rtype, operation)

        def cut(state: exp.Expr, item: exp.Expr) -> exp.Expr:
            taken = self.parsed(item, rtype)
            lower, upper = taken.lower, taken.upper
            before = _concat(
                _substring(state, _int(1), _minus(_length(state), _int(3))),
                _string(","),
                self._bound_text(lower, rtype),
                _case([(lower.inc, _string(")"))], _string("]")),
            )
            after = _concat(
                _case([(lower.inf, _string(""))], _string(",")),
                _case([(upper.inc, _string("("))], _string("[")),
                self._bound_text(upper, rtype),
                _string(",)"),
            )
            return _concat(
                _case([(lower.inf, _string("{"))], before),
                _case([(upper.inf, _string(""))], after),
                _string("}"),
            )

        return self.reduce(self._ranges_list(text), _string("{(,)}"), cut, rtype, operation)

    def intersect_all(self, joined: exp.Expr, rtype: RangeType, operation: str) -> exp.Expr:
        """range_intersect_agg of ranges joined by commas; the empty range holds nothing, and
        the pattern reads only a bracketed range, so it is looked for first."""
        self._list_form(rtype, operation)

        def step(state: exp.Expr, item: exp.Expr) -> exp.Expr:
            met = self.intersect(self.parsed(state, rtype), self.parsed(item, rtype))
            return self.text(replace(met, null=exp.false()))

        fold = self.reduce(self._ranges_list(joined), _string("(,)"), step, rtype, operation)
        has_empty = exp.GT(this=_strpos(joined, "empty"), expression=_int(0))
        return _case([(has_empty, _string("empty"))], fold)

    def multirange_intersect_all(
        self, joined: exp.Expr, rtype: RangeType, operation: str
    ) -> exp.Expr:
        """range_intersect_agg of multiranges joined by commas."""
        self._list_form(rtype, operation)
        return self.reduce(
            self._matches(joined, _MULTIRANGE_PATTERN),
            _string("{(,)}"),
            lambda state, item: self.multirange_intersect(state, item, rtype, operation),
            rtype,
            operation,
        )

    def _delimited(self, text: exp.Expr) -> exp.Expr:
        """A multirange's ranges, each apart from the next by a bar; no braces, no quotes."""
        inner = _substring(text, _int(2), _minus(_length(text), _int(2)))
        return _replace(_replace(_replace(inner, '"', ""), "),", ")|"), "],", "]|")

    def any(
        self, value: _M, test: Callable[[_R], exp.Expr], depth: int, operation: str
    ) -> exp.Expr:
        """Whether any range of a multirange passes ``test``."""
        if value.literal is not None:
            return _or(*(test(self.literal(r)) for r in value.literal.ranges))
        name = f"_r{depth}"
        var = exp.column(name)
        # A multirange of no range splits to one empty item. It is no range, so it passes no test
        # (even a negated one); NULLIF keeps it from a substring, as no WHERE is promised to stop
        # first.
        if self.dialect == "tsql":
            alias = f"_s{depth}"
            item = exp.column("value", table=alias)
            item = exp.Nullif(this=item, expression=_string(""))
            found = (
                exp.select(_int(1))
                .from_(
                    exp.Table(
                        this=_call("STRING_SPLIT", self._delimited(value.text), _string("|")),
                        alias=exp.TableAlias(this=exp.to_identifier(alias)),
                    )
                )
                .where(_and(_not(_is_null(item)), test(self.parsed(item, value.rtype))))
            )
            return exp.Exists(this=found)
        if self.dialect in ("mysql", "oracle"):
            alias = f"_s{depth}"
            item = exp.column(name, table=alias)
            json_list = _concat(
                _string('["'), _replace(self._delimited(value.text), "|", '","'), _string('"]')
            )
            table = self.raw(
                "JSON_TABLE({a}, '$[*]' COLUMNS (" + name + " VARCHAR(255) PATH '$')) AS " + alias,
                a=json_list,
            )
            item = exp.Nullif(this=item, expression=_string(""))
            found = (
                exp.select(_int(1))
                .from_(table)
                .where(_and(_not(_is_null(item)), test(self.parsed(item, value.rtype))))
            )
            return exp.Exists(this=found)
        if self.dialect not in _REGEX_LIST:
            raise SubtypeOperationRefused(value.rtype.multirange, operation, self.dialect)
        items = self._ranges_list(value.text)
        if self.dialect == "bigquery":
            unnested = exp.Unnest(
                expressions=[items],
                alias=exp.TableAlias(columns=[exp.to_identifier(name)]),
            )
            found = exp.select(_int(1)).from_(unnested).where(test(self.parsed(var, value.rtype)))
            return exp.Exists(this=found)
        as_text = _text_cast(var) if self.dialect == "snowflake" else var
        passed = test(self.parsed(as_text, value.rtype))
        lambda_ = exp.Lambda(this=passed, expressions=[exp.to_identifier(name)])
        if self.dialect == "duckdb":
            return _gt_zero_len("len", _call("list_filter", items, lambda_))
        if self.dialect == "snowflake":
            return _gt_zero_len("ARRAY_SIZE", _call("FILTER", items, lambda_))
        if self.dialect == "clickhouse":
            return _call("arrayExists", lambda_, items)
        if self.dialect in ("databricks", "spark"):
            return _call("exists", items, lambda_)
        return _call("any_match", items, lambda_)  # trino, athena

    def multirange_sort_key(self, value: _M, operation: str) -> exp.Expr:
        """A multirange's comparable form: a list of its ranges' sort keys, compared item by item
        and then by length, as multirange_cmp compares it. On the dialects whose list of rows
        compares so."""
        row = {"duckdb": "row", "trino": "ROW", "clickhouse": "tuple", "databricks": "struct"}.get(
            self.dialect
        )
        if row is None:
            raise SubtypeOperationRefused(value.rtype.multirange, operation, self.dialect)
        items = self._ranges_list(value.text)
        var = exp.column("_r0")
        keys = self.order_keys(self.parsed(var, value.rtype), filled=True)
        lambda_ = exp.Lambda(this=_call(row, *keys), expressions=[exp.to_identifier("_r0")])
        if self.dialect == "duckdb":
            return _call("list_transform", items, lambda_)
        if self.dialect == "clickhouse":
            return _call("arrayMap", lambda_, items)
        return _call("transform", items, lambda_)


def _gt_zero_len(size: str, items: exp.Expr) -> exp.Expr:
    return exp.GT(this=_call(size, items), expression=_int(0))


def _numeric_or_element_text(
    writer: _Writer, argument: exp.Expr, typed: exp.Expr, rtype: RangeType
) -> exp.Expr:
    """A continuous bound's text: a numeric as its argument prints, which keeps its scale."""
    if rtype.element == "numeric":
        return _text_cast(argument)
    return writer.element_text(typed, rtype)


def _constant(node: exp.Expr) -> bool:
    inner = _unwrapped(node)
    if isinstance(inner, exp.Neg):
        return _constant(inner.this)
    if _null_value(inner) or isinstance(inner, exp.Literal):
        return True
    return isinstance(inner, exp.Cast) and _constant(inner.this)


def _constant_element(node: exp.Expr, rtype: RangeType) -> object:  # object-ok: an element or None
    inner = _unwrapped(node)
    if _null_value(inner):
        return None
    if isinstance(inner, exp.Neg):
        return pg_range.read_element(rtype, "-" + _constant_text(inner.this))
    return pg_range.read_element(rtype, _constant_text(inner))


def _constant_text(node: exp.Expr) -> str:
    inner = _unwrapped(node)
    while isinstance(inner, exp.Cast):
        inner = _unwrapped(inner.this)
    return inner.this


#: A range operator or its function: (the operator's own name, what it does).
_BOOLEAN_NODES = {
    exp.ArrayOverlaps: ("&&", "overlap"),
    exp.ArrayContainsAll: ("@>", "contains"),
    exp.ArrayContainedBy: ("<@", "contained"),
    exp.BitwiseLeftShift: ("<<", "before"),
    exp.BitwiseRightShift: (">>", "after"),
    exp.ExtendsLeft: ("&<", "overleft"),
    exp.ExtendsRight: ("&>", "overright"),
    exp.Adjacent: ("-|-", "adjacent"),
}
_BOOLEAN_FUNCTIONS = {
    "overlaps": "overlap",
    "contains": "contains",
    "contains_elem": "contains",
    "contains_range": "contains",
    "contains_multirange": "contains",
    "contained_by": "contained",
    "contained_by_range": "contained",
    "contained_by_multirange": "contained",
    "before": "before",
    "after": "after",
    "overleft": "overleft",
    "overright": "overright",
    "adjacent": "adjacent",
}
_COMPARE_FUNCTIONS = {
    "eq": exp.EQ,
    "ne": exp.NEQ,
    "lt": exp.LT,
    "le": exp.LTE,
    "gt": exp.GT,
    "ge": exp.GTE,
}
_VALUE_FUNCTIONS = {"union": "+", "intersect": "*", "minus": "-"}
_VALUE_NODES = {exp.Add: "+", exp.Mul: "*", exp.Sub: "-"}
_BOOLEAN_PARTS = ("isempty", "lower_inc", "upper_inc", "lower_inf", "upper_inf")


def _operand_name(name: str) -> tuple[str | None, str]:
    """The function family of a named range or multirange operator: ``range_overlaps_multirange``
    is the overlap operator, ``elem_contained_by_range`` the containment one."""
    if name.startswith("elem_contained_by_"):
        return "elem", "contained"
    for prefix in ("multirange_", "range_"):
        if not name.startswith(prefix):
            continue
        rest = name[len(prefix) :]
        for suffix in ("_multirange", "_range"):
            if rest.endswith(suffix) and rest[: -len(suffix)] in _BOOLEAN_FUNCTIONS:
                return prefix, rest[: -len(suffix)]
        return prefix, rest
    return None, name


class _Rewrite:
    """One statement's ranges, written for one dialect."""

    def __init__(self, dialect: str) -> None:
        self.writer = _Writer(dialect)
        self.dialect = dialect
        self.depth = 0

    def rewritten(self, node: exp.Expr) -> exp.Expr:
        return node.transform(self.visit)

    # -- what value an expression is ----------------------------------------------------------

    def value(self, node: exp.Expr) -> _Value | None:  # noqa: PLR0911, PLR0912 -- one per form
        node = _unwrapped(node)
        found = marked(node)
        if found is not None:
            column, subtype = found
            if subtype in RANGE_TYPES:
                return self.writer.parsed(column, RANGE_TYPES[subtype])
            if subtype in MULTIRANGE_TYPES:
                return _M(MULTIRANGE_TYPES[subtype], _is_null(column), column.copy())
            return None
        if isinstance(node, exp.Cast):
            return self._cast(node)
        if isinstance(node, (exp.Add, exp.Mul, exp.Sub)):
            a, b = self._pair(node.this, node.expression)
            if a is None or b is None:
                return None
            return self._combined(_VALUE_NODES[type(node)], a, b)
        if not isinstance(node, exp.Anonymous):
            if isinstance(node, exp.Dot) and _catalog(node):
                return self.value(node.expression)
            return None
        name = node.name.lower()
        args = list(node.expressions)
        if name in RANGE_TYPES:
            return self.writer.constructed(RANGE_TYPES[name], [self.rewritten(a) for a in args])
        if name in MULTIRANGE_TYPES:
            return self._multirange_of(MULTIRANGE_TYPES[name], args)
        if name.endswith("_canonical") and name[: -len("_canonical")] in RANGE_TYPES:
            return self._as(args[0], RANGE_TYPES[name[: -len("_canonical")]])
        if name == "multirange":
            inner = self.value(args[0])
            return self.writer.of_range(inner) if isinstance(inner, _R) else inner
        if name == "range_merge":
            return self._merged(args)
        family, operator = _operand_name(name)
        if family in ("range_", "multirange_") and operator in _VALUE_FUNCTIONS:
            a, b = self._pair(args[0], args[1])
            if a is not None and b is not None:
                return self._combined(_VALUE_FUNCTIONS[operator], a, b)
        return None

    def _cast(self, node: exp.Cast) -> _Value | None:
        kind = node.to.this
        if kind in _CAST_RANGES:
            return self._as(node.this, RANGE_TYPES[_CAST_RANGES[kind]])
        if kind in _CAST_MULTIRANGES:
            return self._as_multirange(node.this, MULTIRANGE_TYPES[_CAST_MULTIRANGES[kind]])
        return None

    def _as(self, node: exp.Expr, rtype: RangeType) -> _R:
        written = _string_literal(node)
        if written is not None:
            return self.writer.literal(pg_range.parse_range(rtype, written))
        if _null_value(node):
            return self.writer.null_range(rtype)
        known = self.value(node)
        if isinstance(known, _R):
            return known
        return self.writer.parsed(self.rewritten(node), rtype)

    def _as_multirange(self, node: exp.Expr, rtype: RangeType) -> _M:
        written = _string_literal(node)
        if written is not None:
            return self.writer.literal_multirange(pg_range.parse_multirange(rtype, written))
        if _null_value(node):
            return _M(rtype, exp.true(), _null())
        known = self.value(node)
        if isinstance(known, _M):
            return known
        text = self.rewritten(node)
        return _M(rtype, _is_null(text), text)

    def _pair(self, left: exp.Expr, right: exp.Expr) -> tuple[_Value | None, _Value | None]:
        """Two operands, a literal beside a range or multirange read as its type. Neither is one:
        (None, None)."""
        a, b = self.value(left), self.value(right)
        if a is None and b is None:
            return None, None
        if a is None:
            a = self._beside(left, b)
        elif b is None:
            b = self._beside(right, a)
        return a, b

    def _beside(self, node: exp.Expr, partner: _Value | None) -> _Value | None:
        assert partner is not None
        if _null_value(node):  # an untyped NULL is of its partner's type
            if isinstance(partner, _M):
                return _M(partner.rtype, exp.true(), _null())
            return self.writer.null_range(partner.rtype)
        written = _string_literal(node)
        if written is None:
            return None
        head = written.lstrip()[:1]
        if head == "{":
            return self.writer.literal_multirange(pg_range.parse_multirange(partner.rtype, written))
        if head in ("[", "(") or written.strip().lower() == "empty":
            return self.writer.literal(pg_range.parse_range(partner.rtype, written))
        return None

    # -- combining ----------------------------------------------------------------------------

    def _combined(self, operator: str, a: _Value, b: _Value) -> _Value:
        if isinstance(a, _R) and isinstance(b, _R):
            if a.literal is not None and b.literal is not None:
                fold = {"+": pg_range.union, "*": pg_range.intersect, "-": pg_range.minus}
                return self.writer.literal(fold[operator](a.literal, b.literal))
            write = {"+": self.writer.union, "*": self.writer.intersect, "-": self.writer.minus}
            return write[operator](a, b)
        left, right = self._both_multirange(a), self._both_multirange(b)
        if left.literal is not None and right.literal is not None:
            fold_m = {"+": pg_range.mr_union, "*": pg_range.mr_intersect, "-": pg_range.mr_minus}
            return self.writer.literal_multirange(fold_m[operator](left.literal, right.literal))
        w, rtype = self.writer, left.rtype
        if operator == "+":
            text = w.canonical(_concat(left.text, _string(","), right.text), rtype, operator)
        elif operator == "*":
            text = w.multirange_intersect(left.text, right.text, rtype, operator)
        else:
            rest = w.multirange_complement(right.text, rtype, operator)
            text = w.multirange_intersect(left.text, rest, rtype, operator)
        return _M(rtype, _or(left.null, right.null), text)

    def _both_multirange(self, value: _Value) -> _M:
        return value if isinstance(value, _M) else self.writer.of_range(value)

    def _merged(self, args: list[exp.Expr]) -> _Value | None:
        if len(args) == 1:
            one = self.value(args[0])
            return self.writer.whole(one) if isinstance(one, _M) else None
        a, b = self._pair(args[0], args[1])
        if not isinstance(a, _R) or not isinstance(b, _R):
            return None
        if a.literal is not None and b.literal is not None:
            return self.writer.literal(pg_range.merge(a.literal, b.literal))
        return self.writer.merge(a, b)

    def _multirange_of(self, rtype: RangeType, args: list[exp.Expr]) -> _M:
        """int4multirange(), (range ...), (VARIADIC range[])."""
        if len(args) == 1 and _is_array(args[0]):
            raise PostgresError(
                "42883", f"function {rtype.multirange}({rtype.name}[]) does not exist"
            )
        given = _variadic_array(args[0]) if len(args) == 1 else None
        if given is not None:
            if _null_value(given):
                return _M(rtype, exp.true(), _null())
            inner = _unwrapped(given.this) if isinstance(given, exp.Cast) else given
            if not isinstance(inner, exp.Array):
                return self._of_array(rtype, self.rewritten(given))
            args = list(inner.expressions)
        parts = [self._as(a, rtype) for a in args]
        if not parts:
            return self.writer.literal_multirange(Multirange(rtype))
        if all(p.literal is not None for p in parts):
            literal = pg_range.make_multirange(rtype, [p.literal for p in parts])
            return self.writer.literal_multirange(literal)
        if len(parts) == 1:
            return self.writer.of_range(parts[0])
        pieces: list[exp.Expr] = []
        for part in parts:
            pieces += [*([_string(",")] if pieces else []), self.writer.text(part)]
        null = self.writer.refusal(
            _or(*(p.null for p in parts)),
            "22004",
            "multirange values cannot contain null members",
            exp.false(),
        )
        return _M(rtype, null, self.writer.canonical(_concat(*pieces), rtype, rtype.multirange))

    def _of_array(self, rtype: RangeType, array: exp.Expr) -> _M:
        """A multirange of an array of ranges only the row knows: strict, and a NULL member is
        refused."""
        member_null = exp.NEQ(this=_call("len", array), expression=_call("list_count", array))
        null = self.writer.refusal(
            _and(_not(_is_null(array)), member_null),
            "22004",
            "multirange values cannot contain null members",
            _is_null(array),
        )
        text = exp.ArrayToString(this=array.copy(), expression=_string(","))
        return _M(rtype, null, self.writer.canonical(text, rtype, rtype.multirange))

    # -- predicates ---------------------------------------------------------------------------

    def _three_valued(self, test: exp.Expr, *operands: _Value | _Nothing) -> exp.Expr:
        """``test`` over operands, NULL where one is. A test's result as an integer compared, so
        it is a value on every dialect."""
        null = _or(*(o.null for o in operands))
        if _is_false(null):
            return test
        if isinstance(test, exp.Boolean) and _is_true(null):
            return _null()
        return _eq(_case([(null, _null()), (test, _int(1))], _int(0)), _int(1))

    def _boolean(self, verb: str, shown: str, a: _Value, b: _Value) -> exp.Expr:
        if verb == "contained":
            verb, a, b = "contains", b, a
        elif verb == "after":
            verb, a, b = "before", b, a
        return self._three_valued(self._test(verb, shown, a, b), a, b)

    def _test(self, verb: str, shown: str, a: _Value, b: _Value) -> exp.Expr:  # noqa: PLR0911
        w = self.writer
        if isinstance(a, _R) and isinstance(b, _R):
            if a.literal is not None and b.literal is not None:
                return _bool(_FOLD_TESTS[verb](a.literal, b.literal))
            return _RANGE_TESTS[verb](a, b)
        if verb == "overlap":
            return self._quantified(a, b, _overlaps, shown)
        if verb == "contains":
            if isinstance(a, _R):
                assert isinstance(b, _M)
                return _or(w.is_none(b), _contains(a, w.whole(b)))
            if isinstance(b, _R):
                return _or(b.empty, self._some(a, lambda x: _contains(x, b), shown))
            depth = self.depth
            self.depth += 1
            inner = lambda y: self._some(a, lambda x: _contains(x, y), shown)  # noqa: E731
            lacks = w.any(b, lambda y: _not(inner(y)), depth, shown)
            self.depth -= 1
            return _not(lacks)
        first = self._edge(a, False)
        second = self._edge(b, True)
        if verb == "before":
            return _before(first, second)
        if verb == "overleft":
            return _overleft(first, self._edge(b, False))
        if verb == "overright":
            return _overright(self._edge(a, True), second)
        # multirange_adjacent_multirange: a's end meets b's start, or b's end meets a's start.
        return _and(
            _both(first, second),
            _or(
                _touch(first.upper, second.lower),
                _touch(self._edge(b, False).upper, self._edge(a, True).lower),
            ),
        )

    def _edge(self, value: _Value, at_start: bool) -> _R:
        if isinstance(value, _R):
            return value
        return self.writer.first(value) if at_start else self.writer.last(value)

    def _some(self, value: _M, test: Callable[[_R], exp.Expr], shown: str) -> exp.Expr:
        depth = self.depth
        self.depth += 1
        found = self.writer.any(value, test, depth, shown)
        self.depth -= 1
        return found

    def _quantified(
        self, a: _Value, b: _Value, test: Callable[[_R, _R], exp.Expr], shown: str
    ) -> exp.Expr:
        if isinstance(a, _R):
            assert isinstance(b, _M)
            return self._some(b, lambda y: test(a, y), shown)
        if isinstance(b, _R):
            return self._some(a, lambda x: test(x, b), shown)
        return self._some(a, lambda x: self._some(b, lambda y: test(x, y), shown), shown)

    def _element_test(self, shown: str, container: _Value, element: exp.Expr) -> exp.Expr:
        typed = self.writer.element_value(self.rewritten(element), container.rtype)
        inner = _unwrapped(element)
        # A literal is not NULL, and a cast of its PostgreSQL text may be no value on the engine.
        written = inner.this if isinstance(inner, exp.Cast) else inner
        null = _is_null(element) if _string_literal(written) is None else exp.false()
        if isinstance(container, _R):
            test = _contains_element(container, typed)
        else:
            test = self._some(container, lambda x: _contains_element(x, typed), shown)
        return self._three_valued(test, container, _Nothing(null))

    def _comparison(self, node: exp.Expr, a: _Value, b: _Value) -> exp.Expr:
        cls = type(node)
        if isinstance(a, _R) and isinstance(b, _R):
            if a.literal is not None and b.literal is not None:
                order = pg_range.range_cmp(a.literal, b.literal)
                return _FOLD_ORDER[cls](order)
            sign = _sign(a, b)
            return self._compare_sign(cls, sign, a, b)
        left, right = self._both_multirange(a), self._both_multirange(b)
        if left.literal is not None and right.literal is not None:
            return _FOLD_ORDER[cls](pg_range.mr_cmp(left.literal, right.literal))
        if cls in (exp.EQ, exp.NEQ):
            # Held canonical: equal multiranges have one text.
            return cls(this=left.text.copy(), expression=right.text.copy())
        shown = _COMPARE_SHOWN.get(cls, cls.__name__)
        compared = cls(
            this=self.writer.multirange_sort_key(left, shown),
            expression=self.writer.multirange_sort_key(right, shown),
        )
        null = _or(left.null, right.null)
        return _eq(_case([(null, _null()), (compared, _int(1))], _int(0)), _int(1))

    def _compare_sign(self, cls: type, sign: exp.Expr, a: _R, b: _R) -> exp.Expr:
        null = _or(a.null, b.null)
        if cls is exp.NullSafeEQ:
            return _or(_and(a.null, b.null), _and(_not(a.null), _not(b.null), _eq(sign, _int(0))))
        if cls is exp.NullSafeNEQ:
            same = _or(_and(a.null, b.null), _and(_not(a.null), _not(b.null), _eq(sign, _int(0))))
            return _not(same)
        compared = cls(this=sign, expression=_int(0))
        return _eq(_case([(null, _null()), (compared, _int(1))], _int(0)), _int(1))

    # -- scalars of a value -------------------------------------------------------------------

    def _part(self, name: str, value: _Value, projected: bool, as_text: bool = False) -> exp.Expr:
        w = self.writer
        if isinstance(value, _M):
            if name == "isempty":
                return self._three_valued(w.is_none(value), value)
            first = name.startswith("lower") if name != "isempty" else True
            edge = w.first(value) if first else w.last(value)
            return self._part(name, edge, projected, as_text)
        if name == "isempty":
            return self._three_valued(value.empty, value)
        side = value.lower if name.startswith("lower") else value.upper
        if name.endswith("_inc"):
            return self._three_valued(_and(_not(value.empty), side.inc), value)
        if name.endswith("_inf"):
            return self._three_valued(_and(_not(value.empty), side.inf), value)
        # lower or upper. A numeric keeps its written scale as its text at the root of a result,
        # which pgwire sends as the numeric it is (a numeric of no fixed scale).
        kept = side.value
        if projected and value.rtype.element == "numeric":
            kept = side.text
        elif as_text and value.rtype.element in ("timestamp", "timestamptz"):
            kept = side.text  # as held, in PostgreSQL's form; a dialect's cast writes its own
        return _case([(value.null, _null()), (value.empty, _null()), (side.inf, _null())], kept)

    def _subdiff(self, name: str, a: exp.Expr, b: exp.Expr) -> exp.Expr:
        w = self.writer
        element = RANGE_TYPES[name].element
        if element in ("integer", "bigint", "numeric"):
            kind = exp.DataType.build("DOUBLE")
            return _minus(exp.Cast(this=a, to=kind), exp.Cast(this=b, to=kind.copy()))
        rtype = RANGE_TYPES[name]
        first, second = w.element_value(a, rtype), w.element_value(b, rtype)
        if element == "date":
            days = exp.DateDiff(this=first, expression=second, unit=exp.Var(this="DAY"))
            return exp.Cast(this=days, to=exp.DataType.build("DOUBLE"))
        template = _SECONDS_BETWEEN.get(self.dialect, "(EXTRACT(EPOCH FROM ({a} - {b})))")
        return exp.Cast(this=w.raw(template, a=first, b=second), to=exp.DataType.build("DOUBLE"))

    def _in_range(self, args: list[exp.Expr]) -> exp.Expr:
        """in_range(value, base, offset, subtract, less): a window frame's bound, a comparison."""
        value, base, offset, subtract, less = args
        sum_ = _case([(subtract.copy(), _minus(base, offset))], _plus(base, offset))
        below = exp.LTE(this=value.copy(), expression=sum_.copy())
        above = exp.GTE(this=value.copy(), expression=sum_.copy())
        test = (
            _case([(less.copy(), below)], above)
            if _is_boolean(less)
            else _or(_and(less, below), _and(_not(less), above))
        )
        null = _or(*(_is_null(a) for a in args))
        negative = _and(_not(null), exp.LT(this=offset.copy(), expression=self._zero_like(offset)))
        return _eq(
            _case(
                [
                    (null, _null()),
                    (
                        negative,
                        self.writer.raised(
                            "22013", "invalid preceding or following size in window function"
                        ),
                    ),
                    (test, _int(1)),
                ],
                _int(0),
            ),
            _int(1),
        )

    def _zero_like(self, offset: exp.Expr) -> exp.Expr:
        cast = _unwrapped(offset)
        if isinstance(cast, exp.Cast) and cast.to.this == _T.INTERVAL:
            return exp.Cast(this=_string("0 seconds"), to=cast.to.copy())
        if isinstance(cast, exp.Interval):
            return exp.Interval(this=_string("0"), unit=exp.Var(this="SECOND"))
        return _int(0)

    # -- the walk -----------------------------------------------------------------------------

    def _projected(self, node: exp.Expr) -> bool:
        parent = node.parent
        while isinstance(parent, (exp.Alias, exp.Paren)):
            parent = parent.parent
        return isinstance(parent, exp.Select) and node.arg_key != "where"

    def _read_as_text(self, node: exp.Expr) -> bool:
        parent = node.parent
        return isinstance(parent, exp.Cast) and parent.to.is_type(_T.TEXT, _T.VARCHAR)

    def _as_written(self, node: exp.Expr) -> bool:
        """Whether a bound's value is read as text, or is a result's root: pgwire sends a numeric
        of no fixed scale from its text."""
        return self._projected(node) or self._read_as_text(node)

    def _boolean_value(self, written: exp.Expr, original: exp.Expr) -> exp.Expr:
        if self.dialect in _NO_BOOLEAN_VALUE and self._projected(original):
            return exp.Case(
                ifs=[
                    exp.If(this=written.copy(), true=_int(1)),
                    exp.If(this=_not(written), true=_int(0)),
                ]
            )
        return written

    def visit(self, node: exp.Expr) -> exp.Expr:  # noqa: PLR0911, PLR0912 -- one per form
        if isinstance(node, exp.Dot) and _catalog(node):
            return self.visit(node.expression)
        if isinstance(node, exp.Order):
            return self._order(node)
        if isinstance(node, tuple(_BOOLEAN_NODES)):
            return self._operator(node)
        if isinstance(node, COMPARISONS):
            a, b = self._pair(node.this, node.expression)
            if a is None or b is None:
                return node
            return self._boolean_value(self._comparison(node, a, b), node)
        if isinstance(node, exp.Between):
            return self._between(node)
        if isinstance(node, exp.In):
            return self._in(node)
        if isinstance(node, (exp.Lower, exp.Upper)):
            value = self.value(node.this)
            if value is None:
                return node
            name = "lower" if isinstance(node, exp.Lower) else "upper"
            return self._part(name, value, self._as_written(node), self._read_as_text(node))
        if isinstance(node, (exp.Explode, exp.Unnest)):
            return self._unnest(node)
        if isinstance(node, exp.Anonymous):
            written = self._function(node)
            if written is not None:
                return written
        value = self.value(node)
        return self.writer.text(value) if value is not None else node

    def _operator(self, node: exp.Expr) -> exp.Expr:
        shown, verb = _BOOLEAN_NODES[type(node)]
        left, right = node.this, node.expression
        a, b = self.value(left), self.value(right)
        if a is None and b is None:
            return node
        if verb in ("contains", "contained"):
            if verb == "contained":
                a, b, left, right = b, a, right, left
                verb = "contains"
                shown = "<@"
            if a is None:
                a = self._beside(left, b)
            if a is None:
                return node  # an element @> a range: no such operator
            if b is None:
                b = self._beside(right, a)
            if b is None:
                return self._boolean_value(self._element_test(shown, a, right), node)
        else:
            if a is None:
                a = self._beside(left, b)
            elif b is None:
                b = self._beside(right, a)
            if a is None or b is None:
                return node
        return self._boolean_value(self._boolean(verb, shown, a, b), node)

    def _function(self, node: exp.Anonymous) -> exp.Expr | None:  # noqa: PLR0911, PLR0912
        name = node.name.lower()
        args = list(node.expressions)
        if name in _BOOLEAN_PARTS and len(args) == 1:
            value = self.value(args[0])
            if value is None:
                return None
            return self._boolean_value(self._part(name, value, False), node)
        if name.endswith("_subdiff") and name[: -len("_subdiff")] in RANGE_TYPES:
            return self._subdiff(
                name[: -len("_subdiff")], self.rewritten(args[0]), self.rewritten(args[1])
            )
        if name == "in_range" and len(args) == 5:  # noqa: PLR2004 -- PostgreSQL's five arguments
            return self._boolean_value(self._in_range([self.rewritten(a) for a in args]), node)
        if name in ("range_agg", "range_intersect_agg"):
            return self._aggregate(name, args[0])
        family, operator = _operand_name(name)
        if family is None:
            return None
        if family == "elem":
            container = self.value(args[1])
            if container is None:
                return None
            return self._boolean_value(self._element_test(name, container, args[0]), node)
        if operator in _COMPARE_FUNCTIONS and len(args) == 2:  # noqa: PLR2004
            a, b = self._pair(args[0], args[1])
            if a is None or b is None:
                return None
            cls = _COMPARE_FUNCTIONS[operator]
            return self._boolean_value(self._comparison(cls(), a, b), node)
        if operator == "cmp" and len(args) == 2:  # noqa: PLR2004
            a, b = self._pair(args[0], args[1])
            if isinstance(a, _R) and isinstance(b, _R):
                return _case([(_or(a.null, b.null), _null())], _sign(a, b))
            return None if a is None or b is None else self._multirange_cmp(a, b)
        if operator in _BOOLEAN_FUNCTIONS and len(args) == 2:  # noqa: PLR2004
            verb = _BOOLEAN_FUNCTIONS[operator]
            a = self.value(args[0])
            if a is None:
                return None
            b = self.value(args[1])
            if verb == "contains" and b is None:
                b = self._beside(args[1], a)
                if b is None:
                    return self._boolean_value(self._element_test(name, a, args[1]), node)
            if b is None:
                b = self._beside(args[1], a)
            if b is None:
                return None
            return self._boolean_value(self._boolean(verb, name, a, b), node)
        return None

    def _multirange_cmp(self, a: _Value, b: _Value) -> exp.Expr:
        left, right = self._both_multirange(a), self._both_multirange(b)
        null = _or(left.null, right.null)
        if left.literal is not None and right.literal is not None:
            return _int(pg_range.mr_cmp(left.literal, right.literal))
        if _is_true(null):
            return exp.Cast(this=_null(), to=exp.DataType.build("INT"))
        first = self.writer.multirange_sort_key(left, "multirange_cmp")
        second = self.writer.multirange_sort_key(right, "multirange_cmp")
        return _case(
            [
                (null, _null()),
                (exp.LT(this=first, expression=second), _int(-1)),
                (exp.GT(this=first.copy(), expression=second.copy()), _int(1)),
            ],
            _int(0),
        )

    def _aggregate(self, name: str, argument: exp.Expr) -> exp.Expr:
        value = self.value(argument)
        if value is None:
            return exp.Anonymous(this=name, expressions=[self.rewritten(argument)])
        w, rtype = self.writer, value.rtype
        text = w.text(value)
        joined = w.joined(text)
        if name == "range_agg":
            folded = w.canonical(joined, rtype, name)
        elif isinstance(value, _M):
            folded = w.multirange_intersect_all(joined, rtype, name)
        else:
            folded = w.intersect_all(joined, rtype, name)
        # No value at all is NULL, as a strict aggregate's result is.
        nothing = _eq(exp.Count(this=text.copy()), _int(0))
        return _case([(nothing, _null())], folded)

    def _unnest(self, node: exp.Expr) -> exp.Expr:
        argument = node.this if isinstance(node, exp.Explode) else node.expressions[0]
        value = self.value(argument)
        if not isinstance(value, _M):
            return node
        items: exp.Expr
        if value.literal is not None:
            items = exp.Array(
                expressions=[_string(pg_range.range_text(r)) for r in value.literal.ranges]
            )
        elif self.dialect in _REGEX_LIST:
            items = self.writer._ranges_list(value.text)
        else:
            raise SubtypeOperationRefused(value.rtype.multirange, "unnest", self.dialect)
        if isinstance(node, exp.Unnest):
            node.set("expressions", [items])
            return node
        return exp.Explode(this=items)

    def _order(self, node: exp.Order) -> exp.Expr:
        written: list[exp.Expr] = []
        for ordered in node.expressions:
            value = self.value(ordered.this) if isinstance(ordered, exp.Ordered) else None
            if value is None:
                written.append(ordered)
                continue
            if isinstance(value, _M):
                # NULL by a flag of its own: a dialect's list has no supertype with NULL.
                high = bool(ordered.args.get("desc")) == bool(ordered.args.get("nulls_first"))
                flag = _case([(value.null, _int(1 if high else 0))], _int(0 if high else 1))
                keys = [flag, self.writer.multirange_sort_key(value, "ORDER BY")]
            else:
                keys = self.writer.order_keys(value)
            for key in keys:
                item = ordered.copy()
                item.set("this", key)
                written.append(item)
        node.set("expressions", written)
        return node

    def _between(self, node: exp.Between) -> exp.Expr:
        value, low, high = node.this, node.args["low"], node.args["high"]
        a, b = self._pair(value, low)
        if a is None or b is None:
            return node
        _, c = self._pair(value, high)
        if c is None:
            return node
        at_least = self._comparison(exp.GTE(), a, b)
        at_most = self._comparison(exp.LTE(), a, c)
        return self._boolean_value(_and(at_least, at_most), node)

    def _in(self, node: exp.In) -> exp.Expr:
        items = node.expressions
        value = self.value(node.this)
        if value is None or not items:
            return node
        tests = []
        for item in items:
            a, b = self._pair(node.this, item)
            if a is None or b is None:
                return node
            tests.append(self._comparison(exp.EQ(), a, b))
        return self._boolean_value(_or(*tests), node)


def _is_array(node: exp.Expr) -> bool:
    """An array that is not a VARIADIC argument: the constructor takes ranges, not an array."""
    inner = _unwrapped(node)
    return isinstance(inner, exp.Array) or (
        isinstance(inner, exp.Cast) and inner.to.is_type(_T.ARRAY)
    )


def _variadic_array(node: exp.Expr) -> exp.Expr | None:
    """The array a VARIADIC argument passes; None for any other argument."""
    inner = _unwrapped(node)
    return _unwrapped(inner.this) if isinstance(inner, exp.Variadic) else None


def _is_boolean(node: exp.Expr) -> bool:
    return isinstance(node, exp.Boolean)


def _catalog(node: exp.Dot) -> bool:
    return isinstance(node.this, exp.Identifier) and node.this.name.lower() == "pg_catalog"


_RANGE_TESTS: dict[str, Callable[[_R, _R], exp.Expr]] = {
    "overlap": _overlaps,
    "contains": _contains,
    "before": _before,
    "overleft": _overleft,
    "overright": _overright,
    "adjacent": _adjacent,
}
_FOLD_TESTS: dict[str, Callable[[Range, Range], bool]] = {
    "overlap": pg_range.overlaps,
    "contains": pg_range.contains,
    "before": pg_range.before,
    "overleft": pg_range.overleft,
    "overright": pg_range.overright,
    "adjacent": pg_range.adjacent,
}
_FOLD_ORDER: dict[type, Callable[[int], exp.Expr]] = {
    exp.EQ: lambda c: _bool(c == 0),
    exp.NEQ: lambda c: _bool(c != 0),
    exp.GT: lambda c: _bool(c > 0),
    exp.GTE: lambda c: _bool(c >= 0),
    exp.LT: lambda c: _bool(c < 0),
    exp.LTE: lambda c: _bool(c <= 0),
    exp.NullSafeEQ: lambda c: _bool(c == 0),
    exp.NullSafeNEQ: lambda c: _bool(c != 0),
}
_COMPARE_SHOWN: dict[type, str] = {
    exp.GT: ">",
    exp.GTE: ">=",
    exp.LT: "<",
    exp.LTE: "<=",
}


def rewrite(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree`` (parsed from PostgreSQL) with every range and multirange written for ``dialect``,
    which is not PostgreSQL. A literal PostgreSQL would refuse raises its ``PostgresError``."""
    return tree.transform(_Rewrite(dialect).visit)


__all__ = ["rewrite"]
