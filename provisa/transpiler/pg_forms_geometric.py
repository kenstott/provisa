# Copyright (c) 2026 Kenneth Stott
# Canary: b7d3e1a4-92c6-4f08-8e15-6a0c47d2f931
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's geometric functions as forms (REQ-2167, REQ-2171), ported from geo_ops.c so a form
does the same floating-point steps in the same order and writes float8out's text.

A shape is its text on the engine (provisa.transpiler.pg_held_types). A form reads that text into
a list of doubles, computes, and writes the answer's text, so one definition serves each
:class:`Engine`. A box is read as high x, high y, low x, low y, whatever order its text gave. A
shape holding NaN or an infinity, and an answer that is not finite, is refused by name: PostgreSQL's
float8 checks and its NaN rules are not reproduced. An engine with no entry refuses by name.

Containment of one polygon in another does not recurse as lseg_inside_poly does: each edge of the
inner polygon is split where the outer polygon's boundary meets it, and the midpoint of each piece
must pass point_inside.

Each geo_ops.c helper a form calls more than once is written once, inside a map over its calls, so a
form's size is the sum of its helpers and not their product: line_interpt_line is :func:`_meet`,
lseg_interpt_line :func:`_cut_line`, lseg_interpt_lseg :func:`_cut`, line_closept_point :func:`_foot`,
lseg_closept_line :func:`_near_line`, lseg_closept_point :func:`_near_point`, lseg_closept_lseg
:func:`_near_seg`, box_closept_lseg :func:`_near_box_seg`. A helper that finds a point answers
``[found, x, y]``, one that measures ``[distance, x, y]``, each a list of doubles under names of its
own, so a helper may read another's answer by position.

width_bucket is here because PostgreSQL's catalog groups it here. A numeric's digits are read from
its text into the engine's widest integer, at the scale of the three values, so the bucket is exact;
numeric's division rounds at a scale of its own, so a quotient that close below a whole number is
refused by name, as is a value of more digits than that integer holds."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from provisa.core.pg_error import MARK
from provisa.transpiler.pg_forms_types import REFUSED

_FUZZ = "1e-06"  # geo_decls.h's EPSILON
_NUMBER = r"[-+]?(?:[0-9]+[.]?[0-9]*|[.][0-9]+)(?:[eE][-+]?[0-9]+)?"
_NOT_FINITE = "[a-df-zA-DF-Z]"  # inf, nan: a shape's text has no other letter but e


@dataclass(frozen=True)
class Engine:
    """An engine's spelling of what a form needs: ``{}`` for each operand."""

    name: str
    let: str  # name, value, body
    map: str  # list, name, body
    filter: str  # list, name, body
    upto: str  # 0 .. n - 1
    size: str
    extract: str  # text: the list of its numbers' texts
    double: str
    text: str  # a value's text, '' for a NULL
    matches: str
    join: str  # list of texts, separator
    part: str  # text, one character, which: '' when there is none
    integer: str
    lpad: str
    rpad: str
    ltrim: str
    rtrim: str
    length: str
    finite: str
    infinity: str
    min_of: str
    max_of: str
    sum_of: str
    flatten: str
    concat: str  # two lists
    sort: str
    raises: str  # condition, sqlstate and message's text, answer
    fold: str  # list, the running value's name, its first value, body
    bigint: str
    wide: str  # a text or a number, as the engine's widest integer
    quotient: str  # two of those, the second positive: the first's whole number of the second
    type_name: str  # a value's type, in lower case
    array: str = "[{}]"
    types: tuple[str, str, str, str] = ("VARCHAR", "BOOLEAN", "DOUBLE", "INTEGER")


DUCKDB = Engine(
    name="duckdb",
    let="list_transform([{1}], {0} -> {2})[1]",
    map="list_transform({0}, {1} -> {2})",
    filter="list_filter({0}, {1} -> {2})",
    upto="range(CAST({} AS BIGINT))",
    size="len({})",
    extract="regexp_extract_all({}, '" + _NUMBER + "')",
    double="CAST({} AS DOUBLE)",
    text="CAST({} AS VARCHAR)",
    matches="regexp_matches({}, '{}')",
    join="array_to_string({}, {})",
    part="split_part({}, '{}', {})",
    integer="CAST(replace(coalesce(nullif({}, ''), '0'), '+', '') AS BIGINT)",
    lpad="lpad({}, CAST({} AS INTEGER), '0')",
    rpad="rpad({}, CAST({} AS INTEGER), '0')",
    ltrim="ltrim({}, '0')",
    rtrim="rtrim({}, '0')",
    length="length({})",
    finite="isfinite({})",
    infinity="CAST('Infinity' AS DOUBLE)",
    min_of="list_min({})",
    max_of="list_max({})",
    sum_of="list_sum({})",
    flatten="flatten({})",
    concat="list_concat({}, {})",
    sort="list_sort({})",
    raises=f"CASE WHEN {{0}} THEN error('{MARK} {{1}}') ELSE {{2}} END",
    fold="list_reduce(list_prepend(CAST({2} AS BIGINT), {0}), ({1}, _item) -> {3})",
    bigint="CAST({} AS BIGINT)",
    wide="CAST({} AS HUGEINT)",
    quotient="({} // {})",
    type_name="lower(typeof({}))",
)
TRINO = Engine(
    name="trino",
    let="transform(ARRAY[{1}], {0} -> {2})[1]",
    map="transform({0}, {1} -> {2})",
    filter="filter({0}, {1} -> {2})",
    upto="sequence(0, {} - 1)",
    size="cardinality({})",
    extract="regexp_extract_all({}, '" + _NUMBER + "')",
    double="CAST({} AS DOUBLE)",
    text="CAST({} AS VARCHAR)",
    matches="regexp_like({}, '{}')",
    join="array_join({}, {})",
    part="coalesce(split_part({}, '{}', {}), '')",
    integer="CAST(replace(coalesce(nullif({}, ''), '0'), '+', '') AS BIGINT)",
    lpad="lpad({}, CAST({} AS BIGINT), '0')",
    rpad="rpad({}, CAST({} AS BIGINT), '0')",
    ltrim="ltrim({}, '0')",
    rtrim="rtrim({}, '0')",
    length="length({})",
    finite="is_finite({})",
    infinity="infinity()",
    min_of="array_min({})",
    max_of="array_max({})",
    sum_of="reduce({}, DOUBLE '0', (_s, _x) -> _s + _x, _s -> _s)",
    flatten="flatten({})",
    concat="concat({}, {})",
    sort="array_sort({})",
    raises=f"CASE WHEN {{0}} THEN fail('{MARK} {{1}}') ELSE {{2}} END",
    fold="reduce({0}, CAST({2} AS BIGINT), ({1}, _item) -> {3}, _s -> _s)",
    bigint="CAST({} AS BIGINT)",
    wide="CAST({} AS DECIMAL(38, 0))",
    quotient="(({0} - {0} % {1}) / {1})",
    type_name="lower(typeof({}))",
    array="ARRAY[{}]",
)
CLICKHOUSE = Engine(
    name="clickhouse",
    let="arrayMap({0} -> {2}, [{1}])[1]",
    map="arrayMap({1} -> {2}, {0})",
    filter="arrayFilter({1} -> {2}, {0})",
    upto="arrayMap(_n -> toInt64(_n), range(toUInt64({})))",
    size="toInt64(length({}))",
    extract="extractAll({}, '" + _NUMBER + "')",
    double="toFloat64({})",
    text="ifNull(toString({}), '')",
    matches="match({}, '{}')",
    join="arrayStringConcat({}, {})",
    part="splitByChar('{1}', {0})[{2}]",
    integer="toInt64OrZero(replace({}, '+', ''))",
    lpad="leftPad({}, toUInt64({}), '0')",
    rpad="rightPad({}, toUInt64({}), '0')",
    ltrim="trimLeft({}, '0')",
    rtrim="trimRight({}, '0')",
    length="toInt64(length({}))",
    finite="isFinite({})",
    infinity="toFloat64('inf')",
    min_of="arrayMin({})",
    max_of="arrayMax({})",
    sum_of="arraySum({})",
    flatten="arrayFlatten({})",
    concat="arrayConcat({}, {})",
    sort="arraySort({})",
    # throwIf inside a lambda runs on every row: it is the condition that decides. A tuple carries
    # the answer once: an `if` of two copies doubles a form at each check, and the analyzer's time.
    raises="tupleElement((throwIf(ifNull({0}, 0), '" + MARK + " {1}'), {2}), 2)",
    fold="arrayFold(({1}, _item) -> toInt64({3}), {0}, toInt64({2}))",
    bigint="toInt64({})",
    wide="toInt256OrZero(toString({}))",
    quotient="intDiv({}, {})",
    type_name="lower(toTypeName({}))",
    types=("Nullable(String)", "Nullable(Bool)", "Nullable(Float64)", "Nullable(Int32)"),
)
ENGINES = (DUCKDB, TRINO, CLICKHOUSE)


def _float_text(e: Engine, value: str) -> str:
    """float8out: the engine's shortest digits, written with PostgreSQL's exponent rule."""
    body = (
        "CASE WHEN _d = '' THEN '0' "
        f"WHEN _p - 1 < -4 OR _p - 1 >= 15 THEN substr(_d, 1, 1) || "
        f"CASE WHEN {e.length.format('_d')} > 1 THEN '.' || substr(_d, 2) ELSE '' END || 'e' || "
        "CASE WHEN _p - 1 < 0 THEN '-' ELSE '+' END || "
        f"{e.lpad.format(e.text.format('abs(_p - 1)'), 2)} "
        f"WHEN _p <= 0 THEN '0.' || {e.lpad.format(chr(39) * 2, '-_p')} || _d "
        f"WHEN {e.length.format('_d')} <= _p THEN {e.rpad.format('_d', '_p')} "
        "ELSE substr(_d, 1, _p) || '.' || substr(_d, _p + 1) END"
    )
    whole, fraction = e.part.format("_m", ".", 1), e.part.format("_m", ".", 2)
    digits = e.ltrim.format(f"{whole} || {fraction}")
    place = (
        f"{e.length.format(whole)} + {e.integer.format(e.part.format('_u', 'e', 2))} "
        f"- ({e.length.format(whole)} + {e.length.format(fraction)} - {e.length.format('_l')})"
    )
    named = e.let.format("_d", e.rtrim.format("_l"), e.let.format("_p", place, body))
    unsigned = e.let.format(
        "_u",
        "CASE WHEN substr(_t, 1, 1) = '-' THEN substr(_t, 2) ELSE _t END",
        e.let.format("_m", e.part.format("_u", "e", 1), e.let.format("_l", digits, named)),
    )
    # The first character, so -0 keeps its sign; an exponent's sign is not it.
    sign = "CASE WHEN substr(_t, 1, 1) = '-' THEN '-' ELSE '' END"
    return e.let.format("_t", f"lower({e.text.format(value)})", f"{sign} || {unsigned}")


def _list(e: Engine, *items: str) -> str:
    return e.array.format(", ".join(items))


def _fp_eq(a: str, b: str) -> str:
    return f"({a} = {b} OR abs({a} - {b}) <= {_FUZZ})"


def _fp_lt(a: str, b: str) -> str:
    return f"({a} + {_FUZZ} < {b})"


def _fp_le(a: str, b: str) -> str:
    return f"({a} <= {b} + {_FUZZ})"


def _fp_gt(a: str, b: str) -> str:
    return f"({a} > {b} + {_FUZZ})"


def _fp_ge(a: str, b: str) -> str:
    return f"({a} + {_FUZZ} >= {b})"


_FUZZY = {"eq": _fp_eq, "lt": _fp_lt, "le": _fp_le, "gt": _fp_gt, "ge": _fp_ge}


def _hypot(e: Engine, dx: str, dy: str) -> str:
    """pg_hypot, over finite values."""
    larger, smaller = f"greatest(abs({dx}), abs({dy}))", f"least(abs({dx}), abs({dy}))"
    return (
        f"CASE WHEN {smaller} = 0 THEN {larger} ELSE {larger} * "
        f"sqrt(1.0 + ({smaller} / {larger}) * ({smaller} / {larger})) END"
    )


def _dist(e: Engine, a: tuple[str, str], b: tuple[str, str]) -> str:
    return _hypot(e, f"{a[0]} - {b[0]}", f"{a[1]} - {b[1]}")


def _xy(name: str, at: int = 1) -> tuple[str, str]:
    return f"{name}[{at}]", f"{name}[{at + 1}]"


def _bounds(e: Engine, name: str) -> str:
    """The box of a list of points: high x, high y, low x, low y."""
    pairs = e.upto.format(_half(e, e.size.format(name)))
    xs = e.map.format(pairs, "_i", f"{name}[2 * _i + 1]")
    ys = e.map.format(pairs, "_i", f"{name}[2 * _i + 2]")
    return _list(
        e, e.max_of.format(xs), e.max_of.format(ys), e.min_of.format(xs), e.min_of.format(ys)
    )


def _half(e: Engine, count: str) -> str:
    return {"duckdb": f"({count} // 2)", "trino": f"({count} / 2)"}.get(
        e.name, f"intDiv({count}, 2)"
    )


def _read(e: Engine, param: str, kind: str) -> str:
    numbers = e.extract.format(e.text.format(param))
    doubles = e.map.format(numbers, "_n", e.double.format("_n"))
    if kind == "line":
        return _line_read(e, doubles)
    if kind != "box":
        return doubles
    high = ("greatest(_q[1], _q[3])", "greatest(_q[2], _q[4])")
    low = ("least(_q[1], _q[3])", "least(_q[2], _q[4])")
    return e.let.format("_q", doubles, _list(e, *high, *low))


def _is_open(e: Engine, param: str) -> str:
    return f"substr(ltrim({e.text.format(param)}), 1, 1) = '['"


def _checked(e: Engine, values: str, answer: str, function: str) -> str:
    """``answer`` of ``_r``, the doubles ``values``; refused by name if one is not finite."""
    finite = e.filter.format("_r", "_x", f"NOT {e.finite.format('_x')}")
    bad = f"{e.size.format(finite)} > 0"
    return e.let.format("_r", values, e.raises.format(bad, f"0A000: {function} {REFUSED}", answer))


def _points(e: Engine, name: str) -> str:
    """The points of a list of doubles as ``(x,y)`` texts."""
    pairs = e.upto.format(_half(e, e.size.format(name)))
    x, y = _float_text(e, f"{name}[2 * _i + 1]"), _float_text(e, f"{name}[2 * _i + 2]")
    return e.map.format(pairs, "_i", f"'(' || {x} || ',' || {y} || ')'")


def _shape(e: Engine, values: str, before: str, after: str, function: str) -> str:
    joined = e.join.format(_points(e, "_r"), "','")
    return _checked(e, values, f"'{before}' || {joined} || '{after}'", function)


def _double(e: Engine, value: str, function: str) -> str:
    return _checked(e, _list(e, value), "_r[1]", function)


def _division(e: Engine, divisor: str, answer: str) -> str:
    return e.raises.format(f"{divisor} = 0", "22012: division by zero", answer)


def _box_of(e: Engine, a: tuple[str, str], b: tuple[str, str]) -> str:
    """box_construct: the corners of two points."""
    return _list(
        e,
        f"greatest({a[0]}, {b[0]})",
        f"greatest({a[1]}, {b[1]})",
        f"least({a[0]}, {b[0]})",
        f"least({a[1]}, {b[1]})",
    )


def _center(a: str) -> tuple[str, str]:
    return f"({a}[1] + {a}[3]) / 2.0", f"({a}[2] + {a}[4]) / 2.0"


def _mul(p: tuple[str, str], q: tuple[str, str]) -> tuple[str, str]:
    return f"{p[0]} * {q[0]} - {p[1]} * {q[1]}", f"{p[0]} * {q[1]} + {p[1]} * {q[0]}"


def _div(p: tuple[str, str], q: tuple[str, str]) -> tuple[str, str]:
    div = f"({q[0]} * {q[0]} + {q[1]} * {q[1]})"
    return (
        f"({p[0]} * {q[0]} + {p[1]} * {q[1]}) / {div}",
        f"({p[1]} * {q[0]} - {p[0]} * {q[1]}) / {div}",
    )


_ADD = lambda p, q: (f"{p[0]} + {q[0]}", f"{p[1]} + {q[1]}")  # noqa: E731
_SUB = lambda p, q: (f"{p[0]} - {q[0]}", f"{p[1]} - {q[1]}")  # noqa: E731
_MOVES: dict[str, tuple[Callable, bool]] = {
    "add": (_ADD, False),
    "sub": (_SUB, False),
    "mul": (_mul, False),
    "div": (_div, True),
}
_POINT = ("", "")
_BOX = ("", "")
_CLOSED = ("(", ")")
_LSEG = ("[", "]")


@dataclass(frozen=True)
class Row:
    """One overload: its row's name, its PostgreSQL function, its parameters' kinds, what it
    answers (a type of :attr:`Engine.types`, 0 text, 1 boolean, 2 double, 3 integer)."""

    name: str
    params: str
    answers: int
    body: Callable[[Engine, str], str]


def _sum_by(e: Engine, name: str, value: str) -> str:
    return e.sum_of.format(e.map.format(e.upto.format(_half(e, e.size.format(name))), "_i", value))


def _poly_center(e: Engine, name: str) -> tuple[str, str]:
    n = _half(e, e.size.format(name))
    return (
        f"{_sum_by(e, name, f'{name}[2 * _i + 1]')} / {n}",
        f"{_sum_by(e, name, f'{name}[2 * _i + 2]')} / {n}",
    )


def _circle_text(e: Engine, values: str, f: str) -> str:
    x, y, r = (_float_text(e, f"_r[{i}]") for i in (1, 2, 3))
    return _checked(e, values, f"'<(' || {x} || ',' || {y} || '),' || {r} || '>'", f)


def _line_text(e: Engine, values: str, f: str) -> str:
    a, b, c = (_float_text(e, f"_r[{i}]") for i in (1, 2, 3))
    return _checked(e, values, f"'{{' || {a} || ',' || {b} || ',' || {c} || '}}'", f)


def _line_through(e: Engine, f: str) -> str:
    """line_construct_pp of the points ``_a`` and ``_b``."""
    p, q = _xy("_a"), _xy("_b")
    same = f"{_fp_eq(p[0], q[0])} AND {_fp_eq(p[1], q[1])}"
    c = f"{p[1]} - _m * {p[0]}"
    sloped = e.let.format(
        "_m",
        f"({p[1]} - {q[1]}) / ({p[0]} - {q[0]})",
        f"CASE WHEN _m = 0 THEN {_list(e, '0.0', '-1.0', p[1])} "
        f"ELSE {_list(e, '_m', '-1.0', f'CASE WHEN {c} = 0 THEN 0.0 ELSE {c} END')} END",
    )
    line = (
        f"CASE WHEN {_fp_eq(p[0], q[0])} THEN {_list(e, '-1.0', '0.0', p[0])} "
        f"WHEN {_fp_eq(p[1], q[1])} THEN {_list(e, '0.0', '-1.0', p[1])} ELSE {sloped} END"
    )
    return e.raises.format(
        same,
        "22023: invalid line specification: must be two distinct points",
        _line_text(e, line, f),
    )


def _slope(e: Engine, a: tuple[str, str], b: tuple[str, str], f: str) -> str:
    """point_sl: a vertical line's slope is infinite, and so is that of a point and itself."""
    finite = (
        f"CASE WHEN {_fp_eq(a[1], b[1])} THEN 0.0 ELSE ({a[1]} - {b[1]}) / ({a[0]} - {b[0]}) END"
    )
    return f"CASE WHEN {_fp_eq(a[0], b[0])} THEN {e.infinity} ELSE {_double(e, finite, f)} END"


def _box_contains(outer: str, inner: str) -> str:
    """box_contain_box, each a list of high x, high y, low x, low y."""
    return (
        f"{_fp_ge(f'{outer}[1]', f'{inner}[1]')} AND {_fp_le(f'{outer}[3]', f'{inner}[3]')} "
        f"AND {_fp_ge(f'{outer}[2]', f'{inner}[2]')} AND {_fp_le(f'{outer}[4]', f'{inner}[4]')}"
    )


_ON_POLYGON = "99"  # lseg_crossing's POINT_ON_POLYGON


def _crossing(e: Engine, c: str) -> str:
    """lseg_crossing of ``c``: x, y, previous x, previous y."""
    x, y, prev_x, prev_y = (f"{c}[{i}]" for i in (1, 2, 3, 4))
    zero = lambda a: f"abs({a}) <= {_FUZZ}"  # noqa: E731
    on_x_axis = (
        f"CASE WHEN {zero(x)} THEN {_ON_POLYGON} "
        f"WHEN {_fp_gt(x, '0.0')} THEN "
        f"CASE WHEN {zero(prev_y)} THEN "
        f"CASE WHEN {_fp_gt(prev_x, '0.0')} THEN 0 ELSE {_ON_POLYGON} END "
        f"WHEN {_fp_lt(prev_y, '0.0')} THEN 1 ELSE -1 END "
        f"WHEN {zero(prev_y)} THEN CASE WHEN {_fp_lt(prev_x, '0.0')} THEN 0 ELSE {_ON_POLYGON} END "
        "ELSE 0 END"
    )
    z = f"({x} - {prev_x}) * {y} - ({y} - {prev_y}) * {x}"
    by_z = e.let.format(
        "_z",
        z,
        f"CASE WHEN {zero('_z')} THEN {_ON_POLYGON} "
        f"WHEN (_g < 0 AND {_fp_lt('_z', '0.0')}) OR (_g > 0 AND {_fp_gt('_z', '0.0')}) THEN 0 "
        "ELSE 2 * _g END",
    )
    off_x_axis = e.let.format(
        "_g",
        f"CASE WHEN {_fp_gt(y, '0.0')} THEN 1 ELSE -1 END",
        f"CASE WHEN {zero(prev_y)} THEN CASE WHEN {_fp_lt(prev_x, '0.0')} THEN 0 ELSE _g END "
        f"WHEN (_g < 0 AND {_fp_lt(prev_y, '0.0')}) OR (_g > 0 AND {_fp_gt(prev_y, '0.0')}) THEN 0 "
        f"WHEN {_fp_ge(x, '0.0')} AND {_fp_gt(prev_x, '0.0')} THEN 2 * _g "
        f"WHEN {_fp_lt(x, '0.0')} AND {_fp_le(prev_x, '0.0')} THEN 0 "
        f"ELSE {by_z} END",
    )
    return f"CASE WHEN {zero(y)} THEN {on_x_axis} ELSE {off_x_axis} END"


def _vertex(name: str, at: str) -> tuple[str, str]:
    return f"{name}[2 * ({at}) + 1]", f"{name}[2 * ({at}) + 2]"


def _before(e: Engine, name: str, at: str) -> str:
    """The vertex before ``at``, the last one before the first."""
    count = _half(e, e.size.format(name))
    return f"({at} + {count} - 1) % {count}"


def _inside(e: Engine, poly: str, x: str, y: str) -> str:
    """point_inside, not zero: on the boundary is inside."""
    near = _list(
        e,
        f"{_vertex(poly, '_v')[0]} - {x}",
        f"{_vertex(poly, '_v')[1]} - {y}",
        f"{_vertex(poly, _before(e, poly, '_v'))[0]} - {x}",
        f"{_vertex(poly, _before(e, poly, '_v'))[1]} - {y}",
    )
    crossings = e.map.format(
        e.upto.format(_half(e, e.size.format(poly))),
        "_v",
        e.let.format("_c", near, _crossing(e, "_c")),
    )
    on_it = e.filter.format("_k", "_o", f"_o = {_ON_POLYGON}")
    return e.let.format(
        "_k", crossings, f"({e.size.format(on_it)} > 0 OR {e.sum_of.format('_k')} <> 0)"
    )


def _splits(e: Engine, poly: str) -> str:
    """Where along the edge ``_d`` (_p[1], _p[2] to _q[1], _q[2]) the boundary of ``poly`` meets it,
    as fractions of the edge: 0, 1, each crossing, and each vertex on it, in order."""
    u, v = ("_s[1]", "_s[2]"), ("_s[3]", "_s[4]")
    sx, sy = f"({v[0]} - {u[0]})", f"({v[1]} - {u[1]})"
    ox, oy = f"({u[0]} - _p[1])", f"({u[1]} - _p[2])"
    denominator = f"(_d[1] * {sy} - _d[2] * {sx})"
    along = f"({ox} * {sy} - {oy} * {sx}) / {denominator}"
    across = f"({ox} * _d[2] - {oy} * _d[1]) / {denominator}"
    crossing = (
        f"CASE WHEN _d[3] = 0 OR {denominator} = 0 THEN -1.0 "
        f"WHEN {along} >= 0 AND {along} <= 1 AND {across} >= 0 AND {across} <= 1 THEN {along} "
        "ELSE -1.0 END"
    )
    point = (u[0], u[1])
    start, end = ("_p[1]", "_p[2]"), ("_q[1]", "_q[2]")
    on_edge = _fp_eq(f"({_dist(e, point, start)} + {_dist(e, point, end)})", _dist(e, start, end))
    at_vertex = (
        f"CASE WHEN _d[3] = 0 THEN -1.0 "
        f"WHEN {on_edge} THEN ({ox} * _d[1] + {oy} * _d[2]) / _d[3] ELSE -1.0 END"
    )
    edge = _list(
        e,
        _vertex(poly, _before(e, poly, "_v"))[0],
        _vertex(poly, _before(e, poly, "_v"))[1],
        _vertex(poly, "_v")[0],
        _vertex(poly, "_v")[1],
    )
    met = e.map.format(
        e.upto.format(_half(e, e.size.format(poly))),
        "_v",
        e.let.format("_s", edge, _list(e, crossing, at_vertex)),
    )
    within = e.filter.format(e.flatten.format(met), "_o", "_o > 0 AND _o < 1")
    ends = _list(e, e.double.format("0"), e.double.format("1"))
    return e.sort.format(e.concat.format(ends, within))


def _edge_inside(e: Engine, outer: str, inner: str) -> str:
    """The edge of ``inner`` ending at vertex ``_t``: each piece's midpoint is inside ``outer``."""
    pieces = e.upto.format(f"{e.size.format('_u')} - 1")
    mid = e.let.format(
        "_f",
        "(_u[_w + 1] + _u[_w + 2]) / 2.0",
        _inside(e, outer, "(_p[1] + _d[1] * _f)", "(_p[2] + _d[2] * _f)"),
    )
    outside = e.filter.format(pieces, "_w", f"NOT {mid}")
    test = e.let.format("_u", _splits(e, outer), f"{e.size.format(outside)} = 0")
    delta = _list(
        e,
        "_q[1] - _p[1]",
        "_q[2] - _p[2]",
        "(_q[1] - _p[1]) * (_q[1] - _p[1]) + (_q[2] - _p[2]) * (_q[2] - _p[2])",
    )
    return e.let.format(
        "_p",
        _list(e, *_vertex(inner, _before(e, inner, "_t"))),
        e.let.format("_q", _list(e, *_vertex(inner, "_t")), e.let.format("_d", delta, test)),
    )


def _contains(e: Engine, outer: str, inner: str) -> str:
    """poly_contain_poly: the bounding boxes, then each edge of ``inner``."""
    edges = e.upto.format(_half(e, e.size.format(inner)))
    out = e.filter.format(edges, "_t", f"NOT {_edge_inside(e, outer, inner)}")
    boxes = e.let.format(
        "_h",
        _bounds(e, outer),
        e.let.format("_l", _bounds(e, inner), _box_contains("_h", "_l")),
    )
    return f"({boxes} AND {e.size.format(out)} = 0)"


_ONE, _NIL, _MINUS = "1e0", "0e0", "-1e0"


def _fp_zero(a: str) -> str:
    return f"(abs({a}) <= {_FUZZ})"


def _not_found(e: Engine) -> str:
    return _list(e, _NIL, _NIL, _NIL)


def _at(e: Engine, values: str, index: int) -> str:
    """An element of a list that is not a name: no binding, for a form's depth is sqlglot's stack."""
    return f"({values})[{index}]"


def _any(e: Engine, flags: str) -> str:
    """Whether a helper answered ``found`` for any of a list of its calls."""
    return f"({e.size.format(e.filter.format(flags, '_o', '_o = 1'))} > 0)"


def _sign_free(name: str) -> str:
    """``if (x == 0.0) x = 0.0``: -0 is not written."""
    return f"CASE WHEN {name} = 0 THEN {_NIL} ELSE {name} END"


def _line_of(e: Engine, x: str, y: str, slope: str) -> str:
    """line_construct: the line through a point at a slope, infinite for a vertical line."""
    fit = f"CASE WHEN {y} - _lm * {x} = 0 THEN {_NIL} ELSE {y} - _lm * {x} END"
    return e.let.format(
        "_lm",
        slope,
        f"CASE WHEN _lm = {e.infinity} THEN {_list(e, _MINUS, _NIL, x)} "
        f"WHEN _lm = 0 THEN {_list(e, _NIL, _MINUS, y)} ELSE {_list(e, '_lm', _MINUS, fit)} END",
    )


def _seg_slope(e: Engine, s: str) -> str:
    """point_sl of a segment's ends."""
    return (
        f"CASE WHEN {_fp_eq(f'{s}[1]', f'{s}[3]')} THEN {e.infinity} "
        f"WHEN {_fp_eq(f'{s}[2]', f'{s}[4]')} THEN {_NIL} "
        f"ELSE ({s}[2] - {s}[4]) / ({s}[1] - {s}[3]) END"
    )


def _seg_invslope(e: Engine, s: str) -> str:
    """point_invsl of a segment's ends."""
    return (
        f"CASE WHEN {_fp_eq(f'{s}[1]', f'{s}[3]')} THEN {_NIL} "
        f"WHEN {_fp_eq(f'{s}[2]', f'{s}[4]')} THEN {e.infinity} "
        f"ELSE ({s}[1] - {s}[3]) / ({s}[4] - {s}[2]) END"
    )


def _line_slope(e: Engine, line: str) -> str:
    return (
        f"CASE WHEN {_fp_zero(f'{line}[1]')} THEN {_NIL} "
        f"WHEN {_fp_zero(f'{line}[2]')} THEN {e.infinity} "
        f"ELSE {line}[1] / (-{line}[2]) END"
    )


def _line_invslope(e: Engine, line: str) -> str:
    return (
        f"CASE WHEN {_fp_zero(f'{line}[1]')} THEN {e.infinity} "
        f"WHEN {_fp_zero(f'{line}[2]')} THEN {_NIL} "
        f"ELSE {line}[2] / {line}[1] END"
    )


def _line_read(e: Engine, doubles: str) -> str:
    """line_in: ``{A,B,C}``, or the two points of an lseg's text."""
    points = _list(e, "_q[1]", "_q[2]", "_q[3]", "_q[4]")
    through = e.let.format("_qs", points, _line_of(e, "_qs[1]", "_qs[2]", _seg_slope(e, "_qs")))
    same = f"{_fp_eq('_q[1]', '_q[3]')} AND {_fp_eq('_q[2]', '_q[4]')}"
    as_is = e.raises.format(
        f"{e.size.format('_q')} = 3 AND {_fp_zero('_q[1]')} AND {_fp_zero('_q[2]')}",
        "22P02: invalid line specification: A and B cannot both be zero",
        "_q",
    )
    pair = e.raises.format(
        f"{e.size.format('_q')} = 4 AND {same}",
        "22P02: invalid line specification: must be two distinct points",
        through,
    )
    return e.let.format(
        "_q", doubles, f"CASE WHEN {e.size.format('_q')} = 4 THEN {pair} ELSE {as_is} END"
    )


def _meet(e: Engine, one: str, other: str) -> str:
    """line_interpt_line: ``[found, x, y]``; lines that are one line do not meet."""

    def solved(first: str, second: str) -> str:
        a, b, c = (f"{first}[{i}]" for i in (1, 2, 3))
        d, f, g = (f"{second}[{i}]" for i in (1, 2, 3))
        y = f"(-({a} * _mx + {c})) / {b}"
        point = _list(e, _ONE, _sign_free("_mx"), f"CASE WHEN {y} = 0 THEN {_NIL} ELSE {y} END")
        return (
            f"CASE WHEN {_fp_eq(d, f'({a} * ({f} / {b}))')} THEN {_not_found(e)} ELSE "
            + e.let.format("_mx", f"({b} * {g} - {f} * {c}) / ({a} * {f} - {d} * {b})", point)
            + " END"
        )

    return (
        f"CASE WHEN NOT {_fp_zero(f'{one}[2]')} THEN {solved(one, other)} "
        f"WHEN NOT {_fp_zero(f'{other}[2]')} THEN {solved(other, one)} "
        f"ELSE {_not_found(e)} END"
    )


def _on_seg(e: Engine, s: str, x: str, y: str) -> str:
    """lseg_contain_point."""
    ends = ((f"{s}[1]", f"{s}[2]"), (f"{s}[3]", f"{s}[4]"))
    at = (x, y)
    return _fp_eq(f"({_dist(e, at, ends[0])} + {_dist(e, at, ends[1])})", _dist(e, *ends))


def _same_point(a: tuple[str, str], b: tuple[str, str]) -> str:
    return f"({_fp_eq(a[0], b[0])} AND {_fp_eq(a[1], b[1])})"


def _cut_line(e: Engine, s: str, line: str) -> str:
    """lseg_interpt_line: ``[found, x, y]``, an end of the segment where it is one."""
    found = ("_cm[2]", "_cm[3]")
    answer = (
        f"CASE WHEN _cm[1] = 0 OR NOT {_on_seg(e, s, *found)} THEN {_not_found(e)} "
        f"WHEN {_same_point(_xy(s), found)} THEN {_list(e, _ONE, *_xy(s))} "
        f"WHEN {_same_point(_xy(s, 3), found)} THEN {_list(e, _ONE, *_xy(s, 3))} "
        "ELSE _cm END"
    )
    return e.let.format(
        "_ct",
        _line_of(e, f"{s}[1]", f"{s}[2]", _seg_slope(e, s)),
        e.let.format("_cm", _meet(e, "_ct", line), answer),
    )


def _cut(e: Engine, s: str, t: str) -> str:
    """lseg_interpt_lseg: ``[found, x, y]``."""
    return e.let.format(
        "_st",
        _line_of(e, f"{t}[1]", f"{t}[2]", _seg_slope(e, t)),
        e.let.format(
            "_sm",
            _cut_line(e, s, "_st"),
            f"CASE WHEN _sm[1] = 0 OR NOT {_on_seg(e, t, '_sm[2]', '_sm[3]')} "
            f"THEN {_not_found(e)} ELSE _sm END",
        ),
    )


def _foot(e: Engine, line: str, x: str, y: str) -> str:
    """line_closept_point: ``[distance, x, y]``. Two lines drawn perpendicular meet unless a
    coordinate was not finite, which no form reads: the infinite distance is then refused."""
    measured = _dist(e, ("_fm[2]", "_fm[3]"), (x, y))
    return e.let.format(
        "_ft",
        _line_of(e, x, y, _line_invslope(e, line)),
        e.let.format(
            "_fm",
            _meet(e, "_ft", line),
            f"CASE WHEN _fm[1] = 0 THEN {_list(e, e.infinity, x, y)} "
            f"ELSE {_list(e, measured, '_fm[2]', '_fm[3]')} END",
        ),
    )


def _near_line(e: Engine, s: str, line: str) -> str:
    """lseg_closept_line: ``[distance, x, y]`` of the point of the segment nearest the line."""
    ends = _list(e, _list(e, *_xy(s)), _list(e, *_xy(s, 3)))
    each = e.map.format(ends, "_np", _at(e, _foot(e, line, "_np[1]", "_np[2]"), 1))
    nearer = (
        f"CASE WHEN _nd[1] < _nd[2] THEN {_list(e, '_nd[1]', *_xy(s))} "
        f"ELSE {_list(e, '_nd[2]', *_xy(s, 3))} END"
    )
    return e.let.format(
        "_nm",
        _cut_line(e, s, line),
        f"CASE WHEN _nm[1] = 1 THEN {_list(e, _NIL, '_nm[2]', '_nm[3]')} "
        f"ELSE {e.let.format('_nd', each, nearer)} END",
    )


def _near_point(e: Engine, s: str, x: str, y: str) -> str:
    """lseg_closept_point: ``[distance, x, y]`` of the point of the segment nearest a point."""
    measured = _dist(e, ("_pc[2]", "_pc[3]"), (x, y))
    return e.let.format(
        "_pt",
        _line_of(e, x, y, _seg_invslope(e, s)),
        e.let.format("_pc", _near_line(e, s, "_pt"), _list(e, measured, "_pc[2]", "_pc[3]")),
    )


def _least(e: Engine, candidates: str) -> str:
    """The first of ``[distance, x, y]``s with the smallest distance, as float8_lt keeps it."""
    smallest = e.min_of.format(e.map.format(candidates, "_it", "_it[1]"))
    return e.let.format(
        "_lc0", smallest, e.filter.format(candidates, "_it", "_it[1] = _lc0") + "[1]"
    )


def _near_seg(e: Engine, on: str, to: str) -> str:
    """lseg_closept_lseg: ``[distance, x, y]`` of the point of ``on`` nearest ``to``."""
    pair = lambda seg, point: _list(e, seg, _list(e, *point))  # noqa: E731
    calls = _list(
        e,
        pair(on, _xy(to)),
        pair(on, _xy(to, 3)),
        pair(to, _xy(on)),
        pair(to, _xy(on, 3)),
    )
    each = e.map.format(calls, "_sw", _near_point(e, "_sw[1]", "_sw[2][1]", "_sw[2][2]"))
    candidates = _list(
        e, "_sr[1]", "_sr[2]", _list(e, "_sr[3][1]", *_xy(on)), _list(e, "_sr[4][1]", *_xy(on, 3))
    )
    return e.let.format(
        "_sq",
        _cut(e, on, to),
        f"CASE WHEN _sq[1] = 1 THEN {_list(e, _NIL, '_sq[2]', '_sq[3]')} ELSE "
        + e.let.format("_sr", each, _least(e, candidates))
        + " END",
    )


def _in_box(box: str, x: str, y: str) -> str:
    """box_contain_point: no fuzz."""
    return f"({box}[1] >= {x} AND {box}[3] <= {x} AND {box}[2] >= {y} AND {box}[4] <= {y})"


def _sides(e: Engine, box: str) -> str:
    """The sides of a box as box_closept_point draws them."""
    high, low = _xy(box), _xy(box, 3)
    upper, lower = (low[0], high[1]), (high[0], low[1])
    return _list(
        e,
        _list(e, *low, *upper),
        _list(e, *high, *upper),
        _list(e, *low, *lower),
        _list(e, *high, *lower),
    )


def _near_box_point(e: Engine, box: str, x: str, y: str) -> str:
    """box_closept_point: ``[distance, x, y]``."""
    each = e.map.format(_sides(e, box), "_bs", _near_point(e, "_bs", x, y))
    return (
        f"CASE WHEN {_in_box(box, x, y)} THEN {_list(e, _NIL, x, y)} "
        f"ELSE {e.let.format('_bq', each, _least(e, '_bq'))} END"
    )


def _boxes_meet(a: str, b: str) -> str:
    """box_ov, each a list of high x, high y, low x, low y."""
    return (
        f"({_fp_le(f'{a}[3]', f'{b}[1]')} AND {_fp_le(f'{b}[3]', f'{a}[1]')} "
        f"AND {_fp_le(f'{a}[4]', f'{b}[2]')} AND {_fp_le(f'{b}[4]', f'{a}[2]')})"
    )


def _box_cut(e: Engine, box: str, s: str) -> str:
    """box_interpt_lseg, whether: the segment's box, its ends, then the sides."""
    crossed = e.map.format(_sides(e, box), "_bs", _at(e, _cut(e, "_bs", s), 1))
    ends = f"{_in_box(box, *_xy(s))} OR {_in_box(box, *_xy(s, 3))}"
    return e.let.format(
        "_lb",
        _bounds(e, s),
        f"({_boxes_meet('_lb', box)} AND ({ends} OR {_any(e, crossed)}))",
    )


def _near_box_seg(e: Engine, box: str, s: str, point: bool = True) -> str:
    """box_closept_lseg: ``[distance, x, y]`` of the point nearest the box. Where they cross that is
    the segment's point nearest the box's center, at no distance; a form of the distance alone
    leaves that out, for an engine's planning time grows with the square of a form's size."""
    middle = e.let.format(
        "_mc",
        _list(e, *_center(box)),
        e.let.format(
            "_mp", _near_point(e, s, "_mc[1]", "_mc[2]"), _list(e, _NIL, "_mp[2]", "_mp[3]")
        ),
    )
    each = e.map.format(_sides(e, box), "_bs", _near_seg(e, "_bs", s))
    crossing = middle if point else _not_found(e)
    return (
        f"CASE WHEN {_box_cut(e, box, s)} THEN {crossing} "
        f"ELSE {e.let.format('_bq', each, _least(e, '_bq'))} END"
    )


def _segments(e: Engine, name: str, closed: str) -> str:
    """A path's or polygon's segments, each previous x, y, x, y, index, in geo_ops.c's order: the
    closing segment first, where there is one."""
    each = _list(
        e,
        *_vertex(name, _before(e, name, "_i")),
        *_vertex(name, "_i"),
        e.double.format("_i"),
    )
    every = e.map.format(e.upto.format(_half(e, e.size.format(name))), "_i", each)
    return e.filter.format(every, "_g", f"_g[5] > 0 OR {closed}")


def _is_closed(e: Engine, param: str) -> str:
    return f"(NOT ({_is_open(e, param)}))"


def _whole(e: Engine, a: str, b: str) -> str:
    return {"duckdb": f"({a} // {b})", "trino": f"({a} / {b})"}.get(e.name, f"intDiv({a}, {b})")


def _pairs(e: Engine, one: str, other: str, body: Callable[[str, str], str]) -> str:
    """``body`` of each segment of ``one`` with each of ``other``: a single list, so a form may
    take its least; arrayFlatten would flatten the segments too. Neither may be empty."""
    width = e.size.format(other)
    size = f"{e.size.format(one)} * {width}"
    pair = _list(e, f"{one}[{_whole(e, '_pk', width)} + 1]", f"{other}[_pk % {width} + 1]")
    return e.map.format(
        e.upto.format(size), "_pk", e.let.format("_pw", pair, body("_pw[1]", "_pw[2]"))
    )


def _both(e: Engine, one: tuple[str, str], other: tuple[str, str], body: Callable) -> str:
    """``body`` of the segments of two shapes, each its list of doubles and whether it closes."""
    segments = _list(e, _segments(e, *one), _segments(e, *other))
    return e.let.format("_es", segments, body("_es[1]", "_es[2]"))


def _some_pair(e: Engine, flags: str, one: str, other: str) -> str:
    return (
        f"CASE WHEN {e.size.format(one)} * {e.size.format(other)} = 0 THEN 1 = 0 "
        f"ELSE {_any(e, flags)} END"
    )


def _least_pair(e: Engine, one: str, other: str, f: str) -> str:
    """path_distance over two lists of segments: NULL when one has none."""
    each = _pairs(e, one, other, lambda s, t: _at(e, _near_seg(e, s, t), 1))
    return (
        f"CASE WHEN {e.size.format(one)} * {e.size.format(other)} = 0 THEN NULL "
        f"ELSE {_double(e, e.min_of.format(each), f)} END"
    )


def _from_polygon(e: Engine, poly: str, x: str, y: str) -> str:
    """dist_ppoly_internal: inside is no distance; its segments are not path's, the first runs from
    the first point to the last."""
    n = _half(e, e.size.format(poly))
    last = _vertex(poly, f"{n} - 1")
    seg = (
        f"CASE WHEN _i = 0 THEN {_list(e, *_vertex(poly, '0'), *last)} "
        f"ELSE {_list(e, *_vertex(poly, '_i - 1'), *_vertex(poly, '_i'))} END"
    )
    segments = e.map.format(e.upto.format(n), "_i", seg)
    each = e.map.format(segments, "_ws", _at(e, _near_point(e, "_ws", x, y), 1))
    return f"CASE WHEN {_inside(e, poly, x, y)} THEN {_NIL} ELSE {e.min_of.format(each)} END"


def _overlap(e: Engine, a: str, b: str) -> str:
    """poly_overlap_internal: boxes, then edges, then a vertex of each in the other."""
    crossed = _both(
        e,
        (a, "1 = 1"),
        (b, "1 = 1"),
        lambda p, q: _any(e, _pairs(e, p, q, lambda s, t: _at(e, _cut(e, s, t), 1))),
    )
    within = f"{_inside(e, b, f'{a}[1]', f'{a}[2]')} OR {_inside(e, a, f'{b}[1]', f'{b}[2]')}"
    boxes = e.let.format(
        "_h", _bounds(e, a), e.let.format("_l", _bounds(e, b), _boxes_meet("_h", "_l"))
    )
    return f"({boxes} AND ({crossed} OR {within}))"


def _same_ring(e: Engine, a: str, b: str) -> str:
    """plist_same: from each point of ``b`` that is ``a``'s first, forwards or backwards."""
    n = _half(e, e.size.format(a))
    point = lambda at: _vertex(b, at)  # noqa: E731

    def walked(step: str) -> str:
        wrong = f"NOT {_same_point(point(step), _vertex(a, '_ii'))}"
        return f"{e.size.format(e.filter.format(e.upto.format(n), '_ii', wrong))} = 0"

    forward, backward = f"(_st + _ii) % {n}", f"(_st + {n} - _ii) % {n}"
    start = e.filter.format(e.upto.format(n), "_st", f"{walked(forward)} OR {walked(backward)}")
    return (
        f"CASE WHEN {n} <> {_half(e, e.size.format(b))} THEN 1 = 0 "
        f"ELSE {e.size.format(start)} > 0 END"
    )


def _lines(add: Callable) -> None:
    """line and lseg against their own kind."""
    a, b, c = "_a[1]", "_a[2]", "_a[3]"
    d, f_, g = "_b[1]", "_b[2]", "_b[3]"

    def same(e: Engine, _: str) -> str:
        ratio = (
            f"CASE WHEN NOT {_fp_zero(d)} THEN {a} / {d} WHEN NOT {_fp_zero(f_)} THEN {b} / {f_} "
            f"WHEN NOT {_fp_zero(g)} THEN {c} / {g} ELSE {_ONE} END"
        )
        equal = " AND ".join(_fp_eq(x, f"(_lr * {y})") for x, y in ((a, d), (b, f_), (c, g)))
        return e.let.format("_lr", ratio, f"({equal})")

    def apart(e: Engine, f: str) -> str:
        ratio = (
            f"CASE WHEN NOT {_fp_zero(a)} AND NOT {_fp_zero(d)} THEN {a} / {d} "
            f"WHEN NOT {_fp_zero(b)} AND NOT {_fp_zero(f_)} THEN {b} / {f_} ELSE {_ONE} END"
        )
        gap = e.let.format("_lr", ratio, f"abs({c} - _lr * {g}) / {_hypot(e, a, b)}")
        met = e.let.format("_lm0", _meet(e, "_a", "_b"), "_lm0[1]")
        return _double(e, f"CASE WHEN {met} = 1 THEN {_NIL} ELSE {gap} END", f)

    def found(e: Engine, f: str, values: str) -> str:
        """A helper's point, NULL where it found none."""
        return e.let.format(
            "_v",
            values,
            "CASE WHEN _v[1] = 0 THEN NULL ELSE "
            + _shape(e, _list(e, "_v[2]", "_v[3]"), *_POINT, f)
            + " END",
        )

    add("line_eq", "a:line, b:line", 1, same)
    add("line_distance", "a:line, b:line", 2, apart)
    add("line_interpt", "a:line, b:line", 0, lambda e, f: found(e, f, _meet(e, "_a", "_b")))
    add(
        "line_intersect",
        "a:line, b:line",
        1,
        lambda e, _: f"{_at(e, _meet(e, '_a', '_b'), 1)} = 1",
    )
    add("lseg_interpt", "a:lseg, b:lseg", 0, lambda e, f: found(e, f, _cut(e, "_a", "_b")))
    add(
        "lseg_intersect",
        "a:lseg, b:lseg",
        1,
        lambda e, _: f"{_at(e, _cut(e, '_a', '_b'), 1)} = 1",
    )
    add(
        "lseg_distance",
        "a:lseg, b:lseg",
        2,
        lambda e, f: _double(e, _at(e, _near_seg(e, "_a", "_b"), 1), f),
    )


def _distances(add: Callable) -> None:
    """dist_: each pair in both orders, ``a`` the same kind in each."""

    def both(there: str, back: str, kinds: tuple[str, str], value: Callable) -> None:
        body = lambda e, f: _double(e, value(e), f)  # noqa: E731
        add(there, f"a:{kinds[0]}, b:{kinds[1]}", 2, body)
        add(back, f"b:{kinds[1]}, a:{kinds[0]}", 2, body)

    both(
        "dist_pl",
        "dist_lp",
        ("point", "line"),
        lambda e: _at(e, _foot(e, "_b", "_a[1]", "_a[2]"), 1),
    )
    both(
        "dist_ps",
        "dist_sp",
        ("point", "lseg"),
        lambda e: _at(e, _near_point(e, "_b", "_a[1]", "_a[2]"), 1),
    )
    both(
        "dist_pb",
        "dist_bp",
        ("point", "box"),
        lambda e: _at(e, _near_box_point(e, "_b", "_a[1]", "_a[2]"), 1),
    )
    both("dist_sl", "dist_ls", ("lseg", "line"), lambda e: _at(e, _near_line(e, "_a", "_b"), 1))
    both(
        "dist_sb",
        "dist_bs",
        ("lseg", "box"),
        lambda e: _at(e, _near_box_seg(e, "_b", "_a", point=False), 1),
    )
    both(
        "dist_ppoly",
        "dist_polyp",
        ("point", "polygon"),
        lambda e: _from_polygon(e, "_b", "_a[1]", "_a[2]"),
    )

    def to_circle(e: Engine) -> str:
        gap = f"({_from_polygon(e, '_b', '_a[1]', '_a[2]')}) - _a[3]"
        return e.let.format("_g0", gap, f"CASE WHEN _g0 < 0 THEN {_NIL} ELSE _g0 END")

    both("dist_cpoly", "dist_polyc", ("circle", "polygon"), to_circle)

    def to_path(e: Engine) -> str:
        """dist_ppath_internal: a path of one open point has no segment and answers 0."""
        each = e.map.format("_sg", "_w", _at(e, _near_point(e, "_w", "_a[1]", "_a[2]"), 1))
        return e.let.format(
            "_sg",
            _segments(e, "_b", _is_closed(e, "b")),
            f"CASE WHEN {e.size.format('_sg')} = 0 THEN {_NIL} ELSE {e.min_of.format(each)} END",
        )

    both("dist_ppath", "dist_pathp", ("point", "path"), to_path)


def _closest(add: Callable) -> None:
    """close_: the point itself. A pair that close_ls and close_lseg call parallel answers NULL."""

    def point(e: Engine, values: str, f: str) -> str:
        return e.let.format("_v", values, _shape(e, _list(e, "_v[2]", "_v[3]"), *_POINT, f))

    def unless_parallel(e: Engine, one: str, other: str, answer: str) -> str:
        return e.let.format(
            "_k1",
            one,
            e.let.format("_k2", other, f"CASE WHEN _k1 = _k2 THEN NULL ELSE {answer} END"),
        )

    add(
        "close_pl",
        "a:point, b:line",
        0,
        lambda e, f: point(e, _foot(e, "_b", "_a[1]", "_a[2]"), f),
    )
    add(
        "close_ps",
        "a:point, b:lseg",
        0,
        lambda e, f: point(e, _near_point(e, "_b", "_a[1]", "_a[2]"), f),
    )
    add(
        "close_pb",
        "a:point, b:box",
        0,
        lambda e, f: point(e, _near_box_point(e, "_b", "_a[1]", "_a[2]"), f),
    )
    add(
        "close_lseg",
        "a:lseg, b:lseg",
        0,
        lambda e, f: unless_parallel(
            e, _seg_slope(e, "_a"), _seg_slope(e, "_b"), point(e, _near_seg(e, "_b", "_a"), f)
        ),
    )
    add(
        "close_ls",
        "a:line, b:lseg",
        0,
        lambda e, f: unless_parallel(
            e, _seg_slope(e, "_b"), _line_slope(e, "_a"), point(e, _near_line(e, "_b", "_a"), f)
        ),
    )
    add(
        "close_sb",
        "a:lseg, b:box",
        0,
        lambda e, f: point(e, _near_box_seg(e, "_b", "_a"), f),
    )


def _on_line(line: str, x: str, y: str) -> str:
    """line_contain_point."""
    return _fp_zero(f"{line}[1] * {x} + {line}[2] * {y} + {line}[3]")


def _on(add: Callable) -> None:
    """on_ and inter_."""
    add("on_pl", "a:point, b:line", 1, lambda e, _: _on_line("_b", "_a[1]", "_a[2]"))
    add("on_ps", "a:point, b:lseg", 1, lambda e, _: _on_seg(e, "_b", "_a[1]", "_a[2]"))
    add(
        "on_sl",
        "a:lseg, b:line",
        1,
        lambda e, _: f"{_on_line('_b', *_xy('_a'))} AND {_on_line('_b', *_xy('_a', 3))}",
    )
    add(
        "on_sb",
        "a:lseg, b:box",
        1,
        lambda e, _: f"{_in_box('_b', *_xy('_a'))} AND {_in_box('_b', *_xy('_a', 3))}",
    )
    add(
        "inter_sl",
        "a:lseg, b:line",
        1,
        lambda e, _: f"{_at(e, _cut_line(e, '_a', '_b'), 1)} = 1",
    )
    add("inter_sb", "a:lseg, b:box", 1, lambda e, _: _box_cut(e, "_b", "_a"))
    add(
        "inter_lb",
        "b:line, a:box",
        1,
        lambda e, _: _any(
            e, e.map.format(_sides(e, "_a"), "_bs", _at(e, _cut_line(e, "_bs", "_b"), 1))
        ),
    )


def _on_path(e: Engine) -> str:
    """on_ppath of the point ``_a`` and the path ``_b``: inside if closed, on a segment if open."""
    on = e.filter.format(_segments(e, "_b", "1 = 0"), "_w", _on_seg(e, "_w", "_a[1]", "_a[2]"))
    return (
        f"CASE WHEN {_is_closed(e, 'b')} THEN {_inside(e, '_b', '_a[1]', '_a[2]')} "
        f"ELSE {e.size.format(on)} > 0 END"
    )


def _paths_and_polygons(add: Callable) -> None:
    add("on_ppath", "a:point, b:path", 1, lambda e, _: _on_path(e))
    add("path_contain_pt", "b:path, a:point", 1, lambda e, _: _on_path(e))
    add(
        "poly_contain_pt",
        "a:polygon, b:point",
        1,
        lambda e, _: _inside(e, "_a", "_b[1]", "_b[2]"),
    )
    add("poly_same", "a:polygon, b:polygon", 1, lambda e, _: _same_ring(e, "_a", "_b"))
    add("poly_overlap", "a:polygon, b:polygon", 1, lambda e, _: _overlap(e, "_a", "_b"))

    def apart(e: Engine, f: str) -> str:
        far = _both(e, ("_a", "1 = 1"), ("_b", "1 = 1"), lambda p, q: _least_pair(e, p, q, f))
        return f"CASE WHEN {_overlap(e, '_a', '_b')} THEN 0e0 ELSE {far} END"

    add("poly_distance", "a:polygon, b:polygon", 2, apart)

    def inter(e: Engine, _: str) -> str:
        edges = _both(
            e,
            ("_a", _is_closed(e, "a")),
            ("_b", _is_closed(e, "b")),
            lambda p, q: _some_pair(
                e, _pairs(e, p, q, lambda s, t: _at(e, _cut(e, s, t), 1)), p, q
            ),
        )
        boxes = e.let.format(
            "_h", _bounds(e, "_a"), e.let.format("_l", _bounds(e, "_b"), _boxes_meet("_h", "_l"))
        )
        return f"({boxes} AND {edges})"

    add("path_inter", "a:path, b:path", 1, inter)
    add(
        "path_distance",
        "a:path, b:path",
        2,
        lambda e, f: _both(
            e,
            ("_a", _is_closed(e, "a")),
            ("_b", _is_closed(e, "b")),
            lambda p, q: _least_pair(e, p, q, f),
        ),
    )


_INT_MAX = "2147483647"
_DIGITS_HELD = 27  # 38 digits of the widest integer less a difference's one and a count's ten
#: How near a whole number numeric's division may round a quotient: select_div_scale keeps 16
#: significant digits past the first base-10000 digit, 16 places below 10000, 8 up to 2^31.
_ROUNDED_AWAY = ("1e-15", "1e-7")
_NO_COUNT = "2201G: count must be greater than zero"
_SAME_BOUNDS = "2201G: lower bound cannot equal upper bound"
_OUT_OF_RANGE = "22003: integer out of range"


def _when(when: str, condition: str) -> str:
    """A refusal of one path of a form: ClickHouse runs both paths of a lambda's if."""
    return f"({when}) AND ({condition})"


def _bucket_float(e: Engine, when: str) -> str:
    """width_bucket_float8: a value not finite is refused by name, operand and bounds alike."""
    x, lo, hi = "_b[1]", "_b[2]", "_b[3]"
    up = f"{lo} < {hi}"

    def fraction(top: tuple[str, str], bottom: tuple[str, str], whole: str) -> str:
        halves = f"({top[0]} / 2 - {top[1]} / 2) / ({bottom[0]} / 2 - {bottom[1]} / 2)"
        return (
            f"CASE WHEN {e.finite.format(whole)} "
            f"THEN ({top[0]} - {top[1]}) / ({bottom[0]} - {bottom[1]}) ELSE {halves} END"
        )

    rising = fraction((x, lo), (hi, lo), f"{hi} - {lo}")
    falling = fraction((lo, x), (lo, hi), f"{lo} - {hi}")
    # Equal bounds or a NaN divide to no number, and ClickHouse runs this path before its refusal,
    # where converting that to an integer raises an error of its own instead.
    fraction_of = e.let.format(
        "_w",
        f"CASE WHEN {up} THEN {rising} ELSE {falling} END",
        f"CASE WHEN {e.finite.format('_w')} THEN _w ELSE {_NIL} END",
    )
    bucket = e.let.format(
        "_c",
        e.bigint.format(f"floor(_n * ({fraction_of}))"),
        "least(_c, _n - 1) + 1",
    )
    below = f"({up} AND {x} < {lo}) OR (NOT ({up}) AND {x} > {lo})"
    above = f"({up} AND {x} >= {hi}) OR (NOT ({up}) AND {x} <= {hi})"
    answer = f"CASE WHEN {below} THEN 0 WHEN {above} THEN _n + 1 ELSE {bucket} END"
    guarded = e.raises.format(
        _when(when, f"{lo} = {hi}"),
        _SAME_BOUNDS,
        e.raises.format(_when(when, f"_n = {_INT_MAX} AND ({above})"), _OUT_OF_RANGE, answer),
    )
    values = _list(e, *(e.double.format(p) for p in ("x", "lo", "hi")))
    return e.let.format("_b", values, guarded)


def _bucket_numeric(e: Engine, when: str) -> str:
    """width_bucket_numeric over each value's digits, at the scale of the three."""
    whole = lambda i: e.part.format(f"_t[{i}]", ".", 1)  # noqa: E731
    fraction = lambda i: e.part.format(f"_t[{i}]", ".", 2)  # noqa: E731
    sizes = lambda of: ", ".join(e.length.format(of(i)) for i in (1, 2, 3))  # noqa: E731
    scaled = lambda i: e.wide.format(  # noqa: E731
        f"replace({whole(i)} || {e.rpad.format(fraction(i), '_sc')}, '+', '')"
    )
    digits = f"greatest({sizes(whole)}) + _sc > {_DIGITS_HELD}"
    x, lo, hi = "_z[1]", "_z[2]", "_z[3]"
    up = f"{lo} < {hi}"
    wide_count = e.wide.format("_n")
    # The divisor is never 0 where its answer is read: ClickHouse's intDiv raises on one all the same.
    span = f"CASE WHEN {lo} = {hi} THEN {e.wide.format(1)} WHEN {up} THEN {hi} - {lo} ELSE {lo} - {hi} END"
    numerator = f"(CASE WHEN {up} THEN {x} - {lo} ELSE {lo} - {x} END) * {wide_count}"
    below = f"({up} AND {x} < {lo}) OR (NOT ({up}) AND {x} > {lo})"
    above = f"({up} AND {x} >= {hi}) OR (NOT ({up}) AND {x} <= {hi})"
    left = "(_d - _m % _d)"
    rounded = (
        f"_m % _d <> {e.wide.format(0)} AND NOT ({below}) AND NOT ({above}) AND "
        f"{e.double.format(left)} / {e.double.format('_d')} < "
        f"CASE WHEN _n < 10000 THEN {_ROUNDED_AWAY[0]} ELSE {_ROUNDED_AWAY[1]} END"
    )
    quotient = e.quotient.format("_m", "_d")
    answer = (
        f"CASE WHEN {below} THEN 0 WHEN {above} THEN _n + 1 "
        f"ELSE {e.bigint.format(quotient)} + 1 END"
    )
    guarded = e.raises.format(
        _when(when, f"{lo} = {hi}"),
        _SAME_BOUNDS,
        e.raises.format(
            _when(when, f"_n = {_INT_MAX} AND ({above})"),
            _OUT_OF_RANGE,
            e.raises.format(_when(when, rounded), f"0A000: width_bucket {REFUSED}", answer),
        ),
    )
    worked = e.let.format("_d", span, e.let.format("_m", numerator, guarded))
    read = e.let.format("_z", _list(e, scaled(1), scaled(2), scaled(3)), worked)
    held = e.raises.format(_when(when, digits), f"0A000: width_bucket {REFUSED}", read)
    places = e.let.format("_sc", f"greatest({sizes(fraction)})", held)
    texts = _list(e, *(e.text.format(p) for p in ("x", "lo", "hi")))
    return e.let.format("_t", texts, places)


def _bucket_kind(e: Engine) -> str:
    """Whether the engine holds a numeric here: a statement's casts have chosen a row already. On
    ClickHouse a literal with a fraction is a Float64, which says nothing of what the others are."""
    named = lambda p, word: f"{e.type_name.format(p)} LIKE '%{word}%'"  # noqa: E731
    params = ("x", "lo", "hi")
    decimal = " OR ".join(named(p, "decimal") for p in params)
    if e.name == "clickhouse":
        return f"({decimal})"
    floats = " OR ".join(named(p, w) for p in params for w in ("double", "real", "float"))
    return f"(({decimal}) AND NOT ({floats}))"


def _bucket(e: Engine, kind: str) -> str:
    """Each width_bucket of four arguments, by which of float8's and numeric's it is."""
    texts = [e.text.format(p) for p in ("x", "lo", "hi")]
    numeric = {"numeric": "1 = 1", "float8": "1 = 0"}.get(kind, _bucket_kind(e))
    finite = " OR ".join(e.matches.format(t, _NOT_FINITE) for t in texts)
    # An exponent's e is not a digit either, where the digits are what a numeric is read from.
    spelled = " OR ".join(e.matches.format(t, "[a-zA-Z]") for t in texts)
    answers = {
        "numeric": _bucket_numeric(e, "1 = 1"),
        "float8": _bucket_float(e, "1 = 1"),
    }
    answer = answers.get(kind) or (
        f"CASE WHEN {numeric} THEN {_bucket_numeric(e, numeric)} "
        f"ELSE {_bucket_float(e, f'NOT {numeric}')} END"
    )
    counted = e.let.format(
        "_n", e.bigint.format("n"), e.raises.format("_n <= 0", _NO_COUNT, answer)
    )
    refused = f"({finite}) OR (({numeric}) AND ({spelled}))"
    return e.raises.format(refused, f"0A000: width_bucket {REFUSED}", counted)


def _bucket_of_list(e: Engine) -> str:
    """width_bucket_array: PostgreSQL's binary search, which a list out of order steers too: the
    running value is left * 2^32 + right. Text compares as the collation does for what is below."""
    shift = "4294967296"
    left, right = _whole(e, "_s", shift), f"(_s % {shift})"
    middle = _whole(e, f"({left} + {right})", "2")
    step = (
        f"CASE WHEN {left} >= {right} THEN _s "
        f"WHEN x < t[{middle} + 1] THEN {left} * {shift} + {middle} "
        f"ELSE ({middle} + 1) * {shift} + {right} END"
    )
    start = f"{e.size.format('t')}"
    search = e.fold.format(e.upto.format(32), "_s", start, step)
    answer = _whole(e, search, shift)
    empty_left = e.bigint.format(answer)
    nulls = e.filter.format("t", "_y", "_y IS NULL")
    named = lambda p, word: f"{e.type_name.format(p)} LIKE '%{word}%'"  # noqa: E731
    is_text = f"({named('x', 'char')} OR {named('x', 'string')})"
    numbers = e.filter.format(
        "t", "_y", e.matches.format(f"lower({e.text.format('_y')})", "^[-+]?(nan|inf)")
    )
    plain = e.filter.format(
        "t", "_y", f"NOT {e.matches.format(e.text.format('_y'), '^[a-z0-9]*$')}"
    )
    finite = (
        f"NOT {is_text} AND ({e.size.format(numbers)} > 0 OR "
        f"{e.matches.format(f'lower({e.text.format(chr(120))})', '^[-+]?(nan|inf)')})"
    )
    collated = (
        f"{is_text} AND ({e.size.format(plain)} > 0 OR "
        f"NOT {e.matches.format(e.text.format('x'), '^[a-z0-9]*$')})"
    )
    return e.raises.format(
        f"{e.size.format(nulls)} > 0",
        "22004: thresholds array must not contain NULLs",
        e.raises.format(f"{finite} OR {collated}", f"0A000: width_bucket {REFUSED}", empty_left),
    )


def _buckets(add: Callable) -> None:
    params = "x:number, lo:number, hi:number, n:integer"
    add("width_bucket", params, 3, lambda e, _: _bucket(e, "either"))
    add("width_bucket_float8", params, 3, lambda e, _: _bucket(e, "float8"))
    add("width_bucket_numeric", params, 3, lambda e, _: _bucket(e, "numeric"))
    add("width_bucket", "x:number, t:array", 3, lambda e, _: _bucket_of_list(e))
    add(
        "width_bucket_nested",
        "x:number, t:array",
        3,
        lambda e, _: e.raises.format(
            "1 = 1", "2202E: thresholds must be one-dimensional array", "0"
        ),
    )


def _circles(add: Callable) -> None:
    ax, ay, ar = "_a[1]", "_a[2]", "_a[3]"
    bx, by, br = "_b[1]", "_b[2]", "_b[3]"
    centers = lambda e: _dist(e, (ax, ay), (bx, by))  # noqa: E731
    area_a, area_b = f"({ar} * {ar} * pi())", f"({br} * {br} * pi())"

    def on_two(name: str, test: Callable[[Engine], str]) -> None:
        add(f"circle_{name}", "a:circle, b:circle", 1, lambda e, _: test(e))

    for name, fuzzy in _FUZZY.items():
        on_two(name, lambda e, fuzzy=fuzzy: fuzzy(area_a, area_b))
    on_two("ne", lambda e: f"(NOT {_fp_eq(area_a, area_b)})")
    on_two("same", lambda e: f"{_fp_eq(ar, br)} AND {_fp_eq(ax, bx)} AND {_fp_eq(ay, by)}")
    on_two("overlap", lambda e: _fp_le(centers(e), f"({ar} + {br})"))
    on_two("contain", lambda e: _fp_le(centers(e), f"({ar} - {br})"))
    on_two("contained", lambda e: _fp_le(centers(e), f"({br} - {ar})"))
    on_two("left", lambda e: _fp_lt(f"({ax} + {ar})", f"({bx} - {br})"))
    on_two("overleft", lambda e: _fp_le(f"({ax} + {ar})", f"({bx} + {br})"))
    on_two("right", lambda e: _fp_gt(f"({ax} - {ar})", f"({bx} + {br})"))
    on_two("overright", lambda e: _fp_ge(f"({ax} - {ar})", f"({bx} - {br})"))
    on_two("below", lambda e: _fp_lt(f"({ay} + {ar})", f"({by} - {br})"))
    on_two("overbelow", lambda e: _fp_le(f"({ay} + {ar})", f"({by} + {br})"))
    on_two("above", lambda e: _fp_gt(f"({ay} - {ar})", f"({by} + {br})"))
    on_two("overabove", lambda e: _fp_ge(f"({ay} - {ar})", f"({by} - {br})"))
    q = _xy("_b")
    out = lambda e: _dist(e, (ax, ay), q) + f" - {ar}"  # noqa: E731

    def clear(e: Engine, gap: str, f: str) -> str:
        return _double(e, e.let.format("_g", gap, "CASE WHEN _g < 0 THEN 0.0 ELSE _g END"), f)

    add(
        "circle_distance",
        "a:circle, b:circle",
        2,
        lambda e, f: clear(e, f"{centers(e)} - ({ar} + {br})", f),
    )
    add("dist_cpoint", "a:circle, b:point", 2, lambda e, f: clear(e, out(e), f))
    add("dist_pc", "b:point, a:circle", 2, lambda e, f: clear(e, out(e), f))
    add(
        "circle_contain_pt", "a:circle, b:point", 1, lambda e, _: f"{_dist(e, (ax, ay), q)} <= {ar}"
    )
    add("diameter", "a:circle", 2, lambda e, f: _double(e, f"{ar} * 2.0", f))
    add(
        "circle_of",
        "a:point, r:double",
        0,
        lambda e, f: _circle_text(e, _list(e, "_a[1]", "_a[2]", e.double.format("r")), f),
    )
    size = lambda e: _hypot(e, q[0], q[1])  # noqa: E731
    for verb, radius in (
        ("add", "{r}"),
        ("sub", "{r}"),
        ("mul", "{r} * {s}"),
        ("div", "{r} / {s}"),
    ):

        def moved(e: Engine, f: str, verb: str = verb, radius: str = radius) -> str:
            center = _MOVES[verb][0]((ax, ay), q)
            text = _circle_text(e, _list(e, *center, radius.format(r=ar, s=size(e))), f)
            return (
                _division(e, f"{q[0]} * {q[0]} + {q[1]} * {q[1]}", text) if verb == "div" else text
            )

        add(f"circle_{verb}_pt", "a:circle, b:point", 0, moved)


def _circle_poly(e: Engine, f: str, n: str = "n") -> str:
    """circle_poly: ``n`` points of the circle ``_a``, as doubles."""
    step = f"2.0 * pi() / {n}"
    points = e.map.format(
        e.upto.format(n),
        "_i",
        _list(
            e,
            f"_a[1] - _a[3] * cos(({step}) * _i)",
            f"_a[2] + _a[3] * sin(({step}) * _i)",
        ),
    )
    text = e.join.format(
        e.map.format(
            points,
            "_c",
            f"'(' || {_float_text(e, '_c[1]')} || ',' || {_float_text(e, '_c[2]')} || ')'",
        ),
        "','",
    )
    return e.raises.format(
        f"abs(_a[3]) <= {_FUZZ}",
        "0A000: cannot convert circle with radius zero to polygon",
        e.raises.format(
            f"{n} < 2", "22023: must request at least 2 points", f"'(' || {text} || ')'"
        ),
    )


def _lists(add: Callable) -> None:
    """lseg's comparisons, and what path and polygon answer from their points alone."""
    p0, p1 = _xy("_a"), _xy("_a", 3)
    q0, q1 = _xy("_b"), _xy("_b", 3)
    same = " AND ".join(_fp_eq(f"_a[{i}]", f"_b[{i}]") for i in (1, 2, 3, 4))
    add("lseg_eq", "a:lseg, b:lseg", 1, lambda e, _: same)
    add("lseg_ne", "a:lseg, b:lseg", 1, lambda e, _: f"NOT ({same})")
    for name, fuzzy in _FUZZY.items():
        if name != "eq":
            add(
                f"lseg_{name}",
                "a:lseg, b:lseg",
                1,
                lambda e, _, fuzzy=fuzzy: fuzzy(_dist(e, p0, p1), _dist(e, q0, q1)),
            )
    box = lambda e, name: _bounds(e, name)  # noqa: E731
    count = lambda e, name: _half(e, e.size.format(name))  # noqa: E731
    for name, test in (
        ("left", "_x[1] < _y[3]"),
        ("overleft", "_x[1] <= _y[1]"),
        ("right", "_x[3] > _y[1]"),
        ("overright", "_x[3] >= _y[3]"),
        ("below", "_x[2] < _y[4]"),
        ("overbelow", "_x[2] <= _y[2]"),
        ("above", "_x[4] > _y[2]"),
        ("overabove", "_x[4] >= _y[4]"),
    ):
        add(
            f"poly_{name}",
            "a:polygon, b:polygon",
            1,
            lambda e, _, test=test: e.let.format(
                "_x", box(e, "_a"), e.let.format("_y", box(e, "_b"), test)
            ),
        )
    add(
        "poly_circle",
        "a:polygon",
        0,
        lambda e, f: e.let.format(
            "_c",
            _list(e, *_poly_center(e, "_a")),
            _circle_text(
                e,
                _list(
                    e,
                    "_c[1]",
                    "_c[2]",
                    _sum_by(e, "_a", _dist(e, ("_a[2 * _i + 1]", "_a[2 * _i + 2]"), _xy("_c")))
                    + f" / {count(e, '_a')}",
                ),
                f,
            ),
        ),
    )
    add("poly_npoints", "a:polygon", 3, lambda e, _: count(e, "_a"))
    add("path_npoints", "a:path", 3, lambda e, _: count(e, "_a"))
    for name, op in (("lt", "<"), ("le", "<="), ("eq", "="), ("ge", ">="), ("gt", ">")):
        add(
            f"path_n_{name}",
            "a:path, b:path",
            1,
            lambda e, _, op=op: f"{count(e, '_a')} {op} {count(e, '_b')}",
        )
    add("poly_path", "a:polygon", 0, lambda e, f: _shape(e, "_a", *_CLOSED, f))
    add("pclose", "a:path", 0, lambda e, f: _shape(e, "_a", *_CLOSED, f))
    add("popen", "a:path", 0, lambda e, f: _shape(e, "_a", *_LSEG, f))
    add(
        "path_poly",
        "a:path",
        0,
        lambda e, f: e.raises.format(
            _is_open(e, "a"),
            "22023: open path cannot be converted to polygon",
            _shape(e, "_a", *_CLOSED, f),
        ),
    )
    add("circle_poly", "n:integer, a:circle", 0, _circle_poly)
    add("circle_poly12", "a:circle", 0, lambda e, f: _circle_poly(e, f, "12"))
    for verb, (move, divides) in _MOVES.items():

        def moved(e: Engine, f: str, move: Callable = move, divides: bool = divides) -> str:
            at = ("_a[2 * _i + 1]", "_a[2 * _i + 2]")
            x, y = move(at, _xy("_b"))
            pairs = e.upto.format(count(e, "_a"))
            joined = e.map.format(pairs, "_i", _list(e, x, y))
            flat = {
                "duckdb": f"flatten({joined})",
                "trino": f"flatten({joined})",
                "clickhouse": f"arrayFlatten({joined})",
            }[e.name]
            text = _kept_open(e, flat, f)
            b = _xy("_b")
            return _division(e, f"{b[0]} * {b[0]} + {b[1]} * {b[1]}", text) if divides else text

        add(f"path_{verb}_pt", "a:path, b:point", 0, moved)
    add(
        "path_add",
        "a:path, b:path",
        0,
        lambda e, f: (
            f"CASE WHEN NOT ({_is_open(e, 'a')} AND {_is_open(e, 'b')}) THEN NULL ELSE "
            + _shape(
                e,
                {
                    "duckdb": "list_concat(_a, _b)",
                    "trino": "concat(_a, _b)",
                    "clickhouse": "arrayConcat(_a, _b)",
                }[e.name],
                *_LSEG,
                f,
            )
            + " END"
        ),
    )


def _kept_open(e: Engine, values: str, f: str) -> str:
    """A path's new points, open or closed as the path ``a`` was."""
    joined = e.join.format(_points(e, "_r"), "','")
    opens = f"CASE WHEN {_is_open(e, 'a')} THEN '[' ELSE '(' END"
    closes = f"CASE WHEN {_is_open(e, 'a')} THEN ']' ELSE ')' END"
    return _checked(e, values, f"{opens} || {joined} || {closes}", f)


def _rows() -> list[Row]:
    rows: list[Row] = []

    def add(name: str, params: str, answers: int, body: Callable[[Engine, str], str]) -> None:
        rows.append(Row(name, params, answers, body))

    def on_two(name: str, kind: str, test: Callable[[Engine], str]) -> None:
        add(name, f"a:{kind}, b:{kind}", 1, lambda e, _: test(e))

    # point
    p, q = _xy("_a"), _xy("_b")
    same = f"{_fp_eq(p[0], q[0])} AND {_fp_eq(p[1], q[1])}"
    on_two("point_eq", "point", lambda e: same)
    on_two("point_ne", "point", lambda e: f"NOT ({same})")
    on_two("point_left", "point", lambda e: _fp_lt(p[0], q[0]))
    on_two("point_right", "point", lambda e: _fp_gt(p[0], q[0]))
    on_two("point_above", "point", lambda e: _fp_gt(p[1], q[1]))
    on_two("point_below", "point", lambda e: _fp_lt(p[1], q[1]))
    on_two("point_vert", "point", lambda e: _fp_eq(p[0], q[0]))
    on_two("point_horiz", "point", lambda e: _fp_eq(p[1], q[1]))
    add("point_distance", "a:point, b:point", 2, lambda e, f: _double(e, _dist(e, p, q), f))
    add("slope", "a:point, b:point", 2, lambda e, f: _slope(e, p, q, f))
    for verb, (move, divides) in _MOVES.items():

        def moved(e: Engine, f: str, move: Callable = move, divides: bool = divides) -> str:
            text = _shape(e, _list(e, *move(p, q)), *_POINT, f)
            return _division(e, f"{q[0]} * {q[0]} + {q[1]} * {q[1]}", text) if divides else text

        def boxed(e: Engine, f: str, move: Callable = move, divides: bool = divides) -> str:
            hi, lo = _xy("_a"), _xy("_a", 3)
            text = _shape(e, _box_of(e, move(hi, q), move(lo, q)), *_BOX, f)
            return _division(e, f"{q[0]} * {q[0]} + {q[1]} * {q[1]}", text) if divides else text

        add(f"point_{verb}", "a:point, b:point", 0, moved)
        add(f"box_{verb}", "a:box, b:point", 0, boxed)

    # box: _a and _b are high x, high y, low x, low y
    hx, hy, lx, ly = (f"_a[{i}]" for i in (1, 2, 3, 4))
    bhx, bhy, blx, bly = (f"_b[{i}]" for i in (1, 2, 3, 4))
    overlap = f"{_fp_le(lx, bhx)} AND {_fp_le(blx, hx)} AND {_fp_le(ly, bhy)} AND {_fp_le(bly, hy)}"

    for name, test in (
        ("left", _fp_lt(hx, blx)),
        ("overleft", _fp_le(hx, bhx)),
        ("right", _fp_gt(lx, bhx)),
        ("overright", _fp_ge(lx, blx)),
        ("below", _fp_lt(hy, bly)),
        ("overbelow", _fp_le(hy, bhy)),
        ("above", _fp_gt(ly, bhy)),
        ("overabove", _fp_ge(ly, bly)),
        ("below_eq", _fp_le(hy, bly)),
        ("above_eq", _fp_ge(ly, bhy)),
        ("overlap", overlap),
        ("contain", _box_contains("_a", "_b")),
        ("contained", _box_contains("_b", "_a")),
        ("same", " AND ".join(_fp_eq(f"_a[{i}]", f"_b[{i}]") for i in (1, 2, 3, 4))),
    ):
        on_two(f"box_{name}", "box", lambda e, test=test: test)
    area_a, area_b = f"(({hx} - {lx}) * ({hy} - {ly}))", f"(({bhx} - {blx}) * ({bhy} - {bly}))"
    for name, fuzzy in _FUZZY.items():
        on_two(f"box_{name}", "box", lambda e, fuzzy=fuzzy: fuzzy(area_a, area_b))
    add(
        "box_distance",
        "a:box, b:box",
        2,
        lambda e, f: _double(e, _dist(e, _center("_a"), _center("_b")), f),
    )
    add(
        "bound_box",
        "a:box, b:box",
        0,
        lambda e, f: _shape(
            e,
            _list(
                e,
                f"greatest({hx}, {bhx})",
                f"greatest({hy}, {bhy})",
                f"least({lx}, {blx})",
                f"least({ly}, {bly})",
            ),
            *_BOX,
            f,
        ),
    )
    add(
        "box_intersect",
        "a:box, b:box",
        0,
        lambda e, f: (
            f"CASE WHEN {overlap} THEN "
            + _shape(
                e,
                _list(
                    e,
                    f"least({hx}, {bhx})",
                    f"least({hy}, {bhy})",
                    f"greatest({lx}, {blx})",
                    f"greatest({ly}, {bly})",
                ),
                *_BOX,
                f,
            )
            + " END"
        ),
    )
    inside = "_b[1] <= _a[1] AND _b[1] >= _a[3] AND _b[2] <= _a[2] AND _b[2] >= _a[4]"
    add("box_contain_pt", "a:box, b:point", 1, lambda e, _: inside)
    add("on_pb", "b:point, a:box", 1, lambda e, _: inside)
    add(
        "point_box",
        "a:point",
        0,
        lambda e, f: _shape(e, _list(e, p[0], p[1], p[0], p[1]), *_BOX, f),
    )
    add("points_box", "a:point, b:point", 0, lambda e, f: _shape(e, _box_of(e, p, q), *_BOX, f))
    add("box_center", "a:box", 0, lambda e, f: _shape(e, _list(e, *_center("_a")), *_POINT, f))
    add("lseg_center", "a:lseg", 0, lambda e, f: _shape(e, _list(e, *_center("_a")), *_POINT, f))
    add("box_diagonal", "a:box", 0, lambda e, f: _shape(e, "_a", *_LSEG, f))
    add("lseg_of", "a:point, b:point", 0, lambda e, f: _shape(e, _list(e, *p, *q), *_LSEG, f))
    add("line_through", "a:point, b:point", 0, _line_through)
    add(
        "box_poly",
        "a:box",
        0,
        lambda e, f: _shape(e, _list(e, lx, ly, lx, hy, hx, hy, hx, ly), *_CLOSED, f),
    )
    add(
        "box_circle",
        "a:box",
        0,
        lambda e, f: e.let.format(
            "_c",
            _list(e, *_center("_a")),
            _circle_text(e, _list(e, "_c[1]", "_c[2]", _dist(e, _xy("_c"), p)), f),
        ),
    )
    add("point_of_circle", "a:circle", 0, lambda e, f: _shape(e, _list(e, *p), *_POINT, f))
    add(
        "point_of",
        "x:double, y:double",
        0,
        lambda e, f: _shape(e, _list(e, e.double.format("x"), e.double.format("y")), *_POINT, f),
    )
    add("poly_box", "a:polygon", 0, lambda e, f: _shape(e, _bounds(e, "_a"), *_BOX, f))
    add(
        "poly_center",
        "a:polygon",
        0,
        lambda e, f: _shape(e, _list(e, *_poly_center(e, "_a")), *_POINT, f),
    )
    add(
        "circle_box",
        "a:circle",
        0,
        lambda e, f: e.let.format(
            "_w",
            "_a[3] / sqrt(2.0)",
            _shape(
                e,
                _list(e, "_a[1] + _w", "_a[2] + _w", "_a[1] - _w", "_a[2] - _w"),
                *_BOX,
                f,
            ),
        ),
    )
    _circles(add)
    _lists(add)
    add("poly_contain", "a:polygon, b:polygon", 1, lambda e, _: _contains(e, "_a", "_b"))
    add("poly_contained", "a:polygon, b:polygon", 1, lambda e, _: _contains(e, "_b", "_a"))
    _lines(add)
    _distances(add)
    _closest(add)
    _on(add)
    _paths_and_polygons(add)
    _buckets(add)
    return rows


ROWS = tuple(_rows())
#: What pg_catalog's width_bucket(anycompatible, anycompatiblearray) is told of two dimensions, by
#: the statement: a form could not compare a value with a list.
NESTED = "two dimensions"
#: PostgreSQL's overloads of a name its arguments' types tell apart -> the row of each.
OVERLOADS: dict[tuple[str, int, str], str] = {
    ("box", 1, "point"): "point_box",
    ("box", 1, "circle"): "circle_box",
    ("box", 1, "polygon"): "poly_box",
    ("circle", 1, "box"): "box_circle",
    ("circle", 1, "polygon"): "poly_circle",
    ("point", 1, "box"): "box_center",
    ("point", 1, "circle"): "point_of_circle",
    ("point", 1, "lseg"): "lseg_center",
    ("point", 1, "polygon"): "poly_center",
    ("polygon", 1, "box"): "box_poly",
    ("polygon", 1, "circle"): "circle_poly12",
    ("polygon", 1, "path"): "path_poly",
    ("npoints", 1, "path"): "path_npoints",
    ("npoints", 1, "polygon"): "poly_npoints",
    ("path", 1, "polygon"): "poly_path",
    ("lseg", 1, "box"): "box_diagonal",
    ("width_bucket", 4, "double precision"): "width_bucket_float8",
    ("width_bucket", 4, "real"): "width_bucket_float8",
    ("width_bucket", 4, "decimal"): "width_bucket_numeric",
    ("width_bucket", 2, NESTED): "width_bucket_nested",
}
#: A name whose form is its only overload of that many arguments.
ALIASES = {
    ("box", "a, b"): "points_box",
    ("point", "x, y"): "point_of",
    ("lseg", "a, b"): "lseg_of",
    ("line", "a, b"): "line_through",
    ("circle", "a, r"): "circle_of",
    ("polygon", "n, a"): "circle_poly",
}
BARE = (
    ("box", "a"),
    ("circle", "a"),
    ("point", "a"),
    ("polygon", "a"),
    ("npoints", "a"),
    ("path", "a"),
    ("lseg", "a"),
)


_NOT_READ = ("double", "integer", "number", "array")  # what a form takes as the engine holds it


def _form(e: Engine, row: Row) -> str:
    kinds = [tuple(p.split(":")) for p in row.params.split(", ")]
    shapes = [(p, kind) for p, kind in kinds if kind not in _NOT_READ]
    body = row.body(e, row.name)
    for param, kind in reversed(shapes):
        body = e.let.format(f"_{param}", _read(e, param, kind), body)
    letters = " OR ".join(e.matches.format(e.text.format(p), _NOT_FINITE) for p, _ in shapes)
    nulls = " OR ".join(f"{p} IS NULL" for p, _ in kinds)
    sql_type = e.types[row.answers]
    refused = e.raises.format(letters, f"0A000: {row.name} {REFUSED}", body) if shapes else body
    # A cast of a boolean or an integer: ClickHouse's comparisons are UInt8, a size is a BIGINT.
    typed = f"CAST({refused} AS {sql_type})" if row.answers in (1, 3) else refused
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {typed} END"


def forms(e: Engine) -> dict[tuple[str, str], str]:
    """Each row's form on ``e``: (name, its parameters) -> the SQL."""
    return {
        (r.name, ", ".join(p.split(":")[0] for p in r.params.split(", "))): _form(e, r)
        for r in ROWS
    }
