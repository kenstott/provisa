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
float8 checks and its NaN rules are not reproduced. An engine with no entry refuses by name."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from provisa.core.pg_error import MARK
from provisa.transpiler.pg_forms_types import REFUSED

_FUZZ = "1e-06"  # geo_decls.h's EPSILON
_MAX = "1.7976931348623157e308"  # DBL_MAX, point_sl's slope of a vertical line
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
    min_of: str
    max_of: str
    sum_of: str
    raises: str  # condition, sqlstate and message's text, answer
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
    min_of="list_min({})",
    max_of="list_max({})",
    sum_of="list_sum({})",
    raises=f"CASE WHEN {{0}} THEN error('{MARK} {{1}}') ELSE {{2}} END",
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
    min_of="array_min({})",
    max_of="array_max({})",
    sum_of="reduce({}, DOUBLE '0', (_s, _x) -> _s + _x, _s -> _s)",
    raises=f"CASE WHEN {{0}} THEN fail('{MARK} {{1}}') ELSE {{2}} END",
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
    min_of="arrayMin({})",
    max_of="arrayMax({})",
    sum_of="arraySum({})",
    # throwIf inside a lambda runs on every row: it is the condition that decides.
    raises="if(throwIf(ifNull({0}, 0), '" + MARK + " {1}') = 0, {2}, {2})",
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
    larger, smaller = "greatest(_h[1], _h[2])", "least(_h[1], _h[2])"
    return e.let.format(
        "_h",
        _list(e, f"abs({dx})", f"abs({dy})"),
        f"CASE WHEN {smaller} = 0 THEN {larger} ELSE {larger} * "
        f"sqrt(1.0 + ({smaller} / {larger}) * ({smaller} / {larger})) END",
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


def _slope(a: tuple[str, str], b: tuple[str, str]) -> str:
    return (
        f"CASE WHEN {_fp_eq(a[0], b[0])} THEN {_MAX} WHEN {_fp_eq(a[1], b[1])} THEN 0.0 "
        f"ELSE ({a[1]} - {b[1]}) / ({a[0]} - {b[0]}) END"
    )


def _line_through(e: Engine, f: str) -> str:
    """line_construct_pp of the points ``_a`` and ``_b``."""
    p, q = _xy("_a"), _xy("_b")
    same = f"{_fp_eq(p[0], q[0])} AND {_fp_eq(p[1], q[1])}"
    c = "_m * " + p[0]
    line = e.let.format(
        "_m",
        _slope(p, q),
        f"CASE WHEN _m = {_MAX} THEN {_list(e, '-1.0', '0.0', p[0])} "
        f"WHEN _m = 0 THEN {_list(e, '0.0', '-1.0', p[1])} "
        f"ELSE {_list(e, '_m', '-1.0', f'CASE WHEN {p[1]} - {c} = 0 THEN 0.0 ELSE {p[1]} - {c} END')} END",
    )
    return e.raises.format(
        same,
        "22P02: invalid line specification: must be two distinct points",
        _line_text(e, line, f),
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
    add(
        "circle_box",
        "a:circle",
        0,
        lambda e, f: e.let.format(
            "_w",
            f"{ar} / sqrt(2.0)",
            _shape(e, _list(e, f"{ax} + _w", f"{ay} + _w", f"{ax} - _w", f"{ay} - _w"), *_BOX, f),
        ),
    )


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
    add("poly_box", "a:polygon", 0, lambda e, f: _shape(e, box(e, "_a"), *_BOX, f))
    add(
        "poly_center",
        "a:polygon",
        0,
        lambda e, f: _shape(e, _list(e, *_poly_center(e, "_a")), *_POINT, f),
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
    add("slope", "a:point, b:point", 2, lambda e, f: _double(e, _slope(p, q), f))
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

    def contains(outer: str, inner: str) -> str:
        return (
            f"{_fp_ge(f'{outer}[1]', f'{inner}[1]')} AND {_fp_le(f'{outer}[3]', f'{inner}[3]')} "
            f"AND {_fp_ge(f'{outer}[2]', f'{inner}[2]')} AND {_fp_le(f'{outer}[4]', f'{inner}[4]')}"
        )

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
        ("contain", contains("_a", "_b")),
        ("contained", contains("_b", "_a")),
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
    add("point_of_circle", "a:circle", 0, lambda e, f: _shape(e, _list(e, p[0], p[1]), *_POINT, f))
    _circles(add)
    _lists(add)
    return rows


ROWS = tuple(_rows())
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
}
#: A name whose form is its only overload of that many arguments.
ALIASES = {
    ("box", "a, b"): "points_box",
    ("lseg", "a, b"): "lseg_of",
    ("line", "a, b"): "line_through",
    ("circle", "a, r"): "circle_of",
    ("polygon", "n, a"): "circle_poly",
}
BARE = (("box", "a"), ("circle", "a"), ("point", "a"), ("polygon", "a"), ("npoints", "a"))


def _form(e: Engine, row: Row) -> str:
    kinds = [tuple(p.split(":")) for p in row.params.split(", ")]
    shapes = [(p, kind) for p, kind in kinds if kind not in ("double", "integer")]
    body = row.body(e, row.name)
    for param, kind in reversed(shapes):
        body = e.let.format(f"_{param}", _read(e, param, kind), body)
    letters = " OR ".join(e.matches.format(e.text.format(p), _NOT_FINITE) for p, _ in shapes)
    nulls = " OR ".join(f"{p} IS NULL" for p, _ in kinds)
    sql_type = e.types[row.answers]
    refused = e.raises.format(letters, f"0A000: {row.name} {REFUSED}", body)
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {refused} END"


def forms(e: Engine) -> dict[tuple[str, str], str]:
    """Each row's form on ``e``: (name, its parameters) -> the SQL."""
    return {
        (r.name, ", ".join(p.split(":")[0] for p in r.params.split(", "))): _form(e, r)
        for r in ROWS
    }
