# Copyright (c) 2026 Kenneth Stott
# Canary: b7d3e1a4-92c6-4f08-8e15-6a0c47d2f931
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's point and box functions as forms (REQ-2167, REQ-2171), ported from geo_ops.c so a form
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
    infinity="CAST('Infinity' AS DOUBLE)",
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
    infinity="infinity()",
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
    infinity="toFloat64('inf')",
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


def _slope(e: Engine, a: tuple[str, str], b: tuple[str, str], f: str) -> str:
    """point_sl: a vertical line's slope is infinite, and so is that of a point and itself."""
    finite = (
        f"CASE WHEN {_fp_eq(a[1], b[1])} THEN 0.0 ELSE ({a[1]} - {b[1]}) / ({a[0]} - {b[0]}) END"
    )
    return f"CASE WHEN {_fp_eq(a[0], b[0])} THEN {e.infinity} ELSE {_double(e, finite, f)} END"


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
    return rows


ROWS = tuple(_rows())
#: PostgreSQL's overloads of a name its arguments' types tell apart -> the row of each.
OVERLOADS: dict[tuple[str, int, str], str] = {
    ("box", 1, "point"): "point_box",
    ("box", 1, "circle"): "circle_box",
    ("box", 1, "polygon"): "poly_box",
    ("point", 1, "box"): "box_center",
    ("point", 1, "circle"): "point_of_circle",
    ("point", 1, "lseg"): "lseg_center",
    ("point", 1, "polygon"): "poly_center",
}
#: A name whose form is its only overload of that many arguments.
ALIASES = {("box", "a, b"): "points_box", ("point", "x, y"): "point_of"}
BARE = (("box", "a"), ("point", "a"))


def _form(e: Engine, row: Row) -> str:
    kinds = [tuple(p.split(":")) for p in row.params.split(", ")]
    shapes = [(p, kind) for p, kind in kinds if kind not in ("double", "integer")]
    body = row.body(e, row.name)
    for param, kind in reversed(shapes):
        body = e.let.format(f"_{param}", _read(e, param, kind), body)
    letters = " OR ".join(e.matches.format(e.text.format(p), _NOT_FINITE) for p, _ in shapes)
    nulls = " OR ".join(f"{p} IS NULL" for p, _ in kinds)
    sql_type = e.types[row.answers]
    refused = e.raises.format(letters, f"0A000: {row.name} {REFUSED}", body) if shapes else body
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {refused} END"


def forms(e: Engine) -> dict[tuple[str, str], str]:
    """Each row's form on ``e``: (name, its parameters) -> the SQL."""
    return {
        (r.name, ", ".join(p.split(":")[0] for p in r.params.split(", "))): _form(e, r)
        for r in ROWS
    }
