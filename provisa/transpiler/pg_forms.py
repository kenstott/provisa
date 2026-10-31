# Copyright (c) 2026 Kenneth Stott
# Canary: 5662bd03-ad57-4ee3-932e-d1c458b01b06
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SQL of the forms of PostgreSQL's shapes, tsvector, inet and money, for the rows of
provisa.transpiler.function_macros. A value of a type a dialect does not have is held in
PostgreSQL's text form, so each form reads its parts from that text. Trino's form of a shape is its
geometry: the shape's points written as WKT, and Trino's ST_ function answers. A number here is
SQL over a parameter's name, valid on both dialects."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Sequence

from provisa.core.pg_error import raising

_FUZZ = "1e-06"  # geo_decls.h's EPSILON: PostgreSQL's geometric comparisons allow it
_NUMBER = r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?"


def numbers(value: str) -> str:
    """A shape's numbers as text, in the order its text form writes them."""
    return f"regexp_extract_all(CAST({value} AS VARCHAR), '{_NUMBER}')"


def number(value: str, position: int) -> str:
    return f"CAST({numbers(value)}[{position}] AS DOUBLE)"


def _opens(value: str, char: str) -> str:
    return f"starts_with(ltrim(CAST({value} AS VARCHAR)), '{char}')"


def is_closed(value: str) -> str:
    return _opens(value, "(")


def _is_open(value: str) -> str:
    return _opens(value, "[")


def _is_circle(value: str) -> str:
    return _opens(value, "<")


def _is_line(value: str) -> str:
    return _opens(value, "{")


# --- the direction of a line segment or a line ------------------------------------------------


def _lseg_direction(value: str) -> tuple[str, str]:
    return f"{number(value, 3)} - {number(value, 1)}", f"{number(value, 4)} - {number(value, 2)}"


def _line_direction(value: str) -> tuple[str, str]:
    return number(value, 2), f"-{number(value, 1)}"


def _direction(value: str) -> tuple[str, str]:
    """For a value whose type the statement does not say: its text says which it is."""
    lseg, line = _lseg_direction(value), _line_direction(value)
    return (
        f"CASE WHEN {_is_line(value)} THEN {line[0]} ELSE {lseg[0]} END",
        f"CASE WHEN {_is_line(value)} THEN {line[1]} ELSE {lseg[1]} END",
    )


_DIRECTIONS = {"lseg": _lseg_direction, "line": _line_direction, None: _direction}


def horizontal(of: str | None) -> str:
    return f"abs({_DIRECTIONS[of]('value')[1]}) <= {_FUZZ}"


def vertical(of: str | None) -> str:
    return f"abs({_DIRECTIONS[of]('value')[0]}) <= {_FUZZ}"


def parallel(of: str | None) -> str:
    (ax, ay), (bx, by) = _DIRECTIONS[of]("a"), _DIRECTIONS[of]("b")
    return f"abs(({ax}) * ({by}) - ({ay}) * ({bx})) <= {_FUZZ}"


def perpendicular(of: str | None) -> str:
    (ax, ay), (bx, by) = _DIRECTIONS[of]("a"), _DIRECTIONS[of]("b")
    return f"abs(({ax}) * ({bx}) + ({ay}) * ({by})) <= {_FUZZ}"


def points_differ_by_nothing(index: int) -> str:
    return f"abs({number('a', index)} - {number('b', index)}) <= {_FUZZ}"


# --- DuckDB: a shape's measures, over its numbers ------------------------------------------------

_DOUBLES = f"list_transform({numbers('value')}, _n -> CAST(_n AS DOUBLE))"
_POINT_TEXT = "format('({{:g}},{{:g}})', {x}, {y})"
_BOX_X = (number("value", 1), number("value", 3))
_BOX_Y = (number("value", 2), number("value", 4))

BOX_AREA = f"abs({_BOX_X[0]} - {_BOX_X[1]}) * abs({_BOX_Y[0]} - {_BOX_Y[1]})"
CIRCLE_AREA = f"pi() * pow({number('value', 3)}, 2)"
_SHOELACE = (
    f"list_transform([{_DOUBLES}], _p -> abs(list_sum(list_transform(range(1, len(_p), 2), "
    "_i -> _p[_i] * _p[(_i + 1) % len(_p) + 2] - _p[(_i + 1) % len(_p) + 1] * _p[_i + 1]))) "
    "/ 2.0)[1]"
)
PATH_AREA = f"CASE WHEN NOT {_is_open('value')} THEN {_SHOELACE} END"
#: A value whose type the statement does not say: two points are a box's.
AREA = (
    f"CASE WHEN {_is_circle('value')} THEN {CIRCLE_AREA} WHEN {_is_open('value')} THEN NULL "
    f"WHEN len({numbers('value')}) = 4 THEN {BOX_AREA} ELSE {_SHOELACE} END"
)
BOX_CENTER = _POINT_TEXT.format(
    x=f"({_BOX_X[0]} + {_BOX_X[1]}) / 2.0", y=f"({_BOX_Y[0]} + {_BOX_Y[1]}) / 2.0"
)
CIRCLE_CENTER = _POINT_TEXT.format(x=number("value", 1), y=number("value", 2))
CENTER = f"CASE WHEN {_is_circle('value')} THEN {CIRCLE_CENTER} ELSE {BOX_CENTER} END"
#: Cast, as a NULL box leaves the concatenation no type of its own for pgwire to describe.
BOX_DIAGONAL = (
    "CAST('[' || "
    + _POINT_TEXT.format(x=f"greatest({', '.join(_BOX_X)})", y=f"greatest({', '.join(_BOX_Y)})")
    + " || ',' || "
    + _POINT_TEXT.format(x=f"least({', '.join(_BOX_X)})", y=f"least({', '.join(_BOX_Y)})")
    + " || ']' AS VARCHAR)"
)

_STEP = "sqrt(pow(_p[_i + 2] - _p[_i], 2) + pow(_p[_i + 3] - _p[_i + 1], 2))"
LSEG_LENGTH = (
    f"sqrt(pow({number('value', 3)} - {number('value', 1)}, 2) + "
    f"pow({number('value', 4)} - {number('value', 2)}, 2))"
)
PATH_LENGTH = (
    f"CASE WHEN value IS NOT NULL THEN list_transform([{_DOUBLES}], _q -> list_transform(["
    f"CASE WHEN {is_closed('value')} AND len(_q) > 2 THEN list_concat(_q, _q[1:2]) ELSE _q END], "
    f"_p -> coalesce(list_sum(list_transform(range(1, len(_p) - 1, 2), _i -> {_STEP})), 0.0))[1])"
    "[1] END"
)
POINT_IN_CIRCLE = (
    f"sqrt(pow({number('p', 1)} - {number('c', 1)}, 2) + "
    f"pow({number('p', 2)} - {number('c', 2)}, 2)) <= {number('c', 3)} + {_FUZZ}"
)
_EDGES = "range(1, len(_c), 2)"
_X1, _Y1 = "_c[_i]", "_c[_i + 1]"
_X2, _Y2 = "_c[(_i + 1) % len(_c) + 1]", "_c[(_i + 1) % len(_c) + 2]"
_EDGE_LENGTH = f"sqrt(pow({_X2} - {_X1}, 2) + pow({_Y2} - {_Y1}, 2))"
# A CASE, as an AND guards no division in a lambda: both sides are evaluated.
_ON_EDGE = (
    f"CASE WHEN {_EDGE_LENGTH} > 0 THEN "
    f"abs((_x - {_X1}) * ({_Y2} - {_Y1}) - (_y - {_Y1}) * ({_X2} - {_X1})) / {_EDGE_LENGTH} "
    f"<= {_FUZZ} AND (_x - {_X1}) * ({_X2} - {_X1}) + (_y - {_Y1}) * ({_Y2} - {_Y1}) "
    f"BETWEEN 0 AND pow({_EDGE_LENGTH}, 2) ELSE false END"
)
_CROSSES = (
    f"CASE WHEN ({_Y1} > _y) <> ({_Y2} > _y) THEN "
    f"_x < ({_X2} - {_X1}) * (_y - {_Y1}) / ({_Y2} - {_Y1}) + {_X1} ELSE false END"
)
#: On an edge, or inside by the even-odd rule.
POINT_IN_POLYGON = (
    f"list_transform([{_DOUBLES.replace('value', 'poly')}], _c -> "
    f"list_transform([{number('p', 1)}], _x -> list_transform([{number('p', 2)}], _y -> "
    f"len(list_filter({_EDGES}, _i -> {_ON_EDGE})) > 0 OR "
    f"len(list_filter({_EDGES}, _i -> {_CROSSES})) % 2 = 1)[1])[1])[1]"
)


# --- Trino: a shape as its geometry -------------------------------------------------------------


def _coordinates(value: str) -> str:
    return (
        f"array_join(transform(sequence(1, cardinality({numbers(value)}), 2), _i -> "
        f"{numbers(value)}[_i] || ' ' || {numbers(value)}[_i + 1]), ', ')"
    )


def _first_point(value: str) -> str:
    return f"{numbers(value)}[1] || ' ' || {numbers(value)}[2]"


def _geometry(kind: str, value: str, closing: str = "") -> str:
    return f"ST_GeometryFromText('{kind} (' || {_coordinates(value)}{closing} || ')')"


def _polygon(value: str) -> str:
    ring = f"'(' || {_coordinates(value)} || ', ' || {_first_point(value)} || ')'"
    return f"ST_GeometryFromText('POLYGON (' || {ring} || ')')"


def _envelope(value: str) -> str:
    return f"ST_Envelope({_geometry('MULTIPOINT', value)})"


def _float_text(value: str) -> str:
    return f"regexp_replace(format('%.6f', {value}), '\\.?0+$', '')"


def _trino_point(x: str, y: str) -> str:
    return f"'(' || {_float_text(x)} || ',' || {_float_text(y)} || ')'"


_CENTROID = f"ST_Centroid({_envelope('value')})"
TRINO_BOX_AREA = f"ST_Area({_envelope('value')})"
TRINO_PATH_AREA = f"CASE WHEN NOT {_is_open('value')} THEN ST_Area({_polygon('value')}) END"
TRINO_AREA = (
    f"CASE WHEN {_is_circle('value')} THEN {CIRCLE_AREA} WHEN {_is_open('value')} THEN NULL "
    f"WHEN cardinality({numbers('value')}) = 4 THEN {TRINO_BOX_AREA} "
    f"ELSE ST_Area({_polygon('value')}) END"
)
TRINO_BOX_CENTER = _trino_point(f"ST_X({_CENTROID})", f"ST_Y({_CENTROID})")
TRINO_CIRCLE_CENTER = _trino_point(number("value", 1), number("value", 2))
TRINO_CENTER = (
    f"CASE WHEN {_is_circle('value')} THEN {TRINO_CIRCLE_CENTER} ELSE {TRINO_BOX_CENTER} END"
)
TRINO_BOX_DIAGONAL = (
    "'[' || "
    + _trino_point(f"ST_XMax({_envelope('value')})", f"ST_YMax({_envelope('value')})")
    + " || ',' || "
    + _trino_point(f"ST_XMin({_envelope('value')})", f"ST_YMin({_envelope('value')})")
    + " || ']'"
)
TRINO_WIDTH = f"ST_XMax({_envelope('value')}) - ST_XMin({_envelope('value')})"
TRINO_HEIGHT = f"ST_YMax({_envelope('value')}) - ST_YMin({_envelope('value')})"
TRINO_LSEG_LENGTH = f"ST_Length({_geometry('LINESTRING', 'value')})"
TRINO_PATH_LENGTH = (
    "ST_Length("
    + _geometry(
        "LINESTRING",
        "value",
        f" || CASE WHEN {is_closed('value')} THEN ', ' || {_first_point('value')} ELSE '' END",
    )
    + ")"
)
_TRINO_POINT = f"ST_Point({number('p', 1)}, {number('p', 2)})"
TRINO_POINT_IN_CIRCLE = (
    f"ST_Distance({_TRINO_POINT}, ST_Point({number('c', 1)}, {number('c', 2)})) "
    f"<= {number('c', 3)} + {_FUZZ}"
)
TRINO_POINT_IN_POLYGON = f"ST_Intersects({_polygon('poly')}, {_TRINO_POINT})"


# --- tsvector -----------------------------------------------------------------------------------

_ITEM = "'''(?:[^'']|'''')*''(?::[0-9A-D,]+)?|\\S+'"
_MARKS = "'^(.*?)(?::[0-9A-D,]+)?$'"
_MARKED = "':([0-9A-D,]+)$'"
_TOKENS = f"regexp_extract_all(CAST(value AS VARCHAR), {_ITEM})"
_BARE = f"regexp_extract(_t, {_MARKS}, 1)"
#: A token's lexeme: a quoted one's quotes removed and its doubled quotes read.
_LEXEME = (
    f"CASE WHEN starts_with(_t, '''') THEN replace(trim({_BARE}, ''''), '''''', '''') "
    f"ELSE {_BARE} END"
)
_MARK_LIST = (
    f"list_filter(string_split(regexp_extract(_t, {_MARKED}, 1), ','), _m -> length(_m) > 0)"
)
_ITEMS = (
    f"list_sort(list_transform({_TOKENS}, _t -> struct_pack(lexeme := {_LEXEME}, "
    f"positions := nullif(list_transform({_MARK_LIST}, "
    "_m -> CAST(rtrim(_m, 'ABCD') AS SMALLINT)), []), "
    f"weights := nullif(list_transform({_MARK_LIST}, _m -> "
    "CASE WHEN right(_m, 1) IN ('A', 'B', 'C') THEN right(_m, 1) ELSE 'D' END), []))))"
)
_QUOTED_LEXEME = "'''' || replace(_l, '''', '''''') || ''''"

TSVECTOR_LENGTH = f"len({_TOKENS})"
TSVECTOR_UNNEST = f"unnest({_ITEMS})"
TSVECTOR_STRIP = (
    f"array_to_string(list_transform(list_sort(list_transform({_TOKENS}, _t -> {_LEXEME})), "
    f"_l -> {_QUOTED_LEXEME}), ' ')"
)
TRINO_TSVECTOR_LENGTH = f"cardinality({_TOKENS})"
TRINO_TSVECTOR_STRIP = (
    f"array_join(transform(array_sort(transform({_TOKENS}, _t -> {_LEXEME})), "
    f"_l -> {_QUOTED_LEXEME}), ' ')"
)


# --- inet ---------------------------------------------------------------------------------------

_ADDRESS = "CAST(value AS VARCHAR)"
_MASK = f"split_part({_ADDRESS}, '/', 2)"
_HOST = f"split_part({_ADDRESS}, '/', 1)"
_IS_V6 = f"strpos({_ADDRESS}, ':') > 0"
INET_ABBREV = (
    f"CASE WHEN {_MASK} IN ('', CASE WHEN {_IS_V6} THEN '128' ELSE '32' END) "
    f"THEN {_HOST} ELSE {_ADDRESS} END"
)
_KEPT = f"greatest(CAST(ceil(CAST({_MASK} AS INTEGER) / 8.0) AS INTEGER), 1)"
_WHOLE = f"{_IS_V6} OR strpos({_ADDRESS}, '/') = 0"
CIDR_ABBREV = (
    f"CASE WHEN {_WHOLE} THEN {_ADDRESS} ELSE "
    f"array_to_string(string_split({_HOST}, '.')[1:{_KEPT}], '.') || '/' || {_MASK} END"
)
TRINO_CIDR_ABBREV = (
    f"CASE WHEN {_WHOLE} THEN {_ADDRESS} ELSE "
    f"array_join(slice(split({_HOST}, '.'), 1, {_KEPT}), '.') || '/' || {_MASK} END"
)


# --- money --------------------------------------------------------------------------------------

_CENTS = "CAST(round((value) * 100, 0) AS BIGINT)"
_WIDE_CENTS = f"CAST({_CENTS} AS HUGEINT)"

MONEY_TEXT = (
    f"CASE WHEN {_CENTS} < 0 THEN '-' ELSE '' END || '$' || "
    f"format('{{:,}}', abs({_WIDE_CENTS}) // 100) || '.' || "
    f"printf('%02d', CAST(abs({_WIDE_CENTS}) % 100 AS INTEGER))"
)
TRINO_MONEY_TEXT = (
    "CASE WHEN value < 0 THEN '-' ELSE '' END || "
    "format('$%,.2f', abs(CAST(value AS DECIMAL(19,2))))"
)
MONEY_BYTES = f"unhex(printf('%016x', {_CENTS}))"
TRINO_MONEY_BYTES = f"to_big_endian_64({_CENTS})"

_SMALL = (
    "zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen "
    "fifteen sixteen seventeen eighteen nineteen twenty"
).split()
_TENS = "x x twenty thirty forty fifty sixty seventy eighty ninety".split()
_SCALES = ("quadrillion", "trillion", "billion", "million", "thousand")


def _list(words: Sequence[str]) -> str:
    return "[" + ", ".join(f"'{w}'" for w in words) + "]"


#: cash.c's num_word, of _g below a thousand, over the words bound to _s and _e.
_HUNDREDS = "CASE WHEN _g > 99 THEN _s[_g // 100 + 1] || ' hundred ' ELSE '' END"
_WORD = (
    "CASE WHEN _g <= 20 THEN _s[_g + 1] "
    "WHEN _g % 100 = 0 THEN _s[_g // 100 + 1] || ' hundred' "
    f"WHEN _g % 10 = 0 AND _g % 100 > 10 THEN {_HUNDREDS} || _e[_g % 100 // 10 + 1] "
    "WHEN _g % 100 < 20 AND _g > 99 THEN _s[_g // 100 + 1] || ' hundred and ' || _s[_g % 100 + 1] "
    "WHEN _g % 100 < 20 THEN _s[_g % 100 + 1] "
    f"ELSE {_HUNDREDS} || _e[_g % 100 // 10 + 1] || ' ' || _s[_g % 10 + 1] END"
)
_GROUPS = (
    "list_concat(list_transform([1000000000000000, 1000000000000, 1000000000, 1000000, 1000, 1], "
    "_u -> CAST((abs(_c) // 100) // _u % 1000 AS INTEGER)), [CAST(abs(_c) % 100 AS INTEGER)])"
)
_SCALED = (
    "array_to_string(list_transform(range(1, 6), _i -> CASE WHEN _a[_i] > 0 "
    f"THEN _w[_i] || ' ' || {_list(_SCALES)}[_i] || ' ' ELSE '' END), '')"
)
_SENTENCE = (
    f"CASE WHEN _c < 0 THEN 'minus ' ELSE '' END || {_SCALED} || "
    "CASE WHEN _a[6] > 0 THEN _w[6] ELSE '' END || "
    "CASE WHEN abs(_c) // 100 = 0 THEN 'zero' ELSE '' END || "
    "CASE WHEN abs(_c) // 100 = 1 THEN ' dollar and ' ELSE ' dollars and ' END || _w[7] || "
    "CASE WHEN _a[7] = 1 THEN ' cent' ELSE ' cents' END"
)
_WORDS = (
    f"list_transform([{_list(_SMALL)}], _s -> list_transform([{_list(_TENS)}], _e -> "
    f"list_transform([{_GROUPS}], _a -> list_transform([list_transform(_a, _g -> {_WORD})], "
    f"_w -> list_transform([{_SENTENCE}], _o -> upper(_o[1]) || _o[2:])[1])[1])[1])[1])[1]"
)
MONEY_WORDS = (
    f"CASE WHEN value IS NOT NULL THEN list_transform([{_WIDE_CENTS}], _c -> {_WORDS})[1] END"
)

_SPACE = r"[ \t\n\r\f\v]*"
#: money's input: its sign, whole digits, up to two decimals, the digits past them and what ends it.
_MONEY_INPUT = (
    rf"^{_SPACE}\$?{_SPACE}([-(+]?){_SPACE}\$?{_SPACE}([0-9,]*)(\.,*(?:[0-9],*){{0,2}})?([0-9]*)"
    r"([ \t\n\r\f\v)+$-]*)$"
)
_INVALID = raising("22P02", "'invalid input syntax for type money: \"' || _t || '\"'")
_OUT_OF_RANGE = raising("22003", "'value \"' || _t || '\" is out of range for type money'")
_PARTS = f"regexp_extract(_t, '{_MONEY_INPUT}', ['sign', 'whole', 'part', 'past', 'tail'])"
_DIGITS = (
    "ltrim(replace(struct_extract(_m, 'whole'), ',', '') || "
    "rpad(regexp_replace(struct_extract(_m, 'part'), '[.,]', '', 'g'), 2, '0'), '0')"
)
_NEGATIVE = (
    "struct_extract(_m, 'sign') IN ('-', '(') OR strpos(struct_extract(_m, 'tail'), '-') > 0"
)
_ROUNDED = (
    "CAST('0' || _d AS HUGEINT) + CASE WHEN left(struct_extract(_m, 'past'), 1) >= '5' "
    "THEN 1 ELSE 0 END"
)
_SIGNED = f"CASE WHEN {_NEGATIVE} THEN -({_ROUNDED}) ELSE {_ROUNDED} END"
_READ = (
    f"list_transform([{_PARTS}], _m -> list_transform([{_DIGITS}], _d -> CASE WHEN length(_d) > 19 "
    f"THEN {_OUT_OF_RANGE} ELSE list_transform([{_SIGNED}], _c -> "
    f"CASE WHEN _c < -9223372036854775808 OR _c > 9223372036854775807 THEN {_OUT_OF_RANGE} "
    "ELSE CAST(CAST(_c AS DECIMAL(19,0)) * 0.01 AS DECIMAL(21,4)) END)[1] END)[1])[1]"
)
MONEY_INPUT = (
    "CASE WHEN value IS NOT NULL THEN list_transform([CAST(value AS VARCHAR)], _t -> "
    f"CASE WHEN NOT regexp_matches(_t, '{_MONEY_INPUT}') THEN {_INVALID} ELSE {_READ} END)[1] END"
)


# --- numbers ------------------------------------------------------------------------------------


def bound(name: str, value: str, body: str) -> str:
    """``body`` with ``name`` the value of ``value``, which it reads as often as it needs."""
    return f"list_transform([{value}], {name} -> {body})[1]"


def _refused(sqlstate: str, message: str) -> str:
    return raising(sqlstate, f"'{message}'")


_DIVISION_BY_ZERO = _refused("22012", "division by zero")
_OVERFLOW = _refused("22003", "value out of range: overflow")
_UNDERFLOW = _refused("22003", "value out of range: underflow")
_OUT_OF_DOMAIN = _refused("22003", "input is out of range")
#: Each integer type: its name in the engine and PostgreSQL's.
PG_INTEGERS = {"SMALLINT": "smallint", "INTEGER": "integer", "BIGINT": "bigint"}
PG_FLOATS = {"FLOAT": "real", "DOUBLE": "double precision"}


def of_any_number(value: str, json_form: str, answer: str) -> str:
    """A number held in a jsonb the statement did not say is one: the engine's type says so."""
    return f"CASE WHEN typeof({value}) = 'JSON' THEN {json_form} ELSE {answer} END"


def in_range(value: str, sql_type: str) -> str:
    """``value`` as an integer type, or PostgreSQL's refusal of one it does not fit: a value of a
    wider type, a NaN or an infinity."""
    refused = _refused("22003", f"{PG_INTEGERS[sql_type]} out of range")
    return (
        f"CASE WHEN try_cast({value} AS {sql_type}) IS NULL THEN {refused} "
        f"ELSE try_cast({value} AS {sql_type}) END"
    )


def integer_arithmetic(operator: str, left: str, right: str, result: str) -> str:
    """int24pl and its kin: the integers widened past their result, which is then range-checked.
    ``//`` truncates toward zero, as int4div does."""
    wide = "HUGEINT" if result == "BIGINT" else "BIGINT"
    a, b = f"CAST(CAST(a AS {left}) AS {wide})", f"CAST(CAST(b AS {right}) AS {wide})"
    answer = in_range(f"{a} {operator} {b}", result)
    return (
        f"CASE WHEN {b} = 0 THEN {_DIVISION_BY_ZERO} ELSE {answer} END"
        if operator == "//"
        else answer
    )


def _float_operands(left: str, right: str) -> tuple[str, str]:
    return f"CAST(CAST(a AS {left}) AS DOUBLE)", f"CAST(CAST(b AS {right}) AS DOUBLE)"


def float_comparison(operator: str, left: str, right: str) -> str:
    """float48eq and its kin: the real widened, and the engine's NaN equal to itself and above
    every number, as PostgreSQL's is."""
    a, b = _float_operands(left, right)
    return f"{a} {operator} {b}"


def float_arithmetic(operator: str, left: str, right: str) -> str:
    """float8_pl, float8_mi, float8_mul and float8_div (utils/float.h): an infinity or a zero the
    operands do not explain is refused."""
    a, b = _float_operands(left, right)
    checks = {
        "+": f"isinf(_r) AND NOT isinf({a}) AND NOT isinf({b}) THEN {_OVERFLOW}",
        "-": f"isinf(_r) AND NOT isinf({a}) AND NOT isinf({b}) THEN {_OVERFLOW}",
        "*": (
            f"isinf(_r) AND NOT isinf({a}) AND NOT isinf({b}) THEN {_OVERFLOW} "
            f"WHEN _r = 0 AND {a} <> 0 AND {b} <> 0 THEN {_UNDERFLOW}"
        ),
        "/": (
            f"isinf(_r) AND NOT isinf({a}) THEN {_OVERFLOW} "
            f"WHEN _r = 0 AND {a} <> 0 AND NOT isinf({b}) THEN {_UNDERFLOW}"
        ),
    }
    answer = bound("_r", f"{a} {operator} {b}", f"CASE WHEN {checks[operator]} ELSE _r END")
    if operator == "/":
        return f"CASE WHEN {b} = 0 THEN CASE WHEN isnan({a}) THEN {a} ELSE {_DIVISION_BY_ZERO} END ELSE {answer} END"
    return answer


_X = "CAST(x AS DOUBLE)"
_Y = "CAST(y AS DOUBLE)"

DEXP = (
    f"CASE WHEN isnan({_X}) OR isinf({_X}) THEN exp({_X}) ELSE "
    + bound(
        "_r",
        f"exp({_X})",
        f"CASE WHEN isinf(_r) THEN {_OVERFLOW} WHEN _r = 0 THEN {_UNDERFLOW} ELSE _r END",
    )
    + " END"
)


def _logarithm(function: str) -> str:
    return (
        f"CASE WHEN {_X} = 0 THEN {_refused('2201E', 'cannot take logarithm of zero')} "
        f"WHEN {_X} < 0 THEN {_refused('2201E', 'cannot take logarithm of a negative number')} "
        f"ELSE {function}({_X}) END"
    )


DLOG1 = _logarithm("ln")
DLOG10 = _logarithm("log10")
DSQRT = (
    f"CASE WHEN {_X} < 0 THEN {_refused('2201F', 'cannot take square root of a negative number')} "
    f"ELSE sqrt({_X}) END"
)
#: dpow: POSIX's pow where an infinity or a NaN is in it, which the engine's follows, and the
#: errors PostgreSQL gives where one is not.
DPOW = (
    f"CASE WHEN {_X} = 0 AND {_Y} < 0 THEN "
    + _refused("2201F", "zero raised to a negative power is undefined")
    + f" WHEN {_X} < 0 AND NOT isinf({_X}) AND NOT isinf({_Y}) AND floor({_Y}) <> {_Y} THEN "
    + _refused("2201F", "a negative number raised to a non-integer power yields a complex result")
    + f" WHEN {_X} < 0 AND isinf({_X}) AND NOT isnan({_Y}) AND NOT isinf({_Y}) "
    f"AND floor({_Y}) <> {_Y} THEN "
    + _refused("2201F", "a negative number raised to a non-integer power yields a complex result")
    + f" WHEN isnan({_X}) OR isnan({_Y}) OR isinf({_X}) OR isinf({_Y}) THEN pow({_X}, {_Y}) ELSE "
    + bound(
        "_r",
        f"pow({_X}, {_Y})",
        f"CASE WHEN isinf(_r) THEN {_OVERFLOW} WHEN _r = 0 AND {_X} <> 0 THEN {_UNDERFLOW} "
        "ELSE _r END",
    )
    + " END"
)

# float.c's degree functions: each angle is folded into the first quadrant, where a ratio of two
# of the engine's own results makes the exact angles exact. % is C's fmod; the engine's fmod is not.
_RADIANS_PER_DEGREE = "1.7453292519943295e-02"


def _ratio(a: str, b: str) -> str:
    return f"(({a}) / ({b}))"


def _sind_0_to_30(x: str) -> str:
    return (
        _ratio(f"sin({x} * {_RADIANS_PER_DEGREE})", f"sin(30.0 * {_RADIANS_PER_DEGREE})") + " * 0.5"
    )


def _cosd_0_to_60(x: str) -> str:
    one_minus_cos = _ratio(
        f"(1.0 - cos({x} * {_RADIANS_PER_DEGREE}))", f"(1.0 - cos(60.0 * {_RADIANS_PER_DEGREE}))"
    )
    return f"1.0 - {one_minus_cos} * 0.5"


def _sind_q1(x: str) -> str:
    return (
        f"CASE WHEN {x} <= 30.0 THEN {_sind_0_to_30(x)} ELSE {_cosd_0_to_60(f'(90.0 - {x})')} END"
    )


def _cosd_q1(x: str) -> str:
    return (
        f"CASE WHEN {x} <= 60.0 THEN {_cosd_0_to_60(x)} ELSE {_sind_0_to_30(f'(90.0 - {x})')} END"
    )


def _asind_q1(x: str) -> str:
    return (
        f"CASE WHEN {x} <= 0.5 THEN {_ratio(f'asin({x})', 'asin(0.5)')} * 30.0 "
        f"ELSE 90.0 - {_ratio(f'acos({x})', 'acos(0.5)')} * 60.0 END"
    )


def _acosd_q1(x: str) -> str:
    return (
        f"CASE WHEN {x} <= 0.5 THEN 90.0 - {_ratio(f'asin({x})', 'asin(0.5)')} * 30.0 "
        f"ELSE {_ratio(f'acos({x})', 'acos(0.5)')} * 60.0 END"
    )


def _finite_angle(answer: str) -> str:
    return (
        f"CASE WHEN isnan({_X}) THEN {_X} WHEN isinf({_X}) THEN {_OUT_OF_DOMAIN} ELSE {answer} END"
    )


COSD = _finite_angle(
    bound(
        "_a",
        f"abs({_X} % 360.0)",
        bound(
            "_b",
            "CASE WHEN _a > 180.0 THEN 360.0 - _a ELSE _a END",
            f"CASE WHEN _b > 90.0 THEN -({_cosd_q1('(180.0 - _b)')}) ELSE {_cosd_q1('_b')} END",
        ),
    )
)
_COT_45 = _ratio(f"({_cosd_q1('45.0')})", f"({_sind_q1('45.0')})")
#: dcotd: a minus zero is forced to a plain zero, as the function does.
COTD = _finite_angle(
    bound(
        "_a",
        f"{_X} % 360.0",
        bound(
            "_b",
            "abs(_a)",
            bound(
                "_c",
                "CASE WHEN _b > 180.0 THEN 360.0 - _b ELSE _b END",
                bound(
                    "_d",
                    "CASE WHEN _c > 90.0 THEN 180.0 - _c ELSE _c END",
                    bound(
                        "_r",
                        "(CASE WHEN _a < 0 THEN -1.0 ELSE 1.0 END) * "
                        "(CASE WHEN _b > 180.0 THEN -1.0 ELSE 1.0 END) * "
                        "(CASE WHEN _c > 90.0 THEN -1.0 ELSE 1.0 END) * "
                        # fmod keeps a dividend's minus sign on a zero, and cos over it is -Infinity.
                        "(CASE WHEN _d = 0 THEN CAST(CASE WHEN _a = 0 AND atan2(_a, -1.0) < 0 "
                        "THEN '-Infinity' ELSE 'Infinity' END AS DOUBLE) ELSE "
                        + _ratio(_ratio(_cosd_q1("_d"), _sind_q1("_d")), _COT_45)
                        + " END)",
                        "CASE WHEN _r = 0 THEN 0.0 ELSE _r END",
                    ),
                ),
            ),
        ),
    )
)
_UNIT_RANGE = f"{_X} < -1.0 OR {_X} > 1.0"
ASIND = (
    f"CASE WHEN isnan({_X}) THEN {_X} WHEN {_UNIT_RANGE} THEN {_OUT_OF_DOMAIN} "
    f"WHEN {_X} >= 0 THEN {_asind_q1(_X)} ELSE -({_asind_q1(f'(-{_X})')}) END"
)
ACOSD = (
    f"CASE WHEN isnan({_X}) THEN {_X} WHEN {_UNIT_RANGE} THEN {_OUT_OF_DOMAIN} "
    f"WHEN {_X} >= 0 THEN {_acosd_q1(_X)} ELSE 90.0 + ({_asind_q1(f'(-{_X})')}) END"
)
ATAND = _ratio(f"atan({_X})", "atan(1.0)") + " * 45.0"
ATAN2D = _ratio(f"atan2({_X}, {_Y})", "atan(1.0)") + " * 45.0"

#: Marsaglia's polar form wants a loop; Box and Muller's needs none, and draws from the same
#: distribution. 1 - random() is never zero, so its logarithm always has a value.
RANDOM_NORMAL = "{mean} + {stddev} * sqrt(-2.0 * ln(1.0 - random())) * cos(2.0 * pi() * random())"


# A numeric PostgreSQL has and the engine does not (NaN, an infinity, more than 38 digits) reaches
# these rows as its text, which is also what a DECIMAL casts to.
_DIGITS_AS_TEXT = "CAST(value AS VARCHAR)"
_NAN = "lower(_t) = 'nan'"
_INFINITE = "lower(ltrim(_t, '+-')) IN ('infinity', 'inf')"


def numeric_as_integer(text: str, sql_type: str) -> str:
    """numeric_int2 and its kin: half away from zero, as a DECIMAL casts."""
    name = PG_INTEGERS[sql_type]
    return bound(
        "_t",
        text,
        f"CASE WHEN {_NAN} THEN {_refused('0A000', f'cannot convert NaN to {name}')} "
        f"WHEN {_INFINITE} THEN {_refused('0A000', f'cannot convert infinity to {name}')} "
        f"ELSE {in_range('try_cast(_t AS DECIMAL(38,0))', sql_type)} END",
    )


def numeric_as_float(text: str, sql_type: str) -> str:
    """numeric_float4 and numeric_float8: the digits read as float4in and float8in read them, so
    a value past the type is refused in their words."""
    refused = raising("22003", f"'\"' || _t || '\" is out of range for type {PG_FLOATS[sql_type]}'")
    return bound(
        "_t",
        text,
        bound(
            "_f",
            f"try_cast(_t AS {sql_type})",
            f"CASE WHEN {_NAN} OR {_INFINITE} THEN _f "
            f"WHEN _f IS NULL OR isinf(_f) THEN {refused} "
            f"WHEN _f = 0 AND regexp_matches(_t, '[1-9]') THEN {refused} ELSE _f END",
        ),
    )


#: date_pli: the days since the Unix epoch PostgreSQL's dates run over (IS_VALID_DATE), which are
#: fewer than the engine's.
_FIRST_DATE, _PAST_LAST_DATE = -2440588, 2145042906
DATE_PLUS_DAYS = bound(
    "_e",
    "date_diff('day', DATE '1970-01-01', d) + CAST(n AS BIGINT)",
    f"CASE WHEN _e < {_FIRST_DATE} OR _e >= {_PAST_LAST_DATE} THEN "
    + _refused("22008", "date out of range")
    + " ELSE DATE '1970-01-01' + CAST(_e AS INTEGER) END",
)


def jsonb_number(answer: str, name: str) -> str:
    """jsonb_int2 and its kin: a scalar number's digits, or a refusal naming what it is."""
    kind = "json_type(CAST(value AS JSON))"
    refused = raising(
        "22023",
        f"'cannot cast jsonb ' || CASE {kind} WHEN 'VARCHAR' THEN 'string' WHEN 'NULL' THEN 'null' "
        f"WHEN 'BOOLEAN' THEN 'boolean' WHEN 'ARRAY' THEN 'array' ELSE 'object' END "
        f"|| ' to type {name}'",
    )
    return f"CASE WHEN {kind} IN ('UBIGINT', 'BIGINT', 'DOUBLE') THEN {answer} ELSE {refused} END"


JSONB_DIGITS = _DIGITS_AS_TEXT


def bit_as_integer(sql_type: str, width: int) -> str:
    """bittoint4 and bittoint8: the bits, as the two's complement of the type where they fill it."""
    refused = _refused("22003", f"{PG_INTEGERS[sql_type]} out of range")
    return (
        f"CASE WHEN bit_length(value) > {width} THEN {refused} ELSE CAST(value AS {sql_type}) END"
    )


#: chartoi4: a "char" is one signed byte, and the engine holds it as text.
CHAR_AS_INTEGER = bound(
    "_b",
    "CAST('0x' || coalesce(nullif(left(hex(encode(CAST(value AS VARCHAR))), 2), ''), '00') "
    "AS INTEGER)",
    "CASE WHEN _b > 127 THEN _b - 256 ELSE _b END",
)
#: dtof, and an integer's conversion, which cannot fail.
AS_REAL = bound(
    "_r",
    "try_cast(value AS FLOAT)",
    f"CASE WHEN _r IS NULL THEN {_OVERFLOW} WHEN _r = 0 AND value <> 0 THEN {_UNDERFLOW} ELSE _r END",
)
_TRIMMED = "CASE WHEN contains(_t, '.') THEN rtrim(rtrim(_t, '0'), '.') ELSE _t END"
_NOT_A_NUMBER = f"{_NAN} OR {_INFINITE}"
#: numeric's text with the zeros that end its fraction gone.
TRIM_SCALE = bound("_t", _DIGITS_AS_TEXT, _TRIMMED)
#: How many digits of the fraction are left once they are; a NaN or an infinity has no scale.
MIN_SCALE = bound(
    "_t",
    _DIGITS_AS_TEXT,
    f"CASE WHEN {_NOT_A_NUMBER} THEN CAST(NULL AS INTEGER) ELSE "
    f"CAST(length(split_part({_TRIMMED}, '.', 2)) AS INTEGER) END",
)

# erf and erfc: no engine has them but ClickHouse, and PostgreSQL's are libm's, so these are the
# rational approximations libm's are (fdlibm's s_erf.c), with no loop and no table.
_EFX = "1.28379167095512586316e-01"
_ERX = "8.45062911510467529297e-01"
_PP = ("1.28379167095512558561e-01", "-3.25042107247001499370e-01", "-2.84817495755985104766e-02",
       "-5.77027029648944159157e-03", "-2.37630166566501626084e-05")  # fmt: skip
_QQ = ("1.0e0", "3.97917223959155352819e-01", "6.50222499887672944485e-02",
       "5.08130628187576562776e-03", "1.32494738004321644526e-04", "-3.96022827877036883518e-06")  # fmt: skip
_PA = ("-2.36211856075265944077e-03", "4.14856118683748331666e-01", "-3.72207876035701323847e-01",
       "3.18346619901161753674e-01", "-1.10894694282396677476e-01", "3.54783043256182359371e-02",
       "-2.16637559486879084300e-03")  # fmt: skip
_QA = ("1.0e0", "1.06420880400844228286e-01", "5.40397917702171048937e-01",
       "7.18286544141962662868e-02", "1.26171219808761642112e-01", "1.36370839120290507362e-02",
       "1.19844998467991074170e-02")  # fmt: skip
_RA = ("-9.86494403484714822705e-03", "-6.93858572707181764372e-01", "-1.05586262253232909814e+01",
       "-6.23753324503260060396e+01", "-1.62396669462573470355e+02", "-1.84605092906711035994e+02",
       "-8.12874355063065934246e+01", "-9.81432934416914548592e+00")  # fmt: skip
_SA = ("1.0e0", "1.96512716674392571292e+01", "1.37657754143519042600e+02",
       "4.34565877475229228821e+02", "6.45387271733267880336e+02", "4.29008140027567833386e+02",
       "1.08635005541779435134e+02", "6.57024977031928170135e+00", "-6.04244152148580987438e-02")  # fmt: skip
_RB = ("-9.86494292470009928597e-03", "-7.99283237680523006574e-01", "-1.77579549177547519889e+01",
       "-1.60636384855821916062e+02", "-6.37566443368389627722e+02", "-1.02509513161107724954e+03",
       "-4.83519191608651397019e+02")  # fmt: skip
_SB = ("1.0e0", "3.03380607434824582924e+01", "3.25792512996573918826e+02",
       "1.53672958608443695994e+03", "3.19985821950859553908e+03", "2.55305040643316442583e+03",
       "4.74528541206955367215e+02", "-2.24409524465858183362e+01")  # fmt: skip
_SMALL = 0.84375
_MIDDLE = 1.25
_SWITCH = 2.857142857142857  # 1 / 0.35
_ERF_ONE = 6.0
_ERFC_ZERO = 28.0
_TINY = "1.0e-300"


def _polynomial(variable: str, coefficients: tuple[str, ...]) -> str:
    """Horner's rule over the coefficients, lowest power first."""
    written = coefficients[-1]
    for coefficient in reversed(coefficients[:-1]):
        written = f"{coefficient} + {variable} * ({written})"
    return f"({written})"


#: y of |x| < 0.84375: erf(x) is x + x * y.
_ERF_NEAR_ZERO = bound("_z", f"{_X} * {_X}", _ratio(_polynomial("_z", _PP), _polynomial("_z", _QQ)))
_ERF_NEAR_ONE = bound("_s", "_a - 1.0", _ratio(_polynomial("_s", _PA), _polynomial("_s", _QA)))
#: r / |x|: exp(-x^2) times the tail's ratio, with x^2 split so its exponent keeps every bit.
_ERF_TAIL = bound(
    "_s",
    "1.0 / (_a * _a)",
    bound(
        "_z",
        "floor(_a * 262144.0) / 262144.0",
        f"exp(-_z * _z - 0.5625) * exp((_z - _a) * (_z + _a) + CASE WHEN _a < {_SWITCH} "
        f"THEN {_ratio(_polynomial('_s', _RA), _polynomial('_s', _SA))} "
        f"ELSE {_ratio(_polynomial('_s', _RB), _polynomial('_s', _SB))} END) / _a",
    ),
)
_SIGN = f"CASE WHEN {_X} < 0 THEN -1.0 ELSE 1.0 END"
ERF = bound(
    "_a",
    f"abs({_X})",
    f"CASE WHEN isnan({_X}) THEN {_X} "
    f"WHEN _a < 3.7252902984e-09 THEN {_X} + {_EFX} * {_X} "
    f"WHEN _a < {_SMALL} THEN {_X} + {_X} * ({_ERF_NEAR_ZERO}) "
    f"WHEN _a < {_MIDDLE} THEN ({_SIGN}) * ({_ERX} + {_ERF_NEAR_ONE}) "
    f"WHEN _a >= {_ERF_ONE} THEN {_SIGN} "
    f"ELSE ({_SIGN}) * (1.0 - {_ERF_TAIL}) END",
)
ERFC = bound(
    "_a",
    f"abs({_X})",
    f"CASE WHEN isnan({_X}) THEN {_X} "
    f"WHEN _a < 1.3877787807814457e-17 THEN 1.0 - {_X} "
    f"WHEN _a < {_SMALL} AND {_X} < 0.25 THEN 1.0 - ({_X} + {_X} * ({_ERF_NEAR_ZERO})) "
    f"WHEN _a < {_SMALL} THEN 0.5 - ({_X} * ({_ERF_NEAR_ZERO}) + ({_X} - 0.5)) "
    f"WHEN _a < {_MIDDLE} AND {_X} >= 0 THEN (1.0 - {_ERX}) - {_ERF_NEAR_ONE} "
    f"WHEN _a < {_MIDDLE} THEN 1.0 + ({_ERX} + {_ERF_NEAR_ONE}) "
    f"WHEN _a >= {_ERFC_ZERO} OR ({_X} < 0 AND _a >= {_ERF_ONE}) "
    f"THEN CASE WHEN {_X} > 0 THEN {_TINY} * {_TINY} ELSE 2.0 END "
    f"WHEN {_X} > 0 THEN {_ERF_TAIL} ELSE 2.0 - {_ERF_TAIL} END",
)
INT2VECTOR_SEND = (
    "unhex(printf('%08x%08x%08x%08x%08x', 1, 0, 21, len(v), 0) || "
    "array_to_string(list_transform(v, _o -> printf('00000002%04x', _o & 65535)), ''))"
)
#: int2int4_sum: a transition array is its count and its sum.
INT2INT4_SUM = (
    "CASE WHEN len(v) <> 2 OR len(list_filter(v, _x -> _x IS NULL)) > 0 THEN "
    + _refused("XX000", "expected 2-element int8 array")
    + " WHEN v[1] = 0 THEN CAST(NULL AS BIGINT) ELSE v[2] END"
)


# --- dates and times ----------------------------------------------------------------------------

_USECS_PER_DAY = 86400000000
_USECS_PER_HOUR = 3600000000
_USECS_PER_MINUTE = 60000000
_INTERVAL_OUT_OF_RANGE = _refused("22008", "interval out of range")


def _months(value: str) -> str:
    return f"(date_part('year', {value}) * 12 + date_part('month', {value}))"


def _days(value: str) -> str:
    return f"date_part('day', {value})"


def _micros(value: str) -> str:
    return (
        f"(date_part('hour', {value}) * {_USECS_PER_HOUR} + "
        f"date_part('minute', {value}) * {_USECS_PER_MINUTE} + "
        f"date_part('microseconds', {value}))"
    )


def _interval(months: str, days: str, micros: str) -> str:
    return (
        f"CASE WHEN try_cast({months} AS INTEGER) IS NULL OR try_cast({days} AS INTEGER) IS NULL "
        f"OR try_cast({micros} AS BIGINT) IS NULL THEN {_INTERVAL_OUT_OF_RANGE} "
        f"ELSE to_months(CAST({months} AS INTEGER)) + to_days(CAST({days} AS INTEGER)) + "
        f"to_microseconds(CAST({micros} AS BIGINT)) END"
    )


def _interval_parts(body: str) -> str:
    """``body`` over the interval ``value``'s three fields, _m, _d and _t: wide enough to carry."""
    return bound(
        "_m",
        f"CAST({_months('value')} AS HUGEINT)",
        bound(
            "_d",
            f"CAST({_days('value')} AS HUGEINT)",
            bound("_t", f"CAST({_micros('value')} AS HUGEINT)", body),
        ),
    )


def _borrowed_time(days: str, micros: str, then: str) -> str:
    """justify's last step: the sign of the days carried into the time, bound as _e and _u."""
    return bound(
        "_e",
        f"CASE WHEN {days} > 0 AND {micros} < 0 THEN {days} - 1 "
        f"WHEN {days} < 0 AND {micros} > 0 THEN {days} + 1 ELSE {days} END",
        bound(
            "_u",
            f"CASE WHEN {days} > 0 AND {micros} < 0 THEN {micros} + {_USECS_PER_DAY} "
            f"WHEN {days} < 0 AND {micros} > 0 THEN {micros} - {_USECS_PER_DAY} ELSE {micros} END",
            then,
        ),
    )


JUSTIFY_HOURS = _interval_parts(
    bound(
        "_w",
        f"_d + _t // {_USECS_PER_DAY}",
        bound(
            "_v",
            f"_t % {_USECS_PER_DAY}",
            _borrowed_time("_w", "_v", _interval("_m", "_e", "_u")),
        ),
    )
)
JUSTIFY_DAYS = _interval_parts(
    bound(
        "_y",
        "_m + _d // 30",
        bound(
            "_z",
            "_d % 30",
            _interval(
                "CASE WHEN _y > 0 AND _z < 0 THEN _y - 1 WHEN _y < 0 AND _z > 0 THEN _y + 1 "
                "ELSE _y END",
                "CASE WHEN _y > 0 AND _z < 0 THEN _z + 30 WHEN _y < 0 AND _z > 0 THEN _z - 30 "
                "ELSE _z END",
                "_t",
            ),
        ),
    )
)
#: justify_interval: its steps in its order; carrying the days first keeps nothing from overflowing
#: and changes no answer the later steps give.
JUSTIFY_INTERVAL = _interval_parts(
    bound(
        "_w",
        f"_d + _t // {_USECS_PER_DAY}",
        bound(
            "_v",
            f"_t % {_USECS_PER_DAY}",
            bound(
                "_y",
                "_m + _w // 30",
                bound(
                    "_z",
                    "_w % 30",
                    bound(
                        "_g",
                        "CASE WHEN _y > 0 AND (_z < 0 OR (_z = 0 AND _v < 0)) THEN _y - 1 "
                        "WHEN _y < 0 AND (_z > 0 OR (_z = 0 AND _v > 0)) THEN _y + 1 ELSE _y END",
                        bound(
                            "_h",
                            "CASE WHEN _y > 0 AND (_z < 0 OR (_z = 0 AND _v < 0)) THEN _z + 30 "
                            "WHEN _y < 0 AND (_z > 0 OR (_z = 0 AND _v > 0)) THEN _z - 30 "
                            "ELSE _z END",
                            _borrowed_time("_h", "_v", _interval("_g", "_e", "_u")),
                        ),
                    ),
                ),
            ),
        ),
    )
)

_ROUNDED_SECONDS = "try_cast(round_even(CAST(secs AS DOUBLE) * 1000000.0, 0) AS BIGINT)"
#: make_interval: rint of the seconds in microseconds, the other fields as they are.
MAKE_INTERVAL = (
    f"CASE WHEN {_ROUNDED_SECONDS} IS NULL THEN {_INTERVAL_OUT_OF_RANGE} ELSE "
    + _interval(
        "CAST(years AS HUGEINT) * 12 + months",
        "CAST(weeks AS HUGEINT) * 7 + days",
        f"CAST(hours AS HUGEINT) * {_USECS_PER_HOUR} + "
        f"CAST(mins AS HUGEINT) * {_USECS_PER_MINUTE} + {_ROUNDED_SECONDS}",
    )
    + " END"
)


def _endpoints(start: str, end: str) -> tuple[str, str]:
    """overlaps' interval in order, its known end first where only one is known."""
    return (
        f"CASE WHEN {start} IS NULL THEN {end} "
        f"WHEN {end} IS NOT NULL AND {start} > {end} THEN {end} ELSE {start} END",
        f"CASE WHEN {start} IS NULL OR {end} IS NULL THEN NULL "
        f"WHEN {start} > {end} THEN {start} ELSE {end} END",
    )


_ANSWER_UNKNOWN = "CAST(NULL AS BOOLEAN)"
#: overlaps_timestamp, overlaps_time and overlaps_timetz: SQL's OVERLAPS, whose three-valued answer
#: a NULL end leaves open, not a strict function.
OVERLAPS = bound(
    "_a",
    _endpoints("s1", "e1")[0],
    bound(
        "_b",
        _endpoints("s1", "e1")[1],
        bound(
            "_c",
            _endpoints("s2", "e2")[0],
            bound(
                "_d",
                _endpoints("s2", "e2")[1],
                f"CASE WHEN _a IS NULL OR _c IS NULL THEN {_ANSWER_UNKNOWN} "
                f"WHEN _a > _c THEN CASE WHEN _d IS NULL THEN {_ANSWER_UNKNOWN} "
                f"WHEN _a < _d THEN true WHEN _b IS NULL THEN {_ANSWER_UNKNOWN} ELSE false END "
                f"WHEN _a < _c THEN CASE WHEN _b IS NULL THEN {_ANSWER_UNKNOWN} "
                f"WHEN _c < _b THEN true WHEN _d IS NULL THEN {_ANSWER_UNKNOWN} ELSE false END "
                f"WHEN _b IS NULL OR _d IS NULL THEN {_ANSWER_UNKNOWN} ELSE true END",
            ),
        ),
    ),
)


def _scale(precision: str) -> str:
    return f"CAST(pow(10, 6 - {precision}) AS BIGINT)"


def _rounded_micros(micros: str, precision: str) -> str:
    """AdjustTimestampForTypmod's rounding: half away from zero, at ``precision`` digits."""
    scale = _scale(precision)
    return (
        f"CASE WHEN {micros} >= 0 THEN (({micros} + {scale} // 2) // {scale}) * {scale} "
        f"ELSE -(((-{micros} + {scale} // 2) // {scale}) * {scale}) END"
    )


def _timestamp_scaled(made: str, kind: str) -> str:
    refused = raising(
        "22023",
        f"'{kind}(' || n || ') precision must be between 0 and 6'",
    )
    return (
        "CASE WHEN n = -1 OR n = 6 OR NOT isfinite(value) THEN value "
        f"WHEN n < 0 OR n > 6 THEN {refused} "
        f"ELSE {made}({_rounded_micros('epoch_us(value)', 'n')}) END"
    )


TIMESTAMP_SCALE = _timestamp_scaled("make_timestamp", "timestamp")
TIMESTAMPTZ_SCALE = _timestamp_scaled("make_timestamptz", "timestamp")
_MIDNIGHT = "TIME '00:00:00'"
_END_OF_DAY = "TIME '24:00:00'"


def _time_of(micros: str) -> str:
    """A time from its microseconds; 24:00:00 is one, and the engine's addition would wrap it."""
    return (
        f"CASE WHEN {micros} >= {_USECS_PER_DAY} THEN {_END_OF_DAY} "
        f"ELSE {_MIDNIGHT} + to_microseconds(CAST({micros} AS BIGINT)) END"
    )


def _time_scaled(time: str) -> str:
    rounded = bound("_r", _rounded_micros(f"epoch_us({time})", "n"), _time_of("_r"))
    return f"CASE WHEN n < 0 OR n > 6 THEN {time} ELSE {rounded} END"


TIME_SCALE = _time_scaled("value")
#: The interval's typmod as its fields' mask: PostgreSQL's YEAR, MONTH, DAY, HOUR, MINUTE and SECOND.
_YEAR, _MONTH, _DAY, _HOUR, _MINUTE, _SECOND = 4, 2, 8, 1024, 2048, 4096
_FULL_RANGE = 32767
_FULL_PRECISION = 65535
_MASK = f"((n >> 16) & {_FULL_RANGE})"
_PRECISION = f"(n & {_FULL_PRECISION})"
_NO_TIME = (_YEAR, _MONTH, _YEAR | _MONTH, _DAY)
_TO_THE_HOUR = (_HOUR, _DAY | _HOUR)
_TO_THE_MINUTE = (_MINUTE, _DAY | _HOUR | _MINUTE, _HOUR | _MINUTE)
_KEPT = (
    _FULL_RANGE,
    _SECOND,
    _DAY | _HOUR | _MINUTE | _SECOND,
    _HOUR | _MINUTE | _SECOND,
    _MINUTE | _SECOND,
)


def _of_masks(masks: tuple[int, ...]) -> str:
    return f"{_MASK} IN ({', '.join(map(str, masks))})"


_SCALED_MONTHS = f"CASE WHEN {_MASK} = {_YEAR} THEN (_m // 12) * 12 ELSE _m END"
_SCALED_DAYS = f"CASE WHEN {_of_masks((_YEAR, _MONTH, _YEAR | _MONTH))} THEN 0 ELSE _d END"
_FIELDS_TIME = (
    f"CASE WHEN {_of_masks(_NO_TIME)} THEN 0 "
    f"WHEN {_of_masks(_TO_THE_HOUR)} THEN (_t // {_USECS_PER_HOUR}) * {_USECS_PER_HOUR} "
    f"WHEN {_of_masks(_TO_THE_MINUTE)} THEN (_t // {_USECS_PER_MINUTE}) * {_USECS_PER_MINUTE} "
    "ELSE _t END"
)
#: AdjustIntervalForTypmod: the fields the typmod names, then its fractional digits.
INTERVAL_SCALE = (
    "CASE WHEN n < 0 THEN value "
    f"WHEN NOT {_of_masks((*_NO_TIME, *_TO_THE_HOUR, *_TO_THE_MINUTE, *_KEPT))} THEN "
    + raising("XX000", "'unrecognized interval typmod: ' || n")
    + f" WHEN {_PRECISION} <> {_FULL_PRECISION} AND {_PRECISION} > 6 THEN "
    + raising("22023", f"'interval(' || {_PRECISION} || ') precision must be between 0 and 6'")
    + " ELSE "
    + _interval_parts(
        bound(
            "_u",
            _FIELDS_TIME,
            _interval(
                _SCALED_MONTHS,
                _SCALED_DAYS,
                f"CASE WHEN {_PRECISION} = {_FULL_PRECISION} THEN _u "
                f"ELSE {_rounded_micros('_u', _PRECISION)} END",
            ),
        )
    )
    + " END"
)
_TIMETZ_CLOCK = "CAST(value AS TIME)"
_OFFSET = "date_part('timezone', value)"


def _zoned_time(clock: str, offset: str) -> str:
    """A timetz of a clock and an offset in seconds east of Greenwich."""
    return (
        f"CAST(CAST({clock} AS VARCHAR) || "
        f"printf('%s%02d:%02d:%02d', CASE WHEN {offset} < 0 THEN '-' ELSE '+' END, "
        f"abs({offset}) // 3600, abs({offset}) // 60 % 60, abs({offset}) % 60) AS TIMETZ)"
    )


#: timetz_scale: its clock rounded, its offset as it was.
TIMETZ_SCALE = _zoned_time(_time_scaled(_TIMETZ_CLOCK), _OFFSET)
#: time_timetz: the session's offset today, which the engine's cast leaves at zero.
TIME_TIMETZ = _zoned_time("value", "date_part('timezone', now())")
INTERVAL_TIME = bound(
    "_t",
    f"{_micros('value')} % {_USECS_PER_DAY}",
    f"{_MIDNIGHT} + to_microseconds(CASE WHEN _t < 0 THEN _t + {_USECS_PER_DAY} ELSE _t END)",
)
TIME_INTERVAL = "to_microseconds(epoch_us(value))"
#: A date's or a timestamp's infinity, which the engine's casts between them do not keep.
_INFINITE_VALUE = "CASE WHEN {v} > DATE '2000-01-01' THEN 'infinity' ELSE '-infinity' END"


def _keeping_infinity(value: str, sql_type: str, answer: str) -> str:
    infinite = _INFINITE_VALUE.format(v=value)
    return f"CASE WHEN NOT isfinite({value}) THEN CAST({infinite} AS {sql_type}) ELSE {answer} END"


_CLOCK = "to_microseconds(epoch_us(t))"
DATETIME_PL = _keeping_infinity("d", "TIMESTAMP", f"CAST(d AS TIMESTAMP) + {_CLOCK}")
_ZONED_MICROS = "to_microseconds(epoch_us(CAST(t AS TIME)) - date_part('timezone', t) * 1000000)"
#: The date and the clock read at the time's own offset.
DATETIMETZ_PL = _keeping_infinity(
    "d", "TIMESTAMPTZ", f"timezone('UTC', CAST(d AS TIMESTAMP) + {_ZONED_MICROS})"
)
#: A value whose second type the statement may not say: its offset is its own, or the session's.
TIMESTAMPTZ_OF_DATE_AND_TIME = _keeping_infinity(
    "d",
    "TIMESTAMPTZ",
    f"CASE WHEN typeof(t) = 'TIME WITH TIME ZONE' "
    f"THEN timezone('UTC', CAST(d AS TIMESTAMP) + {_ZONED_MICROS}) "
    "ELSE CAST(CAST(d AS TIMESTAMP) + to_microseconds(epoch_us(CAST(t AS TIME))) AS TIMESTAMPTZ) END",
)
TIMESTAMPTZ_OF = _keeping_infinity("value", "TIMESTAMPTZ", "CAST(value AS TIMESTAMPTZ)")
TIMESTAMP_TIME = "CASE WHEN isfinite(value) THEN CAST(value AS TIME) END"
TIMESTAMPTZ_TIME = "CASE WHEN isfinite(value) THEN CAST(CAST(value AS TIMESTAMP) AS TIME) END"
TIMESTAMPTZ_TIMETZ = "CASE WHEN isfinite(value) THEN CAST(value AS TIMETZ) END"
#: The engine's clock moves within a statement only in a UUID: version 7's holds the milliseconds
#: since the epoch. It is held at the statement's start, which it may be less than a millisecond
#: behind.
CLOCK_TIMESTAMP = "greatest(now(), uuid_extract_timestamp(uuidv7()))"
TIMEOFDAY = f"strftime({CLOCK_TIMESTAMP}, '%a %b %d %H:%M:%S.%f %Y %Z')"
