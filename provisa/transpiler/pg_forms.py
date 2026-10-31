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
BOX_DIAGONAL = (
    "'[' || "
    + _POINT_TEXT.format(x=f"greatest({', '.join(_BOX_X)})", y=f"greatest({', '.join(_BOX_Y)})")
    + " || ',' || "
    + _POINT_TEXT.format(x=f"least({', '.join(_BOX_X)})", y=f"least({', '.join(_BOX_Y)})")
    + " || ']'"
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
