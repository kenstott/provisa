# Copyright (c) 2026 Kenneth Stott
# Canary: 7c0e93b1-5d42-4a6f-b8e7-2f1a64d09c35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SQL of the forms of PostgreSQL's json and jsonb functions on DuckDB, for the rows of
provisa.transpiler.function_macros.

DuckDB's own JSON functions read a value through yyjson, which rewrites what PostgreSQL keeps: a
json value's whitespace and a number's written digits. So a form reads the value's text, as
pg_forms' do: ``_tokens`` splits it, ``_members`` reads one level of a container verbatim, and
``_fold`` is a reduce over the tokens with a stack of open containers, which is how a form reaches
every depth with no recursion. The list items are VARCHAR throughout, as list_reduce's state and
items must be one type.

A frame of the stack is a list of entries [sort key, key, value]; its first entry names it. A
parameter is read once, into a lambda's variable: a form names a parameter as often as it needs it,
and a fold is no thing to run twice."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable

import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import raising
from provisa.transpiler.pg_literals import _as_array, array_elements

_BLANKS = "' ' || chr(9) || chr(10) || chr(13)"
_STRING_TOKEN = r'"(?:[^"\\]|\\.)*"'
_TOKEN = rf"{_STRING_TOKEN}|[{{}}\[\],:]|[^\s{{}}\[\],:" + '"]+'
_ANY_TOKEN = rf"{_STRING_TOKEN}|[{{}}\[\],:]|\s+|[^\s{{}}\[\],:" + '"]+'


def let(name: str, value: str, body: str) -> str:
    """``body`` with ``name`` bound to ``value``, which is evaluated once."""
    return f"list_transform([{value}], {name} -> {body})[1]"


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _tokens(value: str, *, blanks: bool = False) -> str:
    pattern = _ANY_TOKEN if blanks else _TOKEN
    return f"regexp_extract_all({value}, {_quoted(pattern)})"


def as_text(value: str) -> str:
    """A json or jsonb argument's text: a literal's, a column's or a form's."""
    return f"CAST({value} AS VARCHAR)"


def _first(value: str) -> str:
    """The first character of a value, which says what it is."""
    return f"left(ltrim({value}, {_BLANKS}), 1)"


def is_object(value: str) -> str:
    return f"{_first(value)} = '{{'"


def is_array(value: str) -> str:
    return f"{_first(value)} = '['"


def is_container(value: str) -> str:
    return f"{_first(value)} IN ('{{', '[')"


def typeof(value: str) -> str:
    """json_typeof of a value's text."""
    return (
        f"CASE {_first(value)} WHEN '{{' THEN 'object' WHEN '[' THEN 'array' "
        "WHEN '\"' THEN 'string' WHEN 't' THEN 'boolean' WHEN 'f' THEN 'boolean' "
        "WHEN 'n' THEN 'null' ELSE 'number' END"
    )


# --- strings ------------------------------------------------------------------------------------

_SURROGATES = r"\\u[dD][89abAB][0-9a-fA-F]{2}\\u[dD][c-fC-F][0-9a-fA-F]{2}"
_ESCAPE = rf"{_SURROGATES}|\\u[0-9a-fA-F]{{4}}|\\.|[^\\]+"
_NEEDS_ESCAPE = r'["\\\x01-\x1f]'
_ESCAPED_PART = r'["\\\x01-\x1f]|[^"\\\x01-\x1f]+'


def _hex(digits: str) -> str:
    return f"CAST('0x' || {digits} AS INTEGER)"


def _part_read(part: str) -> str:
    """One piece of a string's text: an escape as its character, anything else as it stands."""
    pair = f"(({_hex(f'substr({part}, 3, 4)')} - 55296) * 1024 + {_hex(f'substr({part}, 9, 4)')} - 56320 + 65536)"
    return (
        f"CASE WHEN left({part}, 1) <> chr(92) THEN {part} "
        f"WHEN length({part}) = 12 THEN chr({pair}) "
        f"WHEN lower({part}) = chr(92) || 'u0000' THEN "
        + raising("22P05", "'unsupported Unicode escape sequence'")
        + f" WHEN length({part}) = 6 THEN chr({_hex(f'substr({part}, 3, 4)')}) "
        f"ELSE CASE substr({part}, 2, 1) WHEN 'b' THEN chr(8) WHEN 'f' THEN chr(12) "
        f"WHEN 'n' THEN chr(10) WHEN 'r' THEN chr(13) WHEN 't' THEN chr(9) "
        f"ELSE substr({part}, 2, 1) END END"
    )


def string_read(token: str) -> str:
    """A string token's value."""
    inner = f"substr({token}, 2, length({token}) - 2)"
    pieces = f"regexp_extract_all({inner}, {_quoted(_ESCAPE)})"
    return (
        f"CASE WHEN strpos({token}, chr(92)) = 0 THEN {inner} ELSE "
        f"array_to_string(list_transform({pieces}, _q -> {_part_read('_q')}), '') END"
    )


def string_written(value: str) -> str:
    """A text as a JSON string, as escape_json writes it."""
    written = (
        "CASE _q WHEN '\"' THEN chr(92) || '\"' WHEN chr(92) THEN chr(92) || chr(92) "
        "WHEN chr(8) THEN chr(92) || 'b' WHEN chr(12) THEN chr(92) || 'f' "
        "WHEN chr(10) THEN chr(92) || 'n' WHEN chr(13) THEN chr(92) || 'r' "
        "WHEN chr(9) THEN chr(92) || 't' ELSE CASE WHEN ascii(_q) < 32 "
        "THEN printf('%su%04x', chr(92), ascii(_q)) ELSE _q END END"
    )
    pieces = f"regexp_extract_all({value}, {_quoted(_ESCAPED_PART)})"
    return (
        f"CASE WHEN regexp_matches({value}, {_quoted(_NEEDS_ESCAPE)}) THEN '\"' || "
        f"array_to_string(list_transform({pieces}, _q -> {written}), '') || '\"' "
        f"ELSE '\"' || {value} || '\"' END"
    )


def scalar_text(value: str) -> str:
    """A value as ->> and json_each_text read it: a string's text, NULL for null."""
    return (
        f"CASE WHEN left({value}, 1) = '\"' THEN {string_read(value)} "
        f"WHEN {value} = 'null' THEN NULL ELSE {value} END"
    )


# --- numbers ------------------------------------------------------------------------------------

_NUMBER_PARTS = r"^(-?)([0-9]+)(?:\.([0-9]+))?(?:[eE]([-+]?[0-9]+))?$"


def number_written(token: str) -> str:
    """A number as numeric_out writes what jsonb read: no exponent, no negative zero, the fraction
    as written, read once from its token."""
    parts = f"regexp_extract({token}, {_quoted(_NUMBER_PARTS)}, ['s', 'w', 'f', 'e'])"
    point = "(length(_np.w) + coalesce(TRY_CAST(nullif(_np.e, '') AS INTEGER), 0))"
    plain = (
        "CASE WHEN _pt <= 0 THEN '0.' || repeat('0', -_pt) || _dg "
        "WHEN _pt >= length(_dg) THEN _dg || repeat('0', _pt - length(_dg)) "
        "ELSE substr(_dg, 1, _pt) || '.' || substr(_dg, _pt + 1) END"
    )
    # A value's leading zeros go, as the digits before the point are a number's.
    trimmed_ = f"regexp_replace({plain}, '^0+([0-9])', '\\1')"
    signed = "CASE WHEN regexp_matches(_pl, '^[0.]*$') THEN _pl ELSE _np.s || _pl END"
    return let(
        "_np",
        parts,
        let("_dg", "_np.w || _np.f", let("_pt", point, let("_pl", trimmed_, signed))),
    )


_RANK_NULL, _RANK_STRING, _RANK_NUMBER, _RANK_BOOL = "'1'", "'2'", "'3'", "'4'"
_END = "chr(1)"


def _number_key(token: str) -> str:
    """A number as a text that sorts as the number does, whatever its scale: its sign, then its
    magnitude's exponent and digits, reversed for a negative one."""
    keyed = (
        "CASE WHEN _bare = '' THEN '1' WHEN left(_txt, 1) = '-' THEN "
        "'0' || printf('%010d', 1000000000 - _exp) || "
        "translate(rtrim(_bare, '0'), '0123456789', '9876543210') || '~' "
        f"ELSE '2' || printf('%010d', 1000000000 + _exp) || rtrim(_bare, '0') || {_END} END"
    )
    return let(
        "_txt",
        number_written(token),
        let(
            "_who",
            "split_part(ltrim(_txt, '-'), '.', 1)",
            let(
                "_dgs",
                "_who || split_part(ltrim(_txt, '-'), '.', 2)",
                let(
                    "_bare",
                    "ltrim(_dgs, '0')",
                    let("_exp", "length(_who) - (length(_dgs) - length(_bare))", keyed),
                ),
            ),
        ),
    )


# --- the fold -----------------------------------------------------------------------------------

_TOKEN_ITEM = "_e[1][1][1]"
_TOP = "_s[len(_s)]"
_ROOT = "[[[NULL, '[', NULL]]]"


def _joined(entries: str, each: str, separator: str) -> str:
    return f"array_to_string(list_transform({entries}, _m -> {each}), {separator})"


def _fold(
    value: str,
    atom: str,
    key: str,
    sort_key: str,
    closed_object: str,
    closed_array: str,
) -> str:
    """A reduce over ``value``'s tokens: ``atom`` writes a scalar, ``key`` a key, each over
    ``_t``; ``sort_key`` orders an object's keys, one entry to a key; ``closed_object`` and
    ``closed_array`` write a container from the entries in ``_c``. The root is a frame of one."""
    token = "_t"
    below = "_s[len(_s) - 1]"
    closed = (
        f"list_transform([{_TOP}[2:]], _c -> CASE WHEN {_TOP}[1][2] = '{{' "
        f"THEN {closed_object} ELSE {closed_array} END)[1]"
    )
    keyed = (
        f"list_transform([{sort_key}], _k -> _s[1:len(_s) - 1] || "
        f"[list_filter({_TOP}, _m -> _m[1] IS DISTINCT FROM _k) || [[_k, {key}, NULL]]])[1]"
    )
    step = (
        f"list_transform([{_TOKEN_ITEM}], {token} -> CASE "
        f"WHEN {token} IN ('{{', '[') THEN _s || [[[NULL, {token}, NULL]]] "
        f"WHEN {token} IN ('}}', ']') THEN list_transform([{closed}], _r -> "
        f"_s[1:len(_s) - 2] || [CASE WHEN {below}[1][2] = '{{' THEN "
        f"{below}[1:len({below}) - 1] || "
        f"[[{below}[len({below})][1], {below}[len({below})][2], _r]] "
        f"ELSE {below} || [[NULL, NULL, _r]] END])[1] "
        f"WHEN {token} IN (',', ':') THEN _s "
        f"WHEN {_TOP}[1][2] = '{{' AND (len({_TOP}) = 1 OR {_TOP}[len({_TOP})][3] IS NOT NULL) "
        f"THEN {keyed} "
        f"ELSE list_transform([{atom}], _w -> _s[1:len(_s) - 1] || [CASE WHEN {_TOP}[1][2] = '{{' "
        f"THEN {_TOP}[1:len({_TOP}) - 1] || "
        f"[[{_TOP}[len({_TOP})][1], {_TOP}[len({_TOP})][2], _w]] "
        f"ELSE {_TOP} || [[NULL, NULL, _w]] END])[1] END)[1]"
    )
    items = f"list_transform({_tokens(value)}, _i -> [[[_i]]])"
    return f"list_reduce({items}, (_s, _e) -> {step}, {_ROOT})[1][2][3]"


def _sort_key(key_text: str) -> str:
    return f"printf('%010d', strlen({key_text})) || {key_text}"


_JSONB_ATOM = (
    f"CASE WHEN left(_t, 1) = '\"' THEN {string_written(string_read('_t'))} "
    f"WHEN left(_t, 1) IN ('t', 'f', 'n') THEN _t ELSE {number_written('_t')} END"
)
_JSONB_KEY = string_written(string_read("_t"))
_JSONB_SORT = _sort_key(string_read("_t"))
_JSONB_OBJECT = f"'{{' || {_joined('list_sort(_c)', "_m[2] || ': ' || _m[3]", "', '")} || '}}'"
_JSONB_ARRAY = f"'[' || {_joined('_c', '_m[3]', "', '")} || ']'"


def canonical(value: str) -> str:
    """A json text as jsonb holds it: keys in jsonb's order and one of each, the last, each string
    and number as jsonb_out writes it."""
    return _fold(value, _JSONB_ATOM, _JSONB_KEY, _JSONB_SORT, _JSONB_OBJECT, _JSONB_ARRAY)


def stripped(value: str) -> str:
    """jsonb_strip_nulls: a key of a null goes, at every depth; an array keeps its nulls."""
    kept = "list_filter(list_sort(_c), _m -> _m[3] <> 'null')"
    obj = f"'{{' || {_joined(kept, "_m[2] || ': ' || _m[3]", "', '")} || '}}'"
    return _fold(value, _JSONB_ATOM, _JSONB_KEY, _JSONB_SORT, obj, _JSONB_ARRAY)


def stripped_json(value: str) -> str:
    """json_strip_nulls: as jsonb's, each token as written and no whitespace."""
    kept = "list_filter(_c, _m -> _m[3] <> 'null')"
    obj = f"'{{' || {_joined(kept, "_m[2] || ':' || _m[3]", "','")} || '}}'"
    array = f"'[' || {_joined('_c', '_m[3]', "','")} || ']'"
    # A key is no one key here: json keeps a repeated one, so each has its own sort key.
    unique = "printf('%010d', len(_s[len(_s)]))"
    return _fold(value, "_t", "_t", unique, obj, array)


_NEWLINE = "chr(10)"


def _indented(text: str) -> str:
    return f"replace({text}, {_NEWLINE}, {_NEWLINE} || '    ')"


def pretty(value: str) -> str:
    """jsonb_pretty: four spaces to a level, and an empty container on two lines."""

    def block(opening: str, closing: str, entries: str, each: str) -> str:
        lines = f"{_joined(entries, each, f"',' || {_NEWLINE}")}"
        return (
            f"CASE WHEN len(_c) = 0 THEN '{opening}' || {_NEWLINE} || '{closing}' ELSE "
            f"'{opening}' || {_NEWLINE} || {lines} || {_NEWLINE} || '{closing}' END"
        )

    member = f"'    ' || _m[2] || ': ' || {_indented('_m[3]')}"
    obj = block("{", "}", "list_sort(_c)", member)
    array = block("[", "]", "_c", f"'    ' || {_indented('_m[3]')}")
    return _fold(value, _JSONB_ATOM, _JSONB_KEY, _JSONB_SORT, obj, array)


_KEY_ATOM = (
    f"CASE WHEN left(_t, 1) = '\"' THEN {_RANK_STRING} || {string_read('_t')} || {_END} "
    f"WHEN _t = 'null' THEN {_RANK_NULL} WHEN _t = 'false' THEN {_RANK_BOOL} || '0' "
    f"WHEN _t = 'true' THEN {_RANK_BOOL} || '1' ELSE {_RANK_NUMBER} || {_number_key('_t')} END"
)
_KEY_KEY = f"{_RANK_STRING} || {string_read('_t')} || {_END}"


def _rooted(key: str) -> str:
    """A root scalar is a raw-scalar pseudo-array of one to compareJsonbContainers: after a real
    array of one, and it is a count that is compared first."""
    return f"CASE WHEN left({key}, 1) IN ('5', '6') THEN {key} ELSE '5' || printf('%010d', 1) || '0' || {key} END"


def compare_key(value: str) -> str:
    """A text that sorts as compareJsonbContainers orders jsonb values: Object > Array > Boolean >
    Number > String > Null, an array and an object first by their size. The bytes of a string are
    its order: PostgreSQL's is the database's collation, and this server's is C."""
    count = "printf('%010d', len(_c))"
    obj = f"'6' || {count} || {_joined('list_sort(_c)', '_m[2] || _m[3]', "''")}"
    array = f"'5' || {count} || '1' || {_joined('_c', '_m[3]', "''")}"
    key = _fold(value, _KEY_ATOM, _KEY_KEY, _JSONB_SORT, obj, array)
    return let("_y", key, _rooted("_y"))


# --- one level of a container -------------------------------------------------------------------

_MEMBER = rf"(?s)^({_STRING_TOKEN})\s*:\s*(.*)$"


def members(value: str) -> str:
    """A container's members as written, in order, each [key token, value]; an array's have no
    key. ``value`` is a variable holding text with no blanks around it."""
    depth = "CASE WHEN _x IN ('{', '[') THEN 1 WHEN _x IN ('}', ']') THEN -1 ELSE 0 END"
    levels = (
        f"list_transform(range(1, len(_t) + 1), _i -> "
        f"list_sum(list_transform(_t[1:_i - 1], _x -> {depth})))"
    )
    cuts = (
        "list_concat([1], list_filter(range(2, len(_t)), _i -> _t[_i] = ',' AND _d[_i] = 1), "
        "[len(_t)])"
    )
    parts = (
        "list_transform(range(1, len(_p)), _j -> trim(array_to_string("
        f"_t[_p[_j] + 1:_p[_j + 1] - 1], ''), {_BLANKS}))"
    )
    read = (
        f"CASE WHEN left({value}, 1) = '{{' THEN list_transform(_g, _o -> "
        f"[regexp_extract(_o, {_quoted(_MEMBER)}, 1), regexp_extract(_o, {_quoted(_MEMBER)}, 2)]) "
        "ELSE list_transform(_g, _o -> [NULL, _o]) END"
    )
    return let(
        "_t",
        _tokens(value, blanks=True),
        let(
            "_d",
            levels,
            let("_p", cuts, let("_g", f"list_filter({parts}, _o -> _o <> '')", read)),
        ),
    )


def trimmed(value: str) -> str:
    return f"trim({value}, {_BLANKS})"


def _field(value: str, key: str) -> str:
    """The last member of an object with key ``key``; NULL for no such member, or no object. A
    repeated json key answers its last, as json_object_field does."""
    found = f"list_filter({members(value)}, _m -> {string_read('_m[1]')} = {key})"
    return f"CASE WHEN left({value}, 1) = '{{' THEN {found}[-1][2] END"


def _element(value: str, index: str) -> str:
    return (
        f"CASE WHEN left({value}, 1) = '[' THEN "
        + let(
            "_a",
            members(value),
            f"list_transform([CASE WHEN {index} < 0 THEN len(_a) + {index} ELSE {index} END], "
            "_n -> CASE WHEN _n >= 0 AND _n < len(_a) THEN _a[_n + 1][2] END)[1]",
        )
        + " END"
    )


def field(value: str, key: str, binary: bool) -> str:
    """json_object_field, or jsonb_object_field of a ``binary`` value."""
    read = canonical("_v") if binary else trimmed("_v")
    return let("_v", as_text(value), let("_w", read, let("_f", key, _field("_w", "_f"))))


def element(value: str, index: str, binary: bool) -> str:
    """json_array_element, or jsonb_array_element of a ``binary`` value."""
    read = canonical("_v") if binary else trimmed("_v")
    return let("_v", as_text(value), let("_w", read, _element("_w", index)))


def _path_step(value: str, step: str) -> str:
    """One step of a path into ``value``: a key of an object, an integer index of an array."""
    index = f"TRY_CAST({step} AS INTEGER)"
    return (
        f"CASE WHEN {step} IS NULL THEN NULL WHEN left({value}, 1) = '{{' THEN {_field(value, step)} "
        f"WHEN left({value}, 1) = '[' AND {index} IS NOT NULL THEN {_element(value, index)} END"
    )


def extract_path(value: str, path: str, binary: bool) -> str:
    """json_extract_path, as a reduce over the path's elements with the value so far as its state."""
    read = canonical("_v") if binary else trimmed("_v")
    reduced = (
        "list_reduce(list_transform(_l, _p -> [_p]), "
        f"(_u, _z) -> [{let('_q', '_z[1]', _path_step('_u[1]', '_q'))}], [_w])[1]"
    )
    return let(
        "_v",
        as_text(value),
        let(
            "_w",
            read,
            let("_l", path, f"CASE WHEN _w IS NULL OR _l IS NULL THEN NULL ELSE {reduced} END"),
        ),
    )


def text_of(value: str) -> str:
    return let("_b", value, scalar_text("_b"))


# --- objects and arrays: whole-value operations --------------------------------------------------

_ENTRY = "CASE WHEN _m[1] IS NULL THEN _m[2] ELSE _m[1] || ': ' || _m[2] END"


def written(entries: str) -> str:
    return _joined(entries, _ENTRY, "', '")


def _deconstruct_error(function: str, what: str) -> str:
    return raising("22023", _quoted(f"cannot {function} {what}"))


def array_length(value: str) -> str:
    """jsonb_array_length."""
    return let(
        "_v",
        as_text(value),
        let(
            "_w",
            trimmed("_v"),
            f"CASE WHEN _w IS NULL THEN CAST(NULL AS INTEGER) WHEN left(_w, 1) = '[' "
            f"THEN CAST(len({members('_w')}) AS INTEGER) WHEN left(_w, 1) = '{{' THEN "
            + raising("22023", "'cannot get array length of a non-array'")
            + " ELSE "
            + raising("22023", "'cannot get array length of a scalar'")
            + " END",
        ),
    )


def _rows(
    value: str,
    binary: bool,
    each: str,
    wanted: str,
    on_other: str,
    on_scalar: str,
) -> str:
    """The list of rows a set-returning function unnests: ``each`` of a member ``_m``. A value
    that is no ``wanted`` container is refused as ``on_other`` and ``on_scalar`` say."""
    read = canonical("_v") if binary else trimmed("_v")
    return let(
        "_v",
        as_text(value),
        let(
            "_w",
            read,
            f"CASE WHEN _w IS NULL THEN NULL WHEN left(_w, 1) = '{wanted}' "
            f"THEN list_transform({members('_w')}, _m -> {each}) "
            f"WHEN left(_w, 1) IN ('{{', '[') THEN {on_other} ELSE {on_scalar} END",
        ),
    )


def each_rows(value: str, name: str, binary: bool, text: bool) -> str:
    """The rows of json_each and its kin, key and value."""
    shown = scalar_text("_m[2]") if text else "_m[2]"
    each = f"struct_pack(key := {string_read('_m[1]')}, value := {shown})"
    if binary:
        return _rows(
            value, True, each, "{",
            raising("22023", _quoted(f"cannot call {name} on a non-object")),
            raising("22023", _quoted(f"cannot call {name} on a non-object")),
        )  # fmt: skip
    return _rows(
        value, False, each, "{",
        raising("22023", "'cannot deconstruct an array as an object'"),
        raising("22023", "'cannot deconstruct a scalar'"),
    )  # fmt: skip


def element_rows(value: str, name: str, binary: bool, text: bool) -> str:
    """The rows of json_array_elements and its kin."""
    shown = scalar_text("_m[2]") if text else "_m[2]"
    if binary:
        return _rows(
            value, True, shown, "[",
            raising("22023", "'cannot extract elements from an object'"),
            raising("22023", "'cannot extract elements from a scalar'"),
        )  # fmt: skip
    return _rows(
        value, False, shown, "[",
        raising("22023", _quoted(f"cannot call {name} on a non-array")),
        raising("22023", _quoted(f"cannot call {name} on a scalar")),
    )  # fmt: skip


def key_rows(value: str, name: str, binary: bool) -> str:
    """The rows of json_object_keys, of an object alone."""
    return _rows(
        value, binary, string_read("_m[1]"), "{",
        raising("22023", _quoted(f"cannot call {name} on an array")),
        raising("22023", _quoted(f"cannot call {name} on a scalar")),
    )  # fmt: skip


def stripped_null(value: str, form: str) -> str:
    """A strict form of a value's text, ``form`` over its variable ``_v``."""
    return let("_v", as_text(value), f"CASE WHEN _v IS NULL THEN NULL ELSE {form} END")


def concat(a: str, b: str) -> str:
    """jsonb_concat: two objects merge, two arrays join, and anything else is an array of both,
    a scalar as an element and an array's elements as its own."""

    def spread(side: str) -> str:
        return f"CASE WHEN left({side}, 1) = '[' THEN {written(members(side))} ELSE {side} END"

    both = (
        f"CASE WHEN left(_x, 1) = '{{' AND left(_y, 1) = '{{' THEN "
        f"'{{' || array_to_string(list_filter([{written(members('_x'))}, "
        f"{written(members('_y'))}], _z -> _z <> ''), ', ') || '}}' "
        f"ELSE '[' || array_to_string(list_filter([{spread('_x')}, {spread('_y')}], "
        "_z -> _z <> ''), ', ') || ']' END"
    )
    return let(
        "_x",
        canonical(as_text(a)),
        let(
            "_y",
            canonical(as_text(b)),
            f"CASE WHEN _x IS NULL OR _y IS NULL THEN NULL ELSE {canonical(both)} END",
        ),
    )


def _keys_and_strings(value: str) -> str:
    """What the existence operators look through: an object's keys, an array's strings, a string."""
    scalars = (
        f"list_transform(list_filter({members(value)}, _m -> left(_m[2], 1) = '\"'), "
        f"_m -> {string_read('_m[2]')})"
    )
    keys = f"list_transform({members(value)}, _m -> {string_read('_m[1]')})"
    return (
        f"CASE WHEN left({value}, 1) = '{{' THEN {keys} WHEN left({value}, 1) = '[' THEN {scalars} "
        f"WHEN left({value}, 1) = '\"' THEN [{string_read(value)}] ELSE [] END"
    )


def exists(value: str, keys: str, every: bool) -> str:
    """jsonb_exists_any and jsonb_exists_all: a NULL key is in nothing."""
    count = "len(list_filter(_l, _a -> _a IS NOT NULL AND list_contains(_h, _a)))"
    wanted = "len(list_filter(_l, _a -> _a IS NOT NULL))"
    test = f"{count} = {wanted}" if every else f"{count} > 0"
    return let(
        "_v",
        as_text(value),
        let(
            "_w",
            trimmed("_v"),
            let(
                "_l",
                keys,
                "CASE WHEN _w IS NULL OR _l IS NULL THEN NULL ELSE "
                + let("_h", _keys_and_strings("_w"), test)
                + " END",
            ),
        ),
    )


def delete_key(value: str, key: str) -> str:
    """jsonb_delete(jsonb, text): a key of an object, or a string of an array, equal to ``key``."""
    gone = (
        "_m -> (CASE WHEN _m[1] IS NULL THEN left(_m[2], 1) <> '\"' OR "
        f"{string_read('_m[2]')} <> _f ELSE {string_read('_m[1]')} <> _f END)"
    )
    return let(
        "_f",
        key,
        stripped_null(
            value,
            let(
                "_w",
                canonical("_v"),
                "CASE WHEN _f IS NULL THEN NULL WHEN left(_w, 1) NOT IN ('{', '[') THEN "
                + raising("22023", "'cannot delete from scalar'")
                + f" ELSE left(_w, 1) || {written(f'list_filter({members("_w")}, {gone})')} "
                "|| right(_w, 1) END",
            ),
        ),
    )


def delete_index(value: str, index: str) -> str:
    """jsonb_delete(jsonb, integer)."""
    kept = "list_filter(_a, (_m, _n) -> _n - 1 <> CASE WHEN _i < 0 THEN len(_a) + _i ELSE _i END)"
    return let(
        "_i",
        index,
        stripped_null(
            value,
            let(
                "_w",
                canonical("_v"),
                "CASE WHEN _i IS NULL THEN NULL WHEN left(_w, 1) = '{' THEN "
                + raising("22023", "'cannot delete from object using integer index'")
                + " WHEN left(_w, 1) <> '[' THEN "
                + raising("22023", "'cannot delete from scalar'")
                + " ELSE "
                + let("_a", members("_w"), f"'[' || {written(kept)} || ']'")
                + " END",
            ),
        ),
    )


def delete_array(value: str, keys: str) -> str:
    """jsonb_delete(jsonb, VARIADIC text[]): each named key or string goes."""
    gone = (
        "_m -> NOT coalesce(list_contains(_l, CASE WHEN _m[1] IS NULL THEN "
        f"CASE WHEN left(_m[2], 1) = '\"' THEN {string_read('_m[2]')} END "
        f"ELSE {string_read('_m[1]')} END), false)"
    )
    return let(
        "_l",
        keys,
        stripped_null(
            value,
            let(
                "_w",
                canonical("_v"),
                "CASE WHEN _l IS NULL THEN NULL WHEN len(_l) = 0 THEN _w "
                "WHEN left(_w, 1) NOT IN ('{', '[') THEN "
                + raising("22023", "'cannot delete from scalar'")
                + f" ELSE left(_w, 1) || {written(f'list_filter({members("_w")}, {gone})')} "
                "|| right(_w, 1) END",
            ),
        ),
    )


# --- a path into a value: set, insert, delete ---------------------------------------------------


def _path_edit(value: str, path: str, new: str, op: str, scalar_error: str) -> str:
    """setPath, as a reduce over the path: its state is the text before the value being reached,
    that value, the text after it and whether the walk has ended, so the edit is made in place and
    the result is read as jsonb again. ``op`` is the operation, one of c(reate or replace),
    r(eplace), d(elete), b(efore) and a(fter), as an expression over ``new``."""
    walk_object = (
        "list_transform([list_position(list_transform(_ms, _m -> "
        f"{string_read('_m[1]')}), _st)], _at -> CASE WHEN _at > 0 AND _last THEN "
        "CASE _o WHEN 'd' THEN [_s[1], '{' || "
        + written("list_concat(_ms[1:_at - 1], _ms[_at + 1:])")
        + " || '}', _s[3], '1'] "
        "WHEN 'b' THEN " + raising("22023", "'cannot replace existing key'") + " "
        "WHEN 'a' THEN " + raising("22023", "'cannot replace existing key'") + " "
        "ELSE [_s[1], '{' || "
        + written("list_concat(_ms[1:_at - 1], [[_ms[_at][1], _n]], _ms[_at + 1:])")
        + " || '}', _s[3], '1'] END "
        "WHEN _at > 0 THEN [_s[1] || '{' || "
        + written("_ms[1:_at - 1]")
        + " || CASE WHEN _at > 1 THEN ', ' ELSE '' END || _ms[_at][1] || ': ', _ms[_at][2], "
        "CASE WHEN _at < len(_ms) THEN ', ' ELSE '' END || "
        + written("_ms[_at + 1:]")
        + " || '}' || _s[3], ''] "
        "WHEN _last AND _o <> 'r' AND _o <> 'd' THEN [_s[1], '{' || array_to_string("
        "list_filter([" + written("_ms") + f", {string_written('_st')} || ': ' || _n], "
        "_z -> _z <> ''), ', ') || '}', _s[3], '1'] "
        "ELSE [_s[1], _s[2], _s[3], '1'] END)[1]"
    )
    placed = (
        "CASE _o WHEN 'd' THEN list_concat(_ms[1:_at], _ms[_at + 2:]) "
        "WHEN 'b' THEN list_concat(_ms[1:_at], [[NULL, _n]], _ms[_at + 1:]) "
        "WHEN 'a' THEN list_concat(_ms[1:_at + 1], [[NULL, _n]], _ms[_at + 2:]) "
        "ELSE list_concat(_ms[1:_at], [[NULL, _n]], _ms[_at + 2:]) END"
    )
    walk_array = (
        "CASE WHEN NOT regexp_matches(_st, '^[ ]*[-+]?[0-9]+[ ]*$') THEN "
        + raising(
            "22P02",
            "'path element at position ' || _e[2] || ' is not an integer: \"' || _st || '\"'",
        )
        + " ELSE list_transform([CAST(_st AS INTEGER)], _ix -> "
        "list_transform([CASE WHEN _ix < 0 AND -_ix > len(_ms) THEN -1 WHEN _ix < 0 "
        "THEN len(_ms) + _ix WHEN _ix > len(_ms) THEN len(_ms) ELSE _ix END], _at -> "
        # Nothing at the index: a value is put at the end named, or the walk ends.
        "CASE WHEN (_at = -1 OR len(_ms) = 0) AND _last AND _o NOT IN ('r', 'd') "
        "THEN [_s[1], '[' || array_to_string(list_filter([_n, " + written("_ms") + "], "
        "_z -> _z <> ''), ', ') || ']', _s[3], '1'] "
        "WHEN _at = len(_ms) AND _last AND _o NOT IN ('r', 'd') "
        "THEN [_s[1], '[' || array_to_string(list_filter([" + written("_ms") + ", _n], "
        "_z -> _z <> ''), ', ') || ']', _s[3], '1'] "
        "WHEN _at = -1 OR _at >= len(_ms) THEN [_s[1], _s[2], _s[3], '1'] "
        f"WHEN _last THEN [_s[1], '[' || {written(placed)} || ']', _s[3], '1'] "
        "ELSE [_s[1] || '[' || " + written("_ms[1:_at]") + " || "
        "CASE WHEN _at > 0 THEN ', ' ELSE '' END, _ms[_at + 1][2], "
        "CASE WHEN _at + 1 < len(_ms) THEN ', ' ELSE '' END || "
        + written("_ms[_at + 2:]")
        + " || ']' || _s[3], ''] END)[1])[1] END"
    )
    step = (
        "CASE WHEN _s[4] = '1' THEN _s ELSE "
        + let(
            "_st",
            "_e[1]",
            let(
                "_last",
                "_e[3] = '1'",
                "CASE WHEN left(_s[2], 1) NOT IN ('{', '[') THEN [_s[1], _s[2], _s[3], '1'] ELSE "
                + let(
                    "_ms",
                    members("_s[2]"),
                    f"CASE WHEN left(_s[2], 1) = '{{' THEN {walk_object} ELSE {walk_array} END",
                )
                + " END",
            ),
        )
        + " END"
    )
    steps = (
        "list_transform(range(1, len(_l) + 1), _i -> "
        "[_l[_i], CAST(_i AS VARCHAR), CASE WHEN _i = len(_l) THEN '1' ELSE '' END, ''])"
    )
    reduced = f"list_reduce({steps}, (_s, _e) -> {step}, ['', _w, '', ''])"
    null_at = "list_filter(range(1, len(_l) + 1), _i -> _l[_i] IS NULL)"
    return let(
        "_v",
        as_text(value),
        let(
            "_w",
            canonical("_v"),
            let(
                "_l",
                path,
                let(
                    "_n",
                    new,
                    let(
                        "_o",
                        op,
                        "CASE WHEN _w IS NULL OR _l IS NULL OR _o IS NULL THEN NULL "
                        "WHEN left(_w, 1) NOT IN ('{', '[') THEN "
                        + raising("22023", _quoted(scalar_error))
                        + " WHEN len(_l) = 0 THEN _w "
                        + f"WHEN len({null_at}) > 0 THEN "
                        + raising(
                            "22004",
                            f"'path element at position ' || {null_at}[1] || ' is null'",
                        )
                        + " ELSE "
                        + let("_r", reduced, canonical("_r[1] || _r[2] || _r[3]"))
                        + " END",
                    ),
                ),
            ),
        ),
    )


def set_path(value: str, path: str, new: str, create: str) -> str:
    """jsonb_set."""
    op = f"CASE WHEN {create} THEN 'c' WHEN NOT {create} THEN 'r' END"
    return let(
        "_nv",
        new,
        "CASE WHEN _nv IS NULL THEN NULL ELSE "
        + _path_edit(value, path, canonical(as_text("_nv")), op, "cannot set path in scalar")
        + " END",
    )


def insert_path(value: str, path: str, new: str, after: str) -> str:
    """jsonb_insert."""
    op = f"CASE WHEN {after} THEN 'a' WHEN NOT {after} THEN 'b' END"
    return let(
        "_nv",
        new,
        "CASE WHEN _nv IS NULL THEN NULL ELSE "
        + _path_edit(value, path, canonical(as_text("_nv")), op, "cannot set path in scalar")
        + " END",
    )


def delete_path(value: str, path: str) -> str:
    """jsonb_delete_path."""
    return _path_edit(value, path, "NULL", "'d'", "cannot delete path in scalar")


_TREATMENTS = "'delete_key', 'return_target', 'use_json_null', 'raise_exception'"


def set_lax(value: str, path: str, new: str, create: str, treatment: str) -> str:
    """jsonb_set_lax: as jsonb_set, and a NULL new value is what ``treatment`` says."""
    op = f"CASE WHEN {create} THEN 'c' WHEN NOT {create} THEN 'r' END"
    return let(
        "_nv",
        new,
        let(
            "_tr",
            treatment,
            f"CASE WHEN {value} IS NULL OR {path} IS NULL OR {create} IS NULL THEN NULL "
            f"WHEN _tr IS NULL OR _tr NOT IN ({_TREATMENTS}) THEN "
            + raising(
                "22023",
                """'null_value_treatment must be "delete_key", "return_target", """
                """"use_json_null", or "raise_exception"'""",
            )
            + " WHEN _nv IS NOT NULL THEN "
            + _path_edit(value, path, canonical(as_text("_nv")), op, "cannot set path in scalar")
            + " WHEN _tr = 'raise_exception' THEN "
            + raising("22004", "'JSON value must not be null'")
            + " WHEN _tr = 'return_target' THEN "
            + canonical(as_text(value))
            + " WHEN _tr = 'delete_key' THEN "
            + delete_path(value, path)
            + " ELSE "
            + _path_edit(value, path, "'null'", op, "cannot set path in scalar")
            + " END",
        ),
    )


# --- arrays of text as objects ------------------------------------------------------------------

_ODD = raising("2202E", "'array must have even number of elements'")
_NULL_KEY = raising("22004", "'null value not allowed for object key'")
_DIFFERENT = raising("2202E", "'mismatched array dimensions'")


def _object_pair(key: str, value: str, separator: str) -> str:
    return (
        f"CASE WHEN {key} IS NULL THEN {_NULL_KEY} ELSE "
        f"{string_written(key)} || '{separator}' || "
        f"CASE WHEN {value} IS NULL THEN 'null' ELSE {string_written(value)} END END"
    )


def object_of_one(array: str, binary: bool) -> str:
    """jsonb_object(text[]) and json_object(text[]): keys and values in turn, or two-item rows.
    The two-dimensional form is the same flattened."""
    flat = "CAST(flatten(CAST(CASE WHEN typeof(_a) LIKE '%[][]' THEN _a ELSE [_a] END AS VARCHAR[][])) AS VARCHAR[])"
    separator = ": " if binary else " : "
    pair = _object_pair("_l[_i]", "_l[_i + 1]", separator)
    whole = (
        "'{' || array_to_string(list_transform(range(1, len(_l) + 1, 2), _i -> "
        + pair
        + "), ', ') || '}'"
    )
    written_form = canonical(whole) if binary else whole
    return let(
        "_a",
        array,
        "CASE WHEN _a IS NULL THEN NULL ELSE "
        + let(
            "_l",
            flat,
            f"CASE WHEN len(_l) % 2 = 1 THEN {_ODD} ELSE {written_form} END",
        )
        + " END",
    )


def object_of_two(keys: str, values: str, binary: bool) -> str:
    """jsonb_object(text[], text[]) and json_object(text[], text[])."""
    separator = ": " if binary else " : "
    pair = _object_pair("_k[_i]", "_x[_i]", separator)
    whole = f"'{{' || array_to_string(list_transform(range(1, len(_k) + 1), _i -> {pair}), ', ') || '}}'"
    written_form = canonical(whole) if binary else whole
    return let(
        "_k",
        keys,
        let(
            "_x",
            values,
            "CASE WHEN _k IS NULL OR _x IS NULL THEN NULL "
            f"WHEN len(_k) <> len(_x) THEN {_DIFFERENT} ELSE {written_form} END",
        ),
    )


# --- a SQL value as a JSON value ----------------------------------------------------------------


def value_text(value: str) -> str:
    """to_json of a value, by the type the engine holds it in: every branch binds for every type,
    as the casts are to text, and only the one the type names runs. SQL NULL is json null."""
    bytes_ = f"lower(hex(CAST(CAST({value} AS VARCHAR) AS BLOB)))"
    float_ = (
        f"CASE CAST({value} AS VARCHAR) WHEN 'nan' THEN '\"NaN\"' "
        "WHEN 'inf' THEN '\"Infinity\"' WHEN '-inf' THEN '\"-Infinity\"' "
        f"ELSE CAST({value} AS VARCHAR) END"
    )
    return (
        f"CASE WHEN {value} IS NULL THEN 'null' WHEN typeof({value}) = 'JSON' "
        f"THEN CAST({value} AS VARCHAR) WHEN typeof({value}) = 'VARCHAR' "
        f"THEN {string_written(f'CAST({value} AS VARCHAR)')} "
        f"WHEN typeof({value}) IN ('DOUBLE', 'FLOAT') THEN {float_} "
        f"WHEN typeof({value}) = 'DATE' THEN '\"' || CAST({value} AS VARCHAR) || '\"' "
        f"WHEN typeof({value}) = 'TIMESTAMP' "
        f"THEN '\"' || replace(CAST({value} AS VARCHAR), ' ', 'T') || '\"' "
        f"WHEN typeof({value}) = 'TIMESTAMP WITH TIME ZONE' "
        f"THEN '\"' || replace(CAST({value} AS VARCHAR), ' ', 'T') || ':00\"' "
        f"WHEN typeof({value}) = 'BLOB' "
        f"THEN '\"' || chr(92) || chr(92) || 'x' || {bytes_} || '\"' "
        f"WHEN typeof({value}) LIKE '%[]' OR typeof({value}) LIKE 'STRUCT%' "
        f"THEN CAST(to_json({value}) AS VARCHAR) ELSE CAST({value} AS VARCHAR) END"
    )


def _to_json(value: str, binary: bool) -> str:
    """to_json and to_jsonb: a NULL is SQL's."""
    text = let("_x", value, f"CASE WHEN _x IS NULL THEN NULL ELSE {value_text('_x')} END")
    return canonical(text) if binary else text


def _built(texts: str, binary: bool) -> str:
    """json_build_array of values already json text."""
    whole = f"'[' || array_to_string({texts}, ', ') || ']'"
    return canonical(whole) if binary else whole


def _key_of(key: str, null_key: str) -> str:
    """A json_build_object key: any value's text."""
    return (
        f"CASE WHEN {key} IS NULL THEN {null_key} "
        f"ELSE {let('_ks', f'CAST({key} AS VARCHAR)', string_written('_ks'))} END"
    )


def _null_key(binary: bool, position: int) -> str:
    if binary:
        return raising("22023", _quoted(f"argument {position}: key must not be null"))
    return _NULL_KEY


def _object_of_array(array: str, binary: bool) -> str:
    """json_build_object(VARIADIC array): keys and values in turn."""
    pair = (
        f"{_key_of('_l[_i]', _null_key(binary, 1))} || ' : ' || "
        f"{let('_x', '_l[_i + 1]', value_text('_x'))}"
    )
    whole = (
        f"'{{' || array_to_string(list_transform(range(1, len(_l) + 1, 2), _i -> {pair}), ', ') "
        "|| '}'"
    )
    return let(
        "_l",
        array,
        "CASE WHEN _l IS NULL THEN NULL WHEN len(_l) % 2 = 1 THEN "
        + raising("22023", "'argument list must have even number of elements'")
        + f" ELSE {canonical(whole) if binary else whole} END",
    )


def _aggregated(
    value: str, binary: bool, strict_: bool, opening: str = "[", closing: str = "]"
) -> str:
    """The array of an aggregate's input. No row is NULL; a strict one's nulls go, so that all
    nulls are an empty array."""
    item = f"CASE WHEN {value} IS NULL THEN {'NULL' if strict_ else repr('null')} ELSE {let('_x', value, value_text('_x'))} END"
    whole = (
        f"CASE WHEN count(*) = 0 THEN NULL ELSE '{opening}' || "
        f"coalesce(string_agg({item}, ', '), '') || '{closing}' END"
    )
    return canonical(whole) if binary else whole


def _object_aggregated(key: str, value: str, binary: bool, strict_: bool, unique: bool) -> str:
    """json_object_agg and its kin. A json object keeps a repeated key, with { "a" : 1 } as its
    spaces; a jsonb one keeps the last. A unique one refuses a repeat, a strict one drops a NULL
    value, and a key of NULL is refused, which comes first in each row."""
    null_key = raising("22023", "'field name must not be null'") if binary else _NULL_KEY
    item = (
        f"CASE WHEN {key} IS NULL THEN {null_key} "
        f"WHEN {value} IS NULL THEN {'NULL' if strict_ else repr('null')} "
        f"ELSE {let('_x', value, value_text('_x'))} END"
    )
    keyed = f"CASE WHEN ({item}) IS NULL THEN NULL ELSE {let('_ks', f'CAST({key} AS VARCHAR)', string_written('_ks'))} || ' : ' || ({item}) END"
    keys = f"list(CASE WHEN ({item}) IS NOT NULL THEN CAST({key} AS VARCHAR) END) FILTER (WHERE ({item}) IS NOT NULL)"
    message = (
        "'duplicate JSON object key value'"
        if binary
        else (
            "'duplicate JSON object key value: \"' || "
            "list_filter(_u, (_a, _i) -> list_position(_u, _a) <> _i)[1] || '\"'"
        )
    )
    joined = (
        f"CASE WHEN count(*) = 0 THEN NULL ELSE '{{ ' || "
        f"coalesce(string_agg({keyed}, ', '), '') || ' }}' END"
    )
    if binary:
        joined = canonical(joined)
    if not unique:
        return joined
    return (
        f"CASE WHEN {let('_u', keys, 'len(_u) <> len(list_distinct(_u))')} THEN "
        + let("_u", keys, raising("22030", message))
        + f" ELSE {joined} END"
    )


def row_json(value: str, pretty_: str | None) -> str:
    """row_to_json of a row, a struct to the engine: its own to_json is row_to_json's form."""
    text = let("_x", value, "CAST(to_json(_x) AS VARCHAR)")
    if pretty_ is None:
        return text
    lined = (
        "'{' || array_to_string(list_transform(" + members("_w") + ", "
        "_m -> _m[1] || ':' || _m[2]), ',' || chr(10) || ' ') || '}'"
    )
    return let("_w", text, f"CASE WHEN {pretty_} THEN {lined} ELSE _w END")


# --- records ------------------------------------------------------------------------------------


def _field_of(obj: str, name: str, read: Callable[[str], str]) -> str:
    """``read`` of an object's field, over ``_w``: a field that is missing or null is NULL."""
    found = let("_o", obj, let("_f", name, _field("_o", "_f")))
    return let("_w", found, f"CASE WHEN _w IS NULL OR _w = 'null' THEN NULL ELSE {read('_w')} END")


def record_object(value: str) -> str:
    """The object json_to_record reads, refused as populate_composite refuses another value."""
    return let(
        "_w",
        trimmed(as_text(value)),
        "CASE WHEN left(_w, 1) = '{' THEN _w WHEN left(_w, 1) = '[' THEN "
        + raising("22023", "'cannot call populate_composite on an array'")
        + " WHEN _w IS NOT NULL THEN "
        + raising("22023", "'cannot call populate_composite on a scalar'")
        + " END",
    )


def record_objects(value: str, name: str) -> str:
    """The objects json_to_recordset reads; ``name`` is the function, for its refusals."""
    refused = raising("22023", f"'cannot call ' || {name} || ' on a non-array'")
    not_objects = raising("22023", f"'argument of ' || {name} || ' must be an array of objects'")
    return let(
        "_w",
        trimmed(as_text(value)),
        f"CASE WHEN _w IS NULL THEN NULL WHEN left(_w, 1) <> '[' THEN {refused} ELSE "
        f"list_transform({members('_w')}, _m -> CASE WHEN left(_m[2], 1) = '{{' THEN _m[2] "
        f"ELSE {not_objects} END) END",
    )


def _nodes(value: str) -> str:
    """Every value inside ``value``, the root first, each [id, parent's id, key, value, key to
    compare a scalar by]: a list grown one level at a time, for as many levels as there are open
    brackets."""
    expanded = (
        "flatten(list_transform(list_filter(_s, _y -> _y[6] = '1'), _y -> "
        "list_transform(" + members("_y[4]") + ", (_m, _j) -> "
        "[_y[1] || '.' || _j, _y[1], CASE WHEN _m[1] IS NULL THEN NULL ELSE "
        + string_read("_m[1]")
        + " END, _m[2], NULL, CASE WHEN left(_m[2], 1) IN ('{', '[') THEN '1' ELSE '' END])))"
    )
    grown = (
        "list_concat(list_transform(_s, _y -> [_y[1], _y[2], _y[3], _y[4], _y[5], '']), "
        + expanded
        + ")"
    )
    depth = "len(list_filter(" + _tokens("_r") + ", _i -> _i IN ('{', '[')))"
    return let(
        "_r",
        value,
        f"list_reduce(list_transform(range(0, {depth}), _i -> [[CAST(_i AS VARCHAR)]]), "
        f"(_s, _e) -> {grown}, [['0', NULL, NULL, _r, NULL, "
        "CASE WHEN left(_r, 1) IN ('{', '[') THEN '1' ELSE '' END]])",
    )


def contains(a: str, b: str) -> str:
    """jsonb_contains: JsonbDeepContains read bottom-up. The template ``b`` is folded, each of its
    values becoming the ids of the values of ``a`` that contain it, so a container's answer is
    read from its members'. A top-level scalar is also contained in an array that holds it."""

    def ids(found: str) -> str:
        return f"array_to_string(list_transform({found}, _y -> _y[1]), ',')"

    scalar = (
        "list_transform(["
        + f"CASE WHEN _t IN ('{{', '[') THEN NULL ELSE {_KEY_ATOM} END"
        + "], _z -> "
        + ids(
            "list_filter(_na, _y -> left(_y[4], 1) NOT IN ('{', '[') AND "
            + let("_t", "_y[4]", _KEY_ATOM)
            + " = _z)"
        )
        + ")[1]"
    )
    has_member = (
        "len(list_filter(_na, _x -> _x[2] = _y[1] AND {match} "
        "AND list_contains(string_split(_m[3], ','), _x[1]))) > 0"
    )

    def container(opening: str, match: str) -> str:
        lacking = f"list_filter(_c, _m -> NOT ({has_member.format(match=match)}))"
        return ids(f"list_filter(_na, _y -> left(_y[4], 1) = '{opening}' AND len({lacking}) = 0)")

    obj = container("{", "_x[3] = _m[2]")
    array = container("[", "true")
    fold = _fold("_vb", scalar, string_read("_t"), _JSONB_SORT, obj, array)
    # The empty text is no id, and a value contained nowhere has no id at all.
    answer = (
        "CASE WHEN left(_va, 1) = '[' AND left(_vb, 1) = '{' OR "
        "left(_va, 1) = '{' AND left(_vb, 1) <> '{' THEN false "
        "ELSE list_contains(string_split(_i, ','), '0') OR "
        "(left(_vb, 1) NOT IN ('{', '[') AND left(_va, 1) = '[' AND "
        "len(list_filter(string_split(_i, ','), _z -> regexp_matches(_z, '^0[.][0-9]+$'))) > 0) END"
    )
    return let(
        "_va",
        canonical(as_text(a)),
        let(
            "_vb",
            canonical(as_text(b)),
            "CASE WHEN _va IS NULL OR _vb IS NULL THEN NULL ELSE "
            + let("_na", _nodes("_va"), let("_i", fold, answer))
            + " END",
        ),
    )


def compare(a: str, b: str, operator: str) -> str:
    """``a`` ``operator`` ``b``, two jsonb values in compareJsonbContainers' order."""
    return f"{compare_key(as_text(a))} {operator} {compare_key(as_text(b))}"


def compared(a: str, b: str) -> str:
    """jsonb_cmp."""
    return let(
        "_ka",
        compare_key(as_text(a)),
        let(
            "_kb",
            compare_key(as_text(b)),
            "CASE WHEN _ka < _kb THEN -1 WHEN _ka > _kb THEN 1 WHEN _ka = _kb THEN 0 END",
        ),
    )


def _json(form: str) -> str:
    return f"CAST({form} AS JSON)"


def _text(form: str) -> str:
    return f"CAST({form} AS VARCHAR)"


def _both_kinds() -> tuple[tuple[str, str, str], ...]:
    """The functions json and jsonb both have, by their names: a row's name, parameters, form."""
    rows: list[tuple[str, str, str]] = []
    for kind, binary in (("json", False), ("jsonb", True)):
        extract = f"pg_{kind}_extract_path" if kind == "json" else f"{kind}_extract_path"
        rows += [
            (
                f"{kind}_object_field",
                "value, key",
                _json(field("value", "key", binary)),
            ),
            (
                f"{kind}_object_field_text",
                "value, key",
                _text(text_of(field("value", "key", binary))),
            ),
            (
                f"{kind}_array_element",
                "value, position",
                _json(element("value", "position", binary)),
            ),
            (
                f"{kind}_array_element_text",
                "value, position",
                _text(text_of(element("value", "position", binary))),
            ),
            (extract, "value, path", _json(extract_path("value", "path", binary))),
            (
                f"{extract}_text",
                "value, path",
                _text(text_of(extract_path("value", "path", binary))),
            ),
            (
                f"{kind}_each",
                "value",
                f"unnest({each_rows('value', f'{kind}_each', binary, False)})",
            ),
            (
                f"{kind}_each_text",
                "value",
                f"unnest({each_rows('value', f'{kind}_each_text', binary, True)})",
            ),
            (
                f"{kind}_array_elements",
                "value",
                f"unnest({element_rows('value', f'{kind}_array_elements', binary, False)})",
            ),
            (
                f"{kind}_array_elements_text",
                "value",
                f"unnest({element_rows('value', f'{kind}_array_elements_text', binary, True)})",
            ),
            (
                f"{kind}_object_keys",
                "value",
                f"unnest({key_rows('value', f'{kind}_object_keys', binary)})",
            ),
        ]
    return tuple(rows)


ROWS: tuple[tuple[str, str, str], ...] = (
    *_both_kinds(),
    ("json_typeof", "value", _text(stripped_null("value", typeof("_v")))),
    ("jsonb_array_length", "value", array_length("value")),
    ("pg_json_array_length", "value", array_length("value")),
    ("pg_json_strip_nulls", "value", _json(stripped_null("value", stripped_json("_v")))),
    ("jsonb_strip_nulls", "value", _json(stripped_null("value", stripped("_v")))),
    ("jsonb_pretty", "value", _text(stripped_null("value", pretty("_v")))),
    ("jsonb_concat", "a, b", _json(concat("a", "b"))),
    ("jsonb_cmp", "a, b", compared("a", "b")),
    *(
        (f"jsonb_{name}", "a, b", compare("a", "b", operator))
        for name, operator in (
            ("eq", "="),
            ("ne", "<>"),
            ("lt", "<"),
            ("le", "<="),
            ("gt", ">"),
            ("ge", ">="),
        )
    ),  # fmt: skip
    ("jsonb_exists_any", "value, keys", exists("value", "keys", False)),
    ("jsonb_exists_all", "value, keys", exists("value", "keys", True)),
    ("jsonb_delete", "value, key", _json(delete_key("value", "key"))),
    ("jsonb_delete_idx", "value, position", _json(delete_index("value", "position"))),
    ("jsonb_delete_array", "value, keys", _json(delete_array("value", "keys"))),
    ("jsonb_delete_path", "value, path", _json(delete_path("value", "path"))),
    ("jsonb_set", "value, path, new", _json(set_path("value", "path", "new", "true"))),
    ("jsonb_set", "value, path, new, create_", _json(set_path("value", "path", "new", "create_"))),
    ("jsonb_insert", "value, path, new", _json(insert_path("value", "path", "new", "false"))),
    (
        "jsonb_insert",
        "value, path, new, after_",
        _json(insert_path("value", "path", "new", "after_")),
    ),
    *(
        (
            "jsonb_set_lax",
            ", ".join(("value", "path", "new", *extra)),
            _json(set_lax("value", "path", "new", *defaults)),
        )
        for extra, defaults in (
            ((), ("true", "'use_json_null'")),
            (("create_",), ("create_", "'use_json_null'")),
            (("create_", "treatment"), ("create_", "treatment")),
        )
    ),
    ("pg_to_json", "value", _json(_to_json("value", False))),
    ("to_jsonb", "value", _json(_to_json("value", True))),
    ("pg_json_build", "texts", _json(_built("texts", False))),
    ("pg_jsonb_build", "texts", _json(_built("texts", True))),
    ("pg_json_value", "value", let("_x", "value", value_text("_x"))),
    ("pg_json_key", "k", let("_ks", as_text("k"), string_written("_ks"))),
    (
        "pg_json_values",
        "array",
        let("_l", "array", "list_transform(_l, _a -> " + let("_x", "_a", value_text("_x")) + ")"),
    ),
    ("pg_json_pairs", "array", _json(_object_of_array("array", False))),
    ("pg_jsonb_pairs", "array", _json(_object_of_array("array", True))),
    ("pg_json_verbatim", "value", _json(as_text("value"))),
    ("pg_jsonb_verbatim", "value", _json(canonical(as_text("value")))),
    ("pg_jsonb_agg", "value", _json(_aggregated("value", True, False))),
    ("json_agg_strict", "value", _json(_aggregated("value", False, True))),
    ("jsonb_agg_strict", "value", _json(_aggregated("value", True, True))),
    *(
        (
            f"{prefix}{kind}_object_agg{suffix}",
            "k, value",
            _json(
                _object_aggregated(
                    "k", "value", kind == "jsonb", "strict" in suffix, "unique" in suffix
                )
            ),
        )
        for prefix, kind in (("pg_", "json"), ("pg_", "jsonb"), ("", "json"), ("", "jsonb"))
        for suffix in ("", "_strict", "_unique", "_unique_strict")
        if (prefix == "") == (suffix != "")
    ),
    ("row_to_json", "value", _json(row_json("value", None))),
    ("row_to_json", "value, pretty_", _json(row_json("value", "pretty_"))),
    *(
        (
            f"{kind}_to_record",
            "value",
            "CASE WHEN value IS NOT NULL THEN error('could not determine row type for result of "
            f"{kind}_to_record') ELSE CAST(NULL AS VARCHAR) END",
        )
        for kind in ("json", "jsonb")
    ),
    ("pg_record_object", "value", record_object("value")),
    ("pg_record_objects", "value, name", record_objects("value", "name")),
    ("pg_json_field_text", "o, name", _field_of("o", "name", scalar_text)),
    ("pg_json_field_json", "o, name", _json(_field_of("o", "name", lambda w: w))),
    ("pg_json_field_jsonb", "o, name", _json(_field_of("o", "name", canonical))),
    ("pg_jsonb_contains", "a, b", contains("a", "b")),
    ("jsonb_contained", "a, b", contains("b", "a")),
    ("jsonb_object", "value", _json(object_of_one("value", True))),
    ("jsonb_object", "ks, vs", _json(object_of_two("ks", "vs", True))),
)

#: The functions sqlglot reads as a class of its own, which drops arguments or writes another
#: function: spelled here as a plain call, and named in a row by that spelling.
SPELLED = (
    "jsonb_contains",
    "json_extract_path",
    "json_extract_path_text",
    "json_strip_nulls",
    "json_object_agg",
    "jsonb_object_agg",
)
_SPELLED = re.compile(
    r"(\"?)\b(" + "|".join(sorted(SPELLED, key=len, reverse=True)) + r")\b\1(\s*\()", re.IGNORECASE
)
#: A row's spelling -> the function the statement called, which names its column.
CALLED = {
    **{f"pg_{name}": name for name in SPELLED},
    "pg_to_json": "to_json",
    "pg_jsonb_agg": "jsonb_agg",
    "pg_json_array_length": "json_array_length",
}


def spelled(pg_sql: str) -> str:
    return _SPELLED.sub(lambda m: f"pg_{m[2].lower()}{m[3]}", pg_sql)


#: The functions whose last parameter is a VARIADIC text[], by how many arguments come before it.
_VARIADIC_PATHS = {
    "pg_json_extract_path": 1,
    "pg_json_extract_path_text": 1,
    "jsonb_extract_path": 1,
    "jsonb_extract_path_text": 1,
}
#: A text[] parameter an unknown-typed literal is read as, by its position.
_TEXT_ARRAYS = {
    "jsonb_set": 1,
    "jsonb_set_lax": 1,
    "jsonb_insert": 1,
    "jsonb_delete_path": 1,
    "jsonb_exists_any": 1,
    "jsonb_exists_all": 1,
    "jsonb_object": 0,
}
#: The functions that take a row, which a statement may write as ROW(...) or NULL::record.
_ROW_ARGUMENT = ("to_json", "to_jsonb", "row_to_json")
#: The builders, whose arguments are of any type each.
_BUILDERS = {
    "json_build_array": False,
    "jsonb_build_array": True,
    "json_build_object": False,
    "jsonb_build_object": True,
}
CALLS = frozenset(
    {
        *_VARIADIC_PATHS,
        *_TEXT_ARRAYS,
        *_BUILDERS,
        *_ROW_ARGUMENT,
        "jsonb_delete",
        "jsonb_agg",
        "json_array_length",
    }
)


def _array_literal(node: exp.Expr, sql: Callable[[exp.Expr], str]) -> str:
    """A text[] argument: an unknown-typed '{a,b}' literal is that array."""
    if isinstance(node, exp.Literal) and node.is_string:
        elements = array_elements(node.this)
        two_dimensional = bool(elements) and isinstance(elements[0], list)
        kind = "TEXT[][]" if two_dimensional else "TEXT[]"
        return f"CAST({_as_array(elements).sql(dialect='postgres')} AS {kind})"
    if isinstance(node, exp.Variadic):
        return sql(node.this)
    return sql(node)


def _built_call(name: str, args: list[exp.Expr], sql: Callable[[exp.Expr], str]) -> str:
    """json_build_array and json_build_object, by what they are given: arguments in turn, or a
    VARIADIC array of them."""
    binary = _BUILDERS[name]
    prefix = "jsonb" if binary else "json"
    if len(args) == 1 and isinstance(args[0], exp.Variadic):
        array = sql(args[0].this)
        if name.endswith("_array"):
            return f"pg_{prefix}_build(pg_json_values({array}))"
        return f"pg_{prefix}_pairs({array})"
    if name.endswith("_array"):
        texts = ", ".join(f"pg_json_value({sql(a)})" for a in args)
        return f"pg_{prefix}_build(CAST(ARRAY[{texts}] AS TEXT[]))"
    if not args:
        return "pg_json_verbatim('{}')"
    if len(args) % 2:
        return raising("22023", "'argument list must have even number of elements'")
    each = [
        f"CASE WHEN ({sql(args[i])}) IS NULL THEN {_null_key(binary, i + 1)} "
        f"ELSE pg_json_key({sql(args[i])}) END || ' : ' || pg_json_value({sql(args[i + 1])})"
        for i in range(0, len(args), 2)
    ]
    whole = f"'{{' || ARRAY_TO_STRING(ARRAY[{', '.join(each)}], ', ') || '}}'"
    return f"pg_{prefix}_verbatim({whole})"


def _row(node: exp.Expr, sql: Callable[[exp.Expr], str]) -> str | None:
    """A row written in the statement, as a struct of PostgreSQL's field names f1, f2, ...; None
    for any other argument."""
    if isinstance(node, exp.Cast) and node.to.sql(dialect="postgres").lower() == "record":
        return "CAST(NULL AS TEXT)"  # a NULL of no field: only NULL can be cast to record
    if isinstance(node, exp.Tuple) or (
        isinstance(node, exp.Anonymous) and node.name.lower() == "row"
    ):
        fields = ", ".join(f"{sql(v)} AS f{i}" for i, v in enumerate(node.expressions, start=1))
        return f"STRUCT({fields})"
    return None


def _is_text_array(node: exp.Expr) -> bool:
    return isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.ARRAY)


def _is_integer(node: exp.Expr) -> bool:
    if isinstance(node, (exp.Neg, exp.Paren)):
        return _is_integer(node.this)
    if isinstance(node, exp.Cast):
        return node.to.this in exp.DataType.INTEGER_TYPES
    return isinstance(node, exp.Literal) and node.is_int


def call(name: str, args: list[exp.Expr], sql: Callable[[exp.Expr], str]) -> str | None:
    """A call as its row reads it: a VARIADIC parameter's arguments as one array, a text[] literal
    as an array, and the overload of jsonb_delete its second argument's type names, by its pg_proc
    name. None: the call is as its row reads it, or no row's."""
    if name in _VARIADIC_PATHS and len(args) > _VARIADIC_PATHS[name]:
        fixed = _VARIADIC_PATHS[name]
        rest = args[fixed:]
        if len(rest) == 1 and _is_text_array(rest[0]):
            return None  # read already: the statement passes here once for each of two rewrites
        array = (
            sql(rest[0].this)
            if len(rest) == 1 and isinstance(rest[0], exp.Variadic)
            else f"CAST(ARRAY[{', '.join(sql(a) for a in rest)}] AS TEXT[])"
        )
        return f"{name}({', '.join(sql(a) for a in args[:fixed])}, {array})"
    if name == "jsonb_delete" and len(args) >= 2:  # noqa: PLR2004 -- the value and what goes
        value, rest = sql(args[0]), args[1:]
        if len(rest) > 1 or isinstance(rest[0], exp.Variadic):
            array = (
                sql(rest[0].this)
                if isinstance(rest[0], exp.Variadic)
                else f"CAST(ARRAY[{', '.join(sql(a) for a in rest)}] AS TEXT[])"
            )
            return f"jsonb_delete_array({value}, {array})"
        return f"jsonb_delete_idx({value}, {sql(rest[0])})" if _is_integer(rest[0]) else None
    if name in _BUILDERS:
        return _built_call(name, args, sql)
    if name in _ROW_ARGUMENT and args:
        row = _row(args[0], sql)
        if row is not None or name == "to_json":
            given = sql(args[0]) if row is None else row
            target = "pg_to_json" if name == "to_json" else name
            return f"{target}({', '.join([given, *(sql(a) for a in args[1:])])})"
        return None
    if name == "json_array_length" and len(args) == 1:
        return f"pg_json_array_length({sql(args[0])})"
    if name == "jsonb_agg" and len(args) == 1:
        return f"pg_jsonb_agg({sql(args[0])})"
    position = _TEXT_ARRAYS.get(name)
    if position is not None and len(args) > position:
        written_args = [
            _array_literal(a, sql) if i == position else sql(a) for i, a in enumerate(args)
        ]
        return f"{name}({', '.join(written_args)})"
    return None


#: A set-returning function in FROM: its columns, which a row's struct names.
_TABLE_COLUMNS = {
    **{f"{k}_each": ("key", "value") for k in ("json", "jsonb")},
    **{f"{k}_each_text": ("key", "value") for k in ("json", "jsonb")},
    **{f"{k}_array_elements": ("value",) for k in ("json", "jsonb")},
    **{f"{k}_array_elements_text": ("value",) for k in ("json", "jsonb")},
    **{f"{k}_object_keys": () for k in ("json", "jsonb")},
}
#: The functions whose columns a statement gives, by how they read their object or objects.
_RECORDS = {
    "json_to_record": False,
    "jsonb_to_record": False,
    "json_to_recordset": True,
    "jsonb_to_recordset": True,
}
TABLE_FUNCTIONS = frozenset({*_TABLE_COLUMNS, *_RECORDS})


def _column_of(column: exp.ColumnDef) -> str:
    """A column of a record, read from ``_o`` as populate_record reads it."""
    name = exp.Literal.string(column.name).sql(dialect="postgres")
    kind = column.args["kind"]
    identifier = column.this.sql(dialect="postgres")
    if kind.this in (exp.DataType.Type.JSON, exp.DataType.Type.JSONB):
        reader = (
            "pg_json_field_jsonb" if kind.this == exp.DataType.Type.JSONB else "pg_json_field_json"
        )
        return f"{reader}(_o, {name}) AS {identifier}"
    return f"CAST(pg_json_field_text(_o, {name}) AS {kind.sql(dialect='postgres')}) AS {identifier}"


def _record_table(name: str, value: str, alias: exp.TableAlias) -> exp.Expr:
    columns = ", ".join(_column_of(c) for c in alias.columns)
    if _RECORDS[name]:
        quoted = exp.Literal.string(name).sql(dialect="postgres")
        objects = f"SELECT UNNEST(pg_record_objects({value}, {quoted})) AS _o"
    else:
        objects = f"SELECT pg_record_object({value}) AS _o"
    written = sqlglot.parse_one(f"SELECT {columns} FROM ({objects}) AS _r", read="postgres")
    return exp.Subquery(this=written, alias=exp.TableAlias(this=alias.this.copy()))


def table_form(node: exp.Table, sql: Callable[[exp.Expr], str]) -> exp.Expr | None:
    """A set-returning function of the family in FROM as a subquery of its rows: DuckDB's unnest
    is of the select list, and a struct's fields are its columns only there. None for any other
    table."""
    if isinstance(node.this, exp.Dot):
        function = node.this.expression
    else:
        function = node.this
    if not isinstance(function, exp.Anonymous) or function.name.lower() not in TABLE_FUNCTIONS:
        return None
    name = function.name.lower()
    call_sql = f"{name}({', '.join(sql(a) for a in function.expressions)})"
    alias = node.args.get("alias")
    if name in _RECORDS:
        if alias is None or not alias.columns:
            return None  # PostgreSQL's refusal: a record needs a column definition list
        return _record_table(name, sql(function.expressions[0]), alias)
    columns = _TABLE_COLUMNS[name]
    if len(columns) == 2:  # noqa: PLR2004 -- key and value, of a row's struct
        rows = f"SELECT _r.key AS key, _r.value AS value FROM (SELECT {call_sql} AS _r) AS _rows"
    else:
        # One column: named for the alias a statement gives it, else the function's.
        named = columns[0] if columns else (alias.name if alias and alias.name else name)
        rows = f"SELECT {call_sql} AS {exp.to_identifier(named).sql(dialect='postgres')}"
    subquery = exp.Subquery(this=sqlglot.parse_one(rows, read="postgres"))
    if alias is not None:
        subquery.set("alias", alias.copy())
    return subquery
