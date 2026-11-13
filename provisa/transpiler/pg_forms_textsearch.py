# Copyright (c) 2026 Kenneth Stott
# Canary: c7e15b02-84f3-49d6-a0be-3d62f9c1e874
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's text search functions as forms (REQ-2167, REQ-2171), replayed against postgres:17.11
on DuckDB, Trino and ClickHouse (tests/unit/pgwire/pg17_text_search_answers.py).

A tsvector is on the engine as the text tsvectorout writes: :func:`lowered` writes a literal's so,
and each row of ``ROWS`` reads that form alone and refuses any other by name, since a text of
another shape is no value PostgreSQL stored. A tsquery has no engine form: a statement's query is a
constant, folded here by provisa.core.pg_tsearch, and a search of a vector is written from its
tree. A query that is not a constant is refused by name."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.core import pg_tsearch
from provisa.core import pg_tsearch_parse as parse
from provisa.core.pg_error import END, MARK, PostgresError
from provisa.core.pg_tsearch import Node
from provisa.transpiler import pg_forms_textsearch_document as document
from provisa.transpiler.pg_forms_json import jsonb_text
from provisa.transpiler.pg_literals import values_cast
from provisa.transpiler.pg_forms_types import REFUSED

_D, _T, _C = "duckdb", "trino", "clickhouse"
_OF = {_D: frozenset({_D}), _T: frozenset({_T, "athena", "presto"}), _C: frozenset({_C})}
Forms = dict[frozenset[str], str]
Row = tuple[str, tuple[str, ...], Forms]


@dataclass(frozen=True)
class Engine:
    """An engine's spelling of what a form needs."""

    find_all: str  # text, pattern
    find: str  # text, pattern: its first group
    matches: str  # text, pattern
    replace_all: str  # text, pattern, replacement
    group: str  # the first group, in a replacement
    map: str  # list, name, body
    keep: str  # list, name, condition
    join: str  # list, separator
    split: str  # text, separator
    size: str
    has: str  # list, item
    number: str  # a text of digits
    fails: str  # condition, message: the answer's guard, 0 where the condition is false
    guarded: str  # guard, answer
    guarded_list: str  # guard, a list
    starts: str  # text, prefix
    within: str  # a letter, the text it may be in
    sort: str
    distinct: str
    flatten: str
    concat: str  # two lists
    fold: str  # list of BIGINT, state, item, body, start
    let: str  # name, value, body
    upto: str  # the numbers 1 .. n
    first: str  # a list's first item, NULL for an empty one
    largest: str  # a list of numbers
    bytes: str  # a text's length in bytes
    digits: str  # a number as text
    quotient: str  # whole-number division
    chars: str  # a text's length in characters
    char: str  # text, a character's place: that character
    code: str  # a character's number
    prefix: str  # text, n: its first n characters
    since: str  # text, a character's place: the text from there
    lower: str
    array: str  # items: a list of them
    no_texts: str  # an empty list of text
    head: str  # list, n: its first n items
    nullable_lists: bool = True  # whether a list may be NULL
    backslash: str = "\\"  # a backslash in a string constant

    def text(self, value: str) -> str:
        """A string constant."""
        return "'" + value.replace("\\", self.backslash).replace("'", "''") + "'"


DUCKDB = Engine(
    find_all="regexp_extract_all({}, {})",
    find="regexp_extract({}, {}, 1)",
    matches="regexp_matches({}, {})",
    replace_all="regexp_replace({}, {}, {}, 'g')",
    group="\\1",
    map="list_transform({0}, {1} -> {2})",
    keep="list_filter({0}, {1} -> {2})",
    join="array_to_string({}, {})",
    split="string_split({}, {})",
    size="len({})",
    has="list_contains({}, {})",
    number="CAST({} AS BIGINT)",
    fails="CASE WHEN {} THEN CAST(CAST({} AS INTEGER) AS INTEGER) ELSE 0 END",
    guarded="CASE WHEN {} = 0 THEN {} END",
    guarded_list="CASE WHEN {} = 0 THEN {} END",
    starts="starts_with({}, {})",
    within="strpos({1}, {0}) > 0",
    sort="list_sort({})",
    distinct="list_distinct({})",
    flatten="flatten({})",
    concat="list_concat({}, {})",
    fold="list_reduce({0}, ({1}, {2}) -> {3}, {4})",
    let="list_transform([{1}], {0} -> {2})[1]",
    upto="range(1, {} + 1)",
    first="{}[1]",
    largest="list_max({})",
    bytes="strlen({})",
    digits="CAST({} AS VARCHAR)",
    quotient="({} // {})",
    chars="length({})",
    char="substr({}, {}, 1)",
    code="unicode({})",
    prefix="substr({}, 1, {})",
    since="substr({}, {})",
    lower="lower({})",
    array="[{}]",
    no_texts="CAST([] AS VARCHAR[])",
    head="list_slice({}, 1, {})",
)
TRINO = Engine(
    find_all="regexp_extract_all({}, {})",
    find="coalesce(regexp_extract({}, {}, 1), '')",  # NULL where it finds nothing, unlike the others
    matches="regexp_like({}, {})",
    replace_all="regexp_replace({}, {}, {})",
    group="$1",
    map="transform({0}, {1} -> {2})",
    keep="filter({0}, {1} -> {2})",
    join="array_join({}, {})",
    split="split({}, {})",
    size="cardinality({})",
    has="contains({}, {})",
    number="CAST({} AS BIGINT)",
    fails="CASE WHEN {} THEN CAST(CAST({} AS INTEGER) AS INTEGER) ELSE 0 END",
    guarded="CASE WHEN {} = 0 THEN {} END",
    guarded_list="CASE WHEN {} = 0 THEN {} END",
    starts="starts_with({}, {})",
    within="strpos({1}, {0}) > 0",
    sort="array_sort({})",
    distinct="array_distinct({})",
    flatten="flatten({})",
    concat="concat({}, {})",
    fold="reduce({0}, {4}, ({1}, {2}) -> {3}, _f -> _f)",
    let="transform(ARRAY[{1}], {0} -> {2})[1]",
    # in blocks: Trino's sequence makes no more than 10000 entries
    upto="transform(ARRAY[{}], _un -> flatten(transform(sequence(0, (_un - 1) / 10000), "
    "_ub -> sequence(_ub * 10000 + 1, least(_un, _ub * 10000 + 10000)))))[1]",
    first="element_at({}, 1)",
    largest="array_max({})",
    bytes="length(to_utf8({}))",
    digits="CAST({} AS VARCHAR)",
    quotient="({} / {})",
    chars="length({})",
    char="substr({}, {}, 1)",
    code="codepoint(CAST({} AS VARCHAR(1)))",
    prefix="substr({}, 1, {})",
    since="substr({}, {})",
    lower="lower({})",
    array="ARRAY[{}]",
    no_texts="CAST(ARRAY[] AS ARRAY(VARCHAR))",
    head="slice({}, 1, {})",
)
CLICKHOUSE = Engine(
    find_all="extractAll(ifNull({}, ''), {})",
    find="extract({}, {})",
    matches="match({}, {})",
    replace_all="replaceRegexpAll({}, {}, {})",
    group="\\1",
    map="arrayMap({1} -> {2}, {0})",
    keep="arrayFilter({1} -> {2}, {0})",
    join="arrayStringConcat({}, {})",
    split="splitByString({1}, {0})",
    size="length({})",
    has="has({}, {})",
    number="toInt64({})",
    fails="throwIf({}, {})",
    guarded="if({} = 0, {}, NULL)",
    guarded_list="if({} = 0, {}, [])",
    starts="startsWith({}, {})",
    within="position({1}, {0}) > 0",
    sort="arraySort({})",
    distinct="arrayDistinct({})",
    # one level, as DuckDB's and Trino's flatten: arrayFlatten flattens every level
    flatten="arrayMap(_fl -> arrayFold((_fa, _fe) -> arrayConcat(_fa, _fe), _fl, arrayResize(_fl[1], 0)), [{}])[1]",
    concat="arrayConcat({}, {})",
    fold="arrayFold(({1}, {2}) -> {3}, {0}, {4})",
    let="arrayMap({0} -> {2}, [{1}])[1]",
    upto="range(1, {} + 1)",
    first="{}[1]",
    largest="arrayMax({})",
    bytes="length({})",
    digits="toString({})",
    quotient="intDiv({}, {})",
    chars="lengthUTF8({})",
    char="substringUTF8({}, {}, 1)",
    code="toInt64(ascii({}))",
    prefix="substringUTF8({}, 1, {})",
    since="substringUTF8({}, {})",
    lower="lowerUTF8({})",
    array="[{}]",
    no_texts="CAST([] AS Array(String))",
    head="arraySlice({}, 1, {})",
    nullable_lists=False,
    backslash="\\\\",
)
ENGINES = ((_D, DUCKDB), (_T, TRINO), (_C, CLICKHOUSE))


_ENTRY = r"'(?:[^']|'')*'(?::[0-9]+[A-C]?(?:,[0-9]+[A-C]?)*)?"
#: tsvectorout's form, and nothing else: a text of another shape is no stored value.
_FORM = rf"\A(?:{_ENTRY}(?: {_ENTRY})*)?\z"
_LEXEME = r"\A'((?:[^']|'')*)'"
_MARKS = r":([0-9A-C,]+)\z"


def _tokens(e: Engine, value: str = "v") -> str:
    return e.find_all.format(value, e.text(_ENTRY))


def _lexeme_of(e: Engine, token: str) -> str:
    """A token's lexeme as stored: tsvectorout doubles a quote and a backslash."""
    inner = e.find.format(token, e.text(_LEXEME))
    return f"replace(replace({inner}, '''''', ''''), {e.text(chr(92) * 2)}, {e.text(chr(92))})"


def _marks_of(e: Engine, token: str) -> str:
    return e.find.format(token, e.text(_MARKS))


def _head_of(e: Engine, token: str) -> str:
    return e.find.format(token, e.text(r"\A('(?:[^']|'')*')"))


def _read(e: Engine, function: str, answer: str, *values: str, listed: bool = False) -> str:
    """``answer``, of a NULL for a NULL, once each value is in tsvectorout's form."""
    wrong = " OR ".join(f"NOT {e.matches.format(v, e.text(_FORM))}" for v in values)
    message = e.text(f"{MARK} 0A000: {function} {REFUSED} {END}")
    guard = e.fails.format(f"coalesce({wrong}, false)", message)
    written = (e.guarded_list if listed else e.guarded).format(guard, answer)
    if listed and not e.nullable_lists:
        return written
    nulls = " OR ".join(f"{v} IS NULL" for v in values)
    return f"CASE WHEN {nulls} THEN NULL ELSE {written} END"


def _join_tokens(e: Engine, tokens: str) -> str:
    return e.join.format(tokens, "' '")


def _reweighted(e: Engine, marks: str, weight: str) -> str:
    """Each position of ``marks`` of ``weight``: no letter, for D."""
    letter = "" if weight in "dD" else weight.upper()
    bare = e.replace_all.format(marks, "'[A-C]'", "''")
    return e.replace_all.format(bare, "'([0-9]+)'", e.text(f"{e.group}{letter}"))


def _setweight(e: Engine, weight: str, wanted: str | None = None) -> str:
    marks = _marks_of(e, "_t")
    whom = "true" if wanted is None else e.has.format(wanted, _lexeme_of(e, "_t"))
    written = (
        f"CASE WHEN {marks} <> '' AND {whom} "
        f"THEN {_head_of(e, '_t')} || ':' || {_reweighted(e, marks, weight)} ELSE _t END"
    )
    return _join_tokens(e, e.map.format(_tokens(e), "_t", written))


def _kept_marks(e: Engine, weights: str) -> str:
    """A token's marks of the weights ``weights``, a list of letters."""
    weight = "right(_m, 1)"
    letter = f"CASE WHEN {weight} IN ('A', 'B', 'C') THEN {weight} ELSE 'D' END"
    return e.join.format(
        e.keep.format(
            e.split.format(_marks_of(e, "_t"), "','"), "_m", e.has.format(weights, letter)
        ),
        "','",
    )


def _filtered(e: Engine, weights: str) -> str:
    marks = _kept_marks(e, weights)
    written = e.map.format(_tokens(e), "_t", f"{_head_of(e, '_t')} || ':' || {marks}")
    return _join_tokens(e, e.keep.format(written, "_t", "NOT _t LIKE '%:'"))


def _letter(mark: str) -> str:
    """A mark's weight."""
    last = f"right({mark}, 1)"
    return f"CASE WHEN {last} IN ('A', 'B', 'C') THEN {last} ELSE 'D' END"


def _rank_of(mark: str) -> str:
    last = f"right({mark}, 1)"
    return f"CASE {last} WHEN 'A' THEN 3 WHEN 'B' THEN 2 WHEN 'C' THEN 1 ELSE 0 END"


def _position(e: Engine, mark: str) -> str:
    return e.number.format(e.replace_all.format(mark, "'[A-C]'", "''"))


def _split_marks(e: Engine, marks: str) -> str:
    return e.split.format(marks, "','")


def _count(e: Engine, marks: str) -> str:
    return f"CASE WHEN {marks} = '' THEN 0 ELSE {e.size.format(_split_marks(e, marks))} END"


def _matching(e: Engine) -> str:
    """The tokens of ``v`` of the lexeme ``x``, or of its prefix where ``p``."""
    lexeme = _lexeme_of(e, "_t")
    return e.keep.format(
        _tokens(e), "_t", f"({lexeme} = x OR (p AND {e.starts.format(lexeme, 'x')}))"
    )


def _of_weight(e: Engine, marks: str) -> str:
    """Whether ``marks`` hold a weight of ``w``, every one where ``w`` is empty."""
    held = e.keep.format(_split_marks(e, marks), "_m", e.within.format(_letter("_m"), "w"))
    return f"(w = '' OR {marks} = '' OR {e.size.format(held)} > 0)"


def _term(e: Engine) -> str:
    found = e.keep.format(_matching(e), "_t", _of_weight(e, _marks_of(e, "_t")))
    return f"{e.size.format(found)} > 0"


def _positions(e: Engine) -> str:
    """The positions of the term, each of a weight of ``w``."""
    marked = e.keep.format(_matching(e), "_t", f"{_marks_of(e, '_t')} <> ''")
    weighted = e.keep.format(
        _split_marks(e, _marks_of(e, "_t")),
        "_m",
        f"(w = '' OR {e.within.format(_letter('_m'), 'w')})",
    )
    return e.flatten.format(
        e.map.format(marked, "_t", e.map.format(weighted, "_m", _position(e, "_m")))
    )


def _stripped(e: Engine) -> str:
    """Whether the term is held with no positions: a phrase over it cannot be told."""
    found = e.keep.format(_matching(e), "_t", f"{_marks_of(e, '_t')} = ''")
    return f"{e.size.format(found)} > 0"


def _size_of(e: Engine, value: str) -> str:
    """The value's VARSIZE, less its fixed header: each lexeme's bytes, and its positions after
    an alignment to two bytes."""
    marks, lexeme = _marks_of(e, "_t"), _lexeme_of(e, "_t")
    items = e.map.format(
        _tokens(e, value), "_t", f"{e.bytes.format(lexeme)} * 100000 + {_count(e, marks)}"
    )
    added = f"(_s + {e.quotient.format('_x', '100000')})"
    body = (
        f"CASE WHEN _x % 100000 = 0 THEN {added} "
        f"ELSE {added} + {added} % 2 + 2 + 2 * (_x % 100000) END"
    )
    data = e.fold.format(items, "_s", "_x", body, "CAST(0 AS BIGINT)")
    return f"{data} + 4 * {e.size.format(_tokens(e, value))}"


def _sign(less: str) -> str:
    return f"CASE WHEN {less} THEN -1 ELSE 1 END"


def _marks_compare(e: Engine) -> str:
    left, right = _position(e, "_p[_j]"), _position(e, "_q[_j]")
    size_p, size_q = e.size.format("_p"), e.size.format("_q")
    differs = e.keep.format(e.upto.format(size_p), "_k", "_p[_k] <> _q[_k]")
    heavier = f"{_rank_of('_p[_j]')} > {_rank_of('_q[_j]')}"
    last = (
        f"CASE WHEN {left} <> {right} THEN {_sign(f'{left} > {right}')} ELSE {_sign(heavier)} END"
    )
    return (
        f"CASE WHEN {size_p} <> {size_q} THEN {_sign(f'{size_p} > {size_q}')} "
        f"ELSE {e.let.format('_j', f'{differs}[1]', last)} END"
    )


def _lexeme_difference(e: Engine, a: str, b: str) -> str:
    """tsCompareString of two lexemes that differ: memcmp's answer on the server, the difference of
    the first bytes that differ, or a length's sign where one is the other's start. Past ASCII a
    character's first byte need not be the one that differs, so that is refused by name."""
    shorter = f"least({e.chars.format(a)}, {e.chars.format(b)})"
    at = e.keep.format(
        e.upto.format(shorter), "_k", f"{e.char.format(a, '_k')} <> {e.char.format(b, '_k')}"
    )
    first = (e.code.format(e.char.format(a, "_c")), e.code.format(e.char.format(b, "_c")))
    message = e.text(f"{MARK} 0A000: tsvector_cmp {REFUSED} {END}")
    wide = e.fails.format(f"{first[0]} > 127 OR {first[1]} > 127", message)
    apart = e.guarded.format(wide, f"{first[0]} - {first[1]}")
    return (
        f"CASE WHEN {e.size.format(at)} = 0 THEN {_sign(f'{e.chars.format(a)} < {e.chars.format(b)}')} "
        f"ELSE {e.let.format('_c', f'{at}[1]', apart)} END"
    )


def _entry_compare(e: Engine, raw: bool) -> str:
    """silly_cmp_tsvector, of the two entries ``_a[_i]`` and ``_b[_i]`` that differ: its own answer
    where ``raw``, else its sign."""
    ma, mb = _marks_of(e, "_a[_i]"), _marks_of(e, "_b[_i]")
    la, lb = _lexeme_of(e, "_a[_i]"), _lexeme_of(e, "_b[_i]")
    marks = e.let.format(
        "_p", _split_marks(e, ma), e.let.format("_q", _split_marks(e, mb), _marks_compare(e))
    )
    has_marks = f"{ma} <> ''"
    return (
        f"CASE WHEN ({ma} = '') <> ({mb} = '') THEN {_sign(has_marks)} "
        f"WHEN {la} <> {lb} THEN {_lexeme_difference(e, la, lb) if raw else _sign(f'{la} < {lb}')} "
        f"ELSE {marks} END"
    )


def _compare(e: Engine, raw: bool) -> str:
    size_a, size_b = e.size.format("_a"), e.size.format("_b")
    differs = e.keep.format(e.upto.format(size_a), "_k", "_a[_k] <> _b[_k]")
    entries = e.let.format("_i", f"{differs}[1]", _entry_compare(e, raw))
    body = (
        f"CASE WHEN _sa <> _sb THEN {_sign('_sa < _sb')} "
        f"WHEN {size_a} <> {size_b} THEN {_sign(f'{size_a} < {size_b}')} "
        f"WHEN a = b THEN 0 ELSE {entries} END"
    )
    tokens = e.let.format("_a", _tokens(e, "a"), e.let.format("_b", _tokens(e, "b"), body))
    return e.let.format("_sa", _size_of(e, "a"), e.let.format("_sb", _size_of(e, "b"), tokens))


def _quoted(e: Engine, lexeme: str) -> str:
    """tsvectorout's quoting of a lexeme."""
    backslash = f"replace({lexeme}, {e.text(chr(92))}, {e.text(chr(92) * 2)})"
    return f"'''' || replace({backslash}, '''', '''''') || ''''"


def _marks_in(e: Engine, tokens: str) -> str:
    """The marks of the token of lexeme ``_l``."""
    found = e.keep.format(tokens, "_t", f"{_lexeme_of(e, '_t')} = _l")
    return f"coalesce({e.first.format(e.map.format(found, '_t', _marks_of(e, '_t')))}, '')"


def _concat(e: Engine) -> str:
    """tsvector_concat: b's positions past a's last, each lexeme's in order."""
    digits = e.find_all.format(_marks_of(e, "_t"), "'[0-9]+'")
    held = e.flatten.format(
        e.map.format("_a", "_t", e.map.format(digits, "_d", e.number.format("_d")))
    )
    moved = e.digits.format(f"least({_position(e, '_m')} + _x, 16383)")
    each = f"{moved} || CASE WHEN {_rank_of('_m')} > 0 THEN right(_m, 1) ELSE '' END"
    shifted = e.join.format(e.map.format(_split_marks(e, "_y"), "_m", each), "','")
    merged = (
        "CASE WHEN _u <> '' AND _w <> '' THEN _u || ',' || _w WHEN _u <> '' THEN _u ELSE _w END"
    )
    apart = f"(_k = 1 OR {_position(e, '_z[_k]')} <> {_position(e, '_z[_k - 1]')})"
    once = e.join.format(
        e.map.format(
            e.keep.format(e.upto.format(e.size.format("_z")), "_k", apart), "_k", "_z[_k]"
        ),
        "','",
    )
    written = (
        f"{_quoted(e, '_l')} || CASE WHEN _u = '' AND _w = '' THEN '' ELSE ':' || "
        f"{e.let.format('_z', _split_marks(e, merged), once)} END"
    )
    second = e.let.format("_w", f"CASE WHEN _y = '' THEN '' ELSE {shifted} END", written)
    body = e.let.format("_y", _marks_in(e, "_b"), e.let.format("_u", _marks_in(e, "_a"), second))
    lexemes = e.map.format(e.concat.format("_a", "_b"), "_t", _lexeme_of(e, "_t"))
    joined = e.join.format(
        e.map.format(e.sort.format(e.distinct.format(lexemes)), "_l", body), "' '"
    )
    last = f"CASE WHEN {e.size.format(held)} = 0 THEN 0 ELSE {e.largest.format(held)} END"
    return e.let.format(
        "_a", _tokens(e, "a"), e.let.format("_b", _tokens(e, "b"), e.let.format("_x", last, joined))
    )


def _lexeme_list(e: Engine) -> str:
    """array_to_tsvector of the list ``x``."""
    written = e.map.format(e.sort.format(e.distinct.format("x")), "_l", _quoted(e, "_l"))
    return e.join.format(written, "' '")


#: The configurations and dictionaries a row of a column reads, each its own row.
_CONFIGS = ("english", "simple")
_DICTIONARIES = ("english_stem", "simple")
_DEFAULT_WEIGHTS = ("0.1", "0.2", "0.4", "1.0")
#: Each engine's float4: a form's every step is one, as calc_rank's are.
_REAL = {_D: "CAST({} AS FLOAT)", _T: "CAST({} AS REAL)", _C: "toFloat32({})"}
_DOUBLE = {_D: "CAST({} AS DOUBLE)", _T: "CAST({} AS DOUBLE)", _C: "toFloat64({})"}
_RIEMANN = "1.64493406685"


def _weight_of(mark: str) -> str:
    """The index in ``w`` of a mark's weight, D first as PostgreSQL's array is."""
    return f"({_rank_of(mark)} + 1)"


def _entry_marks(e: Engine) -> str:
    """A token's marks: POSNULL, a single position of weight D, for one with none."""
    return f"{_split_marks(e, f"CASE WHEN {_marks_of(e, '_t')} = '' THEN '0' ELSE {_marks_of(e, '_t')} END")}"


def _rank_item(d: str, e: Engine) -> str:
    """calc_rank_or's addend of one lexeme ``_t``, in float4 as it is computed."""
    real = _REAL[d].format
    weights = e.map.format("_ms", "_m", real(f"w[{_weight_of('_m')}]"))
    squares = e.map.format(
        e.upto.format(e.size.format("_ws")),
        "_j",
        real(f"_ws[_j] / {real('_j * _j')}"),
    )
    total = e.fold.format(squares, "_s", "_x", real("_s + _x"), real("0"))
    heaviest = e.largest.format("_ws")
    where = e.first.format(e.keep.format(e.upto.format(e.size.format("_ws")), "_j", "_ws[_j] = _h"))
    item = real(f"{real('_h + _r')} - {real(f'_h / {real("_i * _i")}')}")
    body = e.let.format("_i", where, item)
    return e.let.format(
        "_ms",
        _entry_marks(e),
        e.let.format(
            "_ws",
            weights,
            e.let.format("_r", total, e.let.format("_h", heaviest, body)),
        ),
    )


def _rank_or(d: str, e: Engine) -> str:
    """calc_rank_or over the query's terms ``x``, sorted and unique, ``p`` their prefix flags."""
    real, double = _REAL[d].format, _DOUBLE[d].format
    lexeme = _lexeme_of(e, "_t")
    of_term = e.keep.format(
        _tokens(e),
        "_t",
        f"({lexeme} = x[_n] OR (p[_n] AND {e.starts.format(lexeme, 'x[_n]')}))",
    )
    items = e.flatten.format(
        e.map.format(
            e.upto.format(e.size.format("x")), "_n", e.map.format(of_term, "_t", _rank_item(d, e))
        )
    )
    added = real(f"{double('_s')} + {double('_x')} / {_RIEMANN}")
    total = e.fold.format(items, "_s", "_x", added, real("0"))
    return real(f"{total} / {real(e.size.format('x'))}")


def _rank_and(d: str, e: Engine) -> str:
    """calc_rank_and over the terms ``x``: each two positions of two terms, in pg's loop order."""
    real, double = _REAL[d].format, _DOUBLE[d].format

    def marks_of(which: str) -> str:
        found = e.keep.format(_tokens(e), "_t", f"{_lexeme_of(e, '_t')} = x[{which}]")
        return e.flatten.format(e.map.format(found, "_t", _entry_marks(e)))

    held = f"abs({_position(e, '_l')} - {_position(e, '_q')})"
    dist = f"CASE WHEN {held} = 0 THEN 16383 ELSE {held} END"
    near = real(f"1.0 / (1.005 + 0.05 * exp({double(real('_d'))} / 1.5 - 2))")
    apart = real(f"CASE WHEN _d > 100 THEN 1e-30 ELSE {near} END")
    first, second = real(f"w[{_weight_of('_l')}]"), real(f"w[{_weight_of('_q')}]")
    weigh = real(f"{real(f'{first} * {second}')} * {apart}")
    each = e.let.format(
        "_d",
        dist,
        f"CASE WHEN {held} <> 0 OR _l = '0' OR _q = '0' THEN {real(f'sqrt({double(weigh)})')} "
        f"ELSE {real('-1')} END",
    )
    pairs = e.flatten.format(e.map.format("_ls", "_l", e.map.format("_qs", "_q", each)))
    below = e.map.format(
        e.upto.format("_n - 1"),
        "_o",
        e.let.format("_qs", marks_of("_o"), pairs),
    )
    terms = e.map.format(
        e.keep.format(e.upto.format(e.size.format("x")), "_n", "_n > 1"),
        "_n",
        e.let.format("_ls", marks_of("_n"), e.flatten.format(below)),
    )
    items = e.keep.format(e.flatten.format(terms), "_c", "_c >= 0")
    step = real(
        f"CASE WHEN _s < 0 THEN {double('_x')} ELSE 1.0 - (1.0 - {double('_s')}) * (1.0 - {double('_x')}) END"
    )
    total = e.fold.format(items, "_s", "_x", step, real("-1"))
    ranked = e.let.format("_e", total, f"CASE WHEN _e < 0 THEN {real('1e-20')} ELSE _e END")
    # calc_rank's 0 for a vector of no lexemes, before calc_rank_and's floor
    return f"CASE WHEN {e.size.format(_tokens(e))} = 0 THEN {real('0')} ELSE {ranked} END"


def _occurrences(e: Engine) -> str:
    """Every position of the terms ``x``, of the weights ``y``, as its mark."""
    lexeme = _lexeme_of(e, "_t")
    of_term = e.keep.format(
        _tokens(e),
        "_t",
        f"({lexeme} = x[_n] OR (p[_n] AND {e.starts.format(lexeme, 'x[_n]')})) "
        f"AND {_marks_of(e, '_t')} <> ''",
    )
    marks = e.keep.format(
        _split_marks(e, _marks_of(e, "_t")),
        "_m",
        f"(y[_n] = '' OR {e.within.format(_letter('_m'), 'y[_n]')})",
    )
    return e.flatten.format(
        e.map.format(
            e.upto.format(e.size.format("x")),
            "_n",
            e.flatten.format(e.map.format(of_term, "_t", marks)),
        )
    )


def _rank_cd_or(d: str, e: Engine) -> str:
    """calc_rank_cd where a cover is one word: the sum of each occurrence's weight."""
    real, double = _REAL[d].format, _DOUBLE[d].format
    items = e.map.format(_occurrences(e), "_m", double(real(f"w[{_weight_of('_m')}]")))
    return e.fold.format(items, "_s", "_x", "_s + _x", double("0"))


def _word_count(e: Engine) -> str:
    """cnt_length: each lexeme's positions, one for a lexeme with none."""
    return e.fold.format(
        e.map.format(
            _tokens(e),
            "_t",
            f"CASE WHEN {_marks_of(e, '_t')} = '' THEN 1 ELSE {_count(e, _marks_of(e, '_t'))} END",
        ),
        "_s",
        "_x",
        "_s + _x",
        "0",
    )


#: The name ts_bound gives a vector its search reads.
_BOUND = "_sv"


def _rows() -> tuple[Row, ...]:
    forms: dict[tuple[str, tuple[str, ...]], Forms] = {}

    def add(d: str, name: str, params: str, sql: str) -> None:
        forms.setdefault((name, tuple(params.split(", "))), {})[_OF[d]] = sql

    for d, e in ENGINES:
        lexemes = e.map.format(_tokens(e), "_t", _lexeme_of(e, "_t"))
        add(d, "tsvector_to_array", "v", _read(e, "tsvector_to_array", lexemes, "v", listed=True))
        kept = e.keep.format(_tokens(e), "_t", f"{_lexeme_of(e, '_t')} <> x")
        add(d, "ts_delete", "v, x", _read(e, "ts_delete", _join_tokens(e, kept), "v"))
        # ts_delete of a list of lexemes (ts_delete_array) is the polyfill grid's row.
        add(d, "tsvector_eq", "a, b", _read(e, "tsvector_eq", "a = b", "a", "b"))
        add(d, "tsvector_ne", "a, b", _read(e, "tsvector_ne", "a <> b", "a", "b"))
        for weight in "ABCD":
            one = _read(e, "setweight", _setweight(e, weight), "v")
            add(d, f"setweight_{weight.lower()}", "v", one)
            some = _read(e, "setweight", _setweight(e, weight, "x"), "v")
            add(d, f"setweight_some_{weight.lower()}", "v, x", some)
        add(d, "ts_filter_weights", "v, w", _read(e, "ts_filter", _filtered(e, "w"), "v"))
        add(d, "tsvector_cmp", "a, b", _read(e, "tsvector_cmp", _compare(e, True), "a", "b"))
        add(d, "tsvector_order", "a, b", _read(e, "tsvector_cmp", _compare(e, False), "a", "b"))
        add(d, "tsvector_concat", "a, b", _read(e, "tsvector_concat", _concat(e), "a", "b"))
        add(d, "ts_term", "v, x, p, w", _read(e, "ts_match_vq", _term(e), "v"))
        add(
            d,
            "ts_positions",
            "v, x, p, w",
            _read(e, "ts_match_vq", _positions(e), "v", listed=True),
        )
        add(d, "ts_stripped", "v, x, p, w", _read(e, "ts_match_vq", _stripped(e), "v"))
        add(d, "ts_phrase", "l, r, n", e.keep.format("r", "_q", e.has.format("l", "_q - n")))
        add(d, "ts_found", "l", f"{e.size.format('l')} > 0")
        add(d, "ts_bound", "v, b", e.let.format(_BOUND, "v", "b"))
        add(d, "ts_lexemes", "x", _lexeme_list(e))
        add(d, "ts_rank_or", "v, w, x, p", _read(e, "ts_rank", _rank_or(d, e), "v"))
        add(d, "ts_rank_and", "v, w, x", _read(e, "ts_rank", _rank_and(d, e), "v"))
        add(d, "ts_rank_cd_or", "v, w, x, p, y", _read(e, "ts_rank_cd", _rank_cd_or(d, e), "v"))
        real, double = _REAL[d].format, _DOUBLE[d].format
        count = _word_count(e)
        lexemes_held = e.size.format(_tokens(e))
        ln = lambda n: f"ln({n})"  # noqa: E731
        by = lambda divisor: real(f"{double('r')} / {double(divisor)}")  # noqa: E731
        for suffix, log_two in (("rank", f" / {ln('2.0')}"), ("cd", "")):
            add(
                d,
                f"ts_norm_{suffix}_1",
                "r, v",
                f"CASE WHEN {lexemes_held} > 0 THEN {by(f'{ln(double(f"{count} + 1"))}{log_two}')} "
                "ELSE r END",
            )
            add(
                d,
                f"ts_norm_{suffix}_2",
                "r, v",
                f"CASE WHEN {count} > 0 THEN {by(count)} ELSE r END",
            )
            add(
                d,
                f"ts_norm_{suffix}_8",
                "r, v",
                f"CASE WHEN {lexemes_held} > 0 THEN {by(lexemes_held)} ELSE r END",
            )
            add(
                d,
                f"ts_norm_{suffix}_16",
                "r, v",
                f"CASE WHEN {lexemes_held} > 0 "
                f"THEN {by(f'{ln(double(f"{lexemes_held} + 1"))}{log_two}')} ELSE r END",
            )
            add(d, f"ts_norm_{suffix}_32", "r, v", real(f"{double('r')} / ({double('r')} + 1)"))
        for config in _CONFIGS:
            add(d, f"ts_document_{config}", "s", document.document(e, config))
            add(d, f"ts_plain_{config}", "s", document.plain_query(e, config))
            add(d, f"ts_phrase_{config}", "s", document.phrase_query(e, config))
        for dictionary in _DICTIONARIES:
            add(d, f"ts_lexize_{dictionary}", "s", document.lexized(e, dictionary))
    return tuple((name, params, each) for (name, params), each in forms.items())


#: A row: name, parameters, each dialect's form.
ROWS: tuple[Row, ...] = _rows()


class NoForm(ValueError):
    """A text search call no form writes: ``called``, the name it is refused by."""

    def __init__(self, called: str) -> None:
        self.called = called
        super().__init__(called)


class _NotConstant(Exception):
    """An argument this module cannot read in the statement."""


#: The functions :func:`lowered` reads, for function_macros.mentioned.
_FOLDED = frozenset(
    """
    numnode querytree tsquery_and tsquery_or tsquery_not tsquery_phrase tsq_mcontains
    tsq_mcontained tsquery_cmp tsquery_eq tsquery_ne tsquery_lt tsquery_le tsquery_gt tsquery_ge
    get_current_ts_config
    """.split()
)
_ORDERED = {"tsvector_lt": "<", "tsvector_le": "<=", "tsvector_gt": ">", "tsvector_ge": ">="}
_SEARCHES = frozenset({"ts_match_vq", "ts_match_qv"})
NAMES = (
    _FOLDED
    | _SEARCHES
    | frozenset(_ORDERED)
    | {
        "setweight", "ts_delete", "ts_filter", "tsvector_to_array", "tsvector_eq", "tsvector_ne",
        "tsvector_cmp", "tsvector_concat", "array_to_tsvector", "ts_rank", "ts_rank_cd",
        "to_tsvector", "json_to_tsvector", "jsonb_to_tsvector", "plainto_tsquery",
        "phraseto_tsquery", "to_tsquery", "websearch_to_tsquery", "ts_match_tt", "ts_match_tq",
        "ts_lexize", "ts_parse", "ts_debug", "ts_stat", "ts_rewrite", "ts_headline",
    }
)  # fmt: skip
_ORDER: dict[str, Callable[[int], bool]] = {
    "tsquery_eq": lambda c: c == 0,
    "tsquery_ne": lambda c: c != 0,
    "tsquery_lt": lambda c: c < 0,
    "tsquery_le": lambda c: c <= 0,
    "tsquery_gt": lambda c: c > 0,
    "tsquery_ge": lambda c: c >= 0,
}
_WEIGHT_NAMES = frozenset("aAbBcCdD")


def _call(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    target = node
    if (
        isinstance(node, exp.Dot)
        and isinstance(node.this, exp.Identifier)
        and node.this.name.lower() == "pg_catalog"
    ):
        target = node.expression
    if isinstance(target, exp.Anonymous):
        return target.name.lower(), list(target.expressions)
    return None


def _constant(value: exp.Expr) -> str | None:
    """A literal's text, None for NULL; a value only the engine can read raises."""
    node = value.unnest()
    if isinstance(node, exp.Cast):
        node = node.this.unnest()
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Literal):
        return node.this
    raise _NotConstant


def _is_constant(value: exp.Expr) -> bool:
    """Whether ``value`` is a literal, read in the statement."""
    try:
        _constant(value)
    except _NotConstant:
        return False
    return True


def _is_null(value: exp.Expr) -> bool:
    node = value.unnest()
    return isinstance(node.this if isinstance(node, exp.Cast) else node, exp.Null)


#: The functions of a query alone, by a name, as what they answer of constants.
_JOINS = {"tsquery_and": "&", "tsquery_or": "|"}


def _query(value: exp.Expr) -> Node | None:
    """A query argument, read in the statement."""
    called = _call(value.unnest())
    if called is not None:
        name, args = called
        if name in _JOINS:
            return pg_tsearch.joined(_JOINS[name], _query(args[0]), _query(args[1]))
        if name == "tsquery_not":
            return pg_tsearch.negated(_query(args[0]))
        if name == "tsquery_phrase":
            return _phrase(args)
        if name in _BUILDERS:
            return _built_query(name, args)
        raise _NotConstant
    text = _constant(value)
    if text is None:
        raise _NotConstant
    return pg_tsearch.tsquery_in(text)


def _phrase(args: list[exp.Expr]) -> Node | None:
    left, right = _query(args[0]), _query(args[1])
    distance = 1
    if len(args) == 3:
        written = _constant(args[2])
        distance = int(written.strip("()")) if written is not None else 1
        if not 0 <= distance <= 16384:
            raise PostgresError(
                "22023",
                "distance in phrase operator must be an integer value between zero and 16384 "
                "inclusive",
            )
    return pg_tsearch.joined("<->", left, right, distance)


def _text_value(value: str) -> exp.Expr:
    return exp.Literal.string(value)


def _boolean(value: bool) -> exp.Expr:
    return exp.Boolean(this=value)


def _folded(name: str, args: list[exp.Expr]) -> exp.Expr:
    """What a function of constants answers."""
    if name == "get_current_ts_config":
        return _text_value("english")
    if any(_is_null(a) for a in args):
        return exp.null()
    if name == "numnode":
        return exp.Literal.number(pg_tsearch.nodes(_query(args[0])))
    if name == "querytree":
        return _text_value(pg_tsearch.querytree(_query(args[0])))
    if name in _JOINS or name in ("tsquery_not", "tsquery_phrase"):
        built = _query(exp.Anonymous(this=name, expressions=args))
        return _text_value(pg_tsearch.tsquery_text(built))
    if name == "tsq_mcontains":
        return _boolean(pg_tsearch.contains(_query(args[0]), _query(args[1])))
    if name == "tsq_mcontained":
        return _boolean(pg_tsearch.contains(_query(args[1]), _query(args[0])))
    order = pg_tsearch.compare(_query(args[0]), _query(args[1]))
    return exp.Literal.number(order) if name == "tsquery_cmp" else _boolean(_ORDER[name](order))


def _vector(value: exp.Expr) -> exp.Expr:
    """A vector argument: a literal in tsvectorout's form, anything else as it is."""
    node = value.unnest()
    inner = node.this if isinstance(node, exp.Cast) else node
    if isinstance(inner, exp.Literal) and inner.is_string:
        return _text_value(pg_tsearch.tsvector_in(inner.this))
    return value


def _items(text: str, name: str) -> list[str | None]:
    """A one-dimensional array literal's items."""
    text = text.strip()
    if not (text.startswith("{") and text.endswith("}")) or "{" in text[1:] or "\\" in text:
        raise NoForm(name)
    body = text[1:-1]
    if not body.strip():
        return []
    found = re.findall(r'\s*(?:"([^"]*)"|([^,"]*?))\s*(?:,|$)', body)
    if found[-1] == ("", ""):
        found.pop()
    return [
        quoted if not bare else (None if bare.upper() == "NULL" else bare) for quoted, bare in found
    ]


def _array(value: exp.Expr, name: str) -> exp.Expr:
    """A text[] literal as a list; a list's reading is the engine's own."""
    node = value.unnest()
    inner = node.this if isinstance(node, exp.Cast) else node
    if not (isinstance(inner, exp.Literal) and inner.is_string):
        return value
    return exp.Array(
        expressions=[
            exp.null() if item is None else _text_value(item) for item in _items(inner.this, name)
        ]
    )


def _no_nulls(array: exp.Expr) -> exp.Expr:
    """A literal list, whose NULL is PostgreSQL's error."""
    if isinstance(array, exp.Array) and any(isinstance(i, exp.Null) for i in array.expressions):
        raise PostgresError("22004", "lexeme array may not contain nulls")
    return array


def _without_nulls(array: exp.Expr) -> exp.Expr:
    """A literal list of the items ts_delete reads."""
    if isinstance(array, exp.Array):
        return exp.Array(expressions=[i for i in array.expressions if not isinstance(i, exp.Null)])
    return array


def _lexemes(value: exp.Expr, name: str) -> exp.Expr:
    """array_to_tsvector: a literal's answer, else a row's call."""
    array = _no_nulls(_array(value, name))
    if not isinstance(array, exp.Array):
        return exp.Anonymous(this="ts_lexemes", expressions=[array])
    items = [i.this for i in array.expressions]
    if "" in items:
        raise PostgresError("2200F", "lexeme array may not contain empty strings")
    return _text_value(pg_tsearch.tsvector_text({item: [] for item in items}))


def _leaf(call: str, vector: exp.Expr, node: Node) -> exp.Expr:
    args = [
        vector.copy(),
        _text_value(node.lexeme),
        _boolean(node.prefix),
        _text_value(node.weight),
    ]
    return exp.Anonymous(this=call, expressions=args)


def _phrase_positions(vector: exp.Expr, node: Node, name: str) -> exp.Expr:
    if node.op == "<->":
        left, right = (_phrase_positions(vector, c, name) for c in node.children)
        step = exp.Literal.number(node.distance)
        return exp.Anonymous(this="ts_phrase", expressions=[left, right, step])
    if node.op:  # what a phrase holds besides words has no positions a form can write
        raise NoForm(name)
    return _leaf("ts_positions", vector, node)


def _search(vector: exp.Expr, node: Node, name: str) -> exp.Expr:
    """``node`` searched for in ``vector``: TS_execute, written from the tree."""
    if node.op == "<->":
        held = exp.Anonymous(this="ts_found", expressions=[_phrase_positions(vector, node, name)])
        leaves = pg_tsearch.operands(node)
        present = [_leaf("ts_term", vector, leaf) for leaf in leaves]
        stripped = [_leaf("ts_stripped", vector, leaf) for leaf in leaves]
        return exp.paren(exp.or_(held, exp.and_(*present, exp.or_(*stripped))))
    if node.op == "!":
        return exp.Not(this=exp.paren(_search(vector, node.children[0], name)))
    if node.op:
        left, right = (_search(vector, c, name) for c in node.children)
        return exp.paren((exp.and_ if node.op == "&" else exp.or_)(left, right))
    return _leaf("ts_term", vector, node)


def _matched(name: str, args: list[exp.Expr]) -> exp.Expr:
    if any(_is_null(a) for a in args):
        return exp.null()
    first, second = args if name == "ts_match_vq" else reversed(args)
    vector, query = _vector(first), _query(second)
    return _boolean(False) if query is None else _bound_search(vector, query, name)


def _bound_search(vector: exp.Expr, query: Node, name: str) -> exp.Expr:
    """:func:`_search`, a vector of a column read once: each term's form holds it, more than
    once."""
    if vector.find(exp.Column) is None:
        return _search(vector, query, name)
    searched = _search(exp.column(_BOUND), query, name)
    return exp.Anonymous(this="ts_bound", expressions=[vector, searched])


_RANKS = frozenset({"ts_rank", "ts_rank_cd"})
_DOCUMENTS = frozenset({"to_tsvector", "json_to_tsvector", "jsonb_to_tsvector"})
_BUILDERS = {
    "plainto_tsquery": parse.plainto_tsquery,
    "phraseto_tsquery": parse.phraseto_tsquery,
    "to_tsquery": parse.to_tsquery,
    "websearch_to_tsquery": parse.websearch_to_tsquery,
}
_SETS = frozenset({"ts_parse", "ts_debug", "ts_stat"})
#: What each function returns, as the engine holds it, where that is not text (a tsvector, a
#: tsquery and text are held as text): a NULL argument answers a NULL of it (REQ-2167).
_RESULTS = {
    **dict.fromkeys(_RANKS, "float"),  # real
    **dict.fromkeys(("ts_lexize", "tsvector_to_array"), "ARRAY<TEXT>"),
    **dict.fromkeys(("numnode", "tsquery_cmp", "tsvector_cmp"), "int"),
    **dict.fromkeys(
        (
            *_SEARCHES, *_ORDERED, *_ORDER, "tsq_mcontains", "tsq_mcontained", "tsvector_eq",
            "tsvector_ne", "ts_match_tt", "ts_match_tq",
        ),
        "boolean",
    ),
}  # fmt: skip
_PARSER = 3722
_RECORD_SPECIAL = re.compile(r'[\s,()"\\]')
_COLUMNS = {
    "ts_parse": ("tokid", "token"),
    "ts_debug": ("alias", "description", "token", "dictionaries", "dictionary", "lexemes"),
    "ts_stat": ("word", "ndoc", "nentry"),
}


def _config(value: exp.Expr, name: str) -> str:
    config = _constant(value)
    if config is None or config.removeprefix("pg_catalog.") not in ("english", "simple"):
        raise NoForm(name)
    return config.removeprefix("pg_catalog.")


def _split_config(args: list[exp.Expr], arity: int, name: str) -> tuple[str, list[exp.Expr]]:
    """The configuration a call names, the session's where it names none."""
    if len(args) > arity:
        return _config(args[0], name), args[1:]
    return "english", args


def _said(value: exp.Expr) -> str | None:
    node = value.unnest()
    return node.to.sql("postgres").lower() if isinstance(node, exp.Cast) else None


def _text_of(value: exp.Expr, name: str) -> str:
    text = _constant(value)
    if text is None:
        raise NoForm(name)
    return text


def _parsed(reading: Callable[[], str], name: str) -> str:
    try:
        return reading()
    except parse.Unread:
        raise NoForm(name) from None


#: What a statement says is text: the overload of a call of no other type.
_TEXTS = frozenset({"text", "varchar", "character varying", "char", "character", "bpchar", "name"})


_SAID = "pg_said_cast"


def _column_cast(node: exp.Expr) -> exp.Expr:
    """A column as the cast of its VALUES list, as :func:`lowered` marked it while it was in the
    statement: a form's copy of it is not."""
    if isinstance(node, exp.Column):
        return node.meta.get(_SAID) or node
    return node


def _told_text(value: exp.Expr) -> bool:
    """Whether the statement says ``value`` is text: its cast, its VALUES list's, or a ||."""
    node = _column_cast(value.unnest())
    if isinstance(node, (exp.DPipe, exp.Concat, exp.Lower, exp.Upper)):
        return True
    said = node.to.sql("postgres").lower().split("(")[0] if isinstance(node, exp.Cast) else None
    return said in _TEXTS


def _row_of(prefix: str, config: str, value: exp.Expr, name: str) -> exp.Expr:
    """A row's reading of a text that is not a constant: refused where the statement does not
    say it is text, as a json document's reading differs."""
    if not _told_text(value):
        raise NoForm(name)
    return exp.Anonymous(this=f"{prefix}_{config}", expressions=[value])


def _document(name: str, args: list[exp.Expr]) -> exp.Expr:
    arity = 1 if name == "to_tsvector" else 2
    config, rest = _split_config(args, arity, name)
    kind = _said(rest[0])
    if name == "to_tsvector" and kind not in ("json", "jsonb") and not _is_constant(rest[0]):
        return _row_of("ts_document", config, rest[0], name)
    if name == "to_tsvector" and kind not in ("json", "jsonb"):
        return _text_value(
            _parsed(
                lambda: pg_tsearch.tsvector_text(
                    parse.to_tsvector(config, _text_of(rest[0], name))
                ),
                name,
            )
        )
    filters = '"string"' if len(rest) == 1 else _text_of(rest[1], name)
    wanted = parse.json_filter(filters)
    document = _text_of(rest[0], name)
    return _text_value(
        _parsed(
            lambda: pg_tsearch.tsvector_text(
                parse.json_to_tsvector(config, document, kind == "jsonb", wanted)
            ),
            name,
        )
    )


#: The builders a row of a column reads, by the prefix of their rows.
_BUILT_ROWS = {"plainto_tsquery": "ts_plain", "phraseto_tsquery": "ts_phrase"}


def _built_query(name: str, args: list[exp.Expr]) -> Node | None:
    """A builder's query of constants."""
    config, rest = _split_config(args, 1, name)
    text = _text_of(rest[0], name)
    try:
        return _BUILDERS[name](config, text)
    except parse.Unread:
        raise NoForm(name) from None


def _built(name: str, args: list[exp.Expr]) -> exp.Expr:
    config, rest = _split_config(args, 1, name)
    if name in _BUILT_ROWS and not _is_constant(rest[0]):
        return _row_of(_BUILT_ROWS[name], config, rest[0], name)
    return _text_value(pg_tsearch.tsquery_text(_built_query(name, args)))


def _searched_text(name: str, args: list[exp.Expr]) -> exp.Expr:
    """ts_match_tt and ts_match_tq: the document's vector, the query's search."""
    if _is_constant(args[0]):
        vector: exp.Expr = _text_value(
            _parsed(
                lambda: pg_tsearch.tsvector_text(
                    parse.to_tsvector("english", _text_of(args[0], name))
                ),
                name,
            )
        )
    else:
        vector = _row_of("ts_document", "english", args[0], name)
    if name == "ts_match_tt":
        try:
            query = parse.plainto_tsquery("english", _text_of(args[1], name))
        except parse.Unread:
            raise NoForm(name) from None
    else:
        query = _query(args[1])
    return _boolean(False) if query is None else _bound_search(vector, query, name)


def _lexized(name: str, args: list[exp.Expr]) -> exp.Expr:
    dictionary = _text_of(args[0], name)
    if dictionary not in _DICTIONARIES:
        raise PostgresError("42704", f'text search dictionary "{dictionary}" does not exist')
    if not _is_constant(args[1]):
        return _row_of("ts_lexize", dictionary, args[1], name)
    found = parse.lexize(dictionary, _text_of(args[1], name))
    return exp.Array(expressions=[_text_value(x) for x in found])


def _rank_terms(query: Node | None) -> list[Node]:
    """SortAndUniqItems: the operands, a NOT's among them, each lexeme once in byte order."""
    terms: dict[bytes, Node] = {}
    for operand in pg_tsearch.operands(query):
        terms.setdefault(operand.lexeme.encode(), operand)
    return [terms[k] for k in sorted(terms)]


def _ranked_weights(value: exp.Expr | None) -> exp.Expr:
    texts: list[str | None] = list(_DEFAULT_WEIGHTS)
    if value is not None:
        given = _items(_text_of(value, "ts_rank"), "ts_rank")
        if not given:
            raise PostgresError("2202E", "array of weight must be one-dimensional")
        if len(given) < 4:
            raise PostgresError("2202E", "array of weight is too short")
        if any(item is None for item in given):
            raise PostgresError("22004", "array of weight must not contain nulls")
        numbers = [float(item) for item in given if item is not None]
        if any(number > 1 for number in numbers):
            raise PostgresError("22023", "weight out of range")
        texts = [given[at] if numbers[at] >= 0 else _DEFAULT_WEIGHTS[at] for at in range(4)]
    return exp.Array(expressions=[exp.Literal.number(t) for t in texts])


def _rank(name: str, args: list[exp.Expr]) -> exp.Expr:
    weighted = bool(_said(args[0]) and _said(args[0]).startswith(("real", "float")))  # type: ignore[union-attr]
    if any(_is_null(a) for a in args):
        return exp.null()
    weights = _ranked_weights(args[0] if weighted else None)
    rest = args[1:] if weighted else args
    held, query = _vector(rest[0]), _query(rest[1])
    # a vector of a column read once, as _bound_search reads it: each row of a rank names it again
    vector = exp.column(_BOUND) if held.find(exp.Column) is not None else held
    method = int(_text_of(rest[2], name).strip("()")) if len(rest) > 2 else 0
    terms = _rank_terms(query)
    if not terms:
        return exp.Cast(this=exp.Literal.number(0), to=exp.DataType.build("float"))
    lexemes = exp.Array(expressions=[_text_value(t.lexeme) for t in terms])
    prefixes = exp.Array(expressions=[_boolean(t.prefix) for t in terms])
    assert query is not None
    if name == "ts_rank":
        if query.op in ("&", "<->") and len(terms) > 1:
            if any(t.prefix for t in terms):
                raise NoForm(name)
            call = "ts_rank_and"
            row = [vector, weights, lexemes]
        else:
            call = "ts_rank_or"
            row = [vector, weights, lexemes, prefixes]
    else:
        if any(n.op not in ("|", "") for n in _walk(query)) or method & 4:
            raise NoForm(name)
        call = "ts_rank_cd_or"
        marks = exp.Array(expressions=[_text_value(t.weight) for t in terms])
        row = [vector, weights, lexemes, prefixes, marks]
    written: exp.Expr = exp.Anonymous(this=call, expressions=row)
    for bit in (1, 2, 8, 16, 32):
        if method & bit:
            suffix = "cd" if name == "ts_rank_cd" else "rank"
            written = exp.Anonymous(
                this=f"ts_norm_{suffix}_{bit}", expressions=[written, vector.copy()]
            )
    if vector is not held:
        written = exp.Anonymous(this="ts_bound", expressions=[held, written])
    return exp.Cast(this=written, to=exp.DataType.build("float"))


def _walk(node: Node) -> list[Node]:
    return [node, *(n for c in node.children for n in _walk(c))]


def _record(values: tuple[str | None, ...]) -> str:
    """record_out."""

    def field(value: str | None) -> str:
        if value is None:
            return ""
        if value == "" or _RECORD_SPECIAL.search(value):
            return '"' + value.replace("\\", "\\\\").replace('"', '""') + '"'
        return value

    return "(" + ",".join(field(v) for v in values) + ")"


def _array_text(items: list[str]) -> str:
    def item(value: str) -> str:
        if value == "" or re.search(r'[\s,{}"\\]', value):
            return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
        return value

    return "{" + ",".join(item(i) for i in items) + "}"


def _parser_rows(args: list[exp.Expr]) -> list[tuple[str | None, ...]]:
    parser = _text_of(args[0], "ts_parse")
    if parser.isdigit():
        if int(parser) != _PARSER:
            raise NoForm("ts_parse")
    elif not re.fullmatch(r"(?:pg_catalog\.)?[a-z_][a-z0-9_]*", parser.lower()):
        raise PostgresError("42602", "invalid name syntax")
    elif parser.removeprefix("pg_catalog.") != "default":
        raise PostgresError("42704", f'text search parser "{parser}" does not exist')
    text = _text_of(args[1], "ts_parse")
    try:
        return [(str(t.kind), t.text) for t in parse.tokens(text)]
    except parse.Unread:
        raise NoForm("ts_parse") from None


def _debug_rows(args: list[exp.Expr]) -> list[tuple[str | None, ...]]:
    config, rest = _split_config(args, 1, "ts_debug")
    rows: list[tuple[str | None, ...]] = []
    try:
        for token in parse.tokens(_text_of(rest[0], "ts_debug")):
            alias, description = parse.TYPES[token.kind]
            found = parse.lexemes(config, token)
            if found is None:
                rows.append((alias, description, token.text, "{}", None, None))
                continue
            dictionary = parse.dictionary_of(config, token)
            lexemes = _array_text(found)
            rows.append((alias, description, token.text, f"{{{dictionary}}}", dictionary, lexemes))
    except parse.Unread:
        raise NoForm("ts_debug") from None
    return rows


def _stat_rows(args: list[exp.Expr]) -> list[tuple[str | None, ...]]:
    """ts_stat of a statement that is one constant vector: anything else runs on no engine here."""
    found = re.fullmatch(
        r"\s*SELECT\s+'((?:[^']|'')*)'::tsvector\s*", _text_of(args[0], "ts_stat"), re.I
    )
    if found is None:
        raise NoForm("ts_stat")
    entries = pg_tsearch.tsvector_entries(found.group(1).replace("''", "'"))
    wanted = (_text_of(args[1], "ts_stat").upper() or None) if len(args) == 2 else None
    rows: list[tuple[str | None, ...]] = []
    for lexeme, marks in reversed(entries.items()):
        held = [m for m in marks if wanted is None or "DCBA"[m[1]] in wanted]
        if marks and not held:
            continue
        if not marks and wanted is not None:
            continue
        rows.append((lexeme, "1", str(len(held) or 1)))
    return rows


def _rows_of(name: str, args: list[exp.Expr]) -> list[tuple[str | None, ...]]:
    if any(_is_null(a) for a in args):
        return []
    return {"ts_parse": _parser_rows, "ts_debug": _debug_rows, "ts_stat": _stat_rows}[name](args)


def _row_select(columns: tuple[str, ...], row: tuple[str | None, ...]) -> str:
    def literal(value: str | None) -> str:
        return "CAST(NULL AS VARCHAR)" if value is None else exp.Literal.string(value).sql()

    return "SELECT " + ", ".join(f"{literal(v)} AS {c}" for c, v in zip(columns, row))


def _rows_query(columns: tuple[str, ...], rows: Sequence[tuple[str | None, ...]]) -> exp.Query:
    """Rows of constants, as every engine reads them."""
    if not rows:
        empty = _row_select(columns, tuple(None for _ in columns))
        written = f"SELECT * FROM ({empty}) AS _r WHERE 1 = 0"
    else:
        written = " UNION ALL ".join(_row_select(columns, row) for row in rows)
    query = sqlglot.parse_one(written)
    assert isinstance(query, exp.Query)
    return query


def _in_select(select: exp.Select, name: str, args: list[exp.Expr], alias: str) -> None:
    """A set-returning call that is a statement's only column: a record's text, row by row."""
    rows = [(_record(row),) for row in _rows_of(name, args)]
    inner = _rows_query((alias,), rows)
    select.set("expressions", [exp.column(alias)])
    select.set("from_", exp.From(this=inner.subquery("_rows")))


def _sets(tree: exp.Expr) -> exp.Expr:
    """Each set-returning call, in a FROM as rows of columns, as a query's only column as records."""
    for table in list(tree.find_all(exp.Table)):
        called = _call(table.this)
        if called is not None and called[0] in _SETS:
            rows = _rows_of(called[0], called[1])
            alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(called[0]))
            sub = _rows_query(_COLUMNS[called[0]], rows).subquery("_r")
            sub.set("alias", alias.copy())
            table.replace(sub)
    for select in list(tree.find_all(exp.Select)):
        projected = select.expressions
        if len(projected) != 1 or select.args.get("from_"):
            continue
        only = projected[0]
        target = only.this if isinstance(only, exp.Alias) else only
        called = _call(target)
        if called is not None and called[0] in _SETS:
            _in_select(select, called[0], called[1], only.alias or called[0])
    return tree


_PAIR = re.compile(
    r"\s*SELECT\s+'((?:[^']|'')*)'::tsquery\s*,\s*'((?:[^']|'')*)'::tsquery\s*", re.I
)


def _rewritten(name: str, args: list[exp.Expr]) -> exp.Expr:
    if len(args) == 3:
        target, by = _query(args[1]), _query(args[2])
    else:
        found = _PAIR.fullmatch(_text_of(args[1], name))
        if found is None:
            raise NoForm(name)
        target, by = (pg_tsearch.tsquery_in(g.replace("''", "'")) for g in found.groups())
    return _text_value(pg_tsearch.tsquery_text(pg_tsearch.rewrite(_query(args[0]), target, by)))


def _names_config(args: list[exp.Expr]) -> bool:
    """Whether a ts_headline call's first argument is its config, its document then second."""
    return _said(args[0]) == "regconfig" or (
        len(args) == 4 or (len(args) == 3 and _said(args[1]) != "tsquery")
    )


def _json_value(value: exp.Expr, dialect: str) -> exp.Expr:
    """A json or jsonb result as the engine's JSON, which pgwire describes as the call's type;
    ClickHouse, which has no JSON form, holds its text."""
    if dialect in ("trino", "athena", "presto") and not isinstance(value, exp.Null):
        return exp.ParseJSON(this=value)
    return value if dialect == "clickhouse" else exp.cast(value, "JSON")


def _null_of(name: str, args: list[exp.Expr], dialect: str) -> exp.Expr:
    """The NULL a call answers for a NULL argument, of the type the function returns."""
    if name == "ts_headline":
        document = args[1] if _names_config(args) else args[0]
        if _said(document) in ("json", "jsonb"):
            return _json_value(exp.null(), dialect)
    result = _RESULTS.get(name, "text")
    if dialect == "clickhouse" and result.startswith("ARRAY"):
        return exp.null()  # ClickHouse has no NULL of an array
    return exp.Cast(this=exp.null(), to=exp.DataType.build(result))


def _headlined(name: str, args: list[exp.Expr], dialect: str) -> exp.Expr:
    named = _names_config(args)
    config = _config(args[0], name) if named else "english"
    rest = args[1:] if named else args
    query = _query(rest[1])
    options = _text_of(rest[2], name) if len(rest) > 2 else ""

    def mark(document: str) -> str | None:
        return parse.headline(config, document, query, options)

    document = _text_of(rest[0], name)
    try:
        if _said(rest[0]) in ("json", "jsonb"):
            written = parse.json_headline(document, mark)
            if _said(rest[0]) == "json":
                return _text_value(written)
            return _json_value(_text_value(jsonb_text(written)), dialect)
        written = mark(document)
    except parse.Unread:
        raise NoForm(name) from None
    if written is None:
        raise NoForm(name)
    return _text_value(written)


def _weight(value: exp.Expr, name: str) -> str:
    text = _constant(value)
    if text is None:
        raise NoForm(name)
    if text not in _WEIGHT_NAMES:
        raise PostgresError("22023", f'unrecognized weight: "{text}"')
    return text.lower()


def _weights(value: exp.Expr, name: str) -> exp.Expr:
    """A "char"[] of weights, as the letters a mark's weight is compared with."""
    array = _array(value, name)
    if not isinstance(array, exp.Array):
        raise NoForm(name)
    letters = []
    for item in array.expressions:
        if isinstance(item, exp.Null):
            raise PostgresError("22004", "weight array may not contain nulls")
        if item.this not in _WEIGHT_NAMES:
            raise PostgresError("22023", f'unrecognized weight: "{item.this}"')
        letters.append(_text_value(item.this.upper()))
    return exp.Array(expressions=letters)


def _on_vector(name: str, args: list[exp.Expr]) -> exp.Expr:
    """A row's call: the overload the statement's types choose."""
    if any(_is_null(a) for a in args):
        return exp.null()
    called = name
    written = [_vector(args[0])]
    if name == "setweight":
        called = f"setweight_{_weight(args[1], name)}"
        if len(args) == 3:
            called = called.replace("_", "_some_")
            written.append(_without_nulls(_array(args[2], name)))
    elif name == "ts_delete":
        written.append(args[1])
        second = args[1].unnest()
        if isinstance(second, exp.Cast) and second.to.is_type("array"):
            called = "ts_delete_array"
            written[1] = _without_nulls(_array(args[1], name))
    elif name == "ts_filter":
        called = "ts_filter_weights"
        written.append(_weights(args[1], name))
    elif len(args) > 1:
        written.append(_vector(args[1]))
    if name in _ORDERED:
        row = exp.Anonymous(this="tsvector_order", expressions=written)
        ordering = sqlglot.parse_one(f"x {_ORDERED[name]} 0", read="postgres")
        ordering.this.replace(row)
        return ordering
    return exp.Anonymous(this=called, expressions=written)


#: The operators :func:`lowered` reads, for function_macros.mentioned.
OPERATORS = "@@"
_VECTOR, _QUERY, _TEXT, _UNKNOWN = "tsvector", "tsquery", "text", "unknown"
#: The calls whose answer is a vector, or a query.
_VECTOR_CALLS = frozenset(
    {"to_tsvector", "json_to_tsvector", "jsonb_to_tsvector", "array_to_tsvector", "setweight",
     "ts_delete", "ts_filter", "tsvector_concat"}
)  # fmt: skip
_QUERY_CALLS = frozenset(_BUILDERS) | frozenset(_JOINS) | {"tsquery_not", "tsquery_phrase"}


def _operand(value: exp.Expr) -> str | None:
    """What an operand of @@ is said to be: None where the statement does not say."""
    node = value.unnest()
    called = _call(node)
    if called is not None:
        return (
            _VECTOR if called[0] in _VECTOR_CALLS else _QUERY if called[0] in _QUERY_CALLS else None
        )
    node = _column_cast(node)
    if isinstance(node, exp.Cast):
        said = node.to.sql("postgres").lower().split("(")[0]
        return said if said in (_VECTOR, _QUERY) else _TEXT if said in _TEXTS else None
    if isinstance(node, exp.Literal) and node.is_string:
        return _UNKNOWN
    if isinstance(node, exp.Null):
        return _UNKNOWN
    return _TEXT if _told_text(node) else None


#: @@ of each two operands, as PostgreSQL resolves it: an unknown literal takes the other side's
#: type where an operator has it, and is text, the preferred string type, where it is open.
_MATCHES: dict[tuple[str | None, str | None], str] = {
    (_VECTOR, _QUERY): "ts_match_vq", (_VECTOR, _UNKNOWN): "ts_match_vq",
    (_QUERY, _VECTOR): "ts_match_qv", (_QUERY, _UNKNOWN): "ts_match_qv",
    (_UNKNOWN, _VECTOR): "ts_match_qv",
    (_TEXT, _QUERY): "ts_match_tq", (_UNKNOWN, _QUERY): "ts_match_tq",
    (_TEXT, _TEXT): "ts_match_tt", (_TEXT, _UNKNOWN): "ts_match_tt", (_UNKNOWN, _TEXT): "ts_match_tt",
}  # fmt: skip


def _match_operator(node: exp.MatchAgainst) -> exp.Expr:
    """``left @@ right``: the function its operand types choose, refused by name where the
    statement does not say them."""
    left, right = node.expressions[0], node.this
    name = _MATCHES.get((_operand(left), _operand(right)))
    if name is None:
        raise NoForm("@@")
    if _is_null(left) or _is_null(right):
        return exp.null()
    if name in _SEARCHES:
        return _matched(name, [left, right])
    return _searched_text(name, [left, right])


def _below(written: exp.Expr, rewrite: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """``written`` with what it holds lowered: transform does not revisit what a form holds, a
    call of a column in a vector argument. Its own root is a row, some of PostgreSQL's name."""
    return written.transform(lambda node: node if node is written else rewrite(node), copy=False)


def lowered(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree``'s text search calls as rows of the table, or as what they answer. Raises
    :class:`NoForm` for what no form writes."""

    def rewrite(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.MatchAgainst):
            try:
                return _below(_match_operator(node), rewrite)
            except _NotConstant:
                raise NoForm("@@") from None
        called = _call(node)
        if called is None or called[0] not in NAMES:
            return node
        name, args = called
        if name in _SETS:
            raise NoForm(name)  # :func:`_sets` has every use it can read
        try:
            if name in _FOLDED:
                written = _folded(name, args)
            elif name in _SEARCHES:
                written = _matched(name, args)
            elif name == "array_to_tsvector":
                written = exp.null() if _is_null(args[0]) else _lexemes(args[0], name)
            elif name in _RANKS:
                written = _rank(name, args)
            elif any(_is_null(a) for a in args):
                written = exp.null()
            elif name in _DOCUMENTS:
                written = _document(name, args)
            elif name in _BUILDERS:
                written = _built(name, args)
            elif name in ("ts_match_tt", "ts_match_tq"):
                written = _searched_text(name, args)
            elif name == "ts_lexize":
                written = _lexized(name, args)
            elif name == "ts_rewrite":
                written = _rewritten(name, args)
            elif name == "ts_headline":
                written = _headlined(name, args, dialect)
            else:
                written = _on_vector(name, args)
        except _NotConstant:
            raise NoForm(name) from None
        if isinstance(written, exp.Null):
            written = _null_of(name, args, dialect)
        written = _below(written, rewrite)
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(written, name)
        return written

    for column in tree.find_all(exp.Column):
        cast = values_cast(column)
        if cast is not None:
            column.meta[_SAID] = cast.copy()
    try:
        tree = _sets(tree)
    except _NotConstant as unread:
        raise NoForm("ts_parse") from unread
    return tree.transform(rewrite, copy=False)
