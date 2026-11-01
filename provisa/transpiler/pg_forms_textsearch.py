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
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.core import pg_tsearch
from provisa.core.pg_error import END, MARK, PostgresError
from provisa.core.pg_tsearch import Node
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
)
TRINO = Engine(
    find_all="regexp_extract_all({}, {})",
    find="regexp_extract({}, {}, 1)",
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
    starts="starts_with({}, {})",
    within="strpos({1}, {0}) > 0",
    sort="array_sort({})",
    distinct="array_distinct({})",
    flatten="flatten({})",
    concat="concat({}, {})",
    fold="reduce({0}, {4}, ({1}, {2}) -> {3}, _f -> _f)",
    let="transform(ARRAY[{1}], {0} -> {2})[1]",
    upto="sequence(1, {})",
    first="element_at({}, 1)",
    largest="array_max({})",
    bytes="length(to_utf8({}))",
    digits="CAST({} AS VARCHAR)",
    quotient="({} / {})",
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
    starts="startsWith({}, {})",
    within="position({1}, {0}) > 0",
    sort="arraySort({})",
    distinct="arrayDistinct({})",
    flatten="arrayFlatten({})",
    concat="arrayConcat({}, {})",
    fold="arrayFold(({1}, {2}) -> {3}, {0}, {4})",
    let="arrayMap({0} -> {2}, [{1}])[1]",
    upto="range(1, {} + 1)",
    first="{}[1]",
    largest="arrayMax({})",
    bytes="length({})",
    digits="toString({})",
    quotient="intDiv({}, {})",
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


def _read(e: Engine, function: str, answer: str, *values: str) -> str:
    """``answer``, of a NULL for a NULL, once each value is in tsvectorout's form."""
    wrong = " OR ".join(f"NOT {e.matches.format(v, e.text(_FORM))}" for v in values)
    message = e.text(f"{MARK} 0A000: {function} {REFUSED} {END}")
    guard = e.fails.format(f"coalesce({wrong}, false)", message)
    return e.guarded.format(guard, answer)


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


def _entry_compare(e: Engine) -> str:
    """silly_cmp_tsvector, of the two entries ``_a[_i]`` and ``_b[_i]`` that differ."""
    ma, mb = _marks_of(e, "_a[_i]"), _marks_of(e, "_b[_i]")
    la, lb = _lexeme_of(e, "_a[_i]"), _lexeme_of(e, "_b[_i]")
    marks = e.let.format(
        "_p", _split_marks(e, ma), e.let.format("_q", _split_marks(e, mb), _marks_compare(e))
    )
    has_marks = f"{ma} <> ''"
    return (
        f"CASE WHEN ({ma} = '') <> ({mb} = '') THEN {_sign(has_marks)} "
        f"WHEN {la} <> {lb} THEN {_sign(f'{la} < {lb}')} ELSE {marks} END"
    )


def _compare(e: Engine) -> str:
    size_a, size_b = e.size.format("_a"), e.size.format("_b")
    differs = e.keep.format(e.upto.format(size_a), "_k", "_a[_k] <> _b[_k]")
    entries = e.let.format("_i", f"{differs}[1]", _entry_compare(e))
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
    moved = e.digits.format(f"{_position(e, '_m')} + _x")
    each = f"{moved} || CASE WHEN {_rank_of('_m')} > 0 THEN right(_m, 1) ELSE '' END"
    shifted = e.join.format(e.map.format(_split_marks(e, "_y"), "_m", each), "','")
    written = (
        f"{_quoted(e, '_l')} || CASE WHEN _u <> '' AND _w <> '' THEN ':' || _u || ',' || _w "
        "WHEN _u <> '' THEN ':' || _u WHEN _w <> '' THEN ':' || _w ELSE '' END"
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


def _rows() -> tuple[Row, ...]:
    forms: dict[tuple[str, tuple[str, ...]], Forms] = {}

    def add(d: str, name: str, params: str, sql: str) -> None:
        forms.setdefault((name, tuple(params.split(", "))), {})[_OF[d]] = sql

    for d, e in ENGINES:
        lexemes = e.map.format(_tokens(e), "_t", _lexeme_of(e, "_t"))
        add(d, "tsvector_to_array", "v", _read(e, "tsvector_to_array", lexemes, "v"))
        kept = e.keep.format(_tokens(e), "_t", f"{_lexeme_of(e, '_t')} <> x")
        add(d, "ts_delete", "v, x", _read(e, "ts_delete", _join_tokens(e, kept), "v"))
        dropped = e.keep.format(_tokens(e), "_t", f"NOT {e.has.format('x', _lexeme_of(e, '_t'))}")
        add(d, "ts_delete_array", "v, x", _read(e, "ts_delete", _join_tokens(e, dropped), "v"))
        add(d, "tsvector_eq", "a, b", _read(e, "tsvector_eq", "a = b", "a", "b"))
        add(d, "tsvector_ne", "a, b", _read(e, "tsvector_ne", "a <> b", "a", "b"))
        for weight in "ABCD":
            one = _read(e, "setweight", _setweight(e, weight), "v")
            add(d, f"setweight_{weight.lower()}", "v", one)
            some = _read(e, "setweight", _setweight(e, weight, "x"), "v")
            add(d, f"setweight_some_{weight.lower()}", "v, x", some)
        add(d, "ts_filter", "v, w", _read(e, "ts_filter", _filtered(e, "w"), "v"))
        add(d, "tsvector_cmp", "a, b", _read(e, "tsvector_cmp", _compare(e), "a", "b"))
        add(d, "tsvector_concat", "a, b", _read(e, "tsvector_concat", _concat(e), "a", "b"))
        add(d, "ts_term", "v, x, p, w", _read(e, "ts_match_vq", _term(e), "v"))
        add(d, "ts_positions", "v, x, p, w", _read(e, "ts_match_vq", _positions(e), "v"))
        add(d, "ts_stripped", "v, x, p, w", _read(e, "ts_match_vq", _stripped(e), "v"))
        add(d, "ts_phrase", "l, r, n", e.keep.format("r", "_q", e.has.format("l", "_q - n")))
        add(d, "ts_found", "l", f"{e.size.format('l')} > 0")
        add(d, "ts_lexemes", "x", _lexeme_list(e))
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
        "tsvector_cmp", "tsvector_concat", "array_to_tsvector",
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
    return _boolean(False) if query is None else _search(vector, query, name)


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
            written.append(_no_nulls(_array(args[2], name)))
    elif name == "ts_delete":
        written.append(args[1])
        second = args[1].unnest()
        if isinstance(second, exp.Cast) and second.to.is_type("array"):
            called = "ts_delete_array"
            written[1] = _without_nulls(_array(args[1], name))
    elif name == "ts_filter":
        written.append(_weights(args[1], name))
    elif len(args) > 1:
        written.append(_vector(args[1]))
    if name in _ORDERED:
        row = exp.Anonymous(this="tsvector_cmp", expressions=written)
        ordering = sqlglot.parse_one(f"x {_ORDERED[name]} 0", read="postgres")
        ordering.this.replace(row)
        return ordering
    return exp.Anonymous(this=called, expressions=written)


def lowered(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree``'s text search calls as rows of the table, or as what they answer. Raises
    :class:`NoForm` for what no form writes."""

    def rewrite(node: exp.Expr) -> exp.Expr:
        called = _call(node)
        if called is None or called[0] not in NAMES:
            return node
        name, args = called
        try:
            if name in _FOLDED:
                written = _folded(name, args)
            elif name in _SEARCHES:
                written = _matched(name, args)
            elif name == "array_to_tsvector":
                written = exp.null() if _is_null(args[0]) else _lexemes(args[0], name)
            else:
                written = _on_vector(name, args)
        except _NotConstant:
            raise NoForm(name) from None
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(written, name)
        return written

    return tree.transform(rewrite)
