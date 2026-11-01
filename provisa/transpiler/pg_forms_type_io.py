# Copyright (c) 2026 Kenneth Stott
# Canary: b84e2d19-7f03-4c6a-9e51-3ad07c1f6e28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each type's input, output and send function as forms (REQ-2167, REQ-2171), on DuckDB, Trino and
ClickHouse, replayed against postgres:17.11 (tests/unit/pgwire/type_io_corpus.py).

``in`` reads the type's text, ``out`` writes it, ``send`` writes its binary form, as bytes. Each
function is strict, so its NULL is NULL. An input function answers the texts its pattern takes, raises
PostgreSQL's own error for a text PostgreSQL refuses, and refuses by name, as ``REFUSED``, a text it
cannot tell: a form has no parser, and an answer it guessed would differ.

A form is written per engine because none shares a spelling of bytes or of a pattern; :class:`Engine`
holds what they differ in, and everything else is one definition."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from provisa.transpiler.pg_forms_types import REFUSED, refused

Forms = dict[frozenset[str], str]
Row = tuple[str, tuple[str, ...], Forms]


@dataclass(frozen=True)
class Engine:
    """An engine's spelling of what a form needs."""

    dialects: frozenset[str]
    text: str  # the type of a text
    binary: str  # the type of bytes
    int2: str
    int4: str
    int8: str
    full_match: Callable[[str, str], str]  # a value, a pattern: all of it matches
    as_text: Callable[[str], str]  # a value's text
    word_hex: Callable[[str, int], str]  # an integer's bytes, big-endian, in hexadecimal
    text_hex: Callable[[str], str]  # a text's UTF-8 bytes in hexadecimal
    bytes_hex: Callable[[str], str]  # bytes in hexadecimal
    of_hex: Callable[[str], str]  # hexadecimal as bytes
    size: Callable[[str], str]  # a text's length in bytes
    chars: Callable[[str], str]  # a text's length in characters
    wide: str = "DECIMAL(38, 0)"  # holds what an integer's text may overflow


def _duckdb_match(value: str, pattern: str) -> str:
    return f"regexp_full_match({value}, '{pattern}')"


def _anchored(pattern: str) -> str:
    return f"'^(?:{pattern})$'"


DUCKDB = Engine(
    dialects=frozenset({"duckdb"}),
    text="VARCHAR",
    binary="BLOB",
    int2="SMALLINT",
    int4="INTEGER",
    int8="BIGINT",
    full_match=_duckdb_match,
    as_text=lambda v: f"CAST({v} AS VARCHAR)",
    word_hex=lambda v, size: f"right(printf('%016x', CAST({v} AS BIGINT)), {2 * size})",
    text_hex=lambda v: f"hex(encode({v}))",
    bytes_hex=lambda v: f"hex({v})",
    of_hex=lambda v: f"unhex({v})",
    size=lambda v: f"strlen({v})",
    chars=lambda v: f"length({v})",
)
TRINO = Engine(
    dialects=frozenset({"trino", "athena", "presto"}),
    text="VARCHAR",
    binary="VARBINARY",
    int2="SMALLINT",
    int4="INTEGER",
    int8="BIGINT",
    full_match=lambda v, p: f"regexp_like({v}, {_anchored(p)})",
    as_text=lambda v: f"CAST({v} AS VARCHAR)",
    word_hex=lambda v, size: (
        f"substr(to_hex(to_big_endian_64(CAST({v} AS BIGINT))), {17 - 2 * size})"
    ),
    text_hex=lambda v: f"to_hex(to_utf8({v}))",
    bytes_hex=lambda v: f"to_hex({v})",
    of_hex=lambda v: f"from_hex({v})",
    size=lambda v: f"length(to_utf8({v}))",
    chars=lambda v: f"length({v})",
)
CLICKHOUSE = Engine(
    dialects=frozenset({"clickhouse"}),
    text="Nullable(String)",
    binary="Nullable(String)",
    int2="Nullable(Int16)",
    int4="Nullable(Int32)",
    int8="Nullable(Int64)",
    full_match=lambda v, p: f"match({v}, {_anchored(p)})",
    as_text=lambda v: f"toString({v})",
    word_hex=lambda v, size: f"substring(leftPad(hex(toInt64({v})), 16, '0'), {17 - 2 * size})",
    text_hex=lambda v: f"hex({v})",
    bytes_hex=lambda v: f"hex({v})",
    of_hex=lambda v: f"unhex({v})",
    size=lambda v: f"length({v})",
    chars=lambda v: f"lengthUTF8({v})",
    wide="Decimal(38, 0)",
)
ENGINES = (DUCKDB, TRINO, CLICKHOUSE)


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _raises(state: str, message: str, sql_type: str) -> str:
    """PostgreSQL's error ``state``, ``message`` an SQL expression, as a value of ``sql_type``."""
    return refused(state, message, sql_type)


def _by_name(function: str, sql_type: str) -> str:
    """A value this form cannot read as PostgreSQL reads it: refused by name, not answered."""
    return _raises("0A000", _quoted(f"{function} {REFUSED}"), sql_type)


def _strict(params: str, sql_type: str, answer: str) -> str:
    nulls = " OR ".join(f"{p} IS NULL" for p in params.split(", "))
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {answer} END"


def _syntax(type_name: str, sql_type: str) -> str:
    """The refusal of a text that is no value of the type."""
    said = _quoted(f'invalid input syntax for type {type_name}: "')
    return _raises("22P02", f"{said} || v || '\"'", sql_type)


def _range(type_name: str, sql_type: str) -> str:
    said = _quoted(f'" is out of range for type {type_name}')
    return _raises("22003", f"'value \"' || v || {said}", sql_type)


_DECIMAL = r"[+-]?[0-9]+"
_SPACES = r"[ \t\n\r\f\v]*"
#: A text of another base, or with a separator, or spaces no trim takes: valid, and no form's.
_OTHER_INTEGER = (
    rf"{_SPACES}[+-]?(0[xX][0-9a-fA-F_]+|0[oO][0-7_]+|0[bB][01_]+|[0-9][0-9_]*){_SPACES}"
)
_LONGEST = 30


def _integer_in(
    e: Engine, function: str, type_name: str, sql_type: str, low: str, high: str
) -> str:
    """An integer's input: its value in range, its error out of it."""
    text = "trim(v)"
    number = f"CAST({text} AS {e.wide})"
    return (
        f"CASE WHEN {e.full_match(text, _DECIMAL)} THEN "
        f"CASE WHEN {e.chars(text)} > {_LONGEST} OR {number} < {low} OR {number} > {high} "
        f"THEN {_range(type_name, sql_type)} ELSE CAST({text} AS {sql_type}) END "
        f"WHEN {e.full_match('v', _OTHER_INTEGER)} THEN {_by_name(function, sql_type)} "
        f"ELSE {_syntax(type_name, sql_type)} END"
    )


_TRUE = "'t', 'tr', 'tru', 'true', 'y', 'ye', 'yes', 'on', '1'"
_FALSE = "'f', 'fa', 'fal', 'fals', 'false', 'n', 'no', 'of', 'off', '0'"


def _bool_in(e: Engine) -> str:
    word = "lower(trim(v))"
    return (
        f"CASE WHEN {word} IN ({_TRUE}) THEN TRUE WHEN {word} IN ({_FALSE}) THEN FALSE "
        f"ELSE {_syntax('boolean', 'BOOLEAN')} END"
    )


def _text_row(
    name: str, params: str, sql_type: Callable[[Engine], str], form: Callable[[Engine], str]
) -> Row:
    return (
        name,
        tuple(params.split(", ")),
        {e.dialects: _strict(params, sql_type(e), form(e)) for e in ENGINES},
    )


def _texts(_: Engine) -> str:
    return _.text


def _bytes(_: Engine) -> str:
    return _.binary


def _out(name: str) -> Row:
    """An output function whose value's text is what it writes."""
    return _text_row(name, "v", _texts, lambda e: e.as_text("v"))


def _word_send(name: str, size: int) -> Row:
    """A send function of a value that is an integer of ``size`` bytes."""
    return _text_row(name, "v", _bytes, lambda e: e.of_hex(e.word_hex("v", size)))


def _integers() -> list[Row]:
    rows: list[Row] = []
    for stem, type_name, size, low, high, sql_type in (
        ("int2", "smallint", 2, "-32768", "32767", lambda e: e.int2),
        ("int4", "integer", 4, "-2147483648", "2147483647", lambda e: e.int4),
        ("int8", "bigint", 8, "-9223372036854775808", "9223372036854775807", lambda e: e.int8),
    ):
        rows.append(
            _text_row(
                f"{stem}in",
                "v",
                sql_type,
                lambda e, s=stem, n=type_name, lo=low, hi=high, t=sql_type: _integer_in(
                    e, f"{s}in", n, t(e), lo, hi
                ),
            )
        )
        rows.append(_out(f"{stem}out"))
        rows.append(_word_send(f"{stem}send", size))
    return rows


def _as_is(name: str, sql_type: Callable[[Engine], str] = _texts) -> Row:
    """A function that answers its text."""
    return _text_row(name, "v", sql_type, lambda _: "v")


def _text_send(name: str) -> Row:
    return _text_row(name, "v", _bytes, lambda e: e.of_hex(e.text_hex("v")))


def _texts_rows() -> list[Row]:
    rows: list[Row] = []
    for stem in ("text", "varchar"):
        rows += [_as_is(f"{stem}out"), _text_send(f"{stem}send")]
    rows.append(_as_is("textin"))
    rows += [_as_is("cstring_in"), _as_is("cstring_out"), _text_send("cstring_send")]
    rows += [_as_is("unknownin"), _as_is("unknownout"), _text_send("unknownsend")]
    return rows


#: A pseudo type's input and output, which refuse any value: PostgreSQL's words for each.
_NO_INPUT = (
    "any", "anyarray", "anycompatible", "anycompatiblearray", "anycompatiblemultirange",
    "anycompatiblenonarray", "anycompatiblerange", "anyelement", "anyenum", "anymultirange",
    "anynonarray", "anyrange", "pg_ddl_command",
)  # fmt: skip
_NO_OUTPUT = ("any", "anycompatible", "anycompatiblenonarray", "anyelement", "anynonarray")


def _no_value(name: str, message: str) -> Row:
    return _text_row(name, "v", _texts, lambda e: _raises("0A000", _quoted(message), e.text))


def _pseudo() -> list[Row]:
    rows = [_no_value(f"{t}_in", f"cannot accept a value of type {t}") for t in _NO_INPUT]
    rows += [_no_value(f"{t}_out", f"cannot display a value of type {t}") for t in _NO_OUTPUT]
    return rows


#: A row: name, parameters, each engine's form.
ROWS: tuple[Row, ...] = (
    *_integers(),
    (
        "boolin",
        ("v",),
        {e.dialects: _strict("v", "BOOLEAN", _bool_in(e)) for e in ENGINES},
    ),
    _text_row("boolout", "v", _texts, lambda _: "CASE WHEN v THEN 't' ELSE 'f' END"),
    _text_row("boolsend", "v", _bytes, lambda e: e.of_hex("CASE WHEN v THEN '01' ELSE '00' END")),
    *_texts_rows(),
    *_pseudo(),
)
