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

import re
from collections.abc import Callable
from dataclasses import dataclass, replace

from decimal import ROUND_HALF_UP, Decimal, localcontext

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

from provisa.core.pg_error import END, MARK
from provisa.transpiler import pg_forms_hash as hashing
from provisa.transpiler import pg_type_literals as literals
from provisa.transpiler.pg_forms_json import jsonb_text
from provisa.transpiler.pg_forms_types import REFUSED, refused

Forms = dict[frozenset[str], str]
Row = tuple[str, tuple[str, ...], Forms]


@dataclass(frozen=True)
class Engine:
    """An engine's spelling of what a form needs."""

    dialects: frozenset[str]
    text: str
    binary: str
    boolean: str
    int2: str
    int4: str
    int8: str
    real: str
    double: str
    date: str
    time: str
    timestamp: str
    timestamptz: str
    full_match: Callable[[str, str], str]  # a value, a pattern: all of it matches
    sub: Callable[[str, str, str], str]  # the first match of a pattern, as a group: \1 is 1
    as_text: Callable[[str], str]  # a value's text
    wide: Callable[[str], str]  # a text's number, of any integer's size; NULL for no number
    word_hex: Callable[[str, int], str]  # an integer's bytes, big-endian, in hexadecimal
    float_hex: Callable[[str, int], str]  # a float's bytes, big-endian, in hexadecimal
    text_hex: Callable[[str], str]  # a text's UTF-8 bytes in hexadecimal
    bytes_hex: Callable[[str], str]  # bytes in hexadecimal
    of_hex: Callable[[str], str]  # hexadecimal as bytes
    size: Callable[[str], str]  # a text's length in bytes
    chars: Callable[[str], str]  # a text's length in characters
    raises: Callable[[str, str, str], str]  # a SQLSTATE, a message's SQL, a type
    let: Callable[[str, str, str], str]  # a name, its value, a body that reads it
    days: Callable[[str], str]  # a date's text: the days since 2000-01-01
    valid_date: Callable[[str], str]  # a date's text as a date; NULL for one no calendar holds
    nan: Callable[[str], str]  # NaN as a float type
    infinity: Callable[[str, str], str]  # a sign, a float type
    numbers: Callable[[str], str]  # the hexadecimal float8s of the numbers in a text, joined
    utc_text: Callable[[str], str]  # an instant's text in UTC, with no zone
    backslash: str = "\\"  # one, in a string literal
    backslash_re: str = "\\\\"  # one, in a pattern
    instant: Callable[[str], str] = lambda v: f"CAST({v} AS TIMESTAMPTZ)"  # text with its zone
    clock: Callable[[str], str] = lambda v: f"CAST({v} AS TIME)"
    json_valid: Callable[[str], str] = lambda v: f"json_valid({v})"
    finite: Callable[[str], str] = lambda v: f"isfinite({v})"
    numeric: Callable[[str], str] = lambda v: f"CAST({v} AS DECIMAL(38, 18))"
    pad: Callable[[str, str], str] = lambda v, n: f"rpad({v}, {n}, ' ')"  # in characters
    left: Callable[[str, str], str] = lambda v, n: f"substr({v}, 1, {n})"  # in characters
    tail: Callable[[str, str], str] = lambda v, n: f"substr({v}, {n})"  # in characters
    real_of: Callable[[str], str] = lambda v: f"CAST({v} AS REAL)"  # a text's float4
    #: A float4's text, of its shortest digits.
    real_text: Callable[[str], str] = lambda v: f"CAST({v} AS VARCHAR)"
    #: A float8's text, of its shortest digits.
    double_text: Callable[[str], str] = lambda v: f"CAST({v} AS VARCHAR)"


def _trino_raises(state: str, message: str, sql_type: str) -> str:
    """fail(), whose unknown type casts to any: Trino refuses a cast of an integer to a date, a time,
    a timestamp, bytes or a uuid while analyzing, so ``refused`` fails a statement whose value never
    reaches it."""
    return f"CAST(fail('{MARK} {state}: ' || {message} || '{END}') AS {sql_type})"


def _trino_pattern(pattern: str) -> str:
    """``pattern`` for Trino's regular expressions, which take ``[[]`` for a class in a class."""
    return pattern.replace("[[]", "\\[").replace("[]]", "\\]")


def _trino_real_text(v: str) -> str:
    """A float4's text on Trino. Its cast keeps seven digits, which need not read back: 1.677722E7
    for 16777216, 3.402823E38 for the largest float4. The first of these that does is the shortest."""
    written = ", ".join(f"format('%.{n}e', _r)" for n in range(9))
    return (
        f"transform(ARRAY[{v}], _r -> CASE WHEN is_finite(_r) THEN "
        f"filter(ARRAY[{written}], _s -> CAST(_s AS REAL) = _r)[1] "
        "ELSE CAST(_r AS VARCHAR) END)[1]"
    )


def _trino_double_text(v: str) -> str:
    """A float8's text on Trino. Java writes two digits at least, 4.9E-324 where one, 5e-324, reads
    back the same value: such a digit is PostgreSQL's. Only a value this small has one."""
    one = f"format('%.0e', {v})"
    return (
        f"CASE WHEN abs({v}) < 1e-300 AND CAST({one} AS DOUBLE) = {v} THEN {one} "
        f"ELSE CAST({v} AS VARCHAR) END"
    )


def _clickhouse_raises(state: str, message: str, sql_type: str) -> str:
    """A refusal ClickHouse reaches only in the branch taken: materialized, so nothing folds it, and
    accurate, so the text that is no number raises."""
    said = f"concat('{MARK} {state}: ', {message}, '{END}')"
    return f"CAST(toString(accurateCast(materialize({said}), 'Int32')) AS {sql_type})"


_NUMBER_TEXT = "-?[0-9.]+(?:[eE][-+]?[0-9]+)?"
#: DuckDB shows a float's bits as a BIT, which reads back as the integer of those bits.
DUCKDB = Engine(
    dialects=frozenset({"duckdb"}),
    text="VARCHAR",
    binary="BLOB",
    boolean="BOOLEAN",
    int2="SMALLINT",
    int4="INTEGER",
    int8="BIGINT",
    real="FLOAT",
    double="DOUBLE",
    date="DATE",
    time="TIME",
    timestamp="TIMESTAMP",
    timestamptz="TIMESTAMPTZ",
    full_match=lambda v, p: f"regexp_full_match({v}, '{p}')",
    sub=lambda v, p, g: f"regexp_replace({v}, '{p}', '{g}')",
    as_text=lambda v: f"CAST({v} AS VARCHAR)",
    wide=lambda v: f"TRY_CAST({v} AS DECIMAL(38, 0))",
    word_hex=lambda v, size: f"right(printf('%016x', CAST({v} AS BIGINT)), {2 * size})",
    float_hex=lambda v, size: (
        f"printf('%016x', CAST(CAST(CAST({v} AS {'DOUBLE' if size == 8 else 'FLOAT'}) AS BIT) "
        f"AS {'BIGINT' if size == 8 else 'INTEGER'}))"
        if size == 8
        else f"right(printf('%016x', CAST(CAST(CAST({v} AS FLOAT) AS BIT) AS INTEGER)), 8)"
    ),
    text_hex=lambda v: f"hex(encode({v}))",
    bytes_hex=lambda v: f"hex({v})",
    of_hex=lambda v: f"unhex({v})",
    size=lambda v: f"strlen({v})",
    chars=lambda v: f"length({v})",
    raises=refused,
    let=lambda n, v, b: f"list_transform([{v}], {n} -> {b})[1]",
    days=lambda v: f"date_diff('day', DATE '2000-01-01', CAST({v} AS DATE))",
    valid_date=lambda v: f"TRY_CAST({v} AS DATE)",
    nan=lambda t: f"CAST('NaN' AS {t})",
    infinity=lambda sign, t: f"CAST('{sign}Infinity' AS {t})",
    utc_text=lambda v: f"strftime({v} AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f')",
    real_text=lambda v: (
        "[_s FOR _s IN ["
        + ", ".join(f"printf('%.{n}e', {v})" for n in range(9))
        + f"] IF CAST(_s AS FLOAT) = {v}][1]"
    ),
    numbers=lambda v: (
        f"array_to_string(list_transform(regexp_extract_all({v}, '{_NUMBER_TEXT}'), "
        "_n -> printf('%016x', CAST(CAST(CAST(_n AS DOUBLE) AS BIT) AS BIGINT))), '')"
    ),
)
TRINO = Engine(
    dialects=frozenset({"trino", "athena", "presto"}),
    text="VARCHAR",
    binary="VARBINARY",
    boolean="BOOLEAN",
    int2="SMALLINT",
    int4="INTEGER",
    int8="BIGINT",
    real="REAL",
    double="DOUBLE",
    date="DATE",
    time="TIME(6)",
    timestamp="TIMESTAMP(6)",
    timestamptz="TIMESTAMP(6) WITH TIME ZONE",
    full_match=lambda v, p: f"regexp_like({v}, '^(?:{_trino_pattern(p)})$')",
    sub=lambda v, p, g: f"regexp_replace({v}, '{_trino_pattern(p)}', '{g.replace(chr(92), '$')}')",
    as_text=lambda v: f"CAST({v} AS VARCHAR)",
    wide=lambda v: f"TRY_CAST({v} AS DECIMAL(38, 0))",
    word_hex=lambda v, size: (
        f"substr(to_hex(to_big_endian_64(CAST({v} AS BIGINT))), {17 - 2 * size})"
    ),
    float_hex=lambda v, size: (
        f"to_hex(to_ieee754_64(CAST({v} AS DOUBLE)))"
        if size == 8
        else f"to_hex(to_ieee754_32(CAST({v} AS REAL)))"
    ),
    text_hex=lambda v: f"to_hex(to_utf8({v}))",
    bytes_hex=lambda v: f"to_hex({v})",
    of_hex=lambda v: f"from_hex({v})",
    size=lambda v: f"length(to_utf8({v}))",
    chars=lambda v: f"length({v})",
    raises=_trino_raises,
    let=lambda n, v, b: f"transform(ARRAY[{v}], {n} -> {b})[1]",
    days=lambda v: f"date_diff('day', DATE '2000-01-01', CAST({v} AS DATE))",
    valid_date=lambda v: f"TRY_CAST({v} AS DATE)",
    nan=lambda t: f"CAST(nan() AS {t})",
    infinity=lambda sign, t: f"CAST({sign}infinity() AS {t})",
    utc_text=lambda v: f"regexp_replace(CAST({v} AT TIME ZONE 'UTC' AS VARCHAR), ' UTC$', '')",
    numbers=lambda v: (
        f"array_join(transform(regexp_extract_all({v}, '{_NUMBER_TEXT}'), "
        "_n -> to_hex(to_ieee754_64(CAST(_n AS DOUBLE)))), '')"
    ),
    json_valid=lambda v: f"(TRY(json_parse({v})) IS NOT NULL)",
    finite=lambda v: f"is_finite({v})",
    instant=lambda v: (
        "CAST(regexp_replace(regexp_replace("
        f"{v}, '([+-][0-9][0-9])$', '$1:00'), '([+-][0-9:]+)$', ' $1') "
        "AS TIMESTAMP(6) WITH TIME ZONE) AT TIME ZONE 'UTC'"
    ),
    clock=lambda v: f"CAST({v} AS TIME(6))",
    real_text=_trino_real_text,
    double_text=_trino_double_text,
)
CLICKHOUSE = Engine(
    dialects=frozenset({"clickhouse"}),
    text="Nullable(String)",
    binary="Nullable(String)",
    boolean="Nullable(Bool)",
    int2="Nullable(Int16)",
    int4="Nullable(Int32)",
    int8="Nullable(Int64)",
    real="Nullable(Float32)",
    double="Nullable(Float64)",
    date="Nullable(Date32)",
    time="Nullable(String)",  # sqlglot reads no Time64: a time is its text here
    timestamp="Nullable(DateTime64(6, 'UTC'))",
    timestamptz="Nullable(DateTime64(6, 'UTC'))",
    full_match=lambda v, p: f"match({v}, '^(?:{p})$')",
    sub=lambda v, p, g: f"replaceRegexpOne({v}, '{p}', '{g.replace(chr(92), chr(92) * 2)}')",
    as_text=lambda v: f"toString({v})",
    wide=lambda v: f"toDecimal128OrNull({v}, 0)",
    word_hex=lambda v, size: f"substring(leftPad(hex(toInt64({v})), 16, '0'), {17 - 2 * size})",
    float_hex=lambda v, size: (
        f"leftPad(hex(reverse(reinterpretAsString(toFloat64({v})))), 16, '0')"
        if size == 8
        else f"leftPad(hex(reverse(reinterpretAsString(toFloat32({v})))), 8, '0')"
    ),
    text_hex=lambda v: f"substring(hex(concat('A', {v})), 3)",
    bytes_hex=lambda v: f"substring(hex(concat('A', {v})), 3)",
    of_hex=lambda v: f"unhex({v})",
    size=lambda v: f"length({v})",
    chars=lambda v: f"lengthUTF8({v})",
    pad=lambda v, n: f"rightPadUTF8({v}, {n}, ' ')",
    left=lambda v, n: f"substringUTF8({v}, 1, {n})",
    tail=lambda v, n: f"substringUTF8({v}, {n})",
    real_of=lambda v: f"CAST(toFloat64({v}) AS Nullable(Float32))",
    double_text=lambda v: f"toString({v})",
    raises=_clickhouse_raises,
    let=lambda n, v, b: f"arrayMap({n} -> {b}, [{v}])[1]",
    days=lambda v: f"dateDiff('day', toDate32('2000-01-01'), toDate32({v}))",
    valid_date=lambda v: f"nullIf(toString(toDate32OrNull({v})) = {v}, FALSE)",
    nan=lambda t: f"CAST(toFloat64('nan') AS {t})",
    infinity=lambda sign, t: f"CAST(toFloat64('{sign}inf') AS {t})",
    utc_text=lambda v: f"toString({v})",
    numbers=lambda v: (
        f"arrayStringConcat(arrayMap(_n -> leftPad(hex(reverse(reinterpretAsString("
        f"toFloat64OrZero(_n)))), 16, '0'), extractAll({v}, '{_NUMBER_TEXT}')))"
    ),
    backslash="\\\\",
    backslash_re="\\\\\\\\",
    instant=lambda v: f"parseDateTime64BestEffortOrNull({v}, 6, 'UTC')",
    clock=lambda v: v,
    json_valid=lambda v: f"isValidJSON({v})",
    finite=lambda v: f"isFinite({v})",
    numeric=lambda v: f"toDecimal128OrNull({v}, 18)",
)
ENGINES = (DUCKDB, TRINO, CLICKHOUSE)
Typed = Callable[[Engine], str]
#: A hash's answer on an engine, of its seed: None for the 32-bit hash.
Hashed = Callable[[Engine, "hashing.Engine", str | None], str]


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _text(e: Engine) -> str:
    return e.text


def _binary(e: Engine) -> str:
    return e.binary


def _int2(e: Engine) -> str:
    return e.int2


def _int4(e: Engine) -> str:
    return e.int4


def _int8(e: Engine) -> str:
    return e.int8


def _boolean(e: Engine) -> str:
    return e.boolean


def _by_name(e: Engine, function: str, sql_type: str) -> str:
    """A value this form cannot read as PostgreSQL reads it: refused by name, not answered."""
    return e.raises("0A000", _quoted(f"{function} {REFUSED}"), sql_type)


def _strict(params: str, sql_type: str, answer: str) -> str:
    nulls = " OR ".join(f"{p} IS NULL" for p in params.split(", "))
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {answer} END"


_PARAMETER = re.compile(r"\b(v|m)\b")


def _unfolded(e: Engine, form: str) -> str:
    """``form`` as ``e`` evaluates it. ClickHouse folds each branch of a constant's CASE, the ones it
    does not take too, so what a branch refuses or overflows is raised for a value that never
    reaches it: a materialized argument is not folded, and only the branch taken runs."""
    return _PARAMETER.sub(r"materialize(\1)", form) if e is CLICKHOUSE else form


def _row(name: str, params: str, sql_type: Typed, form: Callable[[Engine], str]) -> Row:
    return (
        name,
        tuple(params.split(", ")),
        {e.dialects: _strict(params, sql_type(e), _unfolded(e, form(e))) for e in ENGINES},
    )


#: Each function's PostgreSQL result, for a call a NULL answers.
RESULTS: dict[str, str] = {}


def _noted(row: Row, result: str) -> Row:
    RESULTS[row[0]] = result
    return row


def _syntax(e: Engine, type_name: str, sql_type: str) -> str:
    """The refusal of a text that is no value of the type."""
    said = _quoted(f'invalid input syntax for type {type_name}: "')
    return e.raises("22P02", f"{said} || v || '\"'", sql_type)


def _out_of_range(e: Engine, type_name: str, sql_type: str) -> str:
    said = _quoted(f'" is out of range for type {type_name}')
    return e.raises("22003", f"'value \"' || v || {said}", sql_type)


_DECIMAL = "[+-]?[0-9]+"
_SPACES = "[ \t\n\r\f]*"
#: A text of another base or with a separator: valid, and no form's.
_OTHER_INTEGER = (
    f"{_SPACES}[+-]?(0[xX][0-9a-fA-F_]+|0[oO][0-7_]+|0[bB][01_]+|[0-9][0-9_]*){_SPACES}"
)
_LONGEST = 30


def _integer_in(
    e: Engine,
    function: str,
    type_name: str,
    sql_type: str,
    low: str,
    high: str,
    wraps: bool = False,
) -> str:
    """An integer's input: its value in range, its error out of it. An unsigned type's value below
    zero wraps (``wraps``), as strtoul reads it."""
    number = e.wide("trim(v)")
    value = (
        f"CASE WHEN {number} < 0 THEN {number} + 4294967296 ELSE {number} END" if wraps else number
    )
    return (
        f"CASE WHEN {e.full_match('trim(v)', _DECIMAL)} THEN "
        f"CASE WHEN {e.chars('trim(v)')} > {_LONGEST} OR {number} < {low} OR {number} > {high} "
        f"THEN {_out_of_range(e, type_name, sql_type)} ELSE CAST({value} AS {sql_type}) END "
        f"WHEN {e.full_match('v', _OTHER_INTEGER)} THEN {_by_name(e, function, sql_type)} "
        f"ELSE {_syntax(e, type_name, sql_type)} END"
    )


def _out(name: str) -> Row:
    """An output function whose value's text is what it writes."""
    return _noted(_row(name, "v", _text, lambda e: e.as_text("v")), "text")


def _word_send(name: str, size: int) -> Row:
    """A send function of a value that is an integer of ``size`` bytes."""
    return _noted(_row(name, "v", _binary, lambda e: e.of_hex(e.word_hex("v", size))), "bytea")


def _integers() -> list[Row]:
    rows: list[Row] = []
    for stem, type_name, size, low, high, sql_type, result, wraps in (
        ("int2", "smallint", 2, "-32768", "32767", _int2, "smallint", False),
        ("int4", "integer", 4, "-2147483648", "2147483647", _int4, "integer", False),
        (
            "int8",
            "bigint",
            8,
            "-9223372036854775808",
            "9223372036854775807",
            _int8,
            "bigint",
            False,
        ),
        ("oid", "oid", 4, "-2147483648", "4294967295", _int8, "bigint", True),
        ("xid", "xid", 4, "-2147483648", "4294967295", _int8, "bigint", True),
        ("cid", "cid", 4, "-2147483648", "4294967295", _int8, "bigint", True),
    ):

        def read(e: Engine, s=stem, n=type_name, lo=low, hi=high, t=sql_type, w=wraps) -> str:
            return _integer_in(e, f"{s}in", n, t(e), lo, hi, w)

        rows.append(_noted(_row(f"{stem}in", "v", sql_type, read), result))
        rows.append(_out(f"{stem}out"))
        rows.append(_word_send(f"{stem}send", size))
    return rows


_TRUE = "'t', 'tr', 'tru', 'true', 'y', 'ye', 'yes', 'on', '1'"
_FALSE = "'f', 'fa', 'fal', 'fals', 'false', 'n', 'no', 'of', 'off', '0'"


def _bool_in(e: Engine) -> str:
    word = "lower(trim(v))"
    return (
        f"CASE WHEN {word} IN ({_TRUE}) THEN TRUE WHEN {word} IN ({_FALSE}) THEN FALSE "
        f"ELSE {_syntax(e, 'boolean', e.boolean)} END"
    )


def _as_is(name: str, result: str = "text", params: str = "v") -> Row:
    """A function that answers its text."""
    return _noted(_row(name, params, _text, lambda _: "v"), result)


def _text_send(name: str) -> Row:
    return _noted(_row(name, "v", _binary, lambda e: e.of_hex(e.text_hex(e.as_text("v")))), "bytea")


def _texts() -> list[Row]:
    rows: list[Row] = []
    for stem in ("text", "varchar"):
        rows += [_as_is(f"{stem}out"), _text_send(f"{stem}send")]
    rows += [
        _as_is("textin"),
        _as_is("cstring_in"),
        _as_is("cstring_out"),
        _text_send("cstring_send"),
    ]
    rows += [_as_is("unknownout"), _text_send("unknownsend")]
    rows += [_as_is("json_out"), _text_send("json_send")]
    no_conversion = _quoted("failed to find conversion function from unknown to text")
    forms = {e.dialects: e.raises("XX000", no_conversion, e.text) for e in ENGINES}
    rows.append(_noted(("unknownin", ("v",), forms), "text"))

    def json_read(e: Engine) -> str:
        said = _quoted("invalid input syntax for type json")
        return f"CASE WHEN {e.json_valid('v')} THEN v ELSE {e.raises('22P02', said, e.text)} END"

    rows.append(_noted(_row("json_in", "v", _text, json_read), "text"))
    return rows


def _hex_of_digits(e: Engine, digits: str) -> str:
    return e.of_hex(digits)


def _bytea() -> list[Row]:
    def read(e: Engine) -> str:
        hex_form = f"[{e.backslash_re}]x([0-9a-fA-F][0-9a-fA-F])*"
        plain = f"[^{e.backslash_re}]*"
        return (
            f"CASE WHEN {e.full_match('v', hex_form)} THEN {e.of_hex('substr(v, 3)')} "
            f"WHEN {e.full_match('v', plain)} THEN {e.of_hex(e.text_hex('v'))} "
            f"ELSE {_by_name(e, 'byteain', e.binary)} END"
        )

    def written(e: Engine) -> str:
        return f"'{e.backslash}x' || lower({e.bytes_hex('v')})"

    return [
        _noted(_row("byteain", "v", _binary, read), "bytea"),
        _noted(_row("byteaout", "v", _text, written), "text"),
        _noted(_row("byteasend", "v", _binary, lambda _: "v"), "bytea"),
    ]


#: A pseudo type's input and output, which refuse any value, in PostgreSQL's words.
_NO_INPUT = (
    "any", "anyarray", "anycompatible", "anycompatiblearray", "anycompatiblenonarray",
    "anyelement", "anyenum", "anynonarray", "pg_ddl_command", "pg_dependencies", "pg_mcv_list",
    "pg_ndistinct", "pg_node_tree",
)  # fmt: skip
_NO_INPUT_OF_THREE = ("anycompatiblemultirange", "anycompatiblerange", "anymultirange", "anyrange")
_NO_OUTPUT = (
    "any", "anycompatible", "anycompatiblenonarray", "anyelement", "anynonarray", "pg_ddl_command",
)  # fmt: skip


def _no_value(name: str, message: str, params: str = "v") -> Row:
    return _noted(
        _row(name, params, _text, lambda e: e.raises("0A000", _quoted(message), e.text)), "text"
    )


def _pseudo() -> list[Row]:
    rows = [_no_value(f"{t}_in", f"cannot accept a value of type {t}") for t in _NO_INPUT]
    rows += [
        _no_value(f"{t}_in", f"cannot accept a value of type {t}", "v, o, m")
        for t in _NO_INPUT_OF_THREE
    ]
    rows += [_no_value(f"{t}_out", f"cannot display a value of type {t}") for t in _NO_OUTPUT]
    rows += [_no_value("pg_ddl_command_send", "cannot display a value of type pg_ddl_command")]
    rows += [_no_value("shell_out", "cannot display a value of a shell type")]
    rows += [_noted(_row("void_in", "v", _text, lambda _: "''"), "text")]
    rows += [_noted(_row("void_out", "v", _text, lambda _: "''"), "text")]
    rows += [_noted(_row("void_send", "v", _binary, lambda e: e.of_hex("''")), "bytea")]
    return rows


#: Types held as their text: each one's canonical text, the rest of its texts refused by name.
_NUMBER = "-?(0|[1-9][0-9]*)([.][0-9]*[1-9])?"
_UNSIGNED = "(0|[1-9][0-9]*)([.][0-9]*[1-9])?"
_POINT = f"[(]{_NUMBER},{_NUMBER}[)]"
_OCTET = "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])"
_IPV4 = f"{_OCTET}[.]{_OCTET}[.]{_OCTET}[.]{_OCTET}"
_PAIR = "[0-9a-f][0-9a-f]"
_HELD: tuple[tuple[str, str, str, str | None], ...] = (
    # stem, input, output, what its canonical text is
    ("point_in", "point_out", _POINT, None),
    ("line_in", "line_out", f"[{{]{_NUMBER},{_NUMBER},{_NUMBER}[}}]", None),
    (
        "lseg_in",
        "lseg_out",
        f"\\[{_POINT},{_POINT}\\]".replace("\\[", "[[]").replace("\\]", "[]]"),
        None,
    ),
    ("box_in", "box_out", f"{_POINT},{_POINT}", None),
    ("path_in", "path_out", f"[(]{_POINT}(,{_POINT})*[)]|[[]{_POINT}(,{_POINT})*[]]", None),
    ("poly_in", "poly_out", f"[(]{_POINT}(,{_POINT})*[)]", None),
    ("circle_in", "circle_out", f"<{_POINT},{_UNSIGNED}>", None),
    ("inet_in", "inet_out", f"{_IPV4}(/([0-9]|[12][0-9]|3[01]))?", None),
    (
        "cidr_in",
        "cidr_out",
        f"{_OCTET}[.]0[.]0[.]0/8|{_OCTET}[.]{_OCTET}[.]0[.]0/16|{_OCTET}[.]{_OCTET}[.]{_OCTET}[.]0/24",
        None,
    ),
    ("macaddr_in", "macaddr_out", f"{_PAIR}(:{_PAIR}){{5}}", None),
    ("macaddr8_in", "macaddr8_out", f"{_PAIR}(:{_PAIR}){{7}}", None),
    ("tidin", "tidout", "[(][0-9]+,[0-9]+[)]", None),
    ("pg_lsn_in", "pg_lsn_out", "[0-9A-F]+/[0-9A-F]+", None),
    ("int2vectorin", "int2vectorout", "(-?[0-9]+( -?[0-9]+)*)?", None),
    ("oidvectorin", "oidvectorout", "([0-9]+( [0-9]+)*)?", None),
    ("pg_snapshot_in", "pg_snapshot_out", "[0-9]+:[0-9]+:([0-9]+(,[0-9]+)*)?", None),
    ("txid_snapshot_in", "txid_snapshot_out", "[0-9]+:[0-9]+:([0-9]+(,[0-9]+)*)?", None),
    (
        "timetz_in",
        "timetz_out",
        "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]*[1-9])?[+-][0-9][0-9]",
        None,
    ),
)


def _box_ordered(e: Engine, text: str) -> str:
    """Whether a box's first corner is its upper right, as PostgreSQL writes one."""

    def at(pattern: str) -> str:
        return f"CAST({e.sub(text, pattern, chr(92) + '1')} AS {e.double})"

    x1, y1 = at("^[(]([^,]+),.*$"), at("^[(][^,]+,([^)]+)[)].*$")
    x2, y2 = at("^.*,[(]([^,]+),[^,]+$"), at("^.*,([^,)]+)[)]$")
    return f"{x1} >= {x2} AND {y1} >= {y2}"


def _line_is_one(e: Engine, text: str) -> str:
    return f"NOT {e.full_match(text, '[{]-?0,-?0,.*')}"


def _snapshot_is_ordered(e: Engine, text: str) -> str:
    low = f"CAST({e.sub(text, '^([0-9]+):.*$', chr(92) + '1')} AS {e.int8})"
    high = f"CAST({e.sub(text, '^[0-9]+:([0-9]+):.*$', chr(92) + '1')} AS {e.int8})"
    return f"{low} <= {high}"


#: What a canonical text must also hold: else it is a value PostgreSQL normalizes or refuses.
_ALSO: dict[str, Callable[[Engine, str], str]] = {
    "box_in": _box_ordered,
    "box_out": _box_ordered,
    "line_in": _line_is_one,
    "pg_snapshot_in": _snapshot_is_ordered,
    "txid_snapshot_in": _snapshot_is_ordered,
}


def _canonical(function: str, pattern: str, params: str = "v") -> Row:
    def read(e: Engine) -> str:
        text = e.as_text("v")
        refused = [f"NOT ({e.full_match(text, pattern)})"]
        if "m" in params:  # a typmod that would round its fraction
            refused.append(f"m >= 0 AND m < 6 AND {e.full_match(text, '.*[.][0-9]+.*')}")
        also = _ALSO.get(function)
        if also is not None:
            refused.append(f"NOT ({also(e, text)})")
        by_name = _by_name(e, function, e.text)
        return f"CASE {' '.join(f'WHEN {c} THEN {by_name}' for c in refused)} ELSE {text} END"

    return _noted(_row(function, params, _text, read), "text")


#: Three-argument inputs, whose oid and typmod a form of the text does not read.
_OF_THREE = frozenset({"timetz_in"})


def _held() -> list[Row]:
    rows: list[Row] = []
    for in_, out, pattern, _ in _HELD:
        rows.append(_canonical(in_, pattern, "v, o, m" if in_ in _OF_THREE else "v"))
        rows.append(_canonical(out, pattern))
    return rows


def _refused(names: str, params: str, sql_type: Typed, result: str) -> list[Row]:
    """What no form reads yet: a NULL answers, and a value is refused by name."""
    return [
        _noted(_row(n, params, sql_type, lambda e, n=n: _by_name(e, n, sql_type(e))), result)
        for n in names.split()
    ]


def _clean_seconds(e: Engine, text: str) -> str:
    """A clock's text with its fraction's trailing zeros gone, as PostgreSQL writes it."""
    return e.sub(e.sub(text, "([.][0-9]*[1-9])0+$", "\\1"), "[.]0+$", "")


def _micros(e: Engine, clock: str) -> str:
    """A clock's ``HH:MM:SS[.ffffff]`` text as its microseconds."""

    def part(pattern: str) -> str:
        return f"CAST({e.sub(clock, pattern, chr(92) + '1')} AS {e.int8})"

    fraction = (
        f"CASE WHEN {e.full_match(clock, '.*[.][0-9]+')} THEN "
        f"CAST(rpad({e.sub(clock, '^.*[.]([0-9]+)$', chr(92) + '1')}, 6, '0') AS {e.int8}) "
        "ELSE 0 END"
    )
    return (
        f"({part('^([0-9]+):.*$')} * 3600000000 + {part('^[0-9]+:([0-9]+):.*$')} * 60000000 + "
        f"{part('^[0-9]+:[0-9]+:([0-9]+).*$')} * 1000000 + {fraction})"
    )


_DAY_US = 86400000000
_DATE = "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
_CLOCK = "([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9]([.][0-9]{1,6})?)?"
_FULL_CLOCK = "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?"
_ZONE = "[+-](0[0-9]|1[0-5])(:[0-5][0-9])?"


def _instant_us(e: Engine, text: str) -> str:
    """A timestamp's text as its microseconds since 2000-01-01."""
    day = e.sub(text, "^([0-9-]+) .*$", "\\1")
    clock = e.sub(text, "^[0-9-]+ (.*)$", "\\1")
    return f"({e.days(day)} * {_DAY_US} + {_micros(e, clock)})"


def _spell_in(
    function: str, pattern: str, result: Typed, read: Callable[[Engine, str], str], params: str
) -> Callable[[Engine], str]:
    """An input of a text ``pattern`` takes. A typmod that would round its fraction is refused,
    and a date no calendar holds is PostgreSQL's error. One CASE: ClickHouse makes one node of a
    refusal written twice, and a node two CASEs share runs whichever branch is taken."""

    def form(e: Engine) -> str:
        fraction = e.full_match("v", ".*[.][0-9]+.*")
        rounds = f"m >= 0 AND m < 6 AND {fraction}" if "m" in params else "FALSE"
        day = e.sub("v", "^([0-9]+-[0-9]+-[0-9]+).*$", chr(92) + "1")
        wrong = "FALSE" if function == "time_in" else f"{e.valid_date(day)} IS NULL"
        said = _quoted('date/time field value out of range: "') + " || v || '\"'"
        by_name = _by_name(e, function, result(e))
        return (
            f"CASE WHEN NOT ({e.full_match('v', pattern)}) THEN {by_name} WHEN {rounds} THEN {by_name} "
            f"WHEN {wrong} THEN {e.raises('22008', said, result(e))} ELSE {read(e, 'v')} END"
        )

    return form


def _dates() -> list[Row]:
    def date_in(e: Engine, v: str) -> str:
        return f"CAST({v} AS {e.date})"

    def time_in(e: Engine, v: str) -> str:
        return e.clock(v)

    def timestamp_in(e: Engine, v: str) -> str:
        return f"CAST({v} AS {e.timestamp})"

    def instant_in(e: Engine, v: str) -> str:
        return e.instant(v)

    def of_date(e: Engine) -> str:
        return e.date

    def of_time(e: Engine) -> str:
        return e.time

    def of_stamp(e: Engine) -> str:
        return e.timestamp

    def of_instant(e: Engine) -> str:
        return e.timestamptz

    stamp_text = f"{_DATE} {_FULL_CLOCK}"
    return [
        _noted(_row("date_in", "v", of_date, _spell_in("date_in", _DATE, of_date, date_in, "v")), "date"),
        _out("date_out"),
        _word_send_of("date_send", 4, lambda e: e.days(e.as_text("v"))),
        _noted(
            _row("time_in", "v, o, m", of_time, _spell_in("time_in", _FULL_CLOCK, of_time, time_in, "v, o, m")),
            "time without time zone",
        ),
        _noted(_row("time_out", "v", _text, lambda e: _clean_seconds(e, e.as_text("v"))), "text"),
        _word_send_of("time_send", 8, lambda e: _micros(e, e.as_text("v"))),
        _noted(
            _row(
                "timestamp_in", "v, o, m", of_stamp,
                _spell_in("timestamp_in", f"{_DATE}( {_FULL_CLOCK})?", of_stamp, timestamp_in, "v, o, m"),
            ),
            "timestamp without time zone",
        ),
        _noted(_row("timestamp_out", "v", _text, lambda e: _clean_seconds(e, e.as_text("v"))), "text"),
        _word_send_of("timestamp_send", 8, lambda e: _instant_us(e, e.as_text("v"))),
        _noted(
            _row(
                "timestamptz_in", "v, o, m", of_instant,
                _spell_in("timestamptz_in", stamp_text + _ZONE, of_instant, instant_in, "v, o, m"),
            ),
            "timestamp with time zone",
        ),
        _noted(
            _row("timestamptz_out", "v", _text, lambda e: _clean_seconds(e, e.utc_text("v")) + " || '+00'"),
            "text",
        ),
        _word_send_of("timestamptz_send", 8, lambda e: _instant_us(e, e.utc_text("v"))),
    ]  # fmt: skip


def _word_send_of(name: str, size: int, value: Callable[[Engine], str]) -> Row:
    return _noted(_row(name, "v", _binary, lambda e: e.of_hex(e.word_hex(value(e), size))), "bytea")


_FLOAT_TEXT = "[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?"
_NANS = "'nan', '+nan', '-nan'"
_INFINITIES = "'inf', '+inf', 'infinity', '+infinity'"
_NEGATIVE_INFINITIES = "'-inf', '-infinity'"


def _vanished(e: Engine, text: str) -> str:
    """Whether a text of a number reads as zero on an engine that loses a subnormal: an answer that
    would differ, so it is refused."""
    return f"(CAST({text} AS {e.double}) = 0 AND {e.full_match(text, '[^eE]*[1-9].*')})"


def _float_in(e: Engine, name: str, type_name: str, sql_type: str, top: str) -> str:
    value = e.real_of("trim(v)") if sql_type == e.real else f"CAST(trim(v) AS {sql_type})"
    word = "lower(trim(v))"
    return (
        f"CASE WHEN {e.full_match('trim(v)', _FLOAT_TEXT)} THEN CASE WHEN "
        f"abs(CAST(trim(v) AS {e.double})) > {top} THEN "
        + e.raises(
            "22003", f"'\"' || v || {_quoted(f'" is out of range for type {type_name}')}", sql_type
        )
        + f" WHEN {_vanished(e, 'trim(v)')} THEN {_by_name(e, name, sql_type)}"
        + f" ELSE {value} END WHEN {word} IN ({_NANS}) THEN {e.nan(sql_type)} "
        f"WHEN {word} IN ({_INFINITIES}) THEN {e.infinity('', sql_type)} "
        f"WHEN {word} IN ({_NEGATIVE_INFINITIES}) THEN {e.infinity('-', sql_type)} "
        f"ELSE {_syntax(e, type_name, sql_type)} END"
    )


def _shortest(e: Engine, text: str, longest: int) -> str:
    """A float's text as PostgreSQL's shortest form writes it: its digits are the engine's, which
    round-trip, and where the point goes, and an exponent, are PostgreSQL's. ``longest`` is where
    positional notation stops."""
    text = f"lower({text})"
    mantissa = e.sub("_t", "e.*$", "")
    power = f"CASE WHEN {e.full_match('_t', '.*e.*')} THEN CAST({e.sub('_t', '^.*e[+]?', '')} AS {e.int8}) ELSE 0 END"
    whole = e.sub("_m", "[.].*$", "")
    part = (
        f"CASE WHEN {e.full_match('_m', '.*[.].*')} THEN {e.sub('_m', '^[^.]*[.]', '')} ELSE '' END"
    )
    all_digits = f"({whole} || {part})"
    leading = e.chars(e.sub("_a", "[1-9].*$", ""))
    digits = e.sub(e.sub("_a", "^0+", ""), "0+$", "")
    power10 = f"({e.chars(whole)} + _p - {leading} - 1)"

    def fixed() -> str:
        return (
            f"CASE WHEN _x < 0 THEN '0.' || rpad('', CAST(-_x - 1 AS {e.int4}), '0') || _d "
            f"WHEN {e.chars('_d')} <= _x + 1 THEN rpad(_d, CAST(_x + 1 AS {e.int4}), '0') "
            f"ELSE substr(_d, 1, CAST(_x + 1 AS {e.int8})) || '.' || substr(_d, CAST(_x + 2 AS {e.int8})) END"
        )

    def exponent() -> str:
        return (
            f"substr(_d, 1, 1) || CASE WHEN {e.chars('_d')} > 1 THEN '.' || substr(_d, 2) ELSE '' END "
            f"|| 'e' || CASE WHEN _x < 0 THEN '-' ELSE '+' END || CASE WHEN abs(_x) < 10 THEN '0' ELSE '' END || {e.as_text('abs(_x)')}"
        )

    written = f"CASE WHEN _x < -4 OR _x >= {longest} THEN {exponent()} ELSE {fixed()} END"
    body = e.let(
        "_a",
        all_digits,
        f"CASE WHEN {e.full_match('_a', '0*')} THEN '0' ELSE "
        + e.let("_d", digits, e.let("_x", power10, written))
        + " END",
    )
    body = e.let("_m", mantissa, e.let("_p", power, body))
    return e.let(
        "_t",
        text,
        "CASE WHEN _t IN ('nan', '-nan') THEN 'NaN' WHEN _t IN ('inf', 'infinity') THEN 'Infinity' "
        "WHEN _t IN ('-inf', '-infinity') THEN '-Infinity' "
        f"WHEN substr(_t, 1, 1) = '-' THEN '-' || {e.let('_t', 'substr(_t, 2)', body)} ELSE {body} END",
    )


def _float4_out(e: Engine) -> str:
    """float4out. Ryu takes no digits that lie on the midpoint between two floats and the engines'
    shortest texts do, so a value whose shortest text is that midpoint is refused by name."""
    text = e.real_text("v")
    double = f"CAST(v AS {e.double})"
    written = f"CAST({text} AS {e.double})"
    half = f"power(2, floor(log2(abs({double}))) - 24)"
    tie = (
        f"(abs({double}) >= 16777216 AND {e.finite(double)} "
        f"AND abs({written} - {double}) >= {half})"
    )
    return (
        f"CASE WHEN {tie} THEN {_by_name(e, 'float4out', e.text)} ELSE {_shortest(e, text, 6)} END"
    )


def _floats() -> list[Row]:
    def real(e: Engine) -> str:
        return e.real

    def double(e: Engine) -> str:
        return e.double

    return [
        _noted(_row("float4in", "v", real, lambda e: _float_in(e, "float4in", "real", e.real, "3.4028235677973366e38")), "real"),
        _noted(_row("float8in", "v", double, lambda e: _float_in(e, "float8in", "double precision", e.double, "1.7976931348623157e308")), "double precision"),
        _noted(_row("float4out", "v", _text, _float4_out), "text"),
        _noted(_row("float8out", "v", _text, lambda e: _shortest(e, e.double_text("v"), 15)), "text"),
        _noted(_row("float4send", "v", _binary, lambda e: e.of_hex(e.float_hex("v", 4))), "bytea"),
        _noted(_row("float8send", "v", _binary, lambda e: e.of_hex(e.float_hex("v", 8))), "bytea"),
    ]  # fmt: skip


_NAME_BYTES = 63


def _character() -> list[Row]:
    def name_in(e: Engine) -> str:
        return (
            f"CASE WHEN {e.size('v')} <= {_NAME_BYTES} THEN v "
            f"WHEN {e.size('v')} = {e.chars('v')} THEN {e.left('v', str(_NAME_BYTES))} "
            f"ELSE {_by_name(e, 'namein', e.text)} END"
        )

    def char_in(e: Engine) -> str:
        return (
            f"CASE WHEN {e.size('v')} = {e.chars('v')} THEN {e.left('v', '1')} "
            f"ELSE {_by_name(e, 'charin', e.text)} END"
        )

    def char_bytes(e: Engine) -> str:
        return (
            f"CASE WHEN v = '' THEN {e.of_hex(chr(39) + '00' + chr(39))} "
            f"WHEN {e.size('v')} = 1 THEN {e.of_hex(e.text_hex('v'))} "
            f"ELSE {_by_name(e, 'charsend', e.binary)} END"
        )

    def sized(e: Engine, name: str, pads: bool) -> str:
        """character(n)'s and varchar(n)'s input: what fits, a tail of spaces cut, else refused."""
        length = "(m - 4)"
        count = f"CAST({length} AS {e.int4})"
        kept = e.pad(e.left("v", count), count) if pads else e.left("v", count)
        fitted = e.pad("v", count) if pads else "v"
        tail = e.tail("v", f"CAST({length} + 1 AS {e.int4})")
        too_long = e.raises(
            "22001",
            _quoted(f"value too long for type {name}(") + f" || {e.as_text(length)} || ')'",
            e.text,
        )
        return (
            f"CASE WHEN m < 5 THEN v WHEN {e.chars('v')} <= {length} THEN {fitted} "
            f"WHEN trim({tail}) = '' THEN {kept} ELSE {too_long} END"
        )

    return [
        _noted(_row("namein", "v", _text, name_in), "name"),
        _as_is("nameout"),
        _text_send("namesend"),
        _noted(_row("charin", "v", _text, char_in), "text"),
        _as_is("charout"),
        _noted(_row("charsend", "v", _binary, char_bytes), "bytea"),
        _noted(_row("bpcharin", "v, o, m", _text, lambda e: sized(e, "character", True)), "character"),
        _as_is("bpcharout"),
        _text_send("bpcharsend"),
        _noted(_row("varcharin", "v, o, m", _text, lambda e: sized(e, "character varying", False)), "character varying"),
    ]  # fmt: skip


_UUID = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"


def _uuids() -> list[Row]:
    def read(e: Engine) -> str:
        return (
            f"CASE WHEN {e.full_match('v', _UUID)} THEN CAST(lower(v) AS UUID) "
            f"ELSE {_by_name(e, 'uuid_in', 'UUID')} END"
        )

    return [
        _noted(_row("uuid_in", "v", lambda _: "UUID", read), "uuid"),
        _noted(_row("uuid_out", "v", _text, lambda e: e.as_text("v")), "text"),
        _noted(
            _row(
                "uuid_send", "v", _binary, lambda e: e.of_hex(f"replace({e.as_text('v')}, '-', '')")
            ),
            "bytea",
        ),
    ]


def _floats_send(name: str, head: Callable[[Engine, str], str]) -> Row:
    """A send of a geometric value: what ``head`` writes, then each number's float8."""
    return _noted(
        _row(
            name,
            "v",
            _binary,
            lambda e: e.of_hex(f"{head(e, e.as_text('v'))}{e.numbers(e.as_text('v'))}"),
        ),
        "bytea",
    )


def _no_head(_: Engine, __: str) -> str:
    return ""


def _sends() -> list[Row]:
    def path_head(e: Engine, text: str) -> str:
        closed = f"CASE WHEN substr({text}, 1, 1) = '(' THEN '01' ELSE '00' END"
        return f"{closed} || {_points_of_path(e, text)} || "

    return [
        _floats_send("point_send", _no_head),
        _floats_send("line_send", _no_head),
        _floats_send("lseg_send", _no_head),
        _floats_send("box_send", _no_head),
        _floats_send("circle_send", _no_head),
        _noted(_row("path_send", "v", _binary, lambda e: e.of_hex(path_head(e, e.as_text("v")) + e.numbers(e.as_text("v")))), "bytea"),
        _noted(_row("poly_send", "v", _binary, lambda e: e.of_hex(_points_of_path(e, e.as_text("v")) + " || " + e.numbers(e.as_text("v")))), "bytea"),
        _noted(_row("macaddr_send", "v", _binary, lambda e: e.of_hex(f"replace({e.as_text('v')}, ':', '')")), "bytea"),
        _noted(_row("macaddr8_send", "v", _binary, lambda e: e.of_hex(f"replace({e.as_text('v')}, ':', '')")), "bytea"),
    ]  # fmt: skip


def _points_of_path(e: Engine, text: str) -> str:
    """A path's or polygon's number of points, each of two float8s, in four bytes."""
    return e.word_hex(f"CAST({e.chars(e.numbers(text))} / 32 AS {e.int8})", 4)


def _parts_sends() -> list[Row]:
    """The sends a form writes from its text's parts."""

    def tid(e: Engine) -> str:
        text = e.as_text("v")
        block = e.sub(text, "^[(]([0-9]+),.*$", chr(92) + "1")
        tuple_at = e.sub(text, "^.*,([0-9]+)[)]$", chr(92) + "1")
        return e.of_hex(f"{e.word_hex(block, 4)} || {e.word_hex(tuple_at, 2)}")

    def lsn(e: Engine) -> str:
        text = e.as_text("v")
        high = e.sub(text, "/.*$", "")
        low = e.sub(text, "^.*/", "")
        return e.of_hex(f"lpad({high}, 8, '0') || lpad({low}, 8, '0')")

    def timetz(e: Engine) -> str:
        text = e.as_text("v")
        clock = e.sub(text, "[+-][0-9:]+$", "")
        hours = f"CAST({e.sub(text, '^.*[+-]([0-9][0-9])$', chr(92) + '1')} AS {e.int8})"
        east = e.full_match(text, ".*[+][0-9:]+")
        zone = f"CASE WHEN {east} THEN -{hours} * 3600 ELSE {hours} * 3600 END"
        return e.of_hex(f"{e.word_hex(_micros(e, clock), 8)} || {e.word_hex(zone, 4)}")

    return [
        _noted(_row("tidsend", "v", _binary, tid), "bytea"),
        _noted(_row("pg_lsn_send", "v", _binary, lsn), "bytea"),
        _noted(_row("timetz_send", "v", _binary, timetz), "bytea"),
    ]


def _typmods_out() -> list[Row]:
    def length(e: Engine) -> str:
        return f"CASE WHEN v > 4 THEN '(' || {e.as_text('v - 4')} || ')' ELSE '' END"

    def bits(e: Engine) -> str:
        return f"CASE WHEN v >= 0 THEN '(' || {e.as_text('v')} || ')' ELSE '' END"

    def clock(words: str) -> Callable[[Engine], str]:
        def form(e: Engine) -> str:
            return f"CASE WHEN v >= 0 THEN '(' || {e.as_text('v')} || '){words}' ELSE '{words}' END"

        return form

    rows = [
        _noted(_row("varchartypmodout", "v", _text, length), "text"),
        _noted(_row("bpchartypmodout", "v", _text, length), "text"),
        _noted(_row("bittypmodout", "v", _text, bits), "text"),
        _noted(_row("varbittypmodout", "v", _text, bits), "text"),
    ]
    for stem, words in (
        ("time", " without time zone"),
        ("timestamp", " without time zone"),
        ("timetz", " with time zone"),
        ("timestamptz", " with time zone"),
    ):
        rows.append(_noted(_row(f"{stem}typmodout", "v", _text, clock(words)), "text"))
    return rows


def _hashes() -> list[Row]:
    """The hashes of the types whose text says their value, by pg_forms_hash's lookup3: a time's,
    a timestamp's and an LSN's are hashint8's, a uuid's is its bytes', a timetz's its time's xor its
    zone's (src/backend/utils/adt/date.c, timestamp.c, pg_lsn.c, uuid.c)."""

    def int8_word(h: hashing.Engine, value: str) -> str:
        return hashing._word_of_int8(replace(h, value=value))

    def clock(e: Engine, h: hashing.Engine, text: str) -> str:
        return int8_word(h, _micros(e, text))

    def instant(e: Engine, h: hashing.Engine, text: str) -> str:
        return int8_word(
            h,
            f"CASE WHEN {text} = 'infinity' THEN {hashing._MAX_BIGINT} "
            f"WHEN {text} = '-infinity' THEN {hashing._MIN_BIGINT} ELSE {_instant_us(e, text)} END",
        )

    def lsn(e: Engine, h: hashing.Engine, text: str) -> str:
        """hashint8 of the LSN's 64 bits, whose high half says whether they are negative."""
        high = h.hex_word.format(f"lpad({e.sub(text, '/.*$', '')}, 8, '0')")
        low = h.hex_word.format(f"lpad({e.sub(text, '^.*/', '')}, 8, '0')")
        complement = (
            f"CASE WHEN {high} < {hashing._SIGN} THEN {high} ELSE {hashing._MASK} - {high} END"
        )
        return h.xor.format(low, complement)

    def zone(e: Engine, text: str) -> str:
        """A timetz's zone, as PostgreSQL holds it: its seconds west of UTC."""
        offset = e.sub(text, "^.*([+-][0-9:]+)$", chr(92) + "1")
        minutes = (
            f"CASE WHEN length({offset}) >= 6 THEN CAST(substr({offset}, 5, 2) AS {e.int8}) "
            "ELSE 0 END"
        )
        seconds = (
            f"CASE WHEN length({offset}) >= 9 THEN CAST(substr({offset}, 8, 2) AS {e.int8}) "
            "ELSE 0 END"
        )
        east = f"(CAST(substr({offset}, 2, 2) AS {e.int8}) * 3600 + {minutes} * 60 + {seconds})"
        return f"CASE WHEN substr({offset}, 1, 1) = '+' THEN -{east} ELSE {east} END"

    # A word of this module's integers is Nullable on ClickHouse, which lookup3's fold refuses: the
    # form's CASE has answered a NULL, so the word is the hash engine's value of it.
    bare = {
        e.dialects: h.value.replace("value", "({})")
        for e, (_, h) in zip(ENGINES, hashing.ENGINES, strict=True)
    }

    def hash_word(e: Engine, h: hashing.Engine, word: str, seed: str | None) -> str:
        return hashing._of_word(h, bare[e.dialects].format(word), seed)

    def of_word(word: Callable[[Engine, hashing.Engine, str], str]) -> Hashed:
        def hashed(e: Engine, h: hashing.Engine, seed: str | None) -> str:
            return hash_word(e, h, word(e, h, e.as_text(h.value)), seed)

        return hashed

    def uuid(e: Engine, h: hashing.Engine, seed: str | None) -> str:
        return hashing._of_hex(h, f"replace({e.as_text(h.value)}, '-', '')", seed)

    def timetz(e: Engine, h: hashing.Engine, seed: str | None) -> str:
        text = e.as_text(h.value)
        time = hash_word(e, h, clock(e, h, e.sub(text, "[+-][0-9:]+$", "")), seed)
        return h.xor.format(time, hash_word(e, h, zone(e, text), seed))

    rows: list[Row] = []
    for stem, hashed in (
        ("time", of_word(clock)),
        ("timestamp", of_word(instant)),
        ("pg_lsn", of_word(lsn)),
        ("uuid", uuid),
        ("timetz", timetz),
    ):
        for name, seed, result in (
            (f"{stem}_hash", None, "integer"),
            (f"{stem}_hash_extended", "seed", "bigint"),
        ):
            params = "v" if seed is None else "v, seed"
            forms = {}
            for e, (_, h) in zip(ENGINES, hashing.ENGINES, strict=True):
                h = replace(h, value=h.value.replace("value", "v"))
                answer = hashed(e, h, None if seed is None else h.seed)
                sql_type = h.int8_type if seed else h.int4_type
                forms[e.dialects] = _strict(
                    params, sql_type, f"CAST({_unfolded(e, answer)} AS {sql_type})"
                )
            rows.append(_noted((name, tuple(params.split(", ")), forms), result))
    return rows


#: A row: name, parameters, each engine's form.
ROWS: tuple[Row, ...] = (
    *_integers(),
    _noted(
        ("boolin", ("v",), {e.dialects: _strict("v", e.boolean, _bool_in(e)) for e in ENGINES}),
        "boolean",
    ),
    _noted(_row("boolout", "v", _text, lambda _: "CASE WHEN v THEN 't' ELSE 'f' END"), "text"),
    _noted(
        _row("boolsend", "v", _binary, lambda e: e.of_hex("CASE WHEN v THEN '01' ELSE '00' END")),
        "bytea",
    ),
    *_texts(),
    *_bytea(),
    *_pseudo(),
    *_held(),
    *_dates(),
    *_floats(),
    *_character(),
    *_uuids(),
    *_sends(),
    _as_is("enum_out"),
    _as_is("anyenum_out"),
    _text_send("enum_send"),
    *_parts_sends(),
    *_typmods_out(),
    *_hashes(),
    *_refused(
        """aclitemin array_out anyarray_out anycompatiblearray_out anyrange_out anymultirange_out
        anycompatiblerange_out anycompatiblemultirange_out range_out multirange_out record_out
        aclitemout bit_out varbit_out interval_out jsonb_out jsonpath_out
        tsqueryout tsvectorout xml_out pg_dependencies_out pg_mcv_list_out pg_ndistinct_out
        pg_node_tree_out regclassout regcollationout regconfigout regdictionaryout regnamespaceout
        regoperatorout regoperout regprocedureout regprocout regroleout regtypeout xid8out
        numeric_out numerictypmodout intervaltypmodout jsonb_in jsonpath_in tsqueryin tsvectorin
        xml_in regclassin regcollationin regconfigin regdictionaryin regnamespacein regoperatorin
        regoperin regprocedurein regprocin regrolein regtypein xid8in""",
        "v", _text, "text",
    ),
    *_refused(
        """array_send anyarray_send anycompatiblearray_send range_send multirange_send record_send
        bit_send varbit_send interval_send jsonb_send jsonpath_send tsquerysend
        tsvectorsend numeric_send inet_send cidr_send""",
        "v", _binary, "bytea",
    ),
    *_refused(
        """array_in range_in multirange_in bit_in varbit_in interval_in numeric_in""",
        "v, o, m", _text, "text",
    ),
    *_refused("enum_in", "v, o", _text, "text"),
    _no_value("record_in", "input of anonymous composite types is not implemented", "v, o, m"),
    _noted(
        ("domain_in", ("v", "o", "m"), {e.dialects: _by_name(e, "domain_in", e.text) for e in ENGINES}),
        "text",
    ),
    _noted(
        (
            "int8_sum",
            ("a", "b"),
            {
                e.dialects: "CASE WHEN a IS NULL AND b IS NULL THEN NULL "
                f"WHEN a IS NULL THEN {e.numeric(e.as_text('b'))} WHEN b IS NULL THEN a "
                f"ELSE a + {e.numeric(e.as_text('b'))} END"
                for e in ENGINES
            },
        ),
        "numeric",
    ),
    *_refused(
        """bittypmodin bpchartypmodin intervaltypmodin numerictypmodin timestamptypmodin
        timestamptztypmodin timetypmodin timetztypmodin varbittypmodin varchartypmodin
        interval_hash""",
        "v", _int4, "integer",
    ),
    *_refused(
        "interval_hash_extended",
        "v, s", _int8, "bigint",
    ),
    *_refused("int8_avg numeric_exp numeric_ln numeric_sqrt", "v", lambda e: e.text, "numeric"),
    *_refused("numeric_div numeric_div_trunc numeric_log numeric_power", "a, b", lambda e: e.text, "numeric"),
    *_refused("inet_merge", "a, b", _text, "text"),
    *_refused("int8inc_float8_float8", "a, b, c", _int8, "bigint"),
)  # fmt: skip

#: Functions that read a value of a type: a literal of it is read by the type's input function, so
#: the text PostgreSQL normalizes or refuses is treated as PostgreSQL treats it.
_INPUT_OF_TYPE: dict[str, tuple[str, int | None]] = {
    "smallint": ("int2in", None), "int": ("int4in", None), "bigint": ("int8in", None),
    "real": ("float4in", None), "double precision": ("float8in", None),
    "boolean": ("boolin", None), "bytea": ("byteain", None), "date": ("date_in", None),
    "time": ("time_in", 1083), "timestamp": ("timestamp_in", 1114),
    "timestamptz": ("timestamptz_in", 1184), "timetz": ("timetz_in", 1266),
    "uuid": ("uuid_in", None), "inet": ("inet_in", None), "cidr": ("cidr_in", None),
    "macaddr": ("macaddr_in", None), "macaddr8": ("macaddr8_in", None),
    "point": ("point_in", None), "line": ("line_in", None), "lseg": ("lseg_in", None),
    "box": ("box_in", None), "path": ("path_in", None), "polygon": ("poly_in", None),
    "circle": ("circle_in", None), "oid": ("oidin", None), "xid": ("xidin", None),
    "cid": ("cidin", None), "tid": ("tidin", None), "pg_lsn": ("pg_lsn_in", None),
    "int2vector": ("int2vectorin", None), "oidvector": ("oidvectorin", None),
    "name": ("namein", None), '"char"': ("charin", None),
    "pg_snapshot": ("pg_snapshot_in", None), "txid_snapshot": ("txid_snapshot_in", None),
}  # fmt: skip
#: The geometric inputs whose text, where it is canonical, is the value. ClickHouse reaches the
#: input's refusal in a send's arrayMap whatever the value, so _read writes such a literal as its
#: text, and leaves the input only what it refuses by name.
_CANONICAL_LITERAL = {
    held[0]: re.compile(held[2].replace("[[]", r"\[").replace("[]]", r"\]"))
    for held in _HELD
    if held[0] in ("point_in", "lseg_in", "path_in", "poly_in", "circle_in")
}
_NOT_STRICT = frozenset({"domain_in", "int8_sum", "unknownin"})
#: Types a statement names only for a function: what it casts to them is its argument.
_NO_ENGINE_TYPE = frozenset({"oid", "regtype", "regclass", "regproc"})


def _type_said(node: exp.Cast) -> str:
    return node.to.sql(dialect="postgres").lower().removeprefix("pg_catalog.")


def _bare(value: exp.Expr) -> exp.Expr:
    """An oid or a type's name, as what it is cast from: no engine has these casts."""
    node = value.unnest()
    while isinstance(node, exp.Cast) and _type_said(node) in _NO_ENGINE_TYPE:
        node = node.this.unnest()
    return node


def _read(value: exp.Expr) -> exp.Expr:
    """A literal of a type, as the type's input function reads it, and a text row its text."""
    node = value.unnest()
    if not isinstance(node, exp.Cast) or not isinstance(node.this, exp.Literal):
        return value
    if not node.this.is_string:
        return value
    typed = _type_said(node)
    if typed == "char(5)":  # character(n): the row that pads reads it
        return exp.Anonymous(
            this="bpcharin",
            expressions=[node.this.copy(), exp.Literal.number(1042), exp.Literal.number(9)],
        )
    reader = _INPUT_OF_TYPE.get(typed)
    if reader is None:
        return node.this.copy()  # its row reads the text
    name, oid = reader
    canonical = _CANONICAL_LITERAL.get(name)
    if canonical is not None and canonical.fullmatch(node.this.name):
        return node.this.copy()
    extra = [] if oid is None else [exp.Literal.number(oid), exp.Literal.number(-1)]
    return exp.Anonymous(this=name, expressions=[node.this.copy(), *extra])


def _is_null(value: exp.Expr) -> bool:
    node = value.unnest()
    return isinstance(node.this if isinstance(node, exp.Cast) else node, exp.Null)


def _null_of(result: str, dialect: str | None = None) -> exp.Expr:
    """A NULL of ``result``. ClickHouse holds a time as its text (Engine.time): sqlglot would write
    toTime64OrNull, which ClickHouse 24 does not have."""
    if dialect == "clickhouse" and result == "time without time zone":
        result = "text"
    return exp.Cast(this=exp.Null(), to=exp.DataType.build(result, dialect="postgres"))


def _literal_text(value: exp.Expr) -> str | None:
    """The text a statement writes for an argument; None for what it does not write."""
    node = value.unnest()
    if isinstance(node, exp.Cast):
        node = node.this.unnest()
    return node.this if isinstance(node, exp.Literal) and node.is_string else None


def _literal_number(value: exp.Expr) -> int | None:
    node = value.unnest()
    if isinstance(node, exp.Cast):
        node = node.this.unnest()
    if isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        return -int(node.this.this)
    return int(node.this) if isinstance(node, exp.Literal) and not node.is_string else None


def _said_text(text: str) -> exp.Expr:
    return exp.Literal.string(text)


def _said_bytes(value: bytes) -> exp.Expr:
    return exp.Anonymous(this="byteain", expressions=[exp.Literal.string("\\x" + value.hex())])


def _said_number(number: int, sql_type: str) -> exp.Expr:
    literal = exp.Literal.number(abs(number))
    written = exp.Neg(this=literal) if number < 0 else literal
    return exp.Cast(this=written, to=exp.DataType.build(sql_type, dialect="postgres"))


def _said_refusal(refusal: literals.Refused) -> exp.Expr:
    """An expression that raises PostgreSQL's error, whatever type the call would have answered."""
    quoted = "'" + refusal.message.replace("'", "''") + "'"
    return sqlglot.parse_one(refused(refusal.sqlstate, quoted, "TEXT"), read="postgres")


def _array_kind(value: exp.Expr) -> str | None:
    node = value.unnest()
    if not isinstance(node, exp.Cast) or not node.to.is_type(exp.DataType.Type.ARRAY):
        return None
    element = node.to.expressions[0].sql(dialect="postgres").lower()
    return {"int": "int", "text": "text", "boolean": "boolean", "decimal": "decimal"}.get(element)


def _range_kind(value: exp.Expr) -> tuple[str, bool] | None:
    node = value.unnest()
    if not isinstance(node, exp.Cast):
        return None
    typed = _type_said(node)
    for kind in ("int4", "int8", "num", "date"):
        if typed == f"{kind}range":
            return kind, False
        if typed == f"{kind}multirange":
            return kind, True
    return None


_LONGEST_DECIMAL = 38


def _numeric_typmod(text: str, typmod: int) -> str:
    """numeric_in's typmod: rounded to its scale, half away from zero; PostgreSQL's overflow."""
    if text == "NaN":
        return text
    overflow = literals.Refused("22003", "numeric field overflow")
    if text.endswith("Infinity"):
        raise overflow
    bits = typmod - 4
    precision, scale = bits >> 16 & 0xFFFF, ((bits & 0x7FF) ^ 1024) - 1024
    with localcontext(prec=len(text) + abs(scale) + 2):
        rounded = Decimal(text).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
    if rounded.copy_abs() >= Decimal(10) ** (precision - scale):
        raise overflow
    return f"{rounded:f}"


#: Each integer's input: its PostgreSQL name and bounds.
_INTEGER_RANGES = {
    "int2in": ("smallint", -32768, 32767),
    "int4in": ("integer", -2147483648, 2147483647),
    "int8in": ("bigint", -9223372036854775808, 9223372036854775807),
}
_OTHER_BASE = re.compile(r"0[xXoObB]|_")


def _other_base(name: str, text: str) -> exp.Expr | None:
    """An integer's text in another base or with separators, which no engine's cast reads."""
    number = literals.integer_read(text)
    if number is None:
        return None
    typed, low, high = _INTEGER_RANGES[name]
    if not low <= number <= high:
        raise literals.Refused("22003", f'value "{text}" is out of range for type {typed}')
    return _said_number(number, typed)


def _fold_row(name: str, arguments: list[exp.Expr]) -> exp.Expr | None:
    """A call of a literal, answered as PostgreSQL answers it; None where the row is to read it."""
    first = arguments[0]
    text = _literal_text(first)
    if name in _RANGE_OUT or name in _RANGE_SEND:
        kinds = _range_kind(first)
        if kinds is None or text is None:
            return None
        kind, many = kinds
        if many:
            ranges = literals.multirange_read(text, kind)
            if name in _RANGE_OUT:
                return _said_text(literals.multirange_text(ranges))
            return _said_bytes(literals.multirange_bytes(ranges, kind))
        one = literals.range_read(text, kind)
        if name in _RANGE_OUT:
            return _said_text(literals.range_text(one))
        return _said_bytes(literals.range_bytes(one, kind))
    if name in _ARRAY_OUT or name in _ARRAY_SEND:
        kind = _array_kind(first)
        if kind is None or text is None:
            return None
        items, lower = literals.array_read(text, kind)
        if name in _ARRAY_OUT:
            return _said_text(literals.array_text(items, lower))
        return _said_bytes(literals.array_bytes(items, lower, kind))
    if name.endswith("typmodin"):
        return (
            None if text is None else _said_number(literals.typmod_in(name[:-8], text), "integer")
        )
    if name in ("numerictypmodout", "intervaltypmodout"):
        number = _literal_number(first)
        if number is None:
            return None
        said = literals.numeric_typmod_out if name[0] == "n" else literals.interval_typmod_out
        return _said_text(said(number))
    if name in _FLOAT8_STATES and text is not None:
        answer = literals.float8_answer(name, [float(v) for v in text.strip("{}").split(",")])
        return (
            _null_of("double precision")
            if answer is None
            else exp.Anonymous(this="float8in", expressions=[exp.Literal.string(repr(answer))])
        )
    if text is None:
        return None
    return _fold_text(name, text, arguments)


def _fold_text(name: str, text: str, arguments: list[exp.Expr]) -> exp.Expr | None:
    if name in _INTEGER_RANGES and _OTHER_BASE.search(text):
        return _other_base(name, text)
    if name == "uuid_in":
        written = literals.uuid_text(text)
        if written is not None:
            return exp.Cast(this=_said_text(written), to=exp.DataType.build("uuid"))
    if name == "macaddr_in":
        written = literals.macaddr_text(text)
        return None if written is None else _said_text(written)
    if name in ("numeric_out", "numeric_send"):
        written = literals.numeric_text(text)
        if name == "numeric_out":
            return _said_text(written)
        return _said_bytes(literals.numeric_bytes(written))
    if name == "numeric_in":
        written = literals.numeric_text(text)
        typmod = _literal_number(arguments[2])
        if typmod is not None and typmod >= 0:
            written = _numeric_typmod(written, typmod)
        if written in ("NaN", "Infinity", "-Infinity") or len(written) > _LONGEST_DECIMAL:
            return None  # no engine's decimal holds it
        return exp.Cast(
            this=exp.Literal.string(written), to=exp.DataType.build("numeric", dialect="postgres")
        )
    if name in ("interval_in", "interval_out", "interval_send"):
        typmod = _literal_number(arguments[2]) if name == "interval_in" else -1
        if typmod is not None and typmod >= 0:
            return None
        fields = literals.interval_fields(text)
        if fields is None:
            raise literals.Refused("22007", f'invalid input syntax for type interval: "{text}"')
        if name == "interval_send":
            return _said_bytes(literals.interval_bytes(*fields))
        return _said_text(literals.interval_text(*fields))
    if name in ("tsvectorin", "tsvectorout", "tsvectorsend"):
        lexemes = literals.tsvector_lexemes(text)
        if lexemes is None:
            return None
        if name == "tsvectorsend":
            return _said_bytes(literals.tsvector_bytes(lexemes))
        return _said_text(literals.tsvector_text(lexemes))
    if name in ("jsonb_in", "jsonb_out", "jsonb_send"):
        try:
            written = jsonb_text(text)
        except ValueError:
            raise literals.Refused("22P02", "invalid input syntax for type json") from None
        return (
            _said_bytes(literals.jsonb_bytes(written))
            if name == "jsonb_send"
            else _said_text(written)
        )
    if name in ("bit_in", "bit_out", "bit_send", "varbit_in", "varbit_out", "varbit_send"):
        fixed = name.startswith("bit")
        typmod = _literal_number(arguments[2]) if name.endswith("_in") else -1
        if text[:1] in ("x", "X", "b", "B"):
            return None  # a prefixed text: another base's digits
        written = literals.bit_read(text, -1 if typmod is None else typmod, fixed)
        return (
            _said_bytes(literals.bit_bytes(written))
            if name.endswith("send")
            else _said_text(written)
        )
    if name in ("inet_send", "cidr_send"):
        written = literals.inet_bytes(text, name[0] == "c")
        return None if written is None else _said_bytes(written)
    return None


_RANGE_OUT = frozenset(
    {"range_out", "anyrange_out", "anycompatiblerange_out", "multirange_out", "anymultirange_out",
     "anycompatiblemultirange_out"}
)  # fmt: skip
_RANGE_SEND = frozenset({"range_send", "multirange_send"})
_ARRAY_OUT = frozenset({"array_out", "anyarray_out", "anycompatiblearray_out"})
_ARRAY_SEND = frozenset({"array_send", "anyarray_send", "anycompatiblearray_send"})
#: The final functions of a float8 state (pg_forms_aggregate.FINAL_FUNCTIONS): a literal state is
#: answered here, a column's by the form.
_FLOAT8_STATES = frozenset(
    f"float8_{n}" for n in (
        "avg", "var_pop", "var_samp", "stddev_pop", "stddev_samp", "corr", "covar_pop", "covar_samp",
        "regr_avgx", "regr_avgy", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy",
        "regr_syy",
    )
)  # fmt: skip
RESULTS.update(dict.fromkeys(_FLOAT8_STATES, "double precision"))


def _folded(name: str, arguments: list[exp.Expr]) -> exp.Expr | None:
    """``_fold_row``, with PostgreSQL's refusal of a literal as an expression that raises it."""
    try:
        return _fold_row(name, arguments)
    except literals.Refused as refusal:
        return _said_refusal(refusal)


def _record_text(arguments: list[exp.Expr]) -> exp.Expr | None:
    """record_out of a ROW of literals."""
    words: list[str | None] = []
    for value in arguments:
        node = value.unnest()
        if isinstance(node, exp.Null):
            words.append(None)
        elif isinstance(node, exp.Boolean):
            words.append("t" if node.this else "f")
        elif isinstance(node, exp.Literal):
            words.append(node.this)
        else:
            return None
    return _said_text(literals.record_text(words))


def _lowered(tree: exp.Expr, dialect: str | None) -> exp.Expr:
    """``tree`` with each call of these functions the call its row reads: a NULL of a strict function
    answers itself, whatever type no engine has it says, and a literal of a type is its input."""

    def rewrite(node: exp.Expr) -> exp.Expr:
        if dialect == "clickhouse" and isinstance(node, exp.Cast) and _type_said(node) == "time":
            return _read(node)  # ClickHouse 24 has no time: a time is its text, as time_in reads it
        target = node
        if (
            isinstance(node, exp.Dot)
            and isinstance(node.this, exp.Identifier)
            and node.this.name.lower() == "pg_catalog"
        ):
            target = node.expression
        if not isinstance(target, exp.Anonymous) or target.name.lower() not in RESULTS:
            return node
        name = target.name.lower()
        arguments = target.expressions
        in_select = isinstance(node.parent, exp.Select) and node.arg_key == "expressions"
        unset = name == "domain_in" and _is_null(arguments[1])
        if unset or (name not in _NOT_STRICT and any(_is_null(a) for a in arguments)):
            written = _null_of(RESULTS[name], dialect)
            return exp.alias_(written, name) if in_select else written
        if name == "int8_sum":
            return node
        row = arguments[0].unnest() if arguments else None
        if name in ("record_out", "record_send") and isinstance(row, exp.Anonymous):
            said = _record_text(row.expressions) if name == "record_out" else None
            if said is None:  # a row no engine's form of it reads
                target.set("expressions", [exp.Literal.string("")])
                return node
            return exp.alias_(said, name) if in_select else said
        folded = _folded(name, arguments)
        if folded is not None:
            return exp.alias_(folded, name) if in_select else folded
        if name.endswith("in"):
            target.set("expressions", [_bare(a) for a in arguments])
        else:
            target.set("expressions", [_bare(_read(a)) for a in arguments])
        return node

    return tree.transform(rewrite)


_ARRAY_OF_CSTRING = re.compile(r"::\s*cstring\s*\[\s*\]", re.IGNORECASE)
_BIT_VARYING = re.compile(r"::\s*bit varying\b", re.IGNORECASE)
_CALLED = re.compile(r"\b(" + "|".join(sorted(RESULTS)) + r")\"?\s*\(", re.IGNORECASE)


#: What these forms write in UTC, or read a session's zone into: only a session on UTC is answered.
ZONED = frozenset({"timestamptz_in", "timestamptz_out", "timestamptz_send"})
_ZONED_CALLED = re.compile(r"\b(" + "|".join(sorted(ZONED)) + r")\"?\s*\(", re.IGNORECASE)


def _spelled(sql: str) -> str:
    return _BIT_VARYING.sub("::varbit", _ARRAY_OF_CSTRING.sub("::_cstring", sql))


def zoned_call(sql: str) -> str | None:
    """The function of ``ZONED`` that ``sql`` calls. A text sqlglot cannot read is taken to call
    what it names: the cost of being wrong is a refusal, not a wrong answer."""
    named = _ZONED_CALLED.search(sql)
    if named is None:
        return None
    try:
        tree = sqlglot.parse_one(_spelled(sql), read="postgres")
    except SqlglotError:
        return named.group(1).lower()
    calls = (call.name.lower() for call in tree.find_all(exp.Anonymous))
    return next((name for name in calls if name in ZONED), None)


def readable(sql: str, dialect: str | None = None) -> str:
    """``sql`` with each call of these functions as its row reads it. sqlglot takes ``::cstring[]``
    for a subscript and ``::bit varying`` for a bit and an alias, and reads PostgreSQL's other
    spelling of each. A text it cannot parse is left, for the engine to say so. ``dialect`` is the
    engine it is for, where that changes a NULL's type."""
    if _CALLED.search(sql) is None:
        return sql
    try:
        tree = sqlglot.parse_one(_spelled(sql), read="postgres")
        return _lowered(tree, dialect).sql(dialect="postgres")
    except SqlglotError:
        return sql
