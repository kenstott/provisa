# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2f8a41-c7e3-4b95-a0d1-3e8b5f9c2a74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A bytea's functions on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES
BEFORE A REFUSAL; function support run 43490334080).

ClickHouse holds a bytea as a String, which is its bytes: length, substring, position and concat
read a String byte by byte, as PostgreSQL reads a bytea. Each was refused by name there
(provisa.transpiler.engine_refusals: the engine's string function was taken to read text); each is
exact once the value is read as bytes, which this row makes sure of before it answers:

- a value is a bytea the statement says (a cast, or a column of a VALUES list cast to bytea) whose
  literal ClickHouse reads as those bytes: hex input (\\x and its digits), or text with no
  backslash (its UTF-8 bytes). The escape form (\\\\, \\ooo) is written as its text there
  (transpile._bytea_literal), so a call of one keeps its refusal by name, as does a value whose
  type no statement says;
- an index outside the value raises PostgreSQL's SQLSTATE (2202E, 22011, 22003, 22023), in its
  place: the message is PostgreSQL's, less the index and range it names (ClickHouse's throwIf
  takes no message it computes);
- a pattern (like, notlike), an escape (like_escape) and a format (encode) are read as the
  statement writes them: a literal or a NULL. One a row computes is refused by name.

Every other dialect keeps what it had (function_macros' forms, and engine_refusals' refusals)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import base64
import string
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot.expressions as exp

from provisa.transpiler.pg_literals import values_cast
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "bytea on ClickHouse"

_CLICKHOUSE = "clickhouse"
#: A node this module wrote whose value is a bytea: a call of one reads it as one.
_HELD = "provisa_bytea"
_BYTES = "Nullable(String)"
_INT = "Nullable(Int32)"
_BOOL = "Nullable(Bool)"
#: A lambda's parameter no statement's column is named: the row's arguments are grafted in its body.
_BYTE = "provisa_byte"


# --- what the row reads ---------------------------------------------------------------------


def literal_bytes(text: str) -> bytes | None:
    """The bytes ClickHouse holds for a bytea literal of ``text``, where they are PostgreSQL's:
    hex input, or text with no backslash. None for the escape form, which is written as its text."""
    if text.startswith("\\x"):
        digits = text[2:]
        if len(digits) % 2 or any(c not in string.hexdigits for c in digits):
            return None
        return bytes.fromhex(digits)
    if "\\" in text:
        return None
    return text.encode()


def _is_bytea(node: exp.Expr) -> bool:
    said = trino.said(node)
    return said is not None and said.split("(")[0] == "bytea"


#: What a row reads of a value: a NULL, the bytes of a literal, or a value ClickHouse computes.
_NULL = None


@dataclass(frozen=True)
class _Value:
    """A bytea argument the row reads: its bytes where the statement writes them, else None."""

    written: bytes | None


def _cast_value(value: exp.Expr) -> _Value | None | bool:
    """A cast of a literal or a NULL to bytea: the literal's bytes, ``_NULL`` for a NULL, False for
    what ClickHouse would not read as its bytes."""
    inner = value.this.unnest() if isinstance(value, exp.Cast) else value
    if isinstance(inner, exp.Null):
        return _NULL
    if isinstance(inner, exp.Literal) and inner.is_string:
        read = literal_bytes(inner.this)
        return False if read is None else _Value(read)
    return False  # a cast of another value: text's cast to bytea reads its escapes


def _bytea(argument: exp.Expr) -> _Value | None | bool:
    """What the row reads of a bytea ``argument``: its value (a literal's bytes, where it is one),
    ``_NULL`` for a NULL, or False where it does not read it."""
    value = argument.unnest()
    if value.meta.get(_HELD):
        return _Value(None)
    if isinstance(value, exp.Column):
        cast = values_cast(value)
        if cast is None or not _is_bytea(cast):
            return False
        values = cast.find_ancestor(exp.Values)
        assert values is not None  # values_cast reads the cast from a row of a VALUES list
        index = [c.name for c in values.args["alias"].columns].index(value.name)
        for row in values.expressions:
            read = _cast_value(row.expressions[index].unnest())
            if read is False:
                return False
        return _Value(None)
    if not _is_bytea(value):
        return False
    return _cast_value(value)


def _integer(argument: exp.Expr) -> bool:
    """Whether ``argument`` is a value the row reads as an integer: not a text."""
    value = argument.unnest()
    if isinstance(value, exp.Literal):
        return not value.is_string
    said = trino.said(value)
    return said is None or said.split("(")[0] in ("smallint", "integer", "int", "bigint")


def _text(argument: exp.Expr) -> str | None | bool:
    """A text literal's value, ``_NULL`` for a NULL, False for a value the row does not read."""
    value = argument.unnest()
    inner = value.this.unnest() if isinstance(value, exp.Cast) else value
    if isinstance(inner, exp.Null):
        return _NULL
    if isinstance(inner, exp.Literal) and inner.is_string:
        return inner.this
    return False


def _non_negative(argument: exp.Expr) -> bool:
    """A NULL, or an integer literal that is not negative."""
    value = argument.unnest()
    inner = value.this.unnest() if isinstance(value, exp.Cast) else value
    if isinstance(inner, exp.Null):
        return True
    return isinstance(inner, exp.Literal) and inner.is_int and int(inner.this) >= 0


def _at_least_one(argument: exp.Expr) -> bool:
    value = argument.unnest()
    inner = value.this.unnest() if isinstance(value, exp.Cast) else value
    return isinstance(inner, exp.Literal) and inner.is_int and int(inner.this) >= 1


# --- the forms --------------------------------------------------------------------------------


def _raise(sqlstate: str, message: str, held: str) -> str:
    return f"CAST({ch_fail(sqlstate, message).format(condition='1')} AS {held})"


_INDEX = "index out of valid range"
_LENGTH = "negative substring length not allowed"
_RANGE = "integer out of range"
_BIT = "new bit must be 0 or 1"

_LENGTH_OF = f"CAST(length($a) AS {_INT})"
_BYTE_AT = "reinterpretAsUInt8(substring($a, {at}, 1))"


def _trimmed(side: str) -> str:
    """btrim, ltrim and rtrim: the value less each byte of the set at its start, its end or both."""
    a, b = "ifNull($a, '')", "ifNull($b, '')"
    split = f"splitByString('', {a})"
    kept = f"{_BYTE} -> position({b}, {_BYTE}) = 0"
    first = f"arrayFirstIndex({kept}, {split})"
    last = f"arrayFirstIndex({kept}, arrayReverse({split}))"
    body = {
        "both": f"if({first} = 0, '', substring({a}, {first}, length({a}) - {last} - {first} + 2))",
        "leading": f"if({first} = 0, '', substring({a}, {first}))",
        "trailing": f"if({last} = 0, '', substring({a}, 1, length({a}) - {last} + 1))",
    }[side]
    return f"if($a IS NULL OR $b IS NULL, CAST(NULL AS {_BYTES}), {body})"


def _outside(count: str) -> str:
    return f"$n < 0 OR $n >= {count}"


_GET_BIT = (
    f"CASE WHEN {_outside('length($a) * 8')} THEN {_raise('2202E', _INDEX, _INT)} "
    f"ELSE CAST(bitAnd(bitShiftRight({_BYTE_AT.format(at='intDiv($n, 8) + 1')}, bitAnd($n, 7)), 1) "
    f"AS {_INT}) END"
)
_GET_BYTE = (
    f"CASE WHEN {_outside('length($a)')} THEN {_raise('2202E', _INDEX, _INT)} "
    f"ELSE CAST({_BYTE_AT.format(at='$n + 1')} AS {_INT}) END"
)
_BIT_OF = "bitShiftLeft(toUInt8(1), bitAnd($n, 7))"
_SET_BIT = (
    f"CASE WHEN {_outside('length($a) * 8')} THEN {_raise('2202E', _INDEX, _BYTES)} "
    f"WHEN $v <> 0 AND $v <> 1 THEN {_raise('22023', _BIT, _BYTES)} "
    "ELSE concat(substring($a, 1, intDiv($n, 8)), "
    f"char(if($v = 1, bitOr({_BYTE_AT.format(at='intDiv($n, 8) + 1')}, {_BIT_OF}), "
    f"bitAnd({_BYTE_AT.format(at='intDiv($n, 8) + 1')}, bitNot({_BIT_OF})))), "
    "substring($a, intDiv($n, 8) + 2)) END"
)
_SET_BYTE = (
    f"CASE WHEN {_outside('length($a)')} THEN {_raise('2202E', _INDEX, _BYTES)} "
    "ELSE concat(substring($a, 1, $n), char(bitAnd($v, 255)), substring($a, $n + 2)) END"
)


def _overlay(length: str) -> str:
    """overlay: the value to $s - 1, the other, and the value from $s + its length (byteaoverlay)."""
    end = f"$s + {length}"
    return (
        f"CASE WHEN {end} > 2147483647 OR {end} < -2147483648 THEN {_raise('22003', _RANGE, _BYTES)} "
        f"WHEN $s < 1 THEN {_raise('22011', _LENGTH, _BYTES)} "
        f"ELSE concat(substring($a, 1, $s - 1), $b, substring($a, greatest({end}, 1))) END"
    )


#: bytea_substring: from max($s, 1), to $s + $l (none past the end, nothing before 1).
_SUBSTRING = "substring($a, greatest($s, 1))"
_SUBSTRING_FOR = (
    f"CASE WHEN $l < 0 THEN {_raise('22011', _LENGTH, _BYTES)} "
    "ELSE substring($a, greatest($s, 1), greatest($s + $l - greatest($s, 1), 0)) END"
)


def like_regex(pattern: bytes) -> str | None:
    """PostgreSQL's LIKE of a bytea pattern (like_match.c, single-byte, backslash its escape) as
    an RE2 expression over the value's hex digits, which each token reads a whole byte of: % any
    bytes, _ one, any other byte itself. None for a pattern that ends in its escape, which
    PostgreSQL raises at only where matching reaches it."""
    out = []
    escaped = False
    for byte in pattern:
        if escaped:
            out.append(f"{byte:02X}")
            escaped = False
        elif byte == 0x5C:
            escaped = True
        elif byte == 0x25:
            if not out or out[-1] != "(?:..)*":
                out.append("(?:..)*")
        elif byte == 0x5F:
            out.append("..")
        else:
            out.append(f"{byte:02X}")
    if escaped:
        return None
    return "^" + "".join(out) + "$"


def like_escape(pattern: bytes, escape: bytes) -> bytes | None:
    """like_escape(bytea, bytea) as like_match.c's do_like_escape writes it, single-byte; None for
    an escape of more than one byte, which PostgreSQL raises (22025)."""
    if not escape:
        return pattern.replace(b"\\", b"\\\\")
    if len(escape) != 1:
        return None
    if escape == b"\\":
        return pattern
    out = bytearray()
    after = False
    for byte in pattern:
        if byte == escape[0] and not after:
            out.append(0x5C)
            after = True
        elif byte == 0x5C:
            out.append(0x5C)
            if not after:
                out.append(0x5C)
            after = False
        else:
            out.append(byte)
            after = False
    return bytes(out)


def base64_lines(data: bytes) -> str:
    """encode(bytea, 'base64'): its base64, a newline after each 76 characters."""
    text = base64.b64encode(data).decode()
    return "\n".join(text[i : i + 76] for i in range(0, len(text), 76))


#: encode's ClickHouse form of each format PostgreSQL names (pg_find_encoding: case-insensitive).
_ENCODINGS = {
    "hex": "lower(hex($a))",
    "base64": (
        "arrayStringConcat(arrayMap(provisa_at -> substring(base64Encode($a), provisa_at, 76), "
        "range(1, length(base64Encode($a)) + 1, 76)), '\\n')"
    ),
    "escape": (
        "arrayStringConcat(arrayMap(provisa_at -> multiIf("
        "provisa_at = 0 OR provisa_at >= 128, concat('\\\\', toString(intDiv(provisa_at, 64)), "
        "toString(intDiv(provisa_at, 8) % 8), toString(provisa_at % 8)), "
        "provisa_at = 92, '\\\\\\\\', char(provisa_at)), "
        "arrayMap(provisa_byte -> reinterpretAsUInt8(provisa_byte), splitByString('', $a))), '')"
    ),
}


def _sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


# --- the rows ---------------------------------------------------------------------------------


#: A row's reading of a call: its arguments in PostgreSQL's order, or None where it is not one.
_Arguments = Callable[[exp.Expr], list[exp.Expr] | None]


def _named(*names: str, count: tuple[int, ...]) -> _Arguments:
    """The arguments of a call written by one of ``names`` (a plain call, as pg_catalog's is)."""

    def arguments(node: exp.Expr) -> list[exp.Expr] | None:
        if isinstance(node, exp.Anonymous) and node.name.lower() in names:
            args = list(node.expressions)
            return args if len(args) in count else None
        return None

    return arguments


def _typed(kind: type[exp.Expr], *keys: str, also: _Arguments | None = None) -> _Arguments:
    """The arguments of sqlglot's ``kind`` node, by its argument names (absent ones dropped)."""

    def arguments(node: exp.Expr) -> list[exp.Expr] | None:
        if isinstance(node, kind):
            return [node.args[k] for k in keys if node.args.get(k) is not None]
        return None if also is None else also(node)

    return arguments


@dataclass(frozen=True)
class _Call:
    """What a row reads of a call: its arguments, each one's kind (b: a bytea, i: an integer,
    t: a text literal, B: a bytea literal), and its form of them, by their count."""

    arguments: _Arguments
    kinds: str
    forms: Callable[[list[exp.Expr], list[object]], str | None]
    returns_bytes: bool = False
    #: PostgreSQL's operator of a LIKE row (~~, !~~): sqlglot's LIKE node is read by
    #: :func:`_operator_reads`, as PostgreSQL resolves an operator of a bytea.
    operator: str | None = None


def _read(kinds: str, args: list[exp.Expr]) -> list[object] | None:
    reads: list[object] = []
    for kind, arg in zip(kinds, args):
        if kind == "b" or kind == "B":
            read = _bytea(arg)
            if read is False or (kind == "B" and isinstance(read, _Value) and read.written is None):
                return None
        elif kind == "i":
            if not _integer(arg):
                return None
            read = arg
        else:
            read = _text(arg)
            if read is False:
                return None
        reads.append(read)
    return reads


def _said_bytea(argument: exp.Expr) -> bool:
    return trino.said(argument.unnest()) == "bytea"


def _type_name(argument: exp.Expr) -> str:
    """PostgreSQL's name of an operand's type in an operator's error: unknown for an untyped
    string."""
    value = argument.unnest()
    if isinstance(value, exp.Literal) and value.is_string:
        return "unknown"
    said = trino.said(value)
    assert said is not None  # a type no statement says is read as a bytea, never named here
    return said.split("(")[0]


def _operand(argument: exp.Expr) -> _Value | None | bool:
    """An operand of a LIKE whose other operand is a bytea: PostgreSQL reads an untyped string as
    a bytea's input, and a value of a type no statement says as a bytea (the operator of a bytea
    and any other type does not exist). False for a value of another type the statement says."""
    read = _bytea(argument)
    if read is not False:
        return read
    value = argument.unnest()
    if isinstance(value, exp.Null):
        return _NULL
    if isinstance(value, exp.Literal) and value.is_string:
        written = literal_bytes(value.this)
        return _Value(written) if written is not None else False
    said = trino.said(value)
    if said is None or said.split("(")[0] == "bytea":
        return _Value(None)  # ClickHouse's String of it is its bytes, whatever computed it
    return False


def _operator_reads(name: str, operator: str, args: list[exp.Expr]) -> list[object] | None:
    """A LIKE operator's value and pattern where either is a bytea the statement says, or a value
    a row here wrote: the value as whatever computes it, the pattern as the statement writes it.
    None for a LIKE of no bytea (text's); PostgreSQL's 42883 for a bytea beside a value of another
    type; a refusal by name of a pattern a row computes, or of the escape form."""
    value, pattern = args
    if not any(_said_bytea(a) or _bytea(a) is not False for a in args):
        return None
    reads: list[object] = [_operand(value), _operand(pattern)]
    for argument, read in zip(args, reads):
        if read is not False:
            continue
        inner = argument.unnest()
        if isinstance(inner, exp.Literal) and inner.is_string:
            raise Refused(_by_name(name))  # the escape form, which ClickHouse holds as its text
        left, right = _type_name(value), _type_name(pattern)
        raise Refused(Refusal("42883", f"operator does not exist: {left} {operator} {right}"))
    written = reads[1]
    if isinstance(written, _Value) and written.written is None:
        raise Refused(_by_name(name))  # a pattern a row computes
    return reads


def _by_name(name: str) -> Refusal:
    return Refusal("0A000", f"{name}() has no form on {_CLICKHOUSE}")


def _plain(argument: exp.Expr) -> bool:
    """A column, a NULL, or a cast of a literal: what a rule of no pattern reads, as the walk
    writes no argument of what such a rule writes."""
    value = argument.unnest()
    written = value.this.unnest() if isinstance(value, exp.Cast) else value
    return isinstance(written, (exp.Column, exp.Null, exp.Literal))


def _rewrite(
    name: str, call: _Call, *, plain: bool = False, mentions: str | None = None
) -> Rewrite:
    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        if isinstance(node.parent, exp.Escape):
            return None  # LIKE ... ESCAPE: its own escape, read by no row here
        args = call.arguments(node)
        if args is None or (plain and not all(_plain(a) for a in args)):
            return None
        operator = call.operator if isinstance(node, exp.Like) else None
        if operator is not None:
            reads = _operator_reads(name, operator, args)
        else:
            reads = _read(call.kinds, args)
        if reads is None:
            return None
        form = call.forms(args, reads)
        if form is None:
            if operator is not None:
                raise Refused(_by_name(name))  # a pattern that ends in its escape
            return None
        holes = dict(zip("abcd", args))
        # ClickHouse's hex of a form is the bytes' digits: transpile.clickhouse_semantics writes
        # sqlglot's Hex as to_hex's, its leading zeros dropped.
        tree = replacement(form, dialect, _holes_pattern(len(args))).copy().transform(_own_hex)
        written = graft(tree, holes)
        if call.returns_bytes:
            written.meta[_HELD] = True
        return written

    rewrite.__doc__ = f"{name} of a bytea held as ClickHouse's String, its bytes."
    rewrite.__qualname__ = rewrite.__name__ = f"{name}_of_bytes"
    return Rewrite(rewrite, mentions=mentions)


def _own_hex(node: exp.Expr) -> exp.Expr:
    if isinstance(node, exp.LowerHex):
        hexed = exp.Anonymous(this="hex", expressions=[node.this])
        return exp.Anonymous(this="lower", expressions=[hexed])
    if isinstance(node, exp.Hex):
        return exp.Anonymous(this="hex", expressions=[node.this])
    return node


def _holes_pattern(count: int) -> str:
    """The pattern whose holes a form names: $a, $b, ... by the arguments' order."""
    return "f(" + ", ".join("$" + h for h in "abcd"[:count]) + ")"


def _renamed(form: str, names: str) -> str:
    """A form written of $a, $n, $v, $s, $l, $b with each the hole of its argument's place."""
    for i, name in enumerate(names):
        form = form.replace(f"${name}", f"${'abcd'[i]}")
    return form


def _fixed(form: str, names: str = "ab") -> Callable[[list[exp.Expr], list[object]], str | None]:
    return lambda args, reads: _renamed(form, names)


_STATED = (
    "ClickHouse's String is the bytea's bytes. A value is read where the statement says it is a "
    "bytea (a cast, or a column of a VALUES list cast to one) and ClickHouse holds its literal as "
    "those bytes: hex input, or text with no backslash. The escape form, and a value whose type no "
    "statement says, are refused by name."
)
_INDEX_STATED = (
    " An index outside the value raises PostgreSQL's 2202E (index out of valid range); its "
    "message does not name the index and the range, as PostgreSQL's does."
)
_OPERATOR_STATED = (
    " Its operator (LIKE, NOT LIKE, ~~, !~~) beside a bytea: an untyped string is a bytea's "
    "input, a value any expression computes is read as its bytes, and a value of another type the "
    "statement says raises 42883 (operator does not exist). Its ESCAPE is read where the "
    "statement is transpiled (provisa.transpiler.like_patterns): the pattern is like_escape's "
    "bytes. bytea has no ILIKE: one raises 42883."
)
_LITERAL_STATED = " Its {} is read as the statement writes it, a literal or a NULL; one a row computes is refused by name."


def _like(negated: bool) -> Callable[[list[exp.Expr], list[object]], str | None]:
    def form(args: list[exp.Expr], reads: list[object]) -> str | None:
        pattern = reads[1]
        if pattern is _NULL:
            return f"CAST(NULL AS {_BOOL})"
        assert isinstance(pattern, _Value) and pattern.written is not None
        regex = like_regex(pattern.written)
        if regex is None:
            return None
        matched = f"match(hex($a), {_sql_string(regex)})"
        return f"CAST({'NOT ' if negated else ''}{matched} AS {_BOOL})"

    return form


def _like_escape(args: list[exp.Expr], reads: list[object]) -> str | None:
    pattern, escape = reads
    if pattern is _NULL or escape is _NULL:
        return f"CAST(NULL AS {_BYTES})"
    assert isinstance(pattern, _Value) and isinstance(escape, _Value)
    assert pattern.written is not None and escape.written is not None
    written = like_escape(pattern.written, escape.written)
    if written is None:
        return _raise("22025", "invalid escape string", _BYTES)
    return f"unhex('{written.hex().upper()}')"


def _encode(args: list[exp.Expr], reads: list[object]) -> str | None:
    named = reads[1]
    if named is _NULL:
        return "CAST(NULL AS Nullable(String))"
    assert isinstance(named, str)
    form = _ENCODINGS.get(named.lower())
    if form is None:
        return _raise("22023", f'unrecognized encoding: "{named}"', "Nullable(String)")
    return form


def _substring(args: list[exp.Expr], reads: list[object]) -> str | None:
    if len(args) == 2:
        return _renamed(_SUBSTRING, "as")
    return _renamed(_SUBSTRING_FOR, "asl")


def _edged_substring(args: list[exp.Expr], reads: list[object]) -> str | None:
    """sqlglot's Substring: function_macros writes it in PostgreSQL's reading of its position
    before any row, where its start is no constant of at least 1 (_past_the_edge), and that form
    answers a negative length with nothing. This row answers only where that form is exact: no
    length, a NULL or one not negative, or a start the row's own form writes."""
    if len(args) == 3 and not (_non_negative(args[2]) or _at_least_one(args[1])):
        return None
    return _substring(args, reads)


def _trim(side: str) -> Callable[[list[exp.Expr], list[object]], str | None]:
    return lambda args, reads: _trimmed(side)


_TRIMS = {None: "both", "BOTH": "both", "LEADING": "leading", "TRAILING": "trailing"}


def _trim_side(node: exp.Expr) -> str | None:
    position = node.args.get("position")
    key = (
        None
        if position is None
        else str(position.this if isinstance(position, exp.Var) else position).upper()
    )
    return _TRIMS.get(key)


def _trim_arguments(side: str, name: str) -> _Arguments:
    named = _named(name, count=(2,))

    def arguments(node: exp.Expr) -> list[exp.Expr] | None:
        if isinstance(node, exp.Trim):
            if _trim_side(node) != side or node.args.get("expression") is None:
                return None
            return [node.this, node.args["expression"]]
        return named(node)

    return arguments


def _feature(
    feature: str,
    summary: str,
    patterns: tuple[str, ...],
    call: _Call,
    stated: str,
    *,
    plain_call: bool = False,
    mentions: str | None = None,
) -> Feature:
    """A row of ClickHouse's form, every other dialect keeping what it had. ``plain_call``: a call
    by its name (pg_catalog's) is a node no pattern reaches (sqlglot's LIKE and POSITION are no
    function of that name), so a rule of no pattern reads it, of plain arguments only.
    ``mentions``: what a statement that names no function holds where that rule may rewrite it."""
    name = feature.split("(")[0]
    kept = {d: trino.KEPT for d in DIALECTS if d not in NATIVE}
    forms = {**kept, _CLICKHOUSE: _rewrite(name, call)}
    rules = tuple(Rule(p, forms, unlisted_keep=True) for p in patterns)
    if plain_call:
        plain = _rewrite(name, call, plain=True, mentions=mentions)
        rules += (Rule(None, {**kept, _CLICKHOUSE: plain}, unlisted_keep=True),)
    return Feature(
        feature,
        FAMILY,
        summary,
        read=Side(rules=rules, limitations={_CLICKHOUSE: (Limitation(_STATED + stated),)}),
    )


#: A statement that says a bytea and a LIKE (or ~~): its operator names no function, and
#: ClickHouse's own LIKE reads a String as UTF-8, where '_' matches a character of several bytes.
_LIKE_OF_BYTES = r"^(?=[\s\S]*\bbytea\b)[\s\S]*(?:\blike\b|~~)"


def _like_arguments(name: str) -> _Arguments:
    """The arguments of a call of ``name`` (like or notlike), or of the operator it is: sqlglot
    reads NOT LIKE (and !~~) as LIKE marked negated, which is notlike's."""
    named = _named(name, count=(2,))
    negated = name == "notlike"

    def arguments(node: exp.Expr) -> list[exp.Expr] | None:
        if isinstance(node, exp.Like):
            if bool(node.args.get("negate")) is not negated:
                return None
            return [node.this, node.expression]
        return named(node)

    return arguments


FEATURES: tuple[Feature, ...] = (
    _feature(
        "octet_length(bytea)", "A bytea's number of bytes.", ("octet_length($a)",),
        _Call(_named("octet_length", count=(1,)), "b", _fixed(_LENGTH_OF)), "",
    ),
    _feature(
        "length(bytea)", "A bytea's number of bytes.", ("length($a)",),
        _Call(_typed(exp.Length, "this", also=_named("length", count=(1,))), "b",
              _fixed(_LENGTH_OF)), "",
    ),
    _feature(
        "bit_length(bytea)", "A bytea's number of bits.", ("bit_length($a)",),
        _Call(_typed(exp.BitLength, "this", also=_named("bit_length", count=(1,))), "b",
              _fixed(f"CAST(length($a) * 8 AS {_INT})")), "",
    ),
    _feature(
        "bit_count(bytea)", "The number of a bytea's bits that are set.", ("bit_count($a)",),
        _Call(_named("bit_count", count=(1,)), "b",
              _fixed("CAST(bitCount($a) AS Nullable(Int64))")), "",
    ),
    _feature(
        "byteacat(bytea, bytea)", "Two byteas, one after the other.", ("byteacat($a, $b)",),
        _Call(_named("byteacat", count=(2,)), "bb", _fixed("concat($a, $b)"), True), "",
    ),
    _feature(
        "btrim(bytea, bytea)", "A bytea less the bytes of a set at its start and its end.",
        ("btrim($a, $b)", "trim(both $b from $a)"),
        _Call(_trim_arguments("both", "btrim"), "bb", _trim("both"), True), "",
    ),
    _feature(
        "ltrim(bytea, bytea)", "A bytea less the bytes of a set at its start.", ("ltrim($a, $b)",),
        _Call(_trim_arguments("leading", "ltrim"), "bb", _trim("leading"), True), "",
    ),
    _feature(
        "rtrim(bytea, bytea)", "A bytea less the bytes of a set at its end.", ("rtrim($a, $b)",),
        _Call(_trim_arguments("trailing", "rtrim"), "bb", _trim("trailing"), True), "",
    ),
    _feature(
        "get_bit(bytea, bigint)", "A bit of a bytea, the first the low bit of its first byte.",
        ("get_bit($a, $n)",),
        _Call(_typed(exp.Getbit, "this", "expression", also=_named("get_bit", count=(2,))), "bi",
              _fixed(_GET_BIT, "an")), _INDEX_STATED,
    ),
    _feature(
        "set_bit(bytea, bigint, integer)", "A bytea with one of its bits set to 0 or 1.",
        ("set_bit($a, $n, $v)",),
        _Call(_named("set_bit", count=(3,)), "bii", _fixed(_SET_BIT, "anv"), True),
        _INDEX_STATED + " A new bit other than 0 or 1 raises 22023 (new bit must be 0 or 1).",
    ),
    _feature(
        "get_byte(bytea, integer)", "A byte of a bytea.", ("get_byte($a, $n)",),
        _Call(_named("get_byte", count=(2,)), "bi", _fixed(_GET_BYTE, "an")), _INDEX_STATED,
    ),
    _feature(
        "set_byte(bytea, integer, integer)", "A bytea with one of its bytes replaced.",
        ("set_byte($a, $n, $v)",),
        _Call(_named("set_byte", count=(3,)), "bii", _fixed(_SET_BYTE, "anv"), True),
        _INDEX_STATED,
    ),
    _feature(
        "like(bytea, bytea)", "Whether a bytea matches a LIKE pattern.",
        ("$a LIKE $b",),
        _Call(_like_arguments("like"), "bB", _like(False), operator="~~"),
        _LITERAL_STATED.format("pattern") + " A pattern that ends in its escape is refused by name."
        + _OPERATOR_STATED,
        plain_call=True, mentions=_LIKE_OF_BYTES,
    ),
    _feature(
        "bytealike(bytea, bytea)", "Whether a bytea matches a LIKE pattern.",
        ("bytealike($a, $b)",),
        _Call(_named("bytealike", count=(2,)), "bB", _like(False)),
        _LITERAL_STATED.format("pattern") + " A pattern that ends in its escape is refused by name.",
    ),
    _feature(
        "notlike(bytea, bytea)", "Whether a bytea does not match a LIKE pattern.",
        ("notlike($a, $b)", "$a NOT LIKE $b"),
        _Call(_like_arguments("notlike"), "bB", _like(True), operator="!~~"),
        _LITERAL_STATED.format("pattern") + " A pattern that ends in its escape is refused by name."
        + _OPERATOR_STATED,
        plain_call=True, mentions=_LIKE_OF_BYTES,
    ),
    _feature(
        "byteanlike(bytea, bytea)", "Whether a bytea does not match a LIKE pattern.",
        ("byteanlike($a, $b)",),
        _Call(_named("byteanlike", count=(2,)), "bB", _like(True)),
        _LITERAL_STATED.format("pattern") + " A pattern that ends in its escape is refused by name.",
    ),
    _feature(
        "like_escape(bytea, bytea)", "A LIKE pattern with its escape byte written as a backslash.",
        ("like_escape($a, $b)",),
        _Call(_named("like_escape", count=(2,)), "BB", _like_escape, True),
        _LITERAL_STATED.format("pattern and escape")
        + " An escape of more than one byte raises 22025 (invalid escape string).",
    ),
    _feature(
        "overlay(bytea, bytea, integer)", "A bytea with a part of it replaced by another.",
        ("overlay($a, $b, $s)", "overlay($a, $b, $s, $l)"),
        _Call(_typed(exp.Overlay, "this", "expression", "from_", "for_",
                     also=_named("overlay", count=(3, 4))), "bbii",
              lambda args, reads: _renamed(_overlay("length($b)" if len(args) == 3 else "$l"), "absl"),
              True),
        " A start below 1 raises 22011 (negative substring length not allowed), and a start and "
        "length past an integer 22003 (integer out of range).",
    ),
    _feature(
        "position(bytea, bytea)", "Where a bytea first holds another, from 1; 0 where it does not.",
        ("position($b in $a)",),
        _Call(_typed(exp.StrPosition, "this", "substr", also=_named("position", count=(2,))), "bb",
              _fixed(f"CAST(position($a, $b) AS {_INT})")), "", plain_call=True,
    ),
    _feature(
        "substr(bytea, integer)", "A part of a bytea, from a byte and for a number of bytes.",
        ("substr($a, $s)", "substr($a, $s, $l)"),
        _Call(_typed(exp.Substring, "this", "start", "length",
                     also=_named("substr", "substring", count=(2, 3))), "bii", _edged_substring,
              True),
        " A negative length raises 22011 (negative substring length not allowed). A length a row "
        "computes is read only beside a start of at least 1 the statement writes; otherwise the "
        "call is refused by name.",
    ),
    _feature(
        "encode(bytea, text)", "A bytea's text in hex, base64 or escape form.",
        ("encode($a, $f)",),
        _Call(_typed(exp.Encode, "this", "charset", also=_named("encode", count=(2,))), "bt",
              _encode),
        _LITERAL_STATED.format("format")
        + " base64 breaks its text into lines of 76 characters, as PostgreSQL's does.",
    ),
)  # fmt: skip
