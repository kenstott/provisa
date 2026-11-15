# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3c5e17-2f4b-4d86-8c01-b7e6d2a4f953
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Transaction ids, snapshots and an integer's digits on ClickHouse (REQ-2236; REQ-2167, AN
ANSWER WITHIN A STATED BOUND COMES BEFORE A REFUSAL; function support run 43469834291).

Each had a form on DuckDB alone (provisa.transpiler.function_macros), and was refused by name on
ClickHouse; each is exact there:

- xid8eq and its kin, and xid8send: ClickHouse holds an xid8 as a UInt64, PostgreSQL's own width
  and order. xid8send writes its eight bytes, most significant first, as pq_sendint64 does.
- pg_snapshot_xmin, _xmax, _xip, _send, pg_visible_in_snapshot, and their txid_ aliases: ClickHouse
  holds a snapshot as its text. Each form reads it as xid8funcs.c's parse_snapshot does: xmin,
  xmax and the in-progress ids, each a UInt64; xmin and xmax not 0 and xmax not before xmin; the
  ids in order, a duplicate (and a 0 before any other) read once. Text that parse would refuse
  raises PostgreSQL's 22P02 (invalid input syntax for type pg_snapshot) beside the answer, where
  PostgreSQL refused it as it was stored. txid_snapshot_xmin, _xmax and _xip answer a bigint: the
  id's 64 bits as a signed number, as PostgreSQL's int8 reads them; txid_visible_in_snapshot reads
  its bigint as those 64 bits.
- to_hex, to_oct and to_bin of an integer: the digits of its 32 bits (64 of a bigint), a negative
  value's two's complement, as PostgreSQL's convert_to_base writes them. A value ClickHouse holds as
  an Int64 is a bigint's; any other integer, an integer's (a smallint is promoted to one)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Form, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "transaction ids, snapshots and integer digits"

_CLICKHOUSE = "clickhouse"


def _feature(
    function: str, pattern: str | None, form: Form, summary: str, limitation: str
) -> Feature:
    """``function``'s ClickHouse form; every other dialect keeps what it had."""
    forms = {**{d: trino.KEPT for d in DIALECTS if d not in NATIVE}, _CLICKHOUSE: form}
    return Feature(
        function,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(pattern, forms),),
            limitations={_CLICKHOUSE: (Limitation(limitation),)},
        ),
    )


# --- xid8 ---------------------------------------------------------------------------------------

_XID8_HELD = "Exact: ClickHouse holds an xid8 as a UInt64, PostgreSQL's own width and order."
_XID8_ORDERS = {
    "xid8eq": ("=", "Two xid8 values equal."),
    "xid8ne": ("<>", "Two xid8 values not equal."),
    "xid8lt": ("<", "One xid8 before another."),
    "xid8le": ("<=", "One xid8 before or equal to another."),
    "xid8gt": (">", "One xid8 after another."),
    "xid8ge": (">=", "One xid8 after or equal to another."),
}
_NO_BYTES = "CAST(NULL AS Nullable(String))"


def _bytes(value: str, width: int) -> str:
    """``value``'s ``width`` bytes, most significant first, as pq_sendint writes them."""
    return f"leftPad(hex({value}), {2 * width}, '0')"


_XID8_SEND = (
    f"CASE WHEN $v IS NULL THEN {_NO_BYTES} ELSE unhex({_bytes('toUInt64($v)', 8)}) END"
)  # fmt: skip

# --- snapshots ----------------------------------------------------------------------------------

_SNAPSHOT_TEXT = "^[0-9]+:[0-9]+:([0-9]+(,[0-9]+)*)?$"


def _ids(snapshot: str) -> str:
    """The snapshot's in-progress ids as written, each a UInt64 (NULL past its range)."""
    listed = f"splitByChar(',', splitByChar(':', {snapshot})[3])"
    return f"arrayMap(_x -> toUInt64OrNull(_x), arrayFilter(_x -> _x != '', {listed}))"


def _part(snapshot: str, which: int) -> str:
    return f"toUInt64OrNull(splitByChar(':', {snapshot})[{which}])"


def _invalid(snapshot: str) -> str:
    """Where parse_snapshot refuses the text: not its shape, an id past a UInt64, xmin or xmax 0,
    xmax before xmin, or the ids out of order."""
    ids = _ids(snapshot)
    xmin, xmax = _part(snapshot, 1), _part(snapshot, 2)
    ordered = f"arrayAll((_a, _b) -> _a <= _b, arrayPopBack({ids}), arrayPopFront({ids}))"
    return (
        f"NOT match({snapshot}, '{_SNAPSHOT_TEXT}') OR {xmin} IS NULL OR {xmax} IS NULL "
        f"OR arrayExists(_x -> _x IS NULL, {ids}) OR {xmin} = 0 OR {xmax} = 0 "
        f"OR {xmax} < {xmin} OR NOT {ordered}"
    )


def _checked(answer: str, nothing: str) -> str:
    """``answer`` of the snapshot ``$s``, or PostgreSQL's refusal of text its input refuses."""
    raised = ch_fail("22P02", "invalid input syntax for type pg_snapshot").format(
        condition=_invalid("$s")
    )
    return f"CASE WHEN $s IS NULL THEN {nothing} ELSE if({raised} = 0, {answer}, {nothing}) END"


def _held(snapshot: str) -> str:
    """The ids parse_snapshot keeps: each once, a 0 (before any other) not at all."""
    return f"arrayFilter(_x -> _x != 0, arrayDistinct(arrayMap(_x -> assumeNotNull(_x), {_ids(snapshot)})))"


def _bound_of(which: int, signed: bool) -> str:
    value = f"assumeNotNull({_part('$s', which)})"
    if signed:
        return _checked(f"reinterpretAsInt64({value})", "CAST(NULL AS Nullable(Int64))")
    return _checked(value, "CAST(NULL AS Nullable(UInt64))")


def _ids_of(signed: bool) -> str:
    """The ids as rows: no row of a NULL snapshot (arrayJoin of none)."""
    each = "arrayMap(_x -> reinterpretAsInt64(_x), {})" if signed else "{}"
    held = each.format(_held("$s"))
    empty = "CAST([] AS Array(Int64))" if signed else "CAST([] AS Array(UInt64))"
    return f"arrayJoin({_checked(held, empty)})"


def _send() -> str:
    held = _held("$s")
    xmin, xmax = (f"assumeNotNull({_part('$s', i)})" for i in (1, 2))
    each = f"arrayStringConcat(arrayMap(_x -> {_bytes('_x', 8)}, {held}))"
    written = (
        f"unhex(concat({_bytes(f'toUInt32(length({held}))', 4)}, "
        f"{_bytes(xmin, 8)}, {_bytes(xmax, 8)}, {each}))"
    )
    return _checked(written, _NO_BYTES)


def _visible(signed: bool) -> str:
    """is_visible_fxid: before xmin, visible; at or past xmax, not; else, not where in progress.
    A bigint is read as its 64 bits (a negative one past every xmax)."""
    value = "toUInt64($x)" if signed else "$x"
    xmin, xmax = (f"assumeNotNull({_part('$s', i)})" for i in (1, 2))
    answer = (
        f"multiIf({value} < {xmin}, true, {value} >= {xmax}, false, "
        f"NOT has({_held('$s')}, {value}))"
    )
    nothing = "CAST(NULL AS Nullable(Bool))"
    return f"CASE WHEN $x IS NULL THEN {nothing} ELSE {_checked(answer, nothing)} END"


_SNAPSHOT_READ = (
    "Exact: ClickHouse holds a snapshot as its text, read as PostgreSQL's input reads it (each id "
    "a 64-bit number, the in-progress ids in order, each once). Text a column holds that "
    "PostgreSQL's input would have refused, and an id past 2^64 - 1, raises 22P02 (invalid input "
    "syntax for type pg_snapshot) when the call reads it, without the text PostgreSQL's message "
    "quotes."
)

# --- an integer's digits ------------------------------------------------------------------------

#: An integer's bits as convert_to_base reads them: a bigint's 64 (ClickHouse's Int64), any other
#: integer's 32, promoted as PostgreSQL promotes a smallint. A non-negative value is its own.
_BITS = (
    "if($v >= 0 OR toTypeName($v) IN ('Int64', 'Nullable(Int64)'), toUInt64($v), "
    "toUInt64(toUInt32($v)))"
)


def _digits(written: str) -> str:
    """``written`` (digits of the bits ``_u``) without its leading zeros; '0' of zero."""
    return (
        f"CASE WHEN $v IS NULL THEN CAST(NULL AS Nullable(String)) ELSE "
        f"arrayElement(arrayMap(_u -> if(_u = 0, '0', replaceRegexpOne({written}, '^0+', '')), "
        f"[{_BITS}]), 1) END"
    )


_OCTAL = (
    "arrayStringConcat(arrayMap(_i -> toString(bitAnd(bitShiftRight(_u, 3 * _i), 7)), "
    "arrayReverse(range(22))))"
)
_TO_BASE = {
    "to_hex": ("lower(hex(_u))", "An integer's hexadecimal digits."),
    "to_oct": (_OCTAL, "An integer's octal digits."),
    "to_bin": ("bin(_u)", "An integer's binary digits."),
}


def _of_digits(function: str, written: str) -> Rewrite:
    """``function`` of one integer, by its PostgreSQL name: to_hex is sqlglot's Hex, which a
    pattern would read as ClickHouse's own hex, so the call is read by the name it was written by
    (pg_catalog's, or quoted)."""
    form = _digits(written)

    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        if not isinstance(node, exp.Anonymous) or node.name.lower() != function:
            return None
        if len(node.expressions) != 1:
            return None
        return graft(replacement(form, dialect, "f($v)"), {"v": node.expressions[0]})

    rewrite.__doc__ = f"{function} of an integer: the digits of its bits."
    rewrite.__qualname__ = rewrite.__name__ = f"{function}_of_an_integer"
    return Rewrite(rewrite)


_BASE_READ = (
    "Exact: the digits of an integer's 32 bits, or a bigint's 64, a negative value's two's "
    "complement. A value ClickHouse holds as an Int64 is read as a bigint."
)

FEATURES: tuple[Feature, ...] = (
    *(
        _feature(
            name, f"{name}($a, $b)", f"CAST($a {op} $b AS Nullable(Bool))", summary, _XID8_HELD
        )
        for name, (op, summary) in _XID8_ORDERS.items()
    ),
    _feature(
        "xid8send", "xid8send($v)", _XID8_SEND, "An xid8's binary form.", _XID8_HELD
    ),  # fmt: skip
    *(
        _feature(f"{prefix}_snapshot_{part}", f"{prefix}_snapshot_{part}($s)", form, summary, read)
        for prefix, signed in (("pg", False), ("txid", True))
        for part, form, summary, read in (
            ("xmin", _bound_of(1, signed), "A snapshot's earliest active id.", _SNAPSHOT_READ),
            ("xmax", _bound_of(2, signed), "A snapshot's first unassigned id.", _SNAPSHOT_READ),
            ("xip", _ids_of(signed), "A snapshot's in-progress ids, as rows.", _SNAPSHOT_READ),
            ("send", _send(), "A snapshot's binary form.", _SNAPSHOT_READ),
        )
    ),
    *(
        _feature(
            f"{prefix}_visible_in_snapshot",
            f"{prefix}_visible_in_snapshot($x, $s)",
            _visible(signed),
            "Whether a transaction id is visible in a snapshot.",
            _SNAPSHOT_READ,
        )
        for prefix, signed in (("pg", False), ("txid", True))
    ),
    *(
        _feature(name, None, _of_digits(name, written), summary, _BASE_READ)
        for name, (written, summary) in _TO_BASE.items()
    ),
)
