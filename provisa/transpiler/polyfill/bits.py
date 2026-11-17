# Copyright (c) 2026 Kenneth Stott
# Canary: 8a2c4e91-6b3d-4f07-9e15-2d7f0a6c3b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bit strings on ClickHouse (REQ-2236; the function support measure, REQ-2171).

ClickHouse has no bit string type: sqlglot wrote a cast to bit as ClickHouse's unknown BIT, and
every call of one failed on the engine. A bit string is held there as its text of 0s and 1s, as
bit varying already is (provisa.transpiler.pg_held_types). A literal is read as bit_in reads it: its
binary digits, or hexadecimal ones after an x, each four bits; a digit it does not read is
PostgreSQL's 22P02. A cast is explicit: to bit(n) it truncates or pads with zeros to n (bit with no
length is bit(1)), to bit varying(n) it truncates to n. A cast of a value that is no literal to bit is refused by name.

The text of two bit strings orders them as bitcmp does: bitcmp compares their bytes over the
shorter's, its last byte's bits past its length zeros, then the shorter first, which is the order of
the texts. The other functions read the text's characters: and, or and xor of strings of one length
(22026 otherwise), not, shifts that keep the length, concatenation, counts, get_bit and set_bit
(2202E past the string), int4 and int8 of at most 32 and 64 bits (22003 past them), the casts to a
length, substring, overlay and position, and the aggregates bit_and, bit_or and bit_xor.

Each row reads a call whose bit arguments the statement says are bit strings (a cast, or a VALUES
column's cast); an argument whose type is not said keeps what ClickHouse had."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "bit strings"

_CH = "clickhouse"
_BITS = frozenset({"bit", "bit varying", "varbit"})
#: The most bits a form pads a string to, ClickHouse's most of rightPad: past it a padding cast is
#: refused (PostgreSQL's own most, VARBITMAXLEN, is 2147483640).
_MOST_PADDED = 1000000
_VARBITMAXLEN = 2147483640


def _bit_type(to: exp.DataType) -> tuple[bool, int | None] | None:
    """Whether a cast's type is bit (True) or bit varying (False), and its length; None for any
    other type."""
    if to.is_type(exp.DataType.Type.BIT):
        varying = False
    elif to.this == exp.DataType.Type.USERDEFINED and str(to.args.get("kind")).lower() == "varbit":
        varying = True
    else:
        return None
    params = to.expressions
    length = int(params[0].this.this) if params else None
    return varying, length


def _bits_of(text: str) -> str:
    """The binary digits of a bit string's text, as bit_in reads it."""
    if text[:1] in ("x", "X"):
        digits = text[1:]
        bad = next((d for d in digits if d not in "0123456789abcdefABCDEF"), None)
        if bad is not None:
            raise Refused(
                Refusal("22P02", f'"{bad}" is not a valid hexadecimal digit'), ruling=True
            )
        return "".join(f"{int(d, 16):04b}" for d in digits)
    digits = text[1:] if text[:1] in ("b", "B") else text
    bad = next((d for d in digits if d not in "01"), None)
    if bad is not None:
        raise Refused(Refusal("22P02", f'"{bad}" is not a valid binary digit'), ruling=True)
    return digits


def held(value: exp.Expr) -> exp.Expr | None:
    """A bit string literal (or NULL) cast to bit or bit varying, as ClickHouse holds it: its text
    of 0s and 1s; None for any other value. A cast of anything else to bit is refused by name."""
    node = value.unnest()
    if not isinstance(node, exp.Cast) or isinstance(node, exp.TryCast):
        return None
    typed = _bit_type(node.to)
    if typed is None:
        return None
    varying, length = typed
    inner = node.this.unnest()
    while isinstance(inner, exp.Cast) and inner.to.is_type(*exp.DataType.TEXT_TYPES):
        inner = inner.this.unnest()
    if isinstance(inner, exp.Null):
        return exp.Cast(this=exp.Null(), to=exp.DataType.build("TEXT"))
    if not (isinstance(inner, exp.Literal) and inner.is_string):
        if varying:
            return None  # bit varying: held as text already (provisa.transpiler.pg_held_types)
        raise Refused(
            Refusal("0A000", "bit() of a value that is no literal has no form on clickhouse")
        )
    bits = _bits_of(inner.this)
    # A cast in a statement is explicit: bit(n) truncates or pads with zeros to n, bit varying(n)
    # truncates to n (PostgreSQL's bit() and varbit() of an explicit cast).
    if not varying:
        bits = bits[: length or 1].ljust(length or 1, "0")
    elif length is not None:
        bits = bits[:length]
    return exp.Literal.string(bits)


def _literal(node: exp.Expr, dialect: str) -> exp.Expr | None:  # noqa: ARG001 -- a rewrite's
    """A bit string literal cast to bit or bit varying, as its text (:func:`held`)."""
    return held(node)


LITERAL = Feature(
    "a bit string literal",
    FAMILY,
    "A text or NULL cast to bit or bit varying.",
    read=Side(
        rules=(
            Rule(
                None,
                {_CH: Rewrite(_literal, mentions=r"(?:::|\bas)\s*\"?(?:bit|varbit)\b")},
            ),
        ),
        limitations={
            _CH: (
                Limitation(
                    "A bit string is held as its text of 0s and 1s. A literal is read as bit_in "
                    "reads it (22P02 of a digit it does not read), cut or padded to the cast's "
                    "length; a cast to bit of a value that is no literal is refused by name.",
                    Refusal(
                        "0A000", "bit() of a value that is no literal has no form on clickhouse"
                    ),
                ),
            )
        },
    ),
)


def _value(arg: exp.Expr, dialect: str = _CH) -> exp.Expr:
    """An argument as ClickHouse holds it: a bit string literal its text, an integer literal as
    PostgreSQL reads it (polyfill.clickhouse.integer_read: 22003 past the type). A row reads its
    call before the walk reaches the arguments, and what it writes is not walked again."""
    from provisa.transpiler.polyfill import clickhouse  # clickhouse's rows import this module's kin

    written = held(arg)
    if written is None:
        try:
            written = clickhouse.integer_read(arg.unnest(), dialect)
        except Refused as refused:  # the client's error, before any refusal of the call by name
            raise Refused(refused.refusal, ruling=True) from refused
    return arg if written is None else written


#: sqlglot's names of the calls whose PostgreSQL name is another.
_NAMED = {"bitwise_and_agg": "bit_and", "bitwise_or_agg": "bit_or", "bitwise_xor_agg": "bit_xor"}


def _names(names: frozenset[str]) -> frozenset[str]:
    return names | {_NAMED[n] for n in names if n in _NAMED}


#: Each argument's kind: a bit string, an integer of 32 or of 64 bits, or another value.
_B, _V, _I4, _I8 = "bit", "value", "int4", "int8"
_KINDS = {_I4: frozenset({"integer", "smallint"}), _I8: frozenset({"bigint"})}


def _row(name: str, kinds: tuple[str, ...], form: str) -> Rewrite:
    """``name`` of arguments of ``kinds``, each bit string said to be one: ``form``, its holes
    $a, $b, $c, $d in the arguments' order. A bit string whose type is not said keeps what
    ClickHouse had."""
    holes = ("a", "b", "c", "d")[: len(kinds)]
    pattern = f"{name}({', '.join('$' + h for h in holes)})"

    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or name not in _names(call[0]) or len(call[1]) != len(kinds):
            return None
        args = call[1]
        for arg, kind in zip(args, kinds, strict=True):
            said = (trino.said(arg) or "").split("(")[0]
            if (kind == _B and said not in _BITS) or (kind in _KINDS and said not in _KINDS[kind]):
                return None
        bindings = {h: _value(a) for h, a in zip(holes, args, strict=True)}
        return graft(replacement(form, dialect, pattern), bindings)

    rewrite.__doc__ = f"{name} of bit strings held as their text."
    rewrite.__qualname__ = rewrite.__name__ = f"{name}_of_bit_text"
    # Read first: bytea's rows of these names read any call of their pattern, and keep or refuse
    # what is no bytea, before a rule of no pattern is read.
    return Rewrite(rewrite, mentions=rf"\b{re.escape(name)}\b", first=True)


def _feature(name: str, summary: str, kinds: tuple[str, ...], form: str) -> Feature:
    return Feature(
        f"{name}{'' if name.startswith(('bit', 'varbit')) else ' of a bit string'}",
        FAMILY,
        summary,
        read=Side(rules=(Rule(None, {_CH: _row(name, kinds, form)}),)),
    )


def _null_if_any(holes: str, answer: str) -> str:
    nulls = " OR ".join(f"${h} IS NULL" for h in holes)
    return f"if({nulls}, NULL, {answer})"


def _checked(raises: list[str], answer: str) -> str:
    return f"if({' + '.join(raises)} = 0, {answer}, NULL)"


_COMPARED = {
    "eq": ("=", "Two bit strings equal."),
    "ne": ("!=", "Two bit strings not equal."),
    "lt": ("<", "One bit string before another."),
    "le": ("<=", "One bit string before or equal to another."),
    "gt": (">", "One bit string after another."),
    "ge": (">=", "One bit string after or equal to another."),
}


def _byte(hole: str) -> str:
    """The k-th byte (from 0) of a bit string, its bits past its length zeros."""
    return (
        f"arraySum(arrayMap((_c, _i) -> if(_c = '1', bitShiftLeft(1, 8 - _i), 0), "
        f"splitByString('', substring(rightPad(${hole}, toUInt64(intDiv(length(${hole}) + 7, 8) * 8), "
        "'0'), toUInt64(_k * 8 + 1), 8)), range(1, 9)))"
    )


#: bitcmp, as varbit.c's bit_cmp: memcmp of the bytes over the shorter's (the difference of the
#: first that differ), else the shorter first (-1 or 1).
_DIFFERENCE = (
    f"arrayFirst(_d -> _d != 0, arrayMap(_k -> toInt32({_byte('a')}) - toInt32({_byte('b')}), "
    "range(toUInt64(least(intDiv(length(ifNull($a, '')) + 7, 8), "
    "intDiv(length(ifNull($b, '')) + 7, 8))))))"
)
_CMP = (
    "CAST("
    + _null_if_any(
        "ab",
        f"if({_DIFFERENCE} != 0, {_DIFFERENCE}, if(length($a) < length($b), -1, "
        "if(length($a) > length($b), 1, 0)))",
    )
    + " AS Nullable(Int32))"
)

COMPARISONS: tuple[Feature, ...] = (
    *(
        _feature(f"{prefix}{suffix}", summary, (_B, _B), f"CAST($a {op} $b AS Nullable(Bool))")
        for prefix in ("bit", "varbit")
        for suffix, (op, summary) in _COMPARED.items()
    ),
    *(
        _feature(f"{prefix}cmp", "btree's comparison of two bit strings.", (_B, _B), _CMP)
        for prefix in ("bit", "varbit")
    ),
)


def _characters(hole: str) -> str:
    return f"splitByString('', ifNull(${hole}, ''))"


def _bitwise(name: str, verb: str, op: str) -> Feature:
    sizes = ch_fail("22026", f"cannot {verb} bit strings of different sizes").format(
        condition="length($a) != length($b)"
    )
    each = (
        f"arrayStringConcat(arrayMap((_x, _y) -> toString({op}(toUInt8(_x), toUInt8(_y))), "
        f"{_characters('a')}, arrayResize({_characters('b')}, length({_characters('a')}), '0')))"
    )
    return _feature(
        name, f"The bitwise {verb} of two bit strings of one length.", (_B, _B),
        _checked([sizes], _null_if_any("ab", each)),
    )  # fmt: skip


_SHIFTED = (
    "if(toInt64($b) >= 0, "
    "rightPad(substring($a, toUInt64(least(toInt64($b), toInt64(length($a)))) + 1), length($a), '0'), "
    "leftPad(substring($a, 1, toUInt64(greatest(toInt64(length($a)) + toInt64($b), 0))), "
    "length($a), '0'))"
)

BITWISE: tuple[Feature, ...] = (
    _bitwise("bitand", "AND", "bitAnd"),
    _bitwise("bitor", "OR", "bitOr"),
    _bitwise("bitxor", "XOR", "bitXor"),
    _feature("bitnot", "Each bit of a bit string flipped.", (_B,), "translate($a, '01', '10')"),
    _feature(
        "bitshiftleft", "A bit string shifted left, its length kept.", (_B, _V),
        _null_if_any("ab", _SHIFTED),
    ),
    _feature(
        "bitshiftright", "A bit string shifted right, its length kept.", (_B, _V),
        _null_if_any("ab", _SHIFTED.replace("$b", "(-toInt64($b))")),
    ),
    _feature("bitcat", "Two bit strings joined.", (_B, _B), "concat($a, $b)"),
)  # fmt: skip

_INDEX = ch_fail("2202E", "bit index out of valid range").format(
    condition="toInt64($b) < 0 OR toInt64($b) >= length($a)"
)
_NEW_BIT = ch_fail("22023", "new bit must be 0 or 1").format(condition="$c NOT IN (0, 1)")


def _as_integer(bits: int, name: str, ch_type: str) -> str:
    past = ch_fail("22003", f"{name} out of range").format(condition=f"length($a) > {bits}")
    digits = f"splitByString('', if(length(ifNull($a, '')) > {bits}, '', ifNull($a, '')))"
    value = (
        f"arraySum(arrayMap((_c, _i) -> if(_c = '1', bitShiftLeft(toUInt64(1), "
        f"toUInt64(length({digits}) - _i)), toUInt64(0)), {digits}, arrayEnumerate({digits})))"
    )
    unsigned = "toUInt32" if bits == 32 else "toUInt64"
    return _checked(
        [past], _null_if_any("a", f"CAST(to{ch_type}({unsigned}({value})) AS Nullable({ch_type}))")
    )


_AS_INTEGER = {
    "int4": _as_integer(32, "integer", "Int32"),
    "int8": _as_integer(64, "bigint", "Int64"),
}


def _of_bits(name: str, summary: str, kinds: tuple[str, ...], form: str) -> Feature:
    """A function whose name other types share (length, int4): its row names it of a bit string."""
    return _feature(name, summary, kinds, form)


_LENGTH = "CAST(length($a) AS Nullable(Int32))"
_BIT_CAST = _checked(
    [
        ch_fail("22026", "bit string length does not match type bit").format(
            condition=f"toInt64($b) > 0 AND toInt64($b) <= {_VARBITMAXLEN} "
            "AND toInt64($b) != length($a) AND NOT $c"
        ),
        ch_fail("0A000", f"bit() of a length past {_MOST_PADDED} has no form on clickhouse").format(
            condition=f"toInt64($b) > {_MOST_PADDED} AND toInt64($b) <= {_VARBITMAXLEN} "
            "AND toInt64($b) > length($a) AND $c"
        ),
    ],
    _null_if_any(
        "abc",
        f"if(toInt64($b) <= 0 OR toInt64($b) > {_VARBITMAXLEN} OR toInt64($b) = length($a), $a, "
        f"rightPad(substring($a, 1, toUInt64(greatest(least(toInt64($b), {_MOST_PADDED}), 0))), "
        f"toUInt64(greatest(least(toInt64($b), {_MOST_PADDED}), 0)), '0'))",
    ),
)
_VARBIT_CAST = _checked(
    [
        ch_fail("22001", "bit string too long for type bit varying").format(
            condition="toInt64($b) > 0 AND toInt64($b) < length($a) AND NOT $c"
        )
    ],
    _null_if_any(
        "abc",
        "if(toInt64($b) <= 0 OR toInt64($b) >= length($a), $a, "
        "substring($a, 1, toUInt64(toInt64($b))))",
    ),
)
_FIRST = "greatest(toInt64($b), 1)"
_NEGATIVE = ch_fail("22011", "negative substring length not allowed")
_SUBSTRING_3 = _checked(
    [_NEGATIVE.format(condition="toInt64($c) < 0")],
    _null_if_any(
        "abc",
        f"substring($a, toUInt64({_FIRST}), toUInt64(greatest(least(toInt64($b) + toInt64($c), "
        f"toInt64(length($a)) + 1) - {_FIRST}, 0)))",
    ),
)
_SUBSTRING_2 = _null_if_any("ab", f"substring($a, toUInt64({_FIRST}))")

_PAST_INTEGER = ch_fail("22003", "integer out of range")


def _overlaid(count: str) -> str:
    start = "toInt64($c)"
    end = f"{start} + toInt64({count})"
    return _checked(
        [
            _NEGATIVE.format(condition=f"{start} < 1"),
            _PAST_INTEGER.format(
                condition=f"{start} >= 1 AND ({end} > 2147483647 OR {end} < -2147483648)"
            ),
        ],
        f"concat(substring($a, 1, toUInt64(greatest({start} - 1, 0))), $b, "
        f"substring($a, toUInt64(greatest({end}, 1))))",
    )


FUNCTIONS: tuple[Feature, ...] = (
    _of_bits(
        "bit_count", "The 1 bits of a bit string.", (_B,),
        "CAST(length($a) - length(replaceAll($a, '1', '')) AS Nullable(Int64))",
    ),
    _of_bits("bit_length", "A bit string's bits.", (_B,), _LENGTH),
    _of_bits("length", "A bit string's bits.", (_B,), _LENGTH),
    _of_bits(
        "octet_length", "A bit string's bytes.", (_B,),
        "CAST(intDiv(length($a) + 7, 8) AS Nullable(Int32))",
    ),
    _of_bits(
        "get_bit", "A bit of a bit string, by its index from 0.", (_B, _V),
        _checked([_INDEX], _null_if_any("ab", "toInt32OrZero(substring($a, toUInt64(greatest(toInt64($b), 0)) + 1, 1))")),
    ),
    _of_bits(
        "set_bit", "A bit string with a bit set, by its index from 0.", (_B, _V, _V),
        _checked(
            [_INDEX, _NEW_BIT],
            _null_if_any(
                "abc",
                "concat(substring($a, 1, toUInt64(greatest(toInt64($b), 0))), toString($c), "
                "substring($a, toUInt64(greatest(toInt64($b), 0)) + 2))",
            ),
        ),
    ),
    _of_bits("int4", "A bit string's bits as an integer.", (_B,), _AS_INTEGER["int4"]),
    _of_bits("int8", "A bit string's bits as a bigint.", (_B,), _AS_INTEGER["int8"]),
    _of_bits("bit", "A bit string cast to bit of a length.", (_B, _V, _V), _BIT_CAST),
    _of_bits("varbit", "A bit string cast to bit varying of a length.", (_B, _V, _V), _VARBIT_CAST),
    _of_bits("substring", "Part of a bit string, from a start, of a count of bits.", (_B, _V, _V), _SUBSTRING_3),
    _of_bits("position", "Where a bit string first holds another, from 1; 0 where it does not.", (_B, _B),
             "CAST(position($a, $b) AS Nullable(Int32))"),
)  # fmt: skip


def _of_an_integer(width: int) -> str:
    """bitfromint4 or bitfromint8: the low bits of an integer, as many as the length says (1 for
    a length of 0 or less, or past VARBITMAXLEN), each bit past the integer's own its sign."""
    length = f"if(ifNull(toInt64($b), 1) <= 0 OR ifNull(toInt64($b), 1) > {_VARBITMAXLEN}, 1, toInt64($b))"
    held = f"toUInt64(least({length}, {_MOST_PADDED}))"
    past = ch_fail("0A000", f"bit() of a length past {_MOST_PADDED} has no form on clickhouse")
    each = (
        f"arrayStringConcat(arrayMap(_k -> if(_k >= {width}, if($a < 0, '1', '0'), "
        f"toString(bitTest($a, least(_k, {width - 1})))), arrayReverse(range({held}))))"
    )
    return _checked([past.format(condition=f"{length} > {_MOST_PADDED}")], _null_if_any("ab", each))


OF_AN_INTEGER = Feature(
    "bit of an integer",
    FAMILY,
    "An integer's low bits as a bit string of a length, sign-extended past its own.",
    read=Side(
        rules=(
            Rule(None, {_CH: _row("bit", (_I4, _V), _of_an_integer(32))}),
            Rule(None, {_CH: _row("bit", (_I8, _V), _of_an_integer(64))}),
        )
    ),
)


def _two_forms(name: str, summary: str, forms: dict[int, tuple[tuple[str, ...], str]]) -> Feature:
    """``name`` of more than one argument count, each its form."""
    rules = tuple(Rule(None, {_CH: _row(name, kinds, form)}) for kinds, form in forms.values())
    return Feature(f"{name} of a bit string", FAMILY, summary, read=Side(rules=rules))


SUBSTRING_2 = Feature(
    "substring of a bit string from a start",
    FAMILY,
    "Part of a bit string, from a start to its end.",
    read=Side(rules=(Rule(None, {_CH: _row("substring", (_B, _V), _SUBSTRING_2)}),)),
)
OVERLAY = _two_forms(
    "overlay",
    "A bit string with another put in place of a count of its bits from a start.",
    {
        4: ((_B, _B, _V, _V), _null_if_any("abcd", _overlaid("$d"))),
        3: ((_B, _B, _V), _null_if_any("abc", _overlaid("length($b)"))),
    },
)

# --- the aggregates ------------------------------------------------------------------------------

_AGGREGATED = {
    "bit_and": ("AND", "if(has(_g, '0'), '0', '1')"),
    "bit_or": ("OR", "if(has(_g, '1'), '1', '0')"),
    "bit_xor": ("XOR", "toString(countEqual(_g, '1') % 2)"),
}


def _aggregate_form(name: str) -> str:
    verb, bit = _AGGREGATED[name]
    sizes = ch_fail("22026", f"cannot {verb} bit strings of different sizes").format(
        condition="uniqExact(length($a)) > 1"
    )
    width = "length(arrayElement(groupArray($a), 1))"
    each = (
        f"arrayStringConcat(arrayMap((_i, _all) -> arrayJoinLess, range(1, toUInt64({width}) + 1), "
        f"arrayWithConstant(toUInt64({width}), groupArray($a))))"
    ).replace(
        "arrayJoinLess",
        bit.replace("_g", "arrayMap(_x -> substring(_x, _i, 1), _all)"),
    )
    return f"if({sizes} = 0, if(count($a) > 0, {each}, NULL), NULL)"


def _aggregate(name: str) -> Callable[[exp.Expr, str], exp.Expr | None]:
    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or name not in _names(call[0]) or len(call[1]) != 1:
            return None
        if isinstance(node.parent, (exp.Window, exp.Filter)):
            return None
        if (trino.said(call[1][0]) or "").split("(")[0] not in _BITS:
            return None
        form = replacement(_aggregate_form(name), dialect, f"{name}($a)")
        return graft(form, {"a": _value(call[1][0])})

    rewrite.__doc__ = f"{name} of bit strings held as their text, bit by bit."
    rewrite.__qualname__ = rewrite.__name__ = f"{name}_of_bit_text"
    return rewrite


AGGREGATES: tuple[Feature, ...] = tuple(
    Feature(
        f"{name} of bit strings",
        FAMILY,
        f"The bitwise {verb} of the bit strings of a group, of one length.",
        read=Side(
            rules=(
                Rule(None, {_CH: Rewrite(_aggregate(name), mentions=rf"\b{name}\b", first=True)}),
            ),
        ),
    )
    for name, (verb, _) in _AGGREGATED.items()
)

FEATURES: tuple[Feature, ...] = (
    LITERAL,
    *COMPARISONS,
    *BITWISE,
    *FUNCTIONS,
    SUBSTRING_2,
    OVERLAY,
    OF_AN_INTEGER,
    *AGGREGATES,
)
