# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e3d58-91c4-4a2f-b6e0-d8a35c1f9e72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integers and numbers on Trino (REQ-2236; the function support measure, run 43307392116,
REQ-2171).

int2_sum and int4_sum add in a bigint that wraps past its range: the old table
(provisa.transpiler.pg_forms_types) writes the wrap with the numbers 2^63 and 2^64, which Trino
reads as no literal. Trino's forms write them as decimals of their text. int8shl is Trino's own
shift of a bigint, which wraps as C's does. A right shift is
arithmetic in C, its count taken modulo the promoted width: Trino's is
bitwise_right_shift_arithmetic, of an integer count (the old form's count was a bigint, which Trino
takes for no shift). factorial, numeric and bool of an integer, oid of a bigint and random between
two bounds have no Trino function of PostgreSQL's name, so each was sent as written: each is
Trino's form here, every other dialect keeping what it had."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import decimal
import math
import re
import struct

import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import END, MARK
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement

FAMILY = "integers and numbers"


def fail(sqlstate: str, message: str) -> str:
    """Trino's raise of PostgreSQL's error ``sqlstate`` with ``message`` (provisa.core.pg_error)."""
    text = f"{MARK} {sqlstate}: {message}{END}".replace("'", "''")
    return f"fail('{text}')"


def refused(function: str) -> str:
    """Trino's raise of the refusal of a value ``function`` does not read as PostgreSQL does."""
    return fail("0A000", f"{function} does not read this value")


def ch_fail(sqlstate: str, message: str) -> str:
    """ClickHouse's raise of PostgreSQL's error ``sqlstate`` with ``message`` where ``condition``
    holds: throwIf, which answers 0 where it does not, so a form adds it to its answer."""
    text = f"{MARK} {sqlstate}: {message}{END}".replace("'", "''")
    return "throwIf(ifNull({condition}, 0), '" + text + "')"


def ch_only(form: str | Rewrite) -> dict[str, str | Rewrite]:
    """ClickHouse's ``form``, every other dialect keeping what it had (the old table's form)."""
    return {**{d: trino.KEPT for d in DIALECTS if d not in NATIVE}, "clickhouse": form}


def ch_once(form: str, only: frozenset[str] | None = None) -> str:
    """``form`` with each of its holes read once: bound to a lambda's parameter of the same name
    (``$a`` as ``_a``). A form that names its argument several times, written where another form
    names it several times (abs in a range's form), would grow as the product of the two. ``only``:
    the holes to bind, the rest left as they are (a leaf named twice costs nothing); none to bind is
    the form itself."""
    named = list(dict.fromkeys(re.findall(r"\$([A-Za-z]\w*)", form)))
    holes = named if only is None else [hole for hole in named if hole in only]
    if not holes:
        return form
    bound = "|".join(holes)
    body = re.sub(rf"\$({bound})\b", r"_\1", form)
    parameters = ", ".join(f"_{hole}" for hole in holes)
    arrays = ", ".join(f"[${hole}]" for hole in holes)
    return f"arrayElement(arrayMap(({parameters}) -> {body}, {arrays}), 1)"


def ch_checked(raises: list[str], answer: str) -> str:
    """``answer`` where none of ``raises`` (ch_fail's, each 0 where it does not raise) raises:
    ClickHouse reads every branch of a condition, so each raise stands beside the answer."""
    return f"if({' + '.join(raises)} = 0, {answer}, NULL)"


def _decimal(digits: int) -> str:
    """A number past a bigint, as Trino reads it: the decimal of its text."""
    return f"CAST('{digits}' AS DECIMAL(38, 0))"


_HALF, _WHOLE = _decimal(1 << 63), _decimal(1 << 64)


def _wrapped(value: str) -> str:
    """``value``, a decimal, wrapped into a bigint as C's two's complement wraps it."""
    return f"CAST(((({value}) + {_HALF}) % {_WHOLE} + {_WHOLE}) % {_WHOLE} - {_HALF} AS BIGINT)"


def _sum(name: str) -> Feature:
    """int2_sum or int4_sum: not strict, a NULL state or value passed over, the sum wrapping."""
    added = _wrapped("CAST($a AS DECIMAL(38, 0)) + CAST($b AS DECIMAL(38, 0))")
    return Feature(
        name,
        FAMILY,
        "A step of an integer sum, in a bigint that wraps past its range.",
        read=Side(
            rules=(
                Rule(
                    f"{name}($a, $b)",
                    trino.trino_only(
                        "CASE WHEN $a IS NULL THEN CAST($b AS BIGINT) "
                        f"WHEN $b IS NULL THEN CAST($a AS BIGINT) ELSE {added} END"
                    ),
                ),
            )
        ),
    )


#: int8shl: C's int64 << of the count modulo 64 wraps in two's complement, as Java's << of a
#: bigint does: Trino's bitwise_left_shift, of an integer count. A power of two read as a double
#: (the old form's) loses digits past 2^53.
INT8SHL = Feature(
    "int8shl",
    FAMILY,
    "A bigint shifted left, its count taken modulo 64, the result wrapping.",
    read=Side(
        rules=(
            Rule(
                "int8shl($a, $b)",
                {
                    **trino.trino_only(
                        "bitwise_left_shift(CAST($a AS BIGINT), CAST(bitwise_and($b, 63) AS INTEGER))"
                    ),
                    # The old table's form wraps through 2^64, which ClickHouse reads as no exact
                    # literal: int8shl(3, 3) answered 0 (run 43424649612). bitShiftLeft wraps an
                    # Int64 as C does.
                    "clickhouse": "bitShiftLeft(CAST($a AS Nullable(Int64)), bitAnd($b, 63))",
                },
            ),
        )
    ),
)

#: Each right shift: the type it shifts, its C promotion and the count's modulus.
_SHIFTS = {
    "int2shr": ("SMALLINT", "INTEGER", 31),
    "int4shr": ("INTEGER", "INTEGER", 31),
    "int8shr": ("BIGINT", "BIGINT", 63),
}


def _shift(name: str, sql_type: str, promoted: str, mask: int) -> Feature:
    shifted = (
        f"bitwise_right_shift_arithmetic(CAST($a AS {promoted}), "
        f"CAST(bitwise_and($b, {mask}) AS INTEGER))"
    )
    return Feature(
        name,
        FAMILY,
        f"A {sql_type.lower()} shifted right, its sign kept, its count taken modulo {mask + 1}.",
        read=Side(
            rules=(Rule(f"{name}($a, $b)", trino.trino_only(f"CAST({shifted} AS {sql_type})")),)
        ),
    )


#: The largest n whose factorial a decimal of 38 digits holds (33! has 37).
_LARGEST_FACTORIAL = 33

#: The largest n whose factorial ClickHouse's Decimal256 (76 digits) holds: 56! has 75.
CH_LARGEST_FACTORIAL = 56
#: numeric_fac's own bound: past it the answer overflows numeric (22003).
_NUMERIC_FACTORIAL_BOUND = 32177
#: ClickHouse evaluates every branch: the range is bounded so a call past the bound reads none.
_CH_PRODUCT = (
    "arrayFold((_f, _i) -> _f * toDecimal256(_i, 0), "
    f"range(1, toUInt64(least(greatest($n, 0), {CH_LARGEST_FACTORIAL})) + 1), toDecimal256(1, 0))"
)
#: ClickHouse's factorial is of a UInt8 up to 20, a Float64 past it: the product in Decimal256.
_CH_FACTORIAL = ch_checked(
    [
        ch_fail("22003", "factorial of a negative number is undefined").format(condition="$n < 0"),
        ch_fail("22003", "value overflows numeric format").format(
            condition=f"$n > {_NUMERIC_FACTORIAL_BOUND}"
        ),
        ch_fail(
            "0A000",
            f"factorial() of a value past {CH_LARGEST_FACTORIAL} has no exact form on clickhouse",
        ).format(condition=f"$n > {CH_LARGEST_FACTORIAL} AND $n <= {_NUMERIC_FACTORIAL_BOUND}"),
    ],
    f"if($n IS NULL OR $n > {CH_LARGEST_FACTORIAL}, NULL, {_CH_PRODUCT})",
)

FACTORIAL = Feature(
    "factorial",
    FAMILY,
    "The product of the integers from 1 to n.",
    read=Side(
        rules=(
            Rule(
                "factorial($n)",
                {
                    **trino.trino_only(
                        "CASE WHEN $n IS NULL THEN CAST(NULL AS DECIMAL(38, 0)) "
                        f"WHEN $n < 0 THEN {fail('22003', 'factorial of a negative number is undefined')} "
                        f"WHEN $n > {_LARGEST_FACTORIAL} THEN {refused('factorial')} "
                        "ELSE reduce(sequence(1, greatest($n, 1)), CAST(1 AS DECIMAL(38, 0)), "
                        "(_s, _x) -> _s * _x, _s -> _s) END"
                    ),
                    "clickhouse": ch_once(_CH_FACTORIAL),
                },
            ),
        ),
        limitations={
            **dict.fromkeys(
                trino.TRINO,
                (
                    Limitation(
                        f"Past {_LARGEST_FACTORIAL}, whose factorial a decimal of 38 digits does "
                        "not hold, refused with 0A000, where PostgreSQL answers its exact digits."
                    ),
                ),
            ),
            "clickhouse": (
                Limitation(
                    f"From {CH_LARGEST_FACTORIAL + 1} to {_NUMERIC_FACTORIAL_BOUND}, whose factorial "
                    "ClickHouse's Decimal256 (76 digits) does not hold, refused with 0A000, where "
                    "PostgreSQL answers its exact digits; past that, PostgreSQL's 22003."
                ),
            ),
        },
    ),
)

_INTEGERS = frozenset({"smallint", "integer", "bigint"})
_JSON = frozenset({"json", "jsonb"})


#: ClickHouse's name of each type a cast row writes, as a Nullable: every value may be NULL.
_CLICKHOUSE_TYPES = {
    "DECIMAL(38, 0)": "Nullable(Decimal(38, 0))",
    "BOOLEAN": "Nullable(Bool)",
}


#: A JSON number, as RFC 8259 writes one, and the whitespace JSON allows around it. ClickHouse's
#: isValidJSON refuses a number past a double's range, which PostgreSQL's jsonb holds exactly.
_SPACE = "[[:space:]]*"
_JSON_NUMBER = f"^{_SPACE}-?(0|[1-9][0-9]*)([.][0-9]+)?([eE][-+]?[0-9]+)?{_SPACE}$"
#: Each kind of JSON value ClickHouse's JSONType names, as PostgreSQL's error names it.
_JSON_KINDS = {
    "String": "string",
    "Null": "null",
    "Bool": "boolean",
    "Array": "array",
    "Object": "object",
}
#: PostgreSQL's type name of each function's result, as its error names it.
_PG_NAMES = {"bool": "boolean", "numeric": "numeric"}


def _ch_jsonb_cast(function: str) -> str:
    """bool or numeric of a jsonb value ClickHouse holds as its text: NULL of NULL; bool of a JSON
    boolean its value; numeric of a JSON number its own digits (PostgreSQL's numeric holds them
    exactly, so no Decimal bounds them; a negative zero is zero). Every other kind raises
    PostgreSQL's error (22023); a number with an exponent (PostgreSQL's digits shifted) raises
    0A000; text that is no JSON raises 22P02, as PostgreSQL's input did. Each raise is a throwIf
    beside the answer: ClickHouse evaluates every branch of a CASE."""
    name = _PG_NAMES[function]
    number = f"match($v, '{_JSON_NUMBER}')"
    valid = "isValidJSON($v)"
    raises = [
        ch_fail("22P02", "invalid input syntax for type json").format(
            condition=f"NOT {number} AND NOT {valid}"
        )
    ]
    answered = "Bool" if function == "bool" else None
    kinds = {k: v for k, v in _JSON_KINDS.items() if k != answered}
    for kind, said in kinds.items():
        raises.append(
            ch_fail("22023", f"cannot cast jsonb {said} to type {name}").format(
                condition=f"NOT {number} AND {valid} AND toString(JSONType($v)) = '{kind}'"
            )
        )
    text = "replaceRegexpAll($v, '[[:space:]]', '')"
    if function == "bool":
        raises.append(
            ch_fail("22023", f"cannot cast jsonb numeric to type {name}").format(condition=number)
        )
        answer = f"CAST({text} = 'true' AS Nullable(Bool))"
    else:
        raises.append(
            ch_fail("0A000", "numeric does not read this value").format(
                condition=f"{number} AND match($v, '[eE]')"
            )
        )
        answer = f"if(match({text}, '^-0([.]0+)?$'), substring({text}, 2), {text})"
    checked = " + ".join(raises)
    typed = "Nullable(Bool)" if function == "bool" else "Nullable(String)"
    nothing = f"CAST(NULL AS {typed})"  # a bare NULL is no type pgwire describes as the call's
    return f"CASE WHEN $v IS NULL THEN {nothing} ELSE if({checked} = 0, {answer}, {nothing}) END"


#: The digits float8_numeric and float4_numeric keep of a double and a real (DBL_DIG, FLT_DIG).
_FLOAT_DIGITS = {"double precision": 15, "real": 6}
_NUMERIC_PAST = (
    "numeric() of a float of more than 38 digits, NaN or infinity has no exact form on clickhouse"
)


def _float_literal(node: exp.Expr) -> float | None | bool:
    """A float the statement writes (a number or a text, signed, cast): its value, None for a NULL,
    False for any other value."""
    value = node.unnest()
    while isinstance(value, exp.Cast):
        value = value.this.unnest()
    negative = False
    while isinstance(value, (exp.Neg, exp.Paren)):
        negative ^= isinstance(value, exp.Neg)
        value = value.this.unnest()
        while isinstance(value, exp.Cast):
            value = value.this.unnest()
    if isinstance(value, exp.Null):
        return None
    if not isinstance(value, exp.Literal):
        return False
    try:
        number = float(value.this.strip())
    except ValueError:
        return False
    return -number if negative else number


def numeric_of_float(node: exp.Expr, said: str, dialect: str) -> exp.Expr:
    """numeric of a double or a real the statement writes (float8_numeric, float4_numeric): its
    text of DBL_DIG or FLT_DIG significant digits read as a numeric, computed as the statement is
    read; past 38 digits, NaN or infinity, 0A000 (ClickHouse's Decimal holds none of them)."""
    value = _float_literal(node)
    if value is False:
        raise Refused(trino.no_form("numeric", dialect))
    if value is None:
        return sqlglot.parse_one("CAST(NULL AS Nullable(Decimal(38, 0)))", read="clickhouse")
    assert isinstance(value, float)
    if said == "real":
        value = struct.unpack("<f", struct.pack("<f", value))[0] if math.isfinite(value) else value
    if not math.isfinite(value):
        raise Refused(Refusal("0A000", _NUMERIC_PAST), ruling=True)
    number = decimal.Decimal(f"{value:.{_FLOAT_DIGITS[said]}g}")
    text = format(abs(number) if number == 0 else number, "f")
    digits = len(text.lstrip("-").replace(".", "").lstrip("0")) or 1
    scale = len(text.split(".")[1]) if "." in text else 0
    if max(digits, scale) > 38 or len(text.lstrip("-").split(".")[0]) + scale > 38:
        raise Refused(Refusal("0A000", _NUMERIC_PAST), ruling=True)
    return sqlglot.parse_one(f"CAST('{text}' AS Nullable(Decimal(38, {scale})))", read="clickhouse")


def _cast_of(function: str, sources: frozenset[str], sql_type: str):
    """``function`` of a value said to be of ``sources``: its cast to ``sql_type``. Of a jsonb
    value on ClickHouse, :func:`_ch_jsonb_cast`; of a json value there, and of either on Trino,
    refused by name (Trino's cast reads a JSON scalar PostgreSQL refuses, and PostgreSQL casts no
    json to these); of anything else, kept (an argument no statement says the type of is
    ClickHouse's unknown function, an engine error)."""

    def cast(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or len(call[1]) != 1:
            return None
        type_said = trino.said(call[1][0])
        if type_said == "jsonb" and dialect == "clickhouse":
            form = _ch_jsonb_cast(function)
            return graft(replacement(form, dialect, f"{function}($v)"), {"v": call[1][0]})
        if type_said in _JSON:
            raise Refused(trino.no_form(function, dialect))
        if function == "numeric" and dialect == "clickhouse" and type_said in _FLOAT_DIGITS:
            return numeric_of_float(call[1][0], type_said, dialect)
        if type_said not in sources:
            return None
        if dialect == "clickhouse":
            form = f"CAST($v AS {_CLICKHOUSE_TYPES[sql_type]})"
            return graft(replacement(form, dialect, f"{function}($v)"), {"v": call[1][0]})
        return trino.written(f"CAST($v AS {sql_type})", f"{function}($v)", {"v": call[1][0]})

    cast.__doc__ = f"{function} of an integer: its cast to {sql_type}; of a json value, refused."
    cast.__qualname__ = cast.__name__ = f"{function}_of_an_integer"
    return Rewrite(cast)


def _cast_feature(function: str, summary: str, sources: frozenset[str], sql_type: str) -> Feature:
    return Feature(
        function,
        FAMILY,
        summary,
        read=Side(
            rules=(
                Rule(
                    f"{function}($v)",
                    {
                        **trino.trino_only(_cast_of(function, sources, sql_type)),
                        "clickhouse": _cast_of(function, sources, sql_type),
                    },
                ),
            ),
            limitations={
                **dict.fromkeys(
                    trino.TRINO, (Limitation("Of a json or jsonb value, refused by name."),)
                ),
                "clickhouse": (
                    Limitation(_CLICKHOUSE_JSONB[function]),
                    *(_FLOAT_NUMERIC if function == "numeric" else ()),
                ),
            },
        ),
    )


_FLOAT_NUMERIC = (
    Limitation(
        "Of a double or a real the statement writes as a literal: its text of 15 or 6 significant "
        "digits read as a numeric, computed as the statement is read; past 38 digits, NaN or "
        "infinity raises 0A000. Of any other double or real, refused by name.",
        Refusal("0A000", _NUMERIC_PAST),
    ),
)


#: What differs from PostgreSQL on ClickHouse, where a jsonb value is held as its unchecked text.
_UNCHECKED = (
    "Text a jsonb column holds that is not valid JSON, which PostgreSQL refuses when it is "
    "stored, raises 22P02 (invalid input syntax for type json) when the call reads it, without "
    "PostgreSQL's detail line."
)
_CLICKHOUSE_JSONB = {
    "bool": "Of a jsonb value: a JSON true or false answers its value, NULL answers NULL, and any "
    "other kind raises PostgreSQL's error (22023). " + _UNCHECKED + " Of a json value, refused by "
    "name.",
    "numeric": "Of a jsonb value: a JSON number answers its own digits, at any precision and "
    "scale, never rounded; NULL answers NULL, and any other kind raises PostgreSQL's error "
    "(22023). A number written with an exponent (1e3), which PostgreSQL answers with its digits "
    "shifted (1000), is refused with 0A000. " + _UNCHECKED + " Of a json value, refused by name.",
}


#: Each integer of a jsonb number: PostgreSQL's name of it, ClickHouse's type, its bits.
_JSONB_INTEGERS = {"int2": ("smallint", "Int16", 16), "int4": ("integer", "Int32", 32),
                   "int8": ("bigint", "Int64", 64)}  # fmt: skip
#: Each float of a jsonb number: PostgreSQL's name of it and ClickHouse's type.
#: float4 has no row: ClickHouse reads a decimal text to a Float32 through a Float64, which can
#: round twice where PostgreSQL's strtof rounds once.
_JSONB_FLOATS = {"float8": ("double precision", "Float64")}


def _ch_jsonb_number(function: str) -> str:
    """An integer or a float of a jsonb value ClickHouse holds as its text (numeric_int2 and its
    kin, numeric_float8 and float4 of the number's numeric): NULL of NULL. Of a JSON number, an
    integer is its value rounded half away from zero, PostgreSQL's 22003 past the type, and one
    written with an exponent raises 0A000; a float is its digits read to the nearest float (as
    PostgreSQL's float input reads the numeric's text), a negative zero zero, 22003 where that
    overflows or underflows. Every other kind raises PostgreSQL's 22023, and text that is no JSON
    22P02, as _ch_jsonb_cast does."""
    number = f"match($v, '{_JSON_NUMBER}')"
    valid = "isValidJSON($v)"
    name = (_JSONB_INTEGERS.get(function) or _JSONB_FLOATS[function])[0]
    raises = [
        ch_fail("22P02", "invalid input syntax for type json").format(
            condition=f"NOT {number} AND NOT {valid}"
        )
    ]
    for kind, said in _JSON_KINDS.items():
        raises.append(
            ch_fail("22023", f"cannot cast jsonb {said} to type {name}").format(
                condition=f"NOT {number} AND {valid} AND toString(JSONType($v)) = '{kind}'"
            )
        )
    text = "replaceRegexpAll($v, '[[:space:]]', '')"
    if function in _JSONB_INTEGERS:
        _, ch_type, bits = _JSONB_INTEGERS[function]
        body = f"replaceRegexpOne({text}, '^-', '')"
        whole = f"extract({body}, '^[0-9]+')"
        digits = f"concat({whole}, extract({body}, '^[0-9]+[.]([0-9]+)'))"
        significant = f"replaceRegexpOne({digits}, '^0+', '')"
        exponent = f"extract({body}, '[eE]([-+]?[0-9]+)$')"
        # The digits before the point, of the number written without its leading zeros.
        point = (
            f"(toInt64(length({whole})) + toInt64OrZero({exponent}) "
            f"- toInt64(length({digits}) - length({significant})))"
        )
        # A point past 40 digits is past each of these integers (and past Int256).
        wide = f"({significant} != '' AND {point} > 40)"
        # Each branch is read whatever the condition: a negative point is held at 0 within them.
        held = f"toUInt64(greatest(least({point}, 41), 0))"
        kept = f"if({point} <= 0 OR {wide}, '0', rightPad(substring({significant}, 1, {held}), {held}, '0'))"
        rounding = f"if({point} < 0 OR {wide}, 0, substring({significant}, {held} + 1, 1) >= '5')"
        magnitude = f"(toInt256OrZero({kept}) + {rounding})"
        value = f"if(startsWith({text}, '-'), -{magnitude}, {magnitude})"
        lowest, highest = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
        past = f"{wide} OR {value} < {lowest} OR {value} > {highest}"
        raises.append(
            ch_fail("0A000", f"{function} does not read this value").format(
                condition=f"{number} AND length({exponent}) > 6"
            )
        )
        raises.append(
            ch_fail("22003", f"{name} out of range").format(
                condition=f"{number} AND length({exponent}) <= 6 AND ({past})"
            )
        )
        answer = f"CAST(if({past}, 0, {value}) AS Nullable({ch_type}))"
        typed = f"Nullable({ch_type})"
    else:
        ch_type = _JSONB_FLOATS[function][1]
        # JSONExtractFloat reads digits to the nearest double (toFloat64 does not, without
        # precise_float_parsing); a number of no point or exponent is read as a float, not an
        # integer past 64 bits. A number past a double's range is its 0.
        as_float = f"if(match({text}, '[.eE]'), {text}, concat({text}, '.0'))"
        read = f"JSONExtractFloat({as_float})"
        mantissa = f"replaceRegexpOne({text}, '[eE].*$', '')"
        raises.append(
            ch_fail("22003", f"value out of range for type {name}").format(
                condition=f"{number} AND {read} = 0 AND match({mantissa}, '[1-9]')"
            )
        )
        answer = f"CAST({read} + 0 AS Nullable({ch_type}))"  # + 0: a negative zero is zero
        typed = f"Nullable({ch_type})"
    checked = " + ".join(raises)
    nothing = f"CAST(NULL AS {typed})"
    return f"CASE WHEN $v IS NULL THEN {nothing} ELSE if({checked} = 0, {answer}, {nothing}) END"


def _number_of(function: str):
    """``function`` of a value: of a jsonb value on ClickHouse, :func:`_ch_jsonb_number`; of a
    json value there, refused by name; int8 of an oid, held as a bigint, that bigint; of anything
    else, kept."""

    def number(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or len(call[1]) != 1:
            return None
        type_said = trino.said(call[1][0])
        if type_said == "jsonb":
            form = _ch_jsonb_number(function)
        elif type_said == "oid" and function == "int8":
            form = "CAST($v AS Nullable(Int64))"
        elif type_said in _JSON:
            raise Refused(trino.no_form(function, dialect))
        else:
            return None
        return graft(replacement(form, dialect, f"{function}($v)"), {"v": call[1][0]})

    number.__doc__ = f"{function} of a jsonb value, its JSON number; of a json value, refused."
    number.__qualname__ = number.__name__ = f"{function}_of_jsonb"
    return Rewrite(number)


JSONB_NUMBERS: tuple[Feature, ...] = tuple(
    Feature(
        function,
        FAMILY,
        f"A jsonb number as {name}" + (", and an oid as a bigint." if function == "int8" else "."),
        read=Side(
            rules=(
                Rule(
                    f"{function}($v)",
                    {
                        **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
                        "clickhouse": _number_of(function),
                    },
                ),
            ),
            limitations={
                "clickhouse": (
                    Limitation(
                        "Of a jsonb value: a JSON number answers its value"
                        + (
                            " rounded half away from zero, 22003 past the type; one whose "
                            "exponent has more than six digits is refused with 0A000."
                            if function in _JSONB_INTEGERS
                            else " read to the nearest float, 22003 where it overflows or "
                            "underflows."
                        )
                        + " NULL answers NULL, and any other kind raises PostgreSQL's error "
                        "(22023). " + _UNCHECKED + " Of a json value, refused by name."
                    ),
                )
            },
        ),
    )
    for function, name in (
        *((f, v[0]) for f, v in _JSONB_INTEGERS.items()),
        *((f, v[0]) for f, v in _JSONB_FLOATS.items()),
    )
)


NUMERIC = _cast_feature(
    "numeric", "An integer as a numeric.", _INTEGERS, "DECIMAL(38, 0)"
)  # fmt: skip
BOOL = _cast_feature(
    "bool", "An integer as a truth value: false for 0.", frozenset({"smallint", "integer"}),
    "BOOLEAN",
)  # fmt: skip

#: An oid is held as a bigint (provisa.transpiler.pg_held_types); i8tooid refuses one past its
#: 32 unsigned bits.
OID = Feature(
    "oid",
    FAMILY,
    "A bigint as an oid.",
    read=Side(
        rules=(
            Rule(
                "oid($v)",
                {
                    **trino.trino_only(
                        f"CASE WHEN $v < 0 OR $v > 4294967295 "
                        f"THEN {fail('22003', 'OID out of range')} "
                        "ELSE CAST($v AS BIGINT) END"
                    ),
                    "clickhouse": "CAST($v + "
                    + ch_fail("22003", "OID out of range").format(
                        condition="$v < 0 OR $v > 4294967295"
                    )
                    + " AS Nullable(Int64))",
                },
            ),
        )
    ),
)

#: The widest range random's form draws from exactly: a double holds every integer below 2^53.
_EXACT = 1 << 53
_RANDOM = "random($lo, $hi)"


def random_between(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """random of two integers: a uniform integer from the one to the other, both included."""
    call = trino.called(node)
    if call is None or len(call[1]) != 2:  # noqa: PLR2004 -- the two bounds
        return None
    lo, hi = call[1]
    said = {trino.said(lo), trino.said(hi)}
    if not said <= _INTEGERS:
        if said & {"numeric", "decimal"}:
            return None  # refused by name before (provisa.transpiler.engine_refusals)
        raise Refused(trino.no_form("random", dialect))
    sql_type = "BIGINT" if "bigint" in said else "INTEGER"
    if dialect == "clickhouse":
        return graft(replacement(_ch_random(sql_type), dialect, _RANDOM), {"lo": lo, "hi": hi})
    form = (
        "CASE WHEN $lo > $hi THEN "
        f"{fail('22023', 'lower bound must be less than or equal to upper bound')} "
        f"WHEN CAST($hi AS DECIMAL(38, 0)) - CAST($lo AS DECIMAL(38, 0)) >= {_EXACT} "
        f"THEN {refused('random')} "
        f"ELSE CAST($lo + least(CAST(floor(random() * (CAST($hi - $lo AS DOUBLE) + 1)) AS BIGINT), "
        f"$hi - $lo) AS {sql_type}) END"
    )
    return trino.written(form, _RANDOM, {"lo": lo, "hi": hi})


def _ch_random(sql_type: str) -> str:
    """ClickHouse's form of random between two integers: the bound checks raise beside the draw."""
    # Int64 whatever the bounds: the answer lies between them, and the call says its type.
    del sql_type
    ch_type = "Nullable(Int64)"
    reversed_ = ch_fail("22023", "lower bound must be less than or equal to upper bound")
    too_wide = ch_fail("0A000", "random does not read this value")
    return (
        "CAST($lo + least(toInt64(floor(randCanonical() * (toFloat64($hi - $lo) + 1))), "
        "$hi - $lo) + "
        + reversed_.format(condition="$lo > $hi")
        + " + "
        + too_wide.format(condition=f"toDecimal128($hi, 0) - toDecimal128($lo, 0) >= {_EXACT}")
        + f" AS {ch_type})"
    )


RANDOM = Feature(
    "random",
    FAMILY,
    "A uniform random integer between two bounds, both included.",
    read=Side(
        rules=(
            Rule(
                _RANDOM,
                {
                    **trino.trino_only(Rewrite(random_between)),
                    "clickhouse": Rewrite(random_between),
                },
            ),
        ),
        limitations=dict.fromkeys(
            (*trino.TRINO, "clickhouse"),
            (
                Limitation(
                    "Bounds 2^53 or more apart, and bounds of no said integer type, refused by "
                    "name: a double draws no wider range exactly."
                ),
            ),
        ),
    ),
)

#: int2int4_sum (int.c): the {count, sum} state of avg(int2) and avg(int4), the sum, NULL of no
#: rows; XX000 of an array of other than two elements.
INT2INT4_SUM = Feature(
    "int2int4_sum",
    FAMILY,
    "The sum of avg(smallint) and avg(integer)'s {count, sum} state.",
    read=Side(
        rules=(
            Rule(
                "int2int4_sum($a)",
                ch_only(
                    ch_once(
                        "CAST(if(isNull($a), NULL, if("
                        + ch_fail("XX000", "expected 2-element int8 array").format(
                            condition="length($a) != 2 OR has($a, NULL)"
                        )
                        + " = 0 AND arrayElement($a, 1) != 0, arrayElement($a, 2), NULL)) "
                        "AS Nullable(Int64))"
                    )
                ),
            ),
        ),
    ),
)

FEATURES: tuple[Feature, ...] = (
    INT2INT4_SUM,
    _sum("int2_sum"),
    _sum("int4_sum"),
    INT8SHL,
    *(_shift(name, *shape) for name, shape in _SHIFTS.items()),
    FACTORIAL,
    NUMERIC,
    BOOL,
    OID,
    RANDOM,
    *JSONB_NUMBERS,
)
