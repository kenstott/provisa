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

import sqlglot.expressions as exp

from provisa.core.pg_error import END, MARK
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refused, Rewrite, replacement

FAMILY = "integers and numbers"


def fail(sqlstate: str, message: str) -> str:
    """Trino's raise of PostgreSQL's error ``sqlstate`` with ``message`` (provisa.core.pg_error)."""
    text = f"{MARK} {sqlstate}: {message}{END}".replace("'", "''")
    return f"fail('{text}')"


def refused(function: str) -> str:
    """Trino's raise of the refusal of a value ``function`` does not read as PostgreSQL does."""
    return fail("0A000", f"{function} does not read this value")


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

FACTORIAL = Feature(
    "factorial",
    FAMILY,
    "The product of the integers from 1 to n.",
    read=Side(
        rules=(
            Rule(
                "factorial($n)",
                trino.trino_only(
                    "CASE WHEN $n IS NULL THEN CAST(NULL AS DECIMAL(38, 0)) "
                    f"WHEN $n < 0 THEN {fail('22003', 'factorial of a negative number is undefined')} "
                    f"WHEN $n > {_LARGEST_FACTORIAL} THEN {refused('factorial')} "
                    "ELSE reduce(sequence(1, greatest($n, 1)), CAST(1 AS DECIMAL(38, 0)), "
                    "(_s, _x) -> _s * _x, _s -> _s) END"
                ),
            ),
        ),
        limitations=dict.fromkeys(
            trino.TRINO,
            (
                Limitation(
                    f"Past {_LARGEST_FACTORIAL}, whose factorial a decimal of 38 digits does not "
                    "hold, refused with 0A000, where PostgreSQL answers its exact digits."
                ),
            ),
        ),
    ),
)

_INTEGERS = frozenset({"smallint", "integer", "bigint"})
_JSON = frozenset({"json", "jsonb"})


def ch_fail(sqlstate: str, message: str) -> str:
    """ClickHouse's raise of PostgreSQL's error ``sqlstate`` with ``message`` where ``condition``
    holds: throwIf, which answers 0 where it does not, so a form adds it to its answer."""
    text = f"{MARK} {sqlstate}: {message}{END}".replace("'", "''")
    return "throwIf(ifNull({condition}, 0), '" + text + "')"


#: ClickHouse's name of each type a cast row writes, as a Nullable: every value may be NULL.
_CLICKHOUSE_TYPES = {
    "DECIMAL(38, 0)": "Nullable(Decimal(38, 0))",
    "BOOLEAN": "Nullable(Bool)",
}


def _cast_of(function: str, sources: frozenset[str], sql_type: str):
    """``function`` of a value said to be of ``sources``: its cast to ``sql_type``. Of a json or
    jsonb value, refused by name: Trino's and ClickHouse's casts read a JSON scalar PostgreSQL
    refuses; of anything else, kept (an argument no statement says the type of is ClickHouse's
    unknown function, an engine error)."""

    def cast(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or len(call[1]) != 1:
            return None
        type_said = trino.said(call[1][0])
        if type_said in _JSON:
            raise Refused(trino.no_form(function, dialect))
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
            limitations=dict.fromkeys(
                (*trino.TRINO, "clickhouse"),
                (Limitation("Of a json or jsonb value, refused by name."),),
            ),
        ),
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

FEATURES: tuple[Feature, ...] = (
    _sum("int2_sum"),
    _sum("int4_sum"),
    INT8SHL,
    *(_shift(name, *shape) for name, shape in _SHIFTS.items()),
    FACTORIAL,
    NUMERIC,
    BOOL,
    OID,
    RANDOM,
)
