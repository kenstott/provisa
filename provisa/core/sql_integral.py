# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0b7c21-9d4a-4f3e-8a61-2c7d9b4e1f08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Whether a SQL expression's value is known to be an integer (REQ-2236).

SAP HANA and Db2 truncate a fraction in a cast to an integer type, where PostgreSQL rounds: on
both, ``CAST(2.5 AS INTEGER)`` is 2 and PostgreSQL's is 3. Such a cast has PostgreSQL's answer
there only when the value cast has no fraction, so each dialect writes it only for a value this
module knows to be integral, and refuses it by name otherwise.

HANA's quotient of two integers is a decimal, where PostgreSQL's is the integer truncated toward
zero, of the wider operand's type: HANA writes ``a / b`` in a truncating form only where
:func:`integer_type` knows both operands' integer types, and refuses it by name otherwise."""

# Requirements: REQ-2236
from __future__ import annotations

import sqlglot.expressions as exp

#: The integer types a cast to which truncates on HANA and Db2.
INTEGER_TYPES = frozenset({exp.DType.SMALLINT, exp.DType.INT, exp.DType.BIGINT})

#: Functions whose value is an integer whatever their arguments.
_INTEGRAL_FUNCTIONS: tuple[type[exp.Expr], ...] = (
    exp.Count,
    exp.RowNumber,
    exp.Rank,
    exp.DenseRank,
)

#: Operators whose value is an integer when every operand's is (PostgreSQL's integer division
#: truncates, as HANA's and Db2's cast does, so an integral quotient casts alike).
_INTEGRAL_OPERATORS: tuple[type[exp.Expr], ...] = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)


def integral(node: exp.Expr) -> bool:
    """Whether ``node``'s value is known to be an integer: an integer literal, a count or rank, a
    cast to an integer type, an expression whose annotated type is an integer type (an integer
    column, where the caller supplied the column types), or integer arithmetic over these."""
    if node.type is not None and node.type.this in INTEGER_TYPES:
        return True
    if isinstance(node, (exp.Paren, exp.Neg)):
        return integral(node.this)
    if isinstance(node, exp.Literal):
        return not node.is_string and node.this.isdigit()
    if isinstance(node, _INTEGRAL_FUNCTIONS):
        return True
    if isinstance(node, (exp.Floor, exp.Ceil)):
        return node.args.get("decimals") is None
    if isinstance(node, _INTEGRAL_OPERATORS):
        return integral(node.this) and integral(node.expression)
    return isinstance(node, exp.Cast) and node.to.this in INTEGER_TYPES


#: The integer types in PostgreSQL's order of width: an operator over two takes the wider.
_WIDTH: dict[exp.DType, int] = {exp.DType.SMALLINT: 0, exp.DType.INT: 1, exp.DType.BIGINT: 2}

#: The largest integer literal PostgreSQL reads as an integer (int4) and as a bigint (int8); a
#: larger one is numeric.
_INT_MAX = 2**31 - 1
_BIGINT_MAX = 2**63 - 1


def integer_type(node: exp.Expr) -> exp.DType | None:
    """The PostgreSQL integer type of ``node``'s value, where it is known to be one: an integer
    literal (integer, or bigint past integer's range), a count or rank (bigint), a cast to an
    integer type, integer arithmetic (the wider operand's type), or an expression whose annotated
    type is an integer type (an integer column, where the caller supplied the column types). None
    where it is not known to be an integer type: a value can be integral and not of an integer
    type (PostgreSQL's floor of an integer is a double precision)."""
    if isinstance(node, (exp.Paren, exp.Neg)):
        return integer_type(node.this)
    if isinstance(node, exp.Literal):
        if node.is_string or not node.this.isdigit():
            return None
        value = int(node.this)
        if value <= _INT_MAX:
            return exp.DType.INT
        return exp.DType.BIGINT if value <= _BIGINT_MAX else None
    if isinstance(node, _INTEGRAL_FUNCTIONS):
        return exp.DType.BIGINT
    if isinstance(node, exp.Cast):
        return node.to.this if node.to.this in INTEGER_TYPES else None
    if isinstance(node, _INTEGRAL_OPERATORS):
        left, right = integer_type(node.this), integer_type(node.expression)
        if left is None or right is None:
            return None
        return max(left, right, key=_WIDTH.__getitem__)
    if node.type is not None and node.type.this in INTEGER_TYPES:
        return node.type.this
    return None


def fractional_integer_cast(node: exp.Cast) -> bool:
    """Whether ``node`` casts to an integer type a value not known to be integral: the cast HANA
    and Db2 truncate where PostgreSQL rounds."""
    return node.to.this in INTEGER_TYPES and not integral(node.this)
