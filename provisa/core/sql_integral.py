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
module knows to be integral, and refuses it by name otherwise."""

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


def fractional_integer_cast(node: exp.Cast) -> bool:
    """Whether ``node`` casts to an integer type a value not known to be integral: the cast HANA
    and Db2 truncate where PostgreSQL rounds."""
    return node.to.this in INTEGER_TYPES and not integral(node.this)
