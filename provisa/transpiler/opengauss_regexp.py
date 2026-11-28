# Copyright (c) 2026 Kenneth Stott
# Canary: 9b615154-af73-4156-9bfe-4b8bc30bed78
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""regexp_replace from a start on openGauss, its first match replaced (REQ-2236).

PostgreSQL's regexp_replace(source, pattern, replacement, start) replaces the first match at or
after start. openGauss's four-argument call is its Oracle-compatible
regexp_replace(source, pattern, replace_string, position), whose occurrence defaults to 0, every
match: regexp_replace('abc', 'x*', 'X', 3) answered 'abXcX', where PostgreSQL answers 'abXc'. Its
five-argument call reads occurrence as PostgreSQL reads N, so a start that is evidently an integer
(a literal, or a cast to an integer type) is given N 1. A start of no evident type is left as
written: sqlglot reads a fourth argument as the start unless it is text, and PostgreSQL chooses the
overload by its type.

openGauss keeps the postgres dialect; this step runs for the openGauss engine kind only.
"""

# Requirements: REQ-2236

from __future__ import annotations

import sqlglot.expressions as exp

KIND = "opengauss"

_INTEGERS = (exp.DType.TINYINT, exp.DType.SMALLINT, exp.DType.INT, exp.DType.BIGINT)


def mentioned(pg_sql: str) -> bool:
    """Whether ``pg_sql`` may call regexp_replace: a cheap text test before a parse."""
    return "regexp_replace" in pg_sql.lower()


def _evidently_integer(node: exp.Expr) -> bool:
    node = node.unnest()
    if isinstance(node, exp.Literal):
        return not node.is_string and node.is_int
    return isinstance(node, (exp.Cast, exp.TryCast)) and node.to.is_type(*_INTEGERS)


def _first_match(node: exp.Expr) -> exp.Expr:
    if (
        isinstance(node, exp.RegexpReplace)
        and node.args.get("position") is not None
        and node.args.get("occurrence") is None
        and node.args.get("modifiers") is None
        and _evidently_integer(node.args["position"])
    ):
        node.set("occurrence", exp.Literal.number(1))
    return node


def first_match_from_start(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each four-argument regexp_replace from an integer start given N 1."""
    return tree.transform(_first_match, copy=False)
